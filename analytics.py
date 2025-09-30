import asyncio
import aiohttp
import sqlite3
import json
import os
from datetime import datetime, timezone
import time
import shutil

class analytics:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True, batch_size=500):
        # sql resources
        self.__data_dir = data_dir
        self.__markets_db = None
        self.__events_db = None
        self.__analytics_db = None # overview db
        self.__reset = reset
        self.__token_ids = []
        self.__last_market_row = 0
        self.__payload = []
    
        # logging
        self.__verbosity = verbosity.upper()

        # markets endpoint
        self.__books_url = "https://clob.polymarket.com/books"
        self._analytics_cli = None
        self.__sleep = 4

        # liveness
        self.__running = False

        # version
        self.__version = 1
    
    async def start(self)->bool:
        # Can't start if already running
        if self.__running:
            self.__log("analytics already started", "ERROR")
            return False
        try:
            # Ensure directories exists 
            if not os.path.exists(self.__data_dir):
                self.__log(f"analytics datadir don't exist", "ERROR")
                return False
            
            markets_dir = os.path.join(self.__data_dir, "markets")
            if not os.path.exists(markets_dir):
                self.__log(f"analytics markets_dir don't exist", "ERROR")
                return False
            
            events_dir = os.path.join(self.__data_dir, "events")
            if not os.path.exists(events_dir):
                self.__log(f"analytics events_dir don't exist", "ERROR")
                return False
            
            events_db = os.path.join(events_dir, "events.db")
            markets_db = os.path.join(markets_dir, "markets.db")
            analytics_db_path = os.path.join(self.__data_dir, "analytics.db")
            
            # Store persistent connection to markets db
            self.__events_db = sqlite3.connect(events_db)
            self.__markets_db = sqlite3.connect(markets_db)
            self.__analytics_db = sqlite3.connect(analytics_db_path)
        except (OSError, sqlite3.Error) as e:
            self.__log(f"analytics to start: {e}", "ERROR")
            return False

        # At most 3 sockets for pipeling and keepalive forever
        connector = aiohttp.TCPConnector(limit_per_host=2, keepalive_timeout=99999)
        self._analytics_cli = aiohttp.ClientSession(connector=connector)
        
        self.__running = True
        self.__log("analytics started", "INFO")
        while True:
            if not await self.__query_markets():
                self.__log("analytics closing due to abort", "ERROR")
                await self.__clean_up()
                return False

        

    async def stop(self)->bool:
        await self.__clean_up()
        self.__log("market_collector stopped", "DEBUG")
        return True;

    async def __clean_up(self):
        self.__log("analytics cleanup started", "DEBUG")

        # Close aiohttp client
        if self._analytics_cli is not None:
            try:
                await self._analytics_cli.close()
                self.__log("analytics closed aiohttp client", "DEBUG")
            except Exception as e:
                self.__log(f"analytics error closing aiohttp client: {e}", "ERROR")
            self._analytics_cli = None


        # Close main markets.db
        if self.__markets_db is not None:
            try:
                self.__markets_db.close()
                self.__log("analytics closed markets.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"analytics error closing markets.db: {e}", "ERROR")
            self.__markets_db = None

        # Close events db
        if self.__events_db is not None:
            try:
                self.__events_db.close()
                self.__log("analytics closed events.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"analytics error closing events.db: {e}", "ERROR")
            self.__events_db = None

        if self.__analytics_db is not None:
            try:
                self.__analytics_db.close()
                self.__log("analytics closed analytics.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"analytics error closing analytics.db: {e}", "ERROR")
            self.__analytics_db = None

        self.__running = False

        self.__log("analytics cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            print(f"[{now_iso}] [{level}] {msg}")


    async def __query_markets(self)->bool:
    # check for new markets/tokens
        cursor = self.__markets_db.cursor()
        cursor.execute(f"""
            SELECT clobTokenIds1, clobTokenIds2 FROM markets WHERE row_index > {self.__last_market_row}
        """)
        new_token_pairs = cursor.fetchall()
        new_token_count = len(new_token_pairs)

        self.__last_market_row += new_token_count

        self.__log(f"analytics found {new_token_pairs} pairs from markets db", "INFO")

        for [tok1, tok2] in new_token_pairs:
            self.__payload.append({"token_id":tok1})
            self.__payload.append({"token_id":tok2})
            self.__token_ids.append(tok1)
            self.__token_ids.append(tok2)

        if new_token_count > 0 and not self.__create_tables(new_token_pairs):
            self.__log(f"analytics failed to create tables", "ERROR")
            return False

        try:
            # POST with JSON body
            resp = await self._analytics_cli.post(self.__books_url, json=self.__payload)

            if resp.status != 200:
                self.__log(f"analytics fetch failed: {resp.status}", "ERROR")
                await resp.release()
                return False

            books = await resp.json()
            resp.release()  # release the connection back to the pool

            # Process markets here
        except aiohttp.ClientError as e:
            self.__log(f"analytics HTTP request failed: {e}", "ERROR")
            return False

        self.__log(f"analytics fetch succeeded going to sleep for {self.__sleep} before doing analytics", "INFO")
        await asyncio.sleep(self.__sleep_time)
            
        if not self.__process_response(books):
            self.__log(f"analytics failed to process response for {json.dumps(books)}", "ERROR")
            return False
        return True

    
    def __create_tables(self, token_pair_list)->bool:
        buffer = []
        for toks in token_pair_list:
            if len(toks) != 2:
                self.__log(f"event collector create tables found {len(toks)} token ids for a market")
            for tok in toks:
                buffer.append(f"""CREATE TABLE IF NOT EXISTS analytics_{tok} 
                    (row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time TEXT,
                    bids_depth_difference INTEGER,
                    asks_depth_difference INTEGER,
                    ordered_bids_distance REAL,
                    ordered_asks_distance REAL,
                    local_bids TEXT,
                    local_asks TEXT,
                    fetched_bids TEXT,
                    fetched_asks TEXT,
                    server_time INTEGER);""")
        create_table_stmt = "".join(buffer)
        try:
            cursor = self.__analytics_db.cursor()
            cursor.executescript(create_table_stmt)
            self.__analytics_db.commit()
            self.__log(f"analytics created new tables succesfully", "DEBUG")
            return True
        except Exception as e:
            self.__log(f"analytics failed to create new tables for {create_table_stmt}", "DEBUG")
            return False

    def __process_response(self, books):
        """
        Process a list of order books, compute local book distances,
        and insert results into the analytics database.
        """
        # Ensure books is a list
        if not isinstance(books, list):
            if not isinstance(books, dict):
                self.__log(f"analytics invalid books input: {books}", "ERROR")
                return False
            books = [books]

        if len(books) == 0:
            self.__log("analytics empty books array", "WARN")
            return False

        cursor = self.__analytics_db.cursor()
        success = True  # track overall success

        for book in books:
            try:
                tok = book["asset_id"]
                time_str = book["timestamp"]
                bids = book["bids"]
                asks = book["asks"]
            except KeyError as e:
                self.__log(f"analytics invalid book, missing {e}: {json.dumps(book)}", "ERROR")
                success = False
                continue

            if not isinstance(bids, list):
                self.__log(f"analytics bids not list: {json.dumps(bids)}", "ERROR")
                success = False
                continue
            if not isinstance(asks, list):
                self.__log(f"analytics asks not list: {json.dumps(asks)}", "ERROR")
                success = False
                continue

            # Create local book
            try:
                bids_dist, loc_bids, asks_dist, loc_asks = self.__create_loc_book(tok, time_str)
            except Exception as e:
                self.__log(f"analytics failed to create local book for token {tok}: {e}", "ERROR")
                success = False
                continue

            # Compute depth differences
            bids_diff = abs(len(loc_bids) - len(bids))
            asks_diff = abs(len(loc_asks) - len(asks))

            # Current UTC time in ISO format
            now = datetime.now(timezone.utc)
            iso_str = now.isoformat(timespec="microseconds").replace("+00:00", "Z")

            # Prepare insert
            insert_sql = f"""
                INSERT INTO analytics_{tok} (
                    insert_time,
                    bids_depth_difference,
                    asks_depth_difference,
                    ordered_bids_distance,
                    ordered_asks_distance,
                    local_bids,
                    local_asks,
                    fetched_bids,
                    fetched_asks,
                    server_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                iso_str,
                bids_diff,
                asks_diff,
                bids_dist,
                asks_dist,
                json.dumps(loc_bids),
                json.dumps(loc_asks),
                json.dumps(bids),
                json.dumps(asks),
                time_str
            )

            # Insert with error handling per book
            try:
                cursor.execute(insert_sql, params)
            except Exception as e:
                self.__log(f"analytics failed to insert book for token {tok}: {e}", "ERROR")
                success = False
                continue

        # Commit all inserts once
        try:
            self.__analytics_db.commit()
        except Exception as e:
            self.__log(f"analytics failed to commit transaction: {e}", "ERROR")
            success = False

        return success

    def __process_response(self, books):
        """Process order books, compute distances, and insert analytics with detailed logging."""
        if not isinstance(books, list):
            if not isinstance(books, dict):
                self.__log(f"analytics invalid books input: {books}", "ERROR")
                return False
            books = [books]

        if len(books) == 0:
            self.__log("analytics empty books array", "WARN")
            return False

        cursor = self.__analytics_db.cursor()
        success = True

        for book in books:
            try:
                tok = book["asset_id"]
                time_str = book["timestamp"]
                bids = book["bids"]
                asks = book["asks"]
            except KeyError as e:
                self.__log(f"analytics invalid book, missing {e}: {json.dumps(book)}", "ERROR")
                success = False
                continue

            if not isinstance(bids, list) or not isinstance(asks, list):
                self.__log(f"analytics bids/asks invalid: bids={json.dumps(bids)}, asks={json.dumps(asks)}", "ERROR")
                success = False
                continue

            # Debug log for raw input book
            self.__log(f"Processing book for token {tok}: {json.dumps(book)}", "DEBUG")

            try:
                bids_dist, loc_bids, asks_dist, loc_asks = self.__create_loc_book(tok, time_str)
                self.__log(f"Local book for {tok} after processing: bids={json.dumps(loc_bids)}, asks={json.dumps(loc_asks)}", "DEBUG")
            except Exception as e:
                self.__log(f"analytics failed to create local book for token {tok}: {e}", "ERROR")
                success = False
                continue

            bids_diff = abs(len(loc_bids) - len(bids))
            asks_diff = abs(len(loc_asks) - len(asks))
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

            insert_sql = f"""
                INSERT INTO analytics_{tok} (
                    insert_time, bids_depth_difference, asks_depth_difference,
                    ordered_bids_distance, ordered_asks_distance,
                    local_bids, local_asks, fetched_bids, fetched_asks, server_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                now_iso, bids_diff, asks_diff,
                bids_dist, asks_dist,
                json.dumps(loc_bids), json.dumps(loc_asks),
                json.dumps(bids), json.dumps(asks),
                time_str
            )

            try:
                cursor.execute(insert_sql, params)
                self.__log(f"Inserted analytics for {tok} at {now_iso}", "DEBUG")
            except Exception as e:
                self.__log(f"analytics failed to insert book for token {tok}: {e}", "ERROR")
                success = False
                continue

        try:
            self.__analytics_db.commit()
            self.__log("Committed all inserts to analytics DB", "DEBUG")
        except Exception as e:
            self.__log(f"analytics failed to commit transaction: {e}", "ERROR")
            success = False

        return success

    def __create_loc_book(self, bids: dict, asks: dict, tok: str, time_str: str):
        try:
            cursor = self.__events_db.cursor()
            cursor.execute(f"SELECT * FROM book_{tok} ORDER BY insert_time DESC LIMIT 1")
            newest_book = cursor.fetchone()
        except Exception as e:
            self.__log(f"analytics failed to fetch newest book for {tok}: {e}", "ERROR")
            raise

        loc_bids = json.loads(newest_book[3])
        loc_asks = json.loads(newest_book[4])
        book_time = newest_book[5]

        self.__log(f"Newest local book for {tok}: bids={json.dumps(loc_bids)}, asks={json.dumps(loc_asks)}, book_time={book_time}", "DEBUG")

        try:
            cursor.execute(f"SELECT * FROM price_change_{tok} WHERE server_time >= ? ORDER BY server_time ASC", (book_time,))
            price_changes = cursor.fetchall()
            cursor.execute(f"SELECT * FROM last_trade_price_{tok} WHERE server_time >= ? ORDER BY server_time ASC", (book_time,))
            last_trades = cursor.fetchall()
        except Exception as e:
            self.__log(f"analytics failed to fetch price changes/trades for {tok}: {e}", "ERROR")
            raise

        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        end_ms_lb = int(dt.timestamp() * 1000) - 1000
        end_ms_ub = end_ms_lb + 2000

        critical_asks_interval = []
        critical_bids_interval = []

        ordered_bids = sorted(bids.items(), key=lambda x: x[0], reverse=True)
        ordered_asks = sorted(asks.items(), key=lambda x: x[0])

        combined = [("price_change", r) for r in price_changes] + [("last_trade", r) for r in last_trades]
        combined.sort(key=lambda x: x[1][-1])  # server_time last element

        for event_type, row in combined:
            if event_type == "price_change":
                side, price, size, time_ = row[5], row[3], row[4], row[8]
                if side == "BUY": loc_bids[price] = loc_bids.get(price, 0) + size
                elif side == "SELL": loc_asks[price] = loc_asks.get(price, 0) + size
                else:
                    self.__log(f"analytics invalid side {side} in price_change for {tok}", "ERROR")
                    raise ValueError(f"Invalid side: {side}")
            else:  # last_trade
                side, price, size, time_ = row[5], row[4], row[6], row[7]
                if side == "BUY": loc_bids[price] = loc_bids.get(price, 0) - size
                elif side == "SELL": loc_asks[price] = loc_asks.get(price, 0) - size
                else:
                    self.__log(f"analytics invalid side {side} in last_trade for {tok}", "ERROR")
                    raise ValueError(f"Invalid side: {side}")

            # Debug intermediate state per event
            self.__log(f"{event_type} update for {tok} at time {time_}: bids={json.dumps(loc_bids)}, asks={json.dumps(loc_asks)}", "DEBUG")

            if end_ms_lb <= time_ <= end_ms_ub:
                critical_bids_interval.append([self.__distance(loc_bids, ordered_bids, reverse=True), loc_bids.copy()])
                critical_asks_interval.append([self.__distance(loc_asks, ordered_asks, reverse=False), loc_asks.copy()])
            elif time_ > end_ms_ub:
                break

        min_bids_dist = min(critical_bids_interval, key=lambda x: x[0], default=[0, {}])
        min_asks_dist = min(critical_asks_interval, key=lambda x: x[0], default=[0, {}])

        self.__log(f"Final min distance for {tok}: bids={min_bids_dist}, asks={min_asks_dist}", "DEBUG")
        return [min_bids_dist[0], min_bids_dist[1], min_asks_dist[0], min_asks_dist[1]]