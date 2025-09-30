import asyncio
import sqlite3
import json
import os
import time
import shutil
import numpy
import aiohttp

import calendar


class analytics:
    def __init__(self, data_dir="data", verbosity="DEBUG", sleep = 10, market_version=1, rpc_version=1):
        self.__data_dir = data_dir 
        self.__verbosity = verbosity.upper()
        self.__market_version = market_version
        self.__markets_db = None
        self.__last_market_row = 0

        #maps token_id to struct{last row index, sql db conn, dictionary BIDS_local book, dictionary BIDS_remote, dictionary ASKS_local book, dict ASKS_remote book}, the book dictionaries map a real to a real number
        self.__events_map = {}
        self.__rpc_version = rpc_version
        self.__block_db = None
        self.__last_block_row = 0
        self.__running = False
        self.__sleep_time = sleep

        self.__https_cli = None
        self.__https_url = "https://clob.polymarket.com/books"
        self.__request_body = []
    
    async def start(self)->bool:
        if self.__running:
            self.__log("analytics already started", "ERROR")
            return False

        # let other stuff start up
        await asyncio.sleep(self.__sleep_time)

        try:
            # Open markets db
            markets_path = os.path.join(self.__data_dir, f"markets.db")
            if not os.path.exists(markets_path):
                self.__log(f"analytics missing markets db: {markets_path}", "ERROR")
                return False
            self.__markets_db = sqlite3.connect(markets_path)
            self.__log(f"analytics connected to markets db at {markets_path}", "DEBUG")

            # Open blocks db
        
            #blocks_path = os.path.join(self.__data_dir, f"blocks_{self.__rpc_version}.db")
            #if not os.path.exists(blocks_path):
            #    self.__log(f"analytics missing blocks db: {blocks_path}", "ERROR")
             #   return False
            #self.__block_db = sqlite3.connect(blocks_path)
            #self.__log(f"analytics connected to blocks db at {blocks_path}", "DEBUG")
 
        except Exception as e:
            self.__log(f"analytics failed to start: {e}", "ERROR")
          # Create aiohttp client session with persistent connections
        connector = aiohttp.TCPConnector(limit_per_host=3, keepalive_timeout=99999)
        self.__https_cli = aiohttp.ClientSession(connector=connector)

        self.__running = True
        while self.__running:
            if not await self.__new_markets() or not await self.__new_events():
                await self.__clean_up()
                return False
            await asyncio.sleep(self.__sleep_time)


    async def stop(self) -> bool:
        """
        Stop the analytics loop and clean up resources.
        """
        if not self.__running:
            self.__log("analytics is not running", "WARNING")
            return False

        self.__running = False
        await self.__clean_up()
        self.__log("analytics stopped successfully", "INFO")
        return True


    async def __clean_up(self):
        """
        Close all database connections and HTTP client session.
        """
        try:
            # Close all token event DB connections
            for token_id, info in self.__events_map.items():
                conn = info.get("conn")
                if conn:
                    try:
                        conn.close()
                        self.__log(f"Closed DB connection for token_id={token_id}", "DEBUG")
                    except Exception as e:
                        self.__log(f"Failed to close DB for token_id={token_id}: {e}", "ERROR")

            # Close markets DB
            if self.__markets_db:
                try:
                    self.__markets_db.close()
                    self.__log("Closed markets DB", "DEBUG")
                except Exception as e:
                    self.__log(f"Failed to close markets DB: {e}", "ERROR")

            # Close blocks DB
            if self.__block_db:
                try:
                    self.__block_db.close()
                    self.__log("Closed blocks DB", "DEBUG")
                except Exception as e:
                    self.__log(f"Failed to close blocks DB: {e}", "ERROR")

            # Close HTTP client session
            if self.__https_cli:
                try:
                    await self.__https_cli.close()
                    self.__log("Closed HTTP client session", "DEBUG")
                except Exception as e:
                    self.__log(f"Failed to close HTTP client: {e}", "ERROR")

        except Exception as e:
            self.__log(f"Error during cleanup: {e}", "ERROR")

    
    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")
     
    async def __new_markets(self)->bool:
        """
        check if there is row index > last_market_row, 
        is yes then for all new markets and their token ids, add a sqllite conn in _events_map from token_id to its database and set their last row index to 0
        
        """
        self.__log(f"analytics looking for new markets at {time}", "DEBUG")
        cur = self.__markets_db.cursor()
        cur.execute("""
            SELECT row_index, insert_time, market_id, token_id_1, token_id_2
            FROM markets
            WHERE row_index > ?
            ORDER BY row_index ASC
        """, (self.__last_market_row,))

        new_markets = cur.fetchall()

        market_count = len(new_markets)
        self.__log(f"analytics {market_count} new markets from index inclusive {self.__last_market_row + 1}", "INFO")
        self.__last_market_row += market_count

        for market in new_markets:
            market_id = market[2]
            self.__log(f"analytics new markets {market_id}", "DEBUG")
            if not self.__add_token(market_id, market[3]) \
            or not self.__add_token(market_id, market[4]):
                return False
            self.__request_body.append({"token_id": market[3]})
            self.__request_body.append({"token_id": market[4]})

        return True

    def __add_token(self, market_id: str, token_id: str) -> bool:
        """
        Initialize an events database connection for a token and add it to _events_map.
        Structure:
            {
                "last_row_index": 0,
                "conn": sqlite3.Connection,
                "bids_local": {},
                "bids_remote": {},
                "asks_local": {},
                "asks_remote": {}
            }
        """
        if token_id in self.__events_map:
            self.__log(f"analytics token_id={token_id} already has a DB connection", "DEBUG")
            return False
        try:
            events_dir = os.path.join(self.__data_dir, f"{market_id} {token_id}")
            events_db_path = os.path.join(events_dir, f"events_{self.__market_version}.db")

            if not os.path.exists(events_dir):
                self.__log(f"analytics directoty {market_id} {token_id} missing", "ERROR")
                return False

            conn = sqlite3.connect(events_db_path)
        except Exception as e:
            self.__log(f"analytics add_token failed for market_id={market_id} token_id={token_id}: {e}", "ERROR")
            return False
        # Add to _events_map
        self.__events_map[token_id] = {
            "last_server_time": 0,
            "conn": conn,
            "remote_book_time": None,
            "bids_local": [],
            "bids_remote": [],
            "bids_book_distance": None,
            "asks_local": [],
            "asks_remote": [],
            "asks_book_distance": None,
        }

        self.__log(f"analytics added token_id={token_id} in market {market_id}", "DEBUG")
        return True

    async def __new_events(self)->bool:
        try:
            response = await self.__https_cli.post(self.__https_url, json=self.__request_body)
            if response.status != 200:
                self.__log(f"analytics https response not ok: {response.status}", "ERROR")
                await response.release()
                return False

            books = await response.json()
            await response.release()  # release the connection back to the pool
        except aiohttp.ClientError as e:
            self.__log(f"analytics https request failed: {e}", "ERROR")
            return False
        
        if len(self.__request_body) != len(books):
            self.__log(f"analytics requested {len(self.__request_body)} books but got {len(books)} books:", "ERROR")
            return False
        
        for book in books:
            token_id = book["asset_id"]
            #turn timestamp into integer offset in seconds since unix epoch
            # Convert timestamp string to integer seconds since Unix epoch
            try:
                timestamp = int(time.mktime(time.strptime(book["timestamp"], "%Y-%m-%dT%H:%M:%SZ")))
            except Exception as e:
                self.__log(f"analytics failed to parse timestamp for token_id={token_id}: {e}", "ERROR")
                return False
            self.__events_map[token_id]["remote_book_time"] = timestamp
            self.__events_map[token_id]["asks_remote"] = book["asks"]
            self.__events_map[token_id]["bids_remote"] = book["bids"]

            if not self.__create_local_books(token_id, timestamp):
                return False

            if not self.__book_distance(token_id):
                return False
    
        if not self.__print_report():
            return False

    def __create_local_books(self, token_id, remote_timestamp)->bool:
        remote_timestamp *= 1000 # in ms
        #query rows from _events_map[token_id][conn] where row_index > _events_map[token_id][last_row_index]
        try:
            conn = self.__events_map[token_id]["conn"]
            last_time = self.__events_map[token_id]["last_server_time"]
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            cur.execute("""
                SELECT server_time, cli_time, event_object
                FROM events
                WHERE server_time > ? AND server_time <= ?
                ORDER BY server_time ASC
            """, (last_time, remote_timestamp))

            rows = cur.fetchall()
        except Exception as e:
            self.__log(f"analytics failed to create local books for token_id={token_id}: {e}", "ERROR")
            return False

        self.__events_map[token_id]["last_server_time"] = remote_timestamp

        for row in rows:
            row_index = row["row_index"]
            event_obj = json.loads(row["event_object"])
            event_type = event_obj["event_type"]

            if not isinstance(event_type, str):
                self.__log(f"analytics no event type in {token_id} db at row {row_index}", "ERROR")
                return False
            local_bids = self.__events_map[token_id]["bids_local"]
            local_asks = self.__events_map[token_id]["asks_local"]

            side = event_obj.get("side")
            quantity = event_obj.get("size")
            price = event_obj.get("price")

            if event_type == "book":
                local_bids = event_obj["bids"]
                local_asks = event_obj["asks"]
            elif event_type == "price_change":
                if not isinstance(quantity, str) or not isinstance(price, str):
                    self.__log(f"analytics invalid size {quantity }or price {price} for price change", "ERROR")
                    return False
                if side == "BUY":
                    local_bids[price] = str(float(local_bids[price]) + float(quantity))
                elif side == "SELL":
                    local_asks[price] = str(float(local_asks[price]) + float(quantity))    
                else: 
                    self.__log(f"analytics invalid side: {side} for price change", "ERROR")
                    return False
            elif event_type == "last_trade_price":
                if not isinstance(quantity, str) or not isinstance(price, str):
                    self.__log(f"analytics invalid size {quantity }or price {price} for last_trade_price", "ERROR")
                    return False
                if side == "BUY":
                    local_bids[price] = str(float(local_bids[price]) - float(quantity))
                elif side == "SELL":
                    local_asks[price] = str(float(local_asks[price]) - float(quantity))    
                else: 
                    self.__log(f"analytics invalid side: {side} for last_trade_price", "ERROR")
                    return False

        self.__log(f"analytics recrated local books for token id {token_id}, last time {last_time}, remote time {remote_timestamp}\n local bids: {local_asks}\n remote bids: {self.__events_map[token_id]['bids_remote']}", "DEBUG")
        return True
    
    def __book_distance(self, token_id)->bool:
        """
        distance metric should be base on:
            1) books can be seen as pair (price, quantity)
            2) we can order the sets based on how their price
                2.1) For bids, (price1, quantity1) > (price2, quantity2) iff price1 < price2 (lower selling price)
                2.2) For asks, (price1, quantity1) > (price2, quantity2) iff price1 > price2 (higher buying price)
            This way the order reflects how close they are to the market_prices

            3) Now we can use this order to index into the sets ordered descendingly, 
            and define a distance for 2 pairs on the same side at an index i: d_side(i, p1, p2)
            which also defined for all i, even in the case that one p1 or p2 are None (sets are not same length)

            4) Now we can construct a distance D_side(side1, side2) which uses d_side(i, p1, p2)

            5) The idea for d_side(i, p1, p2) is to penalize progressively less for larger i:
                case p1 or p2 is None: the d_side is 0 for now, independent of i
                otherwise d_side(i, p1, p2) = (p1-p2)^(1/(i+1)), where p1-p2 = (|price1-price2| + |quantity1-quantity2|)/2

            6) D_side(side1, side2) simply is the sum of d_side(i, p1, p2) for all  i
        """ 
        try:
            local_bids = self.__events_map[token_id]["bids_local"]
            remote_bids = self.__events_map[token_id]["bids_remote"]
            local_asks = self.__events_map[token_id]["asks_local"]
            remote_asks = self.__events_map[token_id]["asks_remote"]

            # Convert dicts {price_str: qty_str} -> list of tuples [(price, qty)]
            local_bids_list = [(float(p), float(q)) for p, q in local_bids.items()]
            remote_bids_list = [(float(p), float(q)) for p, q in remote_bids.items()]
            local_asks_list = [(float(p), float(q)) for p, q in local_asks.items()]
            remote_asks_list = [(float(p), float(q)) for p, q in remote_asks.items()]

            # Sort bids descending by price (higher price = better bid)
            local_bids_sorted = sorted(local_bids_list, key=lambda x: -x[0])
            remote_bids_sorted = sorted(remote_bids_list, key=lambda x: -x[0])

            # Sort asks ascending by price (lower price = better ask)
            local_asks_sorted = sorted(local_asks_list, key=lambda x: x[0])
            remote_asks_sorted = sorted(remote_asks_list, key=lambda x: x[0])

            def d_side(i, p1, p2):
                if p1 is None or p2 is None:
                    return 0.0
                price_diff = abs(p1[0] - p2[0])
                qty_diff = abs(p1[1] - p2[1])
                return ((price_diff + qty_diff) / 2) ** (1 / (i + 1))

            def D_side(list1, list2):
                max_len = max(len(list1), len(list2))
                dist = 0.0
                for i in range(max_len):
                    p1 = list1[i] if i < len(list1) else None
                    p2 = list2[i] if i < len(list2) else None
                    dist += d_side(i, p1, p2)
                return dist

            bids_distance = D_side(local_bids_sorted, remote_bids_sorted)
            asks_distance = D_side(local_asks_sorted, remote_asks_sorted)

            self.__events_map[token_id]["bids_book_distance"] = bids_distance
            self.__events_map[token_id]["asks_book_distance"] = asks_distance

            return True
        except Exception as e:
            self.__log(f"analytics failed to compute book distance for token_id={token_id}: {e}", "ERROR")
            return False
    
    def __print_report(self):
        """
        Computes percentiles for book distances across all tokens.
        Percentiles: 0.1, 0.2, ..., 0.9, 0.95, 0.975
        """
        try:
            # Collect all distances
            bids_distances = [v["bids_book_distance"] for v in self.__events_map.values() if v["bids_book_distance"] is not None]
            asks_distances = [v["asks_book_distance"] for v in self.__events_map.values() if v["asks_book_distance"] is not None]

            if not bids_distances and not asks_distances:
                self.__log("No book distances available to report.", "INFO")
                return False

            percentiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.975]

            self.__log("Book distance percentiles report:", "INFO")
            self.__log("Bids:", "INFO")
            for p in percentiles:
                value = numpy.percentile(bids_distances, p*100)
                self.__log(f"  {int(p*100)}th percentile: {value:.6f}", "INFO")

            self.__log("Asks:", "INFO")
            for p in percentiles:
                value = numpy.percentile(asks_distances, p*100)
                self.__log(f"  {int(p*100)}th percentile: {value:.6f}", "INFO")

            return True
        except Exception as e:
            self.__log(f"analytics failed to print report: {e}", "ERROR")
            return False


test = analytics()

asyncio.run(test.start());
            
                



