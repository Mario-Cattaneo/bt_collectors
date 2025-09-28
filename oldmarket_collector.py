import asyncio
import aiohttp
import sqlite3
import json
import websockets
from datetime import datetime, timezone
import os
import time
import shutil


class market_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", drop_tables=True):
        # sql resources
        self.__data_dir = data_dir
        self.__markets_db = None
        self.__events_dbs_map = {}
        self.__drop_tables = drop_tables

        # logging
        self.__verbosity = verbosity.upper()

        # markets endpoint
        self.__markets_url = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
        self.__markets_cli = None
        self.__market_offset = 105000

        # events endpoint
        self.__events_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.__events_cli = None
        self.__running = False

        # resubscription
        self.__resubscription_count = 0
        self.__do_resubscribe = False

        # scheduling
        self.__finds_before_reads = 2
        self.__curr_find_count = 0
    
    async def start(self)->bool:
        # Can't start if already running
        if self.__running:
            self.__log("market_collector already started", "ERROR")
            return False
        try:
            # Ensure root data directory exists 
            os.makedirs(self.__data_dir, exist_ok=True)
            markets_db_path = os.path.join(self.__data_dir, "markets.db")

            # Store persistent connection
            self.__markets_db = sqlite3.connect(markets_db_path)
            cur = self.__markets_db.cursor()

            if self.__drop_tables:
                cur.execute("DROP TABLE IF EXISTS markets")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS markets  (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    market_id TEXT,
                    token_id_1 TEXT,
                    token_id_2 TEXT,
                    market_object TEXT
                )
            """)
            self.__markets_db.commit()
        except (OSError, sqlite3.Error) as e:
            self.__log(f"market_collector to start: {e}", "ERROR")
            return False

        # At most 3 sockets for pipeling and keepalive forever
        connector = aiohttp.TCPConnector(limit_per_host=3, keepalive_timeout=99999)
        self.__markets_cli = aiohttp.ClientSession(connector=connector)
        
        self.__running = True
        self.__log("market_collector started", "INFO")

        while self.__running:
            if not await self.__find_markets():
                await self.__clean_up()
                return False
        self.__log(f"market_collector exiting running loop due to abort", "DEBUG")


    async def stop(self)->bool:
        await self.__clean_up()
        self.__log("market_collector stopped", "DEBUG")
        return True;


    async def __clean_up(self):
        self.__log("market_collector cleanup started", "DEBUG")

        # Close aiohttp client
        if self.__markets_cli is not None:
            try:
                await self.__markets_cli.close()
                self.__log("Closed markets aiohttp client", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing markets aiohttp client: {e}", "ERROR")
            self.__markets_cli = None

        # Close websocket client
        if self.__events_cli is not None:
            try:
                await self.__events_cli.close()
                self.__log("Closed events websocket client", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing websocket client: {e}", "ERROR")
            self.__events_cli = None

        # Close main markets_1.db
        if self.__markets_db is not None:
            try:
                self.__markets_db.close()
                self.__log("Closed markets_1.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing markets_1.db: {e}", "ERROR")
            self.__markets_db = None

        # Close all event DBs
        for token_id, conn in list(self.__events_dbs_map.items()):
            try:
                conn.close()
                #self.__log(f"Closed DB for token_id={token_id}", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing DB for token_id={token_id}: {e}", "ERROR")
        self.__events_dbs_map.clear()

        self.__running = False

        self.__log("market_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")

    
    async def __query_markets(self)->bool:
        url = self.__markets_url.format(offset=self.__market_offset)
        try:
            # Reuse the same session -> TCP + TLS persistent
            resp = await self.__markets_cli.get(url)
            if resp.status != 200:
                self.__log(f"market fetch failed: {resp.status}", "ERROR")
                await resp.release()
                return False

            market_arr = await resp.json()
            await resp.release()  # release the connection back to the pool

            # Process markets here
        except aiohttp.ClientError as e:
            self.__log(f"HTTP request failed: {e}", "ERROR")
            return False

        self.__log(f"market_collector fetched {len(market_arr)} markets", "DEBUG")

        if not isinstance(market_arr, list):
            self.__log(f"market_collector market response not list but {type(market_arr)}", "ERROR")
            return False

        for market_obj in market_arr:
            if not isinstance(market_obj, dict):
                self.__log(f"market_collector market_obj is not dict but {type(market_obj)}", "ERROR")
                return False

            market_id = market_obj.get("id")
            if not isinstance(market_id, str):
                self.__log(f"market_collector attribute market_id is not str but {type(market_id)}", "ERROR")
                return False
            
            #closed is of type bool I want to make sure it exists is of type bool
            closed = market_obj.get("closed")
            if not isinstance(closed, bool):
                self.__log(f"closed is not bool but {type(closed)}", "ERROR")
                return False
            if closed:
                continue
            

            token_ids_json = market_obj.get("clobTokenIds")
            if not isinstance(token_ids_json, str):
                self.__log(f"market_collector attribute token_ids_json is not str but {type(token_ids_json)}", "ERROR")
                return False
                
            # is encoded as a json seperate json string array lmao
            token_ids = json.loads(token_ids_json)
            if not isinstance(token_ids, list):
                self.__log(f"market_collector token_ids is not list but {type(token_ids)}", "ERROR")
                return False
            
            if len(token_ids) != 2:
                self.__log(f"market_collector expected 2 token_ids but got {len(token_ids)}", "ERROR")
                return False

            for token_id in token_ids:
                if not isinstance(token_id, str):
                    self.__log(f"market_collector attribute token_id is not str but {type(token_id)}", "ERROR")
                    return False
                if not self.__register_token(market_id, token_id):
                    return False
            
            if not self.__sql_insert_market(market_id,token_ids[0], token_ids[1], market_obj):
                return False

        new_markets = len(market_arr)
        self.__market_offset += new_markets

        self.__curr_find_count += 1

        # case potentially more markets to find
        if new_markets == 500 and self.__curr_find_count < self.__finds_before_reads:
            self.__log(f"market_collector found 500 markets to offset {self.__market_offset}, looking for more", "DEBUG")
            self.__do_resubscribe = True
            return True

        # case new subscriptions but no more markets to find
        elif new_markets > 0 or self.__do_resubscribe:
            self.__log(f"market_collector found all new markets until offset {self.__market_offset}", "DEBUG")
            return await self.__resubscribe()

        # case no new subscriptions
        self.__log(f"market_collector no new markets at offset {self.__market_offset}", "DEBUG")
        return await self.__read_events()

    def __sql_insert_market(self, market_id, token_id_1, token_id_2, market_obj) -> bool:
        try:
            ts = int(time.time())
            cur = self.__markets_db.cursor()
            cur.execute(
                "INSERT INTO markets (insert_time, market_id, token_id_1, token_id_2, market_object) VALUES (?, ?, ?, ?, ?)",
                (ts, market_id, token_id_1, token_id_2, json.dumps(market_obj))
            )
            self.__markets_db.commit()
            return True
        except sqlite3.Error as e:
            self.__log(f"market_collector failed to insert market {market_id}: {e}", "ERROR")
            return False

 
    def __sql_insert_event(self, asset_id, server_time, market, event_type, event_obj)->bool:
        try:
            # millisecond unix timestamp
            cli_time = time.time_ns() // 1_000_000
            server_time_int = int(server_time)
            print(f"difference {cli_time-server_time_int}ms, cli_time {cli_time}ms, server_time {server_time_int}ms")
            conn = self.__events_dbs_map.get(asset_id)

            if conn is None:
                self.__log(f"market_collector no DB found for asset_id={asset_id}", "ERROR")
                return False
            cur = conn.cursor()

            # encodes side as bool
            side = event_obj.get("side") == "BUY"

            if event_type == "book":
                cur.execute(
                    """
                    INSERT INTO books (server_time, cli_time, market, bids, asks, hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        server_time_int,
                        cli_time,
                        market,
                        json.dumps(event_obj.get("bids", [])),
                        json.dumps(event_obj.get("asks", [])),
                        event_obj.get("hash"),
                    ),
                )
            elif event_type == "price_change":
                cur.execute(
                    """
                    INSERT INTO price_changes (server_time, cli_time, market, price, size, side, hash, best_bid, best_ask)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        server_time_int,
                        cli_time,
                        market,
                        float(event_obj.get("price")),
                        float(event_obj.get("size")),
                        side,
                        event_obj.get("hash"),
                        float(event_obj.get("best_bid")),
                        float(event_obj.get("best_ask")),
                    ),
                )

            elif event_type == "last_trade_price":
                cur.execute(
                    """
                    INSERT INTO last_trades (server_time, cli_time, fee_rate_bps, market, price, side, size)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        server_time_int,
                        cli_time,
                        float(event_obj.get("fee_rate_bps")),
                        market,
                        float(event_obj.get("price")),
                        side,
                        float(event_obj.get("size")),
                    ),
                )
            elif event_type == "tick_size_change":
                cur.execute(
                    """
                    INSERT INTO tick_changes (server_time, cli_time, old_tick_size, new_tick_size)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        server_time_int,
                        cli_time,
                        float(event_obj.get("old_tick_size")),
                        float(event_obj.get("new_tick_size")),
                    ),
                )
            else:
                self.__log(f"mark_collector invalid event_type passed to sql insert: {event_type}")
                return False
            conn.commit()
            return True
        except sqlite3.Error as e:
            self.__log(f"market_collector failed to insert event for asset_id={asset_id}: {e}", "ERROR")
            return False

    def __register_token(self, market_id, token_id)->bool:
        directory_name = f"{market_id}_{token_id}"
        events_dir = os.path.join(self.__data_dir, directory_name)
        db_path = os.path.join(events_dir, "events.db")

        try:
            if os.path.exists(events_dir):
                shutil.rmtree(events_dir)
                #self.__log(f"market_collector removed {directory_name} directory", "DEBUG")

            os.makedirs(events_dir, exist_ok=True)
            conn = sqlite3.connect(db_path)
            
            cur = conn.cursor()
            if self.__drop_tables:
                cur.executescript("""DROP TABLE IF EXISTS books;
                            DROP TABLE IF EXISTS price_changes;
                            DROP TABLE IF EXISTS last_trades;
                            DROP TABLE IF EXISTS tick_changes;
                            """)
            cur.executescript("""
                CREATE TABLE books (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_time INTEGER,
                    cli_time INTEGER,
                    market TEXT,
                    bids TEXT,
                    asks TEXT,
                    hash TEXT
                );
                CREATE TABLE price_changes (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_time INTEGER,
                    cli_time INTEGER,
                    market TEXT,
                    price REAL,
                    size REAL,
                    side BOOL,
                    hash TEXT,
                    best_bid TEXT,
                    best_ask TEXT
                );
                CREATE TABLE last_trades (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_time INTEGER,
                    cli_time INTEGER,
                    fee_rate_bps REAL,
                    market TEXT,
                    price REAL,
                    side BOOL,
                    size REAL
                );
                CREATE TABLE tick_changes (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_time INTEGER,
                    cli_time INTEGER,
                    old_tick_size REAL,
                    new_tick_size REAL
                );
                CREATE INDEX IF NOT EXISTS books_time ON books(server_time);
                CREATE INDEX IF NOT EXISTS price_changes_time ON price_changes(server_time);
                CREATE INDEX IF NOT EXISTS last_trades_time ON last_trades(server_time);
                """)
            conn.commit()

        except (OSError, sqlite3.Error) as e:
            self.__log(f"market_collector failed to create DB for market_id={market_id}, token_id={token_id}: {e}", "ERROR")
            return False
        
        self.__events_dbs_map[token_id] = conn
        #self.__log(f"market_collector added events pair: market_id={market_id}, token_id={token_id}, db={db_path}","DEBUG")
        return True

    async def __resubscribe(self)->bool:
        try: 
            self.__log(f"market_collector resubcribing with events_cli {type(self.__events_cli)})", "DEBUG")
            new_cli = await websockets.connect(self.__events_url)
            subscription = {
                "type": "market",
                "initial_dump": True,
                "assets_ids": list(self.__events_dbs_map.keys())
            }
            await new_cli.send(json.dumps(subscription))

            #read events one last time before switching to new wss
            if self.__events_cli is not None:
                if not await self.__read_events():
                    return False
                await self.__events_cli.close()

            self.__events_cli = new_cli
            self.__resubscription_count += 1
            self.__do_resubscribe = False;
            self.__log(f"market_collector resubcription {self.__resubscription_count} complete", "INFO")
            return True
        except Exception as e:
            self.__log(f"market_collector resubscribe failed: {e}", "ERROR")
            return False

    async def __read_events(self)->bool:
        # handle all buffered received messages
        while True:
            try:
                message = await asyncio.wait_for(self.__events_cli.recv(), timeout=0.001)
                self.__log(f"Received raw message: {message}", "DEBUG")  # log the raw message
            except asyncio.TimeoutError:
                self.__log(f"market_collector emptied received messaged buffer","DEBUG")
                break
            msg_json = json.loads(message)
            if isinstance(msg_json, dict):
                if(not self.__insert_event(msg_json)):
                    self.__log(f"market_collector insert of a event_obj failed","DEBUG")
                    return False
            elif isinstance(msg_json, list):
                for event_obj in msg_json:
                    if(not self.__insert_event(event_obj)):
                        self.__log(f"market_collector insert of a event_obj failed","DEBUG")
                        return False
            else:
                self.__log(f"market_collector wss message received which is neither json array nor json object","DEBUG")
                return False

        return True

    def __insert_event(self, event_obj)->bool:
        asset_id = event_obj.get("asset_id")
        server_timestamp = event_obj.get("timestamp")
        market = event_obj.get("market")
        event_type = event_obj.get("event_type")

        # case book, last trade price, tick size change
        if isinstance(asset_id, str):
            if not self.__sql_insert_event(asset_id, server_timestamp, market, event_type, event_obj):
                return False
            return True

        price_changes = event_obj.get("price_changes")
        if not isinstance(price_changes, list):
            self.__log(f"market_collector invalid event_obj","ERROR")
            return False

        for price_change in price_changes:
            if not isinstance(price_change, dict):
                self.__log(f"market_collector invalid price changes array","ERROR")
                return False
            asset_id = price_change.get("asset_id")
            if not self.__sql_insert_event(asset_id, server_timestamp, market, event_type, price_change):
                return False

        return True
        


        

        
        