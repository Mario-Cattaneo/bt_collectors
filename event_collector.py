import asyncio
import websockets
import sqlite3
import json
import os
from datetime import datetime, timezone
import time
import shutil

class event_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True):
        # sql resources
        self.__data_dir = data_dir
        self.__markets_db = None
        self.__events_dir = None
        self.__events_db = None
        self.__token_ids = []
        self.__last_market_row = 0
        self.__reset = reset

        # logging
        self.__verbosity = verbosity.upper()
        self.__resubscription_count = 0

        # events endpoint
        self.__events_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.__events_cli = None

        # liveness
        self.__running = False

        # version
        self.__version = 1
    
    async def start(self)->bool:
        # Can't start if already running
        if self.__running:
            self.__log("event_collector already started", "ERROR")
            return False
        try:
            # Ensure markets is set up
            if not os.path.exists(self.__data_dir):
                self.__log(f"event_collector {self.__data_dir} not found at start")
                return False
            markets_dir = os.path.join(self.__data_dir, "markets")
            if not os.path.exists(markets_dir):
                self.__log(f"event_collector {markets_dir} not found at start")
                return False
            markets_db_path  = os.path.join(markets_dir, "markets.db")
            if not os.path.exists(markets_db_path ):
                self.__log(f"event_collector {markets_db_path } not found at start")
                return False
            self.__markets_db = sqlite3.connect(markets_db_path)

            # set up token sql
            self.__events_dir = os.path.join(self.__data_dir, "tokens")
            if self.__reset and os.path.exists(self.__events_dir):
                shutil.rmtree(self.__events_dir)
            
            os.makedirs(self.__events_dir, exist_ok=True)
            events_db_path = os.path.join(self.__events_dir, "events.db")

            # Store persistent connection to markets db
            self.__events_db = sqlite3.connect(events_db_path)
        except (OSError, sqlite3.Error) as e:
            self.__log(f"event_collector to start: {e}", "ERROR")
            return False

        self.__running = True
        self.__log("event_collector started", "INFO")

        while self.__running:
            if not await self.__query_markets():
                self.__log(f"event_collector exiting running loop due to abort", "DEBUG")
                await self.__clean_up()
                return False

    async def stop(self)->bool:
        await self.__clean_up()
        self.__log("event_collector stopped", "DEBUG")
        return True;

    async def __clean_up(self):
        self.__log("event_collector cleanup started", "DEBUG")

        # Close websocket client
        if self.__events_cli is not None:
            try:
                await self.__events_cli.close()
                self.__log("event_collector losed events websocket client", "DEBUG")
            except Exception as e:
                self.__log(f"event_collector closing websocket client: {e}", "ERROR")
            self.__events_cli = None

        # Close main markets DB
        if self.__markets_db is not None:
            try:
                self.__markets_db.close()
                self.__log("event_collector losed markets.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"event_collector error closing markets.db: {e}", "ERROR")
            self.__markets_db = None

        # Close events DB
        if self.__events_db is not None:
            try:
                self.__events_db.close()
                self.__log("event_collector closed events.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"event_collector error closing events.db: {e}", "ERROR")
            self.__events_db = None

        self.__running = False
        self.__log("event_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")

    
    async def __query_markets(self)->bool:
    # check for new markets/tokens
        cursor = self.__markets_db.cursor()
        cursor.execute(f"""
            SELECT clobTokenIds1, clobTokenIds2 FROM markets WHERE row_index > {self.__last_market_row}
        """)
        new_token_pairs = cursor.fetchall()
        new_token_count = len(new_token_pairs)

        self.__last_market_row += new_token_count

        self.__log(f"event_collector found {new_token_pairs} pairs from markets db", "INFO")
        if new_token_count == 0:
            return await self.__read_events()
        
        for [tok1, tok2] in new_token_pairs:
            self.__token_ids.append(tok1)
            self.__token_ids.append(tok2)

        if not self.__create_tables(new_token_pairs):
            self.__log(f"event_collector failed to create tables", "ERROR")
            return False
        if not await self.__resubscribe():
            self.__log(f"event_collector failed to resubscribe", "ERROR")
            return False
        return True

    def __create_tables(self, token_pair_list)->bool:
        buffer = []
        for toks in token_pair_list:
            if len(toks) != 2:
                self.__log(f"event collector create tables found {len(toks)} token ids for a market")
            for tok in toks:
                buffer.append(f"""CREATE TABLE book_{tok} 
                    (row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    market TEXT,
                    bids TEXT,
                    asks TEXT,
                    server_time INTEGER);""")
                buffer.append(f"""CREATE TABLE price_change_{tok} 
                    (row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    market TEXT,
                    price REAL,
                    size REAL,
                    side TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    server_time INTEGER);""")
                buffer.append(f"""CREATE TABLE last_trade_price_{tok} 
                    (row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    market TEXT,
                    fee_rate_bps REAL,
                    price REAL,
                    side TEXT,
                    size REAL,
                    server_time INTEGER);""")
                buffer.append(f"""CREATE TABLE tick_size_change_{tok} 
                    (row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    market TEXT,
                    old_tick_size REAL,
                    new_tick_size REAL,
                    server_time INTEGER);""")
        create_table_stmt = "".join(buffer)
        try:
            cursor = self.__events_db.cursor()
            cursor.executescript(create_table_stmt)
            self.__events_db.commit()
            self.__log(f"event_collector created new tables succesfully", "DEBUG")
            return True
        except Exception as e:
            self.__log(f"event_collector failed to create new tables for {create_table_stmt}", "DEBUG")
            return False

    async def __resubscribe(self)->bool:
        try: 
            self.__log(f"market_collector resubcribing with events_cli {type(self.__events_cli)})", "DEBUG")
            new_cli = await websockets.connect(self.__events_url)
            subscription = {
                "type": "market",
                "initial_dump": True,
                "assets_ids": self.__token_ids
            }
            await new_cli.send(json.dumps(subscription))

            #read events one last time before switching to new wss
            if self.__events_cli is not None:
                if not await self.__read_events():
                    return False
                await self.__events_cli.close()

            self.__events_cli = new_cli
            self.__resubscription_count += 1
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
                self.__log(f"event_collector raw message: {message}", "DEBUG")  # log the raw message
            except asyncio.TimeoutError:
                self.__log(f"event_collector emptied received messaged buffer","DEBUG")
                break
            msg_json = json.loads(message)

            if not self.__insert(msg_json):
                self.__log("event_collector failed to insert", "ERROR")
                return False

        return True

    def __insert(self, msg_json) -> bool:
        insert_timestamp = int(time.time() * 1000)

        # Ensure msg_json is a list
        if isinstance(msg_json, dict):
            msg_json = [msg_json]

        if not msg_json:
            self.__log("event_collector received empty message list", "WARN")
            return True

        if not isinstance(msg_json, list):
            self.__log(f"event_collector invalid msg_json: {type(msg_json)}", "ERROR")
            return False

        for i, obj in enumerate(msg_json):
            # Decode stringified JSON objects
            if isinstance(obj, str):
                try:
                    msg_json[i] = json.loads(obj)
                except Exception as e:
                    self.__log(f"event_collector failed to decode object: {obj} error: {e}", "ERROR")
                    return False
            if not isinstance(msg_json[i], dict):
                self.__log(f"event_collector invalid type after decode: {type(msg_json[i])}", "ERROR")
                return False

        # Extract type and market from the first object
        try:
            event_type = msg_json[0]["event_type"]
            market = msg_json[0]["market"]
        except KeyError as e:
            self.__log(f"event_collector missing key in message: {e}", "ERROR")
            return False

        try:
            cursor = self.__events_db.cursor()

            if event_type == "book":
                for obj in msg_json:
                    if "bids" not in obj or "asks" not in obj or "asset_id" not in obj:
                        self.__log(f"event_collector invalid book object: {obj}", "ERROR")
                        return False
                    token = obj["asset_id"]
                    bids = obj["bids"]
                    asks = obj["asks"]
                    cursor.execute(
                        f"INSERT INTO book_{token} (insert_time, market, bids, asks, server_time) VALUES (?, ?, ?, ?, ?)",
                        (insert_timestamp, market, json.dumps(bids), json.dumps(asks), obj.get("timestamp", insert_timestamp))
                    )

            elif event_type == "price_change":
                for obj in msg_json:
                    # Check for nested price_changes array
                    changes = obj.get("price_changes")
                    if not changes or not isinstance(changes, list):
                        self.__log(f"event_collector invalid price_change object: {obj}", "ERROR")
                        return False
                    for change in changes:
                        if "asset_id" not in change:
                            self.__log(f"event_collector missing asset_id in price_change: {change}", "ERROR")
                            return False
                        token = change["asset_id"]
                        cursor.execute(
                            f"INSERT INTO price_change_{token} (insert_time, market, price, size, side, best_bid, best_ask, server_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                insert_timestamp,
                                market,
                                change["price"],
                                change["size"],
                                change["side"],
                                change.get("best_bid"),
                                change.get("best_ask"),
                                obj.get("timestamp", insert_timestamp)
                            )
                        )

            # Similar checks can be added for last_trade_price and tick_size_change
            self.__events_db.commit()
            return True

        except Exception as e:
            self.__log(f"event_collector failed to insert {event_type} data: {e}", "ERROR")
            return False


