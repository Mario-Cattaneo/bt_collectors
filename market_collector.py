import asyncio
import aiohttp
import sqlite3
import json
import os
from datetime import datetime, timezone
import time
import shutil

class market_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True, batch_size=500, reset_offset=105000, init_offset=105000):
        # sql resources
        self.__data_dir = data_dir
        self.__markets_dir = None
        self.__markets_db = None # overview db
        self.__markets_dbs = {} # id to db connection map
        self.__reset = reset

        # attribute handling
        self.__primary_attributes = ["id", "clobTokenIds", "negRiskMarketID"]
        self.__attributes = {}

        # logging
        self.__verbosity = verbosity.upper()

        # markets endpoint
        self.__markets_url = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
        self.__markets_cli = None
        self.__market_offset = init_offset
        self.__reset_offset = reset_offset
        self.__rate_limit = 100 * 1000 * 1000 # 100ms or 100 * 1000^2 ns
        self.__last_request = -self.__rate_limit
        self.__batch_size = batch_size

        # liveness
        self.__running = False

        # version
        self.__version = 1

    def change_settings(
        self, 
        verbosity=None,
        markets_url=None,
        reset_offset=None,
        rate_limit=None,
        batch_size=None,
        version=None
    ):
        if verbosity is not None:
            self.__verbosity = verbosity
        if markets_url is not None:
            self.__markets_url = markets_url
        if reset_offset is not None:
            self.__reset_offset = reset_offset
        if rate_limit is not None:
            self.__rate_limit = rate_limit
        if batch_size is not None:
            self.__batch_size = batch_size
        if version is not None:
            self.__version = version

    
    async def start(self)->bool:
        # Can't start if already running
        if self.__running:
            self.__log("market_collector already started", "ERROR")
            return False
        try:
            # Ensure directories exists 
            os.makedirs(self.__data_dir, exist_ok=True)
            self.__markets_dir = os.path.join(self.__data_dir, "markets")

            # clear all markets data
            if self.__reset and os.path.exists(self.__markets_dir):
                shutil.rmtree(self.__markets_dir)
                self.__markets_dir = os.path.join(self.__data_dir, "markets")

            # create markets db if it doesn't exist
            os.makedirs(self.__markets_dir, exist_ok=True)
            markets_db_path = os.path.join(self.__markets_dir, "markets.db")

            # Store persistent connection to markets db
            self.__markets_db = sqlite3.connect(markets_db_path)
            cur = self.__markets_db.cursor()

            cur.executescript("""
                CREATE TABLE IF NOT EXISTS markets  (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    collector_version INTEGER,
                    insert_time TEXT,
                    id TEXT,
                    clobTokenIds1 TEXT,
                    clobTokenIds2 TEXT,
                    negRiskMarketID TEXT --can be NULL
                );
                CREATE TABLE IF NOT EXISTS attributes  (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    collector_version INTEGER,
                    insert_time TEXT,
                    attribute_name TEXT
                );
            """)
            self.__markets_db.commit()
        except (OSError, sqlite3.Error) as e:
            self.__log(f"market_collector to start: {e}", "ERROR")
            return False

        # At most 3 sockets for pipeling and keepalive forever
        connector = aiohttp.TCPConnector(limit_per_host=2, keepalive_timeout=99999)
        self.__markets_cli = aiohttp.ClientSession(connector=connector)
        
        self.__running = True
        self.__log("market_collector started", "INFO")

        while self.__running:
            now = time.monotonic_ns()
            should_be = self.__last_request + self.__rate_limit
            if should_be > now:
                await asyncio.sleep((should_be - now) / 1_000_000_000)
            self.__last_request = time.monotonic_ns()
            if not await self.__query_markets():
                self.__log(f"market_collector exiting running loop due to abort", "DEBUG")
                await self.__clean_up()
                return False

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


        # Close main markets.db
        if self.__markets_db is not None:
            try:
                self.__markets_db.close()
                self.__log("Closed markets.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing markets.db: {e}", "ERROR")
            self.__markets_db = None

        # Close all event DBs
        for market_id, conn in list(self.__markets_dbs.items()):
            try:
                conn.close()
                #self.__log(f"Closed DB for market_id={market_id}", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing market for market_id={market_id}: {e}", "ERROR")
        self.__markets_dbs.clear()

        self.__running = False

        self.__log("market_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")

    def __attribute_sql_type(self, value) -> str:
        if isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, int):
            # Check if the integer fits in SQLite INTEGER (64-bit signed)
            if -(2**63) <= value <= 2**63 - 1:
                return "INTEGER"
            else:

                return "TEXT"  # too large, store as string
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, str):
            return "TEXT"
        elif value is None:
            return "TEXT"
        else:
            return "UNKNOWN"

    def __handle_attribute(self, name, value) -> bool:
        if name in self.__attributes:
            if len(self.__attributes[name]) != 2:
                self.__log(f"market collector handle_attribute name {name} registered with incorrect list length {len(self.__attributes[name])}", "ERROR")
                return False
            if isinstance(value, int) and not -(2**63) <= value <= 2**63 - 1:
                self.__log(f"market collector attribute {name} integer too large, storing as TEXT", "DEBUG")
                value = str(value)
            self.__attributes[name][1] = value
            return True

        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass

        if isinstance(value, list):
            for i, subvalue in enumerate(value, start=1):
                if not self.__handle_attribute(f"{name}{i}", subvalue):
                    self.__log(f"market collector handle_attribute failed for list element {i} of {name}", "ERROR")
                    return False
            return True
        elif isinstance(value, dict):
            for subkey, subvalue in value.items():
                if not self.__handle_attribute(subkey, subvalue):
                    self.__log(f"market collector handle_attribute failed for dict key {subkey} of {name}", "ERROR")
                    return False
            return True

        sql_type = self.__attribute_sql_type(value)
        if sql_type == "UNKNOWN":
            self.__log(f"market collector handle_attributes got unknown type value {type(value)} for {name}", "ERROR")
            return False
        
        if isinstance(value, int) and not -(2**63) <= value <= 2**63 - 1:
            self.__log(f"market collector attribute {name} integer too large, storing as TEXT", "DEBUG")
            value = str(value)

        self.__attributes[name] = [sql_type, value]

        if not self.__add_new_sql_column(name, sql_type):
            self.__log(f"market collector handle_attribute failed to add new SQL column for {name} of type {sql_type}", "ERROR")
            return False

        return True


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

            for key, value in market_obj.items():
                if not self.__handle_attribute(key, value):
                    return False
            
            if not self.__handle_query():
                return False

        new_markets = len(market_arr)
        self.__log(f"market_collector {new_markets} markets fetch at offset {self.__market_offset}")
        if new_markets < self.__batch_size:
            self.__market_offset = self.__reset_offset
        else:
            self.__market_offset += new_markets
        return True

    def __add_new_sql_column(self, name, type, default="NULL")->bool:
        for m_id, conn in self.__markets_dbs.items():
            try:
                cursor = conn.cursor()
                cursor.execute(f"ALTER TABLE attributes ADD COLUMN {name} {type} DEFAULT {default};")
                conn.commit()
            except Exception as e:
                self.__log("market collector failed to add column for {m_id}: {e}", "ERROR")
                return False
        
        try:
            now = datetime.now(timezone.utc)
            iso_str = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
            cursor = self.__markets_db.cursor()
            cursor.execute("""INSERT INTO attributes 
            (collector_version, insert_time, attribute_name) VALUES
            (?, ?, ?)""",
            (self.__version, iso_str, name))
            self.__markets_db.commit()
        except Exception as e:
            self.__log("market collector failed to update attributes table: {e}", "ERROR")
            return False
        return True

    def __create_table_stmt(self) -> str:
        buffer = ["CREATE TABLE IF NOT EXISTS attributes ("]
        buffer.append("row_index INTEGER PRIMARY KEY AUTOINCREMENT, ")
        buffer.append("collector_version INTEGER, ")
        buffer.append("insert_time TEXT")

        for name, attr in self.__attributes.items():
            buffer.append(f', "{name}" {attr[0]}')

        buffer.append(')')
        return "".join(buffer)

    def __create_insert_stmt(self, iso_str) -> str:
        buffer = ["INSERT INTO attributes (collector_version, insert_time"]
        
        # Column names
        for name in self.__attributes.keys():
            buffer.append(f', "{name}"')

        buffer.append(") VALUES (")
        buffer.append(f"{self.__version},'{iso_str}'")  # insert_time

        # Column values
        for name, attr in self.__attributes.items():
            value = attr[1]
            if attr[0] in ("INTEGER", "REAL", "BOOL"):
                buffer.append(f", {value}")
            else:  # TEXT
                safe_value = str(value).replace("'", "''")  # escape single quotes
                buffer.append(f", '{safe_value}'")

        buffer.append(")")
        return "".join(buffer)

    def __handle_query(self) -> bool:
        market_id = self.__attributes.get("id", [None, None])[1]
        if market_id is None:
            self.__log("market collector handle_query failed: missing id attribute", "ERROR")
            return False

        if market_id not in self.__markets_dbs:
            
            try:
                # create db and table
                markets_db_path = os.path.join(self.__markets_dir, f"{market_id}.db")
                self.__markets_dbs[market_id] = sqlite3.connect(markets_db_path)
                cursor = self.__markets_dbs[market_id].cursor()

                # --- ADD PINPOINT LOGS HERE ---
                self.__log(f"market {market_id}: creating table with attributes:", "DEBUG")
                for name, attr in self.__attributes.items():
                    self.__log(f"  {name} ({attr[0]}): {repr(attr[1])}", "DEBUG")
                # --------------------------------

                cursor.execute(self.__create_table_stmt())
                self.__markets_dbs[market_id].commit()

                # insert into main markets.db
                clobTokenIds1 = self.__attributes["clobTokenIds1"][1]
                clobTokenIds2 = self.__attributes["clobTokenIds2"][1]
                negrisk_id = self.__attributes.get("negRiskID", [None, None])[1]
                now = datetime.now(timezone.utc)
                iso_str = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
                cursor = self.__markets_db.cursor()
                cursor.execute("""INSERT INTO markets 
                    (collector_version, insert_time, id, clobTokenIds1, clobTokenIds2, negRiskMarketID) 
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (self.__version, iso_str, market_id, clobTokenIds1, clobTokenIds2, negrisk_id))
                self.__markets_db.commit()
            except Exception as e:
                self.__log(f"market collector failed new market creation for {market_id}: {e}", "ERROR")
                return False

        try:
            now = datetime.now(timezone.utc)
            iso_str = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
            cursor = self.__markets_dbs[market_id].cursor()
            cursor.execute(self.__create_insert_stmt(iso_str))
            self.__markets_dbs[market_id].commit()
            return True
        except Exception as e:
            self.__log(f"market collector failed inserting attributes for market {market_id}: {e}", "ERROR")
            # Add detailed dump of attribute values to help debug
            #for attr_name, attr in self.__attributes.items():
                #self.__log(f"market collector attribute dump: {attr_name}={attr[1]} ({attr[0]})", "DEBUG")
            return False