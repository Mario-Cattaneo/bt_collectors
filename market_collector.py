import asyncio
import aiohttp
import sqlite3
import json
import os
from datetime import datetime, timezone
import time
import shutil

class market_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True, batch_size=500, reset_offset=108000, init_offset=108000):
        # sql resources
        self.__data_dir = data_dir
        self.__markets_dir = None
        self.__markets_db = None # overview db
        self.__market_ids = []
        self.__attributes_db = None
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
            attributes_db_path = os.path.join(self.__markets_dir, "attributes.db")

            # Store persistent connection to markets db
            self.__attributes_db = sqlite3.connect(attributes_db_path)
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

        # Close all atttibutes db
        if self.__attributes_db is not None:
            try:
                self.__attributes_db.close()
                self.__log("Closed markets.db connection", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing markets.db: {e}", "ERROR")
            self.__attributes_db = None

        self.__running = False

        self.__log("market_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")

    def __set_attribute(self, name, value) -> bool:
        if isinstance(value, bool):
            self.__attributes[name][0] = "BOOL"
            self.__attributes[name][1] = value
            return True
        elif isinstance(value, int):
            # Check if the integer fits in SQLite INTEGER (64-bit signed)
            if -(2**63) <= value <= 2**63 - 1:
                self.__attributes[name][0] = "INTEGER"
                self.__attributes[name][1] = value
                return True
            self.__attributes[name][0] = "TEXT"
            self.__attributes[name][1] = str(value)
            return True # too large, store as string
        elif isinstance(value, float):
            self.__attributes[name][0] = "REAL"
            self.__attributes[name][1] = value
            return True
        elif isinstance(value, str):
            self.__attributes[name][0] = "TEXT"
            if value == "":
                self.__attributes[name][1] = "NULL"
            else:
                self.__attributes[name][1] = value.replace("'", "''")
            return True
        elif value is None:
            # keep same type as before
            self.__attributes[name][1] = "NULL"
        else:
            return False

    def __handle_attribute(self, name, value) -> bool:
        if name in self.__attributes:
            if len(self.__attributes[name]) != 2:
                self.__log(f"market collector handle_attribute name {name} registered with incorrect list length {len(self.__attributes[name])}", "ERROR")
                return False
            if not self.__set_attribute(name, value):
                return False
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

        self.__attributes[name] = ["TEXT", "NULL"] # default
        if not self.__set_attribute(name, value):
            self.__log(f"market_collector failed to set_attribute for {name} and {value} of type {type(value)}", "ERROR")
            return False
        try:
            cursor = self.__attributes_db.cursor()
            cursor.executescript(self.__create_alter_stmt(name, self.__attributes[name][0]))
            self.__attributes_db.commit()
            self.__log(f"market_collector added column {name} of type {self.__attributes[name][0]}", "DEBUG")
        except Exception as e:
            self.__log(f"market_collector add column {name} of type {self.__attributes[name][0]}: {e}", "ERROR")
            self.__log(f"market_collector add column stmt: {self.__create_alter_stmt(name, self.__attributes[name][0])}", "ERROR")
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
        self.__log(f"market_collector {new_markets} markets fetched at offset {self.__market_offset}")
        if new_markets < self.__batch_size:
            self.__market_offset = self.__reset_offset
        else:
            self.__market_offset += new_markets
        return True

    def __create_table_stmt(self, market_id) -> str:
        buffer = [f'CREATE TABLE IF NOT EXISTS "{market_id}" (row_index INTEGER PRIMARY KEY AUTOINCREMENT, collector_version INTEGER, insert_time TEXT']
        for name, attr in self.__attributes.items():
            buffer.append(f', "{name}" {attr[0]}')
        buffer.append(")")
        return "".join(buffer)

    def __create_alter_stmt(self, attr_name, attr_sql_type, default="NULL") -> str:
        buffer = []
        for market_id in self.__market_ids:
            buffer.append(f'ALTER TABLE "{market_id}" ADD "{attr_name}" {attr_sql_type};')
        return " ".join(buffer)

    def __create_insert_stmt(self, market_id) -> str:
        now = datetime.now(timezone.utc)
        iso_str = now.isoformat(timespec="microseconds").replace("+00:00", "Z")

        # Column list
        columns = ["collector_version", "insert_time"] + list(self.__attributes.keys())
        columns_escaped = [f'"{col}"' for col in columns]  # Escape column names
        buffer = [f'INSERT INTO "{market_id}" ({", ".join(columns_escaped)}) VALUES (']

        # Value list
        values = [str(self.__version), f"'{iso_str}'"]
        for _, attr in self.__attributes.items():
            if attr[1] == "NULL":
                values.append("NULL")
            elif attr[0] == "TEXT":
                values.append(f"'{attr[1]}'")
            else:
                values.append(str(attr[1]))

        buffer.append(", ".join(values))
        buffer.append(")")
        return "".join(buffer)

    def __handle_query(self) -> bool:
        market_id = self.__attributes.get("id", [None, None])[1]
        if market_id is None:
            self.__log("market collector handle_query failed: missing id attribute", "ERROR")
            return False

        if market_id not in self.__market_ids:
            try:
                # create db and table
                cursor = self.__attributes_db.cursor()

                # --- ADD PINPOINT LOGS HERE ---
                self.__log(f"market {market_id}: creating table with attributes:", "DEBUG")
                for name, attr in self.__attributes.items():
                    self.__log(f"  {name} ({attr[0]}): {repr(attr[1])}", "DEBUG")
                # --------------------------------

                cursor.execute(self.__create_table_stmt(market_id))
                self.__attributes_db.commit()

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
                self.__log(f"market_collector table creation successful for market {market_id} at offset {self.__market_offset}", "DEBUG")
                self.__market_ids.append(market_id)
            except Exception as e:
                self.__log(f"market_collector failed new market creation for {market_id}: {e}", "ERROR")
                self.__log(f"marktet_collector create_table statement: {self.__create_table_stmt(market_id)}", "ERROR")
                return False

        try:
            cursor = self.__attributes_db.cursor()
            cursor.execute(self.__create_insert_stmt(market_id))
            self.__attributes_db.commit()
            self.__log(f"market_collector insert successful for market {market_id} at offset {self.__market_offset}", "DEBUG")
            return True
        except Exception as e:
            self.__log(f"market collector failed inserting attributes for market {market_id}: {e}", "ERROR")
            self.__log(f"marktet_collector insert statement: {self.__create_insert_stmt(market_id)}", "ERROR")
            #for attr_name, attr in self.__attributes.items():
               # self.__log(f"market collector attribute dump: {attr_name}={attr[1]} ({attr[0]})", "DEBUG")
            #return False