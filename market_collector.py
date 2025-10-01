import asyncio
import aiohttp
import asyncpg
import json
import os
from datetime import datetime, timezone
import time
import shutil

class market_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True, batch_size=500, reset_offset=108000, init_offset=108000, pg_config=None):
        # Database & directories
        self.__data_dir = data_dir
        self.__markets_dir = None
        self.__pg_config = pg_config or {
            "user": "mario",
            "password": "mario_is_goated",
            "database": "data",
            "host": "localhost",
            "port": 5432
        }
        self.__markets_db_pool = None
        self.__market_ids = []
        self.__attributes_db_pool = None
        self.__reset = reset

        # attribute handling
        self.__attributes = {}

        # logging
        self.__verbosity = verbosity.upper()

        # markets endpoint
        self.__markets_url = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
        self.__markets_cli = None
        self.__market_offset = init_offset
        self.__reset_offset = reset_offset
        self.__rate_limit = 100 * 1000 * 1000 # 100ms in ns
        self.__last_request = -self.__rate_limit
        self.__batch_size = batch_size

        # liveness
        self.__running = False

        # version
        self.__version = 1

    def change_settings(self, verbosity=None, markets_url=None, reset_offset=None, rate_limit=None, batch_size=None, version=None):
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

    async def start(self) -> bool:
        if self.__running:
            self.__log("market_collector already started", "ERROR")
            return False

        try:
            os.makedirs(self.__data_dir, exist_ok=True)
            self.__markets_dir = os.path.join(self.__data_dir, "markets")
            if self.__reset and os.path.exists(self.__markets_dir):
                shutil.rmtree(self.__markets_dir)
            os.makedirs(self.__markets_dir, exist_ok=True)

            # Initialize PostgreSQL connection pools
            self.__markets_db_pool = await asyncpg.create_pool(**self.__pg_config)
            self.__attributes_db_pool = self.__markets_db_pool  # using same DB for simplicity

            # Ensure tables exist
            async with self.__markets_db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS markets (
                        row_index SERIAL PRIMARY KEY,
                        collector_version INTEGER,
                        insert_time TIMESTAMP WITH TIME ZONE,
                        id TEXT,
                        clobTokenIds1 TEXT,
                        clobTokenIds2 TEXT,
                        negRiskMarketID TEXT
                    );
                    CREATE TABLE IF NOT EXISTS attributes (
                        row_index SERIAL PRIMARY KEY,
                        collector_version INTEGER,
                        insert_time TIMESTAMP WITH TIME ZONE,
                        name TEXT,
                        type TEXT
                    );
                """)

        except Exception as e:
            self.__log(f"market_collector failed to start: {e}", "ERROR")
            return False

        connector = aiohttp.TCPConnector(limit_per_host=2, keepalive_timeout=99999)
        self.__markets_cli = aiohttp.ClientSession(connector=connector)

        self.__running = True
        self.__log("market_collector started", "INFO")
        return True

    async def stop(self) -> bool:
        await self.__clean_up()
        self.__log("market_collector stopped", "DEBUG")
        return True

    async def __clean_up(self):
        self.__log("market_collector cleanup started", "DEBUG")

        if self.__markets_cli is not None:
            try:
                await self.__markets_cli.close()
                self.__log("Closed markets aiohttp client", "DEBUG")
            except Exception as e:
                self.__log(f"Error closing markets aiohttp client: {e}", "ERROR")
            self.__markets_cli = None

        if self.__markets_db_pool is not None:
            await self.__markets_db_pool.close()
            self.__log("Closed PostgreSQL connection pool", "DEBUG")
            self.__markets_db_pool = None

        self.__running = False
        self.__log("market_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= 0:
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            print(f"[{now_iso}] [{level}] {msg}")

    async def __query_markets(self) -> bool:
        url = self.__markets_url.format(offset=self.__market_offset)
        try:
            resp = await self.__markets_cli.get(url)
            if resp.status != 200:
                self.__log(f"market fetch failed: {resp.status}", "ERROR")
                await resp.release()
                return False
            market_arr = await resp.json()
            await resp.release()
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
            if not await self.__handle_query():
                return False

        new_markets = len(market_arr)
        self.__log(f"market_collector {new_markets} markets fetched at offset {self.__market_offset}", "DEBUG")
        self.__market_offset = self.__reset_offset if new_markets < self.__batch_size else self.__market_offset + new_markets
        return True

    async def __handle_query(self) -> bool:
        market_id = self.__attributes.get("id", [None, None])[1]
        if market_id is None:
            self.__log("market collector handle_query failed: missing id attribute", "ERROR")
            return False

        async with self.__attributes_db_pool.acquire() as conn:
            try:
                if market_id not in self.__market_ids:
                    # Create table dynamically
                    columns_def = ', '.join([f'"{name}" {attr[0]}' for name, attr in self.__attributes.items()])
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS "{market_id}" (
                            row_index SERIAL PRIMARY KEY,
                            collector_version INTEGER,
                            insert_time TIMESTAMP WITH TIME ZONE,
                            {columns_def}
                        );
                    """)

                    # Insert into main markets table
                    clobTokenIds1 = self.__attributes["clobTokenIds1"][1]
                    clobTokenIds2 = self.__attributes["clobTokenIds2"][1]
                    negrisk_id = self.__attributes.get("negRiskID", [None, None])[1]
                    now = datetime.now(timezone.utc)
                    await conn.execute("""
                        INSERT INTO markets (collector_version, insert_time, id, clobTokenIds1, clobTokenIds2, negRiskMarketID)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, self.__version, now, market_id, clobTokenIds1, clobTokenIds2, negrisk_id)
                    self.__market_ids.append(market_id)

                # Insert attributes into market-specific table
                columns = ["collector_version", "insert_time"] + list(self.__attributes.keys())
                values = [self.__version, datetime.now(timezone.utc)] + [None if attr[1] == "NULL" else attr[1] for attr in self.__attributes.values()]
                columns_sql = ', '.join([f'"{c}"' for c in columns])
                values_sql = ', '.join(f'${i+1}' for i in range(len(values)))

                await conn.execute(f'INSERT INTO "{market_id}" ({columns_sql}) VALUES ({values_sql})', *values)

            except Exception as e:
                self.__log(f"market collector failed handling market {market_id}: {e}", "ERROR")
                return False
        return True

    # Keep all other helper methods like __set_attribute, __handle_attribute etc. the same.
