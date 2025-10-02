import asyncio
import aiohttp
import asyncpg
import json
import os
import time
from datetime import datetime, timezone
import pathlib

class market_collector:
    def __init__(self, verbosity="DEBUG", reset=True, batch_size=500, offset=80000):
        # Database & directorie
        self.__db_conn = None
        self.__reset = reset

        # logging
        self.__verbosity = verbosity.upper()

        # markets endpoint
        self.__markets_url = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
        self.__markets_cli = None
        self.__market_offset = offset
        self.__rate_limit = 100 * 1000 * 1000 # 100ms in ns
        self.__last_request = -self.__rate_limit
        self.__batch_size = batch_size

        # liveness
        self.__running = False

        # version
        self.__version = 1

    async def start(self) -> bool:
        if self.__running:
            self.__log("market_collector already started", "ERROR")
            return False
        try:
            socket_dir = str(pathlib.Path("../.pgsocket").resolve()) 
            self.__db_conn = await asyncpg.connect(
                user="client",
                password="clientpass",
                database="data",
                host=socket_dir,  # directory containing the socket
                port=5432
            )
            if self.__reset:
                await self.__db_conn.execute("""
                DROP TABLE IF EXISTS markets;
            """)

            await self.__db_conn.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                    row_index SERIAL PRIMARY KEY,
                    collector_version INTEGER,
                    insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                    market_id VARCHAR(100),
                    token_id1 VARCHAR(100),
                    token_id2 VARCHAR(100),
                    negrisk_id VARCHAR(100)
                );
            """)

        except Exception as e:
            self.__log(f"market_collector failed to start: {e}", "ERROR")
            return False

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
                self.__log(f"market_collector aborted", "ERROR")
                await self.__clean_up()
                return False

    async def stop(self) -> bool:
        await self.__clean_up()
        self.__log("market_collector stopped", "DEBUG")
        return True

    async def __clean_up(self):
        self.__log("market_collector cleanup started", "DEBUG")

        if self.__markets_cli is not None:
            try:
                await self.__markets_cli.close()
                self.__log("market_collector closed markets aiohttp client", "DEBUG")
            except Exception as e:
                self.__log(f"market_collector error closing markets aiohttp client: {e}", "ERROR")
            self.__markets_cli = None

        if self.__db_conn is not None:
            try:
                await self.__db_conn.close()
                self.__log("market_collector closed DB connection", "DEBUG")
            except Exception as e:
                self.__log(f"market_collector error closing DB connection: {e}", "ERROR")
            self.__db_conn = None
            
        self.__running = False
        self.__log("market_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= 1:
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            print(f"[{now_iso}] [{level}] {msg}")

    async def __query_markets(self) -> bool:
        url = self.__markets_url.format(offset=self.__market_offset)
        try:
            resp = await self.__markets_cli.get(url)
            if resp.status != 200:
                self.__log(f"market_collector get request failed for {url} with status {resp.status}", "ERROR")
                await resp.release()
                return False
            market_arr = await resp.json()
            await resp.release()
        except aiohttp.ClientError as e:
            self.__log(f"market_collector request failed: {e}", "ERROR")
            return False

        new_market_count = len(market_arr)
        self.__log(f"market_collector fetched {new_market_count} markets", "DEBUG")
        if not isinstance(market_arr, list):
            self.__log(f"market_collector market response not list but {type(market_arr)}", "ERROR")
            return False

        for market_obj in market_arr:
            if not isinstance(market_obj, dict):
                self.__log(f"market_collector market_obj is not dict but {type(market_obj)}", "ERROR")
                return False
            
            market_id = market_obj.get("id", None)
            if not isinstance(market_id, str):
                self.__log(f"market_collector found invalid market_id {market_id} in {json.dumps(market_obj)}", "ERROR")
                return False
            
            closed = market_obj.get("closed", None)
            if not isinstance(closed, bool):
                self.__log(f"market_collector closed attribute is of type {type(closed)} in {json.dumps(market_obj)}", "ERROR")
                return False

            if closed:
                self.__log(f"market_collector skipping closed market {market_id}", "DEBUG")
                continue

            token_ids = market_obj.get("clobTokenIds", None)
            try:
                token_ids = json.loads(token_ids)
            except (json.JSONDecodeError, TypeError):
                pass
            
            if not isinstance(token_ids, list):
                self.__log(f"market_collector found invalid token_ids {token_ids} in {json.dumps(market_obj)}", "WARNING")
                continue
            
            if len(token_ids) != 2:
                self.__log(f"market_collector found invalid token_ids {token_ids} in {json.dumps(market_obj)}", "ERROR")
                return False
            
            negrisk_id = market_obj.get("negRiskMarketID", None)
            try:
                await self.__db_conn.execute("""
                    INSERT INTO markets (collector_version, market_id, token_id1, token_id2, negrisk_id)
                    VALUES ($1, $2, $3, $4, $5)
                """, self.__version, market_id, token_ids[0], token_ids[1], negrisk_id)
            except Exception as e:
                self.__log(f"market_collector failed insert new market row with version {self.__version} , : {e}", "ERROR")
                return False
            self.__log(f"market_collector found new non-closed market {market_id} with tokens {token_ids[0]} and {token_ids[1]}, and negrisk {negrisk_id}", "INFO")
            
        
        self.__market_offset += new_market_count
        if new_market_count > 0:
            self.__log(f"market_collector found {new_market_count} new markets", "INFO")
        return True