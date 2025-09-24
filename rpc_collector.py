import asyncio
import json
import os
import sqlite3
import aiohttp
from dotenv import load_dotenv
import websockets
import time


class rpc_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG"):
        self.__data_dir = data_dir
        self.__verbosity = verbosity
        self.__infura_wss_url = None
        self.__infura_https_url = None
        self.__infura_api_key = None
        self.__blocks_db = None
        self.__running = False
        self.__wss_cli = None
        self.__https_cli = None
    
    async def start(self) -> bool:
        # Load environment and set URLs first
        load_dotenv()
        self.__infura_api_key = os.getenv("INFURA_API_KEY")
        if not isinstance(self.__infura_api_key, str):
            self.__log("rpc_collector INFURA_API_KEY not set in environment", "ERROR")
            return False

        self.__infura_wss_url = f"wss://polygon-mainnet.infura.io/ws/v3/{self.__infura_api_key}"
        self.__infura_https_url = f"https://polygon-mainnet.infura.io/v3/{self.__infura_api_key}"
        self.__log(f"rpc_collector urls set up with API_KEY={self.__infura_api_key}", "DEBUG")

        # Check if already running
        if self.__running:
            self.__log("rpc_collector already started", "ERROR")
            return False

        # Create data directory and initialize database
        try:
            os.makedirs(self.__data_dir, exist_ok=True)
            blocks_db_path = os.path.join(self.__data_dir, "blocks.db")
            if os.path.exists(blocks_db_path):
                os.remove(blocks_db_path)
                self.__log("rpc_collector removed old blocks.db", "DEBUG")

            self.__blocks_db = sqlite3.connect(blocks_db_path)
            cur = self.__blocks_db.cursor()
            cur.execute("DROP TABLE IF EXISTS blocks")
            cur.execute("""
                CREATE TABLE blocks (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    block_number INTEGER,
                    block TEXT
                )
            """)
            self.__blocks_db.commit()
        except (OSError, sqlite3.Error) as e:
            self.__log(f"rpc_collector failed to create collector.db: {e}", "ERROR")
            return False

        self.__log(f"rpc_collector created collector.db at {blocks_db_path}", "DEBUG")

        # Create aiohttp client session with persistent connections
        connector = aiohttp.TCPConnector(limit_per_host=3, keepalive_timeout=99999)
        self.__https_cli = aiohttp.ClientSession(connector=connector)

        # Mark running
        self.__running = True
        self.__log("rpc_collector started", "INFO")

        # Subscribe to newHeads
        if not await self.__subscribe():
            self.__log("rpc_collector failed to subscribe, aborting", "ERROR")
            await self.__clean_up()
            return False

        # Main loop
        while self.__running:
            if not await self.__read_and_fetch():
                await self.__clean_up()
                return False

        self.__log("rpc_collector exiting running loop due to abort", "DEBUG")
        return True


    async def stop(self) -> bool:
        self.__log("rpc_collector stopping...", "INFO")
        self.__running = False
        await self.__clean_up()
        self.__log("rpc_collector stopped", "INFO")
        return True

    async def __clean_up(self):
        try:
            if self.__wss_cli:
                await self.__wss_cli.close()
                self.__log("rpc_collector wss client closed", "DEBUG")
                self.__wss_cli = None
        except Exception as e:
            self.__log(f"rpc_collector error closing wss: {e}", "ERROR")

        try:
            if self.__https_cli:
                await self.__https_cli.close()
                self.__log("rpc_collector https client closed", "DEBUG")
                self.__https_cli = None
        except Exception as e:
            self.__log(f"rpc_collector error closing https client: {e}", "ERROR")

        try:
            if self.__blocks_db:
                self.__blocks_db.close()
                self.__log("rpc_collector database connection closed", "DEBUG")
                self.__blocks_db = None
        except Exception as e:
            self.__log(f"rpc_collector error closing DB: {e}", "ERROR")

        return



    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")
    
    async def __subscribe(self)->bool:
        try:
            self.__wss_cli = await websockets.connect(self.__infura_wss_url)
            await self.__wss_cli.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }))
            subscribe_response = json.loads(await self.__wss_cli.recv())
            self.__log(f"rpc_collector subscribed: {subscribe_response}", "INFO")
            return True 
        except Exception as e:
            self.__log(f"rpc_collector subscription failed: {e}", "ERROR")
            return False

    async def __read_and_fetch(self)->bool:
        try:
            message = await asyncio.wait_for(self.__wss_cli.recv(), timeout=0.001)
            self.__log(f"rpc_collector received raw message: {message}", "DEBUG")  # log the raw message
        except asyncio.TimeoutError:
            self.__log(f"rpc_collector emptied received messaged buffer","DEBUG")
            return True
        msg_json = json.loads(message)

        if not isinstance(msg_json, dict):
            self.__log(f"rpc_collector wss message  has wrong type: {type(msg_json)}","ERROR")
            return False 

        params = msg_json.get("params")
        if not isinstance(params, dict):
            self.__log(f"rpc_collector params in message has wrong type: {type(params)}","ERROR")
            return False
        
        result = params.get("result")
        if not isinstance(params, dict):
            self.__log(f"rpc_collector result in params object has wrong type: {type(result)}","ERROR")
            return False

        block_number = result.get("number")
        if not isinstance(block_number, str):
            self.__log(f"rpc_collector number in result object has wrong type: {type(block_number)}","ERROR")
            return False
        
        payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [block_number, True],
                "id": 1
        }

        try:
            response = await self.__https_cli.post(self.__infura_https_url, json=payload)
            if response.status != 200:
                self.__log(f"rpc_collector https fetch failed: {response.status}", "ERROR")
                await response.release()
                return False

            block = await response.json()
            await response.release()  # release the connection back to the pool
        except aiohttp.ClientError as e:
            self.__log(f"rpc_collector https request failed: {e}", "ERROR")
            return False
            
        self.__log(f"rpc_collector fetched block number {block_number}", "DEBUG")

        block_number_int = int(block_number, 16)

        try:
            ts = int(time.time())
            cur = self.__blocks_db.cursor()
            cur.execute(
                "INSERT INTO blocks (insert_time, block_number, block) VALUES (?, ?, ?)",
                (ts, block_number_int, message)
            )
            self.__blocks_db.commit()
        except sqlite3.Error as e:
            self.__log(f"rpc_collector failed to insert block number {block_number}: {e}", "ERROR")
            return False
            
        self.__log(f"rpc_collector inserted block number {block_number} into blocks.db", "DEBUG")
        return True
    
