import asyncio
import json
import os
import sqlite3
import aiohttp
from dotenv import load_dotenv
from websockets import connect

class rpc_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG"):
        self.__infura_ws_url = f"wss://polygon-mainnet.infura.io/ws/v3/{INFURA_API_KEY}"
        self.__infura_api_key = os.getenv("INFURA_API_KEY")
        self.__blocks_db = None
        self.__running = False
        self.__ws_cli = None
    
    async def start(self):

        if self.__running:
            self.__log("rpc_collector already started", "ERROR")
            return False
        try: 
            os.makedirs(self.__data_dir, exist_ok=True)
            blocks_db_path = os.path.join(self.__data_dir, "blocks.db")
            if os.path.exists(blocks_db_path):
                os.remove(blocks_db_path)
                self.__log(f"rpc_collector removed old blocks.db", "DEBUG")

            self.__blocks_db = sqlite3.connect(blocks_db_path)
            cur = self.__blocks_db.cursor()
            cur.execute("DROP TABLE IF EXISTS blocks")
            cur.execute("""
                CREATE TABLE blocks (
                    row_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    insert_time INTEGER,
                    block_number TEXT,
                    block TEXT
                )
            """)
            self.__blocks_db.commit()
        except (OSError, sqlite3.Error) as e:
            self.__log(f"rpc_collector failed to create collector.db: {e}", "ERROR")
            return False

        self.__log(f"rpc_collector created collector.db at {blocks_db_path}", "DEBUG")

        # keep 3 sokcets and keepalive forever
        connector = aiohttp.TCPConnector(limit_per_host=3, keepalive_timeout=99999)
        self.__markets_cli = aiohttp.ClientSession(connector=connector)
        
        self.__running = True
        self.__log("rpc_collector started", "INFO")

        if not self.__subscribe():
            self.__log(f"market_collector fialed to subscribe aborting", "DEBUG")
        while self.__running:
            if not await self.__read_announcements():
                await self.__clean_up()
        self.__log(f"market_collector exiting running loop due to abort", "DEBUG")


    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            print(f"[{level}] {msg}")
    
    async def __subscribe(self):
        url = self.__infura_ws_url.format(INFURA_API_KEY=self.__infura_api_key)
        try:
            await websockets.connect(self.__infura_ws_url)
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }))
            subscribe_response = json.loads(await ws.recv())
            self.__log(f"rpc_collector subscribed: {subscribe_response}", "INFO")
        except Exception as e:
            self.__log(f"rpc_collector subscription failed: {e}", "ERROR")
            return False



    async def __read_announcements(self):

    async def __fetch_full_block(self):
        
    
