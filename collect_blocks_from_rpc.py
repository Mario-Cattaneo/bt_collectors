import asyncio
import json
import os
import sqlite3
import aiohttp
from dotenv import load_dotenv
from websockets import connect

load_dotenv()

INFURA_API_KEY = os.getenv("INFURA_API_KEY")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")

INFURA_WS = f"wss://polygon-mainnet.infura.io/ws/v3/{INFURA_API_KEY}"
ALCHEMY_WS = f"wss://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

INFURA_HTTP = f"https://polygon-mainnet.infura.io/v3/{INFURA_API_KEY}"
ALCHEMY_HTTP = f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

DB_NAME = "blocks.db"
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS infura_blocks (
    number TEXT PRIMARY KEY,
    block_json TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS alchemy_blocks (
    number TEXT PRIMARY KEY,
    block_json TEXT
)
""")
conn.commit()

async def subscribe_new_blocks(provider_name, ws_url, queue):
    async with connect(ws_url) as ws:
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        }))
        subscribe_response = json.loads(await ws.recv())
        print(f"{provider_name} subscribed: {subscribe_response}")

        while True:
            message = json.loads(await ws.recv())
            if message.get("method") == "eth_subscription":
                block_header = message["params"]["result"]
                block_number = block_header["number"]
                print(f"{provider_name} new block: {int(block_number, 16)}")
                await queue.put((provider_name, block_number))

async def fetch_and_store_blocks_http(provider_name, http_url, queue, table_name):
    async with aiohttp.ClientSession() as session:
        while True:
            provider, block_number = await queue.get()

            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [block_number, True],
                "id": 1
            }

            try:
                async with session.post(http_url, json=payload) as resp:
                    response = await resp.json()
                    full_block = response.get("result")
                    if full_block:
                        cursor.execute(
                            f"INSERT OR REPLACE INTO {table_name} (number, block_json) VALUES (?, ?)",
                            (block_number, json.dumps(full_block))
                        )
                        conn.commit()
                        print(f"{provider_name} stored block {int(block_number,16)}")
                    else:
                        print(f"{provider_name} no block data for {block_number}")

            except Exception as e:
                print(f"Error fetching block {block_number} from {provider_name}: {e}")

async def main():
    infura_queue = asyncio.Queue()
    alchemy_queue = asyncio.Queue()

    await asyncio.gather(
        subscribe_new_blocks("Infura", INFURA_WS, infura_queue),
        fetch_and_store_blocks_http("Infura", INFURA_HTTP, infura_queue, "infura_blocks"),
        subscribe_new_blocks("Alchemy", ALCHEMY_WS, alchemy_queue),
        fetch_and_store_blocks_http("Alchemy", ALCHEMY_HTTP, alchemy_queue, "alchemy_blocks"),
    )

if __name__ == "__main__":
    asyncio.run(main())
