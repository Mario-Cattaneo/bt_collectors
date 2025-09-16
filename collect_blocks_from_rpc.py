import asyncio
import json
import os
import sqlite3
from websockets import connect

# API keys from environment
INFURA_API_KEY = os.getenv("INFURA_API_KEY")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")

# Build WS URLs using API keys
INFURA_WS = f"wss://polygon-mainnet.infura.io/ws/v3/{INFURA_API_KEY}"
ALCHEMY_WS = f"wss://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

DB_NAME = "blocks.db"

# Setup SQLite DB and tables
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

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

async def send_rpc(ws, method, params, request_id):
    request = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": request_id
    }
    await ws.send(json.dumps(request))
    while True:
        response_raw = await ws.recv()
        response = json.loads(response_raw)
        if "id" in response and response["id"] == request_id:
            return response.get("result")

async def handle_new_block(provider_name, ws_url, table_name):
    async with connect(ws_url) as ws:
        # Subscribe to newHeads
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        }))
        subscribe_response_raw = await ws.recv()
        subscribe_response = json.loads(subscribe_response_raw)
        print(f"{provider_name} subscribed: {subscribe_response}")

        request_id_counter = 2

        while True:
            try:
                message_raw = await ws.recv()
                message = json.loads(message_raw)

                # Check for subscription notification of new block header
                if message.get("method") == "eth_subscription":
                    params = message["params"]
                    block_header = params["result"]
                    block_number_hex = block_header["number"]
                    print(f"{provider_name} New block received: {int(block_number_hex,16)}")

                    # Fetch full block over WebSocket RPC call
                    full_block = await send_rpc(
                        ws,
                        "eth_getBlockByNumber",
                        [block_number_hex, True],  # True for full tx objects
                        request_id_counter
                    )
                    request_id_counter += 1

                    if full_block:
                        cursor.execute(
                            f"INSERT OR REPLACE INTO {table_name} (number, block_json) VALUES (?, ?)",
                            (block_number_hex, json.dumps(full_block))
                        )
                        conn.commit()
                        print(f"{provider_name} Stored block {int(block_number_hex,16)}")
                    else:
                        print(f"{provider_name} Failed to fetch block {int(block_number_hex,16)}")

            except Exception as e:
                print(f"{provider_name} error: {e}")
                await asyncio.sleep(5)
                return  # exit to allow restart/reconnection by main

async def main():
    await asyncio.gather(
        handle_new_block("Infura", INFURA_WS, "infura_blocks"),
        handle_new_block("Alchemy", ALCHEMY_WS, "alchemy_blocks"),
    )

if __name__ == "__main__":
    asyncio.run(main())
