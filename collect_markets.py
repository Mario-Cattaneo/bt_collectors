import asyncio
import aiohttp
import sqlite3
import json
from datetime import datetime, timezone
import websockets
import base64
import os

DB_FILE = "markets.db"
API_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


token_map = {}       # Already seen tokens
new_tokens = asyncio.Queue()  # Tokens waiting for subscription


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS markets")
    cur.execute("""
        CREATE TABLE markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


async def get_markets(session):
    OFFSET = 0
    while True:
        print(f"Fetching markets with OFFSET={OFFSET}")
        url = API_URL.format(offset=OFFSET)
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()
            if not data:
                #waiting for next scrape
                print(f"No more data at OFFSET={OFFSET}")
                await asyncio.sleep(15)
                continue
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            for market in data:
                cur.execute("INSERT INTO markets (text) VALUES (?)", (json.dumps(market),))
                end_date_str = market.get("endDate")
                clob_ids_str = market.get("clobTokenIds")
                if end_date_str and clob_ids_str:
                    end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if end_date > now:
                        try:
                            clob_ids = json.loads(clob_ids_str)
                            for token_id in clob_ids:
                                if token_id not in token_map:
                                    token_map[token_id] = 1
                                    await new_tokens.put(token_id)  # enqueue new token
                                    #print(f"New token queued for subscription: {token_id}")
                        except json.JSONDecodeError:
                            print(f"Failed to parse clobTokenIds: {clob_ids_str}")
            conn.commit()
            conn.close()
        OFFSET += 500
        await asyncio.sleep(0)  # yield control

async def send_new_tokens(ws):
    """Send a fixed subscription payload (or could later send queued tokens)."""
    payload = {
        "type": "market",
        "assets_ids": [
            "7988364699307342268260495915445731509602502693363579435248273813409237524599",
            "27120152309306977992209464424197509161348402683341807871062277866907268946377"
        ]
    }
    await ws.send(json.dumps(payload))
    print(f"📤 Sent subscription payload: {json.dumps(payload)}")


async def receive_messages(ws):
    """Continuously print all received messages as strings."""
    async for message in ws:
        # Ignore ping/pong
        if isinstance(message, str) and message.upper() in ("PING", "PONG"):
            continue
        print(f"📥 Received message: {message}")


async def main():
    init_db()
    ws = await websockets.connect(WS_URL)
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            get_markets(session),
            send_new_tokens(ws),
            receive_messages(ws)
        )

if __name__ == "__main__":
    asyncio.run(main())
