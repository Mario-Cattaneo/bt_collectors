import asyncio
import aiohttp
import sqlite3
import json
import time

DB_FILE = "markets.db"
API_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"

# global offset
MARKET_OFFSET = 0


def init_db():
    """Initialize SQLite database and table."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


async def get_markets_from_offset(session):
    """Fetch markets from the API, starting from global MARKET_OFFSET."""
    global MARKET_OFFSET
    while True:
        url = API_URL.format(offset=MARKET_OFFSET)
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()

            # stop if no more markets
            if not data:
                break

            # Store each market object as text
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            for market in data:
                cur.execute("INSERT INTO markets (text) VALUES (?)",
                            (json.dumps(market),))
            conn.commit()
            conn.close()

            print(f"Stored {len(data)} markets at offset {MARKET_OFFSET}")

        MARKET_OFFSET += 500  # update the global offset


async def scrape_markets():
    """Run scraper every 10 seconds."""
    async with aiohttp.ClientSession() as session:
        while True:
            start_time = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{start_time}] Starting market scrape...")
            await get_markets_from_offset(session)
            print("Scrape finished, sleeping 10 seconds...")
            await asyncio.sleep(10)


if __name__ == "__main__":
    init_db()
    asyncio.run(scrape_markets())
