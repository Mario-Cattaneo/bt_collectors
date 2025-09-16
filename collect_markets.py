import asyncio
import aiohttp
import sqlite3
import json
from datetime import datetime, timezone

DB_FILE = "markets.db"
API_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"

OFFSET = 0
token_map = {}  # Map to store token IDs as keys and integers as values


def init_db():
    """Initialize SQLite database and table, dropping existing table first."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Drop the table if it exists
    cur.execute("DROP TABLE IF EXISTS markets")

    # Create a fresh table
    cur.execute("""
        CREATE TABLE markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


async def get_markets_from_offset(session):
    """Fetch markets from the API and store in DB, updating OFFSET."""
    global OFFSET
    while True:
        url = API_URL.format(offset=OFFSET)
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()

            if not data:
                break  # stop if no more markets

            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            for market in data:
                # store raw JSON
                cur.execute("INSERT INTO markets (text) VALUES (?)",
                            (json.dumps(market),))

                # process clobTokenIds if endDate is in the future
                end_date_str = market.get("endDate")
                clob_ids_str = market.get("clobTokenIds")

                if end_date_str and clob_ids_str:
                    end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if end_date > now:
                        # clobTokenIds is a JSON string; parse it
                        try:
                            clob_ids = json.loads(clob_ids_str)
                            for token_id in clob_ids:
                                token_map[token_id] = 0  # store integer placeholder
                                print(f"Added to map: {token_id} -> 0")
                        except json.JSONDecodeError:
                            print(f"Failed to parse clobTokenIds: {clob_ids_str}")

            conn.commit()
            conn.close()

            print(f"Stored {len(data)} markets at offset {OFFSET}")

        OFFSET += 500


async def scrape_new_markets():
    """Run scraper every 10 seconds."""
    async with aiohttp.ClientSession() as session:
        while True:
            print("\nStarting market scrape...")
            await get_markets_from_offset(session)
            print("Scrape finished, sleeping 10 seconds...")
            await asyncio.sleep(10)


if __name__ == "__main__":
    init_db()
    asyncio.run(scrape_new_markets())
