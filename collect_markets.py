import asyncio
import aiohttp
import sqlite3
import json
import websockets
import os
import time
import numpy as np

DB_FILE = "markets.db"
MARKET_HTTP_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
MARKETS_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SUBSCRIBED_TOKEN_COUNT = 0
TOKEN_BOOK_DIR = "token_books"
OFFSET = 95000

markets_ws_cli = None
markets_http_cli = None

# Map token_id -> sqlite connection
token_dbs = {}

event_printed = {
    "book": 0,
    "last_trade_price": 0,
    "tick_size_change": 0
}

latency_bucket = []

conn = None
cur = None

def scrape_db_stats():
    """
    Scrape all token DBs, count rows per table, compute statistics, and print inline.
    Statistics: mean, 2nd & 3rd moments, min, max, and 8-tiles (1/8th, ..., 7/8th).
    """
    if not token_dbs:
        print("⚠️ No token databases to scrape.")
        return {}

    tables = ["price_change", "book", "last_trade_price", "tick_size_change"]
    table_counts_all_tokens = {table: [] for table in tables}

    # Count rows per table for each token
    for token_id, db_conn in token_dbs.items():
        cur_token = db_conn.cursor()
        for table in tables:
            try:
                cur_token.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur_token.fetchone()[0]
            except sqlite3.Error as e:
                print(f"⚠️ Error counting table {table} for token {token_id}: {e}")
                count = 0
            table_counts_all_tokens[table].append(count)

    # Compute and print statistics per table
    stats = {}
    for table, counts in table_counts_all_tokens.items():
        if not counts:
            stats[table] = None
            print(f"📊 {table}: No data")
            continue

        counts_array = np.array(counts)
        mean = counts_array.mean()
        second_moment = np.mean((counts_array - mean)**2)  # variance
        third_moment = np.mean((counts_array - mean)**3)
        min_val = counts_array.min()
        max_val = counts_array.max()
        octiles = np.percentile(counts_array, [12.5, 25, 37.5, 50, 62.5, 75, 87.5])

        stats[table] = {
            "mean": mean,
            "2nd_moment": second_moment,
            "3rd_moment": third_moment,
            "min": min_val,
            "max": max_val,
            "octiles": octiles.tolist()
        }

        # Print inline for each table
        print(f"📊 Stats for table '{table}':")
        print(f"    Mean: {mean:.2f}, 2nd Moment (Var): {second_moment:.2f}, 3rd Moment: {third_moment:.2f}")
        print(f"    Min: {min_val}, Max: {max_val}")
        print(f"    Octiles (1/8th ... 7/8th): {octiles.tolist()}")
        print("-" * 60)


        if latency_bucket:
            arr = np.array(latency_bucket)
            mean = arr.mean()
            second_moment = np.mean((arr - mean)**2)
            third_moment = np.mean((arr - mean)**3)
            min_val = arr.min()
            max_val = arr.max()
            octiles = np.percentile(arr, [12.5, 25, 37.5, 50, 62.5, 75, 87.5])

            print(f"⏱ Latency stats (all websocket events):")
            print(f"    Mean: {mean:.2f} ms, Var: {second_moment:.2f}, 3rd Moment: {third_moment:.2f}")
            print(f"    Min: {min_val} ms, Max: {max_val} ms")
            print(f"    Octiles: {octiles.tolist()}")
            print("-" * 60)
        else:
            print("⏱ Latency: No data yet")

    return stats


def init_db():
    global conn, cur
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS markets")
    cur.execute("""
        CREATE TABLE markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            cli_timestamp INTEGER NOT NULL -- stored in UNIX seconds
        )
    """)
    conn.commit()


def close_db():
    global conn
    if conn:
        conn.commit()
        conn.close()

def init_token_db(token_id, market_id):
    db_name = os.path.join(TOKEN_BOOK_DIR, f"{token_id} {market_id}.db")
    if os.path.exists(db_name):
        conn_token = sqlite3.connect(db_name, check_same_thread=False)
    else:
        conn_token = sqlite3.connect(db_name, check_same_thread=False)
        cur_token = conn_token.cursor()
        for table in ["price_change", "book", "last_trade_price", "tick_size_change"]:
            cur_token.execute(f"DROP TABLE IF EXISTS {table}")
            cur_token.execute(f"""
                CREATE TABLE {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL,
                    cli_timestamp INTEGER NOT NULL -- stored in UNIX ms
                )
            """)
        conn_token.commit()
    token_dbs[token_id] = conn_token
    return conn_token

def init_token_books_dir():
    """Ensure token_books directory exists and clear all existing files."""
    if not os.path.exists(TOKEN_BOOK_DIR):
        os.makedirs(TOKEN_BOOK_DIR)
        print(f"📂 Created directory {TOKEN_BOOK_DIR}")
    else:
        for filename in os.listdir(TOKEN_BOOK_DIR):
            file_path = os.path.join(TOKEN_BOOK_DIR, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"⚠️ Failed to remove {file_path}: {e}")
        print(f"🗑 Cleared existing files in {TOKEN_BOOK_DIR}")
 

def handle_market_data(data):
    global cur, conn
    cli_ts = int(time.time())  # UNIX seconds
    for market in data:
        cur.execute(
            "INSERT INTO markets (text, cli_timestamp) VALUES (?, ?)",
            (json.dumps(market), cli_ts)
        )
        closed = market.get("closed", False)
        clob_ids_str = market.get("clobTokenIds")
        market_id = market.get("id")
        if not closed and clob_ids_str:
            try:
                clob_ids = json.loads(clob_ids_str)
                for token_id in clob_ids:
                    if token_id not in token_dbs:
                        init_token_db(token_id, market_id)
            except json.JSONDecodeError:
                print(f"Failed to parse clobTokenIds: {clob_ids_str}")
    conn.commit()

async def new_markets():
    global OFFSET, markets_http_cli
    while True:
        print(f"Fetching markets with OFFSET={OFFSET}")
        url = MARKET_HTTP_URL.format(offset=OFFSET)
        async with markets_http_cli.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()
            if len(data) == 0:
                print(f"No more data at OFFSET={OFFSET}")
                break
            handle_market_data(data)
        OFFSET += len(data)


async def subscribe_tokens():
    global markets_ws_cli, SUBSCRIBED_TOKEN_COUNT

    print("🔄 Resubscribing to tokens...")

    if markets_ws_cli is not None:
        await markets_ws_cli.close()

    markets_ws_cli = await websockets.connect(MARKETS_WS_URL)
    payload = {
        "type": "market",
        "initial_dump": True,
        "assets_ids": list(token_dbs.keys())
    }
    await markets_ws_cli.send(json.dumps(payload))
    SUBSCRIBED_TOKEN_COUNT = len(token_dbs)
    print(f"✅ Subscribed to {SUBSCRIBED_TOKEN_COUNT} tokens.")


async def handle_tokens():
    global SUBSCRIBED_TOKEN_COUNT
    if len(token_dbs) > SUBSCRIBED_TOKEN_COUNT:
        await subscribe_tokens()


async def process_event(event: dict):
    global event_printed, latency_bucket
    import time
    cli_ts = int(time.time()*1000)  # UNIX ms for ws events

    event_type = event.get("event_type")
    if not event_type:
        print(f"⚠️ Missing event_type in {event}")
        return

    # Track latency if timestamp exists
    msg_ts = event.get("timestamp")
    if msg_ts is not None:
        try:
            latency = cli_ts - int(msg_ts)
            latency_bucket.append(latency)
        except Exception:
            pass

    affected_assets = []
    if event_type == "price_change":
        for pc in event.get("price_changes", []):
            asset_id = pc.get("asset_id")
            if asset_id:
                affected_assets.append(asset_id)
    elif event_type in ["book", "last_trade_price", "tick_size_change"]:
        if event_printed.get(event_type, 0) == 0:
            print(f"📝 First debug for event_type '{event_type}': {event}")
            event_printed[event_type] += 1
        asset_id = event.get("asset_id")
        if asset_id:
            affected_assets.append(asset_id)

    for asset_id in affected_assets:
        db_conn = token_dbs.get(asset_id)
        if db_conn:
            cur_token = db_conn.cursor()
            try:
                cur_token.execute(
                    f"INSERT INTO {event_type} (data, cli_timestamp) VALUES (?, ?)",
                    (json.dumps(event), cli_ts)
                )
                db_conn.commit()
            except sqlite3.Error as e:
                print(f"⚠️ Failed to insert event for token {asset_id}, type {event_type}: {e}")
        else:
            print(f"⚠️ Event for unknown token: {asset_id}, type: {event_type}")

async def handle_curr_ws_responses():
    global markets_ws_cli
    async for message in markets_ws_cli:
        if isinstance(message, str) and message.upper() in ("PING", "PONG"):
            continue

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            print(f"❌ Failed to decode JSON: {message}")
            continue

        if isinstance(data, list):
            if len(data) > 1:
                print(f": Received list of {len(data)} events")
            for entry in data:
                await process_event(entry)
        elif isinstance(data, dict):
            await process_event(data)


async def handle_markets():
    while True:
        print("handling markets")
        await new_markets()
        await handle_tokens()
        scrape_db_stats()
        await asyncio.sleep(15)


async def main():
    global markets_ws_cli, markets_http_cli

    init_db()
    init_token_books_dir()

    markets_http_cli = aiohttp.ClientSession()
    await new_markets()

    markets_ws_cli = await websockets.connect(MARKETS_WS_URL)
    await new_markets()
    await handle_tokens()

    await asyncio.gather(
        handle_markets(),
        handle_curr_ws_responses()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if markets_http_cli:
            asyncio.run(markets_http_cli.close())
        close_db()
        for db_conn in token_dbs.values():
            db_conn.close()
