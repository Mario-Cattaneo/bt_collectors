import asyncio
import aiohttp
import sqlite3
import json
import websockets
import os
import time
import numpy as np

LOG_INTERVAL = 300 # seconds between analytics logging
log_counter = 0    # counts 15s cycles

DB_FILE = "markets.db"
MARKET_HTTP_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
MARKETS_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SUBSCRIBED_TOKEN_COUNT = 0
TOKEN_BOOK_DIR = "token_books"
OFFSET = 95000

markets_ws_cli = None
reserve_ws_cli = None
consumer_task = None
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
    print("\n")
    if not token_dbs:
        print("⚠️ No token databases to scrape.")
        return {}

    tables = ["price_change", "book", "last_trade_price", "tick_size_change"]
    table_counts_all_tokens = {table: [] for table in tables}

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

    stats = {}
    for table, counts in table_counts_all_tokens.items():
        if not counts:
            stats[table] = None
            print(f"📊 {table}: No data")
            continue

        counts_array = np.array(counts)
        mean = counts_array.mean()
        second_moment = np.mean((counts_array - mean)**2)
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

        print(f"📊 Stats for table '{table}':")
        print(f"    Mean: {mean:.2f}, 2nd Moment (Var): {second_moment:.2f}, 3rd Moment: {third_moment:.2f}")
        print(f"    Min: {min_val}, Max: {max_val}")
        print(f"    Octiles (1/8th ... 7/8th): {octiles.tolist()}")
        print("-" * 60)
    # Latency stats
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
        print("-" * 60 + "\n") 
    else:
        print("⏱ Latency: No data yet" + "\n")

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
            cli_timestamp INTEGER NOT NULL
        )
    """)
    conn.commit()

def close_db():
    global conn
    if conn:
        conn.commit()
        conn.close()

def init_token_books_dir():
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
                    cli_timestamp INTEGER NOT NULL
                )
            """)
        conn_token.commit()
    token_dbs[token_id] = conn_token
    return conn_token


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
        #print(f"Fetching markets with OFFSET={OFFSET}")
        url = MARKET_HTTP_URL.format(offset=OFFSET)
        async with markets_http_cli.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()
            if len(data) == 0:
                #print(f"No more data at OFFSET={OFFSET}")
                break
            handle_market_data(data)
        OFFSET += len(data)


async def subscribe_tokens(use_reserve=False):
    global markets_ws_cli, reserve_ws_cli, SUBSCRIBED_TOKEN_COUNT, consumer_task

    ws_name = "reserve_ws_cli" if use_reserve else "markets_ws_cli"
    print(f"\n🔄 Subscribing using {ws_name}...")

    ws = await websockets.connect(MARKETS_WS_URL)
    payload = {
        "type": "market",
        "initial_dump": True,
        "assets_ids": list(token_dbs.keys())
    }
    await ws.send(json.dumps(payload))

    if use_reserve:
        reserve_ws_cli = ws
    else:
        markets_ws_cli = ws
        # start the consumer for active socket
        if consumer_task is None or consumer_task.done():
            consumer_task = asyncio.create_task(handle_ws_responses(markets_ws_cli))

    SUBSCRIBED_TOKEN_COUNT = len(token_dbs)
    print(f"✅ Subscribed to {SUBSCRIBED_TOKEN_COUNT} tokens using {ws_name}.")

async def handle_tokens():
    global SUBSCRIBED_TOKEN_COUNT, markets_ws_cli, reserve_ws_cli, consumer_task

    if len(token_dbs) > SUBSCRIBED_TOKEN_COUNT:
        print("🔄 New token(s) detected, preparing to subscribe...")
        # Create reserve WS
        reserve_ws_cli = await websockets.connect(MARKETS_WS_URL)
        payload = {
            "type": "market",
            "initial_dump": True,
            "assets_ids": list(token_dbs.keys())
        }
        await reserve_ws_cli.send(json.dumps(payload))
        print("✅ Reserve websocket subscribed.\n")

        # Swap connections
        old_ws = markets_ws_cli
        markets_ws_cli = reserve_ws_cli
        reserve_ws_cli = None

        # Cancel old consumer task if running
        if consumer_task is not None and not consumer_task.done():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

        # Start consumer on the new active websocket
        consumer_task = asyncio.create_task(handle_ws_responses(markets_ws_cli))

        # Close old websocket after switching
        if old_ws is not None:
            await old_ws.close()
            print("🛑 Closed old websocket after switching.")

        SUBSCRIBED_TOKEN_COUNT = len(token_dbs)

async def process_event(event: dict):
    global event_printed, latency_bucket
    cli_ts = int(time.time()*1000)  # UNIX ms for WS events
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
            #print(f"📝 First debug for event_type '{event_type}': {event}")
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

async def handle_ws_responses(ws):
    async for message in ws:
        if isinstance(message, str) and message.upper() in ("PING", "PONG"):
            continue
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            print(f"❌ Failed to decode JSON: {message}")
            continue

        if isinstance(data, list):
            for entry in data:
                await process_event(entry)
        elif isinstance(data, dict):
            await process_event(data)


async def handle_markets():
    global log_counter
    while True:
        #print("\nhandling markets")
        await new_markets()
        await handle_tokens()
        if log_counter * 15 >= LOG_INTERVAL:
            scrape_db_stats()
            log_counter = 0
        await asyncio.sleep(15)
        log_counter += 1

async def main():
    global markets_ws_cli, markets_http_cli

    init_db()
    init_token_books_dir()

    markets_http_cli = aiohttp.ClientSession()
    await new_markets()

    await subscribe_tokens()  # active websocket
    await asyncio.gather(
        handle_markets(),
        # the websocket consumer runs as consumer_task
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
