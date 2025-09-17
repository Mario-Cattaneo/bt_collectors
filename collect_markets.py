import asyncio
import aiohttp
import sqlite3
import json
import websockets

DB_FILE = "markets.db"
API_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SUBSCRIBED_TOKEN_COUNT = 0
OFFSET = 85000

ws_curr = None
ws_next = None

token_map = {}  # Predefined tokens with counts

conn = None
cur = None


def init_db():
    global conn, cur
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS markets")
    cur.execute("""
        CREATE TABLE markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL
        )
    """)
    conn.commit()


def close_db():
    global conn
    if conn:
        conn.commit()
        conn.close()


def handle_market_data(data):
    """Insert markets into DB and add new tokens to token_map with all counts set to 0."""
    global cur, conn, token_map
    for market in data:
        cur.execute("INSERT INTO markets (text) VALUES (?)", (json.dumps(market),))
        closed = market.get("closed", False)
        clob_ids_str = market.get("clobTokenIds")
        if not closed and clob_ids_str:
            try:
                clob_ids = json.loads(clob_ids_str)
                for token_id in clob_ids:
                    if token_id not in token_map:
                        # Initialize all counts to 0
                        token_map[token_id] = {
                            "price_change": 0,
                            "book": 0,
                            "last_trade_price": 0,
                            "tick_size_change": 0
                        }
            except json.JSONDecodeError:
                print(f"Failed to parse clobTokenIds: {clob_ids_str}")
    conn.commit()


async def new_markets(session):
    global OFFSET
    while True:
        print(f"Fetching markets with OFFSET={OFFSET}")
        url = API_URL.format(offset=OFFSET)
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Request failed with status {resp.status}")
                break
            data = await resp.json()
            if not data:
                print(f"No more data at OFFSET={OFFSET}")
                break
            handle_market_data(data)
        OFFSET += 500


async def subscribe_tokens():
    global ws_curr, ws_next, SUBSCRIBED_TOKEN_COUNT

    print("🔄 Resubscribing to tokens...")

    if ws_curr is not None:
        await ws_curr.close()

    ws_curr = ws_next
    ws_next = await websockets.connect(WS_URL)

    payload = {
        "type": "market",
        "initial_dump": True,
        "assets_ids": list(token_map.keys())
    }

    await ws_curr.send(json.dumps(payload))
    SUBSCRIBED_TOKEN_COUNT = len(token_map)
    print(f"✅ Subscribed to {SUBSCRIBED_TOKEN_COUNT} tokens.")


async def handle_tokens():
    global SUBSCRIBED_TOKEN_COUNT

    print(f"📊 handle_tokens(): SUBSCRIBED_TOKEN_COUNT={SUBSCRIBED_TOKEN_COUNT}, token_map size={len(token_map)}")

    if len(token_map) > SUBSCRIBED_TOKEN_COUNT:
        await subscribe_tokens()


def dump_token_counts(filename="tokens.txt"):
    """Append current token counts to a file, filtering for book > 0 and including summary."""
    total_tokens = len(token_map)
    tokens_with_book = [tid for tid, counts in token_map.items() if counts['book'] > 0]
    count_with_book = len(tokens_with_book)

    with open(filename, "a") as f:
        f.write("==== Token Counts Dump ====\n")
        f.write(f"Total tokens: {total_tokens}, Tokens with book>0: {count_with_book}\n\n")

        for token_id in tokens_with_book:
            counts = token_map[token_id]
            line = f"{token_id} {counts['price_change']} {counts['book']} {counts['last_trade_price']} {counts['tick_size_change']}\n"
            f.write(line)

        f.write("\n\n")  # 2 new lines to separate dumps


async def handle_markets(session):
    while True:
        print("handling markets")
        #dump_token_counts()
        await new_markets(session)
        await handle_tokens()
        await asyncio.sleep(15)


async def process_event(event: dict):
    """Process a single WS event and increment counters in token_map if token exists."""
    event_type = event.get("event_type")
    if not event_type:
        print(f"⚠️ Missing event_type in {event}")
        return

    affected_assets = []

    if event_type == "price_change":
        for pc in event.get("price_changes", []):
            asset_id = pc.get("asset_id")
            if asset_id:
                affected_assets.append(asset_id)

    elif event_type == "book":
        asset_id = event.get("asset_id")
        if asset_id:
            affected_assets.append(asset_id)

    elif event_type == "last_trade_price":
        ltp = event.get("last_trade_price")
        if isinstance(ltp, dict):
            asset_id = ltp.get("asset_id")
            if asset_id:
                affected_assets.append(asset_id)
        elif isinstance(ltp, list):
            for entry in ltp:
                asset_id = entry.get("asset_id")
                if asset_id:
                    affected_assets.append(asset_id)

    elif event_type == "tick_size_change":
        tsc = event.get("tick_size_change")
        if isinstance(tsc, dict):
            asset_id = tsc.get("asset_id")
            if asset_id:
                affected_assets.append(asset_id)
        elif isinstance(tsc, list):
            for entry in tsc:
                asset_id = entry.get("asset_id")
                if asset_id:
                    affected_assets.append(asset_id)

    for asset_id in affected_assets:
        if asset_id in token_map:
            token_map[asset_id][event_type] += 1
        else:
            print(f"⚠️ Event for unknown token: {asset_id}, type: {event_type}")

    #if affected_assets:
        #print(f"📊 {event_type}: processed {len(affected_assets)} assets")


async def handle_curr_ws_responses(ws):
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
        else:
            print(f"⚠️ Unexpected WS message format: {data}")


async def main():
    global ws_curr, ws_next

    init_db()
    async with aiohttp.ClientSession() as session:
        await new_markets(session)

        ws_curr = await websockets.connect(WS_URL)
        ws_next = await websockets.connect(WS_URL)

        await new_markets(session)
        await handle_tokens()

        await asyncio.gather(
            handle_markets(session),
            handle_curr_ws_responses(ws_curr)
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_db()
