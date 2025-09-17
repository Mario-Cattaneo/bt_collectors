import asyncio
import aiohttp
import sqlite3
import json
import websockets

DB_FILE = "markets.db"
MARKET_HTTP_URL = "https://gamma-api.polymarket.com/markets?limit=500&offset={offset}"
MARKETS_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SUBSCRIBED_TOKEN_COUNT = 0
OFFSET = 85000

markets_ws_cli = None
markets_http_cli = None

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
                        token_map[token_id] = {
                            "price_change": 0,
                            "book": 0,
                            "last_trade_price": 0,
                            "tick_size_change": 0
                        }
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
            if not data:
                print(f"No more data at OFFSET={OFFSET}")
                break
            handle_market_data(data)
        OFFSET += 500


async def subscribe_tokens():
    global markets_ws_cli, SUBSCRIBED_TOKEN_COUNT

    print("🔄 Resubscribing to tokens...")

    if markets_ws_cli is not None:
        await markets_ws_cli.close()

    markets_ws_cli = await websockets.connect(MARKETS_WS_URL)

    payload = {
        "type": "market",
        "initial_dump": True,
        "assets_ids": list(token_map.keys())
    }

    await markets_ws_cli.send(json.dumps(payload))
    SUBSCRIBED_TOKEN_COUNT = len(token_map)
    print(f"✅ Subscribed to {SUBSCRIBED_TOKEN_COUNT} tokens.")


async def handle_tokens():
    global SUBSCRIBED_TOKEN_COUNT
    print(f"📊 handle_tokens(): SUBSCRIBED_TOKEN_COUNT={SUBSCRIBED_TOKEN_COUNT}, token_map size={len(token_map)}")
    if len(token_map) > SUBSCRIBED_TOKEN_COUNT:
        await subscribe_tokens()


def dump_token_counts(filename="tokens.txt"):
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
        f.write("\n\n")


async def handle_markets():
    while True:
        print("handling markets")
        dump_token_counts()
        await new_markets()
        await handle_tokens()
        await asyncio.sleep(15)


async def process_event(event: dict):
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
            for entry in data:
                await process_event(entry)
        elif isinstance(data, dict):
            await process_event(data)
        else:
            print(f"⚠️ Unexpected WS message format: {data}")


async def main():
    global markets_ws_cli, markets_http_cli

    init_db()

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
