import asyncio
import asyncpg
import websockets
import json
import time
from log import log
import market

class analytics:
    __slots__("book","changes")
    def __init__():
        self.book = None
        self.changes = []
    @static_method
    def insert_change()

async def event_init(conn_pool: asyncpg.pool.Pool, ):


async def collector(init_resub_count: int = 0, 
                    init_tokens: list = [], 
                    new_tokens: list, 
                    analytics_queue: asyncio.Queue, 
                    insert_books_queue: asyncio.Queue, 
                    insert_changes_queue: asyncio.Queue, 
                    insert_ticks_queue: asyncio.Queue):
    log(f"event collector starting", "INFO")
    resubscription_count = init_resub_count
    tokens = init_tokens
    analytics_map = {}
    try:
        ws_cli = websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market")
        subscription = {"type": "market", "initial_dump": True, "assets_ids": tokens_arr}
        await ws_cli.send(json.dumps(subscription))
    except
    while True:
            new_token_count = len(new_tokens)
            if  new_token_count > 0:
                tokens.extend(new_tokens)
                for token in new_tokens:
                    analytics_map[token] = analytics()
                try:
                    ws_cli = await resubscribe(ws_cli, 
                            tokens, 
                            analytics_queue,
                            insert_books_queue, 
                            insert_changes_queue,
                            insert_ticks_queue)
                except:

                resubscription_count += 1
                log(f"event collector resubscribed for the {resubscription_count}. time with {len(tokens)} tokens", "INFO")

            try:
                await read_events(ws_cli,
                    analytics_queue,
                    insert_books_queue, 
                    insert_changes_queue,
                    insert_ticks_queue)
            except:

            async for message in ws_cli:
                if __debug__:
                    self.__log(f"event_collector raw message: {message}", "DEBUG")
                msg_json = json.loads(message)

                if isinstance(msg_json, dict):
                    msg_json = [msg_json]
                
                if not isinstance(msg_json, list):
                    log(f"event collector received non list or dict event message: {msg_json}", "WARNING")
                    continue
                    
                if len(msg_json) == 0:
                    log(f"event collector received empty event: {msg_json}", "WARNING")
                    continue

                event_type = msg_json[0].get("event_type", None)
                timestamp = msg_json[0].get("timestamp", None)
                market = msg_json[0].get("market", None)

                if event_type == "book":
                    insert_batch = []
                    for book in msg_json:
                        asset_id = book.get("asset_id", None)
                        if not isinstance(asset_id, str):
                            log(f"event collector found invalid asset_id {asset_id} in {book} in message {msg_json}", "WARNING")
                            continue
                        
                        bids = book.get("bids", None)
                        if not isinstance(bids, list):
                            log(f"event collector found invalid bids {bids} in {book} in message {msg_json}", "WARNING")
                            continue
                        
                        asks = book.get("asks", None)
                        if not isinstance(asks, list):
                            log(f"event collector found invalid asks {asks} in {book} in message {msg_json}", "WARNING")
                            continue
                        
                        timestamp = book.get("timestamp", None)
                        if not isinstance(timestamp, str):
                            log(f"event collector found invalid timestamp {timestamp} in {book} in message {msg_json}", "WARNING")
                            continue
                        
                        market = book.get("market", None)
                        if not isinstance(market, str):
                            log(f"event collector found invalid market {market} in {book} in message {msg_json}", "WARNING")
                            continue
                        
                        insert_batch.append([resubscription_count, timestamp, market, asset_id, json.dumps(bids), json.dumps(asks)])

                        if analytics_map[asset_id].book is not None:
                            if __debug__:
                                log(f"event collector queueing analytics map: {analytics_map}", "DEBUG")
                            await analytics_queue.put(analytics_map)
                            analytics_map = {}
                            for token in tokens:
                                analytics_map[token] = analytics()
                            
                        analytics_map[asset_id].book = [timestamp, json.dumps(bids), json.dumps(asks)]
                    
                    if __debug__:
                        log(f"event collector queueing book batch of size {len(insert_batch)}: {insert_batch}", "DEBUG")
                    await insert_books_queue.put(insert_batch)
             
                elif event_type == "price_change":
                    insert_batch = []
                    price_changes = msg_json[0].get("price_changes", None)
                    if not isinstance(price_changes, list):
                        log(f"event collector found invalid price_changes {price_changes} in {msg_json[0]} in message {msg_json}", "WARNING")
                        continue
                    
                    if not isinstance(timestamp, str):
                        log(f"event collector found invalid timestamp {timestamp} in {msg_json[0]} in message {msg_json}", "WARNING")
                        continue

                     if not isinstance(market, str):
                        log(f"event collector found invalid market {market} in {msg_json[0]} in message {msg_json}", "WARNING")
                        continue

                    for price_change in price_changes:
                        asset_id = price_change.get("asset_id", None)
                        if not isinstance(asset_id, str):
                            log(f"event collector found invalid asset_id {asset_id} in {price_change} in  {price_changes} in {msg_json}", "WARNING")
                            continue

                        price = price_change.get("price", None)
                        if not isinstance(price, str):
                            log(f"event collector found invalid price {price} in {price_change} in {price_changes} in {msg_json}", "WARNING")
                            continue
                            
                        size = price_change.get("size", None)
                        if not isinstance(size, str):
                            log(f"event collector found invalid size {size} in {price_change} in {price_changes} in {msg_json}", "WARNING")
                            continue

                        side = price_change.get("side", None)
                        if not isinstance(side, str):
                            log(f"event collector found invalid side {side} in {price_change} in {price_changes} in {msg_json}", "WARNING")
                            continue

                        best_bid = price_change.get("best_bid", None)
                        if not isinstance(best_bid, str):
                            log(f"event collector found invalid best_bid {best_bid} in {price_change} in {price_changes} in {msg_json}", "WARNING")
                            continue

                        best_ask = price_change.get("best_ask", None)
                        if not isinstance(best_ask, str):
                            log(f"event collector found invalid best_ask {best_ask} in {price_change} in {price_changes} in {msg_json}", "WARNING")
                            continue

                        analytics_map[asset_id].insert([timestamp, event_type, side, price, size, best_bid, best_ask])

                        insert_batch.append([resubscription_count, timestamp, market, event_type, asset_id, side, price, quantitiy, best_bid, best_ask])
                    
                    if __debug__:
                        log(f"event collector queueing price_change batch of size {len(insert_batch)}: {insert_batch}", "DEBUG")
                    await insert_changes_queue.put(insert_batch)
                    
                elif event_type == "last_trade_price":
                    insert_batch = []
                    for last_trade in msg_json:
                        fee_rate_bps = last_trade.get("fee_rate_bps", None)
                        if not isinstance(fee_rate_bps, str):
                            log(f"event collector found invalid fee_rate_bps {fee_rate_bps} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        market = last_trade.get("market", None)
                        if not isinstance(market, str):
                            log(f"event collector found invalid market {market} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        price = last_trade.get("price", None)
                        if not isinstance(price, str):
                            log(f"event collector found invalid price {price} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        side = last_trade.get("side", None)
                        if not isinstance(side, str):
                            log(f"event collector found invalid side {side} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        size = last_trade.get("size", None)
                        if not isinstance(size, str):
                            log(f"event collector found invalid size {size} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        timestamp = last_trade.get("timestamp", None)
                        if not isinstance(timestamp, str):
                            log(f"event collector found invalid timestamp {timestamp} in {last_trade} in message {msg_json}", "WARNING")
                            continue

                        analytics_map[asset_id].insert([timestamp, event_type, side, price, size, fee_rate_bps])

                        insert_batch.append([resubscription_count, timestamp, market, event_type, asset_id, side, price, size, fee_rate_bpss])
                    if __debug__:
                        log(f"event collector queuing last_trade_price batch of size {len(insert_batch)}: {insert_batch}", "DEBUG")
                    await insert_changes_queue.put(insert_batch)
                    
                elif event_type == "tick_size_change":
                    insert_batch = []
                    for tick_change in msg_json:
                        asset_id = tick_change.get("asset_id", None)
                        if not isinstance(asset_id, str):
                            log(f"event collector found invalid asset_id {asset_id} in {tick_change} in message {msg_json}", "WARNING")
                            continue

                        market = tick_change.get("market", None)
                        if not isinstance(market, str):
                            log(f"event collector found invalid market {market} in {tick_change} in message {msg_json}", "WARNING")
                            continue

                        old_tick_size = tick_change.get("old_tick_size", None)
                        if not isinstance(old_tick_size, str):
                            log(f"event collector found invalid old_tick_size {old_tick_size} in {tick_change} in message {msg_json}", "WARNING")
                            continue

                        new_tick_size = tick_change.get("new_tick_size", None)
                        if not isinstance(new_tick_size, str):
                            log(f"event collector found invalid new_tick_size {new_tick_size} in {tick_change} in message {msg_json}", "WARNING")
                            continue

                        timestamp = tick_change.get("timestamp", None)
                        if not isinstance(timestamp, str):
                            log(f"event collector found invalid timestamp {timestamp} in {tick_change} in message {msg_json}", "WARNING")
                            continue

                        insert_batch.append([resubscription_count, timestamp, market, event_type, asset_id, side, price, size, fee_rate_bpss])

                    if __debug__:
                        log(f"event collector queuing tick_size_change batch of size {len(insert_batch)}: {insert_batch}", "DEBUG")
                    await insert_changes_queue.put(insert_batch)
                    
                else:
                    log(f"event collector invalid event type {event_type} found in {msg_json}", "WARNING")
                    continue

        except: 
        
async def resubscribe():
    try:
        
    except:


async def insert(conn_pool: asyncpg.pool.Pool, insert_books_queue: asyncio.Queue, insert_changes_queue: asyncio.Queue, insert_ticks_queue: asyncio.Queue):
    log()
                



