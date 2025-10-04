import asyncio
import aiohttp
import asyncpg
import json
import time
from log import log


async def collector(markets_queue: asyncio.Queue, tokens_arr: list, tcp_session: aiohttp.ClientSession, rate_limit_ns=100 * 1_000_000, offset=0) -> int:
    log(f"market collector starting at offset {offset}", "INFO")
    last_request = -rate_limit_ns
    while True:
        now = time.monotonic_ns()
        should_be = last_request + rate_limit_ns
        if should_be > now:
            await asyncio.sleep((should_be - now) / 1_000_000_000)
        try:
            async with tcp_session.get(f"https://gamma-api.polymarket.com/markets?limit=500&offset={offset}") as response:
                new_markets = await response.json()
        
        except (aiohttp.ServerDisconnectedError, 
                aiohttp.ClientOSError,
                asyncio.TimeoutError) as e:
            log(f"market collector non fatal request failure: {e}", "WARNING")
        except Exception as e:
            log(f"market collector request fatal failure : {e}", "ERROR")
            return offset

        if not isinstance(new_markets, list):
            log(f"market collector received non list json response at offset {offset}: {json.dumps(new_markets)}", "ERROR")
            return offset

        insert_batch = []
        for index, market_obj in enumerate(new_markets):
            if not isinstance(market_obj, dict):
                log(f"market collector invalid market_obj at offset {offset+index}: {json.dumps(market_obj)}", "ERROR")
                continue

            market_id = market_obj.get("id")
            if not isinstance(market_id, str):
                log(f"market collector market missing or non string id attribute at offset {offset+index}", "WARNING")
                continue

            closed = market_obj.get("closed")
            if not isinstance(closed, bool):
                log(f"market collector invalid closed attr at offset {offset+index}", "WARNING")
                continue
            if closed:
                continue

            token_ids = market_obj.get("clobTokenIds")
            if not isinstance(token_ids, str):
                log(f"market collector missing clobTokenIds at offset {offset+index}", "WARNING")
                continue

            try:
                token_ids = json.loads(token_ids)
            except (json.JSONDecodeError, TypeError):
                log(f"market collector clobTokenIds not valid JSON at offset {offset+index}", "WARNING")
                continue

            if not (isinstance(token_ids, list) and len(token_ids) == 2):
                log(f"market collector invalid token_ids {token_ids} at offset {offset+index}", "WARNING")
                continue

            negrisk_id = market_obj.get("negRiskMarketID")

            log(f"collector found market id={market_id}, token_ids={token_ids}, negrisk={negrisk_id}", "INFO")
            tokens_arr.append(token_ids)
            
            insert_batch.append([market_id, token_ids[0], token_ids[1], negrisk_id, json.dumps(market_obj)])
        
        await markets_queue.put(insert_batch)
        offset += len(new_markets)


async def inserter(markets_queue: asyncio.Queue, conn_pool: asyncpg.pool.Pool, reset=True):
    log(f"market inserter started", "INFO")
    try:
        async with conn_pool.acquire() as conn:
            if reset:
                await conn.execute("DROP TABLE IF EXISTS markets")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                    row_index SERIAL PRIMARY KEY,
                    insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                    market_id VARCHAR(100),
                    token_id1 VARCHAR(100),
                    token_id2 VARCHAR(100),
                    negrisk_id VARCHAR(100),
                    market_obj TEXT
                );
            """)
    except (asyncpg.CannotConnectNowError,
            asyncio.TimeoutError,
            asyncio.CancelledError,
            asyncpg.SerializationError) as e:
        log(f"market inserter non fatal failure to initialize: {e}", "WARNING")
        retun True

        
    except Exception as e:
        log(f"market inserter fatal failure to initialize: {e}", "ERROR")
        return False

    while True:
        insert_batch = await markets_queue.get()
        try:
            async with conn_pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO markets (market_id, token_id1, token_id2, negrisk_id, market_obj)
                    VALUES ($1, $2, $3, $4, $5)
                """, insert_batch)
        except (asyncpg.CannotConnectNowError,
                asyncio.TimeoutError,
                asyncio.CancelledError,
                asyncpg.SerializationError) as e:
            log(f"market inserter non fatal failure to insert batch: {e}", "WARNING")
            await asyncio.sleep(1)
            continue
        except Exception as e:
            log(f"market inserter fatal batch insert failure: {e}", "ERROR")
        finally:
            markets_queue.task_done()
