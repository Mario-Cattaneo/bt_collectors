import asyncio
import aiohttp
import asyncpg
import json
import time
from log import log
import market

async def init():
    tokens_arr = []
    markets_queue = asyncio.Queue()
    tcp_conn = aiohttp.TCPConnector()
    try: 
        socket_dir = str(pathlib.Path("../.pgsocket").resolve())
        async with asyncpg.create_pool(
                    user="client",
                    password="clientpass",
                    database="data",
                    host=socket_dir,
                    port=5432,
                    min_size=1,
                    max_size=10,
                ) as db_conn_pool:
            async with aiohttp.ClientSession(connector=tcp_conn) as session:
                tasks = [asyncio.create_task(market.collector(markets_queue=markets_queue, tokens_arr=tokens_arr, tcp_session=session)),
                    asyncio.create_task(market.inserter(markets_queue=markets_queue, conn_pool=db_conn_pool))]

                log(f"dispatcher starting all tasks", "INFO")
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except Exception as e:
        return

asyncio.run(init())



