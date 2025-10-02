import asyncio
import websockets
import asyncpg
import json
import pathlib
from datetime import datetime, timezone


class event_collector:
    def __init__(self, data_dir="data", verbosity="DEBUG", reset=True):
        # DB
        self.__db_pool = None
        self.__token_ids = []
        self.__last_market_row = 0
        self.__reset = reset

        # Logging
        self.__verbosity = verbosity.upper()
        self.__resubscription_count = 0

        # Events endpoint
        self.__events_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.__events_cli = None
        self.__max_subscriptions = 77777

        # Liveness
        self.__running = False

        # Version
        self.__version = 1

        # Async tasks
        self.__market_task = None
        self.__ws_task = None

    async def start(self) -> bool:
        if self.__running:
            self.__log("event_collector already started", "ERROR")
            return False

        try:
            socket_dir = str(pathlib.Path("../.pgsocket").resolve())
            self.__db_pool = await asyncpg.create_pool(
                user="client",
                password="clientpass",
                database="data",
                host=socket_dir,
                port=5432,
                min_size=1,
                max_size=10,
            )

            if self.__reset:
                async with self.__db_pool.acquire() as conn:
                    await conn.execute("""
                        DROP TABLE IF EXISTS changes;
                        DROP TABLE IF EXISTS books;
                        DROP TABLE IF EXISTS tick_changes;
                    """)

            async with self.__db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS changes (
                        row_index SERIAL PRIMARY KEY,
                        collector_version INTEGER,
                        insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                        market VARCHAR(100),
                        token_id VARCHAR(100),
                        event_type VARCHAR(17),
                        fee_rate_bps REAL,
                        price REAL,
                        size REAL,
                        side VARCHAR(5),
                        best_bid REAL,
                        best_ask REAL,
                        server_time BIGINT
                    );
                    CREATE TABLE IF NOT EXISTS books (
                        row_index SERIAL PRIMARY KEY,
                        collector_version INTEGER,
                        insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                        market VARCHAR(100),
                        token_id VARCHAR(100),
                        bids TEXT,
                        asks TEXT,
                        server_time BIGINT
                    );
                    CREATE TABLE IF NOT EXISTS tick_changes (
                        row_index SERIAL PRIMARY KEY,
                        collector_version INTEGER,
                        insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                        market VARCHAR(100),
                        token_id VARCHAR(100),
                        old_tick_size REAL,
                        new_tick_size REAL,
                        server_time BIGINT
                    );
                """)

        except Exception as e:
            self.__log(f"event_collector failed to start: {e}", "ERROR")
            return False

        self.__running = True
        self.__log("event_collector started", "INFO")

        # Start concurrent tasks
        self.__market_task = asyncio.create_task(self.__market_loop())
        self.__ws_task = asyncio.create_task(self.__ws_loop())

        await asyncio.gather(self.__market_task, self.__ws_task)
        return True

    async def stop(self) -> bool:
        self.__running = False
        if self.__market_task:
            self.__market_task.cancel()
        if self.__ws_task:
            self.__ws_task.cancel()
        await self.__clean_up()
        self.__log("event_collector stopped", "DEBUG")
        return True

    async def __clean_up(self):
        self.__log("event_collector cleanup started", "DEBUG")
        if self.__events_cli:
            try:
                await self.__events_cli.close()
                self.__log("event_collector closed events websocket client", "DEBUG")
            except Exception as e:
                self.__log(f"event_collector closing websocket client: {e}", "ERROR")
            self.__events_cli = None

        if self.__db_pool:
            await self.__db_pool.close()
            self.__db_pool = None
            self.__log("event_collector closed DB pool", "DEBUG")

        self.__running = False
        self.__log("event_collector cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= 1:
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            print(f"[{now_iso}] [{level}] {msg}", flush=True)

    # ------------------------------
    # Market discovery loop
    # ------------------------------
    async def __market_loop(self):
        while self.__running:
            try:
                await self.__query_markets()
            except Exception as e:
                self.__log(f"market loop error: {e}", "ERROR")
            await asyncio.sleep(1)

    async def __query_markets(self) -> bool:
        try:
            async with self.__db_pool.acquire() as conn:
                new_token_pairs = await conn.fetch(
                    "SELECT token_id1, token_id2 FROM markets WHERE row_index > $1",
                    self.__last_market_row
                )

            new_token_count = len(new_token_pairs)
            self.__last_market_row += new_token_count

            for tok1, tok2 in new_token_pairs:
                self.__token_ids.extend([tok1, tok2])

            if new_token_count > 0:
                self.__log(f"event_collector found {new_token_count} new token pairs from markets db, now subscribing to {min(len(self.__token_ids), self.__max_subscriptions)}", "INFO")
                # Trigger resubscribe immediately
                if self.__events_cli is None or self.__events_cli.closed:
                    return True
                await self.__resubscribe()
            return True

        except Exception as e:
            self.__log(f"event_collector failed to query markets: {e}", "ERROR")
            return False

    # ------------------------------
    # WebSocket read / reconnect loop
    # ------------------------------
    async def __ws_loop(self):
        while self.__running:
            if not self.__events_cli or self.__events_cli.closed:
                success = await self.__resubscribe()
                if not success:
                    self.__log("failed to resubscribe, retrying in 5s", "ERROR")
                    await asyncio.sleep(5)
                    continue
            try:
                await self.__read_events()
            except Exception as e:
                self.__log(f"ws loop error: {e}", "ERROR")
                await asyncio.sleep(1)

    async def __resubscribe(self) -> bool:
        try:
            self.__log(f"market_collector resubscribing with events_cli {type(self.__events_cli)} and {len(self.__token_ids)} tokens)", "INFO")
            new_cli = await websockets.connect(
                self.__events_url,
                ping_interval=20,
                ping_timeout=5
            )
            subscription = {"type": "market", "initial_dump": True, "assets_ids": self.__token_ids[:self.__max_subscriptions]}
            await new_cli.send(json.dumps(subscription))

            if self.__events_cli:
                await self.__events_cli.close()

            self.__events_cli = new_cli
            self.__resubscription_count += 1
            self.__log(f"market_collector resubscription {self.__resubscription_count} complete", "INFO")
            return True

        except Exception as e:
            self.__log(f"market_collector resubscribe failed: {e}", "ERROR")
            return False

    async def __read_events(self) -> bool:
        try:
            async for message in self.__events_cli:
                self.__log(f"event_collector raw message: {message}", "DEBUG")
                msg_json = json.loads(message)
                if not await self.__insert(msg_json):
                    self.__log("event_collector failed to insert", "ERROR")
                    return False
            return True
        except websockets.exceptions.ConnectionClosed:
            self.__log("WebSocket closed, will attempt reconnect and resubscribe", "WARNING")
            return False

    # ------------------------------
    # Insert into DB
    # ------------------------------
    async def __insert(self, msg_json) -> bool:
        if isinstance(msg_json, dict):
            msg_json = [msg_json]

        if not msg_json:
            self.__log("event_collector received empty message list", "WARNING")
            return True
        if not isinstance(msg_json, list):
            self.__log(f"event_collector invalid msg_json: {type(msg_json)}", "ERROR")
            return False

        for i, obj in enumerate(msg_json):
            if isinstance(obj, str):
                try:
                    msg_json[i] = json.loads(obj)
                except Exception as e:
                    self.__log(f"event_collector failed to decode object: {obj} error: {e}", "ERROR")
                    return False
            if not isinstance(msg_json[i], dict):
                self.__log(f"event_collector invalid type after decode: {type(msg_json[i])}", "ERROR")
                return False

        def cast_int(val):
            try:
                return int(val) if val is not None else None
            except:
                return None

        def cast_float(val):
            try:
                return float(val) if val is not None else None
            except:
                return None

        try:
            async with self.__db_pool.acquire() as conn:
                for obj in msg_json:
                    token = obj.get("asset_id")
                    event_type = obj.get("event_type")
                    market = obj.get("market")
                    collector_version = self.__version
                    ts = cast_int(obj.get("timestamp"))

                    if event_type == "book" and token:
                        await conn.execute(
                            """INSERT INTO books (collector_version, market, token_id, bids, asks, server_time)
                            VALUES ($1,$2,$3,$4,$5,$6)""",
                            collector_version, market, token, json.dumps(obj.get("bids")), json.dumps(obj.get("asks")), ts
                        )

                    elif event_type == "price_change" and token:
                        for change in obj.get("price_changes", []):
                            token_change = change.get("asset_id")
                            await conn.execute(
                                """INSERT INTO changes
                                (collector_version, market, token_id, event_type, price, size, side, best_bid, best_ask, server_time)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                                collector_version, market, token_change, event_type,
                                cast_float(change.get("price")), cast_float(change.get("size")), change.get("side"),
                                cast_float(change.get("best_bid")), cast_float(change.get("best_ask")), ts
                            )

                    elif event_type == "last_trade_price" and token:
                        await conn.execute(
                            """INSERT INTO changes
                            (collector_version, market, token_id, event_type, fee_rate_bps, price, side, size, server_time)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                            collector_version, market, token, event_type,
                            cast_float(obj.get("fee_rate_bps")), cast_float(obj.get("price")), obj.get("side"),
                            cast_float(obj.get("size")), ts
                        )

                    elif event_type == "tick_size_change" and token:
                        await conn.execute(
                            """INSERT INTO tick_changes
                            (collector_version, market, token_id, old_tick_size, new_tick_size, server_time)
                            VALUES ($1,$2,$3,$4,$5,$6)""",
                            collector_version, market, token, cast_float(obj.get("old_tick_size")),
                            cast_float(obj.get("new_tick_size")), ts
                        )

            return True

        except Exception as e:
            self.__log(f"event_collector failed to insert {event_type} data: {e}", "ERROR")
            return False
