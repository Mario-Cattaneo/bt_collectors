import asyncio
import aiohttp
from aiohttp import web
import sqlite3
import json
import os
from datetime import datetime, timezone
import time
import shutil


class analytics:
    def __init__(self, data_dir="data", verbosity="DEBUG"):
        # data sql resources
        self.__data_dir = data_dir
        self.__markets_db = None
        self.__last_market_row = 0

        self.__attributes_db = None
        self.__last_attribute_row = 0

        self.__events_db = None
        self.__last_row_for_event = {}

        self.__rpc_db = None
        self.__last_rpc_row = 0


        self.__general_stats = {
            "markets",
            "tokens",
            "negrisks",
            "events",
            "price_changes",

        }



        # attribute type cache
        self.__attribute_types = {
            ""
        }

        # logging
        self.__verbosity = verbosity.upper()

        # aiohttp app/server
        self.__app = web.Application()
        self.__runner = None
        self.__site = None

    async def start(self, host="127.0.0.1", port=8080):
        # open dbs
        self.__markets_db = sqlite3.connect(
            os.path.join(self.__data_dir, "markets/markets.db")
        )
        self.__attributes_db = sqlite3.connect(
            os.path.join(self.__data_dir, "markets/attributes.db")
        )
        # TODO: events.db, rpc.db similarly

        # set up routes
        self.__app.router.add_get("/", self.__handle_get_index)
        self.__app.router.add_get("/index", self.__handle_get_index)
        self.__app.router.add_get("/css", self.__handle_get_css)
        self.__app.router.add_get("/script", self.__handle_get_script)
        self.__app.router.add_get("/attributes", self.__handle_get_attributes)
        self.__app.router.add_post("/query", self.__handle_post_query)

        self.__runner = web.AppRunner(self.__app)
        await self.__runner.setup()
        self.__site = web.TCPSite(self.__runner, host, port)
        await self.__site.start()
        self.__log(f"analytics server started on http://{host}:{port}", "INFO")

    async def stop(self):
        await self.__clean_up()

    async def __clean_up(self):
        self.__log("analytics cleanup started", "DEBUG")
        if self.__runner:
            await self.__runner.cleanup()
        if self.__markets_db:
            self.__markets_db.close()
        if self.__attributes_db:
            self.__attributes_db.close()
        if self.__events_db:
            self.__events_db.close()
        if self.__rpc_db:
            self.__rpc_db.close()
        self.__log("analytics cleanup finished", "INFO")

    def __log(self, msg, level="INFO"):
    """Enhanced logger with ISO timestamp."""
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if levels.index(level) >= levels.index(self.__verbosity):
            now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            print(f"[{now_iso}] [{level}] {msg}")


    # ---- HTTP handlers ----

    async def __handle_get_index(self, request):
        return web.FileResponse("./webpage/index.html")

    async def __handle_get_css(self, request):
        return web.FileResponse("./webpage/styles.css")

    async def __handle_get_script(self, request):
        return web.FileResponse("./webpage/main.js")

    async def __handle_get_attributes(self, request):
        offset = int(request.query.get("offset", 0))
        cursor = self.__attributes_db.cursor()
        cursor.execute("SELECT * FROM attributes WHERE row_index > ?", (offset,))
        rows = cursor.fetchall()

        # convert to list of dicts
        col_names = [desc[0] for desc in cursor.description]
        response = [dict(zip(col_names, row)) for row in rows]
        return web.json_response(response)

    async def __handle_post_query(self, request):
        try:
            data = await request.json()
        except Exception as e:
            return web.json_response({"error": f"invalid JSON: {e}"}, status=400)

        self.__log(f"analytics query: {json.dumps(data)}", "DEBUG")

        # TODO: actually run query against DB
        query = data.get("query", None)
        if query:
            self.__handle_query(query)
        stats = data.get("stats", None)
        if stats:
            self.__handle_stats(stats)
        
        # send back /data/response.csv
        return web.json_response({"status": "ok", "data": []})

        def __handle_query(query):
            tokens = []
            filter = query.get("filter", None)
            if filter:
                sql_markets_filter  = filter.get("markets", None)

                tokens = sql_b_events_filter = filter.get("book", None)
                sql_pc_events_filter = filter.get("price_change", None)
                sql_ltp_events_filter = filter.get("last_trade_price", None)
                sql_tsc_events_filter = filter.get("tick_size_change", None)
                book_filter = fitler.get("book", None)

            order = query.get("order", None)
            if ordered:
                order_by_date = order.get("order_by_date", None)
                order_by_numbers = order.get("order_by_")


                
            
