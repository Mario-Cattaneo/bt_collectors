import asyncio
import asyncpg
import pathlib

async def drop_all_tables():
    socket_dir = str(pathlib.Path("../.pgsocket").resolve())
    conn = await asyncpg.connect(
        user="client",
        password="clientpass",
        database="data",
        host=socket_dir,
        port=5432
    )

    tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    for table in tables:
        await conn.execute(f"DROP TABLE IF EXISTS {table['tablename']} CASCADE")
        print(f"Dropped table {table['tablename']}")

    await conn.close()

asyncio.run(drop_all_tables())
