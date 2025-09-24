import asyncio
import types

async def main():
    mc = market_collector()
    await mc.start()

if __name__ == "__main__":
    asyncio.run(main())
