# run_collector.py
import asyncio
from rpc_collector import rpc_collector

async def main():
    collector = rpc_collector(data_dir="data", verbosity="DEBUG")
    
    # Start the collector
    started = await collector.start()
    if started:
        print("Collector started successfully.")
    else:
        print("Collector failed to start.")

if __name__ == "__main__":
    asyncio.run(main())
