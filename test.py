import asyncio
from analytics import analytics

async def main():
    # Initialize analytics instance
    data_dir = "data"        # adjust if your DBs are in a different folder
    market_version = 1
    rpc_version = 1
    sleep_time = 5           # seconds between checks
    verbosity = "DEBUG"

    a = analytics(
        data_dir=data_dir,
        verbosity=verbosity,
        sleep=sleep_time,
        market_version=market_version,
        rpc_version=rpc_version
    )

    try:
        # Start analytics
        print("[INFO] Starting analytics...")
        await a.start()
    except asyncio.CancelledError:
        print("[INFO] Analytics cancelled.")
    finally:
        # Stop analytics and clean up resources
        print("[INFO] Stopping analytics...")
        await a.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Keyboard interrupt received. Exiting.")
