#!/bin/bash
set -e

echo "Starting market_collector..."

# Run Python inline with a heredoc
nohup python3 - <<'PYTHON' > ../logs/markets.log 2>&1 &
import sys
import pathlib
import asyncio

# Add parent directory so Python can find market_collector.py
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

from market_collector import market_collector

collector = market_collector()
asyncio.run(collector.start())
PYTHON

MARKETS_PID=$!
echo "market_collector PID: $MARKETS_PID"
