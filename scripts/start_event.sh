#!/bin/bash
set -e

echo "Starting event_collector..."

# Run Python inline with a heredoc
nohup python3 - <<'PYTHON' > ../logs/events.log 2>&1 &
import sys
import pathlib
import asyncio

# Add parent directory so Python can find event_collector.py
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

from event_collector import event_collector

collector = event_collector()
asyncio.run(collector.start())
PYTHON

MARKETS_PID=$!
echo "event_collector PID: $MARKETS_PID"
