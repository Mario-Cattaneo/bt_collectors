#!/bin/bash
# Ensure script stops on errors
set -e

# Start market_collector in background and log to markets.log
echo "Starting market_collector..."
nohup python3 - <<'PYTHON' > markets.log 2>&1 &
import asyncio
from market_collector import market_collector

collector = market_collector()
asyncio.run(collector.start())
PYTHON
MARKETS_PID=$!
echo "market_collector PID: $MARKETS_PID"

sleep 2

# Start event_collector in background and log to events.log
echo "Starting event_collector..."
nohup python3 - <<'PYTHON' > events.log 2>&1 &
import asyncio
from event_collector import event_collector

collector = event_collector()
asyncio.run(collector.start())
PYTHON
EVENTS_PID=$!
echo "event_collector PID: $EVENTS_PID"

sleep 2

# Start analytics in background and log to analytics.log
echo "Starting analytics..."
nohup python3 - <<'PYTHON' > analytics.log 2>&1 &
import asyncio
from analytics import analytics

collector = analytics()
asyncio.run(collector.start())
PYTHON
ANALYTICS_PID=$!
echo "analytics PID: $ANALYTICS_PID"

echo "All collectors started successfully."
