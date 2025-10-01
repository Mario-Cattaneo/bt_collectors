#!/bin/bash
# Ensure script stops on errors
set -e

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