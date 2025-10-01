#!/bin/bash

echo "Stopping collectors..."

# Kill by module name
pkill -f "from market_collector import market_collector"
pkill -f "from event_collector import event_collector"
pkill -f "from analytics import analytics"

echo "All collectors stopped."
