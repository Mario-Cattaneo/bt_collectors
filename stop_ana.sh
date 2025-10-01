#!/bin/bash

echo "Stopping analytics"

pkill -f "from analytics import analytics"

echo "killed analytics"