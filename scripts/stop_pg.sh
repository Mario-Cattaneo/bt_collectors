#!/bin/bash
set -e

PG_BIN="/usr/lib/postgresql/15/bin"
DATA_DIR="$(pwd)/../.pgdata"
SOCKET_DIR="$(pwd)/../.pgsocket"

# -----------------------------
# Stop server if running
# -----------------------------
if [ -f "$DATA_DIR/postmaster.pid" ]; then
    echo "Stopping PostgreSQL server..."
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m fast
    echo "Server stopped."
else
    echo "No running PostgreSQL server found."
fi

# -----------------------------
# Remove data and socket directories
# -----------------------------
echo "Removing data and socket directories..."
rm -rf "$DATA_DIR" "$SOCKET_DIR"

echo "✅ Cleanup complete."
