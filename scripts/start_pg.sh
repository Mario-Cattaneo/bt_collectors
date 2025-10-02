#!/bin/bash
set -e

# -----------------------------
# Config
# -----------------------------
PG_BIN="/usr/lib/postgresql/15/bin"   # Postgres binaries
BASE_DIR="$(pwd)/.."                  # one level up from scripts/
DATA_DIR="$BASE_DIR/.pgdata"          # cluster data directory
SOCKET_DIR="$BASE_DIR/.pgsocket"      # unix socket directory
LOGFILE="$DATA_DIR/logfile"
DB_NAME="data"
CLIENT_ROLE="client"
CLIENT_PASSWORD="clientpass"

# -----------------------------
# Ensure directories exist
# -----------------------------
mkdir -p "$DATA_DIR" "$SOCKET_DIR"
chmod 700 "$DATA_DIR"
chmod 777 "$SOCKET_DIR"

# -----------------------------
# Initialize cluster if missing
# -----------------------------
if [ ! -f "$DATA_DIR/PG_VERSION" ]; then
    echo "Initializing new database cluster..."
    "$PG_BIN/initdb" -D "$DATA_DIR"
fi

# -----------------------------
# Stop existing server if running
# -----------------------------
if [ -f "$DATA_DIR/postmaster.pid" ]; then
    echo "Stopping existing server..."
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m fast || true
fi

# -----------------------------
# Start server (Unix socket IPC only)
# -----------------------------
echo "Starting PostgreSQL server..."
"$PG_BIN/pg_ctl" -D "$DATA_DIR" -o "-k $SOCKET_DIR" -l "$LOGFILE" start

# Wait until server is ready
echo "Waiting for server to accept connections..."
until "$PG_BIN/pg_isready" -h "$SOCKET_DIR" -d postgres -U "$(whoami)" > /dev/null 2>&1; do
    sleep 1
done
echo "✅ PostgreSQL running via Unix socket at $SOCKET_DIR"

# -----------------------------
# Create client role if missing
# -----------------------------
"$PG_BIN/psql" -h "$SOCKET_DIR" -U "$(whoami)" -d postgres -tc \
"SELECT 1 FROM pg_roles WHERE rolname='$CLIENT_ROLE';" | grep -q 1 || \
"$PG_BIN/psql" -h "$SOCKET_DIR" -U "$(whoami)" -d postgres -c \
"CREATE ROLE $CLIENT_ROLE LOGIN PASSWORD '$CLIENT_PASSWORD';"

# -----------------------------
# Drop existing database if it exists
# -----------------------------
"$PG_BIN/psql" -h "$SOCKET_DIR" -U "$(whoami)" -d postgres -tc \
"SELECT 1 FROM pg_database WHERE datname='$DB_NAME';" | grep -q 1 && \
"$PG_BIN/dropdb" -h "$SOCKET_DIR" -U "$(whoami)" "$DB_NAME"

# -----------------------------
# Create database owned by client
# -----------------------------
"$PG_BIN/createdb" -h "$SOCKET_DIR" -U "$(whoami)" -O "$CLIENT_ROLE" "$DB_NAME"

# -----------------------------
# Temporary test table
# -----------------------------
"$PG_BIN/psql" -h "$SOCKET_DIR" -U "$CLIENT_ROLE" -d "$DB_NAME" -c \
"CREATE TABLE records_test(id SERIAL PRIMARY KEY, value TEXT);"

# Drop it immediately (just a test)
"$PG_BIN/psql" -h "$SOCKET_DIR" -U "$CLIENT_ROLE" -d "$DB_NAME" -c \
"DROP TABLE records_test;"

echo "✅ PostgreSQL IPC server setup complete"
echo "Socket directory: $SOCKET_DIR"
echo "Database: $DB_NAME"
echo "User: $CLIENT_ROLE / $CLIENT_PASSWORD"
echo "Log: $LOGFILE"
