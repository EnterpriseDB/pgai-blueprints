#!/bin/bash
# Windows-specific helper script to start the agent in background
# This script handles paths with spaces and SSL certs for corporate proxies
#
# For demonstration purposes only.

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/engine/agent/logs"
LOG_FILE="$LOG_DIR/agent.log"
PID_FILE="$LOG_DIR/agent.pid"

mkdir -p "$LOG_DIR"

# Source .env if it exists (single source of truth for credentials + SSL flags)
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    . "$SCRIPT_DIR/.env"
    set +a
fi

# Start the agent in background
cd "$SCRIPT_DIR/engine/agent"
python app.py >> "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"

# Detach the process so it survives shell exit
disown $PID 2>/dev/null || true

echo $PID
