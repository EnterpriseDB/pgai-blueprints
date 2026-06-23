#!/bin/bash
# For demonstration purposes only.
# Start the Core Banking Fraud Audit stack with AWS credentials

set -e

PIDFILE=/tmp/start-stack.pid
LOGFILE=/tmp/start-stack.log

PID=$$
if [[ -f $PIDFILE ]]; then
    CPID=$(cat $PIDFILE)
    if ps p $CPID 2>/dev/null; then
	echo "INFO  [$(date) $PID] found concurrent start-stack.sh script [PID=$CPID], exiting" | tee -a $LOGFILE
	exit 0
    else
	echo "INFO  [$(date) $PID] updating orphaned pidfile" | tee -a $LOGFILE
	echo "$PID" > $PIDFILE
    fi
else
    echo "INFO  [$(date) $PID] writing pidfile" | tee -a $LOGFILE
    echo "$PID" > $PIDFILE
fi

echo "BEGIN [$(date) $PID] start-stack.sh" | tee -a $LOGFILE

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STACK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Core Banking Fraud Audit Stack ==="

# Verify credentials
if [ -n "$ANTHROPIC_API_KEY" ]; then
    echo "ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY:0:12}..."
else
    echo "WARNING: Anthropic credentials not set"
fi

# Start the stack
cd "$STACK_DIR"
echo ""
echo "Starting stack..."
docker compose "$@" 2>&1 | tee -a /tmp/start-stack.log

echo "END   [$(date) $PID] start-stack.sh" | tee -a /tmp/start-stack.log
rm -f $PIDFILE
