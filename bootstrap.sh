#!/usr/bin/env bash
set -e

echo "==================================================="
echo "  EDB Postgres® AI Blueprints v0.1rc7 - Bootstrap"
echo "  Docker Compose Runtime"
echo "==================================================="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[1/4] Checking Docker..."
if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "ERROR: Docker daemon not running. Start Docker Desktop and try again."
    exit 1
fi
echo "  Docker $(docker --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"

echo "[2/4] Checking Docker Compose..."
if ! docker compose version &>/dev/null 2>&1; then
    echo "ERROR: Docker Compose (v2) not found. Update Docker Desktop."
    exit 1
fi
echo "  $(docker compose version)"

echo "[3/4] Checking port availability..."
AGENT_PORT=4000
BLOCKED=""
for port in $AGENT_PORT 4566 5050 5432 5433 5435 5436 5691 8080 8081 8085 8123 9000 9001 9301 9400; do
    if lsof -ti :$port &>/dev/null 2>&1; then
        pid=$(lsof -ti :$port 2>/dev/null | head -1)
        proc=$(ps -p $pid -o comm= 2>/dev/null || echo "unknown")
        echo "  WARNING: Port $port in use by $proc (PID: $pid)"
        BLOCKED="$BLOCKED $port"
    fi
done
if [ -n "$BLOCKED" ]; then
    echo ""
    echo "  Ports in use:$BLOCKED"
    echo "  Run 'make clean' to stop leftover containers, or free these ports manually."
    echo "  Continuing setup (ports will be needed at deploy time)..."
else
    echo "  All framework ports available"
fi

echo "[4/4] Installing Python dependencies..."
PIP_CMD=""
if pip3 --version &>/dev/null 2>&1; then
    PIP_CMD="pip3"
elif pip --version &>/dev/null 2>&1; then
    PIP_CMD="pip"
elif py -m pip --version &>/dev/null 2>&1; then
    PIP_CMD="py -m pip"
elif python -m pip --version &>/dev/null 2>&1; then
    PIP_CMD="python -m pip"
elif python3 -m pip --version &>/dev/null 2>&1; then
    PIP_CMD="python3 -m pip"
else
    echo "ERROR: Python/pip not found. Install Python: https://www.python.org/downloads/"
    exit 1
fi
$PIP_CMD install -r "$SCRIPT_DIR/engine/agent/requirements.txt" --quiet --break-system-packages 2>/dev/null || \
$PIP_CMD install -r "$SCRIPT_DIR/engine/agent/requirements.txt" --quiet
echo "  Python deps installed"

echo ""
echo "============================================"
echo "  Setup complete"
echo ""
echo "  Next: cp .env.example .env  (add API key)"
echo "  Then: make agent"
echo "============================================"
