#!/bin/bash
# Full demo reset: stops everything, cleans up, removes model cache, rebuilds
#
# For demonstration purposes only.
#
# Usage: ./setup-demo.sh [stack-name]
#   stack-name: Name of the stack to setup (default: core-banking-simulator)
#
# Examples:
#   ./setup-demo.sh                        # Uses core-banking-simulator
#   ./setup-demo.sh core-banking-fraud-audit

set -e

# Stack name parameter (default to core-banking-simulator)
STACK_NAME="${1:-core-banking-simulator}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "=== Demo Setup for '$STACK_NAME' ==="
echo ""

echo "[1/5] Stopping all containers..."
make stop-all

echo ""
echo "[2/5] Cleaning up volumes and networks..."
make clean

echo ""
echo "[3/5] Removing cached model files..."
MODELS_DIR="$PROJECT_ROOT/stacks/$STACK_NAME/models"
if [ -d "$MODELS_DIR" ]; then
    rm -f "$MODELS_DIR"/*.pkl
    echo "  Removed .pkl files from $MODELS_DIR"
else
    echo "  No models directory found"
fi

echo ""
echo "[4/5] Running setup..."
make setup

echo ""
echo "[5/5] Starting agent..."
make agent

echo ""
echo "=== Demo ready at http://127.0.0.1:4000 ==="
