#!/bin/bash
# ============================================================================
# BFSI AI Governance (Usecase 6) — Start Service orchestrator
# ============================================================================
# For demonstration purposes only.
#
# Configures ANTHROPIC_API_KEY for LangFlow agents + LLM judges. This
# unlocks Anthropic-powered evaluations (run-quick-eval / run-full-eval) and
# captures rich agent traces in MLflow.
#
# What it does:
#   1. Verify Usecase 5 (GenAI) prerequisites — LangFlow up + flows loaded
#   2. Restart LangFlow with credentials in env + inject as global variables
#   3. Verify credentials propagated into the LangFlow container
#
# Prerequisites:
#   - Usecase 5 (GenAI Fraud Audit) Start Service complete
#
# Stages:
#   [0/2] Verify GenAI foundation
#   [1/2] Restart LangFlow + configure global variables
#   [2/2] Verify credentials in LangFlow container
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
LANGFLOW_URL="${LANGFLOW_URL:-http://127.0.0.1:7861}"
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"
ANTHROPIC_DEFAULT_MODEL="${ANTHROPIC_DEFAULT_MODEL:-claude-sonnet-4-6}"
OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://host.docker.internal:11434}"
PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }
warn() { echo "  ⚠ $*"; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | head -1 | tr -d ' '
}

# ----------------------------------------------------------------------------
# [0/3] Verify Usecase 5 foundation
# ----------------------------------------------------------------------------
step "[0/2] Verify GenAI (Usecase 5) foundation"
if ! docker inspect -f '{{.State.Health.Status}}' bfsi-langflow 2>/dev/null | grep -q healthy; then
  fail "bfsi-langflow not healthy."
fi
ok "LangFlow healthy"

# audit_results created in Usecase 5 Start Service
if [ "$(pgq "SELECT to_regclass('public.audit_results') IS NOT NULL")" != "t" ]; then
  fail "audit_results table missing. Run Usecase 5 (GenAI Fraud Audit) Start Service first."
fi
ok "Audit schema present (Usecase 5 has run)"

# Probe LangFlow API + check that at least one flow is registered
AUTH=$(curl -sf -m 3 "$LANGFLOW_URL/api/v1/auto_login" 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
if [ -z "$AUTH" ]; then
  fail "Could not authenticate to LangFlow API at $LANGFLOW_URL"
fi
FLOW_COUNT=$(curl -sf -m 5 --compressed -H "Authorization: Bearer $AUTH" "$LANGFLOW_URL/api/v1/flows/" 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(len([f for f in d if isinstance(f,dict)]))" 2>/dev/null || echo 0)
if [ "${FLOW_COUNT:-0}" = "0" ]; then
  fail "No LangFlow flows loaded. Run Usecase 5 Start Service first."
fi
ok "LangFlow has $FLOW_COUNT flow(s) registered"


# ----------------------------------------------------------------------------
# [1/2] Restart LangFlow with credentials + inject as global vars
# ----------------------------------------------------------------------------
step "[1/2] Restart LangFlow + configure Anthropic global variables"
cd "$STACK_DIR"
ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
ANTHROPIC_DEFAULT_MODEL="$ANTHROPIC_DEFAULT_MODEL" \
docker compose up -d langflow 2>&1 \
  | grep -vE "^[[:space:]]*(Container|Volume|Network)[[:space:]].*[[:space:]](Creating|Created|Starting|Started|Running|Healthy|Waiting|Exited|Recreated|Recreate)[[:space:]]*$" \
  | grep -vE "^time=.*level=(warning|info)" \
  || true

# Wait for LangFlow to come back healthy after restart
echo "  Waiting for LangFlow to be healthy after restart..."
for ((i=1; i<=30; i++)); do
  if [ "$(docker inspect -f '{{.State.Health.Status}}' bfsi-langflow 2>/dev/null)" = "healthy" ]; then

    ok "LangFlow healthy (took ${i}s)"
    break
  fi
  sleep 5
done

# If the loop finished without breaking, i will be 31
if [ "$i" -gt 30 ]; then
  fail "LangFlow did not become healthy in 30s after restart"
fi

# Inject Anthropic global variables via setup-langflow-anthropic.py
echo "  Configuring Anthropic global variables in LangFlow ..."
docker compose run --rm \
  -v "$(pwd)/scripts/setup/genai/setup-langflow-anthropic.py:/app/setup.py:ro" \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  -e ANTHROPIC_DEFAULT_MODEL="$ANTHROPIC_DEFAULT_MODEL" \
  langflow-init python /app/setup.py 2>&1 | tail -15 \
  || warn "Anthropic global var setup returned non-zero — check above"
ok "LangFlow global variables configured"

# ----------------------------------------------------------------------------
# [2/2] Verify credentials inside the LangFlow container
# ----------------------------------------------------------------------------
step "[2/2] Verify credentials in LangFlow container"
LF_KEY=$(docker exec bfsi-langflow env 2>/dev/null | grep '^ANTHROPIC_API_KEY=' | head -1 | cut -d= -f2)
if [ -z "$LF_KEY" ]; then
  fail "ANTHROPIC_API_KEY not present in bfsi-langflow env"
fi
if [ "${LF_KEY:0:50}" = "${ANTHROPIC_API_KEY:0:50}" ]; then
  ok "Credentials propagated into LangFlow (matches host profile)"
else
  warn "LangFlow has ANTHROPIC_API_KEY but it differs from host — restart may have used stale env"
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ AI Governance Service Ready ━━━"
echo "  LangFlow:          $LANGFLOW_URL  (Anthropic agents enabled)"
echo "  MLflow traces:     http://127.0.0.1:5001/#/experiments/1/traces"
echo ""
echo "  ☞ Run 'Test Agent (Single Question)' to validate end-to-end."
echo "  ☞ Run 'Run Quick Eval' (3 tests, ~1 min) for fast judge-based scoring."
echo "  ☞ Run 'Run Full Eval' (17 tests, ~10 min) for the full evaluation suite."
