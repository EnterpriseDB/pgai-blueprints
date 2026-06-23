#!/bin/bash
# ============================================================================
# BFSI GenAI Fraud Audit (Usecase 5) — Start Service orchestrator
# ============================================================================
# For demonstration purposes only.
#
# Wires up the GenAI layer on top of OLTP + OLAP + ML:
#   1. Audit / agent-trace / chat tables in pgd
#   2. AIDB semantic search (vector embeddings of fraud rule docs from MinIO)
#      + alignment / drift / misaligned views + assess_new_rule function
#   3. LangFlow configured with local Ollama
#   4. Fraud Audit agent flows loaded into LangFlow
#
# Prerequisites: Usecase 1 (OLTP) + Usecase 3 (ML) Start Service complete.
# Without ML, the drift/misaligned views return zero rows (the queries depend
# on ml_fraud_predictions joined with fraud_labels).
#
# Stages (printed inline):
#   [0/5] Verify OLTP + ML foundation
#   [1/5] Setup audit schema (3 tables + v_fraud_audit_summary)
#   [2/5] Setup AIDB semantic search + alignment/drift views + assess_new_rule
#   [3/5] Configure LangFlow with local Ollama
#   [4/5] Load LangFlow Fraud Audit agent flows
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
LANGFLOW_URL="http://127.0.0.1:7861"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }
warn() { echo "  ⚠ $*"; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | head -1 | tr -d ' '
}

# ----------------------------------------------------------------------------
# [0/5] Verify Usecase 1 + Usecase 3 foundation
# ----------------------------------------------------------------------------
step "[0/5] Verify OLTP + ML foundation"
if ! docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null | grep -q healthy; then
  fail "$PG_CONTAINER not healthy. Run Usecase 1 (OLTP) Start Service first."
fi
TX_COUNT=$(pgq "SELECT COUNT(*) FROM transactions")
TX_COUNT=${TX_COUNT:-0}
if [ "$TX_COUNT" = "0" ]; then
  fail "transactions empty. Run Usecase 1 (OLTP) Start Service first."
fi
ok "OLTP foundation: $TX_COUNT transactions"

if [ "$(pgq "SELECT to_regclass('public.ml_fraud_predictions') IS NOT NULL")" != "t" ]; then
  fail "ml_fraud_predictions missing — re-run Usecase 1 (OLTP) Start Service."
fi
PRED_COUNT=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions")
PRED_COUNT=${PRED_COUNT:-0}
if [ "$PRED_COUNT" = "0" ]; then
  warn "No ML predictions yet — drift/misaligned views will return zero rows."
  echo "    Run Usecase 3 (ML) Start Service for full audit signal."
else
  ok "ML predictions present: $PRED_COUNT rows"
fi

if ! docker inspect -f '{{.State.Health.Status}}' bfsi-langflow 2>/dev/null | grep -q healthy; then
  fail "bfsi-langflow not healthy. Foundation profile must be up (re-deploy)."
fi
ok "LangFlow healthy"

# ----------------------------------------------------------------------------
# [1/5] Audit schema
# ----------------------------------------------------------------------------
step "[1/5] Setup audit schema (audit_results, agent_trace, chat_sessions, v_fraud_audit_summary)"
docker exec -i "$PG_CONTAINER" bash /scripts/setup/genai/setup-audit-schema.sh
ok "Audit tables + summary view created"

# ----------------------------------------------------------------------------
# [2/5] AIDB semantic search + alignment/drift views + assess_new_rule
# ----------------------------------------------------------------------------
step "[2/5] Setup AIDB semantic search + alignment/drift views"
bash "$STACK_DIR/scripts/setup/genai/setup-aidb-semantic.sh"
ok "AIDB embeddings + 4 views (v_ml_rules_alignment, v_drift_summary, v_misaligned_transactions, v_drift_trend) + assess_new_rule()"

# ----------------------------------------------------------------------------
# [3/5] Configure LangFlow with Ollama
# ----------------------------------------------------------------------------
step "[3/5] Configure LangFlow with Ollama"
cd "$STACK_DIR"
docker compose run --rm \
  -v "$(pwd)/scripts/setup/genai/setup-langflow-ollama.py:/app/setup.py:ro" \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -w /app langflow-init python setup.py 2>&1 | tail -10 \
  || warn "LangFlow Ollama setup returned non-zero — proceeding anyway"
ok "LangFlow Ollama integration configured"

# ----------------------------------------------------------------------------
# [4/5] Load Fraud Audit agent flows
# ----------------------------------------------------------------------------
step "[4/5] Load LangFlow Fraud Audit agent flows"
cd "$STACK_DIR"
docker compose run --rm \
  -v "$(pwd)/scripts/setup/genai/load-langflow-flows.py:/app/load-langflow-flows.py:ro" \
  -v "$(pwd)/langflow-templates:/templates:ro" \
  -e TEMPLATES_DIR=/templates \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=gpt-oss:20b \
  -w /app langflow-init python load-langflow-flows.py 2>&1 | tail -15 \
  || warn "Flow load returned non-zero — open LangFlow UI to verify"
ok "Fraud Audit flows loaded (ML Algorithm Audit Agent + Fraud Agent with PG MCP)"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ GenAI Fraud Audit Service Ready ━━━"
echo "  Audit tables:    audit_results, agent_trace, chat_sessions"
echo "  Audit views:     v_fraud_audit_summary, v_ml_rules_alignment,"
echo "                   v_drift_summary, v_misaligned_transactions, v_drift_trend"
echo "  Functions:       search_fraud_rules_semantic(text,int), assess_new_rule(...)"
echo ""
echo "  LangFlow:        $LANGFLOW_URL"
echo "  Agents:          ML Algorithm Audit Agent | Fraud Agent with PG MCP"
echo ""
echo "  ☞ For Bedrock-backed agents, run Usecase 6 (AI Governance) Start Service"
echo "    to inject Anthropic credentials into LangFlow."
