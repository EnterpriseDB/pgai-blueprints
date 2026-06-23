#!/bin/bash
#
# For demonstration purposes only.
#
# ============================================================================
# BFSI GenAI Fraud Audit (Usecase 5) — Check Status
# ============================================================================
# Diagnostic snapshot of the GenAI audit layer:
#   • Audit tables (row counts: audit_results, agent_trace, chat_sessions)
#   • Fraud rules (count + per-region breakdown)
#   • Audit summary view (last 10 hours)
#   • Drift summary (ML vs Rules alignment)
#   • Misaligned transactions sample
#   • Semantic search smoke test
#   • LangFlow flows registered
# ============================================================================

set +e

PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
LANGFLOW_URL="http://localhost:7861"

section() { echo ""; echo "━━━ $* ━━━"; }
ok()      { echo "  ✓ $*"; }
warn()    { echo "  ⚠ $*"; }
miss()    { echo "  ✗ $*"; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc "$1" 2>/dev/null \
    | head -1 | tr -d ' '
}

echo "=== GenAI Fraud Audit Status ==="

# ----------------------------------------------------------------------------
# Audit tables
# ----------------------------------------------------------------------------
section "Audit Tables"
for t in audit_results agent_trace chat_sessions; do
  if [ "$(pgq "SELECT to_regclass('public.$t') IS NOT NULL")" != "t" ]; then
    miss "$t: missing — run Start Service"
  else
    n=$(pgq "SELECT COUNT(*) FROM $t")
    ok "$t: ${n:-0} rows"
  fi
done

# ----------------------------------------------------------------------------
# Fraud rules
# ----------------------------------------------------------------------------
section "Fraud Rules (active)"
RULES=$(pgq "SELECT COUNT(*) FROM fraud_rules WHERE is_active=TRUE")
echo "  Total active: ${RULES:-0}"
if [ "${RULES:-0}" -gt 0 ]; then
  echo "  By region:"
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc \
    "SELECT region, COUNT(*) FROM fraud_rules WHERE is_active=TRUE GROUP BY region ORDER BY region" 2>/dev/null \
    | awk -F'|' 'NF>=2 {printf "    %-12s %s rules\n", $1, $2}'
fi

# ----------------------------------------------------------------------------
# Audit summary (last 10 hours)
# ----------------------------------------------------------------------------
section "Fraud Audit Summary (last 10 hours)"
if [ "$(pgq "SELECT to_regclass('public.v_fraud_audit_summary') IS NOT NULL")" != "t" ]; then
  miss "v_fraud_audit_summary missing — run Start Service"
else
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off \
    -c "SELECT * FROM v_fraud_audit_summary ORDER BY hour DESC LIMIT 10;" 2>/dev/null \
    | head -16 || warn "Could not query view"
fi

# ----------------------------------------------------------------------------
# Drift summary (ML vs Rules)
# ----------------------------------------------------------------------------
section "ML vs Rules Drift"
if [ "$(pgq "SELECT to_regclass('public.v_drift_summary') IS NOT NULL")" != "t" ]; then
  miss "v_drift_summary missing — run Start Service"
else
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off \
    -c "SELECT * FROM v_drift_summary;" 2>/dev/null \
    | head -10 || warn "Could not query view"
fi

# ----------------------------------------------------------------------------
# Misaligned transactions (sample)
# ----------------------------------------------------------------------------
section "Misaligned Transactions (sample)"
if [ "$(pgq "SELECT to_regclass('public.v_misaligned_transactions') IS NOT NULL")" != "t" ]; then
  miss "v_misaligned_transactions missing — run Start Service"
else
  COUNT=$(pgq "SELECT COUNT(*) FROM v_misaligned_transactions")
  echo "  Total misaligned: ${COUNT:-0}"
  if [ "${COUNT:-0}" -gt 0 ]; then
    echo "  Top 5:"
    docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
      psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off \
      -c "SELECT tx_id, amount, region, vendor, fraud_probability, alignment_status FROM v_misaligned_transactions LIMIT 5;" 2>/dev/null \
      | head -10
  fi
fi

# ----------------------------------------------------------------------------
# Semantic search smoke test
# ----------------------------------------------------------------------------
section "Semantic Search Smoke Test"
if [ "$(pgq "SELECT to_regprocedure('public.search_fraud_rules_semantic(text,integer)') IS NOT NULL")" != "t" ]; then
  miss "search_fraud_rules_semantic() missing — run Start Service"
else
  echo "  Query: 'US Stripe high value transaction' (top 3)"
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off \
    -c "SELECT source_doc, ROUND(similarity::numeric, 3) AS sim, LEFT(chunk_text, 60) AS preview FROM search_fraud_rules_semantic('US Stripe high value transaction', 3);" 2>/dev/null \
    | head -10 || warn "Semantic search returned no results — embeddings may not have loaded"
fi

# ----------------------------------------------------------------------------
# LangFlow flows
# ----------------------------------------------------------------------------
section "LangFlow Agents"
if curl -sf -m 3 "$LANGFLOW_URL/health" >/dev/null 2>&1; then
  ok "LangFlow reachable at $LANGFLOW_URL"
  AUTH=$(curl -sf -m 3 "$LANGFLOW_URL/api/v1/auto_login" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
  if [ -n "$AUTH" ]; then
    FLOW_NAMES=$(curl -sf -m 5 --compressed -H "Authorization: Bearer $AUTH" "$LANGFLOW_URL/api/v1/flows/" 2>/dev/null \
      | python3 -c "import sys,json;d=json.load(sys.stdin);print('\n'.join('    - '+f.get('name','?') for f in d if isinstance(f,dict)))" 2>/dev/null)
    if [ -n "$FLOW_NAMES" ]; then
      echo "  Loaded flows:"
      echo "$FLOW_NAMES"
    else
      warn "No flows registered — run Start Service to load Fraud Audit agents"
    fi
  else
    warn "Could not authenticate to LangFlow API"
  fi
else
  miss "LangFlow not reachable at $LANGFLOW_URL"
fi

echo ""
echo "=== GenAI Check Complete ==="
echo "  ☞ Open LangFlow UI: $LANGFLOW_URL"
echo "  ☞ For Anthropic, run Usecase 6 (AI Governance) Start Service"
