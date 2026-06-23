#!/bin/bash
#
# For demonstration purposes only.
#
# ============================================================================
# BFSI AI Governance (Usecase 6) — Check Status
# ============================================================================
# Diagnostic snapshot of the AI Governance layer:
#   • Anthropic credentials inside LangFlow container
#   • LangFlow flows registered (sanity)
#   • Recent agent traces in MLflow (top 10)
#   • audit_results table activity (last hour)
#   • Pointers to evaluation artifacts
# ============================================================================

set +e

PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
LANGFLOW_URL="${LANGFLOW_URL:-http://localhost:7861}"
MLFLOW_URL="${MLFLOW_URL:-http://localhost:5001}"
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"

section() { echo ""; echo "━━━ $* ━━━"; }
ok()      { echo "  ✓ $*"; }
warn()    { echo "  ⚠ $*"; }
miss()    { echo "  ✗ $*"; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc "$1" 2>/dev/null \
    | head -1 | tr -d ' '
}

echo "=== AI Governance Status ==="

# ----------------------------------------------------------------------------
# Anthropic credentials in LangFlow
# ----------------------------------------------------------------------------
section "Anthropic Credentials (in LangFlow container)"
if ! docker inspect -f '{{.State.Status}}' bfsi-langflow >/dev/null 2>&1; then
  miss "bfsi-langflow not deployed"
else
  KEY=$(docker exec bfsi-langflow env 2>/dev/null | grep '^ANTHROPIC_API_KEY=' | head -1 | cut -d= -f2)
  if [ -n "$KEY" ]; then
    ok "ANTHROPIC_API_KEY: ${KEY:0:12}..."
  else
    miss "ANTHROPIC_API_KEY not set in container — run Start Service"
    echo "    (or set ANTHROPIC_API_KEY"
  fi
fi

# ----------------------------------------------------------------------------
# LangFlow flows
# ----------------------------------------------------------------------------
section "LangFlow Flows"
AUTH=$(curl -sf -m 3 "$LANGFLOW_URL/api/v1/auto_login" 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
if [ -z "$AUTH" ]; then
  miss "Could not authenticate to LangFlow API at $LANGFLOW_URL"
else
  curl -sf -m 5 --compressed -H "Authorization: Bearer $AUTH" "$LANGFLOW_URL/api/v1/flows/" 2>/dev/null \
    | python3 -c "
import sys, json
flows = json.load(sys.stdin)
flows = [f for f in flows if isinstance(f, dict)]
if not flows:
    print('  ✗ No flows registered — run Usecase 5 Start Service')
else:
    print(f'  Total flows: {len(flows)}')
    for f in flows[:8]:
        print(f\"    - {f.get('name','?')}\")
" 2>/dev/null || warn "Could not parse flows"
fi

# ----------------------------------------------------------------------------
# MLflow agent traces
# ----------------------------------------------------------------------------
section "Agent Traces (MLflow)"
EXP_IDS=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/experiments/search" \
  -H "Content-Type: application/json" -d '{"max_results":100}' 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(json.dumps([e['experiment_id'] for e in d.get('experiments',[])][:10]))" 2>/dev/null || echo '[]')

if [ "$EXP_IDS" = "[]" ]; then
  warn "No MLflow experiments — agent traces require Usecase 3 + agent runs"
else
  EXP_PARAMS=$(echo "$EXP_IDS" | python3 -c "import sys,json;ids=json.load(sys.stdin);print('&'.join(f'experiment_ids={i}' for i in ids))" 2>/dev/null)
  if [ -n "$EXP_PARAMS" ]; then
    TRACES_JSON=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/traces?${EXP_PARAMS}&max_results=10" 2>/dev/null || echo '{}')
    echo "$TRACES_JSON" | python3 -c "
import sys, json
from datetime import datetime
d = json.load(sys.stdin)
traces = d.get('traces') or []
if not traces:
    print('  (no traces yet — run an agent question to populate)')
    sys.exit(0)
print(f'  Total traces (recent): {len(traces)}')
for t in traces[:10]:
    info = t.get('trace_info') or t
    name = (info.get('tags') or {}).get('mlflow.traceName') if isinstance(info.get('tags'), dict) else '?'
    ts = info.get('timestamp_ms') or info.get('start_time') or 0
    when = datetime.fromtimestamp(int(ts) / 1000).strftime('%m-%d %H:%M:%S') if ts else '?'
    duration = info.get('execution_time_ms') or info.get('duration_ms') or '?'
    status = info.get('status', '?')
    print(f'    {when}  status={status:<10} duration={duration:>6}ms  {(name or \"?\")[:50]}')
" 2>/dev/null || warn "Could not parse traces"
  fi
fi

# ----------------------------------------------------------------------------
# audit_results activity (last hour)
# ----------------------------------------------------------------------------
section "Audit Activity (last hour)"
if [ "$(pgq "SELECT to_regclass('public.audit_results') IS NOT NULL")" != "t" ]; then
  miss "audit_results missing — run Usecase 5 Start Service"
else
  RECENT=$(pgq "SELECT COUNT(*) FROM audit_results WHERE created_at > NOW() - INTERVAL '1 hour'" 2>/dev/null || echo "?")
  TOTAL=$(pgq "SELECT COUNT(*) FROM audit_results")
  echo "  Total: ${TOTAL:-0} | Last hour: ${RECENT:-0}"
fi

# ----------------------------------------------------------------------------
# Evaluation pointers
# ----------------------------------------------------------------------------
section "Evaluation Artifacts"
echo "  Notebook:        http://127.0.0.1:8889/notebooks/evaluation/ml_audit_evaluation.ipynb?token=databox"
echo "  MLflow Models:   $MLFLOW_URL/#/models"
echo "  MLflow Traces:   $MLFLOW_URL/#/experiments/1/traces"

echo ""
echo "=== AI Governance Check Complete ==="
echo "  ☞ Run pipeline step 'Test Agent (Single Question)' for a smoke test"
echo "  ☞ Run 'Run Quick Eval' for 3-test judge scoring (~1 min)"
