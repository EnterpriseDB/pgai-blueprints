#!/bin/bash
# ============================================================================
# BFSI ML Governance (Usecase 4) — Start Service
# ============================================================================
# For demonstration purposes only.
#
# ML Governance has no infrastructure to set up — MLflow is in the foundation
# profile (already running from Deploy) and experiments/runs/models are
# created by Usecase 3 (ML Fraud Detection) training.
#
# Start Service for this use case is a verification + summary step:
#   1. Confirm MLflow is reachable
#   2. Confirm Usecase 3 has produced experiments
#   3. Confirm Usecase 3 has registered the fraud model
#   4. Surface governance entry-point URLs (experiments / models / traces)
#
# Stages (printed inline):
#   [0/4] Verify Usecase 3 (ML Fraud Detection) prerequisites
#   [1/4] Probe MLflow tracking server
#   [2/4] Confirm experiments + run counts
#   [3/4] Confirm registered model + versions
#   [4/4] Print governance entry points
# ============================================================================

set -euo pipefail

MLFLOW_URL="${MLFLOW_URL:-http://127.0.0.1:5001}"
MODEL_NAME="${MODEL_NAME:-fraud-detection-model}"
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
# [0/4] Verify Usecase 3 produced predictions (a soft-but-strong indicator
# the ML pipeline ran end-to-end). Without this, governance has nothing to
# govern.
# ----------------------------------------------------------------------------
step "[0/4] Verify Usecase 3 (ML Fraud Detection) is operational"
if [ "$(pgq "SELECT to_regclass('public.ml_fraud_predictions') IS NOT NULL")" != "t" ]; then
  fail "ml_fraud_predictions missing. Run Usecase 1 (OLTP) Start Service first."
fi
PRED_COUNT=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions")
PRED_COUNT=${PRED_COUNT:-0}
if [ "$PRED_COUNT" = "0" ]; then
  warn "No ML predictions yet — Usecase 3 setup ran but inference may not have started."
  echo "    Continuing — governance metadata still works without predictions."
else
  ok "ML pipeline operational ($PRED_COUNT predictions in pgd)"
fi

# ----------------------------------------------------------------------------
# [1/4] MLflow tracking server health
# ----------------------------------------------------------------------------
step "[1/4] Probe MLflow tracking server"
if ! curl -sf -m 3 "$MLFLOW_URL/api/2.0/mlflow/experiments/search" \
     -H "Content-Type: application/json" -d '{"max_results":1}' >/dev/null 2>&1; then
  fail "MLflow not reachable at $MLFLOW_URL — check bfsi-mlflow container"
fi
ok "MLflow reachable at $MLFLOW_URL"

# ----------------------------------------------------------------------------
# [2/4] Experiments + run counts
# ----------------------------------------------------------------------------
step "[2/4] Confirm experiments and run counts"
EXP_DATA=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/experiments/search" \
  -H "Content-Type: application/json" -d '{"max_results":100}' 2>/dev/null \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
exps = d.get('experiments', [])
ids = [e['experiment_id'] for e in exps if e.get('experiment_id') != '0']
print(len(exps))
print(','.join(ids))
" 2>/dev/null || echo "0\n")
EXP_COUNT=$(echo "$EXP_DATA" | sed -n '1p')
EXP_IDS=$(echo "$EXP_DATA" | sed -n '2p')

if [ "${EXP_COUNT:-0}" = "0" ]; then
  fail "No experiments found in MLflow. Run Usecase 3 (ML Fraud Detection) Start Service first."
fi
ok "Found $EXP_COUNT experiment(s) in MLflow"

if [ -n "$EXP_IDS" ]; then
  # JSON-encode the comma-separated id list. Use `|| true` to keep the
  # outer set -e from aborting if python3 emits anything unexpected.
  EXP_ID_JSON=$(python3 -c "import json,sys;print(json.dumps(sys.argv[1].split(',')))" "$EXP_IDS" 2>/dev/null || echo "[]")
  RUN_COUNT=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/runs/search" \
    -H "Content-Type: application/json" \
    -d "{\"experiment_ids\":$EXP_ID_JSON,\"max_results\":1000}" 2>/dev/null \
    | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('runs',[])))" 2>/dev/null || echo 0)
  ok "Total runs across experiments: ${RUN_COUNT:-0}"
fi

# ----------------------------------------------------------------------------
# [3/4] Registered model
# ----------------------------------------------------------------------------
step "[3/4] Confirm registered fraud model"
REG_RESP=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null || echo "")
if [ -z "$REG_RESP" ]; then
  fail "Model '$MODEL_NAME' not registered. Run Usecase 3 Start Service first."
fi
REG_INFO=$(echo "$REG_RESP" | python3 -c "
import sys, json
d = json.load(sys.stdin).get('registered_model', {})
vs = d.get('latest_versions') or []
if not vs:
    print('NONE')
else:
    print(' | '.join(f\"v{v.get('version')} ({v.get('current_stage','None')})\" for v in vs))
" 2>/dev/null || echo "PARSE_ERR")

if [ "$REG_INFO" = "NONE" ] || [ "$REG_INFO" = "PARSE_ERR" ]; then
  fail "Model '$MODEL_NAME' has no registered versions"
fi
ok "Model: $MODEL_NAME"
ok "Versions: $REG_INFO"

# ----------------------------------------------------------------------------
# [4/4] Governance entry points
# ----------------------------------------------------------------------------
step "[4/4] Governance entry points"
echo "  • Experiments:        $MLFLOW_URL/#/experiments"
echo "  • Model Registry:     $MLFLOW_URL/#/models"
echo "  • Compare Runs:       $MLFLOW_URL/#/experiments/${EXP_IDS%%,*}"
echo "  • Traces (Usecase 5): $MLFLOW_URL/#/experiments/1/traces (populated when GenAI agents run)"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ ML Governance Service Ready ━━━"
echo "  Experiments:        ${EXP_COUNT:-0}"
echo "  Total runs:         ${RUN_COUNT:-0}"
echo "  Registered model:   $MODEL_NAME ($REG_INFO)"
echo ""
echo "  ☞ Use 'Check Status' for detailed experiment/run/version dumps."
