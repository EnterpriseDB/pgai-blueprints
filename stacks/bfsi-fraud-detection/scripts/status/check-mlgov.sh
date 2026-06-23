#!/bin/bash
#
# For demonstration purposes only.
#
# ============================================================================
# BFSI ML Governance (Usecase 4) — Check Status
# ============================================================================
# Detailed governance snapshot:
#   • All experiments (count + creation time)
#   • Best run per experiment (highest F1)
#   • Recent runs (top 10) with key metrics
#   • Registered model + all versions + their stages
#   • Pointers to MLflow UI
# ============================================================================

set +e

MLFLOW_URL="${MLFLOW_URL:-http://localhost:5001}"
MODEL_NAME="${MODEL_NAME:-fraud-detection-model}"

section() { echo ""; echo "━━━ $* ━━━"; }
ok()      { echo "  ✓ $*"; }
warn()    { echo "  ⚠ $*"; }
miss()    { echo "  ✗ $*"; }

echo "=== ML Governance Status ==="

# ----------------------------------------------------------------------------
# MLflow reachability
# ----------------------------------------------------------------------------
section "MLflow Tracking Server"
if ! curl -sf -m 3 "$MLFLOW_URL/api/2.0/mlflow/experiments/search" \
     -H "Content-Type: application/json" -d '{"max_results":1}' >/dev/null 2>&1; then
  miss "Not reachable at $MLFLOW_URL"
  echo "    Check: docker logs bfsi-mlflow"
  exit 0
fi
ok "Reachable at $MLFLOW_URL"

# ----------------------------------------------------------------------------
# Experiments
# ----------------------------------------------------------------------------
section "Experiments"
EXPS_JSON=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/experiments/search" \
  -H "Content-Type: application/json" -d '{"max_results":100}' 2>/dev/null || echo '{}')
echo "$EXPS_JSON" | python3 -c "
import sys, json
from datetime import datetime
d = json.load(sys.stdin)
exps = d.get('experiments', [])
if not exps:
    print('  (none)')
    sys.exit(0)
print(f'  Total: {len(exps)}')
print()
print(f'  {\"ID\":<6} {\"NAME\":<40} {\"CREATED\":<20} {\"STATUS\":<10}')
print('  ' + '-' * 78)
for e in exps:
    eid = e.get('experiment_id', '')
    name = (e.get('name', '') or '')[:38]
    ts = e.get('creation_time', 0)
    created = datetime.fromtimestamp(int(ts) / 1000).strftime('%Y-%m-%d %H:%M') if ts else '?'
    status = e.get('lifecycle_stage', '')
    print(f'  {eid:<6} {name:<40} {created:<20} {status:<10}')
" 2>/dev/null || warn "Could not parse experiments"

# Capture experiment IDs for downstream queries
EXP_IDS=$(echo "$EXPS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ids = [e['experiment_id'] for e in d.get('experiments', [])]
print(json.dumps(ids))
" 2>/dev/null || echo '[]')

# ----------------------------------------------------------------------------
# Recent runs (top 10 across all experiments, ordered by start time)
# ----------------------------------------------------------------------------
section "Recent Runs (top 10)"
RUNS_JSON=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/runs/search" \
  -H "Content-Type: application/json" \
  -d "{\"experiment_ids\":$EXP_IDS,\"max_results\":10,\"order_by\":[\"start_time DESC\"]}" 2>/dev/null || echo '{}')
echo "$RUNS_JSON" | python3 -c "
import sys, json
from datetime import datetime
d = json.load(sys.stdin)
runs = d.get('runs', [])
if not runs:
    print('  (no runs found — Usecase 3 hasnt produced any yet)')
    sys.exit(0)
print(f'  {\"RUN NAME\":<35} {\"STATUS\":<10} {\"F1\":<8} {\"AUC\":<8} {\"START\":<18}')
print('  ' + '-' * 81)
for r in runs:
    info = r.get('info', {})
    data = r.get('data', {})
    metrics = {m['key']: m['value'] for m in data.get('metrics', [])}
    name = (data.get('tags') or [])
    name_tag = next((t['value'] for t in name if t['key'] == 'mlflow.runName'), info.get('run_id', '?')[:8])
    name_tag = (name_tag or '?')[:33]
    status = info.get('status', '?')
    f1 = metrics.get('f1', metrics.get('f1_score', 0))
    auc = metrics.get('auc_roc', metrics.get('roc_auc', 0))
    ts = info.get('start_time', 0)
    start = datetime.fromtimestamp(int(ts) / 1000).strftime('%m-%d %H:%M:%S') if ts else '?'
    print(f'  {name_tag:<35} {status:<10} {f1:<8.4f} {auc:<8.4f} {start:<18}')
" 2>/dev/null || warn "Could not parse runs"

# ----------------------------------------------------------------------------
# Registered model + versions
# ----------------------------------------------------------------------------
section "Registered Model: $MODEL_NAME"
REG_JSON=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null || echo '{}')
echo "$REG_JSON" | python3 -c "
import sys, json
from datetime import datetime
d = json.load(sys.stdin).get('registered_model', {})
if not d:
    print('  ✗ Not registered. Run Usecase 3 Start Service first.')
    sys.exit(0)
desc = d.get('description', '') or '(no description)'
print(f'  Description: {desc}')
vs = d.get('latest_versions', [])
if not vs:
    print('  ✗ No versions registered')
    sys.exit(0)
print()
print(f'  {\"VERSION\":<8} {\"STAGE\":<14} {\"STATUS\":<14} {\"CREATED\":<20} {\"RUN_ID\":<12}')
print('  ' + '-' * 72)
for v in vs:
    ver = v.get('version', '?')
    stage = v.get('current_stage', 'None')
    status = v.get('status', '?')
    ts = v.get('creation_timestamp', 0)
    created = datetime.fromtimestamp(int(ts) / 1000).strftime('%Y-%m-%d %H:%M') if ts else '?'
    run_id = (v.get('run_id', '') or '')[:10]
    print(f'  {ver:<8} {stage:<14} {status:<14} {created:<20} {run_id:<12}')
" 2>/dev/null || warn "Could not parse registered model"

# ----------------------------------------------------------------------------
# Best run by F1 (across the fraud-detection experiments)
# ----------------------------------------------------------------------------
section "Best Run (by F1 score)"
BEST_JSON=$(curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/runs/search" \
  -H "Content-Type: application/json" \
  -d "{\"experiment_ids\":$EXP_IDS,\"max_results\":1,\"order_by\":[\"metrics.f1 DESC\"]}" 2>/dev/null || echo '{}')
echo "$BEST_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
runs = d.get('runs', [])
if not runs:
    print('  (no runs to rank)')
    sys.exit(0)
r = runs[0]
info = r.get('info', {})
data = r.get('data', {})
metrics = {m['key']: m['value'] for m in data.get('metrics', [])}
params = {p['key']: p['value'] for p in data.get('params', [])}
name = next((t['value'] for t in (data.get('tags') or []) if t['key'] == 'mlflow.runName'), info.get('run_id','?'))
print(f'  Run: {name}')
print(f'  F1:        {metrics.get(\"f1\", 0):.4f}')
print(f'  Precision: {metrics.get(\"precision\", 0):.4f}')
print(f'  Recall:    {metrics.get(\"recall\", 0):.4f}')
print(f'  AUC-ROC:   {metrics.get(\"auc_roc\", 0):.4f}')
print(f'  URL:       $MLFLOW_URL/#/experiments/{info.get(\"experiment_id\",1)}/runs/{info.get(\"run_id\",\"\")}')
" 2>/dev/null || warn "Could not parse best run"

echo ""
echo "=== ML Governance Check Complete ==="
echo "  Experiments: $MLFLOW_URL/#/experiments"
echo "  Models:      $MLFLOW_URL/#/models"
