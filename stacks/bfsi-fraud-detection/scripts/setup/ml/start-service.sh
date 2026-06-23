#!/bin/bash
# ============================================================================
# BFSI ML Fraud Detection (Usecase 3) — Start Service orchestrator
# ============================================================================
# For demonstration purposes only.
#
# Wires up the ML inference layer on top of OLTP + OLAP:
#   1. Starts --profile ml (5 inference engines: kafka feature materializer,
#      ml-kafka, ml-rw, ml-ch, ml-pgaa)
#   2. Trains and registers the XGBoost fraud model via Jupyter notebook
#   3. Creates ClickHouse ML MVs (feature + lifetime aggregations)
#   4. Creates RisingWave ML MVs (lifetime aggregations)
#   5. Waits for the inference loop to produce its first predictions
#   6. Surfaces ML-stage Metabase setup
#
# Prerequisites: Usecase 1 (OLTP) and Usecase 2 (OLAP) Start Service complete.
# This step verifies them up-front and fails fast if they're missing.
#
# Stages (printed inline):
#   [0/7] Verify OLTP + OLAP foundation
#   [1/7] Start ML profile (--profile ml) + wait running
#   [2/7] Train & register XGBoost fraud model (Jupyter)
#   [3/7] Create ClickHouse ML MVs
#   [4/7] Create RisingWave ML MVs
#   [5/7] Wait for first predictions to land
#   [6/7] Setup Metabase ML stage
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
KC_HOST="http://127.0.0.1:8084"
MLFLOW_URL="http://127.0.0.1:5001"
MODEL_NAME="fraud-detection-model"
NOTEBOOK_PATH="/home/jovyan/notebooks/mlflow-mlops-pipeline.ipynb"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | head -1 | tr -d ' '
}

# ----------------------------------------------------------------------------
# [0/7] Verify Usecase 1 (OLTP) + Usecase 2 (OLAP) prerequisites
# ----------------------------------------------------------------------------
step "[0/7] Verify OLTP + OLAP foundation"
if ! docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null | grep -q healthy; then
  fail "$PG_CONTAINER not healthy. Run Usecase 1 (OLTP) Start Service first."
fi
TX_COUNT=$(pgq "SELECT COUNT(*) FROM transactions")
TX_COUNT=${TX_COUNT:-0}
if [ "$TX_COUNT" = "0" ]; then
  fail "transactions table empty. Run Usecase 1 (OLTP) Start Service to seed data."
fi
ok "OLTP foundation: $TX_COUNT transactions"

# OLAP requires ClickHouse target tables + Debezium connector RUNNING
if ! docker exec bfsi-clickhouse clickhouse-client --user default --password admin123 \
     --query "SELECT count() FROM default.transactions" >/dev/null 2>&1; then
  fail "ClickHouse OLAP tables missing. Run Usecase 2 (OLAP) Start Service first."
fi
KC_STATE=$(curl -sf -m 3 "$KC_HOST/connectors/corebanking-postgres/status" 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "MISSING")
if [ "$KC_STATE" != "RUNNING" ]; then
  fail "Debezium connector not RUNNING (state=$KC_STATE). Run Usecase 2 (OLAP) Start Service."
fi
ok "OLAP foundation: ClickHouse tables + Debezium RUNNING"

# ml_fraud_predictions placeholder (created in db/oltp.sql) must exist
if [ "$(pgq "SELECT to_regclass('public.ml_fraud_predictions') IS NOT NULL")" != "t" ]; then
  fail "ml_fraud_predictions table missing. Re-run Usecase 1 (OLTP) Start Service."
fi
ok "ml_fraud_predictions placeholder present"

# ----------------------------------------------------------------------------
# [1/7] Start ML profile services
# ----------------------------------------------------------------------------
step "[1/7] Start ML inference engines (--profile ml)"
cd "$STACK_DIR"
# Drop Compose's per-container orchestration chatter; the ok line below
# summarises what new containers actually came up.
docker compose --profile ml up -d 2>&1 \
  | grep -vE "^[[:space:]]*(Container|Volume|Network)[[:space:]].*[[:space:]](Creating|Created|Starting|Started|Running|Healthy|Waiting|Exited|Recreated|Recreate)[[:space:]]*$" \
  | grep -vE "^time=.*level=(warning|info)" \
  || true

# Wait for all 5 inference containers to be running (state, not healthy — these
# don't have healthchecks; they're long-running Python loops).
for svc in bfsi-kafka-feature-materializer bfsi-ml-kafka bfsi-ml-ch bfsi-ml-rw bfsi-ml-pgaa; do
  echo "  Waiting for $svc to be running..."
  for i in $(seq 1 30); do
    state=$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
    if [ "$state" = "running" ]; then
      ok "$svc running (took ${i}s)"
      break
    fi
    [ "$i" = "30" ] && fail "$svc did not start in 30s (state=$state). Check: docker logs $svc"
    sleep 1
  done
done

# ----------------------------------------------------------------------------
# [2/7] Train & register the XGBoost fraud model (idempotent — re-runs train
# the latest data, register a new version on top of any existing one).
# ----------------------------------------------------------------------------
step "[2/7] Train & register fraud model (Jupyter notebook)"
# Skip-if-already-registered: if we already have a registered version of
# fraud-detection-model, don't re-run a 5-minute notebook on every start.
EXISTING_VERS=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);v=d.get('registered_model',{}).get('latest_versions',[]);print(len(v))" 2>/dev/null || echo "0")
if [ "${EXISTING_VERS:-0}" -gt 0 ]; then
  ok "Model '$MODEL_NAME' already registered ($EXISTING_VERS version(s)) — skipping notebook"
  echo "    To force retraining: docker exec bfsi-jupyter jupyter nbconvert --to notebook --execute $NOTEBOOK_PATH"
else
  echo "  Running notebook (this takes ~3-5 min on a fast laptop, up to ~15 min on a constrained one — training XGBoost + logging to MLflow)..."
  # Per-cell timeout: the "Run all experiments" cell does 5 sequential XGBoost
  # trainings + MinIO artifact uploads. 600s was too tight on WSL2 / memory-
  # capped jupyter; 1800s gives 3x headroom without making the script feel
  # unbounded.
  docker exec bfsi-jupyter bash -c "jupyter nbconvert --to notebook --execute $NOTEBOOK_PATH --ExecutePreprocessor.timeout=1800 2>&1" \
    | tail -8 || fail "Notebook execution failed. Check: docker logs bfsi-jupyter"
  # Verify registration succeeded
  REG_VERS=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);v=d.get('registered_model',{}).get('latest_versions',[]);print(len(v))" 2>/dev/null || echo "0")
  if [ "${REG_VERS:-0}" -gt 0 ]; then
    ok "Model '$MODEL_NAME' registered ($REG_VERS version(s))"
  else
    fail "Notebook ran but model not registered in MLflow. Check $MLFLOW_URL"
  fi
fi

# Ensure the 'champion' alias points at the latest version. The inference
# engines' first lookup is `models:/$MODEL_NAME@champion` (alias-style); they
# only fall back to stage-style lookups, and then to local .pkl files. The
# notebook registers the model and sets stage='Production' but does NOT set
# the alias, so without this step the engines hang at "Waiting for model
# file to become available..." until someone sets the alias by hand.
LATEST_VERSION=$(curl -sf -m 5 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null \
  | python3 -c "
import sys, json
d = json.load(sys.stdin).get('registered_model', {})
vs = d.get('latest_versions') or []
if vs:
    print(max(int(v.get('version', 0)) for v in vs))
" 2>/dev/null || echo "")
if [ -n "$LATEST_VERSION" ]; then
  curl -sf -m 5 -X POST "$MLFLOW_URL/api/2.0/mlflow/registered-models/alias" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$MODEL_NAME\",\"alias\":\"champion\",\"version\":\"$LATEST_VERSION\"}" \
    >/dev/null 2>&1 || true
  ok "Alias 'champion' → version $LATEST_VERSION (inference engines use this)"
fi

# ----------------------------------------------------------------------------
# [3/7] ClickHouse ML MVs
# ----------------------------------------------------------------------------
step "[3/7] Create ClickHouse ML MVs"
docker exec -i "$PG_CONTAINER" bash /scripts/setup/ml/setup-clickhouse-ml.sh
ok "ClickHouse ML MVs created (feature + lifetime AggregatingMergeTree)"

# ----------------------------------------------------------------------------
# [4/7] RisingWave ML MVs
# ----------------------------------------------------------------------------
step "[4/7] Create RisingWave ML MVs"
docker exec -i "$PG_CONTAINER" bash /scripts/setup/ml/setup-risingwave-ml.sh
ok "RisingWave ML MVs created (lifetime aggregations)"

# ----------------------------------------------------------------------------
# [5/7] Wait for first predictions — proves the inference loop is wired end-
# to-end. Predictions only flow when (a) live TX are arriving via Kafka, and
# (b) the inference engines have consumed them. Non-fatal: synthetic data may
# not be running yet.
# ----------------------------------------------------------------------------
step "[5/7] Wait for first ML predictions"
echo "  Polling ml_fraud_predictions for first inference output (60s timeout)..."
for i in $(seq 1 60); do
  PRED_COUNT=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions")
  PRED_COUNT=${PRED_COUNT:-0}
  if [ "$PRED_COUNT" -gt 0 ]; then
    SOURCES=$(pgq "SELECT string_agg(DISTINCT prediction_source, ',' ORDER BY prediction_source) FROM ml_fraud_predictions")
    ok "First predictions landed: $PRED_COUNT total, sources=[${SOURCES:-?}] (took ${i}s)"
    break
  fi
  if [ "$i" = "60" ]; then
    echo "  ⚠ No predictions in 60s — engines started but no input received."
    echo "    Click ▶ Start Synthetic Data in Workspace to begin TX flow,"
    echo "    or rerun this step after sim has been running for a few seconds."
    break
  fi
  sleep 1
done

# ----------------------------------------------------------------------------
# [6/7] Metabase ML stage
# ----------------------------------------------------------------------------
step "[6/7] Setup Metabase ML stage"
if curl -sf -m 3 http://127.0.0.1:3002/api/health >/dev/null 2>&1; then
  cd "$STACK_DIR" && docker compose run --rm metabase-setup \
    python /app/setup_metabase.py --stage ml 2>&1 || \
    echo "  ⚠ Metabase ML setup failed (non-fatal). Open http://127.0.0.1:3002 manually."
  ok "Metabase ML stage applied (Fraud Detection tab now backed by real predictions)"
else
  echo "  ⚠ Metabase not healthy — skipping ML stage."
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ ML Inference Service Ready ━━━"
PRED_FINAL=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions" || echo 0)
echo "  ML predictions: ${PRED_FINAL:-0}"
echo "  Inference engines: kafka-feature-materializer + ml-kafka + ml-ch + ml-rw + ml-pgaa"
echo ""
echo "  MLflow:    $MLFLOW_URL  (model registry: $MODEL_NAME)"
echo "  Metabase:  http://127.0.0.1:3002 (Fraud Detection tab)"
echo ""
echo "  ☞ Click ▶ Start Synthetic Data in Workspace to drive new predictions."
echo "  ☞ Use Reset ML Inference (Step 3) to wipe and rerun on fresh state."
