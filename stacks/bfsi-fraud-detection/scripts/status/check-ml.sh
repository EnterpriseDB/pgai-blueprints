#!/bin/bash
#
# For demonstration purposes only.
#
# ============================================================================
# BFSI ML Fraud Detection (Usecase 3) — Check Status
# ============================================================================
# Diagnostic snapshot of the 4 ML inference paths + MLflow registry:
#   • Predictions table (total + per-source counts + fraud rate)
#   • TTDF performance per source (avg / min / p95 ms)
#   • Each inference engine: container state + last 3 log lines
#   • MLflow model registry (registered model + latest version)
# ============================================================================

set +e

PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
MLFLOW_URL="http://localhost:5001"
MODEL_NAME="fraud-detection-model"

section() { echo ""; echo "━━━ $* ━━━"; }
ok()      { echo "  ✓ $*"; }
warn()    { echo "  ⚠ $*"; }
miss()    { echo "  ✗ $*"; }

pgq() {
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc "$1" 2>/dev/null \
    | head -1 | tr -d ' '
}

# ----------------------------------------------------------------------------
echo "=== ML Status ==="

# ----------------------------------------------------------------------------
# Predictions table
# ----------------------------------------------------------------------------
section "ML Predictions (ml_fraud_predictions)"
if [ "$(pgq "SELECT to_regclass('public.ml_fraud_predictions') IS NOT NULL")" != "t" ]; then
  miss "Table ml_fraud_predictions missing — run Usecase 1 (OLTP) Start Service"
else
  TOTAL=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions")
  TOTAL=${TOTAL:-0}
  echo "  Total predictions: $TOTAL"
  if [ "$TOTAL" -gt 0 ]; then
    FRAUD=$(pgq "SELECT COUNT(*) FROM ml_fraud_predictions WHERE is_fraud_predicted")
    echo "  Predicted fraud:   ${FRAUD:-0}"
    echo "  Per-source breakdown:"
    docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
      psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc \
      "SELECT prediction_source, COUNT(*) AS predictions, COUNT(*) FILTER (WHERE is_fraud_predicted) AS fraud
         FROM ml_fraud_predictions GROUP BY prediction_source ORDER BY prediction_source" 2>/dev/null \
      | awk -F'|' 'NF>=3 {printf "    %-12s predictions=%-10s fraud=%s\n", $1, $2, $3}'
  else
    warn "No predictions yet — start Synthetic Data in Workspace, or check engine logs"
  fi
fi

# ----------------------------------------------------------------------------
# TTDF (Time To Detect Fraud) — per-source latency stats
# ----------------------------------------------------------------------------
section "TTDF Performance (ms)"
if [ "${TOTAL:-0}" -gt 0 ]; then
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc \
    "SELECT prediction_source AS source,
            COUNT(*) AS n,
            ROUND(AVG(ttdf_milliseconds)::numeric, 0) AS avg_ms,
            MIN(ttdf_milliseconds) AS min_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 0) AS p95_ms
       FROM ml_fraud_predictions
       WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000
       GROUP BY prediction_source
       ORDER BY avg_ms" 2>/dev/null \
    | awk -F'|' 'NF>=5 {printf "    %-12s n=%-7s avg=%-6s min=%-6s p95=%s\n", $1, $2, $3" ms", $4" ms", $5" ms"}' \
    || warn "Could not compute TTDF stats"
else
  warn "Skipped — no predictions to summarise"
fi

# ----------------------------------------------------------------------------
# Inference engines
# ----------------------------------------------------------------------------
section "ML Inference Engines"
for svc in bfsi-kafka-feature-materializer bfsi-ml-kafka bfsi-ml-ch bfsi-ml-rw bfsi-ml-pgaa; do
  state=$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
  case "$state" in
    running)  ok "$svc: running" ;;
    exited)   miss "$svc: exited (code $(docker inspect -f '{{.State.ExitCode}}' "$svc" 2>/dev/null))" ;;
    missing)  miss "$svc: not deployed" ;;
    *)        warn "$svc: $state" ;;
  esac
  if [ "$state" = "running" ] || [ "$state" = "exited" ]; then
    docker logs --tail 2 "$svc" 2>&1 | sed 's/^/      | /'
  fi
done

# ----------------------------------------------------------------------------
# MLflow model registry
# ----------------------------------------------------------------------------
section "MLflow Model Registry"
if curl -sf -m 3 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" >/dev/null 2>&1; then
  ok "MLflow reachable at $MLFLOW_URL"
  REG_INFO=$(curl -sf -m 3 "$MLFLOW_URL/api/2.0/mlflow/registered-models/get?name=$MODEL_NAME" 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin).get('registered_model', {})
vs = d.get('latest_versions') or []
if not vs:
    print('NONE')
else:
    print(' | '.join(f\"v{v.get('version')} ({v.get('current_stage','None')})\" for v in vs))
" 2>/dev/null || echo "PARSE_ERR")
  if [ "$REG_INFO" = "NONE" ]; then
    miss "Model '$MODEL_NAME' has no registered versions — run Start Service"
  elif [ "$REG_INFO" = "PARSE_ERR" ]; then
    warn "Could not parse registry response"
  else
    echo "  Model: $MODEL_NAME"
    echo "  Versions: $REG_INFO"
  fi
else
  miss "MLflow not reachable at $MLFLOW_URL"
fi

# ----------------------------------------------------------------------------
echo ""
echo "=== ML Check Complete ==="
echo "  ☞ Live Fraud Detection tab: http://localhost:3002 (Core Banking Fraud Detection)"
echo "  ☞ MLflow experiments:        $MLFLOW_URL"
