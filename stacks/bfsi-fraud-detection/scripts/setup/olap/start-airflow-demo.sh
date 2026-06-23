#!/bin/bash
# ============================================================================
# BFSI OLAP Use Case — Optional step: Start Airflow Reconciliation Demo
# ============================================================================
# For demonstration purposes only.
#
# Brings up Airflow (4 containers) and registers the fraud_reconciliation
# DAG that diffs transactions row counts across pgd / ClickHouse / RisingWave
# / Kafka topic offsets. Fails the DAG run if drift > 1%.
#
# Prerequisite: UC1 (OLTP) + UC2 (OLAP) Start Service must already be complete
# so that pgd, kafka, clickhouse, risingwave are all up and CDC is flowing.
#
# Stages:
#   [0/5] Verify UC1 + UC2 prerequisites
#   [1/5] Start Airflow (--profile airflow) and wait healthy
#   [2/5] Verify DAG fraud_reconciliation is registered
#   [3/5] Unpause DAG (so the scheduler will pick it up hourly)
#   [4/5] Trigger one immediate run so the demo has a first result visible
#   [5/5] Print Airflow UI URL
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
AF_URL="http://127.0.0.1:8888"
AF_AUTH="admin:admin"
DAG_ID="fraud_reconciliation"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

compose_quiet() {
  grep -vE "^[[:space:]]*(Container|Volume|Network)[[:space:]]+.*[[:space:]]+(Creating|Created|Starting|Started|Running|Healthy|Waiting|Exited|Recreated|Recreate)[[:space:]]*$" \
  | grep -vE "^time=.*level=(warning|info)" \
  || true
}

# ----------------------------------------------------------------------------
# [0/5] Verify UC1 + UC2 prerequisites
# ----------------------------------------------------------------------------
step "[0/5] Verify UC1 (OLTP) + UC2 (OLAP) are complete"
if ! docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null | grep -q healthy; then
  fail "$PG_CONTAINER not healthy. Run UC1 (OLTP) → Start Service first."
fi
ok "$PG_CONTAINER healthy"

for svc in bfsi-kafka bfsi-clickhouse bfsi-risingwave bfsi-kafka-connect; do
  if ! docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null | grep -q running; then
    fail "$svc not running. Run UC2 (OLAP) → Start Service first."
  fi
done
ok "kafka / clickhouse / risingwave / kafka-connect all running"

# ----------------------------------------------------------------------------
# [1/5] Start Airflow services
# ----------------------------------------------------------------------------
step "[1/5] Start Airflow (--profile airflow up -d)"
cd "$STACK_DIR"
docker compose --profile airflow up -d 2>&1 | compose_quiet
ok "Airflow containers started"

echo "  Waiting for airflow-webserver to become healthy..."
for i in $(seq 1 120); do
  state=$(docker inspect -f '{{.State.Health.Status}}' bfsi-airflow-webserver 2>/dev/null || echo "missing")
  if [ "$state" = "healthy" ]; then
    ok "airflow-webserver healthy (took ${i}s)"
    break
  fi
  [ "$i" = "120" ] && fail "airflow-webserver did not become healthy in 120s"
  sleep 1
done

# ----------------------------------------------------------------------------
# [2/5] Verify DAG is registered
# ----------------------------------------------------------------------------
step "[2/5] Verify DAG '$DAG_ID' is registered"
for i in $(seq 1 30); do
  code=$(curl -sS -o /dev/null -w "%{http_code}" -u "$AF_AUTH" "$AF_URL/api/v1/dags/$DAG_ID" || echo "000")
  if [ "$code" = "200" ]; then
    ok "DAG $DAG_ID registered (took ${i}s)"
    break
  fi
  [ "$i" = "30" ] && fail "DAG $DAG_ID not registered after 30s (Airflow scheduler may not have parsed dags/ yet)"
  sleep 1
done

# ----------------------------------------------------------------------------
# [3/5] Unpause DAG
# ----------------------------------------------------------------------------
step "[3/5] Unpause DAG so the 2-minute schedule activates"
curl -sS -u "$AF_AUTH" -X PATCH \
  -H "Content-Type: application/json" \
  -d '{"is_paused": false}' \
  "$AF_URL/api/v1/dags/$DAG_ID" >/dev/null \
  && ok "DAG unpaused" \
  || fail "Failed to unpause DAG"

# ----------------------------------------------------------------------------
# [4/5] Trigger one immediate run
# ----------------------------------------------------------------------------
step "[4/5] Trigger one DAG run now (so demo shows a result immediately)"
run_id="manual__$(date +%Y%m%dT%H%M%S)"
http_code=$(curl -sS -u "$AF_AUTH" -X POST \
  -H "Content-Type: application/json" \
  -d "{\"dag_run_id\": \"$run_id\", \"conf\": {}}" \
  -o /tmp/airflow-trigger.json -w "%{http_code}" \
  "$AF_URL/api/v1/dags/$DAG_ID/dagRuns" || echo "000")
if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
  ok "Triggered DAG run: $run_id"
else
  ok "Trigger returned HTTP $http_code (may already have a queued run — non-fatal)"
fi

# ----------------------------------------------------------------------------
# [5/5] Print URLs
# ----------------------------------------------------------------------------
step "[5/5] Airflow ready"
ok "Airflow UI:              $AF_URL  (admin / admin)"
ok "Reconciliation runs:     $AF_URL/dags/$DAG_ID/grid"
ok "DAG schedule:            every 2 minutes (cron: */2 * * * *)"
echo ""
echo "=== Airflow Reconciliation Demo Ready ==="
