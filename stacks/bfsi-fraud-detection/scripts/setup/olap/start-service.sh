#!/bin/bash
# ============================================================================
# BFSI OLAP Use Case — Start Service orchestrator
# ============================================================================
# For demonstration purposes only.
#
# Sets up the analytics + CDC pipelines that consume from pgd:
#   pgd  → Debezium → Kafka topics → ClickHouse + RisingWave
#   pgd  → PGAA → Iceberg via Lakekeeper → S3 (MinIO)
# Plus a Metabase dashboard with 6 tabs reading from all three stores.
#
# Prerequisite: Usecase 1 (OLTP) → Start Service must be complete.
# That sets up the publication, replication role, schema, and seed data
# that this pipeline depends on.
#
# Stages (printed inline):
#   [0/7] Verify OLTP setup is in place
#   [1/7] Start fraud-alert (--profile olap)  -- foundation already up
#   [2/7] Setup Debezium CDC connector + wait RUNNING
#   [3/7] Create ClickHouse target tables
#   [4/7] Create RisingWave Kafka sources
#   [5/7] Wait for initial CDC snapshot to populate
#   [6/7] Setup Metabase OLAP dashboard (6-tab view)
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
KC_HOST="http://127.0.0.1:8084"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

pgq() {
  # Run a one-shot psql query inside bfsi-pgd, returning a clean trimmed value
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | head -1 | tr -d ' '
}

# ----------------------------------------------------------------------------
# [0/7] Verify OLTP foundation
# ----------------------------------------------------------------------------
step "[0/7] Verify Usecase 1 (OLTP) is complete"
if ! docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null | grep -q healthy; then
  fail "$PG_CONTAINER is not healthy. Run Usecase 1 (OLTP) → Start Service first."
fi
PUB_EXISTS=$(pgq "SELECT 1 FROM pg_publication WHERE pubname='rw_pub_corebanking'")
TX_COUNT=$(pgq "SELECT COUNT(*) FROM transactions")
RULES_COUNT=$(pgq "SELECT COUNT(*) FROM fraud_rules WHERE is_active=TRUE")
if [ "$PUB_EXISTS" != "1" ]; then
  fail "Publication 'rw_pub_corebanking' not found. Run Usecase 1 (OLTP) → Start Service."
fi
if [ -z "$TX_COUNT" ] || [ "$TX_COUNT" = "0" ]; then
  fail "transactions table is empty. Run Usecase 1 (OLTP) → Start Service to seed data."
fi
ok "OLTP foundation present (publication ✓, $TX_COUNT transactions, $RULES_COUNT active rules)"

# ----------------------------------------------------------------------------
# [1/7] Start fraud-alert (--profile olap adds the fraud-alert container only;
# foundation services like kafka-connect, clickhouse, risingwave were started
# at Deploy time. Compose may echo "Container X Running" for those — expected).
# ----------------------------------------------------------------------------
step "[1/7] Start fraud-alert (--profile olap)"
cd "$STACK_DIR"
# Drop Compose's per-container orchestration chatter and EXP_ID warnings. The
# `ok` line below summarises what actually changed.
docker compose --profile olap up -d 2>&1 \
  | grep -vE "^[[:space:]]*(Container|Volume|Network)[[:space:]].*[[:space:]](Creating|Created|Starting|Started|Running|Healthy|Waiting|Exited|Recreated|Recreate)[[:space:]]*$" \
  | grep -vE "^time=.*level=(warning|info)" \
  || true
ok "fraud-alert container started (foundation services already running from Deploy)"

# Wait for the OLAP-relevant containers to become healthy (kafka-connect must be
# ready before we can POST a connector). RisingWave/ClickHouse waits are quick.
for svc in bfsi-kafka-connect bfsi-clickhouse bfsi-risingwave; do
  echo "  Waiting for $svc to be healthy..."
  for i in $(seq 1 60); do
    state=$(docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "missing")
    if [ "$state" = "healthy" ]; then
      ok "$svc healthy (took ${i}s)"
      break
    fi
    [ "$i" = "60" ] && fail "$svc did not become healthy in 60s (state=$state)"
    sleep 1
  done
done

# ----------------------------------------------------------------------------
# [2/7] Setup Debezium CDC connector
# ----------------------------------------------------------------------------
step "[2/7] Setup Debezium CDC (PGD → Kafka)"
bash "$STACK_DIR/scripts/setup/olap/setup-debezium.sh"

# Wait for connector state = RUNNING (Kafka Connect needs a few seconds after
# the POST to actually start the snapshot/streaming task).
echo "  Waiting for Debezium connector to be RUNNING..."
for i in $(seq 1 30); do
  STATE=$(curl -sf -m 3 "$KC_HOST/connectors/corebanking-postgres/status" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  if [ "$STATE" = "RUNNING" ]; then
    ok "Debezium connector RUNNING (took ${i}s)"
    break
  fi
  [ "$i" = "30" ] && fail "Debezium connector did not reach RUNNING in 30s (state=$STATE)"
  sleep 1
done

# ----------------------------------------------------------------------------
# [3/7] Create ClickHouse target tables
# ----------------------------------------------------------------------------
step "[3/7] Create ClickHouse OLAP tables"
docker exec -i "$PG_CONTAINER" bash /scripts/setup/olap/setup-clickhouse-olap.sh
ok "ClickHouse tables created (4 MergeTree + Kafka engine)"

# ----------------------------------------------------------------------------
# [4/7] Create RisingWave Kafka sources
# ----------------------------------------------------------------------------
step "[4/7] Create RisingWave streaming sources"
# Debezium creates Kafka topics lazily as it streams snapshot rows. RisingWave's
# CREATE SOURCE reads metadata for the topic at DDL time, so we must wait for
# all 4 expected topics to exist before kicking off setup-risingwave-olap.sh.
# Otherwise the 4th source (typically fraud_labels — last in the snapshot
# order) will fail with "topic ... not found".
echo "  Waiting for all 4 CDC topics to exist..."
EXPECTED="corebanking.public.customers corebanking.public.accounts corebanking.public.transactions corebanking.public.fraud_labels"
for i in $(seq 1 60); do
  EXISTING=$(docker exec bfsi-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -E "^corebanking\.public\." | sort | tr '\n' ' ')
  MISSING=""
  for t in $EXPECTED; do
    echo "$EXISTING" | grep -q "$t " || MISSING="$MISSING $t"
  done
  if [ -z "$MISSING" ]; then
    ok "All 4 CDC topics ready (took ${i}s)"
    break
  fi
  [ "$i" = "60" ] && fail "Timed out waiting for CDC topics. Missing:$MISSING"
  sleep 1
done
docker exec -i "$PG_CONTAINER" bash /scripts/setup/olap/setup-risingwave-olap.sh
ok "RisingWave sources created (4 Kafka sources)"

# ----------------------------------------------------------------------------
# [5/7] Wait for initial CDC snapshot to populate ClickHouse
# ----------------------------------------------------------------------------
step "[5/7] Wait for initial CDC snapshot to populate"
echo "  Polling ClickHouse for snapshot completion (target: $TX_COUNT transactions)..."
PREV_CH=0
STABLE_COUNT=0
for i in $(seq 1 60); do
  CH_COUNT=$(docker exec bfsi-clickhouse clickhouse-client --user default --password admin123 \
    --query "SELECT count() FROM default.transactions" 2>/dev/null | tr -d ' ' || echo 0)
  CH_COUNT=${CH_COUNT:-0}
  # Catch up: stop when ClickHouse is within 5% of pgd's count (snapshot is
  # an ASYNC backfill — small lag is expected).
  if [ "$CH_COUNT" -gt 0 ] && [ "$CH_COUNT" -ge $((TX_COUNT * 95 / 100)) ]; then
    ok "ClickHouse caught up: $CH_COUNT / $TX_COUNT transactions (took ${i}s)"
    break
  fi
  if [ "$i" = "60" ]; then
    echo "  ⚠ ClickHouse only has $CH_COUNT / $TX_COUNT after 60s — snapshot may still be running. Continuing anyway."
    break
  fi
  echo "  ClickHouse: $CH_COUNT / $TX_COUNT (waiting...)"
  sleep 2
done

# ----------------------------------------------------------------------------
# [6/7] Setup Metabase OLAP dashboard
# ----------------------------------------------------------------------------
step "[6/7] Setup Metabase OLAP dashboard"
if curl -sf -m 3 http://127.0.0.1:3002/api/health >/dev/null 2>&1; then
  cd "$STACK_DIR" && docker compose run --rm metabase-setup \
    python /app/setup_metabase.py --stage olap 2>&1 || \
    echo "  ⚠ Metabase OLAP setup failed (non-fatal). Open http://127.0.0.1:3002 manually."
  ok "Metabase OLAP dashboard ready (6 tabs: OLTP, Fraud, Analytics, Streaming vs Batch, Kafka CDC, Search)"
else
  echo "  ⚠ Metabase not healthy — skipping dashboard setup."
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ OLAP Service Ready ━━━"
echo "  Source (PGD):     $TX_COUNT transactions"
CH_FINAL=$(docker exec bfsi-clickhouse clickhouse-client --user default --password admin123 \
  --query "SELECT count() FROM default.transactions" 2>/dev/null | tr -d ' ' || echo "n/a")
RW_FINAL=$(docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h risingwave -p 4566 -U root -d dev -tAc "SELECT count(*) FROM kafka_transactions;" 2>/dev/null | head -1 | tr -d ' ' || echo "n/a")
echo "  ClickHouse:       $CH_FINAL transactions"
echo "  RisingWave:       $RW_FINAL transactions (Kafka source)"
echo ""
echo "  Metabase: http://127.0.0.1:3002  (Core Banking Fraud Detection dashboard)"
echo ""
echo "  ☞ For live data flow, click ▶ Start Synthetic Data in workspace"
echo "    (if not already running). New TX will stream pgd → Kafka → CH/RW in real time."
