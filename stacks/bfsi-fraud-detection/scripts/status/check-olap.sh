#!/bin/bash
#
# For demonstration purposes only.
#
# ============================================================================
# BFSI OLAP Use Case — Check Status
# ============================================================================
# Diagnostic snapshot of the OLAP analytics + CDC pipelines:
#   • PGD source counts
#   • Debezium connector state + Kafka CDC topics
#   • ClickHouse target tables + row counts
#   • RisingWave Kafka sources + row counts
#   • PGAA (Iceberg) extension + Lakekeeper catalog
#   • Metabase dashboard public link
# ============================================================================

set +e  # do not exit on individual probe failures — we want the full picture

PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"
KC_HOST="http://localhost:8084"

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
echo "=== OLAP Status ==="

# ----------------------------------------------------------------------------
# Source: PGD
# ----------------------------------------------------------------------------
section "PGD Source"
if ! docker inspect -f '{{.State.Status}}' "$PG_CONTAINER" >/dev/null 2>&1; then
  miss "$PG_CONTAINER not running — Usecase 1 (OLTP) Start Service must be run first"
  exit 0
fi
PG_HEALTH=$(docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null || echo "unknown")
ok "$PG_CONTAINER: running ($PG_HEALTH)"
TX_COUNT=$(pgq "SELECT count(*) FROM transactions" || echo 0)
TX_COUNT=${TX_COUNT:-0}
echo "  Transactions: $TX_COUNT"
PUB=$(pgq "SELECT 1 FROM pg_publication WHERE pubname='rw_pub_corebanking'")
if [ "$PUB" = "1" ]; then ok "Publication rw_pub_corebanking present"
else miss "Publication rw_pub_corebanking missing — Usecase 1 (OLTP) not run"; fi

# ----------------------------------------------------------------------------
# Debezium connector + Kafka CDC topics
# ----------------------------------------------------------------------------
section "Debezium CDC (PGD → Kafka)"
if ! docker inspect -f '{{.State.Status}}' bfsi-kafka-connect >/dev/null 2>&1; then
  miss "bfsi-kafka-connect not running"
else
  KC_STATE=$(curl -sf -m 3 "$KC_HOST/connectors/corebanking-postgres/status" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "MISSING")
  if [ "$KC_STATE" = "RUNNING" ]; then ok "Connector corebanking-postgres: RUNNING"
  elif [ "$KC_STATE" = "MISSING" ]; then miss "Connector not configured — run Start Service"
  else warn "Connector corebanking-postgres: $KC_STATE"; fi

  TASK_STATE=$(curl -sf -m 3 "$KC_HOST/connectors/corebanking-postgres/status" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('tasks',[{}])[0].get('state','UNKNOWN') if d.get('tasks') else 'NO_TASKS')" 2>/dev/null || echo "UNKNOWN")
  echo "  Task[0]: $TASK_STATE"
fi

if docker inspect -f '{{.State.Status}}' bfsi-kafka >/dev/null 2>&1; then
  TOPICS=$(docker exec bfsi-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -E "^corebanking\." | sort)
  if [ -n "$TOPICS" ]; then
    echo "  CDC topics:"
    echo "$TOPICS" | sed 's/^/    /'
  else
    warn "No corebanking.* CDC topics yet"
  fi
else
  miss "bfsi-kafka not running"
fi

# ----------------------------------------------------------------------------
# ClickHouse — batch path
# ----------------------------------------------------------------------------
section "ClickHouse (Batch Analytics)"
if ! docker inspect -f '{{.State.Status}}' bfsi-clickhouse >/dev/null 2>&1; then
  miss "bfsi-clickhouse not running"
else
  ok "bfsi-clickhouse: running"
  CH_TX=$(docker exec bfsi-clickhouse clickhouse-client --user default --password admin123 \
    --query "SELECT count() FROM default.transactions" 2>/dev/null | tr -d ' ' || echo "0")
  CH_TX=${CH_TX:-0}
  echo "  Transactions: $CH_TX"
  if [ "$TX_COUNT" -gt 0 ] && [ "$CH_TX" -gt 0 ]; then
    LAG_PCT=$(awk -v a="$CH_TX" -v b="$TX_COUNT" 'BEGIN{printf "%.1f", (1 - a/b) * 100}')
    echo "  Lag vs PGD: ${LAG_PCT}%"
  fi
  echo "  Tables:"
  docker exec bfsi-clickhouse clickhouse-client --user default --password admin123 \
    --query "SELECT name, engine, total_rows FROM system.tables WHERE database='default' AND engine LIKE '%MergeTree%' ORDER BY name FORMAT TSV" 2>/dev/null \
    | awk '{printf "    %-30s %-22s %s\n", $1, $2, $3}' || echo "    (none)"
fi

# ----------------------------------------------------------------------------
# RisingWave — streaming path
# ----------------------------------------------------------------------------
section "RisingWave (Streaming Analytics)"
if ! docker inspect -f '{{.State.Status}}' bfsi-risingwave >/dev/null 2>&1; then
  miss "bfsi-risingwave not running"
else
  ok "bfsi-risingwave: running"
  RW_TX=$(docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h risingwave -p 4566 -U root -d dev -tAc "SELECT count(*) FROM kafka_transactions;" 2>/dev/null | head -1 | tr -d ' ' || echo "0")
  RW_TX=${RW_TX:-0}
  echo "  Transactions (Kafka source): $RW_TX"
  if [ "$TX_COUNT" -gt 0 ] && [ "$RW_TX" -gt 0 ]; then
    LAG_PCT=$(awk -v a="$RW_TX" -v b="$TX_COUNT" 'BEGIN{printf "%.1f", (1 - a/b) * 100}')
    echo "  Lag vs PGD: ${LAG_PCT}%"
  fi
  echo "  Sources:"
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h risingwave -p 4566 -U root -d dev -tAc "SELECT name FROM rw_sources WHERE schema_id=(SELECT id FROM rw_schemas WHERE name='public') ORDER BY name;" 2>/dev/null \
    | grep -v '^$' | sed 's/^/    /' || echo "    (none)"
fi

# ----------------------------------------------------------------------------
# PGAA — Iceberg path
# ----------------------------------------------------------------------------
section "PGAA (Iceberg via Lakekeeper)"
PGAA_VER=$(pgq "SELECT pgaa.pgaa_version()" 2>/dev/null)
if [ -n "$PGAA_VER" ]; then
  ok "PGAA extension: $PGAA_VER"
else
  miss "PGAA extension not available"
fi

PGAA_CATALOGS=$(docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc \
  "SELECT name||' ('||type||', '||status||')' FROM pgaa.list_catalogs();" 2>/dev/null | grep -v '^$')
if [ -n "$PGAA_CATALOGS" ]; then
  echo "  Catalogs:"
  echo "$PGAA_CATALOGS" | sed 's/^/    /'
else
  warn "No catalogs configured"
fi

PGAA_TABLES=$(docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -P pager=off -tAc \
  "SELECT table_name||' ('||replication_status||')' FROM pgaa.list_analytics_tables();" 2>/dev/null | grep -v '^$')
if [ -n "$PGAA_TABLES" ]; then
  echo "  Analytics tables:"
  echo "$PGAA_TABLES" | sed 's/^/    /'
else
  warn "No analytics tables registered yet"
fi

if docker inspect -f '{{.State.Status}}' bfsi-lakekeeper >/dev/null 2>&1; then
  ok "Lakekeeper catalog: running (http://localhost:8181)"
else
  miss "bfsi-lakekeeper not running"
fi

# ----------------------------------------------------------------------------
# Metabase
# ----------------------------------------------------------------------------
section "Metabase Dashboards"
if curl -sf -m 3 http://localhost:3002/api/health >/dev/null 2>&1; then
  ok "Metabase: healthy (http://localhost:3002)"
  # Metabase /api doesn't accept basic auth — must POST /api/session first
  # to get a token, then send it as X-Metabase-Session on subsequent calls.
  MB_SESSION=$(curl -sf -m 3 -X POST -H "Content-Type: application/json" \
    -d '{"username":"admin@corebanking.local","password":"CoreBank1!"}' \
    "http://localhost:3002/api/session" 2>/dev/null \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  if [ -n "$MB_SESSION" ]; then
    PUB_LINK=$(curl -sf -m 5 -H "X-Metabase-Session: $MB_SESSION" \
      "http://localhost:3002/api/dashboard/" 2>/dev/null \
      | python3 -c "import sys,json;d=json.load(sys.stdin);print(next((x.get('public_uuid','') for x in d if 'Core Banking Fraud' in x.get('name','')),''))" 2>/dev/null)
    if [ -n "$PUB_LINK" ]; then
      echo "  Public dashboard: http://localhost:3002/public/dashboard/$PUB_LINK"
    else
      warn "Public dashboard link not found — run Start Service to create it"
    fi
  else
    warn "Could not log in to Metabase API — skipping public link check"
  fi
else
  miss "Metabase not healthy"
fi

# ----------------------------------------------------------------------------
echo ""
echo "=== OLAP Check Complete ==="
