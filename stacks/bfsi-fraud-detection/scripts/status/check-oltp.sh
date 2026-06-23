#!/bin/bash
# For demonstration purposes only.
# OLTP Status Check - Shows table row counts, Kafka topics, Debezium connector

echo "=== OLTP Status ==="
echo ""

echo "--- PGD Container ---"
if docker inspect -f '{{.State.Status}}' bfsi-pgd >/dev/null 2>&1; then
  echo "bfsi-pgd: $(docker inspect -f '{{.State.Status}} ({{.State.Health.Status}})' bfsi-pgd 2>/dev/null)"
else
  echo "bfsi-pgd: not running"
  exit 0
fi

echo ""
echo "--- PGD Tables (row counts) ---"
# fraud_labels is intentionally one row per transaction (baseline FALSE +
# UPDATE'd TRUE for matches), so listing its raw COUNT alongside transactions
# reads as "every transaction is fraud." Drop it from the table list and show
# the is_fraud split separately below.
docker exec bfsi-pgd psql -U postgres -d demo -P pager=off -c "SELECT relname AS table_name, n_live_tup AS row_count FROM pg_stat_user_tables WHERE schemaname='public' AND relname IN ('customers','accounts','transactions','fraud_rules') ORDER BY relname;" 2>&1 || echo "(tables not created yet — run Setup OLTP Schema)"

echo ""
echo "--- Fraud Labels (is_fraud breakdown) ---"
docker exec bfsi-pgd psql -U postgres -d demo -P pager=off -c "
  SELECT
    COUNT(*) FILTER (WHERE is_fraud)     AS fraud,
    COUNT(*) FILTER (WHERE NOT is_fraud) AS clean,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud) / NULLIF(COUNT(*), 0), 1) AS fraud_pct
  FROM fraud_labels;
" 2>&1 || echo "(fraud_labels table missing — run Usecase 1 Start Service)"

echo ""
echo "--- Fraud Rules ---"
docker exec bfsi-pgd psql -U postgres -d demo -P pager=off -c "SELECT COUNT(*) AS rule_count FROM fraud_rules WHERE is_active=TRUE;" 2>&1 || echo "(fraud_rules table missing — run Setup Basic Fraud Rules)"

echo ""
echo "--- Triggers on transactions ---"
docker exec bfsi-pgd psql -U postgres -d demo -P pager=off -c "SELECT tgname FROM pg_trigger WHERE tgrelid='transactions'::regclass AND NOT tgisinternal;" 2>&1 || echo "(no trigger found)"

echo ""
echo "--- Kafka Topics (optional — requires kafka running) ---"
if docker inspect -f '{{.State.Status}}' bfsi-kafka >/dev/null 2>&1; then
  docker exec bfsi-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "^(demo\.|corebanking\.)" | head -10 || echo "(no CDC topics yet — run Usecase 2: OLAP)"
else
  echo "(bfsi-kafka not running — OLAP stage not started)"
fi

echo ""
echo "--- Debezium Connector (optional) ---"
CONNECTOR_STATUS=$(curl -sf -m 3 http://localhost:8084/connectors/corebanking-postgres/status 2>/dev/null || echo "")
if echo "$CONNECTOR_STATUS" | grep -q "RUNNING"; then
  echo "Status: RUNNING"
else
  echo "Status: not configured (run Usecase 2: OLAP — Setup Debezium)"
fi

echo ""
echo "=== OLTP Check Complete ==="
