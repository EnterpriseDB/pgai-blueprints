#!/bin/bash
# Setup Debezium CDC connector for PGD -> Kafka
# Creates connector that streams transactions to Kafka topics
#
# For demonstration purposes only.

set -e

KC_HOST=${KAFKA_CONNECT_HOST:-http://localhost:8084}

echo "=== Debezium CDC Setup ==="

# Wait for Kafka Connect
echo "[1/3] Waiting for Kafka Connect..."
for i in {1..30}; do
  if curl -sf "$KC_HOST/connectors" > /dev/null 2>&1; then
    echo "  Kafka Connect ready"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "ERROR: Kafka Connect not available at $KC_HOST"
    exit 1
  fi
  echo "  Waiting... ($i/30)"
  sleep 2
done

# Delete existing connector if present
echo ""
echo "[2/3] Configuring Debezium connector..."
curl -sf -X DELETE "$KC_HOST/connectors/corebanking-postgres" 2>/dev/null || true
sleep 2

# Create connector
CONNECTOR_CONFIG='{
  "name": "corebanking-postgres",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "pgd",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "secret",
    "database.dbname": "demo",
    "database.server.name": "corebanking",
    "table.include.list": "public.customers,public.accounts,public.transactions,public.fraud_labels",
    "plugin.name": "pgoutput",
    "publication.name": "rw_pub_corebanking",
    "slot.name": "kafka_slot_corebanking",
    "topic.prefix": "corebanking",
    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "none",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}'

RESULT=$(curl -sf -X POST "$KC_HOST/connectors" \
  -H "Content-Type: application/json" \
  -d "$CONNECTOR_CONFIG" 2>&1)

if echo "$RESULT" | grep -q "corebanking-postgres"; then
  echo "  Connector created successfully"
else
  echo "  ERROR: $RESULT"
  exit 1
fi

# Verify connector status — non-fatal. The connector takes a moment to start
# its task after the POST returns 201, so a transient non-RUNNING here is
# normal. The parent orchestrator (start-service.sh) has its own 30s poll
# that waits for state=RUNNING, which is the authoritative readiness check.
echo ""
echo "[3/3] Verifying connector..."
sleep 3
STATUS=$(curl -s "$KC_HOST/connectors/corebanking-postgres/status" 2>/dev/null || echo "")
if echo "$STATUS" | grep -q "RUNNING"; then
  echo "  Connector status: RUNNING"
else
  echo "  Connector status: starting (parent orchestrator will poll until RUNNING)"
fi

echo ""
echo "=== Debezium CDC Ready ==="
echo "Topics created: corebanking.public.{customers,accounts,transactions,fraud_labels}"
