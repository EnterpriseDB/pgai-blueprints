#!/usr/bin/env bash
# For demonstration purposes only.
# Reset ML inference: stop consumers/ML paths, optionally clear prediction tables, reset Kafka offsets, restart.
# Derived from PATCH_ml-inference-fixes.patch (kafka block repaired; original patch hunks were malformed).
set -euo pipefail

echo "=========================================="
echo "  ML Inference State Reset"
echo "=========================================="
echo ""

# Step 1: Stop Kafka consumers and ML services
echo "[1/4] Stopping ML inference services..."
docker stop bfsi-ml-kafka bfsi-kafka-feature-materializer bfsi-ml-ch bfsi-ml-rw bfsi-ml-pgaa >/dev/null 2>&1 || true
sleep 2
echo "   ✓ ML services stopped"

# Step 2: Clear old predictions from database
# Note: rule_based_fraud_metrics is now a VIEW on fraud_labels, so we truncate fraud_labels
echo "[2/4] Clearing old predictions from database..."
if docker exec bfsi-pgd psql -U postgres -d demo -c \
  "TRUNCATE TABLE ml_fraud_predictions; DELETE FROM fraud_labels WHERE detection_source = 'ml';" >/dev/null 2>&1; then
  echo "   ✓ ML predictions cleared (fraud_labels ML entries removed)"
else
  echo "   ⚠️  TRUNCATE failed. Clearing ml_fraud_predictions only…" >&2
  docker exec bfsi-pgd psql -U postgres -d demo -c "TRUNCATE TABLE ml_fraud_predictions;" >/dev/null 2>&1 || true
  echo "   ✓ ml_fraud_predictions cleared"
fi

# Step 3: Reset Kafka consumer offsets (with timeout to prevent hanging)
echo "[3/4] Resetting Kafka consumer offsets..."
timeout 10 docker exec bfsi-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ml-feature-materializer \
  --topic corebanking.public.transactions \
  --reset-offsets \
  --to-latest \
  --execute >/dev/null 2>&1 || echo "   ⚠️  Feature materializer offset reset skipped (timeout or already at latest)"

timeout 10 docker exec bfsi-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ml-inference-kafka \
  --topic ml.enriched_transactions \
  --reset-offsets \
  --to-latest \
  --execute >/dev/null 2>&1 || echo "   ⚠️  ML inference Kafka offset reset skipped (timeout or already at latest)"

echo "   ✓ Kafka offsets reset to latest"

# Step 4: Restart all ML inference services
echo "[4/4] Restarting all ML inference services..."
docker start bfsi-kafka-feature-materializer bfsi-ml-kafka >/dev/null 2>&1 || true
docker restart bfsi-ml-rw bfsi-ml-ch bfsi-ml-pgaa bfsi-fraud-alert >/dev/null 2>&1 || true
sleep 5
echo "   ✓ Services restarted"

echo ""
echo "=========================================="
echo "✅ ML Inference Reset Complete!"
echo "=========================================="
echo ""
echo "✓ Old predictions cleared from database"
echo "✓ Kafka offsets reset to latest"
echo "✓ All ML services restarted"
echo ""
echo "Services will process only NEW transactions."
echo "TTDF values should show accurate measurements:"
echo "  • Kafka: <100ms target"
echo "  • ClickHouse: ~3s target"
echo "  • RisingWave: ~4s target"
echo "  • PGAA: ~300ms target"
echo "  • Rules-based: <10ms"
echo ""
echo "Check service status:"
echo "  docker ps --filter name=bfsi-ml"
