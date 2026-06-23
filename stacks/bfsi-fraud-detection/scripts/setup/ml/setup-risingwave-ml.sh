#!/bin/bash
# For demonstration purposes only.
# RisingWave ML Setup (Lifetime MVs for fraud detection)
# Requires: OLAP setup must be completed first

set -e
RW_HOST="${RISINGWAVE_HOST:-risingwave}"
RW_PORT="${RISINGWAVE_PORT:-4566}"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  RisingWave ML MVs Setup (Fraud Detection)           ║"
echo "╚══════════════════════════════════════════════════════╝"

if command -v psql &> /dev/null; then
  RW_CMD="psql -h $RW_HOST -p $RW_PORT -U root -d dev"
else
  RW_CMD="docker exec -i bfsi-risingwave psql -h localhost -p 4566 -U root -d dev"
fi

echo "[0/1] Verifying OLAP tables exist..."
if ! $RW_CMD -c "SELECT count(*) FROM transactions" > /dev/null 2>&1; then
  echo "✗ OLAP tables not found. Run setup-risingwave-olap.sh first."
  exit 1
fi
echo "✓ OLAP tables verified"

echo ""
echo "[1/1] Creating ML lifetime MVs..."

$RW_CMD << 'EOSQL'
-- Customer lifetime stats (for z-score computation)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_lifetime AS
SELECT customer_id, COUNT(*) as txn_count, AVG(amount) as avg_amount,
       STDDEV_POP(amount) as stddev_amount
FROM transactions WHERE customer_id IS NOT NULL GROUP BY customer_id;

-- Category lifetime stats (for z-score computation)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_lifetime AS
SELECT category, COUNT(*) as txn_count, AVG(amount) as avg_amount,
       STDDEV_POP(amount) as stddev_amount
FROM transactions WHERE category IS NOT NULL GROUP BY category;
EOSQL

echo "✓ ML lifetime MVs created"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✓ RisingWave ML Ready                               ║"
echo "║  Lifetime MVs: customer, category                    ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "--- ML Materialized Views ---"
$RW_CMD -t -c "SELECT name FROM rw_catalog.rw_materialized_views WHERE name LIKE '%lifetime%' ORDER BY name;" 2>/dev/null || true
