#!/bin/bash
# For demonstration purposes only.
# ClickHouse ML Setup (Feature + Lifetime MVs for fraud detection)
# Requires: OLAP setup must be completed first

set -e
CH_PASS="${1:-admin123}"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  ClickHouse ML MVs Setup (Fraud Detection)           ║"
echo "╚══════════════════════════════════════════════════════╝"

if [ -f /.dockerenv ]; then
  CH_HTTP_HOST="clickhouse:8123"
else
  CH_HTTP_HOST="localhost:8125"
fi

ch_exec() {
  echo "$1" | curl -sf -u "default:${CH_PASS}" "http://${CH_HTTP_HOST}/" --data-binary @-
}

# Check OLAP tables exist
echo "[0/2] Verifying OLAP tables exist..."
if ! ch_exec "SELECT count() FROM default.transactions" > /dev/null 2>&1; then
  echo "✗ OLAP tables not found. Run setup-clickhouse-olap.sh first."
  exit 1
fi
echo "✓ OLAP tables verified"

echo ""
echo "[1/2] Creating ML feature MVs (velocity, patterns)..."

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_customer_velocity_5min_ch
ENGINE = AggregatingMergeTree() ORDER BY (customer_id, window_start) POPULATE AS
SELECT customer_id, toStartOfMinute(received_at) as window_start,
       count() as transaction_count, sum(amount) as total_spent,
       avg(amount) as avg_transaction, max(amount) as max_transaction,
       uniq(merchant) as unique_merchants, uniq(type) as unique_tx_types
FROM transactions GROUP BY customer_id, window_start"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_account_velocity_5min_ch
ENGINE = AggregatingMergeTree() ORDER BY (account_id, window_start) POPULATE AS
SELECT account_id, toStartOfMinute(received_at) as window_start,
       count() as transaction_count, sum(amount) as total_spent,
       avg(amount) as avg_transaction, max(amount) as max_transaction,
       min(balance_after) as min_balance, max(balance_after) as max_balance,
       uniq(customer_id) as unique_customers
FROM transactions GROUP BY account_id, window_start"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_merchant_fraud_1min_ch
ENGINE = AggregatingMergeTree() ORDER BY (merchant, window_start) POPULATE AS
SELECT t.merchant, t.category, toStartOfMinute(t.received_at) as window_start,
       count() as total_transactions, countIf(fl.is_fraud = 1) as fraud_count,
       (countIf(fl.is_fraud = 1) / nullIf(count(), 0)) * 100 as fraud_percentage,
       sum(t.amount) as total_volume, avg(t.amount) as avg_amount
FROM transactions t LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id
WHERE t.merchant != '' GROUP BY t.merchant, t.category, window_start"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_category_patterns_1h_ch
ENGINE = AggregatingMergeTree() ORDER BY (category, hour) POPULATE AS
SELECT t.category, toStartOfHour(t.received_at) as hour,
       count() as tx_count, sum(t.amount) as total_amount,
       avg(t.amount) as avg_amount, stddevPop(t.amount) as stddev_amount,
       min(t.amount) as min_amount, max(t.amount) as max_amount,
       countIf(fl.is_fraud = 1) as fraud_count
FROM transactions t LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id
WHERE t.category != '' GROUP BY t.category, hour"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_channel_fraud_1h_ch
ENGINE = AggregatingMergeTree() ORDER BY (channel, hour) POPULATE AS
SELECT t.channel, toStartOfHour(t.received_at) as hour,
       count() as total_transactions, countIf(fl.is_fraud = 1) as fraud_count,
       (countIf(fl.is_fraud = 1) / nullIf(count(), 0)) * 100 as fraud_percentage,
       avg(t.amount) as avg_transaction_amount, max(t.amount) as max_transaction_amount
FROM transactions t LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id
WHERE t.channel != '' GROUP BY t.channel, hour"

echo "✓ ML feature MVs created"

echo ""
echo "[2/2] Creating ML lifetime MVs (for z-score computation)..."

# Note: Using AggregatingMergeTree with State/Merge combinators for correct incremental aggregation
# SummingMergeTree cannot correctly aggregate avg/stddev across multiple rows
ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_customer_lifetime_ch
ENGINE = AggregatingMergeTree() ORDER BY customer_id POPULATE AS
SELECT customer_id,
       countState() as txn_count_state,
       avgState(amount) as avg_amount_state,
       stddevPopState(amount) as stddev_amount_state
FROM transactions
GROUP BY customer_id"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_category_lifetime_ch
ENGINE = AggregatingMergeTree() ORDER BY category POPULATE AS
SELECT category,
       countState() as txn_count_state,
       avgState(amount) as avg_amount_state,
       stddevPopState(amount) as stddev_amount_state
FROM transactions
WHERE category != '' GROUP BY category"

echo "✓ ML lifetime MVs created (AggregatingMergeTree)"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✓ ClickHouse ML Ready                               ║"
echo "║  Feature MVs: velocity, patterns, fraud rates        ║"
echo "║  Lifetime MVs: customer, category                    ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "--- ML Materialized Views ---"
ch_exec "SELECT name FROM system.tables WHERE database = 'default' AND name LIKE 'mv_%' ORDER BY name" 2>/dev/null || true
