#!/bin/bash
# ClickHouse OLAP Setup (CDC tables only, no ML MVs)
# Creates Kafka engine tables for CDC from PGD
#
# For demonstration purposes only.

set -e
CH_PASS="${1:-admin123}"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  ClickHouse OLAP Setup (Kafka CDC)                   ║"
echo "╚══════════════════════════════════════════════════════╝"

# Detect environment
if [ -f /.dockerenv ]; then
  CH_HTTP_HOST="clickhouse:8123"
  KAFKA_BROKER="kafka:9092"
  USE_CURL=true
else
  CH_HTTP_HOST="localhost:8125"
  KAFKA_BROKER="localhost:9094"
  USE_CURL=true
fi

ch_exec() {
  local sql="$1"
  if [ "$USE_CURL" = "true" ]; then
    echo "$sql" | curl -sf -u "default:${CH_PASS}" "http://${CH_HTTP_HOST}/" --data-binary @-
  fi
}

echo "[1/3] Creating MergeTree target tables..."

ch_exec "CREATE TABLE IF NOT EXISTS default.customers (
    customer_id  String, name String, email String, phone String,
    state String, kyc_status String, created_at DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree() ORDER BY customer_id"

ch_exec "CREATE TABLE IF NOT EXISTS default.accounts (
    account_id String, customer_id String, account_type String,
    balance Float64, initial_balance Float64, bank String,
    routing_number String, status String, created_at DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree() ORDER BY account_id"

ch_exec "CREATE TABLE IF NOT EXISTS default.transactions (
    tx_id Int64, account_id String, customer_id String, type String,
    description String, remarks String, merchant String, category String,
    amount Float64, balance_after Float64, reference_no String, channel String,
    region String, vendor String, currency String DEFAULT 'USD', status String,
    created_at DateTime64(3, 'UTC') DEFAULT now(), received_at DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(tx_id) PARTITION BY toYYYYMM(created_at) ORDER BY (created_at, tx_id)"

ch_exec "CREATE TABLE IF NOT EXISTS default.fraud_labels (
    label_id Int64, tx_id Int64, is_fraud UInt8, rules_triggered Array(String),
    fraud_reason String, ttdf_milliseconds Int32, detected_at DateTime64(3, 'UTC') DEFAULT now(),
    detection_source String DEFAULT 'rules', confidence_score Float64, rule_evaluation_time_ms Int32
) ENGINE = ReplacingMergeTree(label_id) ORDER BY (tx_id, detection_source)"

echo "✓ MergeTree tables created"

echo ""
echo "[2/3] Creating Kafka engine tables + MVs..."

ch_exec "CREATE TABLE IF NOT EXISTS default.kafka_raw_customers (
    customer_id String, name String, email String, phone String, state String, kyc_status String
) ENGINE = Kafka SETTINGS kafka_broker_list='${KAFKA_BROKER}', kafka_topic_list='corebanking.public.customers',
  kafka_group_name='ch_grp_customers', kafka_format='JSONEachRow', kafka_skip_broken_messages=1000"

ch_exec "CREATE TABLE IF NOT EXISTS default.kafka_raw_accounts (
    account_id String, customer_id String, account_type String, balance Float64,
    initial_balance Float64, bank String, routing_number String, status String
) ENGINE = Kafka SETTINGS kafka_broker_list='${KAFKA_BROKER}', kafka_topic_list='corebanking.public.accounts',
  kafka_group_name='ch_grp_accounts', kafka_format='JSONEachRow', kafka_skip_broken_messages=1000"

ch_exec "CREATE TABLE IF NOT EXISTS default.kafka_raw_transactions (
    tx_id Int64, account_id String, customer_id String, type String, description String,
    remarks String, merchant String, category String, amount Float64, balance_after Float64,
    reference_no String, channel String, region String, vendor String, currency String,
    status String, created_at String, received_at String
) ENGINE = Kafka SETTINGS kafka_broker_list='${KAFKA_BROKER}', kafka_topic_list='corebanking.public.transactions',
  kafka_group_name='ch_grp_transactions', kafka_format='JSONEachRow', kafka_skip_broken_messages=1000"

ch_exec "CREATE TABLE IF NOT EXISTS default.kafka_raw_fraud_labels (
    label_id Int64, tx_id Int64, is_fraud UInt8, rules_triggered String, fraud_reason String,
    ttdf_milliseconds Int32, detected_at String, detection_source String, confidence_score Float64,
    rule_evaluation_time_ms Int32
) ENGINE = Kafka SETTINGS kafka_broker_list='${KAFKA_BROKER}', kafka_topic_list='corebanking.public.fraud_labels',
  kafka_group_name='ch_grp_fraud_labels', kafka_format='JSONEachRow', kafka_skip_broken_messages=1000"

# MVs to pipe Kafka -> MergeTree
ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_kafka_customers TO default.customers AS
SELECT customer_id, name, email, phone, state, kyc_status, now() AS created_at FROM default.kafka_raw_customers"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_kafka_accounts TO default.accounts AS
SELECT account_id, customer_id, account_type, balance, initial_balance, bank, routing_number, status, now() AS created_at
FROM default.kafka_raw_accounts"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_kafka_transactions TO default.transactions AS
SELECT tx_id, account_id, customer_id, type, description, remarks, merchant, category, amount, balance_after,
       reference_no, channel, region, vendor, currency, status,
       parseDateTime64BestEffortOrNull(created_at, 3, 'UTC') AS created_at,
       parseDateTime64BestEffortOrNull(received_at, 3, 'UTC') AS received_at
FROM default.kafka_raw_transactions"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_kafka_fraud_labels TO default.fraud_labels AS
SELECT label_id, tx_id, is_fraud,
       splitByChar(',', replaceAll(replaceAll(rules_triggered, '{', ''), '}', '')) AS rules_triggered,
       fraud_reason, ttdf_milliseconds, parseDateTime64BestEffortOrNull(detected_at, 3, 'UTC') AS detected_at,
       detection_source, confidence_score, rule_evaluation_time_ms
FROM default.kafka_raw_fraud_labels"

echo "✓ Kafka CDC pipeline created"

echo ""
echo "[3/4] Creating analytics MVs (for UI dashboards)..."

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_hourly_volume
ENGINE = SummingMergeTree() ORDER BY hour POPULATE AS
SELECT toStartOfHour(created_at) as hour, count() as tx_count, sum(amount) as volume
FROM transactions GROUP BY hour"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_by_type
ENGINE = SummingMergeTree() ORDER BY type POPULATE AS
SELECT type, count() as tx_count, sum(amount) as volume, avg(amount) as avg_amount
FROM transactions GROUP BY type"

ch_exec "CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_fraud_rate
ENGINE = SummingMergeTree() ORDER BY hour POPULATE AS
SELECT toStartOfHour(t.created_at) as hour, count() as total_tx,
       countIf(fl.is_fraud = 1) as fraud_tx,
       (countIf(fl.is_fraud = 1) / nullIf(count(), 0)) * 100 as fraud_rate
FROM transactions t LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id
GROUP BY hour"

echo "✓ Analytics MVs created"

echo ""
echo "[4/4] Triggering initial Kafka poll..."
sleep 2
ch_exec "SELECT count() FROM default.kafka_raw_transactions" > /dev/null 2>&1 || true

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✓ ClickHouse OLAP Ready                             ║"
echo "║  Tables: customers, accounts, transactions, fraud_labels"
echo "║  CDC: Kafka -> ClickHouse streaming active           ║"
echo "╚══════════════════════════════════════════════════════╝"
