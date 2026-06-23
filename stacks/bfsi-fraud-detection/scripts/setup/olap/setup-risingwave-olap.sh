#!/bin/bash
# RisingWave OLAP Setup (CDC tables only, no ML MVs)
# For demonstration purposes only.

set -e
KAFKA_BROKER="${1:-kafka:9092}"
RW_HOST="${RISINGWAVE_HOST:-risingwave}"
RW_PORT="${RISINGWAVE_PORT:-4566}"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  RisingWave OLAP Setup (Kafka CDC)                   ║"
echo "╚══════════════════════════════════════════════════════╝"

# Use psql if in container with it, else use docker exec
if command -v psql &> /dev/null; then
  RW_CMD="psql -h $RW_HOST -p $RW_PORT -U root -d dev"
else
  RW_CMD="docker exec -i bfsi-risingwave psql -h localhost -p 4566 -U root -d dev"
fi

echo "[1/3] Creating Kafka sources..."

$RW_CMD << 'EOSQL'
-- Customers source
CREATE SOURCE IF NOT EXISTS kafka_customers (
    customer_id VARCHAR, name VARCHAR, email VARCHAR, phone VARCHAR,
    state VARCHAR, kyc_status VARCHAR, created_at TIMESTAMPTZ
) WITH (
    connector = 'kafka', topic = 'corebanking.public.customers',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Accounts source
CREATE SOURCE IF NOT EXISTS kafka_accounts (
    account_id VARCHAR, customer_id VARCHAR, account_type VARCHAR,
    balance DOUBLE PRECISION, initial_balance DOUBLE PRECISION,
    bank VARCHAR, routing_number VARCHAR, status VARCHAR, created_at TIMESTAMPTZ
) WITH (
    connector = 'kafka', topic = 'corebanking.public.accounts',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Transactions source
CREATE SOURCE IF NOT EXISTS kafka_transactions (
    tx_id BIGINT, account_id VARCHAR, customer_id VARCHAR, type VARCHAR,
    description VARCHAR, remarks VARCHAR, merchant VARCHAR, category VARCHAR,
    amount DOUBLE PRECISION, balance_after DOUBLE PRECISION, reference_no VARCHAR,
    channel VARCHAR, region VARCHAR, vendor VARCHAR, currency VARCHAR,
    status VARCHAR, created_at TIMESTAMPTZ, received_at TIMESTAMPTZ
) WITH (
    connector = 'kafka', topic = 'corebanking.public.transactions',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Fraud labels source
CREATE SOURCE IF NOT EXISTS kafka_fraud_labels (
    label_id BIGINT, tx_id BIGINT, is_fraud BOOLEAN, rules_triggered VARCHAR,
    fraud_reason VARCHAR, ttdf_milliseconds INT, detected_at TIMESTAMPTZ,
    detection_source VARCHAR, confidence_score DOUBLE PRECISION,
    rule_evaluation_time_ms INT
) WITH (
    connector = 'kafka', topic = 'corebanking.public.fraud_labels',
    properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
EOSQL

echo "✓ Kafka sources created"

echo ""
echo "[2/3] Creating materialized tables..."

$RW_CMD << 'EOSQL'
-- Customers table
CREATE MATERIALIZED VIEW IF NOT EXISTS customers AS
SELECT customer_id, name, email, phone, state, kyc_status, created_at
FROM kafka_customers;

-- Accounts table
CREATE MATERIALIZED VIEW IF NOT EXISTS accounts AS
SELECT account_id, customer_id, account_type, balance, initial_balance,
       bank, routing_number, status, created_at
FROM kafka_accounts;

-- Transactions table (main CDC target)
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions AS
SELECT tx_id, account_id, customer_id, type, description, remarks, merchant,
       category, amount, balance_after, reference_no, channel, region, vendor,
       currency, status, created_at, received_at
FROM kafka_transactions;

-- Fraud labels table
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_labels AS
SELECT label_id, tx_id, is_fraud, rules_triggered, fraud_reason, ttdf_milliseconds,
       detected_at, detection_source, confidence_score, rule_evaluation_time_ms
FROM kafka_fraud_labels;
EOSQL

echo "✓ Materialized tables created"

echo ""
echo "[3/3] Creating OLAP analytics MVs..."

$RW_CMD << 'EOSQL'
-- Real-time analytics MVs (OLAP only, no ML)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tx_per_minute AS
SELECT date_trunc('minute', created_at) AS minute, COUNT(*) AS tx_count,
       SUM(amount) AS total_amount, AVG(amount) AS avg_amount
FROM transactions GROUP BY date_trunc('minute', created_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_merchants AS
SELECT merchant, COUNT(*) AS tx_count, SUM(amount) AS total_volume
FROM transactions WHERE merchant IS NOT NULL GROUP BY merchant;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_type_breakdown AS
SELECT type, COUNT(*) AS tx_count, SUM(amount) AS total_amount
FROM transactions GROUP BY type;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_velocity_5min AS
SELECT customer_id, date_trunc('minute', created_at) AS window_start,
       COUNT(*) AS tx_count, SUM(amount) AS total_amount
FROM transactions GROUP BY customer_id, date_trunc('minute', created_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_account_velocity_5min AS
SELECT account_id, date_trunc('minute', created_at) AS window_start,
       COUNT(*) AS tx_count, SUM(amount) AS total_amount
FROM transactions GROUP BY account_id, date_trunc('minute', created_at);
EOSQL

echo "✓ OLAP analytics MVs created"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✓ RisingWave OLAP Ready                             ║"
echo "║  Tables: customers, accounts, transactions, fraud_labels"
echo "║  Analytics: tx_per_minute, top_merchants, velocity   ║"
echo "╚══════════════════════════════════════════════════════╝"
