-- ============================================================================
-- BFSI Fraud Detection — Consolidated OLTP Schema
-- ============================================================================
-- Single source of truth for OLTP setup. Applied by pipeline step "Start Service".
--
-- For demonstration purposes only.
--  
-- Idempotent: safe to re-run.
--
-- Contents:
--   1. OLTP tables: customers, accounts, transactions, fraud_labels
--   2. fraud_rules table + 9 default rules
--   3. check_fraud_rules() trigger function + trigger
--   4. Replication: roles, ALTER USER WITH REPLICATION, publication
-- ============================================================================

-- Suppress per-statement NOTICEs from DROP IF EXISTS on fresh deploys.
SET client_min_messages = WARNING;

-- ----------------------------------------------------------------------------
-- 1. CLEAN SLATE (drop in dependency order)
-- ----------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_check_fraud ON transactions;
DROP TRIGGER IF EXISTS trg_fraud_labels ON transactions;
DROP TRIGGER IF EXISTS trg_fraud_detection ON transactions;
DROP FUNCTION IF EXISTS check_fraud_rules() CASCADE;
DROP TABLE IF EXISTS fraud_labels CASCADE;
DROP TABLE IF EXISTS fraud_rules CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- ----------------------------------------------------------------------------
-- 2. OLTP TABLES
-- ----------------------------------------------------------------------------
-- Note: DOUBLE PRECISION (not NUMERIC) for PGAA/Iceberg compatibility — PGAA 1.9.0
-- corrupts NUMERIC(p,s) values when read via Seafowl.

CREATE TABLE customers (
    customer_id   VARCHAR(20) PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    email         VARCHAR(150),
    phone         VARCHAR(20),
    state         VARCHAR(30),
    kyc_status    VARCHAR(20) DEFAULT 'PENDING',
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE accounts (
    account_id       VARCHAR(20) PRIMARY KEY,
    customer_id      VARCHAR(20) REFERENCES customers(customer_id),
    account_type     VARCHAR(30) DEFAULT 'CHECKING',
    balance          DOUBLE PRECISION DEFAULT 0,
    initial_balance  DOUBLE PRECISION DEFAULT 0,
    bank             VARCHAR(50),
    routing_number   VARCHAR(20),
    status           VARCHAR(20) DEFAULT 'ACTIVE',
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE transactions (
    tx_id          BIGSERIAL PRIMARY KEY,
    account_id     VARCHAR(20) REFERENCES accounts(account_id),
    customer_id    VARCHAR(20),
    type           VARCHAR(20),
    description    TEXT,
    remarks        TEXT,
    merchant       VARCHAR(100),
    category       VARCHAR(50),
    amount         DOUBLE PRECISION,
    balance_after  DOUBLE PRECISION,
    reference_no   VARCHAR(30),
    channel        VARCHAR(30),
    region         VARCHAR(10),
    vendor         VARCHAR(20),
    currency       VARCHAR(3) DEFAULT 'USD',
    status         VARCHAR(20) DEFAULT 'COMPLETED',
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    received_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fraud_labels (
    label_id                 BIGSERIAL PRIMARY KEY,
    tx_id                    BIGINT NOT NULL,
    is_fraud                 BOOLEAN NOT NULL,
    rules_triggered          TEXT[],
    fraud_reason             TEXT,
    ttdf_milliseconds        INTEGER,
    detected_at              TIMESTAMP DEFAULT NOW(),
    detection_source         VARCHAR(20) DEFAULT 'rules',
    confidence_score         DOUBLE PRECISION,
    rule_evaluation_time_ms  INTEGER,
    UNIQUE(tx_id, detection_source)
);

-- Indexes
CREATE INDEX idx_tx_account               ON transactions(account_id);
CREATE INDEX idx_tx_created               ON transactions(created_at DESC);
CREATE INDEX idx_tx_region_vendor         ON transactions(region, vendor);
CREATE INDEX idx_transactions_received_at ON transactions(received_at);
CREATE INDEX idx_fraud_labels_tx          ON fraud_labels(tx_id);

-- ----------------------------------------------------------------------------
-- 3. FRAUD RULES TABLE + DEFAULT RULES
-- ----------------------------------------------------------------------------
CREATE TABLE fraud_rules (
    rule_id        SERIAL PRIMARY KEY,
    rule_name      VARCHAR(100) NOT NULL,
    description    TEXT,
    region         VARCHAR(10),
    vendor         VARCHAR(20),
    category       VARCHAR(50),
    condition_sql  TEXT NOT NULL,
    severity       VARCHAR(20) DEFAULT 'MEDIUM',
    action         VARCHAR(20) DEFAULT 'FLAG',
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_fraud_rules_region   ON fraud_rules(region);
CREATE INDEX idx_fraud_rules_category ON fraud_rules(category);
CREATE INDEX idx_fraud_rules_active   ON fraud_rules(is_active) WHERE is_active = TRUE;

-- Default rules — thresholds derived from MinIO documents (US-STRIPE-001.txt etc.)
-- Region+vendor specific limits target ~4% fraud rate when paired with synthetic data.
INSERT INTO fraud_rules (rule_name, description, region, vendor, condition_sql, severity, action) VALUES
  ('US Stripe Limit',     'US-STRIPE-001: US Stripe transactions over $5,000',          'US', 'stripe', 'amount > 5000',           'HIGH',   'REVIEW'),
  ('UK Stripe Limit',     'UK-STRIPE-001: UK Stripe transactions over $4,350 (£3,500)', 'UK', 'stripe', 'amount > 4350',           'HIGH',   'REVIEW'),
  ('DE Stripe Limit',     'DE-STRIPE-001: Germany Stripe over $4,300 (€4,000)',         'DE', 'stripe', 'amount > 4300',           'HIGH',   'REVIEW'),
  ('FR Stripe Limit',     'FR-STRIPE-001: France Stripe over $4,350 (€4,000)',          'FR', 'stripe', 'amount > 4350',           'HIGH',   'REVIEW'),
  ('CA Stripe Limit',     'CA-STRIPE-001: Canada Stripe over $3,300 (CAD $4,500)',      'CA', 'stripe', 'amount > 3300',           'HIGH',   'REVIEW'),
  ('US Square Risk',      'US-SQUARE-001: US Square transactions over $5,000',          'US', 'square', 'amount > 5000',           'HIGH',   'REVIEW'),
  ('PayPal Velocity',     'US-PAYPAL-001: PayPal velocity check (3+ txns in 5 min)',    NULL, 'paypal', 'velocity_5min > 3',       'MEDIUM', 'FLAG'),
  ('Adyen Pattern',       'Adyen repeated same amount pattern',                          NULL, 'adyen',  'same_amount_count > 2',   'MEDIUM', 'FLAG'),
  ('Suspicious Category', 'Gambling or crypto transactions',                             NULL, NULL,     'category IN (''gambling'', ''crypto'')', 'HIGH', 'BLOCK');

-- ----------------------------------------------------------------------------
-- 4. FRAUD DETECTION TRIGGER FUNCTION
-- ----------------------------------------------------------------------------
-- Evaluates active rules from fraud_rules on each new transaction. Inserts
-- result into fraud_labels (is_fraud flag + which rules fired).
CREATE OR REPLACE FUNCTION check_fraud_rules()
RETURNS TRIGGER AS $$
DECLARE
    rule              RECORD;
    rules_triggered   TEXT[] := ARRAY[]::TEXT[];
    is_fraud          BOOLEAN := FALSE;
    start_time        TIMESTAMP := clock_timestamp();
    tx_hour           INTEGER;
    is_round_amount   BOOLEAN;
    velocity_5min     INTEGER;
    same_amount_count INTEGER;
    account_age_days  INTEGER;
    account_region    TEXT;
BEGIN
    tx_hour := EXTRACT(HOUR FROM COALESCE(NEW.created_at, NOW()));
    is_round_amount := NEW.amount > 1000 AND NEW.amount::numeric = ROUND(NEW.amount::numeric, -2);

    SELECT COUNT(*) INTO velocity_5min
    FROM transactions
    WHERE customer_id = NEW.customer_id
      AND tx_id != NEW.tx_id
      AND created_at >= NOW() - INTERVAL '5 minutes';

    SELECT COUNT(*) INTO same_amount_count
    FROM transactions
    WHERE customer_id = NEW.customer_id
      AND tx_id != NEW.tx_id
      AND amount = NEW.amount
      AND created_at >= NOW() - INTERVAL '1 hour';

    SELECT EXTRACT(DAY FROM NOW() - a.created_at)::INTEGER INTO account_age_days
    FROM accounts a WHERE a.account_id = NEW.account_id;
    account_age_days := COALESCE(account_age_days, 365);

    SELECT
        CASE
            WHEN c.state IN ('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI') THEN 'US'
            WHEN c.state IN ('ON','BC','QC','AB') THEN 'CA'
            WHEN c.state IN ('England','Scotland','Wales') THEN 'UK'
            ELSE 'US'
        END INTO account_region
    FROM customers c WHERE c.customer_id = NEW.customer_id;
    account_region := COALESCE(account_region, 'US');

    FOR rule IN SELECT * FROM fraud_rules WHERE is_active = TRUE LOOP
        IF rule.region IS NOT NULL AND rule.region != NEW.region THEN
            CONTINUE;
        END IF;
        IF rule.vendor IS NOT NULL AND LOWER(rule.vendor) != LOWER(NEW.vendor) THEN
            CONTINUE;
        END IF;

        IF rule.condition_sql ~ '^amount > [0-9]+$' THEN
            IF NEW.amount > CAST(SUBSTRING(rule.condition_sql FROM 'amount > ([0-9]+)') AS NUMERIC) THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql ~ 'velocity_5min > [0-9]+' THEN
            IF velocity_5min > CAST(SUBSTRING(rule.condition_sql FROM 'velocity_5min > ([0-9]+)') AS INTEGER) THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql ~ 'same_amount_count > [0-9]+' THEN
            IF same_amount_count > CAST(SUBSTRING(rule.condition_sql FROM 'same_amount_count > ([0-9]+)') AS INTEGER) THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql LIKE 'category IN%' THEN
            IF LOWER(NEW.category) IN ('gambling','crypto','crypto exchange') THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql LIKE '%account_age_days%' THEN
            IF account_age_days < 7 AND NEW.amount > 3000 THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql LIKE '%hour BETWEEN%' THEN
            IF tx_hour BETWEEN 2 AND 5 THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql LIKE '%ROUND(amount%' THEN
            IF is_round_amount THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        ELSIF rule.condition_sql LIKE '%tx_region != account_region%' THEN
            IF NEW.region IS NOT NULL AND NEW.region != account_region THEN
                rules_triggered := array_append(rules_triggered, rule.rule_name);
                is_fraud := TRUE;
            END IF;
        END IF;
    END LOOP;

    IF is_fraud THEN
        INSERT INTO fraud_labels (tx_id, is_fraud, rules_triggered, fraud_reason, ttdf_milliseconds, detection_source)
        VALUES (
            NEW.tx_id, TRUE, rules_triggered, array_to_string(rules_triggered, ', '),
            EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_time))::INTEGER, 'rules'
        )
        ON CONFLICT (tx_id, detection_source) DO UPDATE SET
            is_fraud         = TRUE,
            rules_triggered  = EXCLUDED.rules_triggered,
            fraud_reason     = EXCLUDED.fraud_reason,
            ttdf_milliseconds = EXCLUDED.ttdf_milliseconds,
            detected_at      = NOW();
    ELSE
        INSERT INTO fraud_labels (tx_id, is_fraud, rules_triggered, ttdf_milliseconds, detection_source)
        VALUES (
            NEW.tx_id, FALSE, ARRAY[]::TEXT[],
            EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_time))::INTEGER, 'rules'
        )
        ON CONFLICT (tx_id, detection_source) DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_fraud
    AFTER INSERT ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION check_fraud_rules();

-- ----------------------------------------------------------------------------
-- 5. REPLICATION SETUP (was previously in Bank App's ensurePostgresPrereqs)
-- ----------------------------------------------------------------------------
-- Roles required by Debezium/RisingWave/ClickHouse CDC consumers.
DO $$ BEGIN
    CREATE ROLE rds_superuser;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE ROLE rds_replication;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

GRANT rds_superuser   TO postgres;
GRANT rds_replication TO postgres;
ALTER USER postgres WITH REPLICATION;

-- Recreate the publication consumed by Debezium's connector and ClickHouse
-- MaterializedPostgreSQL engine. DROP+CREATE is idempotent and safe — connectors
-- recreate their slots on next attach.
DROP PUBLICATION IF EXISTS rw_pub_corebanking;
CREATE PUBLICATION rw_pub_corebanking FOR TABLE customers, accounts, transactions, fraud_labels;

-- Drop any stale replication slots from previous runs. On a re-run the slot
-- may be actively held by Debezium / RisingWave — terminate the consumer
-- session first so the drop can proceed. Consumers reconnect and re-create
-- the slot on their own.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT slot_name, active_pid
          FROM pg_replication_slots
         WHERE slot_name IN ('rw_slot_comparison', 'kafka_slot_corebanking')
    LOOP
        IF r.active_pid IS NOT NULL THEN
            PERFORM pg_terminate_backend(r.active_pid);
            PERFORM pg_sleep(0.3);
        END IF;
        PERFORM pg_drop_replication_slot(r.slot_name);
    END LOOP;
END $$;

-- ----------------------------------------------------------------------------
-- 6. ML PREDICTIONS PLACEHOLDER (populated by Usecase 3)
-- ----------------------------------------------------------------------------
-- Created empty here so the OLAP dashboard's "Fraud Detection" tab can render
-- without erroring before Usecase 3 runs. Each query on that tab has a
-- "Run Usecase 3" placeholder via WHERE NOT EXISTS — but that fallback only
-- fires when the table exists (and has zero rows). Without this DDL, Metabase
-- gets a "relation does not exist" error and the entire tab shows blank cards.
CREATE TABLE IF NOT EXISTS ml_fraud_predictions (
    prediction_id     BIGSERIAL PRIMARY KEY,
    tx_id             BIGINT NOT NULL,
    fraud_probability DECIMAL(5,4),
    is_fraud_predicted BOOLEAN,
    prediction_source VARCHAR(20),
    predicted_at      TIMESTAMP DEFAULT NOW(),
    ttdf_milliseconds INTEGER,
    feature_vector    JSONB,
    region            VARCHAR(10),
    vendor            VARCHAR(20),
    currency          VARCHAR(10),
    UNIQUE(tx_id, prediction_source)
);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_tx ON ml_fraud_predictions(tx_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_source ON ml_fraud_predictions(prediction_source);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_time ON ml_fraud_predictions(predicted_at DESC);

-- ml_fraud_alerts + ml_model_metadata + rule_based_fraud_metrics: queried by
-- Bank App's Fraud Detection tab. Without these the /api/ml/* endpoints 500.
CREATE TABLE IF NOT EXISTS ml_fraud_alerts (
    alert_id           BIGSERIAL PRIMARY KEY,
    tx_id              BIGINT NOT NULL,
    fraud_probability  DOUBLE PRECISION NOT NULL,
    prediction_source  VARCHAR(20) NOT NULL,
    alert_severity     VARCHAR(20) DEFAULT 'high',
    alert_sent_at      TIMESTAMP DEFAULT NOW(),
    alert_details      JSONB,
    resolution_status  VARCHAR(20) DEFAULT 'pending',
    resolution_notes   TEXT,
    resolved_at        TIMESTAMP,
    resolved_by        VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_ml_alert_tx     ON ml_fraud_alerts(tx_id);
CREATE INDEX IF NOT EXISTS idx_ml_alert_time   ON ml_fraud_alerts(alert_sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_ml_alert_status ON ml_fraud_alerts(resolution_status);

CREATE TABLE IF NOT EXISTS ml_model_metadata (
    model_id            SERIAL PRIMARY KEY,
    model_name          VARCHAR(100) NOT NULL,
    model_version       VARCHAR(50)  NOT NULL,
    model_type          VARCHAR(50)  DEFAULT 'xgboost',
    training_date       TIMESTAMP    DEFAULT NOW(),
    training_records    INT,
    training_accuracy   DOUBLE PRECISION,
    validation_accuracy DOUBLE PRECISION,
    feature_columns     JSONB,
    hyperparameters     JSONB,
    model_path          VARCHAR(255),
    is_active           BOOLEAN      DEFAULT FALSE,
    deployed_at         TIMESTAMP,
    notes               TEXT
);

-- View over fraud_labels for code that still reads rule_based_fraud_metrics.
CREATE OR REPLACE VIEW rule_based_fraud_metrics AS
SELECT
    label_id AS metric_id,
    tx_id,
    is_fraud,
    ttdf_milliseconds,
    rules_triggered,
    fraud_reason,
    detected_at,
    rule_evaluation_time_ms
FROM fraud_labels
WHERE detection_source = 'rules';

-- ----------------------------------------------------------------------------
-- DONE
-- ----------------------------------------------------------------------------
SELECT 'OLTP schema ready: 4 tables, fraud_rules (' ||
       (SELECT COUNT(*) FROM fraud_rules)::text ||
       ' rules), trigger, publication rw_pub_corebanking.' AS status;
