#!/bin/bash
# GenAI Fraud Audit Schema
#
# For demonstration purposes only.
#
# Creates audit and agent trace tables for GenAI workflows
# Called during GenAI Usecase 5 setup

set -e

PGHOST=${POSTGRES_HOST:-localhost}
PGPORT=${POSTGRES_PORT:-5432}
PGUSER=${POSTGRES_USER:-postgres}
PGPASSWORD=${POSTGRES_PASSWORD:-secret}
PGDB=${POSTGRES_DB:-demo}

echo "=== GenAI Fraud Audit Schema ==="

# Wait for PostgreSQL
until PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -c '\q' 2>/dev/null; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "[1/2] Creating audit results table..."
PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" <<'EOF'
-- AI Agent audit results
CREATE TABLE IF NOT EXISTS audit_results (
    audit_id SERIAL PRIMARY KEY,
    tx_id BIGINT,
    session_id VARCHAR(100),
    audit_type VARCHAR(50),
    findings TEXT,
    recommendations TEXT,
    risk_assessment VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_results_timestamp ON audit_results(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_results_tx ON audit_results(tx_id);
CREATE INDEX IF NOT EXISTS idx_audit_results_session ON audit_results(session_id);
EOF

echo "[2/2] Creating agent trace and chat sessions..."
PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" <<'EOF'
-- Agent execution traces for debugging and audit
CREATE TABLE IF NOT EXISTS agent_trace (
    trace_id SERIAL PRIMARY KEY,
    session_id VARCHAR(100),
    audit_id INTEGER,
    step_name VARCHAR(100),
    tool_name VARCHAR(100),
    input_data JSONB,
    output_data JSONB,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_trace_session ON agent_trace(session_id);
CREATE INDEX IF NOT EXISTS idx_agent_trace_audit ON agent_trace(audit_id);

-- Chat sessions for LangFlow/agent conversations
CREATE TABLE IF NOT EXISTS chat_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100),
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    message_count INTEGER DEFAULT 0
);

-- View: Fraud audit summary by hour and source (requires ml_fraud_predictions from ML usecase)
CREATE OR REPLACE VIEW v_fraud_audit_summary AS
SELECT
    DATE_TRUNC('hour', p.predicted_at) AS hour,
    p.prediction_source,
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN p.is_fraud_predicted THEN 1 ELSE 0 END) AS fraud_count,
    AVG(p.ttdf_milliseconds)::INT AS avg_ttdf_ms,
    MIN(p.ttdf_milliseconds) AS min_ttdf_ms,
    MAX(p.ttdf_milliseconds) AS max_ttdf_ms
FROM ml_fraud_predictions p
WHERE p.predicted_at > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', p.predicted_at), p.prediction_source
ORDER BY hour DESC, prediction_source;
EOF

echo ""
echo "=== GenAI Fraud Audit Schema Ready ==="
echo "Created: audit_results, agent_trace, chat_sessions"
echo "Views: v_fraud_audit_summary"
