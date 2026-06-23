#!/bin/bash
# AIDB Semantic Search Setup for Fraud Audit
#
# For demonstration purposes only.
#
# Creates vector embeddings from MinIO fraud rule documents
# Called during GenAI Usecase 5 setup (after audit schema)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

echo "=== AIDB Semantic Search Setup ==="

# Step 1: Upload docs to MinIO (minio-init may have already done this, but ensure it's there)
echo "[1/3] Ensuring fraud rule docs are in MinIO..."
docker run --rm --network bfsi-network \
  -v "$STACK_DIR/docs:/docs:ro" \
  --entrypoint /bin/sh \
  minio/mc -c '
    mc alias set cbminio http://minio:9000 minioadmin minioadmin123 2>/dev/null
    mc mb --ignore-existing cbminio/fraud-rules 2>/dev/null
    count=$(mc ls cbminio/fraud-rules/ 2>/dev/null | wc -l)
    if [ "$count" -lt 10 ]; then
      echo "  Uploading documents..."
      mc cp /docs/*.txt cbminio/fraud-rules/ 2>/dev/null
      echo "  Uploaded $(ls /docs/*.txt | wc -l) documents"
    else
      echo "  Documents already present ($count files)"
    fi
  '

# Step 2: Run AIDB setup SQL (creates PGFS, volume, embeddings)
echo "[2/3] Creating AIDB pipelines and embeddings..."
docker exec bfsi-pgd psql -U postgres -d demo -f /scripts/setup/oltp/setup-minio-aidb.sql 2>&1 | grep -E "^(✓|✔|Step|═|Verification|Chunks|Embeddings|Pipeline|Test|total)" || true

# Step 3: Create drift analysis views
echo "[3/3] Creating drift analysis views..."
docker exec bfsi-pgd psql -U postgres -d demo -c "
-- View: ML predictions vs Rules alignment analysis
DROP VIEW IF EXISTS v_drift_trend CASCADE;
DROP VIEW IF EXISTS v_misaligned_transactions CASCADE;
DROP VIEW IF EXISTS v_drift_summary CASCADE;
DROP VIEW IF EXISTS v_ml_rules_alignment CASCADE;

CREATE VIEW v_ml_rules_alignment AS
SELECT
    p.tx_id,
    p.fraud_probability,
    p.is_fraud_predicted AS ml_flagged,
    p.prediction_source,
    f.is_fraud AS rules_flagged,
    f.rules_triggered,
    t.amount,
    t.region,
    t.vendor,
    t.merchant,
    t.category,
    CASE
        WHEN p.is_fraud_predicted AND f.is_fraud THEN 'ALIGNED_FRAUD'
        WHEN NOT p.is_fraud_predicted AND NOT f.is_fraud THEN 'ALIGNED_LEGIT'
        WHEN p.is_fraud_predicted AND NOT f.is_fraud THEN 'ML_FALSE_POSITIVE'
        WHEN NOT p.is_fraud_predicted AND f.is_fraud THEN 'ML_FALSE_NEGATIVE'
    END AS alignment_status,
    p.predicted_at
FROM ml_fraud_predictions p
JOIN fraud_labels f ON p.tx_id = f.tx_id AND f.detection_source = 'rules'
JOIN transactions t ON p.tx_id = t.tx_id;

CREATE VIEW v_drift_summary AS
SELECT
    prediction_source,
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN alignment_status = 'ALIGNED_FRAUD' THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN alignment_status = 'ALIGNED_LEGIT' THEN 1 ELSE 0 END) AS true_negatives,
    SUM(CASE WHEN alignment_status = 'ML_FALSE_POSITIVE' THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN alignment_status = 'ML_FALSE_NEGATIVE' THEN 1 ELSE 0 END) AS false_negatives,
    ROUND(100.0 * SUM(CASE WHEN alignment_status IN ('ALIGNED_FRAUD', 'ALIGNED_LEGIT') THEN 1 ELSE 0 END) / COUNT(*), 2) AS alignment_pct,
    ROUND(100.0 * SUM(CASE WHEN alignment_status = 'ML_FALSE_NEGATIVE' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN alignment_status IN ('ALIGNED_FRAUD', 'ML_FALSE_NEGATIVE') THEN 1 ELSE 0 END), 0), 2) AS miss_rate_pct
FROM v_ml_rules_alignment
GROUP BY prediction_source;

CREATE VIEW v_misaligned_transactions AS
SELECT tx_id, amount, region, vendor, merchant, category, fraud_probability, ml_flagged, rules_flagged, rules_triggered, alignment_status, prediction_source, predicted_at
FROM v_ml_rules_alignment
WHERE alignment_status IN ('ML_FALSE_POSITIVE', 'ML_FALSE_NEGATIVE');

CREATE VIEW v_drift_trend AS
SELECT
    DATE_TRUNC('hour', predicted_at) AS hour,
    prediction_source,
    COUNT(*) AS total,
    SUM(CASE WHEN alignment_status IN ('ML_FALSE_POSITIVE', 'ML_FALSE_NEGATIVE') THEN 1 ELSE 0 END) AS misaligned,
    ROUND(100.0 * SUM(CASE WHEN alignment_status IN ('ML_FALSE_POSITIVE', 'ML_FALSE_NEGATIVE') THEN 1 ELSE 0 END) / COUNT(*), 2) AS drift_pct
FROM v_ml_rules_alignment
WHERE predicted_at > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', predicted_at), prediction_source;
"

# Create assess_new_rule function
docker exec -i bfsi-pgd psql -U postgres -d demo << 'EOFUNC'
DROP FUNCTION IF EXISTS assess_new_rule;
CREATE FUNCTION assess_new_rule(
    p_desc TEXT,
    p_amount NUMERIC,
    p_region TEXT,
    p_vendor TEXT,
    p_category TEXT
) RETURNS TABLE (
    would_flag BIGINT,
    already_ml_flagged BIGINT,
    ml_missed BIGINT,
    retrain_recommended BOOLEAN,
    recommendation TEXT
) LANGUAGE plpgsql AS $func$
DECLARE
    v_flagged BIGINT;
    v_ml_flagged BIGINT;
    v_missed BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_flagged
    FROM transactions t
    WHERE (p_amount IS NULL OR t.amount > p_amount)
      AND (p_region IS NULL OR t.region = p_region)
      AND (p_vendor IS NULL OR LOWER(t.vendor) = LOWER(p_vendor))
      AND (p_category IS NULL OR LOWER(t.category) = LOWER(p_category));

    SELECT COUNT(*) INTO v_ml_flagged
    FROM transactions t
    JOIN ml_fraud_predictions p ON t.tx_id = p.tx_id
    WHERE p.is_fraud_predicted = TRUE
      AND (p_amount IS NULL OR t.amount > p_amount)
      AND (p_region IS NULL OR t.region = p_region)
      AND (p_vendor IS NULL OR LOWER(t.vendor) = LOWER(p_vendor))
      AND (p_category IS NULL OR LOWER(t.category) = LOWER(p_category));

    v_missed := v_flagged - v_ml_flagged;

    RETURN QUERY SELECT
        v_flagged,
        v_ml_flagged,
        v_missed,
        (v_missed::FLOAT / NULLIF(v_flagged, 0) > 0.1),
        CASE
            WHEN v_flagged = 0 THEN 'No transactions match'
            WHEN v_missed = 0 THEN 'ML catches all - no retraining needed'
            WHEN v_missed::FLOAT / v_flagged > 0.3 THEN 'HIGH: retrain now'
            WHEN v_missed::FLOAT / v_flagged > 0.1 THEN 'MEDIUM: consider retraining'
            ELSE 'LOW: monitor only'
        END::TEXT;
END;
$func$;
EOFUNC

echo ""
echo "=== AIDB Semantic Search Ready ==="
echo "Created:"
echo "  - PGFS storage location: minio_fraud_rules"
echo "  - AIDB volume: fraud_rules_volume"
echo "  - BERT embeddings: fraud_rule_embeddings"
echo "  - Function: search_fraud_rules_semantic(text, int)"
echo "  - Views: v_ml_rules_alignment, v_drift_summary, v_misaligned_transactions, v_drift_trend"
echo "  - Function: assess_new_rule(text, numeric, varchar, varchar, varchar)"
