#!/usr/bin/env python3
"""
ML Fraud Detection - PGAA with Incremental Delta Aggregation
=============================================================
For demonstration purposes only.

OLTP-isolated ML inference using incremental delta updates.

Architecture:
  1. Persistent aggregate tables with PRIMARY KEY
  2. Poll new transactions from Iceberg (delta only)
  3. INSERT...ON CONFLICT to incrementally update aggregates (O(delta))
  4. Join with aggregate tables for features (O(1) lookups)
  5. ML inference -> Save predictions

Key Benefits:
  - 100% OLTP isolation (all queries via Iceberg/Seafowl)
  - O(delta) refresh instead of O(n) full table scan
  - Scales indefinitely - refresh cost stays constant
  - No staleness - aggregates updated on every batch

Performance:
  - Full CTAS @ 1M rows: ~10-30s refresh
  - Incremental delta: <100ms regardless of table size
"""

import os
import time
import json
import logging
import math
from datetime import datetime
from decimal import Decimal
import psycopg2
import psycopg2.extras
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PG_HOST = os.getenv('POSTGRES_HOST', 'pgd')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
PG_DB = os.getenv('POSTGRES_DB', 'demo')

MODEL_PATH = os.getenv('MODEL_PATH_PGAA', '/models/fraud_model_pgaa.pkl')
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', 0.7))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))


class PGAACTASInferenceService:
    """PGAA ML inference with incremental delta aggregation - 100% OLTP isolated."""

    def __init__(self):
        logger.info("Starting PGAA Incremental Delta Inference Service...")
        logger.info("Mode: Incremental delta aggregation (O(delta) updates)")

        self._connect_database()
        self._init_aggregate_tables()

        self.model = self._load_model()
        self.last_processed_tx_id = self._get_current_max_tx_id()
        self.last_agg_tx_id = self._get_last_aggregated_tx_id()

        logger.info(f"Starting from tx_id > {self.last_processed_tx_id}")
        logger.info(f"Aggregates current to tx_id = {self.last_agg_tx_id}")
        logger.info("PGAA Incremental Service ready - 100% OLTP isolated")

    def _connect_database(self):
        """Create connections for analytics queries."""
        max_retries = 30
        retry_delay = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to PGD at {PG_HOST}:{PG_PORT} (attempt {attempt}/{max_retries})")

                # Connection for OLTP writes (predictions only)
                self.oltp_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, user=PG_USER,
                    password=PG_PASSWORD, dbname=PG_DB
                )
                self.oltp_conn.autocommit = True

                # Connection for analytics queries (Iceberg via Seafowl)
                self.analytics_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, user=PG_USER,
                    password=PG_PASSWORD, dbname=PG_DB
                )
                self.analytics_conn.autocommit = True

                # Route all analytics queries to Seafowl/Iceberg
                with self.analytics_conn.cursor() as cur:
                    cur.execute("SET bdr.prefer_analytics_engine = TRUE")

                logger.info("PGD connected (analytics routed to Iceberg)")
                break

            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"Connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"PGD failed after {max_retries} attempts")
                    raise

    def _init_aggregate_tables(self):
        """Create persistent aggregate tables with PRIMARY KEY for incremental updates."""
        logger.info("Initializing incremental aggregate tables...")

        try:
            with self.analytics_conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS delta_customer_agg (
                        customer_id TEXT PRIMARY KEY,
                        txn_count BIGINT DEFAULT 0,
                        sum_amount DOUBLE PRECISION DEFAULT 0,
                        sum_sq_amount DOUBLE PRECISION DEFAULT 0,
                        max_tx_id BIGINT DEFAULT 0
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS delta_category_agg (
                        category TEXT PRIMARY KEY,
                        txn_count BIGINT DEFAULT 0,
                        sum_amount DOUBLE PRECISION DEFAULT 0,
                        sum_sq_amount DOUBLE PRECISION DEFAULT 0,
                        max_tx_id BIGINT DEFAULT 0
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS delta_agg_watermark (
                        id INT PRIMARY KEY DEFAULT 1,
                        last_tx_id BIGINT DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    INSERT INTO delta_agg_watermark (id, last_tx_id)
                    VALUES (1, 0)
                    ON CONFLICT (id) DO NOTHING
                """)

                logger.info("Incremental aggregate tables ready")
        except Exception as e:
            logger.warning(f"Could not create aggregate tables: {e}")

    def _load_model(self):
        """Load ML model from MLflow Registry or fallback to pkl file."""
        # Try MLflow Registry first
        mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
        model_name = os.getenv('MLFLOW_MODEL_NAME', 'fraud-detection-model')
        model_stage = os.getenv('MLFLOW_MODEL_STAGE', 'Production')

        try:
            import mlflow
            mlflow.set_tracking_uri(mlflow_uri)

            model_uri = f"models:/{model_name}/{model_stage}"
            logger.info(f"Loading model from MLflow Registry: {model_uri}")

            model = mlflow.xgboost.load_model(model_uri)
            self.model_source = "mlflow_registry"
            self.model_version = model_stage

            # Load training metrics for drift detection
            self._load_training_metrics()

            logger.info(f"Model loaded from MLflow Registry: {model_name}/{model_stage}")
            return model

        except Exception as e:
            logger.warning(f"MLflow Registry unavailable ({e}), falling back to pkl files")
            self.model_source = "pkl_file"
            self.model_version = "local"

        # Fallback to pkl files
        import joblib

        model_paths = [
            MODEL_PATH,
            '/models/fraud_model_clickhouse.pkl',
            '/models/fraud_model_risingwave.pkl',
            '/models/fraud_model_kafka.pkl',
            '/models/fraud_model.pkl'
        ]

        while True:
            for path in model_paths:
                try:
                    if os.path.exists(path):
                        model = joblib.load(path)
                        logger.info(f"Model loaded from {path}")
                        self._load_training_metrics()
                        return model
                except Exception as e:
                    logger.warning(f"Failed to load model from {path}: {e}")

            logger.info("Waiting for model file...")
            time.sleep(10)

    def _load_training_metrics(self):
        """Load training metrics for drift detection."""
        self.training_metrics = {}
        config_path = "/models/inference_config.json"

        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                self.training_metrics = config.get("training_metrics", {})
                logger.info(f"Loaded training metrics for drift detection")
            except Exception as e:
                logger.warning(f"Could not load training metrics: {e}")

    def _get_current_max_tx_id(self) -> int:
        """Get current max transaction ID from Iceberg."""
        try:
            with self.analytics_conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(tx_id), 0) FROM transactions")
                return cur.fetchone()[0]
        except Exception as e:
            logger.warning(f"Failed to get max tx_id: {e}")
            return 0

    def _get_last_aggregated_tx_id(self) -> int:
        """Get watermark - last tx_id included in aggregates."""
        try:
            with self.analytics_conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(last_tx_id, 0) FROM delta_agg_watermark WHERE id = 1"
                )
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get watermark: {e}")
            return 0

    def _update_watermark(self, tx_id: int):
        """Update watermark after processing delta."""
        try:
            with self.analytics_conn.cursor() as cur:
                cur.execute("""
                    UPDATE delta_agg_watermark
                    SET last_tx_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, (tx_id,))
            self.last_agg_tx_id = tx_id
        except Exception as e:
            logger.warning(f"Failed to update watermark: {e}")

    def refresh_delta_aggregates(self):
        """Incrementally update aggregates with new transactions only (O(delta))."""
        start_time = time.time()

        try:
            with self.analytics_conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(MAX(tx_id), 0) FROM transactions"
                )
                current_max = cur.fetchone()[0]

                if current_max <= self.last_agg_tx_id:
                    return True

                delta_count = 0

                cur.execute("""
                    INSERT INTO delta_customer_agg
                        (customer_id, txn_count, sum_amount, sum_sq_amount, max_tx_id)
                    SELECT
                        customer_id,
                        COUNT(*),
                        SUM(amount),
                        SUM(amount * amount),
                        MAX(tx_id)
                    FROM transactions
                    WHERE tx_id > %s AND tx_id <= %s
                    GROUP BY customer_id
                    ON CONFLICT (customer_id) DO UPDATE SET
                        txn_count = delta_customer_agg.txn_count + EXCLUDED.txn_count,
                        sum_amount = delta_customer_agg.sum_amount + EXCLUDED.sum_amount,
                        sum_sq_amount = delta_customer_agg.sum_sq_amount + EXCLUDED.sum_sq_amount,
                        max_tx_id = GREATEST(delta_customer_agg.max_tx_id, EXCLUDED.max_tx_id)
                """, (self.last_agg_tx_id, current_max))
                delta_count += cur.rowcount

                cur.execute("""
                    INSERT INTO delta_category_agg
                        (category, txn_count, sum_amount, sum_sq_amount, max_tx_id)
                    SELECT
                        category,
                        COUNT(*),
                        SUM(amount),
                        SUM(amount * amount),
                        MAX(tx_id)
                    FROM transactions
                    WHERE tx_id > %s AND tx_id <= %s AND category IS NOT NULL
                    GROUP BY category
                    ON CONFLICT (category) DO UPDATE SET
                        txn_count = delta_category_agg.txn_count + EXCLUDED.txn_count,
                        sum_amount = delta_category_agg.sum_amount + EXCLUDED.sum_amount,
                        sum_sq_amount = delta_category_agg.sum_sq_amount + EXCLUDED.sum_sq_amount,
                        max_tx_id = GREATEST(delta_category_agg.max_tx_id, EXCLUDED.max_tx_id)
                """, (self.last_agg_tx_id, current_max))
                delta_count += cur.rowcount

                self._update_watermark(current_max)

            refresh_time = (time.time() - start_time) * 1000
            delta_txns = current_max - self.last_agg_tx_id
            logger.info(
                f"Delta refresh: {delta_txns} txns, {delta_count} agg updates "
                f"in {refresh_time:.0f}ms"
            )
            return True

        except Exception as e:
            logger.error(f"Delta refresh failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def poll_new_transactions(self) -> list:
        """Poll for new transactions from Iceberg."""
        try:
            with self.analytics_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT tx_id, customer_id, account_id, amount, type,
                           merchant, category, channel, created_at, received_at,
                           region, vendor, currency,
                           EXTRACT(HOUR FROM created_at)::int as tx_hour,
                           EXTRACT(DOW FROM created_at)::int as tx_day_of_week
                    FROM transactions
                    WHERE tx_id > %s
                    ORDER BY tx_id
                    LIMIT %s
                """, (self.last_processed_tx_id, BATCH_SIZE))

                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to poll transactions: {e}")
            return []

    def get_features_for_transactions(self, transactions: list) -> list:
        """Get pre-computed features from incremental delta aggregate tables."""
        if not transactions:
            return []

        start_time = time.time()

        customer_ids = list(set(
            t['customer_id'] for t in transactions if t['customer_id']
        ))
        categories = list(set(
            t['category'] for t in transactions if t['category']
        ))

        try:
            with self.analytics_conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cust_features = {}
                if customer_ids:
                    cur.execute("""
                        SELECT
                            customer_id,
                            txn_count,
                            CASE WHEN txn_count > 0
                                THEN sum_amount / txn_count ELSE 0 END as avg_amount,
                            CASE WHEN txn_count > 1
                                THEN SQRT((sum_sq_amount - (sum_amount * sum_amount / txn_count))
                                     / (txn_count - 1))
                                ELSE 0 END as stddev_amount
                        FROM delta_customer_agg
                        WHERE customer_id = ANY(%s)
                    """, (customer_ids,))
                    for row in cur.fetchall():
                        cust_features[row['customer_id']] = dict(row)

                cat_features = {}
                if categories:
                    cur.execute("""
                        SELECT
                            category,
                            txn_count,
                            CASE WHEN txn_count > 0
                                THEN sum_amount / txn_count ELSE 0 END as avg_amount,
                            CASE WHEN txn_count > 1
                                THEN SQRT((sum_sq_amount - (sum_amount * sum_amount / txn_count))
                                     / (txn_count - 1))
                                ELSE 0 END as stddev_amount
                        FROM delta_category_agg
                        WHERE category = ANY(%s)
                    """, (categories,))
                    for row in cur.fetchall():
                        cat_features[row['category']] = dict(row)

            # Assemble features for each transaction
            features_list = []
            for txn in transactions:
                amount = float(txn['amount']) if txn['amount'] else 0

                # Customer features
                cf = cust_features.get(txn['customer_id'], {})
                cust_avg = float(cf.get('avg_amount', 0) or 0)
                cust_stddev = float(cf.get('stddev_amount', 0) or 0)
                cust_txn_count = int(cf.get('txn_count', 0) or 0)

                # Category features
                catf = cat_features.get(txn['category'], {})
                cat_avg = float(catf.get('avg_amount', 0) or 0)
                cat_stddev = float(catf.get('stddev_amount', 0) or 0)

                # Compute z-scores
                amount_zscore_customer = self._compute_zscore(amount, cust_avg, cust_stddev)
                amount_zscore_category = self._compute_zscore(amount, cat_avg, cat_stddev)
                customer_lifetime_txn_count_log = math.log1p(cust_txn_count)

                features_list.append({
                    'tx_id': txn['tx_id'],
                    'amount': amount,
                    'type': txn['type'],
                    'merchant': txn['merchant'],
                    'category': txn['category'],
                    'channel': txn['channel'],
                    'received_at': txn['received_at'],
                    # Audit fields (region/vendor/currency)
                    'region': txn.get('region'),
                    'vendor': txn.get('vendor'),
                    'currency': txn.get('currency'),
                    # 11 ML features
                    'amount_zscore_customer': amount_zscore_customer,
                    'amount_zscore_category': amount_zscore_category,
                    'customer_lifetime_txn_count_log': customer_lifetime_txn_count_log,
                    'tx_hour': int(txn['tx_hour'] or 0),
                    'tx_day_of_week': int(txn['tx_day_of_week'] or 1),
                })

            feature_time = (time.time() - start_time) * 1000
            logger.debug(f"Feature extraction: {feature_time:.0f}ms for {len(features_list)} txns")
            return features_list

        except Exception as e:
            logger.error(f"Feature extraction failed: {e}")
            return []

    def _compute_zscore(self, value: float, mean: float, stddev: float) -> float:
        """Compute z-score, bounded to [-5, 5]."""
        if stddev == 0 or stddev is None:
            return 0.0
        z = (value - mean) / stddev
        return max(-5.0, min(5.0, z))

    def prepare_feature_matrix(self, features_list: list) -> np.ndarray:
        """Prepare 11-feature matrix matching training notebook FEATURE_COLUMNS order."""
        type_map = {'DEBIT': 0, 'CREDIT': 1, 'TRANSFER': 2, 'PAYMENT': 3}
        channel_map = {
            'mobile': 0, 'web': 1, 'atm': 2, 'branch': 3, 'online': 4,
            'Mobile App': 0, 'Online Banking': 1, 'ATM': 2, 'Branch Teller': 3
        }
        category_map = {
            'grocery': 0, 'travel': 1, 'crypto': 2, 'gambling': 3,
            'shopping': 4, 'dining': 5, 'entertainment': 6, 'utilities': 7,
            'Groceries': 0, 'Travel': 1, 'Crypto': 2, 'Gambling': 3,
            'Shopping': 4, 'Dining': 5, 'Entertainment': 6, 'Utilities': 7,
            'Salary': 8, 'Bonus': 9, 'Refund': 10, 'Transfer': 11,
            'Pharmacy': 12, 'Healthcare': 13, 'Gas': 14, 'Subscription': 15,
            'Crypto Exchange': 2,
            'ACH': 16, 'Gas & Auto': 14, 'Government': 17, 'Insurance': 18,
            'Interest': 19, 'Internal': 20, 'Loan Payment': 21, 'Payroll': 8,
            'Person-to-Person': 22, 'Retail Shopping': 4, 'Streaming': 15,
            'Telecom': 23, 'Wire Transfer': 11
        }
        # Region encoding for audit (fraud-audit stack)
        region_map = {
            'US': 0, 'EU': 1, 'APAC': 2, 'LATAM': 3, 'EMEA': 4,
            'us': 0, 'eu': 1, 'apac': 2, 'latam': 3, 'emea': 4
        }
        # Vendor encoding for audit (fraud-audit stack)
        vendor_map = {
            'stripe': 0, 'adyen': 1, 'square': 2, 'paypal': 3, 'braintree': 4,
            'Stripe': 0, 'Adyen': 1, 'Square': 2, 'PayPal': 3, 'Braintree': 4
        }

        feature_matrix = []
        for f in features_list:
            # 11 features - MUST match FEATURE_COLUMNS order from training notebook!
            # Order: amount, amount_zscore_customer, amount_zscore_category, customer_txn_count_log,
            #        tx_hour, tx_day_of_week, type_encoded, channel_encoded, category_encoded,
            #        region_encoded, vendor_encoded
            feature_vector = [
                float(f.get('amount', 0)),                           # [0] amount
                float(f.get('amount_zscore_customer', 0)),           # [1] amount_zscore_customer
                float(f.get('amount_zscore_category', 0)),           # [2] amount_zscore_category
                float(f.get('customer_lifetime_txn_count_log', 0)),  # [3] customer_txn_count_log
                int(f.get('tx_hour', 0)),                            # [4] tx_hour
                int(f.get('tx_day_of_week', 1)),                     # [5] tx_day_of_week
                type_map.get(f.get('type', 'DEBIT'), 0),             # [6] type_encoded
                channel_map.get(f.get('channel', ''), 0),            # [7] channel_encoded
                category_map.get(f.get('category', ''), 0),          # [8] category_encoded
                region_map.get(f.get('region', ''), 0),              # [9] region_encoded
                vendor_map.get(f.get('vendor', ''), 0)               # [10] vendor_encoded
            ]
            feature_matrix.append(feature_vector)

        return np.array(feature_matrix)

    def predict_batch(self, features_list: list) -> list:
        """Run batch inference."""
        if not features_list:
            return []

        feature_matrix = self.prepare_feature_matrix(features_list)
        fraud_probs = self.model.predict_proba(feature_matrix)[:, 1]

        return [(float(prob), bool(prob >= FRAUD_THRESHOLD)) for prob in fraud_probs]

    def save_predictions(self, predictions: list):
        """Save predictions to OLTP (only OLTP write)."""
        if not predictions:
            return

        try:
            with self.oltp_conn.cursor() as cur:
                values = [
                    (p['tx_id'], p['fraud_prob'], p['is_fraud'], 'pgaa',
                     p['ttdf_ms'], json.dumps(self._convert_decimals(p['features'])),
                     p['features'].get('region'), p['features'].get('vendor'),
                     p['features'].get('currency'))
                    for p in predictions
                ]

                psycopg2.extras.execute_values(cur, """
                    INSERT INTO ml_fraud_predictions
                    (tx_id, fraud_probability, is_fraud_predicted,
                     prediction_source, ttdf_milliseconds, feature_vector,
                     region, vendor, currency)
                    VALUES %s
                    ON CONFLICT (tx_id, prediction_source) DO UPDATE SET
                        fraud_probability = EXCLUDED.fraud_probability,
                        is_fraud_predicted = EXCLUDED.is_fraud_predicted,
                        ttdf_milliseconds = EXCLUDED.ttdf_milliseconds,
                        feature_vector = EXCLUDED.feature_vector,
                        region = EXCLUDED.region,
                        vendor = EXCLUDED.vendor,
                        currency = EXCLUDED.currency,
                        predicted_at = NOW()
                """, values)

                self.oltp_conn.commit()
        except Exception as e:
            logger.error(f"Failed to save predictions: {e}")

    def _convert_decimals(self, obj):
        """Convert Decimal types for JSON serialization."""
        if isinstance(obj, dict):
            return {k: self._convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def process_batch(self, transactions: list):
        """Process a batch of transactions."""
        if not transactions:
            return

        batch_start = time.time()

        # Get features from CTAS tables
        features_list = self.get_features_for_transactions(transactions)
        if not features_list:
            return

        # Run inference
        predictions_results = self.predict_batch(features_list)

        # Calculate TTDF and prepare predictions
        prediction_time = datetime.now()
        predictions = []
        fraud_count = 0
        ttdf_sum = 0

        for features, (fraud_prob, is_fraud) in zip(features_list, predictions_results):
            received_at = features.get('received_at')
            if received_at:
                if isinstance(received_at, str):
                    received_at = datetime.fromisoformat(received_at.replace('Z', '+00:00'))
                if hasattr(received_at, 'tzinfo') and received_at.tzinfo is not None:
                    received_at = received_at.replace(tzinfo=None)
                ttdf_ms = int((prediction_time - received_at).total_seconds() * 1000)
            else:
                ttdf_ms = int((time.time() - batch_start) * 1000)

            ttdf_sum += ttdf_ms

            predictions.append({
                'tx_id': features['tx_id'],
                'fraud_prob': fraud_prob,
                'is_fraud': is_fraud,
                'ttdf_ms': ttdf_ms,
                'features': features
            })

            if is_fraud:
                fraud_count += 1
                logger.info(f"FRAUD DETECTED: tx_id={features['tx_id']}, prob={fraud_prob:.4f}")

            # Update last processed
            if features['tx_id'] > self.last_processed_tx_id:
                self.last_processed_tx_id = features['tx_id']

        # Save predictions
        self.save_predictions(predictions)

        batch_time = int((time.time() - batch_start) * 1000)
        avg_ttdf = int(ttdf_sum / len(predictions)) if predictions else 0
        logger.info(f"Processed {len(predictions)} txns (batch={batch_time}ms, avg_TTDF={avg_ttdf}ms, fraud={fraud_count})")

    def run(self):
        """Main loop: Delta refresh -> Poll -> Process."""
        logger.info("Starting incremental delta polling loop (O(delta) updates)")

        while True:
            try:
                cycle_start = time.time()

                self.refresh_delta_aggregates()

                transactions = self.poll_new_transactions()

                if transactions:
                    self.process_batch(transactions)
                else:
                    time.sleep(1)

                elapsed = time.time() - cycle_start
                if elapsed < 0.5:
                    time.sleep(0.5 - elapsed)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)


if __name__ == '__main__':
    service = PGAACTASInferenceService()
    service.run()
