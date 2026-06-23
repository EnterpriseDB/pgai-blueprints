#!/usr/bin/env python3
"""
ML Fraud Detection Inference Service - RisingWave
=================================================
For demonstration purposes only.

Real-time fraud detection using RisingWave materialized views.
Uses separate simple queries instead of LATERAL joins for performance.
"""

import os
import sys
import time
import json
import logging
import math
from datetime import datetime
from typing import Dict, Optional, Tuple, List
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
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
PG_DB = os.getenv('POSTGRES_DB', 'demo')

RW_HOST = os.getenv('RISINGWAVE_HOST', 'risingwave')
RW_PORT = int(os.getenv('RISINGWAVE_PORT', 4566))

MODEL_PATH = os.getenv('MODEL_PATH_RW',
                       '/models/fraud_model_risingwave.pkl')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
POLL_INTERVAL = float(os.getenv('POLL_INTERVAL', 5.0))
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', 0.7))


class RisingWaveInferenceService:
    def __init__(self):
        logger.info("🚀 Initializing RisingWave Inference Service...")

        # Model tracking
        self.model_source = "unknown"
        self.model_version = "unknown"
        self.training_metrics = {}

        # PostgreSQL connection
        self.pg_conn = None
        self.rw_conn = None
        self._connect_databases()

        # Load model
        self.model = self.load_model()

        # Initialize last_processed_id to current max tx_id (ignore backlog)
        self.last_processed_id = self._get_current_max_tx_id()
        logger.info(f"✓ Starting from tx_id > {self.last_processed_id} (ignoring backlog)")

        logger.info("✓ RisingWave Inference Service ready")

    def _connect_databases(self):
        """Establish database connections with retry logic"""
        max_retries = 30
        retry_delay = 2

        # Connect to PostgreSQL with retries
        for attempt in range(1, max_retries + 1):
            try:
                if self.pg_conn is None or self.pg_conn.closed:
                    logger.info(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT} (attempt {attempt}/{max_retries})")
                    self.pg_conn = psycopg2.connect(
                        host=PG_HOST, port=PG_PORT, user=PG_USER,
                        password=PG_PASSWORD, dbname=PG_DB
                    )
                    self.pg_conn.autocommit = True
                    logger.info("✓ PostgreSQL connected")
                    break
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"⚠ PostgreSQL connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"✗ PostgreSQL failed after {max_retries} attempts")
                    raise

        # Connect to RisingWave with retries
        for attempt in range(1, max_retries + 1):
            try:
                if self.rw_conn is None or self.rw_conn.closed:
                    logger.info(f"Connecting to RisingWave at {RW_HOST}:{RW_PORT} (attempt {attempt}/{max_retries})")
                    self.rw_conn = psycopg2.connect(
                        host=RW_HOST, port=RW_PORT, user='root',
                        password='', dbname='dev'
                    )
                    logger.info("✓ RisingWave connected")
                    break
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"⚠ RisingWave connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"✗ RisingWave failed after {max_retries} attempts")
                    raise

    def _ensure_rw_connection(self):
        """Ensure RisingWave connection is alive, reconnect if needed"""
        try:
            if self.rw_conn is None or self.rw_conn.closed:
                logger.warning("⚠️  RisingWave connection closed, reconnecting...")
                self.rw_conn = psycopg2.connect(
                    host=RW_HOST, port=RW_PORT, user='root',
                    password='', dbname='dev'
                )
                logger.info("✓ RisingWave reconnected")
        except Exception as e:
            logger.error(f"✗ RisingWave reconnection failed: {e}")
            raise

    def _ensure_pg_connection(self):
        """Ensure PostgreSQL connection is alive, reconnect if needed"""
        try:
            if self.pg_conn is None or self.pg_conn.closed:
                logger.warning("⚠️  PostgreSQL connection closed, reconnecting...")
                self.pg_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, user=PG_USER,
                    password=PG_PASSWORD, dbname=PG_DB
                )
                self.pg_conn.autocommit = True
                logger.info("✓ PostgreSQL reconnected")
        except Exception as e:
            logger.error(f"✗ PostgreSQL reconnection failed: {e}")
            raise

    def _get_current_max_tx_id(self) -> int:
        """Get current max transaction ID to ignore backlog"""
        try:
            self._ensure_rw_connection()
            with self.rw_conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(tx_id), 0) FROM transactions")
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.warning(f"⚠️  Failed to get max tx_id, starting from 0: {e}")
            return 0

    def _load_training_metrics(self):
        """Load training metrics for drift detection."""
        self.training_metrics = {}
        config_path = "/models/inference_config.json"
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                self.training_metrics = config.get("training_metrics", {})
                logger.info("✓ Loaded training metrics for drift detection")
            except Exception as e:
                logger.warning(f"⚠️  Could not load training metrics: {e}")

    def load_model(self):
        """Load XGBoost model from MLflow Registry or fallback to pkl file."""
        # Try MLflow Registry first
        mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
        model_name = os.getenv('MLFLOW_MODEL_NAME', 'fraud-detection-model')
        model_stage = os.getenv('MLFLOW_MODEL_STAGE', 'Production')

        try:
            import mlflow
            mlflow.set_tracking_uri(mlflow_uri)

            # Try @champion alias first, then Production stage
            for model_uri in [f"models:/{model_name}@champion", f"models:/{model_name}/{model_stage}"]:
                try:
                    model = mlflow.xgboost.load_model(model_uri)
                    self.model_source = "mlflow_registry"
                    self.model_version = model_stage
                    self._load_training_metrics()
                    logger.info(f"✓ Model loaded from MLflow Registry: {model_uri}")
                    return model
                except Exception:
                    continue

            raise Exception("No model found in registry")

        except Exception as e:
            logger.warning(f"MLflow Registry unavailable ({e}), falling back to pkl files")
            self.model_source = "pkl_file"
            self.model_version = "local"

        # Fallback to pkl files
        import joblib

        model_paths = [
            MODEL_PATH,
            '/models/fraud_model_clickhouse.pkl',
            '/models/fraud_model_kafka.pkl',
            '/models/fraud_model.pkl'
        ]

        check_interval = 10  # seconds between checks
        logged_waiting = False

        while True:
            for path in model_paths:
                try:
                    if os.path.exists(path):
                        model = joblib.load(path)
                        self._load_training_metrics()
                        logger.info(f"✓ Model loaded from {path}")
                        return model
                except Exception as e:
                    logger.warning(f"⚠️  Failed to load model from {path}: {e}")

            if not logged_waiting:
                logger.info(f"⏳ Waiting for model file to become available...")
                logger.info(f"   Expected paths: {model_paths}")
                logged_waiting = True

            time.sleep(check_interval)

    def convert_decimals(self, obj):
        """Convert Decimal to float for JSON"""
        if isinstance(obj, dict):
            return {k: self.convert_decimals(v)
                    for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def compute_zscore(self, value: float, mean: float, stddev: float) -> float:
        """Compute z-score, bounded to [-5, 5]"""
        if stddev == 0 or stddev is None:
            return 0.0
        z = (value - mean) / stddev
        return max(-5.0, min(5.0, z))

    def extract_features_batch(self, tx_ids: list) -> list:
        """
        Extract 12 features using LIFETIME MVs (no time filter).
        Features: amount, z-scores, fraud rates, time features, encodings.
        """
        start_time = time.time()

        try:
            self._ensure_rw_connection()

            with self.rw_conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                # Step 1: Get base transaction data (including region/vendor for audit)
                cur.execute("""
                    SELECT
                        tx_id, amount, type, merchant, category, channel,
                        created_at, received_at, customer_id, account_id,
                        region, vendor, currency,
                        EXTRACT(HOUR FROM created_at) as tx_hour,
                        EXTRACT(DOW FROM created_at) as tx_day_of_week
                    FROM transactions
                    WHERE tx_id = ANY(%s)
                    ORDER BY tx_id
                """, (tx_ids,))

                base_rows = cur.fetchall()

                tx_map = {}
                customer_ids = set()
                categories = set()

                for row in base_rows:
                    tx_map[row['tx_id']] = dict(row)
                    customer_ids.add(row['customer_id'])
                    if row['category']:
                        categories.add(row['category'])

                # Step 2: Query LIFETIME customer stats (no time filter)
                customer_features = {}
                if customer_ids:
                    cur.execute("""
                        SELECT
                            customer_id,
                            txn_count,
                            avg_amount,
                            stddev_amount
                        FROM mv_customer_lifetime
                        WHERE customer_id = ANY(%s)
                    """, (list(customer_ids),))

                    for row in cur.fetchall():
                        customer_features[row['customer_id']] = {
                            'txn_count': row['txn_count'] or 0,
                            'avg_amount': float(row['avg_amount'] or 0),
                            'stddev_amount': float(row['stddev_amount'] or 0)
                        }

                # Step 3: Query LIFETIME category stats (no time filter)
                category_features = {}
                if categories:
                    cur.execute("""
                        SELECT
                            category,
                            txn_count,
                            avg_amount,
                            stddev_amount
                        FROM mv_category_lifetime
                        WHERE category = ANY(%s)
                    """, (list(categories),))

                    for row in cur.fetchall():
                        category_features[row['category']] = {
                            'txn_count': row['txn_count'] or 0,
                            'avg_amount': float(row['avg_amount'] or 0),
                            'stddev_amount': float(row['stddev_amount'] or 0)
                        }

                # Step 4: Assemble features with z-scores
                features_list = []
                extraction_time_ms = (time.time() - start_time) * 1000

                for tx_id in tx_ids:
                    if tx_id not in tx_map:
                        continue

                    tx = tx_map[tx_id]
                    amount = float(tx['amount'])

                    # Customer features
                    cf = customer_features.get(tx['customer_id'], {})
                    cust_avg = cf.get('avg_amount', 0)
                    cust_stddev = cf.get('stddev_amount', 0)
                    cust_txn_count = cf.get('txn_count', 0)

                    # Category features
                    catf = category_features.get(tx['category'], {})
                    cat_avg = catf.get('avg_amount', 0)
                    cat_stddev = catf.get('stddev_amount', 0)

                    # Compute z-scores
                    amount_zscore_customer = self.compute_zscore(amount, cust_avg, cust_stddev)
                    amount_zscore_category = self.compute_zscore(amount, cat_avg, cat_stddev)

                    # Log-scaled customer history depth
                    customer_lifetime_txn_count_log = math.log1p(cust_txn_count)

                    features_list.append({
                        'tx_id': tx['tx_id'],
                        'amount': amount,
                        'type': tx['type'],
                        'merchant': tx['merchant'],
                        'category': tx['category'],
                        'channel': tx['channel'],
                        'created_at': tx['created_at'],
                        'received_at': tx['received_at'],
                        # Audit fields
                        'region': tx.get('region', ''),
                        'vendor': tx.get('vendor', ''),
                        'currency': tx.get('currency', 'USD'),
                        # 11 ML features (must match training notebook)
                        'amount_zscore_customer': amount_zscore_customer,
                        'amount_zscore_category': amount_zscore_category,
                        'customer_lifetime_txn_count_log': customer_lifetime_txn_count_log,
                        'tx_hour': int(tx['tx_hour'] or 0),
                        'tx_day_of_week': int(tx['tx_day_of_week'] or 1),
                        'extraction_time_ms': extraction_time_ms
                    })

                return features_list

        except Exception as e:
            logger.error(f"✗ Batch feature extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    def prepare_features_batch(self, features_list: list) -> np.ndarray:
        """Prepare feature matrix (N × 11) - matches training notebook."""
        # Encoding maps (must match training notebook)
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
        # Audit feature encoding maps
        region_map = {'APAC': 0, 'EMEA': 1, 'AMER': 2, 'LATAM': 3}
        vendor_map = {'Visa': 0, 'Mastercard': 1, 'Amex': 2, 'Discover': 3, 'PayPal': 4}

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
        """Batch model inference (1 call instead of N)"""
        if not features_list:
            return []

        feature_matrix = self.prepare_features_batch(features_list)
        # Get fraud probabilities (class 1)
        fraud_probs = self.model.predict_proba(feature_matrix)[:, 1]

        results = []
        for prob in fraud_probs:
            fraud_prob = float(prob)
            is_fraud = fraud_prob >= FRAUD_THRESHOLD
            results.append((fraud_prob, is_fraud))

        return results

    def save_predictions_batch(self, predictions: list):
        """Bulk INSERT (1 query instead of N)"""
        if not predictions:
            return

        try:
            # Ensure connection is alive
            self._ensure_pg_connection()

            with self.pg_conn.cursor() as cur:
                from psycopg2.extras import execute_values

                values = [
                    (
                        pred['tx_id'],
                        pred['fraud_prob'],
                        pred['is_fraud'],
                        'risingwave',
                        pred['ttdf_ms'],
                        json.dumps(
                            self.convert_decimals(pred['features'])
                        ),
                        pred['features'].get('region'),
                        pred['features'].get('vendor'),
                        pred['features'].get('currency')
                    )
                    for pred in predictions
                ]

                execute_values(cur, """
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

                self.pg_conn.commit()
        except Exception as e:
            logger.error(f"✗ Failed to save batch predictions: {e}")

    def process_transactions_batch(self, transactions: list):
        """Process batch of transactions"""
        if not transactions:
            return

        start_time = time.time()
        tx_ids = [tx[0] for tx in transactions]

        # Step 1: Extract features (multiple simple queries)
        features_list = self.extract_features_batch(tx_ids)

        if not features_list:
            return

        # Step 2: Batch model inference (1 call)
        predictions_results = self.predict_batch(features_list)

        # Step 3: Get prediction timestamp
        batch_time_ms = int((time.time() - start_time) * 1000)
        prediction_time = datetime.now()

        # Step 4: Prepare predictions with per-transaction TTDF from received_at
        predictions = []
        fraud_count = 0
        ttdf_sum = 0
        for features, (fraud_prob, is_fraud) in zip(
            features_list, predictions_results
        ):
            # Calculate TTDF from received_at: prediction_time - received_at
            tx_received_at = features.get('received_at')
            if tx_received_at:
                if isinstance(tx_received_at, str):
                    tx_received_at = datetime.fromisoformat(tx_received_at.replace('Z', '+00:00'))
                # Remove timezone for subtraction
                if hasattr(tx_received_at, 'tzinfo') and tx_received_at.tzinfo is not None:
                    tx_received_at = tx_received_at.replace(tzinfo=None)
                prediction_time_naive = prediction_time.replace(tzinfo=None)
                ttdf_ms = int((prediction_time_naive - tx_received_at).total_seconds() * 1000)
            else:
                # Fallback to batch time if received_at not available
                ttdf_ms = batch_time_ms

            ttdf_sum += ttdf_ms

            predictions.append({
                'tx_id': features['tx_id'],
                'fraud_prob': fraud_prob,
                'is_fraud': is_fraud,
                'ttdf_ms': ttdf_ms,  # Per-transaction TTDF from received_at
                'features': features
            })

            if is_fraud:
                fraud_count += 1
                logger.info(
                    f"🚨 RisingWave fraud detected: "
                    f"tx_id={features['tx_id']}, "
                    f"prob={fraud_prob:.4f}, "
                    f"TTDF={ttdf_ms}ms"
                )

        # Step 5: Bulk insert (1 query)
        self.save_predictions_batch(predictions)

        avg_ttdf = int(ttdf_sum / len(predictions)) if predictions else 0
        logger.info(
            f"✓ Processed {len(predictions)} transactions "
            f"(batch time={batch_time_ms}ms, avg TTDF={avg_ttdf}ms, "
            f"{fraud_count} fraud detected)"
        )

    def get_new_transactions(self) -> list:
        """Fetch ALL new transactions from RisingWave (not OLTP)"""
        # Ensure connection is alive
        self._ensure_rw_connection()

        with self.rw_conn.cursor() as cur:
            # Process all available transactions (no fixed batch size)
            # Cap at 1000 to prevent memory issues
            # Use DISTINCT to handle Kafka redelivery duplicates
            cur.execute("""
                SELECT DISTINCT tx_id, created_at
                FROM transactions
                WHERE tx_id > %s
                ORDER BY tx_id
                LIMIT 1000
            """, (self.last_processed_id,))
            return cur.fetchall()

    def run(self):
        """Main processing loop - processes all available transactions"""
        logger.info(
            f"📊 Starting RisingWave inference loop "
            f"(polling every {POLL_INTERVAL}s, "
            f"processes all available transactions)"
        )

        while True:
            try:
                transactions = self.get_new_transactions()

                if transactions:
                    # Process entire batch at once
                    self.process_transactions_batch(transactions)

                    # Update last processed ID
                    self.last_processed_id = transactions[-1][0]

                time.sleep(POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("⏹ Shutting down...")
                break
            except Exception as e:
                logger.error(f"✗ Error in main loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    service = RisingWaveInferenceService()
    service.run()
