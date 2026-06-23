#!/usr/bin/env python3
"""
ML Fraud Detection - ClickHouse (Polling Mode)
==============================================
For demonstration purposes only.

Polls ClickHouse directly for new transactions. This aligns with the
CDC architecture where data arrives in ClickHouse via Debezium/Kafka.

Architecture:
  Transaction INSERT → CDC (Debezium) → Kafka → ClickHouse
                                                    ↓
                              ML Service polls ClickHouse for new tx_ids
                                                    ↓
                              Feature extraction from ClickHouse MVs
                                                    ↓
                              ML Inference → Save prediction to PG

Note: No PG NOTIFY - we poll ClickHouse directly since that's where
the data arrives via CDC. This avoids the timing mismatch between
PG commits and CDC replication.
"""

import os
import sys
import time
import json
import logging
import math
from datetime import datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import psycopg2.extras
import clickhouse_connect
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

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'http://clickhouse:8123')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')

MODEL_PATH = os.getenv('MODEL_PATH_CH', '/models/fraud_model_clickhouse.pkl')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 50))
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', 0.7))
POLL_INTERVAL = float(os.getenv('POLL_INTERVAL', 0.1))  # Reduced from 1.0s for faster TTDF


class ClickHouseInferenceService:
    def __init__(self):
        logger.info("Starting ClickHouse Polling Inference Service...")

        # Model tracking
        self.model_source = "unknown"
        self.model_version = "unknown"
        self.training_metrics = {}

        self._connect_databases()
        self.model = self.load_model()

        self.processed_tx_ids = set()
        self.max_processed_window = BATCH_SIZE * 100

        self.last_seen_max = self._get_current_max_tx_id()
        logger.info(f"Starting from tx_id > {self.last_seen_max} (backlog ignored)")
        logger.info("ClickHouse Polling Service ready")

    def _connect_databases(self):
        """Connect to PostgreSQL (for saving predictions) and ClickHouse (for features)."""
        max_retries = 30
        retry_delay = 2

        # PostgreSQL connection (for saving predictions only)
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT} (attempt {attempt}/{max_retries})")
                self.pg_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, user=PG_USER,
                    password=PG_PASSWORD, dbname=PG_DB
                )
                self.pg_conn.autocommit = True
                logger.info("PostgreSQL connected (for saving predictions)")
                break
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"PostgreSQL connection failed: {e}. Retrying...")
                    time.sleep(retry_delay)
                else:
                    raise

        # ClickHouse connection (for polling and feature extraction)
        for attempt in range(1, max_retries + 1):
            try:
                ch_host_clean = CH_HOST.replace('http://', '').replace('https://', '').split(':')[0]
                logger.info(f"Connecting to ClickHouse at {ch_host_clean}:8123 (attempt {attempt}/{max_retries})")
                self.ch_client = clickhouse_connect.get_client(
                    host=ch_host_clean, port=8123,
                    username=CH_USER, password=CH_PASSWORD
                )
                logger.info("ClickHouse connected (for polling and features)")
                break
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"ClickHouse connection failed: {e}. Retrying...")
                    time.sleep(retry_delay)
                else:
                    raise

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
            '/models/fraud_model_risingwave.pkl',
            '/models/fraud_model_kafka.pkl',
            '/models/fraud_model.pkl'
        ]

        check_interval = 10
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
                logger.info("⏳ Waiting for model file to become available...")
                logger.info(f"   Expected paths: {model_paths}")
                logged_waiting = True

            time.sleep(check_interval)

    def _get_current_max_tx_id(self) -> int:
        """Get current max transaction ID from ClickHouse."""
        try:
            result = self.ch_client.query("SELECT COALESCE(MAX(tx_id), 0) FROM default.transactions")
            if result.result_rows:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.warning(f"Failed to get max tx_id: {e}")
            return 0

    def convert_decimals(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def _get_ch_client(self):
        """Create a new ClickHouse client for parallel queries."""
        ch_host_clean = CH_HOST.replace('http://', '').replace('https://', '').split(':')[0]
        return clickhouse_connect.get_client(
            host=ch_host_clean, port=8123,
            username=CH_USER, password=CH_PASSWORD
        )

    def extract_features_batch(self, tx_ids: list) -> list:
        """Extract features using parallel MV queries with separate clients."""
        start_time = time.time()

        try:
            # Get base transaction data (including region/vendor/currency for audit)
            base_query = f"""
                SELECT tx_id, amount, type, merchant, category, channel,
                       created_at, received_at, customer_id, account_id,
                       region, vendor, currency,
                       toHour(created_at) as tx_hour,
                       toDayOfWeek(created_at) as tx_day_of_week
                FROM default.transactions
                WHERE tx_id IN ({','.join(map(str, tx_ids))})
                ORDER BY tx_id
            """
            base_result = self.ch_client.query(base_query)

            if not base_result.result_rows:
                return []

            tx_map = {}
            for row in base_result.result_rows:
                tx_map[row[0]] = {
                    'tx_id': row[0], 'amount': row[1], 'type': row[2],
                    'merchant': row[3], 'category': row[4], 'channel': row[5],
                    'created_at': row[6], 'received_at': row[7],
                    'customer_id': row[8], 'account_id': row[9],
                    'region': row[10], 'vendor': row[11], 'currency': row[12],
                    'tx_hour': row[13], 'tx_day_of_week': row[14]
                }

            if not tx_map:
                return []

            # Extract unique IDs
            customer_ids = list(set(tx['customer_id'] for tx in tx_map.values()))
            account_ids = list(set(tx['account_id'] for tx in tx_map.values()))
            merchants = list(set(tx['merchant'] for tx in tx_map.values() if tx['merchant']))
            categories = list(set(tx['category'] for tx in tx_map.values() if tx['category']))
            channels = list(set(tx['channel'] for tx in tx_map.values() if tx['channel']))

            # Get max received_at as reference point (like RisingWave does)
            # This ensures we query relative to transaction time, not current time
            # Use created_at (transaction time) for feature aggregation windows
            max_created_at = max(tx['created_at'] for tx in tx_map.values())
            ref_ts = max_created_at.strftime('%Y-%m-%d %H:%M:%S') if hasattr(max_created_at, 'strftime') else str(max_created_at)

            # PARALLEL QUERIES - each uses its own client connection
            customer_features, account_features = {}, {}
            merchant_features, category_features, channel_features = {}, {}, {}

            # ═══════════════════════════════════════════════════════════════════
            # LIFETIME MV QUERIES - No time filter, aggregate over ALL data
            # Returns: avg_amount, stddev_amount, txn_count for z-score computation
            # ═══════════════════════════════════════════════════════════════════

            def query_customer():
                """Query customer lifetime stats for z-score computation"""
                if not customer_ids:
                    return {}
                client = self._get_ch_client()
                # Use -Merge combinators to finalize AggregatingMergeTree state
                result = client.query(f"""
                    SELECT customer_id,
                           countMerge(txn_count_state) as txn_count,
                           avgMerge(avg_amount_state) as avg_amount,
                           stddevPopMerge(stddev_amount_state) as stddev_amount
                    FROM mv_customer_lifetime_ch
                    WHERE customer_id IN ({','.join([f"'{c}'" for c in customer_ids])})
                    GROUP BY customer_id
                """)
                # Returns: (txn_count, avg_amount, stddev_amount)
                return {row[0]: (row[1] or 0, row[2] or 0, row[3] or 0)
                        for row in result.result_rows}

            def query_category():
                """Query category lifetime stats for z-score computation"""
                if not categories:
                    return {}
                client = self._get_ch_client()
                # Use -Merge combinators to finalize AggregatingMergeTree state
                result = client.query(f"""
                    SELECT category,
                           avgMerge(avg_amount_state) as avg_amount,
                           stddevPopMerge(stddev_amount_state) as stddev_amount,
                           countMerge(txn_count_state) as txn_count
                    FROM mv_category_lifetime_ch
                    WHERE category IN ({','.join([f"'{c}'" for c in categories])})
                    GROUP BY category
                """)
                # Returns: (avg_amount, stddev_amount, txn_count)
                return {row[0]: (row[1] or 0, row[2] or 0, row[3] or 0)
                        for row in result.result_rows}

            # Execute all queries in parallel with separate clients (2 queries now)
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(query_customer): 'customer',
                    executor.submit(query_category): 'category',
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        result = future.result()
                        if key == 'customer':
                            customer_features = result
                        elif key == 'category':
                            category_features = result
                    except Exception as e:
                        logger.warning(f"Parallel query {key} failed: {e}")

            extraction_time_ms = (time.time() - start_time) * 1000

            # Helper function for z-score computation
            def compute_zscore(value, avg, stddev):
                if stddev == 0 or stddev is None:
                    return 0.0
                z = (value - avg) / stddev
                return max(-5.0, min(5.0, z))  # Clip to [-5, 5]

            # Assemble features with z-scores (11 features)
            features_list = []
            for tx_id in tx_ids:
                if tx_id not in tx_map:
                    continue
                tx = tx_map[tx_id]
                amount = tx['amount']

                # Customer: (txn_count, avg_amount, stddev_amount)
                cv = customer_features.get(tx['customer_id'], (0, 0, 0))
                cust_zscore = compute_zscore(amount, cv[1], cv[2])
                cust_txn_count_log = math.log1p(cv[0]) if cv[0] > 0 else 0

                # Category: (avg_amount, stddev_amount, txn_count)
                cp = category_features.get(tx['category'], (0, 0, 0))
                cat_zscore = compute_zscore(amount, cp[0], cp[1])

                features_list.append({
                    'tx_id': tx['tx_id'],
                    'amount': amount,
                    'type': tx['type'],
                    'merchant': tx['merchant'],
                    'category': tx['category'],
                    'channel': tx['channel'],
                    'created_at': tx['created_at'],
                    'received_at': tx['received_at'],
                    # Audit fields (region/vendor/currency)
                    'region': tx['region'],
                    'vendor': tx['vendor'],
                    'currency': tx['currency'],
                    # 11 features for ML model
                    'amount_zscore_customer': cust_zscore,
                    'amount_zscore_category': cat_zscore,
                    'customer_lifetime_txn_count_log': cust_txn_count_log,
                    'tx_hour': tx['tx_hour'],
                    'tx_day_of_week': tx['tx_day_of_week'],
                    'extraction_time_ms': extraction_time_ms
                })

            return features_list

        except Exception as e:
            logger.error(f"Parallel feature extraction failed: {e}")
            return []

    def prepare_features_batch(self, features_list: list) -> np.ndarray:
        """Prepare 11-feature vectors matching training notebook FEATURE_COLUMNS order"""
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
            'Crypto Exchange': 2,  # Map to same as 'Crypto' (high-risk)
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
            feature_matrix.append([
                float(f.get('amount', 0)),                              # [0] amount
                float(f.get('amount_zscore_customer', 0)),              # [1] amount_zscore_customer
                float(f.get('amount_zscore_category', 0)),              # [2] amount_zscore_category
                float(f.get('customer_lifetime_txn_count_log', 0)),     # [3] customer_txn_count_log
                int(f.get('tx_hour', 0)),                               # [4] tx_hour
                int(f.get('tx_day_of_week', 1)),                        # [5] tx_day_of_week
                type_map.get(f.get('type', 'DEBIT'), 0),                # [6] type_encoded
                channel_map.get(f.get('channel', ''), 0),               # [7] channel_encoded
                category_map.get(f.get('category', ''), 0),             # [8] category_encoded
                region_map.get(f.get('region', ''), 0),                 # [9] region_encoded
                vendor_map.get(f.get('vendor', ''), 0)                  # [10] vendor_encoded
            ])
        return np.array(feature_matrix)

    def predict_batch(self, features_list: list) -> list:
        if not features_list:
            return []
        feature_matrix = self.prepare_features_batch(features_list)
        fraud_probs = self.model.predict_proba(feature_matrix)[:, 1]
        return [(float(p), bool(p >= FRAUD_THRESHOLD)) for p in fraud_probs]

    def save_predictions_batch(self, predictions: list):
        if not predictions:
            return
        try:
            with self.pg_conn.cursor() as cur:
                values = [(p['tx_id'], p['fraud_prob'], p['is_fraud'], 'clickhouse',
                          p['ttdf_ms'], json.dumps(self.convert_decimals(p['features'])),
                          p['features'].get('region'), p['features'].get('vendor'),
                          p['features'].get('currency'))
                         for p in predictions]
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
                self.pg_conn.commit()
        except Exception as e:
            logger.error(f"Failed to save predictions: {e}")

    def process_batch(self, tx_ids: list):
        if not tx_ids:
            return

        start_time = time.time()

        # Filter already processed
        new_tx_ids = [t for t in tx_ids if t not in self.processed_tx_ids]
        if not new_tx_ids:
            return

        features_list = self.extract_features_batch(new_tx_ids)
        if not features_list:
            return

        predictions_results = self.predict_batch(features_list)
        prediction_time = datetime.now()

        predictions = []
        fraud_count = 0
        ttdf_sum = 0

        for features, (fraud_prob, is_fraud) in zip(features_list, predictions_results):
            tx_received_at = features.get('received_at')
            if tx_received_at:
                if isinstance(tx_received_at, str):
                    tx_received_at = datetime.fromisoformat(tx_received_at.replace('Z', '+00:00'))
                if hasattr(tx_received_at, 'tzinfo') and tx_received_at.tzinfo:
                    tx_received_at = tx_received_at.replace(tzinfo=None)
                ttdf_ms = int((prediction_time - tx_received_at).total_seconds() * 1000)
            else:
                ttdf_ms = int((time.time() - start_time) * 1000)

            ttdf_sum += ttdf_ms
            predictions.append({
                'tx_id': features['tx_id'], 'fraud_prob': fraud_prob,
                'is_fraud': is_fraud, 'ttdf_ms': ttdf_ms, 'features': features
            })

            if is_fraud:
                fraud_count += 1
                logger.info(f"ClickHouse fraud: tx_id={features['tx_id']}, prob={fraud_prob:.4f}, TTDF={ttdf_ms}ms")

            self.processed_tx_ids.add(features['tx_id'])

        self.save_predictions_batch(predictions)

        if len(self.processed_tx_ids) > self.max_processed_window:
            sorted_ids = sorted(self.processed_tx_ids)
            self.processed_tx_ids = set(sorted_ids[-self.max_processed_window:])

        avg_ttdf = int(ttdf_sum / len(predictions)) if predictions else 0
        logger.info(f"Processed {len(predictions)} txns via ClickHouse "
                    f"(batch={int((time.time()-start_time)*1000)}ms, avg_TTDF={avg_ttdf}ms, fraud={fraud_count})")

    def run(self):
        """Main polling loop - polls ClickHouse directly for new transactions."""
        logger.info(f"Starting ClickHouse polling loop (interval={POLL_INTERVAL}s)")

        while True:
            try:
                # Poll ClickHouse for new transactions
                result = self.ch_client.query(f"""
                    SELECT tx_id FROM default.transactions
                    WHERE tx_id > {self.last_seen_max}
                    ORDER BY tx_id LIMIT {BATCH_SIZE}
                """)

                if result.result_rows:
                    tx_ids = [row[0] for row in result.result_rows]
                    self.process_batch(tx_ids)
                    self.last_seen_max = max(tx_ids)

                time.sleep(POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    service = ClickHouseInferenceService()
    service.run()
