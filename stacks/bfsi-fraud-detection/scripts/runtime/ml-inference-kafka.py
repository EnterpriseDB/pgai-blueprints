#!/usr/bin/env python3
"""
ML Fraud Detection Inference Service - Kafka (Event-Driven)
============================================================
For demonstration purposes only.

Real-time fraud detection using Kafka event streams with pre-computed features.

Architecture:
  Kafka (enriched_transactions) → Inference Service → Postgres (predictions)

Performance: <100ms TTDF (30-80x faster than MV-based approach)
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import Dict, List

import psycopg2
import psycopg2.extras
import numpy as np
from kafka import KafkaConsumer
from kafka.errors import KafkaError

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

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'ml.enriched_transactions')
MODEL_PATH = os.getenv('MODEL_PATH', '/models/fraud_model_kafka.pkl')
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', 0.7))


class KafkaInferenceService:
    def __init__(self):
        logger.info("🚀 Initializing Kafka Inference Service...")

        # Model tracking
        self.model_source = "unknown"
        self.model_version = "unknown"
        self.training_metrics = {}

        # PostgreSQL connection
        self.pg_conn = None
        self._connect_postgres()

        # Load model
        self.model = self.load_model()

        # Kafka consumer
        self.consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='ml-inference-kafka',
            auto_offset_reset='latest',  # Start from latest
            enable_auto_commit=True,
            max_poll_records=100
        )
        logger.info(f"✓ Kafka consumer connected: {SOURCE_TOPIC}")

        # Statistics
        self.processed_count = 0
        self.fraud_count = 0
        self.total_ttdf_ms = 0
        self.last_log_time = time.time()

        logger.info("✓ Kafka Inference Service ready")

    def _connect_postgres(self):
        """Connect to PostgreSQL with retry logic"""
        max_retries = 30
        retry_delay = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT} "
                          f"(attempt {attempt}/{max_retries})")
                self.pg_conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, user=PG_USER,
                    password=PG_PASSWORD, dbname=PG_DB
                )
                self.pg_conn.autocommit = True
                logger.info("✓ PostgreSQL connected")
                return
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"⚠ PostgreSQL connection failed: {e}. "
                                 f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"✗ PostgreSQL failed after {max_retries} attempts")
                    raise

    def _ensure_pg_connection(self):
        """Ensure PostgreSQL connection is alive"""
        try:
            if self.pg_conn is None or self.pg_conn.closed:
                logger.warning("⚠️  PostgreSQL connection closed, reconnecting...")
                self._connect_postgres()
        except Exception as e:
            logger.error(f"✗ PostgreSQL reconnection failed: {e}")
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
                logger.info(f"✓ Loaded training metrics for drift detection")
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
            '/models/fraud_model_clickhouse.pkl',
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

            # No model found - wait and retry
            if not logged_waiting:
                logger.info(f"⏳ Waiting for model file to become available...")
                logger.info(f"   Expected paths: {model_paths}")
                logged_waiting = True

            time.sleep(check_interval)

    def prepare_features(self, enriched_tx: Dict) -> np.ndarray:
        """
        Prepare 11-feature vector from enriched transaction.
        Features are already computed by kafka-feature-materializer!
        """
        # ═══════════════════════════════════════════════════════════════════
        # 11 FEATURES - Lifetime aggregates + z-scores (matches training)
        # All features are bounded/normalized for consistent inference
        # ═══════════════════════════════════════════════════════════════════

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

        # Extract 11 features (MUST match FEATURE_COLUMNS order from training notebook!)
        # Order: amount, amount_zscore_customer, amount_zscore_category, customer_txn_count_log,
        #        tx_hour, tx_day_of_week, type_encoded, channel_encoded, category_encoded,
        #        region_encoded, vendor_encoded
        feature_vector = [
            float(enriched_tx.get('amount', 0)),                           # [0] amount
            float(enriched_tx.get('amount_zscore_customer', 0)),           # [1] amount_zscore_customer
            float(enriched_tx.get('amount_zscore_category', 0)),           # [2] amount_zscore_category
            float(enriched_tx.get('customer_txn_count_log', 0)),  # [3] customer_txn_count_log
            int(enriched_tx.get('tx_hour', 0)),                            # [4] tx_hour
            int(enriched_tx.get('tx_day_of_week', 1)),                     # [5] tx_day_of_week
            type_map.get(enriched_tx.get('type', 'DEBIT'), 0),             # [6] type_encoded
            channel_map.get(enriched_tx.get('channel', ''), 0),            # [7] channel_encoded
            category_map.get(enriched_tx.get('category', ''), 0),          # [8] category_encoded
            region_map.get(enriched_tx.get('region', ''), 0),              # [9] region_encoded
            vendor_map.get(enriched_tx.get('vendor', ''), 0)               # [10] vendor_encoded
        ]

        return np.array([feature_vector])

    def predict(self, enriched_tx: Dict) -> tuple:
        """Run inference on enriched transaction"""
        start_time = time.time()

        # Prepare features
        features = self.prepare_features(enriched_tx)

        # Model inference
        fraud_prob = float(self.model.predict_proba(features)[0, 1])
        is_fraud = fraud_prob >= FRAUD_THRESHOLD

        inference_time_ms = int((time.time() - start_time) * 1000)

        return fraud_prob, is_fraud, inference_time_ms

    def calculate_ttdf(self, enriched_tx: Dict, prediction_time: datetime) -> int:
        """Calculate Time To Detect Fraud (received_at to prediction)"""
        received_at_str = enriched_tx.get('received_at')
        if not received_at_str:
            return 0

        try:
            received_at = datetime.fromisoformat(received_at_str.replace('Z', '+00:00'))
            if hasattr(received_at, 'tzinfo') and received_at.tzinfo is not None:
                received_at = received_at.replace(tzinfo=None)

            prediction_time_naive = prediction_time.replace(tzinfo=None)
            ttdf_ms = int((prediction_time_naive - received_at).total_seconds() * 1000)
            return ttdf_ms
        except Exception as e:
            logger.warning(f"⚠️  Failed to calculate TTDF: {e}")
            return 0

    def save_prediction(self, tx_id: int, fraud_prob: float, is_fraud: bool,
                       ttdf_ms: int, features: Dict):
        """Save prediction to PostgreSQL"""
        try:
            self._ensure_pg_connection()

            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ml_fraud_predictions
                    (tx_id, fraud_probability, is_fraud_predicted,
                     prediction_source, ttdf_milliseconds, feature_vector,
                     region, vendor, currency)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_id, prediction_source) DO UPDATE SET
                        fraud_probability = EXCLUDED.fraud_probability,
                        is_fraud_predicted = EXCLUDED.is_fraud_predicted,
                        ttdf_milliseconds = EXCLUDED.ttdf_milliseconds,
                        feature_vector = EXCLUDED.feature_vector,
                        region = EXCLUDED.region,
                        vendor = EXCLUDED.vendor,
                        currency = EXCLUDED.currency,
                        predicted_at = NOW()
                """, (
                    tx_id,
                    fraud_prob,
                    is_fraud,
                    'kafka',
                    ttdf_ms,
                    json.dumps(features),
                    features.get('region'),
                    features.get('vendor'),
                    features.get('currency')
                ))

            self.pg_conn.commit()
        except Exception as e:
            logger.error(f"✗ Failed to save prediction for tx_id={tx_id}: {e}")

    def run(self):
        """Main processing loop - event-driven (no polling!)"""
        logger.info(f"📊 Starting Kafka inference loop (event-driven)")
        logger.info(f"   Source: {SOURCE_TOPIC}")
        logger.info(f"   Model: {MODEL_PATH}")
        logger.info(f"   Threshold: {FRAUD_THRESHOLD}")

        try:
            for message in self.consumer:
                enriched_tx = message.value
                tx_id = enriched_tx.get('tx_id')

                if not tx_id:
                    logger.warning("⚠️  Received message without tx_id, skipping")
                    continue

                # Run inference
                fraud_prob, is_fraud, inference_time_ms = self.predict(enriched_tx)

                # Calculate TTDF
                prediction_time = datetime.now()
                ttdf_ms = self.calculate_ttdf(enriched_tx, prediction_time)

                # Save prediction
                self.save_prediction(tx_id, fraud_prob, is_fraud, ttdf_ms, enriched_tx)

                # Statistics
                self.processed_count += 1
                self.total_ttdf_ms += ttdf_ms
                if is_fraud:
                    self.fraud_count += 1
                    logger.info(
                        f"🚨 Kafka fraud detected: "
                        f"tx_id={tx_id}, prob={fraud_prob:.4f}, "
                        f"TTDF={ttdf_ms}ms, inference={inference_time_ms}ms"
                    )

                # Log progress
                if time.time() - self.last_log_time > 10:
                    avg_ttdf = (self.total_ttdf_ms / self.processed_count
                               if self.processed_count > 0 else 0)
                    logger.info(
                        f"✓ Processed {self.processed_count} transactions "
                        f"(avg_TTDF={int(avg_ttdf)}ms, {self.fraud_count} fraud detected)"
                    )
                    self.last_log_time = time.time()

        except KeyboardInterrupt:
            logger.info("⏹ Shutting down...")
        except Exception as e:
            logger.error(f"✗ Error in main loop: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.consumer.close()
            if self.pg_conn:
                self.pg_conn.close()

            avg_ttdf = (self.total_ttdf_ms / self.processed_count
                       if self.processed_count > 0 else 0)
            logger.info(
                f"✓ Shut down cleanly "
                f"(processed={self.processed_count}, "
                f"fraud={self.fraud_count}, avg_TTDF={int(avg_ttdf)}ms)"
            )


if __name__ == '__main__':
    service = KafkaInferenceService()
    service.run()
