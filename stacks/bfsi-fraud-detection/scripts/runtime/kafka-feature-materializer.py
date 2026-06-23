#!/usr/bin/env python3
"""
Kafka Feature Materializer - Real-time ML Feature Computation
==============================================================
For demonstration purposes only.

Consumes transactions from Kafka, maintains LIFETIME statistics using
Welford's algorithm for incremental mean/stddev, and produces enriched
transactions with pre-computed ML features (z-scores).

Architecture:
  Kafka (transactions) → Feature Materializer → Kafka (enriched_transactions)
                              ↓
                        Welford State
                        (O(1) memory per entity)

Key Features:
  - Welford's online algorithm for incremental mean/stddev
  - Z-score computation for anomaly detection
  - Consistent with ClickHouse/RisingWave lifetime MVs
"""

import os
import json
import logging
import time
import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'corebanking.public.transactions')
TARGET_TOPIC = os.getenv('TARGET_TOPIC', 'ml.enriched_transactions')

PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
PG_DB = os.getenv('POSTGRES_DB', 'demo')


class WelfordStats:
    """
    Welford's online algorithm for computing mean and variance incrementally.

    Memory: O(1) - only stores count, mean, and M2 (sum of squared differences)
    Update: O(1) - single pass, no need to store all values

    Reference: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    """

    def __init__(self, count: int = 0, mean: float = 0.0, m2: float = 0.0, fraud_count: int = 0):
        self.count = count
        self.mean = mean
        self.M2 = m2  # Sum of squared differences from mean
        self.fraud_count = fraud_count

    def update(self, value: float, is_fraud: bool = False):
        """Update running statistics with a new value"""
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.M2 += delta * delta2

        if is_fraud:
            self.fraud_count += 1

    @property
    def variance(self) -> float:
        """Population variance"""
        if self.count < 2:
            return 0.0
        return self.M2 / self.count

    @property
    def stddev(self) -> float:
        """Population standard deviation"""
        return math.sqrt(self.variance)

    @property
    def fraud_rate(self) -> float:
        """Fraud rate as percentage (0-100)"""
        if self.count == 0:
            return 0.0
        return (self.fraud_count / self.count) * 100

    def get_zscore(self, value: float) -> float:
        """Compute z-score for a value, bounded to [-5, 5]"""
        if self.stddev == 0:
            return 0.0
        z = (value - self.mean) / self.stddev
        return max(-5.0, min(5.0, z))  # Clip to [-5, 5]


class LifetimeState:
    """
    Maintains lifetime statistics for ML feature computation using Welford's algorithm.

    Memory efficient: O(1) per customer/category/merchant/channel
    (vs O(n) for storing all historical values)
    """

    def __init__(self):
        # Welford stats per entity
        self.customer_stats: Dict[str, WelfordStats] = defaultdict(WelfordStats)
        self.category_stats: Dict[str, WelfordStats] = defaultdict(WelfordStats)
        self.merchant_stats: Dict[str, WelfordStats] = defaultdict(WelfordStats)
        self.channel_stats: Dict[str, WelfordStats] = defaultdict(WelfordStats)

        logger.info("✓ Lifetime state initialized (Welford's algorithm)")

    def warm_start_from_postgres(self, pg_host: str, pg_port: int, pg_user: str,
                                  pg_password: str, pg_db: str, max_retries: int = 30):
        """
        Warm-start Welford state from Postgres historical data.
        Queries lifetime aggregates and reconstructs M2 from variance.
        Retries connection to handle startup ordering on clean systems.
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔄 Warm-starting Welford state from Postgres (attempt {attempt}/{max_retries})...")
                conn = psycopg2.connect(
                    host=pg_host, port=pg_port, user=pg_user,
                    password=pg_password, dbname=pg_db
                )
                cur = conn.cursor()

                # Load customer stats (no is_fraud - column doesn't exist in fraud-audit schema)
                # fraud_count stays 0 since fraud detection is separate (fraud_labels table)
                cur.execute("""
                    SELECT customer_id,
                           COUNT(*) as txn_count,
                           AVG(amount) as avg_amount,
                           COALESCE(VAR_POP(amount), 0) as var_amount
                    FROM transactions
                    GROUP BY customer_id
                """)
                for row in cur.fetchall():
                    cust_id, count, mean, variance = row
                    # Reconstruct M2 from variance: M2 = variance * count (convert to float!)
                    m2 = float(variance * count) if count > 0 else 0.0
                    self.customer_stats[str(cust_id)] = WelfordStats(
                        count=count, mean=float(mean or 0), m2=m2, fraud_count=0
                    )
                logger.info(f"  ✓ Loaded {len(self.customer_stats)} customer stats")

                # Load category stats (no is_fraud - column doesn't exist in fraud-audit schema)
                cur.execute("""
                    SELECT category,
                           COUNT(*) as txn_count,
                           AVG(amount) as avg_amount,
                           COALESCE(VAR_POP(amount), 0) as var_amount
                    FROM transactions
                    WHERE category IS NOT NULL
                    GROUP BY category
                """)
                for row in cur.fetchall():
                    category, count, mean, variance = row
                    m2 = float(variance * count) if count > 0 else 0.0
                    self.category_stats[category] = WelfordStats(
                        count=count, mean=float(mean or 0), m2=m2, fraud_count=0
                    )
                logger.info(f"  ✓ Loaded {len(self.category_stats)} category stats")

                # Load merchant stats (no is_fraud - column doesn't exist in fraud-audit schema)
                cur.execute("""
                    SELECT merchant,
                           COUNT(*) as txn_count,
                           AVG(amount) as avg_amount,
                           COALESCE(VAR_POP(amount), 0) as var_amount
                    FROM transactions
                    WHERE merchant IS NOT NULL
                    GROUP BY merchant
                """)
                for row in cur.fetchall():
                    merchant, count, mean, variance = row
                    m2 = float(variance * count) if count > 0 else 0.0
                    self.merchant_stats[merchant] = WelfordStats(
                        count=count, mean=float(mean or 0), m2=m2, fraud_count=0
                    )
                logger.info(f"  ✓ Loaded {len(self.merchant_stats)} merchant stats")

                # Load channel stats (no is_fraud - column doesn't exist in fraud-audit schema)
                cur.execute("""
                    SELECT channel,
                           COUNT(*) as txn_count,
                           AVG(amount) as avg_amount,
                           COALESCE(VAR_POP(amount), 0) as var_amount
                    FROM transactions
                    WHERE channel IS NOT NULL
                    GROUP BY channel
                """)
                for row in cur.fetchall():
                    channel, count, mean, variance = row
                    m2 = float(variance * count) if count > 0 else 0.0
                    self.channel_stats[channel] = WelfordStats(
                        count=count, mean=float(mean or 0), m2=m2, fraud_count=0
                    )
                logger.info(f"  ✓ Loaded {len(self.channel_stats)} channel stats")

                cur.close()
                conn.close()
                logger.info("✓ Welford state warm-started from Postgres (consistent with lifetime MVs)")
                return  # Success - exit retry loop

            except psycopg2.OperationalError as e:
                if attempt < max_retries:
                    logger.warning(f"⚠️  Postgres not ready: {e}. Retrying in 2s...")
                    time.sleep(2)
                    continue
                else:
                    logger.warning(f"⚠️  Failed to connect to Postgres after {max_retries} attempts")
                    logger.warning("   Continuing with empty state (will build up from new transactions)")
                    return

            except Exception as e:
                # Non-connection error (e.g., table doesn't exist on clean system)
                logger.info(f"ℹ️  Warm-start skipped: {e}")
                logger.info("   Starting with empty state (clean system detected)")
                return

    def update_customer(self, customer_id: str, amount: float, is_fraud: bool):
        """Update customer lifetime statistics"""
        self.customer_stats[customer_id].update(amount, is_fraud)

    def update_category(self, category: str, amount: float, is_fraud: bool):
        """Update category lifetime statistics"""
        if category:
            self.category_stats[category].update(amount, is_fraud)

    def update_merchant(self, merchant: str, amount: float, is_fraud: bool):
        """Update merchant lifetime statistics"""
        if merchant:
            self.merchant_stats[merchant].update(amount, is_fraud)

    def update_channel(self, channel: str, amount: float, is_fraud: bool):
        """Update channel lifetime statistics"""
        if channel:
            self.channel_stats[channel].update(amount, is_fraud)

    def get_customer_features(self, customer_id: str, amount: float) -> Dict:
        """Get customer features including z-score"""
        stats = self.customer_stats.get(customer_id)

        if not stats or stats.count == 0:
            return {
                'amount_zscore_customer': 0.0,
                'customer_lifetime_txn_count_log': 0.0,
                'customer_avg_amount': 0.0,
                'customer_stddev_amount': 0.0
            }

        return {
            'amount_zscore_customer': stats.get_zscore(amount),
            'customer_lifetime_txn_count_log': math.log1p(stats.count),
            'customer_avg_amount': stats.mean,
            'customer_stddev_amount': stats.stddev
        }

    def get_category_features(self, category: str, amount: float) -> Dict:
        """Get category features including z-score"""
        stats = self.category_stats.get(category)

        if not stats or stats.count == 0:
            return {
                'amount_zscore_category': 0.0,
                'category_avg_amount': 0.0,
                'category_stddev_amount': 0.0
            }

        return {
            'amount_zscore_category': stats.get_zscore(amount),
            'category_avg_amount': stats.mean,
            'category_stddev_amount': stats.stddev
        }


class KafkaFeatureMaterializer:
    """Materializes ML features from Kafka transaction stream using Welford's algorithm"""

    def __init__(self):
        logger.info("🚀 Initializing Kafka Feature Materializer (Welford's Algorithm)...")

        # Lifetime state using Welford's algorithm
        self.state = LifetimeState()

        # Warm-start Welford state from Postgres (for consistent predictions)
        self.state.warm_start_from_postgres(PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB)

        # Initialize last_processed_id from Postgres to ignore historical backlog
        self.last_processed_id = self._get_current_max_tx_id()
        logger.info(f"✓ Starting from tx_id > {self.last_processed_id} (processing new transactions only)")

        # Kafka consumer
        self.consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='ml-feature-materializer',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            max_poll_records=100
        )
        logger.info(f"✓ Kafka consumer connected: {SOURCE_TOPIC}")

        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(f"✓ Kafka producer connected: {TARGET_TOPIC}")

        # Encoding maps (must match training notebook - 14 features)
        self.type_map = {'DEBIT': 0, 'CREDIT': 1, 'TRANSFER': 2, 'PAYMENT': 3}
        self.channel_map = {
            'mobile': 0, 'web': 1, 'atm': 2, 'branch': 3, 'online': 4,
            'Mobile App': 0, 'Online Banking': 1, 'ATM': 2, 'Branch Teller': 3
        }
        self.category_map = {
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
        # Audit features - region and vendor (added for fraud-audit stack)
        self.region_map = {'APAC': 0, 'EMEA': 1, 'AMER': 2, 'LATAM': 3}
        self.vendor_map = {'Visa': 0, 'Mastercard': 1, 'Amex': 2, 'Discover': 3, 'PayPal': 4}

        # Statistics
        self.processed_count = 0
        self.last_log_time = time.time()

        logger.info("✓ Kafka Feature Materializer ready (Welford's incremental stats)")

    def _get_current_max_tx_id(self) -> int:
        """Get current max transaction ID from Postgres to ignore backlog"""
        try:
            logger.info(f"Querying Postgres for current max_tx_id...")
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, user=PG_USER,
                password=PG_PASSWORD, dbname=PG_DB
            )
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(tx_id), 0) FROM transactions")
            max_tx_id = cur.fetchone()[0]
            cur.close()
            conn.close()
            logger.info(f"✓ Current max_tx_id: {max_tx_id}")
            return max_tx_id
        except Exception as e:
            logger.warning(f"⚠️  Failed to get max_tx_id from Postgres, starting from 0: {e}")
            return 0

    def parse_debezium_message(self, message) -> Optional[Dict]:
        """Parse Debezium CDC message format"""
        try:
            # Handle None/tombstone messages
            if message is None:
                return None

            # Debezium sends: {"payload": {"after": {...transaction...}}}
            if isinstance(message, dict):
                payload = message.get('payload', {})
                if payload and 'after' in payload and payload['after']:
                    return payload['after']
                # Fallback: assume message is transaction itself (has tx_id)
                if 'tx_id' in message:
                    return message

            return None
        except Exception as e:
            logger.error(f"✗ Failed to parse message: {e}")
            return None

    def compute_features(self, transaction: Dict) -> Dict:
        """
        Compute all ML features for a transaction.

        Features (11 total, matching training notebook):
        [0] amount
        [1] amount_zscore_customer
        [2] amount_zscore_category
        [3] customer_lifetime_txn_count_log
        [4] tx_hour
        [5] tx_day_of_week
        [6] type_encoded
        [7] channel_encoded
        [8] category_encoded
        [9] region_encoded    (audit feature)
        [10] vendor_encoded    (audit feature)
        """
        start_time = time.time()

        # Parse transaction (including audit fields: region, vendor, currency)
        tx_id = transaction.get('tx_id')
        customer_id = transaction.get('customer_id', '')
        account_id = transaction.get('account_id', '')
        amount = float(transaction.get('amount', 0))
        merchant = transaction.get('merchant', '')
        category = transaction.get('category', '')
        channel = transaction.get('channel', '')
        tx_type = transaction.get('type', 'DEBIT')
        # Audit fields (new in fraud-audit schema)
        region = transaction.get('region', '')
        vendor = transaction.get('vendor', '')
        currency = transaction.get('currency', 'USD')
        # Note: is_fraud doesn't exist in fraud-audit schema - detection is separate
        is_fraud = False

        # Parse timestamps
        created_at = transaction.get('created_at')
        received_at = transaction.get('received_at')

        # Convert to datetime if needed
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except:
                created_at = datetime.now()
        elif isinstance(created_at, int):
            created_at = datetime.fromtimestamp(created_at / 1000)
        else:
            created_at = datetime.now()

        if isinstance(received_at, str):
            try:
                received_at = datetime.fromisoformat(received_at.replace('Z', '+00:00'))
            except:
                received_at = datetime.now()
        elif isinstance(received_at, int):
            received_at = datetime.fromtimestamp(received_at / 1000)

        # Extract time features
        tx_hour = created_at.hour if created_at else 0
        tx_day_of_week = (created_at.weekday() + 1) if created_at else 1  # 1-7

        # Get features BEFORE updating state (features reflect state before this tx)
        customer_features = self.state.get_customer_features(customer_id, amount)
        category_features = self.state.get_category_features(category, amount)

        # Update state with this transaction (for next transaction's features)
        self.state.update_customer(customer_id, amount, is_fraud)
        self.state.update_category(category, amount, is_fraud)
        self.state.update_merchant(merchant, amount, is_fraud)
        self.state.update_channel(channel, amount, is_fraud)

        # Encode categoricals (including audit features)
        type_encoded = self.type_map.get(tx_type, 0)
        channel_encoded = self.channel_map.get(channel, 0)
        category_encoded = self.category_map.get(category, 0)
        region_encoded = self.region_map.get(region, 0)
        vendor_encoded = self.vendor_map.get(vendor, 0)

        # Build feature vector (11 features matching training notebook)
        features = {
            'tx_id': tx_id,
            # Core features (must match training notebook FEATURE_COLUMNS order)
            'amount': amount,
            'amount_zscore_customer': customer_features['amount_zscore_customer'],
            'amount_zscore_category': category_features['amount_zscore_category'],
            'customer_txn_count_log': customer_features['customer_lifetime_txn_count_log'],
            'tx_hour': tx_hour,
            'tx_day_of_week': tx_day_of_week,
            'type_encoded': type_encoded,
            'channel_encoded': channel_encoded,
            'category_encoded': category_encoded,
            'region_encoded': region_encoded,
            'vendor_encoded': vendor_encoded,
            # Audit metadata (for traceability, not used in model)
            'region': region,
            'vendor': vendor,
            'currency': currency,
            # Other metadata (not used in model, for debugging)
            'type': tx_type,
            'merchant': merchant,
            'category': category,
            'channel': channel,
            'received_at': received_at.isoformat() if received_at else None,
            'feature_computation_ms': int((time.time() - start_time) * 1000)
        }

        return features

    def run(self):
        """Main processing loop"""
        logger.info(f"📊 Starting feature materialization loop (Welford's algorithm)...")
        logger.info(f"   Source: {SOURCE_TOPIC}")
        logger.info(f"   Target: {TARGET_TOPIC}")
        logger.info(f"   Features: 14 (lifetime aggregates + z-scores + region/vendor for audit)")

        try:
            for message in self.consumer:
                # Parse Debezium message
                transaction = self.parse_debezium_message(message.value)
                if not transaction:
                    continue

                # Skip historical backlog
                tx_id = transaction.get('tx_id')
                if tx_id and tx_id <= self.last_processed_id:
                    continue

                # Compute features
                enriched_tx = self.compute_features(transaction)

                # Produce enriched transaction
                self.producer.send(TARGET_TOPIC, value=enriched_tx)

                # Update last_processed_id
                if tx_id:
                    self.last_processed_id = max(self.last_processed_id, tx_id)

                # Statistics
                self.processed_count += 1

                # Log progress
                if time.time() - self.last_log_time > 10:
                    stats_summary = (
                        f"customers={len(self.state.customer_stats)}, "
                        f"categories={len(self.state.category_stats)}, "
                        f"merchants={len(self.state.merchant_stats)}"
                    )
                    logger.info(
                        f"✓ Processed {self.processed_count} transactions "
                        f"({stats_summary})"
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
            self.producer.close()
            logger.info(f"✓ Shut down cleanly (processed {self.processed_count} transactions)")


if __name__ == '__main__':
    materializer = KafkaFeatureMaterializer()
    materializer.run()
