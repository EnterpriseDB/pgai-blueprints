#!/usr/bin/env python3
"""
Fraud Alert Service
===================
For demonstration purposes only.

Centralized alert service for ALL fraud detection sources.

Features:
- Polls ml_fraud_predictions for high-confidence ML fraud (>0.8 threshold)
- Polls fraud_labels for rules-based fraud detections
- Creates alerts in ml_fraud_alerts table
- Broadcasts alerts to UI via WebSocket (through server.js API)
- Logs alerts to console with TTDF metrics
- Optional webhook notifications

Stopping this service stops ALL fraud alert popups in the UI.

Environment Variables:
- POSTGRES_HOST: Postgres host (default: postgres)
- POSTGRES_PORT: Postgres port (default: 5432)
- POSTGRES_USER: Postgres user (default: postgres)
- POSTGRES_PASSWORD: Postgres password
- POSTGRES_DB: Postgres database (default: corebanking)
- ALERT_POLL_INTERVAL: Polling interval in seconds (default: 10.0)
- ALERT_THRESHOLD: Fraud probability threshold for alerts (default: 0.8)
- FRAUD_WEBHOOK_URL: Optional webhook URL for notifications
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'demo')

ALERT_POLL_INTERVAL = float(os.getenv('ALERT_POLL_INTERVAL', '10.0'))
ALERT_THRESHOLD = float(os.getenv('ALERT_THRESHOLD', '0.8'))
FRAUD_WEBHOOK_URL = os.getenv('FRAUD_WEBHOOK_URL', '')


class FraudAlertService:
    """Monitors both ML predictions and rule-based fraud labels for alerts"""

    def __init__(self):
        self.pg_conn = None
        self.last_checked_prediction_id = 0
        self.last_checked_label_id = 0

    def connect_database(self):
        """Connect to Postgres with retry logic"""
        max_retries = 30
        retry_delay = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to Postgres at {POSTGRES_HOST}:{POSTGRES_PORT} (attempt {attempt}/{max_retries})")
                self.pg_conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    database=POSTGRES_DB
                )
                self.pg_conn.autocommit = True
                logger.info("✓ Connected to Postgres")
                return
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"⚠ Connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"✗ Failed to connect after {max_retries} attempts")
                    raise

    def get_transaction_details(self, tx_id: int) -> Optional[Dict]:
        """Get transaction details for alert"""
        try:
            with self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        t.tx_id,
                        t.account_id,
                        t.customer_id,
                        t.type,
                        t.merchant,
                        t.category,
                        t.channel,
                        t.amount,
                        t.balance_after,
                        COALESCE(fl.is_fraud, FALSE) as actual_fraud,
                        fl.fraud_reason,
                        t.created_at,
                        c.name as customer_name,
                        c.email as customer_email
                    FROM transactions t
                    LEFT JOIN customers c ON t.customer_id = c.customer_id
                    LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
                    WHERE t.tx_id = %s
                """, (tx_id,))

                result = cur.fetchone()
                return dict(result) if result else None

        except Exception as e:
            logger.error(f"✗ Failed to get transaction details for tx_id={tx_id}: {e}")
            return None

    def determine_alert_severity(self, fraud_probability: float, amount: float) -> str:
        """Determine alert severity based on probability and amount"""
        if fraud_probability >= 0.95 and amount > 5000:
            return 'critical'
        elif fraud_probability >= 0.9 or amount > 10000:
            return 'high'
        elif fraud_probability >= 0.85:
            return 'medium'
        else:
            return 'low'

    def create_alert(self, prediction: Dict, tx_details: Dict):
        """Create a fraud alert"""
        try:
            # Determine severity
            severity = self.determine_alert_severity(
                prediction['fraud_probability'],
                tx_details['amount']
            )

            # Build alert details JSON
            alert_details = {
                'tx_id': tx_details['tx_id'],
                'customer_id': tx_details['customer_id'],
                'customer_name': tx_details.get('customer_name'),
                'customer_email': tx_details.get('customer_email'),
                'account_id': tx_details['account_id'],
                'merchant': tx_details['merchant'],
                'category': tx_details['category'],
                'channel': tx_details['channel'],
                'amount': float(tx_details['amount']),
                'balance_after': float(tx_details['balance_after']),
                'transaction_type': tx_details['type'],
                'fraud_probability': float(prediction['fraud_probability']),
                'prediction_source': prediction['prediction_source'],
                'ttdf_milliseconds': prediction['ttdf_milliseconds'],
                'rule_based_fraud': tx_details.get('actual_fraud', False),
                'rule_based_reason': tx_details.get('fraud_reason'),
                'agreement': prediction['prediction_source'] == 'both' or
                           (tx_details.get('actual_fraud', False) == prediction['is_fraud_predicted']),
                'timestamp': tx_details['created_at'].isoformat() if tx_details['created_at'] else None
            }

            # Insert alert
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ml_fraud_alerts
                    (tx_id, fraud_probability, prediction_source, alert_severity, alert_details)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING alert_id
                """, (
                    tx_details['tx_id'],
                    prediction['fraud_probability'],
                    prediction['prediction_source'],
                    severity,
                    json.dumps(alert_details)
                ))

                alert_id = cur.fetchone()[0]

            self.pg_conn.commit()

            # Log alert
            self.log_alert(alert_id, severity, tx_details, prediction)

            # Broadcast to WebSocket clients via backend API
            self.broadcast_to_ui(alert_id, severity, tx_details, prediction, alert_details)

            # Send webhook notification if configured
            if FRAUD_WEBHOOK_URL:
                self.send_webhook_notification(alert_details)

            return alert_id

        except Exception as e:
            logger.error(f"✗ Failed to create alert for tx_id={tx_details['tx_id']}: {e}")
            self.pg_conn.rollback()
            return None

    def log_alert(self, alert_id: int, severity: str, tx_details: Dict, prediction: Dict):
        """Log alert to console"""
        severity_emoji = {
            'critical': '🔴',
            'high': '🟠',
            'medium': '🟡',
            'low': '🟢'
        }

        emoji = severity_emoji.get(severity, '⚪')

        logger.warning("=" * 80)
        logger.warning(f"{emoji} FRAUD ALERT #{alert_id} - Severity: {severity.upper()}")
        logger.warning("-" * 80)
        logger.warning(f"Transaction ID:    {tx_details['tx_id']}")
        logger.warning(f"Customer:          {tx_details.get('customer_name', 'N/A')} (ID: {tx_details['customer_id']})")
        logger.warning(f"Merchant:          {tx_details['merchant']}")
        logger.warning(f"Amount:            ${tx_details['amount']:,.2f}")
        logger.warning(f"Category:          {tx_details['category']}")
        logger.warning(f"Channel:           {tx_details['channel']}")
        logger.warning("-" * 80)
        logger.warning(f"ML Fraud Score:    {prediction['fraud_probability']:.4f} ({prediction['prediction_source']})")
        logger.warning(f"Rule-based:        {'FRAUD' if tx_details.get('actual_fraud') else 'SAFE'}")
        if tx_details.get('fraud_reason'):
            logger.warning(f"Rule reason:       {tx_details['fraud_reason']}")
        logger.warning(f"TTDF:              {prediction['ttdf_milliseconds']}ms")
        logger.warning("-" * 80)

        # Show agreement/disagreement
        if tx_details.get('actual_fraud') and prediction['is_fraud_predicted']:
            logger.warning("✓ Agreement: Both ML and rules detected fraud")
        elif not tx_details.get('actual_fraud') and prediction['is_fraud_predicted']:
            logger.warning("⚠ Disagreement: ML detected fraud, rules did not")
        elif tx_details.get('actual_fraud') and not prediction['is_fraud_predicted']:
            logger.warning("⚠ Disagreement: Rules detected fraud, ML did not")

        logger.warning("=" * 80)

    def send_webhook_notification(self, alert_details: Dict):
        """Send webhook notification (if configured)"""
        if not FRAUD_WEBHOOK_URL:
            return

        try:
            import requests

            payload = {
                'event': 'fraud_alert',
                'severity': alert_details.get('severity', 'high'),
                'details': alert_details
            }

            response = requests.post(
                FRAUD_WEBHOOK_URL,
                json=payload,
                timeout=5
            )

            if response.status_code == 200:
                logger.info(f"✓ Webhook notification sent for tx_id={alert_details['tx_id']}")
            else:
                logger.warning(f"⚠ Webhook returned status {response.status_code}")

        except Exception as e:
            logger.error(f"✗ Failed to send webhook notification: {e}")

    def broadcast_to_ui(self, alert_id: int, severity: str, tx_details: Dict, prediction: Dict, alert_details: Dict):
        """Broadcast fraud alert to WebSocket clients via backend API"""
        try:
            import requests

            # Determine backend URL
            backend_host = os.getenv('BACKEND_HOST', 'app')
            backend_port = os.getenv('BACKEND_PORT', '3001')
            backend_url = f"http://{backend_host}:{backend_port}/api/ml/alert/broadcast"

            payload = {
                'tx_id': tx_details['tx_id'],
                'fraud_probability': float(prediction['fraud_probability']),
                'prediction_source': prediction['prediction_source'],
                'alert_severity': severity,
                'alert_details': alert_details
            }

            response = requests.post(backend_url, json=payload, timeout=2)

            if response.status_code == 200:
                logger.info(f"✓ Alert #{alert_id} broadcast to UI")
            else:
                logger.warning(f"⚠ Broadcast returned status {response.status_code}")

        except Exception as e:
            logger.debug(f"⚠ Failed to broadcast to UI (non-critical): {e}")

    def get_new_high_confidence_predictions(self) -> List[Dict]:
        """Get new high-confidence fraud predictions"""
        try:
            with self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        p.prediction_id,
                        p.tx_id,
                        p.fraud_probability,
                        p.is_fraud_predicted,
                        p.prediction_source,
                        p.ttdf_milliseconds,
                        p.predicted_at
                    FROM ml_fraud_predictions p
                    WHERE p.prediction_id > %s
                      AND p.is_fraud_predicted = TRUE
                      AND p.fraud_probability >= %s
                      AND NOT EXISTS (
                          SELECT 1 FROM ml_fraud_alerts a
                          WHERE a.tx_id = p.tx_id
                            AND a.prediction_source = p.prediction_source
                      )
                    ORDER BY p.prediction_id
                """, (self.last_checked_prediction_id, ALERT_THRESHOLD))

                return cur.fetchall()

        except Exception as e:
            logger.error(f"✗ Failed to fetch predictions: {e}")
            return []

    def process_predictions(self):
        """Process new high-confidence predictions and create alerts"""
        predictions = self.get_new_high_confidence_predictions()

        if not predictions:
            return 0

        logger.info(f"Found {len(predictions)} new high-confidence fraud predictions")

        alerts_created = 0

        for prediction in predictions:
            # Get transaction details
            tx_details = self.get_transaction_details(prediction['tx_id'])

            if not tx_details:
                logger.warning(f"⚠ Transaction details not found for tx_id={prediction['tx_id']}")
                continue

            # Create alert
            alert_id = self.create_alert(dict(prediction), tx_details)

            if alert_id:
                alerts_created += 1

            # Update last checked ID
            if prediction['prediction_id'] > self.last_checked_prediction_id:
                self.last_checked_prediction_id = prediction['prediction_id']

        return alerts_created

    def get_new_rules_based_fraud(self) -> List[Dict]:
        """Get new rules-based fraud from fraud_labels table"""
        try:
            with self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        fl.label_id,
                        fl.tx_id,
                        fl.is_fraud,
                        fl.rules_triggered,
                        fl.fraud_reason,
                        fl.ttdf_milliseconds,
                        fl.detected_at
                    FROM fraud_labels fl
                    WHERE fl.label_id > %s
                      AND fl.is_fraud = TRUE
                      AND fl.detection_source = 'rules'
                      AND NOT EXISTS (
                          SELECT 1 FROM ml_fraud_alerts a
                          WHERE a.tx_id = fl.tx_id
                            AND a.prediction_source = 'rules'
                      )
                    ORDER BY fl.label_id
                    LIMIT 100
                """, (self.last_checked_label_id,))

                return cur.fetchall()

        except Exception as e:
            logger.error(f"✗ Failed to fetch rules-based fraud: {e}")
            return []

    def process_rules_based_fraud(self):
        """Process new rules-based fraud and create alerts"""
        fraud_labels = self.get_new_rules_based_fraud()

        if not fraud_labels:
            return 0

        logger.info(f"Found {len(fraud_labels)} new rules-based fraud detections")

        alerts_created = 0

        for label in fraud_labels:
            tx_details = self.get_transaction_details(label['tx_id'])

            if not tx_details:
                logger.warning(f"⚠ Transaction details not found for tx_id={label['tx_id']}")
                if label['label_id'] > self.last_checked_label_id:
                    self.last_checked_label_id = label['label_id']
                continue

            # Create a prediction-like dict for rules-based fraud
            prediction = {
                'fraud_probability': 1.0,
                'is_fraud_predicted': True,
                'prediction_source': 'rules',
                'ttdf_milliseconds': label['ttdf_milliseconds'] or 0
            }

            alert_id = self.create_alert(prediction, tx_details)

            if alert_id:
                alerts_created += 1

            if label['label_id'] > self.last_checked_label_id:
                self.last_checked_label_id = label['label_id']

        return alerts_created

    def get_alert_statistics(self) -> Dict:
        """Get alert statistics for logging"""
        try:
            with self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_alerts,
                        COUNT(*) FILTER (WHERE resolution_status = 'pending') as pending,
                        COUNT(*) FILTER (WHERE resolution_status = 'confirmed') as confirmed,
                        COUNT(*) FILTER (WHERE resolution_status = 'false_positive') as false_positives,
                        COUNT(*) FILTER (WHERE alert_severity = 'critical') as critical,
                        COUNT(*) FILTER (WHERE alert_severity = 'high') as high,
                        AVG(fraud_probability) as avg_fraud_score
                    FROM ml_fraud_alerts
                    WHERE alert_sent_at > NOW() - INTERVAL '1 hour'
                """)

                return dict(cur.fetchone())

        except Exception as e:
            logger.error(f"✗ Failed to get alert statistics: {e}")
            return {}

    def run(self):
        """Main service loop"""
        logger.info("=" * 60)
        logger.info("Fraud Alert Monitoring Service")
        logger.info("=" * 60)
        logger.info(f"Monitoring: ML predictions + Rules-based fraud")
        logger.info(f"ML Alert threshold: {ALERT_THRESHOLD}")
        logger.info(f"Poll interval: {ALERT_POLL_INTERVAL}s")
        if FRAUD_WEBHOOK_URL:
            logger.info(f"Webhook URL: {FRAUD_WEBHOOK_URL}")
        logger.info("=" * 60)

        # Connect to database
        self.connect_database()

        logger.info("✓ Service ready - monitoring for fraud alerts")

        # Main monitoring loop
        iteration = 0
        while True:
            try:
                # Process ML predictions
                ml_alerts = self.process_predictions()

                # Process rules-based fraud
                rules_alerts = self.process_rules_based_fraud()

                alerts_created = ml_alerts + rules_alerts

                if alerts_created > 0:
                    logger.info(f"✓ Created {alerts_created} alerts (ML: {ml_alerts}, Rules: {rules_alerts})")

                # Log statistics every 10 iterations (~1.5 minutes)
                iteration += 1
                if iteration % 10 == 0:
                    stats = self.get_alert_statistics()
                    if stats:
                        logger.info("-" * 60)
                        logger.info("Alert Statistics (Last Hour):")
                        logger.info(f"  Total: {stats.get('total_alerts', 0)}")
                        logger.info(f"  Pending: {stats.get('pending', 0)}")
                        logger.info(f"  Confirmed: {stats.get('confirmed', 0)}")
                        logger.info(f"  False Positives: {stats.get('false_positives', 0)}")
                        logger.info(f"  Critical: {stats.get('critical', 0)}")
                        logger.info(f"  High: {stats.get('high', 0)}")
                        logger.info(f"  Avg Score: {stats.get('avg_fraud_score', 0):.4f}")
                        logger.info("-" * 60)

                time.sleep(ALERT_POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"✗ Error in main loop: {e}")
                time.sleep(ALERT_POLL_INTERVAL)


if __name__ == '__main__':
    service = FraudAlertService()
    service.run()
