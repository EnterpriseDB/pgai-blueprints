"""
ML Algorithm Audit Tool - Compare ML predictions against rule-based detection

For demonstration purposes only.

This tool audits ML fraud predictions by comparing them against rule-based detection
results, calculating precision, recall, F1 scores, and generating audit reports.

Key Features:
- Queries ml_fraud_predictions and rule_based_fraud_metrics tables
- Calculates TP, FP, FN, TN classifications
- Computes precision, recall, F1, and accuracy metrics
- Generates detailed audit reports with insights
- Supports filtering by prediction source, time range, and thresholds
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import psycopg2
from psycopg2.extras import RealDictCursor
from lfx.custom.custom_component.component import Component
from lfx.io import (
    StrInput, IntInput, FloatInput, MessageTextInput,
    DropdownInput, BoolInput, Output
)
from lfx.schema.data import Data


class MLAuditTool(Component):
    """
    Audits ML fraud predictions against rule-based detection.
    Calculates precision, recall, F1 scores and generates audit reports.
    """

    display_name = "ML Algorithm Audit"
    description = (
        "Audit ML fraud predictions against rule-based detection. "
        "Calculates TP, FP, FN metrics, precision, recall, F1 scores. "
        "Generates comprehensive audit reports with drift detection insights."
    )
    icon = "audit"
    name = "MLAuditTool"

    inputs = [
        DropdownInput(
            name="audit_type",
            display_name="Audit Type",
            info="Type of audit to perform",
            options=["full_comparison", "metrics_only", "drift_analysis", "detailed_transactions"],
            value="full_comparison",
            tool_mode=True,
        ),
        DropdownInput(
            name="prediction_source",
            display_name="Prediction Source",
            info="ML inference engine to audit (or 'all' for comparison)",
            options=["all", "kafka", "pgaa", "risingwave", "clickhouse"],
            value="all",
            tool_mode=True,
        ),
        DropdownInput(
            name="time_range",
            display_name="Time Range",
            info="Time window for audit analysis",
            options=["1h", "6h", "24h", "7d", "30d"],
            value="24h",
            tool_mode=True,
        ),
        IntInput(
            name="limit",
            display_name="Max Transactions",
            info="Maximum number of transactions to include in detailed report",
            value=100,
        ),
        FloatInput(
            name="fraud_threshold",
            display_name="Fraud Score Threshold",
            info="Threshold above which ML prediction is considered fraud",
            value=0.5,
        ),
    ]

    outputs = [
        Output(display_name="Audit Report", name="output", method="run_audit"),
    ]

    def _get_connection(self):
        """Create database connection."""
        return psycopg2.connect(
            host='pgd',
            port=5432,
            user='postgres',
            password='secret',
            database='demo',
            cursor_factory=RealDictCursor
        )

    def _parse_time_range(self, time_range: str) -> str:
        """Convert time range to PostgreSQL interval."""
        mappings = {
            '1h': '1 hour',
            '6h': '6 hours',
            '24h': '24 hours',
            '7d': '7 days',
            '30d': '30 days',
        }
        return mappings.get(time_range, '24 hours')

    def run_audit(self) -> Data:
        """Execute the ML algorithm audit."""
        audit_type = self.audit_type
        prediction_source = self.prediction_source
        time_range = self.time_range
        limit = self.limit
        fraud_threshold = self.fraud_threshold

        self.status = f"Running {audit_type} audit..."

        try:
            conn = self._get_connection()
            cur = conn.cursor()

            interval = self._parse_time_range(time_range)

            if audit_type == "full_comparison":
                result = self._full_comparison_audit(cur, prediction_source, interval, fraud_threshold)
            elif audit_type == "metrics_only":
                result = self._metrics_only_audit(cur, prediction_source, interval)
            elif audit_type == "drift_analysis":
                result = self._drift_analysis_audit(cur, prediction_source, interval)
            elif audit_type == "detailed_transactions":
                result = self._detailed_transactions_audit(cur, prediction_source, interval, limit, fraud_threshold)
            else:
                result = self._full_comparison_audit(cur, prediction_source, interval, fraud_threshold)

            cur.close()
            conn.close()

            self.status = f"Audit complete: {audit_type}"
            return Data(value=result)

        except Exception as e:
            error_msg = f"Audit Error: {str(e)}"
            self.status = error_msg
            return Data(value=error_msg)

    def _full_comparison_audit(self, cur, source: str, interval: str, threshold: float) -> str:
        """Generate full comparison audit report."""
        report = []
        report.append("=" * 70)
        report.append("ML ALGORITHM AUDIT REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Time Range: Last {interval}")
        report.append(f"Fraud Threshold: {threshold}")
        report.append("=" * 70)

        # Get overall metrics
        source_filter = f"AND mp.prediction_source = '{source}'" if source != 'all' else ""

        cur.execute(f"""
            WITH classifications AS (
                SELECT
                    mp.prediction_source,
                    CASE
                        WHEN mp.is_fraud_predicted = TRUE AND rbm.tx_id IS NOT NULL THEN 'TP'
                        WHEN mp.is_fraud_predicted = TRUE AND rbm.tx_id IS NULL THEN 'FP'
                        WHEN mp.is_fraud_predicted = FALSE AND rbm.tx_id IS NOT NULL THEN 'FN'
                        ELSE 'TN'
                    END as classification
                FROM ml_fraud_predictions mp
                LEFT JOIN rule_based_fraud_metrics rbm ON mp.tx_id = rbm.tx_id
                WHERE mp.predicted_at > NOW() - INTERVAL '{interval}'
                {source_filter}
            )
            SELECT
                prediction_source,
                COUNT(*) FILTER (WHERE classification = 'TP') as tp,
                COUNT(*) FILTER (WHERE classification = 'FP') as fp,
                COUNT(*) FILTER (WHERE classification = 'FN') as fn,
                COUNT(*) FILTER (WHERE classification = 'TN') as tn,
                COUNT(*) as total
            FROM classifications
            GROUP BY prediction_source
            ORDER BY prediction_source
        """)

        rows = cur.fetchall()

        if not rows:
            report.append("\nNo predictions found in the specified time range.")
            return "\n".join(report)

        report.append("\n1. CLASSIFICATION METRICS BY ENGINE")
        report.append("-" * 70)

        overall_stats = {'tp': 0, 'fp': 0, 'fn': 0, 'tn': 0, 'total': 0}

        for row in rows:
            src = row['prediction_source']
            tp, fp, fn, tn, total = row['tp'], row['fp'], row['fn'], row['tn'], row['total']

            overall_stats['tp'] += tp
            overall_stats['fp'] += fp
            overall_stats['fn'] += fn
            overall_stats['tn'] += tn
            overall_stats['total'] += total

            # Calculate metrics
            precision = 100.0 * tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = 100.0 * tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            accuracy = 100.0 * (tp + tn) / total if total > 0 else 0

            report.append(f"\n  {src.upper()}")
            report.append(f"  {'=' * 30}")
            report.append(f"  Total Predictions: {total:,}")
            report.append(f"  True Positives:    {tp:,} (ML + Rules flagged)")
            report.append(f"  False Positives:   {fp:,} (ML flagged, Rules didn't)")
            report.append(f"  False Negatives:   {fn:,} (Rules flagged, ML didn't)")
            report.append(f"  True Negatives:    {tn:,} (Neither flagged)")
            report.append(f"  Precision:         {precision:.2f}%")
            report.append(f"  Recall:            {recall:.2f}%")
            report.append(f"  F1 Score:          {f1:.2f}%")
            report.append(f"  Accuracy:          {accuracy:.2f}%")

        # Overall summary
        if len(rows) > 1:
            report.append("\n  OVERALL (All Engines)")
            report.append(f"  {'=' * 30}")
            tp, fp, fn, tn = overall_stats['tp'], overall_stats['fp'], overall_stats['fn'], overall_stats['tn']
            total = overall_stats['total']
            precision = 100.0 * tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = 100.0 * tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            accuracy = 100.0 * (tp + tn) / total if total > 0 else 0

            report.append(f"  Total Predictions: {total:,}")
            report.append(f"  Precision:         {precision:.2f}%")
            report.append(f"  Recall:            {recall:.2f}%")
            report.append(f"  F1 Score:          {f1:.2f}%")
            report.append(f"  Accuracy:          {accuracy:.2f}%")

        # Audit insights
        report.append("\n\n2. AUDIT INSIGHTS")
        report.append("-" * 70)

        # False Positives analysis
        cur.execute(f"""
            SELECT
                t.category,
                COUNT(*) as fp_count,
                ROUND(AVG(mp.fraud_probability)::numeric, 3) as avg_score
            FROM ml_fraud_predictions mp
            JOIN transactions t ON mp.tx_id = t.tx_id
            LEFT JOIN rule_based_fraud_metrics rbm ON mp.tx_id = rbm.tx_id
            WHERE mp.predicted_at > NOW() - INTERVAL '{interval}'
                AND mp.is_fraud_predicted = TRUE
                AND rbm.tx_id IS NULL
                {source_filter}
            GROUP BY t.category
            ORDER BY fp_count DESC
            LIMIT 5
        """)
        fp_by_category = cur.fetchall()

        if fp_by_category:
            report.append("\n  False Positives by Category (ML flagged, Rules didn't):")
            for row in fp_by_category:
                report.append(f"    - {row['category']}: {row['fp_count']} FPs (avg score: {row['avg_score']})")

        # False Negatives analysis
        cur.execute(f"""
            SELECT
                unnest(rbm.rules_triggered) as rule_triggered,
                COUNT(*) as fn_count
            FROM ml_fraud_predictions mp
            JOIN rule_based_fraud_metrics rbm ON mp.tx_id = rbm.tx_id
            WHERE mp.predicted_at > NOW() - INTERVAL '{interval}'
                AND mp.is_fraud_predicted = FALSE
                AND rbm.tx_id IS NOT NULL
                {source_filter}
            GROUP BY rule_triggered
            ORDER BY fn_count DESC
            LIMIT 5
        """)
        fn_by_rule = cur.fetchall()

        if fn_by_rule:
            report.append("\n  False Negatives by Rule (Rules flagged, ML didn't):")
            for row in fn_by_rule:
                report.append(f"    - {row['rule_triggered']}: {row['fn_count']} missed")

        # Recommendations
        report.append("\n\n3. RECOMMENDATIONS")
        report.append("-" * 70)

        if overall_stats['fp'] > overall_stats['tp'] * 0.5:
            report.append("  [HIGH PRIORITY] High false positive rate detected.")
            report.append("  Consider retraining model with more negative samples.")

        if overall_stats['fn'] > overall_stats['tp'] * 0.3:
            report.append("  [HIGH PRIORITY] High false negative rate - ML missing rule-flagged fraud.")
            report.append("  Review rule coverage in ML training features.")

        if overall_stats['fn'] == 0 and overall_stats['tp'] == 0:
            report.append("  [INFO] No rule-based detections in this period.")
            report.append("  Rules may need adjustment or fraud rate is very low.")

        report.append("\n" + "=" * 70)
        report.append("END OF AUDIT REPORT")

        return "\n".join(report)

    def _metrics_only_audit(self, cur, source: str, interval: str) -> str:
        """Generate concise metrics-only report."""
        source_filter = f"WHERE prediction_source = '{source}'" if source != 'all' else ""

        cur.execute(f"""
            SELECT * FROM v_ml_audit_metrics
            {source_filter}
        """)

        rows = cur.fetchall()

        report = []
        report.append("ML AUDIT METRICS")
        report.append(f"Time Range: Last {interval}")
        report.append("-" * 50)

        for row in rows:
            report.append(f"\n{row['prediction_source'].upper()}:")
            report.append(f"  TP: {row['true_positives']} | FP: {row['false_positives']} | FN: {row['false_negatives']} | TN: {row['true_negatives']}")
            report.append(f"  Precision: {row['precision_pct']}% | Recall: {row['recall_pct']}% | F1: {row['f1_score']}%")
            report.append(f"  Accuracy: {row['accuracy_pct']}% | Total: {row['total']}")

        return "\n".join(report)

    def _drift_analysis_audit(self, cur, source: str, interval: str) -> str:
        """Analyze drift between ML and rules over time."""
        source_filter = f"AND mp.prediction_source = '{source}'" if source != 'all' else ""

        cur.execute(f"""
            WITH hourly_metrics AS (
                SELECT
                    DATE_TRUNC('hour', mp.predicted_at) as hour,
                    mp.prediction_source,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE mp.is_fraud_predicted = TRUE AND rbm.tx_id IS NOT NULL) as tp,
                    COUNT(*) FILTER (WHERE mp.is_fraud_predicted = TRUE AND rbm.tx_id IS NULL) as fp,
                    COUNT(*) FILTER (WHERE mp.is_fraud_predicted = FALSE AND rbm.tx_id IS NOT NULL) as fn
                FROM ml_fraud_predictions mp
                LEFT JOIN rule_based_fraud_metrics rbm ON mp.tx_id = rbm.tx_id
                WHERE mp.predicted_at > NOW() - INTERVAL '{interval}'
                {source_filter}
                GROUP BY DATE_TRUNC('hour', mp.predicted_at), mp.prediction_source
            )
            SELECT
                hour,
                prediction_source,
                total,
                tp,
                fp,
                fn,
                CASE WHEN tp + fp > 0 THEN ROUND(100.0 * tp / (tp + fp), 1) ELSE 0 END as precision,
                CASE WHEN tp + fn > 0 THEN ROUND(100.0 * tp / (tp + fn), 1) ELSE 0 END as recall
            FROM hourly_metrics
            ORDER BY prediction_source, hour DESC
        """)

        rows = cur.fetchall()

        report = []
        report.append("DRIFT ANALYSIS REPORT")
        report.append(f"Time Range: Last {interval}")
        report.append("=" * 70)

        if not rows:
            report.append("No data available for drift analysis.")
            return "\n".join(report)

        # Group by source
        by_source = {}
        for row in rows:
            src = row['prediction_source']
            if src not in by_source:
                by_source[src] = []
            by_source[src].append(row)

        for src, metrics in by_source.items():
            report.append(f"\n{src.upper()} - Hourly Trend")
            report.append("-" * 50)
            report.append("Hour                    | Total | TP  | FP  | FN  | Prec% | Rec%")
            report.append("-" * 70)

            for m in metrics[:12]:  # Last 12 hours
                hour_str = m['hour'].strftime('%Y-%m-%d %H:00')
                report.append(
                    f"{hour_str} | {m['total']:5} | {m['tp']:3} | {m['fp']:3} | {m['fn']:3} | "
                    f"{m['precision']:5.1f} | {m['recall']:5.1f}"
                )

            # Drift detection
            if len(metrics) >= 2:
                latest = metrics[0]
                previous = metrics[-1]

                precision_drift = float(latest['precision']) - float(previous['precision'])
                recall_drift = float(latest['recall']) - float(previous['recall'])

                report.append(f"\nDrift (latest vs earliest in window):")
                report.append(f"  Precision: {precision_drift:+.1f}%")
                report.append(f"  Recall: {recall_drift:+.1f}%")

                if abs(precision_drift) > 10 or abs(recall_drift) > 10:
                    report.append("  [ALERT] Significant drift detected - consider model retraining")

        return "\n".join(report)

    def _detailed_transactions_audit(self, cur, source: str, interval: str, limit: int, threshold: float) -> str:
        """Get detailed transaction-level audit data."""
        source_filter = f"AND prediction_source = '{source}'" if source != 'all' else ""

        cur.execute(f"""
            SELECT
                tx_id,
                amount,
                merchant,
                category,
                channel,
                ml_score,
                ml_flagged,
                prediction_source,
                rule_flagged,
                rules_triggered,
                classification,
                audit_insight,
                tx_time
            FROM v_ml_audit_comparison
            WHERE predicted_at > NOW() - INTERVAL '{interval}'
                {source_filter}
            ORDER BY
                CASE classification
                    WHEN 'FN' THEN 1  -- False negatives first (ML missed)
                    WHEN 'FP' THEN 2  -- False positives second
                    WHEN 'TP' THEN 3
                    ELSE 4
                END,
                ml_score DESC
            LIMIT {limit}
        """)

        rows = cur.fetchall()

        report = []
        report.append("DETAILED TRANSACTION AUDIT")
        report.append(f"Time Range: Last {interval}")
        report.append(f"Showing: {len(rows)} transactions (prioritized by classification)")
        report.append("=" * 70)

        # Group by classification
        by_class = {'FN': [], 'FP': [], 'TP': [], 'TN': []}
        for row in rows:
            cls = row['classification']
            if cls in by_class:
                by_class[cls].append(row)

        for cls_name, cls_label in [('FN', 'FALSE NEGATIVES (Rules flagged, ML missed)'),
                                     ('FP', 'FALSE POSITIVES (ML flagged, Rules didnt)'),
                                     ('TP', 'TRUE POSITIVES (Both flagged)')]:
            txns = by_class[cls_name]
            if txns:
                report.append(f"\n{cls_label}")
                report.append("-" * 50)
                for txn in txns[:10]:  # Show top 10 of each
                    report.append(f"  TX #{txn['tx_id']}: ${txn['amount']:,.2f} at {txn['merchant']}")
                    report.append(f"    Category: {txn['category']} | Channel: {txn['channel']}")
                    report.append(f"    ML Score: {txn['ml_score']:.3f} | Source: {txn['prediction_source']}")
                    if txn['rules_triggered']:
                        report.append(f"    Rules: {', '.join(txn['rules_triggered'])}")
                    report.append(f"    Insight: {txn['audit_insight']}")
                    report.append("")

        # Summary JSON
        report.append("\n" + "=" * 70)
        report.append("SUMMARY DATA (JSON)")
        summary = {
            'total_transactions': len(rows),
            'false_negatives': len(by_class['FN']),
            'false_positives': len(by_class['FP']),
            'true_positives': len(by_class['TP']),
            'true_negatives': len(by_class['TN']),
        }
        report.append(json.dumps(summary, indent=2))

        return "\n".join(report)
