"""
Fraud reconciliation: compare transactions row count across pgd, ClickHouse,
RisingWave, and Kafka topic offsets. Fails the DAG run if drift > DRIFT_THRESHOLD_PCT.

For demonstration purposes only.

UC1 (OLTP) seeds pgd. UC2 (OLAP) wires Debezium -> Kafka -> CH + RW.
If those are healthy, all four counts should converge.

Schedule is intentionally tight (every 2 minutes) so a live demo can show a
scheduled run firing without standing around. Tune higher in production.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

PGD_DSN = dict(host="pgd", port=5432, user="postgres", password="secret", dbname="demo")
RW_DSN = dict(host="risingwave", port=4566, user="root", dbname="dev")
CH_URL = "http://clickhouse:8123/"
CH_AUTH = dict(user="default", password="admin123")
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "corebanking.public.transactions"

DRIFT_THRESHOLD_PCT = 0.01


@dag(
    dag_id="fraud_reconciliation",
    description="Diff transactions count across pgd / CH / RW / Kafka, alert on drift",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["bfsi", "reconciliation"],
)
def fraud_reconciliation():

    @task
    def count_pgd() -> int:
        import psycopg2
        with psycopg2.connect(**PGD_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM transactions")
            return int(cur.fetchone()[0])

    @task
    def count_clickhouse() -> int:
        import requests
        r = requests.post(CH_URL, params=CH_AUTH,
                          data="SELECT count() FROM transactions FINAL", timeout=15)
        r.raise_for_status()
        return int(r.text.strip())

    @task
    def count_risingwave() -> int:
        # Count distinct primary keys, not raw rows: RW's `transactions` is a
        # passthrough MV over the Kafka source, so it carries every Debezium
        # INSERT and UPDATE event. With pgd's check_fraud_rules() trigger
        # firing one UPDATE per row after each INSERT, raw count is ~6x the
        # logical row count. Distinct tx_id gives the real number.
        import psycopg2
        with psycopg2.connect(**RW_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(DISTINCT tx_id) FROM transactions")
            return int(cur.fetchone()[0])

    @task
    def count_kafka() -> int:
        # Return total Kafka offsets for the topic. This is total CDC EVENTS
        # (INSERT + every UPDATE), not logical row count — it'll be ~Nx the
        # pgd row count where N is avg updates per row. We keep this for
        # visibility but do NOT include Kafka in the drift threshold (see
        # diff_and_alert). Consuming + de-duping by key was OOM-killing the
        # scheduler on a 768MB limit because kafka-python buffers messages.
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP],
                                 consumer_timeout_ms=5000)
        parts = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in parts]
        if not tps:
            consumer.close()
            return 0
        ends = consumer.end_offsets(tps)
        consumer.close()
        return int(sum(ends.values()))

    @task
    def diff_and_alert(pgd: int, ch: int, rw: int, kafka: int) -> dict:
        # Drift threshold applies to row-level comparisons only: pgd vs CH (FINAL)
        # vs RW (count DISTINCT). Kafka is reported as an event-count metric for
        # visibility — its "natural" ratio to pgd is ~Nx where N = avg CDC events
        # per row (depends on trigger fan-out), so including it in the drift
        # threshold would force every demo to special-case the ratio.
        baseline = max(pgd, 1)
        row_drifts = {
            "ch": abs(pgd - ch) / baseline * 100,
            "rw": abs(pgd - rw) / baseline * 100,
        }
        kafka_ratio = (kafka / baseline) if baseline > 0 else 0
        report = {
            "pgd": pgd, "ch": ch, "rw": rw, "kafka_events": kafka,
            "row_drifts_pct": row_drifts,
            "kafka_event_ratio_to_pgd": round(kafka_ratio, 2),
        }
        print(f"reconciliation report: {report}")
        worst = max(row_drifts.values())
        if worst > DRIFT_THRESHOLD_PCT:
            print("=" * 60)
            print(f"DRIFT ALERT: {worst:.2f}% > threshold {DRIFT_THRESHOLD_PCT}%")
            print(f"  pgd={pgd}  ch={ch}  rw={rw}  kafka_events={kafka}")
            print("=" * 60)
            # Suppress Python frames in Airflow's task-failure traceback — only
            # the ValueError message remains under "Task failed with exception".
            import sys
            sys.tracebacklimit = 0
            raise ValueError(
                f"drift {worst:.2f}% exceeds threshold {DRIFT_THRESHOLD_PCT}% "
                f"(pgd={pgd} ch={ch} rw={rw}); kafka events={kafka}"
            )
        return report

    diff_and_alert(count_pgd(), count_clickhouse(), count_risingwave(), count_kafka())


fraud_reconciliation()
