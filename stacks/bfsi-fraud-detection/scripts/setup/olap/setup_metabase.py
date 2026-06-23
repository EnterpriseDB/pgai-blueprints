"""
BFSI Fraud Detection — Metabase Setup (staged)

For demonstration purposes only.

Single script invoked once per use case. Each stage adds its own dashboard /
data sources without disturbing earlier stages:

  python setup_metabase.py --stage oltp       (run after Usecase 1)
  python setup_metabase.py --stage olap       (run after Usecase 2)
  python setup_metabase.py --stage ml         (run after Usecase 3)
  python setup_metabase.py --stage genai      (Usecase 5 — stub)
  python setup_metabase.py --stage ai-gov     (Usecase 6 — stub)

OLTP stage  → "BFSI: OLTP Quick View" dashboard (PGD only, ~6 starter charts)
OLAP stage  → "Core Banking Fraud Detection"  dashboard (full 6-tab view)

Stages are isolated by dashboard name + question name prefix so re-running one
stage does not wipe another.
"""

import os
import sys
import time
import json
import re
import argparse
import requests
import uuid

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# ── Config ────────────────────────────────────────────────
METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
ADMIN_EMAIL = "admin@corebanking.local"
ADMIN_PASSWORD = "CoreBank1!"
SITE_NAME = "Core Banking Fraud Detection"

PGD_HOST = os.getenv("PGD_HOST", "pgd")
PGD_PORT = int(os.getenv("PGD_PORT", "5432"))
PGD_USER = os.getenv("PGD_USER", "postgres")
PGD_PASSWORD = os.getenv("PGD_PASSWORD", "secret")
PGD_DATABASE = os.getenv("PGD_DATABASE", "demo")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin123")

RW_HOST = os.getenv("RW_HOST", "risingwave")
RW_PORT = int(os.getenv("RW_PORT", "4566"))
RW_USER = os.getenv("RW_USER", "root")
RW_PASSWORD = os.getenv("RW_PASSWORD", "")
RW_DATABASE = os.getenv("RW_DATABASE", "dev")


# ── HTTP helpers ──────────────────────────────────────────

def api(method, path, payload=None, session=None):
    url = f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    fn = getattr(requests, method)
    kwargs = {"headers": headers, "timeout": 30}
    if payload is not None:
        kwargs["json"] = payload
    return fn(url, **kwargs)


def wait_for_metabase(timeout=300):
    """Wait for Metabase to be ready."""
    print(f"Waiting for Metabase at {METABASE_URL} ...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("  Metabase is ready.")
                return True
        except Exception:
            pass
        time.sleep(5)
    print("ERROR: Metabase did not become ready.")
    return False


# ── Setup / Login ─────────────────────────────────────────

def setup_metabase():
    """Complete initial setup or login if already configured."""
    r = api("get", "/api/session/properties")
    props = r.json() if r.status_code == 200 else {}
    setup_token = props.get("setup-token")

    if setup_token:
        print("First-time setup ...")
        payload = {
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "site_name": SITE_NAME,
            },
            "database": {
                "engine": "postgres",
                "name": "EDB PGD (OLTP + ML)",
                "details": {
                    "host": PGD_HOST,
                    "port": PGD_PORT,
                    "dbname": PGD_DATABASE,
                    "user": PGD_USER,
                    "password": PGD_PASSWORD,
                    "ssl": False,
                    "tunnel-enabled": False,
                },
            },
            "prefs": {
                "site_name": SITE_NAME,
                "site_locale": "en",
                "allow_tracking": False,
            },
        }
        r2 = api("post", "/api/setup", payload)
        if r2.status_code == 200:
            session = r2.json().get("id")
            print(f"  Setup complete. Session: {session[:8]}...")
            return session
        else:
            print(f"  Setup call returned {r2.status_code}: {r2.text[:200]}")
            # Fall through to login
    else:
        print("Metabase already configured — logging in ...")

    r3 = api("post", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if r3.status_code == 200:
        session = r3.json().get("id")
        print(f"  Logged in. Session: {session[:8]}...")
        return session
    else:
        print(f"ERROR: Login failed ({r3.status_code}): {r3.text[:200]}")
        sys.exit(1)


# ── Database management ───────────────────────────────────

def find_database(session, name_contains):
    """Find a database by name substring."""
    r = api("get", "/api/database", session=session)
    body = r.json()
    dbs = body if isinstance(body, list) else body.get("data", [])
    for db in dbs:
        if name_contains.lower() in db.get("name", "").lower() and not db.get("is_sample"):
            return db["id"]
    return None


def add_database(session, engine, name, details):
    """Add a database connection."""
    payload = {"engine": engine, "name": name, "details": details}
    r = api("post", "/api/database", payload, session=session)
    if r.status_code == 200:
        db_id = r.json().get("id")
        print(f"  Added database '{name}' (id={db_id})")
        return db_id
    else:
        print(f"  WARN: Add database '{name}' returned {r.status_code}: {r.text[:200]}")
        return None


def wait_for_sync(session, db_id, timeout=120):
    """Wait for database schema sync to complete."""
    start = time.time()
    while time.time() - start < timeout:
        r = api("get", f"/api/database/{db_id}", session=session)
        if r.status_code == 200:
            status = r.json().get("initial_sync_status", "")
            if status == "complete":
                return True
        time.sleep(5)
    print(f"  WARN: Sync timeout for db {db_id} — proceeding anyway.")
    return False


def ensure_databases(session):
    """Ensure all 3 data sources are connected. Returns dict of db IDs."""
    db_ids = {}

    # 1. PGD — may already exist from setup; 5-min socketTimeout for BERT cold-load
    pgd_details = {
        "host": PGD_HOST, "port": PGD_PORT, "dbname": PGD_DATABASE,
        "user": PGD_USER, "password": PGD_PASSWORD,
        "ssl": False, "tunnel-enabled": False,
        "additional-options": "socketTimeout=300",
    }
    pgd_id = find_database(session, "PGD")
    if not pgd_id:
        pgd_id = add_database(session, "postgres", "EDB PGD (OLTP + ML)", pgd_details)
    else:
        # Patch existing — needed when adding fields (e.g., socketTimeout) post-registration.
        api("put", f"/api/database/{pgd_id}", {"details": pgd_details}, session=session)
    db_ids["pgd"] = pgd_id
    print(f"  PGD database id: {pgd_id}")

    # 2. ClickHouse
    ch_id = find_database(session, "ClickHouse")
    if not ch_id:
        ch_id = add_database(session, "clickhouse", "ClickHouse (Analytics)", {
            "host": CH_HOST, "port": CH_PORT, "dbname": "default",
            "user": CH_USER, "password": CH_PASSWORD,
            "ssl": False, "tunnel-enabled": False,
        })
    db_ids["clickhouse"] = ch_id
    print(f"  ClickHouse database id: {ch_id}")

    # 3. RisingWave (uses postgres wire protocol)
    rw_id = find_database(session, "RisingWave")
    if not rw_id:
        rw_id = add_database(session, "postgres", "RisingWave (Streaming)", {
            "host": RW_HOST, "port": RW_PORT, "dbname": RW_DATABASE,
            "user": RW_USER, "password": RW_PASSWORD,
            "ssl": False, "tunnel-enabled": False,
        })
    db_ids["risingwave"] = rw_id
    print(f"  RisingWave database id: {rw_id}")

    # Wait for syncs
    for name, db_id in db_ids.items():
        if db_id:
            print(f"  Waiting for {name} sync ...")
            wait_for_sync(session, db_id, timeout=90)

    return db_ids


# ── Hidden collection ────────────────────────────────────

def ensure_hidden_collection(session):
    """Create or find a collection for CBS questions — keeps them off the main page."""
    coll_name = "CBS Internal (do not delete)"
    r = api("get", "/api/collection", session=session)
    if r.status_code == 200:
        for c in r.json():
            if c.get("name") == coll_name:
                return c["id"]

    r2 = api("post", "/api/collection", {"name": coll_name, "description": "Auto-generated questions for the Core Banking dashboard. Do not delete."}, session=session)
    if r2.status_code == 200:
        coll_id = r2.json().get("id")
        print(f"  Created hidden collection (id={coll_id})")
        return coll_id
    return None


# ── Cleanup ───────────────────────────────────────────────

def _delete_dashboards_named(session, names):
    """Delete dashboards whose name exactly matches any in `names`."""
    r = api("get", "/api/dashboard", session=session)
    if r.status_code != 200:
        return
    for d in r.json():
        if d.get("name") in names:
            api("delete", f"/api/dashboard/{d['id']}", session=session)
            print(f"  Deleted old dashboard: {d['name']}")


def _delete_cards_with_prefix(session, prefix):
    """Delete questions (cards) whose name starts with `prefix`."""
    r = api("get", "/api/card", session=session)
    if r.status_code != 200:
        return
    n = 0
    for c in r.json():
        if c.get("name", "").startswith(prefix):
            api("delete", f"/api/card/{c['id']}", session=session)
            n += 1
    if n:
        print(f"  Deleted {n} old questions with prefix '{prefix}'")


def cleanup_olap(session):
    """Remove OLAP-stage artifacts (legacy 'CBS:'-prefixed full dashboard)."""
    _delete_dashboards_named(session, [
        "Core Banking Fraud Detection",
        # Tolerate legacy variants
    ])
    _delete_cards_with_prefix(session, "CBS:")
    # Legacy collection
    r = api("get", "/api/collection", session=session)
    if r.status_code == 200:
        for c in r.json():
            if c.get("name", "").startswith("CBS Internal"):
                api("delete", f"/api/collection/{c['id']}", session=session)


def cleanup_oltp(session):
    """Remove OLTP-stage artifacts only (Quick View dashboard + 'OLTP:' cards)."""
    _delete_dashboards_named(session, ["BFSI: OLTP Quick View"])
    _delete_cards_with_prefix(session, "OLTP:")


# Backwards-compat alias used elsewhere in this script
def cleanup_old(session):
    cleanup_olap(session)


# ── Question builders ─────────────────────────────────────

# Set by main() before creating questions
_hidden_collection_id = None


def create_question(session, name, display, db_id, sql, viz_settings=None):
    """Create a saved question in the hidden collection."""
    payload = {
        "name": name,
        "display": display,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "visualization_settings": viz_settings or {},
    }
    if _hidden_collection_id:
        payload["collection_id"] = _hidden_collection_id
    r = api("post", "/api/card", payload, session=session)
    if r.status_code == 200:
        card_id = r.json().get("id")
        print(f"    + {name} (id={card_id})")
        return card_id
    else:
        print(f"    WARN: '{name}' failed ({r.status_code}): {r.text[:200]}")
        return None


# ══════════════════════════════════════════════════════════
#  TAB 1: OLTP Dashboard
# ══════════════════════════════════════════════════════════

def create_oltp_questions(session, pgd_id):
    cards = []

    cards.append(create_question(session, "CBS: Total Transactions", "scalar", pgd_id,
        "SELECT COUNT(*) AS total FROM transactions"))

    cards.append(create_question(session, "CBS: Total Volume", "scalar", pgd_id,
        "SELECT ROUND(SUM(amount)::numeric, 2) AS volume FROM transactions"))

    cards.append(create_question(session, "CBS: Avg Transaction", "scalar", pgd_id,
        "SELECT ROUND(AVG(amount)::numeric, 2) AS avg_tx FROM transactions"))

    cards.append(create_question(session, "CBS: TPS (Transactions per Second)", "scalar", pgd_id,
        """SELECT ROUND(COUNT(*)::numeric / 60, 2) AS tps
        FROM transactions
        WHERE received_at > NOW() - INTERVAL '1 minute'"""))

    cards.append(create_question(session, "CBS: Active DB Connections", "scalar", pgd_id,
        """SELECT COUNT(*) AS active
        FROM pg_stat_activity
        WHERE state = 'active' AND datname = current_database()"""))

    cards.append(create_question(session, "CBS: Cache Hit Ratio (%)", "scalar", pgd_id,
        """SELECT ROUND(100.0 * sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) AS ratio
        FROM pg_statio_user_tables WHERE relname = 'transactions'"""))

    cards.append(create_question(session, "CBS: Transaction Breakdown by Type", "bar", pgd_id,
        """SELECT type, COUNT(*) AS tx_count, ROUND(SUM(amount)::numeric, 2) AS volume
        FROM transactions GROUP BY type ORDER BY tx_count DESC""",
        {"graph.dimensions": ["type"], "graph.metrics": ["tx_count", "volume"]}))

    cards.append(create_question(session, "CBS: Volume Over Time (last 2h)", "line", pgd_id,
        """SELECT DATE_TRUNC('minute', created_at) AS minute,
            COUNT(*) AS tx_count, ROUND(SUM(amount)::numeric, 2) AS volume
        FROM transactions
        WHERE created_at > NOW() - INTERVAL '2 hours'
        GROUP BY minute ORDER BY minute ASC""",
        {"graph.dimensions": ["minute"], "graph.metrics": ["tx_count", "volume"],
         "graph.x_axis.title_text": "Time", "graph.y_axis.title_text": "Count / Volume"}))

    cards.append(create_question(session, "CBS: Commit Delay P95 (ms)", "scalar", pgd_id,
        """SELECT ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (received_at - created_at)) * 1000
        )::numeric, 2) AS p95_ms
        FROM transactions
        WHERE received_at > NOW() - INTERVAL '5 minutes'"""))

    cards.append(create_question(session, "CBS: Top Account Balances", "table", pgd_id,
        """SELECT a.account_id, c.name AS customer, a.account_type,
            ROUND(a.balance::numeric, 2) AS balance,
            ROUND(a.initial_balance::numeric, 2) AS initial_balance,
            a.bank, a.status
        FROM accounts a
        JOIN customers c ON a.customer_id = c.customer_id
        WHERE a.status = 'ACTIVE'
        ORDER BY a.balance DESC LIMIT 25"""))

    cards.append(create_question(session, "CBS: Recent Transactions", "table", pgd_id,
        """SELECT t.tx_id, c.name AS customer, t.type, t.merchant, t.category,
            ROUND(t.amount::numeric, 2) AS amount, t.channel,
            CASE WHEN fl.is_fraud THEN 'FRAUD' ELSE '' END AS fraud,
            t.created_at
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
        ORDER BY t.created_at DESC LIMIT 50"""))

    return cards


# ══════════════════════════════════════════════════════════
#  TAB 2: Fraud Detection (ML)
# ══════════════════════════════════════════════════════════

def create_fraud_questions(session, pgd_id):
    cards = []

    # ── [0] Fastest Path — the headline number ──
    cards.append(create_question(session, "CBS: Fastest Detection Path", "scalar", pgd_id,
        """SELECT COALESCE(
            (SELECT prediction_source || ' (' || ROUND(avg_ttdf::numeric, 0) || ' ms)'
             FROM (SELECT prediction_source, AVG(ttdf_milliseconds) AS avg_ttdf
                   FROM ml_fraud_predictions
                   WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000
                   GROUP BY prediction_source
                   ORDER BY avg_ttdf LIMIT 1) t),
            'Run ML Inference (Usecase 3)') AS fastest"""))

    # ── [1] Total ML Predictions ──
    cards.append(create_question(session, "CBS: Total ML Predictions", "scalar", pgd_id,
        "SELECT COUNT(*) AS total FROM ml_fraud_predictions"))

    # ── [2] ML Fraud Detected ──
    cards.append(create_question(session, "CBS: ML Fraud Detected", "scalar", pgd_id,
        "SELECT COUNT(*) AS fraud FROM ml_fraud_predictions WHERE is_fraud_predicted"))

    # ── [3] Rules-Based Detections ──
    cards.append(create_question(session, "CBS: Rules-Based Detections", "scalar", pgd_id,
        "SELECT COUNT(*) AS total FROM fraud_labels WHERE detection_source = 'rules'"))

    # ── [4-7] Per-path Avg TTDF scalars (the key comparison) ──
    for source, label in [("kafka", "Kafka ML"), ("clickhouse", "ClickHouse ML"),
                          ("risingwave", "RisingWave ML"), ("pgaa", "PGAA ML")]:
        cards.append(create_question(session, f"CBS: {label} Avg TTDF (ms)", "scalar", pgd_id,
            f"""SELECT COALESCE(
                ROUND(AVG(ttdf_milliseconds)::numeric, 0)::text,
                'N/A') AS avg_ttdf_ms
            FROM ml_fraud_predictions
            WHERE prediction_source = '{source}'
              AND ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000"""))

    # ── [8] TTDF Leaderboard — ranked table ──
    cards.append(create_question(session, "CBS: TTDF Leaderboard", "table", pgd_id,
        """SELECT * FROM (
            SELECT
                ROW_NUMBER() OVER (ORDER BY AVG(ttdf_milliseconds)) AS rank,
                prediction_source AS path,
                COUNT(*) AS predictions,
                COUNT(*) FILTER (WHERE is_fraud_predicted) AS fraud_found,
                ROUND(AVG(ttdf_milliseconds)::numeric, 0) AS avg_ttdf_ms,
                ROUND(MIN(ttdf_milliseconds)::numeric, 0) AS min_ttdf_ms,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 0) AS p95_ttdf_ms
            FROM ml_fraud_predictions
            WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000
            GROUP BY prediction_source
            UNION ALL
            SELECT 0, 'Run ML Inference (Usecase 3)', 0, 0, NULL, NULL, NULL
            WHERE NOT EXISTS (SELECT 1 FROM ml_fraud_predictions LIMIT 1)
        ) t ORDER BY rank"""))

    # ── [9] TTDF Performance bar chart ──
    cards.append(create_question(session, "CBS: TTDF by Path", "bar", pgd_id,
        """SELECT * FROM (
            SELECT prediction_source AS path,
                ROUND(AVG(ttdf_milliseconds) FILTER (WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000)::numeric, 0) AS avg_ttdf_ms,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)
                    FILTER (WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000)::numeric, 0) AS p95_ttdf_ms
            FROM ml_fraud_predictions
            GROUP BY prediction_source
            UNION ALL
            SELECT 'No ML Data - Run Usecase 3', 0, 0
            WHERE NOT EXISTS (SELECT 1 FROM ml_fraud_predictions LIMIT 1)
        ) t ORDER BY avg_ttdf_ms DESC""",
        {"graph.dimensions": ["path"], "graph.metrics": ["avg_ttdf_ms", "p95_ttdf_ms"],
         "graph.x_axis.title_text": "ML Path", "graph.y_axis.title_text": "TTDF (ms)"}))

    # ── [10] TTDF Comparison Over Time ──
    cards.append(create_question(session, "CBS: TTDF Over Time", "line", pgd_id,
        """WITH ml_buckets AS (
            SELECT DATE_TRUNC('minute', predicted_at) AS bucket, prediction_source,
                ROUND(AVG(ttdf_milliseconds)::numeric, 2) AS avg_ttdf
            FROM ml_fraud_predictions
            WHERE ttdf_milliseconds IS NOT NULL AND ttdf_milliseconds < 10000
            GROUP BY bucket, prediction_source
        ),
        rules_buckets AS (
            SELECT DATE_TRUNC('minute', detected_at) AS bucket,
                ROUND(AVG(ttdf_milliseconds)::numeric, 2) AS avg_ttdf
            FROM fraud_labels
            WHERE detection_source = 'rules' AND ttdf_milliseconds IS NOT NULL
            GROUP BY bucket
        )
        SELECT COALESCE(m.bucket, r.bucket) AS time,
            MAX(CASE WHEN prediction_source = 'kafka' THEN m.avg_ttdf END) AS kafka_ms,
            MAX(CASE WHEN prediction_source = 'clickhouse' THEN m.avg_ttdf END) AS clickhouse_ms,
            MAX(CASE WHEN prediction_source = 'risingwave' THEN m.avg_ttdf END) AS risingwave_ms,
            MAX(CASE WHEN prediction_source = 'pgaa' THEN m.avg_ttdf END) AS pgaa_ms,
            MAX(r.avg_ttdf) AS rules_ms
        FROM ml_buckets m
        FULL OUTER JOIN rules_buckets r ON m.bucket = r.bucket
        GROUP BY COALESCE(m.bucket, r.bucket)
        ORDER BY time DESC LIMIT 60""",
        {"graph.dimensions": ["time"],
         "graph.metrics": ["kafka_ms", "clickhouse_ms", "risingwave_ms", "pgaa_ms", "rules_ms"],
         "graph.x_axis.title_text": "Time", "graph.y_axis.title_text": "Avg TTDF (ms)"}))

    # ── [11] Predictions by Source bar ──
    cards.append(create_question(session, "CBS: Predictions by Source", "bar", pgd_id,
        """SELECT * FROM (
            SELECT prediction_source AS source,
                COUNT(*) AS predictions,
                COUNT(*) FILTER (WHERE is_fraud_predicted) AS fraud_detected
            FROM ml_fraud_predictions
            GROUP BY prediction_source
            UNION ALL
            SELECT 'No ML Data - Run Usecase 3', 0, 0
            WHERE NOT EXISTS (SELECT 1 FROM ml_fraud_predictions LIMIT 1)
        ) t ORDER BY predictions DESC""",
        {"graph.dimensions": ["source"], "graph.metrics": ["predictions", "fraud_detected"],
         "graph.x_axis.title_text": "ML Path", "graph.y_axis.title_text": "Count"}))

    # ── [12] ML vs Rules Agreement pie ──
    cards.append(create_question(session, "CBS: ML vs Rules Agreement", "pie", pgd_id,
        """SELECT * FROM (
            SELECT
                CASE
                    WHEN ml.is_fraud_predicted = fl.is_fraud THEN 'Agreement'
                    WHEN ml.is_fraud_predicted AND NOT fl.is_fraud THEN 'ML False Positive'
                    WHEN NOT ml.is_fraud_predicted AND fl.is_fraud THEN 'ML False Negative'
                    ELSE 'Unknown'
                END AS detection_agreement,
                COUNT(*) AS count
            FROM ml_fraud_predictions ml
            JOIN fraud_labels fl ON ml.tx_id = fl.tx_id AND fl.detection_source = 'rules'
            GROUP BY detection_agreement
            UNION ALL
            SELECT 'No ML Data - Run Usecase 3', 1
            WHERE NOT EXISTS (SELECT 1 FROM ml_fraud_predictions LIMIT 1)
        ) t""",
        {"pie.dimension": "detection_agreement", "pie.metric": "count"}))

    # ── [13] Detection Results table ──
    cards.append(create_question(session, "CBS: Detection Results", "table", pgd_id,
        """SELECT * FROM (
            SELECT p.tx_id, p.prediction_source AS source, c.name AS customer,
                t.merchant, ROUND(t.amount::numeric, 2) AS amount,
                ROUND(p.fraud_probability::numeric, 4) AS ml_score,
                CASE WHEN p.is_fraud_predicted THEN 'FRAUD' ELSE 'OK' END AS ml_result,
                CASE WHEN fl.is_fraud THEN 'FRAUD' ELSE 'OK' END AS rules,
                p.ttdf_milliseconds AS ttdf_ms,
                a.alert_severity,
                p.predicted_at
            FROM ml_fraud_predictions p
            JOIN transactions t ON p.tx_id = t.tx_id
            LEFT JOIN customers c ON t.customer_id = c.customer_id
            LEFT JOIN fraud_labels fl ON p.tx_id = fl.tx_id AND fl.detection_source = 'rules'
            LEFT JOIN ml_fraud_alerts a ON p.tx_id = a.tx_id AND p.prediction_source = a.prediction_source
            UNION ALL
            SELECT NULL, 'No ML predictions yet', 'Run Usecase 3 to start ML inference', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            WHERE NOT EXISTS (SELECT 1 FROM ml_fraud_predictions LIMIT 1)
        ) t ORDER BY predicted_at DESC NULLS LAST LIMIT 100"""))

    # ── [14] Fraud Alerts ──
    cards.append(create_question(session, "CBS: Fraud Alerts", "table", pgd_id,
        """SELECT a.tx_id, a.prediction_source AS source,
            ROUND(a.fraud_probability::numeric, 4) AS score,
            a.alert_severity, c.name AS customer,
            t.merchant, ROUND(t.amount::numeric, 2) AS amount,
            a.alert_sent_at
        FROM ml_fraud_alerts a
        JOIN transactions t ON a.tx_id = t.tx_id
        LEFT JOIN customers c ON t.customer_id = c.customer_id
        ORDER BY a.alert_sent_at DESC LIMIT 50"""))

    return cards


# ══════════════════════════════════════════════════════════
#  TAB 3: Analytics (ClickHouse)
# ══════════════════════════════════════════════════════════

def create_analytics_questions(session, ch_id):
    cards = []

    cards.append(create_question(session, "CBS: Analytics — Total Transactions", "scalar", ch_id,
        "SELECT count() AS total FROM default.transactions FINAL"))

    cards.append(create_question(session, "CBS: Analytics — Total Volume", "scalar", ch_id,
        "SELECT round(sum(assumeNotNull(amount)), 2) AS volume FROM default.transactions FINAL"))

    cards.append(create_question(session, "CBS: Analytics — Avg Transaction", "scalar", ch_id,
        "SELECT round(avg(assumeNotNull(amount)), 2) AS avg_tx FROM default.transactions FINAL"))

    cards.append(create_question(session, "CBS: Analytics — Fraud Rate (%)", "scalar", ch_id,
        """SELECT round(countIf(fl.is_fraud = 1) * 100.0 / count(), 2) AS fraud_pct
        FROM default.transactions t FINAL
        LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id"""))

    cards.append(create_question(session, "CBS: Analytics — Volume by Hour", "line", ch_id,
        """SELECT toStartOfHour(created_at) AS hour,
            count() AS tx_count, round(sum(assumeNotNull(amount)), 2) AS volume
        FROM default.transactions FINAL
        WHERE created_at >= now() - INTERVAL 48 HOUR
        GROUP BY hour ORDER BY hour ASC""",
        {"graph.dimensions": ["hour"], "graph.metrics": ["tx_count", "volume"],
         "graph.x_axis.title_text": "Hour", "graph.y_axis.title_text": "Count / Volume"}))

    cards.append(create_question(session, "CBS: Analytics — TX by Type", "pie", ch_id,
        """SELECT type, count() AS tx_count, round(sum(assumeNotNull(amount)), 2) AS volume
        FROM default.transactions FINAL WHERE type IS NOT NULL
        GROUP BY type ORDER BY tx_count DESC""",
        {"pie.dimension": "type", "pie.metric": "tx_count"}))

    cards.append(create_question(session, "CBS: Analytics — Fraud Trend", "line", ch_id,
        """SELECT toDate(t.created_at) AS day, count() AS total,
            countIf(fl.is_fraud = 1) AS fraud_count,
            round(countIf(fl.is_fraud = 1) * 100.0 / count(), 2) AS fraud_pct
        FROM default.transactions t FINAL
        LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
        GROUP BY day ORDER BY day ASC""",
        {"graph.dimensions": ["day"], "graph.metrics": ["fraud_pct"],
         "graph.x_axis.title_text": "Day", "graph.y_axis.title_text": "Fraud Rate (%)"}))

    cards.append(create_question(session, "CBS: Analytics — TX Velocity (per min)", "line", ch_id,
        """SELECT toStartOfMinute(created_at) AS minute,
            count() AS tx_count, round(sum(assumeNotNull(amount)), 2) AS volume
        FROM default.transactions FINAL
        WHERE created_at >= now() - INTERVAL 60 MINUTE
        GROUP BY minute ORDER BY minute ASC""",
        {"graph.dimensions": ["minute"], "graph.metrics": ["tx_count"],
         "graph.x_axis.title_text": "Minute", "graph.y_axis.title_text": "TX Count"}))

    cards.append(create_question(session, "CBS: Analytics — Top Merchants", "table", ch_id,
        """SELECT t.merchant, count() AS tx_count,
            round(sum(assumeNotNull(t.amount)), 2) AS total_volume,
            countIf(fl.is_fraud = 1) AS fraud_count
        FROM default.transactions t FINAL
        LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
        WHERE t.merchant IS NOT NULL AND length(t.merchant) > 0
        GROUP BY t.merchant ORDER BY total_volume DESC LIMIT 15"""))

    cards.append(create_question(session, "CBS: Analytics — By Category", "bar", ch_id,
        """SELECT category, count() AS tx_count, round(sum(assumeNotNull(amount)), 2) AS volume
        FROM default.transactions FINAL WHERE category IS NOT NULL
        GROUP BY category ORDER BY tx_count DESC LIMIT 12""",
        {"graph.dimensions": ["category"], "graph.metrics": ["tx_count", "volume"]}))

    cards.append(create_question(session, "CBS: Analytics — By Channel", "bar", ch_id,
        """SELECT channel, count() AS tx_count, round(sum(assumeNotNull(amount)), 2) AS volume
        FROM default.transactions FINAL WHERE channel IS NOT NULL
        GROUP BY channel ORDER BY tx_count DESC""",
        {"graph.dimensions": ["channel"], "graph.metrics": ["tx_count", "volume"]}))

    cards.append(create_question(session, "CBS: Analytics — Top Customers", "table", ch_id,
        """SELECT t.customer_id, any(c.name) AS name,
            count() AS tx_count, round(sum(assumeNotNull(t.amount)), 2) AS volume,
            countIf(fl.is_fraud = 1) AS fraud_count
        FROM default.transactions t FINAL
        LEFT JOIN default.customers c ON t.customer_id = c.customer_id
        LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
        GROUP BY t.customer_id ORDER BY volume DESC LIMIT 10"""))

    return cards


# ══════════════════════════════════════════════════════════
#  TAB 4: Streaming vs Batch Comparison
# ══════════════════════════════════════════════════════════

def create_comparison_questions(session, pgd_id, ch_id, rw_id):
    cards = []

    cards.append(create_question(session, "CBS: Comparison — PG Row Count", "scalar", pgd_id,
        "SELECT count(*) AS total FROM transactions"))

    cards.append(create_question(session, "CBS: Comparison — CH Row Count", "scalar", ch_id,
        "SELECT count() AS total FROM default.transactions"))

    cards.append(create_question(session, "CBS: Comparison — RW Row Count", "scalar", rw_id,
        "SELECT count(*) AS total FROM transactions"))

    # Replication lag calculation (PG source count for reference)
    cards.append(create_question(session, "CBS: Comparison — PG Source Count", "scalar", pgd_id,
        "SELECT count(*) AS pg_rows FROM transactions"))

    cards.append(create_question(session, "CBS: Comparison — RW Type Breakdown", "bar", rw_id,
        """SELECT type, tx_count AS cnt, total_amount AS vol FROM mv_type_breakdown ORDER BY tx_count DESC""",
        {"graph.dimensions": ["type"], "graph.metrics": ["cnt", "vol"]}))

    cards.append(create_question(session, "CBS: Comparison — CH Type Breakdown", "bar", ch_id,
        """SELECT type, count() AS cnt, round(sum(assumeNotNull(amount)), 2) AS vol
        FROM default.transactions FINAL GROUP BY type ORDER BY cnt DESC""",
        {"graph.dimensions": ["type"], "graph.metrics": ["cnt", "vol"]}))

    cards.append(create_question(session, "CBS: Comparison — RW TX per Minute", "line", rw_id,
        """SELECT minute, tx_count FROM mv_tx_per_minute ORDER BY minute DESC LIMIT 30""",
        {"graph.dimensions": ["minute"], "graph.metrics": ["tx_count"]}))

    cards.append(create_question(session, "CBS: Comparison — CH TX per Minute", "line", ch_id,
        """SELECT toStartOfMinute(created_at) AS minute, count() AS tx_count
        FROM default.transactions FINAL
        WHERE created_at >= now() - INTERVAL 2 HOUR
        GROUP BY minute ORDER BY minute DESC LIMIT 30""",
        {"graph.dimensions": ["minute"], "graph.metrics": ["tx_count"]}))

    return cards


# ══════════════════════════════════════════════════════════
#  TAB 5: Kafka CDC
# ══════════════════════════════════════════════════════════

def create_cdc_questions(session, pgd_id, ch_id):
    cards = []

    cards.append(create_question(session, "CBS: CDC — PG Transactions", "scalar", pgd_id,
        "SELECT count(*) AS total FROM transactions"))

    cards.append(create_question(session, "CBS: CDC — CH Analytics Rows", "scalar", ch_id,
        "SELECT count() AS total FROM default.transactions"))

    cards.append(create_question(session, "CBS: CDC — CH CDC Rows", "scalar", ch_id,
        "SELECT count() AS total FROM default.mv_kafka_transactions"))

    cards.append(create_question(session, "CBS: CDC — PG vs CH Combined", "table", pgd_id,
        """SELECT topic, pg_rows FROM (
            SELECT 'customers' AS topic, (SELECT count(*) FROM customers) AS pg_rows
            UNION ALL
            SELECT 'accounts', (SELECT count(*) FROM accounts)
            UNION ALL
            SELECT 'transactions', (SELECT count(*) FROM transactions)
        ) t ORDER BY topic"""))

    cards.append(create_question(session, "CBS: CDC — CH Topic Counts", "table", ch_id,
        """SELECT 'customers' AS topic, count() AS ch_rows FROM default.customers
        UNION ALL
        SELECT 'accounts', count() FROM default.accounts
        UNION ALL
        SELECT 'transactions', count() FROM default.transactions
        UNION ALL
        SELECT 'kafka_transactions (CDC)', count() FROM default.mv_kafka_transactions"""))

    cards.append(create_question(session, "CBS: CDC — PG Replication Slots", "table", pgd_id,
        """SELECT slot_name, plugin, slot_type, active,
            pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
        FROM pg_replication_slots"""))

    return cards


# ══════════════════════════════════════════════════════════
#  TAB 6: Transaction Search
# ══════════════════════════════════════════════════════════

def create_search_questions(session, pgd_id):
    cards = []

    cards.append(create_question(session, "CBS: Search — All Transactions", "table", pgd_id,
        """SELECT t.tx_id, c.name AS customer, t.customer_id,
            t.account_id, t.type, t.merchant, t.category,
            ROUND(t.amount::numeric, 2) AS amount,
            ROUND(t.balance_after::numeric, 2) AS balance_after,
            t.channel, t.reference_no,
            CASE WHEN fl.is_fraud THEN 'FRAUD' ELSE '' END AS fraud,
            fl.fraud_reason, t.status, t.created_at
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
        ORDER BY t.created_at DESC LIMIT 500"""))

    cards.append(create_question(session, "CBS: Search — Fraud Transactions", "table", pgd_id,
        """SELECT t.tx_id, c.name AS customer, t.type, t.merchant,
            t.category, ROUND(t.amount::numeric, 2) AS amount,
            t.channel, fl.fraud_reason, t.created_at
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
        WHERE fl.is_fraud = TRUE
        ORDER BY t.created_at DESC LIMIT 200"""))

    cards.append(create_question(session, "CBS: Search — High Value (>$5000)", "table", pgd_id,
        """SELECT t.tx_id, c.name AS customer, t.type, t.merchant,
            t.category, ROUND(t.amount::numeric, 2) AS amount,
            t.channel, CASE WHEN fl.is_fraud THEN 'FRAUD' ELSE '' END AS fraud,
            t.created_at
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
        WHERE t.amount > 5000
        ORDER BY t.amount DESC LIMIT 200"""))

    return cards


# ══════════════════════════════════════════════════════════
#  Dashboard with tabs
# ══════════════════════════════════════════════════════════

def create_dashboard(session, all_cards):
    """Create the main dashboard with 6 tabs — default width, section headings."""
    r = api("post", "/api/dashboard", {
        "name": "Core Banking Fraud Detection",
        "description": "Real-time fraud detection across 4 ML inference paths (Kafka, ClickHouse, RisingWave, PGAA) with CDC replication monitoring.",
    }, session=session)
    if r.status_code != 200:
        print(f"ERROR: Create dashboard failed ({r.status_code}): {r.text[:200]}")
        return None
    dash_id = r.json().get("id")
    print(f"  Dashboard created (id={dash_id})")

    # Standard 18-column Metabase grid (default width)
    W = 18

    tabs = [
        {"id": -1, "name": "OLTP Dashboard"},
        {"id": -2, "name": "Fraud Detection"},
        {"id": -3, "name": "Analytics"},
        {"id": -4, "name": "Streaming vs Batch"},
        {"id": -5, "name": "Kafka CDC"},
        {"id": -6, "name": "Transaction Search"},
    ]

    dashcards = []
    card_counter = -1

    def add_card(card_id, tab_id, row, col, w, h):
        nonlocal card_counter
        if card_id is None:
            return
        dashcards.append({
            "id": card_counter,
            "card_id": card_id,
            "dashboard_tab_id": tab_id,
            "row": row, "col": col,
            "size_x": w, "size_y": h,
        })
        card_counter -= 1

    def add_heading(tab_id, row, col, w, text):
        """Add a section heading (Metabase text card, no underlying question)."""
        nonlocal card_counter
        dashcards.append({
            "id": card_counter,
            "card_id": None,
            "dashboard_tab_id": tab_id,
            "row": row, "col": col,
            "size_x": w, "size_y": 1,
            "visualization_settings": {
                "virtual_card": {
                    "name": None,
                    "display": "heading",
                    "visualization_settings": {},
                    "dataset_query": {},
                    "archived": False,
                },
                "text": text,
            },
        })
        card_counter -= 1

    # ═══════════════ Tab 1: OLTP Dashboard ═══════════════
    # 11 cards: [0]TotalTX [1]Volume [2]AvgTX [3]TPS [4]Conns
    #           [5]Cache [6]Breakdown [7]VolTime [8]P95 [9]Balances [10]Recent
    t = -1
    c = all_cards["oltp"]
    r_ = 0
    add_card(c[0], t, r_, 0, 6, 4)       # Total TX
    add_card(c[1], t, r_, 6, 6, 4)       # Total Volume
    add_card(c[2], t, r_, 12, 6, 4)      # Avg TX
    r_ = 4
    add_card(c[3], t, r_, 0, 6, 4)       # TPS
    add_card(c[4], t, r_, 6, 6, 4)       # Active Connections
    add_card(c[5], t, r_, 12, 6, 4)      # Cache Hit
    r_ = 8
    add_heading(t, r_, 0, W, "Transaction Activity")
    r_ = 9
    add_card(c[6], t, r_, 0, 11, 8)      # TX Breakdown bar
    add_card(c[7], t, r_, 11, 7, 8)      # Volume Over Time
    r_ = 17
    add_card(c[8], t, r_, 0, 5, 4)       # Commit Delay P95
    add_card(c[9], t, r_, 5, 13, 8)      # Top Account Balances
    r_ = 25
    add_heading(t, r_, 0, W, "Recent Transactions")
    r_ = 26
    add_card(c[10], t, r_, 0, W, 10)     # Recent TX table

    # ═══════════════ Tab 2: Fraud Detection ═══════════════
    # 15 cards: [0]Fastest [1]TotalPred [2]FraudDet [3]Rules
    #           [4]KafkaTTDF [5]CHTTDF [6]RWTTDF [7]PGAATTDF
    #           [8]Leaderboard [9]TTDFbar [10]TTDFline [11]PredBySource
    #           [12]Agreement [13]Results [14]Alerts
    t = -2
    c = all_cards["fraud"]
    r_ = 0
    # Hero row — "Fastest Path" prominently + key counts
    add_card(c[0], t, r_, 0, 6, 4)       # Fastest Detection Path (headline)
    add_card(c[1], t, r_, 6, 4, 4)       # Total ML Predictions
    add_card(c[2], t, r_, 10, 4, 4)      # ML Fraud Detected
    add_card(c[3], t, r_, 14, 4, 4)      # Rules-Based Count
    r_ = 4
    add_heading(t, r_, 0, W, "Who Detects Fraud Fastest?")
    r_ = 5
    # 4 per-path TTDF scalars — instant visual comparison
    add_card(c[4], t, r_, 0, 5, 4)       # Kafka Avg TTDF
    add_card(c[5], t, r_, 5, 5, 4)       # ClickHouse Avg TTDF
    add_card(c[6], t, r_, 10, 4, 4)      # RisingWave Avg TTDF
    add_card(c[7], t, r_, 14, 4, 4)      # PGAA Avg TTDF
    r_ = 9
    # Leaderboard + TTDF bar side by side
    add_card(c[8], t, r_, 0, 9, 8)       # TTDF Leaderboard table
    add_card(c[9], t, r_, 9, 9, 8)       # TTDF by Path bar chart
    r_ = 17
    add_heading(t, r_, 0, W, "TTDF Over Time")
    r_ = 18
    add_card(c[10], t, r_, 0, W, 9)      # TTDF Over Time line
    r_ = 27
    add_heading(t, r_, 0, W, "Detection Volume & Agreement")
    r_ = 28
    add_card(c[11], t, r_, 0, 10, 8)     # Predictions by Source bar
    add_card(c[12], t, r_, 10, 8, 8)     # ML vs Rules Agreement pie
    r_ = 36
    add_heading(t, r_, 0, W, "Detection Details")
    r_ = 37
    add_card(c[13], t, r_, 0, W, 10)     # Detection Results table
    r_ = 47
    add_card(c[14], t, r_, 0, W, 8)      # Fraud Alerts table

    # ═══════════════ Tab 3: Analytics (ClickHouse) ═══════════════
    # 12 cards. Tab is skipped entirely when ClickHouse isn't a configured
    # Metabase database (card-creation phase logs "SKIP: ClickHouse not
    # connected" and the cards list ends up empty). Without the guard we'd
    # IndexError on c[0] and crash before Tabs 4-6 can be built.
    t = -3
    c = all_cards.get("analytics") or []
    if c:
        r_ = 0
        add_card(c[0], t, r_, 0, 5, 4)       # Total TX
        add_card(c[1], t, r_, 5, 5, 4)       # Total Volume
        add_card(c[2], t, r_, 10, 4, 4)      # Avg TX
        add_card(c[3], t, r_, 14, 4, 4)      # Fraud Rate
        r_ = 4
        add_heading(t, r_, 0, W, "Volume & Trends")
        r_ = 5
        add_card(c[4], t, r_, 0, 11, 8)      # Volume by Hour
        add_card(c[5], t, r_, 11, 7, 8)      # TX by Type pie
        r_ = 13
        add_card(c[6], t, r_, 0, 9, 8)       # Fraud Trend
        add_card(c[7], t, r_, 9, 9, 8)       # TX Velocity
        r_ = 21
        add_heading(t, r_, 0, W, "Top Performers")
        r_ = 22
        add_card(c[8], t, r_, 0, W, 8)       # Top Merchants
        r_ = 30
        add_card(c[9], t, r_, 0, 9, 8)       # By Category
        add_card(c[10], t, r_, 9, 9, 8)      # By Channel
        r_ = 38
        add_card(c[11], t, r_, 0, W, 8)      # Top Customers

    # ═══════════════ Tab 4: Streaming vs Batch ═══════════════
    # 8 cards: [0]PG rows [1]CH rows [2]RW rows [3]PG source (for lag calc)
    #          [4]RW Type [5]CH Type [6]RW TX/min [7]CH TX/min
    # Needs both ClickHouse AND RisingWave; same skip pattern as Tab 3.
    t = -4
    c = all_cards.get("comparison") or []
    if c:
        r_ = 0
        add_heading(t, r_, 0, W, "Row Counts (Replication Lag = PG - Target)")
        r_ = 1
        add_card(c[0], t, r_, 0, 6, 4)       # PG rows
        add_card(c[1], t, r_, 6, 6, 4)       # CH rows
        add_card(c[2], t, r_, 12, 6, 4)      # RW rows
        r_ = 5
        add_heading(t, r_, 0, W, "Type Breakdown")
        r_ = 6
        add_card(c[4], t, r_, 0, 9, 8)       # RW Type Breakdown
        add_card(c[5], t, r_, 9, 9, 8)       # CH Type Breakdown
        r_ = 14
        add_heading(t, r_, 0, W, "Throughput (TX per Minute)")
        r_ = 15
        add_card(c[6], t, r_, 0, 9, 8)       # RW TX/min
        add_card(c[7], t, r_, 9, 9, 8)       # CH TX/min

    # ═══════════════ Tab 5: Kafka CDC ═══════════════
    # 6 cards — needs ClickHouse. Skip if empty.
    t = -5
    c = all_cards.get("cdc") or []
    if c:
        r_ = 0
        add_card(c[0], t, r_, 0, 6, 4)       # PG TX
        add_card(c[1], t, r_, 6, 6, 4)       # CH Analytics
        add_card(c[2], t, r_, 12, 6, 4)      # CH CDC
        r_ = 4
        add_heading(t, r_, 0, W, "Row Counts by Topic")
        r_ = 5
        add_card(c[3], t, r_, 0, 9, 8)       # PG topic counts
        add_card(c[4], t, r_, 9, 9, 8)       # CH topic counts
        r_ = 13
        add_heading(t, r_, 0, W, "Replication Slots")
        r_ = 14
        add_card(c[5], t, r_, 0, W, 8)       # PG Replication Slots

    # ═══════════════ Tab 6: Transaction Search ═══════════════
    # 3 cards
    t = -6
    c = all_cards["search"]
    r_ = 0
    add_heading(t, r_, 0, W, "All Transactions")
    r_ = 1
    add_card(c[0], t, r_, 0, W, 12)      # All TX
    r_ = 13
    add_heading(t, r_, 0, W, "Fraud Transactions")
    r_ = 14
    add_card(c[1], t, r_, 0, W, 10)      # Fraud TX
    r_ = 24
    add_heading(t, r_, 0, W, "High Value (> $5,000)")
    r_ = 25
    add_card(c[2], t, r_, 0, W, 10)      # High Value TX

    # Update dashboard with tabs and cards
    r2 = api("put", f"/api/dashboard/{dash_id}", {
        "tabs": tabs,
        "dashcards": dashcards,
    }, session=session)

    if r2.status_code == 200:
        print(f"  Dashboard layout updated: {len(tabs)} tabs, {len(dashcards)} cards.")
    else:
        print(f"  WARN: Dashboard update returned {r2.status_code}: {r2.text[:300]}")

    return dash_id


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════
#  Hybrid Search Demo — VectorChord-BM25 vs AIDB BERT vs RRF
# ══════════════════════════════════════════════════════════

_PSQL_META_RE = re.compile(r"^\s*\\[a-zA-Z].*$", re.MULTILINE)
HYBRID_SQL_PATH = "/app/setup-hybrid-search.sql"


def _build_hybrid_search_index():
    """Apply setup-hybrid-search.sql to pgd: extensions, BM25 column/trigger,
    AIDB pipeline, transactions_hybrid_search() function. Then run BM25
    backfill in batched UPDATEs from Python (the per-batch COMMITs can't live
    inside a SQL DO block via psycopg2's multi-statement execute)."""
    if not HAS_PSYCOPG2:
        print("  SKIP: psycopg2 not available")
        return False
    if not os.path.exists(HYBRID_SQL_PATH):
        print(f"  SKIP: {HYBRID_SQL_PATH} not mounted")
        return False
    sql = _PSQL_META_RE.sub("", open(HYBRID_SQL_PATH).read())
    conn = psycopg2.connect(
        host=PGD_HOST, port=PGD_PORT, user=PGD_USER,
        password=PGD_PASSWORD, dbname=PGD_DATABASE,
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
        print("  ok BM25 column + AIDB pipeline + RRF function created")
    except Exception as e:
        print(f"  WARN: hybrid search SQL setup failed: {e}")
        cur.close(); conn.close()
        return False

    # BM25 backfill — batched. Single-large UPDATE OOMs pgd's tokenizer
    # (BERT model ~1.2GB resident + per-row working set). Batched UPDATEs
    # with explicit commit per batch let memory drop between iters.
    # Tuneable via env vars so power users on bigger hosts can scale up:
    #   HYBRID_BM25_CORPUS  — total rows to tokenize (default 2000)
    #   HYBRID_BM25_BATCH   — rows per UPDATE batch (default 200)
    # The defaults work on the BFSI-spec floor (32 GB host, 4 GB pgd).
    TOTAL_TARGET = int(os.environ.get("HYBRID_BM25_CORPUS", "2000"))
    BATCH_SIZE   = int(os.environ.get("HYBRID_BM25_BATCH", "200"))
    backfilled = 0
    try:
        while backfilled < TOTAL_TARGET:
            cur.execute("""
                WITH demo_batch AS (
                    SELECT tx_id FROM transactions
                    WHERE bm25_tokens IS NULL
                    ORDER BY tx_id DESC
                    LIMIT %s
                )
                UPDATE transactions t SET bm25_tokens = tokenizer_catalog.tokenize(
                    coalesce(t.description,'') || ' ' ||
                    coalesce(t.remarks,'')     || ' ' ||
                    coalesce(t.merchant,'')    || ' ' ||
                    coalesce(t.category,''),
                    'bert'
                )
                FROM demo_batch s
                WHERE t.tx_id = s.tx_id;
            """, (BATCH_SIZE,))
            updated = cur.rowcount
            if updated == 0:
                break
            backfilled += updated
            print(f"  BM25 backfill: {backfilled} rows tokenized so far")
        print(f"  ok BM25 backfill complete: {backfilled} rows tokenized")
    except Exception as e:
        print(f"  WARN: BM25 backfill failed at {backfilled} rows: {e}")
        if backfilled == 0:
            cur.close(); conn.close()
            return False
        # Partial backfill is still usable — fall through to AIDB kickoff.

    # Populate the bounded AIDB corpus table from BM25-tokenized rows, then
    # kick off the AIDB pipeline. The corpus table caps AIDB's embedding work
    # to ~2000 rows (vs. 127k if pointed at the full transactions table).
    try:
        cur.execute("""
            INSERT INTO public.transactions_hybrid_corpus (tx_id, search_content)
            SELECT tx_id, search_content
            FROM transactions
            WHERE bm25_tokens IS NOT NULL
            ON CONFLICT (tx_id) DO NOTHING;
        """)
        corpus_rows = cur.rowcount
        print(f"  ok AIDB corpus populated: {corpus_rows} rows")
    except Exception as e:
        print(f"  WARN: AIDB corpus populate failed: {e} — AIDB+RRF cards will be empty")
        cur.close(); conn.close()
        return True

    # AIDB pipeline kickoff. On AIDB 7.4.0 + BDR 6.3.1 + EPE 17.10, run_pipeline
    # intermittently fails with "value X is out of range for type integer"
    # because BDR allocates a snowflake bigint where AIDB internals expect an
    # int4. Try run_pipeline first; if it errors, fall back to a manual
    # encode_text loop that writes the destination rows directly. The fallback
    # produces identical embeddings (same 'bert' model, same vector shape) so
    # the Semantic / RRF cards work either way.
    aidb_done = False
    try:
        cur.execute("SELECT aidb.run_pipeline('transactions_kb');")
        print(f"  ok AIDB pipeline kicked off (embedding {corpus_rows} rows in background)")
        aidb_done = True
    except Exception as e:
        print(f"  WARN: aidb.run_pipeline failed ({e}) — falling back to manual encode_text loop")
        conn.rollback()
    if not aidb_done:
        try:
            # Look up the destination's pipeline_id (registry assigns it on
            # create_pipeline). Needed because the manual insert can't rely on
            # AIDB's internal trigger to fill it.
            cur.execute("SELECT id FROM aidb.pipeline_registry WHERE name = 'transactions_kb';")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("transactions_kb pipeline_registry row missing — was create_pipeline run?")
            pipe_id = row[0]
            # Backfill in batches of 200 to bound BERT model state per
            # transaction (per CLAUDE.md G41 — a single large INSERT OOMs pgd).
            # ON CONFLICT skips rows already present so reruns are idempotent.
            BATCH = 200
            total = 0
            while True:
                cur.execute("""
                    INSERT INTO pipeline_transactions_kb (pipeline_id, source_id, part_ids, value)
                    SELECT %s, c.tx_id::text, ARRAY[0]::bigint[], aidb.encode_text('bert', c.search_content)::vector
                    FROM transactions_hybrid_corpus c
                    WHERE NOT EXISTS (
                      SELECT 1 FROM pipeline_transactions_kb k
                      WHERE k.pipeline_id = %s AND k.source_id = c.tx_id::text
                    )
                    LIMIT %s;
                """, (pipe_id, pipe_id, BATCH))
                inserted = cur.rowcount
                conn.commit()
                if inserted == 0:
                    break
                total += inserted
                print(f"  manual encode: {total}/{corpus_rows} rows embedded")
            print(f"  ok manual encode_text backfill complete: {total} rows")
        except Exception as e:
            print(f"  WARN: manual encode backfill failed: {e} — AIDB+RRF cards will be empty")
            conn.rollback()

    # Pre-warm BERT — first call loads ~1.2GB model into memory and is the
    # slow step on the user's first search. By issuing dummy tokenize + encode
    # calls here, the model stays resident and subsequent searches skip the
    # cold-start cost (~700ms-1s saved on the user's first click).
    try:
        cur.execute("SELECT tokenizer_catalog.tokenize('warmup query for bert', 'bert');")
        cur.execute("SELECT aidb.kb_query_encode('public.pipeline_transactions_kb', 'warmup query for bert');")
        print(f"  ok BERT model pre-warmed (tokenizer + encoder)")
    except Exception as e:
        print(f"  WARN: BERT pre-warm failed: {e} — first search will be slower")
    cur.close(); conn.close()
    return True


def _create_hybrid_mode_card(session, pgd_id, mode_label, mode_value):
    """One native-SQL card per retrieval mode (bm25 / aidb / rrf / reranked / fraud).
    Columns are ordered so the ranking signals (score / bm25_rank / sem_rank /
    found_by / query_ms) stay visible without horizontal scroll. Description
    is last because it's the widest."""
    q_tag = str(uuid.uuid4())
    sql = (
        "SELECT "
        "  ROW_NUMBER() OVER () AS rank, "
        "  query_ms AS \"latency (ms)\", "
        "  tx_id, merchant, "
        "  ROUND(score, 4) AS score, "
        "  bm25_rank, sem_rank, found_by, "
        "  is_fraud, description "
        f"FROM transactions_hybrid_search({{{{q}}}}, 5, '{mode_value}')"
    )
    payload = {
        "name": mode_label,  # Card title — dashboard name already adds context.
        "display": "table",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": sql,
                "template-tags": {
                    "q": {
                        "id": q_tag, "name": "q", "display-name": "Search",
                        "type": "text", "required": True,
                        "default": "wire transfer overseas",
                    },
                },
            },
            "database": pgd_id,
        },
        "visualization_settings": {
            "table.pivot": False,
        },
    }
    if _hidden_collection_id:
        payload["collection_id"] = _hidden_collection_id
    r = api("post", "/api/card", payload, session=session)
    if r.status_code != 200:
        print(f"  WARN: card '{mode_label}' failed ({r.status_code}): {r.text[:200]}")
        return None, None
    cid = r.json().get("id")
    print(f"  ok card '{mode_label}' (id={cid})")
    return cid, q_tag


def _create_hybrid_search_demo_dashboard(session, pgd_id):
    """Dashboard with 3 side-by-side cards (BM25 | AIDB | RRF) sharing one
    {{q}} parameter. Single click → all 3 cards re-render."""
    _delete_dashboards_named(session, ["BFSI: Hybrid Search Demo"])
    # Delete by any prior or current card title so reruns don't leave orphans.
    _delete_cards_with_prefix(session, "BFSI: Hybrid Search Demo")
    for nm in ("Lexical (BM25)", "Semantic (AIDB BERT)", "Hybrid (RRF)",
               "Reranked (cross-encoder)", "Compliance (fraud-only RRF)"):
        _delete_cards_with_prefix(session, nm)
    global _hidden_collection_id
    if not _hidden_collection_id:
        _hidden_collection_id = ensure_hidden_collection(session)

    cards = []
    for label, mode in [("Lexical (BM25)",       "bm25"),
                        ("Semantic (AIDB BERT)", "aidb"),
                        ("Hybrid (RRF)",         "rrf")]:
        cid, tag = _create_hybrid_mode_card(session, pgd_id, label, mode)
        if cid is None:
            return None
        cards.append((cid, tag))

    q_param = uuid.uuid4().hex[:8]
    create = api("post", "/api/dashboard", {
        "name": "BFSI: Hybrid Search Demo",
        "description": "Lexical / Semantic / Hybrid / Reranked / Compliance — same query, five retrieval modes. found_by column shows which engine surfaced each row.",
        "parameters": [{
            "id": q_param, "name": "Search", "slug": "q",
            "type": "category", "sectionId": "string",
            "default": "wire transfer overseas",
        }],
    }, session=session)
    if create.status_code != 200:
        print(f"  WARN: dashboard create failed ({create.status_code}): {create.text[:200]}")
        return None
    dash_id = create.json().get("id")
    # Stack the 3 cards vertically — each card gets the full 24-col grid
    # width so all columns (rank, tx_id, merchant, score, bm25_rank,
    # sem_rank, is_fraud, description) are visible without horizontal
    # scroll. Per-row cross-mode comparison: each card shows BOTH this
    # row's bm25_rank AND sem_rank, so disagreement between modes is
    # readable inline ("BM25 #1, semantic #47 — pure keyword hit").
    dashcards = []
    CARD_HEIGHT = 8
    for i, (cid, _tag) in enumerate(cards):
        dashcards.append({
            "id": -(i + 1), "card_id": cid,
            "row": i * CARD_HEIGHT, "col": 0, "size_x": 24, "size_y": CARD_HEIGHT,
            "parameter_mappings": [{
                "parameter_id": q_param, "card_id": cid,
                "target": ["variable", ["template-tag", "q"]],
            }],
        })
    layout = api("put", f"/api/dashboard/{dash_id}", {"dashcards": dashcards}, session=session)
    if layout.status_code != 200:
        print(f"  WARN: dashboard layout returned {layout.status_code}: {layout.text[:200]}")
    return dash_id, [cid for (cid, _) in cards]


def _prewarm_hybrid_cards(session, card_ids):
    """Fire each card's query once so Metabase's pool warms its BERT model.
    Without this, the user's first dashboard load cold-loads BERT on 5 fresh
    backends serially (~3-5 min). With this, the cold-load is paid here
    during Build, and the dashboard renders in <2s after."""
    import concurrent.futures
    print(f"[Hybrid Search] Pre-warming Metabase pool: {len(card_ids)} card(s) ...")
    def warm_one(cid):
        t0 = time.time()
        try:
            r = requests.post(
                f"{METABASE_URL}/api/card/{cid}/query",
                headers={"Content-Type": "application/json", "X-Metabase-Session": session},
                timeout=300,
            )
            return (cid, r.status_code, int(time.time() - t0))
        except Exception as e:
            return (cid, str(e)[:40], int(time.time() - t0))
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(card_ids)) as ex:
        for cid, status, elapsed in ex.map(warm_one, card_ids):
            tag = "ok" if status == 200 else "WARN"
            print(f"  {tag} card {cid}: {status} in {elapsed}s")


def setup_hybrid_search_demo(session):
    """End-to-end: apply hybrid-search SQL + create Metabase 3-card dashboard."""
    print("[Hybrid Search] Applying SQL: extensions + BM25 index + AIDB pipeline + RRF function ...")
    if not _build_hybrid_search_index():
        print("[Hybrid Search] Skipped dashboard — SQL setup failed.")
        return
    print("[Hybrid Search] Building Metabase 3-card demo dashboard ...")
    pgd_id = find_database(session, "PGD") or find_database(session, "pgd")
    if not pgd_id:
        print("  SKIP: PGD data source not registered in Metabase yet.")
        return
    result = _create_hybrid_search_demo_dashboard(session, pgd_id)
    if result:
        dash_id, card_ids = result
        pub = _enable_public_sharing(session, dash_id)
        if pub:
            print(f"  ok Hybrid Search Demo: http://127.0.0.1:3002/public/dashboard/{pub}")
        else:
            print(f"  ok Hybrid Search Demo dashboard (id={dash_id}) — public link unavailable.")
        # Bust the agent's URL cache so the UC2 link picks up the new public_uuid.
        try:
            import urllib.request, urllib.parse
            url = "http://host.docker.internal:4000/api/metabase-dashboard-cache/invalidate?name=" + urllib.parse.quote("BFSI: Hybrid Search Demo")
            req = urllib.request.Request(url, method="POST")
            urllib.request.urlopen(req, timeout=3).read()
            print("  ok cleared dashboard URL cache")
        except Exception as e:
            print(f"  WARN: could not clear dashboard URL cache ({e}) — first click may hit stale link")
        # Pre-warm Metabase's PG pool so first user dashboard load is fast.
        if card_ids:
            _prewarm_hybrid_cards(session, card_ids)


def run_olap_stage():
    """Original full-dashboard setup (Usecase 2 OLAP). 6-tab dashboard with all data sources."""
    print("=" * 60)
    print("  Stage: OLAP — Core Banking Fraud Detection (full)")
    print("=" * 60)

    if not wait_for_metabase():
        sys.exit(1)

    session = setup_metabase()
    print()

    print("[1/6] Ensuring data sources ...")
    db_ids = ensure_databases(session)
    print()

    print("[2/6] Cleaning up old OLAP objects ...")
    cleanup_olap(session)
    print()

    print("[3/6] Setting up hidden collection ...")
    global _hidden_collection_id
    _hidden_collection_id = ensure_hidden_collection(session)
    print()

    print("[4/6] Creating questions ...")
    all_cards = {}

    print("  -- OLTP Dashboard --")
    all_cards["oltp"] = create_oltp_questions(session, db_ids["pgd"])

    print("  -- Fraud Detection --")
    all_cards["fraud"] = create_fraud_questions(session, db_ids["pgd"])

    print("  -- Analytics (ClickHouse) --")
    if db_ids.get("clickhouse"):
        all_cards["analytics"] = create_analytics_questions(session, db_ids["clickhouse"])
    else:
        print("    SKIP: ClickHouse not connected")
        all_cards["analytics"] = []

    print("  -- Streaming vs Batch --")
    if db_ids.get("clickhouse") and db_ids.get("risingwave"):
        all_cards["comparison"] = create_comparison_questions(
            session, db_ids["pgd"], db_ids["clickhouse"], db_ids["risingwave"])
    else:
        print("    SKIP: CH or RW not connected")
        all_cards["comparison"] = []

    print("  -- Kafka CDC --")
    if db_ids.get("clickhouse"):
        all_cards["cdc"] = create_cdc_questions(session, db_ids["pgd"], db_ids["clickhouse"])
    else:
        print("    SKIP: ClickHouse not connected")
        all_cards["cdc"] = []

    print("  -- Transaction Search --")
    all_cards["search"] = create_search_questions(session, db_ids["pgd"])
    print()

    print("[5/6] Creating dashboard with tabs ...")
    dash_id = create_dashboard(session, all_cards)
    print()

    total_questions = sum(len([c for c in v if c]) for v in all_cards.values())
    print("[6/6] Setup complete!")
    pub = _enable_public_sharing(session, dash_id)
    print(f"  Dashboard:  http://127.0.0.1:3002/dashboard/{dash_id}")
    if pub:
        print(f"  Public:     http://127.0.0.1:3002/public/dashboard/{pub}")
    print(f"  Auto-login: http://127.0.0.1:4000/api/metabase-login")
    print(f"  Questions:  {total_questions}")
    print()

    # Hybrid Search demo is NOT auto-run from UC2 — BERT tokenization across
    # the full transactions table can take 10+ min and has crashed pgd on
    # large corpora. Run on demand:
    #   docker compose run --rm metabase-setup python /app/setup_metabase.py --stage hybrid-reindex
    print("[Hybrid Search] Skipped during UC2 (run --stage hybrid-reindex on demand).")

    print()
    print("Tip: Use auto-refresh (clock icon, top-right) for live data updates.")
    print()


# ══════════════════════════════════════════════════════════
#  Stage: OLTP — minimal dashboard after Usecase 1
# ══════════════════════════════════════════════════════════

def _ensure_pgd_only(session):
    """Add PGD as a Metabase data source if missing. Returns its db_id."""
    pgd_id = find_database(session, "PGD")
    if not pgd_id:
        pgd_id = add_database(session, "postgres", "EDB PGD (OLTP + ML)", {
            "host": PGD_HOST, "port": PGD_PORT, "dbname": PGD_DATABASE,
            "user": PGD_USER, "password": PGD_PASSWORD,
            "ssl": False, "tunnel-enabled": False,
        })
    print(f"  PGD database id: {pgd_id}")
    if pgd_id:
        wait_for_sync(session, pgd_id, timeout=90)
    return pgd_id


def _create_oltp_quickview_questions(session, pgd_id):
    """Six small questions answering 'Is OLTP working and seeded?'.
    All names start with 'OLTP:' so cleanup_oltp can find them.
    """
    cards = []
    cards.append(create_question(session, "OLTP: Total Transactions", "scalar", pgd_id,
        "SELECT COUNT(*) AS total FROM transactions"))
    cards.append(create_question(session, "OLTP: Total Customers", "scalar", pgd_id,
        "SELECT COUNT(*) AS total FROM customers"))
    cards.append(create_question(session, "OLTP: Total Volume (USD)", "scalar", pgd_id,
        "SELECT COALESCE(ROUND(SUM(amount)::numeric, 2), 0) AS total FROM transactions"))
    cards.append(create_question(session, "OLTP: Fraud Rate (%)", "scalar", pgd_id,
        """SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud) / NULLIF(COUNT(*), 0)::numeric, 2) AS pct
           FROM fraud_labels WHERE detection_source = 'rules'"""))
    cards.append(create_question(session, "OLTP: Transactions by Region & Vendor", "bar", pgd_id,
        """SELECT region || '/' || vendor AS rv, COUNT(*) AS n
           FROM transactions GROUP BY 1 ORDER BY n DESC LIMIT 12"""))
    cards.append(create_question(session, "OLTP: Daily Transaction Volume", "line", pgd_id,
        """SELECT DATE_TRUNC('day', created_at)::date AS day,
                  COUNT(*) AS tx_count,
                  ROUND(SUM(amount)::numeric, 2) AS volume
             FROM transactions
             WHERE created_at > NOW() - INTERVAL '30 days'
             GROUP BY 1 ORDER BY 1"""))
    cards.append(create_question(session, "OLTP: Recent Transactions", "table", pgd_id,
        """SELECT t.tx_id, t.customer_id, t.merchant, t.amount, t.region, t.vendor,
                  fl.is_fraud, t.created_at
             FROM transactions t
             LEFT JOIN fraud_labels fl ON fl.tx_id = t.tx_id AND fl.detection_source = 'rules'
             ORDER BY t.tx_id DESC LIMIT 50"""))
    return cards


def _enable_public_sharing(session, dash_id):
    """Enable Metabase public sharing for the dashboard. Returns the public_uuid."""
    # Site-level toggle (idempotent)
    api("put", "/api/setting/enable-public-sharing", {"value": True}, session=session)
    # Try to read existing UUID first
    r = api("get", f"/api/dashboard/{dash_id}", session=session)
    if r.status_code == 200:
        existing = r.json().get("public_uuid")
        if existing:
            return existing
    # Otherwise create one
    r2 = api("post", f"/api/dashboard/{dash_id}/public_link", session=session)
    if r2.status_code == 200:
        return r2.json().get("uuid")
    print(f"  WARN: public_link returned {r2.status_code}: {r2.text[:200]}")
    return None


def _create_oltp_quickview_dashboard(session, cards):
    """Create the small 'BFSI: OLTP Quick View' dashboard with one tab."""
    r = api("post", "/api/dashboard", {
        "name": "BFSI: OLTP Quick View",
        "description": "Quick health view after Usecase 1 (OLTP). Use 'Core Banking Fraud Detection' for the full multi-source view (Usecase 2+).",
    }, session=session)
    if r.status_code != 200:
        print(f"ERROR: Create OLTP dashboard failed: {r.text[:200]}")
        return None
    dash_id = r.json().get("id")
    W = 18
    dashcards = []
    cid = -1
    def add(card_id, row, col, w, h):
        nonlocal cid
        if card_id is None: return
        dashcards.append({"id": cid, "card_id": card_id,
                          "row": row, "col": col, "size_x": w, "size_y": h})
        cid -= 1
    # Hero scalars
    add(cards[0], 0, 0, 6, 3)   # Total TX
    add(cards[1], 0, 6, 6, 3)   # Total Customers
    add(cards[2], 0, 12, 6, 3)  # Total Volume
    # Fraud rate scalar (smaller)
    add(cards[3], 3, 0, 6, 3)   # Fraud %
    # Charts
    add(cards[4], 3, 6, 12, 8)  # Region/vendor bar
    add(cards[5], 11, 0, 18, 8)  # Daily volume line
    # Table
    add(cards[6], 19, 0, 18, 10)
    r2 = api("put", f"/api/dashboard/{dash_id}", {"dashcards": dashcards}, session=session)
    if r2.status_code == 200:
        print(f"  Dashboard created and laid out (id={dash_id}, {len(dashcards)} cards)")
    else:
        print(f"  WARN: Dashboard layout returned {r2.status_code}: {r2.text[:200]}")
    return dash_id


def run_oltp_stage():
    """OLTP stage: PGD source + a small starter dashboard. Idempotent."""
    print("=" * 60)
    print("  Stage: OLTP — Quick View (Usecase 1)")
    print("=" * 60)

    if not wait_for_metabase():
        sys.exit(1)

    session = setup_metabase()
    print()

    print("[1/4] Ensuring PGD data source ...")
    pgd_id = _ensure_pgd_only(session)
    if not pgd_id:
        print("ERROR: Could not add PGD as data source.")
        sys.exit(1)
    print()

    print("[2/4] Cleaning up prior OLTP Quick View objects ...")
    cleanup_oltp(session)
    print()

    print("[3/4] Setting up hidden collection ...")
    global _hidden_collection_id
    _hidden_collection_id = ensure_hidden_collection(session)
    print()

    print("[4/4] Creating OLTP Quick View dashboard ...")
    cards = _create_oltp_quickview_questions(session, pgd_id)
    dash_id = _create_oltp_quickview_dashboard(session, cards)
    print()

    if dash_id:
        # Enable public sharing so workspace UI can iframe it without auth
        pub = _enable_public_sharing(session, dash_id)
        print(f"  ✓ OLTP Quick View dashboard: http://127.0.0.1:3002/public/dashboard/{pub}" if pub else f"  ✓ OLTP Quick View dashboard ready (id={dash_id})")
    print()


# ══════════════════════════════════════════════════════════
#  Stubs for future stages
# ══════════════════════════════════════════════════════════

def _stub_stage(name, parent_usecase):
    print("=" * 60)
    print(f"  Stage: {name} (Usecase {parent_usecase})")
    print("=" * 60)
    print(f"This stage is a placeholder. {name.upper()} dashboard tabs will be")
    print("added when the corresponding use case is refactored.")
    print()


def run_ml_stage():
    """Usecase 3 (ML Fraud Detection) — surface the existing OLAP dashboard's
    Fraud Detection tab now that ml_fraud_predictions has real data.

    The 6-tab "Core Banking Fraud Detection" dashboard (created by --stage olap)
    already has all the ML-aware questions — TTDF leaderboard, ML-vs-Rules
    agreement, predictions by source, etc. Until Usecase 3 ran, those queries
    returned zero rows or hit "Run Usecase 3" placeholders. After Usecase 3,
    they show real numbers without any new question creation.

    So this stage is intentionally minimal: re-confirm the public link, refresh
    the schema cache so Metabase picks up any new columns, and print the URL.
    """
    print("=" * 60)
    print("  Stage: ML — Surface Fraud Detection metrics (Usecase 3)")
    print("=" * 60)

    if not wait_for_metabase():
        sys.exit(1)
    session = setup_metabase()
    print()

    # Re-sync PGD so Metabase sees any new rows in ml_fraud_predictions and
    # ml_fraud_alerts immediately (instead of waiting for its periodic scan).
    print("[1/3] Refreshing PGD schema cache ...")
    pgd_id = find_database(session, "PGD") or find_database(session, "EDB PGD (OLTP + ML)")
    if pgd_id:
        api("post", f"/api/database/{pgd_id}/sync_schema", session=session)
        wait_for_sync(session, pgd_id, timeout=30)
        print(f"  ✓ PGD schema synced (id={pgd_id})")
    else:
        print("  ⚠ PGD database not registered in Metabase — run --stage olap first")

    # Locate the OLAP dashboard. ML stage is meaningless without it.
    print()
    print("[2/3] Locating Core Banking Fraud Detection dashboard ...")
    r = api("get", "/api/dashboard", session=session)
    dash_id = None
    if r.status_code == 200:
        for d in r.json():
            if d.get("name") == "Core Banking Fraud Detection":
                dash_id = d["id"]
                break
    if not dash_id:
        print("  ✗ Dashboard not found. Run Usecase 2 (OLAP) Start Service first.")
        sys.exit(1)
    print(f"  ✓ Dashboard found (id={dash_id})")

    # Public link — idempotent
    print()
    print("[3/3] Ensuring public link ...")
    pub = _enable_public_sharing(session, dash_id)
    print()
    print("ML stage complete!")
    print(f"  Dashboard:  http://127.0.0.1:3002/dashboard/{dash_id}")
    if pub:
        print(f"  Public:     http://127.0.0.1:3002/public/dashboard/{pub}")
    print()
    print("Tip: Open the 'Fraud Detection' tab to see TTDF leaderboard + ML-vs-Rules.")
    print()


def run_genai_stage():
    _stub_stage("genai", 4)


def run_aigov_stage():
    _stub_stage("ai-gov", 5)


def run_hybrid_reindex():
    """On-demand rebuild of the Hybrid Search Demo (SQL + 3-card dashboard)."""
    print("=" * 60)
    print("  Stage: Hybrid Search Demo — Reindex")
    print("=" * 60)
    if not wait_for_metabase():
        sys.exit(1)
    session = setup_metabase()
    setup_hybrid_search_demo(session)


# ══════════════════════════════════════════════════════════
#  CLI dispatch
# ══════════════════════════════════════════════════════════

STAGES = {
    "oltp":            run_oltp_stage,
    "olap":            run_olap_stage,
    "ml":              run_ml_stage,
    "genai":           run_genai_stage,
    "ai-gov":          run_aigov_stage,
    "hybrid-reindex":  run_hybrid_reindex,
}


def main():
    parser = argparse.ArgumentParser(description="BFSI Metabase setup (staged).")
    parser.add_argument("--stage", choices=list(STAGES.keys()), default="olap",
                        help="Which use case stage to apply. Default 'olap' preserves legacy behavior.")
    args = parser.parse_args()
    STAGES[args.stage]()


if __name__ == "__main__":
    main()
