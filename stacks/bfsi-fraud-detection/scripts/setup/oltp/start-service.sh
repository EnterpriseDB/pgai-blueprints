#!/bin/bash
# ============================================================================
# BFSI Fraud Detection — Pipeline Step 1: Start Service
# ============================================================================
# For demonstration purposes only.
#
# Brings up OLTP containers, applies consolidated schema, generates synthetic
# data via SDV, loads into pgd, and backfills fraud_labels in one go.
#
# Stages (printed inline):
#   [1/6] Start Bank App (--profile oltp)  -- foundation already up from Deploy
#   [2/6] Apply schema from db/oltp.sql
#   [3/6] Generate synthetic data via SDV (engine/synthdb)
#   [4/6] Load CSVs into pgd
#   [5/6] Fix FKs + backfill fraud_labels
#   [6/6] Metabase OLTP Quick View dashboard (best-effort; non-fatal)
# ============================================================================

set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
REPO_ROOT="$(cd "$STACK_DIR/../.." && pwd)"
SYNTHDB_DIR="$REPO_ROOT/engine/synthdb"
SYNTHDB_OUT="$SYNTHDB_DIR/output/fraud_bank"
SYNTHDB_API="http://127.0.0.1:8050"
TOTAL_ROWS="${OLTP_TOTAL_ROWS:-250000}"

PG_CONTAINER="bfsi-pgd"
PG_USER="postgres"
PG_DB="demo"
PG_PASS="secret"

step() { echo ""; echo "━━━ $* ━━━"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

# Filter docker compose's per-container chatter (Container/Volume/Network
# Creating/Started/Healthy/...) and EXP_ID-style env-var warnings, leaving real
# errors. Use with a pipe: `docker compose ... 2>&1 | compose_quiet`.
compose_quiet() {
  grep -vE "^[[:space:]]*(Container|Volume|Network)[[:space:]].*[[:space:]](Creating|Created|Starting|Started|Running|Healthy|Waiting|Exited|Recreated|Recreate)[[:space:]]*$" \
  | grep -vE "^time=.*level=(warning|info)" \
  || true
}

# ----------------------------------------------------------------------------
# [1/6] Start Bank App (--profile oltp adds: app, minio-fraud-init, aidb-init)
# Foundation services (pgd, kafka, clickhouse, etc.) already came up at Deploy
# time. Compose may print "Container X Running" for those — that's expected.
# ----------------------------------------------------------------------------
step "[1/6] Start Bank App (--profile oltp)"
cd "$STACK_DIR"
# Drop Compose's per-container orchestration chatter (Container/Volume/Network
# Created/Started/Healthy/Waiting/Exited/Running) and the EXP_ID warning. Keep
# only true errors. The `ok` line below summarises the outcome.
docker compose --profile oltp up -d 2>&1 | compose_quiet
ok "Bank App container started (foundation services already running from Deploy)"

echo "  Waiting for $PG_CONTAINER to become healthy..."
for i in $(seq 1 90); do
  state=$(docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null || echo "missing")
  if [ "$state" = "healthy" ]; then
    ok "$PG_CONTAINER healthy (took ${i}s)"
    break
  fi
  if [ "$i" = "90" ]; then
    fail "$PG_CONTAINER did not become healthy in 90s (state=$state). Check: docker logs $PG_CONTAINER"
  fi
  sleep 1
done

# ----------------------------------------------------------------------------
# [2/6] Apply consolidated OLTP schema
# ----------------------------------------------------------------------------
step "[2/6] Apply schema from db/oltp.sql"
docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -f /db/oltp.sql
ok "Schema applied (4 tables, fraud_rules, trigger, publication)"

# ----------------------------------------------------------------------------
# [3/6] Generate synthetic data via SDV
# ----------------------------------------------------------------------------
step "[3/6] Generate synthetic data via SDV (target: $TOTAL_ROWS rows)"

# Ensure synthdb container is up
if ! curl -sf -m 2 "$SYNTHDB_API/health" >/dev/null 2>&1; then
  echo "  Starting synthdb container..."
  cd "$SYNTHDB_DIR" && docker compose up -d
  for i in $(seq 1 60); do
    if curl -sf -m 2 "$SYNTHDB_API/health" >/dev/null 2>&1; then
      ok "synthdb healthy (took ${i}s)"
      break
    fi
    [ "$i" = "60" ] && fail "synthdb did not become healthy in 60s"
    sleep 1
  done
else
  ok "synthdb already running"
fi

# Clean previous output to avoid mixing runs
rm -rf "$SYNTHDB_OUT"

# Trigger generation via API (CSV output only — pgd schema is ours, not synthdb's)
echo "  Generating fraud_bank dataset (this takes ~10–30s)..."
RESP=$(curl -sf -X POST "$SYNTHDB_API/api/generate?model=fraud_bank&total_rows=$TOTAL_ROWS&output=csv" \
  --max-time 300 || echo '{"success":false,"error":"request failed"}')

if ! echo "$RESP" | grep -q '"success": true\|"success":true'; then
  echo "$RESP"
  fail "synthdb generation failed"
fi

# Show generated counts
echo "$RESP" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for f in d.get('files', []):
    print(f\"  ✓ {f['table']}: {f['rows']:,} rows ({f['size']:,} bytes)\")
" 2>/dev/null || true

# ----------------------------------------------------------------------------
# [4/6] Load CSVs into pgd
# ----------------------------------------------------------------------------
step "[4/6] Load CSVs into pgd"

# Disable trigger during bulk load (rule eval is slow on 250K rows; we backfill in stage 5)
docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -c \
  "ALTER TABLE transactions DISABLE TRIGGER trg_check_fraud;"

for table in customers accounts transactions; do
  CSV="$SYNTHDB_OUT/$table.csv"
  if [ ! -f "$CSV" ]; then
    fail "Expected CSV not found: $CSV"
  fi
  echo "  Copying $table..."
  docker cp "$CSV" "$PG_CONTAINER:/tmp/$table.csv"
  docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
    psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -c \
    "\\COPY $table FROM '/tmp/$table.csv' WITH CSV HEADER"
  docker exec "$PG_CONTAINER" rm -f "/tmp/$table.csv" 2>/dev/null || true
  ok "$table loaded"
done

# Subsample to demo-friendly counts: 500 customers, 1000 accounts, all transactions
# (synthdb spreads --total-rows proportionally across seed sizes — for our seed
# this overshoots customers/accounts. We trim down here while preserving FK validity.)
TARGET_CUSTOMERS="${OLTP_CUSTOMERS:-500}"
TARGET_ACCOUNTS="${OLTP_ACCOUNTS:-1000}"
echo "  Trimming to $TARGET_CUSTOMERS customers / $TARGET_ACCOUNTS accounts (transactions kept full)..."
# -i is required for heredoc to actually pipe to psql's stdin; without it
# docker exec closes stdin and psql silently no-ops.
docker exec -i -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q <<EOSQL
-- Plan the subsample using temp tables to side-step FK violation order issues.
-- 1. Pick keep_customers (first N by customer_id)
-- 2. Pick keep_accounts (up to M accounts whose customer_id is in keep_customers)
-- 3. Re-FK every transaction to one of the kept accounts (random by hash)
-- 4. Then it is safe to delete non-kept accounts (no transactions reference them)
-- 5. Then it is safe to delete non-kept customers (no accounts reference them)
DROP TABLE IF EXISTS keep_customers;
DROP TABLE IF EXISTS keep_accounts;
CREATE TEMP TABLE keep_customers AS
  SELECT customer_id FROM customers ORDER BY customer_id LIMIT $TARGET_CUSTOMERS;
CREATE TEMP TABLE keep_accounts AS
  SELECT account_id, customer_id FROM accounts
  WHERE customer_id IN (SELECT customer_id FROM keep_customers)
  ORDER BY account_id LIMIT $TARGET_ACCOUNTS;

-- Re-FK every transaction to a surviving account (deterministic via hash)
WITH valid_accs AS (
  SELECT account_id, customer_id, ROW_NUMBER() OVER () AS rn,
         (SELECT COUNT(*) FROM keep_accounts) AS total
  FROM keep_accounts
)
UPDATE transactions t
   SET account_id  = va.account_id,
       customer_id = va.customer_id
  FROM valid_accs va
 WHERE va.rn = ((ABS(HASHTEXT(t.tx_id::text)) % va.total) + 1);

-- Now FK-safe to drop the extras
DELETE FROM accounts  WHERE account_id  NOT IN (SELECT account_id  FROM keep_accounts);
DELETE FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM keep_customers);
EOSQL
ok "subsampled (transactions re-FK'd to valid accounts)"

# ----------------------------------------------------------------------------
# [5/6] Fix transactions.customer_id FK + backfill fraud_labels
# ----------------------------------------------------------------------------
step "[5/6] Fix FK consistency + backfill fraud_labels"

# synthdb's FK engine fills only declared relationships. Our schema's
# transactions.customer_id is a redundant copy of accounts.customer_id; we set
# it correctly here so dashboards joining on customer_id work as expected.
docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -c "
    UPDATE transactions t
       SET customer_id = a.customer_id
      FROM accounts a
     WHERE t.account_id = a.account_id;
    "
ok "transactions.customer_id aligned to accounts"

# Backfill fraud_labels.
# SDV-generated data tends to over-represent the seed's fraud-pattern rows
# (the 28-row seed has ~36% fraud-pattern → generated data is ~26% over-threshold).
# We target a realistic ~4% fraud rate (matching Bank App's "Occasional" mode)
# by:
#   Pass 1: insert baseline FALSE for every transaction
#   Pass 2: from rule-matching candidates, pick the top OLTP_TARGET_FRAUD_PCT %
#           by HASHTEXT(tx_id) — deterministic, re-runnable.
TARGET_FRAUD_PCT="${OLTP_TARGET_FRAUD_PCT:-4}"
# -i is required for heredoc to pipe to psql's stdin
docker exec -i -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -v fraud_pct="$TARGET_FRAUD_PCT" <<'EOSQL'
-- Pass 1: baseline (every transaction starts as not fraud)
INSERT INTO fraud_labels (tx_id, is_fraud, rules_triggered, ttdf_milliseconds, detection_source)
SELECT t.tx_id, FALSE, ARRAY[]::TEXT[], 0, 'rules'
FROM transactions t
ON CONFLICT (tx_id, detection_source) DO NOTHING;

-- Pass 2: pick a CAPPED subset of rule-matching rows to mark as fraud.
-- The cap ensures the demo shows ~:'fraud_pct'% fraud rather than 26% from raw matches.
WITH matches AS (
  SELECT t.tx_id, ARRAY_AGG(fr.rule_name) AS rule_names
  FROM transactions t
  JOIN fraud_rules fr ON fr.is_active = TRUE
  WHERE
        (fr.region IS NULL OR fr.region = t.region)
    AND (fr.vendor IS NULL OR LOWER(fr.vendor) = LOWER(t.vendor))
    AND (
          (fr.condition_sql ~ '^amount > [0-9]+$'
              AND t.amount > CAST(SUBSTRING(fr.condition_sql FROM 'amount > ([0-9]+)') AS NUMERIC))
       OR (fr.condition_sql LIKE 'category IN%'
              AND LOWER(t.category) IN ('gambling','crypto','crypto exchange'))
    )
  GROUP BY t.tx_id
),
-- Deterministic top-N selection by hash so re-runs produce the same fraud set
target AS (
  SELECT GREATEST(1, FLOOR(:fraud_pct::numeric / 100.0 * (SELECT COUNT(*) FROM transactions)))::bigint AS n
),
picked AS (
  SELECT tx_id, rule_names
  FROM matches
  ORDER BY ABS(HASHTEXT(tx_id::text))
  LIMIT (SELECT n FROM target)
)
UPDATE fraud_labels fl
   SET is_fraud        = TRUE,
       rules_triggered = p.rule_names,
       fraud_reason    = array_to_string(p.rule_names, ', ')
  FROM picked p
 WHERE fl.tx_id = p.tx_id
   AND fl.detection_source = 'rules';
EOSQL
ok "fraud_labels backfilled (capped at ${TARGET_FRAUD_PCT}% fraud rate)"

# Re-enable trigger so live transactions (Bank App ▶ Start) populate fraud_labels in real time
docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -c \
  "ALTER TABLE transactions ENABLE TRIGGER trg_check_fraud;"
ok "Trigger re-enabled for live transactions"

# ----------------------------------------------------------------------------
# [6/6] Metabase OLTP Quick View dashboard
# ----------------------------------------------------------------------------
# Optional — only runs if Metabase is healthy. Failure here does not fail the
# pipeline (Metabase is a viewer, not a hard dependency for OLTP).
step "[6/6] Metabase OLTP Quick View dashboard"
if curl -sf -m 3 http://127.0.0.1:3002/api/health >/dev/null 2>&1; then
  if (cd "$STACK_DIR" && docker compose run --rm metabase-setup \
        python /app/setup_metabase.py --stage oltp 2>&1 | compose_quiet); then
    ok "Metabase OLTP Quick View ready"
  else
    echo "  ⚠ Metabase OLTP setup failed (non-fatal). Open http://127.0.0.1:3002 manually."
  fi
else
  echo "  ⚠ Metabase not healthy yet at port 3002 — skipping. Run \"Check Status\" later or open http://127.0.0.1:3002 manually."
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "━━━ Service Ready ━━━"
docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" \
  psql -h localhost -U "$PG_USER" -d "$PG_DB" -t -A -F ' | ' -c "
    SELECT 'customers',           COUNT(*)::text FROM customers
    UNION ALL SELECT 'accounts',  COUNT(*)::text FROM accounts
    UNION ALL SELECT 'transactions', COUNT(*)::text FROM transactions
    UNION ALL SELECT 'fraud',
      COUNT(*) FILTER (WHERE is_fraud) || ' (' ||
      ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud) / NULLIF(COUNT(*), 0), 1) || '%)'
      FROM fraud_labels
    UNION ALL SELECT 'fraud_rules (active)', COUNT(*)::text FROM fraud_rules WHERE is_active = TRUE;
  " | sed 's/^/  /'
