#!/bin/bash

# For demonstration purposes only.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Set default password if not provided
PGPASSWORD=${POSTGRES_PASSWORD:-secret}
export PGPASSWORD

# Check if database is already initialized
if [ ! -f "/var/lib/postgresql/db/PG_VERSION" ]; then
    log_info "Initializing PGD cluster..."

    # Initialize PGD cluster using pgd node setup
    # Use LOCAL_HOST env var for proper container networking (default: pgd)
    LOCAL_HOST=${LOCAL_HOST:-pgd}
    /usr/lib/edb-pge/17/bin/pgd node db1 setup \
        --dsn "host=$LOCAL_HOST dbname=demo user=postgres password=${POSTGRES_PASSWORD}" \
        --pgdata /var/lib/postgresql/db \
        --log-file /var/lib/postgresql/logfile \
        --group-name pgd-group

    # Configure settings in postgresql.conf
    cat >> /var/lib/postgresql/db/postgresql.conf <<EOF

# BigApp PGD Configuration
listen_addresses = '*'
max_connections = 200

# PGAA Configuration (analytics acceleration)
pgfs.allowed_local_fs_paths = '/var/lib/postgresql/pgd-analytics'
pgaa.autostart_seafowl = 'on'
pgaa.autostart_seafowl_port = 5445
pgaa.seafowl_url = 'http://localhost:5445'
pgaa.enable_maintenance_worker = 'on'
pgaa.enable_metastore_sync_worker = 'on'

# Real-time PGAA optimization (minimum latency for analytics replication)
bdr.taskmgr_nap_time = 1000              # Minimum allowed (1s WAL→Iceberg sync)
# NOTE: bdr.prefer_analytics_engine is set per-session by PGAA ML service,
# NOT system-wide, to avoid breaking RisingWave CDC which needs OLTP heap
pgaa.max_replication_lag_s = 1           # Tighten replication lag tolerance
edb.collect_table_statistics = all
max_worker_processes = 64

# Increase global lock timeout for DDL operations (default 60s)
bdr.global_lock_timeout = 300s           # 5 min timeout for CREATE INDEX etc

EOF

    # Update pg_hba.conf to allow connections from all hosts
    echo "host    all             all             0.0.0.0/0               md5" >> /var/lib/postgresql/db/pg_hba.conf

    # Restart PostgreSQL to apply configuration changes (listen_addresses requires restart)
    /usr/lib/edb-pge/17/bin/pg_ctl -D /var/lib/postgresql/db -l /var/lib/postgresql/logfile restart

    log_info "PGD cluster initialized successfully"
else
    log_info "Database already initialized, starting PostgreSQL..."
    # Start PostgreSQL manually since pgd node setup was already run
    /usr/lib/edb-pge/17/bin/pg_ctl -D /var/lib/postgresql/db -l /var/lib/postgresql/logfile start
fi

# Trap SIGTERM/SIGINT to gracefully stop PostgreSQL (set early so signals during init are handled)
trap '/usr/lib/edb-pge/17/bin/pg_ctl -D /var/lib/postgresql/db stop -m fast; exit 0' SIGTERM SIGINT

# Wait for PostgreSQL to be ready
log_info "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if /usr/lib/edb-pge/17/bin/pg_isready -h localhost -U postgres -d demo > /dev/null 2>&1; then
        log_info "PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        log_error "PostgreSQL failed to be ready within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Create extensions
log_info "Creating extensions..."
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "CREATE EXTENSION IF NOT EXISTS pgaa CASCADE;" 2>/dev/null || {
    log_warn "PGAA extension creation skipped (may require catalog setup)"
}
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>/dev/null || {
    log_warn "pgvector extension not available"
}
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "CREATE EXTENSION IF NOT EXISTS aidb CASCADE;" 2>/dev/null || {
    log_warn "AIDB extension not available"
}

# ── Hybrid Search: VectorChord-BM25 + pg_tokenizer require shared_preload_libraries ──
# Guard with .so-file existence checks so a missing apt package doesn't brick
# the PG instance via a preload entry that won't resolve.
PKGLIB=$(/usr/lib/edb-pge/17/bin/pg_config --pkglibdir 2>/dev/null)
HAS_VCHORD_SO=0; HAS_TOKENIZER_SO=0
[ -n "$PKGLIB" ] && [ -f "$PKGLIB/vchord_bm25.so" ] && HAS_VCHORD_SO=1
[ -n "$PKGLIB" ] && [ -f "$PKGLIB/pg_tokenizer.so" ] && HAS_TOKENIZER_SO=1
log_info "Hybrid search libs: vchord_bm25=$HAS_VCHORD_SO pg_tokenizer=$HAS_TOKENIZER_SO (pkglib=$PKGLIB)"

if [ $HAS_VCHORD_SO -eq 1 ] || [ $HAS_TOKENIZER_SO -eq 1 ]; then
    log_info "Configuring shared_preload_libraries for available hybrid-search libs..."
    CURRENT_SPL=$(PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -tAc "SHOW shared_preload_libraries;" 2>/dev/null | tr -d ' ')
    NEW_SPL="$CURRENT_SPL"
    if [ $HAS_VCHORD_SO -eq 1 ]; then
        echo "$CURRENT_SPL" | grep -q vchord_bm25 || NEW_SPL="${NEW_SPL:+$NEW_SPL,}vchord_bm25"
    fi
    if [ $HAS_TOKENIZER_SO -eq 1 ]; then
        echo "$CURRENT_SPL" | grep -q pg_tokenizer || NEW_SPL="${NEW_SPL:+$NEW_SPL,}pg_tokenizer"
    fi
    if [ "$NEW_SPL" != "$CURRENT_SPL" ]; then
        log_info "  Was: $CURRENT_SPL"
        log_info "  Now: $NEW_SPL"
        # Bypass ALTER SYSTEM — it escapes "$libdir/..." with extra quotes, which
        # causes PG to read the whole comma-separated value as a single missing
        # library name. Direct file write avoids the escaping.
        AUTOCONF=/var/lib/postgresql/db/postgresql.auto.conf
        touch "$AUTOCONF"
        sed -i '/^shared_preload_libraries/d' "$AUTOCONF" 2>/dev/null || true
        echo "shared_preload_libraries = '$NEW_SPL'" >> "$AUTOCONF"
        /usr/lib/edb-pge/17/bin/pg_ctl -D /var/lib/postgresql/db -l /var/lib/postgresql/logfile restart
        for i in {1..20}; do
            /usr/lib/edb-pge/17/bin/pg_isready -h localhost -U postgres -d demo > /dev/null 2>&1 && break
            sleep 1
        done
    fi
    [ $HAS_VCHORD_SO -eq 1 ] && PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo \
        -c "CREATE EXTENSION IF NOT EXISTS vchord_bm25;" 2>/dev/null \
        && log_info "✓ vchord_bm25 extension created"
    [ $HAS_TOKENIZER_SO -eq 1 ] && PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo \
        -c "CREATE EXTENSION IF NOT EXISTS pg_tokenizer;" 2>/dev/null \
        && log_info "✓ pg_tokenizer extension created"
else
    log_warn "vchord_bm25 and pg_tokenizer .so files not found — hybrid search disabled"
fi

log_info "Installed extensions:"
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "SELECT extname, extversion FROM pg_extension ORDER BY extname;" 2>/dev/null

# Configure WAL for CDC (Debezium needs logical replication)
log_info "Configuring WAL for CDC..."
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "ALTER SYSTEM SET wal_level = 'logical';" 2>/dev/null || true
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "ALTER SYSTEM SET max_wal_senders = 10;" 2>/dev/null || true
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "ALTER SYSTEM SET max_replication_slots = 10;" 2>/dev/null || true
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d demo -c "ALTER USER postgres WITH REPLICATION;" 2>/dev/null || true

# Create Lakekeeper database (for Iceberg REST catalog metadata)
log_info "Creating Lakekeeper database..."
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -c "CREATE DATABASE lakekeeper;" 2>/dev/null || log_warn "Lakekeeper DB may already exist"
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -c "CREATE USER lakekeeper WITH PASSWORD 'lakekeeper';" 2>/dev/null || log_warn "Lakekeeper user may already exist"
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -c "ALTER DATABASE lakekeeper OWNER TO lakekeeper;" 2>/dev/null || true
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -d lakekeeper -c "GRANT ALL ON SCHEMA public TO lakekeeper;" 2>/dev/null || true

# Create Metabase database (for dashboard persistence)
log_info "Creating Metabase database..."
PGPASSWORD=$POSTGRES_PASSWORD /usr/lib/edb-pge/17/bin/psql -h localhost -U postgres -c "CREATE DATABASE metabase;" 2>/dev/null || log_warn "Metabase DB may already exist"

# OLTP + ML application schema is owned by the BFSI use-case pipelines, not by
# this entrypoint. Container init only handles postgres-level prereqs (extensions,
# WAL, replication role, framework databases). App tables are applied by:
#   Usecase 1 (OLTP) → db/oltp.sql (customers/accounts/transactions/fraud_labels/fraud_rules)
#   Usecase 3 (ML)   → applied by ML pipeline scripts (predictions/alerts/model_metadata)

# PostgreSQL is already running from pgd node setup, just keep it running
log_info "============================================="
log_info "EDB PGD ready — Core Banking Fraud Detection"
log_info "============================================="
log_info "  Host: pgd (internal) / 127.0.0.1:7434 (host)"
log_info "  Database: demo"
log_info "  User: postgres / Password: secret"
log_info "  Extensions: pgaa, aidb, vector, bdr, pgfs"
log_info "  ML tables: ml_fraud_predictions, ml_fraud_alerts, ml_model_metadata"
log_info "  WAL: logical replication enabled for CDC"
log_info "============================================="
log_info ""
log_info "BigApp connection string:"
log_info "  postgresql://postgres:secret@localhost:7432/demo"
log_info "============================================="

# Mark init as complete (used by healthcheck)
touch /var/lib/postgresql/.init_complete
log_info "Init complete marker created"

# Keep container running by tailing PostgreSQL log
tail -f /var/lib/postgresql/logfile &
wait $!
