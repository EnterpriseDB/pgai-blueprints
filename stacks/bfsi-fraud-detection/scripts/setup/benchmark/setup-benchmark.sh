#!/bin/bash

# Benchmark Schema Setup - Creates benchmark tables
# For demonstration purposes only.

usage () {
    cat <<EOF

Usage:

    setup-benchmark.sh SERVICE [SCALE]

EOF
}

set -e

export PGUSER=${POSTGRES_USER:-postgres}
export PGDATABASE=${POSTGRES_DB:-demo}
export PGPASSFILE=/scripts/var-pgpass
export PGSERVICEFILE=/scripts/pg_service.conf

if [[ $# -ge 1 ]]; then
    export PGSERVICE=$1
    shift 1
else
    usage
    exit 1
fi

if [[ $# -ge 1 ]]; then
    SCALE=$1
    shift
else
    SCALE=10
fi

echo "=== Benchmark Schema Setup ==="

# Wait for Postgres
until psql -c '\q'; do
  echo "Waiting for Postgres..."
  sleep 2
done

echo "[1/2] (Re)creating tables..."
pgbench -i -s $SCALE --quiet

echo "[2/2] Verifying tables..."
psql -c "
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN
( 'pgbench_accounts'
, 'pgbench_branches'
, 'pgbench_history'
, 'pgbench_tellers'
) ORDER BY table_name
"

echo
echo "=== Benchmark Schema Ready ==="
echo
