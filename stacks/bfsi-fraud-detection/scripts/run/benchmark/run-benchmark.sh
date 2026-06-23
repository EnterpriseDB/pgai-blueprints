#!/bin/bash

# Benchmark Run - runs some benchmark transactions
# For demonstration purposes only.

usage () {
    cat <<EOF

Usage:

    run-benchmark.sh SERVICE [TXCOUNT [CLIENTS [OPTS]]]

        Run one new benchmark. OPTS are passed verbatim to pgbench.

    run-benchmark.sh reset

        Reset the benchmark statistics

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
    TXCOUNT=$1
    shift
else
    TXCOUNT=10
fi

if [[ $# -ge 1 ]]; then
    CLIENTS=$1
    shift
else
    CLIENTS=1
fi

if [[ -f /scripts/tpa_cluster ]]; then
    export TPA_CLUSTER=$(cat /scripts/tpa_cluster)
else
    export TPA_CLUSTER=none
fi

if [[ $PGSERVICE == reset ]]; then
    echo "=== Reset starting ==="
    echo "[1/1] Reset pgbench stats..."

    cat /dev/null > /shared-tmp/pgbench-runs.out
    cat /dev/null > /shared-tmp/pgbench-runs.csv
    echo "[]" > /shared-tmp/pgbench-runs.json

    PGSERVICE=pgaa-docker psql -qf /scripts/util/pgbench-process-output.sql

    echo "=== Reset completed ==="
    echo
else
    echo "=== Benchmark starting ==="

    echo "[1/4] Running ${TXCOUNT} transactions with $CLIENTS clients..."

    t1="$(date --rfc-3339=ns --utc)"

    pgbench -N -t $TXCOUNT -c $CLIENTS $@ \
	    >  /shared-tmp/pgbench.out \
	    2> /shared-tmp/pgbench.err

    cat >> /shared-tmp/pgbench.out <<EOF
start pgbench: $t1
stop pgbench: $(date --rfc-3339=ns --utc)
postgres service: $PGSERVICE
tpa cluster: $TPA_CLUSTER
EOF

    psql -Aqtc "\\copy (SELECT mtime FROM public.pgbench_history) TO '/shared-tmp/pgbench-mtime.txt'"

    psql -Aqtf /scripts/util/pgbench-atps-mtps.sql \
	 >> /shared-tmp/pgbench.out

    cat /shared-tmp/pgbench.out >> /shared-tmp/pgbench-runs.out

    psql -Aqtc "
    SELECT format('[2/4] Found %s rows...',count(*))
    FROM public.pgbench_history
    "

    echo "[3/4] Parse pgbench output..."
    echo

    bash /scripts/util/pgbench-parse.sh /shared-tmp/pgbench-runs
    psql -f /scripts/util/pgbench-process-output.sql

    echo "[4/4] Display benchmark statistics..."
    echo
    grep -e '^tps' -e '^number of clients' /shared-tmp/pgbench.out

    echo
    echo "=== Benchmark completed ==="
    echo
fi
