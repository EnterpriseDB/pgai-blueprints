-- For demonstration purposes only.

BEGIN;

CREATE TEMP TABLE t
( cluster text     NOT NULL
, service text     NOT NULL
, clients int      NOT NULL
, txs_pc  bigint   NOT NULL
, tps  numeric     NOT NULL
, mtps numeric     NOT NULL
, atps numeric     NOT NULL
, started timestamptz NOT NULL
);

\copy t from '/shared-tmp/pgbench-runs.csv' csv

CREATE TEMP VIEW v AS
SELECT cluster, txs_pc, service, clients
, count(*)              AS runs
, round(   avg(mtps),2) AS avg_mtps
, round(stddev(mtps),2) AS std_mtps
, round(   avg(atps),2) AS avg_atps
, round(stddev(atps),2) AS std_atps
FROM t
WHERE tps < txs_pc * clients
GROUP BY cluster, txs_pc, service, clients
ORDER BY cluster, txs_pc, service, clients;

\copy (TABLE v) to '/shared-tmp/summary.csv' CSV HEADER

--
-- Display an overview
--

\o /shared-tmp/summary.txt
\qecho # Overview (excluding Smoke tests)
\qecho
TABLE v;
\o

ROLLBACK;
