-- For demonstration purposes only.

BEGIN;

CREATE TEMP TABLE h
( mtime timestamp NOT NULL
);

\copy h from '/shared-tmp/pgbench-mtime.txt'

WITH a(dt) AS (
  SELECT EXTRACT ( epoch FROM max(mtime) -
                              min(mtime) )
  FROM h
), b AS (
  SELECT *
  , count(*) OVER w AS tps
  FROM h
  WINDOW w AS (
    ORDER BY mtime
    RANGE BETWEEN '1 second' PRECEDING
    AND CURRENT ROW
  )
)
SELECT format('transactions found: %s
interval found: %s
maximum tps found: %s
average tps found: %s', count(*), dt, max(tps), count(*) / dt)
FROM a, b
GROUP BY dt;

ROLLBACK;
