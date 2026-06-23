#!/bin/bash
# ============================================================================
# For demonstration purposes only.
# Status snapshot for the Airflow reconciliation demo.
# Re-runnable; no side effects.
# ============================================================================

AF_URL="http://127.0.0.1:8888"
AF_AUTH="admin:admin"
DAG_ID="fraud_reconciliation"

echo "=== Airflow Status ==="
echo ""

echo "--- Airflow Containers ---"
for svc in bfsi-airflow-postgres bfsi-airflow-scheduler bfsi-airflow-webserver; do
  if docker inspect -f '{{.State.Status}}' "$svc" >/dev/null 2>&1; then
    state=$(docker inspect -f '{{.State.Status}} ({{.State.Health.Status}})' "$svc" 2>/dev/null)
    echo "$svc: $state"
  else
    echo "$svc: not running"
  fi
done

echo ""
echo "--- Webserver Health ---"
if curl -fsS -o /dev/null -w "HTTP %{http_code} (latency %{time_total}s)\n" "$AF_URL/health" 2>/dev/null; then
  :
else
  echo "Webserver not reachable at $AF_URL (Run UC2 step 4 to start Airflow)"
  exit 0
fi

echo ""
echo "--- DAG: $DAG_ID ---"
dag_json=$(curl -sS -u "$AF_AUTH" "$AF_URL/api/v1/dags/$DAG_ID" 2>/dev/null)
if echo "$dag_json" | grep -q '"dag_id"'; then
  is_paused=$(echo "$dag_json" | python3 -c "import sys, json; print(json.load(sys.stdin).get('is_paused'))")
  schedule=$(echo "$dag_json" | python3 -c "import sys, json; print(json.load(sys.stdin).get('schedule_interval', {}).get('value', '?'))" 2>/dev/null)
  echo "registered: yes"
  echo "is_paused:  $is_paused"
  echo "schedule:   $schedule"
else
  echo "DAG not found (scheduler may still be parsing dags/)"
fi

echo ""
echo "--- Last 5 DAG Runs ---"
curl -sS -u "$AF_AUTH" \
  "$AF_URL/api/v1/dags/$DAG_ID/dagRuns?order_by=-execution_date&limit=5" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    runs = json.load(sys.stdin).get('dag_runs', [])
    if not runs:
        print('(no runs yet)')
    else:
        for r in runs:
            print(f\"{r.get('start_date','?'):<35}  state={r.get('state','?'):<10}  run_id={r.get('dag_run_id','?')}\")
except Exception as e:
    print(f'(failed to parse runs: {e})')
"

echo ""
echo "--- Reconciliation URLs ---"
echo "Airflow UI:           $AF_URL  (admin / admin)"
echo "DAG grid view:        $AF_URL/dags/$DAG_ID/grid"
echo "DAG graph view:       $AF_URL/dags/$DAG_ID/graph"
echo ""
echo "=== Airflow Check Complete ==="
