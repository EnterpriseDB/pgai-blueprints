#!/bin/bash
# For demonstration purposes only.
DC="docker compose"
case "$1" in
  start)
    echo "Cleaning up existing containers..."
    $DC down --remove-orphans 2>/dev/null || true
    docker network prune -f 2>/dev/null || true
    echo "Starting all services (ClickHouse, RisingWave, Zookeeper, Kafka, Kafka Connect, App)..."
    $DC up -d --build
    echo ""
    echo "✅ App        → http://$(hostname -I | awk '{print $1}'):3001"
    echo "✅ ClickHouse → http://localhost:8123"
    echo "✅ RisingWave → localhost:4566"
    echo "✅ Kafka      → localhost:9092"
    echo "✅ KC REST    → http://localhost:8083"
    echo ""
    echo "NOTE: Kafka Connect takes ~90s to start. RisingWave ~60s."
    echo "Wait for all containers healthy before clicking Initialize."
    ;;
  stop)    $DC stop && echo "Stopped." ;;
  update)
    echo "Rebuilding app only (keeps ClickHouse data)..."
    docker stop cb-app 2>/dev/null || true
    docker rm   cb-app 2>/dev/null || true
    $DC up -d --build app
    echo "✅ App restarted"
    ;;
  destroy)
    read -p "Delete ALL containers + volumes? (y/N): " c
    [[ $c == [yY] ]] || { echo "Cancelled."; exit 0; }
    $DC down -v --remove-orphans 2>/dev/null || true
    docker network prune -f 2>/dev/null || true
    echo "Destroyed."
    ;;
  logs)    $DC logs -f app ;;
  ch-logs) $DC logs -f clickhouse ;;
  rw-logs) $DC logs -f risingwave ;;
  kc-logs) $DC logs -f kafka-connect ;;
  status)  $DC ps ;;
  ch-query)
    shift; SQL="${*:-SELECT 1}"
    curl -s "http://localhost:8123/" --data-binary "$SQL"; echo ""
    ;;
  rw-sql)
    shift; SQL="${*:-SELECT 1}"
    psql -h localhost -p 4566 -U root -d dev -c "$SQL"
    ;;
  kc-status)
    echo "=== Connectors ==="; curl -s http://localhost:8083/connectors | python3 -m json.tool
    echo "=== Status ==="; curl -s http://localhost:8083/connectors/corebanking-postgres/status | python3 -m json.tool
    ;;
  *)
    echo "Usage: ./docker-run.sh start|stop|update|destroy|status"
    echo "       logs|ch-logs|rw-logs|kc-logs"
    echo "       ch-query \"SQL\"  |  rw-sql \"SQL\"  |  kc-status"
    ;;
esac
