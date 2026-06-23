#!/bin/bash
# ─────────────────────────────────────────────────────────
# For demonstration purposes only.
#
# Install & configure ClickHouse on EC2 host (Ubuntu 24.04)
# Run once. ClickHouse will be accessible at localhost:8123
# and from Docker containers via host.docker.internal:8123
# ─────────────────────────────────────────────────────────
set -e

echo "=== Installing ClickHouse ==="
curl https://clickhouse.com/install.sh | sudo bash

echo "=== Configuring ClickHouse to listen on all interfaces ==="
# ClickHouse by default only listens on localhost
# Add listen_host to make it accessible from Docker containers
sudo tee /etc/clickhouse-server/config.d/listen.xml > /dev/null << 'XMLEOF'
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <listen_host>::</listen_host>
</clickhouse>
XMLEOF

# Allow the experimental MaterializedPostgreSQL (not used but doesn't hurt)
sudo tee /etc/clickhouse-server/users.d/allow_experimental.xml > /dev/null << 'XMLEOF'
<clickhouse>
    <profiles>
        <default>
            <allow_experimental_database_materialized_postgresql>1</allow_experimental_database_materialized_postgresql>
        </default>
    </profiles>
</clickhouse>
XMLEOF

echo "=== Starting ClickHouse ==="
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server
sleep 3

echo "=== Verifying ClickHouse ==="
curl -sf http://localhost:8123/ping
echo ""
clickhouse-client --query "SELECT version()"

echo ""
echo "✅ ClickHouse installed and running!"
echo "   HTTP: http://localhost:8123"
echo "   CLI:  clickhouse-client"
echo "   From Docker containers: http://host.docker.internal:8123"
echo ""
echo "Next steps:"
echo "  1. cd /home/ubuntu/corebanking_full"
echo "  2. ./docker-run.sh destroy"
echo "  3. ./docker-run.sh start"
echo "  4. Open browser → Initialize"
