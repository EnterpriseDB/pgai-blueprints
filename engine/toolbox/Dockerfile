# For demonstration purposes only.

FROM debian:bookworm-slim

# PostgreSQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    redis-tools \
    default-mysql-client \
    curl jq bash wget netcat-openbsd ca-certificates gnupg \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ClickHouse client (official repo)
RUN curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

# Kafka CLI tools (from Apache Kafka binary — needs Java)
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ADD https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz /tmp/kafka.tgz
RUN tar -xzf /tmp/kafka.tgz -C /opt && ln -s /opt/kafka_2.13-3.9.2 /opt/kafka && rm /tmp/kafka.tgz

# MinIO client
ADD https://dl.min.io/client/mc/release/linux-amd64/mc /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc

ENV PATH="/opt/kafka/bin:$PATH"

# Stay running — ready for `docker exec` at any time
ENTRYPOINT ["sleep", "infinity"]
