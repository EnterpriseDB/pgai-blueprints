# For demonstration purposes only.

FROM postgres:17

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Build context is parent (stacks/bfsi-fraud-detection/) so we can see
# lakekeeper/. Compose mount overlays /tmp/create-warehouse.json during laptop dev.
COPY lakekeeper/create-warehouse.json /tmp/create-warehouse.json
