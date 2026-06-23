# For demonstration purposes only.

# syntax=docker/dockerfile:1.4
FROM node:20-alpine

# Trust corporate SSL-intercepting proxy when present (no-op otherwise).
# Mounted via docker-compose secret `corp_cert`; see certs/README.md.
RUN --mount=type=secret,id=corp_cert,target=/tmp/corp-ca.pem \
    if [ -s /tmp/corp-ca.pem ]; then \
      mkdir -p /usr/local/share/ca-certificates && \
      cp /tmp/corp-ca.pem /usr/local/share/ca-certificates/corp-ca.crt && \
      cat /tmp/corp-ca.pem >> /etc/ssl/certs/ca-certificates.crt ; \
    fi

ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# curl + bash + nc + psql: for shell scripts (RisingWave, ClickHouse setup)
RUN apk add --no-cache curl bash netcat-openbsd postgresql-client
WORKDIR /app

# Build context is the parent (stacks/bfsi-fraud-detection/) so we can see
# app/, scripts/, db/. Laptop compose bind mounts overlay these paths during dev.
COPY app/package.json ./
RUN npm install --production
COPY app/server.js ./
COPY app/public/ ./public/

# Bake in pipeline-referenced read-only assets so NF has them at runtime.
COPY scripts /app/scripts
COPY db /app/db

EXPOSE 3001
CMD ["node", "server.js"]
