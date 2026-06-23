# For demonstration purposes only.
FROM ubuntu:24.04

# Build arguments
ARG PLATFORM=linux/amd64

# Install dependencies
RUN apt-get update && apt-get install -y \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  python3 \
  python3-pip \
  python3-venv \
  && rm -rf /var/lib/apt/lists/*

# Trust corporate SSL-intercepting proxy when present (no-op otherwise).
# Must run before any https calls (EDB curls below, EDB apt repos later).
# Mounted via docker-compose secret `corp_cert`; see certs/README.md.
RUN --mount=type=secret,id=corp_cert,target=/tmp/corp-ca.pem \
    if [ -s /tmp/corp-ca.pem ]; then \
      cp /tmp/corp-ca.pem /usr/local/share/ca-certificates/corp-ca.crt && \
      update-ca-certificates ; \
    fi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Add EDB repository (uses BuildKit secret to avoid leaking token in image layers)
RUN --mount=type=secret,id=edb_token \
  curl -1sLf "https://downloads.enterprisedb.com/$(cat /run/secrets/edb_token)/postgres_distributed/setup.deb.sh" | bash && \
  curl -1sLf "https://downloads.enterprisedb.com/$(cat /run/secrets/edb_token)/enterprise/setup.deb.sh" | bash && \
  curl -1sLf "https://downloads.enterprisedb.com/$(cat /run/secrets/edb_token)/dev/setup.deb.sh" | bash

# Install PGD + PGAA + AIDB + pgvector
RUN apt-get update && \
  echo "=== Available edb-pgextended packages ===" && \
  apt-cache search edb-pgextended-17 2>/dev/null || true && \
  echo "=== Installing packages ===" && \
  apt-get install -y \
  edb-pgd6-expanded-pgextended17 \
  edb-postgresextended-17-pgaa \
  && rm -rf /var/lib/apt/lists/*

# Install AIDB + pgvector
RUN apt-get update && \
  echo "=== Searching for aidb packages ===" && \
  (apt-cache search aidb 2>/dev/null || true) && \
  echo "=== Searching for pgvector packages ===" && \
  (apt-cache search pgvector 2>/dev/null || true) && \
  echo "=== Installing AIDB ===" && \
  (apt-get install -y edb-pgextended-17-aidb 2>/dev/null || \
   apt-get install -y edb-postgresextended-17-aidb 2>/dev/null || \
   echo "WARN: AIDB not found under either package name") && \
  echo "=== Installing pgvector ===" && \
  (apt-get install -y edb-postgresextended17-pgvector-0 2>/dev/null || \
   apt-get install -y edb-pgextended-17-pgvector 2>/dev/null || \
   echo "WARN: pgvector not found") && \
  rm -rf /var/lib/apt/lists/*

# Install VectorChord-BM25 + pg_tokenizer (hybrid search keyword arm).
# Required shared_preload_libraries entries are wired in by docker-entrypoint.sh.
# The PG17 PostgresExtended naming pattern is edb-postgresextended17-<name>
# (no dashes around the major version), confirmed via apt-cache search.
RUN apt-get update && \
  echo "=== Installing VectorChord-BM25 ===" && \
  (apt-get install -y edb-postgresextended17-vectorchord-bm25 2>/dev/null || \
   apt-get install -y edb-pg17-vectorchord-bm25 2>/dev/null || \
   echo "WARN: vectorchord-bm25 not found — hybrid search will fall back to tsvector") && \
  echo "=== Installing pg_tokenizer ===" && \
  (apt-get install -y edb-postgresextended17-pg-tokenizer 2>/dev/null || \
   apt-get install -y edb-pg17-pg-tokenizer 2>/dev/null || \
   echo "WARN: pg_tokenizer not found") && \
  rm -rf /var/lib/apt/lists/*

# Install jq
RUN apt-get update && apt-get install -y jq

# Install Python dependencies for config validation
RUN python3 -m pip install --break-system-packages tomli psycopg2-binary

# Create directories and set ownership
RUN mkdir -p /var/lib/postgresql/db /var/lib/postgresql/pgd-analytics && \
  chown -R postgres:postgres /var/lib/postgresql

# Copy entrypoint script + bake in bind-mounted assets so NF has them at runtime.
# Build context is now stacks/bfsi-fraud-detection/ (parent of pgd/) so we can see
# pgd/, db/, scripts/. Laptop bind mounts still overlay these paths during dev.
COPY pgd/docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Init SQL — runs on first container start via docker-entrypoint.sh
COPY db/01-infrastructure.sql /docker-entrypoint-initdb.d/01-infrastructure.sql

# Pipeline-referenced read-only assets (compose mounts these at /db and /scripts).
COPY --chown=postgres:postgres db /db
COPY --chown=postgres:postgres scripts /scripts

# Set environment
ENV PGDATA=/var/lib/postgresql/db
ENV PATH=/usr/lib/edb-pge/17/bin:$PATH
ENV POSTGRES_PASSWORD=secret

# Expose ports
# 5432: Direct Postgres connection
# 6432: PGD proxy read-write
# 6433: PGD proxy read-only
EXPOSE 5432 6432 6433

# Set user
USER postgres

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["postgres"]
