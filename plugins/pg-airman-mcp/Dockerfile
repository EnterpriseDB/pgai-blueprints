# For demonstration purposes only.

# Build pg-airman-mcp from source with proper DNS rebinding configuration
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Install build dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates gcc libpq-dev python3-dev git patch \
  && rm -rf /var/lib/apt/lists/*

# Trust corporate SSL-intercepting proxy when present (no-op otherwise).
# Required before the git clone / uv sync below (both go over HTTPS).
# Mounted via docker-compose secret `corp_cert`; see certs/README.md.
RUN --mount=type=secret,id=corp_cert,target=/tmp/corp-ca.pem \
    if [ -s /tmp/corp-ca.pem ]; then \
      cp /tmp/corp-ca.pem /usr/local/share/ca-certificates/corp-ca.crt && \
      update-ca-certificates ; \
    fi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    GIT_SSL_CAINFO=/etc/ssl/certs/ca-certificates.crt

# Clone the official repository (DNS rebinding protection is now built-in upstream)
RUN git clone https://github.com/EnterpriseDB/pg-airman-mcp.git /app

# Build the application
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --no-dev


FROM python:3.12-slim-bookworm

# Install runtime dependencies (including libpq5 for PostgreSQL client library)
RUN apt-get update && apt-get install -y --no-install-recommends \
  dnsutils \
  iputils-ping \
  libpq5 \
  libpq-dev \
  net-tools \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --no-cache-dir --upgrade pip \
  && groupadd -r app --gid=1000 \
  && useradd -r -g app --uid=1000 --home-dir=/app --shell=/bin/bash app \
  && mkdir -p /app \
  && chown -R app:app /app

# Clean up only libsqlite3-0 (keep libldap and libpq for PostgreSQL connectivity)
RUN apt-get purge -y libsqlite3-0 \
  && apt-get clean

COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:$PATH"

LABEL org.opencontainers.image.description="Pg Airman MCP Agent - Docker-optimized build with proper DNS rebinding configuration"
LABEL org.opencontainers.image.source="https://github.com/EnterpriseDB/pg-airman-mcp"

# Copy docker-entrypoint.sh from the repo
RUN chmod +755 /app/docker-entrypoint.sh && chown app:app /app/docker-entrypoint.sh

# Switch to non-root user
USER app
WORKDIR /app

EXPOSE 8000

ENTRYPOINT ["/app/docker-entrypoint.sh", "pg-airman-mcp"]
CMD []
