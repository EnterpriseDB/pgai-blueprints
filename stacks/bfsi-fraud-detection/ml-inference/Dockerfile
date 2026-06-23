# For demonstration purposes only.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Trust corporate SSL-intercepting proxy when present (no-op otherwise).
# Mounted via docker-compose secret `corp_cert`; see certs/README.md.
RUN --mount=type=secret,id=corp_cert,target=/tmp/corp-ca.pem \
    if [ -s /tmp/corp-ca.pem ]; then \
      cp /tmp/corp-ca.pem /usr/local/share/ca-certificates/corp-ca.crt && \
      update-ca-certificates ; \
    fi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

RUN pip install --no-cache-dir \
    kafka-python \
    psycopg2-binary \
    clickhouse-connect \
    numpy \
    scikit-learn \
    xgboost \
    joblib \
    mlflow \
    boto3 \
    requests

WORKDIR /app

# Build context is parent dir (stacks/bfsi-fraud-detection/) so we can see
# scripts/ and models/. Each service overrides `command:` to pick which
# script to run; bake them all in so any service can launch its own.
COPY scripts/runtime/ /app/
COPY scripts/config/ /app/
COPY models /models
