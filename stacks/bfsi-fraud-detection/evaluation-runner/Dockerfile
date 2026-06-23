# For demonstration purposes only.
FROM python:3.11-slim

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
    mlflow \
    requests \
    pandas \
    numpy \
    jupyter \
    nbconvert

WORKDIR /eval

# Build context is parent (stacks/bfsi-fraud-detection/) so we can see
# evaluation/. Compose mount overlays this path during laptop dev.
COPY evaluation/ /eval/
