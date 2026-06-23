# For demonstration purposes only.

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates gcc build-essential curl \
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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir fastapi uvicorn python-multipart

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 8050

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8050"]
