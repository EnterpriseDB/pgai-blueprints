# For demonstration purposes only.
FROM ghcr.io/mlflow/mlflow:v3.6.0

# Install curl for health checks and PostgreSQL/S3 drivers
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

# Install MLflow with GenAI (AI Gateway) support and provider SDKs
# - mlflow[genai]: Enables AI Gateway with LiteLLM for unified LLM access
# - anthropic: Claude API support (via Anthropic API)
# - ollama: Local Ollama model support
# - openai: OpenAI-compatible endpoints
RUN pip install --no-cache-dir \
    psycopg2-binary \
    anthropic \
    ollama \
    openai \
    'mlflow[genai]'

# Default command - MLflow server with gateway enabled
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
