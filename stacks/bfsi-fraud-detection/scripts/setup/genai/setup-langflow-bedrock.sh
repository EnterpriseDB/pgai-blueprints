#!/bin/bash
# Setup LangFlow with AWS Bedrock and MLflow Tracing
# Passes AWS credentials from host environment to the setup container
#
# For demonstration purposes only.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker run --rm --network bfsi-network \
  -v "$SCRIPT_DIR/setup-langflow-bedrock.py:/app/setup.py:ro" \
  -v "${HOME}/.aws:/root/.aws:ro" \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
  -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
  -e AWS_PROFILE="${AWS_PROFILE:-}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  python:3.11-slim python /app/setup.py
