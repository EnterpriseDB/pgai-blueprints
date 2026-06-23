#!/bin/bash
# Setup LangFlow with Anthropic and MLflow Tracing
# Passes AWS credentials from host environment to the setup container
#
# For demonstration purposes only.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker run --rm --network bfsi-network \
  -v "$SCRIPT_DIR/setup-langflow-anthropic.py:/app/setup.py:ro" \
  -v "${HOME}/.aws:/root/.aws:ro" \
  -e ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" \
  python:3.11-slim python /app/setup.py
