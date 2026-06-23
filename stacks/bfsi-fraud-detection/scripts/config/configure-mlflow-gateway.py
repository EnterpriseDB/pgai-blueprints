#!/usr/bin/env python3
"""
MLflow AI Gateway Configuration Script

For demonstration purposes only.

Configures MLflow AI Gateway with routes for:
1. Claude models via Anthropic
2. Local models via Ollama (host-based)

The gateway provides a unified interface for LLM access with:
- Centralized API key management
- Request/response logging
- Rate limiting and governance
- Model switching without code changes

Usage:
    python configure-mlflow-gateway.py

Environment Variables:
    MLFLOW_TRACKING_URI: MLflow server URL (default: http://mlflow:5000)
    ANTHROPIC_API_KEY: Anthropic API key
    OLLAMA_BASE_URL: Ollama server URL (default: http://host.docker.internal:11434)
"""

import os
import sys
import time
import json
import anthropic
import requests
from typing import Optional

# Configuration
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GATEWAY_URI = f"{MLFLOW_URI}/gateway"

# Anthropic configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_DEFAULT_MODEL = os.getenv("ANTHROPIC_DEFAULT_MODEL", "claude-sonnet-4-6")

# Ollama configuration
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")


def wait_for_mlflow(max_retries: int = 30, delay: int = 5) -> bool:
    """Wait for MLflow server to be healthy."""
    print(f"Waiting for MLflow at {MLFLOW_URI}...")

    for i in range(max_retries):
        try:
            response = requests.get(f"{MLFLOW_URI}/health", timeout=5)
            if response.status_code == 200:
                print(f"  MLflow is healthy (attempt {i+1})")
                return True
        except requests.exceptions.RequestException:
            pass

        print(f"  Waiting... (attempt {i+1}/{max_retries})")
        time.sleep(delay)

    print("  ERROR: MLflow did not become healthy")
    return False


def create_api_key(name: str, provider: str, credentials: dict) -> Optional[str]:
    """
    Create an API key in MLflow Gateway.

    In MLflow 3.0, API keys are created via the UI or management API.
    This function uses the management API to create encrypted credentials.
    """
    try:
        payload = {
            "name": name,
            "provider": provider,
            "credentials": credentials
        }

        response = requests.post(
            f"{GATEWAY_URI}/api-keys",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code in [200, 201]:
            result = response.json()
            print(f"  Created API key: {name}")
            return result.get("api_key_id")
        elif response.status_code == 409:
            print(f"  API key already exists: {name}")
            # Try to get existing key ID
            return get_api_key_id(name)
        else:
            print(f"  Failed to create API key {name}: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"  Error creating API key {name}: {e}")
        return None


def get_api_key_id(name: str) -> Optional[str]:
    """Get API key ID by name."""
    try:
        response = requests.get(f"{GATEWAY_URI}/api-keys", timeout=10)
        if response.status_code == 200:
            keys = response.json().get("api_keys", [])
            for key in keys:
                if key.get("name") == name:
                    return key.get("api_key_id")
    except Exception:
        pass
    return None


def create_endpoint(name: str, provider: str, model: str, api_key_id: Optional[str] = None,
                   endpoint_type: str = "llm/v1/chat", config: dict = None) -> bool:
    """
    Create a gateway endpoint in MLflow.

    Endpoints provide unified access to LLM providers with:
    - OpenAI-compatible API format
    - Automatic request/response logging
    - Traffic routing and load balancing
    """
    try:
        payload = {
            "name": name,
            "endpoint_type": endpoint_type,
            "model": {
                "provider": provider,
                "name": model,
                "config": config or {}
            }
        }

        if api_key_id:
            payload["model"]["config"]["api_key_id"] = api_key_id

        response = requests.post(
            f"{GATEWAY_URI}/endpoints",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code in [200, 201]:
            print(f"  Created endpoint: {name} -> {provider}/{model}")
            return True
        elif response.status_code == 409:
            print(f"  Endpoint already exists: {name}")
            return True
        else:
            print(f"  Failed to create endpoint {name}: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"  Error creating endpoint {name}: {e}")
        return False


def configure_ollama_endpoints() -> int:
    """Configure Ollama endpoints for local models."""
    print(f"\nConfiguring Ollama endpoints (host: {OLLAMA_URL})...")

    # Check which models are available on Ollama
    available_models = get_ollama_models()

    if not available_models:
        print("  No Ollama models found. Pull models with: ollama pull <model>")
        print("  Recommended models: llama3.2, mistral, codellama, qwen2.5")
        # Create endpoints anyway for common models
        available_models = ["llama3.2", "mistral", "codellama"]

    success_count = 0
    for model in available_models[:5]:  # Limit to first 5 models
        if create_endpoint(
            name=f"ollama-{model.replace(':', '-').replace('.', '-')}",
            provider="ollama",
            model=model,
            config={"base_url": OLLAMA_URL}
        ):
            success_count += 1

    return success_count


def get_ollama_models() -> list:
    """Get list of available Ollama models."""
    try:
        response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=10)
        if response.status_code == 200:
            data = response.json()
            models = [m.get("name", "") for m in data.get("models", [])]
            if models:
                print(f"  Found {len(models)} Ollama models: {', '.join(models[:5])}")
            return models
    except requests.exceptions.RequestException as e:
        print(f"  Could not connect to Ollama at {OLLAMA_URL}: {e}")
    return []


def print_usage_examples():
    """Print usage examples for the configured gateway."""
    print("\n" + "=" * 70)
    print("MLFLOW AI GATEWAY CONFIGURED")
    print("=" * 70)

    print("""
Usage Examples:

1. Python (OpenAI SDK compatible):

   from openai import OpenAI

   client = OpenAI(
       base_url="http://localhost:5001/gateway/claude-sonnet/v1",
       api_key="not-needed"  # Auth handled by gateway
   )

   response = client.chat.completions.create(
       model="claude-sonnet",
       messages=[{"role": "user", "content": "Hello!"}]
   )

2. Python (MLflow client):

   import mlflow

   mlflow.set_tracking_uri("http://localhost:5001")

   # Query via gateway
   response = mlflow.gateway.query(
       route="claude-sonnet",
       data={"messages": [{"role": "user", "content": "Hello!"}]}
   )

2. Ollama (local):

   curl -X POST http://localhost:5001/gateway/ollama-llama3-2/invocations \\
     -H "Content-Type: application/json" \\
     -d '{"messages": [{"role": "user", "content": "Hello!"}]}'

MLflow UI: http://localhost:5001
Gateway endpoints: http://localhost:5001/gateway
""")


def main():
    """Main configuration flow."""
    print("=" * 70)
    print("MLflow AI Gateway Configuration")
    print("=" * 70)

    # Wait for MLflow to be ready
    if not wait_for_mlflow():
        sys.exit(1)

    ollama_count = 0

    # Configure Ollama endpoints
    ollama_count = configure_ollama_endpoints()

    # Print summary
    print("\n" + "-" * 70)
    print(f"Configuration Summary:")
    print(f"  Ollama (local) endpoints: {ollama_count}")
    print("-" * 70)

    if ollama_count > 0:
        print_usage_examples()
        print("Gateway configuration complete!")
    else:
        print("\nNo endpoints configured. Check your credentials and Ollama installation.")
        sys.exit(1)


if __name__ == "__main__":
    main()
