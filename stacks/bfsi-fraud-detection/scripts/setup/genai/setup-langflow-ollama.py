#!/usr/bin/env python3
"""
Configure LangFlow with Ollama and MLflow Tracing

For demonstration purposes only.

Sets up OLLAMA_BASE_URL in LangFlow global variables so the Agent component
can discover and use Ollama models.

Environment Variables:
    OLLAMA_BASE_URL: Ollama server URL (default: http://host.docker.internal:11434)
    MLFLOW_TRACKING_URI: MLflow server URL (default: http://mlflow:5000)
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# Configuration
LANGFLOW_URL = os.getenv("LANGFLOW_URL", "http://langflow:7860")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
MAX_RETRIES = 30
RETRY_DELAY = 3


def api_request(url: str, method: str = "GET", data: dict = None, token: str = None) -> dict:
    """Make API request."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req_data = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        body = e.read().decode() if e.fp else ""
        print(f"  HTTP {e.code}: {body[:200]}")
        raise


def wait_for_langflow() -> bool:
    """Wait for LangFlow to be healthy."""
    print(f"Waiting for LangFlow at {LANGFLOW_URL}...")

    for i in range(MAX_RETRIES):
        try:
            result = api_request(f"{LANGFLOW_URL}/health")
            if result and result.get("status") == "ok":
                print("  LangFlow is healthy")
                return True
        except Exception:
            pass

        print(f"  Waiting... ({i+1}/{MAX_RETRIES})")
        time.sleep(RETRY_DELAY)

    return False


def wait_for_mlflow() -> bool:
    """Wait for MLflow to be healthy."""
    print("Waiting for MLflow...")

    for i in range(10):  # Shorter timeout for MLflow
        try:
            req = urllib.request.Request(f"{MLFLOW_URI}/health")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    print("  MLflow is healthy")
                    return True
        except Exception:
            pass

        print(f"  Waiting... ({i+1}/10)")
        time.sleep(RETRY_DELAY)

    return False


def check_ollama() -> tuple:
    """Check if Ollama is reachable and list available models."""
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            models = [m.get("name") for m in data.get("models", [])]
            return True, models
    except Exception as e:
        return False, str(e)


def get_access_token() -> str:
    """Get LangFlow access token via auto_login."""
    try:
        result = api_request(f"{LANGFLOW_URL}/api/v1/auto_login")
        return result.get("access_token", "")
    except Exception as e:
        print(f"  Error getting token: {e}")
        return ""


def get_variables(token: str) -> list:
    """Get all global variables from LangFlow."""
    try:
        return api_request(f"{LANGFLOW_URL}/api/v1/variables/", token=token) or []
    except Exception:
        return []


def set_variable(token: str, name: str, value: str, var_type: str = "Generic") -> bool:
    """Set a global variable in LangFlow (create or update)."""
    try:
        variables = get_variables(token)
        existing = next((v for v in variables if v.get("name") == name), None)

        if existing:
            var_id = existing["id"]
            data = {"id": var_id, "name": name, "type": var_type, "value": value, "default_fields": []}
            api_request(f"{LANGFLOW_URL}/api/v1/variables/{var_id}", method="PATCH", data=data, token=token)
        else:
            data = {"name": name, "type": var_type, "value": value, "default_fields": []}
            api_request(f"{LANGFLOW_URL}/api/v1/variables/", method="POST", data=data, token=token)
        return True
    except Exception as e:
        print(f"  Error setting variable {name}: {e}")
        return False


def main():
    print("=" * 55)
    print("LangFlow + Ollama + MLflow Tracing Setup")
    print("=" * 55)

    # Check Ollama
    print("\n=== Checking Ollama ===")
    print(f"  URL: {OLLAMA_URL}")
    ollama_ok, ollama_result = check_ollama()

    if not ollama_ok:
        print(f"  WARNING: Ollama not reachable: {ollama_result}")
        print("  Make sure Ollama is running on your host machine.")
        print("  Install: https://ollama.ai")
        print("  Pull a model: ollama pull llama3.2")
    else:
        print(f"  Available models: {', '.join(ollama_result[:5]) if ollama_result else 'none'}")

    # Check LangFlow
    print("\n=== Checking LangFlow ===")
    if not wait_for_langflow():
        print("ERROR: LangFlow not available")
        sys.exit(1)

    # Get LangFlow token
    token = get_access_token()
    if not token:
        print("ERROR: Could not get LangFlow access token")
        sys.exit(1)
    print("  LangFlow authenticated")

    # Configure Ollama in LangFlow global variables
    print("\n=== Configuring Ollama in LangFlow ===")
    if set_variable(token, "OLLAMA_BASE_URL", OLLAMA_URL):
        print(f"  OLLAMA_BASE_URL = {OLLAMA_URL}")
    else:
        print("  WARNING: Could not set OLLAMA_BASE_URL")

    # Configure MLflow tracing
    print("\n=== Configuring MLflow Tracing ===")
    if set_variable(token, "MLFLOW_TRACKING_URI", MLFLOW_URI):
        print(f"  MLFLOW_TRACKING_URI = {MLFLOW_URI}")
    else:
        print("  WARNING: Could not set MLFLOW_TRACKING_URI")

    # Create/get langflow-agents experiment in MLflow and set ID in global vars
    exp_name = "langflow-agents"
    exp_id = None
    try:
        exp_data = {"name": exp_name}
        req = urllib.request.Request(
            f"{MLFLOW_URI}/api/2.0/mlflow/experiments/create",
            data=json.dumps(exp_data).encode(),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            exp_id = result.get("experiment_id")
            print(f"  Created experiment '{exp_name}' (id: {exp_id})")
    except urllib.error.HTTPError as e:
        if e.code == 400:
            # Experiment exists, get its ID
            try:
                get_url = f"{MLFLOW_URI}/api/2.0/mlflow/experiments/get-by-name"
                get_url += f"?experiment_name={exp_name}"
                req = urllib.request.Request(get_url)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode())
                    exp_id = result.get("experiment", {}).get("experiment_id")
                    print(f"  Experiment '{exp_name}' exists (id: {exp_id})")
            except Exception:
                pass
        else:
            print(f"  Could not create experiment: {e}")
    except Exception as e:
        print(f"  Could not create experiment: {e}")

    if exp_id:
        set_variable(token, "MLFLOW_EXPERIMENT_ID", exp_id)
        print(f"  MLFLOW_EXPERIMENT_ID = {exp_id}")

    # Configure MLflow tracing
    print("\n=== MLflow Tracing ===")
    if wait_for_mlflow():
        print("  Tracing enabled")
        print("  View traces: http://127.0.0.1:5001/#/traces")
    else:
        print("  WARNING: MLflow not available, tracing disabled")

    # Summary
    print("\n" + "=" * 55)
    print("Setup Complete!")
    print("=" * 55)
    print("\nLangFlow UI: http://127.0.0.1:7861")
    print("MLflow UI:   http://127.0.0.1:5001")
    print(f"\nOllama URL: {OLLAMA_URL}")
    if ollama_ok and ollama_result:
        print(f"Models: {', '.join(ollama_result[:3])}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
