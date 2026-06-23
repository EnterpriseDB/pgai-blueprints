#!/usr/bin/env python3
"""
Configure LangFlow with AWS Bedrock and MLflow Tracing

For demonstration purposes only.

Uses the same AWS credentials as the main agent (from environment or .env).
Shows clear error if credentials are missing.

Environment Variables (same as agent):
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
    AWS_SESSION_TOKEN: AWS session token (for SSO)
    AWS_PROFILE: AWS profile name (alternative to keys)
    AWS_DEFAULT_REGION: AWS region (default: us-east-1)
    MLFLOW_TRACKING_URI: MLflow server URL
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# Configuration (runs in separate container on bfsi-network)
LANGFLOW_URL = os.getenv("LANGFLOW_URL", "http://langflow:7860")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
MAX_RETRIES = 30
RETRY_DELAY = 3

# Bedrock model mappings for LangFlow
BEDROCK_MODELS = {
    "claude-sonnet": "anthropic.claude-3-5-sonnet-20241022-v2:0",
    "claude-haiku": "anthropic.claude-3-5-haiku-20241022-v1:0",
}
CLAUDE_SONNET = BEDROCK_MODELS["claude-sonnet"]


def api_request(url: str, method: str = "GET", data: dict = None, token: str = None) -> dict:
    """Make API request."""
    import gzip
    headers = {"Content-Type": "application/json", "Accept": "application/json", "Accept-Encoding": "gzip, deflate"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req_data = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw_data = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                raw_data = gzip.decompress(raw_data)
            return json.loads(raw_data.decode())
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
            # LangFlow health endpoint is /health (not /api/v1/health)
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

    for i in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(f"{MLFLOW_URI}/health")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    print("  MLflow is healthy")
                    return True
        except Exception:
            pass

        print(f"  Waiting... ({i+1}/{MAX_RETRIES})")
        time.sleep(RETRY_DELAY)

    return False


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
            # Update existing - id must be in both URL and body
            var_id = existing["id"]
            data = {"id": var_id, "name": name, "type": var_type, "value": value, "default_fields": []}
            result = api_request(f"{LANGFLOW_URL}/api/v1/variables/{var_id}", method="PATCH", data=data, token=token)
        else:
            # Create new
            data = {"name": name, "type": var_type, "value": value, "default_fields": []}
            result = api_request(f"{LANGFLOW_URL}/api/v1/variables/", method="POST", data=data, token=token)

        # For Credential types, LangFlow masks the value on read (returns None)
        # So we just trust the API call succeeded if no exception
        if var_type == "Credential":
            return True

        # For non-credential types, verify the value was stored
        updated_vars = get_variables(token)
        found = next((v for v in updated_vars if v.get("name") == name), None)
        if found:
            stored_value = found.get("value") or ""
            if stored_value == value:
                return True
            else:
                print(f"    WARNING: Value mismatch for {name}.")
                return False
        else:
            print(f"    WARNING: Variable {name} not found after set")
            return False
    except Exception as e:
        print(f"  Error setting variable {name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def configure_ollama(token: str) -> bool:
    """Configure Ollama provider in LangFlow global variables."""
    ollama_url = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
    print(f"\n=== Configuring Ollama ===")
    print(f"  URL: {ollama_url}")

    if set_variable(token, "OLLAMA_BASE_URL", ollama_url):
        print("  OLLAMA_BASE_URL configured")
        return True
    return False


def read_aws_credentials_from_file():
    """
    Read AWS credentials from ~/.aws/credentials or SSO cache.
    Returns (access_key, secret_key, session_token, region) or (None, None, None, region).
    """
    import configparser

    profile = os.getenv("AWS_PROFILE", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip()

    # Try reading from credentials file
    creds_file = "/root/.aws/credentials"
    config_file = "/root/.aws/config"

    access_key = None
    secret_key = None
    session_token = None

    # Read credentials file
    if os.path.exists(creds_file):
        config = configparser.ConfigParser()
        config.read(creds_file)

        # Build list of profiles to try:
        # 1. Exact profile name from AWS_PROFILE
        # 2. Any profile containing "Bedrock" (case-insensitive)
        # 3. Any profile with aws_access_key_id (first found)
        # 4. default
        profiles_to_try = []
        if profile:
            profiles_to_try.append(profile)

        # Find profiles containing "bedrock"
        for section in config.sections():
            if "bedrock" in section.lower():
                profiles_to_try.append(section)

        # Add all other profiles
        for section in config.sections():
            if section not in profiles_to_try:
                profiles_to_try.append(section)

        profiles_to_try.append("default")

        for section in profiles_to_try:
            if section in config:
                access_key = config[section].get("aws_access_key_id", "").strip()
                secret_key = config[section].get("aws_secret_access_key", "").strip()
                session_token = config[section].get("aws_session_token", "").strip()
                if access_key and secret_key:
                    print(f"  Read credentials from ~/.aws/credentials [{section}]")
                    break

    # Read region from config file
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)

        # Profile section in config is "profile <name>" except for default
        config_section = profile if profile == "default" else f"profile {profile}"
        if config_section in config:
            file_region = config[config_section].get("region", "").strip()
            if file_region:
                region = file_region

    return access_key, secret_key, session_token, region


def configure_aws_bedrock_variables(token: str) -> bool:
    """Configure AWS Bedrock credentials as LangFlow global variables."""
    print("\n=== Configuring AWS Bedrock Global Variables ===")

    # First try environment variables
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    session_token = os.getenv("AWS_SESSION_TOKEN", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip()

    # Filter out MinIO credentials
    if access_key == "minioadmin":
        access_key = ""
    if secret_key == "minioadmin123":
        secret_key = ""

    # If no env vars, try reading from mounted ~/.aws files
    if not access_key or not secret_key:
        print("  No credentials in environment, checking ~/.aws files...")
        file_access, file_secret, file_token, file_region = read_aws_credentials_from_file()
        if file_access and file_secret:
            access_key = file_access
            secret_key = file_secret
            session_token = file_token or session_token
            region = file_region or region

    configured = 0

    # Set AWS region (always)
    if set_variable(token, "AWS_DEFAULT_REGION", region, var_type="Generic"):
        print(f"  AWS_DEFAULT_REGION = {region}")
        configured += 1

    # Set credentials if available
    if access_key:
        if set_variable(token, "AWS_ACCESS_KEY_ID", access_key, var_type="Credential"):
            print(f"  AWS_ACCESS_KEY_ID = {access_key[:8]}...")
            configured += 1

    if secret_key:
        if set_variable(token, "AWS_SECRET_ACCESS_KEY", secret_key, var_type="Credential"):
            print(f"  AWS_SECRET_ACCESS_KEY = ****")
            configured += 1

    if session_token:
        if set_variable(token, "AWS_SESSION_TOKEN", session_token, var_type="Credential"):
            print(f"  AWS_SESSION_TOKEN = ****")
            configured += 1

    # Set Bedrock model as a convenience variable
    if set_variable(token, "BEDROCK_MODEL_ID", CLAUDE_SONNET, var_type="Generic"):
        print(f"  BEDROCK_MODEL_ID = {CLAUDE_SONNET}")
        configured += 1

    if configured < 3:
        print("\n  WARNING: AWS credentials not fully configured!")
        print("  LangFlow flows may not be able to use Bedrock.")
        print("  Ensure ~/.aws/credentials has valid keys or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")

    print(f"  Configured {configured} global variable(s)")
    return configured > 0


def check_aws_credentials():
    """
    Check if AWS credentials are configured (same logic as agent).
    Returns (has_credentials, method_description)
    """
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    session_token = os.getenv("AWS_SESSION_TOKEN", "").strip()
    profile = os.getenv("AWS_PROFILE", "").strip()

    # Check for explicit access keys
    if access_key and secret_key:
        # Make sure these aren't MinIO credentials
        if access_key == "minioadmin":
            return False, "MinIO credentials detected (not Bedrock)"
        method = "access keys"
        if session_token:
            method = "SSO session (via access keys + session token)"
        return True, method

    # Check for AWS profile
    if profile:
        # Check if ~/.aws/credentials or ~/.aws/config exists
        aws_dir = os.path.expanduser("~/.aws")
        creds_file = f"{aws_dir}/credentials"
        config_file = f"{aws_dir}/config"
        if os.path.exists(creds_file) or os.path.exists(config_file):
            return True, f"AWS profile '{profile}'"

    # Check for mounted AWS directory (in container)
    if os.path.exists("/root/.aws/credentials"):
        return True, "AWS credentials file (mounted)"
    if os.path.exists("/root/.aws/config"):
        return True, "AWS config file (mounted)"

    return False, "No credentials found"


def get_flows(token: str) -> list:
    """Get all flows from LangFlow."""
    try:
        return api_request(f"{LANGFLOW_URL}/api/v1/flows/", token=token) or []
    except Exception:
        return []


def update_flow_to_bedrock(token: str, flow: dict) -> bool:
    """
    Update a flow to use AWS Bedrock instead of Ollama/OpenAI.
    """
    flow_id = flow.get("id")
    flow_name = flow.get("name", "Unknown")

    if not flow_id:
        return False

    data = flow.get("data", {})
    nodes = data.get("nodes", [])
    modified = False

    for node in nodes:
        node_data = node.get("data", {})
        node_info = node_data.get("node", {})
        template = node_info.get("template", {})

        # Check if this is an Agent component with model configuration
        if node_info.get("display_name") == "Agent":
            # Update model provider to Bedrock
            if "agent_llm" in template:
                current = template["agent_llm"].get("value", "")
                if current in ["Ollama", "OpenAI", ""]:
                    template["agent_llm"]["value"] = "Amazon Bedrock"
                    opts = template["agent_llm"].get("options", [])
                    if "Amazon Bedrock" not in opts:
                        template["agent_llm"]["options"] = ["Amazon Bedrock"] + opts
                    modified = True

            # Update model name to Claude Sonnet on Bedrock
            if "model_name" in template:
                current_model = template["model_name"].get("value", "")
                is_local = "gpt-oss" in current_model.lower()
                is_local = is_local or "ollama" in current_model.lower()
                if is_local or not current_model:
                    template["model_name"]["value"] = CLAUDE_SONNET
                    modified = True

    if modified:
        try:
            api_request(
                f"{LANGFLOW_URL}/api/v1/flows/{flow_id}",
                method="PATCH",
                data={"data": data},
                token=token
            )
            print(f"  Updated flow '{flow_name}' to use Bedrock")
            return True
        except Exception as e:
            print(f"  Failed to update flow '{flow_name}': {e}")

    return False


def main():
    print("=" * 55)
    print("LangFlow + AWS Bedrock + MLflow Tracing Setup")
    print("=" * 55)

    # Check AWS credentials FIRST
    print("\n=== Checking AWS Credentials ===")
    has_creds, creds_method = check_aws_credentials()

    if not has_creds:
        print("")
        print("=" * 55)
        print("ERROR: AWS Bedrock credentials not found!")
        print("=" * 55)
        print("")
        print("LangFlow needs the same AWS credentials as the agent.")
        print("These should be configured in your .env file or environment:")
        print("")
        print("  Option 1: AWS SSO (recommended)")
        print("    AWS_PROFILE=Bedrock")
        print("    (run: aws sso login --profile Bedrock)")
        print("")
        print("  Option 2: Access Keys")
        print("    AWS_ACCESS_KEY_ID=your-key")
        print("    AWS_SECRET_ACCESS_KEY=your-secret")
        print("    AWS_DEFAULT_REGION=us-east-1")
        print("")
        print("After setting credentials, restart the stack:")
        print("  cd stacks/bfsi-fraud-detection")
        print("  docker compose down && docker compose up -d")
        print("")
        sys.exit(1)

    print(f"  Credentials found: {creds_method}")
    print(f"  Region: {AWS_REGION}")

    # Check LangFlow
    print("\n=== Checking Services ===")
    if not wait_for_langflow():
        print("ERROR: LangFlow not available")
        sys.exit(1)

    # Get LangFlow token
    token = get_access_token()
    if not token:
        print("ERROR: Could not get LangFlow access token")
        sys.exit(1)
    print("  LangFlow authenticated")

    # Configure MLflow tracing
    print("\n=== MLflow Tracing ===")
    if wait_for_mlflow():
        print("  Tracing enabled")
        print("  View traces: http://127.0.0.1:5001/#/traces")
    else:
        print("  WARNING: MLflow not available, tracing disabled")

    # Configure MLflow global variables
    print("\n=== Configuring MLflow Global Variables ===")
    if set_variable(token, "MLFLOW_TRACKING_URI", MLFLOW_URI):
        print(f"  MLFLOW_TRACKING_URI = {MLFLOW_URI}")

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

    # Configure Ollama (for local model fallback)
    configure_ollama(token)

    # Configure AWS Bedrock global variables
    configure_aws_bedrock_variables(token)

    # Update flows to use Bedrock
    print("\n=== Updating Flows to Use Bedrock ===")
    flows = get_flows(token)
    if not flows:
        print("  No flows found (load flows first)")
    else:
        updated = 0
        for flow in flows:
            if update_flow_to_bedrock(token, flow):
                updated += 1
        print(f"  Updated {updated}/{len(flows)} flow(s)")

    # Summary
    print("\n" + "=" * 55)
    print("Setup Complete!")
    print("=" * 55)
    print("\nLangFlow UI: http://127.0.0.1:7861")
    print("MLflow UI:   http://127.0.0.1:5001")
    print(f"\nBedrock Model: {CLAUDE_SONNET}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"Auth Method: {creds_method}")


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
