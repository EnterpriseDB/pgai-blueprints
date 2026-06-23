#!/usr/bin/env python3
"""
Load LangFlow flows from templates directory

For demonstration purposes only.

Called by: Usecase 5 pipeline (GenAI Fraud Audit)
"""

import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

# Configuration
LANGFLOW_URL = os.getenv("LANGFLOW_URL", "http://langflow:7860")
TEMPLATES_DIR = Path(os.getenv("TEMPLATES_DIR", "/templates"))


def api_request(endpoint: str, method: str = "GET", data: dict = None, token: str = None) -> dict:
    """Make API request to Langflow"""
    import gzip
    url = f"{LANGFLOW_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req_data = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw_data = response.read()
            # Handle gzip compressed responses
            if response.headers.get('Content-Encoding') == 'gzip':
                raw_data = gzip.decompress(raw_data)
            return json.loads(raw_data.decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def get_access_token() -> str:
    """Get access token from auto_login"""
    try:
        result = api_request("/api/v1/auto_login")
        return result.get("access_token", "")
    except Exception:
        return ""


def get_user_info(token: str) -> dict:
    """Get current user info"""
    try:
        return api_request("/api/v1/users/whoami", token=token)
    except Exception:
        return {}


def get_folders(token: str) -> list:
    """Get user folders"""
    try:
        return api_request("/api/v1/folders/", token=token) or []
    except Exception:
        return []


def get_flows(token: str) -> list:
    """Get all flows"""
    try:
        return api_request("/api/v1/flows/", token=token) or []
    except Exception:
        return []


def find_flow_by_name(token: str, flow_name: str) -> dict:
    """Find flow by name (exact or with duplicate suffix), returns flow dict or None

    Langflow auto-renames duplicates as 'Name (1)', 'Name (2)', etc.
    This finds the base name or any duplicate and returns the first match.
    """
    import re
    flows = get_flows(token)
    # Pattern matches exact name or name with (N) suffix
    pattern = re.compile(rf"^{re.escape(flow_name)}(?: \(\d+\))?$")
    for f in flows:
        if pattern.match(f.get("name", "")):
            return f
    return None


def find_all_flows_by_name(token: str, flow_name: str) -> list:
    """Find all flows matching name (exact or with duplicate suffix)"""
    import re
    flows = get_flows(token)
    pattern = re.compile(rf"^{re.escape(flow_name)}(?: \(\d+\))?$")
    return [f for f in flows if pattern.match(f.get("name", ""))]


def create_flow(token: str, flow_data: dict) -> dict:
    """Create a new flow"""
    return api_request("/api/v1/flows/", method="POST", data=flow_data, token=token)


def update_flow(token: str, flow_id: str, flow_data: dict) -> dict:
    """Update an existing flow"""
    return api_request(f"/api/v1/flows/{flow_id}", method="PATCH", data=flow_data, token=token)


def delete_flow(token: str, flow_id: str) -> bool:
    """Delete a flow"""
    try:
        api_request(f"/api/v1/flows/{flow_id}", method="DELETE", token=token)
        return True
    except Exception:
        return False


def load_flows_from_templates(token: str, user_id: str, folder_id: str):
    """Load flows from templates directory (idempotent - cleans duplicates, updates or creates)"""
    print("\n=== Loading Flows from Templates ===")

    if not TEMPLATES_DIR.exists():
        print(f"Templates directory not found: {TEMPLATES_DIR}")
        return 0, 0

    template_files = list(TEMPLATES_DIR.glob("*.json"))
    if not template_files:
        print("No template files found")
        return 0, 0

    created_count = 0
    updated_count = 0

    for template_file in template_files:
        print(f"\nProcessing: {template_file.name}")

        try:
            with open(template_file, 'r') as f:
                flow_data = json.load(f)

            flow_name = flow_data.get("name", template_file.stem)

            # Update user_id and folder_id
            flow_data["user_id"] = user_id
            flow_data["folder_id"] = folder_id

            # Remove id to let server generate new one
            flow_data.pop("id", None)

            # Find all existing flows with this name (including duplicates)
            existing_flows = find_all_flows_by_name(token, flow_name)

            if existing_flows:
                # Keep the first one, delete the rest (clean up duplicates)
                keep_flow = existing_flows[0]
                for dup_flow in existing_flows[1:]:
                    delete_flow(token, dup_flow["id"])
                    print(f"  Deleted duplicate '{dup_flow.get('name')}'")

                # Update the kept flow
                flow_id = keep_flow["id"]
                result = update_flow(token, flow_id, flow_data)
                if result:
                    print(f"  Updated flow '{flow_name}' (id: {flow_id})")
                    updated_count += 1
                else:
                    print(f"  Failed to update flow '{flow_name}'")
            else:
                # Create new flow
                result = create_flow(token, flow_data)
                if result and result.get("id"):
                    print(f"  Created flow '{flow_name}' (id: {result['id']})")
                    created_count += 1
                else:
                    print(f"  Failed to create flow '{flow_name}'")

        except Exception as e:
            print(f"  Error loading {template_file.name}: {e}")

    return created_count, updated_count


def main():
    print("=" * 50)
    print("LangFlow Flow Loader")
    print("=" * 50)

    # Get API token
    print("\n=== API Authentication ===")
    token = get_access_token()
    if not token:
        print("ERROR: Could not get access token")
        sys.exit(1)
    print("  Access token obtained")

    # Get user info
    user_info = get_user_info(token)
    user_id = user_info.get("id")
    if not user_id:
        print("ERROR: Could not get user ID")
        sys.exit(1)
    print(f"  User ID: {user_id}")

    # Get folder
    folders = get_folders(token)
    folder_id = folders[0]["id"] if folders else None
    if not folder_id:
        print("ERROR: No folders found")
        sys.exit(1)
    print(f"  Folder ID: {folder_id}")

    # Load flows
    created_count, updated_count = load_flows_from_templates(token, user_id, folder_id)

    # Summary
    print("\n" + "=" * 50)
    print(f"Flow Loading Complete! ({created_count} created, {updated_count} updated)")
    print("=" * 50)
    print(f"\nLangFlow UI: {LANGFLOW_URL.replace('langflow', '127.0.0.1').replace('7860', '7861')}")


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
