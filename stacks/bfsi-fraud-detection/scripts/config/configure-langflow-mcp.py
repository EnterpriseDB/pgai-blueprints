#!/usr/bin/env python3
"""
Configure pg-airman-mcp server in Langflow (foundational)
Links LangFlow to the MCP server for database queries

For demonstration purposes only.
"""

import json
import os
import sys
import time
from pathlib import Path

# Configuration
LANGFLOW_DATA_DIR = Path(os.getenv("LANGFLOW_DATA_DIR", "/var/lib/langflow"))
MCP_SERVER_NAME = "pg-airman-mcp"
MAX_RETRIES = 30
RETRY_DELAY = 2


def find_user_directory(data_dir: Path, max_retries: int = MAX_RETRIES) -> Path:
    """Find the Langflow user directory (UUID format)"""
    print(f"Finding Langflow user directory in {data_dir}...")

    for attempt in range(1, max_retries + 1):
        user_dirs = [d for d in data_dir.glob("*-*-*-*-*") if d.is_dir()]
        if user_dirs:
            print(f"  User directory found: {user_dirs[0]}")
            return user_dirs[0]

        if attempt == max_retries:
            print(f"  User directory not found after {max_retries} retries")
            sys.exit(1)

        print(f"  Attempt {attempt}/{max_retries} - retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)

    sys.exit(1)


def configure_mcp_server(user_dir: Path):
    """Configure MCP server in Langflow config file"""
    print("\n=== Configuring MCP Server ===")

    user_id = user_dir.name
    config_file = user_dir / f"_mcp_servers_{user_id}.json"

    # Load existing config
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        print("  Creating new MCP configuration file")
        config = {"mcpServers": {}}

    # Check if already configured
    if MCP_SERVER_NAME in config.get("mcpServers", {}):
        print(f"  {MCP_SERVER_NAME} already configured")
        return False  # No restart needed

    # Add pg-airman-mcp
    if "mcpServers" not in config:
        config["mcpServers"] = {}

    config["mcpServers"][MCP_SERVER_NAME] = {
        "command": "uvx",
        "args": [
            "mcp-proxy",
            "--transport",
            "sse",
            "http://pg-airman-mcp:8200/sse"
        ]
    }

    # Save config
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    config_file.chmod(0o644)

    print(f"  Added {MCP_SERVER_NAME} to configuration")
    return True  # Restart needed


def main():
    print("=" * 50)
    print("Langflow MCP Server Configuration")
    print("=" * 50)

    # Wait for data directory
    print("\nWaiting for Langflow data directory...")
    for attempt in range(1, MAX_RETRIES + 1):
        if LANGFLOW_DATA_DIR.exists():
            print("  Data directory found")
            break
        if attempt == MAX_RETRIES:
            print(f"  Data directory not found after {MAX_RETRIES} retries")
            sys.exit(1)
        time.sleep(RETRY_DELAY)

    # Find user directory
    user_dir = find_user_directory(LANGFLOW_DATA_DIR)

    # Configure MCP server
    mcp_changed = configure_mcp_server(user_dir)

    # Summary
    print("\n" + "=" * 50)
    print("MCP Configuration Complete!")
    print("=" * 50)
    print(f"\nMCP Server: {MCP_SERVER_NAME}")
    print(f"Endpoint: http://pg-airman-mcp:8200/sse")
    print("\nCustom components: mounted via /custom_components volume")

    if mcp_changed:
        print("\nNOTE: Restart Langflow to apply MCP config:")
        print("  docker restart bfsi-langflow")


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
