#!/usr/bin/env python3
"""
Configure LangFlow flows to use Ollama LLM.

For demonstration purposes only.

This script adds an Ollama model node to Agent-based flows and connects it.
Run after load-langflow-flows.py to configure the LLM.
"""

import json
import os
import sys
import urllib.request
import urllib.error
import uuid

LANGFLOW_URL = os.getenv("LANGFLOW_URL", "http://langflow:7860")
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")


def api_request(url: str, method: str = "GET", data: dict = None, token: str = None):
    """Make API request."""
    import gzip
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
            raw = response.read()
            if response.headers.get('Content-Encoding') == 'gzip':
                raw = gzip.decompress(raw)
            return json.loads(raw.decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"  HTTP {e.code}: {body[:300]}")
        raise


def get_token() -> str:
    result = api_request(f"{LANGFLOW_URL}/api/v1/auto_login")
    return result.get("access_token", "")


def get_flows(token: str) -> list:
    try:
        return api_request(f"{LANGFLOW_URL}/api/v1/flows/", token=token) or []
    except Exception:
        return []


def create_ollama_node(node_id: str, x_pos: float, y_pos: float) -> dict:
    """Create an Ollama LLM node definition for LangFlow."""
    return {
        "id": node_id,
        "type": "genericNode",
        "position": {"x": x_pos, "y": y_pos},
        "data": {
            "id": node_id,
            "type": "OllamaModel",
            "node": {
                "display_name": "Ollama",
                "description": "Generate text using Ollama Local LLMs.",
                "template": {
                    "_type": "Component",
                    "base_url": {
                        "type": "str",
                        "value": OLLAMA_URL,
                        "display_name": "Base URL",
                        "advanced": False,
                        "show": True,
                    },
                    "model_name": {
                        "type": "str",
                        "value": OLLAMA_MODEL,
                        "display_name": "Model Name",
                        "advanced": False,
                        "show": True,
                    },
                    "temperature": {
                        "type": "float",
                        "value": 0.1,
                        "display_name": "Temperature",
                        "advanced": True,
                        "show": True,
                    },
                    "mirostat": {
                        "type": "str",
                        "value": "Disabled",
                        "display_name": "Mirostat",
                        "advanced": True,
                        "show": True,
                        "options": ["Disabled", "Mirostat", "Mirostat 2.0"],
                    },
                    "format": {
                        "type": "str",
                        "value": "",
                        "display_name": "Format",
                        "advanced": True,
                        "show": True,
                    },
                },
                "base_classes": ["LanguageModel"],
                "outputs": [
                    {
                        "name": "model",
                        "display_name": "Language Model",
                        "types": ["LanguageModel"],
                        "selected": "LanguageModel",
                        "method": "build_model",
                        "cache": True,
                    }
                ],
            },
        },
    }


def create_edge(source_id: str, target_id: str) -> dict:
    """Create an edge connecting Ollama output to Agent model input."""
    edge_id = f"xy-edge__{source_id}-{target_id}"
    return {
        "id": edge_id,
        "source": source_id,
        "target": target_id,
        "sourceHandle": json.dumps({
            "dataType": "OllamaModel",
            "id": source_id,
            "name": "model",
            "output_types": ["LanguageModel"]
        }).replace('"', '\u0153'),
        "targetHandle": json.dumps({
            "fieldName": "model",
            "id": target_id,
            "inputTypes": ["LanguageModel"],
            "type": "model"
        }).replace('"', '\u0153'),
        "data": {
            "sourceHandle": {
                "dataType": "OllamaModel",
                "id": source_id,
                "name": "model",
                "output_types": ["LanguageModel"]
            },
            "targetHandle": {
                "fieldName": "model",
                "id": target_id,
                "inputTypes": ["LanguageModel"],
                "type": "model"
            }
        },
        "animated": False,
        "className": "",
    }


def configure_flow_with_ollama(token: str, flow: dict) -> bool:
    """Add Ollama node to a flow and connect to Agent."""
    flow_id = flow.get("id")
    flow_name = flow.get("name", "Unknown")
    data = flow.get("data", {})
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Find Agent node
    agent_node = None
    agent_pos = {"x": 0, "y": 0}
    for node in nodes:
        nd = node.get("data", {})
        ni = nd.get("node", {})
        if ni.get("display_name") == "Agent":
            agent_node = node
            agent_pos = node.get("position", {"x": 0, "y": 0})
            break

    if not agent_node:
        print(f"  No Agent node in flow '{flow_name}'")
        return False

    agent_id = agent_node.get("data", {}).get("id", agent_node.get("id"))

    # Check if Ollama node already exists
    for node in nodes:
        nd = node.get("data", {})
        if nd.get("type") == "OllamaModel":
            print(f"  Ollama already configured in '{flow_name}'")
            return False

    # Check if there's already an edge to the model input
    for edge in edges:
        th = edge.get("data", {}).get("targetHandle", {})
        if th.get("fieldName") == "model" and th.get("id") == agent_id:
            print(f"  Model already connected in '{flow_name}'")
            return False

    # Create Ollama node (position left of Agent)
    ollama_id = f"OllamaModel-{uuid.uuid4().hex[:5]}"
    ollama_x = agent_pos.get("x", 0) - 350
    ollama_y = agent_pos.get("y", 0) + 100
    ollama_node = create_ollama_node(ollama_id, ollama_x, ollama_y)

    # Create edge
    edge = create_edge(ollama_id, agent_id)

    # Update flow
    nodes.append(ollama_node)
    edges.append(edge)
    data["nodes"] = nodes
    data["edges"] = edges

    try:
        api_request(
            f"{LANGFLOW_URL}/api/v1/flows/{flow_id}",
            method="PATCH",
            data={"data": data},
            token=token
        )
        print(f"  Added Ollama ({OLLAMA_MODEL}) to '{flow_name}'")
        return True
    except Exception as e:
        print(f"  Failed to update '{flow_name}': {e}")
        return False


def main():
    print("=" * 50)
    print("Configure LangFlow Flows with Ollama")
    print("=" * 50)
    print(f"Ollama URL: {OLLAMA_URL}")
    print(f"Ollama Model: {OLLAMA_MODEL}")

    token = get_token()
    if not token:
        print("ERROR: Could not get access token")
        sys.exit(1)

    flows = get_flows(token)
    if not flows:
        print("No flows found")
        sys.exit(0)

    print(f"\nFound {len(flows)} flows")
    configured = 0

    for flow in flows:
        flow_name = flow.get("name", "")
        # Only configure flows that have "Agent" or "Audit" in name
        if "agent" in flow_name.lower() or "audit" in flow_name.lower():
            if configure_flow_with_ollama(token, flow):
                configured += 1

    print(f"\n{'=' * 50}")
    print(f"Configured {configured} flows with Ollama")
    print("=" * 50)


if __name__ == "__main__":
    main()
