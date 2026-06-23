"""
ML Audit Agent Wrapper for MLflow Evaluation.

Calls the existing LangFlow ML Algorithm Audit Agent via its API.
This wrapper enables MLflow evaluation of the LangFlow-hosted agent.

For demonstration purposes only.
"""

import os
import json
import requests


class LangFlowAgentClient:
    """
    Client for calling the LangFlow ML Audit Agent.
    Wraps the LangFlow API for MLflow evaluation.
    """

    def __init__(
        self,
        langflow_url: str = None,
        flow_name: str = "ML Algorithm Audit Agent",
    ):
        self.langflow_url = langflow_url or os.environ.get("LANGFLOW_URL", "http://localhost:7861")
        self.flow_name = flow_name
        self.flow_id = None
        self.auth_token = None
        self._init_client()

    def _init_client(self):
        """Initialize client and get auth token."""
        try:
            # Get auto-login token (LangFlow dev mode)
            response = requests.get(
                f"{self.langflow_url}/api/v1/auto_login",
                timeout=10
            )
            if response.ok:
                data = response.json()
                self.auth_token = data.get("access_token")

            # Find the ML Audit Agent flow
            self._find_flow()
        except Exception as e:
            print(f"[LangFlow] Init warning: {e}")

    def _get_headers(self) -> dict:
        """Get request headers with auth token."""
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        return headers

    def _find_flow(self):
        """Find the ML Audit Agent flow by name."""
        try:
            response = requests.get(
                f"{self.langflow_url}/api/v1/flows/",
                headers=self._get_headers(),
                timeout=10
            )
            if response.ok:
                flows = response.json()
                for flow in flows:
                    if self.flow_name.lower() in flow.get("name", "").lower():
                        self.flow_id = flow.get("id")
                        print(f"[LangFlow] Found flow: {flow.get('name')} ({self.flow_id})")
                        return

                # If exact match not found, try partial match
                for flow in flows:
                    if "audit" in flow.get("name", "").lower():
                        self.flow_id = flow.get("id")
                        print(f"[LangFlow] Using flow: {flow.get('name')} ({self.flow_id})")
                        return

            print(f"[LangFlow] Flow '{self.flow_name}' not found")
        except Exception as e:
            print(f"[LangFlow] Error finding flow: {e}")

    def run(self, question: str, timeout: int = 120) -> str:
        """
        Run the ML Audit Agent with a question.

        Args:
            question: The audit question to ask
            timeout: Request timeout in seconds

        Returns:
            Agent response text
        """
        if not self.flow_id:
            return "Error: ML Audit Agent flow not found in LangFlow"

        try:
            # LangFlow run endpoint
            run_url = f"{self.langflow_url}/api/v1/run/{self.flow_id}"

            payload = {
                "input_value": question,
                "output_type": "chat",
                "input_type": "chat",
            }

            response = requests.post(
                run_url,
                json=payload,
                headers=self._get_headers(),
                timeout=timeout
            )

            if not response.ok:
                return f"Error: LangFlow API returned {response.status_code}: {response.text}"

            result = response.json()

            # Extract the agent response from LangFlow output
            # LangFlow returns nested structure with outputs
            if "outputs" in result:
                outputs = result["outputs"]
                if isinstance(outputs, list) and len(outputs) > 0:
                    output = outputs[0]
                    if "outputs" in output:
                        inner_outputs = output["outputs"]
                        if isinstance(inner_outputs, list) and len(inner_outputs) > 0:
                            message = inner_outputs[0].get("results", {}).get("message", {})
                            if isinstance(message, dict):
                                return message.get("text", str(message))
                            return str(message)

            # Fallback: try to extract any text from response
            return self._extract_text(result)

        except requests.Timeout:
            return f"Error: Request timed out after {timeout}s"
        except Exception as e:
            return f"Error calling LangFlow agent: {str(e)}"

    def _extract_text(self, result: dict) -> str:
        """Extract text from various response formats."""
        if isinstance(result, str):
            return result

        # Try common paths
        for path in [
            ["outputs", 0, "outputs", 0, "results", "message", "text"],
            ["outputs", 0, "results", "message", "text"],
            ["outputs", 0, "message", "text"],
            ["message", "text"],
            ["text"],
            ["result"],
            ["output"],
        ]:
            try:
                value = result
                for key in path:
                    if isinstance(value, list):
                        value = value[key]
                    elif isinstance(value, dict):
                        value = value.get(key)
                    else:
                        break
                if isinstance(value, str) and value:
                    return value
            except (KeyError, IndexError, TypeError):
                continue

        # Last resort: return JSON
        return json.dumps(result, indent=2, default=str)

    def health_check(self) -> bool:
        """Check if LangFlow is healthy."""
        try:
            response = requests.get(f"{self.langflow_url}/health", timeout=5)
            return response.ok
        except Exception:
            return False


def predict_fn(question: str) -> str:
    """
    Prediction function for MLflow evaluation.

    Args:
        question: The audit question to ask the agent

    Returns:
        Agent response string
    """
    langflow_url = os.environ.get("LANGFLOW_URL", "http://langflow:7860")
    client = LangFlowAgentClient(langflow_url=langflow_url)

    if not question:
        return "Error: No question provided"

    return client.run(question)


# For testing outside MLflow
if __name__ == "__main__":
    import sys

    question = sys.argv[1] if len(sys.argv) > 1 else "What are the current fraud detection metrics?"

    client = LangFlowAgentClient()
    print(f"Health check: {client.health_check()}")
    print(f"\nQuestion: {question}")
    print(f"\nResponse:\n{client.run(question)}")
