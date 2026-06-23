"""Per-infra deploy translators.

For demonstration purposes only.

Each translator owns the logic to take a compose-defined stack and run it on
its target infra. The agent dispatches to the right translator based on the
deploy target. Public LabAgent methods remain unchanged so app.py / the UI
keep working — they just delegate here.

Currently shipping:
    northflank — NF SaaS / BYOC. See NorthflankTranslator.
    laptop    — `docker compose up` against the local Docker daemon.
                Handles both Docker Desktop and Colima (Mac/Linux). On
                Windows, Colima is unavailable so only Docker Desktop is
                offered. See LaptopTranslator.

The laptop translator owns preflight (Colima sizing, Apple-Silicon Rosetta,
docker context auto-switch, /var/run/docker.sock probe). The deploy commands
themselves still go through agent.run_command() so the LLM tool-use loop in
agent.py can drive them too.
"""

from .laptop import LaptopTranslator, colima_supported, host_os
from .northflank import NorthflankTranslator

__all__ = [
    "LaptopTranslator",
    "NorthflankTranslator",
    "colima_supported",
    "host_os",
]
