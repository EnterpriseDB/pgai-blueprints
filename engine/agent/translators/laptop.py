"""Laptop (local Docker daemon) deploy translator.

For demonstration purposes only.

Handles both `laptop-docker` (Docker Desktop) and `laptop-colima` (Colima VM).
The deploy code itself is identical for the two — both end up running
`docker compose up -d --build`. The differences live in pre-flight:

  laptop-docker: just confirm Docker Desktop is running.
  laptop-colima: confirm the Colima VM is up AND sized for the chosen stack,
                 confirm the active `docker context` points at colima, and on
                 Apple Silicon recommend `--vm-type=vz --vz-rosetta` so the
                 `platform: linux/amd64` services (BFSI pgd) don't pay QEMU
                 emulation cost.

Mirrors the NorthflankTranslator pattern: agent owns dispatch, translator
owns the per-infra knowledge.
"""

from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
from pathlib import Path


def host_os() -> str:
    """'macos' | 'windows' | 'linux' | 'other' — for UI gating of Colima."""
    s = platform.system().lower()
    if s == "darwin":
        return "macos"
    if s == "windows":
        return "windows"
    if s == "linux":
        return "linux"
    return "other"


def colima_supported() -> bool:
    """Colima only ships for macOS + Linux. Windows users get Docker Desktop."""
    return host_os() in ("macos", "linux")


# Per-stack VM sizing for Colima. Numbers come from observed worst-case
# steady-state usage during BFSI demos (21 services + 10 init jobs).
# Update when stacks add heavy services.
HEAVY_STACKS = {
    "bfsi-fraud-detection":   {"cpu": 8, "memory": 32, "disk": 100},
    "core-banking-simulator": {"cpu": 6, "memory": 24, "disk": 80},
}

# Reasonable floor for any stack on Colima — the default 2 CPU / 4 GB / 60 GB
# is too small even for `_template`.
COLIMA_MIN = {"cpu": 4, "memory": 8, "disk": 60}


def detect_docker_runtime() -> str:
    """One of 'docker-desktop', 'colima', 'unknown', 'unavailable'.

    Reads `docker info` once. Same logic that lived at module level in
    agent.py before the translator split; kept here so the laptop-specific
    detection sits with the laptop code.
    """
    try:
        r = subprocess.run(
            ["docker", "info", "--format", "{{.Name}} {{.OperatingSystem}}"],
            capture_output=True, text=True, timeout=4,
        )
        if r.returncode != 0:
            return "unavailable"
        out = r.stdout.strip().lower()
        if "colima" in out:
            return "colima"
        if "docker desktop" in out or "docker-desktop" in out:
            return "docker-desktop"
        return "unknown"
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return "unavailable"


def _run(cmd: list[str], timeout: float = 5) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except FileNotFoundError:
        return 127, "", f"{cmd[0]}: not installed"
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]}: timed out"
    except Exception as e:
        return 1, "", str(e)


def _is_apple_silicon() -> bool:
    """True on M1/M2/M3/M4 Macs. False on Intel."""
    rc, out, _ = _run(["uname", "-m"], timeout=2)
    return rc == 0 and out.strip() in ("arm64", "aarch64")


def _active_docker_context() -> str:
    """Name of the currently selected docker CLI context.

    Returns '' if `docker context show` is unavailable (older clients) — in
    that case fall back to runtime detection. Typical values: 'colima',
    'desktop-linux', 'default'.
    """
    rc, out, _ = _run(["docker", "context", "show"], timeout=3)
    if rc == 0 and out:
        return out.strip()
    # Older docker CLIs don't have `context show`; fall through.
    return ""


def _colima_status() -> dict:
    """Best-effort snapshot of the active Colima VM.

    Combines `colima status --extended` (for liveness + arch + cpu/mem/disk)
    with `~/.colima/default/colima.yaml` (for vmType + rosetta, which the
    status output doesn't include in colima >=0.10).

    Returns: {
        'running': bool,
        'arch': str|None,       # 'aarch64' | 'x86_64'
        'cpu': int|None,
        'memory_gb': int|None,
        'disk_gb': int|None,
        'vm_type': str|None,    # 'vz' | 'qemu' | ...
        'rosetta': bool|None,
        'raw': str,
    }
    Returns {'running': False, ...} if colima isn't installed or no VM is up.
    """
    info: dict = {"running": False, "arch": None, "cpu": None,
                  "memory_gb": None, "disk_gb": None, "vm_type": None,
                  "rosetta": None, "raw": ""}

    if not shutil.which("colima"):
        info["raw"] = "colima not installed"
        return info

    rc, out, err = _run(["colima", "status", "--extended"], timeout=5)
    # colima prints all its info to stderr at level=info; stdout is usually
    # empty on success. Concatenate both so the regexes find what they need.
    info["raw"] = (out or "") + ("\n" + err if err else "")
    txt = info["raw"]
    if rc != 0 or "not running" in txt.lower():
        return info

    info["running"] = True

    # Sample colima 0.10 status output (lines come on stderr, prefixed with
    # a level=info timestamp):
    #   colima is running using macOS Virtualization.Framework
    #   arch: aarch64
    #   runtime: docker
    #   mountType: virtiofs
    #   docker socket: unix:///Users/.../docker.sock
    #   cpu: 8
    #   mem: 32GiB
    #   disk: 100GiB
    if "virtualization.framework" in txt.lower():
        info["vm_type"] = "vz"
    elif "qemu" in txt.lower():
        info["vm_type"] = "qemu"

    m = re.search(r"arch:\s*(\S+)", txt)
    if m:
        info["arch"] = m.group(1)

    m = re.search(r"cpu:\s*(\d+)", txt)
    if m:
        info["cpu"] = int(m.group(1))

    # Field name is `mem:` in colima 0.10 status; older versions used `memory:`.
    m = re.search(r"\b(?:mem|memory):\s*(\d+)\s*GiB", txt, re.IGNORECASE)
    if m:
        info["memory_gb"] = int(m.group(1))

    m = re.search(r"disk:\s*(\d+)\s*GiB", txt, re.IGNORECASE)
    if m:
        info["disk_gb"] = int(m.group(1))

    # vmType + rosetta aren't in the status output reliably — read the
    # persisted config. `vmType: vz` + `rosetta: true` is the authoritative
    # source on Apple Silicon.
    cfg = Path.home() / ".colima" / "default" / "colima.yaml"
    if cfg.exists():
        try:
            cfg_txt = cfg.read_text()
            m = re.search(r"^\s*vmType:\s*(\S+)", cfg_txt, re.MULTILINE)
            if m:
                vt = m.group(1).strip().strip('"').strip("'")
                if vt:
                    info["vm_type"] = vt
            m = re.search(r"^\s*rosetta:\s*(true|false)", cfg_txt,
                          re.MULTILINE | re.IGNORECASE)
            if m:
                info["rosetta"] = m.group(1).lower() == "true"
        except Exception:
            pass

    return info


def _compose_mounts_docker_sock(stack_dir: Path) -> bool:
    """True if the stack's compose file mounts /var/run/docker.sock.

    Used to surface a clear preflight check when the stack needs the socket
    (BFSI bank-app uses it to query container status via Docker API). Colima
    wires this through automatically via its context, but if the user
    accidentally points the CLI at a different context, the mount will
    silently bind a non-existent socket on the host.
    """
    compose = stack_dir / "docker-compose.yaml"
    if not compose.exists():
        return False
    try:
        return "/var/run/docker.sock" in compose.read_text()
    except Exception:
        return False


def _recommended_colima_start(stack_name: str | None) -> str:
    """The exact `colima start` command the user should run for this stack."""
    spec = HEAVY_STACKS.get(stack_name or "", COLIMA_MIN)
    base = f"colima start --cpu {spec['cpu']} --memory {spec['memory']} --disk {spec['disk']}"
    if _is_apple_silicon():
        # vz + rosetta are Apple-Silicon-only. They turn `platform: linux/amd64`
        # services (like BFSI pgd) from QEMU-emulated (slow) to near-native.
        base += " --vm-type=vz --vz-rosetta"
    return base


class LaptopTranslator:
    """Preflight + helpers for local-docker deploys.

    Deploy/destroy themselves are still driven by the existing chat tool-use
    loop (Claude generates `docker compose up/down` via run_command). What the
    translator adds is a real preflight that catches Colima sizing problems
    *before* `docker compose up` starts spawning containers that will OOM.
    """

    def __init__(self, agent):
        self.agent = agent
        self.runtime = detect_docker_runtime()

    # ── public entrypoints ──────────────────────────────────────────────

    def preflight(self, target: str, stack_name: str | None = None):
        """Return (checks, config, missing) tuples for the dispatcher.

        Shape matches NorthflankTranslator.preflight so agent.preflight() can
        fold both into one report.

        target: 'laptop-docker' | 'laptop-colima'
        """
        checks: list[dict] = []
        config: dict = {}
        missing: list[str] = []

        # 0. Gate Colima on Windows. We hide the option in the UI, but if
        # someone still calls the API with target='laptop-colima' on Windows,
        # surface a clear message instead of running through Colima checks
        # that will all return "not installed".
        if target == "laptop-colima" and not colima_supported():
            checks.append({"name": "Platform", "status": "fail",
                           "detail": f"Colima is not available on {host_os()}"})
            missing.append("Use Laptop · Docker Desktop on Windows.")
            return checks, config, missing

        # 1. Docker CLI present at all
        if not shutil.which("docker"):
            checks.append({"name": "Docker CLI", "status": "fail",
                           "detail": "`docker` not installed on PATH"})
            missing.append("Install docker CLI: brew install docker docker-compose")
            return checks, config, missing
        checks.append({"name": "Docker CLI", "status": "ok",
                       "detail": shutil.which("docker") or "found"})

        # 2. Runtime / daemon
        rt = self.runtime
        wanted = "docker-desktop" if target == "laptop-docker" else "colima"

        if rt == "unavailable":
            checks.append({"name": "Docker daemon", "status": "fail",
                           "detail": "no docker daemon reachable"})
            if target == "laptop-colima":
                missing.append(
                    "Start Colima:\n    " + _recommended_colima_start(stack_name) +
                    "\n    docker context use colima"
                )
            else:
                missing.append("Start Docker Desktop.")
            return checks, config, missing

        if rt == wanted:
            checks.append({"name": "Docker daemon", "status": "ok",
                           "detail": f"runtime = {rt}"})
        else:
            # Mismatch — offer the auto-switch instead of a vague warn.
            ctx = _active_docker_context()
            checks.append({
                "name": "Docker daemon",
                "status": "fail",
                "detail": f"you selected {wanted} but the active runtime is {rt} (context={ctx or 'unknown'})",
            })
            if target == "laptop-colima":
                missing.append(
                    "Switch the Docker CLI to Colima:\n    docker context use colima\n"
                    "(If Colima isn't running yet: " + _recommended_colima_start(stack_name) + ")"
                )
            else:
                missing.append(
                    "Switch the Docker CLI to Docker Desktop:\n    docker context use desktop-linux"
                )

        config["Deploy runtime"] = rt

        # 3. Colima-specific deep checks
        if target == "laptop-colima":
            self._colima_preflight(checks, config, missing, stack_name)

        # 4. /var/run/docker.sock probe for stacks that need it
        if stack_name and stack_name in self.agent.stacks:
            stack_dir = Path(self.agent.stacks[stack_name]["_path"])
            if _compose_mounts_docker_sock(stack_dir):
                # Probe via a throwaway alpine container; if the socket mount
                # works inside a container the same way it'll work for the
                # stack's services.
                rc, out, err = _run(
                    ["docker", "run", "--rm",
                     "-v", "/var/run/docker.sock:/var/run/docker.sock:ro",
                     "alpine:3", "sh", "-c",
                     "test -S /var/run/docker.sock && echo ok || echo missing"],
                    timeout=20,
                )
                if rc == 0 and "ok" in out:
                    checks.append({"name": "Docker socket mount", "status": "ok",
                                   "detail": "/var/run/docker.sock reachable inside containers"})
                else:
                    checks.append({"name": "Docker socket mount", "status": "warn",
                                   "detail": err or out or "probe failed"})
                    missing.append(
                        f"The {stack_name} stack mounts /var/run/docker.sock. "
                        "If you're on Colima, make sure the active context is `colima` "
                        "(docker context use colima)."
                    )

        # 5. EDB subscription token (required for pgd image build on first deploy)
        edb = os.environ.get("EDB_SUBSCRIPTION_TOKEN", "").strip()
        if edb:
            checks.append({"name": "EDB_SUBSCRIPTION_TOKEN", "status": "ok",
                           "detail": "set"})
            config["EDB_SUBSCRIPTION_TOKEN"] = edb[:8] + "..."
        else:
            checks.append({"name": "EDB_SUBSCRIPTION_TOKEN", "status": "warn",
                           "detail": "not set — pgd image build will fail"})
            missing.append(
                "Set EDB_SUBSCRIPTION_TOKEN in .env (from https://www.enterprisedb.com/repos-downloads)"
            )

        return checks, config, missing

    def ensure_context(self, target: str) -> tuple[bool, str]:
        """Switch the docker CLI context so it matches the chosen target.

        Returns (ok, message). Idempotent. Called from app.py before deploy
        when preflight detected a context mismatch and the user asked us to
        auto-correct.
        """
        wanted_ctx = "colima" if target == "laptop-colima" else "desktop-linux"
        rc, out, err = _run(["docker", "context", "use", wanted_ctx], timeout=5)
        if rc == 0:
            # Refresh cached runtime so subsequent preflights are accurate.
            self.runtime = detect_docker_runtime()
            return True, f"docker context → {wanted_ctx} (runtime now {self.runtime})"
        return False, err or out or f"could not switch to {wanted_ctx}"

    # ── internal ────────────────────────────────────────────────────────

    def _colima_preflight(self, checks: list, config: dict,
                          missing: list, stack_name: str | None):
        """Add Colima-VM-specific checks (sizing, vm-type, rosetta) in place."""
        status = _colima_status()
        if not status["running"]:
            checks.append({"name": "Colima VM", "status": "fail",
                           "detail": "VM not running"})
            missing.append("Start Colima:\n    " + _recommended_colima_start(stack_name))
            return

        # Sizing vs stack requirement
        need = HEAVY_STACKS.get(stack_name or "", COLIMA_MIN)
        cpu, mem, disk = status["cpu"], status["memory_gb"], status["disk_gb"]
        sizing_ok = (cpu and mem and disk and
                     cpu >= need["cpu"] and mem >= need["memory"] and disk >= need["disk"])
        detail = f"cpu={cpu} mem={mem}GiB disk={disk}GiB"
        if sizing_ok:
            checks.append({"name": "Colima VM size", "status": "ok",
                           "detail": detail + f" (need ≥{need['cpu']}/{need['memory']}/{need['disk']})"})
        else:
            checks.append({
                "name": "Colima VM size",
                "status": "fail",
                "detail": f"{detail} — {stack_name or 'this stack'} needs "
                          f"≥{need['cpu']} CPU, ≥{need['memory']} GB, ≥{need['disk']} GB disk",
            })
            missing.append(
                "Resize Colima:\n    colima stop\n    " +
                _recommended_colima_start(stack_name) +
                "\n    docker context use colima"
            )

        # Apple Silicon → recommend vz + Rosetta for amd64 services
        if _is_apple_silicon():
            stack_needs_amd64 = self._stack_needs_amd64(stack_name)
            if status["vm_type"] != "vz":
                level = "fail" if stack_needs_amd64 else "warn"
                checks.append({
                    "name": "Colima VM type",
                    "status": level,
                    "detail": f"vm_type={status['vm_type'] or 'qemu'} "
                              + ("(amd64 services will run under QEMU — slow)"
                                 if stack_needs_amd64 else "(vz is faster on Apple Silicon)"),
                })
                if stack_needs_amd64:
                    missing.append(
                        f"{stack_name} has platform=linux/amd64 services (pgd). "
                        "Restart Colima with Virtualization.framework + Rosetta:\n    "
                        + _recommended_colima_start(stack_name)
                    )
            elif stack_needs_amd64 and not status.get("rosetta"):
                checks.append({"name": "Rosetta in Colima", "status": "warn",
                               "detail": "vz is on but Rosetta is not; amd64 services will be slow"})
                missing.append(
                    "Re-create the Colima VM with Rosetta:\n    colima delete\n    "
                    + _recommended_colima_start(stack_name)
                )
            else:
                checks.append({"name": "Colima VM type", "status": "ok",
                               "detail": "vz" + (" + Rosetta" if status.get("rosetta") else "")})

        # Active docker context
        ctx = _active_docker_context()
        if ctx == "colima":
            checks.append({"name": "docker context", "status": "ok",
                           "detail": "colima"})
        elif ctx:
            checks.append({"name": "docker context", "status": "fail",
                           "detail": f"active context = {ctx}"})
            missing.append("docker context use colima")

        config["Colima"] = (
            f"vm_type={status['vm_type'] or '?'}, "
            f"cpu={cpu}, mem={mem}GiB, disk={disk}GiB"
        )

    def _stack_needs_amd64(self, stack_name: str | None) -> bool:
        """True if the stack pins any service to platform: linux/amd64."""
        if not stack_name or stack_name not in self.agent.stacks:
            return False
        compose = Path(self.agent.stacks[stack_name]["_path"]) / "docker-compose.yaml"
        if not compose.exists():
            return False
        try:
            return "linux/amd64" in compose.read_text()
        except Exception:
            return False
