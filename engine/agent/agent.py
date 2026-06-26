"""
EDB Postgres® AI Blueprints Agent - Claude API with streaming support.
Auto-detects Anthropic direct API or AWS Bedrock.

For demonstration purposes only.
"""

pgai_version = "v0.1rc9"

import os
import sys
import json
import time
import subprocess
import yaml
import logging
import urllib.request
import urllib.error
from pathlib import Path

logger = logging.getLogger("dbox")

# Per-infra deploy logic lives under translators/. The agent instantiates one
# translator per infra and forwards public API calls (deploy_to_northflank,
# preflight, etc.) so the UI / REST routes in app.py keep working unchanged.
from translators import LaptopTranslator, NorthflankTranslator, colima_supported, host_os
from translators.laptop import detect_docker_runtime as _detect_docker_runtime


class LabAgent:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.stacks = self._load_stacks()
        self.plugins = self._load_plugins()
        self.history = []
        self.client = None
        self.model = None
        # NF-specific state lives on the translator; agent exposes nf_deployments
        # and nf_plans via @property for backward-compat with app.py.
        self.nf = NorthflankTranslator(self)
        # Laptop translator owns preflight + Colima checks. Deploys themselves
        # still flow through the LLM tool-use loop's run_command() calls.
        self.laptop = LaptopTranslator(self)
        self.docker_runtime = self.laptop.runtime
        self.host_os = host_os()
        self.colima_supported = colima_supported()
        logger.info("[runtime] host_os=%s docker_runtime=%s colima_supported=%s",
                    self.host_os, self.docker_runtime, self.colima_supported)
        self.nf.sync_deployments()
        self._init_llm()

    # ─── Backward-compat property forwards for app.py ───────────────────────
    @property
    def nf_deployments(self):
        return self.nf.deployments

    @nf_deployments.setter
    def nf_deployments(self, value):
        self.nf.deployments = value

    @property
    def nf_plans(self):
        return self.nf.plans

    @nf_plans.setter
    def nf_plans(self, value):
        self.nf.plans = value

    # ─── Thin shims delegating to the Northflank translator ─────────────────
    # These keep the public API (deploy_to_northflank, etc.) stable so the UI
    # and existing REST routes in app.py keep working.
    def deploy_to_northflank(self, stack_name):
        return self.nf.deploy(stack_name)

    def stop_on_northflank(self, stack_name):
        return self.nf.stop(stack_name)

    def resume_on_northflank(self, stack_name):
        return self.nf.resume(stack_name)

    def destroy_on_northflank(self, stack_name):
        return self.nf.destroy(stack_name)

    def nuke_northflank_project(self):
        """Force-wipe every service, job, secret in the configured NF
        project, regardless of what the agent thinks is deployed. Escape
        hatch for when destroy fails because the agent's in-memory state
        is out of sync with NF (typical after a restart or a partial
        deploy that errored)."""
        return self.nf.nuke_project()

    def cancel_on_northflank(self, stack_name):
        """Request an in-flight NF deploy to stop. The deploy loop checks
        the flag before each service create; cleanup of any partial state
        happens inside deploy() itself."""
        self.nf.request_cancel(stack_name)
        return {"ok": True}

    def get_nf_status(self):
        return self.nf.get_status()

    def get_nf_console_url(self):
        return self.nf.get_console_url()

    def _init_llm(self):
        provider = os.environ.get("LLM_PROVIDER", "").strip().lower()
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        aws_profile = os.environ.get("AWS_PROFILE", "").strip()
        if provider == "bedrock" or (not anthropic_key and (aws_key or aws_profile)):
            self._init_bedrock()
        elif provider == "anthropic" or anthropic_key:
            self._init_anthropic()
        else:
            print("[agent] No API key found. Running in offline mode.")
            print("[agent] Set ANTHROPIC_API_KEY or AWS_PROFILE in .env")

    def _init_anthropic(self):
        try:
            import anthropic
            ssl_cert_file = os.environ.get("SSL_CERT_FILE", "").strip()
            disable_ssl = os.environ.get("DISABLE_SSL_VERIFY", "").lower() in ("1", "true", "yes")
            client_kwargs = {}
            if disable_ssl or ssl_cert_file:
                import httpx
                if disable_ssl:
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    client_kwargs["http_client"] = httpx.Client(verify=False)
                else:
                    client_kwargs["http_client"] = httpx.Client(verify=ssl_cert_file)
            self.client = anthropic.Anthropic(**client_kwargs)
            self.model = "claude-sonnet-4-6"
            ssl_note = " [SSL verify disabled]" if disable_ssl else (f" [CA={ssl_cert_file}]" if ssl_cert_file else "")
            print(f"[agent] Using Anthropic direct API ({self.model}){ssl_note}")
        except Exception as e:
            print(f"[agent] Anthropic init failed: {e}")

    def _init_bedrock(self):
        try:
            import anthropic
            import httpx
            aws_profile = os.environ.get("AWS_PROFILE", "").strip()
            aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip()

            # Check if we need to disable SSL verification (corporate proxy)
            ssl_cert_file = os.environ.get("SSL_CERT_FILE", "").strip()
            disable_ssl = os.environ.get("DISABLE_SSL_VERIFY", "").lower() in ("1", "true", "yes")

            if aws_profile:
                # Windows: Disable SSL verification for SSO credential fetch
                # (Corporate proxy certs conflict with AWS SSO endpoints)
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                # Clear SSL env vars that interfere with AWS SSO
                saved_ssl_env = {}
                for k in ["SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "AWS_CA_BUNDLE", "CURL_CA_BUNDLE"]:
                    if k in os.environ:
                        saved_ssl_env[k] = os.environ.pop(k)

                # Patch botocore to disable SSL verification
                import botocore.httpsession
                _original_send = botocore.httpsession.URLLib3Session.send
                def _patched_send(self, request):
                    self._verify = False
                    return _original_send(self, request)
                botocore.httpsession.URLLib3Session.send = _patched_send

                import boto3
                session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
                creds = session.get_credentials().get_frozen_credentials()

                # Restore original behavior
                botocore.httpsession.URLLib3Session.send = _original_send
                for k, v in saved_ssl_env.items():
                    os.environ[k] = v

                # Create httpx client with SSL settings for anthropic SDK
                # If corporate cert exists and SSL not explicitly disabled, try using it
                # Otherwise disable verification (needed when cert doesn't cover AWS endpoints)
                if disable_ssl or ssl_cert_file:
                    http_client = httpx.Client(verify=False if disable_ssl else ssl_cert_file)
                else:
                    http_client = None

                client_kwargs = {
                    "aws_access_key": creds.access_key,
                    "aws_secret_key": creds.secret_key,
                    "aws_session_token": creds.token,
                    "aws_region": aws_region,
                }
                if http_client:
                    client_kwargs["http_client"] = http_client

                self.client = anthropic.AnthropicBedrock(**client_kwargs)
                print(f"[agent] Using AWS Bedrock via SSO profile '{aws_profile}' ({aws_region})")
            else:
                self.client = anthropic.AnthropicBedrock(
                    aws_region=aws_region,
                )
                print(f"[agent] Using AWS Bedrock via access keys ({aws_region})")
            self.model = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        except Exception as e:
            print(f"[agent] Bedrock init failed: {e}")

    def _load_stacks(self):
        stacks = {}
        stacks_dir = self.project_root / "stacks"
        if not stacks_dir.exists():
            return stacks
        for d in stacks_dir.iterdir():
            if not d.is_dir() or d.name.startswith(("_", ".")):
                continue
            meta_file = d / "stack.yaml"
            compose_file = d / "docker-compose.yaml"
            if meta_file.exists() and compose_file.exists():
                with open(meta_file, encoding="utf-8") as f:
                    stacks[d.name] = yaml.safe_load(f)
                stacks[d.name]["_path"] = str(d)
        return stacks

    def _load_plugins(self):
        plugins = {}
        plugins_dir = self.project_root / "plugins"
        if not plugins_dir.exists():
            return plugins
        for d in plugins_dir.iterdir():
            if d.is_dir() and not d.name.startswith("_"):
                meta_file = d / "plugin.yaml"
                compose_file = d / "docker-compose.yaml"
                if meta_file.exists() and compose_file.exists():
                    with open(meta_file, encoding="utf-8") as f:
                        plugins[d.name] = yaml.safe_load(f)
                    plugins[d.name]["_path"] = str(d)
        return plugins

    def preflight(self, target, stack_name=None):
        """Return a structured pre-flight report for the chosen deploy target.

        target: one of 'laptop-docker', 'laptop-colima', 'northflank'
        stack_name: optional — adds stack-specific checks (e.g. GHCR_IMAGES coverage)

        Returns: {
            'target': str,
            'ok': bool,
            'checks': [{'name': str, 'status': 'ok'|'warn'|'fail', 'detail': str}, ...],
            'config': {'<title>': '<semi-masked value>'},
            'missing_guidance': [str, ...],   # human-readable instructions
        }
        """
        def mask(v, keep_prefix=8, keep_suffix=0):
            if not v:
                return ""
            if len(v) <= keep_prefix + keep_suffix + 3:
                return v[:keep_prefix] + "..."
            return v[:keep_prefix] + "..." + (v[-keep_suffix:] if keep_suffix else "")

        checks = []
        config = {}
        missing = []

        if target in ("laptop-docker", "laptop-colima"):
            lt_checks, lt_config, lt_missing = self.laptop.preflight(target, stack_name)
            checks.extend(lt_checks)
            config.update(lt_config)
            missing.extend(lt_missing)

        elif target == "northflank":
            # NF preflight lives in the Northflank translator. The translator
            # returns (checks, config, missing) tuples that we fold in here so
            # the report shape is identical to before the refactor.
            nf_checks, nf_config, nf_missing = self.nf.preflight(stack_name)
            checks.extend(nf_checks)
            config.update(nf_config)
            missing.extend(nf_missing)

        # Stack-specific checks
        if stack_name and stack_name in self.stacks:
            meta = self.stacks[stack_name]
            targets = meta.get("deploy_targets", ["laptop-docker", "laptop-colima"])
            # Expand legacy 'laptop' for matching
            expanded = set()
            for t in targets:
                if t == "laptop":
                    expanded.update({"laptop-docker", "laptop-colima"})
                else:
                    expanded.add(t)
            if target not in expanded:
                checks.append({"name": f"Stack '{stack_name}' opts in", "status": "fail",
                               "detail": f"deploy_targets={sorted(expanded)}"})
                missing.append(f"Add '{target}' to stacks/{stack_name}/stack.yaml deploy_targets.")
            else:
                checks.append({"name": f"Stack '{stack_name}' opts in", "status": "ok",
                               "detail": f"target {target} allowed"})

        ok = all(c["status"] != "fail" for c in checks)
        return {
            "target": target,
            "ok": ok,
            "checks": checks,
            "config": config,
            "missing_guidance": missing,
        }


    # ─────────────────────────────────────────────────────────────────────

    def _get_system_prompt(self):
        stack_list = ""
        for name, meta in self.stacks.items():
            stack_list += f"\n  - {name}: {meta.get('description', 'No description')}"
            if "access" in meta:
                for a in meta["access"]:
                    stack_list += f"\n      {a['name']}: {a['url']}"
            if "credentials" in meta:
                creds = meta["credentials"]
                if isinstance(creds, list):
                    for c in creds:
                        if isinstance(c, dict):
                            stack_list += f"\n      {c.get('service','')}: user={c.get('username','')}, pass={c.get('password','')}"
                elif isinstance(creds, dict):
                    stack_list += f"\n      user={creds.get('username','')}, pass={creds.get('password','')}"
            if "sample_commands" in meta:
                for sc in meta["sample_commands"]:
                    if isinstance(sc, dict):
                        stack_list += f"\n      $ {sc.get('command', sc.get('name', ''))}"
                    else:
                        stack_list += f"\n      $ {sc}"
            if "after_deploy" in meta:
                stack_list += f"\n      After deploy info: {meta['after_deploy'][:500]}"
            if "pipelines" in meta:
                for p in meta["pipelines"]:
                    stack_list += f"\n      Use Case '{p['name']}': {', '.join(s['name'] for s in p['steps'])}"

        plugin_list = ""
        for name, meta in self.plugins.items():
            plugin_list += f"\n  - {name}: {meta.get('description', 'No description')}"

        return f"""You are the EDB Postgres® AI Blueprints assistant (v{pgai_version}, Docker Compose runtime).
You help users deploy, manage, and explore data integration environments.

Available integrations:{stack_list or ' (none found)'}

Available plugins:{plugin_list or ' (none found)'}

When deploying an integration or plugin, use the run_command tool with docker compose commands.
Project root: {self.project_root}

DEPLOYMENT: cd <stack-path> && docker compose up -d --build && for p in $(docker compose config --profiles 2>/dev/null); do docker compose --profile "$p" build 2>/dev/null || true; done
  - The second loop pre-builds images for every profile-gated service (e.g. airflow) so runtime toggles warm-boot in seconds instead of blocking on a multi-minute image build the first time a user flips them. No containers are started; only images are written to the Docker cache. Stacks with no profiles get a no-op loop.
  - Stack paths vary; use the _path metadata.
TEARDOWN: cd <stack-path> && docker compose down -v -t 2 --remove-orphans
STATUS: docker ps --filter 'label=com.docker.compose.project' --format 'table {{{{.Names}}}}\\t{{{{.Status}}}}\\t{{{{.Ports}}}}'
LOGS: docker logs <container-name> --tail 50

DEPLOY LIMIT: Maximum 2 integrations can run at the same time. Before deploying, check running integrations with 'docker compose ls'. If 2 are already running, tell the user to destroy one first. This limit ensures port conflicts are manageable.
IMPORTANT: Ignore 'synthdb' in the running count — it is an infrastructure service, not a user integration. Only count projects under stacks/ or plugins/ folders.

RESPONSE RULES:
- Keep ALL responses concise. No emojis. No ## headers. Short paragraphs.
- After build_stack tool: do NOT add connection details, URLs, ports, or credentials. The tool output is the complete message. Just confirm build is done and remind to deploy.
- After deploy: just confirm containers are running and count. Do NOT repeat access URLs, credentials, sample commands, or next steps — these are already shown in the Workspace tab. Say "Check the Workspace tab for details."
- After destroy: just confirm it's stopped. No extra commentary.
- When listing use cases, call them "Available Use Cases" not "Available Pipelines".
- Do not repeat information already shown in tool output or the stack info card.

FRAMEWORK GUIDE (when users ask how to add their own integration):
Keep the response short — 5 lines max:
1. cp -r stacks/_template stacks/your-name/
2. Drop in your docker-compose.yaml (existing files work as-is)
3. Edit stack.yaml with name, components, credentials, and use case steps
4. Click "Reload integrations" in the UI — no restart needed
That's it. The _template folder has examples and Dockerfile templates for Python, Node, Java.

INTEGRATION BUILDER (when users want to create an integration from plugins):
Use the build_stack tool when users ask to combine plugins into a new integration.
Available plugins: {', '.join(self.plugins.keys())}
After building, just confirm and remind to deploy. No connection details until after deploy."""

    def _get_tools(self):
        return [{
            "name": "run_command",
            "description": "Run a shell command. Use for docker compose up/down, docker ps, docker logs, docker exec.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"}
                },
                "required": ["command"]
            }
        }, {
            "name": "build_stack",
            "description": "Build a new integration by combining plugins. Creates a docker-compose.yaml with all selected plugins on a shared network, plus a stack.yaml metadata file. Use when users ask to create/build a custom integration from plugins.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "stack_name": {"type": "string", "description": "Name for the new stack (e.g. 'my-pg-ch-stack'). Lowercase, hyphens ok."},
                    "plugins": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of plugin names to include (e.g. ['postgres', 'clickhouse'])"
                    },
                    "description": {"type": "string", "description": "Short description of what this stack does"}
                },
                "required": ["stack_name", "plugins"]
            }
        }]

    # ---------------------------------------------------------------------------
    # Allowlist — only these command prefixes are permitted through run_command.
    # This makes the SECURITY.md §1 claim true in code, not just documentation.
    # ---------------------------------------------------------------------------
    _CMD_ALLOWLIST = (
        "docker compose ",
        "docker-compose ",
        "docker ps",
        "docker logs",
        "docker exec",
        "docker stats",
        "docker inspect",
        "docker images",
        "docker network",
        "docker volume",
        "docker info",
        "cd ",    # always paired with docker compose, e.g. cd stacks/foo && docker compose up
        "make ",  # Makefile targets that forward to docker compose
        "grep ",
        "for ", "do ", "true", "done"
    )

    def _is_allowed_command(self, cmd: str) -> bool:
        """
        Validate every &&/;/|-separated segment against the allowlist.
        Returns False (and logs a warning) if any segment is not permitted.
        """
        import re as _re
        segments = _re.split(r"&&|;|\|", cmd.strip())
        for seg in segments:
            seg = seg.strip()
            if not seg:
                continue
            if not any(seg.startswith(prefix) for prefix in self._CMD_ALLOWLIST):
                logger.warning("[CMD] BLOCKED (not in allowlist): %s", seg[:120])
                return False
        return True

    def run_command(self, cmd):
        logger.info("[CMD] %s", cmd[:150])
        # Security: reject anything outside the allowlist before touching the shell
        if not self._is_allowed_command(cmd):
            return (
                "ERROR: Command blocked by security allowlist. "
                "Only docker compose / docker ps / docker logs / docker exec / "
                "docker stats / docker inspect are permitted."
            )
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True,
                timeout=600, cwd=str(self.project_root)
            )
            output = result.stdout
            if result.stderr:
                output += "\n" + result.stderr
            if result.returncode != 0:
                logger.warning("[CMD] exit=%d: %s", result.returncode, cmd[:80])
            return output.strip() or "(no output)"
        except subprocess.TimeoutExpired:
            logger.error("[CMD] TIMEOUT: %s", cmd[:80])
            return "ERROR: Command timed out after 10 minutes"
        except Exception as e:
            logger.error("[CMD] ERROR: %s — %s", cmd[:80], str(e))
            return f"ERROR: {str(e)}"

    def _get_all_used_ports(self):
        """Scan all stack/plugin compose files to find host ports in use."""
        used = set()
        scan_dirs = []
        stacks_dir = self.project_root / "stacks"
        if stacks_dir.exists():
            for d in stacks_dir.iterdir():
                if d.is_dir() and not d.name.startswith(("_", ".")):
                    scan_dirs.append(d)
        plugins_dir = self.project_root / "plugins"
        if plugins_dir.exists():
            for d in plugins_dir.iterdir():
                if d.is_dir() and not d.name.startswith(("_", ".")):
                    scan_dirs.append(d)
        for d in scan_dirs:
            cf = d / "docker-compose.yaml"
            if cf.exists():
                with open(cf) as f:
                    comp = yaml.safe_load(f) or {}
                for svc in comp.get("services", {}).values():
                    for p in svc.get("ports", []):
                        host_port = str(p).split(":")[0].strip('"')
                        try:
                            used.add(int(host_port))
                        except ValueError:
                            pass
        used.add(4000)
        return used

    def _get_running_stacks(self):
        """Return list of currently running compose projects."""
        result = subprocess.run(
            "docker compose ls --format json 2>/dev/null || echo '[]'",
            shell=True, capture_output=True, text=True, timeout=10
        )
        try:
            projects = json.loads(result.stdout.strip())
            return [p.get("Name", "") for p in projects if p.get("Status", "").startswith("running")]
        except (json.JSONDecodeError, TypeError):
            return []

    def _find_free_port(self, desired, used_ports):
        """Find a free port starting from desired, incrementing until one is available."""
        port = desired
        while port in used_ports:
            port += 1
            if port > 65535:
                return desired  # fallback
        return port

    def _get_stack_dir(self, stack_name):
        """Get the directory path for a stack, using _path metadata if available."""
        if stack_name in self.stacks and "_path" in self.stacks[stack_name]:
            return Path(self.stacks[stack_name]["_path"])
        return self.project_root / "stacks" / stack_name

    def build_stack(self, stack_name, plugin_names, description=""):
        """Build a new stack by combining plugin compose definitions on a shared network with smart port allocation."""
        stack_name = stack_name.lower().strip().replace(" ", "-")
        stack_dir = self.project_root / "stacks" / stack_name
        if stack_dir.exists():
            return f"ERROR: Stack '{stack_name}' already exists at {stack_dir}"

        missing = [p for p in plugin_names if p not in self.plugins]
        if missing:
            return f"ERROR: Unknown plugins: {', '.join(missing)}. Available: {', '.join(self.plugins.keys())}"

        if len(plugin_names) < 1:
            return "ERROR: Need at least one plugin to build a stack"

        # Create stack dir early (needed for copying build context files)
        stack_dir.mkdir(parents=True)

        # Smart port allocation: scan all existing compose files for used ports
        used_ports = self._get_all_used_ports()
        port_map = {}  # original_port -> assigned_port

        # Read each plugin's docker-compose.yaml
        services = {}
        volumes = {}
        assigned_ports = []

        for plugin_name in plugin_names:
            plugin_meta = self.plugins[plugin_name]
            plugin_path = Path(plugin_meta["_path"])
            compose_file = plugin_path / "docker-compose.yaml"

            with open(compose_file) as f:
                plugin_compose = yaml.safe_load(f)

            for svc_name, svc_def in plugin_compose.get("services", {}).items():
                # Set standardized container name: lab-<stack>-<service>
                svc_def["container_name"] = f"lab-{stack_name}-{svc_name}"
                # Add to shared network
                svc_def["networks"] = [f"{stack_name}-net"]

                # If plugin has build context, copy build files to new stack dir
                if svc_def.get("build"):
                    build_ctx = svc_def["build"]
                    if isinstance(build_ctx, str):
                        src_dir = plugin_path / build_ctx
                    elif isinstance(build_ctx, dict):
                        src_dir = plugin_path / build_ctx.get("context", ".")
                    else:
                        src_dir = plugin_path
                    # Copy all build files to a subfolder in the new stack
                    import shutil
                    build_dest = stack_dir / svc_name
                    if src_dir.exists():
                        shutil.copytree(str(src_dir), str(build_dest), dirs_exist_ok=True)
                    # Update build context to point to the subfolder
                    if isinstance(build_ctx, dict):
                        svc_def["build"]["context"] = f"./{svc_name}"
                    else:
                        svc_def["build"] = f"./{svc_name}"

                # Smart port remapping
                new_ports = []
                for p in svc_def.get("ports", []):
                    p_str = str(p)
                    parts = p_str.split(":")
                    if len(parts) == 2:
                        host_port = int(parts[0].strip('"'))
                        container_port = parts[1].strip('"')
                    elif len(parts) == 3:
                        # e.g. "127.0.0.1:8123:8123"
                        host_port = int(parts[1])
                        container_port = parts[2].strip('"')
                    else:
                        new_ports.append(p)
                        continue

                    assigned = self._find_free_port(host_port, used_ports)
                    used_ports.add(assigned)
                    port_map[f"{svc_name}:{container_port}"] = assigned

                    if len(parts) == 3:
                        new_ports.append(f"{parts[0]}:{assigned}:{container_port}")
                    else:
                        new_ports.append(f"{assigned}:{container_port}")
                    assigned_ports.append(f"{svc_name}:{container_port} -> 127.0.0.1:{assigned}")

                svc_def["ports"] = new_ports
                services[svc_name] = svc_def

            # Collect volumes
            for vol_name in plugin_compose.get("volumes", {}):
                volumes[vol_name] = {}

        # Build docker-compose.yaml
        compose = {
            "services": services,
            "networks": {f"{stack_name}-net": {"driver": "bridge"}},
        }
        if volumes:
            compose["volumes"] = volumes

        # Build stack.yaml with correct access URLs
        components = []
        access_list = []
        cred_list = []
        cmd_list = []
        for plugin_name in plugin_names:
            pm = self.plugins[plugin_name]
            components.append({
                "name": plugin_name,
                "type": "database" if plugin_name in ("postgres", "clickhouse") else "service",
                "image": pm.get("image", ""),
                "description": pm.get("description", ""),
            })
            creds = pm.get("credentials", {})
            if creds:
                cred_list.append({
                    "service": plugin_name,
                    "username": creds.get("username", ""),
                    "password": creds.get("password", ""),
                })
            # Add port from port_map
            for key, assigned in port_map.items():
                svc, cport = key.split(":")
                if svc == plugin_name and cred_list:
                    cred_list[-1]["port"] = str(assigned)
                    break
            # Remap ports in sample commands
            for sc in pm.get("sample_commands", []):
                cmd = sc.get("command", "")
                for key, assigned in port_map.items():
                    svc, cport = key.split(":")
                    if svc == plugin_name:
                        # Replace default port references in commands
                        cmd = cmd.replace(f"-p {cport}", f"-p {assigned}")
                        cmd = cmd.replace(f"--port {cport}", f"--port {assigned}")
                        cmd = cmd.replace(f":{cport}", f":{assigned}")
                cmd_list.append({"name": sc.get("name", ""), "command": cmd})

        stack_meta = {
            "name": stack_name,
            "version": "1.0",
            "description": description or f"Custom stack with {', '.join(plugin_names)}",
            "built_from_plugins": plugin_names,
            "components": components,
            "credentials": cred_list,
            "sample_commands": cmd_list,
        }

        # Write files

        with open(stack_dir / "docker-compose.yaml", "w") as f:
            yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

        with open(stack_dir / "stack.yaml", "w") as f:
            yaml.dump(stack_meta, f, default_flow_style=False, sort_keys=False)

        # Reload stacks
        self.stacks = self._load_stacks()

        # Build summary - keep it short, details come after deploy
        return (
            f"Integration **{stack_name}** built successfully!\n\n"
            f"Services: {', '.join(services.keys())} ({len(services)} containers)\n"
            f"Network: {stack_name}-net (shared)\n\n"
            f"**Next: Deploy it from the Integrations tab or type: Deploy {stack_name}**"
        )

    def _clean_content(self, content):
        """Strip extra fields from SDK objects for Bedrock compatibility."""
        cleaned = []
        for block in content:
            if block.type == "text":
                cleaned.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                cleaned.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
        return cleaned

    def _validate_history(self):
        """Ensure history has no orphaned tool_use/tool_result blocks before API call."""
        if not self.history:
            return

        clean = []
        i = 0
        while i < len(self.history):
            msg = self.history[i]

            # Check if this is an assistant message with tool_use
            if msg.get("role") == "assistant" and isinstance(msg.get("content"), list):
                tool_use_ids = set()
                for b in msg["content"]:
                    if isinstance(b, dict) and b.get("type") == "tool_use":
                        tool_use_ids.add(b.get("id"))

                if tool_use_ids:
                    # Must have a matching tool_result in the next message
                    if i + 1 < len(self.history):
                        next_msg = self.history[i + 1]
                        if next_msg.get("role") == "user" and isinstance(next_msg.get("content"), list):
                            result_ids = set()
                            for b in next_msg["content"]:
                                if isinstance(b, dict) and b.get("type") == "tool_result":
                                    result_ids.add(b.get("tool_use_id"))
                            # Check all tool_use ids have matching results
                            if tool_use_ids <= result_ids:
                                clean.append(msg)
                                clean.append(next_msg)
                                i += 2
                                continue
                    # Orphaned tool_use - skip both this and any following tool_result
                    logger.warning("[HISTORY] Removing orphaned tool_use message")
                    i += 1
                    # Skip following tool_result if present
                    if i < len(self.history):
                        next_msg = self.history[i]
                        if next_msg.get("role") == "user" and isinstance(next_msg.get("content"), list):
                            has_result = any(isinstance(b, dict) and b.get("type") == "tool_result" for b in next_msg["content"])
                            if has_result:
                                i += 1
                    continue

            # Check if this is an orphaned tool_result (user message with only tool_result)
            if msg.get("role") == "user" and isinstance(msg.get("content"), list):
                has_result = any(isinstance(b, dict) and b.get("type") == "tool_result" for b in msg["content"])
                if has_result:
                    # Check if previous message in clean is a matching tool_use
                    if clean and clean[-1].get("role") == "assistant":
                        # Already handled above, but double-check
                        pass
                    else:
                        # Orphaned tool_result - skip
                        logger.warning("[HISTORY] Removing orphaned tool_result message")
                        i += 1
                        continue

            clean.append(msg)
            i += 1

        # Ensure history starts with user message
        while clean and clean[0].get("role") != "user":
            clean = clean[1:]

        # Ensure history doesn't end with tool_use without result
        while clean and clean[-1].get("role") == "assistant":
            last = clean[-1]
            if isinstance(last.get("content"), list):
                has_tool_use = any(isinstance(b, dict) and b.get("type") == "tool_use" for b in last["content"])
                if has_tool_use:
                    clean = clean[:-1]
                    continue
            break

        self.history = clean

    def chat_stream(self, message):
        """Streaming chat - yields text chunks as they arrive."""
        if not self.client:
            yield self._offline_chat(message)
            return

        self.history.append({"role": "user", "content": message})
        try:
            while True:
                # Validate history before each API call to prevent orphaned tool_use/tool_result errors
                self._validate_history()

                with self.client.messages.stream(
                    model=self.model, max_tokens=2048,
                    system=self._get_system_prompt(),
                    tools=self._get_tools(), messages=self.history,
                ) as stream:
                    for text in stream.text_stream:
                        yield text

                    final_message = stream.get_final_message()
                    stop_reason = final_message.stop_reason

                if stop_reason == "tool_use":
                    self.history.append({"role": "assistant", "content": self._clean_content(final_message.content)})
                    tool_results = []
                    for block in final_message.content:
                        if block.type == "tool_use":
                            output = ""
                            if block.name == "run_command":
                                cmd = block.input.get("command", "")
                                print(f"[agent] Running: {cmd}")
                                yield f"\n`Running: {cmd}`\n"
                                output = self.run_command(cmd)
                                # Stream output to chat (truncated for readability)
                                if output and output != "(no output)":
                                    display = output[:1500]
                                    if len(output) > 1500:
                                        display += f"\n... ({len(output)} chars total)"
                                    yield f"\n```\n{display}\n```\n"
                            elif block.name == "build_stack":
                                sn = block.input.get("stack_name", "")
                                plugins = block.input.get("plugins", [])
                                desc = block.input.get("description", "")
                                print(f"[agent] Building stack: {sn} with {plugins}")
                                yield f"\n`Building stack: {sn} from plugins: {', '.join(plugins)}`\n"
                                output = self.build_stack(sn, plugins, desc)
                                yield f"\n```\n{output}\n```\n"
                            else:
                                output = f"Unknown tool: {block.name}"
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": output[:8000],
                            })
                    self.history.append({"role": "user", "content": tool_results})
                else:
                    self.history.append({"role": "assistant", "content": self._clean_content(final_message.content)})
                    break

            # Trim history if too long (validation will clean up orphaned pairs)
            if len(self.history) > 40:
                self.history = self.history[-30:]
                self._validate_history()

        except Exception as e:
            error_msg = f"API error: {str(e)}"
            logger.error("[CHAT] %s", error_msg)
            print(f"[agent] {error_msg}")
            yield error_msg

    def chat(self, message):
        """Non-streaming fallback."""
        return "".join(self.chat_stream(message))

    def _get_container_status(self, compose_path):
        """Check container status after deploy and return a formatted summary."""
        import time
        time.sleep(3)  # Wait for containers to settle
        try:
            result = self.run_command(f"cd {compose_path} && docker compose ps --format json 2>/dev/null")
            if not result or result == "(no output)":
                return ""
            import json as _json
            lines = ["**Container status:**"]
            running = 0
            failed = 0
            for line in result.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    c = _json.loads(line)
                    name = c.get("Name", c.get("Service", "?"))
                    state = c.get("State", "unknown")
                    status = c.get("Status", "")
                    if "running" in state.lower() or "up" in status.lower():
                        lines.append(f"  {name}: running")
                        running += 1
                    elif "exited" in state.lower() and "0" in status:
                        lines.append(f"  {name}: completed (one-shot)")
                    else:
                        lines.append(f"  {name}: **FAILED** ({state})")
                        failed += 1
                except:
                    pass
            if failed > 0:
                lines.append(f"\n**{failed} container(s) failed to start.** Check logs: `docker logs <container_name>`")
            return "\n".join(lines)
        except:
            return ""

    def _get_deploy_summary(self, name):
        """Build a post-deploy summary from stack.yaml or plugin.yaml metadata."""
        meta = self.stacks.get(name, self.plugins.get(name, {}))
        if not meta:
            return ""
        lines = []
        desc = meta.get("description", "")
        if desc:
            lines.append(f"**{name}** — {desc}")

        # Components
        comps = meta.get("components", [])
        if comps:
            lines.append("\n**Components:** " + ", ".join(c.get("name", "") for c in comps))

        # Access URLs
        access = meta.get("access", [])
        if access:
            lines.append("\n**Access:**")
            for a in access:
                lines.append(f"  {a.get('name','')}: {a.get('url','')}")

        # Credentials
        creds = meta.get("credentials", [])
        if creds:
            lines.append("\n**Credentials:**")
            for c in creds:
                port_info = f", port {c['port']}" if 'port' in c else ""
                port_info = f", ports {c['ports']}" if 'ports' in c else port_info
                lines.append(f"  {c.get('service','')}: user=`{c.get('username','')}`, pass=`{c.get('password','')}`{port_info}")

        # Sample commands
        cmds = meta.get("sample_commands", [])
        if cmds:
            lines.append("\n**Sample commands:**")
            for sc in cmds:
                lines.append(f"  `{sc.get('command','')}`")

        # After deploy
        after = meta.get("after_deploy", "")
        if after:
            lines.append(f"\n**Next steps:**\n{after.strip()}")

        return "\n".join(lines)

    def _offline_chat(self, message):
        msg = message.lower().strip()
        if "status" in msg or "running" in msg:
            return "**Running containers:**\n```\n" + self.run_command("docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'") + "\n```"
        if "deploy" in msg or "start" in msg:
            for name in list(self.stacks.keys()) + list(self.plugins.keys()):
                if name.replace("-", "") in msg.replace("-", ""):
                    path = self.stacks.get(name, self.plugins.get(name, {})).get("_path", "")
                    if path:
                        # Source root .env into shell so docker compose secrets + build args can access it
                        env_file = self.project_root / ".env"
                        env_source = f"set -a; . {env_file}; set +a; " if env_file.exists() else ""
                        env_flag = f"--env-file {env_file}" if env_file.exists() else ""
                        # Check if images need building (custom Dockerfiles)
                        build_flag = "--build"
                        try:
                            compose_check = self.run_command(f"{env_source}cd {path} && docker compose {env_flag} images -q 2>/dev/null")
                            if compose_check and compose_check.strip() and "ERROR" not in compose_check:
                                build_flag = ""
                        except:
                            pass
                        output = self.run_command(f"{env_source}cd {path} && docker compose {env_flag} up -d {build_flag}")
                        # Check container status after deploy
                        status_check = self._get_container_status(path)
                        # Count running containers
                        running_count = status_check.count(": running")
                        failed_count = status_check.count("FAILED")
                        # Show truncated deploy logs + summary
                        log_lines = output.strip().split('\n') if output else []
                        log_preview = '\n'.join(log_lines[-15:]) if len(log_lines) > 15 else output
                        result = f"Deploying **{name}**...\n```\n{log_preview}\n```"
                        if failed_count > 0:
                            result += f"\n\n**{running_count} containers running, {failed_count} failed.** Check logs: `docker logs <container>`"
                        else:
                            result += f"\n\n**{running_count} containers running.** Switching to Workspace tab."
                            # Pre-warm toolbox container in background so terminal is instant
                            import threading
                            threading.Thread(target=self._ensure_toolbox, args=(name,), daemon=True).start()
                        return result
            return "Available: " + ", ".join(list(self.stacks.keys()) + list(self.plugins.keys()))
        if "destroy" in msg or "stop" in msg or "down" in msg:
            if "all" in msg:
                results = []
                for name in list(self.stacks.keys()) + list(self.plugins.keys()):
                    path = self.stacks.get(name, self.plugins.get(name, {})).get("_path", "")
                    if path:
                        self.stop_toolbox(name)
                        # Enumerate profiles so profiled services (like bfsi-app under `oltp`) are included
                        output = self.run_command(
                            f"cd {path} && "
                            f"PROFS=$(docker compose config --profiles 2>/dev/null | awk '{{printf \" --profile %s\", $0}}'); "
                            f"eval \"docker compose $PROFS kill\" 2>/dev/null; "
                            f"eval \"docker compose $PROFS down -v --remove-orphans -t 1\" 2>&1"
                        )
                        results.append(f"{name}: stopped")
                if results:
                    return "**Stopped all integrations:**\n" + "\n".join(results)
                return "No running integrations found."
            for name in list(self.stacks.keys()) + list(self.plugins.keys()):
                if name.replace("-", "") in msg.replace("-", ""):
                    path = self.stacks.get(name, self.plugins.get(name, {})).get("_path", "")
                    if path:
                        # Tear down the toolbox first — it holds an attachment to
                        # the project network, which would otherwise block the
                        # network removal in `down`.
                        self.stop_toolbox(name)
                        # Profile-aware compose down. Without profiles, services
                        # declared with `profiles: [oltp/olap/ml/...]` are skipped.
                        output = self.run_command(
                            f"cd {path} && "
                            f"PROFS=$(docker compose config --profiles 2>/dev/null | awk '{{printf \" --profile %s\", $0}}'); "
                            f"eval \"docker compose $PROFS kill\" 2>/dev/null; "
                            f"eval \"docker compose $PROFS down -v --remove-orphans -t 1\""
                        )
                        # Defensive orphan sweep — force-remove anything left over
                        # by container-name prefix (handles cases where compose
                        # lost track of a container, or a third party started one
                        # that joined the project network). Mirrors the prefixes
                        # in /api/exit and Makefile.
                        prefix_map = {
                            "bfsi-fraud-detection": ("bfsi-", "bfsi-network"),
                            "core-banking-simulator": ("cb-", "cb-network"),
                            "analytics-comparison": ("bfd-", "bfd-network"),
                            "unified-analytics-intelligence": ("uai-", "uai-network"),
                            "real-time-analytics": ("rta-", "rta-net"),
                        }
                        prefix_info = prefix_map.get(name)
                        if prefix_info:
                            prefix, network = prefix_info
                            self.run_command(
                                f"IDS=$(docker ps -aq --filter \"name={prefix}\"); "
                                f"if [ -n \"$IDS\" ]; then docker rm -f $IDS 2>/dev/null; fi; "
                                f"docker network rm {network} 2>/dev/null; true"
                            )
                        # Always sweep our toolbox container by exact name (the
                        # earlier stop_toolbox call should have done this, but
                        # belt-and-braces — it's cheap and idempotent).
                        self.run_command(f"docker rm -f diab-toolbox-{name} 2>/dev/null; true")
                        return f"Destroying **{name}**...\n```\n{output}\n```"
        if "log" in msg:
            for name in ["postgres", "clickhouse", "minio", "peerdb-ui", "catalog", "temporal"]:
                if name in msg:
                    output = self.run_command(f"docker logs {name} --tail 30 2>&1")
                    return f"**Logs for {name}:**\n```\n{output}\n```"
        return "Offline mode. Commands: deploy, destroy, status, logs. Set ANTHROPIC_API_KEY for AI chat."

    # ── Toolbox Container Methods ──────────────────────────────────────

    TOOLBOX_IMAGE = "diab-toolbox:latest"

    def _get_stack_network(self, stack_name):
        """Detect the Docker network for a stack by parsing its compose file.

        Network patterns across stacks:
        - Named with explicit `name:` key  → use that name directly (cb-network, bfd-network, uai-network)
        - Default network with `name:` key → use that name directly (peerdb_network)
        - Named without `name:` key        → {project}_{network_key} (e.g. real-time-analytics_rta-net)
        - No networks section              → {dir-name}_default
        """
        stack_dir = self._get_stack_dir(stack_name)
        compose_file = stack_dir / "docker-compose.yaml"
        if not compose_file.exists():
            return f"{stack_name}_default"

        with open(compose_file) as f:
            compose = yaml.safe_load(f) or {}

        networks = compose.get("networks", {})
        if not networks:
            return f"{stack_name}_default"

        # Take the first network defined
        net_key = next(iter(networks))
        net_config = networks[net_key] or {}

        # If there's an explicit `name:` key, use it directly
        if isinstance(net_config, dict) and "name" in net_config:
            return net_config["name"]

        # No explicit name — Docker Compose prefixes with project name
        # The project name is the directory name by default
        return f"{stack_name}_{net_key}"

    def _get_port_mapping(self, stack_name):
        """Parse compose ports to build host→container port map per service.

        Returns: {"pgd": {"7434": "5432"}, "clickhouse": {"8125": "8123"}, ...}
        """
        stack_dir = self._get_stack_dir(stack_name)
        compose_file = stack_dir / "docker-compose.yaml"
        if not compose_file.exists():
            return {}

        with open(compose_file) as f:
            compose = yaml.safe_load(f) or {}

        mapping = {}
        for svc_name, svc_config in compose.get("services", {}).items():
            ports = svc_config.get("ports", [])
            if ports:
                svc_ports = {}
                for p in ports:
                    p_str = str(p).strip('"')
                    parts = p_str.split(":")
                    if len(parts) == 2:
                        svc_ports[parts[0]] = parts[1]
                    elif len(parts) == 3:
                        # "host_ip:host_port:container_port"
                        svc_ports[parts[1]] = parts[2]
                if svc_ports:
                    mapping[svc_name] = svc_ports
        return mapping

    def _get_container_to_service_map(self, stack_name):
        """Map container_name → service_name from compose file.

        Returns: {"cb-pgd": "pgd", "cb-clickhouse": "clickhouse", ...}
        """
        stack_dir = self._get_stack_dir(stack_name)
        compose_file = stack_dir / "docker-compose.yaml"
        if not compose_file.exists():
            return {}

        with open(compose_file) as f:
            compose = yaml.safe_load(f) or {}

        result = {}
        for svc_name, svc_config in compose.get("services", {}).items():
            cname = svc_config.get("container_name", "")
            if cname:
                result[cname] = svc_name
        return result

    def _rewrite_command_for_toolbox(self, stack_name, cmd):
        """Transform a host-mapped CLI command to run inside the toolbox container.

        Handles:
        - psql -h 127.0.0.1 -p 7434 → psql -h pgd -p 5432 (+ PGPASSWORD)
        - docker exec cb-clickhouse clickhouse-client ... → clickhouse-client --host clickhouse ...
        - docker exec cb-pgd psql -U postgres -d demo → psql -h pgd -U postgres -d demo (+ PGPASSWORD)
        """
        port_map = self._get_port_mapping(stack_name)
        container_map = self._get_container_to_service_map(stack_name)
        meta = self.stacks.get(stack_name, {})
        credentials = meta.get("credentials", [])

        def _find_cred(service_name):
            for c in credentials:
                if c.get("service", "") == service_name:
                    return c
            return {}

        # Reverse port map: {"7434": ("pgd", "5432")}
        reverse_ports = {}
        for svc, ports in port_map.items():
            for host_port, container_port in ports.items():
                reverse_ports[host_port] = (svc, container_port)

        # Case 1: docker exec <container> <command...>
        if cmd.strip().startswith("docker exec"):
            parts = cmd.strip().split()
            # Skip "docker exec" and flags like -it
            idx = 2
            while idx < len(parts) and parts[idx].startswith("-"):
                idx += 1
            if idx >= len(parts):
                return cmd
            container_name = parts[idx]
            inner_cmd_parts = parts[idx + 1:]
            inner_cmd = " ".join(inner_cmd_parts)
            svc_name = container_map.get(container_name, container_name)
            cred = _find_cred(svc_name)

            if inner_cmd_parts and inner_cmd_parts[0] in ("psql", "pg_isready"):
                # psql command — add host and password
                pwd = cred.get("password", "")
                prefix = f"PGPASSWORD={pwd} " if pwd else ""
                # Add -h if not present
                if "-h" not in inner_cmd and "--host" not in inner_cmd:
                    inner_cmd = inner_cmd.replace("psql", f"psql -h {svc_name}", 1)
                return f"{prefix}{inner_cmd}"

            elif inner_cmd_parts and inner_cmd_parts[0] == "clickhouse-client":
                # clickhouse-client — add --host if not present
                if "--host" not in inner_cmd:
                    inner_cmd = inner_cmd.replace("clickhouse-client", f"clickhouse-client --host {svc_name}", 1)
                return inner_cmd

            else:
                # Generic docker exec — just strip the exec prefix
                return inner_cmd

        # Case 2: psql -h 127.0.0.1 -p <host_port> ...
        if "psql" in cmd and ("127.0.0.1" in cmd or "localhost" in cmd):
            import re
            port_match = re.search(r'-p\s+(\d+)', cmd)
            if port_match:
                host_port = port_match.group(1)
                if host_port in reverse_ports:
                    svc_name, container_port = reverse_ports[host_port]
                    cred = _find_cred(svc_name)
                    pwd = cred.get("password", "")
                    prefix = f"PGPASSWORD={pwd} " if pwd else ""
                    rewritten = cmd
                    rewritten = re.sub(r'127\.0\.0\.1|localhost', svc_name, rewritten)
                    rewritten = re.sub(r'-p\s+\d+', f'-p {container_port}', rewritten)
                    return f"{prefix}{rewritten}"

        # Case 3: clickhouse-client with host port
        if "clickhouse-client" in cmd and ("127.0.0.1" in cmd or "localhost" in cmd):
            import re
            for svc, ports in port_map.items():
                for hp, cp in ports.items():
                    if hp in cmd:
                        cmd = cmd.replace(f"127.0.0.1:{hp}", f"{svc}:{cp}")
                        cmd = cmd.replace(f"localhost:{hp}", f"{svc}:{cp}")
            return cmd

        return cmd

    _toolbox_image_ok = False

    def _ensure_toolbox_image(self):
        """Build toolbox image locally if it doesn't exist."""
        if self._toolbox_image_ok:
            return True, "ok"
        result = subprocess.run(
            f"docker image inspect {self.TOOLBOX_IMAGE} > /dev/null 2>&1",
            shell=True, capture_output=True, timeout=10
        )
        if result.returncode != 0:
            dockerfile_dir = self.project_root / "engine" / "toolbox"
            if not (dockerfile_dir / "Dockerfile").exists():
                return False, "Toolbox Dockerfile not found"
            logger.info("[TOOLBOX] Building toolbox image (first time)...")
            build_result = subprocess.run(
                f"docker build -t {self.TOOLBOX_IMAGE} {dockerfile_dir}",
                shell=True, capture_output=True, text=True, timeout=300
            )
            if build_result.returncode != 0:
                logger.error("[TOOLBOX] Build failed: %s", build_result.stderr[-500:])
                return False, f"Toolbox build failed: {build_result.stderr[-200:]}"
            logger.info("[TOOLBOX] Image built successfully")
        self._toolbox_image_ok = True
        return True, "ok"

    _toolbox_running = set()

    def _ensure_toolbox(self, stack_name):
        """Ensure the toolbox container is running for a stack. Returns container name or error."""
        container_name = f"diab-toolbox-{stack_name}"

        # Fast path: already confirmed running — no Docker calls at all
        if container_name in self._toolbox_running:
            return container_name, None

        # Check if already running
        result = subprocess.run(
            f'docker inspect --format="{{{{.State.Running}}}}" {container_name} 2>/dev/null',
            shell=True, capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and "true" in result.stdout.lower():
            self._toolbox_running.add(container_name)
            return container_name, None

        # Build image if needed
        ok, msg = self._ensure_toolbox_image()
        if not ok:
            return None, msg

        # Get the network for this stack
        network = self._get_stack_network(stack_name)

        # Remove stale container if exists
        subprocess.run(f"docker rm -f {container_name} 2>/dev/null", shell=True, capture_output=True, timeout=5)

        # Start toolbox container on the stack's network
        run_cmd = (
            f"docker run -d --name {container_name} "
            f"--network {network} "
            f"--restart unless-stopped "
            f"{self.TOOLBOX_IMAGE}"
        )
        run_result = subprocess.run(run_cmd, shell=True, capture_output=True, text=True, timeout=15)
        if run_result.returncode != 0:
            err = run_result.stderr.strip()
            logger.error("[TOOLBOX] Start failed for %s: %s", stack_name, err)
            return None, f"Toolbox start failed: {err[-200:]}"

        self._toolbox_running.add(container_name)
        logger.info("[TOOLBOX] Started %s on network %s", container_name, network)
        return container_name, None

    def stop_toolbox(self, stack_name):
        """Stop and remove the toolbox container for a stack."""
        container_name = f"diab-toolbox-{stack_name}"
        self._toolbox_running.discard(container_name)
        subprocess.run(f"docker rm -f {container_name} 2>/dev/null", shell=True, capture_output=True, timeout=5)
        logger.info("[TOOLBOX] Stopped %s", container_name)

    def get_terminal_commands(self, stack_name):
        """Return toolbox-ready commands for a running stack.

        Returns list of dicts: {name, original, toolbox_cmd, service, type}
        """
        meta = self.stacks.get(stack_name, {})
        if not meta:
            return []

        commands = []
        # Dedup by (service, type) so multiple sample_commands targeting the same
        # backend (e.g. interactive `psql` plus example query `psql ... -c "..."`)
        # collapse into a single terminal entry. First occurrence wins —
        # stack.yaml should put the interactive command first.
        seen_keys = set()
        sample_cmds = meta.get("sample_commands", [])
        credentials = meta.get("credentials", [])

        def _find_cred(service_name):
            for c in credentials:
                if c.get("service", "") == service_name:
                    return c
            return {}

        container_map = self._get_container_to_service_map(stack_name)
        port_map = self._get_port_mapping(stack_name)

        # Reverse port map for lookups
        reverse_ports = {}
        for svc, ports in port_map.items():
            for hp, cp in ports.items():
                reverse_ports[hp] = (svc, cp)

        for sc in sample_cmds:
            override_name = ""
            override_service = ""
            override_type = ""
            override_target = ""
            if isinstance(sc, dict):
                original = sc.get("command", "")
                override_name = sc.get("name", "")
                override_service = sc.get("service", "")
                override_type = sc.get("type", "")
                override_target = sc.get("target_container", "")
            else:
                original = str(sc)

            if not original:
                continue

            # If target_container is set, bypass the toolbox rewrite — the WS
            # terminal will exec directly into that container.
            if override_target:
                toolbox_cmd = original
            else:
                toolbox_cmd = self._rewrite_command_for_toolbox(stack_name, original)

            # Determine service and type (overrides win when the dict form is used)
            service = override_service
            cmd_type = override_type or "shell"

            if "psql" in original:
                cmd_type = "psql"
                # Find service from port or docker exec target
                import re
                port_match = re.search(r'-p\s+(\d+)', original)
                if port_match and port_match.group(1) in reverse_ports:
                    service = reverse_ports[port_match.group(1)][0]
                elif original.startswith("docker exec"):
                    parts = original.split()
                    idx = 2
                    while idx < len(parts) and parts[idx].startswith("-"):
                        idx += 1
                    if idx < len(parts):
                        service = container_map.get(parts[idx], parts[idx])
            elif "clickhouse-client" in original:
                cmd_type = "clickhouse"
                if original.startswith("docker exec"):
                    parts = original.split()
                    idx = 2
                    while idx < len(parts) and parts[idx].startswith("-"):
                        idx += 1
                    if idx < len(parts):
                        service = container_map.get(parts[idx], parts[idx])
                else:
                    service = "clickhouse"
            elif "redis-cli" in original:
                cmd_type = "redis"
            elif "mysql" in original:
                cmd_type = "mysql"

            cred = _find_cred(service) if service else {}
            detail = ""
            if cred:
                port_str = cred.get("port", "")
                detail = f"{cred.get('username', '')}@{service}:{port_str}"

            key = (service, cmd_type)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            commands.append({
                "name": override_name or f"{service.upper() or cmd_type} — {service or 'shell'}",
                "original": original,
                "toolbox_cmd": toolbox_cmd,
                "service": service,
                "type": cmd_type,
                "detail": detail,
                "target_container": override_target,
            })

        # Also generate connect commands from credentials that have psql-able ports
        seen_services = {c["service"] for c in commands if c.get("service")}
        for cred in credentials:
            svc = cred.get("service", "")
            if svc in seen_services:
                continue
            port = str(cred.get("port", ""))
            username = cred.get("username", "")
            password = cred.get("password", "")

            # Check if this is a postgres-like service with a standard port
            if port and svc in port_map:
                svc_ports = port_map[svc]
                # Find the container port for this host port
                container_port = svc_ports.get(port, "5432")
                if username and any(p in container_port for p in ["5432", "4566"]):
                    toolbox_cmd = f"PGPASSWORD={password} psql -h {svc} -p {container_port} -U {username}"
                    commands.append({
                        "name": f"PostgreSQL ({svc})",
                        "original": f"psql -h 127.0.0.1 -p {port} -U {username}",
                        "toolbox_cmd": toolbox_cmd,
                        "service": svc,
                        "type": "psql",
                        "detail": f"{username}@{svc}:{container_port}",
                    })

        return commands

    # ── End Toolbox Methods ──────────────────────────────────────────

    def reset(self):
        self.history = []
        return "Conversation reset."

    def get_pipelines(self, stack_name):
        return {"pipelines": self.stacks.get(stack_name, {}).get("pipelines", [])}

    # Active subprocess for pipeline steps — can be killed on stop
    _active_process = None

    def run_pipeline_step_stream(self, stack_name, pipeline_id, step_id):
        """Run a pipeline step using Popen, yielding output lines. Stores process handle for kill."""
        # NF-deployed stacks: the laptop bash script (docker compose +
        # docker exec) does not apply. Dispatch to the NF translator which
        # triggers the matching use-case jobs and streams their logs.
        if stack_name in (self.nf.deployments or {}):
            yield from self.nf.run_pipeline_step(stack_name, pipeline_id, step_id)
            return
        meta = self.stacks.get(stack_name, {})
        for pipeline in meta.get("pipelines", []):
            if pipeline["id"] == pipeline_id:
                for step in pipeline["steps"]:
                    if step["id"] == step_id:
                        import time, select, io
                        cmd = step["command"]
                        # Force line-buffered output for docker exec commands
                        if "docker exec " in cmd and " -e PYTHONUNBUFFERED" not in cmd:
                            cmd = cmd.replace("docker exec ", "docker exec -e PYTHONUNBUFFERED=1 ")
                        logger.info("[CMD] Step stream: %s", cmd[:150])
                        t0 = time.time()
                        try:
                            proc = subprocess.Popen(
                                cmd, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, cwd=str(self.project_root),
                                bufsize=1, preexec_fn=os.setsid,
                                env={**os.environ, "PYTHONUNBUFFERED": "1"}
                            )
                            LabAgent._active_process = proc
                            # Patterns that indicate a long-running server is ready
                            ready_patterns = ['ready in', 'listening on', 'server started', 'started on port',
                                              'running on', 'accepting connections', 'ready to accept',
                                              'webpack compiled', 'compiled successfully', 'serving on']
                            import selectors
                            sel = selectors.DefaultSelector()
                            sel.register(proc.stdout, selectors.EVENT_READ)
                            last_heartbeat = time.time()
                            while True:
                                if LabAgent._active_process is None:
                                    try: os.killpg(os.getpgid(proc.pid), 9)
                                    except: pass
                                    try: proc.wait(timeout=3)
                                    except: pass
                                    sel.close()
                                    yield {"type": "stopped", "elapsed_ms": int((time.time() - t0) * 1000)}
                                    return
                                # Wait up to 2s for data (allows heartbeat + stop check)
                                events = sel.select(timeout=2.0)
                                if events:
                                    line = proc.stdout.readline()
                                    if line:
                                        stripped = line.rstrip()
                                        yield {"type": "line", "text": stripped}
                                        last_heartbeat = time.time()
                                        lower = stripped.lower()
                                        if any(p in lower for p in ready_patterns):
                                            LabAgent._active_process = None
                                            sel.close()
                                            yield {"type": "done", "success": True, "elapsed_ms": int((time.time() - t0) * 1000)}
                                            return
                                    elif proc.poll() is not None:
                                        sel.close()
                                        break
                                else:
                                    if proc.poll() is not None:
                                        sel.close()
                                        break
                                    # Send heartbeat every 5s to keep SSE alive
                                    if time.time() - last_heartbeat > 5:
                                        yield {"type": "heartbeat"}
                                        last_heartbeat = time.time()
                            # Drain remaining output
                            remaining = proc.stdout.read()
                            if remaining:
                                for l in remaining.strip().split('\n'):
                                    if l: yield {"type": "line", "text": l.rstrip()}
                            LabAgent._active_process = None
                            elapsed_ms = int((time.time() - t0) * 1000)
                            if proc.returncode != 0:
                                logger.warning("[CMD] Step exit=%d", proc.returncode)
                                yield {"type": "done", "success": False, "elapsed_ms": elapsed_ms}
                            else:
                                yield {"type": "done", "success": True, "elapsed_ms": elapsed_ms}
                        except Exception as e:
                            LabAgent._active_process = None
                            logger.error("[CMD] Step error: %s", str(e))
                            yield {"type": "error", "text": str(e), "elapsed_ms": int((time.time() - t0) * 1000)}
                        return
        yield {"type": "error", "text": f"Step {step_id} not found"}

    @staticmethod
    def stop_active_step():
        """Kill the currently running pipeline step process and all children."""
        proc = LabAgent._active_process
        if proc:
            logger.info("[CMD] Killing active step process PID=%s", proc.pid)
            LabAgent._active_process = None
            try:
                import signal
                # Kill entire process group (shell + children like curl, sleep, make)
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            except Exception:
                # Fallback: kill just the process
                try:
                    proc.kill()
                except:
                    pass
            try:
                proc.wait(timeout=5)
            except:
                pass
            return True
        return False
