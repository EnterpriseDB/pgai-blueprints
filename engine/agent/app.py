"""
EDB Postgres® AI Blueprints Chat UI - FastAPI with SSE streaming.
Tab layout: Stacks | Plugins | Use Cases + Chat panel.
Port 4000 (PeerDB uses 3000).

For demonstration purposes only.
"""

pgai_version = "0.1rc8"

import os
import json
import hmac
import logging
import logging.handlers
import uvicorn
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, RedirectResponse
from agent import LabAgent

# --- Structured Logging ---
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "agent.log")

logger = logging.getLogger("dbox")
logger.setLevel(logging.INFO)
file_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(console_handler)

logger.info("[APP] EDB Postgres® AI Blueprints starting")

app = FastAPI(title="EDB Postgres® AI Blueprints", version=pgai_version)
agent = LabAgent()
_start_time = datetime.now()
_last_activity = datetime.now()

# Paths under /api/* that stay open (no API key required).
_AUTH_EXEMPT_PATHS = {"/api/health"}


@app.middleware("http")
async def _agent_api_key_auth(request: Request, call_next):
    """Require `Authorization: Bearer <AGENT_API_KEY>` on the JSON API.

    Only guards /api/* (except the health check). HTML UI pages, /assets, and
    the WebSocket endpoints are intentionally left open — browser WebSockets
    cannot send an Authorization header, so /ws/* is not gated here (HTTP
    middleware never runs for the WebSocket scope anyway).

    Fail closed: if AGENT_API_KEY is unset/empty the server refuses all
    protected requests with 503 rather than serving them unauthenticated.
    """
    path = request.url.path
    if path.startswith("/api/") and path not in _AUTH_EXEMPT_PATHS:
        expected = os.environ.get("AGENT_API_KEY", "").strip()
        if not expected:
            return JSONResponse(
                {"detail": "AGENT_API_KEY is not configured on the server"},
                status_code=503,
            )
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(token.strip(), expected):
            return JSONResponse(
                {"detail": "Invalid or missing API key"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
    return await call_next(request)


def _api_key_ok(token: str) -> bool:
    """Constant-time check of a presented key against AGENT_API_KEY.

    Fails closed: returns False when AGENT_API_KEY is unset/empty. Used by the
    WebSocket endpoints, which cannot rely on the HTTP middleware (it never runs
    for the WebSocket scope) and cannot receive an Authorization header — the
    browser sends the key inside the first JSON frame instead.
    """
    expected = os.environ.get("AGENT_API_KEY", "").strip()
    if not expected:
        return False
    return hmac.compare_digest((token or "").strip(), expected)


@app.get("/api/health")
async def api_health():
    """Health check with uptime and last activity."""
    global _last_activity
    uptime = (datetime.now() - _start_time).total_seconds()
    idle = (datetime.now() - _last_activity).total_seconds()
    return {"status": "ok", "uptime_seconds": round(uptime), "idle_seconds": round(idle), "log_file": LOG_FILE}


def _expand_deploy_targets(raw):
    """Backward-compat: expand 'laptop' to both 'laptop-docker' + 'laptop-colima'.
    Same Docker daemon underneath — choice is informational. Returns deduped list."""
    if not raw:
        raw = ["laptop"]
    out = []
    for t in raw:
        if t == "laptop":
            for v in ("laptop-docker", "laptop-colima"):
                if v not in out:
                    out.append(v)
        elif t not in out:
            out.append(t)
    return out


@app.get("/api/stacks")
async def api_stacks():
    """Return all stacks and plugins metadata for the UI.

    When a stack is currently deployed to Northflank, access[].url is rewritten
    from the compose-declared `http://127.0.0.1:<external-port>/` to the NF
    public URL of the service exposing that port. Compose binds
    `<external>:<internal>` on the host but NF only knows the internal port,
    so we read compose's `ports:` to build an external→internal map first.
    Without this remap the Workspace tab opens 127.0.0.1 links that have
    nothing listening because the deploy is on NF.
    """
    import re
    from pathlib import Path
    import yaml as _yaml
    global _last_activity
    _last_activity = datetime.now()
    result = {"stacks": {}, "plugins": {}}
    for name, meta in sorted(agent.stacks.items(), key=lambda x: x[1].get("name", x[0])):
        d = {k: v for k, v in meta.items() if not k.startswith("_")}
        # Normalize deploy_targets for the UI: "laptop" expands to docker+colima.
        d["deploy_targets"] = _expand_deploy_targets(meta.get("deploy_targets"))

        # If deployed to NF, remap access URLs to the NF public URLs.
        nf_dep = (agent.nf_deployments or {}).get(name)
        if nf_dep:
            # Build the external→internal port map from this stack's compose.
            # Compose port syntax: "external:internal" or "external:internal/proto"
            # or "ip:external:internal". When external == internal (e.g.
            # "3001:3001") the map is identity.
            ext_to_int = {}
            try:
                compose_path = Path(meta.get("_path", "")) / "docker-compose.yaml"
                if compose_path.exists():
                    with open(compose_path) as f:
                        compose = _yaml.safe_load(f) or {}
                    for _svc_def in (compose.get("services") or {}).values():
                        for p in (_svc_def.get("ports") or []):
                            ps = str(p)
                            parts = ps.split(":")
                            try:
                                if len(parts) == 2:
                                    ext = int(parts[0].strip('"').split("/")[0])
                                    iport = int(parts[1].strip('"').split("/")[0])
                                elif len(parts) == 3:
                                    ext = int(parts[1].strip('"').split("/")[0])
                                    iport = int(parts[2].strip('"').split("/")[0])
                                else:
                                    continue
                            except ValueError:
                                continue
                            ext_to_int[ext] = iport
            except Exception:
                pass

            # Internal port → NF public URL, from sync_deployments.
            int_to_nf = {}
            for svc in nf_dep.get("services", []):
                for p, url in (svc.get("urls") or {}).items():
                    try:
                        int_to_nf[int(p)] = url
                    except (TypeError, ValueError):
                        continue

            if int_to_nf:
                from urllib.parse import urlparse, urlunparse
                remapped = []
                for a in (d.get("access") or []):
                    a2 = dict(a)
                    old_url = a2.get("url", "")
                    # Match :<port> followed by /, #, ?, or end-of-string.
                    m = re.search(r":(\d{2,5})(?=[/#?]|$)", old_url)
                    if m:
                        ext_port = int(m.group(1))
                        # If compose maps external→internal, follow it; else
                        # try the external port directly (covers identity
                        # mappings like 3001:3001 where compose may have a
                        # bare port too).
                        target_port = ext_to_int.get(ext_port, ext_port)
                        if target_port in int_to_nf:
                            # Preserve the original URL's path + query + hash
                            # when swapping to the NF host. Without this we
                            # lose `?token=databox` (Jupyter), `#phase2`
                            # (Bank App), and `/play` (ClickHouse), and
                            # iframe loads land on the wrong page.
                            nf_base = int_to_nf[target_port]
                            try:
                                old = urlparse(old_url)
                                new = urlparse(nf_base)
                                # Use NF scheme+host, original path+query+fragment.
                                # If the original path is empty/`/`, use NF's
                                # path as-is so we don't introduce a trailing /
                                # the user didn't want.
                                path = old.path if (old.path and old.path != "/") else new.path or "/"
                                a2["url"] = urlunparse(
                                    (new.scheme, new.netloc, path, "", old.query, old.fragment)
                                )
                            except Exception:
                                a2["url"] = nf_base
                            a2["_origin"] = "northflank"
                    remapped.append(a2)
                d["access"] = remapped
                d["_deploy_target"] = "northflank"
        result["stacks"][name] = d
    for name, meta in agent.plugins.items():
        result["plugins"][name] = {
            k: v for k, v in meta.items() if not k.startswith("_")
        }
    return result


@app.get("/api/runtime")
async def api_runtime():
    """Return detected Docker runtime + agent's NF readiness."""
    import os
    return {
        "docker_runtime": getattr(agent, "docker_runtime", "unknown"),
        "host_os": getattr(agent, "host_os", "other"),
        "colima_supported": getattr(agent, "colima_supported", True),
        "nf_configured": bool(os.environ.get("NORTHFLANK_API_KEY", "").strip()
                              and os.environ.get("NORTHFLANK_PROJECT", "").strip()),
    }


@app.get("/api/preflight/{target}")
async def api_preflight(target: str, stack: str = ""):
    """Pre-flight check for a deploy target. Returns structured report
    with checks, semi-masked config display, and guidance for missing vars."""
    return agent.preflight(target, stack_name=(stack or None))


@app.post("/api/aws/setup-bedrock")
async def api_aws_setup_bedrock(payload: dict = None):
    """Run aws sso login then export Bedrock credentials and persist to .env.

    Body: { "profile": "Bedrock" }  (defaults to "Bedrock")
    Returns: { ok: bool, message: str, expires_at?: str }

    Side effects on success:
      - .env is updated with AWS_BEDROCK_* keys
      - os.environ is updated in the running process (no agent restart needed)
    """
    import os, subprocess, re
    from pathlib import Path

    profile = ((payload or {}).get("profile") or "Bedrock").strip()
    if not re.match(r"^[A-Za-z0-9_-]+$", profile):
        return {"ok": False, "message": f"Invalid profile name: {profile}"}

    # Step 1: aws sso login (opens browser; waits for completion)
    try:
        r = subprocess.run(
            ["aws", "sso", "login", "--profile", profile],
            capture_output=True, text=True, timeout=180
        )
    except FileNotFoundError:
        return {"ok": False, "message": "aws CLI not found in PATH"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "message": "aws sso login timed out after 3 minutes (did you complete the browser flow?)"}
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()[:500]
        return {"ok": False, "message": f"aws sso login failed: {err}"}

    # Step 2: export credentials as env format
    try:
        r = subprocess.run(
            ["aws", "--profile", profile, "configure", "export-credentials", "--format", "env"],
            capture_output=True, text=True, timeout=30
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "message": "export-credentials timed out"}
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()[:500]
        return {"ok": False, "message": f"export-credentials failed: {err}"}

    creds = {}
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:]
        if "=" in line:
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            creds[k.strip()] = v
    if not creds.get("AWS_ACCESS_KEY_ID") or not creds.get("AWS_SECRET_ACCESS_KEY"):
        return {"ok": False, "message": "Could not parse credentials from export-credentials output"}

    # Map to Bedrock-specific env var names; preserve region/expiration if present
    bedrock_map = {
        "AWS_BEDROCK_ACCESS_KEY_ID": creds.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_BEDROCK_SECRET_ACCESS_KEY": creds.get("AWS_SECRET_ACCESS_KEY", ""),
    }
    if creds.get("AWS_SESSION_TOKEN"):
        bedrock_map["AWS_BEDROCK_SESSION_TOKEN"] = creds["AWS_SESSION_TOKEN"]
    # Region: prefer existing env, else the export, else us-east-1
    existing_region = os.environ.get("AWS_BEDROCK_REGION", "").strip()
    bedrock_map["AWS_BEDROCK_REGION"] = (
        existing_region
        or creds.get("AWS_DEFAULT_REGION", "")
        or creds.get("AWS_REGION", "")
        or "us-east-1"
    )

    # Step 3: update .env (preserving existing lines, replacing same keys)
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    try:
        if env_path.exists():
            lines = env_path.read_text().splitlines()
        else:
            lines = []
        new_lines = []
        seen = set()
        for line in lines:
            stripped = line.strip()
            if "=" in stripped and not stripped.startswith("#"):
                k = stripped.split("=", 1)[0].strip()
                if k in bedrock_map:
                    new_lines.append(f"{k}={bedrock_map[k]}")
                    seen.add(k)
                    continue
            # Convert commented placeholder lines into the actual key
            for k in list(bedrock_map.keys()):
                if k in seen:
                    continue
                if stripped.startswith(f"# {k}=") or stripped.startswith(f"#{k}="):
                    new_lines.append(f"{k}={bedrock_map[k]}")
                    seen.add(k)
                    break
            else:
                new_lines.append(line)
                continue
        # Append any missing
        appended = [k for k in bedrock_map if k not in seen]
        if appended:
            if new_lines and new_lines[-1].strip():
                new_lines.append("")
            new_lines.append("# Bedrock credentials (auto-populated by /api/aws/setup-bedrock)")
            for k in ["AWS_BEDROCK_ACCESS_KEY_ID", "AWS_BEDROCK_SECRET_ACCESS_KEY",
                      "AWS_BEDROCK_SESSION_TOKEN", "AWS_BEDROCK_REGION"]:
                if k in bedrock_map:
                    new_lines.append(f"{k}={bedrock_map[k]}")
        env_path.write_text("\n".join(new_lines) + "\n")
    except Exception as e:
        return {"ok": False, "message": f"Failed to write .env: {e}"}

    # Step 4: update in-process env so current agent session sees the new values
    for k, v in bedrock_map.items():
        os.environ[k] = v

    return {
        "ok": True,
        "message": f"Bedrock credentials populated from profile '{profile}'. "
                   f"Session valid for ~8-12 hrs (re-run if expired).",
        "keys_set": list(bedrock_map.keys()),
    }


@app.get("/assets/{filename:path}")
async def api_assets(filename: str):
    """Serve static assets (SVG diagrams, etc.) shipped under engine/agent/assets/."""
    from fastapi.responses import FileResponse, Response
    from pathlib import Path
    base = Path(__file__).resolve().parent / "assets"
    target = (base / filename).resolve()
    # Path traversal guard
    try:
        target.relative_to(base)
    except ValueError:
        return Response(status_code=403)
    if not target.exists() or not target.is_file():
        return Response(status_code=404)
    media = "image/svg+xml" if target.suffix == ".svg" else None
    # No-cache so edits to architecture SVGs etc. show up on a normal refresh
    # without forcing the user to hard-refresh.
    return FileResponse(str(target), media_type=media,
                        headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.post("/api/chat/stream")
async def api_chat_stream(request: Request):
    """SSE streaming endpoint."""
    global _last_activity
    _last_activity = datetime.now()
    body = await request.json()
    message = body.get("message", "")
    logger.info("[CHAT] %s", message[:100])
    if not message:
        return JSONResponse({"error": "No message"}, status_code=400)

    def generate():
        for chunk in agent.chat_stream(message):
            # SSE format: data: <text>\n\n
            escaped = json.dumps(chunk)
            yield f"data: {escaped}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/chat")
async def api_chat(request: Request):
    """Non-streaming fallback."""
    body = await request.json()
    message = body.get("message", "")
    if not message:
        return JSONResponse({"error": "No message"}, status_code=400)
    return {"response": agent.chat(message)}


@app.post("/api/reset")
async def api_reset():
    return {"response": agent.reset()}


@app.post("/api/destroy/{stack_name}")
async def api_destroy(stack_name: str):
    """Deterministic per-stack destroy — bypasses the AI chat tool-use flow.
    Stop button in the UI calls this directly so the destroy is consistent
    regardless of whether Claude/Bedrock is online or offline.

    Steps:
      1. Stop the toolbox container (it holds an attachment to the project
         network, blocking compose's own network removal otherwise).
      2. Profile-aware `docker compose down -v --remove-orphans -t 1`.
      3. Defensive orphan sweep: force-remove any lingering containers by
         the stack's container-name prefix, then remove the project network.
    """
    import asyncio, subprocess as _sub
    meta = agent.stacks.get(stack_name) or agent.plugins.get(stack_name) or {}
    path = meta.get("_path", "")
    if not path or not os.path.isdir(path):
        return {"ok": False, "error": f"Stack '{stack_name}' not found or has no path"}

    # Map stack name → (container_prefix, network_name) for the orphan sweep.
    # Mirrors the logic in /api/exit and Makefile clean.
    prefix_map = {
        "bfsi-fraud-detection":           ("bfsi-", "bfsi-network"),
        "core-banking-simulator":         ("cb-",  "cb-network"),
        "analytics-comparison":           ("bfd-", "bfd-network"),
        "unified-analytics-intelligence": ("uai-", "uai-network"),
        "real-time-analytics":            ("rta-", "rta-net"),
    }
    prefix, network = prefix_map.get(stack_name, ("", ""))

    def teardown():
        log = []
        # 1. Stop toolbox
        try:
            agent.stop_toolbox(stack_name)
            log.append(f"toolbox(diab-toolbox-{stack_name}): removed")
        except Exception as e:
            log.append(f"toolbox: error ({e})")
        # 2. Profile-aware compose down
        try:
            r = _sub.run(
                "PROFS=$(docker compose config --profiles 2>/dev/null | awk '{printf \" --profile %s\", $0}'); "
                "eval \"docker compose $PROFS kill\" 2>/dev/null; "
                "eval \"docker compose $PROFS down -v --remove-orphans -t 1\"",
                shell=True, cwd=path, timeout=60, capture_output=True, text=True
            )
            log.append(f"compose down: rc={r.returncode}")
        except _sub.TimeoutExpired:
            _sub.run("docker compose kill", shell=True, cwd=path, timeout=10, capture_output=True)
            log.append("compose down: timed out — force killed")
        except Exception as e:
            log.append(f"compose down: error ({e})")
        # 3. Defensive orphan sweep — by container-name prefix
        if prefix:
            try:
                r = _sub.run(
                    f'IDS=$(docker ps -aq --filter "name={prefix}"); '
                    f'if [ -n "$IDS" ]; then docker rm -f $IDS 2>/dev/null; echo "$IDS" | wc -l; fi',
                    shell=True, timeout=15, capture_output=True, text=True
                )
                count = (r.stdout or "0").strip().split()[0] if r.stdout else "0"
                log.append(f"orphan sweep ({prefix}*): {count} removed")
            except Exception as e:
                log.append(f"orphan sweep: error ({e})")
        # 4. Network removal (best-effort — succeeds only if no containers attached)
        if network:
            try:
                _sub.run(f"docker network rm {network}", shell=True, timeout=5, capture_output=True)
                log.append(f"network({network}): removed")
            except Exception:
                pass
        # 5. Belt-and-braces: explicit toolbox removal again in case stop_toolbox
        # missed (it's a noop if already gone).
        try:
            _sub.run(f"docker rm -f diab-toolbox-{stack_name}", shell=True, timeout=5, capture_output=True)
        except Exception:
            pass
        return log

    log = await asyncio.to_thread(teardown)
    logger.info("[DESTROY] %s: %s", stack_name, log)
    return {"ok": True, "stack": stack_name, "log": log}


@app.post("/api/exit")
async def api_exit():
    """Clean up all stacks/plugins and shut down the agent.

    Mirrors `make clean`: tears down NF-tracked deployments first, then local
    compose projects on the active runtime, then the inactive runtime (so
    switching deploy target after exit can't hit stale containers/ports),
    then sweeps host ports. See scripts/cross-runtime-clean.sh and
    scripts/clean-ports.sh — those are the same scripts make clean uses.
    """
    import asyncio, subprocess as _sub, signal, concurrent.futures
    from pathlib import Path
    results = []
    project_root = agent.project_root

    # Step 0: Destroy any NF-tracked deployments before local cleanup so we
    # don't leak NF services + secrets when the user hits "Exit".
    for stack_name in list((agent.nf_deployments or {}).keys()):
        try:
            agent.destroy_on_northflank(stack_name)
            results.append(f"{stack_name}: NF destroyed")
        except Exception as e:
            results.append(f"{stack_name}: NF destroy failed ({e})")

    # Step 1: Collect project directories that have containers (running or stopped)
    project_dirs = []
    for parent in [project_root / "stacks", project_root / "plugins"]:
        if not parent.is_dir():
            continue
        for d in sorted(parent.iterdir()):
            if not d.is_dir() or d.name.startswith("_"):
                continue
            candidates = [d] if (d / "docker-compose.yaml").exists() else []
            for sub in sorted(d.iterdir()) if d.is_dir() else []:
                if sub.is_dir() and not sub.name.startswith("_") and (sub / "docker-compose.yaml").exists():
                    candidates.append(sub)
            for cand in candidates:
                try:
                    r = _sub.run("docker compose ps -aq",
                                  shell=True, cwd=str(cand), capture_output=True,
                                  text=True, timeout=5)
                    if r.stdout.strip():
                        project_dirs.append(cand)
                except Exception:
                    pass

    # Step 2: Tear down projects with containers in parallel
    def teardown_project(proj_dir):
        name = proj_dir.name
        try:
            _sub.run(
                "PROFS=$(docker compose config --profiles 2>/dev/null | awk '{printf \" --profile %s\", $0}'); "
                "eval \"docker compose $PROFS kill\" 2>/dev/null; "
                "eval \"docker compose $PROFS down -v --remove-orphans -t 1\"",
                shell=True, cwd=str(proj_dir), timeout=30,
                capture_output=True)
            return f"{name}: cleaned"
        except _sub.TimeoutExpired:
            _sub.run("docker compose kill",
                      shell=True, cwd=str(proj_dir), timeout=10,
                      capture_output=True)
            return f"{name}: force killed"
        except Exception:
            return f"{name}: error"

    if project_dirs:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(teardown_project, d): d.name for d in project_dirs}
            for f in concurrent.futures.as_completed(futures, timeout=120):
                try:
                    results.append(f.result())
                except Exception:
                    results.append(f"{futures[f]}: timeout")
    else:
        results.append("No containers to clean")

    # Step 3: Remove orphan containers by known name prefixes
    # NOTE: bfsi- (BFSI Fraud Detection containers) is distinct from cb-;
    # `docker ps --filter name=cb-` does NOT match `bfsi-app` (substring "cb-"
    # not present in "bfsi-..."). Both prefixes must be listed explicitly.
    prefixes = ["rta-", "lab-", "cb-", "bfsi-", "bfd-", "uai-", "bench-",
                "cdc-rw-", "eapi-rw-", "kafka-rw-", "wh-rw-",
                "dbox-", "sovereign-", "tpl-", "pg-expense-",
                "diab-toolbox-"]
    for prefix in prefixes:
        try:
            r = _sub.run(f'docker ps -aq --filter "name={prefix}"',
                         shell=True, capture_output=True, text=True, timeout=5)
            ids = r.stdout.strip()
            if ids:
                _sub.run(f"docker rm -f {ids}", shell=True, timeout=10, capture_output=True)
        except Exception:
            pass

    # Step 4: Remove known project networks
    for net in ["rta-net", "cb-network", "bfsi-network", "bfd-network", "uai-network", "peerdb_network", "app-net"]:
        try:
            _sub.run(f"docker network rm {net}", shell=True, timeout=5, capture_output=True)
        except Exception:
            pass

    # Step 5: Delete plugin-built integration folders
    import shutil
    for name, meta in agent.stacks.items():
        if meta.get("built_from_plugins") or name.startswith("lab-"):
            path = meta.get("_path", "")
            if path and os.path.exists(path):
                shutil.rmtree(path)
                results.append(f"{name}: deleted (plugin-built)")

    # Step 6: Cross-runtime cleanup — same scripts make clean uses, so the
    # chat-side Exit button leaves the laptop in the same state as a shell-
    # invoked `make clean`. If the user switches deploy target after this,
    # there are no stale containers or port forwards on the other runtime.
    scripts_dir = Path(agent.project_root) / "scripts"
    for script in ("cross-runtime-clean.sh", "clean-ports.sh"):
        path = scripts_dir / script
        if not path.exists():
            continue
        try:
            r = _sub.run(["bash", str(path)], capture_output=True, text=True, timeout=60)
            results.append(f"{script}: rc={r.returncode}")
            if r.returncode != 0 and r.stdout:
                # Surface the foreign-port warning so it appears in the
                # exit response (clean-ports.sh exits 1 when a non-Colima
                # process still holds a swept port).
                results.append(r.stdout.strip().splitlines()[-1] if r.stdout.strip() else "")
        except _sub.TimeoutExpired:
            results.append(f"{script}: timeout")
        except Exception as e:
            results.append(f"{script}: error ({e})")

    logger.info("[APP] Exit: %s", results)
    # Schedule shutdown after response is sent
    async def shutdown():
        await asyncio.sleep(2)
        os.kill(os.getpid(), signal.SIGTERM)
    asyncio.create_task(shutdown())
    return {"response": "All containers stopped. Agent shutting down.", "details": results}


@app.get("/api/pipelines")
async def api_pipelines():
    result = {}
    for name, meta in agent.stacks.items():
        pipelines = meta.get("pipelines", [])
        if pipelines:
            result[name] = {"pipelines": pipelines, "name": meta.get("name", name)}
    with _completed_lock:
        completed = dict(_completed_steps)
    return {"stacks": result, "completed": completed}


import threading

# Step execution state
_step_state = {"running": False, "lines": [], "done": False, "result": None, "start_time": 0, "step_key": ""}
_step_lock = threading.Lock()

# Tracks which pipeline steps have been completed (survives tab navigation)
# Key: "stack_name/pipeline_id/step_id", Value: {"success": bool, "elapsed_ms": int}
_completed_steps = {}
_completed_lock = threading.Lock()

# Pipeline step output history — persisted so the Recent Activity panel can
# rehydrate across page reload, new browser, and agent restart. Without this,
# a user who opens the Workspace tab after a deploy that finished in a prior
# session sees an empty log (the body HTML lives in JS memory only).
# Key: "stack_name/pipeline_id/step_id"
# Value: {"lines": [str], "success": bool, "elapsed_ms": int,
#         "outcome": "done"|"stopped"|"error", "ts": float}
_step_output_history = {}
_step_output_lock = threading.Lock()
_step_output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state", "step-outputs.json")


def _load_step_output_history():
    global _step_output_history
    try:
        if os.path.exists(_step_output_path):
            with open(_step_output_path, "r") as f:
                _step_output_history = json.load(f)
    except Exception as e:
        logger.warning("Could not load step output history: %s", e)
        _step_output_history = {}


def _save_step_output_history():
    # Caller holds _step_output_lock.
    try:
        os.makedirs(os.path.dirname(_step_output_path), exist_ok=True)
        with open(_step_output_path, "w") as f:
            json.dump(_step_output_history, f)
    except Exception as e:
        logger.warning("Could not save step output history: %s", e)


_load_step_output_history()


def _run_step_background(stack_name, pipeline_id, step_id):
    global _step_state
    import time as _time
    for event in agent.run_pipeline_step_stream(stack_name, pipeline_id, step_id):
        with _step_lock:
            if event.get("type") == "line":
                _step_state["lines"].append(event.get("text", ""))
            elif event.get("type") in ("done", "stopped", "error"):
                key = f"{stack_name}/{pipeline_id}/{step_id}"
                elapsed = int((_time.time() - _step_state["start_time"]) * 1000)
                outcome = event.get("type")
                success = bool(outcome == "done" and event.get("success"))
                # Order matters: write _completed_steps and history BEFORE
                # flipping _step_state["done"] = True. /api/pipelines/poll reads
                # _step_state; /api/pipelines reads _completed_steps. The
                # frontend's completion handler often re-fetches /api/pipelines
                # the moment it sees done=True from poll — if we flipped done
                # first, that re-fetch could observe the step as "still running"
                # because _completed_steps wasn't populated yet, and the
                # re-render would briefly show "Running" instead of "Done".
                # Persist the step body so Recent Activity rehydrates across
                # reload / new browser / agent restart. Stored regardless of
                # outcome — failed runs are part of the story too.
                with _step_output_lock:
                    _step_output_history[key] = {
                        "lines": list(_step_state["lines"]),
                        "success": success,
                        "elapsed_ms": elapsed,
                        "outcome": outcome,
                        "ts": _time.time(),
                    }
                    _save_step_output_history()
                if success:
                    with _completed_lock:
                        _completed_steps[key] = {"success": True, "elapsed_ms": elapsed}
                # Now signal completion via the poll-state. Anything reading
                # _completed_steps after this point is guaranteed to see the
                # key (write happened-before via Python's GIL + the explicit
                # ordering here).
                _step_state["result"] = event
                _step_state["done"] = True
                _step_state["running"] = False
                if success:
                    # Pipelines may create or replace dashboards (e.g., OLAP
                    # Start Service creates the 6-tab "Core Banking Fraud
                    # Detection" dashboard). Drop the public-URL cache so the
                    # next iframe open re-resolves to the preferred dashboard.
                    try:
                        _mb_public_url_cache["url"] = None
                        _mb_public_url_cache["ts"] = 0
                        _mb_public_url_cache["err"] = None
                        _mb_public_url_cache["err_ts"] = 0
                    except Exception:
                        pass


@app.post("/api/pipelines/{stack_name}/{pipeline_id}/{step_id}")
async def api_run_pipeline_step(stack_name: str, pipeline_id: str, step_id: str):
    """Start a pipeline step in background thread."""
    global _step_state
    logger.info("[CMD] Pipeline step: %s/%s/%s", stack_name, pipeline_id, step_id)
    import time
    step_key = f"{stack_name}/{pipeline_id}/{step_id}"
    with _step_lock:
        _step_state = {"running": True, "lines": [], "done": False, "result": None, "start_time": time.time(), "step_key": step_key}
    t = threading.Thread(target=_run_step_background, args=(stack_name, pipeline_id, step_id), daemon=True)
    t.start()
    return {"ok": True, "message": "Step started"}


@app.get("/api/pipelines/poll")
async def api_poll_step():
    """Poll for step execution progress."""
    import time
    with _step_lock:
        elapsed_ms = int((time.time() - _step_state["start_time"]) * 1000) if _step_state["start_time"] else 0
        # Return ALL lines — the frontend now caches the full output and shows
        # it in the workspace activity card with native browser scrolling.
        # Earlier we trimmed to last 30 to keep the in-pipeline output compact,
        # but that hid the full log from users who wanted to inspect it.
        return {
            "running": _step_state["running"],
            "lines": list(_step_state["lines"]),
            "line_count": len(_step_state["lines"]),
            "done": _step_state["done"],
            "result": _step_state["result"],
            "elapsed_ms": elapsed_ms
        }


@app.post("/api/stop-step")
async def api_stop_step():
    """Kill the currently running pipeline step process."""
    logger.info("[CMD] Stop step requested")
    killed = agent.stop_active_step()
    return {"ok": True, "killed": killed}


@app.post("/api/pipelines/reset")
async def api_reset_pipeline_state(request: Request):
    """Clear completed-step tracking and step output history for a stack (or all)."""
    global _completed_steps, _step_output_history
    try:
        data = await request.json()
    except Exception:
        data = {}
    with _completed_lock:
        if data and data.get("stack"):
            prefix = data["stack"] + "/"
            _completed_steps = {k: v for k, v in _completed_steps.items() if not k.startswith(prefix)}
        else:
            _completed_steps = {}
    with _step_output_lock:
        if data and data.get("stack"):
            prefix = data["stack"] + "/"
            _step_output_history = {k: v for k, v in _step_output_history.items() if not k.startswith(prefix)}
        else:
            _step_output_history = {}
        _save_step_output_history()
    return {"ok": True}


@app.get("/api/pipelines/step-output/{stack_name}")
async def api_step_output_for_stack(stack_name: str):
    """Return all persisted step output bodies for a stack. Used by the
    Workspace tab to rehydrate Recent Activity on page load / new browser."""
    with _step_output_lock:
        prefix = f"{stack_name}/"
        return {k: v for k, v in _step_output_history.items() if k.startswith(prefix)}


@app.delete("/api/pipelines/step-output/{stack_name}")
async def api_clear_step_output_for_stack(stack_name: str):
    """Clear persisted step output bodies for a stack. Called from the
    'Clear log' button in Recent Activity."""
    global _step_output_history
    with _step_output_lock:
        prefix = f"{stack_name}/"
        _step_output_history = {k: v for k, v in _step_output_history.items() if not k.startswith(prefix)}
        _save_step_output_history()
    return {"ok": True}


@app.post("/api/pipelines/add")
async def api_add_pipeline(request: Request):
    """Add a new use case (pipeline) to a stack's stack.yaml."""
    import yaml
    data = await request.json()
    stack_name = data.get("stack")
    uc_name = data.get("name", "")
    steps = data.get("steps", [])
    if not stack_name or not uc_name or not steps:
        return {"success": False, "error": "Missing stack, name, or steps"}
    meta = agent.stacks.get(stack_name)
    if not meta:
        return {"success": False, "error": f"Stack '{stack_name}' not found"}
    stack_path = meta.get("_path", "")
    yaml_path = os.path.join(stack_path, "stack.yaml") if stack_path else ""
    if not yaml_path or not os.path.exists(yaml_path):
        return {"success": False, "error": "stack.yaml not found"}
    try:
        with open(yaml_path) as f:
            sy = yaml.safe_load(f)
        if "pipelines" not in sy:
            sy["pipelines"] = []
        # Generate pipeline ID from name
        pid = uc_name.lower().replace(" ", "-").replace(":", "")[:30]
        # Build steps
        new_steps = []
        for i, s in enumerate(steps):
            sid = f"{pid}-{i+1}"
            new_steps.append({"id": sid, "name": s["name"], "command": s["cmd"]})
        sy["pipelines"].append({"id": pid, "name": uc_name, "steps": new_steps, "user_added": True})
        with open(yaml_path, "w") as f:
            yaml.dump(sy, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=200)
        # Reload stacks
        agent.stacks = agent._load_stacks()
        return {"success": True, "pipeline_id": pid, "steps": len(new_steps)}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _find_runtime_control(stack_name: str, pipeline_id: str, control_id: str):
    """Locate a runtime_controls entry in stack.yaml. Returns (control, error)."""
    meta = agent.stacks.get(stack_name)
    if not meta:
        return None, f"Stack '{stack_name}' not found"
    for p in meta.get("pipelines", []):
        if p.get("id") != pipeline_id:
            continue
        for c in p.get("runtime_controls", []) or []:
            if c.get("id") == control_id:
                return c, None
        return None, f"Runtime control '{control_id}' not found in pipeline '{pipeline_id}'"
    return None, f"Pipeline '{pipeline_id}' not found"


@app.post("/api/runtime/{stack_name}/{pipeline_id}/{control_id}/{action}")
async def api_runtime_control(stack_name: str, pipeline_id: str, control_id: str, action: str):
    """Execute a runtime_controls start/stop command (e.g., toggle Bank App simulator)."""
    import subprocess as _sp
    if action not in ("start", "stop"):
        return {"success": False, "error": "action must be 'start' or 'stop'"}
    control, err = _find_runtime_control(stack_name, pipeline_id, control_id)
    if err:
        return {"success": False, "error": err}
    cmd = control.get(f"{action}_command")
    if not cmd:
        return {"success": False, "error": f"No {action}_command defined for '{control_id}'"}
    # detached: long-running start scripts (e.g. Airflow image build + boot,
    # 30-90s warm) would otherwise hit the 15s subprocess cap and report
    # failure while the containers keep coming up. Spawn detached and let the
    # status poll drive the toggle state.
    detached = bool(control.get("detached", False)) and action == "start"
    # Match pipeline-step CWD so relative paths (e.g. "bash stacks/.../foo.sh")
    # resolve the same way as a pipeline step's command does.
    rtc_cwd = str(agent.project_root)
    try:
        if detached:
            # Use /tmp explicitly (not tempfile.gettempdir which returns
            # /var/folders/.../T/ on macOS) so the path is predictable for
            # tailing and matches the path the agent advertises to the UI.
            log_path = f"/tmp/diab-rtc-{stack_name}-{pipeline_id}-{control_id}.log"
            log_fh = open(log_path, "w")
            _sp.Popen(cmd, shell=True, stdout=log_fh, stderr=_sp.STDOUT, start_new_session=True, cwd=rtc_cwd)
            return {"success": True, "detached": True, "log_path": log_path}
        r = _sp.run(cmd, shell=True, capture_output=True, text=True, timeout=15, cwd=rtc_cwd)
        if r.returncode != 0:
            return {"success": False, "error": (r.stderr or r.stdout or "command failed").strip()[:500]}
        return {"success": True, "output": (r.stdout or "").strip()[:500]}
    except _sp.TimeoutExpired:
        return {"success": False, "error": "Command timed out after 15s"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/runtime/{stack_name}/{pipeline_id}/{control_id}/status")
async def api_runtime_control_status(stack_name: str, pipeline_id: str, control_id: str):
    """Poll a runtime_controls status_url and return the configured status_field as a boolean."""
    import httpx
    control, err = _find_runtime_control(stack_name, pipeline_id, control_id)
    if err:
        return {"available": False, "error": err}
    url = control.get("status_url")
    field = control.get("status_field")
    if not url or not field:
        return {"available": False, "error": "status_url or status_field not configured"}
    # Optional: status_value lets the field be compared by equality instead of
    # truthiness. Needed when the upstream returns "RUNNING" vs "PAUSED" (both
    # truthy strings) and "running" maps to a specific value (e.g. PAUSED for
    # a "pause-toggle" semantics — when the connector is paused, the toggle is
    # in its "active" state). Combine with dot-path in field for nested JSON
    # like {"connector":{"state":"PAUSED"}}.
    status_value = control.get("status_value")
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"available": False, "error": f"status_url returned HTTP {r.status_code}"}
            data = r.json()
            raw = _dot_path(data, field) if "." in field else data.get(field, False)
            running = (str(raw) == str(status_value)) if status_value is not None else bool(raw)
            return {"available": True, "running": running}
    except Exception as e:
        return {"available": False, "error": str(e)[:200]}


def _dot_path(obj, path: str):
    """Traverse nested dict/list by dotted path. Integer segments index arrays
    (e.g. 'dag_runs.0.state'). Returns None on miss."""
    if obj is None or not path:
        return None
    for seg in path.split("."):
        if seg == "":
            continue
        if isinstance(obj, list):
            try:
                idx = int(seg)
            except ValueError:
                return None
            if idx < 0 or idx >= len(obj):
                return None
            obj = obj[idx]
        elif isinstance(obj, dict):
            if seg not in obj:
                return None
            obj = obj[seg]
        else:
            return None
    return obj


@app.get("/api/runtime/{stack_name}/{pipeline_id}/{control_id}/last-run")
async def api_runtime_control_last_run(stack_name: str, pipeline_id: str, control_id: str):
    """Fetch a 'last run summary' from a runtime_control's last_run_url and
    extract a normalized state + time via dot paths configured in stack.yaml:

      last_run_url:          GET this URL
      last_run_auth_basic:   "user:pass" sent as HTTP Basic (optional)
      last_run_state_path:   e.g. "dag_runs.0.state" — string in {success, failed, running, queued, ...}
      last_run_time_path:    e.g. "dag_runs.0.end_date" — ISO 8601 string

    Returns {available, state, time, error?}. The frontend renders the line."""
    import httpx, base64
    control, err = _find_runtime_control(stack_name, pipeline_id, control_id)
    if err:
        return {"available": False, "error": err}
    url = control.get("last_run_url")
    sp  = control.get("last_run_state_path")
    tp  = control.get("last_run_time_path")
    if not url or not sp:
        return {"available": False, "error": "last_run_url or last_run_state_path not configured"}
    auth = control.get("last_run_auth_basic")
    headers = {}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                return {"available": False, "error": f"last_run_url returned HTTP {r.status_code}"}
            data = r.json()
            state = _dot_path(data, sp)
            time_v = _dot_path(data, tp) if tp else None
            if state is None:
                return {"available": True, "state": None, "time": None}
            return {"available": True, "state": str(state), "time": str(time_v) if time_v is not None else None}
    except Exception as e:
        return {"available": False, "error": str(e)[:200]}


@app.delete("/api/pipelines/{stack_name}/{pipeline_id}")
async def api_delete_pipeline(stack_name: str, pipeline_id: str):
    """Delete a user-added use case from stack.yaml."""
    import yaml
    meta = agent.stacks.get(stack_name)
    if not meta:
        return {"success": False, "error": f"Stack '{stack_name}' not found"}
    stack_path = meta.get("_path", "")
    yaml_path = os.path.join(stack_path, "stack.yaml") if stack_path else ""
    if not yaml_path or not os.path.exists(yaml_path):
        return {"success": False, "error": "stack.yaml not found"}
    try:
        with open(yaml_path) as f:
            sy = yaml.safe_load(f)
        pipelines = sy.get("pipelines", [])
        # Find and verify it's user-added
        found = None
        for p in pipelines:
            if p.get("id") == pipeline_id:
                found = p
                break
        if not found:
            return {"success": False, "error": f"Use case '{pipeline_id}' not found"}
        if not found.get("user_added"):
            return {"success": False, "error": "Only user-added use cases can be deleted"}
        pipelines.remove(found)
        sy["pipelines"] = pipelines
        with open(yaml_path, "w") as f:
            yaml.dump(sy, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=200)
        agent.stacks = agent._load_stacks()
        return {"success": True, "deleted": pipeline_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.delete("/api/integration/{stack_name}")
async def api_delete_integration(stack_name: str):
    """Delete a user-built integration (only plugin-built integrations can be deleted)."""
    import subprocess, shutil
    meta = agent.stacks.get(stack_name)
    if not meta:
        return JSONResponse({"error": f"Integration '{stack_name}' not found"}, status_code=404)
    if not meta.get("built_from_plugins") and not stack_name.startswith("lab-") and not meta.get("deletable"):
        return JSONResponse({"error": "Only plugin-built integrations can be deleted"}, status_code=403)
    # Check if running
    try:
        result = subprocess.run(
            "docker compose ls --format json 2>/dev/null || echo '[]'",
            shell=True, capture_output=True, text=True, timeout=10
        )
        projects = json.loads(result.stdout.strip())
        running = [p.get("Name", "") for p in projects if "running" in p.get("Status", "").lower()]
        if any(stack_name.replace("-", "") in r.replace("-", "") for r in running):
            return JSONResponse({"error": "Stop the integration before deleting"}, status_code=400)
    except Exception:
        pass
    # Delete folder
    path = meta.get("_path", "")
    if path and os.path.exists(path):
        shutil.rmtree(path)
    # Reload stacks
    agent.stacks = agent._load_stacks()
    return {"status": "deleted", "name": stack_name}


@app.get("/api/logs/{container_name}")
async def api_container_logs(container_name: str, lines: int = 50):
    """Return tail logs for a container."""
    import subprocess
    try:
        result = subprocess.run(
            f"docker logs {container_name} --tail {lines} 2>&1",
            shell=True, capture_output=True, text=True, timeout=10
        )
        return {"container": container_name, "logs": result.stdout + result.stderr, "lines": lines}
    except Exception as e:
        return {"container": container_name, "logs": f"Error: {str(e)}", "lines": 0}


@app.post("/api/reload")
async def api_reload():
    """Rescan stacks and plugins folders without restarting agent."""
    agent.stacks = agent._load_stacks()
    agent.plugins = agent._load_plugins()
    return {"status": "reloaded", "stacks": len(agent.stacks), "plugins": len(agent.plugins)}


# ── SynthDB Proxy Endpoints ──
import subprocess as _sp
import httpx

SYNTHDB_URL = "http://127.0.0.1:8050"
SYNTHDB_COMPOSE = os.path.join(os.path.dirname(__file__), "..", "synthdb", "docker-compose.yaml")
_synthdb_started = False


def _synthdb_compose_path():
    return os.path.abspath(SYNTHDB_COMPOSE)


@app.post("/api/synthdb/start")
async def api_synthdb_start():
    """Start the SynthDB container on demand."""
    global _synthdb_started
    try:
        compose = _synthdb_compose_path()
        result = _sp.run(
            f"docker compose -f {compose} up -d --build",
            shell=True, capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return {"success": False, "error": result.stderr[:500]}
        # Wait for healthy
        import time
        for i in range(30):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.get(f"{SYNTHDB_URL}/health", timeout=3)
                    if r.status_code == 200:
                        _synthdb_started = True
                        return {"success": True}
            except:
                pass
            time.sleep(2)
        return {"success": False, "error": "Container started but health check timed out"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/synthdb/stop")
async def api_synthdb_stop():
    """Stop the SynthDB container."""
    global _synthdb_started
    try:
        compose = _synthdb_compose_path()
        _sp.run(f"docker compose -f {compose} down -v", shell=True, capture_output=True, text=True, timeout=30)
        _synthdb_started = False
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/synthdb/models")
async def api_synthdb_models():
    """Proxy to synthdb: list models."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{SYNTHDB_URL}/api/models", timeout=10)
            return r.json()
    except Exception as e:
        return {"models": [], "error": str(e)}


@app.get("/api/synthdb/preview/{name}")
async def api_synthdb_preview(name: str):
    """Proxy to synthdb: preview seed data."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{SYNTHDB_URL}/api/models/{name}/preview?limit=25", timeout=10)
            return r.json()
    except Exception as e:
        return {"preview": {}, "error": str(e)}


@app.get("/api/synthdb/targets")
async def api_synthdb_targets():
    """Auto-detect running database containers by inspecting Docker images."""
    targets = []
    try:
        # Get all running containers with their images
        result = _sp.run(
            "docker ps --format '{{.Names}}|{{.Image}}|{{.Ports}}'",
            shell=True, capture_output=True, text=True, timeout=10
        )
        if not result.stdout.strip():
            return {"targets": targets}

        # Map: container_name -> image
        running_containers = {}
        for line in result.stdout.strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 2:
                name = parts[0].strip()
                image = parts[1].strip().lower()
                if "synthdb" not in name.lower():
                    running_containers[name] = image

        # DB image patterns -> type
        db_patterns = {
            "postgres": "postgresql",
            "paradedb": "postgresql",
            "pg_clickhouse": "postgresql",
            "edb": "postgresql",
            "pgd": "postgresql",
            "pgextended": "postgresql",
            "oracle": "oracle",
            "oracledb": "oracle",
        }

        # For each stack, find running DB containers and match with credentials
        for sname, meta in agent.stacks.items():
            compose_path = meta.get("_path", "")
            if not compose_path:
                continue
            try:
                import yaml
                cf = os.path.join(compose_path, "docker-compose.yaml")
                if not os.path.exists(cf):
                    continue
                with open(cf) as f:
                    comp = yaml.safe_load(f)

                for sv_name, sv_def in comp.get("services", {}).items():
                    # Get the image
                    image = (sv_def.get("image", "") or "").lower()
                    # Also check build context (custom Dockerfiles often extend postgres/oracle)
                    if not image and sv_def.get("build"):
                        # Check Dockerfile for FROM postgres/oracle
                        build_ctx = sv_def.get("build")
                        if isinstance(build_ctx, str):
                            df_path = os.path.join(compose_path, build_ctx, "Dockerfile")
                        elif isinstance(build_ctx, dict):
                            ctx = build_ctx.get("context", ".")
                            df = build_ctx.get("dockerfile", "Dockerfile")
                            df_path = os.path.join(compose_path, ctx, df)
                        else:
                            df_path = ""
                        if df_path and os.path.exists(df_path):
                            with open(df_path) as f:
                                df_content = f.read()
                                for line in df_content.split('\n'):
                                    if line.strip().upper().startswith("FROM"):
                                        image = line.strip().split()[1].lower()
                                        break
                                # If FROM base doesn't match, check if Dockerfile installs DB packages
                                if not any(p in image for p in db_patterns):
                                    df_lower = df_content.lower()
                                    if "edb-pgd" in df_lower or "pgextended" in df_lower or "pgaa" in df_lower:
                                        image = "edb-pgd"
                                    elif "postgresql" in df_lower and "apt-get install" in df_lower:
                                        image = "postgres"

                    # Check if this image is a database
                    db_type = None
                    for pattern, dtype in db_patterns.items():
                        if pattern in image:
                            db_type = dtype
                            break
                    if not db_type:
                        continue

                    # Find the running container name
                    explicit_name = sv_def.get("container_name", "")
                    container = None
                    if explicit_name and explicit_name in running_containers:
                        container = explicit_name
                    else:
                        # Docker-generated: <project>-<service>-<num>
                        for rc in running_containers:
                            if sv_name in rc and (sname.replace("-", "") in rc.replace("-", "") or sname in rc):
                                container = rc
                                break

                    if not container:
                        continue

                    # Find credentials from stack.yaml
                    username = ""
                    password = ""
                    port = ""
                    db_name = "postgres"

                    # Try credentials list
                    for cred in meta.get("credentials", []):
                        csvc = cred.get("service", "").lower()
                        if csvc in sv_name.lower() or sv_name.lower() in csvc:
                            username = cred.get("username", "")
                            password = cred.get("password", "")
                            port = str(cred.get("port", ""))
                            break

                    # Fallback: read from compose environment
                    if not username:
                        env = sv_def.get("environment", {})
                        if isinstance(env, dict):
                            username = env.get("POSTGRES_USER", env.get("POSTGRES_USERNAME", "postgres"))
                            password = env.get("POSTGRES_PASSWORD", "postgres")
                            db_name = env.get("POSTGRES_DB", env.get("POSTGRES_DATABASE", "postgres"))
                        elif isinstance(env, list):
                            for e in env:
                                if "POSTGRES_USER=" in str(e):
                                    username = str(e).split("=", 1)[1]
                                elif "POSTGRES_PASSWORD=" in str(e):
                                    password = str(e).split("=", 1)[1]
                                elif "POSTGRES_DB=" in str(e):
                                    db_name = str(e).split("=", 1)[1]

                    # Get host port from compose port mapping
                    if not port:
                        for p in sv_def.get("ports", []):
                            p_str = str(p)
                            parts = p_str.replace('"', '').split(":")
                            if len(parts) == 2:
                                port = parts[0]
                            elif len(parts) == 3:
                                port = parts[1]
                            if port:
                                break

                    if not port or not username:
                        continue

                    # Get db_name from environment if not already set
                    env = sv_def.get("environment", {})
                    if isinstance(env, dict):
                        db_name = env.get("POSTGRES_DB", env.get("POSTGRES_DATABASE", db_name))

                    conn = f"postgresql://{username}:{password}@host.docker.internal:{port}/{db_name}"
                    targets.append({
                        "container": container,
                        "type": db_type,
                        "port": port,
                        "conn": conn,
                        "stack": sname
                    })
            except:
                pass
    except:
        pass
    # Deduplicate by container name — keep first match (most specific)
    seen = set()
    unique = []
    for t in targets:
        if t["container"] not in seen:
            seen.add(t["container"])
            unique.append(t)
    return {"targets": unique}


@app.post("/api/synthdb/generate")
async def api_synthdb_generate(request: Request):
    """Proxy to synthdb: generate data. Uses host.docker.internal for DB connectivity."""
    params = dict(request.query_params)
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{SYNTHDB_URL}/api/generate", params=params, timeout=300)
            return r.json()
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/synthdb/upload")
async def api_synthdb_upload(request: Request):
    """Proxy to synthdb: upload model JSON files."""
    form = await request.form()
    files = {}
    for key in form:
        f = form[key]
        files[key] = (f.filename, await f.read(), f.content_type)
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{SYNTHDB_URL}/api/models/upload", files=files, timeout=30)
            return r.json()
    except Exception as e:
        return {"valid": False, "errors": [str(e)]}


@app.post("/api/synthdb/upload-csv")
async def api_synthdb_upload_csv(request: Request):
    """Proxy to synthdb: upload CSV files."""
    form = await request.form()
    files = []
    for key in form:
        f = form[key]
        files.append(("files", (f.filename, await f.read(), f.content_type)))
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{SYNTHDB_URL}/api/models/upload-csv", files=files, timeout=30)
            return r.json()
    except Exception as e:
        return {"valid": False, "errors": [str(e)]}


@app.get("/api/synthdb/generated-preview/{model}/{table}")
async def api_synthdb_gen_preview(model: str, table: str):
    """Proxy to synthdb: preview generated data."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{SYNTHDB_URL}/api/generate/{model}/preview?table={table}&limit=25", timeout=10)
            return r.json()
    except Exception as e:
        return {"rows": [], "error": str(e)}


@app.get("/api/synthdb/download/{model}")
async def api_synthdb_download(model: str):
    """Proxy to synthdb: download generated CSV zip."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{SYNTHDB_URL}/api/generate/{model}/download", timeout=30)
            return StreamingResponse(
                iter([r.content]),
                media_type="application/zip",
                headers={"Content-Disposition": f"attachment; filename={model}_synthetic_data.zip"}
            )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/monitoring")
async def api_monitoring():
    """Lightweight container monitoring via docker stats."""
    import subprocess
    try:
        result = subprocess.run(
            'docker stats --no-stream --format \'{"name":"{{.Name}}","cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}","mem_pct":"{{.MemPerc}}","net":"{{.NetIO}}","block":"{{.BlockIO}}","pids":"{{.PIDs}}"}\'',
            shell=True, capture_output=True, text=True, timeout=15
        )
        containers = []
        for line in result.stdout.strip().split("\n"):
            if line.strip():
                try:
                    c = json.loads(line)
                    if "synthdb" not in c.get("name", "").lower():
                        containers.append(c)
                except json.JSONDecodeError:
                    pass
        return {"containers": containers, "count": len(containers)}
    except Exception as e:
        return {"containers": [], "error": str(e)}


@app.get("/api/running")
async def api_running():
    """Return currently running compose projects + Northflank deployments."""
    import subprocess
    deploy_targets = {}  # stack_name -> "laptop" or "northflank"
    running_names = []
    slot_count = 0
    try:
        result = subprocess.run(
            "docker compose ls --format json 2>/dev/null || echo '[]'",
            shell=True, capture_output=True, text=True, timeout=10
        )
        projects = json.loads(result.stdout.strip())
        for p in projects:
            if "running" in p.get("Status", "").lower():
                name = p.get("Name", "")
                config = p.get("ConfigFiles", "")
                if "synthdb" in name.lower():
                    continue
                slot_count += 1
                canonical = name
                if config:
                    import os
                    folder = os.path.basename(os.path.dirname(config.split(",")[0]))
                    if folder:
                        canonical = folder
                if canonical not in running_names:
                    running_names.append(canonical)
                    deploy_targets[canonical] = "laptop"
    except Exception:
        pass
    # Add Northflank-deployed stacks
    for stack_name in (agent.nf_deployments or {}):
        if stack_name == "__unknown__":
            continue
        if stack_name not in running_names:
            running_names.append(stack_name)
            slot_count += 1
        deploy_targets[stack_name] = "northflank"
    return {
        "running": running_names,
        "count": slot_count,
        "deploy_targets": deploy_targets,
        "nf_console_url": agent.get_nf_console_url() if agent.nf_deployments else None,
        "docker_runtime": getattr(agent, "docker_runtime", "unknown"),
        "host_os": getattr(agent, "host_os", "other"),
        "colima_supported": getattr(agent, "colima_supported", True),
    }


@app.post("/api/laptop/switch-context")
async def api_laptop_switch_context(payload: dict = None):
    """Switch the docker CLI context to match the chosen target.
    Used by the UI when preflight detects a context mismatch and the user
    clicks 'auto-switch'."""
    target = (payload or {}).get("target", "")
    if target not in ("laptop-docker", "laptop-colima"):
        return {"ok": False, "error": f"invalid target: {target}"}
    ok, msg = agent.laptop.ensure_context(target)
    if ok:
        agent.docker_runtime = agent.laptop.runtime
    return {"ok": ok, "message": msg, "runtime": agent.docker_runtime}


@app.get("/api/stackinfo")
async def api_stackinfo():
    """Return access info for running stacks."""
    import subprocess
    try:
        result = subprocess.run(
            "docker compose ls --format json 2>/dev/null || echo '[]'",
            shell=True, capture_output=True, text=True, timeout=10
        )
        projects = json.loads(result.stdout.strip())
        running_names = []
        for p in projects:
            if "running" in p.get("Status", "").lower():
                pname = p.get("Name", "")
                if "synthdb" in pname.lower():
                    continue
                canonical = pname
                config = p.get("ConfigFiles", "")
                if config:
                    import os
                    folder = os.path.basename(os.path.dirname(config.split(",")[0]))
                    if folder:
                        canonical = folder
                if canonical not in running_names:
                    running_names.append(canonical)
    except Exception:
        running_names = []
    info = []
    for name, meta in agent.stacks.items():
        is_nf = name in (agent.nf_deployments or {})
        if any(name in r or r in name for r in running_names) or is_nf:
            entry = {"name": meta.get("name", name)}
            entry["access"] = meta.get("access", [])
            entry["credentials"] = meta.get("credentials", [])
            entry["sample_commands"] = meta.get("sample_commands", [])
            if is_nf:
                entry["deploy_target"] = "northflank"
                entry["nf_console_url"] = agent.get_nf_console_url()
                entry["nf_services"] = agent.nf_deployments[name].get("services", [])
            else:
                entry["deploy_target"] = "laptop"
            info.append(entry)
    return {"stacks": info}


# ─────────────────────────────────────────────────────────────────────
# Northflank deploy / lifecycle endpoints
# ─────────────────────────────────────────────────────────────────────


@app.post("/api/nf/deploy")
async def api_nf_deploy(payload: dict):
    """Deploy a stack to Northflank."""
    stack_name = (payload or {}).get("stack", "").strip()
    if not stack_name:
        return {"error": "stack name required"}
    if stack_name not in agent.stacks:
        return {"error": f"unknown stack: {stack_name}"}
    msg = agent.deploy_to_northflank(stack_name)
    return {"message": msg, "console_url": agent.get_nf_console_url()}


@app.post("/api/nf/stop")
async def api_nf_stop(payload: dict):
    """Pause all NF services for a stack."""
    stack_name = (payload or {}).get("stack", "").strip()
    if not stack_name:
        return {"error": "stack name required"}
    msg = agent.stop_on_northflank(stack_name)
    return {"message": msg}


@app.post("/api/nf/resume")
async def api_nf_resume(payload: dict):
    """Resume paused NF services for a stack."""
    stack_name = (payload or {}).get("stack", "").strip()
    if not stack_name:
        return {"error": "stack name required"}
    msg = agent.resume_on_northflank(stack_name)
    return {"message": msg}


@app.post("/api/nf/destroy")
async def api_nf_destroy(payload: dict):
    """Delete all NF services for a stack."""
    stack_name = (payload or {}).get("stack", "").strip()
    if not stack_name:
        return {"error": "stack name required"}
    msg = agent.destroy_on_northflank(stack_name)
    return {"message": msg}


@app.post("/api/local/cleanup")
async def api_local_cleanup():
    """Force-cleanup for laptop targets: iterate every known stack and run
    `docker compose down -v --remove-orphans` plus a container-prefix orphan
    sweep. Mirrors what /api/destroy/{stack} does but unconditional and
    project-wide, so it works even when the agent's running-stacks view is
    stale or empty.

    Doesn't stop the agent or remove the toolbox image — just tears down
    stack containers/networks/volumes."""
    import asyncio, subprocess as _sub
    prefix_map = {
        "bfsi-fraud-detection":           ("bfsi-", "bfsi-network"),
        "core-banking-simulator":         ("cb-",  "cb-network"),
        "analytics-comparison":           ("bfd-", "bfd-network"),
        "unified-analytics-intelligence": ("uai-", "uai-network"),
        "real-time-analytics":            ("rta-", "rta-net"),
    }
    log_lines = []
    stacks_cleaned = 0
    for stack_name, meta in agent.stacks.items():
        path = meta.get("_path", "")
        if not path or not os.path.isdir(path):
            continue
        # Best-effort toolbox stop (frees the project network)
        try:
            agent.stop_toolbox(stack_name)
        except Exception:
            pass
        # Profile-aware compose down
        rc = "?"
        try:
            r = await asyncio.to_thread(_sub.run,
                "PROFS=$(docker compose config --profiles 2>/dev/null | awk '{printf \" --profile %s\", $0}'); "
                "eval \"docker compose $PROFS kill\" 2>/dev/null; "
                "eval \"docker compose $PROFS down -v --remove-orphans -t 1\"",
                shell=True, cwd=path, timeout=60, capture_output=True, text=True)
            rc = r.returncode
            stacks_cleaned += 1
        except _sub.TimeoutExpired:
            rc = "timeout"
        except Exception as e:
            rc = f"err:{e}"
        prefix, network = prefix_map.get(stack_name, ("", ""))
        # Orphan container sweep by name prefix
        if prefix:
            try:
                _sub.run(f'IDS=$(docker ps -aq --filter "name={prefix}"); '
                         f'[ -n "$IDS" ] && docker rm -f $IDS 2>/dev/null || true',
                         shell=True, timeout=15, capture_output=True)
            except Exception:
                pass
        # Network removal (best-effort)
        if network:
            try:
                _sub.run(f"docker network rm {network} 2>/dev/null || true",
                         shell=True, timeout=5, capture_output=True)
            except Exception:
                pass
        log_lines.append(f"{stack_name}: compose down rc={rc}")
    msg = f"**Local cleanup complete:** {stacks_cleaned} stack(s) torn down (compose down -v + orphan sweep + network rm)."
    if log_lines:
        msg += "\n\n" + "\n".join(f"  - {l}" for l in log_lines)
    return {"message": msg, "stacks_cleaned": stacks_cleaned, "log": log_lines}


@app.post("/api/nf/cleanup")
async def api_nf_cleanup():
    """Force-wipe every service, job, and secret in the configured NF
    project. Doesn't depend on the agent's in-memory deployment tracking,
    so it works even when destroy fails because the agent restarted and
    lost track of what it had deployed. Returns counts of what was deleted."""
    res = agent.nuke_northflank_project()
    msg = (f"**Cleanup complete:** deleted {res['services']} services, "
           f"{res['jobs']} jobs, {res['secrets']} secrets")
    if res["errors"]:
        msg += "\n\nErrors:\n" + "\n".join(f"  - {e}" for e in res["errors"][:10])
    return {"message": msg, **res}


@app.post("/api/nf/cancel")
async def api_nf_cancel(payload: dict):
    """Signal an in-flight NF deploy to stop. The deploy() loop in
    NorthflankTranslator checks the cancel flag before each service create
    and exits early; it then destroys any partial state before returning.
    The original /api/nf/deploy fetch will resolve with the cancel message."""
    stack_name = (payload or {}).get("stack", "").strip()
    if not stack_name:
        return {"error": "stack name required"}
    return agent.cancel_on_northflank(stack_name)


@app.get("/api/nf/status")
async def api_nf_status():
    """Return readiness/status of NF services in the configured project."""
    return {
        **agent.get_nf_status(),
        "console_url": agent.get_nf_console_url(),
    }


_mb_public_url_cache = {"url": None, "ts": 0}


_data_activity_cache = {"key": None, "ts": 0.0, "payload": None}


@app.get("/api/data-activity/{stack_name}")
async def api_data_activity(stack_name: str):
    """
    Synthetic Data Story payload for the Workspace home banner.

    Returns three sections — used by the 3-pill UI:
      seed:        rows in the SDV seed JSON (what the model learned from)
      synthesized: live row counts in pgd from the OLTP tables
      streaming:   Bank App live simulator state (running flag + simple counters)

    All fields are best-effort; any missing source returns zeros so the UI can
    render gracefully on fresh deploys.

    Internally, all `subprocess.run` calls are dispatched via
    `asyncio.to_thread` so the event loop stays responsive. A short in-memory
    cache (1.5s) coalesces bursty polls (frontend ticks every 3s but Data
    Story banner + Use Case Activity card both call this).
    """
    import json as _json
    import subprocess as _sp
    import httpx as _httpx
    import asyncio as _asyncio
    import time as _t
    from pathlib import Path

    # Coalesce: serve a recent cached payload to avoid stacking docker execs
    now = _t.time()
    cached = _data_activity_cache
    if cached["key"] == stack_name and (now - cached["ts"]) < 1.5 and cached["payload"]:
        return cached["payload"]

    out = {
        "seed": {"rows": 0, "tables": []},
        "synthesized": {"customers": 0, "accounts": 0, "transactions": 0,
                        "fraud_labels": 0, "fraud_rate_pct": 0.0},
        "streaming": {"sim_running": False, "session_total": 0, "session_fraud": 0},
        # OLAP fan-out: CH/RW counts + lag vs PGD. Each store is None when its
        # container isn't running so the UI can hide its pill cleanly.
        "olap": {"clickhouse": None, "risingwave": None,
                 "ch_lag_pct": None, "rw_lag_pct": None},
    }

    # 1. Seed (read SDV model JSON)
    repo_root = Path(__file__).resolve().parents[2]
    seed_file = repo_root / "engine" / "synthdb" / "models" / "fraud_bank_seed_data.json"
    try:
        if seed_file.exists():
            seed = _json.loads(seed_file.read_text())
            for k, v in seed.items():
                if not k.startswith("_") and isinstance(v, list):
                    out["seed"]["tables"].append({"table": k, "rows": len(v)})
                    out["seed"]["rows"] += len(v)
    except Exception:
        pass

    def _run(cmd, timeout):
        return _sp.run(cmd, capture_output=True, text=True, timeout=timeout)

    # 2. Synthesized — query pgd via docker exec (only if container is up)
    try:
        # Cast all values to text to avoid UNION ALL type mismatch (counts
        # are bigint, fraud_rate_pct is numeric — Postgres rejects mixed types).
        sql = ("SELECT 'customers',COUNT(*)::text FROM customers "
               "UNION ALL SELECT 'accounts',COUNT(*)::text FROM accounts "
               "UNION ALL SELECT 'transactions',COUNT(*)::text FROM transactions "
               "UNION ALL SELECT 'fraud_labels',COUNT(*)::text FROM fraud_labels "
               "UNION ALL SELECT 'fraud_rate_pct', "
               "COALESCE(ROUND(100.0*COUNT(*) FILTER (WHERE is_fraud) "
               "/ NULLIF(COUNT(*),0)::numeric, 2), 0)::text "
               "FROM fraud_labels WHERE detection_source='rules';")
        r = await _asyncio.to_thread(
            _run,
            ["docker", "exec", "-e", "PGPASSWORD=secret", "bfsi-pgd",
             "psql", "-U", "postgres", "-d", "demo", "-t", "-A", "-F", "|", "-c", sql],
            4
        )
        for line in (r.stdout or "").strip().split("\n"):
            if not line or "|" not in line:
                continue
            k, v = line.split("|", 1)
            try:
                out["synthesized"][k] = float(v) if k.endswith("_pct") else int(v)
            except ValueError:
                pass
    except Exception:
        pass

    # 3. Streaming — Bank App health
    try:
        async with _httpx.AsyncClient(timeout=2) as client:
            h = await client.get("http://127.0.0.1:3001/api/health")
            if h.status_code == 200:
                out["streaming"]["sim_running"] = bool(h.json().get("simRunning", False))
    except Exception:
        pass

    # 4. OLAP fan-out — CH and RW counts + lag vs PGD (best-effort, fast-fail)
    # Both ClickHouse and RisingWave receive Debezium CDC events (one INSERT
    # event + one UPDATE event per OLTP row). Without dedup, raw counts are
    # 2-5x PG. We use:
    #   - CH:  ReplacingMergeTree FINAL → exact dedup (matches PG row count)
    #   - RW:  count(DISTINCT tx_id)   → dedup by primary key on the MV
    pg_tx = out["synthesized"].get("transactions", 0) or 0
    try:
        r = await _asyncio.to_thread(
            _run,
            ["docker", "exec", "bfsi-clickhouse", "clickhouse-client",
             "--user", "default", "--password", "admin123",
             "--query", "SELECT count() FROM default.transactions FINAL"],
            5
        )
        if r.returncode == 0:
            ch = int((r.stdout or "0").strip() or 0)
            out["olap"]["clickhouse"] = ch
            if pg_tx > 0:
                out["olap"]["ch_lag_pct"] = round((1 - ch / pg_tx) * 100, 1)
    except Exception:
        pass
    try:
        r = await _asyncio.to_thread(
            _run,
            ["docker", "exec", "-e", "PGPASSWORD=secret", "bfsi-pgd",
             "psql", "-h", "risingwave", "-p", "4566", "-U", "root", "-d", "dev",
             "-tAc", "SELECT count(DISTINCT tx_id) FROM transactions;"],
            5
        )
        if r.returncode == 0:
            rw = int((r.stdout or "0").strip().splitlines()[0] or 0)
            out["olap"]["risingwave"] = rw
            if pg_tx > 0:
                out["olap"]["rw_lag_pct"] = round((1 - rw / pg_tx) * 100, 1)
    except Exception:
        pass

    _data_activity_cache["key"] = stack_name
    _data_activity_cache["ts"] = _t.time()
    _data_activity_cache["payload"] = out
    return out


@app.get("/api/console-probe")
async def api_console_probe(url: str, kind: str = "auto"):
    """
    Probe a console URL to decide whether the iframe will render usefully.

    Returns:
      state ∈ {ok, starting, down, unconfigured}
      hint  : human-readable one-liner
      action: optional next step the user should take

    `kind` lets us layer service-specific awareness on top of generic HTTP probe:
      auto      — generic 200/connection check only
      metabase  — also detect first-run wizard / not-yet-configured state
      bankapp   — detect "Phase 1 form" (i.e., DB not connected / not seeded)
    """
    import httpx
    import asyncio as _asyncio
    try:
        async with httpx.AsyncClient(timeout=2.5, follow_redirects=True) as client:
            # Generic reachability
            try:
                r = await client.get(url)
            except httpx.ConnectError:
                return {"state": "down", "hint": "Service is not reachable. The container may not be running yet."}
            except httpx.ReadTimeout:
                return {"state": "starting", "hint": "Service is starting — try again in a few seconds."}
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError):
                # Transient: server accepted the connection then dropped it
                # before sending a response. Common on busy services (Airflow
                # during DAG parse, Metabase during dashboard build). One
                # quick retry stops the iframe overlay from flashing red on
                # every refresh just because the service blinked.
                await _asyncio.sleep(0.3)
                try:
                    r = await client.get(url)
                except Exception:
                    return {"state": "starting", "hint": "Service is busy — retrying in a few seconds."}

            if r.status_code >= 500:
                return {"state": "starting", "hint": f"Service returned HTTP {r.status_code}. Likely still warming up."}
            if r.status_code in (502, 503, 504):
                return {"state": "starting", "hint": "Service is starting up..."}

            # Detect iframe-blocking response headers. Metabase/MinIO and many
            # other tools send X-Frame-Options: DENY or frame-ancestors 'none'
            # for clickjacking protection. On laptop the nginx-proxy strips
            # these; on NF there's nothing in front of the service, so the
            # browser refuses to render the iframe ("refused to connect").
            # We tell the workspace to render an "Open in new tab" overlay
            # instead of attempting to iframe.
            xfo = (r.headers.get("x-frame-options", "") or "").upper()
            csp = (r.headers.get("content-security-policy", "") or "").lower()
            blocked_by_csp = False
            if "frame-ancestors" in csp:
                # Extract the frame-ancestors directive
                fa_part = csp.split("frame-ancestors", 1)[1].split(";", 1)[0].strip()
                # 'none' or no allowed origins → blocked. We don't try to be
                # clever about matching specific origins here.
                if "none" in fa_part or fa_part == "" or fa_part == "'none'":
                    blocked_by_csp = True
            if xfo in ("DENY", "SAMEORIGIN") or blocked_by_csp:
                return {
                    "state": "iframe-blocked",
                    "hint": "This service blocks iframe embedding (X-Frame-Options / CSP).",
                    "action": "Open it in a new tab using the button above.",
                }

            # Service-specific detection
            if kind == "metabase":
                # Metabase is reachable. Is it set up? Use 'has-user-setup'
                # (true after admin account is created). The 'setup-token'
                # field stays populated even after setup completes, so don't
                # use it as the unconfigured signal.
                try:
                    p = await client.get(url.rstrip("/") + "/api/session/properties")
                    props = p.json() if p.status_code == 200 else {}
                    if props.get("has-user-setup") is True:
                        return {"state": "ok"}
                    return {
                        "state": "unconfigured",
                        "hint": "Metabase is running but not configured yet.",
                        "action": "Run the OLTP pipeline (Usecase 1 — Start Service) which auto-configures Metabase with an OLTP dashboard.",
                    }
                except Exception:
                    return {"state": "ok"}

            if kind == "bankapp":
                # Bank App is up. Phase 2 needs DB connected + data seeded.
                # /api/health returns dbConnected + simRunning.
                try:
                    h = await client.get(url.rstrip("/") + "/api/health")
                    if h.status_code == 200 and h.json().get("dbConnected"):
                        return {"state": "ok"}
                    return {
                        "state": "unconfigured",
                        "hint": "Bank App is running but not connected to the database.",
                        "action": "Run the OLTP pipeline (Usecase 1 — Start Service) to seed data and connect.",
                    }
                except Exception:
                    return {"state": "ok"}

            return {"state": "ok"}
    except Exception as e:
        return {"state": "down", "hint": f"Probe failed: {e}"}


@app.get("/api/metabase-public-url")
async def metabase_public_url():
    """Return the public (no-login) dashboard URL for Metabase iframe embedding.

    When BFSI is deployed to NF, swap the laptop URL for the NF public URL of
    the metabase service. The /public/dashboard/<uuid> route returns headers
    that allow iframe embedding, while the regular Metabase UI does not — so
    even on NF, the workspace tab can still embed the dashboard inline.
    """
    import time, httpx
    # Default to laptop URL; override with NF URL if BFSI is NF-deployed.
    mb_url = "http://127.0.0.1:3002"
    for stack_name, dep in (agent.nf_deployments or {}).items():
        for svc in dep.get("services", []):
            if "metabase" not in svc.get("original_name", "").lower():
                continue
            urls = svc.get("urls", {}) or {}
            # Metabase listens on internal port 3000 (compose maps 3002:3000).
            nf_url = urls.get(3000) or next(iter(urls.values()), None)
            if nf_url:
                mb_url = nf_url.rstrip("/")
                break
        if mb_url.startswith("https://"):
            break
    # Cache success for 1 hour; cache failure for 15s to avoid repeat 10s timeouts
    now = time.time()
    if _mb_public_url_cache["url"] and now - _mb_public_url_cache["ts"] < 3600:
        return {"url": _mb_public_url_cache["url"]}
    if _mb_public_url_cache.get("err_ts") and now - _mb_public_url_cache["err_ts"] < 15:
        return {"url": None, "error": _mb_public_url_cache.get("err", "unavailable")}
    def _cache_err(msg):
        _mb_public_url_cache["err"] = msg
        _mb_public_url_cache["err_ts"] = time.time()
        return {"url": None, "error": msg}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(f"{mb_url}/api/session",
                json={"username": "admin@corebanking.local", "password": "CoreBank1!"})
            if r.status_code != 200:
                return _cache_err(f"Metabase login failed ({r.status_code})")
            session_id = r.json().get("id", "")
            headers = {"X-Metabase-Session": session_id}
            r2 = await client.get(f"{mb_url}/api/dashboard", headers=headers)
            if r2.status_code != 200:
                return _cache_err("Cannot list dashboards")
            # Prefer the full OLAP dashboard if it exists; fall back to OLTP Quick View.
            preferred = ["Core Banking Fraud Detection", "BFSI: OLTP Quick View"]
            dashes = r2.json()
            dash_id = None
            for want in preferred:
                for d in dashes:
                    if d.get("name") == want:
                        dash_id = d["id"]
                        break
                if dash_id:
                    break
            if not dash_id:
                # Last resort: fuzzy match anything we may own
                for d in dashes:
                    n = d.get("name", "")
                    if "Core Banking" in n or n.startswith("BFSI:"):
                        dash_id = d["id"]
                        break
            if not dash_id:
                return _cache_err("No BFSI dashboard found — run Usecase 1 (OLTP) first")
            r3 = await client.get(f"{mb_url}/api/dashboard/{dash_id}", headers=headers)
            pub_uuid = None
            if r3.status_code == 200:
                pub_uuid = r3.json().get("public_uuid")
            if not pub_uuid:
                r4 = await client.post(f"{mb_url}/api/dashboard/{dash_id}/public_link", headers=headers)
                if r4.status_code == 200:
                    pub_uuid = r4.json().get("uuid")
            if pub_uuid:
                public_url = f"{mb_url}/public/dashboard/{pub_uuid}"
                _mb_public_url_cache["url"] = public_url
                _mb_public_url_cache["ts"] = time.time()
                return {"url": public_url}
            return _cache_err("Could not create public link")
    except Exception as e:
        return _cache_err(str(e))


# dashboard name -> (public_url, expires_ts). 1-hour TTL avoids repeating
# the 3-round-trip Metabase lookup on every click — that flow was costing
# ~1-2s per redirect when the dashboard already had a public link.
_mb_dashboard_url_cache: dict = {}


@app.post("/api/metabase-dashboard-cache/invalidate")
async def metabase_dashboard_cache_invalidate(name: str = ""):
    """Clear cached public URL — call after rebuilding a dashboard so the
    next click resolves the new public_uuid instead of the stale one."""
    if name:
        _mb_dashboard_url_cache.pop(name, None)
    else:
        _mb_dashboard_url_cache.clear()
    return {"ok": True, "cleared": name or "all"}


@app.get("/api/metabase-dashboard-redirect")
async def metabase_dashboard_redirect(name: str = ""):
    """Resolve a Metabase dashboard by name and 302 to its public URL.
    Lets stack.yaml `links:` entries point at a stable URL even though
    the underlying public_uuid is dynamic and only known after the
    dashboard is built. Returns a friendly HTML page if the dashboard
    doesn't exist yet so the iframe shows a clear "build it first" hint
    instead of a raw error.
    """
    from fastapi.responses import RedirectResponse, HTMLResponse
    import httpx, time
    if not name:
        return HTMLResponse("<p>Missing <code>?name=</code> parameter.</p>", status_code=400)
    # Cache hit — skip the 3-round-trip Metabase lookup entirely.
    cached = _mb_dashboard_url_cache.get(name)
    if cached and cached[1] > time.time():
        return RedirectResponse(cached[0], status_code=302)
    mb_url = "http://127.0.0.1:3002"
    not_built_html = (
        "<div style='font-family:system-ui;padding:32px;max-width:560px;color:#0f172a'>"
        f"<h2 style='margin:0 0 12px'>Dashboard not built yet</h2>"
        f"<p>The Metabase dashboard <b>{name}</b> hasn't been created yet.</p>"
        "<p>Go to <b>Usecase 2: OLAP</b> in the Workspace and click "
        "<b>Build Hybrid Search Demo</b>. The button rerun-safe; takes ~1–2 min.</p>"
        "</div>"
    )
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(f"{mb_url}/api/session",
                json={"username": "admin@corebanking.local", "password": "CoreBank1!"})
            if r.status_code != 200:
                return HTMLResponse(not_built_html, status_code=200)
            headers = {"X-Metabase-Session": r.json().get("id", "")}
            r2 = await client.get(f"{mb_url}/api/dashboard", headers=headers)
            if r2.status_code != 200:
                return HTMLResponse(not_built_html, status_code=200)
            dash_id = next((d["id"] for d in r2.json() if d.get("name") == name), None)
            if not dash_id:
                return HTMLResponse(not_built_html, status_code=200)
            r3 = await client.get(f"{mb_url}/api/dashboard/{dash_id}", headers=headers)
            pub_uuid = r3.json().get("public_uuid") if r3.status_code == 200 else None
            if not pub_uuid:
                r4 = await client.post(f"{mb_url}/api/dashboard/{dash_id}/public_link", headers=headers)
                pub_uuid = r4.json().get("uuid") if r4.status_code == 200 else None
            if not pub_uuid:
                return HTMLResponse(not_built_html, status_code=200)
            public_url = f"{mb_url}/public/dashboard/{pub_uuid}"
            _mb_dashboard_url_cache[name] = (public_url, time.time() + 3600)
            return RedirectResponse(public_url, status_code=302)
    except Exception:
        return HTMLResponse(not_built_html, status_code=200)


# ─── Terminal / Toolbox Endpoints ───────────────────────────────────

@app.get("/api/terminal/commands/{stack_name}")
async def api_terminal_commands(stack_name: str):
    """Return toolbox-ready commands for a running stack.

    Terminals run `docker exec` into a local toolbox container. On NF-deployed
    stacks there is no local container — services live in EKS pods reachable
    only via the NF Console's exec UI. Short-circuit and tell the UI to render
    a "use NF Console" hint instead of an empty terminal list.
    """
    if stack_name in (agent.nf_deployments or {}):
        return {
            "stack": stack_name,
            "commands": [],
            "unavailable": True,
            "reason": "northflank",
            "hint": "Terminals here run docker exec on local containers. This stack's pods are running on your AWS EKS (BYOC) — open NF Console and use its in-browser pod shell, which brokers an exec session through to the pod.",
            "console_url": agent.get_nf_console_url(),
        }
    commands = agent.get_terminal_commands(stack_name)
    return {"stack": stack_name, "commands": commands}


@app.get("/api/containers/{stack_name}")
async def api_containers(stack_name: str):
    """List running containers for a stack — used by the Logs tab sidebar.
    Accepts either the agent.stacks key (folder name) OR the docker-compose
    project name (the `name:` field, which can differ — e.g. BFSI's folder
    is bfsi-fraud-detection but its compose name is bfsi-fraud-detection).
    """
    folder = stack_name if stack_name in agent.stacks else None
    if not folder:
        for k, m in agent.stacks.items():
            cpath = os.path.join(m.get("_path", ""), "docker-compose.yaml")
            if not os.path.exists(cpath):
                continue
            try:
                with open(cpath) as f:
                    c = yaml.safe_load(f) or {}
                if c.get("name") == stack_name:
                    folder = k
                    break
            except Exception:
                continue
    if not folder:
        return {"containers": [], "error": f"Unknown stack: {stack_name}"}
    container_to_svc = agent._get_container_to_service_map(folder)
    if not container_to_svc:
        return {"containers": []}
    # List ALL running containers, intersect Python-side. Avoids the
    # docker --filter regex quirks across Docker versions/distros that made
    # the pipe-joined name regex unreliable.
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}|{{.Status}}"],
            capture_output=True, text=True, timeout=5,
        )
        running = {}
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                n, s = line.split("|", 1)
                if n in container_to_svc:
                    running[n] = s
    except Exception as e:
        return {"containers": [], "error": str(e)}
    containers = []
    for cname, svc in sorted(container_to_svc.items()):
        if cname in running:
            containers.append({"name": cname, "service": svc, "status": running[cname]})
    return {"containers": containers}


_log_streams_lock = threading.Lock()
_active_log_subprocs: dict = {}  # ws_id -> subprocess


@app.websocket("/ws/logs")
async def ws_logs(websocket: WebSocket):
    """Stream `docker logs -f` for one container to a WebSocket.
    Client sends a start frame: {"type":"start","container":"bfsi-pgd","tail":500}.
    Validates the container belongs to a known stack (no arbitrary host access).
    Subprocess is killed on disconnect."""
    import asyncio
    await websocket.accept()
    proc = None
    ws_id = id(websocket)
    try:
        start = await websocket.receive_json()
        if not _api_key_ok(start.get("key", "")):
            await websocket.send_json({"type": "error", "message": "Invalid or missing API key"})
            await websocket.close(code=1008)
            return
        if start.get("type") != "start":
            await websocket.send_json({"type": "error", "message": "Expected start frame"})
            return
        container = (start.get("container") or "").strip()
        tail = int(start.get("tail", 500))
        if not container:
            await websocket.send_json({"type": "error", "message": "Missing container"})
            return
        # Validate: container must belong to some known stack.
        valid = False
        for sn in agent.stacks:
            if container in agent._get_container_to_service_map(sn):
                valid = True
                break
        if not valid:
            await websocket.send_json({"type": "error",
                "message": f"Container '{container}' not in any known stack"})
            return
        await websocket.send_json({"type": "started", "container": container})
        proc = await asyncio.create_subprocess_exec(
            "docker", "logs", "-f", "--tail", str(tail), container,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        with _log_streams_lock:
            _active_log_subprocs[ws_id] = proc
        # Stream until either side closes.
        async def pump():
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                try:
                    await websocket.send_text(line.decode("utf-8", errors="replace"))
                except Exception:
                    break
        pump_task = asyncio.create_task(pump())
        # Detect client-side close.
        async def drain_recv():
            while True:
                try:
                    await websocket.receive_text()
                except WebSocketDisconnect:
                    break
                except Exception:
                    break
        recv_task = asyncio.create_task(drain_recv())
        done, pending = await asyncio.wait(
            [pump_task, recv_task], return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        with _log_streams_lock:
            _active_log_subprocs.pop(ws_id, None)
        if proc and proc.returncode is None:
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass


@app.websocket("/ws/terminal")
async def ws_terminal(websocket: WebSocket):
    """WebSocket terminal: PTY-based docker exec into the toolbox container."""
    import asyncio
    import pty
    import struct
    import fcntl
    import termios

    await websocket.accept()
    master_fd = None
    proc = None
    loop = asyncio.get_event_loop()

    try:
        start_msg = await websocket.receive_json()
        if not _api_key_ok(start_msg.get("key", "")):
            await websocket.send_json({"type": "error", "message": "Invalid or missing API key"})
            await websocket.close(code=1008)
            return
        if start_msg.get("type") != "start":
            await websocket.send_json({"type": "error", "message": "Expected start message"})
            return

        stack_name = start_msg.get("stack", "")
        command = start_msg.get("command", "bash")
        target_container = start_msg.get("target_container", "")
        cols = start_msg.get("cols", 120)
        rows = start_msg.get("rows", 40)

        if not stack_name:
            await websocket.send_json({"type": "error", "message": "Missing stack name"})
            return

        # target_container: exec directly into a stack container (e.g. airflow-
        # webserver for the airflow CLI). Otherwise use the toolbox path.
        if target_container:
            container_name = target_container
        else:
            container_name, err = await loop.run_in_executor(None, agent._ensure_toolbox, stack_name)
            if err:
                await websocket.send_json({"type": "error", "message": err})
                return

        await websocket.send_json({"type": "started", "container": container_name})

        master_fd, slave_fd = pty.openpty()
        winsize = struct.pack("HHHH", rows, cols, 0, 0)
        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

        # When using a target_container, the user's "command" is already a full
        # shell intent (e.g. "bash"); we run it as the container's own shell.
        # When going through the toolbox, we wrap with bash -c so the toolbox
        # rewriter's strings (psql, clickhouse-client, etc.) execute correctly.
        exec_argv = (["docker", "exec", "-it", container_name] + command.split()) if target_container \
            else ["docker", "exec", "-it", container_name, "bash", "-c", command]
        proc = await asyncio.create_subprocess_exec(
            *exec_argv,
            stdin=slave_fd, stdout=slave_fd, stderr=slave_fd
        )
        os.close(slave_fd)

        # Read PTY → WebSocket (raw text frames, 16KB buffer)
        async def read_pty():
            while True:
                try:
                    data = await loop.run_in_executor(None, lambda: os.read(master_fd, 16384))
                    if not data:
                        break
                    await websocket.send_text('\x01' + data.decode("utf-8", errors="replace"))
                except OSError:
                    break
                except Exception:
                    break

        # WebSocket → PTY
        async def write_pty():
            while True:
                try:
                    msg = await websocket.receive_json()
                    if msg.get("type") == "input":
                        os.write(master_fd, msg.get("data", "").encode("utf-8"))
                    elif msg.get("type") == "resize":
                        r = msg.get("rows", rows)
                        c = msg.get("cols", cols)
                        ws2 = struct.pack("HHHH", r, c, 0, 0)
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, ws2)
                except WebSocketDisconnect:
                    break
                except Exception:
                    break

        read_task = asyncio.create_task(read_pty())
        write_task = asyncio.create_task(write_pty())

        done, pending = await asyncio.wait(
            [read_task, write_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

        if proc and proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3)
            except Exception:
                proc.kill()

        exit_code = proc.returncode if proc else -1
        try:
            await websocket.send_json({"type": "exited", "code": exit_code})
        except Exception:
            pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("[WS] Terminal error: %s", str(e))
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        if master_fd is not None:
            try:
                os.close(master_fd)
            except OSError:
                pass
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass


@app.get("/", response_class=HTMLResponse)
async def index():
    return CHAT_HTML


@app.get("/pipelines", response_class=HTMLResponse)
async def pipelines_page():
    return PIPELINES_HTML


@app.get("/monitoring", response_class=HTMLResponse)
async def monitoring_page():
    return MONITORING_HTML


# ─── Chat UI HTML (tab layout) ─────────────────────────────────


CHAT_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EDB Postgres® AI Blueprints v0.1rc8</title>
<style>
:root{--bg:#f5f6f8;--bg2:#fff;--bg3:#f8f9fb;--text:#1a1a2e;--text2:#333;--text3:#666;--muted:#888;--border:#e2e4e8;--border2:#eee;--accent:#4a90d9;--accent2:#3a7bc8;--header-bg:#1a1a2e;--header-text:#fff;--card-bg:#fff;--card-hover:#f0f8ff;--code-bg:#f0f2f5;--code-border:#e8eaed;--success:#28a745;--danger:#dc3545;--warning:#f0ad4e;--chat-user-bg:#e8f0fe;--chat-user-text:#1a1a2e;--chat-bot-bg:#fff;--chat-bot-text:#333;--input-bg:#fff;--input-border:#e2e4e8;--shadow:0 1px 3px rgba(0,0,0,.08)}
[data-theme="dark"]{--bg:#1e1e1e;--bg2:#252526;--bg3:#2d2d2d;--text:#d4d4d4;--text2:#cccccc;--text3:#969696;--muted:#858585;--border:#3e3e3e;--border2:#333333;--accent:#569cd6;--accent2:#9cdcfe;--header-bg:#1a1a1a;--header-text:#d4d4d4;--card-bg:#2d2d2d;--card-hover:#333333;--code-bg:#1e1e1e;--code-border:#3e3e3e;--success:#6a9955;--danger:#f14c4c;--warning:#cca700;--chat-user-bg:#264f78;--chat-user-text:#ffffff;--chat-bot-bg:#2d2d2d;--chat-bot-text:#d4d4d4;--input-bg:#3c3c3c;--input-border:#3e3e3e;--shadow:0 2px 6px rgba(0,0,0,.35)}
*{margin:0;padding:0;box-sizing:border-box}
html{font-size:14px}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);height:100vh;display:flex;flex-direction:column;color:var(--text);font-size:14px}

/* Header */
.hdr{background:var(--header-bg);color:var(--header-text);padding:8px 20px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.hdr-left{display:flex;align-items:baseline;gap:10px}
.hdr h1{font-size:17px;font-weight:600;letter-spacing:-.3px}
.hdr .ver{font-size:11px;opacity:.5}
.hdr .running-bar{display:flex;align-items:center;gap:6px;font-size:13px;opacity:.85}
.hdr .running-bar .dot{width:7px;height:7px;border-radius:50%;background:var(--success);animation:pulse 2s infinite}
.hdr .running-bar .slot{font-size:11px;opacity:.6;margin-left:4px}
.hdr .nav{display:flex;gap:5px;align-items:center}
.hdr .nav button{background:rgba(255,255,255,.1);color:var(--header-text);border:none;padding:5px 12px;border-radius:4px;cursor:pointer;font-size:12px;transition:background .15s}
.hdr .nav button:hover{background:rgba(255,255,255,.2)}
.hdr .nav .exit-btn{background:rgba(220,53,69,.6)}
.hdr .nav .exit-btn:hover{background:rgba(220,53,69,.8)}
.theme-toggle{background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.2);color:var(--header-text);padding:4px 10px;border-radius:14px;cursor:pointer;font-size:13px;transition:all .15s;display:flex;align-items:center;gap:4px}
.theme-toggle:hover{background:rgba(255,255,255,.22)}

/* Tab bar */
.tab-bar{display:flex;background:var(--bg2);border-bottom:2px solid var(--border);flex-shrink:0;padding:0 20px}
.tab-bar .tab{padding:10px 20px;font-size:14px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .15s;user-select:none}
.tab-bar .tab:hover{color:var(--text);background:var(--bg3)}
.tab-bar .tab.active{color:var(--text);border-bottom-color:var(--accent)}
.tab-bar .tab .badge{display:inline-block;background:var(--code-bg);color:var(--muted);font-size:10px;font-weight:700;padding:1px 6px;border-radius:8px;margin-left:5px;vertical-align:middle}
.tab-bar .tab.active .badge{background:var(--accent);color:#fff}

/* Main split: content + chat */
.main{flex:1;display:flex;overflow:hidden}
.content-panel{width:40%;display:flex;flex-direction:column;overflow:hidden;background:var(--bg);padding-bottom:0}
.content-scroll{flex:1;overflow-y:auto;padding:20px}
.chat-panel{width:60%;display:flex;flex-direction:column;background:var(--bg2);border-left:1px solid var(--border)}

/* Tab content */
.tab-content{display:none}
.tab-content.active{display:block}

/* ── Stacks tab ── */
.stack-list{display:flex;flex-direction:column;gap:6px}
.stack-group{margin-bottom:8px}
.sg-header{display:flex;align-items:center;gap:6px;padding:6px 14px;cursor:pointer;font-size:14px;font-weight:600;color:#555;user-select:none}
.sg-header:hover{color:#333}
.sg-arrow{font-size:10px;transition:transform .2s;display:inline-block;width:12px}
.stack-group.collapsed .sg-arrow{transform:rotate(-90deg)}
.stack-group.collapsed .sg-items{display:none}
.sg-count{color:#999;font-weight:400}
.sg-items{padding:0 0 4px}
.stack-row{display:flex;align-items:center;gap:10px;background:#fff;border:1px solid #e2e4e8;border-left:3px solid #4a90d9;border-radius:6px;padding:10px 14px;cursor:pointer;transition:all .15s}
.stack-row:hover{box-shadow:0 1px 4px rgba(0,0,0,.06);border-left-color:#3a7bc8}
.stack-row .sr-name{font-size:14px;font-weight:600;color:#1a1a2e;min-width:120px;flex:1}
.stack-row .sr-tags{display:flex;flex-wrap:wrap;gap:3px;flex:1}
.stack-row .sr-tag{font-size:12px;background:#f0f4ff;color:#4a90d9;padding:2px 6px;border-radius:3px;font-weight:500}
.stack-row .sr-status{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.3px;padding:3px 8px;border-radius:4px;flex-shrink:0}
.stack-row .sr-status.stopped{background:#f0f0f0;color:#999}
.stack-row .sr-status.running{background:#d4edda;color:#1a7a3a}
.stack-row .sr-status.deploying{background:#fff3cd;color:#856404;animation:pulse 1.5s infinite}
.stack-row .sr-status.limit{background:#f0f0f0;color:#bbb}
.stack-row.is-running{border-color:#28a745;border-left-color:#28a745;background:#f8fdf9}
.stack-row.is-running .sr-name{color:#1a7a3a}
.stack-row.is-deploying{border-color:#f0ad4e;background:#fffdf5}
.stack-row.is-disabled{opacity:.4;cursor:not-allowed;pointer-events:none}
.stack-row .sr-delete{background:none;border:1px solid #ddd;color:#999;padding:2px 8px;border-radius:3px;cursor:pointer;font-size:10px;flex-shrink:0;transition:all .15s}
.stack-row .sr-delete:hover{background:#fff5f5;border-color:#dc3545;color:#dc3545}
/* ── Industry tab — category headers ── */
.ind-section{margin-bottom:24px}
.ind-cat-hdr{margin-bottom:10px;padding:8px 14px;border-radius:6px;display:flex;align-items:baseline;justify-content:space-between}
.ind-cat-hdr.cat-ml{background:#eff6ff}
.ind-cat-hdr.cat-sa{background:#f5f3ff}
.ind-cat-hdr.cat-sl{background:#ecfdf5}
.ind-cat-title{font-size:15px;font-weight:700;line-height:1.4}
.cat-ml .ind-cat-title{color:#1d4ed8}
.cat-sa .ind-cat-title{color:#6d28d9}
.cat-sl .ind-cat-title{color:#047857}
.ind-cat-count{font-size:12px;color:#9ca3af;flex-shrink:0}
/* ── Industry cards ── */
.ind-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:8px}
.ind-card{background:#fff;border:1px solid #e5e7eb;border-radius:6px;padding:10px 12px;cursor:pointer;transition:all .15s;display:flex;flex-direction:column;gap:8px;border-left:3px solid var(--cat-color,#3b82f6)}
.ind-card:hover{border-color:var(--cat-color,#4a90d9);box-shadow:0 1px 6px rgba(0,0,0,.05)}
.ind-card.placeholder{opacity:.7;cursor:default;border-left-color:#d1d5db}
.ind-card.placeholder:hover{border-color:#e5e7eb;border-left-color:#d1d5db;box-shadow:none}
.ind-card.placeholder .ind-deploy-btn{display:none}
.ind-card-title{font-size:13px;font-weight:600;color:var(--cat-color,#1d4ed8);line-height:1.3;min-width:0}
.ind-card.placeholder .ind-card-title{color:#6b7280}
.ind-card-foot{display:flex;align-items:center;gap:6px}
.ind-card-foot .sr-status{font-size:9px;font-weight:500;text-transform:uppercase;letter-spacing:.3px;padding:2px 6px;border-radius:3px}
.ind-card-foot .sr-status.stopped{background:#f3f4f6;color:#9ca3af}
.ind-card-foot .sr-status.running{background:#d1fae5;color:#065f46}
.ind-card-foot .sr-status.deploying{background:#fef3c7;color:#92400e;animation:pulse 1.5s infinite}
.ind-card-foot .sr-status.soon{color:#9ca3af;font-size:9px;font-style:italic;letter-spacing:0;text-transform:none;font-weight:400}
.ind-card-action{display:block;width:100%}
.ind-card.placeholder .ind-card-action{display:none}
.ind-deploy-btn{background:var(--cat-color,#3b82f6);color:#fff;border:none;padding:4px 12px;border-radius:4px;font-size:10px;font-weight:600;cursor:pointer;transition:all .12s}
.ind-deploy-btn:hover:not(:disabled){filter:brightness(.9)}
.ind-deploy-btn:disabled{background:#94a3b8;color:#e2e8f0;cursor:not-allowed;opacity:.6}
.ind-deploy-btn:disabled:hover{filter:none}
.ind-card-action .ind-deploy-btn{width:100%;padding:8px 12px;font-size:12px;letter-spacing:.3px}
.ind-card.is-running{border-color:#059669;border-left-color:#059669;background:#f0fdf4}
.ind-card.is-running .ind-card-title{color:#065f46}
.ind-card.is-running .ind-card-action .ind-deploy-btn{background:#dc2626}
.ind-card.is-running .ind-card-action .ind-deploy-btn:hover{background:#b91c1c}
.ind-card.is-deploying{border-color:#d97706;border-left-color:#d97706;background:#fffbeb;cursor:not-allowed;pointer-events:none}
.ind-card.is-deploying .ind-card-action .ind-deploy-btn{background:#d97706;cursor:not-allowed;opacity:.85}
.ind-card.is-deploying .ind-card-action .ind-deploy-btn:hover{filter:none}
.ind-card.is-stopping{border-color:#dc2626;border-left-color:#dc2626;background:#fef2f2;cursor:not-allowed;pointer-events:none}
.ind-card.is-stopping .ind-card-action{display:block}
.ind-card.is-stopping .ind-card-action .ind-deploy-btn{background:#dc2626;cursor:not-allowed;opacity:.85}
.ind-card.is-stopping .sr-status{background:#fee2e2;color:#991b1b;animation:pulse 1.5s infinite}
.ind-card.is-disabled{opacity:.4;cursor:not-allowed;pointer-events:none}
.limit-msg{margin-top:12px;padding:10px 14px;background:#fff8e6;border:1px solid #f0e0a0;border-radius:6px;font-size:12px;color:#8a6d3b;display:none;text-align:center}
.limit-msg.show{display:block}
.build-hint{margin-top:14px;padding:12px 14px;background:#fff;border:1px solid #e8eaed;border-radius:6px;font-size:12px;line-height:1.5;color:#666}
.build-hint a{color:#4a90d9;cursor:pointer;text-decoration:none}
.build-hint a:hover{text-decoration:underline}

/* ── Plugins tab ── */
.plugin-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}
.plugin-card{background:#fff;border:1px solid #e2e4e8;border-radius:8px;padding:14px;text-align:center;transition:all .15s;user-select:none}
.plugin-card .pc-check{width:16px;height:16px;border:2px solid #ccc;border-radius:3px;display:none;margin:0 auto 6px;transition:all .15s}
.plugin-card .pc-name{font-size:14px;font-weight:600;color:#1a1a2e;margin-bottom:4px}
.plugin-card .pc-image{font-size:10px;color:#888;font-family:'SF Mono',Monaco,Consolas,monospace}
/* Build mode */
.plugins-build-mode .plugin-card{cursor:pointer}
.plugins-build-mode .plugin-card:hover{border-color:#4a90d9;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.plugins-build-mode .plugin-card .pc-check{display:inline-block}
.plugins-build-mode .plugin-card.selected{border-color:#4a90d9;background:#f0f7ff}
.plugins-build-mode .plugin-card.selected .pc-check{background:#4a90d9;border-color:#4a90d9;position:relative}
.plugins-build-mode .plugin-card.selected .pc-check::after{content:'\2713';color:#fff;font-size:11px;position:absolute;top:-2px;left:2px}
.build-bar{margin-top:14px;padding:12px 14px;background:#fff;border:1px solid #e2e4e8;border-radius:6px;display:none;align-items:center;justify-content:space-between;gap:10px}
.plugins-build-mode .build-bar{display:flex}
.build-bar .bb-selected{font-size:12px;color:#666}
.build-bar .bb-selected strong{color:#1a1a2e}
.build-bar .bb-name{padding:6px 10px;border:1px solid #ddd;border-radius:4px;font-size:12px;width:180px;outline:none}
.build-bar .bb-name:focus{border-color:#4a90d9}
.build-bar .bb-btn{background:#4a90d9;color:#fff;border:none;padding:7px 16px;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;transition:background .15s}
.build-bar .bb-btn:hover{background:#3a7bc8}
.build-bar .bb-btn:disabled{opacity:.4;cursor:not-allowed}
.build-mode-toggle{margin-bottom:12px;padding:8px 14px;background:#fff;border:1px solid #e2e4e8;border-radius:6px;display:flex;align-items:center;justify-content:space-between}
.build-mode-toggle span{font-size:12px;color:#666}
.build-mode-toggle button{background:#4a90d9;color:#fff;border:none;padding:5px 14px;border-radius:4px;cursor:pointer;font-size:11px;font-weight:600}
.build-mode-toggle button:hover{background:#3a7bc8}
.build-mode-toggle button.cancel{background:#f0f2f5;color:#555;border:1px solid #ddd}
.build-mode-toggle button.cancel:hover{background:#e8eaed}

/* ── Pipelines tab ── */
.pipe-section{background:#fff;border:1px solid #e2e4e8;border-radius:8px;margin-bottom:14px;overflow:hidden}
.pipe-section .ps-stack{font-size:13px;color:#4a90d9;font-weight:600;text-transform:uppercase;letter-spacing:.4px;padding:10px 16px;background:#f8f9fb;border-bottom:1px solid #eee}
.pipe-case-hdr{font-size:12px;font-weight:700;color:var(--text);margin:0 0 8px;padding:4px 0;border-bottom:1px solid var(--border2)}
.pipe-header{font-size:14px;color:#1a1a2e;font-weight:600;padding:12px 16px;cursor:pointer;display:flex;align-items:center;gap:6px;border-bottom:1px solid #f0f0f0;transition:background .1s}
.pipe-header:hover{background:#f8f9fb}
.pipe-header .p-arrow{font-size:10px;transition:transform .15s;display:inline-block;color:#999}
.pipe-header .p-arrow.open{transform:rotate(90deg)}
.pipe-header .p-count{margin-left:auto;font-size:11px;color:#999;font-weight:400}
.pipe-steps{display:none}
/* Quick-access link bar under a pipeline header — declarative from stack.yaml `links:` field */
.pipe-links{display:flex;flex-wrap:wrap;gap:6px;padding:8px 16px;background:#fafbfd;border-bottom:1px solid #f0f0f0}
.pipe-link-btn{display:inline-flex;align-items:center;gap:5px;padding:5px 10px;background:#fff;border:1px solid #e2e8f0;border-radius:14px;font-size:11px;color:#1a1a2e;cursor:pointer;transition:all .12s;font-weight:500;line-height:1}
.pipe-link-btn:hover{background:#eff6ff;border-color:#93c5fd;color:#1d4ed8}
.pipe-link-btn .pl-icon{font-size:12px}
.pipe-link-btn.pipe-link-gated{opacity:.5;cursor:not-allowed;color:#64748b;border-style:dashed}
.pipe-link-btn.pipe-link-gated:hover{background:#fff;border-color:#e2e8f0;color:#64748b}
[data-theme="dark"] .pipe-links{background:var(--bg2);border-bottom-color:var(--border)}
[data-theme="dark"] .pipe-link-btn{background:var(--bg3);border-color:var(--border);color:var(--text)}
[data-theme="dark"] .pipe-link-btn:hover{background:var(--bg);border-color:#3b82f6}
[data-theme="dark"] .pipe-link-btn.pipe-link-gated:hover{background:var(--bg3);border-color:var(--border);color:var(--text)}

/* ════════════════════════════════════════════════════════════════════
   Workspace Home Redesign (2026-04-29) — 2-col Dashboard | Use Cases
   + full-width Recent Activity, right vertical removed.
   ════════════════════════════════════════════════════════════════════ */
/* Hide the legacy right-vertical panel (Industry/Monitoring/Chat).
   Kept in DOM so functions that reference its IDs don't error. */
.ws-chat{display:none !important}
.ws-chat-resize{display:none !important}

/* Synthetic Data card — top of home, full width. Wraps the live readout
   pills + Start/Stop button + active pipeline label inside a titled card. */
.ws-synth-card{background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:8px;overflow:hidden;flex:0 0 auto}
.ws-synth-card-h{display:flex;align-items:center;gap:10px;padding:10px 14px;background:linear-gradient(90deg,rgba(99,102,241,0.08),rgba(99,102,241,0.02));border-bottom:1px solid var(--border)}
.ws-synth-card-h .ws-synth-icon{font-size:18px}
.ws-synth-card-h .ws-synth-title{font-size:14px;font-weight:700;color:var(--text)}
.ws-synth-card-h .ws-synth-sub{font-size:12px;color:var(--muted,#6b7280);font-weight:400;margin-left:6px}
.ws-synth-card-h .ws-synth-pipe{font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px;margin-left:auto;background:var(--bg2,#f1f5f9);padding:3px 9px;border-radius:9px}
.ws-live-strip{display:flex;align-items:center;gap:14px;padding:12px 14px;flex-wrap:wrap}
[data-theme="dark"] .ws-synth-card{background:var(--bg3);border-color:var(--border)}
[data-theme="dark"] .ws-synth-card-h{background:linear-gradient(90deg,rgba(99,102,241,0.14),rgba(99,102,241,0.03));border-bottom-color:var(--border)}
[data-theme="dark"] .ws-synth-card-h .ws-synth-pipe{background:var(--bg2)}

/* 2-column grid: left = Synthetic + Dashboard stacked, right = Use Cases.
   Default grid stretch so the right column fills the same height as the
   stacked left column — Use Cases gets the extra vertical formerly burned
   by the full-width Synthetic banner. */
.ws-home-2col{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;flex:0 0 auto}
@media (max-width: 1100px){ .ws-home-2col{grid-template-columns:1fr} }
/* Left column stacks Synthetic Data (top) above Dashboard. */
.ws-home-left{display:flex;flex-direction:column;gap:12px;min-width:0}
/* Both 2-col children fill the grid cell vertically. */
.ws-home-2col > #wsUseCasesHost{display:flex;flex-direction:column;min-width:0}
.ws-home-2col > #wsUseCasesHost > .ws-pane{flex:1 1 auto;min-height:0}
/* Activity host fills the remaining vertical inside the flex column .ws-home. */
#wsActivityHost{flex:1 1 auto;min-height:0;display:flex;flex-direction:column}
#wsLiveStripHost{flex:0 0 auto}
.ws-pane{background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;gap:10px;min-height:0}
.ws-pane-h{display:flex;align-items:center;gap:8px;font-size:14px;font-weight:700;color:var(--text);padding-bottom:8px;border-bottom:1px solid var(--border,#f0f0f0);margin-bottom:2px}
.ws-pane-h .ws-pane-icon{font-size:16px}
[data-theme="dark"] .ws-pane{background:var(--bg3);border-color:var(--border)}

/* Dashboard sub-sections — laid out as a 2x2 internal grid inside the left pane */
.ws-pane-dashboard .ws-dash-grid{display:grid;grid-template-columns:1fr 1fr;grid-template-rows:auto auto;gap:10px;flex:1 1 auto}
.ws-dash-sec{display:flex;flex-direction:column;gap:6px;padding:8px 10px;border:1px solid var(--border,#f0f0f0);border-radius:6px;background:var(--bg2,#fafbfd);min-width:0}
.ws-dash-sec-h{font-size:11px;font-weight:700;color:var(--muted,#6b7280);text-transform:uppercase;letter-spacing:.5px;display:flex;align-items:center;gap:6px;padding:0 0 4px;border-bottom:1px solid var(--border,#f0f0f0);margin-bottom:4px}
.ws-dash-sec-h .ws-section-count{margin-left:auto;background:var(--card-bg,#fff);color:var(--muted);padding:1px 7px;border-radius:9px;font-size:10px;font-weight:600;text-transform:none}
[data-theme="dark"] .ws-dash-sec{background:var(--bg2);border-color:var(--border)}
[data-theme="dark"] .ws-dash-sec-h{border-bottom-color:var(--border)}
[data-theme="dark"] .ws-dash-sec-h .ws-section-count{background:var(--bg3)}
/* Make tile icons smaller inside the cramped 2x2 cells */
.ws-dash-sec .ws-tile{padding:6px 8px;gap:8px}
.ws-dash-sec .ws-tile .ws-tile-icon{width:28px;height:28px;font-size:11px}
.ws-dash-sec .ws-tile .ws-tile-name{font-size:12px}
.ws-dash-sec .ws-tile .ws-tile-status{font-size:10px}
.ws-dash-sec .ws-tile-grid{grid-template-columns:repeat(2, minmax(0, 1fr));gap:6px}

/* Generic dropdown style for Other UIs + Terminals (matches Credentials style) */
.ws-list-dd-wrap{position:relative;width:100%}
.ws-list-dd-btn{width:100%;padding:7px 10px;border:1px solid var(--border);border-radius:5px;background:var(--card-bg,#fff);color:var(--text);cursor:pointer;font-size:12px;font-weight:500;display:flex;align-items:center;gap:6px;text-align:left}
.ws-list-dd-btn:hover{border-color:#3b82f6;color:#1d4ed8}
.ws-list-dd-btn.active{border-color:#3b82f6;background:rgba(59,130,246,0.06)}
.ws-list-dd-btn .ws-list-dd-count{margin-left:auto;background:var(--bg2,#f1f5f9);color:var(--muted);padding:1px 7px;border-radius:9px;font-size:10px;font-weight:600}
.ws-list-dd-btn .ws-list-dd-caret{font-size:10px;color:var(--muted)}
.ws-list-dd-panel{display:none;position:absolute;top:calc(100% + 4px);left:0;right:0;z-index:50;background:var(--card-bg,#fff);border:1px solid var(--border);border-radius:6px;box-shadow:0 6px 18px rgba(0,0,0,0.10);padding:4px;max-height:300px;overflow-y:auto}
.ws-list-dd-panel.open{display:block}
.ws-list-dd-row{display:flex;align-items:center;gap:9px;padding:6px 8px;border-radius:5px;cursor:pointer;font-size:12px}
.ws-list-dd-row:hover{background:rgba(59,130,246,0.08)}
.ws-list-dd-row .ws-list-dd-icon{width:24px;height:24px;border-radius:50%;background:var(--bg2,#f1f5f9);color:var(--muted);font-size:10px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.ws-list-dd-row .ws-list-dd-name{flex:1;min-width:0;color:var(--text);font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ws-list-dd-row .ws-list-dd-state{font-size:10px;font-weight:600;color:#10b981}
[data-theme="dark"] .ws-list-dd-btn{background:var(--bg3);border-color:var(--border);color:var(--text)}
[data-theme="dark"] .ws-list-dd-panel{background:var(--bg3);border-color:var(--border)}

/* Use case visual grid (right pane) — 3×2 connected tiles, compact & legible */
.ws-uc-grid{display:grid;grid-template-columns:repeat(3, 1fr);grid-auto-rows:1fr;gap:14px;padding:6px 0}
.ws-uc-tile{position:relative;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px;padding:9px 8px;border:2px solid var(--border,#e2e8f0);border-radius:10px;background:var(--card-bg,#fff);cursor:pointer;transition:transform .12s,border-color .12s,box-shadow .12s;text-align:center;min-height:82px}
.ws-uc-tile:hover{transform:translateY(-2px);box-shadow:0 6px 14px rgba(0,0,0,0.08)}
.ws-uc-tile .ws-uc-num{width:30px;height:30px;border-radius:50%;background:var(--bg2,#f1f5f9);color:var(--muted);font-weight:700;font-size:14px;display:flex;align-items:center;justify-content:center;border:2px solid var(--border)}
.ws-uc-tile .ws-uc-name{font-size:14px;font-weight:600;color:var(--text);line-height:1.2;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.ws-uc-tile .ws-uc-status{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;padding:2px 9px;border-radius:9px;background:var(--bg2);color:var(--muted)}
.ws-uc-tile .ws-uc-desc{display:none}
/* States */
.ws-uc-tile.completed{border-color:#10b981}
.ws-uc-tile.completed .ws-uc-num{background:#d1fae5;color:#047857;border-color:#10b981}
.ws-uc-tile.completed .ws-uc-status{background:#d1fae5;color:#047857}
.ws-uc-tile.running{border-color:#22c55e;background:#ecfdf5;animation:pulseRunning 1.4s infinite}
.ws-uc-tile.running .ws-uc-num{background:#22c55e;color:#fff;border-color:#16a34a;animation:spinNum 1.6s linear infinite}
.ws-uc-tile.running .ws-uc-status{background:#22c55e;color:#fff;font-weight:800}
.ws-uc-tile.running .ws-uc-name{color:#15803d}
@keyframes pulseRunning{0%{box-shadow:0 0 0 0 rgba(34,197,94,0.55)}70%{box-shadow:0 0 0 8px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}
@keyframes spinNum{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
[data-theme="dark"] .ws-uc-tile.running{background:rgba(34,197,94,0.10);border-color:#22c55e}
.ws-uc-tile.pending{border-color:#3b82f6}
.ws-uc-tile.pending .ws-uc-num{background:#dbeafe;color:#1e40af;border-color:#3b82f6}
.ws-uc-tile.pending .ws-uc-status{background:#dbeafe;color:#1e40af}
.ws-uc-tile.locked{cursor:not-allowed;opacity:0.45;background:var(--bg2,#f8fafc)}
.ws-uc-tile.locked .ws-uc-num{background:var(--bg2);color:var(--muted)}
.ws-uc-tile.locked .ws-uc-status{background:var(--bg2);color:var(--muted)}
.ws-uc-tile.locked:hover{transform:none;box-shadow:none}
.ws-uc-tile.failed{border-color:#dc2626;background:#fef2f2}
.ws-uc-tile.failed .ws-uc-num{background:#fee2e2;color:#991b1b;border-color:#dc2626}
.ws-uc-tile.failed .ws-uc-status{background:#dc2626;color:#fff;font-weight:800}
.ws-uc-tile.failed .ws-uc-name{color:#991b1b}
[data-theme="dark"] .ws-uc-tile.failed{background:rgba(220,38,38,0.10);border-color:#dc2626}
@keyframes pulseBorder{0%{box-shadow:0 0 0 0 rgba(245,158,11,0.4)}70%{box-shadow:0 0 0 6px rgba(245,158,11,0)}100%{box-shadow:0 0 0 0 rgba(245,158,11,0)}}
[data-theme="dark"] .ws-uc-tile{background:var(--bg3);border-color:var(--border)}
[data-theme="dark"] .ws-uc-tile.locked{background:var(--bg2)}
/* (Connector arrows removed — DAG branches don't align cleanly in a fixed 3×2 grid;
   dependency is communicated via the locked state + tooltip instead.) */

/* Inline pipeline detail (when a tile is clicked — replaces the grid in the same pane) */
.ws-uc-detail{display:flex;flex-direction:column;gap:10px}
.ws-uc-detail-h{display:flex;align-items:center;gap:10px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.ws-uc-detail-back{font-size:11px;padding:4px 10px;border:1px solid var(--border);border-radius:5px;background:var(--bg2);color:var(--text);cursor:pointer;font-weight:500}
.ws-uc-detail-back:hover{border-color:#3b82f6;color:#1d4ed8}
.ws-uc-detail-title{font-size:14px;font-weight:700;color:var(--text);flex:1;min-width:0}
.ws-uc-detail-status{font-size:11px;font-weight:700;padding:3px 9px;border-radius:10px;text-transform:uppercase;letter-spacing:.5px}
.ws-uc-detail-runall{font-size:11px;color:#fff;cursor:pointer;padding:5px 12px;border:none;border-radius:5px;background:#2563eb;font-weight:600}
.ws-uc-detail-runall:hover:not(:disabled){background:#1d4ed8}
.ws-uc-detail-runall:disabled{opacity:.6;cursor:not-allowed}
.ws-uc-detail-links{display:flex;flex-wrap:wrap;gap:5px}
.ws-uc-detail-steps{display:flex;flex-direction:column}
[data-theme="dark"] .ws-uc-detail-back{background:var(--bg2);border-color:var(--border);color:var(--text)}

/* Use Case flow panel — declarative narrative pulled from stack.yaml `flow:`.
   The first paragraph (always visible) is a one-liner overview. The numbered
   step-by-step list goes inside a <details> that's collapsed by default so
   the panel doesn't dominate the screen. Click "Show steps" to expand. */
.ws-uc-flow{margin:8px 0 12px 0;background:#eff6ff;border:1px solid #bfdbfe;border-left:4px solid #3b82f6;border-radius:6px;padding:8px 14px;font-size:12px;line-height:1.5;color:#1e3a8a}
.ws-uc-flow b{color:#1e40af;font-weight:700}
.ws-uc-flow code{background:#dbeafe;color:#1e40af;padding:1px 5px;border-radius:3px;font-size:11px}
.ws-uc-flow .ws-uc-flow-intro{margin:0}
.ws-uc-flow-details{margin-top:6px}
.ws-uc-flow-details summary{cursor:pointer;user-select:none;font-size:10px;color:#1e40af;text-transform:uppercase;letter-spacing:.5px;font-weight:700;padding:3px 0;list-style:none;display:inline-flex;align-items:center;gap:4px}
.ws-uc-flow-details summary::-webkit-details-marker{display:none}
.ws-uc-flow-details summary::before{content:'▸';display:inline-block;font-size:11px;line-height:1;transition:transform .12s}
.ws-uc-flow-details[open] summary::before{transform:rotate(90deg)}
.ws-uc-flow-details summary:hover{color:#1d4ed8}
.ws-uc-flow-details ol{margin:6px 0 2px 0;padding-left:22px}
.ws-uc-flow-details ol li{margin:3px 0}
[data-theme="dark"] .ws-uc-flow{background:rgba(59,130,246,0.10);border-color:#1e40af;color:#bfdbfe}
[data-theme="dark"] .ws-uc-flow b{color:#dbeafe}
[data-theme="dark"] .ws-uc-flow code{background:rgba(59,130,246,0.20);color:#bfdbfe}
[data-theme="dark"] .ws-uc-flow-details summary{color:#bfdbfe}
[data-theme="dark"] .ws-uc-flow-details summary:hover{color:#dbeafe}

/* Recent Activity panel */
.ws-act-panel{background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;min-height:0;flex:1 1 auto}
.ws-act-h{display:flex;align-items:center;gap:10px;font-size:14px;font-weight:700;color:var(--text);padding-bottom:8px;border-bottom:1px solid var(--border,#f0f0f0);margin-bottom:8px}
.ws-act-h .ws-act-collapse{margin-left:auto;font-size:11px;color:var(--muted);background:none;border:1px solid var(--border);padding:4px 9px;border-radius:5px;cursor:pointer}
.ws-act-h .ws-act-collapse:hover{border-color:#3b82f6;color:#3b82f6}
.ws-act-h .ws-act-clear{margin-left:6px;font-size:11px;color:var(--muted);background:none;border:1px solid var(--border);padding:4px 9px;border-radius:5px;cursor:pointer}
.ws-act-h .ws-act-clear:hover{border-color:#dc2626;color:#dc2626}
.ws-act-filters{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:8px}
.ws-act-filter{font-size:11px;padding:4px 10px;border-radius:12px;border:1px solid var(--border);background:var(--bg2,#fafbfd);color:var(--muted);cursor:pointer;font-weight:500}
.ws-act-filter:hover{border-color:#3b82f6;color:#1d4ed8}
.ws-act-filter.active{background:#3b82f6;color:#fff;border-color:#3b82f6}
[data-theme="dark"] .ws-act-panel{background:var(--bg3);border-color:var(--border)}
[data-theme="dark"] .ws-act-filter{background:var(--bg2)}
.ws-act-log{flex:1 1 auto;min-height:0;overflow-y:auto;background:var(--bg2,#f8fafc);border:1px solid var(--border,#e5e7eb);border-radius:6px;padding:10px 12px;font-size:12px}
[data-theme="dark"] .ws-act-log{background:var(--bg2)}
.ws-act-log .ws-log-item.filter-hidden{display:none}
.pipe-rt-controls{padding:10px 16px;background:#fafbfd;border-top:1px solid #f0f0f0;display:flex;flex-direction:column;gap:8px}
.pipe-rt-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.pipe-rt-btn{background:#10b981;color:#fff;border:none;padding:8px 14px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;transition:background .12s,opacity .15s;min-width:200px}
.pipe-rt-btn:hover:not(:disabled){background:#059669}
.pipe-rt-btn.running{background:#dc2626}
.pipe-rt-btn.running:hover:not(:disabled){background:#b91c1c}
.pipe-rt-btn:disabled{background:#9ca3af;cursor:not-allowed;opacity:.7}
.rtc-last-run{margin-left:10px;font-size:11px;color:#64748b;font-weight:500;white-space:nowrap}
.rtc-last-run.rtc-last-success{color:#047857}
.rtc-last-run.rtc-last-failed{color:#dc2626}
.rtc-last-run.rtc-last-running,.rtc-last-run.rtc-last-queued{color:#92400e}
.pipe-rt-desc{font-size:12px;color:var(--muted,#6b7280);flex:1;min-width:0}
.pipe-rt-hint{font-size:11px;color:#b45309;font-style:italic}
[data-theme="dark"] .pipe-rt-controls{background:var(--bg3,#1e1e1e);border-top-color:var(--border,#3a3a3a)}
.p-step{display:flex;align-items:center;gap:8px;padding:10px 16px;cursor:pointer;font-size:14px;color:#444;transition:background .1s,opacity .2s;border-bottom:1px solid #f5f5f5;border-left:4px solid transparent;position:relative}
.p-step:last-child{border-bottom:none}
.p-step:hover{background:#e8f0fe}
.p-step.done{opacity:.85;background:#f0fdf4;border-left-color:#22c55e;cursor:not-allowed;pointer-events:none}
.p-step.done.rerun{opacity:.85;cursor:pointer;pointer-events:auto;background:#f0f7ff;border-left-color:#3b82f6}
.p-step.done:hover{background:#f0fdf4}
.p-step.done.rerun:hover{background:#e0edff}
.p-step.locked{opacity:.35;cursor:not-allowed}
.p-step.locked:hover{background:transparent}
.p-step.locked .p-num{background:#d0d0d0;color:#999;font-size:8px}
.p-step.done.rerun{opacity:1;cursor:pointer;pointer-events:auto;background:#f0f7ff}
.p-step.done.rerun:hover{background:#e0edff}
.p-step.done.rerun .p-num{background:#4a90d9;color:#fff}
.p-step.failed{background:#fff5f5;border-left-color:#dc3545}
.p-step.failed .p-label{color:#dc3545}
/* Running step — highlighted prominently in green */
.p-step.running{background:#ecfdf5;border-left-color:#22c55e;animation:pulseStepRunning 1.6s infinite}
.p-step.running .p-label{color:#15803d;font-weight:600}
.p-step.running:hover{background:#dcfce7}
@keyframes pulseStepRunning{0%{box-shadow:inset 4px 0 0 0 #22c55e}50%{box-shadow:inset 4px 0 0 0 #16a34a, 0 0 0 1px rgba(34,197,94,0.3)}100%{box-shadow:inset 4px 0 0 0 #22c55e}}
.p-step .p-num{width:22px;height:22px;border-radius:50%;background:#e8eaed;color:#888;font-size:10px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;cursor:pointer}
.p-step .p-num:hover{transform:scale(1.1);box-shadow:0 0 4px rgba(0,0,0,0.2)}
.p-step .p-num.ok{background:#28a745;color:#fff}
.p-step .p-num.err{background:#dc3545;color:#fff}
.p-step .p-num.run{background:#22c55e;color:#fff;animation:spinNum 1.6s linear infinite}
/* Per-step inline status badge (Ready / Running / Done / Locked) */
.p-step-state{margin-left:auto;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;padding:3px 9px;border-radius:10px;background:#e8eaed;color:#6b7280;flex-shrink:0}
.p-step.done .p-step-state{background:#d1fae5;color:#047857}
.p-step.done.rerun .p-step-state{background:#dbeafe;color:#1e40af}
.p-step.running .p-step-state{background:#22c55e;color:#fff;animation:pulse 1.4s infinite}
.p-step.failed .p-step-state{background:#fee2e2;color:#dc2626}
.p-step.locked .p-step-state{background:#f3f4f6;color:#9ca3af}
[data-theme="dark"] .p-step-state{background:var(--bg2);color:var(--muted)}
.p-step .p-label{flex:1;line-height:1.3}
.p-step-output{padding:0 16px 6px 46px;font-size:11px;background:transparent}
.p-step-output pre{display:none}
.p-step-output .step-card,.p-step-output .step-links,.p-step-output .sc-raw{display:none}
.p-step-output .step-jump:hover{color:#3a7bc8 !important}
.pipe-next-hint{display:flex;gap:8px;align-items:flex-start;padding:10px 16px;margin:6px 12px 8px;background:#ecfdf5;border:1px solid #a7f3d0;border-radius:6px;color:#065f46;font-size:12px;line-height:1.5}
.pipe-next-icon{font-size:16px;flex-shrink:0;color:#10b981}
.pipe-next-text{flex:1}
[data-theme="dark"] .pipe-next-hint{background:#0f2d22;border-color:#10b981;color:#a7f3d0}
.step-card{margin:6px 0;padding:8px 12px;background:#fff;border:1px solid #e2e4e8;border-radius:6px}
.sc-stage{font-weight:600;font-size:12px;color:#4a90d9;margin:6px 0 4px;letter-spacing:.2px}
.sc-stage:first-child{margin-top:2px}
.sc-tag{font-size:10px;font-weight:500;color:#888;background:#eef2f7;padding:1px 5px;border-radius:3px;margin-right:4px;font-family:'SF Mono',Monaco,Consolas,monospace}
.sc-checks{list-style:none;padding:0;margin:0 0 6px 0}
.sc-checks li{font-size:12px;padding:2px 0;color:#333;line-height:1.5;display:flex;gap:6px;align-items:flex-start}
.sc-mark{display:inline-block;width:14px;font-weight:700;text-align:center;flex-shrink:0}
.sc-ok .sc-mark{color:#28a745}
.sc-err{color:#a01919}
.sc-err .sc-mark{color:#dc3545}
.sc-raw{margin-top:4px;font-size:11px;color:#666}
.sc-raw summary{cursor:pointer;padding:2px 0;user-select:none}
.sc-raw summary:hover{color:#4a90d9}
.sc-raw pre{margin-top:4px;max-height:280px}
/* Inside workspace activity card, show the FULL raw logs (no scroll cap) */
.ws-log-body .sc-raw pre{max-height:none}
.ws-log-body .step-card{display:none}
.ws-log-body .sc-raw summary{font-weight:600;color:var(--text)}
[data-theme="dark"] .step-card{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .sc-stage{color:#7baee0}
[data-theme="dark"] .sc-tag{background:var(--bg2,#1a1a1a);color:var(--muted)}
[data-theme="dark"] .sc-checks li{color:var(--text)}
[data-theme="dark"] .sc-raw{color:var(--muted)}
.p-step-output pre{background:#1e1e1e;color:#d4d4d4;padding:8px 10px;border-radius:5px;overflow:auto;max-height:180px;margin:4px 0;font-size:10px;white-space:pre-wrap;word-break:break-all}
.p-step-output .step-status{font-weight:600;font-size:11px;margin-bottom:4px}
.p-step-output .step-status.ok{color:#28a745}
.p-step-output .step-status.err{color:#dc3545}
.p-step-output .step-status.running{color:#f0ad4e}
.p-step-output .step-links{margin-top:4px}
.pipe-steps.open{display:block;max-height:300px;overflow-y:auto}

/* ── Monitoring tab ── */
.mon-table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;border:1px solid #e2e4e8}
.mon-table th{background:#1a1a2e;color:#fff;font-size:12px;text-transform:uppercase;letter-spacing:.5px;padding:10px 14px;text-align:left}
.mon-table td{padding:10px 14px;font-size:13px;border-bottom:1px solid #f0f0f0;color:#333}
.mon-table tr:last-child td{border-bottom:none}
.mon-table tr:hover td{background:#f8f9fb}
.bar-wrap{width:80px;height:8px;background:#e8eaed;border-radius:4px;display:inline-block;vertical-align:middle;margin-right:6px}
.bar{height:100%;border-radius:4px;transition:width .3s}
.bar.low{background:#28a745}.bar.med{background:#f0ad4e}.bar.high{background:#dc3545}
.mon-info{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.mon-info span{font-size:13px;color:#666}
.mon-info .live-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#28a745;margin-right:6px;animation:pulse 2s infinite}
.mon-info button{background:#1a1a2e;color:#fff;border:none;padding:5px 14px;border-radius:5px;cursor:pointer;font-size:12px}
.mon-empty{text-align:center;padding:40px;color:#888;font-size:14px}

/* ── Monitoring tab ── */
.mon-tab-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.mon-tab-header span{font-size:13px;color:#666;display:flex;align-items:center;gap:6px}
.mon-tab-header .live-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#28a745;animation:pulse 2s infinite}
.mon-tab-header button{background:#1a1a2e;color:#fff;border:none;padding:5px 14px;border-radius:5px;cursor:pointer;font-size:12px}
.mon-table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;border:1px solid #e2e4e8;margin-bottom:14px}
.mon-table th{background:#1a1a2e;color:#fff;font-size:12px;text-transform:uppercase;letter-spacing:.5px;padding:8px 12px;text-align:left}
.mon-table td{padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0f0;color:#333}
.mon-table tr:last-child td{border-bottom:none}
.mon-table tr:hover td{background:#f8f9fb}
.mon-table .log-btn{background:#f0f2f5;border:1px solid #ddd;color:#555;padding:3px 10px;border-radius:3px;cursor:pointer;font-size:10px;transition:all .12s}
.mon-table .log-btn:hover{background:#e8f0fe;border-color:#4a90d9;color:#4a90d9}
.mon-table .log-btn.active{background:#4a90d9;color:#fff;border-color:#4a90d9}
.bar-wrap{width:60px;height:6px;background:#e8eaed;border-radius:3px;display:inline-block;vertical-align:middle;margin-right:4px}
.bar{height:100%;border-radius:3px;transition:width .3s}
.bar.low{background:#28a745}.bar.med{background:#f0ad4e}.bar.high{background:#dc3545}
.log-viewer{background:#1e1e1e;color:#d4d4d4;border-radius:6px;overflow:hidden;display:none}
.log-viewer.open{display:block}
.log-viewer .lv-header{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:#2d2d2d;border-bottom:1px solid #444}
.log-viewer .lv-title{font-size:12px;font-weight:600;color:#4a90d9}
.log-viewer .lv-actions{display:flex;gap:6px}
.log-viewer .lv-actions button{background:rgba(255,255,255,.1);color:#aaa;border:none;padding:3px 8px;border-radius:3px;cursor:pointer;font-size:10px}
.log-viewer .lv-actions button:hover{background:rgba(255,255,255,.2);color:#fff}
.log-viewer .lv-body{padding:10px 12px;font-family:'SF Mono',Monaco,Consolas,monospace;font-size:11px;line-height:1.5;max-height:350px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.mon-empty{text-align:center;padding:40px;color:#888;font-size:14px}

/* ── Monitoring bottom panel ── */
.mon-bottom{margin:0 12px 12px;background:#fff;border:1px solid #e2e4e8;border-radius:6px;flex-shrink:0;max-height:220px;overflow-y:auto}
.mon-bottom .mon-header{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:#f8f9fb;border-bottom:1px solid #eee;border-radius:6px 6px 0 0}
.mon-bottom .mon-header .mon-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#1a1a2e;display:flex;align-items:center;gap:5px}
.mon-bottom .mon-header .mon-title .live-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#28a745;animation:pulse 2s infinite}
.mon-bottom .mon-header button{background:none;border:none;color:#999;cursor:pointer;font-size:10px}
.mon-bottom .mon-header button:hover{color:#333}
.mon-bottom .mon-grid{display:grid;grid-template-columns:1fr 1fr;gap:0}
.mon-bottom .mon-card{display:flex;align-items:center;gap:6px;padding:6px 10px;font-size:13px;border-bottom:1px solid #f0f0f0;border-right:1px solid #f0f0f0}
.mon-bottom .mon-card:nth-child(2n){border-right:none}
.mon-bottom .mon-card:nth-last-child(-n+2){border-bottom:none}
.mon-bottom .mon-card .h-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;background:#28a745}
.mon-bottom .mon-card .m-name{font-weight:500;color:#333;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mon-bottom .mon-card .m-stats{font-size:9px;color:#888;white-space:nowrap}
.mon-bottom-empty{padding:12px 14px;color:#aaa;font-size:11px;text-align:center}

/* ── Use Case Builder ── */
.uc-add-bar{display:flex;align-items:center;justify-content:space-between;padding:8px 14px;margin:8px 0;background:#f0f8ff;border:1px solid #d0e4f5;border-left:3px solid #4a90d9;border-radius:6px}
.uc-add-bar span{font-size:11px;color:#4a90d9;font-weight:500}
.uc-add-bar button{background:#4a90d9;color:#fff;border:none;padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer}
.uc-add-bar button:hover{background:#3a7bc8}
.uc-builder{background:#fff;border:1px solid #e2e4e8;border-radius:6px;margin:10px 0;overflow:hidden}
.uc-builder-header{padding:10px 14px;background:#f8f9fb;border-bottom:1px solid #eee;display:flex;align-items:center;justify-content:space-between}
.uc-builder-title{font-size:13px;font-weight:600;color:#1a1a2e}
.uc-builder-close{font-size:11px;color:#888;cursor:pointer}
.uc-builder-close:hover{color:#c33}
.uc-builder-body{padding:14px}
.uc-field{margin-bottom:12px}
.uc-field label{display:block;font-size:11px;font-weight:500;color:#555;margin-bottom:4px}
.uc-field input,.uc-field textarea{width:100%;padding:7px 10px;border:1px solid #ddd;border-radius:5px;font-size:12px;font-family:inherit;box-sizing:border-box}
.uc-field textarea{font-family:monospace;min-height:36px;resize:vertical}
.uc-field .uc-hint{font-size:10px;color:#aaa;margin-top:3px}
.uc-steps-list{margin:8px 0}
.uc-step-row{display:flex;align-items:center;gap:8px;padding:6px 10px;background:#f8f9fb;border:1px solid #eee;border-radius:5px;margin-bottom:4px;font-size:11px}
.uc-step-num{width:20px;height:20px;border-radius:50%;background:#4a90d9;color:#fff;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:600;flex-shrink:0}
.uc-step-name{flex:1;font-weight:500;color:#333}
.uc-step-cmd{font-family:monospace;font-size:10px;color:#888;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px}
.uc-step-remove{font-size:10px;color:#ccc;cursor:pointer}
.uc-step-remove:hover{color:#c33}
.uc-add-step{display:flex;gap:6px;align-items:flex-end}
.uc-add-step input{flex:1}
.uc-add-step button{padding:7px 12px;font-size:11px;white-space:nowrap}
.uc-tips{background:#fffbeb;border:1px solid #fde68a;border-radius:6px;padding:10px 14px;margin-top:10px;font-size:11px;color:#92400e;line-height:1.6}
.uc-tips b{color:#78350f}
.uc-tips code{background:#fef3c7;padding:1px 4px;border-radius:3px;font-family:monospace;font-size:10px}
.uc-example{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;padding:10px 14px;margin-top:8px;font-size:11px}
.uc-example-title{font-weight:600;color:#166534;margin-bottom:6px}
.uc-example-step{padding:3px 0;color:#15803d;font-family:monospace;font-size:10px}
.uc-example-step b{font-family:inherit;font-size:11px}
.uc-builder-footer{padding:10px 14px;background:#f8f9fb;border-top:1px solid #eee;display:flex;align-items:center;justify-content:flex-end;gap:8px}
.uc-builder-footer button{padding:6px 14px;border-radius:5px;font-size:11px;cursor:pointer}
.uc-builder-footer .uc-cancel{background:#fff;border:1px solid #ddd;color:#555}
.uc-builder-footer .uc-save{background:#4a90d9;border:none;color:#fff}
.uc-builder-footer .uc-save:hover{background:#3a7bc8}
.uc-builder-footer .uc-save:disabled{opacity:.4;cursor:not-allowed}
.uc-or-chat{text-align:center;margin:12px 0;font-size:11px;color:#aaa}
.uc-chat-hint{background:#f8faff;border:1px solid #d0e4f5;border-left:3px solid #4a90d9;border-radius:6px;padding:10px 14px;display:flex;align-items:center;gap:8px;cursor:pointer;transition:all .15s}
.uc-chat-hint:hover{background:#eef4ff;border-color:#4a90d9}
.uc-chat-hint .ai-badge{background:#4a90d9;color:#fff;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;letter-spacing:.5px}
.uc-chat-hint span{font-size:11px;color:#555}

/* ── Synthetic Data tab ── */
.sd-loading{text-align:center;padding:60px 20px}
.sd-spinner{width:28px;height:28px;border:3px solid #e2e4e8;border-top:3px solid #4a90d9;border-radius:50%;animation:sdspin .8s linear infinite;margin:0 auto 12px}
@keyframes sdspin{to{transform:rotate(360deg)}}
.sd-loading p{font-size:13px;color:#888}
.sd-loading .sd-sub{font-size:13px;color:#bbb;margin-top:4px}
.sd-section{margin-bottom:16px}
.sd-label{font-size:13px;color:#888;margin-bottom:5px;display:block;font-weight:500}
.sd-row{display:flex;gap:8px;align-items:center}
.sd-select{flex:1;padding:7px 10px;border:1px solid #ddd;border-radius:5px;font-size:12px;background:#fff}
.sd-btn{padding:6px 14px;border:1px solid #ddd;border-radius:5px;font-size:13px;cursor:pointer;background:#fff;color:#333}
.sd-btn:hover{background:#f5f5f5}
.sd-btn-primary{background:#4a90d9;color:#fff;border:none}
.sd-btn-primary:hover{background:#3a7bc8}
.sd-btn:disabled{opacity:.4;cursor:not-allowed}
.sd-info{background:#f8f9fb;border-radius:6px;padding:10px 14px;font-size:13px;color:#666;line-height:1.6;margin-top:8px}
.sd-info b{color:#333;font-size:12px}
.sd-slider-row{display:flex;align-items:center;gap:10px;margin-top:4px}
.sd-slider-row input[type=range]{flex:1;height:4px}
.sd-slider-val{font-size:12px;font-weight:600;min-width:40px;text-align:right;color:#333}
.sd-or{text-align:center;font-size:12px;color:#bbb;margin:6px 0}
.sd-input{padding:7px 10px;border:1px solid #ddd;border-radius:5px;font-size:12px;width:110px}
.sd-radio-group{display:flex;gap:14px;margin-top:4px}
.sd-radio{display:flex;align-items:center;gap:5px;font-size:12px;color:#333;cursor:pointer}
.sd-radio input{accent-color:#4a90d9}
.sd-target{background:#f8f9fb;border-radius:6px;padding:10px 14px;margin-top:8px}
.sd-detected{font-size:13px;color:#28a745;margin-top:6px}
.sd-detected .dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#28a745;margin-right:4px;vertical-align:middle}
.sd-check{display:flex;align-items:center;gap:5px;font-size:13px;color:#666;margin-top:6px}
.sd-check input{accent-color:#4a90d9}
.sd-footer{display:flex;align-items:center;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid #eee}
.sd-status{font-size:13px;color:#888}
.sd-status .dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#28a745;margin-right:4px;vertical-align:middle}
.sd-preview-tabs{display:flex;gap:0;border-bottom:1px solid #e2e4e8;margin-top:12px}
.sd-preview-tab{padding:5px 12px;font-size:13px;color:#888;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px}
.sd-preview-tab.active{color:#4a90d9;border-bottom-color:#4a90d9;font-weight:600}
.sd-preview-tab .cnt{font-size:13px;color:#bbb;margin-left:3px}
.sd-preview-wrap{border:1px solid #e2e4e8;border-top:none;border-radius:0 0 6px 6px;overflow:auto;max-height:240px}
.sd-ptable{width:100%;font-size:13px;border-collapse:collapse}
.sd-ptable th{text-align:left;padding:5px 8px;color:#888;font-weight:500;background:#f8f9fb;border-bottom:1px solid #eee;position:sticky;top:0}
.sd-ptable td{padding:4px 8px;color:#333;border-bottom:1px solid #f5f5f5}
.sd-ptable .pk{color:#4a90d9;font-weight:600}
.sd-ptable .fk{color:#e67e22}
.sd-pfooter{padding:5px 8px;font-size:12px;color:#bbb;text-align:center;background:#f8f9fb;border-top:1px solid #eee}
.sd-option{border:1px solid #e2e4e8;border-radius:6px;padding:12px 14px;cursor:pointer;margin-bottom:8px;transition:all .15s}
.sd-option:hover{border-color:#4a90d9;background:#f8f9fb}
.sd-option-title{font-size:13px;font-weight:600;color:#333;margin-bottom:3px}
.sd-option-desc{font-size:13px;color:#888}
.sd-dropzone{border:1.5px dashed #ccc;border-radius:6px;padding:20px;text-align:center;cursor:pointer;transition:all .2s}
.sd-dropzone:hover{border-color:#4a90d9;background:#f8f9fb}
.sd-dropzone-text{font-size:12px;color:#888}
.sd-dropzone-hint{font-size:12px;color:#bbb;margin-top:3px}
.sd-example{border:1px solid #e2e4e8;border-radius:6px;margin-top:16px;overflow:hidden}
.sd-example-header{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#f8f9fb;cursor:pointer}
.sd-example-title{font-size:12px;font-weight:600;color:#333}
.sd-example-badge{font-size:13px;background:#e8f0fe;color:#4a90d9;padding:2px 6px;border-radius:4px;margin-left:6px}
.sd-example-toggle{font-size:12px;color:#4a90d9}
.sd-example-body{display:none;padding:12px 14px;font-size:13px}
.sd-example-diagram{display:flex;align-items:center;justify-content:center;gap:10px;margin-bottom:10px}
.sd-ebox{border:1px solid #ddd;border-radius:5px;padding:8px 12px;background:#fff;min-width:100px;font-size:13px}
.sd-ebox-name{font-weight:600;color:#333;margin-bottom:4px;padding-bottom:3px;border-bottom:1px solid #eee}
.sd-ebox-col{color:#888;line-height:1.6}
.sd-ebox .pk{color:#4a90d9;font-weight:500}
.sd-ebox .fk{color:#e67e22;font-weight:500}
.sd-earrow{font-size:12px;color:#bbb}
.sd-chat-examples{margin-top:8px}
.sd-chat-ex{display:inline-block;background:#fff;border:1px solid #e2e4e8;border-radius:5px;padding:5px 10px;font-size:13px;color:#333;cursor:pointer;margin:3px 4px 3px 0;transition:all .15s}
.sd-chat-ex:hover{border-color:#4a90d9;color:#4a90d9}
.sd-gen-result{background:#f0fdf4;border:1px solid #86efac;border-radius:6px;padding:10px 14px;margin-top:12px;font-size:13px;color:#166534}

/* ── Chat panel ── */
.chat-header{padding:8px 16px;background:#f8f9fb;border-bottom:1px solid #e2e4e8;display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.chat-header .ch-title{font-size:14px;font-weight:600;color:var(--text)}
.chat-header button{background:#f0f2f5;border:1px solid #e0e2e6;color:#555;padding:3px 10px;border-radius:4px;cursor:pointer;font-size:12px}
.chat-header button:hover{background:#e8f0fe}

/* Stack info in chat panel */
.ws-cred-row{font-size:13px;color:#555;padding:3px 12px}
.ws-cred-row b{color:#1a1a2e}

.chat{flex:1;overflow-y:auto;padding:12px 16px;display:flex;flex-direction:column;gap:8px}
.chat-pinned{flex-shrink:0;padding:6px 16px 0;background:var(--bg2);border-bottom:1px solid var(--border)}
.chat-pinned:empty{display:none}
.chat-pinned > .msg{margin:0 0 6px;padding:0;background:transparent;border:0}
.chat-pinned > .msg .ts{display:none}
.msg{max-width:90%;padding:10px 14px;border-radius:10px;line-height:1.5;font-size:14px;word-wrap:break-word}
.msg.user{align-self:flex-end;background:#1a1a2e;color:#fff;border-bottom-right-radius:3px}
.msg.assistant{align-self:flex-start;background:#f8f9fb;color:#1a1a2e;border:1px solid #e0e0e0;border-bottom-left-radius:3px}
.msg .ts{font-size:9px;opacity:.4;margin-top:4px;display:block}
.msg pre{background:#1e1e1e;color:#d4d4d4;padding:8px 10px;border-radius:5px;overflow:auto;max-height:300px;margin:6px 0;font-size:12px;white-space:pre-wrap;word-break:break-all;position:relative}
.msg pre .copy-btn{position:absolute;top:4px;right:4px;background:rgba(255,255,255,.15);color:#aaa;border:none;padding:2px 6px;border-radius:3px;cursor:pointer;font-size:10px;opacity:0;transition:opacity .15s}
.msg pre:hover .copy-btn{opacity:1}
.msg pre .copy-btn:hover{background:rgba(255,255,255,.25);color:#fff}
.msg code{font-family:'SF Mono',Monaco,Consolas,monospace;font-size:12px}
.msg a{color:#4a90d9}
.msg strong{font-weight:600}
.msg .sec-hdr{margin:6px 0 2px;font-weight:600;font-size:12px;color:#1a1a2e;display:flex;align-items:center;gap:4px}
.msg .sec-hdr .sec-toggle{cursor:pointer;font-size:10px;color:#999;width:14px;text-align:center;user-select:none;border:1px solid #ddd;border-radius:3px;line-height:14px;background:#f8f9fb}
.msg .sec-hdr .sec-toggle:hover{background:#e8eaed;color:#333}
.msg .sec-body{padding:2px 0 4px 18px;font-size:12px;line-height:1.5}
.msg .sec-body.collapsed{display:none}
.msg .sec-body pre{margin:4px 0}
.msg .code-collapse{position:relative}
.msg .code-collapse pre{max-height:none}
.msg .code-collapse.collapsed pre{max-height:100px;overflow:hidden}
.msg .code-collapse .code-toggle{display:inline;font-size:10px;color:#4a90d9;cursor:pointer;margin-left:4px}
.msg .code-collapse.collapsed .code-toggle::after{content:''}
.msg .code-collapse:not(.collapsed) .code-toggle::after{content:''}
.typing{align-self:flex-start;padding:8px 13px;color:#888;font-size:12px}
.typing .dots{display:inline-block;animation:blink 1.2s infinite}
.ws-thinking{align-self:flex-start;display:flex;align-items:center;gap:8px;padding:8px 12px;margin:4px 0;background:#f0f4ff;border:1px solid #d0dcf4;border-radius:8px;font-size:12px;color:#4a90d9;font-weight:500}
.ws-think-dots{display:inline-flex;gap:3px;align-items:center}
.ws-think-dots span{width:6px;height:6px;border-radius:50%;background:#4a90d9;display:inline-block;animation:wsBounce 1.2s infinite ease-in-out}
.ws-think-dots span:nth-child(2){animation-delay:0.15s}
.ws-think-dots span:nth-child(3){animation-delay:0.3s}
@keyframes wsBounce{0%,80%,100%{transform:scale(0.5);opacity:0.4}40%{transform:scale(1);opacity:1}}
.ws-think-label{font-weight:600}
.ws-think-timer{font-weight:700;color:#2563eb;margin-left:auto;font-variant-numeric:tabular-nums}
[data-theme="dark"] .ws-thinking{background:#1a2a4d;border-color:#2a3e6e;color:#6fa8dc}
[data-theme="dark"] .ws-think-timer{color:#8ab4f8}
.ws-chat-input:disabled{opacity:0.6;cursor:not-allowed}
.ws-chat-input-row button:disabled{opacity:0.6;cursor:not-allowed}
@keyframes blink{0%,80%,100%{opacity:.3}40%{opacity:1}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}

.input-bar{padding:8px 16px 10px;background:#fff;border-top:1px solid #e2e4e8;display:flex;gap:6px;flex-shrink:0}
.input-bar input{flex:1;padding:9px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px;outline:none;transition:border-color .15s}
.input-bar input:focus{border-color:#1a1a2e}
.input-bar input:disabled{background:#f5f6f8;color:#999}
.input-bar button{background:#1a1a2e;color:#fff;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-size:12px;transition:opacity .15s}
.input-bar button:disabled{opacity:.35;cursor:not-allowed}
.input-bar .stop-btn{background:#dc3545;display:none}
.input-bar .stop-btn.active{display:inline-block}

/* Mobile */
@media(max-width:900px){.content-panel{display:none}.chat-panel{width:100%}}

/* ═══════════════════════════════════════
   WORKSPACE TAB — v9
   ═══════════════════════════════════════ */
.main.ws-active .content-panel{display:none}
.main.ws-active .chat-panel{display:none}
.ws-layout{display:none;flex:1;overflow:hidden}
.main.ws-active .ws-layout{display:flex}
.ws-body{flex:1;display:flex;overflow:hidden}

/* Main workspace area */
.ws-main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.ws-tab-bar{display:flex;align-items:flex-end;background:#e8eaed;padding:4px 8px 0;flex-shrink:0;min-height:36px;gap:2px;overflow-x:auto}
.ws-tab-bar::-webkit-scrollbar{height:3px}
.ws-tab-bar::-webkit-scrollbar-thumb{background:#ccc;border-radius:2px}
.ws-tab{display:flex;align-items:center;gap:5px;padding:7px 12px;font-size:11px;font-weight:500;color:#666;cursor:pointer;border-radius:8px 8px 0 0;background:#dde0e4;border:1px solid #ccc;border-bottom:none;border-left:3px solid var(--sc,#ccc);transition:all .12s;user-select:none;max-width:200px;white-space:nowrap;overflow:hidden;flex-shrink:0}
.ws-tab:hover{background:#f0f2f5;color:#333}
.ws-tab.active{background:#fff;color:#1a1a2e;font-weight:600;border-color:#ddd;border-bottom:none;border-left-color:var(--sc,#ddd);z-index:1}
.ws-tab.term-tab.active{background:#1a1a2e;color:#64ffda;border-color:#2a2a4a;border-bottom:none;border-left-color:var(--sc,#2a2a4a)}
.ws-t-icon{width:13px;height:13px;border-radius:3px;display:flex;align-items:center;justify-content:center;font-size:7px;font-weight:700;flex-shrink:0}
.ws-t-icon.app-icon{background:#f0f4ff;color:#4a90d9}
.ws-t-icon.term-icon{background:#1a1a2e;color:#64ffda}
.ws-tab.term-tab.active .ws-t-icon{background:#28a745;color:#fff}
.ws-t-label{overflow:hidden;text-overflow:ellipsis}
.ws-tab .ws-close{font-size:13px;color:#999;cursor:pointer;margin-left:auto;line-height:1;padding:0 2px;border-radius:3px;flex-shrink:0}
.ws-tab .ws-close:hover{color:#dc3545;background:rgba(220,53,69,.08)}
.ws-tab.term-tab .ws-close{color:#8b949e}
.ws-tab.term-tab .ws-close:hover{color:#ff6b6b;background:rgba(255,107,107,.15)}
.ws-tab-home{padding:8px 14px;font-size:13px;font-weight:600;cursor:pointer;color:#888;border-radius:8px 8px 0 0;background:#dde0e4;border:1px solid #ccc;border-bottom:none;transition:all .12s;flex-shrink:0}
.ws-tab-home:hover{background:#f0f2f5;color:#333}
.ws-tab-home.active{background:#fff;color:#1a1a2e;border-color:#ddd;z-index:1}

.ws-content{flex:1;overflow:hidden;position:relative;background:var(--bg2);border-top:1px solid var(--border)}
.ws-page{display:none;width:100%;height:100%;position:absolute;top:0;left:0}
.ws-page.active{display:flex;flex-direction:column}

/* Logs sub-tab — sidebar (containers) + live-tail pane */
.ws-logs-layout{display:flex;flex:1;min-height:0;background:var(--card-bg,#fff)}
.ws-logs-sidebar{width:240px;border-right:1px solid var(--border);display:flex;flex-direction:column;background:var(--bg2,#fafbfd)}
.ws-logs-side-h{padding:10px 12px;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted,#6b7280);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.ws-logs-refresh{background:transparent;border:0;color:var(--muted);font-size:16px;cursor:pointer;padding:2px 6px;border-radius:4px}
.ws-logs-refresh:hover{background:var(--bg3,#e5e7eb);color:var(--text)}
.ws-logs-list{flex:1;overflow-y:auto;padding:4px}
.ws-logs-empty{padding:14px;font-size:12px;color:var(--muted);text-align:center}
.ws-logs-row{padding:7px 10px;font-size:12px;cursor:pointer;border-radius:5px;display:flex;align-items:center;gap:7px;color:var(--text)}
.ws-logs-row:hover{background:var(--bg3,#e5e7eb)}
.ws-logs-row.active{background:rgba(59,130,246,.12);color:#1d4ed8;font-weight:600}
.ws-logs-row .ws-logs-dot{width:7px;height:7px;border-radius:50%;background:#10b981;flex-shrink:0}
.ws-logs-row .ws-logs-name{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ws-logs-pane{flex:1;display:flex;flex-direction:column;min-width:0}
.ws-logs-pane-h{display:flex;align-items:center;gap:10px;padding:8px 14px;border-bottom:1px solid var(--border);background:var(--bg2,#fafbfd);flex-wrap:wrap}
.ws-logs-target{font-size:13px;font-weight:700;color:var(--text);font-family:'SF Mono','Monaco',Menlo,monospace}
.ws-logs-toolbar{margin-left:auto;display:flex;align-items:center;gap:6px}
.ws-logs-toolbar button{padding:4px 10px;font-size:11px;border:1px solid var(--border);background:var(--card-bg,#fff);border-radius:4px;cursor:pointer;color:var(--text)}
.ws-logs-toolbar button:hover:not(:disabled){border-color:#3b82f6;color:#1d4ed8}
.ws-logs-toolbar button:disabled{opacity:.5;cursor:not-allowed}
.ws-logs-toolbar input{padding:4px 8px;font-size:11px;border:1px solid var(--border);border-radius:4px;background:var(--card-bg,#fff);color:var(--text);width:140px}
.ws-logs-toolbar input:disabled{opacity:.5}
.ws-logs-view{flex:1;margin:0;padding:10px 14px;overflow:auto;background:#0b0f17;color:#e5e7eb;font-family:'SF Mono','Monaco',Menlo,monospace;font-size:12px;line-height:1.45;white-space:pre-wrap;word-break:break-all}
[data-theme="dark"] .ws-logs-sidebar,[data-theme="dark"] .ws-logs-pane-h{background:var(--bg3)}

/* Home page */
.ws-home{padding:24px 32px;height:100%;background:var(--bg);font-size:15px;display:flex;flex-direction:column;overflow:hidden}
/* Children of ws-home keep their natural height; the activity card grows to fill */
.ws-home > .ws-stack-sel,.ws-home > .ws-creds-dd-wrap,.ws-home > .ws-grid-3col,.ws-home > .ws-grid-2col,.ws-home > .ws-empty{flex-shrink:0}
.ws-data-story{display:flex;gap:8px;align-items:stretch;margin-bottom:18px;flex-wrap:wrap}
.ws-ds-pill{flex:1;min-width:200px;display:flex;align-items:center;gap:12px;padding:14px 16px;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:10px;cursor:pointer;transition:all .15s;border-left:4px solid #94a3b8}
.ws-ds-pill:hover{box-shadow:0 2px 6px rgba(0,0,0,.06);transform:translateY(-1px)}
.ws-ds-pill.ws-ds-seed{border-left-color:#7c3aed}
.ws-ds-pill.ws-ds-synth{border-left-color:#2563eb}
.ws-ds-pill.ws-ds-stream{border-left-color:#9ca3af}
.ws-ds-pill.ws-ds-stream[data-running="true"]{border-left-color:#10b981;background:linear-gradient(90deg,rgba(16,185,129,0.04),transparent)}
.ws-ds-pill.ws-ds-stream[data-running="true"] .ws-ds-icon{color:#10b981;animation:pulse 1.4s infinite}
.ws-ds-icon{font-size:24px;color:var(--muted,#94a3b8);min-width:30px;text-align:center}
.ws-ds-pill.ws-ds-seed .ws-ds-icon{color:#7c3aed}
.ws-ds-pill.ws-ds-synth .ws-ds-icon{color:#2563eb}
.ws-ds-content{flex:1;min-width:0}
.ws-ds-title{font-size:10px;font-weight:700;color:var(--muted,#6b7280);letter-spacing:1.4px;margin-bottom:4px}
.ws-ds-num{font-size:22px;font-weight:700;color:var(--text);line-height:1.1;margin-bottom:3px}
.ws-ds-sub{font-size:11px;color:var(--muted,#6b7280);line-height:1.3}
.ws-ds-arrow{display:flex;align-items:center;color:var(--muted,#94a3b8);font-size:18px;font-weight:700}
[data-theme="dark"] .ws-ds-pill{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
.ws-grid-2col{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:6px}
@media(max-width:980px){.ws-grid-2col{grid-template-columns:1fr}}
.ws-grid-3col{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-top:6px}
@media(max-width:1100px){.ws-grid-3col{grid-template-columns:1fr 1fr}}
@media(max-width:780px){.ws-grid-3col{grid-template-columns:1fr}}
.ws-creds-dd-wrap{position:relative;display:inline-block;margin-bottom:10px}
.ws-creds-dd-btn{font-size:13px;padding:8px 14px;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:6px;color:var(--text);cursor:pointer;display:inline-flex;align-items:center;gap:8px;font-weight:600}
.ws-creds-dd-btn:hover{border-color:#4a90d9}
.ws-creds-dd-btn.active{border-color:#4a90d9;background:#f0f7ff}
.ws-creds-dd-count{font-size:10px;color:var(--muted,#6b7280);background:var(--bg2,#f3f4f6);padding:1px 6px;border-radius:8px}
.ws-creds-dd-caret{font-size:10px;color:var(--muted,#6b7280)}
.ws-creds-dd-panel{display:none;position:absolute;top:100%;left:0;margin-top:4px;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:6px;box-shadow:0 4px 12px rgba(0,0,0,0.08);padding:10px 12px;z-index:100;min-width:340px;max-width:520px}
.ws-creds-dd-panel.open{display:block}
[data-theme="dark"] .ws-creds-dd-btn{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .ws-creds-dd-btn.active{background:#1a3050}
[data-theme="dark"] .ws-creds-dd-panel{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
.ws-log-item{margin:4px 0;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-left:4px solid var(--border,#e5e7eb);border-radius:6px;overflow:hidden;transition:border-color .12s,background .12s}
.ws-log-item summary{display:flex;align-items:center;gap:10px;padding:8px 12px;cursor:pointer;font-size:12px;list-style:none;user-select:none}
.ws-log-item summary::-webkit-details-marker{display:none}
.ws-log-item summary:hover{background:var(--bg2,#f8fafc)}
.ws-log-item[open] summary{border-bottom:1px solid var(--border,#f0f0f0)}
.ws-log-mark{display:inline-block;width:18px;text-align:center;font-weight:700;font-size:14px}
/* Status-driven row colors for at-a-glance scanning */
.ws-log-ok{background:#f0fdf4;border-left-color:#22c55e;border-color:#bbf7d0}
.ws-log-ok summary:hover{background:#dcfce7}
.ws-log-ok .ws-log-mark{color:#16a34a}
.ws-log-ok .ws-log-state{color:#16a34a;font-weight:700}
.ws-log-err{background:#fef2f2;border-left-color:#dc2626;border-color:#fecaca}
.ws-log-err summary:hover{background:#fee2e2}
.ws-log-err .ws-log-mark{color:#dc2626}
.ws-log-err .ws-log-state{color:#dc2626;font-weight:700}
.ws-log-running{background:#fffbeb;border-left-color:#f59e0b;border-color:#fde68a;animation:pulseRunning 1.6s infinite}
.ws-log-running summary:hover{background:#fef3c7}
.ws-log-running .ws-log-mark{color:#d97706;animation:pulse 1.4s infinite}
.ws-log-running .ws-log-state{color:#d97706;font-weight:700}
.ws-log-label{flex:0 1 auto;color:var(--text);font-weight:600}
.ws-log-chev{display:inline-block;font-size:11px;line-height:1;color:var(--muted,#6b7280);transition:transform .12s;width:10px;text-align:center;flex-shrink:0}
.ws-log-item[open] .ws-log-chev{transform:rotate(90deg)}
.ws-log-hint{flex:1;font-size:11px;color:var(--muted,#9ca3af);font-style:italic;margin-left:6px}
.ws-log-item[open] .ws-log-hint{display:none}
.ws-log-state{font-size:11px;color:var(--muted,#6b7280);text-transform:uppercase;letter-spacing:.4px}
.ws-log-body{padding:8px 12px;font-size:11px;background:var(--card-bg,#fff)}
[data-theme="dark"] .ws-log-item{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a);border-left-color:var(--border)}
[data-theme="dark"] .ws-log-item summary:hover{background:var(--bg2,#1a1a1a)}
[data-theme="dark"] .ws-log-ok{background:rgba(34,197,94,0.10);border-left-color:#22c55e}
[data-theme="dark"] .ws-log-err{background:rgba(220,38,38,0.10);border-left-color:#dc2626}
[data-theme="dark"] .ws-log-running{background:rgba(245,158,11,0.10);border-left-color:#f59e0b}
[data-theme="dark"] .ws-log-body{background:var(--bg3)}
.pipe-next-hint.inline{margin:0 16px 6px 46px;padding:8px 12px;font-size:11px}
.ws-uco-live{margin-top:10px;padding:10px 12px;background:var(--bg2,#f8fafc);border:1px solid var(--border,#e5e7eb);border-radius:6px;font-size:12px}
.ws-live-row{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.ws-live-dot{width:9px;height:9px;border-radius:50%;background:#9ca3af;display:inline-block;flex-shrink:0}
.ws-live-dot.live{background:#10b981;box-shadow:0 0 0 4px rgba(16,185,129,0.18);animation:pulse 1.4s infinite}
.ws-live-state{font-weight:700;letter-spacing:.5px;font-size:11px;color:var(--muted,#6b7280)}
.ws-live-state.live{color:#10b981}
.ws-live-stat{font-size:12px;color:var(--text);font-variant-numeric:tabular-nums}
.ws-live-store{padding:2px 8px;border-radius:10px;background:rgba(99,102,241,0.10);color:var(--text);font-size:11px;font-weight:600}
[data-theme="dark"] .ws-uco-live{background:var(--bg2,#1a1a1a);border-color:var(--border,#3a3a3a)}
.ws-quad{background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:6px;display:flex;flex-direction:column}
.ws-quad-h{padding:10px 14px;font-size:13px;font-weight:700;color:var(--text);letter-spacing:.5px;text-transform:uppercase;border-bottom:1px solid var(--border,#f0f0f0);display:flex;align-items:center;gap:8px}
.ws-quad-body{padding:8px 10px;flex:1}
[data-theme="dark"] .ws-quad{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
.ws-uco-card{background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:10px;margin-top:18px;padding:14px 16px;display:flex;flex-direction:column;min-height:0;margin-bottom:12px}
/* The activity card grows to fill the remaining vertical space, with the
   recent-activity log inside scrolling instead of the page. */
#wsUcoSection{flex:1 1 auto;display:flex;flex-direction:column;min-height:0}
#wsUcoSection > .ws-uco-card{flex:1 1 auto;min-height:0}
.ws-uco-card-h{display:flex;align-items:baseline;gap:10px;margin-bottom:12px}
.ws-uco-card-icon{font-size:20px;color:#10b981}
.ws-uco-card-title{font-size:18px;font-weight:700;color:var(--text)}
.ws-uco-card-sub{font-size:13px;color:var(--muted,#6b7280)}
.ws-uco-controls{margin-top:8px;border-top:1px solid var(--border,#f0f0f0)}
.ws-uco-log-h{margin-top:14px;font-size:13px;font-weight:700;color:var(--text);text-transform:uppercase;letter-spacing:.5px}
.ws-uco-log{margin-top:6px;background:var(--bg2,#f8fafc);border:1px solid var(--border,#e5e7eb);border-radius:6px;padding:10px 12px;font-size:12px;flex:1 1 auto;min-height:0;overflow-y:auto}
.ws-uco-log-step{font-size:11px;color:#4a90d9;font-weight:600;margin-bottom:8px;font-family:'SF Mono',Monaco,Consolas,monospace}
[data-theme="dark"] .ws-uco-card{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .ws-uco-log{background:var(--bg2,#1a1a1a);border-color:var(--border,#3a3a3a)}
.ws-section-h{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text);letter-spacing:.3px;margin:18px 2px 10px;text-transform:uppercase}
.ws-section-count{font-size:11px;font-weight:500;color:var(--muted,#6b7280);background:var(--bg2,#f3f4f6);padding:1px 7px;border-radius:10px;text-transform:none}
.ws-tile-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:6px}
.ws-tile{display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:6px;cursor:pointer;transition:all .12s}
.ws-tile:hover{box-shadow:0 2px 6px rgba(0,0,0,.06);border-color:#4a90d9;transform:translateY(-1px)}
.ws-tile-primary{border-left:4px solid #4a90d9;background:linear-gradient(90deg,rgba(74,144,217,0.04),transparent)}
.ws-tile-open{border-color:#10b981;background:#f0fdf4}
.ws-tile-icon{width:36px;height:36px;background:#e8f0fe;color:#1e40af;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;letter-spacing:.4px;flex-shrink:0;font-family:'SF Mono',Monaco,Consolas,monospace}
.ws-tile-primary .ws-tile-icon{background:linear-gradient(135deg,#4a90d9,#3a7bc8);color:#fff}
.ws-tile-body{flex:1;min-width:0}
.ws-tile-name{font-size:14px;font-weight:600;color:var(--text);line-height:1.25;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ws-tile-status{font-size:11px;color:var(--muted,#6b7280);display:flex;align-items:center;gap:4px}
.ws-tile-dot{width:7px;height:7px;border-radius:50%;background:#10b981;display:inline-block}
.ws-tile-dot.open{background:#10b981;box-shadow:0 0 0 2px rgba(16,185,129,0.2)}
[data-theme="dark"] .ws-tile{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .ws-tile-icon{background:#1e3a5f;color:#7baee0}
[data-theme="dark"] .ws-tile-primary .ws-tile-icon{background:linear-gradient(135deg,#4a90d9,#3a7bc8);color:#fff}
[data-theme="dark"] .ws-tile-open{background:#0f2d22;border-color:#10b981}
[data-theme="dark"] .ws-section-count{background:var(--bg3,#2a2a2a)}
.ws-creds-card{border:1px solid var(--border,#e5e7eb);border-radius:8px;background:var(--card-bg,#fff);overflow:hidden}
.ws-creds-card[data-wssec="credentials"] .ws-creds-body{display:none}
.ws-creds-card.open .ws-creds-body{display:block}
.ws-creds-header{display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer;user-select:none}
.ws-creds-header:hover{background:var(--bg2,#f8fafc)}
.ws-creds-icon{font-size:14px}
.ws-creds-label{font-size:13px;font-weight:600;color:var(--text);flex:1}
.ws-creds-count{font-size:11px;font-weight:500;color:var(--muted,#6b7280);background:var(--bg2,#f3f4f6);padding:1px 7px;border-radius:10px}
.ws-creds-toggle{font-size:12px;color:var(--muted,#6b7280)}
.ws-creds-body{padding:0 14px 14px}
.ws-creds-table{width:100%;border-collapse:collapse;font-size:12px}
.ws-creds-table tr{border-top:1px solid var(--border,#f0f0f0)}
.ws-creds-table td{padding:6px 4px}
.ws-creds-table td:first-child{width:140px;color:var(--muted,#6b7280)}
.ws-creds-table code{background:var(--bg2,#f3f4f6);padding:1px 5px;border-radius:3px;font-family:'SF Mono',Monaco,Consolas,monospace;font-size:11px}
.ws-creds-port{font-size:10px;color:var(--muted,#6b7280);margin-left:6px}
[data-theme="dark"] .ws-creds-card{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .ws-creds-table code{background:var(--bg2,#1a1a1a)}
.ws-term-pills{display:flex;flex-wrap:wrap;gap:8px}
.ws-term-pill{display:inline-flex;align-items:center;gap:8px;padding:8px 14px;background:var(--card-bg,#fff);border:1px solid var(--border,#e5e7eb);border-radius:18px;cursor:pointer;font-size:13px;font-weight:500;color:var(--text);transition:all .12s}
.ws-term-pill:hover{border-color:#4a90d9;box-shadow:0 1px 3px rgba(0,0,0,.05)}
.ws-term-pill.open{border-color:#10b981;background:#f0fdf4}
.ws-term-pill-icon{font-family:'SF Mono',Monaco,Consolas,monospace;font-size:11px;font-weight:700;color:#1e40af;background:#e8f0fe;padding:2px 7px;border-radius:10px;letter-spacing:.5px}
.ws-term-pill.open .ws-term-pill-icon{color:#065f46;background:#d1fae5}
.ws-term-pill-name{}
.ws-term-pill-dot{width:7px;height:7px;border-radius:50%;background:#10b981;display:inline-block}
[data-theme="dark"] .ws-term-pill{background:var(--bg3,#262626);border-color:var(--border,#3a3a3a)}
[data-theme="dark"] .ws-term-pill-icon{background:#1e3a5f;color:#7baee0}
[data-theme="dark"] .ws-term-pill.open{background:#0f2d22;border-color:#10b981}
.ws-uco-row{display:flex;align-items:center;gap:14px;padding:10px 12px;border-bottom:1px solid var(--border,#f0f0f0)}
.ws-uco-row:last-child{border-bottom:none}
.ws-uco-btn{min-width:200px;flex-shrink:0}
.ws-uco-row-name{font-weight:600;color:var(--text);min-width:180px;flex-shrink:0;font-size:13px}
.ws-uco-row-info{flex:1;display:flex;align-items:center;gap:14px;color:var(--muted,#6b7280);font-size:12px;flex-wrap:wrap}
.ws-uco-meta{flex:1;min-width:0}
.ws-uco-name{font-size:14px;font-weight:600;color:var(--text);margin-bottom:2px}
.ws-uco-desc{font-size:12px;color:var(--muted,#6b7280);line-height:1.4}
.ws-uco-hint{font-size:11px;color:#b45309;font-style:italic;margin-top:3px}
@media(max-width:900px){.ws-ds-arrow{display:none}.ws-ds-pill{min-width:100%}}

/* Stack selector — segmented control */
.ws-stack-sel{display:flex;gap:0;margin-bottom:24px;border:1px solid var(--border);border-radius:10px;overflow:hidden;max-width:700px}
.ws-stack-btn{flex:1;display:flex;align-items:center;gap:10px;padding:12px 16px;cursor:pointer;transition:all .18s;background:var(--bg2);border-right:1px solid var(--border);user-select:none}
.ws-stack-btn:last-child{border-right:none}
.ws-stack-btn:hover{background:var(--bg3)}
.ws-stack-btn.active{background:linear-gradient(135deg,#f0f7ff,#e8f0fe)}
.ws-stack-btn.active .wsb-name{color:var(--sc,#4a90d9)}
.wsb-dot{width:8px;height:8px;border-radius:50%;background:#28a745;flex-shrink:0;animation:pulse 2s infinite}
.wsb-info{flex:1;min-width:0}
.wsb-name{font-size:13px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wsb-meta{font-size:10px;color:var(--muted);margin-top:1px}

/* Metabase card */
/* (ws-mb-card removed — workspace uses ws-sec sections now) */

/* Section header (collapsible) */
.ws-sec{margin-bottom:16px}
.ws-sec-hdr{display:flex;align-items:center;gap:8px;margin-bottom:2px;padding:6px 0}
.ws-sec-toggle{cursor:pointer;user-select:none}
.ws-sec-toggle:hover .ws-sec-label{color:#4a90d9}
.ws-sec-icon{width:20px;height:20px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;color:#fff;flex-shrink:0}
.ws-sec-icon.si-console{background:#4a90d9}
.ws-sec-icon.si-term{background:#28a745}
.ws-sec-label{font-size:15px;font-weight:700;color:var(--text);letter-spacing:.2px;transition:color .12s}
.ws-sec-count{font-size:13px;color:var(--muted);margin-left:auto}
.ws-sec-pm{font-size:11px;color:var(--muted);width:18px;height:18px;text-align:center;line-height:18px;border:1px solid var(--border);border-radius:3px;background:var(--bg3);cursor:pointer;margin-left:4px;user-select:none;font-weight:600;flex-shrink:0}
.ws-sec-pm:hover{background:var(--border);color:var(--text)}
.ws-sec-body{}
.ws-sec.collapsed .ws-sec-body{display:none}

/* Row-based list */
.ws-list{border:1px solid var(--border);border-radius:8px;overflow:hidden;background:var(--bg2)}
.ws-row{display:flex;align-items:center;gap:12px;padding:10px 14px;border-bottom:1px solid var(--border2);cursor:pointer;transition:background .12s}
.ws-row:last-child{border-bottom:none}
.ws-row:hover{background:var(--card-hover,#f8fbff)}
.ws-row.term-row:hover{background:var(--card-hover,#f5fff8)}
.wr-icon{width:30px;height:30px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0}
.wr-icon.app{background:#f0f4ff;color:#4a90d9}
.wr-icon.term{background:#e8f5e9;color:#28a745}
.wr-main{flex:1;min-width:0}
.wr-name{font-size:15px;font-weight:600;color:var(--text)}
.wr-desc{font-size:13px;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wr-details{display:flex;align-items:center;gap:16px;flex-shrink:0}
.wr-url{font-size:10px;color:var(--muted);font-family:'SF Mono',Monaco,Consolas,monospace;background:var(--bg3);padding:2px 8px;border-radius:4px;white-space:nowrap}
.wr-cred{font-size:10px;color:var(--text3);font-family:'SF Mono',Monaco,Consolas,monospace;white-space:nowrap;max-width:180px;overflow:hidden;text-overflow:ellipsis}
.wr-conn{font-size:10px;color:var(--text3);font-family:'SF Mono',Monaco,Consolas,monospace;background:var(--bg3);padding:2px 8px;border-radius:4px;white-space:nowrap}
.wr-actions{display:flex;gap:4px;flex-shrink:0}
.wr-btn{font-size:10px;padding:4px 10px;border-radius:5px;cursor:pointer;font-weight:600;transition:all .15s;border:none}
.wr-btn.primary{background:#4a90d9;color:#fff}
.wr-btn.primary:hover{background:#3a7bc8;box-shadow:0 1px 6px rgba(74,144,217,.2)}
.wr-btn.primary.green{background:#28a745}
.wr-btn.primary.green:hover{background:#218838;box-shadow:0 1px 6px rgba(40,167,69,.2)}
.ws-empty-hint{padding:8px 12px;color:var(--muted,#aaa);font-size:11px}
.ws-mb-btn{font-size:12px;padding:5px 14px;border:none;border-radius:4px;background:#4a90d9;color:#fff;cursor:pointer;font-weight:600;transition:background .15s}
.ws-mb-btn:hover{background:#3a7bc8}
.wr-btn.secondary{background:#f0f2f5;color:#555;border:1px solid #ddd}
.wr-btn.secondary:hover{background:#e8eaed;color:#333}
.wr-open-badge{font-size:9px;color:#4a90d9;font-weight:700;background:#e8f0fe;padding:2px 6px;border-radius:4px;flex-shrink:0}
.wr-open-badge.term-badge{color:#28a745;background:#e8f5e9}

/* App detail page */
.ws-detail-page{display:flex;align-items:flex-start;justify-content:center;height:100%;background:#f8f9fb;overflow-y:auto;padding:32px}
.ws-detail-card{background:#fff;border:1px solid #e2e4e8;border-radius:12px;max-width:560px;width:100%;box-shadow:0 2px 16px rgba(0,0,0,.03);overflow:hidden}
.ws-dc-header{padding:24px 28px 20px;border-bottom:1px solid #f0f2f5}
.ws-dc-header-top{display:flex;align-items:center;gap:14px;margin-bottom:8px}
.ws-dc-icon{width:44px;height:44px;border-radius:10px;background:#f0f4ff;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:700;color:#4a90d9;flex-shrink:0}
.ws-dc-title-area{flex:1}
.ws-dc-name{font-size:17px;font-weight:700;color:#1a1a2e}
.ws-dc-stack{font-size:10px;color:#999;margin-top:2px;display:flex;align-items:center;gap:4px}
.ws-dc-stack .sbd{width:5px;height:5px;border-radius:50%;display:inline-block}
.ws-dc-desc{font-size:12px;color:#666;line-height:1.6}
.ws-dc-body{padding:20px 28px}
.ws-dc-section{margin-bottom:16px}
.ws-dc-section:last-child{margin-bottom:0}
.ws-dc-section-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#999;margin-bottom:6px}
.ws-dc-url-row{background:#f8f9fb;border:1px solid #eee;border-radius:6px;padding:8px 12px;font-size:11px;color:#555;font-family:'SF Mono',Monaco,Consolas,monospace;display:flex;align-items:center;gap:8px}
.ws-dc-url-row span{flex:1;word-break:break-all}
.ws-dc-url-row button{font-size:9px;padding:2px 8px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#4a90d9;cursor:pointer;flex-shrink:0}
.ws-dc-url-row button:hover{background:#e8f0fe}
.ws-dc-creds{border:1px solid #eee;border-radius:6px;overflow:hidden}
.ws-dc-cr{display:flex;padding:6px 12px;font-size:11px;border-bottom:1px solid #f5f5f5;align-items:center}
.ws-dc-cr:last-child{border-bottom:none}
.ws-dc-cr-key{color:#999;width:70px;flex-shrink:0;font-weight:500}
.ws-dc-cr-val{color:#1a1a2e;font-family:'SF Mono',Monaco,Consolas,monospace;flex:1}
.ws-dc-cr-copy{font-size:9px;color:#4a90d9;cursor:pointer;opacity:.3;background:none;border:none;padding:2px 6px;border-radius:3px}
.ws-dc-cr-copy:hover{opacity:1;background:#e8f0fe}
.ws-dc-footer{padding:16px 28px 24px;display:flex;gap:10px;border-top:1px solid #f0f2f5}
.ws-dc-btn{display:inline-flex;align-items:center;gap:6px;padding:9px 22px;border-radius:7px;font-size:13px;font-weight:600;cursor:pointer;transition:all .15s;border:none}
.ws-dc-btn.launch{background:#4a90d9;color:#fff}
.ws-dc-btn.launch:hover{background:#3a7bc8;box-shadow:0 2px 10px rgba(74,144,217,.25)}
.ws-dc-btn.ext{background:#fff;color:#555;border:1px solid #ddd}
.ws-dc-btn.ext:hover{background:#f8f9fb;border-color:#bbb}

/* Iframe */
.ws-iframe-bar{display:flex;align-items:center;gap:8px;padding:4px 12px;background:#f8f9fb;border-bottom:1px solid #eee;flex-shrink:0}
.ws-ib-url{font-size:12px;color:#999;font-family:'SF Mono',Monaco,Consolas,monospace;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ws-iframe-bar button{font-size:11px;padding:4px 10px;border:1px solid #4a90d9;border-radius:4px;background:#fff;color:#4a90d9;cursor:pointer;font-weight:600}
.ws-iframe-bar button:hover{background:#e8f0fe}
.ws-iframe-refresh-hint{font-size:10px;color:#16a34a;font-weight:500;background:#f0fdf4;border:1px solid #bbf7d0;padding:2px 8px;border-radius:10px}
.ws-iframe-wrap{width:100%;flex:1;position:relative}
.ws-iframe-wrap iframe{width:100%;height:100%;border:none;background:#fff}
.ws-loading{position:absolute;top:0;left:0;width:100%;height:100%;background:#f8f9fb;display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:10;transition:opacity .3s}
.ws-loading.hidden{opacity:0;pointer-events:none}
.ws-console-overlay{flex:1;display:flex;align-items:center;justify-content:center;padding:32px;background:#f8fafc}
.ws-console-card{display:flex;gap:18px;align-items:flex-start;padding:18px 22px;border-radius:8px;max-width:640px;box-shadow:0 1px 3px rgba(0,0,0,0.05)}
.ws-console-text{flex:1;min-width:0}
.ws-console-heading{font-size:15px;font-weight:600;margin-bottom:6px}
.ws-console-hint{font-size:13px;line-height:1.5;margin-bottom:8px;opacity:.9}
.ws-console-action{font-size:13px;line-height:1.5;margin-bottom:8px;font-weight:500}
.ws-console-foot{font-size:11px;opacity:.6;font-style:italic;margin-top:6px}
[data-theme="dark"] .ws-console-overlay{background:var(--bg2,#1a1a1a)}
[data-theme="dark"] .ws-console-card{background:var(--bg3,#2a2a2a)!important;color:var(--text)!important}
.ws-spinner{width:28px;height:28px;border:3px solid #e8eaed;border-top-color:#4a90d9;border-radius:50%;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.ws-loading .ws-load-text{margin-top:10px;font-size:12px;color:#888}

/* Terminal */
.ws-term-page{background:#0d1117;display:none;flex-direction:column}
.ws-term-page.active{display:flex}
.ws-term-toolbar{display:flex;align-items:center;gap:8px;padding:5px 12px;background:#161b22;border-bottom:1px solid #30363d;flex-shrink:0}
.tt-badge{display:flex;align-items:center;gap:5px;background:#1f6feb22;border:1px solid #1f6feb55;border-radius:4px;padding:2px 8px}
.tt-dot{width:6px;height:6px;border-radius:50%;background:#8b949e}
.tt-dot.on{background:#3fb950;animation:pulse 2s infinite}
.tt-name{font-size:10px;color:#58a6ff;font-weight:600;font-family:'SF Mono',Monaco,Consolas,monospace}
.tt-conn{font-size:10px;color:#8b949e;font-family:'SF Mono',Monaco,Consolas,monospace;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ws-term-toolbar button{font-size:10px;padding:2px 8px;border:1px solid #30363d;border-radius:4px;background:#21262d;color:#8b949e;cursor:pointer}
.ws-term-toolbar button:hover{background:#30363d;color:#c9d1d9}
.tt-disc{color:#f85149!important;border-color:#f8514955!important}
.tt-disc:hover{background:#f8514922!important}
.ws-xterm{flex:1;padding:4px}

/* Right chat panel (workspace) */
.ws-chat-resize{width:4px;background:transparent;cursor:ew-resize;flex-shrink:0;transition:background .15s}
.ws-chat-resize:hover,.ws-chat-resize.dragging{background:#4a90d9}
.ws-chat{width:420px;min-width:300px;max-width:600px;display:flex;flex-direction:column;background:var(--bg2);border-left:1px solid var(--border);flex-shrink:0;overflow:hidden;transition:width .25s,min-width .25s}
.ws-chat.collapsed{width:36px;min-width:36px;cursor:pointer}
.ws-chat.collapsed .ws-panel-sections,.ws-chat.collapsed .ws-chat-header{display:none}
.ws-chat.collapsed .ws-chat-strip{display:flex}
.ws-chat-strip{display:none;flex-direction:column;align-items:center;padding:10px 0;height:100%;background:var(--bg3);gap:8px;user-select:none}
.ws-chat-strip .cps-icon{font-size:14px;color:#4a90d9;cursor:pointer;padding:4px;border-radius:4px}
.ws-chat-strip .cps-icon:hover{background:#e8f0fe}
.ws-chat-strip .cps-dot{width:7px;height:7px;border-radius:50%;background:#28a745;animation:pulse 2s infinite}
.ws-chat-strip .cps-label{writing-mode:vertical-rl;text-orientation:mixed;font-size:11px;font-weight:600;color:#4a90d9;cursor:pointer;padding:6px 4px;border-radius:4px;flex:1;display:flex;align-items:center}
.ws-chat-strip .cps-label:hover{background:#e8f0fe}
.ws-chat-header{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;flex-shrink:0}
.ws-chat-title{font-size:12px;font-weight:600;display:flex;align-items:center;gap:6px}
.ws-chat-title .wch-dot{width:6px;height:6px;border-radius:50%;background:#28a745;animation:pulse 2s infinite}
.ws-chat-btns{display:flex;gap:4px}
.ws-chat-btn{background:rgba(255,255,255,.1);border:none;color:#fff;width:24px;height:24px;border-radius:4px;cursor:pointer;font-size:12px;display:flex;align-items:center;justify-content:center}
.ws-chat-btn:hover{background:rgba(255,255,255,.2)}
.ws-chat-body{flex:1;display:flex;flex-direction:column;overflow:hidden}
/* VS Code-style expandable panel sections */
.ws-panel-sections{flex:1;display:flex;flex-direction:column;overflow:hidden}
.ws-panel-sec{border-bottom:1px solid var(--border);flex-shrink:0;overflow:hidden;min-height:0}
.ws-panel-sec.open{flex:1 1 0;overflow:hidden;display:flex;flex-direction:column;min-height:0}
.ws-panel-hdr{display:flex;align-items:center;gap:4px;padding:8px 12px;background:var(--bg3);cursor:pointer;user-select:none;font-size:14px;font-weight:700;color:var(--text);text-transform:uppercase;letter-spacing:.3px;border-bottom:1px solid var(--border)}
.ws-panel-hdr:hover{background:var(--border)}
.ws-panel-chev{font-size:10px;color:var(--text3);transition:transform .15s;width:12px;text-align:center;flex-shrink:0}
.ws-panel-sec:not(.open) .ws-panel-chev{transform:rotate(-90deg)}
.ws-panel-sec:not(.open) .ws-panel-body{display:none}
.ws-panel-body{overflow-y:auto;padding:8px 10px;font-size:13px;flex:1;min-height:0}
.ws-panel-title{flex-shrink:0}
.ws-chat-msgs{flex:1;overflow-y:auto;padding:10px 12px}
.ws-chat-input{display:flex;gap:4px;padding:8px 10px;border-top:1px solid #eee;flex-shrink:0;background:#fafbfc}
.ws-chat-input input{flex:1;padding:8px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px;outline:none}
.ws-chat-input input:focus{border-color:#4a90d9}
.ws-chat-input button{background:#4a90d9;color:#fff;border:none;padding:7px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600}

/* Empty state */
.ws-empty{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:var(--muted);text-align:center;padding:30px}
.ws-empty-icon{font-size:40px;margin-bottom:10px;opacity:.2}
.ws-empty-title{font-size:15px;font-weight:600;color:var(--text3);margin-bottom:4px}
.ws-empty-desc{font-size:13px;line-height:1.4;max-width:340px}

/* ── Dark mode overrides ── */
[data-theme="dark"] .tab-bar{background:var(--bg2);border-bottom-color:var(--border)}
[data-theme="dark"] .content-panel{background:var(--bg)}
[data-theme="dark"] .chat-panel{background:var(--bg2);border-left-color:var(--border)}
[data-theme="dark"] .chat-header{background:var(--bg3);border-bottom-color:var(--border);color:var(--text)}
[data-theme="dark"] .chat .msg.user{background:#264f78;color:#ffffff;border:1px solid #37699e}
[data-theme="dark"] .chat .msg.assistant{background:var(--card-bg);color:var(--text2);border:1px solid var(--border)}
[data-theme="dark"] .input-bar{border-top-color:var(--border);background:var(--bg2)}
[data-theme="dark"] .input-bar input{background:var(--input-bg);color:var(--text);border-color:var(--input-border)}
[data-theme="dark"] .stack-row{background:var(--card-bg);border-color:var(--border);color:var(--text)}
[data-theme="dark"] .stack-row:hover{box-shadow:var(--shadow)}
[data-theme="dark"] .stack-row .sr-name{color:var(--text)}
[data-theme="dark"] .stack-row .sr-tag{background:var(--bg3);color:var(--accent)}
[data-theme="dark"] .stack-row .sr-status.stopped{background:var(--bg3);color:var(--muted)}
[data-theme="dark"] .stack-row .sr-status.running{background:#2a3a2a;color:var(--success)}
[data-theme="dark"] .stack-row.is-running{border-color:var(--success);background:#252e25}
[data-theme="dark"] .stack-row.is-running .sr-name{color:var(--success)}
[data-theme="dark"] .ind-card{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .ind-card:hover{box-shadow:var(--shadow)}
[data-theme="dark"] .ind-card.placeholder{opacity:.6}
[data-theme="dark"] .ind-card.placeholder .ind-card-title{color:var(--muted)}
[data-theme="dark"] .ind-card.is-running{background:#252e25;border-color:var(--success)}
[data-theme="dark"] .ind-card.is-running .ind-card-title{color:var(--success)}
[data-theme="dark"] .ind-cat-hdr.cat-ml{background:#252830}
[data-theme="dark"] .ind-cat-hdr.cat-sa{background:#2a2530}
[data-theme="dark"] .ind-cat-hdr.cat-sl{background:#242a28}
[data-theme="dark"] .pipe-section{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .pipe-section .ps-stack{background:var(--bg3);border-bottom-color:var(--border);color:var(--accent)}
[data-theme="dark"] .pipe-header{color:var(--text);border-bottom-color:var(--border)}
[data-theme="dark"] .pipe-header:hover{background:var(--bg3)}
[data-theme="dark"] .p-step{color:var(--text2);border-bottom-color:var(--border)}
[data-theme="dark"] .p-step:hover{background:var(--card-hover)}
[data-theme="dark"] .p-step.done{background:var(--bg3)}
[data-theme="dark"] .p-step.done.rerun{background:var(--card-hover)}
[data-theme="dark"] .p-step.failed{background:#3a2020}
[data-theme="dark"] .p-result{background:var(--bg);border-color:var(--border)}
[data-theme="dark"] .p-result pre{background:var(--code-bg);color:var(--text2)}
[data-theme="dark"] .plugin-card{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .plugin-card .pc-name{color:var(--text)}
[data-theme="dark"] .mon-table{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .mon-table th{background:var(--bg3);color:var(--text)}
[data-theme="dark"] .mon-table td{color:var(--text2);border-bottom-color:var(--border)}
[data-theme="dark"] .mon-table tr:hover td{background:var(--card-hover)}
[data-theme="dark"] .mon-bottom{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .mon-bottom .mon-header{background:var(--bg3);border-bottom-color:var(--border)}
[data-theme="dark"] .mon-bottom .mon-header .mon-title{color:var(--text)}
[data-theme="dark"] .mon-bottom .mon-card{border-bottom-color:var(--border);border-right-color:var(--border)}
[data-theme="dark"] .mon-bottom .mon-card .m-name{color:var(--text2)}
[data-theme="dark"] .log-viewer{background:var(--bg2);border-color:var(--border)}
[data-theme="dark"] .log-viewer .lv-body{color:var(--text2)}
[data-theme="dark"] .build-hint{background:var(--card-bg);border-color:var(--border);color:var(--text3)}
[data-theme="dark"] .limit-msg{background:#3a3520;border-color:#555020;color:var(--warning)}
[data-theme="dark"] .build-mode-toggle{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .build-mode-toggle span{color:var(--text3)}
[data-theme="dark"] .build-bar{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .build-bar .bb-name{background:var(--input-bg);color:var(--text);border-color:var(--input-border)}
[data-theme="dark"] .ws-layout{background:var(--bg)}
[data-theme="dark"] .ws-sidebar{background:var(--bg2);border-right-color:var(--border)}
[data-theme="dark"] .ws-main{background:var(--bg)}
[data-theme="dark"] .ws-sec{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .ws-sec-hdr{color:var(--text)}
[data-theme="dark"] .ws-row{border-bottom-color:var(--border)}
[data-theme="dark"] .ws-row:hover{background:var(--card-hover)}
[data-theme="dark"] .wr-name{color:var(--text)}
[data-theme="dark"] .wr-desc{color:var(--text3)}
[data-theme="dark"] .ws-chat{background:var(--bg2);border-left-color:var(--border)}
[data-theme="dark"] .ws-panel-sec{border-bottom-color:var(--border)}
[data-theme="dark"] .ws-panel-hdr{color:var(--text);background:var(--bg3);border-bottom-color:var(--border)}
[data-theme="dark"] .ws-panel-body{color:var(--text2)}
[data-theme="dark"] .ws-chat-msgs{background:var(--bg);border-color:var(--border);color:var(--text2)}
[data-theme="dark"] .ws-chat-msgs .msg.user{background:var(--chat-user-bg);color:var(--chat-user-text)}
[data-theme="dark"] .ws-chat-msgs .msg.assistant{background:var(--chat-bot-bg);color:var(--chat-bot-text)}
[data-theme="dark"] .ws-chat-input input{background:var(--input-bg);color:var(--text);border-color:var(--input-border)}
[data-theme="dark"] .ws-tab-bar{background:var(--bg2);border-bottom-color:var(--border)}
[data-theme="dark"] .ws-cred-row{color:var(--text3)}
[data-theme="dark"] .ws-cred-row b{color:var(--text)}
[data-theme="dark"] .sg-header{color:var(--text3)}
[data-theme="dark"] .sg-header:hover{color:var(--text)}
[data-theme="dark"] .sg-count{color:var(--muted)}
[data-theme="dark"] .uc-add-bar{background:var(--bg3);border-color:var(--border)}
[data-theme="dark"] .uc-builder{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .uc-builder-header{background:var(--bg3);border-bottom-color:var(--border)}
[data-theme="dark"] .chat .msg pre{background:var(--code-bg);color:var(--text2);border-color:var(--code-border)}
[data-theme="dark"] .chat .msg code{background:var(--code-bg);color:var(--text2)}
[data-theme="dark"] .content-scroll{color:var(--text)}
[data-theme="dark"] .typing{color:var(--muted)}
[data-theme="dark"] .ind-card-foot .sr-status.stopped{background:var(--bg3);color:var(--muted)}
[data-theme="dark"] .ind-card-foot .sr-status.running{background:#2a3a2a;color:var(--success)}
[data-theme="dark"] .ind-card-foot .sr-status.soon{color:var(--muted)}
[data-theme="dark"] .ind-card.placeholder{border-left-color:var(--border)}
[data-theme="dark"] .ind-card.placeholder:hover{border-color:var(--border);border-left-color:var(--border)}
[data-theme="dark"] .hdr{background:var(--header-bg)}
[data-theme="dark"] .mon-bottom-empty{color:var(--muted)}
[data-theme="dark"] .pipe-header .p-count{color:var(--muted)}
[data-theme="dark"] .cat-ml .ind-cat-title{color:#9cdcfe}
[data-theme="dark"] .cat-sa .ind-cat-title{color:#c586c0}
[data-theme="dark"] .cat-sl .ind-cat-title{color:#b5cea8}
[data-theme="dark"] .ind-card-title{color:#9cdcfe}
[data-theme="dark"] .ind-card.placeholder .ind-card-title{color:var(--text3)}
[data-theme="dark"] .ind-card-foot .sr-status.stopped{background:var(--bg3);color:var(--text3)}
[data-theme="dark"] .ind-deploy-btn{background:#0e639c}
[data-theme="dark"] .ws-sec{border-color:var(--border)}
[data-theme="dark"] .ws-sec-label{color:var(--text)}
[data-theme="dark"] .ws-sec-pm{color:var(--accent)}
[data-theme="dark"] .wr-open-badge{background:var(--bg3);color:var(--success)}
[data-theme="dark"] .ws-mb-btn{background:var(--accent)}
[data-theme="dark"] .ws-home{background:var(--bg)}
[data-theme="dark"] .ws-content{background:var(--bg);border-top-color:var(--border)}
[data-theme="dark"] .ws-list{background:var(--card-bg);border-color:var(--border)}
[data-theme="dark"] .wr-icon.app{background:#1e3a5f;color:#569cd6}
[data-theme="dark"] .wr-icon.term{background:#1e3a1e;color:#6a9955}
[data-theme="dark"] .ws-row.term-row:hover{background:var(--card-hover)}
[data-theme="dark"] .wr-url{background:var(--bg3);color:var(--text3)}
[data-theme="dark"] .wr-conn{background:var(--bg3);color:var(--success)}
[data-theme="dark"] .wr-cred{color:var(--text3)}
[data-theme="dark"] .ws-stack-btn{background:var(--card-bg);border-right-color:var(--border);border-color:var(--border)}
[data-theme="dark"] .ws-stack-btn:hover{background:var(--bg3)}
[data-theme="dark"] .ws-stack-btn.active{background:linear-gradient(135deg,#1e3a5f,#264f78)}
[data-theme="dark"] .ws-stack-sel{border-color:var(--border)}
[data-theme="dark"] .wsb-name{color:var(--text)}
[data-theme="dark"] .wsb-meta{color:var(--text3)}
[data-theme="dark"] .ws-sec-count{color:var(--text3)}
[data-theme="dark"] .ws-sec-pm:hover{background:var(--bg3);color:var(--text)}
[data-theme="dark"] .ws-empty-hint{color:var(--muted)}
[data-theme="dark"] .ws-empty-title{color:var(--text)}
[data-theme="dark"] .ws-empty-desc{color:var(--text3)}
[data-theme="dark"] .chat-header{color:var(--text)}
[data-theme="dark"] .chat-header button{background:var(--bg3);border-color:var(--border);color:var(--text3)}
[data-theme="dark"] .p-num{background:var(--bg3);color:var(--text3)}
[data-theme="dark"] .p-name{color:var(--text)}
</style>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/css/xterm.min.css">
</head>
<body>
<!-- Header -->
<div class="hdr">
  <div class="hdr-left">
    <h1>EDB Postgres® AI Blueprints</h1><span class="ver">v0.1rc8</span>
    <div class="running-bar" id="runningBar" style="display:none">
      <span class="dot"></span><span id="runningNames"></span><span class="slot" id="slotInfo"></span>
    </div>
  </div>
  <div class="nav">
    <label for="deployTargetSel" style="font-size:13px;color:var(--muted);margin-right:6px;font-weight:600">Select Infra:</label>
    <select id="deployTargetSel" onchange="setDeployTarget(this.value)" title="Pick where stacks will deploy" style="font-weight:600">
      <option value="">— select —</option>
      <option value="laptop-docker">Laptop · Docker Desktop</option>
      <option value="laptop-colima">Laptop · Colima</option>
      <option value="northflank">Northflank Cloud</option>
    </select>
    <span id="runtimeBadge" style="margin-left:6px;font-size:11px;color:var(--muted);display:none" title="Detected active Docker runtime"></span>
    <button class="theme-toggle" onclick="toggleTheme()" id="themeBtn" title="Toggle light/dark mode">&#9790; Dark</button>
    <button id="cleanupBtn" onclick="cleanupInfra()" title="Tear down everything for the selected infra. Use when Stop fails or to clear stale state." style="display:none">Cleanup</button>
    <button onclick="resetChat()">Reset</button>
    <button class="exit-btn" onclick="exitLab()">Exit</button>
  </div>
</div>

<!-- Tab bar -->
<div class="tab-bar">
  <div class="tab active" data-tab="stacks" onclick="switchTab('stacks')">Industry <span class="badge" id="stackBadge">0</span></div>
  <div class="tab" data-tab="workspace" onclick="switchTab('workspace')"><span id="wsTabLabel">Workspace</span> <span class="badge" id="wsBadge">0</span></div>
  <div class="tab" data-tab="synthdata" onclick="switchTab('synthdata')">Synthetic Data <span id="synthDot" style="display:none;width:6px;height:6px;border-radius:50%;background:#28a745;margin-left:4px;vertical-align:middle"></span></div>
</div>

<!-- Main split -->
<div class="main">
  <!-- Left: Tab content + monitoring bottom -->
  <div class="content-panel">
    <div class="content-scroll">
      <div class="tab-content active" id="tab-stacks"></div>
      <div class="tab-content" id="tab-synthdata"></div>
    </div>
    <div class="mon-bottom" id="monBottom">
      <div class="mon-header" onclick="refreshMonBottom()">
        <span class="mon-title"><span class="live-dot"></span> Containers</span>
        <button onclick="event.stopPropagation();refreshMonBottom()">refresh</button>
      </div>
      <div id="monBottomContent"><p class="mon-bottom-empty">No containers running</p></div>
    </div>
  </div>

  <!-- Right: Chat (always visible) -->
  <div class="chat-panel">
    <div class="chat-header">
      <span class="ch-title">Chat Agent</span>
      <button onclick="resetChat()">Clear</button>
    </div>
    <!-- credentials/access info moved to Workspace Home tab -->
    <!-- Pinned region: infra title + architecture card stick here so they stay visible while chat scrolls. -->
    <div class="chat-pinned" id="chatPinned"></div>
    <div class="chat" id="chat"></div>
    <div class="input-bar">
      <input type="text" id="input" placeholder="Deploy a stack, ask anything..." autofocus />
      <button id="sendBtn" onclick="send()">Send</button>
      <button id="stopBtn" class="stop-btn" onclick="stopStream()">Stop</button>
    </div>
  </div>

  <!-- Workspace Layout (hidden until Workspace tab is active) -->
  <div class="ws-layout" id="wsLayout">
    <div class="ws-body">
      <!-- Main workspace: no sidebar, tabbed apps + terminal -->
      <div class="ws-main">
        <div class="ws-tab-bar" id="wsTabBar">
          <div class="ws-tab-home active" data-wstab="home" onclick="wsSwitchTab('home')">Home</div>
          <div class="ws-tab-home" data-wstab="logs" onclick="wsSwitchTab('logs')">Logs</div>
        </div>
        <div class="ws-content" id="wsContent">
          <div class="ws-page active" id="ws-home">
            <div class="ws-home" id="wsHomeInner">
              <div class="ws-empty">
                <div class="ws-empty-icon">&#9881;</div>
                <div class="ws-empty-title">No stacks running</div>
                <div class="ws-empty-desc">Deploy a stack from the Industry tab to see its apps and terminals here.</div>
              </div>
            </div>
          </div>
          <div class="ws-page" id="ws-logs">
            <div class="ws-logs-layout">
              <div class="ws-logs-sidebar" id="wsLogsSidebar">
                <div class="ws-logs-side-h">Containers <button class="ws-logs-refresh" onclick="initLogsTab()" title="Refresh container list">&#8635;</button></div>
                <div class="ws-logs-list" id="wsLogsList"><div class="ws-logs-empty">Loading...</div></div>
              </div>
              <div class="ws-logs-pane">
                <div class="ws-logs-pane-h">
                  <span class="ws-logs-target" id="wsLogsTarget">Select a container</span>
                  <div class="ws-logs-toolbar">
                    <button id="wsLogsPause" onclick="toggleLogsPause()" disabled>Pause</button>
                    <button id="wsLogsClear" onclick="clearLogsView()" disabled>Clear</button>
                    <button id="wsLogsSave" onclick="saveLogsView()" disabled>Save</button>
                    <input id="wsLogsFilter" type="text" placeholder="filter (regex)" oninput="applyLogsFilter()" disabled>
                  </div>
                </div>
                <pre class="ws-logs-view" id="wsLogsView">No container selected.</pre>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Chat resize handle -->
      <div class="ws-chat-resize" id="wsChatResize"></div>

      <!-- Right panel: VS Code-style expandable sections -->
      <div class="ws-chat" id="wsChat">
        <div class="ws-chat-strip" onclick="wsToggleChat()">
          <span class="cps-icon">&#9666;</span>
          <span class="cps-dot"></span>
          <span class="cps-label">Panel</span>
        </div>
        <div class="ws-panel-sections">
          <!-- Industry Cases section -->
          <div class="ws-panel-sec" id="wsPanelUC">
            <div class="ws-panel-hdr" onclick="wsTogglePanel('wsPanelUC')">
              <span class="ws-panel-chev">&#9662;</span>
              <span class="ws-panel-title">Industry Cases</span>
              <span class="badge" id="pipelineBadge" style="margin-left:auto">0</span>
            </div>
            <div class="ws-panel-body" id="wsPanelUCBody"></div>
          </div>
          <!-- Monitoring section -->
          <div class="ws-panel-sec" id="wsPanelMon">
            <div class="ws-panel-hdr" onclick="wsTogglePanel('wsPanelMon')">
              <span class="ws-panel-chev">&#9662;</span>
              <span class="ws-panel-title">Monitoring</span>
            </div>
            <div class="ws-panel-body" id="wsPanelMonBody"></div>
          </div>
          <!-- Chat section (open by default) -->
          <div class="ws-panel-sec open" id="wsPanelChat" style="flex:1;display:flex;flex-direction:column;min-height:0">
            <div class="ws-panel-hdr" onclick="wsTogglePanel('wsPanelChat')">
              <span class="ws-panel-chev">&#9662;</span>
              <span class="ws-panel-title">Chat Agent</span>
              <span class="wch-dot" style="margin-left:6px"></span>
            </div>
            <div class="ws-panel-body" style="flex:1;display:flex;flex-direction:column;min-height:0">
              <div class="ws-chat-msgs" id="wsChatMsgs"></div>
              <div class="ws-chat-input">
                <input type="text" id="wsChatInput" placeholder="Ask anything..." onkeydown="if(event.key==='Enter')wsSendChat()" />
                <button onclick="wsSendChat()">Send</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-fit@0.10.0/lib/addon-fit.min.js"></script>
<script>
// ── API key auth ──────────────────────────────────────────────────────────
// The agent's /api/* routes (and the /ws/* sockets) require AGENT_API_KEY.
// The key is NEVER embedded in this page (that would hand it to any network
// user who can load the UI); the operator enters it once and it is kept only
// in this browser's localStorage, attached as a Bearer header on every /api/*
// request and inside the first frame of each WebSocket.
let _keyPromptCooldown=0;
function _getApiKey(){ return (localStorage.getItem('diab-api-key')||'').trim(); }
function _promptForApiKey(msg){
  const now=Date.now();
  if(now<_keyPromptCooldown) return _getApiKey();   // avoid prompt storms from pollers
  _keyPromptCooldown=now+2000;
  const k=window.prompt(msg||'Enter AGENT_API_KEY to use this agent:','');
  if(k!==null && k.trim()) localStorage.setItem('diab-api-key',k.trim());
  return _getApiKey();
}
(function(){
  const _origFetch=window.fetch.bind(window);
  window.fetch=function(input,init){
    const url=(typeof input==='string')?input:((input&&input.url)||'');
    const isApi=url.startsWith('/api/')||url.startsWith(location.origin+'/api/');
    if(!isApi) return _origFetch(input,init);
    init=init?Object.assign({},init):{};
    const h=new Headers(init.headers||{});
    const key=_getApiKey();
    if(key && !h.has('Authorization')) h.set('Authorization','Bearer '+key);
    init.headers=h;
    return _origFetch(input,init).then(function(resp){
      if(resp.status!==401) return resp;
      const nk=_promptForApiKey('API key missing or rejected. Enter AGENT_API_KEY:');
      if(!nk) return resp;
      const h2=new Headers(init.headers||{}); h2.set('Authorization','Bearer '+nk);
      return _origFetch(input,Object.assign({},init,{headers:h2}));
    });
  };
  if(!_getApiKey()) _promptForApiKey();   // ask up front so the first poll is authenticated
})();
const chatEl=document.getElementById('chat'),inputEl=document.getElementById('input'),sendBtn=document.getElementById('sendBtn'),stopBtn=document.getElementById('stopBtn');
let stackData={stacks:{},plugins:{}};
let abortController=null;

// ── Theme toggle ──
function toggleTheme(){
  const html=document.documentElement;
  const isDark=html.getAttribute('data-theme')==='dark';
  html.setAttribute('data-theme',isDark?'light':'dark');
  localStorage.setItem('diab-theme',isDark?'light':'dark');
  const btn=document.getElementById('themeBtn');
  if(btn)btn.innerHTML=isDark?'&#9790; Dark':'&#9788; Light';
}
(function(){
  const saved=localStorage.getItem('diab-theme');
  if(saved==='dark'){
    document.documentElement.setAttribute('data-theme','dark');
    const btn=document.getElementById('themeBtn');
    if(btn)btn.innerHTML='&#9788; Light';
  }
})();

function lockUI(){sendBtn.disabled=true;inputEl.disabled=true;stopBtn.classList.add('active')}
function unlockUI(){sendBtn.disabled=false;inputEl.disabled=false;stopBtn.classList.remove('active');inputEl.focus()}
function stopStream(){if(abortController){abortController.abort();abortController=null}}

// ── Tabs ──
let _currentTab='stacks';
function switchTab(name){
  // Save pipeline state when leaving pipelines tab
  if(_currentTab==='pipelines' && name!=='pipelines'){
    savePipelineState();
  }
  _currentTab=name;
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.toggle('active',t.id==='tab-'+name));
  const mainEl=document.querySelector('.main');
  if(name==='workspace'){
    mainEl.classList.add('ws-active');
    // Only re-render workspace home if it's empty (first visit) — otherwise
    // preserve current state (open/closed log entries, dropdown state, etc.).
    // wsRefreshSidebar() will diff and re-render only when something changes.
    const wsCt = document.getElementById('wsHomeInner');
    if(!wsCt || !wsCt.innerHTML.trim()){
      wsRenderHome();
    }
    wsRefreshSidebar(); // background diff-refresh
  } else {
    mainEl.classList.remove('ws-active');
  }
  if(name==='workspace'){refreshPipelinesTab()}
  if(name==='synthdata')initSynthData();
  // Logs stream is scoped to a workspace sub-tab. Leaving Workspace closes
  // any active log WebSocket.
  if(name!=='workspace')stopLogStream();
}

// ── Monitoring Tab ──
let activeLogContainer=null;

async function refreshMonTab(){
  const ct=document.getElementById('wsPanelMonBody')||document.getElementById('tab-monitoring');
  if(!ct)return;
  try{
    const r=await fetch('/api/monitoring');
    const d=await r.json();
    if(!d.containers||!d.containers.length){
      ct.innerHTML='<p class="mon-empty">No running containers</p>';
      return;
    }
    let h='<div style="padding:12px 14px 0"><h3 style="font-size:15px;font-weight:600;color:var(--text);margin:0 0 4px">Monitoring</h3><p style="font-size:11px;color:var(--muted);margin:0 0 8px">Live container stats and logs for running integrations.</p></div>';
    h+='<div class="mon-tab-header"><span><span class="live-dot"></span> Auto-refresh every 5s</span><button onclick="refreshMonTab()">Refresh Now</button></div>';
    h+='<table class="mon-table"><tr><th>Container</th><th>CPU</th><th>Memory</th><th>Net I/O</th><th>PIDs</th><th>Logs</th></tr>';
    for(const c of d.containers){
      const cpu=parseFloat(c.cpu)||0;const mem=parseFloat(c.mem_pct)||0;
      const cpuCls=cpu<30?'low':cpu<70?'med':'high';
      const memCls=mem<50?'low':mem<80?'med':'high';
      const isActive=activeLogContainer===c.name;
      h+='<tr><td><strong>'+c.name+'</strong></td>';
      h+='<td><div class="bar-wrap"><div class="bar '+cpuCls+'" style="width:'+Math.min(cpu,100)+'%"></div></div> '+c.cpu+'</td>';
      h+='<td><div class="bar-wrap"><div class="bar '+memCls+'" style="width:'+Math.min(mem,100)+'%"></div></div> '+c.mem+'</td>';
      h+='<td>'+c.net+'</td><td>'+c.pids+'</td>';
      h+='<td><button class="log-btn'+(isActive?' active':'')+'" onclick="toggleLogs(\''+c.name+'\')">tail</button></td></tr>';
    }
    h+='</table>';
    h+='<div class="log-viewer'+(activeLogContainer?' open':'')+'" id="logViewer">';
    h+='<div class="lv-header"><span class="lv-title" id="logTitle">'+(activeLogContainer||'')+'</span>';
    h+='<div class="lv-actions"><button onclick="refreshLogs()">refresh</button><button onclick="closeLogs()">close</button></div></div>';
    h+='<div class="lv-body" id="logBody">Click "tail" on a container to view logs</div></div>';
    ct.innerHTML=h;
    if(activeLogContainer)refreshLogs();
  }catch(e){ct.innerHTML='<p class="mon-empty">Error loading monitoring data</p>'}
}

async function toggleLogs(name){
  if(activeLogContainer===name){closeLogs();return}
  activeLogContainer=name;
  document.querySelectorAll('.log-btn').forEach(b=>{b.classList.toggle('active',b.closest('tr').querySelector('strong').textContent===name)});
  const viewer=document.getElementById('logViewer');
  const title=document.getElementById('logTitle');
  const body=document.getElementById('logBody');
  if(viewer)viewer.classList.add('open');
  if(title)title.textContent=name;
  if(body)body.textContent='Loading...';
  await refreshLogs();
}

async function refreshLogs(){
  if(!activeLogContainer)return;
  const body=document.getElementById('logBody');
  if(!body)return;
  try{
    const r=await fetch('/api/logs/'+activeLogContainer+'?lines=50');
    const d=await r.json();
    body.textContent=d.logs||'No logs available';
    body.scrollTop=body.scrollHeight;
  }catch(e){body.textContent='Error: '+e.message}
}

function closeLogs(){
  activeLogContainer=null;
  const viewer=document.getElementById('logViewer');
  if(viewer)viewer.classList.remove('open');
  document.querySelectorAll('.log-btn').forEach(b=>b.classList.remove('active'));
}

// ── Monitoring Bottom Panel ──
async function refreshMonBottom(){
  const ct=document.getElementById('monBottomContent');
  try{
    const r=await fetch('/api/monitoring');
    const d=await r.json();
    if(!d.containers||!d.containers.length){ct.innerHTML='<p class="mon-bottom-empty">No containers running</p>';return}
    let h='<div class="mon-grid">';
    for(const c of d.containers){
      h+='<div class="mon-card">';
      h+='<span class="h-dot"></span>';
      h+='<span class="m-name">'+c.name+'</span>';
      h+='<span class="m-stats">'+c.cpu+' | '+c.mem+'</span>';
      h+='</div>';
    }
    h+='</div>';
    ct.innerHTML=h;
  }catch(e){ct.innerHTML='<p class="mon-bottom-empty">Error loading</p>'}
}

// ── Stack Info Panel ──
function refreshStackInfo(){}

// ── Running state ──
let runningStacks=[];
let _deployTargets={};        // stack_name -> 'laptop-docker' | 'laptop-colima' | 'northflank'
let _nfConsoleUrl=null;       // populated from /api/running when NF integration is configured
let _dockerRuntime='unknown'; // 'docker-desktop' | 'colima' | 'unknown' | 'unavailable'
let _hostOs='other';          // 'macos' | 'windows' | 'linux' | 'other' — set from /api/runtime
let _colimaSupported=true;    // false on Windows; gates the Colima dropdown option
let currentDeployTarget='';  // user choice from header dropdown — empty = nothing selected
const VALID_TARGETS=['laptop-docker','laptop-colima','northflank'];
function setDeployTarget(v){
  currentDeployTarget = VALID_TARGETS.includes(v) ? v : '';
  try{ localStorage.setItem('diabDeployTarget', currentDeployTarget); }catch(e){}
  // Toggle Deploy buttons across all stack cards based on selection.
  updateDeployButtons();
  // Refresh the dropdown's own colour (green when selected, amber when locked).
  if(typeof updateInfraLock === 'function') updateInfraLock();
  // Keep the header Cleanup button label/visibility in sync with the
  // selected infra (Cleanup NF vs Cleanup Local, or hidden if none).
  if(typeof updateCleanupBtn === 'function') updateCleanupBtn();
  // Remove any previous infra-related messages so the chat shows ONLY the
  // current selection's title + preflight + architecture frame (no stacking).
  clearInfraMessages();
  if(!currentDeployTarget){
    addMsg('assistant','Please select an infra target to see prerequisites and enable Deploy.');
    return;
  }
  renderInfraTitle(currentDeployTarget);
  renderPreflight(currentDeployTarget);
}
function updateDeployButtons(){
  const ok = !!currentDeployTarget;
  document.querySelectorAll('.ind-deploy-btn').forEach(btn=>{
    // Don't override stop / deploying / stopping states — only manage the "Deploy" state.
    const card = btn.closest('.ind-card,.stack-row');
    const isRunning = card && card.classList.contains('is-running');
    if(isRunning) return;
    btn.disabled = !ok;
    btn.style.cursor = ok ? 'pointer' : 'not-allowed';
    btn.style.opacity = ok ? '' : '0.5';
    btn.title = ok ? '' : 'Select an infra target first';
  });
}

async function setupBedrockFromChat(ev){
  const btn=ev.target;
  btn.disabled=true; btn.textContent='Logging in...';
  addMsg('assistant','Opening browser for AWS SSO login. Click **Allow** in the browser tab. This may take 30-60 seconds.');
  try{
    const r=await fetch('/api/aws/setup-bedrock',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({profile:'Bedrock'})});
    const d=await r.json();
    if(d.ok){
      addMsg('assistant','✓ '+d.message+' Keys written: '+(d.keys_set||[]).join(', '));
      renderPreflight(currentDeployTarget);
    }else{
      addMsg('assistant','**Bedrock setup failed:** '+d.message);
      btn.disabled=false; btn.textContent='Set up Bedrock credentials';
    }
  }catch(e){
    addMsg('assistant','**Bedrock setup error:** '+e);
    btn.disabled=false; btn.textContent='Set up Bedrock credentials';
  }
}

// Track per-target messages so we can replace (not stack) on re-selection.
function clearInfraMessages(){
  document.querySelectorAll('.msg-infra-title, .msg-preflight, .msg-bedrock-btn, .msg-architecture, .msg-stack-arch, .msg-next-steps').forEach(el=>el.remove());
}
function clearStackArchMessage(){
  document.querySelectorAll('.msg-stack-arch').forEach(el=>el.remove());
}
function clearNextStepsMessage(){
  document.querySelectorAll('.msg-next-steps').forEach(el=>el.remove());
}

const INFRA_TITLES = {
  'laptop-docker':  {title:'Deploying stacks via Docker Compose on your laptop', sub:'Everything runs on this Mac. No cloud, no external dependencies.'},
  'laptop-colima':  {title:'Deploying stacks via Colima on your laptop',          sub:'Same compose flow as Docker Desktop, but the daemon runs inside a Colima VM.'},
  'northflank':     {title:'Deploying stacks on Northflank Cloud (BYOC)',         sub:'Cluster runs in your AWS account. Northflank orchestrates, never holds your data.'},
};

function renderInfraTitle(target){
  const t = INFRA_TITLES[target];
  if(!t) return;
  const archTitleMap={
    'laptop-docker':'How stacks deploy on your laptop (Docker Desktop)',
    'laptop-colima':'How stacks deploy on your laptop (Colima)',
    'northflank':   'How stacks deploy on Northflank Cloud (BYOC)',
  };
  const archTitle=archTitleMap[target]||('Architecture — '+target);
  const archTitleAttr=archTitle.replace(/'/g,'&#39;');
  const h='<div style="border-left:4px solid #2563eb;background:#eff6ff;padding:10px 14px;border-radius:6px;margin:6px 0;display:flex;align-items:center;justify-content:space-between;gap:12px">'
    +'<div style="min-width:0">'
    +'<div style="font-size:18px;font-weight:800;color:#1e3a8a;line-height:1.2">'+t.title+'</div>'
    +'<div style="font-size:12px;color:#3b82f6;margin-top:4px">'+t.sub+'</div>'
    +'</div>'
    +'<button onclick="openArchModal(\''+target+'\',\''+archTitleAttr+'\')" style="background:#0f172a;color:#fff;border:0;padding:9px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;flex-shrink:0">View diagram ↗</button>'
    +'</div>';
  const el=addPinnedHtml('assistant',h);
  if(el) el.classList.add('msg-infra-title');
}

async function renderPreflight(target){
  // Plain-text summary in chat.
  try{
    const r=await fetch('/api/preflight/'+encodeURIComponent(target));
    const d=await r.json();
    const checks=d.checks||[];
    const cfg=d.config||{};
    const missing=d.missing_guidance||[];
    const labelMap={'laptop-docker':'Laptop · Docker Desktop','laptop-colima':'Laptop · Colima','northflank':'Northflank Cloud'};
    const label=labelMap[target]||target;
    const okCount=checks.filter(c=>c.status==='ok').length;
    const warnCount=checks.filter(c=>c.status==='warn').length;
    const failCount=checks.filter(c=>c.status==='fail').length;
    const ico={ok:'✓',warn:'⚠',fail:'✗'};

    let msg='**Infra selected: '+label+'**  '+(d.ok?'`READY`':'`BLOCKED`')+'\n\n';
    msg+='Prerequisites — '+checks.length+' checks: '+okCount+' set ✓ · '+warnCount+' warning ⚠ · '+failCount+' missing ✗\n\n';
    msg+='```\n';
    for(const c of checks){
      msg+=ico[c.status]+' '+c.name.padEnd(34)+'  '+c.detail+'\n';
    }
    msg+='```\n';
    if(Object.keys(cfg).length){
      msg+='\n**Active config (semi-masked):**\n```\n';
      for(const k in cfg){msg+='  '+k.padEnd(20)+'  '+(cfg[k]||'(empty)')+'\n';}
      msg+='```\n';
    }
    if(missing.length){
      msg+='\n**To unblock ('+missing.length+'):**\n';
      for(let i=0;i<missing.length;i++){msg+=(i+1)+'. '+missing[i]+'\n';}
    }
    const el=addMsg('assistant',msg);
    if(el) el.classList.add('msg-preflight');

    // Bedrock self-service button (appended separately so it actually clicks).
    const needsBedrock=checks.some(c=>c.name==='Bedrock credentials' && c.status!=='ok');
    if(needsBedrock){
      const btn=addMsgHtml('assistant','<button onclick="setupBedrockFromChat(event)" style="background:#7c3aed;color:#fff;border:0;padding:8px 14px;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">Set up Bedrock credentials</button> <span style="font-size:12px;color:#64748b;margin-left:8px">opens browser for AWS SSO, writes AWS_BEDROCK_* to .env</span>');
      if(btn) btn.classList.add('msg-bedrock-btn');
    }
  }catch(e){
    addMsg('assistant','Preflight check failed: '+e);
  }
}

function renderArchitecture(target){
  // Compact frame in chat with a button that opens the full-size diagram in a modal.
  const archTitleMap={
    'laptop-docker':'How stacks deploy on your laptop (Docker Desktop)',
    'laptop-colima':'How stacks deploy on your laptop (Colima)',
    'northflank':   'How stacks deploy on Northflank Cloud (BYOC)',
  };
  const title=archTitleMap[target]||('Architecture — '+target);
  const h='<div style="border:2px solid var(--border,#cbd5e1);border-radius:10px;padding:14px;background:#ffffff;margin:8px 0;display:flex;align-items:center;justify-content:space-between;gap:12px">'
    +'<div>'
    +'<div style="font-size:14px;font-weight:700;color:#0f172a">'+title+'</div>'
    +'<div style="font-size:12px;color:#64748b;margin-top:4px">High-level diagram of where the deploy runs and how data flows.</div>'
    +'</div>'
    +'<button onclick="openArchModal(\''+target+'\',\''+title.replace(/\'/g,"&#39;")+'\')" style="background:#0f172a;color:#fff;border:0;padding:10px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap">View diagram ↗</button>'
    +'</div>';
  const el=addPinnedHtml('assistant',h);
  if(el) el.classList.add('msg-architecture');
}

function openArchModal(target,label){
  // Full-screen modal showing the SVG at maximum readable size.
  const existing=document.getElementById('archModal');
  if(existing) existing.remove();
  const m=document.createElement('div');
  m.id='archModal';
  m.style.cssText='position:fixed;inset:0;background:rgba(15,23,42,.78);z-index:99999;display:flex;align-items:center;justify-content:center;padding:20px';
  m.onclick=function(e){if(e.target===m) m.remove();};
  m.innerHTML=
    '<div style="background:#fff;border-radius:12px;max-width:96vw;max-height:96vh;width:1500px;display:flex;flex-direction:column;box-shadow:0 25px 50px rgba(0,0,0,.25)">'
    +'<div style="padding:14px 20px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between">'
    +'<div style="font-size:16px;font-weight:700;color:#0f172a">'+label+'</div>'
    +'<button onclick="document.getElementById(\'archModal\').remove()" style="background:transparent;border:0;font-size:22px;cursor:pointer;color:#64748b;line-height:1;padding:4px 10px">×</button>'
    +'</div>'
    +'<div style="padding:20px;overflow:auto;flex:1">'
    +'<img src="/assets/arch-'+target+'.svg" alt="Architecture for '+label+'" style="width:100%;height:auto;display:block"/>'
    +'</div>'
    +'<div style="padding:10px 20px;border-top:1px solid #e2e8f0;font-size:12px;color:#64748b;text-align:right">Click outside or press × to close</div>'
    +'</div>';
  document.body.appendChild(m);
  // ESC closes
  const onkey=function(e){if(e.key==='Escape'){m.remove();document.removeEventListener('keydown',onkey);}};
  document.addEventListener('keydown',onkey);
}

// Render a compact pinned card for the stack architecture diagram. Only shows
// if /assets/stack-arch-<stackId>.svg exists (probed via HEAD). Re-renders
// replace the previous stack-arch card so the pinned region stays compact.
async function renderStackArch(stackId, label){
  if(!stackId) return;
  // Probe for an asset; silently no-op if the stack has no diagram shipped.
  // Use GET (HEAD is rejected by @app.get with 405) but don't await the body.
  try{
    const probe=await fetch('/assets/stack-arch-'+stackId+'.svg',{method:'GET'});
    if(!probe.ok) return;
  }catch(e){ return; }
  clearStackArchMessage();
  const safeLabel=(label||stackId).replace(/'/g,'&#39;');
  const h='<div style="border-left:4px solid #0f172a;background:#f8fafc;padding:10px 14px;border-radius:6px;margin:6px 0;display:flex;align-items:center;justify-content:space-between;gap:12px">'
    +'<div style="min-width:0">'
    +'<div style="font-size:14px;font-weight:700;color:#0f172a;line-height:1.2">Stack: '+safeLabel+'</div>'
    +'<div style="font-size:11px;color:#64748b;margin-top:3px">OLTP &middot; CDC &middot; OLAP &middot; ML &mdash; click for the full pipeline diagram.</div>'
    +'</div>'
    +'<button onclick="openStackArchModal(\''+stackId+'\',\''+safeLabel+'\')" style="background:#0f172a;color:#fff;border:0;padding:9px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;flex-shrink:0">View stack diagram ↗</button>'
    +'</div>';
  const el=addPinnedHtml('assistant',h);
  if(el) el.classList.add('msg-stack-arch');
}

// Pinned post-deploy guidance: numbered list of the stack's use cases pulled
// from /api/pipelines, so the order stays in sync with stack.yaml without
// hard-coding stack-specific content here.
async function renderNextSteps(stackId, label){
  if(!stackId) return;
  // Idempotent: refreshRunning calls this every 5s while the stack is up.
  // Skip if a card for this stack is already in the chat (in pinned area or
  // scrollable history — same .msg-next-steps[data-stack=...] selector).
  const existing=document.querySelector('.msg-next-steps[data-stack="'+stackId+'"]');
  if(existing) return;
  try{
    const resp=await fetch('/api/pipelines');
    if(!resp.ok) return;
    const data=await resp.json();
    const stackData=data && data.stacks && data.stacks[stackId];
    const pipelines=stackData && stackData.pipelines;
    if(!pipelines || !pipelines.length) return;
    const items=pipelines.map(p=>{
      const nm=String(p.name||p.id||'').replace(/[<>&'"]/g, c=>({'<':'&lt;','>':'&gt;','&':'&amp;',"'":'&#39;','"':'&quot;'}[c]));
      return '<li style="margin:2px 0">'+nm+'</li>';
    }).join('');
    const safeLabel=String(label||stackId).replace(/'/g,'&#39;');
    const h='<div style="border-left:4px solid #16a34a;background:#f0fdf4;padding:10px 14px;border-radius:6px;margin:6px 0">'
      +'<div style="font-size:14px;font-weight:700;color:#14532d;line-height:1.2">Deployment complete &mdash; follow these steps to begin</div>'
      +'<div style="font-size:11px;color:#15803d;margin-top:3px">'+safeLabel+' is up. Open the Workspace tab and run these in order. Each Start Service locks after success; Check Status is re-runnable.</div>'
      +'<ol style="margin:8px 0 0 22px;padding:0;font-size:12px;color:#166534;line-height:1.55">'+items+'</ol>'
      +'</div>';
    // Render in the scrollable chat (after any prior deploy messages), not in
    // the pinned region — the pinned region renders ABOVE all chat history,
    // which made the card appear before the "Containers running" reply.
    const el=addMsgHtml('assistant',h);
    if(el){
      el.classList.add('msg-next-steps');
      el.setAttribute('data-stack', stackId);
    }
  }catch(e){}
}

function openStackArchModal(stackId,label){
  const existing=document.getElementById('archModal');
  if(existing) existing.remove();
  const m=document.createElement('div');
  m.id='archModal';
  m.style.cssText='position:fixed;inset:0;background:rgba(15,23,42,.78);z-index:99999;display:flex;align-items:center;justify-content:center;padding:20px';
  m.onclick=function(e){if(e.target===m) m.remove();};
  m.innerHTML=
    '<div style="background:#fff;border-radius:12px;max-width:96vw;max-height:96vh;width:1500px;display:flex;flex-direction:column;box-shadow:0 25px 50px rgba(0,0,0,.25)">'
    +'<div style="padding:14px 20px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between">'
    +'<div style="font-size:16px;font-weight:700;color:#0f172a">'+label+' &mdash; Stack architecture</div>'
    +'<button onclick="document.getElementById(\'archModal\').remove()" style="background:transparent;border:0;font-size:22px;cursor:pointer;color:#64748b;line-height:1;padding:4px 10px">×</button>'
    +'</div>'
    +'<div id="stackArchBody" style="padding:20px;overflow:auto;flex:1;min-height:70vh;background:#fff">'
    +'<div style="color:#64748b;font-size:13px">Loading diagram…</div>'
    +'</div>'
    +'<div style="padding:10px 20px;border-top:1px solid #e2e8f0;font-size:12px;color:#64748b;text-align:right">Click outside or press × to close</div>'
    +'</div>';
  document.body.appendChild(m);
  // Inline the SVG so its viewBox sizes correctly inside the flex body (img tags
  // can collapse when intrinsic dimensions race with the parent's layout).
  fetch('/assets/stack-arch-'+stackId+'.svg').then(r=>r.ok?r.text():Promise.reject(r.status)).then(svg=>{
    const body=document.getElementById('stackArchBody');
    if(!body) return;
    // Strip the XML prolog (IE-era; not valid inside HTML) and inject.
    body.innerHTML=svg.replace(/^<\?xml[^?]*\?>\s*/,'');
    const svgEl=body.querySelector('svg');
    if(svgEl){
      svgEl.removeAttribute('width');
      svgEl.removeAttribute('height');
      svgEl.style.width='100%';
      svgEl.style.height='auto';
      svgEl.style.display='block';
    }
  }).catch(err=>{
    const body=document.getElementById('stackArchBody');
    if(body) body.innerHTML='<div style="color:#b91c1c;font-size:13px">Failed to load diagram: '+err+'</div>';
  });
  const onkey=function(e){if(e.key==='Escape'){m.remove();document.removeEventListener('keydown',onkey);}};
  document.addEventListener('keydown',onkey);
}
// No restore. Each session starts with an empty selection so the user
// makes a deliberate choice. Clear any stale value left by older versions.
try{ localStorage.removeItem('diabDeployTarget'); }catch(e){}
async function loadRuntime(){
  try{
    const r=await fetch('/api/runtime');
    const d=await r.json();
    _dockerRuntime=d.docker_runtime||'unknown';
    _hostOs = d.host_os || 'other';
    _colimaSupported = d.colima_supported !== false;
    // Hide Laptop · Colima on Windows (or anywhere Colima isn't available).
    // Same dropdown is used for first-time selection and for switching infra
    // between deploys, so do this every refresh — not just first load.
    const colimaOpt = document.querySelector('#deployTargetSel option[value="laptop-colima"]');
    if(colimaOpt){
      if(_colimaSupported){
        colimaOpt.hidden = false;
        colimaOpt.disabled = false;
      } else {
        colimaOpt.hidden = true;
        colimaOpt.disabled = true;
        // If Colima was somehow selected (e.g. stale localStorage), reset.
        if(currentDeployTarget === 'laptop-colima'){
          currentDeployTarget = '';
          const sel = document.getElementById('deployTargetSel');
          if(sel) sel.value = '';
        }
      }
    }
    const badge=document.getElementById('runtimeBadge');
    if(badge && _dockerRuntime!=='unavailable' && _dockerRuntime!=='unknown'){
      badge.style.display='inline';
      badge.textContent='('+_dockerRuntime+' active)';
    }
    // Sync the Cleanup button label/visibility to the current infra selection
    // (NF Cleanup vs Local Cleanup, or hidden if no infra picked).
    if(typeof updateCleanupBtn === 'function') updateCleanupBtn();
  }catch(e){}
}
const MAX_RUNNING=2;

// Freeze the infra dropdown while any stack is deployed OR while a deploy
// is in flight. User must stop everything before switching infra (mixing
// laptop and NF deploys in the same session leads to confusing state).
function updateInfraLock(){
  const sel=document.getElementById('deployTargetSel');
  if(!sel) return;
  const inFlight = window._inFlightDeploys && window._inFlightDeploys.size > 0;
  const anyRunning = (runningStacks && runningStacks.length > 0);
  const lock = inFlight || anyRunning;
  const hasSelection = !!currentDeployTarget;
  sel.disabled = lock;
  sel.style.cursor = lock ? 'not-allowed' : '';
  // Three visual states, all using inline styles so they survive theme switches:
  //   - locked (stack running / in-flight): amber tint + bold border, signals "in use".
  //   - selected + unlocked: green tint, signals "ready, click Deploy on a stack".
  //   - empty: neutral, default styling.
  if(lock){
    sel.style.background    = '#fef3c7';   // amber-100
    sel.style.color         = '#78350f';   // amber-900
    sel.style.borderColor   = '#f59e0b';   // amber-500
    sel.style.borderWidth   = '2px';
    sel.style.borderStyle   = 'solid';
    sel.style.opacity       = '1';
    sel.title = 'Infra is locked while a stack is deployed. Stop the stack to change infra.';
  } else if(hasSelection){
    sel.style.background    = '#dcfce7';   // green-100
    sel.style.color         = '#14532d';   // green-900
    sel.style.borderColor   = '#22c55e';   // green-500
    sel.style.borderWidth   = '2px';
    sel.style.borderStyle   = 'solid';
    sel.style.opacity       = '1';
    sel.title = 'Selected: ' + currentDeployTarget + ' — pick a stack to deploy.';
  } else {
    sel.style.background    = '';
    sel.style.color         = '';
    sel.style.borderColor   = '';
    sel.style.borderWidth   = '';
    sel.style.borderStyle   = '';
    sel.style.opacity       = '';
    sel.title = 'Pick where stacks will deploy';
  }
}

async function refreshRunning(){
  try{
    const r=await fetch('/api/running');
    const d=await r.json();
    runningStacks=d.running||[];
    window._wsRunning=runningStacks;
    _deployTargets=d.deploy_targets||{};
    _nfConsoleUrl=d.nf_console_url||null;
    const slotCount=d.count||0;
    _runningSlotCount=slotCount;
    updateInfraLock();
    // Update header bar
    const bar=document.getElementById('runningBar');
    const names=document.getElementById('runningNames');
    const slot=document.getElementById('slotInfo');
    if(slotCount>0){
      bar.style.display='flex';
      // Show only unique project names (filter out folder aliases)
      const uniqueNames=runningStacks.filter((v,i)=>i===0||!runningStacks.slice(0,i).some(prev=>prev.includes(v)||v.includes(prev)));
      names.textContent=uniqueNames.join(', ');
      slot.textContent='('+slotCount+'/'+MAX_RUNNING+' slots)';
    }else{bar.style.display='none'}
    // Update stack rows
    const atLimit=slotCount>=MAX_RUNNING;
    document.querySelectorAll('.stack-row[data-stack],.ind-card[data-stack]:not(.placeholder)').forEach(el=>{
      const name=el.dataset.stack;
      const isRunning=runningStacks.some(p=>p.includes(name)||name.includes(p));
      el.classList.toggle('is-running',isRunning);
      el.classList.toggle('is-disabled',!isRunning&&atLimit);
      // NOTE: deploy success / Workspace auto-switch is handled in the SSE
      // completion handler (~line 4030, search "_pendingSwitchToWorkspace")
      // — NOT here. refreshRunning sees a stack as "running" the moment the
      // first container starts, which is well before `docker compose up`
      // returns. Triggering the switch here causes it to fire mid-deploy.
      if(isRunning){
        // Stack appeared in /api/running — the deploy is *actually* done.
        // Clear the in-flight flag + deploying class so the button transitions
        // from "Deploying…" to "Stop". The SSE handler in send() used to do
        // this, but it fired when the chat response ended (often before
        // containers were fully up), so a re-click could trigger another
        // docker compose up.
        if(window._inFlightDeploys && window._inFlightDeploys.has(name)){
          window._inFlightDeploys.delete(name);
        }
        el.classList.remove('is-deploying');
        // Next-steps card is rendered from the SSE completion handler (search
        // "deploySucceeded"), not here — refreshRunning fires the moment
        // /api/running shows the stack, which is *before* the AI has finished
        // narrating the deploy. Rendering here puts the card above the AI's
        // final reply, which looks wrong.
      } else {
        // Stack is no longer running — clear stopping flag if any
        if(window._inFlightStops) window._inFlightStops.delete(name);
        // Don't strip is-deploying while a deploy is in flight. refreshRunning
        // polls every 5s and sees "not running" for the first 30-60s of any
        // deploy (containers spinning up before they show up in `/api/running`).
        // The authoritative signal for "deploy done" is the SSE completion
        // handler (~line 4030). Without this guard, the button text flips
        // Deploy → Deploying… → Deploy → Stop, which looks broken.
        if(!window._inFlightDeploys || !window._inFlightDeploys.has(name)){
          el.classList.remove('is-deploying');
        }
        el.classList.remove('is-stopping');
      }
      // Update deploy button text + disabled state
      const isDeploying=el.classList.contains('is-deploying');
      const isStopping=el.classList.contains('is-stopping');
      let btnText='Deploy';
      let btnDisabled=false;
      if(isStopping){btnText='Stopping…'; btnDisabled=true;}
      else if(isDeploying){btnText='Deploying…'; btnDisabled=true;}
      else if(isRunning) btnText='Stop';
      el.querySelectorAll('.ind-deploy-btn').forEach(b => {
        b.textContent = btnText;
        b.disabled = btnDisabled;
        b.style.cursor = btnDisabled ? 'not-allowed' : '';
      });
      const badge=el.querySelector('.sr-status');
      if(badge){
        if(isRunning){
          badge.textContent='running';badge.className='sr-status running';
          if(!el.querySelector('.sr-stop')&&!deployBtn){
            const stopBtn=document.createElement('button');
            stopBtn.className='sr-stop';
            stopBtn.textContent='Stop';
            stopBtn.style.cssText='font-size:10px;padding:2px 8px;background:#fff;border:1px solid #dc3545;color:#dc3545;border-radius:4px;cursor:pointer;margin-left:6px;flex-shrink:0';
            stopBtn.onclick=function(e){e.stopPropagation();stopIntegration(name)};
            badge.insertAdjacentElement('afterend',stopBtn);
          }
        }
        else{
          if(el.classList.contains('is-deploying')){badge.textContent='deploying';badge.className='sr-status deploying'}
          else if(atLimit){badge.textContent='full';badge.className='sr-status limit'}
          else{badge.textContent='stopped';badge.className='sr-status stopped'}
          const stopBtn=el.querySelector('.sr-stop');
          if(stopBtn)stopBtn.remove();
        }
      }
    });
    const lb=document.getElementById('limitMsg');
    if(lb)lb.classList.toggle('show',atLimit);
  }catch(e){}
}

let _runningSlotCount=0;

// ── Deploy ──
function filterStacks(q){
  q=q.toLowerCase().trim();
  document.querySelectorAll('.stack-row[data-stack]').forEach(el=>{
    const name=(el.querySelector('.sr-name')?.textContent||'').toLowerCase();
    el.style.display=(!q||name.includes(q))?'flex':'none';
  });
}

function indCardAction(name){
  const isRunning=runningStacks.some(p=>p.includes(name)||name.includes(p));
  if(isRunning){stopIntegration(name);return}
  if(!currentDeployTarget){
    addMsg('assistant','**Select an infra target first** — use the "Select Infra" dropdown at the top right.');
    return;
  }
  deployStack(name);
}

async function deployStack(name){
  const isRunning=runningStacks.some(p=>p.includes(name)||name.includes(p));
  if(isRunning){send('Show status');return}
  if(_runningSlotCount>=MAX_RUNNING){
    addMsg('assistant','**Deploy limit reached.** Max '+MAX_RUNNING+' integrations can run. Destroy a running one first.');
    return;
  }
  // Validate stack opts into the chosen target. /api/stacks already expanded
  // 'laptop' to ['laptop-docker','laptop-colima'] for backward compat.
  const meta=stackData.stacks[name]||{};
  const targets=meta.deploy_targets||['laptop-docker','laptop-colima'];
  if(!targets.includes(currentDeployTarget)){
    addMsg('assistant','**'+name+'** is not enabled for **'+currentDeployTarget+'**. Available targets: '+targets.join(', ')+'.');
    return;
  }
  // Guard against double-deploy: if a deploy for THIS stack is already in
  // flight, ignore. This runs for BOTH targets so a second click during the
  // NF confirm/fetch or laptop spin-up can't trigger a duplicate deploy.
  if(window._inFlightDeploys && window._inFlightDeploys.has(name)) return;
  if(!window._inFlightDeploys) window._inFlightDeploys = new Set();
  window._inFlightDeploys.add(name);
  // Pin a stack-architecture card to the chat header so the deploy context stays
  // visible while logs scroll. No-op if the stack doesn't ship a diagram.
  renderStackArch(name, meta.label || name);
  // Next-steps card is pinned by refreshRunning() once the stack actually
  // appears in /api/running — i.e. when the deploy is complete, not now.
  // Lock the infra dropdown immediately — refreshRunning's 5s poll would
  // pick this up too, but doing it now closes the small window where the
  // user could change infra mid-deploy.
  if(typeof updateInfraLock === 'function') updateInfraLock();
  // Disable the deploy button + flag the card immediately. Applies to both
  // NF and laptop paths so the user can't re-click during a slow confirm()
  // or the await of /api/nf/deploy.
  const card=document.querySelector('.stack-row[data-stack="'+name+'"],.ind-card[data-stack="'+name+'"]');
  if(card){
    card.classList.add('is-deploying');
    // Flip the button label *now* so the user sees the click took effect
    // immediately. refreshRunning() polls every 5s and re-derives the label
    // from is-deploying class, but waiting that long invites re-clicks.
    card.querySelectorAll('.ind-deploy-btn').forEach(b => {
      b.textContent = 'Deploying…';
      b.disabled = true;
      b.style.cursor = 'not-allowed';
    });
    const badge=card.querySelector('.sr-status');
    if(badge){badge.textContent='deploying';badge.className='sr-status deploying'}
  }
  // Helper: rollback the disabled state if the deploy is aborted by the user
  // (e.g. they Cancel a confirm() dialog).
  const reEnableDeployUI = () => {
    if(window._inFlightDeploys) window._inFlightDeploys.delete(name);
    if(typeof updateInfraLock === 'function') updateInfraLock();
    if(card){
      card.classList.remove('is-deploying');
      card.querySelectorAll('.ind-deploy-btn').forEach(b => {
        b.textContent = 'Deploy';
        b.disabled = false;
        b.style.cursor = '';
      });
      const badge=card.querySelector('.sr-status');
      if(badge){badge.textContent='';badge.className='sr-status'}
    }
  };
  // Heavy stacks on Colima need explicit memory — Colima default is 2 GB,
  // BFSI foundation alone is ~17.9 GB. Show a one-time hint before deploying.
  const HEAVY_STACKS={'bfsi-fraud-detection':32, 'core-banking-simulator':24};
  if(currentDeployTarget==='laptop-colima' && HEAVY_STACKS[name]){
    const dismissedKey='diabColimaHint:'+name;
    if(!localStorage.getItem(dismissedKey)){
      const memGb=HEAVY_STACKS[name];
      const ok=confirm(name+' on Colima needs ≥'+memGb+' GB allocated to the Colima VM.\n\nIf you haven\'t already, restart Colima with:\n\n  colima stop\n  colima start --cpu 8 --memory '+memGb+' --disk 100\n\nClick OK to continue, Cancel to abort.');
      if(!ok){ reEnableDeployUI(); return; }
      try{ localStorage.setItem(dismissedKey, '1'); }catch(e){}
    }
  }
  // Northflank target: deferred. Don't fire any NF API — surface a chat
  // message pointing the user at the NF Console instead. NF deployment
  // requires GHCR image pre-builds + AWS BYOC setup outside diab; keeping
  // the option visible signals it's on the roadmap, but the actual deploy
  // happens in NF's own UI.
  if(currentDeployTarget==='northflank'){
    const nfUrl = (typeof _nfConsoleUrl === 'string' && _nfConsoleUrl) || 'https://app.northflank.com/';
    const card_h =
      '<div style="border-left:4px solid #3b82f6;background:#eff6ff;padding:12px 16px;border-radius:6px;margin:6px 0">'
      + '<div style="font-size:14px;font-weight:700;color:#1e3a8a;line-height:1.2">Deploy on Northflank Cloud</div>'
      + '<div style="font-size:12px;color:#3b82f6;margin-top:6px;line-height:1.5">'
      +   'Cloud deployment runs in your AWS BYOC cluster via Northflank — managed outside diab. '
      +   'Open the Northflank Console, deploy <strong>'+escHtml(name)+'</strong> there, then return to diab and use the workspace as normal once it\'s running.'
      + '</div>'
      + '<div style="margin-top:10px"><a href="'+escHtml(nfUrl)+'" target="_blank" rel="noopener" '
      +   'style="display:inline-block;background:#0f172a;color:#fff;padding:8px 14px;border-radius:6px;'
      +   'font-size:12px;font-weight:600;text-decoration:none">Open Northflank Console ↗</a></div>'
      + '</div>';
    addMsgHtml('assistant', card_h);
    // Clear the deploying state on the originating card — no actual deploy
    // fired, button reverts immediately so user can re-pick infra or retry.
    if(window._inFlightDeploys) window._inFlightDeploys.delete(name);
    if(card){
      card.classList.remove('is-deploying');
      card.querySelectorAll('.ind-deploy-btn').forEach(b => { b.disabled = false; b.style.cursor = ''; });
      const badge=card.querySelector('.sr-status');
      if(badge){badge.textContent='';badge.className='sr-status'}
    }
    return;
  }
  // Mark this deploy as wanting an auto-switch to Workspace once the stack
  // shows up in the running list (refreshRunning observes this flag).
  window._pendingSwitchToWorkspace = name;
  resetPipelineSteps();
  addMsg('assistant','**Deploying '+name+'...** This may take a few minutes on first run (pulling images).');
  // Add live timer spinner
  const timerEl=document.createElement('div');
  timerEl.className='msg assistant';
  timerEl.style.background='#fff8e6';timerEl.style.border='1px solid #fce5c5';timerEl.style.borderRadius='8px';
  timerEl.innerHTML='<span style="display:inline-block;animation:pulse 1s infinite;margin-right:6px">⏳</span> Spinning up containers... <span id="deployTimer" style="font-weight:700;color:#4a90d9">0s</span>';
  chatEl.appendChild(timerEl);chatEl.scrollTop=chatEl.scrollHeight;
  const dt0=Date.now();window._deployTimerStart=dt0;
  const dti=setInterval(()=>{const el=document.getElementById('deployTimer');if(el)el.textContent=Math.round((Date.now()-dt0)/1000)+'s'},1000);
  window._deployTimerInterval=dti;window._deployTimerEl=timerEl;
  send('Deploy '+name);
}

// Cancel an in-flight NF deploy. Signals the server via /api/nf/cancel; the
// deploy() loop exits early after the current service and destroys partial
// state inside the same request, so the awaiting /api/nf/deploy fetch resolves
// with a "cancelled" message. No client-side abort needed — we let the fetch
// finish naturally so we get a clean cleanup summary back.
// Escape-hatch: cleanup all resources for the currently-selected infra.
// - Northflank → wipes services/jobs/secrets in the NF project
// - laptop-docker / laptop-colima → docker compose down -v + orphan sweep
//   across every known stack
// Bypasses the agent's in-memory deployment state, so it works even when
// Stop fails (e.g. after an agent restart or partial-deploy crash).
async function cleanupInfra(){
  if(!currentDeployTarget){
    addMsg('assistant','**Select an infra first** — use the dropdown at the top right.');
    return;
  }
  const isNF = currentDeployTarget === 'northflank';
  const target = isNF ? 'Northflank' : 'laptop (Docker / Colima)';
  const detail = isNF
    ? 'every service, job, and secret in the NF project. EBS volumes and the cluster are NOT touched.'
    : 'every known stack via `docker compose down -v --remove-orphans` plus orphan container sweep. Local Docker daemon and the agent stay running.';
  if(!confirm('Cleanup ALL resources on '+target+'?\n\nThis will tear down '+detail+'\n\nContinue?')) return;
  const btn = document.getElementById('cleanupBtn');
  const orig = btn ? btn.textContent : 'Cleanup';
  if(btn){ btn.disabled = true; btn.textContent = 'Cleaning...'; }
  addMsg('assistant','**Cleaning '+target+'...** Direct API call; bypassing agent state.');
  try{
    const url = isNF ? '/api/nf/cleanup' : '/api/local/cleanup';
    const r = await fetch(url, {method:'POST'});
    const j = await r.json();
    addMsg('assistant', j.message || 'Cleanup complete.');
  }catch(e){
    addMsg('assistant', '**Cleanup failed:** ' + e);
  }
  if(btn){ btn.disabled = false; btn.textContent = orig; }
  refreshRunning();
}

// Adjust the Cleanup button label to reflect the selected infra. Hidden
// entirely when no infra is selected (no action to take).
function updateCleanupBtn(){
  const btn = document.getElementById('cleanupBtn');
  if(!btn) return;
  if(!currentDeployTarget){
    btn.style.display = 'none';
    return;
  }
  btn.style.display = 'inline-block';
  if(currentDeployTarget === 'northflank'){
    btn.textContent = 'Cleanup NF';
    btn.title = 'Force-wipe every service, job, and secret in the Northflank project.';
  }else{
    btn.textContent = 'Cleanup Local';
    btn.title = 'Tear down every known stack on the local Docker / Colima runtime (docker compose down -v).';
  }
}

async function cancelNfDeploy(name){
  if(!window._nfDeployState || !window._nfDeployState[name]) return;
  // Disable the Cancel link immediately to avoid double-clicks.
  const sp = document.getElementById(window._nfDeployState[name].spinnerId);
  if(sp){
    const link = sp.querySelector('a');
    if(link){ link.style.opacity='0.5'; link.style.pointerEvents='none'; link.textContent='× Cancelling...'; }
  }
  try{
    await fetch('/api/nf/cancel',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stack:name})});
  }catch(e){
    addMsg('assistant','**Cancel signal failed:** '+e);
  }
  // The deploy fetch will return shortly with a cancellation message; the
  // finally{} block in deployStack handles spinner teardown + card reset.
}

async function stopIntegration(name){
  if(!confirm('Stop '+name+'? This will destroy all containers and volumes.'))return;
  if(window._inFlightStops && window._inFlightStops.has(name)) return;
  if(!window._inFlightStops) window._inFlightStops = new Set();
  window._inFlightStops.add(name);
  // If this stack is currently deployed to Northflank, route to NF destroy.
  if(_deployTargets[name]==='northflank'){
    addMsg('assistant','**Destroying '+name+' on Northflank...**');
    try{
      const r=await fetch('/api/nf/destroy',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stack:name})});
      const j=await r.json();
      addMsg('assistant',j.message||'Destroyed.');
    }catch(e){
      addMsg('assistant','**NF destroy failed:** '+e);
    }
    clearStackArchMessage();
    clearNextStepsMessage();
    if(window._inFlightStops) window._inFlightStops.delete(name);
    refreshRunning();
    return;
  }
  const card=document.querySelector('.stack-row[data-stack="'+name+'"],.ind-card[data-stack="'+name+'"]');
  if(card){
    card.classList.add('is-stopping');
    card.querySelectorAll('.ind-deploy-btn').forEach(b => {
      b.textContent = 'Stopping…';
      b.disabled = true;
      b.style.cursor = 'not-allowed';
    });
    const badge=card.querySelector('.sr-status');
    if(badge){badge.textContent='stopping';badge.className='sr-status deploying'}
  }
  addMsg('assistant','**Stopping '+name+'...** Removing containers, volumes, and toolbox.');
  const timerEl=document.createElement('div');
  timerEl.className='msg assistant';
  timerEl.style.background='#fff8e6';timerEl.style.border='1px solid #fce5c5';timerEl.style.borderRadius='8px';
  timerEl.innerHTML='<span style="display:inline-block;animation:pulse 1s infinite;margin-right:6px">⏳</span> Destroying containers... <span id="destroyTimer" style="font-weight:700;color:#c0392b">0s</span>';
  chatEl.appendChild(timerEl);chatEl.scrollTop=chatEl.scrollHeight;
  const dt0=Date.now();
  const dti=setInterval(()=>{const el=document.getElementById('destroyTimer');if(el)el.textContent=Math.round((Date.now()-dt0)/1000)+'s'},1000);

  // Hit the dedicated destroy endpoint — bypasses the AI chat / tool-use flow,
  // so the destroy logic is identical whether Claude/Bedrock is online or not.
  // The endpoint stops the toolbox, runs profile-aware compose down, and
  // sweeps any orphan containers by name prefix.
  let result;
  try{
    const r = await fetch('/api/destroy/'+encodeURIComponent(name), {method:'POST'});
    result = await r.json();
  }catch(e){
    result = {ok:false, error:String(e)};
  }
  clearInterval(dti);
  const elapsed = Math.round((Date.now()-dt0)/1000);
  if(window._inFlightStops) window._inFlightStops.delete(name);
  clearStackArchMessage();
  clearNextStepsMessage();
  if(card){
    card.classList.remove('is-stopping');
    card.querySelectorAll('.ind-deploy-btn').forEach(b => {
      b.disabled = false;
      b.style.cursor = '';
    });
  }
  if(result && result.ok){
    timerEl.style.background='#e8f8ee'; timerEl.style.border='1px solid #b7e4c7';
    timerEl.innerHTML='✅ Containers destroyed in <b>'+elapsed+'s</b><span class="ts">'+timeStr()+'</span>';
    if(result.log && result.log.length){
      const detail = document.createElement('div');
      detail.style.cssText='margin-top:6px;font-size:11px;color:#444;font-family:Monaco,monospace';
      detail.innerHTML = result.log.map(l => '• '+l).join('<br>');
      timerEl.appendChild(detail);
    }
    resetPipelineSteps(name);
  }else{
    timerEl.style.background='#fdeaea'; timerEl.style.border='1px solid #f5c1c1';
    timerEl.innerHTML='❌ Destroy error: '+(result && result.error || 'unknown')+'<span class="ts">'+timeStr()+'</span>';
  }
  // Refresh running state so card reflects reality
  setTimeout(()=>{refreshRunning();refreshStackInfo();refreshMonBottom();loadUI()},500);
}

function resetPipelineSteps(stackName){
  // Clear server-side completion state
  fetch('/api/pipelines/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(stackName?{stack:stackName}:{})});
  document.querySelectorAll('.p-step').forEach((el,i)=>{
    el.classList.remove('done','failed','locked');
    el.style.opacity='1';el.style.background='';
    const num=el.querySelector('.p-num');
    const stepNum=el.dataset.stepnum||String(Array.from(el.parentNode.children).indexOf(el)+1);
    if(num){num.className='p-num';num.textContent=stepNum}
    // Lock all except first in each pipeline group
    if(Array.from(el.parentNode.children).indexOf(el)>0){
      el.classList.add('locked');
      if(num)num.textContent='🔒';
    }
  });
}

// ── Plugin selection ──
function enterBuildMode(){
  const ct=document.getElementById('pluginContainer');
  if(ct)ct.classList.add('plugins-build-mode');
  const bar=document.getElementById('buildModeBar');
  if(bar)bar.innerHTML='<span><strong>Build Mode:</strong> Select plugins to combine into an integration</span><button class="cancel" onclick="exitBuildMode()">Cancel</button>';
}

function exitBuildMode(){
  const ct=document.getElementById('pluginContainer');
  if(ct)ct.classList.remove('plugins-build-mode');
  document.querySelectorAll('.plugin-card.selected').forEach(el=>el.classList.remove('selected'));
  const bar=document.getElementById('buildModeBar');
  if(bar)bar.innerHTML='<span>Available plugins for building integrations</span><button onclick="enterBuildMode()">Build Integration</button>';
  const cnt=document.getElementById('selectedCount');
  if(cnt)cnt.textContent='0';
  const btn=document.getElementById('buildBtn');
  if(btn)btn.disabled=true;
}

function togglePlugin(el){
  if(!document.getElementById('pluginContainer').classList.contains('plugins-build-mode'))return;
  el.classList.toggle('selected');
  const selected=document.querySelectorAll('.plugin-card.selected');
  const cnt=selected.length;
  document.getElementById('selectedCount').textContent=cnt;
  document.getElementById('buildBtn').disabled=cnt===0;
}

function buildFromPlugins(){
  const selected=document.querySelectorAll('.plugin-card.selected');
  if(!selected.length)return;
  const plugins=Array.from(selected).map(el=>el.dataset.plugin);
  const nameInput=document.getElementById('buildName');
  const name=nameInput.value.trim()||plugins.join('-');
  send('Build me an integration called '+name+' with '+plugins.join(' and '));
  exitBuildMode();
  nameInput.value='';
}

async function deleteIntegration(name){
  if(!confirm('Delete integration "'+name+'"? This removes all files and cannot be undone.'))return;
  try{
    const r=await fetch('/api/integration/'+name,{method:'DELETE'});
    const d=await r.json();
    if(d.error){
      addMsg('assistant','**Cannot delete:** '+d.error);
    }else{
      addMsg('assistant','**Deleted integration:** '+name);
      loadUI();
    }
  }catch(e){addMsg('assistant','**Error:** '+e.message)}
}

async function reloadIntegrations(){
  try{
    const r=await fetch('/api/reload',{method:'POST'});
    const d=await r.json();
    addMsg('assistant','**Reloaded:** '+d.stacks+' integrations, '+d.plugins+' plugins found.');
    loadUI();
  }catch(e){addMsg('assistant','**Error reloading:** '+e.message)}
}

function togglePipeline(id){
  const el=document.getElementById('psteps-'+id);
  const arrow=document.getElementById('parrow-'+id);
  if(el){el.classList.toggle('open');arrow.classList.toggle('open')}
}

// Build the quick-access link-bar HTML for a pipeline (declarative `links:`
// field in stack.yaml). Returns empty string if the pipeline has no links so
// callers can unconditionally splice it into the rendered output.
function _pipeLinksHtml(stack, pipeline){
  const links = pipeline && pipeline.links;
  if(!Array.isArray(links) || !links.length) return '';
  const pid = (pipeline && pipeline.id) || '';
  let h = '<div class="pipe-links">';
  for(const lnk of links){
    if(!lnk || !lnk.url) continue;
    h += _pipeLinkBtnHtml(stack, pid, lnk);
  }
  h += '</div>';
  return h;
}

// Single source of truth for a link capsule. gated_by on a link references a
// runtime_controls.id in the same pipeline — when that control reports
// running=false (or unavailable), the capsule is rendered muted+non-clickable.
function _pipeLinkBtnHtml(stack, pid, lnk){
  const label = (lnk.label||'Open').replace(/'/g,"\\'");
  const url   = (lnk.url||'').replace(/'/g,"\\'");
  const icon  = (lnk.icon||'').replace(/'/g,"\\'");
  const display = (icon ? '<span class="pl-icon">'+icon+'</span>' : '') + escHtml(lnk.label||'Open');
  // target:"_blank" opens in a real browser tab via window.open, bypassing
  // the workspace iframe flow entirely.
  const action = (lnk.target === '_blank')
    ? "window.open('"+url+"','_blank','noopener')"
    : "wsOpenPipelineLink('"+stack+"','"+label+"','"+url+"','"+icon+"')";
  const gatedBy = lnk.gated_by ? String(lnk.gated_by) : '';
  if(!gatedBy){
    const onclick = "event.stopPropagation();"+action;
    return '<button type="button" class="pipe-link-btn" onclick="'+onclick+'">'+display+'</button>';
  }
  // Start gated; the 5s rtc poll un-gates once the controlling toggle reports running.
  const onclick = "event.stopPropagation();if(this.classList.contains('pipe-link-gated')){return;}"+action;
  return '<button type="button" class="pipe-link-btn pipe-link-gated"'
       + ' data-gated-by="'+escHtml(gatedBy)+'"'
       + ' data-gated-stack="'+escHtml(stack)+'"'
       + ' data-gated-pid="'+escHtml(pid)+'"'
       + ' data-gated-label="'+escHtml(lnk.label||'')+'"'
       + ' title="Start the toggle above first"'
       + ' onclick="'+onclick+'">'+display+'</button>';
}

// ── Runtime controls (Start/Stop toggle buttons under pipelines) ─────────────
async function toggleRuntimeControl(btn){
  if(btn.disabled || btn.dataset.busy==='1') return;
  const isRunning = btn.dataset.running==='true';
  const action = isRunning ? 'stop' : 'start';
  const stack = btn.dataset.stack, pid = btn.dataset.pid, cid = btn.dataset.cid;
  btn.dataset.busy = '1';
  const origText = btn.textContent;
  btn.textContent = action==='start' ? 'Starting…' : 'Stopping…';
  btn.disabled = true;
  try{
    const r = await fetch('/api/runtime/'+encodeURIComponent(stack)+'/'+encodeURIComponent(pid)+'/'+encodeURIComponent(cid)+'/'+action, {method:'POST'});
    const d = await r.json();
    if(!d.success){
      btn.textContent = origText;
      btn.disabled = false;
      btn.dataset.busy = '0';
      addMsg('assistant','**Runtime control error:** '+(d.error||'unknown'));
      return;
    }
    // Optimistic flip; status poll will reconcile
    btn.dataset.running = (action==='start') ? 'true' : 'false';
    btn.textContent = (action==='start') ? btn.dataset.labelStop : btn.dataset.labelStart;
    btn.classList.toggle('running', action==='start');
    btn.disabled = false;
    btn.dataset.busy = '0';
    setTimeout(()=>refreshRuntimeControlStatus(btn), 500);
  }catch(e){
    btn.textContent = origText;
    btn.disabled = false;
    btn.dataset.busy = '0';
    addMsg('assistant','**Runtime control error:** '+e.message);
  }
}

async function refreshRuntimeControlStatus(btn){
  if(!btn || btn.dataset.enabled!=='1' || btn.dataset.busy==='1') return;
  const stack = btn.dataset.stack, pid = btn.dataset.pid, cid = btn.dataset.cid;
  try{
    const r = await fetch('/api/runtime/'+encodeURIComponent(stack)+'/'+encodeURIComponent(pid)+'/'+encodeURIComponent(cid)+'/status');
    const d = await r.json();
    const running = (!!d.available) && !!d.running;
    if(d.available){
      btn.dataset.running = running ? 'true' : 'false';
      btn.textContent = running ? btn.dataset.labelStop : btn.dataset.labelStart;
      btn.classList.toggle('running', running);
    }
    // Link mirrors the toggle button. If status poll confirmed running, use it.
    // If status is unavailable (service still booting), fall back to the button's
    // current optimistic running state — otherwise the user sees a red Stop
    // button next to a muted link, which reads as broken. Click-through during
    // boot shows the iframe's own auto-refreshing "not reachable" card.
    const linkRunning = d.available ? running : (btn.dataset.running === 'true');
    _applyGatedLinkState(stack, pid, cid, linkRunning, btn.dataset.labelStart || '');
  }catch(e){
    const linkRunning = (btn.dataset.running === 'true');
    _applyGatedLinkState(stack, pid, cid, linkRunning, btn.dataset.labelStart || '');
  }
}

// Toggle the .pipe-link-gated state on every capsule matching this control.
// startLabel feeds the tooltip ("Start ▶ Start Airflow Reconciliation above first").
function _applyGatedLinkState(stack, pid, cid, running, startLabel){
  const sel = '.pipe-link-btn[data-gated-by="'+CSS.escape(cid)+'"]'
            + '[data-gated-stack="'+CSS.escape(stack)+'"]'
            + '[data-gated-pid="'+CSS.escape(pid)+'"]';
  document.querySelectorAll(sel).forEach(el => {
    el.classList.toggle('pipe-link-gated', !running);
    if(!running){
      const hint = startLabel ? ('Click "'+startLabel+'" above first') : 'Start the toggle above first';
      el.setAttribute('title', hint);
    } else {
      el.removeAttribute('title');
    }
  });
}

// Poll a control's status when its toggle button isn't currently in the DOM
// but at least one gated link capsule referencing it is. Keeps the workspace
// Use Case detail view in sync without depending on the Pipelines panel being
// rendered.
async function _refreshGatedLinkOrphan(stack, pid, cid){
  try{
    const r = await fetch('/api/runtime/'+encodeURIComponent(stack)+'/'+encodeURIComponent(pid)+'/'+encodeURIComponent(cid)+'/status');
    const d = await r.json();
    const running = (!!d.available) && !!d.running;
    _applyGatedLinkState(stack, pid, cid, running, '');
  }catch(e){
    _applyGatedLinkState(stack, pid, cid, false, '');
  }
}

// "Last run" summary line for a runtime control (e.g. Airflow's most recent
// DAG run). Backend fetches the configured last_run_url and dot-paths out a
// state + time. We just format and render.
async function _refreshLastRun(span){
  if(!span) return;
  const stack = span.dataset.stack, pid = span.dataset.pid, cid = span.dataset.cid;
  if(!stack || !pid || !cid) return;
  try{
    const r = await fetch('/api/runtime/'+encodeURIComponent(stack)+'/'+encodeURIComponent(pid)+'/'+encodeURIComponent(cid)+'/last-run');
    const d = await r.json();
    if(!d.available){
      span.textContent = '';
      span.style.display = 'none';
      return;
    }
    if(!d.state){
      span.textContent = 'No runs yet';
      span.className = 'rtc-last-run';
      span.style.display = '';
      return;
    }
    const s = String(d.state).toLowerCase();
    let label = String(d.state).toUpperCase();
    if(s === 'success') label = 'PASS';
    else if(s === 'failed') label = 'FAIL';
    let t = '';
    if(d.time){
      try{
        const tdt = new Date(d.time);
        if(!isNaN(tdt.getTime())){
          t = ' at ' + tdt.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
        }
      }catch(e){}
    }
    span.textContent = 'Last run: ' + label + t;
    span.className = 'rtc-last-run rtc-last-' + s;
    span.style.display = '';
  }catch(e){
    span.style.display = 'none';
  }
}

// Periodically refresh all visible runtime control buttons + gated capsules
// whose toggle isn't currently rendered (every 5s). Dedup so each unique
// (stack, pid, cid) triggers at most one status fetch per tick.
function _rtcPollAll(){
  const seen = new Set();
  // First pass: promote any disabled button whose prerequisite step is now
  // complete. Prevents the toggle from staying stuck when its enabled_after
  // step finished but the live strip didn't re-render with fresh data.
  const completedMap = (window._pipelineData && window._pipelineData.completed) || {};
  document.querySelectorAll('.pipe-rt-btn[data-enabled="0"]').forEach(btn => {
    const reqStep = btn.dataset.reqStep || '';
    if(!reqStep) return;
    const key = btn.dataset.stack+'/'+btn.dataset.pid+'/'+reqStep;
    if(completedMap[key]){
      btn.dataset.enabled = '1';
      btn.disabled = false;
    }
  });
  document.querySelectorAll('.pipe-rt-btn[data-enabled="1"]').forEach(btn => {
    const key = btn.dataset.stack+'/'+btn.dataset.pid+'/'+btn.dataset.cid;
    seen.add(key);
    refreshRuntimeControlStatus(btn);
  });
  document.querySelectorAll('.pipe-link-btn[data-gated-by]').forEach(el => {
    const stack = el.dataset.gatedStack || '';
    const pid   = el.dataset.gatedPid || '';
    const cid   = el.dataset.gatedBy || '';
    if(!stack || !pid || !cid) return;
    const key = stack+'/'+pid+'/'+cid;
    if(seen.has(key)) return;
    seen.add(key);
    _refreshGatedLinkOrphan(stack, pid, cid);
  });
  document.querySelectorAll('.rtc-last-run').forEach(_refreshLastRun);
}
setInterval(_rtcPollAll, 5000);
// Also refresh once shortly after pipelines render
setTimeout(_rtcPollAll, 1500);

// Extract URLs from output and create link capsules that open in iframe
function extractLinkCapsules(output) {
  const urlPattern = /https?:\/\/[^\s"'<>]+/g;
  const urls = output.match(urlPattern) || [];
  if (urls.length === 0) return '';

  // Port-based labels (checked first, more specific)
  const portLabels = {
    ':8888': 'Jupyter', ':8889': 'Jupyter', ':8890': 'Jupyter',
    ':5001': 'MLflow', ':5010': 'MLflow',
    ':7861': 'LangFlow', ':7860': 'LangFlow', ':7870': 'LangFlow',
    ':3001': 'App',
    ':8123': 'ClickHouse', ':8125': 'ClickHouse', ':8130': 'ClickHouse',
    ':9001': 'MinIO', ':9004': 'MinIO', ':9011': 'MinIO',
    ':8181': 'Lakekeeper', ':8190': 'Lakekeeper',
    ':3002': 'Metabase', ':3010': 'Metabase',
    ':5691': 'RisingWave', ':5697': 'RisingWave', ':5700': 'RisingWave',
    ':3000': 'PeerDB',
    ':4000': 'Agent'
  };
  // Keyword-based labels (fallback)
  const keywordLabels = {
    'jupyter': 'Jupyter', 'mlflow': 'MLflow', 'langflow': 'LangFlow',
    'grafana': 'Grafana', 'clickhouse': 'ClickHouse', 'minio': 'MinIO',
    'lakekeeper': 'Lakekeeper', 'metabase': 'Metabase', 'peerdb': 'PeerDB',
    'risingwave': 'RisingWave'
  };

  // Map direct ports to nginx proxy ports (iframe-safe)
  const proxyPortMap = {
    ':8889': ':8890',   // Jupyter
    ':5001': ':5010',   // MLflow
    ':3002': ':3010',   // Metabase
    ':7861': ':7870',   // LangFlow
    ':9004': ':9011',   // MinIO Console
    ':8125': ':8130',   // ClickHouse
    ':5697': ':5700',   // RisingWave
    ':8181': ':8190'    // Lakekeeper
  };

  // Dedupe by service label so e.g. http://...:3002 and http://...:3002/public/dashboard/X
  // collapse into ONE "Metabase" button. Prefer the more specific URL (longer path).
  const byLabel = new Map();  // label → url
  for (let url of urls) {
    let name = 'Open Link';
    // Check port first (more specific)
    for (const [port, label] of Object.entries(portLabels)) {
      if (url.includes(port)) { name = label; break; }
    }
    // Fall back to keyword match
    if (name === 'Open Link') {
      for (const [key, label] of Object.entries(keywordLabels)) {
        if (url.toLowerCase().includes(key)) { name = label; break; }
      }
    }
    // Keep the longer URL for this label (more specific path = better)
    if (!byLabel.has(name) || url.length > byLabel.get(name).length) {
      byLabel.set(name, url);
    }
  }
  let capsules = '';
  for (const [name, url] of byLabel) {
    let iframeUrl = url;
    for (const [directPort, proxyPort] of Object.entries(proxyPortMap)) {
      if (url.includes(directPort)) {
        iframeUrl = url.replace(directPort, proxyPort);
        break;
      }
    }
    capsules += "<span onclick=\"openIframeModal('"+iframeUrl+"', '"+name+"')\" style=\"display:inline-block;margin:4px 4px 4px 0;padding:4px 10px;background:#e0f2fe;color:#0369a1;border-radius:12px;font-size:11px;font-weight:500;text-decoration:none;border:1px solid #7dd3fc;cursor:pointer\">" + name + " ↗</span>";
  }
  return capsules ? '<div style="margin-top:8px">' + capsules + '</div>' : '';
}

// Open URL in iframe modal
function openIframeModal(url, title) {
  var existing = document.getElementById('iframeModal');
  if (existing) existing.remove();

  var modal = document.createElement('div');
  modal.id = 'iframeModal';
  modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center';

  var container = document.createElement('div');
  container.style.cssText = 'width:90%;height:90%;background:#fff;border-radius:8px;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 4px 20px rgba(0,0,0,0.3)';

  var header = document.createElement('div');
  header.style.cssText = 'padding:10px 16px;background:#f8f9fa;border-bottom:1px solid #e5e7eb;display:flex;align-items:center;justify-content:space-between';
  header.innerHTML = '<span style="font-weight:600;color:#374151">' + title + '</span>';

  var controls = document.createElement('div');
  var newTabLink = document.createElement('a');
  newTabLink.href = url;
  newTabLink.target = '_blank';
  newTabLink.style.cssText = 'margin-right:12px;color:#2563eb;font-size:12px;text-decoration:none';
  newTabLink.textContent = 'Open in new tab ↗';

  var closeBtn = document.createElement('button');
  closeBtn.textContent = 'Close';
  closeBtn.style.cssText = 'background:#ef4444;color:#fff;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-weight:500';
  closeBtn.onclick = function() { modal.remove(); };

  controls.appendChild(newTabLink);
  controls.appendChild(closeBtn);
  header.appendChild(controls);

  var iframe = document.createElement('iframe');
  iframe.src = url;
  iframe.style.cssText = 'flex:1;border:none;width:100%';

  container.appendChild(header);
  container.appendChild(iframe);
  modal.appendChild(container);
  modal.onclick = function(e) { if (e.target === modal) modal.remove(); };
  document.body.appendChild(modal);
}

let _stepRunning=false;
let _runAllAbort=false;
// Render step output as a structured checklist when our scripts emit
// `━━━ [N/M] ... ━━━` stage markers and `  ✓ ...` / `  ✗ ...` sub-step lines.
// Falls back to a raw <pre> when no markers are found.
function formatStepOutput(lines, status, elapsedSuffix, linkCapsules){
  const stages = [];
  let cur = null;
  const stageRe = /^━━━\s*(?:\[(\d+\/\d+)\]\s*)?(.*?)\s*━━━$/;
  const okRe = /^\s+✓\s+(.*)$/;
  const errRe = /^\s+✗\s+(.*)$/;
  for(const ln of lines){
    const t = ln.trim();
    let m;
    if((m = stageRe.exec(t))){
      cur = {label:m[1]||'', title:m[2]||t, steps:[]};
      stages.push(cur);
      continue;
    }
    if((m = okRe.exec(ln))){
      if(!cur){cur={label:'',title:'',steps:[]};stages.push(cur)}
      cur.steps.push({ok:true, text:m[1]});
      continue;
    }
    if((m = errRe.exec(ln))){
      if(!cur){cur={label:'',title:'',steps:[]};stages.push(cur)}
      cur.steps.push({ok:false, text:m[1]});
    }
  }
  const statusText = status==='ok' ? 'Completed' : status==='err' ? 'Failed' : status;
  let h = '<div class="step-status '+status+'">'+statusText+(elapsedSuffix||'')+'</div>';
  if(stages.length && stages.some(s=>s.steps.length)){
    h += '<div class="step-card">';
    for(const s of stages){
      const lbl = s.label ? '<span class="sc-tag">['+s.label+']</span> ' : '';
      if(s.title) h += '<div class="sc-stage">'+lbl+escHtml(s.title)+'</div>';
      if(s.steps.length){
        h += '<ul class="sc-checks">';
        for(const c of s.steps){
          h += '<li class="sc-'+(c.ok?'ok':'err')+'"><span class="sc-mark">'+(c.ok?'✓':'✗')+'</span>'+escHtml(c.text)+'</li>';
        }
        h += '</ul>';
      }
    }
    h += '</div>';
    const raw = lines.map(l=>l.replace(/</g,'&lt;')).join('\n');
    // Default-collapsed in pipeline panel, default-OPEN in workspace card
    // (the workspace activity card scopes display via .ws-log-body where this
    // .sc-raw is visible). 'open' attr keeps it expanded so users see all logs
    // without an extra click.
    h += '<details class="sc-raw" open><summary>Raw logs ('+lines.length+' lines)</summary><pre>'+raw+'</pre></details>';
  } else {
    const raw = lines.map(l=>l.replace(/</g,'&lt;')).join('\n');
    h += '<pre>'+(raw||'Done')+'</pre>';
  }
  if(linkCapsules) h += '<div class="step-links">'+linkCapsules+'</div>';
  return h;
}

// Pipeline UI state persistence (survives re-renders and tab switches)
const _pipelineUIState = {
  currentIntegration: null,  // Track which integration state belongs to
  expandedPipelines: new Set(),
  completedSteps: new Set(),  // Format: "stack-pipelineId-stepId"
  failedSteps: new Set(),
  unlockedSteps: new Set()
};
// Cached HTML for each step's output panel — survives DOM rebuilds (tab switches, etc.)
// Format: { "stack/pid/sid": "<inner html for .p-step-output>" }
const _pipelineStepOutputs = {};
// Also expose on window so helpers in any scope can read the same object
window._pipelineStepOutputs = _pipelineStepOutputs;
function clearPipelineState(){
  _pipelineUIState.expandedPipelines.clear();
  _pipelineUIState.completedSteps.clear();
  _pipelineUIState.failedSteps.clear();
  _pipelineUIState.unlockedSteps.clear();
}
function savePipelineState(){
  const ct=document.getElementById('wsPanelUCBody')||document.getElementById('ucContent');
  if(!ct)return;
  // Save expanded pipelines
  ct.querySelectorAll('.pipe-steps.open').forEach(el=>{
    const id=el.id.replace('psteps-','');
    if(id)_pipelineUIState.expandedPipelines.add(id);
  });
  ct.querySelectorAll('.pipe-steps:not(.open)').forEach(el=>{
    const id=el.id.replace('psteps-','');
    _pipelineUIState.expandedPipelines.delete(id);
  });
  // Save step states
  ct.querySelectorAll('.p-step').forEach(el=>{
    const onclick=el.getAttribute('onclick')||'';
    const match=onclick.match(/runPipelineStep\('([^']+)','([^']+)','([^']+)'/);
    if(match){
      const key=match[1]+'-'+match[2]+'-'+match[3];
      if(el.classList.contains('done'))_pipelineUIState.completedSteps.add(key);
      else _pipelineUIState.completedSteps.delete(key);
      if(el.classList.contains('failed'))_pipelineUIState.failedSteps.add(key);
      else _pipelineUIState.failedSteps.delete(key);
      if(!el.classList.contains('locked'))_pipelineUIState.unlockedSteps.add(key);
    }
  });
}
function restorePipelineState(){
  const ct=document.getElementById('wsPanelUCBody')||document.getElementById('ucContent');
  if(!ct)return;
  // Restore expanded pipelines
  _pipelineUIState.expandedPipelines.forEach(id=>{
    const stepsEl=document.getElementById('psteps-'+id);
    const arrowEl=document.getElementById('parrow-'+id);
    if(stepsEl){stepsEl.classList.add('open')}
    if(arrowEl)arrowEl.classList.add('open');
  });
  // Restore step states
  ct.querySelectorAll('.p-step').forEach(el=>{
    const onclick=el.getAttribute('onclick')||'';
    const match=onclick.match(/runPipelineStep\('([^']+)','([^']+)','([^']+)'/);
    if(match){
      const key=match[1]+'-'+match[2]+'-'+match[3];
      const num=el.querySelector('.p-num');
      if(_pipelineUIState.completedSteps.has(key)){
        el.classList.add('done');
        if(num){num.className='p-num ok';num.textContent='✓'}
      }
      if(_pipelineUIState.failedSteps.has(key)){
        el.classList.add('failed');
        if(num){num.className='p-num err';num.textContent='!'}
      }
      if(_pipelineUIState.unlockedSteps.has(key)){
        el.classList.remove('locked');
        if(num&&!el.classList.contains('done')&&!el.classList.contains('failed'))num.textContent=el.dataset.stepnum||'';
      }
    }
  });
}
function stripAnsi(s){return s.replace(/\x1b\[[0-9;]*m/g,'').replace(/\[\d+;\d+m/g,'').replace(/\[0m/g,'')}
function scrollToStepResult(stack,pid,sid){
  const outDiv=document.getElementById('step-out-'+stack+'-'+pid+'-'+sid);
  if(outDiv){
    outDiv.scrollIntoView({block:'nearest',behavior:'smooth'});
    outDiv.style.boxShadow='0 0 8px #2563eb';
    setTimeout(()=>outDiv.style.boxShadow='',1500);
  }
}
// Patch the Recent Activity log entry for this step in place so its class,
// summary badge, mark, and body all reflect the terminal state. Without this
// the entry's "RUNNING" badge sticks even after the step row flips to Done.
function _pipeStepUpdateLogEntry(cacheKey, statusKind, statusLabel, elapsedMs, htmlBody){
  const item = document.querySelector('.ws-log-item[data-key="'+cacheKey+'"]');
  if(!item) return;
  item.classList.remove('ws-log-running','ws-log-ok','ws-log-err');
  item.classList.add('ws-log-' + statusKind);
  const mark = item.querySelector('.ws-log-mark');
  if(mark) mark.textContent = statusKind === 'ok' ? '✓' : statusKind === 'err' ? '✗' : '…';
  const state = item.querySelector('.ws-log-state');
  if(state){
    const dur = elapsedMs ? (typeof _wsFmtElapsed === 'function' ? _wsFmtElapsed(elapsedMs) : '') : '';
    state.textContent = statusLabel + (dur ? ' · ' + dur : '');
  }
  const body = item.querySelector('.ws-log-body');
  if(body) body.innerHTML = htmlBody;
}

// Apply terminal state to every live copy of a step row (Use Cases tab +
// Workspace UC detail). Re-finds by data attrs so mid-step re-renders that
// detach `el` don't leave the visible row stuck on "Running".
function _pipeStepApplyTerminal(stack, pid, sid, kind, isRerun, label){
  const sel = '.p-step[data-stack="'+stack+'"][data-pid="'+pid+'"][data-sid="'+sid+'"]';
  document.querySelectorAll(sel).forEach(el => {
    el.classList.remove('running','failed','done');
    el.classList.add(kind);
    if(kind === 'done' && isRerun) el.classList.add('rerun');
    const num = el.querySelector('.p-num');
    if(num){
      if(kind === 'done'){ num.className = 'p-num ok'; num.textContent = '✓'; }
      else { num.className = 'p-num err'; num.textContent = '!'; }
    }
    let badge = el.querySelector('.p-step-state');
    if(!badge){
      badge = document.createElement('span');
      badge.className = 'p-step-state';
      el.appendChild(badge);
    }
    badge.textContent = label;
  });
}

async function runPipelineStep(stack,pid,sid,el){
  const outputId='step-out-'+stack+'-'+pid+'-'+sid;
  const isRerun=el.dataset.rerun==='1';
  if(el.classList.contains('done')&&!isRerun)return;
  if(_stepRunning)return;
  if(el.classList.contains('locked')&&!isRerun)return;
  _stepRunning=true;
  document.querySelectorAll('.p-step').forEach(s=>{if(!s.classList.contains('done')&&s!==el)s.style.opacity='0.3'});
  el.style.opacity='1';el.style.background='';  // class-driven now
  el.classList.add('running');
  const num=el.querySelector('.p-num');
  const label=el.querySelector('.p-label');
  const origLabel=label.textContent;
  num.className='p-num run';
  // Maintain the per-step inline status badge ("Running" while live, "Done" on success).
  let stateEl = el.querySelector('.p-step-state');
  if(!stateEl){
    stateEl = document.createElement('span');
    stateEl.className = 'p-step-state';
    el.appendChild(stateEl);
  }
  stateEl.textContent = 'Running';
  // Create or reuse inline output div right after the step element
  let outDiv=document.getElementById(outputId);
  if(!outDiv){outDiv=document.createElement('div');outDiv.id=outputId;outDiv.className='p-step-output';el.insertAdjacentElement('afterend',outDiv)}
  outDiv.style.display='block';
  // Start step in background
  await fetch('/api/pipelines/'+stack+'/'+pid+'/'+sid,{method:'POST'});
  const rt0=Date.now();
  const cacheKey = stack+'/'+pid+'/'+sid;
  // Recent Activity entries stay collapsed by default — user expands as
  // needed. The body still streams in the background so a click reveals the
  // latest output without any extra fetch.
  // While running, show a small in-pipeline status line only (no log dump).
  // Full structured output lives in the Workspace "Recent activity" card.
  function updateInline(lines){
    const secs=Math.round((Date.now()-rt0)/1000);
    let h='<div class="step-status running"><span style="display:inline-block;animation:pulse 1s infinite;margin-right:4px">⏳</span> Running... '+secs+'s <button onclick="fetch(\'/api/stop-step\',{method:\'POST\'})" style="margin-left:8px;padding:2px 8px;background:#fff;border:1px solid #dc3545;color:#dc3545;border-radius:3px;cursor:pointer;font-size:10px;font-weight:600">Stop</button></div>';
    outDiv.innerHTML=h;
    // Cache the FULL structured payload (lines + status) for the workspace card
    const full=formatStepOutput(lines,'running','',null);
    _pipelineStepOutputs[cacheKey]=full;
    outDiv.scrollIntoView({block:'nearest',behavior:'smooth'});
    // Update only this step's body in the Workspace log without rebuilding
    // the whole list (so other expand/collapse states are preserved)
    const logBody = document.querySelector('.ws-log-item[data-key="'+cacheKey+'"] .ws-log-body');
    if(logBody){
      logBody.innerHTML = full;
      // Auto-scroll the .ws-uco-log container — but only when the user is
      // actively "tailing" (their last scroll position was near the bottom).
      // wsUcoLogTailSetup() installs a one-shot scroll listener that flips
      // the _tailing flag based on how far from bottom they are. This way
      // the user can scroll up freely without being yanked back.
      wsUcoLogTailSetup();
      const logCt = document.getElementById('wsUcoLog');
      // _tailing defaults to true; only flips to false when the user has
      // scrolled up by more than 80px. Tail unless explicitly stopped.
      if(logCt && logCt._tailing !== false){
        // Use rAF so layout is up-to-date before computing scrollHeight
        requestAnimationFrame(() => { logCt.scrollTop = logCt.scrollHeight; });
      }
    }
    else if(_currentTab==='workspace' && typeof wsRenderUseCaseOutput==='function'){
      // First time we see this step in the workspace — full re-render
      wsRenderUseCaseOutput(stack);
    }
    // Refresh the use case tile state classes/labels in-place so the user
    // sees "Running" the moment the step kicks off (no full re-render).
    if(typeof wsRefreshUseCaseTileStates === 'function') wsRefreshUseCaseTileStates(stack);
  }
  updateInline([]);
  const pollInterval=setInterval(async()=>{
    try{
      const r=await fetch('/api/pipelines/poll');
      const d=await r.json();
      if(!d.running&&!d.done){updateInline(d.lines);return}
      updateInline(d.lines);
      if(d.done){
        clearInterval(pollInterval);
        const evt=d.result||{};
        const rawLines=d.lines.map(l=>stripAnsi(l));
        const rawOutput=rawLines.join('\n')||'Done';
        // Link capsules disabled — users open services from the Workspace tab,
        // not from log buttons. Keeps the step output clean.
        const linkCapsules=null;
        const elapsed=d.elapsed_ms?' ('+d.elapsed_ms+'ms)':'';
        if(evt.type==='done'&&evt.success){
          num.className='p-num ok';num.textContent='✓';
          el.classList.remove('running','failed');
          el.classList.add('done');
          // Update or create inline status badge so a step that rendered
          // without a default badge (the "Ready" label was removed) still
          // shows "Done" after completion without waiting for re-render.
          let stateEl = el.querySelector('.p-step-state');
          if(!stateEl){
            stateEl = document.createElement('span');
            stateEl.className = 'p-step-state';
            el.appendChild(stateEl);
          }
          stateEl.textContent = isRerun ? 'Done · Re-run' : 'Done';
          _pipeStepApplyTerminal(stack, pid, sid, 'done', isRerun, isRerun ? 'Done · Re-run' : 'Done');
          const next=el.nextElementSibling;
          if(next&&next.classList.contains('p-step')){next.classList.remove('locked');const nn=next.querySelector('.p-num');if(nn)nn.textContent=next.dataset.stepnum||''}
          // Pipeline shows a tiny status line; full output lives in the
          // Workspace "Recent activity" card (expand-to-view per step).
          const fullOk=formatStepOutput(rawLines,'ok',elapsed,linkCapsules);
          outDiv.innerHTML='<div class="step-status ok">Completed'+elapsed+'</div>';
          _pipelineStepOutputs[cacheKey]=fullOk;
          _pipeStepUpdateLogEntry(cacheKey, 'ok', 'Completed', d.elapsed_ms||0, fullOk);
          // Seed local completed map so a re-render that races backend sees Done.
          if(window._pipelineData){
            if(!window._pipelineData.completed) window._pipelineData.completed = {};
            window._pipelineData.completed[cacheKey] = {success:true, elapsed_ms: (d.elapsed_ms||0)};
          }
          const stepKey=stack+'-'+pid+'-'+sid;
          _pipelineUIState.completedSteps.add(stepKey);
          _pipelineUIState.failedSteps.delete(stepKey);
          if(next){const nk=stack+'-'+pid+'-'+next.dataset.sid;_pipelineUIState.unlockedSteps.add(nk);}
          // If this step has a runtime_control anchored to it, inject the
          // "Now go to Workspace" hint right under the step (without rebuilding
          // the whole pipeline DOM, which would lose runtime state).
          try{
            const pipeData = (window._pipelineData && window._pipelineData.stacks && window._pipelineData.stacks[stack]) || null;
            const pipe = pipeData && (pipeData.pipelines||[]).find(pp => pp.id === pid);
            const matchCtrls = pipe ? (pipe.runtime_controls||[]).filter(c => c.enabled_after === sid) : [];
            if(matchCtrls.length){
              // Remove any prior hint slot below this step before adding a new one
              const existingHint = outDiv.nextElementSibling;
              if(existingHint && existingHint.classList && existingHint.classList.contains('pipe-next-hint')){
                existingHint.remove();
              }
              for(const c of matchCtrls){
                const lbl = (c.label_start||'Start');
                const hint = document.createElement('div');
                hint.className = 'pipe-next-hint inline';
                hint.innerHTML = '<span class="pipe-next-icon">&#9755;</span>'
                  + '<span class="pipe-next-text">Now go to '
                  + '<a onclick="switchTab(\'workspace\')" style="cursor:pointer;color:#4a90d9;text-decoration:underline;font-weight:600">Workspace</a>'
                  + ' and click <b>'+lbl+'</b> in the Use Case Activity card.</span>';
                outDiv.insertAdjacentElement('afterend', hint);
              }
            }
          }catch(_){}
          // Notify Workspace home to reflect completion (enable the runtime
          // control button + show the latest log in the activity card).
          if(typeof wsRenderUseCaseOutput==='function'){
            wsRenderUseCaseOutput(stack);
            if(typeof wsRefreshDataStory==='function') wsRefreshDataStory();
          }
        }else if(evt.type==='stopped'){
          num.className='p-num err';num.textContent='!';
          el.classList.remove('running','done');
          el.classList.add('failed');
          const stateEl = el.querySelector('.p-step-state');
          if(stateEl) stateEl.textContent = 'Stopped';
          _pipeStepApplyTerminal(stack, pid, sid, 'failed', isRerun, 'Stopped');
          outDiv.innerHTML='<div class="step-status err">Stopped by user'+elapsed+'</div>';
          _pipelineStepOutputs[cacheKey]='<div class="step-status err">Stopped by user'+elapsed+'</div>';
          _pipeStepUpdateLogEntry(cacheKey, 'err', 'Stopped', d.elapsed_ms||0, _pipelineStepOutputs[cacheKey]);
          const stepKey=stack+'-'+pid+'-'+sid;
          _pipelineUIState.failedSteps.add(stepKey);
          // Surface the failure to the Workspace tile + log too (otherwise
          // the use-case card stays stuck on "Running" even though the
          // step has terminated). Same JS path runs for both laptop and
          // NF — keeping the failure UX consistent across infras.
          if(typeof wsRefreshUseCaseTileStates === 'function') wsRefreshUseCaseTileStates(stack);
          if(typeof wsRenderUseCaseOutput === 'function') wsRenderUseCaseOutput(stack);
        }else{
          num.className='p-num err';num.textContent='!';
          el.classList.remove('running','done');
          el.classList.add('failed');
          const stateEl = el.querySelector('.p-step-state');
          if(stateEl) stateEl.textContent = 'Failed';
          _pipeStepApplyTerminal(stack, pid, sid, 'failed', isRerun, 'Failed');
          const fullErr=formatStepOutput(rawLines,'err',elapsed,linkCapsules);
          _pipeStepUpdateLogEntry(cacheKey, 'err', 'Failed', d.elapsed_ms||0, fullErr);
          outDiv.innerHTML='<div class="step-status err">Failed'+elapsed+' &mdash; <a onclick="document.getElementById(\''+outputId+'\').scrollIntoView({behavior:\'smooth\'})" style="color:#dc3545;text-decoration:underline;cursor:pointer">view log</a></div>';
          _pipelineStepOutputs[cacheKey]=fullErr;
          const stepKey=stack+'-'+pid+'-'+sid;
          _pipelineUIState.failedSteps.add(stepKey);
          _pipelineUIState.completedSteps.delete(stepKey);
          // Same as the stopped branch: tell the Workspace to transition
          // the use-case tile out of "Running" into a failed state and
          // re-render the log card with the captured output. Applies to
          // both laptop and NF flows.
          if(typeof wsRefreshUseCaseTileStates === 'function') wsRefreshUseCaseTileStates(stack);
          if(typeof wsRenderUseCaseOutput === 'function') wsRenderUseCaseOutput(stack);
          // Briefly chat-toast the failure so it's not buried in the pipeline.
          if(typeof addMsg === 'function'){
            addMsg('assistant','**Step failed:** `'+pid+'/'+sid+'` &mdash; see the failed step in the pipeline (red **!**) for the log. Status badge on the use-case tile is also red.');
          }
        }
        _stepRunning=false;label.textContent=origLabel;el.style.background='';
        document.querySelectorAll('.p-step').forEach(s=>s.style.opacity='1');
      }
    }catch(e){}
  },500);
}

// ── Run All Pipeline Steps ──
async function runAllPipelineSteps(stack, pid, stepIds) {
  if(_stepRunning) { alert('A step is already running'); return; }
  const gid = stack + '-' + pid;
  const stepsEl = document.getElementById('psteps-' + gid);
  const arrowEl = document.getElementById('parrow-' + gid);
  const runAllBtn = document.getElementById('runall-' + gid);
  if(stepsEl) { stepsEl.classList.add('open'); }
  if(arrowEl) { arrowEl.classList.add('open'); }
  if(runAllBtn) { runAllBtn.textContent = 'Stop'; runAllBtn.style.background = '#dc3545'; runAllBtn.onclick = function(e){ e.stopPropagation(); _runAllAbort = true; fetch('/api/stop-step',{method:'POST'}); }; }
  _runAllAbort = false;
  let failed = false;

  for(let i = 0; i < stepIds.length; i++) {
    const sid = stepIds[i];
    const stepEls = stepsEl ? stepsEl.querySelectorAll('.p-step') : [];
    const el = stepEls[i];
    if(!el) continue;
    if(_runAllAbort) { failed = true; break; }
    if(runAllBtn) runAllBtn.textContent = 'Stop (' + (i+1) + '/' + stepIds.length + ')';

    if(el.dataset.optional === '1') {
      el.classList.add('skipped');
      el.style.opacity = '0.5';
      continue;
    }

    if(el.dataset.manual === '1') {
      el.classList.remove('locked');
      const numEl = el.querySelector('.p-num');
      if(numEl) numEl.textContent = (i + 1).toString();
      await runPipelineStepAndWait(stack, pid, sid, el);
      if(el.classList.contains('failed') || _runAllAbort) { failed = true; break; }
      const outDiv = document.getElementById('step-out-'+stack+'-'+pid+'-'+sid);
      if(outDiv) {
        await new Promise(resolve => {
          const btn = document.createElement('button');
          btn.textContent = 'Continue Run All';
          btn.style.cssText = 'margin:6px 0;padding:6px 14px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;font-weight:500;font-size:11px';
          btn.onclick = function(){ btn.remove(); resolve(); };
          outDiv.appendChild(btn);
          outDiv.scrollIntoView({block:'nearest',behavior:'smooth'});
        });
      }
      continue;
    }

    el.classList.remove('locked');
    const numEl = el.querySelector('.p-num');
    if(numEl) numEl.textContent = (i + 1).toString();
    await runPipelineStepAndWait(stack, pid, sid, el);
    if(el.classList.contains('failed') || _runAllAbort) { failed = true; break; }
  }

  const stepIdsJsonRestore = JSON.stringify(stepIds).replace(/"/g, '&quot;');
  if(runAllBtn) {
    runAllBtn.onclick = function(e){ e.stopPropagation(); runAllPipelineSteps(stack, pid, stepIds); };
    if(_runAllAbort) {
      runAllBtn.textContent = 'Stopped — Re-run';
      runAllBtn.style.background = '#f0ad4e';
    } else if(failed) {
      runAllBtn.textContent = 'Failed — Re-run';
      runAllBtn.style.background = '#dc3545';
    } else {
      runAllBtn.textContent = 'Completed ✓';
      runAllBtn.style.background = '#28a745';
    }
    setTimeout(() => { runAllBtn.textContent = 'Run All'; runAllBtn.style.background = '#2563eb'; }, 5000);
  }

  // Defensive: rebuild Recent Activity from latest cache so any entry whose
  // in-place patch was skipped (early-return for already-done steps, etc.)
  // picks up the right Completed/Failed badge.
  try{
    const logCt = document.getElementById('wsUcoLog');
    if(logCt && typeof wsBuildAllStepsLogHtml === 'function'){
      const pd = window._pipelineData || {stacks:{}};
      logCt.innerHTML = wsBuildAllStepsLogHtml(stack, pd);
    }
  }catch(e){}
}

async function runPipelineStepAndWait(stack, pid, sid, el) {
  return new Promise(async (resolve) => {
    if(_stepRunning) { resolve(); return; }
    const outputId = 'step-out-' + stack + '-' + pid + '-' + sid;
    const isRerun = el.dataset.rerun === '1';
    if(el.classList.contains('done') && !isRerun) { resolve(); return; }
    _stepRunning = true;
    document.querySelectorAll('.p-step').forEach(s => { if(!s.classList.contains('done') && s !== el) s.style.opacity = '0.3'; });
    el.style.opacity = '1'; el.style.background = '#fff8e6';
    const num = el.querySelector('.p-num');
    const label = el.querySelector('.p-label');
    const origLabel = label.textContent;
    label.textContent = origLabel + ' — running...';
    num.className = 'p-num run';
    let outDiv = document.getElementById(outputId);
    if(!outDiv) { outDiv = document.createElement('div'); outDiv.id = outputId; outDiv.className = 'p-step-output'; el.insertAdjacentElement('afterend', outDiv); }
    outDiv.style.display = 'block';
    await fetch('/api/pipelines/' + stack + '/' + pid + '/' + sid, { method: 'POST' });
    const rt0 = Date.now();
    const cacheKey = stack + '/' + pid + '/' + sid;
    // Recent Activity entries stay collapsed by default in Run All too.
    function updateInline(lines) {
      const secs = Math.round((Date.now() - rt0) / 1000);
      let h = '<div class="step-status running"><span style="display:inline-block;animation:pulse 1s infinite;margin-right:4px">⏳</span> Running... ' + secs + 's</div>';
      if(lines && lines.length > 0) {
        const last = lines.slice(-20);
        h += '<pre>' + last.map(l => stripAnsi(l).replace(/</g, '&lt;')).join('\n') + '</pre>';
      }
      outDiv.innerHTML = h;
      const pre = outDiv.querySelector('pre');
      if(pre) pre.scrollTop = pre.scrollHeight;
      outDiv.scrollIntoView({block:'nearest',behavior:'smooth'});
      // Mirror to Recent Activity (same wiring as single-step runPipelineStep):
      // write the full formatStepOutput payload to the cache + patch the log
      // entry's body in place so users see Run All progress stream live.
      const full = formatStepOutput(lines, 'running', '', null);
      window._pipelineStepOutputs[cacheKey] = full;
      const logBody = document.querySelector('.ws-log-item[data-key="'+cacheKey+'"] .ws-log-body');
      if(logBody){
        logBody.innerHTML = full;
        if(typeof wsUcoLogTailSetup === 'function') wsUcoLogTailSetup();
        const logCt = document.getElementById('wsUcoLog');
        if(logCt && logCt._tailing !== false){
          requestAnimationFrame(() => { logCt.scrollTop = logCt.scrollHeight; });
        }
      } else if(_currentTab === 'workspace' && typeof wsRenderUseCaseOutput === 'function'){
        wsRenderUseCaseOutput(stack);
      }
    }
    updateInline([]);
    const pollInterval = setInterval(async () => {
      // Check abort flag — stop immediately without waiting for backend
      if(_runAllAbort) {
        clearInterval(pollInterval);
        num.className = 'p-num err'; num.textContent = '!';
        el.classList.add('failed');
        outDiv.innerHTML = '<div class="step-status err">Stopped by user</div>';
        _stepRunning = false; label.textContent = origLabel; el.style.background = '';
        document.querySelectorAll('.p-step').forEach(s => s.style.opacity = '1');
        resolve();
        return;
      }
      try {
        const r = await fetch('/api/pipelines/poll');
        const d = await r.json();
        updateInline(d.lines);
        if(d.done) {
          clearInterval(pollInterval);
          const evt = d.result || {};
          const rawOutput = d.lines.map(l => stripAnsi(l)).join('\n') || 'Done';
          // Link capsules disabled — services are launched from the Workspace tab.
          const linkCapsules = null;
          const output = rawOutput.split('\n').filter(l => !l.trim().startsWith('open http')).join('\n');
          const elapsed = d.elapsed_ms ? ' (' + d.elapsed_ms + 'ms)' : '';
          // Cache key matches the single-step runner's format
          // (stack/pid/sid), so Recent Activity sees Run All-completed
          // steps without waiting for a hydrate-from-backend round trip.
          const cacheKey = stack + '/' + pid + '/' + sid;
          // Update or create the inline state badge so the row immediately
          // shows "Done" / "Failed" — without this, users see a row with no
          // badge text after Run All and think Start Service is still
          // clickable, even though the .done class has applied pointer-events:none.
          let stateEl = el.querySelector('.p-step-state');
          if(!stateEl){
            stateEl = document.createElement('span');
            stateEl.className = 'p-step-state';
            el.appendChild(stateEl);
          }
          const isRerunStep = el.dataset.rerun === '1';
          if(evt.type === 'done' && evt.success) {
            num.className = 'p-num ok'; num.textContent = '✓';
            el.classList.remove('running','failed');
            el.classList.add('done');
            if(stateEl) stateEl.textContent = isRerunStep ? 'Done · Re-run' : 'Done';
            _pipeStepApplyTerminal(stack, pid, sid, 'done', isRerunStep, isRerunStep ? 'Done · Re-run' : 'Done');
            const next = el.nextElementSibling;
            if(next && next.classList.contains('p-step')) { next.classList.remove('locked'); const nn = next.querySelector('.p-num'); if(nn) nn.textContent = next.dataset.stepnum || ''; }
            const fullOk = '<div class="step-status ok">Completed' + elapsed + '</div><pre>' + output.replace(/</g, '&lt;') + '</pre>' + (linkCapsules ? '<div class="step-links">' + linkCapsules + '</div>' : '');
            outDiv.innerHTML = fullOk;
            window._pipelineStepOutputs[cacheKey] = fullOk;
            _pipeStepUpdateLogEntry(cacheKey, 'ok', 'Completed', d.elapsed_ms||0, fullOk);
            // Seed local completed map so a re-render that races backend sees Done.
            if(window._pipelineData){
              if(!window._pipelineData.completed) window._pipelineData.completed = {};
              window._pipelineData.completed[cacheKey] = {success:true, elapsed_ms: (d.elapsed_ms||0)};
            }
            const stepKey = stack + '-' + pid + '-' + sid;
            _pipelineUIState.completedSteps.add(stepKey);
            _pipelineUIState.failedSteps.delete(stepKey);
            if(next) { const nk = stack + '-' + pid + '-' + next.dataset.sid; _pipelineUIState.unlockedSteps.add(nk); }
          } else {
            num.className = 'p-num err'; num.textContent = '!';
            el.classList.remove('running','done');
            el.classList.add('failed');
            const failBadge = (evt.type === 'stopped') ? 'Stopped' : 'Failed';
            if(stateEl) stateEl.textContent = failBadge;
            _pipeStepApplyTerminal(stack, pid, sid, 'failed', isRerunStep, failBadge);
            const failLabel = (evt.type === 'stopped') ? 'Stopped by user' : 'FAILED';
            const fullErr = '<div class="step-status err">' + failLabel + elapsed + '</div><pre>' + output.replace(/</g, '&lt;') + '</pre>' + (linkCapsules ? '<div class="step-links">' + linkCapsules + '</div>' : '');
            outDiv.innerHTML = fullErr;
            window._pipelineStepOutputs[cacheKey] = fullErr;
            _pipeStepUpdateLogEntry(cacheKey, 'err', failBadge, d.elapsed_ms||0, fullErr);
            const stepKey = stack + '-' + pid + '-' + sid;
            _pipelineUIState.failedSteps.add(stepKey);
            _pipelineUIState.completedSteps.delete(stepKey);
          }
          _stepRunning = false; label.textContent = origLabel; el.style.background = '';
          document.querySelectorAll('.p-step').forEach(s => s.style.opacity = '1');
          resolve();
        }
      } catch(e) {}
    }, 500);
  });
}

// ── Use Case Builder ──
let ucSteps=[];

function toggleUcBuilder(){
  const b=document.getElementById('ucBuilder');
  if(b.style.display==='none'){b.style.display='block';ucSteps=[];renderUcSteps()}
  else{b.style.display='none'}
}

function addUcStep(){
  const name=document.getElementById('ucStepName').value.trim();
  const cmd=document.getElementById('ucStepCmd').value.trim();
  if(!name||!cmd){return}
  ucSteps.push({name,cmd});
  document.getElementById('ucStepName').value='';
  document.getElementById('ucStepCmd').value='';
  renderUcSteps();
  document.getElementById('ucSaveBtn').disabled=false;
}

function removeUcStep(i){
  ucSteps.splice(i,1);
  renderUcSteps();
  if(ucSteps.length===0)document.getElementById('ucSaveBtn').disabled=true;
}

function renderUcSteps(){
  const list=document.getElementById('ucStepsList');
  if(!list)return;
  if(ucSteps.length===0){list.innerHTML='<p style="font-size:11px;color:#bbb;text-align:center;padding:8px 0">No steps added yet</p>';return}
  let h='';
  ucSteps.forEach((s,i)=>{
    const isQuery=s.name.toLowerCase().startsWith('query')||s.name.toLowerCase().startsWith('show')||s.name.toLowerCase().startsWith('verify');
    h+='<div class="uc-step-row">';
    h+='<span class="uc-step-num">'+(i+1)+'</span>';
    h+='<span class="uc-step-name">'+(isQuery?'<span style="color:#4a90d9">'+s.name+'</span>':s.name)+'</span>';
    h+='<span class="uc-step-cmd">'+s.cmd+'</span>';
    h+='<span class="uc-step-remove" onclick="removeUcStep('+i+')">x</span>';
    h+='</div>';
  });
  list.innerHTML=h;
}

async function saveUcSteps(){
  const stack=document.getElementById('ucStack').value;
  const ucName=document.getElementById('ucName').value.trim();
  if(!stack||!ucName||ucSteps.length===0){addMsg('assistant','Please fill in the integration, use case name, and at least one step.');return}
  const btn=document.getElementById('ucSaveBtn');
  btn.textContent='Saving...';btn.disabled=true;
  try{
    const r=await fetch('/api/pipelines/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stack,name:ucName,steps:ucSteps})});
    const d=await r.json();
    if(d.success){
      addMsg('assistant','Use case **'+ucName+'** added to '+stack+' with '+ucSteps.length+' steps.');
      ucSteps=[];
      toggleUcBuilder();
      loadUI();
    }else{
      addMsg('assistant','Failed to save: '+(d.error||'Unknown error'));
      btn.textContent='Save use case';btn.disabled=false;
    }
  }catch(e){addMsg('assistant','Error: '+e.message);btn.textContent='Save use case';btn.disabled=false}
}

function ucAskHelp(){
  const stack=document.getElementById('ucStack').value;
  inputEl.value='What docker exec commands can I use to test the '+stack+' integration? List the actual table names and container names so I can paste the commands directly.';
  inputEl.focus();
  // Highlight the AI help button in green
  const hint=document.querySelector('.uc-chat-hint');
  if(hint){hint.style.borderColor='#28a745';hint.style.borderLeft='3px solid #28a745';hint.style.background='#f0fdf4'}
}

async function deleteUseCase(stack,pid,name){
  if(!confirm('Delete use case "'+name+'" from '+stack+'?'))return;
  try{
    const r=await fetch('/api/pipelines/'+stack+'/'+pid,{method:'DELETE'});
    const d=await r.json();
    if(d.success){
      addMsg('assistant','Use case **'+name+'** deleted from '+stack+'.');
      await refreshPipelinesTab();
    }else{
      addMsg('assistant','Failed to delete: '+(d.error||'Unknown error'));
    }
  }catch(e){addMsg('assistant','Error: '+e.message)}
}

function onUcIntegrationChange(){
  const sn=document.getElementById('ucIntegrationSelect').value;
  const ct=document.getElementById('ucContent');
  if(!sn||!window._pipelineData||!window._pipelineData.stacks||!window._pipelineData.stacks[sn]){
    ct.innerHTML='';return;
  }
  // Clear state if switching to a different integration (fresh start)
  if(_pipelineUIState.currentIntegration && _pipelineUIState.currentIntegration !== sn){
    clearPipelineState();
  }
  _pipelineUIState.currentIntegration = sn;
  // Save current state before re-render (only if same integration)
  savePipelineState();
  const info=window._pipelineData.stacks[sn];
  const completed=window._pipelineData.completed||{};
  let h='';
  for(const p of(info.pipelines||[])){
    const gid=sn+'-'+p.id;
    const isUserAdded=p.user_added?true:false;
    h+='<div class="pipe-section">';
    h+='<div class="pipe-header" onclick="togglePipeline(\''+gid+'\')">';
    h+='<span class="p-arrow" id="parrow-'+gid+'">&#9654;</span>'+p.name;
    const stepIdsJson = JSON.stringify(p.steps.map(s=>s.id)).replace(/"/g, '&quot;');
    h+='<button id="runall-'+gid+'" onclick="event.stopPropagation();runAllPipelineSteps(\''+sn+'\',\''+p.id+'\','+stepIdsJson+')" style="font-size:10px;color:#fff;cursor:pointer;margin-left:auto;padding:4px 10px;border:none;border-radius:4px;background:#2563eb;font-weight:500">Run All</button>';
    h+='<span class="p-count" style="margin-left:8px">'+p.steps.length+' steps</span>';
    if(isUserAdded)h+='<span onclick="event.stopPropagation();deleteUseCase(\''+sn+'\',\''+p.id+'\',\''+p.name+'\')" style="font-size:10px;color:#c33;cursor:pointer;margin-left:8px;padding:2px 6px;border:1px solid #fca5a5;border-radius:3px;background:#fef2f2">delete</span>';
    h+='</div>';
    h+=_pipeLinksHtml(sn,p);
    h+='<div class="pipe-steps" id="psteps-'+gid+'">';
    let stepNum=0;
    let allPrevDone=true;
    for(const s of p.steps){
      stepNum++;
      const key=sn+'/'+p.id+'/'+s.id;
      const isDone=!!completed[key];
      const isRerun=s.rerun||(s.name||'').toLowerCase().startsWith('query')||(s.name||'').toLowerCase().startsWith('show')||(s.name||'').toLowerCase().startsWith('verify')||(s.name||'').toLowerCase().startsWith('check')||(s.name||'').toLowerCase().startsWith('get ');
      const isManual=s.manual===true;
      const isOptional=s.optional===true;
      const locked=(stepNum>1&&!allPrevDone&&!isRerun)?' locked':'';
      const doneClass=isDone?' done':'';
      const numClass=isDone?'p-num ok':'p-num';
      const numDisplay=isDone?'✓':(locked?'🔒':stepNum);
      let stepName=s.name;
      let stepStyle='';
      if(isManual){stepName+=' [Manual]';stepStyle='border-left:3px solid #ffc107;';}
      if(isOptional){stepName+=' [Optional]';stepStyle='border-left:3px solid #6b7280;opacity:0.8;';}
      h+='<div class="p-step'+doneClass+(isRerun?' rerun':'')+locked+'" data-rerun="'+(isRerun||s.rerun?'1':'0')+'" data-manual="'+(isManual?'1':'0')+'" data-optional="'+(isOptional?'1':'0')+'" data-stepnum="'+stepNum+'" style="'+stepStyle+'" data-stack="'+sn+'" data-pid="'+p.id+'" data-sid="'+s.id+'"><span class="'+numClass+'" onclick="event.stopPropagation();scrollToStepResult(\''+sn+"','"+p.id+"','"+s.id+'\')">'+numDisplay+'</span><span class="p-label" onclick="runPipelineStep(\''+sn+"','"+p.id+"','"+s.id+'\',this.parentElement)">'+stepName+'</span></div>';
      if(!isDone&&!isRerun)allPrevDone=false;
    }
    h+='</div></div>';
  }
  ct.innerHTML=h;
  // Restore state after re-render
  restorePipelineState();
  // Auto-set the builder dropdown to match
  const ucStackEl=document.getElementById('ucStack');
  if(ucStackEl)ucStackEl.value=sn;
}

// ── Refresh Pipelines Tab ──
async function refreshPipelinesTab(){
  try{
    savePipelineState();
    const pr=await fetch('/api/pipelines');
    const pd=await pr.json();
    window._pipelineData=pd;
    const allStacks=window._wsStacks||stackData?.stacks||{};
    const allPipeStacks2=pd.stacks?Object.keys(pd.stacks):[];
    const pipeStacks=allPipeStacks2.filter(sn=>runningStacks.some(r=>r.includes(sn)||sn.includes(r)));
    let totalSteps=0;
    let pph='<div style="padding:10px 12px">';
    if(!pipeStacks.length){
      pph+='<p class="mon-empty" style="color:var(--muted);font-size:11px">Deploy an industry case to see its workflows here.</p>';
    }
    for(const sn of pipeStacks){
      const info=pd.stacks[sn];
      const sMeta=allStacks[sn]||{};
      const caseName=sMeta.industry_case||info.name||sn;
      const completed=pd.completed||{};
      _pipelineUIState.currentIntegration=sn;
      pph+='<div class="pipe-case-hdr">'+caseName+'</div>';
      for(const p of(info.pipelines||[])){
        const gid=sn+'-'+p.id;
        pph+='<div class="pipe-section">';
        pph+='<div class="pipe-header" onclick="togglePipeline(\''+gid+'\')">';
        pph+='<span class="p-arrow" id="parrow-'+gid+'">&#9654;</span>'+p.name;
        const stepIdsJson=JSON.stringify(p.steps.map(s=>s.id)).replace(/"/g,'&quot;');
        pph+='<button id="runall-'+gid+'" onclick="event.stopPropagation();runAllPipelineSteps(\''+sn+'\',\''+p.id+'\','+stepIdsJson+')" style="font-size:10px;color:#fff;cursor:pointer;margin-left:auto;padding:4px 10px;border:none;border-radius:4px;background:#2563eb;font-weight:500">Run All</button>';
        pph+='<span class="p-count" style="margin-left:8px">'+p.steps.length+' steps</span>';
        pph+='</div>';
        pph+=_pipeLinksHtml(sn,p);
        pph+='<div class="pipe-steps" id="psteps-'+gid+'">';
        // Note: runtime_controls (e.g. Start Synthetic Data) used to render
        // inline here. They now live in the Workspace home "Use Case Output"
        // section so users can see+control demo state without expanding the
        // pipeline. See wsRenderUseCaseOutput().
        let stepNum=0;let allPrevDone=true;
        for(const s of p.steps){
          stepNum++;totalSteps++;
          const key=sn+'/'+p.id+'/'+s.id;
          const isDone=!!completed[key];
          const isRerun=s.rerun||(s.name||'').toLowerCase().startsWith('query')||(s.name||'').toLowerCase().startsWith('show')||(s.name||'').toLowerCase().startsWith('verify')||(s.name||'').toLowerCase().startsWith('check')||(s.name||'').toLowerCase().startsWith('get ');
          const locked=(stepNum>1&&!allPrevDone&&!isRerun)?' locked':'';
          const doneClass=isDone?' done':'';
          const numClass=isDone?'p-num ok':'p-num';
          const numDisplay=isDone?'\u2713':(locked?'\uD83D\uDD12':stepNum);
          pph+='<div class="p-step'+doneClass+(isRerun?' rerun':'')+locked+'" data-rerun="'+(isRerun||s.rerun?'1':'0')+'" data-stepnum="'+stepNum+'" data-stack="'+sn+'" data-pid="'+p.id+'" data-sid="'+s.id+'"><span class="'+numClass+'" onclick="event.stopPropagation();scrollToStepResult(\''+sn+"','"+p.id+"','"+s.id+'\')">'+numDisplay+'</span><span class="p-label" onclick="runPipelineStep(\''+sn+"','"+p.id+"','"+s.id+'\',this.parentElement)">'+s.name+'</span></div>';
          // Reserve slot for cached step output (restored after DOM rebuild)
          pph+='<div class="p-step-output" id="step-out-'+sn+'-'+p.id+'-'+s.id+'" style="display:none"></div>';
          if(!isDone&&!isRerun)allPrevDone=false;
          // Inline notification: render a hint AFTER the step that satisfies
          // a runtime_control's enabled_after — so the user sees the pointer
          // exactly where they expect "what's next".
          const matchingCtrls = (p.runtime_controls||[]).filter(c => c.enabled_after === s.id);
          if(matchingCtrls.length && isDone){
            for(const c of matchingCtrls){
              const lbl = (c.label_start||'Start').replace(/'/g,"&#39;");
              pph += '<div class="pipe-next-hint inline">';
              pph += '<span class="pipe-next-icon">&#9755;</span>';
              pph += '<span class="pipe-next-text">Now go to ';
              pph += '<a onclick="switchTab(\'workspace\')" style="cursor:pointer;color:#4a90d9;text-decoration:underline;font-weight:600">Workspace</a>';
              pph += ' and click <b>'+escHtml(lbl)+'</b> in the Use Case Activity card.';
              pph += '</span></div>';
            }
          }
        }
        pph+='</div>';
        pph+='</div>';
      }
    }
    pph+='</div>';
    const pipTarget=document.getElementById('wsPipelinesPageBody')||document.getElementById('wsPanelUCBody')||document.getElementById('tab-pipelines');
    if(pipTarget)pipTarget.innerHTML=pph;
    document.getElementById('pipelineBadge').textContent=totalSteps;
    restorePipelineState();
    // Paint a small status line under each cached step (no link — details
    // are in the Workspace activity card).
    Object.keys(_pipelineStepOutputs).forEach(k=>{
      const parts=k.split('/'); if(parts.length!==3) return;
      const div=document.getElementById('step-out-'+parts[0]+'-'+parts[1]+'-'+parts[2]);
      if(!div) return;
      const cached=_pipelineStepOutputs[k]||'';
      let stat='ok';
      if(/step-status err/.test(cached)) stat='err';
      else if(/step-status running/.test(cached)) stat='running';
      const label = stat==='ok' ? 'Completed' : (stat==='err' ? 'Failed' : 'Running...');
      div.innerHTML='<div class="step-status '+stat+'">'+label+'</div>';
      div.style.display='block';
    });
  }catch(e){}
}

// ── Load all tabs ──
async function loadUI(){
  // Sync header dropdown with persisted choice (defaults to empty "— select —")
  try{
    const sel=document.getElementById('deployTargetSel');
    if(sel) sel.value=currentDeployTarget||'';
  }catch(e){}
  // Detect Docker runtime in background (non-blocking).
  loadRuntime();
  try{
    const r=await fetch('/api/stacks');
    stackData=await r.json();
    window._wsStacks=stackData.stacks||{};

    // ── Industry tab (3 categories × 4 cases = 12 cards) ──
    const CARD_DEFS=[
      {cls:'cat-ml',title:'Real-Time ML Inference at the Data Layer',color:'#2563eb',
       cases:[
         {key:'bfsi-fraud-detection',title:'BFSI Fraud Detection'},
         {key:'telecom-churn-prediction',title:'Telecom Churn Prediction'},
         {key:'healthcare-claims-anomaly',title:'Healthcare Claims Anomaly'},
         {key:'manufacturing-defect-detection',title:'Manufacturing Defect Detection'}
       ]},
      {cls:'cat-sa',title:'High-Performance Search, Analytics & AI (Agentic) Reasoning',color:'#7c3aed',
       cases:[
         {key:'ecommerce-product-search',title:'E-Commerce Product Search'},
         {key:'legal-document-intelligence',title:'Legal Document Intelligence'},
         {key:'media-content-discovery',title:'Media Content Discovery'},
         {key:'pharma-drug-interaction',title:'Pharma Drug Interaction'}
       ]},
      {cls:'cat-sl',title:'Sovereign Data Lakehouse & Regulatory Analytics',color:'#059669',
       cases:[
         {key:'government-citizen-data',title:'Government Citizen Data'},
         {key:'financial-regulatory-reporting',title:'Financial Regulatory Reporting'},
         {key:'healthcare-patient-records',title:'Healthcare Patient Records'},
         {key:'energy-grid-telemetry',title:'Energy Grid Telemetry'}
       ]}
    ];
    const realStacks=stackData.stacks||{};
    let sh='<div style="padding:18px 20px">';
    for(const cat of CARD_DEFS){
      const liveCount=cat.cases.filter(c=>!!realStacks[c.key]).length;
      sh+='<div class="ind-section">';
      sh+='<div class="ind-cat-hdr '+cat.cls+'">';
      sh+='<div class="ind-cat-title">'+cat.title+'</div>';
      sh+='<div class="ind-cat-count">'+liveCount+' of '+cat.cases.length+' available</div>';
      sh+='</div>';
      sh+='<div class="ind-grid" style="--cat-color:'+cat.color+'">';
      for(const cs of cat.cases){
        const isReal=!!realStacks[cs.key];
        sh+='<div class="ind-card'+(isReal?'':' placeholder')+'" data-stack="'+cs.key+'"'+(isReal?' onclick="deployStack(\''+cs.key+'\')"':'')+'>';
        sh+='<div class="ind-card-title">'+cs.title+'</div>';
        sh+='<div class="ind-card-foot">';
        if(isReal){
          sh+='<span class="sr-status stopped" data-stack="'+cs.key+'">stopped</span>';
        }else{
          sh+='<span class="sr-status soon">Coming Soon</span>';
        }
        sh+='</div>';
        if(isReal){
          sh+='<div class="ind-card-action"><button class="ind-deploy-btn" onclick="event.stopPropagation();indCardAction(\''+cs.key+'\')">Deploy</button></div>';
        }
        sh+='</div>';
      }
      sh+='</div></div>';
    }
    sh+='<div class="limit-msg" id="limitMsg">Max '+MAX_RUNNING+' integrations. Destroy one to deploy another.</div>';
    sh+='</div>';
    document.getElementById('tab-stacks').innerHTML=sh;
    document.getElementById('stackBadge').textContent=Object.keys(realStacks).length;
    // Apply Deploy-button disabled state based on whether infra is selected.
    updateDeployButtons();

    // ── Industry Cases panel (flat — no dropdown, all steps listed) ──
    try{
      savePipelineState();
      const pr=await fetch('/api/pipelines');
      const pd=await pr.json();
      window._pipelineData=pd;
      let totalSteps=0;
      let pph='<div style="padding:10px 12px">';
      const allPipeStacks=pd.stacks?Object.keys(pd.stacks):[];
      const pipeStacks=allPipeStacks.filter(sn=>runningStacks.some(r=>r.includes(sn)||sn.includes(r)));
      if(!pipeStacks.length){
        pph+='<p class="mon-empty" style="color:#888;font-size:11px">Deploy an industry case to see its workflows here.</p>';
      }
      for(const sn of pipeStacks){
        const info=pd.stacks[sn];
        const sMeta=stackData.stacks[sn]||{};
        const caseName=sMeta.industry_case||info.name||sn;
        const completed=pd.completed||{};
        _pipelineUIState.currentIntegration=sn;
        pph+='<div class="pipe-case-hdr">'+caseName+'</div>';
        for(const p of(info.pipelines||[])){
          const gid=sn+'-'+p.id;
          const isUserAdded=p.user_added?true:false;
          pph+='<div class="pipe-section">';
          pph+='<div class="pipe-header" onclick="togglePipeline(\''+gid+'\')">';
          pph+='<span class="p-arrow" id="parrow-'+gid+'">&#9654;</span>'+p.name;
          const stepIdsJson=JSON.stringify(p.steps.map(s=>s.id)).replace(/"/g,'&quot;');
          pph+='<button id="runall-'+gid+'" onclick="event.stopPropagation();runAllPipelineSteps(\''+sn+'\',\''+p.id+'\','+stepIdsJson+')" style="font-size:10px;color:#fff;cursor:pointer;margin-left:auto;padding:4px 10px;border:none;border-radius:4px;background:#2563eb;font-weight:500">Run All</button>';
          pph+='<span class="p-count" style="margin-left:8px">'+p.steps.length+' steps</span>';
          if(isUserAdded)pph+='<span onclick="event.stopPropagation();deleteUseCase(\''+sn+'\',\''+p.id+'\',\''+p.name+'\')" style="font-size:10px;color:#c33;cursor:pointer;margin-left:8px;padding:2px 6px;border:1px solid #fca5a5;border-radius:3px;background:#fef2f2">delete</span>';
          pph+='</div>';
          pph+=_pipeLinksHtml(sn,p);
          pph+='<div class="pipe-steps" id="psteps-'+gid+'">';
          let stepNum=0;let allPrevDone=true;
          for(const s of p.steps){
            stepNum++;totalSteps++;
            const key=sn+'/'+p.id+'/'+s.id;
            const isDone=!!completed[key];
            const isRerun=s.rerun||(s.name||'').toLowerCase().startsWith('query')||(s.name||'').toLowerCase().startsWith('show')||(s.name||'').toLowerCase().startsWith('verify')||(s.name||'').toLowerCase().startsWith('check')||(s.name||'').toLowerCase().startsWith('get ');
            const isManual=s.manual===true;
            const isOptional=s.optional===true;
            const locked=(stepNum>1&&!allPrevDone&&!isRerun)?' locked':'';
            const doneClass=isDone?' done':'';
            const numClass=isDone?'p-num ok':'p-num';
            const numDisplay=isDone?'\u2713':(locked?'\uD83D\uDD12':stepNum);
            let stepName=s.name;let stepStyle='';
            if(isManual){stepName+=' [Manual]';stepStyle='border-left:3px solid #ffc107;';}
            if(isOptional){stepName+=' [Optional]';stepStyle='border-left:3px solid #6b7280;opacity:0.8;';}
            pph+='<div class="p-step'+doneClass+(isRerun?' rerun':'')+locked+'" data-rerun="'+(isRerun||s.rerun?'1':'0')+'" data-manual="'+(isManual?'1':'0')+'" data-optional="'+(isOptional?'1':'0')+'" data-stepnum="'+stepNum+'" style="'+stepStyle+'" data-stack="'+sn+'" data-pid="'+p.id+'" data-sid="'+s.id+'"><span class="'+numClass+'" onclick="event.stopPropagation();scrollToStepResult(\''+sn+"','"+p.id+"','"+s.id+'\')">'+numDisplay+'</span><span class="p-label" onclick="runPipelineStep(\''+sn+"','"+p.id+"','"+s.id+'\',this.parentElement)">'+stepName+'</span></div>';
            if(!isDone&&!isRerun)allPrevDone=false;
          }
          pph+='</div></div>';
        }
      }
      pph+='</div>';
      const pipTarget=document.getElementById('wsPipelinesPageBody')||document.getElementById('wsPanelUCBody')||document.getElementById('tab-pipelines');
      if(pipTarget)pipTarget.innerHTML=pph;
      document.getElementById('pipelineBadge').textContent=totalSteps;
      restorePipelineState();
    }catch(e){
      const pipTarget=document.getElementById('wsPipelinesPageBody')||document.getElementById('wsPanelUCBody')||document.getElementById('tab-pipelines');
      if(pipTarget)pipTarget.innerHTML='<p class="mon-empty">Could not load pipelines</p>';
    }

    // ── Monitoring (initial) ──
    const monTarget=document.getElementById('wsPanelMonBody');
    if(monTarget)monTarget.innerHTML='<p class="mon-empty" style="padding:8px;color:#888;font-size:11px">Deploy a stack to see monitoring</p>';

    // ── Monitoring bottom (initial load) ──
    refreshMonBottom();

    // Welcome message
    if(!window._welcomeShown){
      window._welcomeShown=true;
      addMsg('assistant','Welcome to **EDB Postgres® AI Blueprints v0.1rc8**\n\nPick an industry case to deploy, then switch to Workspace to run demo workflows.');
    }
    refreshRunning();
  }catch(e){
    document.getElementById('tab-stacks').innerHTML='<p class="mon-empty">Could not load stacks</p>';
    addMsg('assistant','Welcome to EDB Postgres® AI Blueprints vv0.1rc8. Type "deploy real-time-analytics" to get started.');
  }
}

// ── Formatter ──
function fmt(t){
  // 1. Extract code blocks into placeholders
  const codeBlocks=[];
  t=t.replace(/```(\w*)\n?([\s\S]*?)```/g,(_,lang,code)=>{
    const escaped=code.replace(/</g,'&lt;');
    const lines=escaped.trim().split('\n');
    let html;
    if(lines.length>10){
      html='<div class="code-collapse collapsed"><pre><code>'+escaped+'</code><button class="copy-btn" onclick="copyCode(this)">copy</button></pre><span class="code-toggle" onclick="toggleCode(this)">[+] '+(lines.length-5)+' more lines</span></div>';
    }else{
      html='<pre><code>'+escaped+'</code><button class="copy-btn" onclick="copyCode(this)">copy</button></pre>';
    }
    codeBlocks.push(html);
    return '\x00CB'+(codeBlocks.length-1)+'\x00';
  });
  // 2. Inline formatting
  t=t.replace(/`([^`]+)`/g,'<code>$1</code>');
  t=t.replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>');
  t=t.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g,'<a href="$2" target="_blank">$1</a>');
  t=t.replace(/(?<!="|'>)(https?:\/\/[^\s<)"']+)/g,'<a href="$1" target="_blank">$1</a>');
  // 3. Wrap **Section:** headers — title always visible, body open, [+]/[-] to toggle
  const secRe=/(<strong>[^<]+:<\/strong>)/g;
  const parts=t.split(secRe);
  if(parts.length>=5){
    let out=parts[0];
    for(let i=1;i<parts.length;i+=2){
      const hdr=parts[i];
      const body=(parts[i+1]||'').replace(/^\s*\n?/,'');
      const sid='sec-'+Math.random().toString(36).substr(2,6);
      out+='<div class="sec-hdr">'+hdr+' <span class="sec-toggle" onclick="toggleSec(\''+sid+'\',this)">[-]</span></div><div class="sec-body" id="'+sid+'">'+body+'</div>';
    }
    t=out;
  }
  // 4. Restore code blocks
  t=t.replace(/\x00CB(\d+)\x00/g,(_,i)=>codeBlocks[parseInt(i)]);
  t=t.replace(/\n/g,'<br>');
  return t;
}
function toggleSec(id,el){
  const b=document.getElementById(id);
  if(!b)return;
  b.classList.toggle('collapsed');
  el.textContent=b.classList.contains('collapsed')?'[+]':'[-]';
}
function toggleCode(el){
  const wrap=el.parentElement;
  const lines=wrap.querySelector('code').textContent.trim().split('\n');
  if(wrap.classList.contains('collapsed')){wrap.classList.remove('collapsed');el.textContent='[-] collapse'}
  else{wrap.classList.add('collapsed');el.textContent='[+] '+(lines.length-5)+' more lines'}
}
function copyCode(btn){
  const code=btn.previousElementSibling.textContent;
  navigator.clipboard.writeText(code).then(()=>{btn.textContent='copied!';setTimeout(()=>btn.textContent='copy',1500)});
}
function timeStr(){const d=new Date();return d.getHours().toString().padStart(2,'0')+':'+d.getMinutes().toString().padStart(2,'0')}
function addMsg(role,text){
  const d=document.createElement('div');
  d.className='msg '+role;
  d.innerHTML=fmt(text)+'<span class="ts">'+timeStr()+'</span>';
  chatEl.appendChild(d);
  chatEl.scrollTop=chatEl.scrollHeight;
  return d;
}
// Inject raw HTML (no markdown formatting / escaping) — used for pre-built UI cards.
function addMsgHtml(role,html){
  const d=document.createElement('div');
  d.className='msg '+role;
  d.innerHTML=html+'<span class="ts">'+timeStr()+'</span>';
  chatEl.appendChild(d);
  chatEl.scrollTop=chatEl.scrollHeight;
  return d;
}
// Append a card to the pinned region (above the scrolling chat). Used for infra title + architecture card.
function addPinnedHtml(role,html){
  const host=document.getElementById('chatPinned');
  if(!host) return addMsgHtml(role,html);
  const d=document.createElement('div');
  d.className='msg '+role;
  d.innerHTML=html+'<span class="ts">'+timeStr()+'</span>';
  host.appendChild(d);
  return d;
}

// ── Streaming send ──
async function send(override){
  const m=(typeof override==='string'?override:inputEl.value).trim();
  if(!m)return;
  addMsg('user',m);
  inputEl.value='';
  lockUI();
  const typ=document.createElement('div');
  typ.className='typing';typ.innerHTML='<span class="dots">...</span>';
  chatEl.appendChild(typ);chatEl.scrollTop=chatEl.scrollHeight;
  const bubble=document.createElement('div');
  bubble.className='msg assistant';bubble.style.display='none';
  chatEl.appendChild(bubble);
  let fullText='';
  abortController=new AbortController();
  try{
    const resp=await fetch('/api/chat/stream',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:m}),signal:abortController.signal});
    const reader=resp.body.getReader();const decoder=new TextDecoder();let buf='';
    while(true){
      const{done,value}=await reader.read();if(done)break;
      buf+=decoder.decode(value,{stream:true});
      const lines=buf.split('\n');buf=lines.pop()||'';
      for(const line of lines){
        if(!line.startsWith('data: '))continue;const payload=line.slice(6);
        if(payload==='[DONE]')continue;
        try{const chunk=JSON.parse(payload);fullText+=chunk;typ.style.display='none';bubble.style.display='';bubble.innerHTML=fmt(fullText);chatEl.scrollTop=chatEl.scrollHeight}catch(e){}
      }
    }
  }catch(e){if(e.name==='AbortError'){fullText+=' *[stopped]*'}else{fullText='Connection error: '+e.message}}
  abortController=null;typ.remove();
  if(!fullText){bubble.remove();addMsg('assistant','No response received.')}
  else{bubble.innerHTML=fmt(fullText)+'<span class="ts">'+timeStr()+'</span>'}
  unlockUI();chatEl.scrollTop=chatEl.scrollHeight;
  // Clear deploy timer if active and show elapsed
  if(window._deployTimerInterval){
    const elapsed=Math.round((Date.now()-window._deployTimerStart)/1000);
    clearInterval(window._deployTimerInterval);window._deployTimerInterval=null;
    if(window._deployTimerEl){
      // Check if deploy actually succeeded by looking at the response text
      const failed=fullText.includes('0 containers running')||fullText.includes('failed')||fullText.includes('Error');
      if(failed){
        window._deployTimerEl.innerHTML='❌ Deploy failed after <b>'+elapsed+'s</b><span class="ts">'+timeStr()+'</span>';
        window._deployTimerEl.style.background='#fdeaea';window._deployTimerEl.style.border='1px solid #f5c1c1';
      }else{
        window._deployTimerEl.innerHTML='✅ Containers ready in <b>'+elapsed+'s</b><span class="ts">'+timeStr()+'</span>';
        window._deployTimerEl.style.background='#e8f8ee';window._deployTimerEl.style.border='1px solid #b7e4c7';
      }
    }
  }
  // Clear destroy timer if active and show elapsed
  if(window._destroyTimerInterval){
    const elapsed=Math.round((Date.now()-window._destroyTimerStart)/1000);
    clearInterval(window._destroyTimerInterval);window._destroyTimerInterval=null;
    if(window._destroyTimerEl){
      const lt=fullText.toLowerCase();
      const failed=lt.includes('error')&&!lt.includes('destroyed')&&!lt.includes('stopped');
      if(failed){
        window._destroyTimerEl.innerHTML='❌ Destroy failed after <b>'+elapsed+'s</b><span class="ts">'+timeStr()+'</span>';
        window._destroyTimerEl.style.background='#fdeaea';window._destroyTimerEl.style.border='1px solid #f5c1c1';
      }else{
        window._destroyTimerEl.innerHTML='✅ Containers destroyed in <b>'+elapsed+'s</b><span class="ts">'+timeStr()+'</span>';
        window._destroyTimerEl.style.background='#e8f8ee';window._destroyTimerEl.style.border='1px solid #b7e4c7';
      }
    }
  }
  if(fullText.toLowerCase().includes('destroyed')||fullText.toLowerCase().includes('stopped'))resetPipelineSteps();
  if(fullText.includes('built successfully'))loadUI();
  setTimeout(()=>{refreshRunning();refreshStackInfo();refreshMonBottom()},2000);

  // ── Deploy completion: clear in-flight + auto-switch to Workspace ──
  // Only fires when the SSE stream has actually ended (not on partial
  // running-list detection). Detects success/failure from the final chat text.
  const lt = fullText.toLowerCase();
  const deployFailed = fullText.includes('0 containers running')
                    || lt.includes('error')
                    || lt.includes('failed');
  // Match either ordering the model might use:
  //   "X containers running"   →  exact "containers running"
  //   "running with X containers"  →  not adjacent, regex'd
  //   "containers are running"  →  belt-and-suspenders
  const deploySucceeded = !fullText.includes('0 containers running')
                       && (fullText.includes('containers running')
                           || /running with \d+\s+container/i.test(fullText)
                           || /containers?\s+are\s+running/i.test(fullText));
  const pending = window._pendingSwitchToWorkspace;
  if(pending){
    if(deployFailed){
      // Deploy explicitly failed — unlock the card NOW so the user can retry.
      // The chat clearly reported failure; keeping the card locked just to wait
      // for /api/running would frustrate the retry path.
      if(window._inFlightDeploys) window._inFlightDeploys.delete(pending);
      document.querySelectorAll('.ind-card[data-stack="'+pending+'"],.stack-row[data-stack="'+pending+'"]').forEach(el=>{
        el.classList.remove('is-deploying');
      });
      window._pendingSwitchToWorkspace = null;
      setTimeout(() => { refreshRunning(); }, 1000);
    } else {
      // Success or ambiguous — KEEP the in-flight flag and is-deploying class
      // until refreshRunning observes the stack in /api/running. The chat
      // response ending early (e.g. AI says "Containers running" before
      // Docker has actually finished bringing them up) used to clear the
      // flag here, which let a re-click trigger ANOTHER docker compose up
      // while the first was still finishing — the ever-ending build loop.
      // refreshRunning() now owns the clear (see the isRunning branch).
      if(deploySucceeded){
        // Clean success — flip to Workspace after a brief grace so the user
        // sees the ✅ "Containers ready" message land in chat first.
        setTimeout(() => { loadUI(); switchTab('workspace'); }, 1500);
        // Append the "Deployment complete — follow these steps" card to the
        // chat scrollback now that the AI's reply has fully landed. Look up
        // the stack label from the cached running list so the card title
        // matches what the user clicked.
        try{
          const apps = (window._wsStacks && window._wsStacks[pending]) || {};
          const lbl = apps.label || apps.name || pending;
          renderNextSteps(pending, lbl);
        }catch(e){}
      }
      // Force-poll /api/running so the button state updates promptly when
      // the stack actually appears.
      setTimeout(() => { refreshRunning(); }, 1000);
    }
  }
}

async function resetChat(){await fetch('/api/reset',{method:'POST'});chatEl.innerHTML='';const p=document.getElementById('chatPinned');if(p)p.innerHTML='';addMsg('assistant','Chat reset. How can I help?')}
async function exitLab(){
  if(!confirm('Stop all containers and shut down the agent?'))return;
  addMsg('assistant','Stopping all containers...');lockUI();
  // Step 1: Kill any running step subprocess
  try{await fetch('/api/stop-step',{method:'POST',signal:AbortSignal.timeout(5000)})}catch(e){}
  // Step 2: Abort any running step fetch
  if(window._stepAbort){try{window._stepAbort.abort()}catch(e){}}
  // Step 3: Clear any running timers
  if(window._deployTimerInterval){clearInterval(window._deployTimerInterval);window._deployTimerInterval=null}
  if(window._destroyTimerInterval){clearInterval(window._destroyTimerInterval);window._destroyTimerInterval=null}
  // Step 4: Wait for subprocess to die
  await new Promise(r=>setTimeout(r,1000));
  // Step 5: Send exit with timeout
  try{
    const controller=new AbortController();
    const timeout=setTimeout(()=>controller.abort(),15000);
    const r=await fetch('/api/exit',{method:'POST',signal:controller.signal});
    clearTimeout(timeout);
    const d=await r.json();
    let msg='**Cleanup complete:**\n';
    if(d.details)d.details.forEach(s=>msg+=s+'\n');
    msg+='\nAgent shut down. You can close this tab.';
    addMsg('assistant',msg);
  }catch(e){
    addMsg('assistant','Agent may not have responded. Run **make stop-all** from terminal to ensure cleanup.\n\nYou can close this tab.');
  }
}
inputEl.addEventListener('keydown',e=>{if(e.key==='Enter'&&!sendBtn.disabled&&!inputEl.disabled)send()});

// ── Synthetic Data Tab ──
let synthReady=false;
let synthModels=[];
let synthTargets=[];
let synthView='main'; // main, upload

async function initSynthData(){
  const ct=document.getElementById('tab-synthdata');
  if(!ct)return;
  if(synthReady){
    await loadSynthTargets();
    const targetSelect=document.getElementById('synthTargetSelect');
    if(targetSelect){
      let opts='';
      if(synthTargets.length===0)opts='<option>No databases running</option>';
      else synthTargets.forEach(t=>{opts+='<option value="'+t.conn+'">'+t.container+' ('+t.type+' :'+t.port+')</option>'});
      targetSelect.innerHTML=opts;
    }
    return;
  }
  // First time — show Start button, don't auto-start
  ct.innerHTML='<div class="sd-loading" style="text-align:center;padding:40px"><h3 style="margin:0 0 8px;font-size:16px">Synthetic Data Engine</h3><p style="color:#666;margin:0 0 16px;font-size:13px">Generate test data and push to running databases.</p><button class="sd-btn" onclick="startSynthDB()" style="padding:8px 24px;font-size:13px;background:#28a745;color:#fff;border:none;border-radius:6px;cursor:pointer">Start Engine</button><p style="color:#999;margin:8px 0 0;font-size:11px">Starts a lightweight container (~10s)</p></div>';
}

async function startSynthDB(){
  const ct=document.getElementById('tab-synthdata');
  if(!ct)return;
  ct.innerHTML='<div class="sd-loading"><div class="sd-spinner"></div><p>Starting Synthetic Data engine...</p><p class="sd-sub">Building container, please wait</p></div>';
  try{
    const sr=await fetch('/api/synthdb/start',{method:'POST'});
    const sd=await sr.json();
    if(!sd.success){ct.innerHTML='<div class="sd-loading"><p style="color:#c33">Setup failed: '+sd.error+'</p><button class="sd-btn" onclick="startSynthDB()">Retry</button></div>';return}
    synthReady=true;
    const dot=document.getElementById('synthDot');if(dot)dot.style.display='inline-block';
    await loadSynthModels();
    await loadSynthTargets();
    await renderSynthMain();
  }catch(e){ct.innerHTML='<div class="sd-loading"><p style="color:#c33">Error: '+e.message+'</p><button class="sd-btn" onclick="startSynthDB()">Retry</button></div>'}
}

async function stopSynthDB(){
  if(!confirm('Stop the Synthetic Data engine?'))return;
  try{
    await fetch('/api/synthdb/stop',{method:'POST'});
    synthReady=false;
    const dot=document.getElementById('synthDot');if(dot)dot.style.display='none';
    initSynthData();
  }catch(e){alert('Error: '+e.message)}
}

async function loadSynthModels(){
  try{const r=await fetch('/api/synthdb/models');const d=await r.json();synthModels=d.models||[]}catch(e){synthModels=[]}
}

async function loadSynthTargets(){
  try{const r=await fetch('/api/synthdb/targets');const d=await r.json();synthTargets=d.targets||[]}catch(e){synthTargets=[]}
}

async function renderSynthMain(){
  await loadSynthTargets();
  await loadSynthModels();
  const ct=document.getElementById('tab-synthdata');
  let h='<div style="padding:12px 14px">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">';
  h+='<h3 style="font-size:15px;font-weight:600;color:#1a1a2e;margin:0">Synthetic Data</h3>';
  h+='<button onclick="stopSynthDB()" style="font-size:10px;padding:3px 10px;background:#fff;border:1px solid #dc3545;color:#dc3545;border-radius:4px;cursor:pointer;font-weight:600">Stop Engine</button>';
  h+='</div>';
  h+='<p style="font-size:11px;color:#888;margin:0 0 12px">Generate realistic test data from seed models and load into running databases.</p>';
  // Model select row
  h+='<div class="sd-row" style="margin-bottom:8px">';
  h+='<select class="sd-select" id="synthModelSelect" onchange="onSynthModelChange()">';
  h+='<option value="">Select a model...</option>';
  const builtin=synthModels.filter(m=>m.source==='built-in');
  const uploaded=synthModels.filter(m=>m.source==='uploaded');
  if(builtin.length){builtin.forEach(m=>{h+='<option value="'+m.name+'">'+m.name+(m.name==='products-orders'?' (example)':'')+'</option>'})}
  if(uploaded.length){h+='<option disabled>───────────</option>';uploaded.forEach(m=>{h+='<option value="'+m.name+'">'+m.name+' (uploaded)</option>'})}
  h+='</select>';
  h+='<button class="sd-btn" onclick="renderSynthUpload()">Upload model</button>';
  h+='</div>';
  // Model info (compact) + collapsible preview
  h+='<div id="synthModelInfo"></div>';
  h+='<div id="synthPreview"></div>';
  // Options panel (hidden until model selected)
  h+='<div id="synthOptionsPanel" style="display:none">';
  // Two columns: Generation | Output
  h+='<div style="display:flex;gap:14px;margin-top:8px">';
  // Left: Generation
  h+='<div style="flex:1;min-width:0">';
  h+='<span class="sd-label">Generation</span>';
  h+='<div class="sd-slider-row" style="margin-top:4px"><input type="range" min="1" max="100" value="1" id="synthScale" oninput="document.getElementById(\'synthScaleVal\').textContent=this.value+\'x\'"><span class="sd-slider-val" id="synthScaleVal">1x</span></div>';
  h+='<div style="display:flex;align-items:center;gap:6px;margin-top:4px"><span style="font-size:12px;color:#aaa">or rows:</span><input type="number" class="sd-input" id="synthTotalRows" value="100" style="width:70px;padding:4px 8px;font-size:11px"></div>';
  h+='</div>';
  // Right: Output
  h+='<div style="flex:1;min-width:0">';
  h+='<span class="sd-label">Output</span>';
  h+='<div class="sd-radio-group" style="margin:4px 0 6px">';
  h+='<label class="sd-radio"><input type="radio" name="synthOut" value="csv" '+(synthTargets.length===0?'checked':'')+' onchange="toggleSynthTarget()"> CSV</label>';
  h+='<label class="sd-radio"><input type="radio" name="synthOut" value="db" '+(synthTargets.length>0?'checked':'')+' onchange="toggleSynthTarget()"> Push to DB</label>';
  h+='</div>';
  h+='<div id="synthTargetSection"'+(synthTargets.length===0?' style="display:none"':'')+'>';
  h+='<select class="sd-select" id="synthTargetSelect" style="font-size:11px;padding:4px 8px">';
  if(synthTargets.length===0)h+='<option>No databases running</option>';
  else synthTargets.forEach(t=>{h+='<option value="'+t.conn+'">'+t.container+' (:'+t.port+')</option>'});
  h+='</select>';
  if(synthTargets.length>0)h+='<div style="font-size:11px;color:#28a745;margin-top:3px"><span class="dot"></span>'+synthTargets.length+' DB'+(synthTargets.length>1?'s':'')+' detected</div>';
  h+='<div style="display:flex;align-items:center;gap:6px;margin-top:4px">';
  h+='<select class="sd-select" id="synthDbMode" style="width:80px;font-size:12px;padding:4px 6px"><option>append</option><option>truncate</option><option>replace</option></select>';
  h+='<label style="font-size:12px;color:#666;display:flex;align-items:center;gap:3px;white-space:nowrap"><input type="checkbox" id="synthRecreate" checked style="margin:0"> Recreate</label>';
  h+='</div>';
  h+='<div style="font-size:11px;color:#aaa;margin-top:2px">First run: check. Next: uncheck to append.</div>';
  h+='</div>';
  h+='</div>';
  h+='</div>';
  // Generate footer
  h+='<div class="sd-footer" style="margin-top:10px">';
  h+='<div class="sd-status"><span class="dot"></span>Ready</div>';
  h+='<button class="sd-btn sd-btn-primary" id="synthGenBtn" onclick="doSynthGenerate()">Generate</button>';
  h+='</div>';
  h+='</div>';
  // Result
  h+='<div id="synthResult"></div>';
  h+='</div>';
  ct.innerHTML=h;
}

function onSynthModelChange(){
  const name=document.getElementById('synthModelSelect').value;
  if(!name){
    document.getElementById('synthModelInfo').innerHTML='';
    document.getElementById('synthPreview').innerHTML='';
    document.getElementById('synthOptionsPanel').style.display='none';
    return;
  }
  const m=synthModels.find(x=>x.name===name);
  if(!m)return;
  // Compact single-line info
  const tableNames=Object.keys(m.tables).join(', ');
  let h='<div class="sd-info" style="padding:8px 12px;margin-bottom:0">';
  h+='<b>'+m.name+'</b>'+(m.description?' — '+m.description:'');
  h+=' | '+Object.keys(m.tables).length+' tables, '+m.relationships.length+' rels, '+m.total_seed_rows+' seed rows';
  // Cross-link badge for models that are pipeline-driven
  const SD_USED_BY = {
    'fraud_bank': {stack:'bfsi-fraud-detection', label:'Used by: BFSI Fraud Detection (Usecase 1 OLTP)'}
  };
  const used = SD_USED_BY[m.name];
  if(used){
    h+=' <span style="display:inline-block;font-size:10px;background:#ecfdf5;color:#065f46;border:1px solid #a7f3d0;padding:2px 8px;border-radius:10px;margin-left:6px;font-weight:600">'+used.label+'</span>';
  }
  h+='</div>';
  document.getElementById('synthModelInfo').innerHTML=h;
  document.getElementById('synthOptionsPanel').style.display='block';
  // Collapsible preview
  document.getElementById('synthPreview').innerHTML='<details style="margin:6px 0"><summary style="font-size:11px;color:#4a90d9;cursor:pointer">Preview seed data ('+tableNames+')</summary><div id="synthPreviewInner"><p style="font-size:10px;color:#aaa;padding:4px 0">Loading...</p></div></details>';
  loadSynthPreviewInner(name);
}

async function loadSynthPreviewInner(name){
  const pv=document.getElementById('synthPreviewInner');
  if(!pv)return;
  pv.innerHTML='<p style="font-size:10px;color:#aaa;padding:4px 0">Loading...</p>';
  try{
    const r=await fetch('/api/synthdb/preview/'+name);
    const d=await r.json();
    const tables=Object.keys(d.preview);
    if(tables.length===0){pv.innerHTML='';return}
    const m=synthModels.find(x=>x.name===name);
    let h='<div class="sd-preview-tabs">';
    tables.forEach((t,i)=>{
      const cnt=d.preview[t].length;
      h+='<div class="sd-preview-tab'+(i===0?' active':'')+'" onclick="switchSynthPreviewTab(this,\''+t+'\')" data-table="'+t+'">'+t+'<span class="cnt">('+cnt+')</span></div>';
    });
    h+='</div><div class="sd-preview-wrap" style="max-height:180px">';
    tables.forEach((t,i)=>{
      const rows=d.preview[t];
      if(rows.length===0)return;
      const cols=Object.keys(rows[0]);
      const pk=m&&m.tables[t]?m.tables[t].primary_key:'';
      const fks=m&&m.tables[t]?Object.keys(m.tables[t].foreign_keys||{}):[];
      h+='<table class="sd-ptable" id="synthPT_'+t+'" style="'+(i>0?'display:none':'')+'"><tr>';
      cols.forEach(c=>{
        const cls=c===pk?'pk':(fks.includes(c)?'fk':'');
        h+='<th'+(cls?' class="'+cls+'"':'')+'>'+c+'</th>';
      });
      h+='</tr>';
      rows.forEach(r=>{
        h+='<tr>';
        cols.forEach(c=>{
          const cls=c===pk?'pk':(fks.includes(c)?'fk':'');
          h+='<td'+(cls?' class="'+cls+'"':'')+'>'+((r[c]!==null&&r[c]!==undefined)?r[c]:'')+'</td>';
        });
        h+='</tr>';
      });
      h+='</table>';
    });
    h+='</div><div class="sd-pfooter">First 25 rows | <span class="pk">PK</span> <span class="fk" style="color:#e67e22">FK</span></div>';
    pv.innerHTML=h;
  }catch(e){pv.innerHTML='<p style="font-size:11px;color:#c33">Failed to load preview</p>'}
}

function switchSynthPreviewTab(el,table){
  el.parentElement.querySelectorAll('.sd-preview-tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  const wrap=el.parentElement.nextElementSibling;
  wrap.querySelectorAll('.sd-ptable').forEach(t=>t.style.display='none');
  const target=document.getElementById('synthPT_'+table);
  if(target)target.style.display='table';
}

function toggleSynthTarget(){
  const val=document.querySelector('input[name="synthOut"]:checked').value;
  const sec=document.getElementById('synthTargetSection');
  sec.style.display=val==='db'?'block':'none';
}

async function doSynthGenerate(){
  const name=document.getElementById('synthModelSelect').value;
  if(!name)return;
  const btn=document.getElementById('synthGenBtn');
  const status=document.querySelector('.sd-status');
  const resultEl=document.getElementById('synthResult');
  // Clear previous results
  resultEl.innerHTML='';
  btn.textContent='Generating...';btn.disabled=true;
  status.innerHTML='<span class="dot" style="background:#e67e22"></span>Generating...';
  const scale=document.getElementById('synthScale').value;
  const totalRows=document.getElementById('synthTotalRows').value;
  const output=document.querySelector('input[name="synthOut"]:checked').value;
  let url='/api/synthdb/generate?model='+name;
  if(totalRows&&parseInt(totalRows)>0)url+='&total_rows='+totalRows;
  else url+='&scale='+scale;
  url+='&output='+output;
  if(output==='db'){
    const conn=document.getElementById('synthTargetSelect').value;
    const mode=document.getElementById('synthDbMode').value;
    const recreate=document.getElementById('synthRecreate').checked;
    url+='&db_type=postgresql&db_conn='+encodeURIComponent(conn)+'&db_mode='+mode;
    if(recreate)url+='&recreate_tables=true';
  }
  try{
    const r=await fetch(url,{method:'POST'});
    const d=await r.json();
    btn.textContent='Generate';btn.disabled=false;
    if(d.success){
      status.innerHTML='<span class="dot"></span>Complete';
      const modeLabel=output==='db'?(document.getElementById('synthDbMode').value):'csv';
      const recreateLabel=(output==='db'&&document.getElementById('synthRecreate').checked)?' (tables recreated)':'';
      let rh='<div class="sd-gen-result"><b>Generation complete</b> — mode: '+modeLabel+recreateLabel+'<br>';
      if(d.files)d.files.forEach(f=>{rh+=f.table+': <b>'+f.rows+'</b> rows<br>'});
      if(output==='csv')rh+='<br><button class="sd-btn sd-btn-primary" onclick="downloadSynthCSV(\''+name+'\')">Download CSV</button>';
      else rh+='<br>Data pushed to database successfully.';
      rh+='</div>';
      resultEl.innerHTML=rh;
      if(output==='csv')loadSynthGenPreview(name,d.files);
    }else{
      status.innerHTML='<span class="dot" style="background:#c33"></span>Failed';
      resultEl.innerHTML='<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:6px;padding:10px 14px;margin-top:12px;font-size:11px;color:#991b1b">'+(d.error||'Unknown error')+'</div>';
    }
  }catch(e){btn.textContent='Generate';btn.disabled=false;status.innerHTML='<span class="dot" style="background:#c33"></span>Error'}
}

async function loadSynthGenPreview(name,files){
  if(!files||files.length===0)return;
  const pv=document.getElementById('synthPreview');
  // Reload preview with generated data
  try{
    let h='<div style="margin-top:12px"><span class="sd-label">Generated data preview</span>';
    h+='<div class="sd-preview-tabs">';
    files.forEach((f,i)=>{h+='<div class="sd-preview-tab'+(i===0?' active':'')+'" onclick="switchSynthPreviewTab(this,\'gen_'+f.table+'\')" data-table="gen_'+f.table+'">'+f.table+'<span class="cnt">('+f.rows+')</span></div>'});
    h+='</div><div class="sd-preview-wrap">';
    for(let i=0;i<files.length;i++){
      const f=files[i];
      const r=await fetch('/api/synthdb/generated-preview/'+name+'/'+f.table);
      const d=await r.json();
      if(d.rows&&d.rows.length>0){
        const cols=Object.keys(d.rows[0]);
        h+='<table class="sd-ptable" id="synthPT_gen_'+f.table+'" style="'+(i>0?'display:none':'')+'"><tr>';
        cols.forEach(c=>{h+='<th>'+c+'</th>'});
        h+='</tr>';
        d.rows.forEach(r=>{h+='<tr>';cols.forEach(c=>{h+='<td>'+((r[c]!==null&&r[c]!==undefined)?r[c]:'')+'</td>'});h+='</tr>'});
        h+='</table>';
      }
    }
    h+='</div><div class="sd-pfooter">Showing first 25 generated rows</div></div>';
    document.getElementById('synthResult').insertAdjacentHTML('beforeend',h);
  }catch(e){}
}

function downloadSynthCSV(name){window.open('/api/synthdb/download/'+name,'_blank')}

function renderSynthUpload(){
  const ct=document.getElementById('tab-synthdata');
  let h='<div style="padding:12px 14px">';
  h+='<a style="font-size:11px;color:#4a90d9;cursor:pointer" onclick="renderSynthMain()">back to Synthetic Data</a>';
  h+='<h3 style="font-size:15px;font-weight:600;color:#1a1a2e;margin:10px 0 4px">Add custom model</h3>';
  h+='<p style="font-size:11px;color:#888;margin-bottom:14px">Choose how you\'d like to create your data model</p>';
  // Option 1: Chat
  h+='<div class="sd-option" id="sdOptChat" onclick="synthSelectOption(\'chat\')">';
  h+='<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">';
  h+='<span style="background:#4a90d9;color:#fff;font-size:10px;font-weight:700;padding:3px 8px;border-radius:4px;letter-spacing:.5px">AI</span>';
  h+='<span class="sd-option-title">Describe it in chat</span></div>';
  h+='<div class="sd-option-desc">Tell the AI agent what tables and data you need — it generates the schema and seed data for you.</div>';
  h+='<div class="sd-chat-examples" style="margin-top:8px">';
  h+='<span class="sd-chat-ex" onclick="event.stopPropagation();synthChatExample(this,\'Create a model with customers and orders tables, 50 seed rows each\')">customers + orders, 50 rows</span>';
  h+='<span class="sd-chat-ex" onclick="event.stopPropagation();synthChatExample(this,\'Create a synthdb model: products, categories, and inventory with relationships\')">products + categories + inventory</span>';
  h+='<span class="sd-chat-ex" onclick="event.stopPropagation();synthChatExample(this,\'Create a hospital model with patients, doctors, appointments, and prescriptions\')">hospital: patients, doctors, appointments</span>';
  h+='</div></div>';
  h+='<div class="sd-or">or</div>';
  // Option 2: Upload
  h+='<div class="sd-option" id="sdOptUpload" onclick="synthSelectOption(\'upload\')">';
  h+='<div class="sd-option-title">Upload files</div>';
  h+='<div class="sd-option-desc">Upload JSON model files or CSV files — we auto-detect the schema.</div>';
  h+='<div style="display:flex;gap:8px;margin-top:10px">';
  h+='<button class="sd-btn" onclick="event.stopPropagation();synthSelectOption(\'upload\');synthShowJsonUpload()">Upload JSON</button>';
  h+='<button class="sd-btn" onclick="event.stopPropagation();synthSelectOption(\'upload\');synthShowCsvUpload()">Upload CSV</button>';
  h+='</div></div>';
  h+='<div id="synthUploadArea"></div>';
  // Example
  h+='<div class="sd-example">';
  h+='<div class="sd-example-header" onclick="toggleSynthExample()">';
  h+='<div><span class="sd-example-title">Example: products-orders</span><span class="sd-example-badge">reference</span></div>';
  h+='<span class="sd-example-toggle" id="synthExToggle">show</span></div>';
  h+='<div class="sd-example-body" id="synthExBody">';
  h+='<div class="sd-example-diagram">';
  h+='<div class="sd-ebox"><div class="sd-ebox-name">customers</div><div class="sd-ebox-col"><span class="pk">customer_id (PK)</span><br>name<br>email<br>city</div></div>';
  h+='<div class="sd-earrow">1:N ></div>';
  h+='<div class="sd-ebox"><div class="sd-ebox-name">orders</div><div class="sd-ebox-col"><span class="pk">order_id (PK)</span><br><span class="fk">customer_id (FK)</span><br>order_date<br>total</div></div>';
  h+='<div class="sd-earrow">1:N ></div>';
  h+='<div class="sd-ebox"><div class="sd-ebox-name">order_items</div><div class="sd-ebox-col"><span class="pk">item_id (PK)</span><br><span class="fk">order_id (FK)</span><br><span class="fk">product_id (FK)</span><br>quantity</div></div>';
  h+='</div>';
  h+='<p style="font-size:11px;color:#888;margin:8px 0">This model is available in the built-in models list as "products-orders".</p>';
  h+='<button class="sd-btn sd-btn-primary" onclick="renderSynthMain();setTimeout(()=>{document.getElementById(\'synthModelSelect\').value=\'products-orders\';onSynthModelChange()},100)">Use this model</button>';
  h+='</div></div>';
  h+='</div>';
  ct.innerHTML=h;
}

function synthSelectOption(choice){
  const chat=document.getElementById('sdOptChat');
  const upload=document.getElementById('sdOptUpload');
  if(choice==='chat'){
    chat.style.borderColor='#28a745';chat.style.borderLeft='3px solid #28a745';chat.style.background='#f0fdf4';
    upload.style.borderColor='#e2e4e8';upload.style.borderLeft='';upload.style.background='';
    inputEl.focus();
  }else{
    upload.style.borderColor='#28a745';upload.style.borderLeft='3px solid #28a745';upload.style.background='#f0fdf4';
    chat.style.borderColor='#e2e4e8';chat.style.borderLeft='';chat.style.background='';
  }
}

function toggleSynthExample(){
  const body=document.getElementById('synthExBody');
  const toggle=document.getElementById('synthExToggle');
  if(body.style.display==='block'){body.style.display='none';toggle.textContent='show'}
  else{body.style.display='block';toggle.textContent='hide'}
}

function synthChatExample(el,text){
  el.style.background='#4a90d9';el.style.color='#fff';
  inputEl.value=text;send();
  switchTab('synthdata');
}

function synthSendToChat(){
  synthSelectOption('chat');
  inputEl.value='Help me create a synthdb model';
  inputEl.focus();
}

function synthShowJsonUpload(){
  const area=document.getElementById('synthUploadArea');
  area.innerHTML='<div style="margin-top:10px"><div class="sd-dropzone" onclick="document.getElementById(\'synthJsonInput\').click()"><div class="sd-dropzone-text">Drop schema + seed data JSON files here</div><div class="sd-dropzone-hint">Two files: mymodel_schema.json + mymodel_seed_data.json</div></div><input type="file" id="synthJsonInput" accept=".json" multiple style="display:none" onchange="synthHandleJsonUpload(this.files)"></div>';
}

function synthShowCsvUpload(){
  const area=document.getElementById('synthUploadArea');
  area.innerHTML='<div style="margin-top:10px"><div class="sd-dropzone" onclick="document.getElementById(\'synthCsvInput\').click()"><div class="sd-dropzone-text">Drop CSV files here (each CSV becomes a table)</div><div class="sd-dropzone-hint">Column names auto-detected. PKs and FKs inferred from naming patterns.</div></div><input type="file" id="synthCsvInput" accept=".csv" multiple style="display:none" onchange="synthHandleCsvUpload(this.files)"></div>';
}

async function synthHandleJsonUpload(files){
  if(files.length<2){document.getElementById('synthUploadArea').insertAdjacentHTML('beforeend','<p style="font-size:11px;color:#c33;margin-top:6px">Please upload both schema and seed data JSON files.</p>');return}
  const fd=new FormData();
  for(const f of files){
    if(f.name.includes('schema'))fd.append('schema',f);
    else fd.append('seed_data',f);
  }
  try{
    const r=await fetch('/api/synthdb/upload',{method:'POST',body:fd});
    const d=await r.json();
    if(d.valid){
      await loadSynthModels();
      renderSynthMain();
      setTimeout(()=>{document.getElementById('synthModelSelect').value=d.name;onSynthModelChange()},100);
    }else{
      document.getElementById('synthUploadArea').insertAdjacentHTML('beforeend','<p style="font-size:11px;color:#c33;margin-top:6px">Validation failed: '+d.errors.join(', ')+'</p>');
    }
  }catch(e){document.getElementById('synthUploadArea').insertAdjacentHTML('beforeend','<p style="font-size:11px;color:#c33;margin-top:6px">Upload error: '+e.message+'</p>')}
}

async function synthHandleCsvUpload(files){
  const fd=new FormData();
  for(const f of files)fd.append('files',f);
  try{
    const r=await fetch('/api/synthdb/upload-csv',{method:'POST',body:fd});
    const d=await r.json();
    if(d.valid){
      await loadSynthModels();
      renderSynthMain();
      setTimeout(()=>{document.getElementById('synthModelSelect').value=d.name;onSynthModelChange()},100);
    }else{
      document.getElementById('synthUploadArea').insertAdjacentHTML('beforeend','<p style="font-size:11px;color:#c33;margin-top:6px">Error: '+(d.errors||['Unknown error']).join(', ')+'</p>');
    }
  }catch(e){document.getElementById('synthUploadArea').insertAdjacentHTML('beforeend','<p style="font-size:11px;color:#c33;margin-top:6px">Upload error: '+e.message+'</p>')}
}

// Auto-refresh
setInterval(()=>{
  refreshRunning();refreshStackInfo();refreshMonBottom();
  if(document.querySelector('.tab[data-tab="workspace"]').classList.contains('active')){wsRefreshSidebar();const monPanel=document.getElementById('wsPanelMon');if(monPanel&&monPanel.classList.contains('open'))refreshMonTab()}
},5000);
// Save pipeline state when tab loses focus, restore when regains focus
document.addEventListener('visibilitychange',()=>{
  if(document.hidden){savePipelineState()}
  else{restorePipelineState()}
});
loadUI();

// ═══════════════════════════════════════
// WORKSPACE TAB — v9 (no sidebar, stack pills, compact rows)
// ═══════════════════════════════════════

let wsActiveStack=null;
const wsOpenTabs=new Map();
const wsTerminals=new Map(); // serviceId → {term, ws, fitAddon}
// Stack color palette — cycles for multiple stacks
const WS_COLORS=['#4a90d9','#6c5ce7','#e17055','#00b894','#fdcb6e','#e84393'];
let wsStackColors={}; // stackName → color

function escHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/'/g,'\\&#39;').replace(/"/g,'&quot;')}

// ── Refresh: fetch running stacks and rebuild home ──
async function wsRefreshSidebar(){
  try{
    const [stacksR, runningR]=await Promise.all([fetch('/api/stacks'),fetch('/api/running')]);
    const stacksD=await stacksR.json();
    const runningD=await runningR.json();
    const running=runningD.running||[];

    // Update badge
    const badge=document.getElementById('wsBadge');
    if(badge)badge.textContent=running.length;

    // Assign colors to running stacks
    running.forEach((name,i)=>{if(!wsStackColors[name])wsStackColors[name]=WS_COLORS[i%WS_COLORS.length]});

    // Auto-select first stack if none selected or current is no longer running
    if(!wsActiveStack||!running.includes(wsActiveStack)){
      wsActiveStack=running.length?running[0]:null;
    }

    // Cache stack data for home rendering
    window._wsRunning=running;
    window._wsStacks=stacksD.stacks||{};

    // Rehydrate persisted Recent Activity from the backend for any running
    // stacks we haven't hydrated yet this page-load. Fire-and-forget — the
    // function re-renders #wsUcoLog itself when it has data.
    for(const stk of running){ wsRehydrateStepOutputs(stk); }

    // Update workspace tab label to show active industry case
    const wsLabel=document.getElementById('wsTabLabel');
    if(wsLabel){
      if(wsActiveStack){
        const am=stacksD.stacks[wsActiveStack]||{};
        const caseName=am.industry_case||am.name||wsActiveStack;
        wsLabel.textContent=caseName+' Workspace';
      }else{
        wsLabel.textContent='Workspace';
      }
    }

    // Only rebuild home DOM if the structural set changed (running stacks
    // or active stack). Otherwise we'd flash the panels every 5s and lose
    // user interaction state (e.g. terminals section open/closed).
    const sig = JSON.stringify({r:running, a:wsActiveStack});
    if(window._wsHomeSig !== sig){
      window._wsHomeSig = sig;
      wsRenderHome();
    }
  }catch(e){console.error('wsRefreshSidebar',e)}
}

// ── Home page: Metabase card + collapsible consoles + collapsible terminals ──
// ── Use-case dependency map (drives the locked/highlighted state machine) ──
// STRICT sequential: each UC unlocks only after the immediately previous one
// has completed Start Service. The scripts themselves check richer
// prerequisites at [0/N] — this map is the *UI policy*, deliberately
// stricter than what the scripts technically require, so the demo flows
// 1 → 2 → 3 → 4 → 5 → 6 without two cards being highlighted as "next".
const _useCaseDeps = {
  'oltp':           [],
  'olap':           ['oltp'],
  'ml-fraud':       ['olap'],
  'ml-governance':  ['ml-fraud'],
  'genai':          ['ml-governance'],
  'ai-governance':  ['genai']
};

// Use-case display metadata (subtitle + state badge order). Driven from
// stack.yaml's pipeline name, but we want a tighter one-liner subtitle that
// answers "what does this give me".
const _useCaseSubtitle = {
  'oltp':           'Foundation — seeded transactional data',
  'olap':           'Debezium CDC → ClickHouse + RisingWave',
  'ml-fraud':       '4-path XGBoost inference (TTDF leaderboard)',
  'ml-governance':  'MLflow experiments + model registry',
  'genai':          'LangFlow agents + AIDB semantic search',
  'ai-governance':  'Bedrock + LLM judges + evaluation suite'
};

// Derive a use-case state from the completed-steps map.
//   completed = true if start-service has succeeded
//   running   = true if cached output for stack/pid/start-service contains "step-status running"
//   pending   = deps met, not yet run
//   locked    = a dependency has not yet completed
function wsGetUseCaseState(stack, pid, completedMap){
  const deps = _useCaseDeps[pid] || [];
  for(const d of deps){
    if(!completedMap[stack+'/'+d+'/start-service']) return {state:'locked', missingDep:d};
  }
  // All deps met. Check this pipeline's start-service.
  if(completedMap[stack+'/'+pid+'/start-service']) return {state:'completed'};
  const stepKey = stack+'-'+pid+'-start-service';
  // Failed branch — the pipeline runner marked this step's stepKey as
  // failed (any infra: NF runner emits done.success=false; laptop runner
  // emits done.success=false on non-zero exit). Surface as 'failed' so the
  // tile turns red instead of staying on "Running" forever.
  if(_pipelineUIState && _pipelineUIState.failedSteps && _pipelineUIState.failedSteps.has(stepKey)){
    return {state:'failed'};
  }
  // Currently running? Look at cached step output.
  const cached = (window._pipelineStepOutputs||{})[stack+'/'+pid+'/start-service'] || '';
  if(/step-status running/.test(cached)) return {state:'running'};
  return {state:'pending'};
}

// Build the Dashboard panel HTML (left half of the 2-col grid).
// Sections: Metabase tile, Other UIs, Terminals (loaded async), Credentials.
function wsRenderDashboard(stack, allStacks){
  const meta = allStacks[stack] || {};
  const access = meta.access || [];
  const credentials = meta.credentials || [];
  const apps = access.filter(a => a.url && !a.url.startsWith('psql'));
  let mbApp = null, otherApps = [];
  for(const a of apps){
    if((a.name||'').toLowerCase().includes('metabase')) mbApp = a;
    else otherApps.push(a);
  }
  // Persist app metadata for wsOpenAppDetail
  if(!window._wsApps) window._wsApps = {};

  let h = '<div class="ws-pane ws-pane-dashboard">';
  h += '<div class="ws-pane-h"><span class="ws-pane-icon">📊</span> Dashboard</div>';
  h += '<div class="ws-dash-grid">';

  // Metabase
  h += '<div class="ws-dash-sec"><div class="ws-dash-sec-h">Metabase</div>';
  if(mbApp){
    const safeId = stack+'-'+mbApp.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
    const isOpen = wsOpenTabs.has(safeId);
    const matchedCred = credentials.find(c => (c.service||'').toLowerCase()==='metabase');
    window._wsApps[safeId] = {name: mbApp.name, url: mbApp.url, desc: mbApp.note||mbApp.description||'',
      cred: matchedCred||null, stack, initials: 'MB'};
    h += '<div class="ws-tile ws-tile-primary'+(isOpen?' ws-tile-open':'')+'" onclick="wsOpenAppDetail(\''+safeId+'\')">';
    h += '<div class="ws-tile-icon">MB</div>';
    h += '<div class="ws-tile-body"><div class="ws-tile-name">'+escHtml(mbApp.name)+'</div>';
    h += '<div class="ws-tile-status">'+(isOpen?'<span class="ws-tile-dot open"></span>Open':'Click to launch')+'</div></div>';
    h += '</div>';
  } else {
    h += '<div class="ws-empty-hint">Metabase not configured</div>';
  }
  h += '</div>';

  // Other UIs — collapsed dropdown (mirrors Credentials style)
  h += '<div class="ws-dash-sec"><div class="ws-dash-sec-h">Other UIs</div>';
  if(otherApps.length){
    h += '<div class="ws-list-dd-wrap">';
    h += '<button class="ws-list-dd-btn" onclick="wsToggleListDD(this)">';
    h += '🖥 Open UI <span class="ws-list-dd-count">'+otherApps.length+'</span> <span class="ws-list-dd-caret">&#9662;</span>';
    h += '</button>';
    h += '<div class="ws-list-dd-panel">';
    for(const a of otherApps){
      const safeId = stack+'-'+a.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
      const initials = a.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
      const isOpen = wsOpenTabs.has(safeId);
      const appLower = a.name.toLowerCase();
      const matchedCred = credentials.find(c => {
        const svc = (c.service||'').toLowerCase();
        return appLower.includes(svc) || svc.includes(appLower.split(' ')[0]);
      });
      window._wsApps[safeId] = {name:a.name, url:a.url, desc:a.note||a.description||'',
        cred:matchedCred||null, stack, initials, target:a.target||null};
      h += '<div class="ws-list-dd-row" onclick="wsOpenAppDetail(\''+safeId+'\')">';
      h += '<div class="ws-list-dd-icon">'+escHtml(initials)+'</div>';
      h += '<div class="ws-list-dd-name">'+escHtml(a.name)+'</div>';
      if(isOpen) h += '<span class="ws-list-dd-state">●</span>';
      h += '</div>';
    }
    h += '</div></div>';
  } else {
    h += '<div class="ws-empty-hint">No console UIs</div>';
  }
  h += '</div>';

  // Terminals — collapsed dropdown (loaded async into wsTermBody)
  h += '<div class="ws-dash-sec"><div class="ws-dash-sec-h">Terminals</div>';
  h += '<div id="wsTermBody"><div class="ws-empty-hint">Loading...</div></div>';
  h += '</div>';

  // Credentials dropdown
  const creds = Array.isArray(credentials) ? credentials : [];
  if(creds.length){
    h += '<div class="ws-dash-sec"><div class="ws-dash-sec-h">Credentials</div>';
    h += '<div class="ws-creds-dd-wrap">';
    h += '<button class="ws-creds-dd-btn" onclick="wsToggleCredsDD(this)" style="width:100%;text-align:left">';
    h += '🔒 View Credentials <span class="ws-creds-dd-count">'+creds.length+'</span> <span class="ws-creds-dd-caret">&#9662;</span>';
    h += '</button>';
    h += '<div class="ws-creds-dd-panel">';
    h += '<table class="ws-creds-table"><tbody>';
    for(const c of creds){
      if(typeof c==='object' && c){
        h += '<tr><td><b>'+escHtml(c.service||'')+'</b></td><td>'+escHtml(c.username||'')+' / <code>'+escHtml(c.password||'')+'</code>'+(c.port?' <span class="ws-creds-port">port '+c.port+'</span>':'')+'</td></tr>';
      }
    }
    h += '</tbody></table></div></div></div>';
  } else {
    h += '<div class="ws-dash-sec"><div class="ws-dash-sec-h">Credentials</div><div class="ws-empty-hint">None defined</div></div>';
  }

  h += '</div>'; // end ws-dash-grid
  h += '</div>'; // end ws-pane
  return h;
}

// Tile order = natural pipeline order from stack.yaml. With this layout the
// number on each tile matches the "Usecase N:" prefix in the pipeline name,
// which matches the user's mental model. Arrow connectors were removed
// because the dependency DAG branches and arrows can't be aligned cleanly
// in a fixed 3×2 grid.

// Persistent selection state — when a tile is clicked, the right pane swaps
// from the grid to a pipeline-detail view. Survives async re-renders.
window._wsSelectedUseCase = window._wsSelectedUseCase || null;

function wsOpenUseCase(stack, pipelineId){
  window._wsSelectedUseCase = {stack, pipelineId};
  // Re-render just the use cases pane
  const ucHost = document.getElementById('wsUseCasesHost');
  if(ucHost){
    const pd = window._pipelineData || {stacks:{}, completed:{}};
    ucHost.innerHTML = wsRenderUseCaseGrid(stack, pd);
  }
}

// Lightweight in-place update of all visible Use Case tiles' state classes
// and status labels. Called from pipeline polling so a click on Start Service
// flips the tile to "Running" within ~500ms — without rebuilding the whole
// pane (which would lose the user's open dropdown / scroll position).
function wsRefreshUseCaseTileStates(stack){
  if(!stack) return;
  const completed = (window._pipelineData && window._pipelineData.completed) || {};
  document.querySelectorAll('.ws-uc-tile[data-pid]').forEach(tile => {
    const pid = tile.getAttribute('data-pid');
    if(!pid) return;
    const st = wsGetUseCaseState(stack, pid, completed);
    // Update state class
    if(!tile.classList.contains(st.state)){
      tile.classList.remove('completed','running','pending','locked','failed');
      tile.classList.add(st.state);
    }
    // Update status label
    const statusEl = tile.querySelector('.ws-uc-status');
    if(statusEl){
      let label = '⚪ Ready';
      if(st.state === 'completed') label = '✓ Done';
      else if(st.state === 'running') label = '⚙ Running';
      else if(st.state === 'locked')  label = '🔒 Locked';
      else if(st.state === 'failed')  label = '✗ Failed';
      if(statusEl.textContent !== label) statusEl.textContent = label;
    }
  });
}
function wsBackToUseCaseGrid(){
  window._wsSelectedUseCase = null;
  const ucHost = document.getElementById('wsUseCasesHost');
  if(ucHost && wsActiveStack){
    const pd = window._pipelineData || {stacks:{}, completed:{}};
    ucHost.innerHTML = wsRenderUseCaseGrid(wsActiveStack, pd);
  }
}

// Build the Use Cases pane — either the 3x2 tile grid or the pipeline detail
// (when a tile has been clicked).
function wsRenderUseCaseGrid(stack, pipelinesData){
  const sel = window._wsSelectedUseCase;
  if(sel && sel.stack === stack){
    return wsRenderUseCasePipeline(stack, sel.pipelineId, pipelinesData);
  }
  const info = (pipelinesData.stacks||{})[stack];
  const completed = pipelinesData.completed || {};
  let h = '<div class="ws-pane ws-pane-usecases">';
  h += '<div class="ws-pane-h"><span class="ws-pane-icon">🎯</span> Use Cases</div>';
  if(!info || !(info.pipelines||[]).length){
    h += '<div class="ws-empty-hint" style="padding:20px 0">No use cases defined for this stack.</div>';
    h += '</div>';
    return h;
  }
  h += '<div class="ws-uc-grid">';
  for(const p of info.pipelines || []){
    const st = wsGetUseCaseState(stack, p.id, completed);
    // Number = Usecase number from stack.yaml name (matches user mental model)
    const numMatch = (p.name||'').match(/Usecase\s+(\d+)/i);
    const num = numMatch ? numMatch[1] : '?';
    const display = (p.name||p.id).replace(/^Usecase\s+\d+:\s*/i,'').trim();
    const clickable = st.state !== 'locked';
    const click = clickable ? 'onclick="wsOpenUseCase(\''+stack+'\',\''+p.id+'\')"' : '';
    const tooltip = st.state === 'locked'
      ? 'title="Complete \''+(_useCaseDeps[p.id]||[]).join(', ')+'\' first"'
      : 'title="'+escHtml(display)+' — click to open"';
    let label = '⚪ Ready';
    if(st.state === 'completed') label = '✓ Done';
    else if(st.state === 'running') label = '⚙ Running';
    else if(st.state === 'locked')  label = '🔒 Locked';
    else if(st.state === 'failed')  label = '✗ Failed';
    h += '<div class="ws-uc-tile '+st.state+'" data-pid="'+p.id+'" '+click+' '+tooltip+'>';
    h += '<div class="ws-uc-num">'+num+'</div>';
    h += '<div class="ws-uc-name">'+escHtml(display)+'</div>';
    h += '<div class="ws-uc-status">'+label+'</div>';
    h += '</div>';
  }
  h += '</div></div>';
  return h;
}

// Pipeline detail view — replaces the tile grid in the right pane when a
// tile is clicked. Reuses the existing .p-step DOM so runPipelineStep,
// scrollToStepResult, etc. all work unchanged.
function wsRenderUseCasePipeline(stack, pipelineId, pipelinesData){
  const info = (pipelinesData.stacks||{})[stack];
  const p = (info && info.pipelines || []).find(x => x.id === pipelineId);
  const completed = pipelinesData.completed || {};
  let h = '<div class="ws-pane ws-pane-usecases">';
  h += '<div class="ws-pane-h"><span class="ws-pane-icon">🎯</span> Use Cases</div>';
  if(!p){
    h += '<div class="ws-empty-hint">Pipeline not found.</div>';
    h += '<button class="ws-uc-detail-back" onclick="wsBackToUseCaseGrid()">&larr; Back</button>';
    h += '</div>';
    return h;
  }
  const st = wsGetUseCaseState(stack, pipelineId, completed);
  const display = (p.name||p.id).replace(/^Usecase\s+\d+:\s*/i,'').trim();
  let stateLabel = '⚪ Ready', stateBg = 'var(--bg2)', stateFg = 'var(--muted)';
  if(st.state === 'completed'){ stateLabel='✓ Done'; stateBg='#d1fae5'; stateFg='#047857'; }
  else if(st.state === 'running'){ stateLabel='⚙ Running'; stateBg='#fef3c7'; stateFg='#92400e'; }
  else if(st.state === 'locked'){ stateLabel='🔒 Locked'; }

  h += '<div class="ws-uc-detail">';
  h += '<div class="ws-uc-detail-h">';
  h += '<button class="ws-uc-detail-back" onclick="wsBackToUseCaseGrid()">&larr; Back</button>';
  h += '<span class="ws-uc-detail-title">'+escHtml(display)+'</span>';
  h += '<span class="ws-uc-detail-status" style="background:'+stateBg+';color:'+stateFg+'">'+stateLabel+'</span>';
  const stepIdsJson = JSON.stringify((p.steps||[]).map(s=>s.id)).replace(/"/g,'&quot;');
  const gid = stack+'-'+pipelineId;
  h += '<button id="runall-'+gid+'" class="ws-uc-detail-runall" onclick="event.stopPropagation();runAllPipelineSteps(\''+stack+'\',\''+pipelineId+'\','+stepIdsJson+')">Run All</button>';
  h += '</div>';

  // Pipeline-level links (declarative from stack.yaml `links:`). Uses the
  // shared _pipeLinkBtnHtml helper so gated_by behavior matches the
  // Pipelines tab capsules exactly.
  if(p.links && p.links.length){
    h += '<div class="ws-uc-detail-links">';
    for(const lnk of p.links){
      if(!lnk || !lnk.url) continue;
      h += _pipeLinkBtnHtml(stack, pipelineId, lnk);
    }
    h += '</div>';
  }

  // Flow panel (declarative `flow:` field in stack.yaml). Teaches testers what
  // the use case shows and the order to click. Optional — pipelines without
  // `flow:` render no panel.
  if(p.flow && p.flow.trim()){
    h += '<div class="ws-uc-flow">'+_wsRenderFlowMarkdown(p.flow)+'</div>';
  }

  // Steps — same DOM the Pipelines panel emits, so existing handlers work.
  h += '<div class="pipe-steps open" id="psteps-'+gid+'">';
  let stepNum = 0;
  let allPrevDone = true;
  // Detect currently-running step from cached step output (so re-renders
  // during polling preserve the green "Running" highlight + badge).
  const cachedOut = window._pipelineStepOutputs || {};
  for(const s of (p.steps||[])){
    stepNum++;
    const key = stack+'/'+pipelineId+'/'+s.id;
    const isDone = !!completed[key];
    const isRerun = s.rerun || (s.name||'').toLowerCase().startsWith('check') || (s.name||'').toLowerCase().startsWith('verify');
    const isManual = s.manual === true;
    const isOptional = s.optional === true;
    const cached = cachedOut[key] || '';
    const isRunning = !isDone && /step-status running/.test(cached);
    const locked = (stepNum > 1 && !allPrevDone && !isRerun && !isRunning) ? ' locked' : '';
    const doneClass = isDone ? ' done' : '';
    const runClass = isRunning ? ' running' : '';
    const numClass = isDone ? 'p-num ok' : (isRunning ? 'p-num run' : 'p-num');
    const numDisplay = isDone ? '✓' : (isRunning ? stepNum : (locked ? '🔒' : stepNum));
    let stepName = s.name;
    let stepStyle = '';
    if(isManual){ stepName += ' [Manual]'; stepStyle = 'border-left:3px solid #ffc107;'; }
    if(isOptional){ stepName += ' [Optional]'; stepStyle = 'border-left:3px solid #6b7280;opacity:0.8;'; }
    // Inline state badge — order matters: running takes priority over done.
    // The default "ready to click" state shows no badge so the row reads as
    // an actionable step without an extra word that adds nothing.
    let stateLabel = '';
    if(isRunning) stateLabel = 'Running';
    else if(isDone) stateLabel = isRerun ? 'Done · Re-run' : 'Done';
    else if(locked) stateLabel = 'Locked';
    else if(isManual) stateLabel = 'Manual';
    else if(isOptional) stateLabel = 'Optional';

    h += '<div class="p-step'+doneClass+runClass+(isRerun?' rerun':'')+locked+'" data-rerun="'+(isRerun||s.rerun?'1':'0')+'" data-manual="'+(isManual?'1':'0')+'" data-optional="'+(isOptional?'1':'0')+'" data-stepnum="'+stepNum+'" style="'+stepStyle+'" data-stack="'+stack+'" data-pid="'+pipelineId+'" data-sid="'+s.id+'">';
    h += '<span class="'+numClass+'" onclick="event.stopPropagation();scrollToStepResult(\''+stack+"','"+pipelineId+"','"+s.id+'\')">'+numDisplay+'</span>';
    h += '<span class="p-label" onclick="runPipelineStep(\''+stack+"','"+pipelineId+"','"+s.id+'\',this.parentElement)">'+escHtml(stepName)+'</span>';
    if(stateLabel) h += '<span class="p-step-state">'+stateLabel+'</span>';
    h += '</div>';
    if(!isDone && !isRerun) allPrevDone = false;
  }
  h += '</div>';

  h += '</div></div>';
  return h;
}

// (The earlier wsEnsurePipelinesTab + tab-based wsOpenUseCase have been
// removed. Pipeline detail now renders inline in the Use Cases pane via
// wsRenderUseCasePipeline, with wsOpenUseCase/wsBackToUseCaseGrid managing
// the _wsSelectedUseCase state.)

// Build the bottom Recent Activity panel HTML (full width).
// Replaces the old in-card activity log with a wider, filterable view.
function wsRenderRecentActivity(stack, pipelinesData){
  const info = (pipelinesData.stacks||{})[stack];
  let h = '<div class="ws-act-panel">';
  h += '<div class="ws-act-h"><span class="ws-pane-icon">📜</span> Recent Activity';
  h += '<button class="ws-act-collapse" onclick="wsCollapseAllActivity(\''+stack+'\')">Collapse all</button>';
  h += '<button class="ws-act-clear" onclick="wsClearActivityForStack(\''+stack+'\')">Clear log</button>';
  h += '</div>';
  // Filter pills — All + one per pipeline
  h += '<div class="ws-act-filters">';
  h += '<button class="ws-act-filter active" data-pid="" onclick="wsActivityFilter(this,\'\')">All</button>';
  for(const p of (info && info.pipelines || [])){
    const display = (p.name||p.id).replace(/^Usecase\s+\d+:\s*/i,'').trim();
    h += '<button class="ws-act-filter" data-pid="'+p.id+'" onclick="wsActivityFilter(this,\''+p.id+'\')">'+escHtml(display)+'</button>';
  }
  h += '</div>';
  h += '<div class="ws-act-log" id="wsUcoLog">'+wsBuildAllStepsLogHtml(stack, pipelinesData)+'</div>';
  h += '</div>';
  return h;
}

// Toggle the active filter pill + show/hide log entries by their data-key prefix.
function wsActivityFilter(btn, pid){
  // Update pill styling
  const filters = document.querySelectorAll('.ws-act-filter');
  filters.forEach(f => f.classList.toggle('active', f === btn));
  // Toggle entries
  const items = document.querySelectorAll('#wsUcoLog .ws-log-item');
  items.forEach(it => {
    const key = it.getAttribute('data-key') || '';
    const matches = !pid || key.indexOf('/'+pid+'/') !== -1;
    it.classList.toggle('filter-hidden', !matches);
  });
}

// Clear cached step outputs for a stack — wipes both the in-memory cache and
// the backend's persisted history so Recent Activity stays empty after the
// user reloads or re-opens on another browser. Pipeline completion state
// (lockout) is preserved.
// Collapse every log entry for this stack. Frontend-only; preserves cache + logs.
function wsCollapseAllActivity(stack){
  const items = document.querySelectorAll('#wsUcoLog .ws-log-item');
  items.forEach(d => {
    const key = d.getAttribute('data-key') || '';
    if(!key.startsWith(stack+'/')) return;
    d.removeAttribute('open');
    if(window._wsLogOpenSet) window._wsLogOpenSet.delete(key);
  });
}

async function wsClearActivityForStack(stack){
  if(!confirm('Clear activity log for this stack? Pipeline completion state is preserved.')) return;
  const out = window._pipelineStepOutputs || {};
  for(const k of Object.keys(out)){
    if(k.startsWith(stack+'/')) delete out[k];
  }
  try{
    await fetch('/api/pipelines/step-output/'+encodeURIComponent(stack), {method:'DELETE'});
  }catch(e){ console.warn('Backend clear failed:', e); }
  // Re-hydrate marker so a later wsRefreshSidebar tick doesn't re-pull what
  // we just cleared. The DELETE returned 0 rows so there's nothing to fetch
  // anyway, but skipping the fetch saves a round-trip.
  if(window._wsHydrated) window._wsHydrated.add(stack);
  if(typeof wsRenderHome === 'function') wsRenderHome();
}

// Pull persisted step-output bodies from the backend and re-format them into
// the `_pipelineStepOutputs` cache that `wsBuildAllStepsLogHtml` reads.
//
// The backend stores raw stdout lines per step (see `_step_output_history`);
// here we re-run `formatStepOutput()` so the panel renders identically to a
// freshly-completed step. Idempotent: skipped if we've already hydrated this
// stack in the current page session, OR if the cache already has entries
// for this stack (meaning a live run populated it during this session).
async function wsRehydrateStepOutputs(stack){
  if(!stack) return;
  if(!window._wsHydrated) window._wsHydrated = new Set();
  if(window._wsHydrated.has(stack)) return;
  window._wsHydrated.add(stack);
  // If the in-memory cache already has rows for this stack, the user has
  // run steps in this session — don't overwrite with backend snapshot.
  const out = window._pipelineStepOutputs || {};
  for(const k of Object.keys(out)){
    if(k.startsWith(stack+'/')) return;
  }
  try{
    const r = await fetch('/api/pipelines/step-output/'+encodeURIComponent(stack));
    const data = await r.json();
    let hydrated = 0;
    for(const key of Object.keys(data)){
      const entry = data[key] || {};
      const lines = entry.lines || [];
      const elapsedSuffix = entry.elapsed_ms ? ' ('+entry.elapsed_ms+'ms)' : '';
      const status = entry.success ? 'ok' : 'err';
      window._pipelineStepOutputs[key] = formatStepOutput(lines, status, elapsedSuffix, null);
      hydrated++;
    }
    if(hydrated > 0){
      // Re-render Recent Activity if it's currently mounted for this stack.
      const log = document.getElementById('wsUcoLog');
      if(log && stack === wsActiveStack){
        const pipelinesData = {stacks: {[stack]: (window._wsStacks||{})[stack] || {}}};
        log.innerHTML = wsBuildAllStepsLogHtml(stack, pipelinesData);
      }
    }
  }catch(e){
    console.warn('Step output rehydrate failed for', stack, e);
  }
}

// Build the Live strip HTML (top of home, full width). Reuses the existing
// wsLive* element IDs so wsRefreshLiveReadout keeps populating them.
function wsRenderLiveStrip(stack, pipelinesData){
  const info = (pipelinesData.stacks||{})[stack];
  if(!info) return '';
  // Hide the Synthetic Data card on NF deploys. synthdb is a laptop-only
  // tool (engine/synthdb runs as a local Docker container at :8050). On NF
  // the OLTP data flow comes from the oltp-seed Job + Bank App's own
  // generation — exposing Start/Stop buttons that would only spin up
  // synthdb on the user's laptop (which can't reach NF pgd anyway) would
  // be misleading.
  if((_deployTargets||{})[stack] === 'northflank') return '';
  // Find runtime_controls to render a Start/Stop button inline
  const rcItems = [];
  for(const p of (info.pipelines||[])){
    for(const c of (p.runtime_controls||[])){
      rcItems.push({ctrl:c, pipelineId:p.id, pipelineName:p.name});
    }
  }
  // Detect the "active" pipeline (most recently touched) for the right-side label
  let activeName = '';
  const out = window._pipelineStepOutputs || {};
  let latestKey = null;
  for(const k of Object.keys(out)){ if(k.startsWith(stack+'/')) latestKey = k; }
  if(latestKey){
    const lpid = latestKey.split('/')[1];
    const lp = (info.pipelines||[]).find(p => p.id === lpid);
    if(lp) activeName = (lp.name||lp.id||'').replace(/^Usecase\s+\d+:\s*/i,'').trim();
  }

  // Use Case Controls card — wraps the live readout + all runtime control
  // buttons (synthetic data, Airflow reconciliation, etc.).
  let h = '<div class="ws-synth-card">';
  h += '<div class="ws-synth-card-h">';
  h += '<span class="ws-synth-icon">🔄</span>';
  h += '<span class="ws-synth-title">Use Case Controls</span>';
  h += '<span class="ws-synth-sub">Live transactions + scheduled reconciliation drive the CDC fan-out demo</span>';
  if(activeName){
    h += '<span class="ws-synth-pipe">'+escHtml(activeName)+'</span>';
  }
  h += '</div>';
  // One row per runtime control: [Name] [info: live counters / last-run] [Button]
  // Display name is derived from label_start by stripping the leading ▶/■
  // glyph + "Start "/"Stop " — keeps the row label clean ("Synthetic Data",
  // "Airflow Reconciliation") without needing a separate field in stack.yaml.
  const _rtcName = (lbl) => (lbl||'').replace(/^[\s▶■⏹▶◼▶■⏹]*\s*(Start|Stop)\s+/i, '').trim();
  const completed = pipelinesData.completed || {};
  for(const item of rcItems){
    const c = item.ctrl;
    const reqStep = c.enabled_after;
    const reqDone = !reqStep || !!completed[stack+'/'+item.pipelineId+'/'+reqStep];
    const btnId = 'ucobtn-'+stack+'-'+item.pipelineId+'-'+c.id;
    const labelStart = (c.label_start||'Start');
    const labelStop  = (c.label_stop ||'Stop');
    const displayName = _rtcName(labelStart) || c.id;
    // Info area content: synth-data toggles host the live counters (wsLive*
    // IDs are populated by wsRefreshLiveReadout). Other controls (e.g. the
    // airflow toggle with last_run_url) show their own status pill.
    const isSynth = /sim|synth|synthetic/i.test(c.id);
    let infoHtml = '';
    if(isSynth){
      infoHtml = '<span class="ws-live-dot" id="wsLiveDot"></span>'
               + '<span class="ws-live-state" id="wsLiveState">Idle</span>'
               + '<span class="ws-live-stat" id="wsLiveTx">— transactions</span>'
               + '<span class="ws-live-stat" id="wsLiveRate">—</span>'
               + '<span class="ws-live-stat" id="wsLiveFraud">—</span>'
               + '<span class="ws-live-stat ws-live-store" id="wsLiveCh" style="display:none">—</span>'
               + '<span class="ws-live-stat ws-live-store" id="wsLiveRw" style="display:none">—</span>';
    }
    if(c.last_run_url){
      infoHtml += '<span class="rtc-last-run" data-stack="'+stack+'" data-pid="'+item.pipelineId
                + '" data-cid="'+c.id+'" style="display:none"></span>';
    }
    h += '<div class="ws-uco-row" data-cid="'+c.id+'">';
    h += '<span class="ws-uco-row-name">'+escHtml(displayName)+'</span>';
    h += '<div class="ws-uco-row-info">'+infoHtml+'</div>';
    h += '<button id="'+btnId+'" class="pipe-rt-btn ws-uco-btn" '
       + 'data-stack="'+stack+'" data-pid="'+item.pipelineId+'" data-cid="'+c.id+'" '
       + 'data-req-step="'+escHtml(reqStep||'')+'" '
       + 'data-label-start="'+escHtml(labelStart)+'" data-label-stop="'+escHtml(labelStop)+'" '
       + 'data-running="false" data-enabled="'+(reqDone?'1':'0')+'" '
       + (reqDone?'':'disabled ')
       + 'style="min-width:auto;padding:6px 14px;font-size:12px" '
       + 'onclick="toggleRuntimeControl(this)">'+escHtml(labelStart)+'</button>';
    h += '</div>';
  }
  h += '</div>'; // end ws-synth-card
  return h;
}

function wsRenderHome(){
  const ct=document.getElementById('wsHomeInner');
  if(!ct)return;
  const running=window._wsRunning||[];
  const allStacks=window._wsStacks||{};

  if(!running.length){
    ct.innerHTML='<div class="ws-empty"><div class="ws-empty-icon">&#9881;</div><div class="ws-empty-title">No stacks running</div><div class="ws-empty-desc">Deploy a stack from the Integrations tab to see its apps and terminals here.</div></div>';
    return;
  }

  let h='';

  // Stack selector pills (segmented control) — only when 2+ stacks
  if(running.length>1){
    h+='<div class="ws-stack-sel">';
    for(const sName of running){
      const meta=allStacks[sName]||{};
      const color=wsStackColors[sName]||'#4a90d9';
      const isActive=wsActiveStack===sName;
      h+='<div class="ws-stack-btn'+(isActive?' active':'')+'" style="--sc:'+color+'" onclick="wsSelectStack(\''+sName+'\')">';
      h+='<span class="wsb-dot"></span>';
      h+='<div class="wsb-info"><div class="wsb-name">'+(meta.name||sName)+'</div>';
      h+='<div class="wsb-meta">loading...</div></div></div>';
    }
    h+='</div>';
  }

  if(!wsActiveStack){ct.innerHTML=h;return}

  // Reset window._wsApps so wsOpenAppDetail picks up fresh definitions on each render.
  window._wsApps={};

  // 2-col layout: left column stacks Synthetic Data above Dashboard so the
  // right column (Use Cases) can claim full vertical from the top.
  ct.innerHTML = h
    + '<div class="ws-home-2col">'
      + '<div class="ws-home-left">'
        + '<div id="wsLiveStripHost"></div>'
        + '<div id="wsDashHost">'+wsRenderDashboard(wsActiveStack, allStacks)+'</div>'
      + '</div>'
      + '<div id="wsUseCasesHost"><div class="ws-pane"><div class="ws-pane-h"><span class="ws-pane-icon">&#127919;</span> Use Cases</div><div class="ws-empty-hint">Loading...</div></div></div>'
    + '</div>'
    + '<div id="wsActivityHost"></div>';

  // Update pill meta
  if(running.length>1){
    const pills=ct.querySelectorAll('.ws-stack-btn');
    running.forEach((sName,i)=>{
      const m=allStacks[sName]||{};
      const ac=(m.access||[]).filter(a=>a.url&&!a.url.startsWith('psql'));
      const metaEl=pills[i]&&pills[i].querySelector('.wsb-meta');
      if(metaEl)metaEl.textContent=ac.length+' app'+(ac.length!==1?'s':'')+' · loading terminals...';
    });
  }

  // Async: terminals + pipeline-driven panes
  wsLoadTerminals(wsActiveStack,running,allStacks);
  wsRenderHomePipelineAsync(wsActiveStack);
  if(typeof _kickLiveReadout==='function') _kickLiveReadout();
}

// Async fetch of /api/pipelines; populates Live Strip, Use Cases grid,
// and Recent Activity panel.
async function wsRenderHomePipelineAsync(stack){
  let pd = window._pipelineData;
  try{
    if(!pd){
      const r = await fetch('/api/pipelines');
      pd = await r.json();
      window._pipelineData = pd;
    }
  }catch(e){ pd = {stacks:{}, completed:{}}; }

  const liveHost = document.getElementById('wsLiveStripHost');
  const ucHost = document.getElementById('wsUseCasesHost');
  const actHost = document.getElementById('wsActivityHost');

  if(liveHost) liveHost.innerHTML = wsRenderLiveStrip(stack, pd);
  if(ucHost)   ucHost.innerHTML   = wsRenderUseCaseGrid(stack, pd);
  if(actHost)  actHost.innerHTML  = wsRenderRecentActivity(stack, pd);

  const root = document.getElementById('wsHomeInner');
  if(root){
    setTimeout(()=>{
      root.querySelectorAll('.ws-uco-btn[data-enabled="1"]').forEach(refreshRuntimeControlStatus);
    }, 100);
  }
  setTimeout(()=>{
    if(typeof wsUcoLogTailSetup==='function') wsUcoLogTailSetup();
    const logCt = document.getElementById('wsUcoLog');
    if(logCt){ logCt._tailing=true; logCt.scrollTop=logCt.scrollHeight; }
  }, 50);
}

// ── Below: legacy 3-col render code preserved as a fallback (unreachable). ──
// Kept intact (never invoked) so a quick git revert of the redesign restores
// the old behaviour without re-pasting hundreds of lines. The legacy entry
// would have started with `const meta=allStacks[wsActiveStack]||{};`.
function _wsRenderHomeLegacy_DO_NOT_CALL(){
  const ct=document.getElementById('wsHomeInner');
  const allStacks=window._wsStacks||{};
  let h='';
  const meta=allStacks[wsActiveStack]||{};
  const access=meta.access||[];
  const credentials=meta.credentials||[];
  const apps=access.filter(a=>a.url&&!a.url.startsWith('psql'));
  const stackColor=wsStackColors[wsActiveStack]||'#4a90d9';

  // Data Story banner moved into the Use Case Activity card (rendered below).
  // The pills now live inside that card alongside the Start/Stop button + logs.

  // Terminal commands loaded async — don't block render
  let cmds=[];

  // Store app data for tabs
  window._wsApps={};

  // Separate Metabase from other consoles
  let mbApp=null,otherApps=[];
  for(const a of apps){
    if(a.name.toLowerCase().includes('metabase')){mbApp=a}else{otherApps.push(a)}
  }

  // 1x3 grid: Metabase | UI Consoles | Terminals
  h+='<div class="ws-grid-3col">';

  // Cell r1c1: Metabase (primary tile inside a quadrant card)
  h+='<div class="ws-quad"><div class="ws-quad-h">Metabase</div><div class="ws-quad-body">';
  if(mbApp){
    const safeId=wsActiveStack+'-'+mbApp.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
    const isOpen=wsOpenTabs.has(safeId);
    const matchedCred=credentials.find(c=>(c.service||'').toLowerCase()==='metabase');
    window._wsApps[safeId]={name:mbApp.name,url:mbApp.url,desc:mbApp.note||mbApp.description||'',
      cred:matchedCred||null,stack:wsActiveStack,initials:'MB'};
    h+='<div class="ws-tile ws-tile-primary'+(isOpen?' ws-tile-open':'')+'" onclick="wsOpenAppDetail(\''+safeId+'\')">';
    h+='<div class="ws-tile-icon">MB</div>';
    h+='<div class="ws-tile-body"><div class="ws-tile-name">'+escHtml(mbApp.name)+'</div>';
    h+='<div class="ws-tile-status">'+(isOpen?'<span class="ws-tile-dot open"></span>Open':'Click to launch')+'</div></div>';
    h+='</div>';
  }else{
    h+='<div class="ws-empty-hint">Metabase not configured for this stack</div>';
  }
  h+='</div></div>';

  // Cell r1c2: UI Consoles (everything that's not Metabase)
  h+='<div class="ws-quad"><div class="ws-quad-h">UI Consoles <span class="ws-section-count">'+otherApps.length+'</span></div><div class="ws-quad-body">';
  if(otherApps.length){
    h+='<div class="ws-tile-grid">';
    for(const a of otherApps){
      const safeId=wsActiveStack+'-'+a.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
      const initials=a.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
      const isOpen=wsOpenTabs.has(safeId);
      const appNameLower=a.name.toLowerCase();
      const matchedCred=credentials.find(c=>{
        const svcLower=(c.service||'').toLowerCase();
        return appNameLower.includes(svcLower)||svcLower.includes(appNameLower.split(' ')[0]);
      });
      window._wsApps[safeId]={name:a.name,url:a.url,desc:a.note||a.description||'',
        cred:matchedCred||null,stack:wsActiveStack,initials:initials,target:a.target||null};
      h+='<div class="ws-tile'+(isOpen?' ws-tile-open':'')+'" onclick="wsOpenAppDetail(\''+safeId+'\')">';
      h+='<div class="ws-tile-icon">'+escHtml(initials)+'</div>';
      h+='<div class="ws-tile-body"><div class="ws-tile-name">'+escHtml(a.name)+'</div>';
      h+='<div class="ws-tile-status">'+(isOpen?'<span class="ws-tile-dot open"></span>Open':'Click')+'</div></div>';
      h+='</div>';
    }
    h+='</div>';
  }else{
    h+='<div class="ws-empty-hint">No console UIs available</div>';
  }
  h+='</div></div>';

  // Cell c3: Terminals (loaded async)
  h+='<div class="ws-quad"><div class="ws-quad-h">Terminals <span class="ws-section-count" id="wsTermCount">...</span></div><div class="ws-quad-body" id="wsTermBody"><div class="ws-empty-hint">Loading...</div></div></div>';

  h+='</div>';  // end ws-grid-3col

  // (Legacy Section 1: Metabase fully removed — replaced by Apps tile grid)

  // (Legacy Section 2 + 3 + 4 removed below — replaced by tile grid + creds card above)
  if(false){
    const conOpen=_wsSecState['consoles']===true;
    if(otherApps.length){
      for(const a of otherApps){
        const safeId=wsActiveStack+'-'+a.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
        const initials=a.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
        const isOpen=wsOpenTabs.has(safeId);
        const appNameLower=a.name.toLowerCase();
        const matchedCred=credentials.find(c=>{
          const svcLower=(c.service||'').toLowerCase();
          return appNameLower.includes(svcLower)||svcLower.includes(appNameLower.split(' ')[0]);
        });
        window._wsApps[safeId]={name:a.name,url:a.url,desc:a.note||a.description||'',
          cred:matchedCred||null,stack:wsActiveStack,initials:initials};
        h+='<div class="ws-row" onclick="wsOpenAppDetail(\''+safeId+'\')">';
        h+='<div class="wr-icon app">'+initials+'</div>';
        h+='<div class="wr-main"><div class="wr-name">'+escHtml(a.name)+'</div>';
        let descParts=[];
        if(a.note||a.description)descParts.push(a.note||a.description);
        if(matchedCred){
          let credStr=(matchedCred.username||'')+'/'+(matchedCred.password||'');
          if(matchedCred.port)credStr+=' (port '+matchedCred.port+')';
          descParts.push(credStr);
        }
        h+='<div class="wr-desc">'+escHtml(descParts.join(' \u2014 '))+'</div></div>';
        if(isOpen)h+='<span class="wr-open-badge">Open</span>';
        h+='</div>';
      }
      h+='</div>';
    }else{
      h+='<div class="ws-empty-hint">No console apps available</div>';
    }
    h+='</div></div>';
  }

  // ── Section 5: Use Case Output (runtime controls) ──
  // Only rendered for stacks with at least one runtime_control declared in
  // any of their pipelines. Built async from /api/pipelines.
  h += '<div id="wsUcoSection"></div>';

  ct.innerHTML=h;

  // Update pill meta (app count now, terminal count after async load)
  if(running.length>1){
    const pills=ct.querySelectorAll('.ws-stack-btn');
    running.forEach((sName,i)=>{
      const m=allStacks[sName]||{};
      const ac=(m.access||[]).filter(a=>a.url&&!a.url.startsWith('psql'));
      const metaEl=pills[i]&&pills[i].querySelector('.wsb-meta');
      if(metaEl)metaEl.textContent=ac.length+' app'+(ac.length!==1?'s':'')+' \u00b7 loading terminals...';
    });
  }

  // Async terminal loading — doesn't block initial render
  wsLoadTerminals(wsActiveStack,running,allStacks);

  // Use Case Output section (runtime controls from pipelines)
  wsRenderUseCaseOutput(wsActiveStack);

  // Refresh Data Story banner (idempotent)
  wsRefreshDataStory();
}

// ── Live Use Case readout poller ─────────────────────────────────────────────
// Polls /api/data-activity to update the inline TX counter + rate + fraud %
// next to the Start/Stop button. Calls itself every 3s while the workspace
// home is visible.
const _wsLive = {prevTx: 0, prevTs: 0, lastStack: null};
async function wsRefreshLiveReadout(){
  const live = document.getElementById('wsUcoLive');
  if(!live) return;
  const stack = wsActiveStack;
  if(!stack) return;
  let d;
  try{
    const r = await fetch('/api/data-activity/'+encodeURIComponent(stack));
    d = await r.json();
  }catch(_){ return; }
  const synth = (d && d.synthesized) || {};
  const stream = (d && d.streaming) || {};
  const olap = (d && d.olap) || {};
  const running = !!stream.sim_running;

  const dot = document.getElementById('wsLiveDot');
  const stateEl = document.getElementById('wsLiveState');
  const txEl = document.getElementById('wsLiveTx');
  const rateEl = document.getElementById('wsLiveRate');
  const fraudEl = document.getElementById('wsLiveFraud');
  const chEl = document.getElementById('wsLiveCh');
  const rwEl = document.getElementById('wsLiveRw');
  if(!dot||!stateEl||!txEl||!rateEl||!fraudEl) return;

  // State + dot color
  if(running){ dot.className='ws-live-dot live'; stateEl.textContent='LIVE'; stateEl.className='ws-live-state live'; }
  else { dot.className='ws-live-dot'; stateEl.textContent='Idle'; stateEl.className='ws-live-state'; }

  const cur = synth.transactions || 0;
  txEl.textContent = cur.toLocaleString('en-US') + ' transactions';

  // Compute TX/min from delta across polls (only when sim is running)
  if(running && stack === _wsLive.lastStack && _wsLive.prevTs){
    const dt = (Date.now() - _wsLive.prevTs) / 1000;
    const dn = Math.max(0, cur - _wsLive.prevTx);
    if(dt > 0){
      const perMin = Math.round((dn / dt) * 60);
      rateEl.textContent = perMin > 0 ? perMin.toLocaleString('en-US') + ' TX/min' : 'starting…';
    }
  } else {
    rateEl.textContent = running ? 'starting…' : '0 TX/min';
  }
  _wsLive.prevTx = cur; _wsLive.prevTs = Date.now(); _wsLive.lastStack = stack;

  const pct = synth.fraud_rate_pct;
  fraudEl.textContent = (pct === undefined || pct === null) ? '— fraud' : (pct.toFixed(1) + '% fraud');

  // OLAP fan-out: show ClickHouse / RisingWave lag pills only when those
  // containers are up (backend returns null when not running).
  if(chEl){
    if(olap.clickhouse === null || olap.clickhouse === undefined){
      chEl.style.display = 'none';
    } else {
      chEl.style.display = '';
      const lag = olap.ch_lag_pct;
      chEl.textContent = 'ClickHouse: ' + olap.clickhouse.toLocaleString('en-US')
        + (lag !== null && lag !== undefined ? ' (lag '+lag.toFixed(1)+'%)' : '');
    }
  }
  if(rwEl){
    if(olap.risingwave === null || olap.risingwave === undefined){
      rwEl.style.display = 'none';
    } else {
      rwEl.style.display = '';
      const lag = olap.rw_lag_pct;
      rwEl.textContent = 'RisingWave: ' + olap.risingwave.toLocaleString('en-US')
        + (lag !== null && lag !== undefined ? ' (lag '+lag.toFixed(1)+'%)' : '');
    }
  }
}
// Poll while the workspace tab is active
setInterval(()=>{
  if(_currentTab==='workspace') wsRefreshLiveReadout();
}, 3000);
// First-render update (after card appears)
function _kickLiveReadout(){ setTimeout(wsRefreshLiveReadout, 500); }

// ── Use Case Activity card: data-story pills + runtime controls + recent logs ──
// Compatibility shim: the pipeline-polling code calls this when a step
// first appears (no log entry yet exists in the DOM). With the new home
// layout the live strip + use case grid + activity panel live in their
// own hosts; we just delegate to wsRenderHomePipelineAsync which knows
// how to repaint all three from the latest pipeline data.
async function wsRenderUseCaseOutput(stack){
  // Drop the cached pipeline-data snapshot so the next async render picks
  // up the freshly-completed step. The async function will re-fetch.
  window._pipelineData = null;
  if(typeof wsRenderHomePipelineAsync === 'function'){
    await wsRenderHomePipelineAsync(stack);
  }
}

// Toggle the small Credentials dropdown panel.
function wsToggleCredsDD(btn){
  const panel = btn.parentElement.querySelector('.ws-creds-dd-panel');
  if(!panel) return;
  panel.classList.toggle('open');
  btn.classList.toggle('active');
}
// Toggle a generic list dropdown (Other UIs, Terminals).
function wsToggleListDD(btn){
  const panel = btn.parentElement.querySelector('.ws-list-dd-panel');
  if(!panel) return;
  // Close any other open list dropdown first (single-open behaviour)
  document.querySelectorAll('.ws-list-dd-panel.open').forEach(p => { if(p !== panel) p.classList.remove('open'); });
  document.querySelectorAll('.ws-list-dd-btn.active').forEach(b => { if(b !== btn) b.classList.remove('active'); });
  panel.classList.toggle('open');
  btn.classList.toggle('active');
}
// Close any dropdown when clicking outside it.
document.addEventListener('click', (e)=>{
  if(!e.target.closest('.ws-creds-dd-wrap')){
    document.querySelectorAll('.ws-creds-dd-panel.open').forEach(p=>p.classList.remove('open'));
    document.querySelectorAll('.ws-creds-dd-btn.active').forEach(b=>b.classList.remove('active'));
  }
  if(!e.target.closest('.ws-list-dd-wrap')){
    document.querySelectorAll('.ws-list-dd-panel.open').forEach(p=>p.classList.remove('open'));
    document.querySelectorAll('.ws-list-dd-btn.active').forEach(b=>b.classList.remove('active'));
  }
});

// Tail-intent for the activity log container: install a scroll listener
// (once per element) that toggles ._tailing based on whether the user is
// near the bottom. Default to tailing. Pollers consult ._tailing before
// auto-scrolling, so the user can freely scroll up to read older entries
// without being yanked back to the bottom on every poll tick.
function wsUcoLogTailSetup(){
  const logCt = document.getElementById('wsUcoLog');
  if(!logCt || logCt._tailListenerAdded) return;
  logCt._tailListenerAdded = true;
  if(typeof logCt._tailing !== 'boolean') logCt._tailing = true;
  logCt.addEventListener('scroll', ()=>{
    const dist = logCt.scrollHeight - logCt.scrollTop - logCt.clientHeight;
    logCt._tailing = dist < 80;  // within 80px of bottom = "still tailing"
  }, {passive: true});
}

// Track which log entries are currently expanded so re-rendering doesn't
// collapse them on the user.
window._wsLogOpenSet = window._wsLogOpenSet || new Set();
function wsToggleLogEntry(detailsEl, key){
  // Wrapped because <details> 'toggle' fires after attribute changes
  setTimeout(()=>{
    if(detailsEl.open){
      window._wsLogOpenSet.add(key);
      // When user just expanded, jump to the bottom of the raw <pre>
      const pre = detailsEl.querySelector('.ws-log-body pre');
      if(pre) pre.scrollTop = pre.scrollHeight;
    } else {
      window._wsLogOpenSet.delete(key);
    }
  }, 0);
}

// Returns HTML for ALL pipeline step outputs for this stack, as a list of
// collapsible <details> entries in chronological order (oldest first, newest
// at the bottom). Combined with auto-scroll-to-bottom of .ws-uco-log, this
// gives a tail-f UX so the running step's latest output is always in view.
function wsBuildAllStepsLogHtml(stack, pipelinesData){
  const out = window._pipelineStepOutputs || {};
  // Sort by pipeline declaration order (UC 1,2,3,4,5,6) — not click order —
  // so the panel reads top-to-bottom in the same sequence as the Use Cases
  // tab even if the user runs them out of order (e.g. UC1 → UC2 → UC5).
  const info = (pipelinesData && pipelinesData.stacks || {})[stack];
  const pipelineOrder = {};
  (info && info.pipelines || []).forEach((p, idx) => { pipelineOrder[p.id] = idx; });
  const keys = [];
  for(const k of Object.keys(out)){
    if(k.startsWith(stack+'/')) keys.push(k);
  }
  keys.sort((a, b) => {
    const pidA = a.split('/')[1], pidB = b.split('/')[1];
    const idxA = (pidA in pipelineOrder) ? pipelineOrder[pidA] : 999;
    const idxB = (pidB in pipelineOrder) ? pipelineOrder[pidB] : 999;
    return idxA - idxB;
  });
  if(!keys.length){
    return '<div class="ws-empty-hint" style="padding:10px 0">No steps run yet — go to Use Cases tab and click <b>Start Service</b>.</div>';
  }
  // Build a pid/sid → friendly-name lookup from stack.yaml so the activity
  // row reads as "OLTP — Start Service" instead of "oltp / start-service".
  const pipeMap = {};
  for(const p of (info && info.pipelines || [])){
    const dispName = (p.name||p.id).replace(/^Usecase\s+\d+:\s*/i,'').trim();
    const stepNames = {};
    for(const s of (p.steps||[])) stepNames[s.id] = s.name || s.id;
    pipeMap[p.id] = {name: dispName, steps: stepNames};
  }
  // Backend's completed map overrides cached "running" content. Some completion
  // handler paths (early-return for already-done steps, etc.) skip the cache
  // overwrite, leaving stale 'step-status running' even though the step finished.
  const completedMap = (pipelinesData && pipelinesData.completed) || {};
  let h = '';
  for(const k of keys){
    const parts = k.split('/');
    const pid = parts[1], sid = parts[2];
    const cached = out[k] || '';
    let stat='ok', label='Completed';
    if(/step-status err/.test(cached)){ stat='err'; label='Failed'; }
    else if(completedMap[k]){ stat='ok'; label='Completed'; }
    else if(/step-status running/.test(cached)){ stat='running'; label='Running'; }
    // Pull elapsed_ms out of the cached "(1234ms)" already rendered inside
    // the body. Cheaper than threading a parallel data structure through;
    // the body HTML is the source of truth for per-step state.
    const m = cached.match(/\((\d+)ms\)/);
    const durLabel = m ? _wsFmtElapsed(parseInt(m[1],10)) : '';
    const isOpen = window._wsLogOpenSet.has(k);
    const escKey = k.replace(/'/g,"\\'");
    const friendlyPipe = (pipeMap[pid] && pipeMap[pid].name) || pid;
    const friendlyStep = (pipeMap[pid] && pipeMap[pid].steps && pipeMap[pid].steps[sid]) || sid;
    const friendlyLabel = friendlyPipe + ' — ' + friendlyStep;
    const hintHtml = stat==='running'
      ? '<span class="ws-log-hint">click row to view logs</span>'
      : '<span class="ws-log-hint">click to view logs</span>';
    h += '<details class="ws-log-item ws-log-'+stat+'" data-key="'+k+'"'+(isOpen?' open':'')+' ontoggle="wsToggleLogEntry(this,\''+escKey+'\')">';
    h += '<summary><span class="ws-log-chev" aria-hidden="true">▸</span>';
    h += '<span class="ws-log-mark">'+(stat==='ok'?'✓':stat==='err'?'✗':'…')+'</span>';
    h += '<span class="ws-log-label">'+escHtml(friendlyLabel)+'</span>';
    h += hintHtml;
    h += '<span class="ws-log-state">'+label+(durLabel?' · '+durLabel:'')+'</span></summary>';
    h += '<div class="ws-log-body">'+cached+'</div>';
    h += '</details>';
  }
  return h;
}

// Format elapsed milliseconds for the Recent Activity row summary.
// < 1 s → "<1s"   |   < 60 s → "12s"   |   ≥ 60 s → "1m 24s"
function _wsFmtElapsed(ms){
  if(!ms || ms < 0) return '';
  if(ms < 1000) return '<1s';
  const s = Math.round(ms / 1000);
  if(s < 60) return s + 's';
  const m = Math.floor(s / 60);
  const rs = s % 60;
  return m + 'm ' + rs + 's';
}

// Minimal markdown renderer for stack.yaml `flow:` text. Supports **bold**,
// `code`, and numbered lists (lines starting with "N. ").
//
// Output shape:
//   <div class="ws-uc-flow-intro">...one-liner overview...</div>
//   <details class="ws-uc-flow-details">
//     <summary>Show steps</summary>
//     <ol>...numbered narrative...</ol>
//   </details>
//
// The intro stays visible always; the step list is collapsed by default so the
// flow panel doesn't dominate the screen. Pipelines whose flow text has only
// an intro (no numbered list) render just the intro with no disclosure.
function _wsRenderFlowMarkdown(text){
  const inline = (s) => {
    let x = escHtml(s);
    x = x.replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>');
    x = x.replace(/`([^`]+)`/g, '<code>$1</code>');
    return x;
  };
  const lines = text.replace(/\r\n/g, '\n').split('\n');
  let intro = '';
  let list = '';
  let inList = false;
  let introLines = [];
  let listSeen = false;
  let stepCount = 0;
  for(const raw of lines){
    const line = raw.trimEnd();
    const m = line.match(/^\s*(\d+)\.\s+(.*)$/);
    if(m){
      // First numbered item — flush intro buffer.
      if(introLines.length && !listSeen){
        intro += '<div class="ws-uc-flow-intro">'+introLines.map(inline).join(' ')+'</div>';
        introLines = [];
      }
      if(!inList){ list += '<ol>'; inList = true; listSeen = true; }
      list += '<li>'+inline(m[2])+'</li>';
      stepCount++;
    } else if(line.trim() === ''){
      if(inList){ list += '</ol>'; inList = false; }
    } else {
      if(inList){ list += '</ol>'; inList = false; }
      // Stop appending to intro once the list has started — trailing
      // commentary after the steps would otherwise visually bleed up.
      if(!listSeen) introLines.push(line);
    }
  }
  if(introLines.length && !listSeen){
    intro += '<div class="ws-uc-flow-intro">'+introLines.map(inline).join(' ')+'</div>';
  }
  if(inList) list += '</ol>';

  if(!list) return intro;
  const summary = 'Show steps ('+stepCount+')';
  return intro +
    '<details class="ws-uc-flow-details">' +
    '<summary>'+summary+'</summary>' +
    list +
    '</details>';
}

// ── Data Story banner: Seed → Synthesize → Stream ─────────────────────────
const _wsTxRateState = {prev: 0, prevTs: 0};
function _wsFmtNum(n){
  n = Number(n) || 0;
  if(n >= 1000) return n.toLocaleString('en-US');
  return String(n);
}
async function wsRefreshDataStory(){
  const banner = document.getElementById('wsDataStory');
  if(!banner) return;
  const stack = banner.dataset.stack;
  if(!stack) return;
  let d;
  try{
    const r = await fetch('/api/data-activity/'+encodeURIComponent(stack));
    d = await r.json();
  }catch(_){ return; }
  // Seed
  const seedRows = (d.seed && d.seed.rows) || 0;
  banner.querySelector('[data-field="seed_rows"]').textContent = _wsFmtNum(seedRows);
  // Synthesize: sum customer+account+transaction rows for the headline number
  const s = d.synthesized || {};
  const synthTotal = (s.customers||0) + (s.accounts||0) + (s.transactions||0);
  banner.querySelector('[data-field="synth_rows"]').textContent = _wsFmtNum(synthTotal);
  banner.querySelector('[data-field="fraud_rate"]').textContent = (s.fraud_rate_pct ?? 0).toFixed(1) + '%';
  // Stream: tx-per-min from delta of synthesized.transactions across polls
  const streamPill = banner.querySelector('.ws-ds-stream');
  const txField = banner.querySelector('[data-field="tx_rate"]');
  const subField = banner.querySelector('[data-field="stream_sub"]');
  const running = !!(d.streaming && d.streaming.sim_running);
  streamPill.dataset.running = running ? 'true' : 'false';
  const now = Date.now();
  const cur = s.transactions || 0;
  if(running){
    let rate = 0;
    if(_wsTxRateState.prevTs && cur >= _wsTxRateState.prev){
      const dt = (now - _wsTxRateState.prevTs) / 1000; // seconds
      const dn = cur - _wsTxRateState.prev;
      if(dt > 0) rate = Math.round((dn / dt) * 60); // per minute
    }
    txField.innerHTML = '&#x25CF; ' + (rate>0 ? rate.toLocaleString('en-US') + ' <span style="font-size:11px;font-weight:500">TX/min</span>' : 'starting…');
    subField.innerHTML = 'Live &middot; <span style="text-decoration:underline">Open Bank App Phase 2 &rarr;</span>';
  }else{
    txField.textContent = 'idle';
    subField.textContent = 'Click ▶ Start Synthetic Data in pipeline';
  }
  _wsTxRateState.prev = cur;
  _wsTxRateState.prevTs = now;
}
function wsOpenMetabaseFromBanner(){
  // Find Metabase access entry of the active stack and open it
  const allStacks = window._wsStacks||{};
  const meta = allStacks[wsActiveStack]||{};
  const mb = (meta.access||[]).find(a => (a.name||'').toLowerCase().includes('metabase'));
  if(!mb){ addMsg('assistant','Metabase access not configured for this stack.'); return; }
  const safeId = wsActiveStack+'-'+mb.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
  if(!window._wsApps[safeId]){
    const initials = mb.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
    window._wsApps[safeId]={name:mb.name,url:mb.url,desc:'',cred:null,stack:wsActiveStack,initials:initials};
  }
  wsOpenAppDetail(safeId);
}
function wsOpenBankAppFromBanner(){
  const allStacks = window._wsStacks||{};
  const meta = allStacks[wsActiveStack]||{};
  const ba = (meta.access||[]).find(a => (a.name||'').toLowerCase().includes('bank app'));
  if(!ba){ addMsg('assistant','Bank App access not configured.'); return; }
  const safeId = wsActiveStack+'-'+ba.name.replace(/[^a-zA-Z0-9]/g,'').substring(0,10).toLowerCase();
  if(!window._wsApps[safeId]){
    const initials = ba.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
    window._wsApps[safeId]={name:ba.name,url:ba.url,desc:'',cred:null,stack:wsActiveStack,initials:initials,target:ba.target||null};
  }
  wsOpenAppDetail(safeId);
}
// Refresh Data Story every 5s while workspace tab is active
setInterval(()=>{
  if(_currentTab==='workspace') wsRefreshDataStory();
}, 5000);

async function wsLoadTerminals(stack,running,allStacks){
  let cmds=[];
  let unavailable=null;
  try{
    const cmdsR=await fetch('/api/terminal/commands/'+stack);
    const cmdsD=await cmdsR.json();
    cmds=cmdsD.commands||[];
    if(cmdsD.unavailable) unavailable=cmdsD;
  }catch(e){}
  const termBody=document.getElementById('wsTermBody');
  const termCount=document.getElementById('wsTermCount');
  if(!termBody)return;
  if(termCount)termCount.textContent=cmds.length;
  // NF-deployed stacks: terminals run docker exec on local containers, which
  // doesn't apply on NF. Surface a clean hint + a link to the NF Console
  // where the user can use the in-browser pod shell instead.
  if(unavailable && unavailable.reason==='northflank'){
    let nfh = '<div class="ws-empty-hint" style="line-height:1.5">';
    nfh += '<div style="margin-bottom:8px"><strong>DIAB terminals are not available for NF-deployed stacks.</strong></div>';
    nfh += '<div style="margin-bottom:10px;color:#64748b">'+escHtml(unavailable.hint||'')+'</div>';
    if(unavailable.console_url){
      nfh += '<a href="'+escHtml(unavailable.console_url)+'" target="_blank" style="display:inline-block;padding:6px 12px;background:#4a90d9;color:#fff;text-decoration:none;border-radius:6px;font-weight:600">Open NF Console &#8599;</a>';
    }
    nfh += '</div>';
    termBody.innerHTML = nfh;
    // Skip the pill-update count block below — fall through to early return.
    if(running&&running.length>1){
      const ct=document.getElementById('wsHomeInner');
      if(ct){
        const pills=ct.querySelectorAll('.ws-stack-btn');
        running.forEach((sName,i)=>{
          const m=(allStacks||{})[sName]||{};
          const ac=(m.access||[]).filter(a=>a.url&&!a.url.startsWith('psql'));
          const metaEl=pills[i]&&pills[i].querySelector('.wsb-meta');
          if(metaEl)metaEl.textContent=ac.length+' app'+(ac.length!==1?'s':'')+' · NF Console for shell';
        });
      }
    }
    return;
  }
  if(cmds.length){
    // Render as a dropdown to mirror the Other UIs / Credentials style.
    let th = '<div class="ws-list-dd-wrap">';
    th += '<button class="ws-list-dd-btn" onclick="wsToggleListDD(this)">';
    th += '⌨ Open Terminal <span class="ws-list-dd-count">'+cmds.length+'</span> <span class="ws-list-dd-caret">&#9662;</span>';
    th += '</button>';
    th += '<div class="ws-list-dd-panel">';
    for(const c of cmds){
      const icon = (c.type==='psql'?'PG':c.type==='clickhouse'?'CH':c.type==='redis'?'RD':'$');
      const connKey = stack+'-'+c.service;
      const isOpen = wsOpenTabs.has('term-'+connKey);
      th += '<div class="ws-list-dd-row" onclick="wsOpenTerminal(\''+stack+'\',\''+escHtml(c.service)+'\',\''+escHtml(c.toolbox_cmd)+'\',\''+escHtml(c.name)+'\',\''+escHtml(c.target_container||'')+'\')">';
      th += '<div class="ws-list-dd-icon">'+icon+'</div>';
      th += '<div class="ws-list-dd-name">'+escHtml(c.name)+'</div>';
      if(isOpen) th += '<span class="ws-list-dd-state">●</span>';
      th += '</div>';
    }
    th += '</div></div>';
    termBody.innerHTML = th;
  }else{
    termBody.innerHTML='<div class="ws-empty-hint">No terminals available for this stack</div>';
  }
  // Update pill meta with final terminal count
  if(running&&running.length>1){
    const ct=document.getElementById('wsHomeInner');
    if(!ct)return;
    const pills=ct.querySelectorAll('.ws-stack-btn');
    running.forEach((sName,i)=>{
      const m=(allStacks||{})[sName]||{};
      const ac=(m.access||[]).filter(a=>a.url&&!a.url.startsWith('psql'));
      const metaEl=pills[i]&&pills[i].querySelector('.wsb-meta');
      if(metaEl)metaEl.textContent=ac.length+' app'+(ac.length!==1?'s':'')+' \u00b7 '+cmds.length+' terminal'+(cmds.length!==1?'s':'');
    });
  }
}

function wsSelectStack(name){
  wsActiveStack=name;
  wsRenderHome();
}

const _wsSecState={};
function wsToggleSec(hdrEl){
  const sec=hdrEl.parentElement;
  sec.classList.toggle('collapsed');
  const pm=hdrEl.querySelector('.ws-sec-pm');
  if(pm)pm.textContent=sec.classList.contains('collapsed')?'[+]':'[-]';
  const key=sec.dataset.wssec;
  if(key)_wsSecState[key]=!sec.classList.contains('collapsed');
}

// ── Chat panel toggle ──
let wsChatCollapsed=false;
function wsToggleChat(){
  wsChatCollapsed=!wsChatCollapsed;
  document.getElementById('wsChat').classList.toggle('collapsed',wsChatCollapsed);
  if(!wsChatCollapsed)setTimeout(()=>document.getElementById('wsChatInput').focus(),300);
}

function wsTogglePanel(id){
  const sec=document.getElementById(id);
  if(!sec)return;
  sec.classList.toggle('open');
}

// ── Workspace chat (independent from main chat) ──
let _wsChatAbort=null;
function wsAddMsg(role,text){
  const ct=document.getElementById('wsChatMsgs');
  if(!ct)return;
  const d=document.createElement('div');
  d.className='msg '+role;
  d.innerHTML=fmt(text)+'<span class="ts">'+timeStr()+'</span>';
  ct.appendChild(d);
  ct.scrollTop=ct.scrollHeight;
  return d;
}
async function wsSendChat(){
  const input=document.getElementById('wsChatInput');
  const ct=document.getElementById('wsChatMsgs');
  const msg=input.value.trim();
  if(!msg||!ct)return;
  input.value='';
  wsAddMsg('user',msg);
  // Disable input + send button while processing
  input.disabled=true;
  const sendBtn=input.nextElementSibling;
  if(sendBtn){sendBtn.disabled=true;sendBtn.textContent='...'}
  // Enhanced typing indicator with bouncing dots + elapsed timer
  const typ=document.createElement('div');
  typ.className='ws-thinking';
  typ.innerHTML='<span class="ws-think-dots"><span></span><span></span><span></span></span>'
              +'<span class="ws-think-label">Thinking</span>'
              +'<span class="ws-think-timer" id="wsThinkTimer">0s</span>';
  ct.appendChild(typ);ct.scrollTop=ct.scrollHeight;
  const tt0=Date.now();
  const tti=setInterval(()=>{const el=document.getElementById('wsThinkTimer');if(el)el.textContent=Math.round((Date.now()-tt0)/1000)+'s'},1000);
  const bubble=document.createElement('div');
  bubble.className='msg assistant';bubble.style.display='none';
  ct.appendChild(bubble);
  let fullText='';
  _wsChatAbort=new AbortController();
  try{
    const resp=await fetch('/api/chat/stream',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg}),signal:_wsChatAbort.signal});
    const reader=resp.body.getReader();const decoder=new TextDecoder();let buf='';
    while(true){
      const{done,value}=await reader.read();if(done)break;
      buf+=decoder.decode(value,{stream:true});
      const lines=buf.split('\n');buf=lines.pop()||'';
      for(const line of lines){
        if(!line.startsWith('data: '))continue;const payload=line.slice(6);
        if(payload==='[DONE]')continue;
        try{const chunk=JSON.parse(payload);fullText+=chunk;typ.style.display='none';bubble.style.display='';bubble.innerHTML=fmt(fullText);ct.scrollTop=ct.scrollHeight}catch(e){}
      }
    }
  }catch(e){if(e.name==='AbortError'){fullText+=' *[stopped]*'}else{fullText='Connection error: '+e.message}}
  clearInterval(tti);_wsChatAbort=null;typ.remove();
  input.disabled=false;
  if(sendBtn){sendBtn.disabled=false;sendBtn.textContent='Send'}
  input.focus();
  if(!fullText){bubble.remove();wsAddMsg('assistant','No response received.')}
  else{bubble.innerHTML=fmt(fullText)+'<span class="ts">'+timeStr()+'</span>'}
  ct.scrollTop=ct.scrollHeight;
}

// ── Workspace tab system ──
function wsSwitchTab(id){
  document.querySelectorAll('#wsTabBar .ws-tab, #wsTabBar .ws-tab-home').forEach(t=>t.classList.toggle('active',t.dataset.wstab===id));
  document.querySelectorAll('#wsContent .ws-page').forEach(p=>p.classList.toggle('active',p.id==='ws-'+id));
  if(id.startsWith('term-')){
    const key=id.replace('term-','');
    const t=wsTerminals.get(key);
    if(t&&t.fitAddon)try{t.fitAddon.fit()}catch(e){}
  }
  if(id==='logs') initLogsTab(); else stopLogStream();
}

// ── Logs sub-tab — single-container live tail via WebSocket ──
let _logsWS=null, _logsPaused=false, _logsActive=null, _logsBuf=[], _logsFilter='';
async function initLogsTab(){
  const stack=(typeof wsActiveStack!=='undefined' && wsActiveStack) || (window._wsRunning && window._wsRunning[0]) || '';
  const list=document.getElementById('wsLogsList');
  if(!list) return;
  if(!stack){ list.innerHTML='<div class="ws-logs-empty">No stack running.<br>Deploy a stack from the Industry tab.</div>'; return; }
  list.innerHTML='<div class="ws-logs-empty">Loading containers...</div>';
  try{
    const r=await fetch('/api/containers/'+encodeURIComponent(stack));
    const d=await r.json();
    const cs=d.containers||[];
    if(!cs.length){ list.innerHTML='<div class="ws-logs-empty">No running containers.</div>'; return; }
    list.innerHTML='';
    cs.forEach(c=>{
      const row=document.createElement('div');
      row.className='ws-logs-row'+(_logsActive===c.name?' active':'');
      row.dataset.name=c.name;
      row.innerHTML='<span class="ws-logs-dot"></span><span class="ws-logs-name" title="'+c.status.replace(/"/g,'&quot;')+'">'+c.name+'</span>';
      row.onclick=()=>selectLogContainer(c.name);
      list.appendChild(row);
    });
    // Auto-select first if nothing active.
    if(!_logsActive) selectLogContainer(cs[0].name);
  }catch(e){
    list.innerHTML='<div class="ws-logs-empty">Failed to load: '+e+'</div>';
  }
}
function selectLogContainer(name){
  if(_logsActive===name && _logsWS && _logsWS.readyState<=1) return;
  stopLogStream();
  _logsActive=name; _logsBuf=[]; _logsPaused=false;
  document.querySelectorAll('#wsLogsList .ws-logs-row').forEach(r=>r.classList.toggle('active',r.dataset.name===name));
  const tgt=document.getElementById('wsLogsTarget'); if(tgt) tgt.textContent=name+'  (live)';
  const view=document.getElementById('wsLogsView'); if(view) view.textContent='';
  ['wsLogsPause','wsLogsClear','wsLogsSave','wsLogsFilter'].forEach(id=>{const e=document.getElementById(id); if(e) e.disabled=false;});
  const pauseBtn=document.getElementById('wsLogsPause'); if(pauseBtn) pauseBtn.textContent='Pause';
  const proto=location.protocol==='https:'?'wss:':'ws:';
  _logsWS=new WebSocket(proto+'//'+location.host+'/ws/logs');
  _logsWS.onopen=()=>{ try{ _logsWS.send(JSON.stringify({type:'start',container:name,tail:500,key:_getApiKey()})); }catch(e){} };
  _logsWS.onmessage=(ev)=>{
    let text=ev.data;
    if(typeof text!=='string') return;
    // Backend may send JSON control frames (started/error) or raw log lines.
    if(text.startsWith('{') && text.endsWith('}')){
      try{ const j=JSON.parse(text); if(j.type==='error'){ appendLogLine('[error] '+(j.message||'')); return; } if(j.type==='started') return; }catch(e){}
    }
    appendLogLine(text);
  };
  _logsWS.onerror=()=>appendLogLine('[ws] connection error');
  _logsWS.onclose=()=>appendLogLine('[ws] stream closed');
}
function appendLogLine(line){
  _logsBuf.push(line);
  if(_logsBuf.length>5000) _logsBuf=_logsBuf.slice(-5000);
  if(_logsPaused) return;
  renderLogsView();
}
function renderLogsView(){
  const view=document.getElementById('wsLogsView'); if(!view) return;
  let lines=_logsBuf;
  if(_logsFilter){
    try{ const rx=new RegExp(_logsFilter,'i'); lines=lines.filter(l=>rx.test(l)); }catch(e){}
  }
  const atBottom=(view.scrollHeight - view.scrollTop - view.clientHeight) < 40;
  view.textContent=lines.join('');
  if(atBottom) view.scrollTop=view.scrollHeight;
}
function toggleLogsPause(){
  _logsPaused=!_logsPaused;
  const b=document.getElementById('wsLogsPause'); if(b) b.textContent=_logsPaused?'Resume':'Pause';
  if(!_logsPaused) renderLogsView();
}
function clearLogsView(){ _logsBuf=[]; renderLogsView(); }
function saveLogsView(){
  const blob=new Blob([_logsBuf.join('')],{type:'text/plain'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a'); a.href=url; a.download=(_logsActive||'logs')+'.log'; a.click();
  setTimeout(()=>URL.revokeObjectURL(url),3000);
}
function applyLogsFilter(){
  const inp=document.getElementById('wsLogsFilter');
  _logsFilter=inp?inp.value.trim():''; renderLogsView();
}
function stopLogStream(){
  if(_logsWS){ try{ _logsWS.close(); }catch(e){} _logsWS=null; }
}

// ── App detail page (v9: show details first, then Launch) ──
// Nginx proxy port map — proxy strips X-Frame-Options so iframes work
const WS_PROXY_PORTS={':8889':':8890',':5001':':5010',':3002':':3010',':7861':':7870',':9004':':9011',':8125':':8130',':5697':':5700',':8181':':8190',':8888':':8898'};
function wsProxyUrl(url){
  for(const[dp,pp] of Object.entries(WS_PROXY_PORTS)){if(url.includes(dp))return url.replace(dp,pp)}
  return url;
}

// Open a pipeline-declared link (from stack.yaml `pipelines[].links[]`) in
// a workspace iframe tab. Reuses wsOpenAppDetail's full flow (nginx-proxy
// rewrite, console probe, auto-refresh for Metabase). Switches to the
// workspace tab if not already there.
async function wsOpenPipelineLink(stack, label, url, icon){
  const safe = (label||'link').replace(/[^a-zA-Z0-9]/g,'').substring(0,16).toLowerCase();
  const tabId = 'plink-'+stack+'-'+safe;
  if(wsOpenTabs.has(tabId)){
    if(typeof switchTab==='function' && _currentTab!=='workspace') switchTab('workspace');
    wsSwitchTab(tabId); return;
  }
  if(!window._wsApps) window._wsApps = {};
  // Tab title prefers icon + label; initials drive the small badge in the tab strip.
  const initials = (icon || (label||'??').replace(/[^A-Z]/g,'').substring(0,2) || (label||'??').substring(0,2).toUpperCase());
  window._wsApps[tabId] = {name: label, url, stack, desc: '', cred: [], initials};
  if(typeof switchTab==='function' && _currentTab!=='workspace') switchTab('workspace');
  await wsOpenAppDetail(tabId);
}

async function wsOpenAppDetail(tabId){
  const appData=window._wsApps[tabId];
  if(!appData)return;
  const{name,desc,cred,stack,initials}=appData;
  let url=appData.url;
  const extUrl=url;

  // target:"_blank" on the source entry (stack.yaml's access: or
  // pipelines[].links:) bypasses the workspace iframe flow entirely and
  // opens a real browser tab. The iframe bar's "External ↗" button does
  // this on every iframe — for apps where the iframe wraps no useful UX
  // (like Bank App, which has its own full-page Phase 2 console), the
  // intermediate iframe step is just clutter.
  if(appData.target === '_blank'){
    window.open(extUrl, '_blank', 'noopener');
    return;
  }

  if(wsOpenTabs.has(tabId)){wsSwitchTab(tabId);return}
  if(window._wsOpening&&window._wsOpening[tabId])return;
  if(!window._wsOpening)window._wsOpening={};
  window._wsOpening[tabId]=true;

  // Detect by URL+name so pipeline links with non-standard labels (e.g.
  // "Fraud Detection Dashboard" pointing at port 3002) still get the
  // service-specific auto-login treatment.
  const lurl = (extUrl||'').toLowerCase();
  const lname0 = name.toLowerCase();
  const isMb = lname0.includes('metabase') || /:3002\b/.test(lurl);
  const isLf = lname0.includes('langflow') || /:7861\b/.test(lurl);
  const isJp = lname0.includes('jupyter')  || /:8889\b/.test(lurl);

  // Metabase: prefer the public (no-login) dashboard URL.
  if(isMb){
    let publicOk=false;
    try{
      const r=await fetch('/api/metabase-public-url');
      const d=await r.json();
      if(d && d.url){ url=d.url; publicOk=true; }
    }catch(e){}
    if(!publicOk) url = wsProxyUrl(url);
  } else if(isLf){
    // LangFlow: trigger a backend-side auto_login fetch first so the
    // browser-cookie session is warmed; then load the proxied URL inside
    // the iframe (which now reuses that session). The /api/v1/auto_login
    // endpoint sets a cookie under origin 127.0.0.1:7861, which the iframe
    // will inherit because the iframe loads from the same origin.
    try{ await fetch('http://127.0.0.1:7861/api/v1/auto_login', {credentials:'include'}); }catch(_){}
    url = wsProxyUrl(url);
  } else if(isJp){
    // Jupyter: ensure the auth token is in the URL. The pipeline link
    // already includes ?token=databox; for safety we re-append if missing.
    if(!/[?&]token=/.test(url)){
      url = url + (url.includes('?') ? '&' : '?') + 'token=databox';
    }
    url = wsProxyUrl(url);
  } else {
    // Generic: rewrite URL to nginx proxy port for iframe compatibility
    url = wsProxyUrl(url);
  }

  // Bank App: append #phase2 hash so the iframe always lands in Phase 2
  // (skips the Phase 1 form even if checkAutoInit is still polling for data).
  // Matches the experience users get when opening Bank App from a fresh tab.
  const lname = name.toLowerCase();
  if(lname.includes('bank app') && !url.includes('#')){
    url = url + '#phase2';
  }

  // Probe before loading — if down/unconfigured, render a friendly overlay
  // instead of letting the browser show "127.0.0.1 refused to connect".
  const probeKind = isMb ? 'metabase' : (lname.includes('bank app') ? 'bankapp' : 'auto');
  let probeState = {state:'ok'};
  try{
    const pr = await fetch('/api/console-probe?url='+encodeURIComponent(extUrl)+'&kind='+probeKind);
    probeState = await pr.json();
  }catch(_){}

  wsOpenTabs.set(tabId,{name,url,stack});
  const color=wsStackColors[stack]||'#4a90d9';

  const tab=document.createElement('div');tab.className='ws-tab';tab.dataset.wstab=tabId;
  tab.style.cssText='--sc:'+color;
  tab.onclick=()=>wsSwitchTab(tabId);
  tab.innerHTML='<span class="ws-t-icon app-icon">'+initials+'</span><span class="ws-t-label">'+name+'</span><span class="ws-close" onclick="event.stopPropagation();wsCloseTab(\''+tabId+'\')">&times;</span>';
  document.getElementById('wsTabBar').appendChild(tab);

  const page=document.createElement('div');page.className='ws-page';page.id='ws-'+tabId;
  if(probeState.state && probeState.state !== 'ok'){
    page.innerHTML = wsRenderConsoleOverlay(tabId, name, extUrl, url, probeState);
  }else{
    const autoHint = isMb ? '<span class="ws-iframe-refresh-hint" id="ws-refresh-cd-'+tabId+'" title="Auto-refreshes every 60 seconds">&#x21bb; Auto-refresh in 60s</span>' : '';
    page.innerHTML='<div class="ws-iframe-bar"><span class="ws-ib-url">'+escHtml(url)+'</span>'+autoHint+'<button onclick="wsReloadIframe(\''+tabId+'\')">&#x21bb; Reload</button><button onclick="window.open(\''+escHtml(extUrl)+'\',\'_blank\')">External &#8599;</button></div><div class="ws-iframe-wrap"><div class="ws-loading" id="ws-loading-'+tabId+'"><div class="ws-spinner"></div><div class="ws-load-text">Loading '+escHtml(name)+'...</div></div><iframe id="ws-iframe-'+tabId+'" src="'+escHtml(url)+'" allow="clipboard-write" onload="wsHideLoading(\''+tabId+'\')"></iframe></div>';
    // Client-side auto-refresh for Metabase (60s countdown). Reliable because
    // we control the iframe — Metabase's own ?refresh= param doesn't always
    // apply to public dashboards.
    if(isMb){ wsStartIframeAutoRefresh(tabId, 60); }
  }
  document.getElementById('wsContent').appendChild(page);
  delete window._wsOpening[tabId];
  wsRenderHome();
  wsSwitchTab(tabId);
}

function wsRenderConsoleOverlay(tabId, name, extUrl, proxyUrl, state){
  const stateStyles = {
    'starting':       {color:'#b45309', bg:'#fef3c7', border:'#fcd34d', icon:'⏳', heading:'Service starting'},
    'down':           {color:'#991b1b', bg:'#fee2e2', border:'#fca5a5', icon:'⚠',  heading:'Service not reachable'},
    'unconfigured':   {color:'#1e40af', bg:'#dbeafe', border:'#93c5fd', icon:'ℹ',  heading:'Setup needed'},
    'iframe-blocked': {color:'#374151', bg:'#f3f4f6', border:'#9ca3af', icon:'🔗', heading:'Service blocks iframe embedding'}
  };
  const s = stateStyles[state.state] || stateStyles.down;
  const hint = (state.hint || '').replace(/</g, '&lt;');
  const action = (state.action || '').replace(/</g, '&lt;');
  // For iframe-blocked services we drop the auto-refresh footer (refreshing
  // won't help — the iframe will keep being blocked) and emphasise the
  // "Open in new tab" CTA in the card itself.
  const isBlocked = state.state === 'iframe-blocked';
  // Metabase gets a smarter "Open in new tab" handler that lands the user
  // straight on the BFSI public dashboard (no login prompt). Falls back to
  // raw URL if the public dashboard isn't available yet.
  const lname = (name||'').toLowerCase();
  const lurl  = (extUrl||'').toLowerCase();
  const isMb  = lname.includes('metabase') || /:3002\b/.test(lurl);
  const openFn = isMb
    ? `wsOpenMetabaseNewTab('${escHtml(extUrl)}')`
    : `window.open('${escHtml(extUrl)}','_blank')`;
  const headerBtnLabel = isMb ? 'Open dashboard in new tab' : 'Open in new tab';
  const bigBtnLabel    = isMb ? `Open ${escHtml(name)} dashboard in new tab` : `Open ${escHtml(name)} in new tab`;
  return `
    <div class="ws-iframe-bar">
      <span class="ws-ib-url">${escHtml(proxyUrl)}</span>
      ${isBlocked ? '' : `<button onclick="wsRetryConsole('${tabId}')">Retry</button>`}
      <button onclick="${openFn}">${headerBtnLabel} &#8599;</button>
    </div>
    <div class="ws-console-overlay" data-tab="${tabId}" data-ext="${escHtml(extUrl)}" data-proxy="${escHtml(proxyUrl)}" data-name="${escHtml(name)}" data-state="${state.state}">
      <div class="ws-console-card" style="border-left:4px solid ${s.border};background:${s.bg};color:${s.color}">
        <div class="ws-console-icon" style="font-size:32px">${s.icon}</div>
        <div class="ws-console-text">
          <div class="ws-console-heading">${s.heading}: ${escHtml(name)}</div>
          ${hint ? `<div class="ws-console-hint">${hint}</div>` : ''}
          ${action ? `<div class="ws-console-action">→ ${action}</div>` : ''}
          ${isBlocked
            ? `<div style="margin-top:14px"><button onclick="${openFn}" style="padding:8px 16px;background:#4a90d9;color:#fff;border:none;border-radius:6px;font-weight:600;cursor:pointer">${bigBtnLabel} ↗</button>${isMb ? '<div style="margin-top:8px;font-size:12px;color:#475569">Opens the BFSI public dashboard — no login prompt.</div>' : ''}</div>`
            : '<div class="ws-console-foot">Auto-refreshing every 5s while this view is open.</div>'}
        </div>
      </div>
    </div>`;
}

// Open Metabase in a new tab, preferring the public dashboard URL (no login)
// when DIAB has already minted one for this stack. Falls back to the raw
// Metabase URL when no public link exists.
async function wsOpenMetabaseNewTab(extUrl){
  // Open the tab first to keep the user-gesture intact (popup blockers).
  const w = window.open('about:blank', '_blank');
  try{
    const r = await fetch('/api/metabase-public-url');
    const d = await r.json();
    const target = (d && d.url) ? d.url : extUrl;
    if(w){ w.location.href = target; } else { window.open(target, '_blank'); }
  }catch(e){
    if(w){ w.location.href = extUrl; } else { window.open(extUrl, '_blank'); }
  }
}

async function wsRetryConsole(tabId){
  const page = document.getElementById('ws-'+tabId);
  if(!page) return;
  const ov = page.querySelector('.ws-console-overlay');
  if(!ov) return;
  const ext = ov.dataset.ext, proxy = ov.dataset.proxy, name = ov.dataset.name;
  const lname = (name||'').toLowerCase();
  const kind = lname.includes('metabase') ? 'metabase' : (lname.includes('bank app') ? 'bankapp' : 'auto');
  try{
    const pr = await fetch('/api/console-probe?url='+encodeURIComponent(ext)+'&kind='+kind);
    const st = await pr.json();
    if(st.state === 'ok'){
      // Service is ready — replace overlay with iframe
      page.innerHTML = '<div class="ws-iframe-bar"><span class="ws-ib-url">'+escHtml(proxy)+'</span><button onclick="wsReloadIframe(\''+tabId+'\')">Reload</button><button onclick="window.open(\''+escHtml(ext)+'\',\'_blank\')">External &#8599;</button></div><div class="ws-iframe-wrap"><div class="ws-loading" id="ws-loading-'+tabId+'"><div class="ws-spinner"></div><div class="ws-load-text">Loading '+escHtml(name)+'...</div></div><iframe id="ws-iframe-'+tabId+'" src="'+escHtml(proxy)+'" allow="clipboard-write" onload="wsHideLoading(\''+tabId+'\')"></iframe></div>';
    }else{
      // Still not OK — refresh the overlay (state may have changed: starting → unconfigured, etc.)
      page.innerHTML = wsRenderConsoleOverlay(tabId, name, ext, proxy, st);
    }
  }catch(_){}
}

// Auto-refresh any visible "down/starting/unconfigured" overlays every 5s.
// Skip overlays whose state is permanent (iframe-blocked) — the response
// headers won't change between polls.
setInterval(()=>{
  document.querySelectorAll('.ws-console-overlay').forEach(ov=>{
    if(ov.dataset.state === 'iframe-blocked') return;
    const tabId = ov.dataset.tab;
    const page = document.getElementById('ws-'+tabId);
    // Only refresh if this page is currently visible (active)
    if(page && page.classList.contains('active')) wsRetryConsole(tabId);
  });
}, 5000);

function wsLaunchApp(tabId,name,url){
  const page=document.getElementById('ws-'+tabId);
  if(!page)return;
  const iframeUrl=wsProxyUrl(url);
  page.innerHTML='<div class="ws-iframe-bar"><span class="ws-ib-url">'+iframeUrl+'</span><button onclick="wsReloadIframe(\''+tabId+'\')">Reload</button><button onclick="window.open(\''+url+'\',\'_blank\')">External &#8599;</button></div><div class="ws-iframe-wrap"><div class="ws-loading" id="ws-loading-'+tabId+'"><div class="ws-spinner"></div><div class="ws-load-text">Loading '+name+'...</div></div><iframe id="ws-iframe-'+tabId+'" src="'+iframeUrl+'" allow="clipboard-write" onload="wsHideLoading(\''+tabId+'\')"></iframe></div>';
}

function wsCloseTab(id){
  wsOpenTabs.delete(id);
  // Stop any auto-refresh timer for this tab
  if(typeof wsStopIframeAutoRefresh==='function') wsStopIframeAutoRefresh(id);
  const tab=document.querySelector('#wsTabBar .ws-tab[data-wstab="'+id+'"]');
  const page=document.getElementById('ws-'+id);
  if(page){const iframe=page.querySelector('iframe');if(iframe)iframe.src='about:blank'}
  if(id.startsWith('term-')){
    const key=id.replace('term-','');
    const t=wsTerminals.get(key);
    if(t){
      if(t.ws&&t.ws.readyState===WebSocket.OPEN)t.ws.close();
      if(t.term)t.term.dispose();
      wsTerminals.delete(key);
    }
  }
  if(tab)tab.remove();
  if(page)page.remove();
  wsSwitchTab('home');
  wsRenderHome(); // refresh badges
}

function wsReloadIframe(id){const iframe=document.getElementById('ws-iframe-'+id);if(iframe){const l=document.getElementById('ws-loading-'+id);if(l){l.classList.remove('hidden');l.style.display=''}iframe.src=iframe.src}}

// ── Iframe auto-refresh with visible countdown ───────────────────────────
// Stores active timers per tab so we can cancel on tab close.
window._wsRefreshTimers = window._wsRefreshTimers || {};
function wsStartIframeAutoRefresh(tabId, intervalSec){
  // Cancel any existing timer for this tab
  if(window._wsRefreshTimers[tabId]){
    clearInterval(window._wsRefreshTimers[tabId]);
    delete window._wsRefreshTimers[tabId];
  }
  let remaining = intervalSec;
  const cd = document.getElementById('ws-refresh-cd-'+tabId);
  if(cd) cd.innerHTML = '&#x21bb; Auto-refresh in '+remaining+'s';
  window._wsRefreshTimers[tabId] = setInterval(()=>{
    // Stop if the tab no longer exists
    const iframe = document.getElementById('ws-iframe-'+tabId);
    if(!iframe){
      clearInterval(window._wsRefreshTimers[tabId]);
      delete window._wsRefreshTimers[tabId];
      return;
    }
    remaining--;
    const cdEl = document.getElementById('ws-refresh-cd-'+tabId);
    if(remaining <= 0){
      // Reload the iframe; reset countdown
      if(cdEl) cdEl.innerHTML = '&#x21bb; Refreshing…';
      try{ iframe.src = iframe.src; }catch(_){}
      remaining = intervalSec;
      // Show "in 60s" again after the reload kick
      setTimeout(()=>{
        const c = document.getElementById('ws-refresh-cd-'+tabId);
        if(c) c.innerHTML = '&#x21bb; Auto-refresh in '+intervalSec+'s';
      }, 800);
    } else {
      if(cdEl) cdEl.innerHTML = '&#x21bb; Auto-refresh in '+remaining+'s';
    }
  }, 1000);
}
function wsStopIframeAutoRefresh(tabId){
  if(window._wsRefreshTimers && window._wsRefreshTimers[tabId]){
    clearInterval(window._wsRefreshTimers[tabId]);
    delete window._wsRefreshTimers[tabId];
  }
}
function wsHideLoading(id){const el=document.getElementById('ws-loading-'+id);if(el){el.classList.add('hidden');setTimeout(()=>{el.style.display='none'},300)}}

// ── Terminal: auto-connect on single click ──
function wsOpenTerminal(stackName,serviceId,command,displayName,targetContainer){
  const key=stackName+'-'+serviceId;
  const tabId='term-'+key;

  // If tab already exists, just switch to it
  if(wsOpenTabs.has(tabId)){wsSwitchTab(tabId);return}

  wsOpenTabs.set(tabId,{name:displayName,type:'terminal',stack:stackName});
  const color=wsStackColors[stackName]||'#4a90d9';

  // Create tab with stack color
  const tab=document.createElement('div');tab.className='ws-tab term-tab';tab.dataset.wstab=tabId;
  tab.style.cssText='--sc:'+color;
  tab.onclick=()=>wsSwitchTab(tabId);
  tab.innerHTML='<span class="ws-t-icon term-icon">$</span><span class="ws-t-label">'+serviceId+'</span><span class="ws-close" onclick="event.stopPropagation();wsCloseTab(\''+tabId+'\')">&times;</span>';
  document.getElementById('wsTabBar').appendChild(tab);

  // Create terminal page with "Connecting..." banner
  const page=document.createElement('div');page.className='ws-page ws-term-page';page.id='ws-'+tabId;
  page.innerHTML='<div class="ws-term-toolbar"><div class="tt-badge"><span class="tt-dot" id="ws-ttd-'+key+'"></span><span class="tt-name">'+serviceId+'</span></div><span class="tt-conn" id="ws-tt-cmd-'+key+'">Connecting...</span><button onclick="wsCloseTab(\''+tabId+'\')">Clear</button><button class="tt-disc" onclick="wsCloseTab(\''+tabId+'\')">Disconnect</button></div><div class="ws-xterm" id="ws-xterm-'+key+'"></div>';
  document.getElementById('wsContent').appendChild(page);
  wsRenderHome(); // refresh badges
  wsSwitchTab(tabId);

  // Auto-connect immediately
  wsDoConnect(key,stackName,serviceId,command,displayName,targetContainer);
}

function wsDoConnect(key,stackName,serviceId,command,displayName,targetContainer){
  const termEl=document.getElementById('ws-xterm-'+key);
  if(!termEl||typeof Terminal==='undefined'){
    if(termEl)termEl.innerHTML='<div style="color:#f85149;padding:12px;font-size:12px">xterm.js not loaded. Check network.</div>';
    return;
  }

  termEl.innerHTML='';

  const term=new Terminal({
    cursorBlink:true,
    fontSize:13,
    fontFamily:"'SF Mono',Monaco,'Cascadia Code',Consolas,monospace",
    theme:{background:'#0d1117',foreground:'#c9d1d9',cursor:'#3fb950',selectionBackground:'#1f6feb44',black:'#0d1117',red:'#f85149',green:'#3fb950',yellow:'#d29922',blue:'#58a6ff',magenta:'#bc8cff',cyan:'#39d353',white:'#c9d1d9',brightBlack:'#484f58',brightRed:'#f85149',brightGreen:'#3fb950',brightYellow:'#d29922',brightBlue:'#58a6ff',brightMagenta:'#bc8cff',brightCyan:'#39d353',brightWhite:'#f0f6fc'}
  });

  let fitAddon=null;
  if(typeof FitAddon!=='undefined'){
    fitAddon=new FitAddon.FitAddon();
    term.loadAddon(fitAddon);
  }

  term.open(termEl);
  if(fitAddon)try{fitAddon.fit()}catch(e){}

  term.writeln('\x1b[38;2;88;166;255mConnecting to '+displayName+(targetContainer ? ' (container: '+targetContainer+')' : ' via toolbox')+'...\x1b[0m');

  const tabId='term-'+key;
  const toolbarDot=document.getElementById('ws-ttd-'+key);
  const cmdEl=document.getElementById('ws-tt-cmd-'+key);
  if(cmdEl)cmdEl.textContent='Connecting...';

  // WebSocket connection
  const proto=location.protocol==='https:'?'wss:':'ws:';
  const ws=new WebSocket(proto+'//'+location.host+'/ws/terminal');

  ws.onopen=()=>{
    const cols=term.cols||120;
    const rows=term.rows||40;
    ws.send(JSON.stringify({type:'start',stack:stackName,command:command,cols:cols,rows:rows,target_container:targetContainer||'',key:_getApiKey()}));
  };

  ws.onmessage=(e)=>{
    const d=e.data;
    // Fast path: raw text output prefixed with \x01 — no JSON parse needed
    if(typeof d==='string'&&d.charCodeAt(0)===1){
      term.write(d.substring(1));
      return;
    }
    try{
      const msg=JSON.parse(d);
      if(msg.type==='started'){
        if(cmdEl)cmdEl.textContent=command.replace(/PGPASSWORD=\S+\s/,'').substring(0,60);
        if(toolbarDot){toolbarDot.classList.add('on')}
      }else if(msg.type==='exited'){
        term.writeln('\r\n\x1b[38;2;248;81;73m[Disconnected - exit code '+msg.code+']\x1b[0m');
        if(toolbarDot){toolbarDot.classList.remove('on');toolbarDot.style.background='#f85149'}
      }else if(msg.type==='error'){
        term.writeln('\r\n\x1b[38;2;248;81;73m[Error: '+msg.message+']\x1b[0m');
        if(toolbarDot){toolbarDot.classList.remove('on');toolbarDot.style.background='#f85149'}
      }
    }catch(err){}
  };

  ws.onclose=()=>{
    term.writeln('\r\n\x1b[38;2;139;148;158m[Connection closed]\x1b[0m');
    if(toolbarDot){toolbarDot.classList.remove('on');toolbarDot.style.background='#484f58'}
  };

  term.onData(data=>{
    if(ws.readyState===WebSocket.OPEN){
      ws.send(JSON.stringify({type:'input',data:data}));
    }
  });

  term.onResize(({cols,rows})=>{
    if(ws.readyState===WebSocket.OPEN){
      ws.send(JSON.stringify({type:'resize',cols:cols,rows:rows}));
    }
  });

  const resizeHandler=()=>{if(fitAddon)try{fitAddon.fit()}catch(e){}};
  window.addEventListener('resize',resizeHandler);

  wsTerminals.set(key,{term,ws,fitAddon,resizeHandler});
}

// ── Chat resize ──
(function(){
  const handle=document.getElementById('wsChatResize');
  const panel=document.getElementById('wsChat');
  if(!handle||!panel)return;
  let dragging=false;
  handle.addEventListener('mousedown',e=>{dragging=true;handle.classList.add('dragging');e.preventDefault()});
  document.addEventListener('mousemove',e=>{
    if(!dragging)return;
    const r=panel.parentElement.getBoundingClientRect();
    let w=r.right-e.clientX;
    w=Math.max(260,Math.min(560,w));
    panel.style.width=w+'px';panel.style.minWidth=w+'px';panel.style.maxWidth=w+'px';
    if(wsChatCollapsed){wsChatCollapsed=false;panel.classList.remove('collapsed')}
  });
  document.addEventListener('mouseup',()=>{dragging=false;handle.classList.remove('dragging')});
})();

// Ctrl+` to toggle workspace chat
document.addEventListener('keydown',e=>{if(e.ctrlKey&&e.key==='`'){e.preventDefault();wsToggleChat()}});
</script>
</body>
</html>"""


# ─── Standalone pages (kept for backward compat) ─────────────────────

PIPELINES_HTML = r"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Pipelines</title></head><body><script>window.location='/'</script></body></html>"""

MONITORING_HTML = r"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Monitoring</title></head><body><script>window.location='/'</script></body></html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 4000))
    print(f"[app] Starting EDB Postgres® AI Blueprints v{pgai_version} on http://127.0.0.1:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port)
