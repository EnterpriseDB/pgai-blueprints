"""Northflank translator.

For demonstration purposes only.

Translates docker-compose stacks into NF API calls. Owns:
  - the compose → NF payload translation (`_parse_compose_for_nf`)
  - service lifecycle (deploy, stop, resume, destroy)
  - env-vars-via-/secrets workaround for NF's silent-drop of runtimeEnvironment
  - postgres-family plan + shm_size floors
  - hostname rewriting for K8s service DNS
  - startup sync of orphaned services

The class takes the parent LabAgent as `agent` so it can access shared state:
`agent.stacks`, `agent.project_root`, etc. State that's NF-specific
(deployments map, plan catalog cache) lives on the translator. The agent
exposes those via @property for backward-compat with app.py.
"""

import os
import json
import re
import logging
import urllib.request
import urllib.error
from pathlib import Path

import yaml

logger = logging.getLogger("dbox")

# ─── Configuration (read once from process env) ─────────────────────────────
# All values come from .env, no hardcoded defaults. See .env.example for keys
# and where to find them in NF Console.
NF_API_BASE = "https://api.northflank.com/v1"
NF_CONSOLE_BASE = "https://app.northflank.com"
NF_TEAM = os.environ.get("NORTHFLANK_TEAM", "").strip()
NF_PROJECT = os.environ.get("NORTHFLANK_PROJECT", "").strip()
NF_BILLING_PLAN = os.environ.get("NORTHFLANK_BILLING_PLAN", "").strip()
# Team-level registry integration ID for private image pulls (e.g. GHCR).
# Set in NF Console → team → Integrations → Registries → name field.
# Passed as `deployment.external.credentials` (string) on service create.
NF_REGISTRY_CREDENTIALS = os.environ.get("NORTHFLANK_REGISTRY_CREDENTIALS", "").strip()
GHCR_PREFIX = os.environ.get("GHCR_PREFIX", "").strip().rstrip("/")


def _ghcr_image(name):
    """Return a GHCR image path under GHCR_PREFIX, or None if prefix unset."""
    if not GHCR_PREFIX:
        return None
    return f"{GHCR_PREFIX}/{name}"


# Mapping: "stack_name/service_name" -> image path
# For services that use build: in docker-compose, NF pulls these instead.
# Uses GHCR_PREFIX from .env. Tags pinned (no :latest) for demo accounting:
# the tag a stack deploys today is the same one it deploys tomorrow, even if
# someone pushes new builds. Update tags here when you push a new image.
GHCR_IMAGES = {
    "_template/sample-app": _ghcr_image("sample-app:2026-05-07"),
    "sovereign-data-tier/postgres": "postgres:16",

    # ── BFSI custom images ────────────────────────────────────────────
    # ⚠ Images below must be built locally and pushed to GHCR_PREFIX
    # before NF deploy will succeed. See k8s-work/eks/CORP-RUNBOOK.md
    # for the build/push playbook. 7 unique images, 13 service mappings.
    # Until pushed, NF deploy of bfsi-fraud-detection will fail at image
    # pull with a clear "image not found" error.
    "bfsi-fraud-detection/pgd": _ghcr_image("pgd:bfsi-2026-05-07"),
    "bfsi-fraud-detection/app": _ghcr_image("bank-app:bfsi-2026-05-07"),
    "bfsi-fraud-detection/mlflow": _ghcr_image("mlflow-genai:bfsi-2026-05-07"),
    "bfsi-fraud-detection/mlflow-model-server": _ghcr_image("mlflow-genai:bfsi-2026-05-07"),
    "bfsi-fraud-detection/evaluation-runner": _ghcr_image("evaluation-runner:bfsi-2026-05-07"),
    "bfsi-fraud-detection/lakekeeper-init": _ghcr_image("init-tools:bfsi-2026-05-07"),
    "bfsi-fraud-detection/pg-airman-mcp": _ghcr_image("pg-airman-mcp:bfsi-2026-05-07"),
    # ml-inference image is reused across 7 services with different commands.
    "bfsi-fraud-detection/kafka-feature-materializer": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/ml-inference-kafka": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/ml-inference-risingwave": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/ml-inference-clickhouse": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/ml-inference-pgaa": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/fraud-alert": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/langflow-init": _ghcr_image("ml-inference:bfsi-2026-05-07"),
    "bfsi-fraud-detection/metabase-setup": _ghcr_image("ml-inference:bfsi-2026-05-07"),

    # ── BFSI public images, pinned to known stable versions ──────────
    # Pulled directly from upstream (Docker Hub / Quay.io) — NOT mirrored
    # to GHCR. Pinning here avoids :latest tag drift between deploys.
    # Bump version + commit when you want NF to pick up an upstream update.
    "bfsi-fraud-detection/nginx-proxy": "nginx:1.27-alpine",
    "bfsi-fraud-detection/kafka": "apache/kafka:3.8.0",
    "bfsi-fraud-detection/clickhouse": "clickhouse/clickhouse-server:24.10",
    "bfsi-fraud-detection/jupyter": "jupyter/datascience-notebook:python-3.11",
    "bfsi-fraud-detection/langflow": "langflowai/langflow:1.9.2",
    "bfsi-fraud-detection/metabase": "metabase/metabase:v0.51.7",
    "bfsi-fraud-detection/minio": "minio/minio:RELEASE.2024-12-13T22-19-12Z",
    "bfsi-fraud-detection/minio-init": "minio/mc:RELEASE.2025-04-16T18-13-26Z",
    "bfsi-fraud-detection/minio-fraud-init": "minio/mc:RELEASE.2025-04-16T18-13-26Z",
    "bfsi-fraud-detection/lakekeeper": "quay.io/lakekeeper/catalog:v0.10.3",
    "bfsi-fraud-detection/lakekeeper-migrate": "quay.io/lakekeeper/catalog:v0.10.3",
    "bfsi-fraud-detection/mlflow-experiment-init": "curlimages/curl:8.10.1",
    # postgres:17 used by 3 init services (mlflow-db-init, langflow-db-init, aidb-init)
    "bfsi-fraud-detection/mlflow-db-init": "postgres:17",
    "bfsi-fraud-detection/langflow-db-init": "postgres:17",
    "bfsi-fraud-detection/aidb-init": "postgres:17",
    "bfsi-fraud-detection/oltp-seed": "postgres:17",
    # risingwave + kafka-connect are already pinned in compose; mirror here for clarity.
    "bfsi-fraud-detection/risingwave": "risingwavelabs/risingwave:v2.2.2",
    "bfsi-fraud-detection/kafka-connect": "debezium/connect:2.4",
}


class NorthflankTranslator:
    """Deploy/manage stacks on Northflank. Owns the NF-side state + API surface.

    Backward-compat note: the parent LabAgent exposes `nf_deployments` and
    `nf_plans` as @property forwards to this class's `deployments` / `plans`.
    App.py reads `agent.nf_deployments[...]` directly, so those names must
    keep working.
    """

    def __init__(self, agent):
        self.agent = agent
        self.deployments = {}  # stack_name -> {"services": [...], "deployed_at": ...}
        self.plans = []        # list of {id, cpu, ram_mb} sorted by ram asc — loaded lazily
        self.cancel_requests = set()  # stack_names a user has asked to cancel mid-deploy

    def request_cancel(self, stack_name):
        """Mark a stack for mid-deploy cancellation. The deploy() loop checks
        this flag before each service create and exits early if set. After the
        loop exits early, deploy() also destroys any services it had already
        created, so the cancel leaves no partial state behind."""
        self.cancel_requests.add(stack_name)
        logger.info("[NF] Cancel requested for %s", stack_name)

    # ─── Low-level API transport ────────────────────────────────────────────

    def api(self, method, endpoint, data=None):
        """Make a Northflank API call. Returns parsed JSON or error dict."""
        api_key = os.environ.get("NORTHFLANK_API_KEY", "").strip()
        if not api_key:
            return {"error": {"message": "NORTHFLANK_API_KEY not set in .env"}}
        # Endpoints that reference {team} or {project} need those vars set.
        if "/projects/" in endpoint and not NF_PROJECT:
            return {"error": {"message": "NORTHFLANK_PROJECT not set in .env"}}
        if "/teams/" in endpoint and not NF_TEAM:
            return {"error": {"message": "NORTHFLANK_TEAM not set in .env"}}

        url = f"{NF_API_BASE}{endpoint}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            try:
                return json.loads(error_body)
            except json.JSONDecodeError:
                return {"error": {"status": e.code, "message": error_body or str(e)}}
        except Exception as e:
            return {"error": {"message": str(e)}}

    # ─── Plan catalog + service plan picking ────────────────────────────────

    def load_plans(self):
        """Fetch the NF deployment-plan catalog once and cache. Called lazily on
        first use so agent startup doesn't fail if NF is unreachable."""
        if self.plans:
            return self.plans
        resp = self.api("GET", "/plans")
        if "error" in resp:
            logger.warning("[NF] Could not fetch plan catalog: %s", resp["error"])
            return []
        out = []
        for p in resp.get("data", {}).get("plans", []):
            if "deployment" not in p.get("type", []):
                continue
            out.append({
                "id": p["id"],
                "cpu": p.get("cpuResource", 0),
                "ram_mb": p.get("ramResource", 0),
            })
        out.sort(key=lambda p: (p["ram_mb"], p["cpu"]))
        self.plans = out
        logger.info("[NF] Loaded %d deployment plans", len(out))
        return out

    @staticmethod
    def parse_compose_memory(val):
        """Parse compose memory string like '2g', '512m', '256M', '1024' (bytes implicit M)
        to megabytes. Returns 0 if unparseable."""
        if val is None:
            return 0
        s = str(val).strip().lower()
        if not s:
            return 0
        try:
            if s.endswith("g") or s.endswith("gb"):
                return int(float(s.rstrip("gb")) * 1024)
            if s.endswith("m") or s.endswith("mb"):
                return int(float(s.rstrip("mb")))
            if s.endswith("k") or s.endswith("kb"):
                return max(1, int(float(s.rstrip("kb")) / 1024))
            # bare number — bytes
            return int(float(s) / (1024 * 1024))
        except ValueError:
            return 0

    def pick_plan_for_service(self, mem_str, default_plan):
        """Pick the smallest NF deployment plan whose RAM ≥ compose mem_limit.
        If compose has no mem_limit, fall back to default_plan. Plan catalog
        failures also fall back to default_plan."""
        plans = self.load_plans()
        if not plans:
            return default_plan
        compose_mb = self.parse_compose_memory(mem_str)
        if compose_mb <= 0:
            return default_plan
        for p in plans:
            if p["ram_mb"] >= compose_mb:
                return p["id"]
        return plans[-1]["id"]

    # ─── Env-vars-via-/secrets workaround ───────────────────────────────────

    def attach_env_secret(self, resource_id, env_vars, resource_type="service", files=None):
        """Create a NF secret of type environment-arguments restricted to this
        resource (service or job). NF silently drops `runtimeEnvironment` on
        the create request; the documented primitive for runtime env vars is
        a separate /secrets resource with restrictions.nfObjects pointing at
        the resource. The same secret can carry both env vars (`variables`)
        and mounted files (`files`) — used to inject bind-mounted compose
        scripts (e.g. setup_metabase.py) into init-job pods without rebuilding
        their images. `resource_type` ∈ {"service", "job"}.
        `files` is dict {container_path: utf-8 string content} OR None."""
        if not env_vars and not files:
            return
        import base64
        secret_name = f"{resource_id}-env"[:63]
        secrets_block = {}
        if env_vars:
            secrets_block["variables"] = {str(k): str(v) for k, v in env_vars.items()}
        if files:
            secrets_block["files"] = {}
            for path, content in files.items():
                if isinstance(content, str):
                    raw = content.encode("utf-8")
                else:
                    raw = bytes(content)
                secrets_block["files"][path] = {
                    "data": base64.b64encode(raw).decode("ascii"),
                    "encoding": "utf-8",
                }
        payload = {
            "type": "secret",
            "secretType": "environment-arguments",
            "priority": 10,
            "name": secret_name,
            "secrets": secrets_block,
            "restrictions": {
                "restricted": True,
                "nfObjects": [{"id": resource_id, "type": resource_type}],
            },
        }
        resp = self.api("POST", f"/projects/{NF_PROJECT}/secrets", payload)
        if "error" in resp:
            err = resp["error"]
            logger.error("[NF] env-secret attach failed for %s %s: %s %s",
                         resource_type, resource_id,
                         err.get("message"), err.get("details") or "")
        else:
            logger.info("[NF] Attached %d env vars + %d files to %s %s via secret %s",
                        len(env_vars or {}), len(files or {}),
                        resource_type, resource_id, secret_name)

    # ─── Init-job deploy ────────────────────────────────────────────────────

    def _deploy_init_jobs(self, stack_name, init_jobs, results):
        """Create + attach-env + trigger each init job. Returns the list of
        successfully created jobs (with id, name, image) for tracking.

        Each compose init container (entrypoint + no ports) becomes an NF
        Manual Job: create the job spec, attach env vars via a secret with
        type=job, then POST a `/runs` trigger so it actually executes once.
        Init scripts typically retry-loop on connect failures, so we don't
        sequence them — they run in parallel and self-stall until their
        target service comes up."""
        deployed = []
        for job in init_jobs:
            ext = {"imagePath": job["image"]}
            if NF_REGISTRY_CREDENTIALS and GHCR_PREFIX and job["image"].startswith(GHCR_PREFIX):
                ext["credentials"] = NF_REGISTRY_CREDENTIALS
            # Init jobs are short-lived; use the smallest plan that fits the
            # compose mem_limit. Compose mem_limit on inits is usually small
            # (128-256 MB) since they just run psql / mc / curl.
            plan_id = self.pick_plan_for_service(job.get("compose_mem_limit"), NF_BILLING_PLAN)
            # Init jobs may have either an entrypoint (script we built by
            # combining compose entrypoint+command) or just a cmd (compose
            # `command:` only, no entrypoint). Pick the right NF docker
            # config so the image's default ENTRYPOINT is used in the latter
            # case (e.g. python images already have `python` as entrypoint,
            # so customCommand="/app/setup.py" runs `python /app/setup.py`).
            docker_cfg = {}
            if job.get("entrypoint"):
                docker_cfg = {
                    "configType": "customEntrypoint",
                    "customEntrypoint": job["entrypoint"],
                }
            elif job.get("cmd"):
                docker_cfg = {
                    "configType": "customCommand",
                    "customCommand": job["cmd"],
                }
            payload = {
                "name": job["name"],
                "billing": {"deploymentPlan": plan_id},
                "deployment": {
                    "external": ext,
                    **({"docker": docker_cfg} if docker_cfg else {}),
                },
                # backoffLimit=3 so transient connect-refused retries up to 3
                # times before NF gives up. Most init scripts self-retry too,
                # so this is a backstop for whole-container crashes (e.g. OOM
                # on first attempt before the user-script even starts).
                "backoffLimit": 3,
            }
            resp = self.api("POST", f"/projects/{NF_PROJECT}/jobs/manual", payload)
            if "error" in resp:
                err = resp["error"]
                err_msg = err.get("message", "Unknown error")
                err_details = err.get("details", err.get("validationErrors", ""))
                if err_details:
                    err_msg += f" | Details: {json.dumps(err_details) if isinstance(err_details, (dict, list)) else err_details}"
                results.append(f"  {job['original_name']} (init): FAILED — {err_msg}")
                logger.error("[NF] Failed to create init job %s: %s", job["name"], err_msg)
                continue
            job_data = resp.get("data", {})
            job_id = job_data.get("id", job["name"])
            # Attach env vars BEFORE triggering the run so the run sees them.
            if job["env"]:
                self.attach_env_secret(job_id, job["env"], resource_type="job",
                                       files=job.get("files"))
            # Foundation init jobs (no compose profile) are part of the app's
            # bootstrap — DBs, schemas, etc. that services need to start. We
            # auto-trigger these. Use-case init jobs (profile=oltp/setup/ml/
            # olap) are user-triggered via the pipeline runner when the user
            # clicks "Run X use case" in the workspace, so we create the job
            # spec but DON'T trigger a run here.
            profiles = job.get("profiles") or []
            is_foundation = len(profiles) == 0
            auto_triggered = False
            if is_foundation:
                run_resp = self.api("POST", f"/projects/{NF_PROJECT}/jobs/{job_id}/runs", {})
                if "error" in run_resp:
                    logger.warning("[NF] Created init job %s but failed to trigger run: %s",
                                   job_id, run_resp["error"].get("message"))
                else:
                    auto_triggered = True
            deployed.append({
                "id": job_id,
                "name": job["name"],
                "original_name": job["original_name"],
                "image": job["image"],
                "profiles": profiles,
                "auto_triggered": auto_triggered,
            })
            if auto_triggered:
                results.append(f"  {job['original_name']} (init): triggered ({job['image']})")
                logger.info("[NF] Triggered foundation init job %s (%s)", job_id, job["image"])
            else:
                results.append(f"  {job['original_name']} (init): created — user triggers via {profiles}")
                logger.info("[NF] Created use-case init job %s (profiles=%s, not auto-run)",
                            job_id, profiles)
        return deployed

    # ─── Startup sync ───────────────────────────────────────────────────────

    def sync_deployments(self):
        """On startup, query NF API to discover services already running.
        Rebuilds self.deployments so DIAB knows about orphaned NF containers.

        The bulk /services endpoint returns services with empty `ports` and
        empty `external` — so we must fetch each service individually to get
        the real port → DNS mapping. Without this, agent restarts lose the
        URLs and the Workspace tab shows broken laptop links for NF stacks.
        """
        api_key = os.environ.get("NORTHFLANK_API_KEY", "").strip()
        if not api_key:
            return
        try:
            resp = self.api("GET", f"/projects/{NF_PROJECT}/services")
            if "error" in resp:
                logger.warning("[NF] Could not sync deployments: %s", resp["error"])
                return
            services = resp.get("data", {}).get("services", [])
            if not services:
                logger.info("[NF] No existing services found on Northflank")
                return
            stacks_map = {}
            for svc_summary in services:
                nf_name = svc_summary.get("name", "")
                # Fetch full service detail to get ports + image (list endpoint
                # omits both).
                detail = self.api("GET", f"/projects/{NF_PROJECT}/services/{svc_summary.get('id', nf_name)}")
                svc = detail.get("data", svc_summary) if "data" in detail else svc_summary
                matched_stack = None
                for stack_name in sorted(self.agent.stacks.keys(), key=len, reverse=True):
                    if nf_name.startswith(stack_name + "-"):
                        matched_stack = stack_name
                        break
                if not matched_stack:
                    matched_stack = "__unknown__"
                if matched_stack not in stacks_map:
                    stacks_map[matched_stack] = []
                nf_urls = {}
                for port_info in svc.get("ports", []):
                    dns = port_info.get("dns", "")
                    iport = port_info.get("internalPort", 0)
                    if dns:
                        nf_urls[iport] = f"https://{dns}"
                original = nf_name
                if matched_stack != "__unknown__":
                    original = nf_name[len(matched_stack) + 1:]
                stacks_map[matched_stack].append({
                    "id": svc.get("id", nf_name),
                    "name": nf_name,
                    "original_name": original,
                    "image": svc.get("deployment", {}).get("external", {}).get("imagePath", "unknown"),
                    "urls": nf_urls,
                })
            # Also pick up init jobs in the same project. They share the
            # stack-name prefix so we can map them back.
            jobs_map = {}
            try:
                jresp = self.api("GET", f"/projects/{NF_PROJECT}/jobs")
                jobs_list = jresp.get("data", {}).get("jobs", []) if "error" not in jresp else []
                for job_summary in jobs_list:
                    jname = job_summary.get("name", "")
                    matched_stack = None
                    for stack_name in sorted(self.agent.stacks.keys(), key=len, reverse=True):
                        if jname.startswith(stack_name + "-"):
                            matched_stack = stack_name
                            break
                    if not matched_stack:
                        matched_stack = "__unknown__"
                    jobs_map.setdefault(matched_stack, []).append({
                        "id": job_summary.get("id", jname),
                        "name": jname,
                        "original_name": jname[len(matched_stack) + 1:] if matched_stack != "__unknown__" else jname,
                        "image": job_summary.get("deployment", {}).get("external", {}).get("imagePath", "unknown"),
                    })
            except Exception as e:
                logger.warning("[NF] Failed to sync NF jobs on startup: %s", e)
            import datetime
            all_stack_names = set(stacks_map.keys()) | set(jobs_map.keys())
            for stack_name in all_stack_names:
                svcs = stacks_map.get(stack_name, [])
                jobs = jobs_map.get(stack_name, [])
                self.deployments[stack_name] = {
                    "services": svcs,
                    "jobs": jobs,
                    "deployed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "synced_from_nf": True,
                }
                logger.info("[NF] Synced %d services + %d jobs for stack '%s' from Northflank",
                            len(svcs), len(jobs), stack_name)
        except Exception as e:
            logger.warning("[NF] Failed to sync NF deployments on startup: %s", e)

    # ─── Compose → NF translation ───────────────────────────────────────────

    def parse_compose(self, stack_name):
        """Parse a stack's docker-compose.yaml and return NF resource definitions.

        Returns (services, init_jobs, skipped):
          - services: long-running deployment services (have ports OR no
            custom entrypoint). These become NF Deployment Services.
          - init_jobs: one-shot containers (custom entrypoint + no ports).
            These become NF Manual Jobs. Compose typically uses these for
            DB creation, schema seed, bucket setup, etc. — they exit 0 once
            done. Deployment Services can't host them (NF treats exit-0 as
            "needs restart" and crashloops).
          - skipped: human-readable reasons for anything left out (no image
            mapping, profile-gated, etc.)
        """
        import shlex
        stack_meta = self.agent.stacks.get(stack_name)
        if not stack_meta:
            return [], [], [f"Stack '{stack_name}' not found"]

        compose_path = Path(stack_meta["_path"]) / "docker-compose.yaml"
        with open(compose_path) as f:
            compose = yaml.safe_load(f)

        services = []
        init_jobs = []
        skipped = []

        for svc_name, svc_def in compose.get("services", {}).items():
            # Compose `profiles:` partitions services into named groups. In
            # BFSI's compose, profiles (`oltp`, `ml`, `olap`, `setup`) are
            # used as logical partitioning, not opt-in gates — the laptop
            # deploy auto-discovers all profiles via
            # `docker compose config --profiles`. So we deploy profile-gated
            # services normally; only flag the `eval` profile (one-shot
            # evaluator with no entrypoint that exits 0 on the image's
            # default CMD — would crashloop as an NF Service).
            profiles = svc_def.get("profiles") or []
            if "eval" in profiles and not svc_def.get("entrypoint") and not svc_def.get("ports"):
                skipped.append(f"{svc_name} (profile: eval — one-shot evaluator)")
                continue

            # Determine image source. Precedence:
            #   1. GHCR_IMAGES mapping (overrides compose's image: tag)
            #   2. compose's image: directive (used for public images)
            #   3. skip with clear reason
            # Override is intentional — BFSI services have local tags like
            # 'bfsi-ml-inference:latest' that NF can't pull; GHCR mapping
            # rewrites them to the registry path.
            ghcr_key = f"{stack_name}/{svc_name}"
            image = GHCR_IMAGES.get(ghcr_key) or svc_def.get("image")
            if not image:
                if not GHCR_PREFIX:
                    skipped.append(f"{svc_name} (build: service — set GHCR_PREFIX in .env to enable)")
                else:
                    skipped.append(f"{svc_name} (no GHCR mapping)")
                continue

            # NF requires explicit image tag
            if ":" not in image and "@" not in image:
                image += ":latest"

            # Build NF resource name (3-39 chars, alphanumeric + hyphens)
            nf_name = f"{stack_name}-{svc_name}"[:39].rstrip("-")

            # Extract environment variables (skip None/empty values)
            env_vars = {}
            env = svc_def.get("environment", {})
            if isinstance(env, list):
                for item in env:
                    if "=" in str(item):
                        k, v = str(item).split("=", 1)
                        if v and v != "None":
                            env_vars[k] = v
            elif isinstance(env, dict):
                for k, v in env.items():
                    if v is not None:
                        env_vars[k] = str(v)

            # Strip env vars that don't make sense on NF:
            #   1. OLLAMA_*  — host-based; NF uses Bedrock-only LLM path
            #   2. anything pointing at host.docker.internal — won't resolve
            # And inject DEPLOY_TARGET so app code can branch (e.g. hide UI
            # buttons that depend on local Docker socket).
            stripped = {}
            for k, v in env_vars.items():
                if k.startswith("OLLAMA_"):
                    continue
                if isinstance(v, str) and "host.docker.internal" in v:
                    continue
                stripped[k] = v
            stripped["DEPLOY_TARGET"] = "northflank"
            env_vars = stripped

            # Extract ports
            ports = []
            for p in svc_def.get("ports", []):
                p_str = str(p)
                if "-" in p_str.split(":")[-1]:
                    continue
                parts = p_str.split(":")
                try:
                    if len(parts) == 2:
                        internal = int(parts[1].strip('"').split("/")[0])
                    elif len(parts) == 3:
                        internal = int(parts[2].strip('"').split("/")[0])
                    else:
                        continue
                except ValueError:
                    continue
                port_name = f"p{internal}"[:8]
                # Database/message ports use TCP protocol; HTTP ports use HTTP.
                _TCP_PORTS = {5432, 3306, 27017, 6379, 9000, 9092, 2181}
                is_tcp = internal in _TCP_PORTS
                ports.append({
                    "name": port_name,
                    "internalPort": internal,
                    "public": not is_tcp,
                    "protocol": "TCP" if is_tcp else "HTTP",
                })

            # Extract entrypoint + command from compose. Two patterns:
            #   1. `entrypoint: ["/bin/bash", "-c"]` + `command: ["<script>"]`
            #      → Docker runs ENTRYPOINT+CMD as one ARGV. We must combine
            #        both into a single NF customEntrypoint string. Otherwise
            #        only `/bin/bash -c` runs, no script attached.
            #   2. `command: <script>` (no entrypoint) → regular CMD override.
            # `shlex.quote` on each arg preserves multi-line script strings
            # as a single shell argument when joined.
            raw_entry = svc_def.get("entrypoint")
            raw_cmd = svc_def.get("command")
            if isinstance(raw_entry, list):
                entrypoint_str = " ".join(shlex.quote(str(a)) for a in raw_entry)
            else:
                entrypoint_str = raw_entry or ""
            if isinstance(raw_cmd, list):
                command_str = " ".join(shlex.quote(str(a)) for a in raw_cmd)
            elif isinstance(raw_cmd, str):
                command_str = raw_cmd.lstrip(">").strip() if raw_cmd.startswith(">") else raw_cmd
            else:
                command_str = ""
            # If entrypoint is set, combine entrypoint+command into one
            # customEntrypoint and zero out cmd (the script is now part of
            # the entrypoint). If only command, leave entrypoint empty so
            # the deploy path emits customCommand.
            if entrypoint_str and command_str:
                entrypoint = f"{entrypoint_str} {command_str}"
                cmd = None
            elif entrypoint_str:
                entrypoint = entrypoint_str
                cmd = None
            else:
                entrypoint = None
                cmd = command_str or None

            # Capture compose mem_limit (v2 `mem_limit` or v3 `deploy.resources.limits.memory`)
            # so deploy can pick an NF plan that has enough RAM.
            mem_limit = (svc_def.get("deploy", {})
                                .get("resources", {})
                                .get("limits", {})
                                .get("memory")) or svc_def.get("mem_limit")

            # Extract bind-mounted files from compose `volumes:` and inline
            # their content. On laptop, compose mounts host files like
            # `./scripts/setup_metabase.py:/app/setup_metabase.py:ro` directly.
            # On NF there is no bind mount, so the scripts the init jobs
            # depend on are missing inside the pod and the job fails. Instead
            # we read each file here and pass it through NF's /secrets
            # `files` mechanism so it materialises at the expected container
            # path at runtime. Skips named volumes (no leading `./` or `/`)
            # — those are runtime data, not source files.
            mounted_files = {}
            for v in (svc_def.get("volumes") or []):
                if not isinstance(v, str):
                    continue
                parts = v.split(":")
                if len(parts) < 2:
                    continue
                host_path, container_path = parts[0], parts[1]
                if not (host_path.startswith("./") or host_path.startswith("/")):
                    # Named volume (e.g. `pgd-data`) — no source file to inline.
                    continue
                if host_path.startswith("./"):
                    host_abs = (Path(stack_meta["_path"]) / host_path[2:]).resolve()
                else:
                    host_abs = Path(host_path)
                try:
                    if host_abs.is_file():
                        mounted_files[container_path] = host_abs.read_bytes()
                    elif host_abs.is_dir():
                        # Walk the directory: each file becomes a separate
                        # secret-files entry rooted at the container_path.
                        for f in host_abs.rglob("*"):
                            if not f.is_file():
                                continue
                            rel = f.relative_to(host_abs).as_posix()
                            cp = container_path.rstrip("/") + "/" + rel
                            mounted_files[cp] = f.read_bytes()
                except Exception:
                    continue

            record = {
                "name": nf_name,
                "original_name": svc_name,
                "image": image,
                "env": env_vars,
                "ports": ports,
                "cmd": cmd,
                "entrypoint": entrypoint,
                "compose_mem_limit": mem_limit,
                # Compose profiles. Foundation init jobs (no profile) auto-run
                # on deploy; profile-tagged ones (oltp, setup, ml, olap) are
                # user-triggered via the use-case pipeline runner. We carry
                # the profiles forward so _deploy_init_jobs can skip the
                # /runs trigger for the latter.
                "profiles": list(svc_def.get("profiles") or []),
                "files": mounted_files,
            }

            # Classify as init job vs long-running service. Init = one-shot
            # that exits after setup. Two signals (no ports in either case;
            # if it has ports, it's by definition a long-running service):
            #   (a) `entrypoint:` set — the explicit "I override the image
            #       startup with my own script" pattern.
            #   (b) `restart: "no"` — compose's explicit "don't restart on
            #       exit" signal. Catches services with only `command:`
            #       (no entrypoint) that still mean to run once and exit,
            #       e.g. langflow-init, metabase-setup, lakekeeper-migrate.
            # NF Deployment Services that exit 0 get crashloop-restarted, so
            # we route these to NF Manual Jobs instead (one-shot semantics).
            restart_no = str(svc_def.get("restart", "")).strip('"').lower() == "no"
            is_one_shot = (entrypoint or restart_no) and not ports
            if is_one_shot:
                init_jobs.append(record)
            else:
                services.append(record)

        # Rewrite inter-service hostnames for NF internal networking.
        # Env keys we treat as hostname-bearing:
        #   - HOST/HOSTNAME/SERVER/ADDR/ADDRESS/ENDPOINT/BROKER/NODE → value is the host
        #   - URL/URI/DSN/CONNECTION → value embeds a host inside a URL/URI
        _HOST_KEY_PATTERNS = {"HOST", "HOSTNAME", "SERVER", "ADDR", "ADDRESS",
                              "ENDPOINT", "BROKER", "NODE",
                              "URL", "URI", "DSN", "CONNECTION"}
        # Build the name map from BOTH services and init jobs, so jobs can
        # reach services AND services can reach (just-completed) jobs.
        name_map = {r["original_name"]: r["name"] for r in (services + init_jobs)}
        # Longest first so `ml-inference-risingwave` is tried before `risingwave`.
        sorted_names = sorted(name_map.items(), key=lambda x: len(x[0]), reverse=True)
        for svc in (services + init_jobs):
            for k, v in list(svc["env"].items()):
                k_upper = k.upper()
                if not any(p in k_upper for p in _HOST_KEY_PATTERNS):
                    continue
                new_v = v
                # If the value is URL-shaped (contains `://`), rewrite only the
                # host position. Otherwise treat tokens as bare hostnames.
                is_url = "://" in new_v
                # Self-rewrite: only on bare value == own name. On K8s the
                # service name resolves to a ClusterIP that isn't bindable on
                # the pod's own NIC, so postgres' listen_addresses=<ClusterIP>
                # fails. `localhost` works for self-binding; other pods still
                # reach this one via the NF service name.
                if new_v == svc["original_name"]:
                    svc["env"][k] = "localhost"
                    continue
                if is_url:
                    # Parse out the host(s) inside URL-shaped values. Pattern:
                    # `scheme://[user[:pw]@]host[:port][/...]`. Match the
                    # authority section and rewrite ONLY the host token.
                    def rewrite_url(m, _svc=svc):
                        scheme = m.group(1)
                        userinfo = m.group(2) or ""
                        host = m.group(3)
                        port_path = m.group(4) or ""
                        if host == _svc["original_name"]:
                            new_host = "localhost"
                        else:
                            new_host = name_map.get(host, host)
                        return f"{scheme}://{userinfo}{new_host}{port_path}"
                    # scheme://[userinfo@]host[:port|/path|end]
                    url_pat = re.compile(
                        r'([a-zA-Z][a-zA-Z0-9+.\-]*)://([^/@\s]*@)?([A-Za-z0-9.\-]+)([:/][^\s]*)?'
                    )
                    new_v = url_pat.sub(rewrite_url, new_v)
                else:
                    # Bare-hostname value (no `://`). Rewrite each compose name
                    # at host-position boundaries (not preceded/followed by
                    # alnum or hyphen) so `risingwave-foo` and `myrisingwave`
                    # don't match.
                    for compose_name, nf_name in sorted_names:
                        if compose_name == svc["original_name"]:
                            continue
                        pattern = r'(?<![A-Za-z0-9-])' + re.escape(compose_name) + r'(?![A-Za-z0-9-])'
                        new_v = re.sub(pattern, nf_name, new_v)
                svc["env"][k] = new_v
            # Cmd rewriting: narrow to URL-style host positions only. Substring
            # replacement broke script filenames (e.g. `ml-inference-risingwave.py`).
            # The compose pattern that DOES need rewriting is a URL embedded in a
            # CLI flag: `--backend-store-uri postgresql://user:pw@pgd:5432/db`.
            # Match `[scheme]://[user[:pw]@]<host>[:port][/path]` and rewrite only
            # the <host> token.
            if svc.get("cmd"):
                new_cmd = svc["cmd"]
                for compose_name, nf_name in sorted_names:
                    if compose_name == svc["original_name"]:
                        # Don't rewrite own name in cmd — could be in --name flags
                        # or script filenames.
                        continue
                    # Host position: after `//` or `@`, before `:` or `/` or end.
                    pattern = r'(//|@)' + re.escape(compose_name) + r'(?=[:/]|$)'
                    new_cmd = re.sub(pattern, lambda m, n=nf_name: m.group(1) + n, new_cmd)
                svc["cmd"] = new_cmd

            # Entrypoint rewriting (init jobs): narrowly rewrite hostnames at
            # known host positions only. We previously did a broad bare-token
            # replacement and that broke SQL: `CREATE DATABASE mlflow` got
            # rewritten because `mlflow` is also a service name in the same
            # compose. Hostnames in shell scripts only appear in well-defined
            # positions, so we match just those:
            #   - URL host:          `://[user[:pw]@]<host>[:/...]` or `@<host>[:/]`
            #   - psql/mc -h flag:   `-h <host>`
            #   - --host flag:       `--host=<host>` or `--host <host>`
            #   - psql conn string:  `host=<host>`
            # Anything else (DB names, table names, user names, comments) is
            # left alone even if it happens to match a service name.
            if svc.get("entrypoint"):
                new_ep = svc["entrypoint"]
                for compose_name, nf_name in sorted_names:
                    if compose_name == svc["original_name"]:
                        continue
                    esc = re.escape(compose_name)
                    boundary = r'(?![A-Za-z0-9-])'  # name must not continue into alnum/-
                    # URL host position: after `://` or `@`
                    new_ep = re.sub(
                        r'(//|@)' + esc + boundary,
                        lambda m, n=nf_name: m.group(1) + n,
                        new_ep,
                    )
                    # -h <host>  (psql, mc, curl-resolve, …). Capture the
                    # `-h` plus whitespace so it isn't consumed.
                    new_ep = re.sub(
                        r'(-h\s+)' + esc + boundary,
                        lambda m, n=nf_name: m.group(1) + n,
                        new_ep,
                    )
                    # --host=<host>  or  --host <host>
                    new_ep = re.sub(
                        r'(--host[=\s]+)' + esc + boundary,
                        lambda m, n=nf_name: m.group(1) + n,
                        new_ep,
                    )
                    # host=<host>  (psql connection string keyword form)
                    new_ep = re.sub(
                        r'(\bhost=)' + esc + boundary,
                        lambda m, n=nf_name: m.group(1) + n,
                        new_ep,
                    )
                svc["entrypoint"] = new_ep

        return services, init_jobs, skipped

    # ─── Bedrock pre-flight ─────────────────────────────────────────────────

    def check_bedrock_creds(self, stack_name):
        """Return a warning string if the stack uses AWS_BEDROCK_* env vars but
        they're not set in the current process env. Returns empty string otherwise.
        On laptop, Bedrock auth comes from ~/.aws via bind mount; on NF that mount
        doesn't exist so credentials must be passed as env vars."""
        meta = self.agent.stacks.get(stack_name, {})
        compose_path = Path(meta.get("_path", "")) / "docker-compose.yaml"
        if not compose_path.exists():
            return ""
        try:
            with open(compose_path) as f:
                contents = f.read()
        except Exception:
            return ""
        if "AWS_BEDROCK_" not in contents:
            return ""
        present_keys = [k for k in (
            "AWS_BEDROCK_ACCESS_KEY_ID",
            "AWS_BEDROCK_SECRET_ACCESS_KEY",
            "AWS_BEDROCK_REGION",
        ) if os.environ.get(k, "").strip()]
        if len(present_keys) >= 2:
            return ""
        return (
            "⚠ Bedrock-using services were deployed but AWS_BEDROCK_* env vars "
            "aren't set in the agent's environment. LLM features will fail at "
            "runtime. Set these in .env (or in NF Secrets at the project level) "
            "and redeploy: AWS_BEDROCK_ACCESS_KEY_ID, AWS_BEDROCK_SECRET_ACCESS_KEY, "
            "AWS_BEDROCK_REGION."
        )

    # ─── Public lifecycle methods ───────────────────────────────────────────

    def deploy(self, stack_name):
        """Deploy a stack's services to Northflank. Returns status message."""
        # Required env vars must be set before any NF API call
        missing = [k for k, v in {
            "NORTHFLANK_API_KEY": os.environ.get("NORTHFLANK_API_KEY", "").strip(),
            "NORTHFLANK_TEAM": NF_TEAM,
            "NORTHFLANK_PROJECT": NF_PROJECT,
            "NORTHFLANK_BILLING_PLAN": NF_BILLING_PLAN,
        }.items() if not v]
        if missing:
            return f"ERROR: Required Northflank settings missing in .env: {', '.join(missing)}"

        # Bedrock pre-flight: stacks that mention AWS_BEDROCK_* in their compose
        # need real credentials on NF (no ~/.aws bind mount). Warn early.
        bedrock_warning = self.check_bedrock_creds(stack_name)

        meta = self.agent.stacks.get(stack_name, {})
        targets = meta.get("deploy_targets", ["laptop"])
        # 'laptop' implies docker+colima but never northflank, so direct check is fine.
        if "northflank" not in targets:
            return f"ERROR: '{stack_name}' is not available for Northflank deployment (EDB-dependent images)."

        if stack_name in self.deployments:
            return f"'{stack_name}' is already deployed to Northflank. Destroy it first to redeploy."

        nf_services, init_jobs, skipped = self.parse_compose(stack_name)
        if not nf_services and not init_jobs:
            return f"ERROR: No deployable resources found in '{stack_name}'. Skipped: {', '.join(skipped)}"

        results = []
        deployed_services = []
        # Clear any stale cancel flag from a previous attempt before we start.
        self.cancel_requests.discard(stack_name)
        cancelled = False

        for svc in nf_services:
            # Before each service create, check whether the user has clicked
            # Cancel in the UI (via /api/nf/cancel → request_cancel). If so,
            # break out of the loop and let the cleanup block below destroy
            # whatever was already created.
            if stack_name in self.cancel_requests:
                self.cancel_requests.discard(stack_name)
                cancelled = True
                logger.info("[NF] Deploy of %s cancelled by user after %d/%d services created",
                            stack_name, len(deployed_services), len(nf_services))
                break
            ext = {"imagePath": svc["image"]}
            # Attach registry credentials for our private GHCR images.
            # NF rejects deploys of private images without authenticated pull.
            # GHCR_PREFIX is the only known-private path in our config; public
            # images (postgres:17, nginx:alpine, etc.) skip the credential.
            if NF_REGISTRY_CREDENTIALS and GHCR_PREFIX and svc["image"].startswith(GHCR_PREFIX):
                ext["credentials"] = NF_REGISTRY_CREDENTIALS
            # Pick the smallest NF plan that fits the compose mem_limit; if the
            # service has no mem_limit, fall back to NORTHFLANK_BILLING_PLAN. This
            # keeps the heavy stateful services (pgd, kafka, clickhouse) on plans
            # with enough RAM while letting one-shot init scripts and ml-inference
            # workers run on tiny plans — so 25 services fit in a 24-vCPU cluster.
            plan_id = self.pick_plan_for_service(svc.get("compose_mem_limit"), NF_BILLING_PLAN)
            # Postgres-family services need real headroom on top of compose limits
            # (BDR + shared_buffers + Flask wrapper). Compose's mem_limit was
            # tuned for Docker-on-laptop overcommit; on NF the cgroup limit is
            # strict, so under-provisioning silently OOM-kills pg_ctl. Floor
            # postgres images at nf-compute-200 (4 GB) regardless of compose.
            img_lower = svc["image"].lower()
            if any(t in img_lower for t in ("postgres", "/pgd:", "/pgd-", "edb-postgres", "postgresql")):
                plans = self.load_plans()
                if plans:
                    floor_mb = next((p["ram_mb"] for p in plans if p["id"] == "nf-compute-200"), 4096)
                    cur_mb = next((p["ram_mb"] for p in plans if p["id"] == plan_id), 0)
                    if cur_mb < floor_mb:
                        plan_id = "nf-compute-200"
            deployment_block = {
                "instances": 1,
                "external": ext,
            }
            # Postgres-family images need more shared memory than NF's 64 MB default.
            # 1 GB is enough for pgd + bdr + Flask wrapper.
            if any(tag in img_lower for tag in ("postgres", "/pgd:", "/pgd-", "edb-postgres", "postgresql")):
                deployment_block["storage"] = {
                    "ephemeralStorage": {"storageSize": 1024},
                    "shmSize": 1024,
                }
            payload = {
                "name": svc["name"],
                "billing": {"deploymentPlan": plan_id},
                "deployment": deployment_block,
                "ports": svc["ports"] if svc["ports"] else [],
            }
            # Env vars cannot go in the create payload — NF silently drops them.
            # See `attach_env_secret` below: we POST a separate `/secrets` resource
            # with restrictions.nfObjects pointing at this service.
            if svc["cmd"]:
                payload["deployment"]["docker"] = {
                    "configType": "customCommand",
                    "customCommand": svc["cmd"],
                }

            resp = self.api("POST", f"/projects/{NF_PROJECT}/services/deployment", payload)

            if "error" in resp:
                err = resp["error"]
                err_msg = err.get("message", "Unknown error")
                err_details = err.get("details", err.get("validationErrors", ""))
                if err_details:
                    err_msg += f" | Details: {json.dumps(err_details) if isinstance(err_details, (dict, list)) else err_details}"
                results.append(f"  {svc['original_name']}: FAILED — {err_msg}")
                logger.error("[NF] Failed to deploy %s: %s (image=%s, payload=%s)", svc["name"], err_msg, svc["image"], json.dumps(payload)[:500])
            else:
                svc_data = resp.get("data", {})
                svc_id = svc_data.get("id", svc["name"])
                # Attach env vars via the NF /secrets API. The `runtimeEnvironment`
                # field on /services/deployment is silently dropped by NF; the
                # documented primitive is a `secret` resource with
                # secretType=environment-arguments restricted to this service.
                if svc["env"] or svc.get("files"):
                    self.attach_env_secret(svc_id, svc["env"], files=svc.get("files"))
                nf_urls = {}
                for port_info in svc_data.get("ports", []):
                    dns = port_info.get("dns", "")
                    iport = port_info.get("internalPort", 0)
                    if dns:
                        nf_urls[iport] = f"https://{dns}"
                deployed_services.append({
                    "id": svc_id,
                    "name": svc["name"],
                    "original_name": svc["original_name"],
                    "image": svc["image"],
                    "urls": nf_urls,
                })
                url_list = " ".join(f"{v}" for v in nf_urls.values())
                results.append(f"  {svc['original_name']}: deployed ({svc['image']})")
                if url_list:
                    results.append(f"    URL: {url_list}")
                logger.info("[NF] Deployed %s (%s) urls=%s", svc["name"], svc["image"], nf_urls)

        # If the user cancelled mid-deploy, destroy whatever made it through
        # before returning. We don't write to self.deployments first because
        # destroy() reads from it; we add a temporary entry, run destroy, then
        # return a cancellation message instead of the normal success message.
        if cancelled:
            destroyed_count = 0
            for svc in deployed_services:
                secret_name = f"{svc['id']}-env"[:63]
                self.api("DELETE", f"/projects/{NF_PROJECT}/secrets/{secret_name}")
                resp = self.api("DELETE", f"/projects/{NF_PROJECT}/services/{svc['id']}")
                if "error" not in resp or resp.get("error", {}).get("status") == 404:
                    destroyed_count += 1
            stack_display = meta.get("name", stack_name)
            return (f"**Cancelled deploy of {stack_display}.** "
                    f"{destroyed_count} of {len(deployed_services)} created services were removed; "
                    f"the rest ({len(nf_services) - len(deployed_services)}) were never created.")

        # Deploy init jobs (one-shot DB/bucket/schema setup containers).
        # Triggered after services are created, so by the time the jobs run,
        # their target services are at least starting. Init scripts typically
        # have `until ... ; done` wait-loops, so brief connection-refused on
        # first attempt is handled inside the script.
        # Check cancel one more time before starting jobs.
        deployed_jobs = []
        if stack_name in self.cancel_requests:
            self.cancel_requests.discard(stack_name)
            results.append("  init jobs skipped (cancel requested after services)")
        else:
            deployed_jobs = self._deploy_init_jobs(stack_name, init_jobs, results)

        if deployed_services or deployed_jobs:
            import datetime
            self.deployments[stack_name] = {
                "services": deployed_services,
                "jobs": deployed_jobs,
                "deployed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

        stack_display = meta.get("name", stack_name)
        msg = f"**{stack_display}** deployed to Northflank\n\n"

        for svc in deployed_services:
            urls = svc.get("urls", {})
            primary_url = next(iter(urls.values()), None) if urls else None
            img = svc['image']
            if "/" in img:
                img = img.rsplit("/", 1)[-1]
            line = f"  ✓ **{svc['original_name']}** ({img})"
            if primary_url:
                line += f"\n    {primary_url}"
            msg += line + "\n"

        # Append init-job summary, split into foundation (auto-triggered) vs
        # use-case (user-triggered when they click a pipeline action).
        if deployed_jobs:
            foundation = [j for j in deployed_jobs if j.get("auto_triggered")]
            use_case   = [j for j in deployed_jobs if not j.get("auto_triggered")]
            if foundation:
                msg += "\n**Foundation init jobs** (auto-triggered now):\n"
                for j in foundation:
                    img = j['image']
                    if "/" in img:
                        img = img.rsplit("/", 1)[-1]
                    msg += f"  ▶ **{j['original_name']}** ({img}) — triggered\n"
            if use_case:
                msg += "\n**Use-case init jobs** (created, you trigger them when running a use case):\n"
                for j in use_case:
                    img = j['image']
                    if "/" in img:
                        img = img.rsplit("/", 1)[-1]
                    profs = ",".join(j.get("profiles", []))
                    msg += f"  ◷ **{j['original_name']}** ({img}) — profile: {profs}\n"

        msg += "\n> Services are starting. URLs may take 1-2 min to become reachable while containers start and DNS propagates. Foundation init jobs run in parallel; their wait-loops handle dependency timing. Use-case init jobs (e.g. metabase-setup, aidb-init) sit idle until you click the matching use case in the workspace.\n"

        access_urls = meta.get("access", [])
        if access_urls and deployed_services:
            msg += "\n**Access:**\n"
            for access in access_urls:
                aname = access.get("name", "")
                local_url = access.get("url", "")
                port_match = re.search(r":(\d{4,5})", local_url)
                local_port = port_match.group(1) if port_match else ""
                nf_url = None
                for svc in deployed_services:
                    for iport, url in svc.get("urls", {}).items():
                        if str(iport) == local_port:
                            nf_url = url
                            break
                    if nf_url:
                        break
                if not nf_url:
                    for svc in deployed_services:
                        urls = svc.get("urls", {})
                        if not urls:
                            continue
                        svc_orig = svc["original_name"].lower()
                        if svc_orig in aname.lower() or aname.lower().split()[0] in svc_orig:
                            nf_url = next(iter(urls.values()))
                            break
                if nf_url:
                    msg += f"  • {aname}: {nf_url}\n"

        if bedrock_warning:
            msg += f"\n{bedrock_warning}\n"
        msg += f"\n[NF Console]({NF_CONSOLE_BASE}/t/{NF_TEAM}/project/{NF_PROJECT})"
        return msg

    def stop(self, stack_name):
        """Pause all NF services for a stack."""
        if stack_name not in self.deployments:
            return f"'{stack_name}' is not deployed to Northflank."
        results = []
        for svc in self.deployments[stack_name]["services"]:
            resp = self.api("POST", f"/projects/{NF_PROJECT}/services/{svc['id']}/pause")
            if "error" in resp:
                results.append(f"  {svc['original_name']}: FAILED — {resp['error'].get('message', '')}")
            else:
                results.append(f"  {svc['original_name']}: paused")
        return f"**Stopped on Northflank: {stack_name}**\n" + "\n".join(results)

    def resume(self, stack_name):
        """Resume paused NF services for a stack."""
        if stack_name not in self.deployments:
            return f"'{stack_name}' is not deployed to Northflank."
        results = []
        for svc in self.deployments[stack_name]["services"]:
            resp = self.api("POST", f"/projects/{NF_PROJECT}/services/{svc['id']}/resume")
            if "error" in resp:
                results.append(f"  {svc['original_name']}: FAILED — {resp['error'].get('message', '')}")
            else:
                results.append(f"  {svc['original_name']}: resumed")
        return f"**Resumed on Northflank: {stack_name}**\n" + "\n".join(results)

    def destroy(self, stack_name):
        """Delete all NF services for a stack, plus the env-secrets we created."""
        if stack_name not in self.deployments:
            return f"'{stack_name}' is not deployed to Northflank."
        results = []
        for svc in self.deployments[stack_name].get("services", []):
            # Delete the env-secret first (best-effort; 404 is fine if absent).
            secret_name = f"{svc['id']}-env"[:63]
            self.api("DELETE", f"/projects/{NF_PROJECT}/secrets/{secret_name}")
            resp = self.api("DELETE", f"/projects/{NF_PROJECT}/services/{svc['id']}")
            if "error" in resp:
                err_msg = resp['error'].get('message', '')
                if 'not find' in err_msg.lower() or resp['error'].get('status') == 404:
                    results.append(f"  {svc['original_name']}: already gone")
                else:
                    results.append(f"  {svc['original_name']}: FAILED — {err_msg}")
            else:
                results.append(f"  {svc['original_name']}: deleted")
        for job in self.deployments[stack_name].get("jobs", []):
            secret_name = f"{job['id']}-env"[:63]
            self.api("DELETE", f"/projects/{NF_PROJECT}/secrets/{secret_name}")
            resp = self.api("DELETE", f"/projects/{NF_PROJECT}/jobs/{job['id']}")
            if "error" in resp:
                err_msg = resp['error'].get('message', '')
                if 'not find' in err_msg.lower() or resp['error'].get('status') == 404:
                    results.append(f"  {job['original_name']} (job): already gone")
                else:
                    results.append(f"  {job['original_name']} (job): FAILED — {err_msg}")
            else:
                results.append(f"  {job['original_name']} (job): deleted")
        del self.deployments[stack_name]
        return f"**Destroyed on Northflank: {stack_name}**\n" + "\n".join(results)

    def nuke_project(self):
        """Wipe every service, job, and secret in the configured NF project,
        regardless of what's tracked in self.deployments. Survives agent
        restarts and any sync drift — queries NF, deletes everything found.

        Returns a summary dict with counts. Use as the "I don't care what
        DIAB thinks; just clear NF" escape hatch."""
        results = {"services": 0, "jobs": 0, "secrets": 0, "errors": []}
        # Jobs first — they may reference secrets which we delete next.
        for kind, plural in (("jobs", "jobs"), ("services", "services"), ("secrets", "secrets")):
            list_resp = self.api("GET", f"/projects/{NF_PROJECT}/{kind}")
            if "error" in list_resp:
                results["errors"].append(f"list {kind}: {list_resp['error'].get('message','?')}")
                continue
            for item in list_resp.get("data", {}).get(plural, []):
                item_id = item.get("id") or item.get("name")
                if not item_id:
                    continue
                dr = self.api("DELETE", f"/projects/{NF_PROJECT}/{kind}/{item_id}")
                if "error" in dr and dr.get("error", {}).get("status") != 404:
                    results["errors"].append(f"delete {kind}/{item_id}: {dr['error'].get('message','?')}")
                else:
                    results[kind] += 1
                    logger.info("[NF] nuke: deleted %s/%s", kind, item_id)
        # Forget any local deployment tracking so the UI doesn't lie about
        # what's running.
        self.deployments.clear()
        return results

    def run_pipeline_step(self, stack_name, pipeline_id, step_id):
        """Generator equivalent of agent.run_pipeline_step_stream() for NF-deployed
        stacks. On laptop, the pipeline step runs a bash script that does
        `docker compose --profile=<x> up` + `docker exec` + local data
        loading — none of that applies on NF. Here we trigger the use-case
        NF Jobs whose compose profile matches the pipeline_id (e.g.
        pipeline 'oltp' → jobs with profile in {'oltp','setup'}), then poll
        their logs until each one completes.

        Yields dicts compatible with the existing SSE stream:
        {'type': 'line'|'heartbeat'|'done'|'error', ...}.
        """
        import time
        deps = self.deployments.get(stack_name, {})
        all_jobs = deps.get("jobs", [])
        if not all_jobs:
            yield {"type": "error",
                   "text": "No NF jobs found for this stack. Re-deploy first."}
            return

        # Pipeline → set of compose profiles to trigger. Default: the
        # pipeline ID itself + 'setup' (Metabase/dashboard setup is shared
        # across OLTP/OLAP use cases). Override on a per-pipeline basis if
        # we ever need finer control.
        pipeline_profiles = {pipeline_id.lower(), "setup"}
        targets = [j for j in all_jobs
                   if any(p in pipeline_profiles for p in (j.get("profiles") or []))]
        if not targets:
            yield {"type": "error",
                   "text": f"No use-case jobs on NF match pipeline '{pipeline_id}'. "
                           f"Foundation init jobs are already running automatically; "
                           f"this use case may not have setup steps."}
            return

        yield {"type": "line", "text": f"=== Running '{pipeline_id}' use case on Northflank ==="}
        yield {"type": "line", "text": f"Found {len(targets)} matching job(s): "
                                       f"{', '.join(j['original_name'] for j in targets)}"}
        yield {"type": "line", "text": ""}

        # Trigger each job (POST /jobs/{id}/runs). NF queues the run; the
        # pod is scheduled by the in-cluster reconciler shortly after.
        triggered = []
        for j in targets:
            run_resp = self.api("POST", f"/projects/{NF_PROJECT}/jobs/{j['id']}/runs", {})
            if "error" in run_resp:
                err = run_resp["error"].get("message", "?")
                yield {"type": "line", "text": f"  ✗ {j['original_name']}: trigger failed — {err}"}
                continue
            yield {"type": "line", "text": f"  ▶ {j['original_name']} → triggered"}
            triggered.append(j)

        if not triggered:
            yield {"type": "done", "success": False, "elapsed_ms": 0}
            return

        yield {"type": "line", "text": ""}
        yield {"type": "line", "text": "=== Streaming logs (until each job completes) ==="}

        # Poll each job's runtime logs every 3s. Stop tracking a job when
        # we see its "Process terminated with exit code N" line.
        last_ts = {j["id"]: 0 for j in triggered}
        completed = set()
        succeeded = set()
        t0 = time.time()
        max_wait_s = 600  # 10 minutes — generous for slowest init (synthdb, schema)
        last_heartbeat = time.time()

        while len(completed) < len(triggered) and (time.time() - t0) < max_wait_s:
            any_progress = False
            for j in triggered:
                if j["id"] in completed:
                    continue
                resp = self.api("GET",
                    f"/projects/{NF_PROJECT}/jobs/{j['id']}/logs?type=runtime&perPage=80")
                events = resp.get("data", []) if "error" not in resp else []
                events.sort(key=lambda e: int(e.get("unixTs", "0")))
                for ev in events:
                    ts = int(ev.get("unixTs", "0"))
                    if ts <= last_ts[j["id"]]:
                        continue
                    last_ts[j["id"]] = ts
                    any_progress = True
                    raw = ev.get("log", "")
                    parts = raw.split(" ", 3)
                    msg = parts[-1] if len(parts) >= 3 else raw
                    yield {"type": "line", "text": f"  [{j['original_name']}] {msg[:300]}"}
                    if "terminated with exit code" in msg.lower():
                        completed.add(j["id"])
                        try:
                            code = int(msg.split("exit code")[-1].strip().rstrip(" .").split()[0])
                        except Exception:
                            code = -1
                        if code == 0:
                            succeeded.add(j["id"])
                        yield {"type": "line",
                               "text": f"  {'✓' if code == 0 else '✗'} {j['original_name']} finished (exit {code})"}
            if not any_progress:
                if time.time() - last_heartbeat > 5:
                    yield {"type": "heartbeat"}
                    last_heartbeat = time.time()
                time.sleep(3)

        elapsed_ms = int((time.time() - t0) * 1000)
        if len(completed) < len(triggered):
            yield {"type": "line",
                   "text": f"  ⏱ Timeout waiting for {len(triggered) - len(completed)} job(s) to finish "
                           f"(after {max_wait_s}s). They may still be running in NF Console."}
            yield {"type": "done", "success": False, "elapsed_ms": elapsed_ms}
            return

        all_ok = len(succeeded) == len(triggered)
        yield {"type": "line", "text": ""}
        yield {"type": "line",
               "text": f"=== Done: {len(succeeded)}/{len(triggered)} jobs succeeded ==="}
        yield {"type": "done", "success": all_ok, "elapsed_ms": elapsed_ms}

    def get_status(self):
        """Get status of all services in the NF diab project, including readiness."""
        resp = self.api("GET", f"/projects/{NF_PROJECT}/services")
        if "error" in resp:
            return {"services": [], "error": resp["error"].get("message", "")}

        services = []
        for svc in resp.get("data", {}).get("services", []):
            ports = svc.get("ports", [])
            raw_status = svc.get("status", {})
            if isinstance(raw_status, dict):
                dep_status = raw_status.get("deployment", {})
                status = dep_status.get("status", "unknown") if isinstance(dep_status, dict) else str(dep_status)
            else:
                status = str(raw_status)

            paused = svc.get("servicePaused", False)
            is_ready = status == "COMPLETED" and not paused

            svc_urls = []
            for p in ports:
                dns = p.get("dns", "")
                if dns:
                    svc_urls.append(f"https://{dns}")

            svc_name = svc.get("name", "").lower()
            is_http_service = bool(svc_urls) and not any(db in svc_name for db in ["postgres", "clickhouse", "redis", "kafka", "minio"])
            urls_ready = True
            if is_http_service:
                for url in svc_urls:
                    try:
                        probe = urllib.request.Request(url, method="HEAD")
                        urllib.request.urlopen(probe, timeout=5)
                    except Exception:
                        urls_ready = False

            services.append({
                "id": svc.get("id", ""),
                "name": svc.get("name", ""),
                "status": "running" if is_ready else status.lower(),
                "ready": is_ready,
                "urls_ready": urls_ready if is_http_service else is_ready,
                "urls": svc_urls,
            })
        return {"services": services}

    def get_console_url(self):
        """Return the NF console URL for the configured project."""
        return f"{NF_CONSOLE_BASE}/t/{NF_TEAM}/project/{NF_PROJECT}"

    # ─── Preflight (NF-specific branch) ─────────────────────────────────────

    def preflight(self, stack_name=None):
        """NF-specific preflight checks. Returns (checks, config, missing) tuples
        for the caller to combine into the full report. Keeps the agent's
        public `preflight()` API stable while moving NF logic out."""
        def mask(v, keep_prefix=8, keep_suffix=0):
            if not v:
                return ""
            if len(v) <= keep_prefix + keep_suffix + 3:
                return v[:keep_prefix] + "..."
            return v[:keep_prefix] + "..." + (v[-keep_suffix:] if keep_suffix else "")

        checks = []
        config = {}
        missing = []

        api_key = os.environ.get("NORTHFLANK_API_KEY", "").strip()
        if api_key:
            checks.append({"name": "NORTHFLANK_API_KEY", "status": "ok", "detail": "set"})
            config["NF API key"] = mask(api_key, 6)
        else:
            checks.append({"name": "NORTHFLANK_API_KEY", "status": "fail", "detail": "not set"})
            missing.append("Generate NF API token: NF Console → Account → API tokens. Set NORTHFLANK_API_KEY in .env.")

        if NF_TEAM:
            checks.append({"name": "NORTHFLANK_TEAM", "status": "ok", "detail": NF_TEAM})
            config["NF team"] = NF_TEAM
        else:
            checks.append({"name": "NORTHFLANK_TEAM", "status": "fail", "detail": "not set"})
            missing.append("Set NORTHFLANK_TEAM in .env (the path segment after /t/ in NF Console URL).")

        if NF_PROJECT:
            checks.append({"name": "NORTHFLANK_PROJECT", "status": "ok", "detail": NF_PROJECT})
            config["NF project"] = NF_PROJECT
        else:
            checks.append({"name": "NORTHFLANK_PROJECT", "status": "fail", "detail": "not set"})
            missing.append("Create an NF project bound to a cluster, then set NORTHFLANK_PROJECT in .env.")

        if NF_BILLING_PLAN:
            checks.append({"name": "NORTHFLANK_BILLING_PLAN", "status": "ok", "detail": NF_BILLING_PLAN})
            config["NF billing plan"] = NF_BILLING_PLAN
        else:
            checks.append({"name": "NORTHFLANK_BILLING_PLAN", "status": "fail", "detail": "not set"})
            missing.append("Set NORTHFLANK_BILLING_PLAN in .env (e.g. nf-compute-20).")

        if GHCR_PREFIX:
            checks.append({"name": "GHCR_PREFIX", "status": "ok", "detail": GHCR_PREFIX})
            config["Image registry"] = GHCR_PREFIX
        else:
            checks.append({"name": "GHCR_PREFIX", "status": "warn",
                           "detail": "not set — services with build: will be skipped"})

        if NF_REGISTRY_CREDENTIALS:
            checks.append({"name": "NORTHFLANK_REGISTRY_CREDENTIALS", "status": "ok",
                           "detail": NF_REGISTRY_CREDENTIALS})
            config["Registry creds id"] = NF_REGISTRY_CREDENTIALS
        else:
            checks.append({"name": "NORTHFLANK_REGISTRY_CREDENTIALS", "status": "warn",
                           "detail": "not set — private images will fail to pull"})
            missing.append("NF Console → team → Integrations → Registries → Add new (provider github, username + PAT with read:packages). Set NORTHFLANK_REGISTRY_CREDENTIALS=<integration id> in .env.")

        # Live NF reachability probe
        if api_key and NF_PROJECT:
            resp = self.api("GET", f"/projects/{NF_PROJECT}")
            if "error" in resp:
                checks.append({"name": "NF project reachable", "status": "fail",
                               "detail": resp["error"].get("message", "API error")})
            else:
                proj = resp.get("data", {})
                cluster = proj.get("cluster", {})
                checks.append({"name": "NF project reachable", "status": "ok",
                               "detail": f"region={proj.get('deployment',{}).get('region','?')}"})
                if cluster.get("id"):
                    config["NF cluster"] = cluster["id"]
                    checks.append({"name": "NF cluster bound", "status": "ok",
                                   "detail": cluster["id"]})
                counts = (len(proj.get("services", [])), len(proj.get("jobs", [])), len(proj.get("addons", [])))
                config["Existing resources"] = f"{counts[0]} services, {counts[1]} jobs, {counts[2]} addons"

        # Bedrock for deployed services
        bedrock_keys = [k for k in (
            "AWS_BEDROCK_ACCESS_KEY_ID",
            "AWS_BEDROCK_SECRET_ACCESS_KEY",
            "AWS_BEDROCK_REGION",
        ) if os.environ.get(k, "").strip()]
        if len(bedrock_keys) >= 2:
            checks.append({"name": "Bedrock credentials", "status": "ok",
                           "detail": f"{len(bedrock_keys)}/3 vars set"})
            config["Bedrock region"] = os.environ.get("AWS_BEDROCK_REGION", "?")
        else:
            checks.append({"name": "Bedrock credentials", "status": "warn",
                           "detail": "not set — LLM features will fail at runtime"})
            missing.append("aws sso login --profile Bedrock && aws --profile Bedrock configure export-credentials --format env — paste AWS_BEDROCK_* into .env.")

        return checks, config, missing
