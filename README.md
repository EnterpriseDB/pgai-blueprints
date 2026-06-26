# EDB Postgres® AI Blueprints v0.1rc8

*For demonstration purposes only.*

A ready-to-deploy reference architecture demonstrating end-to-end integration between operational databases, analytics, AI, and real-time pipelines with industry-standard use cases.

This project provides a comprehensive proof-of-concept (PoC) environment that showcases the seamless integration between enterprise database systems and modern data analytics platforms. It demonstrates how organizations can leverage the power of EDB PG AI Platform alongside other components to build robust, scalable, and intelligent data solutions.

## Disclaimer

This project is for demonstration purposes only.

---

## Use Cases

The active **BFSI Fraud Detection** stack exposes 6 progressive use cases. Each builds on the previous and is click-to-run from the UI's *Use Cases* tab, with an inline collapsible Flow panel explaining the narrative.

- **OLTP** — Bank App writes transactions into EDB PGD; start a live simulator from the UI to keep TX flowing.
- **OLAP** — Debezium CDC fans the OLTP feed into ClickHouse + RisingWave + Iceberg, viewable side-by-side in Metabase.
- **ML Fraud Detection** — Jupyter trains a fraud model, MLflow registers it, four inference paths score the live TX stream.
- **ML Governance** — MLflow experiment tracking + model registry over Use Case 3's runs.
- **GenAI Fraud Audit** — LangFlow agent auditing flagged transactions, using AIDB semantic search and an Ollama LLM.
- **AI Governance** — LLM-as-judge evaluation of Use Case 5's agent on AWS Bedrock; metrics land back in MLflow.

---

## Technology Stack

Partner technologies that power the BFSI stack:

| Partner | Role | Status |
|---|---|---|
| EDB Postgres Distributed (PGD + PGAA + AIDB) | OLTP + vector / AI extensions | ✓ Active |
| RisingWave | Streaming SQL, materialized views | ✓ Active |
| ClickHouse | OLAP / batch analytics | ✓ Active |
| MinIO | S3-compatible object storage (Iceberg + rule docs) | ✓ Active |
| MLflow | ML lifecycle (experiments, model registry, traces) | ✓ Active |
| JupyterHub / Jupyter | ML training environment | ✓ Active |
| LangFlow | Visual GenAI agent builder | ✓ Active |
| Lakekeeper | Apache Iceberg REST catalog | ✓ Active |
| Metabase | BI dashboards across the data layers | ✓ Active |
| Airflow / Astronomer | Workflow orchestration | Planned |
| Fivetran, DBT | Alt ingestion + transformation paths | Planned |

---

## Framework Principle

Stacks are **vendored as-is** — we never modify the upstream `docker-compose.yaml` or scripts. The value-add is the chat agent + `stack.yaml` metadata that translates **one stack definition** into the target infra:

| Deploy target | What the agent does with `docker-compose.yaml` |
|---|---|
| Laptop · Docker Desktop | `docker compose up -d --build` against the desktop-linux context |
| Laptop · Colima | Same command, against the `colima` context, plus VM preflight (CPU / memory / Rosetta / `/var/run/docker.sock`) |
| Cloud · Northflank (BYOC) | Translates services → NF Services (pods), one-shot init containers → NF Jobs, env + bind-mounts → NF Secrets; deploys via NF REST API |

**One stack file. Three deploy targets. No per-infra forks.** Pick the target from the UI header dropdown at deploy time.

---

## Supported Targets

DIAB supports three deploy targets, selectable from the UI header dropdown.

### Target 1 — Laptop · Docker Desktop (default)

The path most users land on. Works on macOS, Windows (WSL2), and Linux.

**Pre-requisites**

- Docker Desktop installed and running (macOS / Windows) **or** Docker Engine + Compose plugin (Linux)
- Python 3.9+
- Git
- An LLM credential: Anthropic API key **or** AWS Bedrock (SSO profile / access keys)
- **EDB subscription token** (`EDB_SUBSCRIPTION_TOKEN` in `.env`) — required for BFSI and any stack that pulls EDB images from `docker.enterprisedb.com`. Get it at https://www.enterprisedb.com/repos-downloads
- For BFSI: ≥8 vCPU / 32 GiB / 100 GB allocated to Docker Desktop (Settings → Resources). Lighter stacks fit in 4/8/60.

**Configuration**

```bash
cp .env.example .env
# Set ANTHROPIC_API_KEY or AWS Bedrock vars (see Operating Guide → .env Configuration)
```

**Windows (WSL2) — corporate SSL / Netskope**

Run all commands in the WSL2 shell. Some networks use a TLS proxy (e.g. **Netskope**) with a custom root certificate. Testers have reported that **AWS Bedrock** can work with the usual AWS SSL environment, while the **Anthropic (Claude) path** may need extra trust settings so Python / Node pick up your corporate CA.

1. Complete **AWS Bedrock** login / SSO as you normally would (`aws sso login`, etc.).
2. Export your browser's SSL / org CA to a PEM file and place it somewhere stable, e.g. `C:\certs\certadmin.pem`.
3. Point `.env` at it (centralizes the path for all scripts):

   ```bash
   CORP_SSL_CERT=C:/certs/certadmin.pem
   ```

4. For **WSL** or **Git Bash**, scripts pick up `CORP_SSL_CERT` automatically. If running tools manually outside the scripts:

   ```bash
   export NODE_EXTRA_CA_CERTS="${CORP_SSL_CERT:-C:/certs/certadmin.pem}"
   export AWS_CA_BUNDLE="${CORP_SSL_CERT:-C:/certs/certadmin.pem}"
   export NODE_OPTIONS="--use-openssl-ca"
   ```

   Inside WSL, `C:\...` maps to `/mnt/c/...`.

5. Then run **`make agent`**. Order matters: Bedrock auth first, then these variables, then the tool.

If TLS errors persist, confirm the PEM matches what the browser trusts and that IT has approved the cert for API access. This is an environment-specific workaround, not a product requirement.

**Steps**

```bash
make setup                 # one-time prereq check
make agent                 # opens http://127.0.0.1:4000
```

In the UI: pick **Laptop · Docker Desktop** in the header dropdown → click **Deploy** on BFSI Fraud Detection. First deploy takes ~10 min for cold image pulls.

---

### Target 2 — Laptop · Colima (macOS / Linux only)

Free, open-source alternative to Docker Desktop. Runs `dockerd` inside a lightweight VM. Hidden in the UI on Windows.

**Pre-requisites**

- Homebrew
- ≥40 GB host RAM (VM wants 32 GB for BFSI; host needs headroom)
- ≥120 GB free disk on `/`
- Apple Silicon Mac recommended (for `--vz-rosetta`, which makes amd64-pinned containers like BFSI's PGD ~10× faster); Intel Mac and Linux work without it.
- Same `EDB_SUBSCRIPTION_TOKEN`, LLM credential, Python 3.9+, and Git as Target 1.

**Configuration**

```bash
brew install docker docker-compose colima        # installs docker CLI only — NOT Docker Desktop

# Apple Silicon (M1/M2/M3/M4)
colima start --cpu 8 --memory 32 --disk 100 --vm-type=vz --vz-rosetta

# Intel Mac / Linux
colima start --cpu 8 --memory 32 --disk 100

docker context use colima                        # point the docker CLI at Colima
docker context show                              # expect: colima
```

Sizing reference: BFSI floor is **8 / 32 / 100**, `core-banking-simulator` runs at 6/24/80, lighter stacks fit in 4/8/60.

Lifecycle:

```bash
colima stop                # pause VM (state preserved)
colima delete              # destroy VM entirely
colima start --memory 40   # resize (stop first, then start with new flags)
```

**Steps**

```bash
make setup
make agent
```

In the UI: pick **Laptop · Colima** in the header dropdown → click **Deploy**. The chat shows a preflight report — if the VM is undersized, the docker context is wrong, or Rosetta is off when needed, the message tells you the exact command to fix it.

---

### Target 3 — Cloud · Northflank (BYOC)

Production-grade option for shared / partner demos. Northflank provisions an EKS cluster in **your** AWS account and acts as the control plane; DIAB translates `docker-compose.yaml` into NF Services + Jobs + Secrets via the NF REST API. Currently NF-enabled stacks: `bfsi-fraud-detection`, `core-banking-simulator`, `sovereign-data-tier`.

**Pre-requisites**

- AWS account with billing alerts configured
- Northflank account + API key (project-scoped)
- AWS SSO profile that can create IAM roles (`aws sso login`)
- GitHub PAT with `read:packages` scope (for ghcr.io image pulls)
- `EDB_SUBSCRIPTION_TOKEN` if you plan to (re)build any EDB-based GHCR images locally before pushing; not needed at deploy time since NF pulls from your pre-built GHCR
- Budget for ~$26/day with 3× t3a.2xlarge while the cluster is running

**Configuration**

One-time setup (~30 min):

1. **NF Console → Cloud Providers → Add AWS integration**. Copy the trust policy, inline policy, and the NF principal ARN it shows.

2. **Create the cross-account IAM role** in your AWS account. Save the trust + inline policies from step 1 to local files (e.g. `nf-trust-policy.json`, `nf-inline-policy.json`), then:

   ```bash
   aws --profile edb-eks iam create-role --role-name northflank-byoc \
     --assume-role-policy-document file://nf-trust-policy.json
   aws --profile edb-eks iam put-role-policy --role-name northflank-byoc \
     --policy-name northflank-byoc-policy \
     --policy-document file://nf-inline-policy.json
   ```

   Paste the role ARN back into NF → Verify (must show 50/50 OK).

3. **Provision the EKS cluster** from NF Console: `us-east-1`, default VPC, 3× `t3a.2xlarge` node pool. ~20 min.

4. **Create the NF project** bound to that cluster, then under Registries add `ghcr.io` with your GitHub PAT.

5. **Build + push** any private images listed in `engine/agent/translators/northflank.py:GHCR_IMAGES`. Public images pull directly from upstream.

6. **`.env` entries**:

   ```bash
   NORTHFLANK_API_KEY=nf-...
   NORTHFLANK_TEAM=<team>
   NORTHFLANK_PROJECT=<project-id>
   NORTHFLANK_BILLING_PLAN=nf-compute-200
   NORTHFLANK_REGISTRY_CREDENTIALS=<registry-id>
   GHCR_PREFIX=ghcr.io/<github-user>/<image-prefix>
   ```

**Steps**

```bash
make agent
```

In the UI: pick **Northflank Cloud** in the header dropdown → click **Deploy**.

**Cost & cleanup**: tear the cluster down from NF Console after each test session — AWS keeps billing for control plane + nodes otherwise.

---

## Operating Guide

### .env Configuration

Copy `.env.example` → `.env`. The agent auto-detects LLM credentials in order: direct Anthropic key → AWS profile / SSO → Bedrock-oriented profile names → default AWS chain. Explicit overrides:

```bash
# LLM (pick one)
ANTHROPIC_API_KEY=sk-ant-...                      # Anthropic API
# OR
AWS_PROFILE=Bedrock                               # AWS Bedrock via SSO
AWS_DEFAULT_REGION=us-east-1
# OR
AWS_ACCESS_KEY_ID=...                             # AWS Bedrock via access keys
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# EDB subscription (REQUIRED for stacks that pull from docker.enterprisedb.com —
# BFSI Fraud Detection, analytics-comparison, core-banking-simulator,
# unified-analytics-intelligence, and the edb-pgd / edb-whpg plugins).
# Get the token at: https://www.enterprisedb.com/repos-downloads
EDB_SUBSCRIPTION_TOKEN=

# Northflank (only if using Target 3)
NORTHFLANK_API_KEY=nf-...
NORTHFLANK_TEAM=<team>
NORTHFLANK_PROJECT=<project-id>
NORTHFLANK_BILLING_PLAN=nf-compute-200
NORTHFLANK_REGISTRY_CREDENTIALS=<registry-id>
GHCR_PREFIX=ghcr.io/<github-user>/<image-prefix>

# Optional overrides
# LLM_PROVIDER=anthropic | bedrock
# PORT=4000
# AGENT_API_KEY=...                  # protects selected HTTP endpoints if set
# CORP_SSL_CERT=/path/to/ca.pem      # corporate TLS proxy (Netskope etc.)
```

### Make Commands

| Command | One-liner |
|---|---|
| `make setup` | Verify Docker is running, install Python deps, check free ports. |
| `make agent` | Start the chat agent UI at **http://127.0.0.1:4000**. Does not deploy a stack. |
| `make stop` | Kill the agent process only. Containers + volumes keep running. |
| `make restart` | `stop` then `agent`. |
| `make stop-all` | Stop every stack's containers; keep data volumes. |
| `make clean` | **Cross-infra full reset**: agent + containers + volumes + networks + ports across Docker Desktop *and* Colima, plus NF destroys for tracked stacks. Refuses to kill foreign processes unless `FORCE_KILL_PORTS=1`. |
| `make status` | Read-only: running containers, compose projects, framework ports. |
| `make logs` | Last 50 lines of `engine/agent/logs/agent.log`. |
| `make logs-follow` | `tail -f` the agent log. |

### Repeatable Deploy Recipe

Three steps for first-time testers — never has to think about state.

```bash
# 1. ONE-TIME PER LAPTOP
make setup

# 2. EVERY TIME — start the agent
make agent
# → open http://127.0.0.1:4000

# 3. IN THE BROWSER
#    a. Pick a deploy target in the header (Docker Desktop / Colima / Northflank)
#    b. Click "Deploy" on BFSI Fraud Detection
#    c. Wait ~10 min for cold image pulls
#    d. Use Cases tab → Use Case 1 → expand the Flow panel for guidance
```

When anything looks wrong, the one-liner rule is:

```bash
make clean && make agent
```

`make clean` is cross-runtime aware — it tears down compose projects on both Docker Desktop and Colima, frees diab ports, stops Colima if it was holding any port, and destroys NF stacks the agent is tracking. After it returns you can pick a different deploy target with zero leftover state.

### Quick Guide

| Situation | Command |
|---|---|
| First time on this laptop | `make setup && make agent` |
| Resume a session | `make agent` |
| Pause for the day, keep data | `make stop-all` |
| Switch between Docker Desktop ↔ Colima ↔ NF | `make clean && make agent` |
| Agent acting weird | `make restart` |
| Anything else looks broken | `make clean && make agent` |
| Done for the day | `make clean` (frees ports, stops Colima, destroys NF stacks) |

---

## Active Stacks

### BFSI Fraud Detection — Live

The reference implementation. 6 progressive use cases (OLTP → OLAP → ML Fraud → ML Gov → GenAI Audit → AI Gov), with the Flow panel teaching what each step does.

- Folder: [`stacks/bfsi-fraud-detection/`](stacks/bfsi-fraud-detection/)
- NF-enabled: yes
- Default sizing: 8 vCPU / 32 GiB / 100 GB

### Coming Soon

Industry-case placeholders that render greyed-out on the Industry tab. Defined in `engine/agent/app.py` (`CARD_DEFS`, ~line 5148). To activate one, add a `/stacks/<key>/` folder with `docker-compose.yaml` + `stack.yaml` where `<key>` matches.

- Telecom Churn Prediction
- Healthcare Claims Anomaly
- Manufacturing Defect Detection
- E-Commerce Product Search
- Legal Document Intelligence
- Media Content Discovery
- Pharma Drug Interaction
- Government Citizen Data
- Financial Regulatory Reporting
- Healthcare Patient Records
- Energy Grid Telemetry

### Vendored References (`_template` and others)

Folders under `/stacks/` that don't appear on the Industry tab today. They're reusable building blocks and copy-from-templates for new stacks.

| Folder | Purpose |
|---|---|
| `_template` | Skeleton to copy when adding a new stack. |
| `analytics-comparison` | ClickHouse vs RisingWave vs PGD vs WHPG side-by-side. |
| `core-banking-simulator` | Pre-BFSI core banking demo (precursor of BFSI). |
| `data-engineering` | Fraud-detection-style ETL pipeline (Grafana, Jupyter, Langflow). |
| `paradedb` | BM25 + pgvector hybrid search. |
| `pg-clickhouse` | Postgres → ClickHouse CDC (PeerDB). |
| `real-time-analytics` | RisingWave + PG integration. |
| `redpanda-vs-kafka-benchmark` | Kafka vs Redpanda E2E benchmark. |
| `sovereign-data-tier` | Postgres WAL archive + base backup to MinIO. |
| `unified-analytics-intelligence` | Metabase unified view across PGD + RisingWave. |
| `cdc-pg-to-rw-pg` | Postgres CDC → RisingWave → Postgres. |
| `events-api-to-rw-pg` | HTTP event API → RisingWave → Postgres. |
| `kafka-to-rw-pg` | Kafka → RisingWave → Postgres. |
| `webhook-to-rw-pg` | Webhook → RisingWave → Postgres. |
| `k8s-playground` | Local k3s sandbox. |

Plugins (single-service compose for ad-hoc work) live under `/plugins/`: clickhouse, grafana, jupyter, k3s, kafka, kafka-ui, langflow, minio, paradedb, postgres, redpanda, redpanda-console, risingwave.

---

## Adding a New Stack

```bash
cp -r stacks/_template stacks/my-stack
# Edit:
#   stacks/my-stack/docker-compose.yaml   ← services, ports, env
#   stacks/my-stack/stack.yaml            ← name, deploy_targets, pipelines, flow
# Click "Reload" in the UI — no agent restart required.
```

**To surface it on the Industry tab**: add an entry to `CARD_DEFS` in `engine/agent/app.py` whose `key` matches your folder name.

**To add a new use case to an existing stack**: edit `stacks/<stack>/stack.yaml` under `pipelines:` — name it, list shell commands as steps, optionally add a `flow:` block:

```yaml
flow: |
  **What this use case shows:** one-liner overview always visible.
  1. **Step name** — what this step does.
  2. **Next step** — and so on.
```

The first paragraph stays visible; the numbered list collapses behind a "Show steps" disclosure.

---

## Directory Structure

```
databricks-in-a-box/
  engine/
    agent/                # Chat agent (FastAPI + Claude / Bedrock)
      translators/        # Per-target deploy logic
        laptop.py         # Docker Desktop + Colima preflight
        northflank.py     # NF API translation
    synthdb/              # Synthetic data service (SDV), on-demand
  stacks/                 # 16 folders (1 active on Industry tab)
  plugins/                # 13 standalone services
  scripts/                # clean-ports.sh, cross-runtime-clean.sh, list-stack-ports.py
  Makefile
  bootstrap.sh
```

---

## Documentation

- [Security](SECURITY.md) — defaults, credentials, hardening
- [CI/CD Guidelines](CI.md) — maintaining pipelines
- [Contributing](CONTRIBUTING.md) — contributor guide
- [Data engineering — getting started](stacks/data-engineering/GETTING_STARTED.md) — fraud-detection-style demo
- [Data engineering — developer notes](stacks/data-engineering/README.md)

## License

Copyright (c) 2025-2026 EnterpriseDB Corporation. All rights reserved.

See [LICENSE](LICENSE) for details.

---

## Author

- **Raghavendra Rao** - Pioneer Team - Conceived the EDB Postgres® AI Blueprints framework and drove its initial design.

## Committers

- **Pranish Kumar** - Project coordination, issue triage, and progress tracking
- **Vibhor Kumar** - Carrying the framework forward on AWS, and LeaseWeb
- **Maneesh Goyal** - Contributor - Data Engineering Integration
- **Rahul Saha** - Contributor - Sovereign Data Tier
- **Ajit Gadge** - Contributor - ParadeDB Hybrid Search
- **Gianni Ciolli** - Contributor - Deploy on AWS with TPA, release infrastructure
