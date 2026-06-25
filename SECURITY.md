# Security

*For demonstration purposes only.*

This document describes the security measures implemented in EDB Postgres® AI Blueprints.

## Security Features

### 1. Command Execution Protection

The chat agent does not enforce a command whitelist. Its `run_command` tool (`engine/agent/agent.py`) passes the string it receives straight to the shell via `subprocess.run(cmd, shell=True)`. The tool description suggests it is limited to Docker commands, but that is advisory only and does not constrain what runs — anyone who can reach the agent can run arbitrary shell commands on the host.

This is acceptable only under the trusted-host assumption in the Threat Model: run it on your own machine, on a trusted network or behind a VPN, never on a shared or internet-facing host.

**Mitigation:** restrict network access to the agent. For genuine enforcement, replace the free-form `run_command` tool with structured deploy/destroy/logs actions that build the Docker command server-side, rather than filtering shell
strings after the fact.

### 2. API Authentication

All \/api/*` endpoints except `/api/health` are protected with API key authentication:

| Endpoint | Protection | Description |
|----------|------------|-------------|
| `/api/exit` | API Key Required | Stops all containers and shuts down agent |
| `/api/reset` | API Key Required | Resets chat history |
| `/api/chat` | API Key Required | Chat functionality |
| `/api/stacks` | API Key Required | List available stacks |
| `/api/health` | Public | Health check (the only unauthenticated route) |

The API key is:
- Auto-generated on startup (printed to console)
- Or set via `AGENT_API_KEY` environment variable

**Scope limit:** this guard is HTTP-only and does not cover the WebSocket endpoints. `/ws/terminal` opens a shell into containers without an API key (it validates the container name, not the caller). The shipped UI also does not send an `Authorization` header on its `/api/*` calls, so the key must be supplied by a reverse proxy or run configuration.

### 3. Input Validation

All user inputs are validated using Pydantic models:

- Message length: 1-10,000 characters
- Required fields enforced
- Type validation on all inputs

### 4. CORS Protection

The agent application (`engine/agent/app.py`) does not register any CORS middleware, and the SynthDB API (`engine/synthdb/api.py`) is configured with `allow_origins=["*"]`, which accepts requests from any origin.

In the shipped setup this carries little practical risk: browsers never call the SynthDB service directly. UI requests go to `/api/synthdb/*` on the agent, which proxies to SynthDB server-side, so the permissive policy is not reachable from a browser. It should still be tightened as a matter of hygiene.

**Mitigation:** keep all services bound to localhost. To enforce origin restrictions, add `CORSMiddleware` to `app.py` and replace the wildcard in `synthdb/api.py` with an explicit allow-list (for example, `http://127.0.0.1:4000`).

### 5. Network Security

Database ports are bound to localhost only:

| Service | Port Binding | Access |
|---------|--------------|--------|
| PostgreSQL | `127.0.0.1:5432` | Local only |
| ClickHouse | `127.0.0.1:8123` | Local only |
| Catalog DB | `127.0.0.1:9901` | Local only |

UI ports (3000, 4000, 7860, 8888) are exposed for browser access.

### 6. Credential Protection

- Passwords are NOT included in LLM system prompts
- Credentials are stored in `stack.yaml` files (not transmitted to AI)
- Default credentials are for development only

### 7. Container Security

- Logging limits prevent disk exhaustion (10MB max, 3 files)
- Healthchecks monitor service availability
- Restart policies ensure recovery from failures
- GRANT_SUDO disabled in Jupyter containers

## Default Credentials

**These are development-only credentials. Change them for any shared environment.**

### Customizing Passwords

Each stack supports password configuration via environment variables:

```bash
cd stacks/data-engineering
cp .env.example .env
# Edit .env with your custom passwords
docker compose up -d
```

### Default Values (if no .env file)

| Service | Username | Password | Environment Variable |
|---------|----------|----------|---------------------|
| PostgreSQL | admin | admin123 | `POSTGRES_PASSWORD` |
| PostgreSQL | aidev | aidev123 | `POSTGRES_PASSWORD_AIDEV` |
| ClickHouse | admin | admin123 | `CLICKHOUSE_PASSWORD` |
| ClickHouse | default | (no password) | - |
| MinIO | _peerdb_minioadmin | _peerdb_minioadmin | `MINIO_SECRET_KEY` |
| Catalog DB | postgres | postgres | `CATALOG_PASSWORD` |
| Jupyter | N/A | Token: databox | `JUPYTER_TOKEN` |
| Grafana | admin | admin123 | `GRAFANA_ADMIN_PASSWORD` |
| Langflow | admin | admin123 | `LANGFLOW_SUPERUSER_PASSWORD` |

**Note:** ClickHouse passwords are defined in `volumes/clickhouse/etc/clickhouse-server/users.d/users.xml`.
To change them, edit this file directly before starting the stack.

## Security Best Practices

### For Development

1. Run on localhost only
2. Use auto-generated API keys
3. Don't expose ports to external networks
4. Regularly update Docker images

### For Shared Environments

1. Change all default credentials
2. Set `AGENT_API_KEY` environment variable
3. Use a reverse proxy with TLS
4. Implement network segmentation
5. Enable Docker content trust

### For Production (Not Recommended)

This framework is not designed for production use. In order to make it
production ready, some improvements are required, including the
following:

1. Remove or secure the chat agent
2. Implement proper IAM/authentication
3. Use secrets management (Vault, AWS Secrets Manager)
4. Enable audit logging
5. Perform security assessment
6. Consider using managed services instead

## Reporting Security Issues

If you discover a security vulnerability:

1. **Do NOT** open a public issue
2. Email the maintainers directly
3. Include detailed steps to reproduce
4. Allow time for a fix before disclosure

## Security Checklist for Contributors

When adding new stacks or features:

- [ ] No hardcoded production credentials
- [ ] Database ports bound to localhost
- [ ] Input validation on all user inputs
- [ ] No arbitrary command execution
- [ ] Healthchecks for all services
- [ ] Logging configuration included
- [ ] Documentation updated

## Vulnerability Scanning

CI/CD includes automated security scanning:

- **Trivy**: Fails on CRITICAL vulnerabilities
- **TruffleHog**: Checks for exposed secrets
- **Lychee**: Validates documentation links

## Version History

### Version 0.1

Released on 25 June 2026.

  - Initial security implementation
  - Command whitelist
  - API authentication
  - input validation
