# Security

*For demonstration purposes only.*

This document describes the security measures implemented in EDB Postgres® AI Blueprints.

## Security Features

### 1. Command Execution Protection

The chat agent restricts command execution to a whitelist of safe Docker commands:

- `docker compose` (up, down, etc.)
- `docker ps`
- `docker logs`
- `docker exec`
- `docker stats`
- `docker inspect`

Arbitrary shell commands are blocked to prevent command injection attacks.

### 2. API Authentication

Critical endpoints are protected with API key authentication:

| Endpoint | Protection | Description |
|----------|------------|-------------|
| `/api/exit` | API Key Required | Stops all containers and shuts down agent |
| `/api/reset` | API Key Required | Resets chat history |
| `/api/chat` | Public | Chat functionality |
| `/api/stacks` | Public | List available stacks |

The API key is:
- Auto-generated on startup (printed to console)
- Or set via `AGENT_API_KEY` environment variable

### 3. Input Validation

All user inputs are validated using Pydantic models:

- Message length: 1-10,000 characters
- Required fields enforced
- Type validation on all inputs

### 4. CORS Protection

Cross-Origin Resource Sharing is restricted to localhost:

- `http://localhost:4000`
- `http://127.0.0.1:4000`
- `http://localhost:3000` (PeerDB UI)
- `http://127.0.0.1:3000`

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
