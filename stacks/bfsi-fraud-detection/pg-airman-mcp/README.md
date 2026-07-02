# PG Airman MCP - Custom Build

*For demonstration purposes only.*

This directory contains a custom Dockerfile that builds pg-airman-mcp from source with a fix for a missing PostgreSQL client library in the official image.

## Issue Fixed

### Missing PostgreSQL Client Library

The official `enterprisedb/pg-airman-mcp` image is missing `libpq.so.5`, causing:
```
ImportError: no pq wrapper available.
Attempts made:
- couldn't import psycopg 'c' implementation: libpq.so.5: cannot open shared object file
```

**Solution**: Install `libpq5` and `libpq-dev` packages in the runtime image.

> **Note**: A previous version of this build patched the source to disable DNS rebinding
> protection. That patch is no longer needed — upstream `pg-airman-mcp` now handles this
> natively.

## Build

The image is built automatically by docker-compose:

```bash
cd stacks/data-engineering
docker compose build pg-airman-mcp
```

Or build manually:

```bash
cd stacks/bfsi-fraud-detection/pg-airman-mcp
docker build -t pg-airman-mcp .
```

## Configuration

### Environment Variables

- `DATABASE_URI` - PostgreSQL connection string (required)
- `ACCESS_MODE` - `restricted` (read-only) or `unrestricted` (read-write)
- `ALLOW_COMMENT_IN_RESTRICTED` - Allow COMMENT ON in restricted mode (`true`/`false`)

### Command Format

```yaml
command: ["pg-airman-mcp", "--access-mode=restricted", "--transport=sse", "--sse-port=8200"]
```

**Important**:
- Use `--sse-port` not `--port` for SSE transport
- Include `pg-airman-mcp` as the first argument

### Healthcheck

Since SSE transport doesn't expose a `/health` endpoint, we check if port 8200 is listening:

```yaml
healthcheck:
  test: ["CMD-SHELL", "netstat -an | grep 8200 || exit 1"]
```

## Usage

### Check Status

```bash
# View logs
docker logs pg-airman-mcp

# Check container status
docker compose ps pg-airman-mcp

# Test SSE endpoint
curl http://localhost:8200/sse
```

## SSE Endpoint

The MCP server is available at:
- **URL**: http://localhost:8200/sse
- **Transport**: Server-Sent Events (SSE)
- **Protocol**: JSON-RPC 2.0

## Resources

- [Official pg-airman-mcp repo](https://github.com/EnterpriseDB/pg-airman-mcp)
- [MCP Python SDK docs](https://github.com/modelcontextprotocol/python-sdk)
