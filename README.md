# Apple Health MCP

Single-user service that receives Health Auto Export JSON, stores the original payload and normalized records in SQLite, and exposes read-only queries over MCP Streamable HTTP.

Public deployment paths:

- Upload: `POST /apple-health/ingest`
- MCP: `/apple-health/mcp`
- Health check: `GET /apple-health/health`

Upload and MCP use separate bearer tokens. The application never logs request bodies or authorization values.

## Health Auto Export settings

- Automation type: REST API
- URL: `https://apps.supxh.xin/apple-health/ingest`
- Header key: `Authorization`
- Header value: `Bearer <INGEST_TOKEN>`
- Export format: JSON
- Export version: 2
- Date range: Since Last Sync
- Batch requests: enabled
- Timeout: 60 seconds

`Content-Type` is set automatically by Health Auto Export. Retrieve the upload token directly on the server with:

```bash
sudo cat /etc/apple-health-mcp/ingest-token
```

## MCP connection

- URL: `https://apps.supxh.xin/apple-health/mcp`
- Header: `Authorization: Bearer <MCP_TOKEN>`
- Transport: Streamable HTTP

Retrieve the separate MCP token directly on the server with:

```bash
sudo cat /etc/apple-health-mcp/mcp-token
```

The server exposes four read-only tools:

- `AppleHealth:list_available_data`
- `AppleHealth:query_health_metrics`
- `AppleHealth:query_health_events`
- `AppleHealth:get_health_overview`

For lossless field access, call `AppleHealth:query_health_metrics` with `aggregation="raw"` and
`output_format="detailed"`. The response then includes the complete original exported sample alongside normalized
columns. Overlapping exports are reconciled by metric/source/time bucket so the latest value is queried while every
original request remains retained for audit and future reprocessing.

## Operations

```bash
systemctl status apple-health-mcp.service
curl https://apps.supxh.xin/apple-health/health
journalctl -u apple-health-mcp.service -n 100 --no-pager
```

Production data is stored at `/var/lib/apple-health-mcp/health.db`. Back up the database and its WAL files together, or use SQLite's online backup API while the service is running.
