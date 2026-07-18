# Apple Health MCP

Personal health semantic layer for conversational AI. The service receives Health Auto Export JSON, preserves the raw
history, cleans/deduplicates/normalizes it, computes local-day and personal-baseline summaries, exposes stable health
HTTP APIs, and presents those APIs as thin read-only MCP tools.

The default interface is question-oriented and compact. Raw metric/event queries are fallback tools for upload
debugging and explicit record inspection, not the normal conversational path.

Public deployment paths:

- Upload: `POST /apple-health/ingest`
- MCP: `/apple-health/mcp`
- Semantic API: `/apple-health/api/v1/health/*`
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

The server exposes thirteen read-only tools.

Primary scene tools:

- `AppleHealth:guide`
- `AppleHealth:get_sleep_summary`
- `AppleHealth:get_sleep_trend`
- `AppleHealth:get_recovery_snapshot`
- `AppleHealth:get_daily_activity`
- `AppleHealth:get_activity_trend`
- `AppleHealth:list_workouts`
- `AppleHealth:get_workout_detail`
- `AppleHealth:compare_workouts`
- `AppleHealth:get_health_overview`

Fallback/debug tools:

- `AppleHealth:list_available_data`
- `AppleHealth:query_health_metrics`
- `AppleHealth:query_health_events`

Scene tools return a common envelope with `status`, requested/actual coverage, semantic `summary`, personal-baseline
comparison, bounded highlights, and data quality. Default timestamps use `Asia/Shanghai`; missing values remain `null`.

## Semantic HTTP API

All semantic endpoints use the MCP read token:

```text
Authorization: Bearer <MCP_TOKEN>
```

- `GET /apple-health/api/v1/health/capabilities`
- `GET /apple-health/api/v1/health/sleep/summary?date=2026-07-18`
- `GET /apple-health/api/v1/health/sleep/trend?nights=7`
- `GET /apple-health/api/v1/health/recovery/snapshot?date=2026-07-18`
- `GET /apple-health/api/v1/health/activity/daily?date=2026-07-18`
- `GET /apple-health/api/v1/health/activity/trend?days=7`
- `GET /apple-health/api/v1/health/workouts?days=7`
- `GET /apple-health/api/v1/health/workouts/detail?workout_id=<WORKOUT_ID>`
- `GET /apple-health/api/v1/health/workouts/compare?workout_id=<WORKOUT_ID>&baseline_count=5`
- `GET /apple-health/api/v1/health/overview?days=7`

Workout scene APIs return compact session summaries and sensor coverage without exposing full route coordinates or raw
heart-rate samples. Cycle-tracking events are detected in capabilities and remain available through the raw-event
fallback until a dedicated conversational scene contract is defined.

For lossless debugging, call `AppleHealth:query_health_metrics` with `aggregation="raw"` and
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
