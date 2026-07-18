from __future__ import annotations

import hmac
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .storage import (
    health_overview as storage_health_overview,
    ingestion_status,
    initialize_database,
    list_available_data as storage_list_available_data,
    query_health_events as storage_query_health_events,
    query_health_metrics as storage_query_health_metrics,
    store_ingestion,
)


@dataclass(frozen=True)
class Settings:
    db_path: Path
    ingest_token: str
    mcp_token: str
    max_body_bytes: int


def _load_secret(value_name: str, file_name: str) -> str:
    direct = os.getenv(value_name)
    if direct:
        return direct.strip()
    path_value = os.getenv(file_name)
    if not path_value:
        raise RuntimeError(f"Set {value_name} or {file_name}")
    value = Path(path_value).read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Secret file configured by {file_name} is empty")
    return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings(
        db_path=Path(os.getenv("APPLE_HEALTH_DB_PATH", "/var/lib/apple-health-mcp/health.db")),
        ingest_token=_load_secret("APPLE_HEALTH_INGEST_TOKEN", "APPLE_HEALTH_INGEST_TOKEN_FILE"),
        mcp_token=_load_secret("APPLE_HEALTH_MCP_TOKEN", "APPLE_HEALTH_MCP_TOKEN_FILE"),
        max_body_bytes=int(os.getenv("APPLE_HEALTH_MAX_BODY_BYTES", str(128 * 1024 * 1024))),
    )
    if len(settings.ingest_token) < 32 or len(settings.mcp_token) < 32:
        raise RuntimeError("Bearer tokens must contain at least 32 characters")
    return settings


def _bearer_token(headers: list[tuple[bytes, bytes]]) -> str | None:
    for key, value in headers:
        if key.lower() == b"authorization":
            decoded = value.decode("latin-1")
            scheme, separator, token = decoded.partition(" ")
            if separator and scheme.lower() == "bearer":
                return token.strip()
    return None


def _authorized(received: str | None, expected: str) -> bool:
    return bool(received) and hmac.compare_digest(received, expected)


class MCPBearerAuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and str(scope.get("path", "")).startswith("/mcp"):
            token = _bearer_token(scope.get("headers", []))
            if not _authorized(token, get_settings().mcp_token):
                response = JSONResponse(
                    {"error": "unauthorized", "resolution": "Send Authorization: Bearer <MCP_TOKEN>"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


mcp = FastMCP(
    name="AppleHealth",
    instructions=(
        "Read-only access to the owner's Apple Health exports. 当用户询问如何使用、有哪些功能或工具选择不明确时，"
        "先调用 guide，并立即按其建议继续调用数据工具。Use list_available_data before querying unknown metric or "
        "event names. Treat results as personal health records, minimize disclosure, and never present them as medical "
        "diagnosis. Aggregates are mechanical summaries of exported samples."
    ),
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "apps.supxh.xin",
            "apps.supxh.xin:*",
            "172.17.0.1",
            "172.17.0.1:*",
            "127.0.0.1",
            "127.0.0.1:*",
            "localhost",
            "localhost:*",
        ],
        allowed_origins=["https://apps.supxh.xin"],
    ),
)


@mcp.tool(name="guide")
def guide() -> dict[str, Any]:
    """Return the AppleHealth MCP usage and tool-selection guide in Chinese and English.

    Use this as the first call when the user asks how to use AppleHealth, what health information is available, or
    which tool should answer a request. This tool accepts no inputs and returns exact fully qualified tool names,
    selection rules, valid argument examples, and a recommended call sequence. It does not return health values;
    after reading the guide, immediately call the recommended data tool instead of answering from the guide alone.
    """
    return {
        "server": "AppleHealth",
        "purpose": "读取已上传到服务器的个人 Apple 健康数据；所有数据工具均为只读。",
        "required_behavior": (
            "根据用户问题选择下方工具并继续调用。guide 只负责导航，不能代替实际健康数据查询。"
        ),
        "recommended_sequence": [
            {
                "when": "首次使用、用户问有哪些数据，或不知道准确指标名",
                "tool": "AppleHealth:list_available_data",
                "arguments": {"output_format": "concise"},
            },
            {
                "when": "用户要求最近几天的总体健康总结",
                "tool": "AppleHealth:get_health_overview",
                "arguments": {"days": 7},
            },
            {
                "when": "用户查询步数、心率、睡眠、HRV、血氧、能量等时间序列",
                "tool": "AppleHealth:query_health_metrics",
                "arguments_example": {
                    "metric_names": ["heart_rate"],
                    "start_time": "2026-07-16",
                    "end_time": "2026-07-18",
                    "aggregation": "hour",
                    "limit": 200,
                    "output_format": "concise",
                },
            },
            {
                "when": "用户需要睡眠原始字段、入床/离床时间或完整导出对象",
                "tool": "AppleHealth:query_health_metrics",
                "arguments_example": {
                    "metric_names": ["sleep_analysis"],
                    "aggregation": "raw",
                    "limit": 50,
                    "output_format": "detailed",
                },
            },
            {
                "when": "用户查询锻炼、症状、ECG、药物或其他非指标事件",
                "tool": "AppleHealth:query_health_events",
                "arguments_example": {
                    "categories": ["workouts"],
                    "limit": 100,
                    "output_format": "concise",
                },
            },
        ],
        "important_rules": [
            "工具名称使用完整命名，例如 AppleHealth:get_health_overview。",
            "日期使用 ISO-8601，例如 2026-07-18 或 2026-07-18T10:00:00+02:00。",
            "不知道 metric_names 或 categories 时先调用 AppleHealth:list_available_data。",
            "output_format=detailed 仅能与 aggregation=raw 一起用于健康指标查询。",
            "没有返回记录通常表示客户端尚未上传该类别，不代表服务器连接失败。",
            "健康数据仅用于信息总结，不应作为医疗诊断。",
        ],
    }


@mcp.tool(name="list_available_data")
def list_available_data(output_format: Literal["concise", "detailed"] = "concise") -> dict[str, Any]:
    """List health metric names, event categories, coverage, and ingestion status.

    Use this before other AppleHealth tools when the exact exported metric or category names are unknown.
    `output_format="concise"` returns names, units, counts, and latest timestamps; `detailed` also returns first
    timestamps. Returns empty lists when the phone has not uploaded data yet. Errors identify database problems and
    are safe to retry after the service is healthy.
    """
    return storage_list_available_data(get_settings().db_path, output_format)


@mcp.tool(name="query_health_metrics")
def query_health_metrics(
    metric_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    aggregation: Literal["raw", "hour", "day"] = "day",
    limit: int = 200,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Query numeric or categorical Apple Health metric samples over time.

    Use for steps, heart rate, HRV, sleep stages, energy, weight, oxygen saturation, and other entries reported by
    `list_available_data`. Metric names are exact exported names; omit them to query all metrics. Times accept ISO-8601
    dates or timestamps and default to the last 7 days. `raw` returns samples, while `hour` and `day` return mechanical
    aggregates. Set `output_format="detailed"` with `aggregation="raw"` to include the complete original exported sample,
    including fields not promoted into standard columns. `limit` is 1-1000. Returns the resolved query window plus
    records. Validation errors state the accepted format and can be corrected without changing other parameters.
    """
    return storage_query_health_metrics(
        get_settings().db_path,
        metric_names=metric_names,
        start_time=start_time,
        end_time=end_time,
        aggregation=aggregation,
        limit=limit,
        output_format=output_format,
    )


@mcp.tool(name="query_health_events")
def query_health_events(
    categories: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Query non-metric Apple Health records such as workouts, symptoms, ECGs, medications, or cycle events.

    Use event category names returned by `list_available_data`; omit categories to query all. Times accept ISO-8601
    dates or timestamps and default to the last 30 days. `concise` returns identifying fields; `detailed` also returns
    the complete exported event object. `limit` is 1-500. Returns resolved filters and newest events first. Validation
    errors specify the parameter that must be corrected.
    """
    return storage_query_health_events(
        get_settings().db_path,
        categories=categories,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        output_format=output_format,
    )


@mcp.tool(name="get_health_overview")
def get_health_overview(days: int = 7) -> dict[str, Any]:
    """Return a compact cross-metric overview for a recent period.

    Use for broad questions such as "summarize my last week" before requesting specific time series. `days` must be
    1-365 and defaults to 7. Returns per-metric count, average, min, max, total, latest timestamp, and event counts.
    Values are mechanical summaries whose meaning depends on each metric and unit; they are not medical advice.
    """
    return storage_health_overview(get_settings().db_path, days=days)


async def health(request: Request) -> JSONResponse:
    del request
    try:
        status = ingestion_status(get_settings().db_path)
        return JSONResponse({"ok": True, "service": "apple-health-mcp", **status})
    except Exception:
        return JSONResponse({"ok": False, "service": "apple-health-mcp"}, status_code=503)


async def ingest(request: Request) -> JSONResponse:
    token = _bearer_token(list(request.scope.get("headers", [])))
    if not _authorized(token, get_settings().ingest_token):
        return JSONResponse(
            {"error": "unauthorized", "resolution": "Send Authorization: Bearer <INGEST_TOKEN>"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("application/json"):
        return JSONResponse(
            {"error": "invalid_content_type", "expected": "application/json", "received": content_type},
            status_code=415,
        )

    size = 0
    spool = SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    try:
        async for chunk in request.stream():
            size += len(chunk)
            if size > get_settings().max_body_bytes:
                return JSONResponse(
                    {
                        "error": "payload_too_large",
                        "max_bytes": get_settings().max_body_bytes,
                        "resolution": "Enable JSON batch requests or reduce the export date range and granularity.",
                    },
                    status_code=413,
                )
            spool.write(chunk)
        spool.seek(0)
        raw_bytes = spool.read()
    finally:
        spool.close()

    try:
        raw_json = raw_bytes.decode("utf-8")
        payload = json.loads(raw_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return JSONResponse(
            {"error": "invalid_json", "resolution": "Export as JSON version 2.", "detail": str(error)},
            status_code=400,
        )

    safe_headers = {
        key.lower(): value
        for key, value in request.headers.items()
        if key.lower()
        in {
            "session-id",
            "automation-name",
            "automation-id",
            "automation-aggregation",
            "automation-period",
        }
    }
    try:
        result = store_ingestion(get_settings().db_path, payload, raw_json, safe_headers)
    except Exception:
        return JSONResponse(
            {
                "error": "storage_failed",
                "resolution": "Retry later; the same session and body are safely deduplicated.",
            },
            status_code=503,
        )
    return JSONResponse(result, status_code=200)


mcp_http_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(app: Starlette):
    del app
    settings = get_settings()
    initialize_database(settings.db_path)
    async with mcp.session_manager.run():
        yield


core_app = Starlette(
    debug=False,
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/ingest", ingest, methods=["POST"]),
        Mount("/", app=mcp_http_app),
    ],
    lifespan=lifespan,
)

app = MCPBearerAuthMiddleware(core_app)
