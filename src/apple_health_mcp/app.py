from __future__ import annotations

import hmac
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .semantic import (
    DEFAULT_TIMEZONE,
    capabilities,
    compare_workouts as semantic_compare_workouts,
    get_activity_trend as semantic_get_activity_trend,
    get_daily_activity as semantic_get_daily_activity,
    get_recovery_snapshot as semantic_get_recovery_snapshot,
    get_sleep_summary as semantic_get_sleep_summary,
    get_sleep_trend as semantic_get_sleep_trend,
    get_workout_detail as semantic_get_workout_detail,
    list_available_data_semantic,
    list_workouts as semantic_list_workouts,
    localize_fallback_metric_result,
    query_sleep_fallback,
    semantic_health_overview,
)
from .storage import (
    ingestion_status,
    initialize_database,
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
        path = str(scope.get("path", ""))
        if scope["type"] == "http" and (path.startswith("/mcp") or path.startswith("/api/")):
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
        "Personal health semantic layer for conversational AI. 普通睡眠、恢复和活动问题必须优先调用专用场景工具，"
        "一次通常只调用1-2个工具。guide用于工具路由。list_available_data/query_health_metrics/query_health_events"
        "仅作为上传排查和原始数据调试的fallback。服务器负责时区、清洗、聚合、覆盖率和个人基线机械比较；"
        "AI负责结合对话解释，禁止医学诊断。"
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
        "purpose": "为对话AI提供已经清洗、聚合并带个人基线的健康语义上下文；所有数据工具均为只读。",
        "required_behavior": (
            "根据用户问题选择下方工具并继续调用。guide 只负责导航，不能代替实际健康数据查询。"
        ),
        "recommended_sequence": [
            {
                "when": "用户问昨晚睡得怎么样",
                "tool": "AppleHealth:get_sleep_summary",
                "arguments_example": {"date": "2026-07-18"},
            },
            {
                "when": "用户问最近一周/两周/一月睡眠趋势",
                "tool": "AppleHealth:get_sleep_trend",
                "arguments_example": {"nights": 7},
            },
            {
                "when": "用户问疲劳、晨练前恢复状态或今天是否适合高强度运动",
                "tool": "AppleHealth:get_recovery_snapshot",
                "arguments_example": {"date": "2026-07-18"},
            },
            {
                "when": "用户问今天的步数、活动量或晨练情况",
                "tool": "AppleHealth:get_daily_activity",
                "arguments_example": {"date": "2026-07-18"},
            },
            {
                "when": "用户问最近活动趋势",
                "tool": "AppleHealth:get_activity_trend",
                "arguments_example": {"days": 7},
            },
            {
                "when": "用户问最近做过哪些锻炼、跑步、骑行或健身",
                "tool": "AppleHealth:list_workouts",
                "arguments_example": {"days": 7},
            },
            {
                "when": "用户问某一次锻炼的距离、心率、能量、速度或路线是否存在",
                "tool": "AppleHealth:get_workout_detail",
                "arguments_example": {"workout_id": "先从AppleHealth:list_workouts取得"},
            },
            {
                "when": "用户问这次锻炼相比上次或个人同类历史表现如何",
                "tool": "AppleHealth:compare_workouts",
                "arguments_example": {"workout_id": "先从AppleHealth:list_workouts取得", "baseline_count": 5},
            },
            {
                "when": "用户要求最近7/14/30天的综合健康概览",
                "tool": "AppleHealth:get_health_overview",
                "arguments_example": {"days": 7},
            },
            {
                "when": "专用工具无法回答、精确指标名未知、上传排查或明确要求原始记录",
                "tool": "AppleHealth:list_available_data / query_health_metrics / query_health_events",
                "arguments_example": {"output_format": "concise"},
            },
        ],
        "important_rules": [
            "普通问题优先专用场景工具，不要先拉list再查询多个底层指标。",
            "默认时区Asia/Shanghai，日期使用YYYY-MM-DD。",
            "场景工具默认concise；detailed仍是有界语义数据，不含原始payload。",
            "只有fallback raw+detailed用于排查或用户明确要求原始记录。",
            "缺失值是null，不得解释为0。",
            "fetch.complete表示查询未被截断；coverage.complete表示请求日/晚是否都有数据。",
            "valid_days/valid_nights不足时不得宣称长期趋势。",
            "健康数据仅用于信息总结，不应作为医疗诊断。",
        ],
        "capabilities": capabilities(get_settings().db_path),
    }


@mcp.tool(name="get_sleep_summary")
def get_sleep_summary(
    date: str,
    timezone: str = DEFAULT_TIMEZONE,
    include_recovery_metrics: bool = True,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Return one semantic sleep object for the night assigned to its wake date.

    Use directly for questions such as "昨晚睡得怎么样". `date` is YYYY-MM-DD in `timezone` (default
    Asia/Shanghai); all other inputs are optional. The server validates reported boundaries, returns in-bed duration
    only when explicitly trustworthy, suppresses interval recovery joins for low-confidence boundaries, and compares
    with prior personal nights. Concise is bounded; detailed adds quality/capability metadata but never raw payloads.
    """
    return semantic_get_sleep_summary(
        get_settings().db_path, date, timezone, include_recovery_metrics, output_format
    )


@mcp.tool(name="get_sleep_trend")
def get_sleep_trend(
    nights: Literal[7, 14, 30] = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    """Return nightly sleep summaries and a bounded trend for the latest 7, 14, or 30 wake dates.

    Use for recent sleep-trend questions. Returns valid_nights, missing_nights, actual coverage, averages/medians, and
    trend strength. Fewer than three valid nights always returns insufficient_data and must not be described as a trend.
    """
    return semantic_get_sleep_trend(get_settings().db_path, nights, timezone, output_format, end_date)


@mcp.tool(name="get_recovery_snapshot")
def get_recovery_snapshot(
    date: str | None = None,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Return a recovery context for one wake date using sleep, heart rate, HRV, breathing, oxygen, and wrist temperature.

    Use for fatigue, morning training, and recovery questions. Omit `date` to use the latest uploaded sleep night.
    Each metric states its value, sample count, query window, and confidence. Fewer than three baseline nights returns
    insufficient_data without a recovery judgment. The server never produces a medical diagnosis or readiness score.
    """
    return semantic_get_recovery_snapshot(get_settings().db_path, date, timezone, output_format)


@mcp.tool(name="get_daily_activity")
def get_daily_activity(
    date: str,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Return one local day's semantic activity summary and personal-baseline comparison.

    Use for today's or a specified day's steps, distance, active energy, exercise minutes, stand hours, daylight, and
    uploaded workout summary. `date` is YYYY-MM-DD. `period.is_closed=false` and `may_still_update=true` identify an
    unfinished current day. Missing values remain null rather than being treated as zero.
    """
    return semantic_get_daily_activity(get_settings().db_path, date, timezone, output_format)


@mcp.tool(name="get_activity_trend")
def get_activity_trend(
    days: Literal[7, 14, 30] = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    """Return semantic activity coverage and trends for the latest 7, 14, or 30 local days.

    Use for recent activity-volume questions. Returns valid_days, missing_days, bounded daily averages/totals, and a
    previous-equal-period personal baseline. A complete database fetch can still have incomplete day coverage.
    """
    return semantic_get_activity_trend(get_settings().db_path, days, timezone, output_format, end_date)


@mcp.tool(name="list_workouts")
def list_workouts(
    days: Literal[7, 14, 30] = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
    workout_type: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Return compact workout sessions for the latest 7, 14, or 30 local days.

    Use when the user asks which workouts, runs, rides, walks, or gym sessions occurred recently. Results include stable
    `workout_id` values for follow-up calls, global duration/energy totals, and independent per-type distance/speed/pace
    statistics. Anomalous records are retained and flagged but excluded from same-type baselines. `workout_type` is an
    optional exact type returned by this tool; `limit` defaults to 20. Raw routes/sensor samples are never returned.
    """
    return semantic_list_workouts(
        get_settings().db_path, days, timezone, output_format, end_date, workout_type, limit
    )


@mcp.tool(name="get_workout_detail")
def get_workout_detail(
    workout_id: str,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Return one cleaned workout session with a same-type personal-baseline comparison.

    Use after `AppleHealth:list_workouts` when the user asks about one specific workout. `workout_id` must come from that
    tool. Returns local start/end time, duration, distance, energy, heart-rate range, speed/pace, elevation, sensor
    coverage, missing fields, and comparison with up to five prior workouts of the same type. Even detailed output is
    bounded and does not expose route coordinates or full heart-rate samples; use the raw fallback only for debugging.
    """
    return semantic_get_workout_detail(get_settings().db_path, workout_id, timezone, output_format)


@mcp.tool(name="compare_workouts")
def compare_workouts(
    workout_id: str,
    comparison_workout_id: str | None = None,
    baseline_count: Literal[3, 5, 10] = 5,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    """Compare one workout with another workout or with the user's recent same-type baseline.

    Use for questions such as "这次骑行比上次怎样" or "这次跑步表现怎样". Obtain IDs from
    `AppleHealth:list_workouts`. When `comparison_workout_id` is provided, the tool compares those two sessions;
    otherwise it compares `workout_id` with up to `baseline_count` prior workouts of the same type. Returns mechanical
    absolute/percentage differences with coverage strength, not a medical or readiness judgment. `baseline_count`
    defaults to 5 and `output_format` defaults to concise.
    """
    return semantic_compare_workouts(
        get_settings().db_path,
        workout_id,
        comparison_workout_id,
        baseline_count,
        timezone,
        output_format,
    )


@mcp.tool(name="list_available_data")
def list_available_data(
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    """List health metric names, event categories, coverage, and ingestion status.

    Fallback only: use when a specialized scene tool cannot answer, the exact exported metric/category names are
    unknown, or an upload needs debugging. Do not call this before ordinary sleep, recovery, or activity questions.
    `output_format="concise"` returns names, units, counts, and latest timestamps; `detailed` also returns first
    timestamps. Returns empty lists when the phone has not uploaded data yet. Errors identify database problems and
    are safe to retry after the service is healthy.
    """
    return list_available_data_semantic(get_settings().db_path, output_format, timezone)


@mcp.tool(name="query_health_metrics")
def query_health_metrics(
    metric_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    aggregation: Literal["raw", "hour", "day"] = "day",
    limit: int = 200,
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    """Query numeric or categorical Apple Health metric samples over time.

    Fallback only: use for exact metric debugging or explicit raw record requests after specialized tools cannot answer.
    Metric names are exact exported names; omit them to query all metrics. Times accept ISO-8601
    dates or timestamps and default to the last 7 days. `raw` returns samples, while `hour` and `day` return mechanical
    aggregates. Set `output_format="detailed"` with `aggregation="raw"` to include the complete original exported sample,
    including fields not promoted into standard columns. `limit` is 1-1000. Returns the resolved query window plus
    records. Validation errors state the accepted format and can be corrected without changing other parameters.
    """
    requested_names = metric_names or []
    sleep_names = [name for name in requested_names if name.lower().startswith("sleep_analysis") or any(
        token in name.lower() for token in ("sleep_stage_timeline", "awaken", "waso", "sleep_cycle")
    )]
    non_sleep_names = [name for name in requested_names if name not in sleep_names]
    sleep_result = None
    if sleep_names:
        if output_format == "detailed" and aggregation != "raw":
            raise ValueError("sleep detailed output requires aggregation='raw'")
        sleep_result = query_sleep_fallback(
            get_settings().db_path,
            sleep_names,
            start_time=start_time,
            end_time=end_time,
            timezone=timezone,
            output_format=output_format,
            limit=limit,
        )
        if not non_sleep_names:
            return sleep_result
    metric_result = storage_query_health_metrics(
        get_settings().db_path,
        metric_names=non_sleep_names or metric_names,
        start_time=start_time,
        end_time=end_time,
        aggregation=aggregation,
        limit=limit,
        output_format=output_format,
    )
    metric_result = localize_fallback_metric_result(metric_result, timezone)
    if sleep_result is not None:
        return {
            "status": "partial" if sleep_result["status"] == "partial" else "ok",
            "timezone": timezone,
            "sleep": sleep_result,
            "other_metrics": metric_result,
        }
    return metric_result


@mcp.tool(name="query_health_events")
def query_health_events(
    categories: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    """Query non-metric Apple Health records such as workouts, symptoms, ECGs, medications, or cycle events.

    Fallback only: use for upload debugging or explicit raw event requests. Use event category names returned by
    `list_available_data`; omit categories to query all. Times accept ISO-8601
    dates or timestamps and default to the last 30 days. `concise` returns identifying fields; `detailed` also returns
    the complete exported event object. `limit` is 1-500. Returns resolved filters and newest events first. Validation
    errors specify the parameter that must be corrected.
    """
    result = storage_query_health_events(
        get_settings().db_path,
        categories=categories,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        output_format=output_format,
    )
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError("timezone must be an IANA name, for example Asia/Shanghai") from error
    for event in result.get("events", []):
        for key in ("start_time", "end_time"):
            value = event.get(key)
            if value:
                parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
                event[key] = parsed.astimezone(zone).isoformat(timespec="seconds")
    result["timezone"] = timezone
    result["role"] = "fallback_raw_event_query"
    return result


@mcp.tool(name="get_health_overview")
def get_health_overview(
    days: Literal[7, 14, 30] = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    """Return a compact cross-metric overview for a recent period.

    Use for broad 7/14/30-day context when a narrower sleep, recovery, or activity tool is not sufficient. Returns
    compact semantic activity, cardiovascular, respiratory, sleep, body/environment, and latest recovery sections.
    Rate/percentage/temperature metrics never receive meaningless totals. Coverage and baseline strength are explicit.
    """
    return semantic_health_overview(get_settings().db_path, days, timezone, output_format, end_date)


async def health(request: Request) -> JSONResponse:
    del request
    try:
        status = ingestion_status(get_settings().db_path)
        return JSONResponse({"ok": True, "service": "apple-health-mcp", **status})
    except Exception:
        return JSONResponse({"ok": False, "service": "apple-health-mcp"}, status_code=503)


def _query_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("boolean query values must be true or false")


def _query_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as error:
        raise ValueError("integer query value is invalid") from error


def _semantic_api_response(callable_: Any) -> JSONResponse:
    try:
        return JSONResponse(callable_())
    except ValueError as error:
        return JSONResponse(
            {
                "status": "error",
                "error": "invalid_request",
                "message": str(error),
                "resolution": "Correct the query parameters and retry.",
            },
            status_code=400,
        )
    except Exception:
        return JSONResponse(
            {
                "status": "error",
                "error": "semantic_api_failed",
                "resolution": "Retry later; raw ingestion data remains safely stored.",
            },
            status_code=503,
        )


async def api_capabilities(request: Request) -> JSONResponse:
    del request
    return JSONResponse({"status": "ok", "capabilities": capabilities(get_settings().db_path)})


async def api_sleep_summary(request: Request) -> JSONResponse:
    query = request.query_params
    date = query.get("date")
    if not date:
        return JSONResponse(
            {"status": "error", "error": "missing_date", "resolution": "Provide date=YYYY-MM-DD."},
            status_code=400,
        )
    return _semantic_api_response(
        lambda: semantic_get_sleep_summary(
            get_settings().db_path,
            date,
            query.get("timezone", DEFAULT_TIMEZONE),
            _query_bool(query.get("include_recovery_metrics"), True),
            query.get("output_format", "concise"),
        )
    )


async def api_sleep_trend(request: Request) -> JSONResponse:
    query = request.query_params
    return _semantic_api_response(
        lambda: semantic_get_sleep_trend(
            get_settings().db_path,
            _query_int(query.get("nights"), 7),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
            query.get("end_date"),
        )
    )


async def api_recovery_snapshot(request: Request) -> JSONResponse:
    query = request.query_params
    return _semantic_api_response(
        lambda: semantic_get_recovery_snapshot(
            get_settings().db_path,
            query.get("date"),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
        )
    )


async def api_daily_activity(request: Request) -> JSONResponse:
    query = request.query_params
    date = query.get("date")
    if not date:
        return JSONResponse(
            {"status": "error", "error": "missing_date", "resolution": "Provide date=YYYY-MM-DD."},
            status_code=400,
        )
    return _semantic_api_response(
        lambda: semantic_get_daily_activity(
            get_settings().db_path,
            date,
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
        )
    )


async def api_activity_trend(request: Request) -> JSONResponse:
    query = request.query_params
    return _semantic_api_response(
        lambda: semantic_get_activity_trend(
            get_settings().db_path,
            _query_int(query.get("days"), 7),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
            query.get("end_date"),
        )
    )


async def api_list_workouts(request: Request) -> JSONResponse:
    query = request.query_params
    return _semantic_api_response(
        lambda: semantic_list_workouts(
            get_settings().db_path,
            _query_int(query.get("days"), 7),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
            query.get("end_date"),
            query.get("workout_type"),
            _query_int(query.get("limit"), 20),
        )
    )


async def api_workout_detail(request: Request) -> JSONResponse:
    query = request.query_params
    workout_id = query.get("workout_id")
    if not workout_id:
        return JSONResponse(
            {
                "status": "error",
                "error": "missing_workout_id",
                "resolution": "Call /api/v1/health/workouts first, then provide workout_id.",
            },
            status_code=400,
        )
    return _semantic_api_response(
        lambda: semantic_get_workout_detail(
            get_settings().db_path,
            workout_id,
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
        )
    )


async def api_compare_workouts(request: Request) -> JSONResponse:
    query = request.query_params
    workout_id = query.get("workout_id")
    if not workout_id:
        return JSONResponse(
            {
                "status": "error",
                "error": "missing_workout_id",
                "resolution": "Call /api/v1/health/workouts first, then provide workout_id.",
            },
            status_code=400,
        )
    return _semantic_api_response(
        lambda: semantic_compare_workouts(
            get_settings().db_path,
            workout_id,
            query.get("comparison_workout_id"),
            _query_int(query.get("baseline_count"), 5),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
        )
    )


async def api_health_overview(request: Request) -> JSONResponse:
    query = request.query_params
    return _semantic_api_response(
        lambda: semantic_health_overview(
            get_settings().db_path,
            _query_int(query.get("days"), 7),
            query.get("timezone", DEFAULT_TIMEZONE),
            query.get("output_format", "concise"),
            query.get("end_date"),
        )
    )


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
        Route("/api/v1/health/capabilities", api_capabilities, methods=["GET"]),
        Route("/api/v1/health/sleep/summary", api_sleep_summary, methods=["GET"]),
        Route("/api/v1/health/sleep/trend", api_sleep_trend, methods=["GET"]),
        Route("/api/v1/health/recovery/snapshot", api_recovery_snapshot, methods=["GET"]),
        Route("/api/v1/health/activity/daily", api_daily_activity, methods=["GET"]),
        Route("/api/v1/health/activity/trend", api_activity_trend, methods=["GET"]),
        Route("/api/v1/health/workouts", api_list_workouts, methods=["GET"]),
        Route("/api/v1/health/workouts/detail", api_workout_detail, methods=["GET"]),
        Route("/api/v1/health/workouts/compare", api_compare_workouts, methods=["GET"]),
        Route("/api/v1/health/overview", api_health_overview, methods=["GET"]),
        Mount("/", app=mcp_http_app),
    ],
    lifespan=lifespan,
)

app = MCPBearerAuthMiddleware(core_app)
