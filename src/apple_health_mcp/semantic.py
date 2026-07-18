from __future__ import annotations

import json
import math
from datetime import UTC, date as Date, datetime, time, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .storage import connect, initialize_database


DEFAULT_TIMEZONE = "Asia/Shanghai"
SleepOutputFormat = Literal["concise", "detailed"]


def _zone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError("timezone must be an IANA name, for example Asia/Shanghai") from error


def _parse_date(value: str) -> Date:
    try:
        return Date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("date must use YYYY-MM-DD, for example 2026-07-18") from error


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    try:
        parsed = datetime.fromisoformat(candidate[:-1] + "+00:00" if candidate.endswith("Z") else candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _local_iso(value: datetime | None, zone: ZoneInfo) -> str | None:
    if value is None:
        return None
    return value.astimezone(zone).isoformat(timespec="seconds")


def _day_bounds(day: Date, zone: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=zone)
    return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)


def _date_range(start: Date, end: Date) -> list[Date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _round(value: float | None, digits: int = 1) -> float | None:
    return round(value, digits) if value is not None else None


def _minutes(hours: Any) -> int | None:
    value = _number(hours)
    return round(value * 60) if value is not None else None


def _sum_optional(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _mean(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return mean(present) if present else None


def _median(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return median(present) if present else None


def _strength(sample_count: int) -> str:
    if sample_count < 3:
        return "insufficient_data"
    if sample_count < 7:
        return "limited"
    return "established"


def _validate_output_format(value: str) -> None:
    if value not in {"concise", "detailed"}:
        raise ValueError("output_format must be concise or detailed")


def _latest_ingestion(db_path: Path, zone: ZoneInfo) -> str | None:
    initialize_database(db_path)
    with connect(db_path) as connection:
        value = connection.execute("SELECT MAX(received_at) AS latest FROM ingestions").fetchone()["latest"]
    return _local_iso(_parse_datetime(value), zone)


def capabilities(db_path: Path | None = None) -> dict[str, bool]:
    has_workouts = False
    has_cycle_tracking = False
    if db_path is not None:
        initialize_database(db_path)
        with connect(db_path) as connection:
            has_workouts = bool(
                connection.execute(
                    "SELECT 1 FROM health_events WHERE category COLLATE NOCASE = 'workouts' LIMIT 1"
                ).fetchone()
            )
            has_cycle_tracking = bool(
                connection.execute(
                    "SELECT 1 FROM health_events WHERE category COLLATE NOCASE = 'cycleTracking' LIMIT 1"
                ).fetchone()
            )
    return {
        "nightly_sleep_totals": True,
        "sleep_stage_totals": True,
        "sleep_stage_timeline": False,
        "sleep_awakenings_count": False,
        "sleep_waso": False,
        "sleep_recovery_join": True,
        "daily_activity_summary": True,
        "personal_baseline_comparison": True,
        "workout_session_summary": has_workouts,
        "workout_detail_summary": has_workouts,
        "workout_comparison": has_workouts,
        "cycle_tracking_events": has_cycle_tracking,
        "subjective_context_logging": False,
    }


def _metric_rows(
    db_path: Path,
    metric_name: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    initialize_database(db_path)
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT ms.metric_name, ms.unit, ms.start_time, ms.end_time, ms.source,
                   ms.numeric_value, ms.min_value, ms.max_value, ms.avg_value,
                   ms.sum_value, ms.value_text, ms.payload_json,
                   i.session_id, i.received_at
            FROM metric_samples ms
            JOIN ingestions i ON i.id = ms.ingestion_id
            WHERE ms.metric_name COLLATE NOCASE = ?
              AND ms.start_time >= ? AND ms.start_time < ?
            ORDER BY ms.start_time
            """,
            (metric_name, _utc_iso(start), _utc_iso(end)),
        ).fetchall()
    return [dict(row) for row in rows]


def _row_value(row: dict[str, Any]) -> float | None:
    for field in ("numeric_value", "avg_value", "sum_value"):
        value = _number(row.get(field))
        if value is not None:
            return value
    return None


def _sleep_sessions(db_path: Path, zone: ZoneInfo) -> list[dict[str, Any]]:
    initialize_database(db_path)
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT ms.start_time, ms.end_time, ms.source, ms.numeric_value,
                   ms.payload_json, i.session_id, i.received_at
            FROM metric_samples ms
            JOIN ingestions i ON i.id = ms.ingestion_id
            WHERE ms.metric_name = 'sleep_analysis'
            ORDER BY ms.start_time
            """
        ).fetchall()

    by_wake_date: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        sleep_start = _parse_datetime(payload.get("sleepStart")) or _parse_datetime(row["start_time"])
        sleep_end = _parse_datetime(payload.get("sleepEnd")) or _parse_datetime(row["end_time"])
        in_bed_start = _parse_datetime(payload.get("inBedStart"))
        in_bed_end = _parse_datetime(payload.get("inBedEnd"))
        if sleep_end is None:
            continue
        wake_date = sleep_end.astimezone(zone).date().isoformat()
        total_sleep = _minutes(payload.get("totalSleep"))
        unclassified = _minutes(payload.get("asleep"))
        awake = _minutes(payload.get("awake"))
        core = _minutes(payload.get("core"))
        deep = _minutes(payload.get("deep"))
        rem = _minutes(payload.get("rem"))
        stage_total = _sum_optional([unclassified, core, deep, rem])
        derived_fields: list[str] = []
        missing_fields: list[str] = []
        quality_flags: list[str] = []

        if total_sleep is None and stage_total is not None:
            total_sleep = round(stage_total)
            derived_fields.append("summary.sleep_window.total_sleep_minutes")

        sleep_window_minutes = None
        if sleep_start and sleep_end and sleep_end >= sleep_start:
            sleep_window_minutes = round((sleep_end - sleep_start).total_seconds() / 60)
            derived_fields.append("summary.sleep_window.window_minutes")
        else:
            missing_fields.extend(["sleepStart", "sleepEnd"])

        raw_in_bed = _number(payload.get("inBed"))
        in_bed_minutes = round(raw_in_bed * 60) if raw_in_bed is not None and raw_in_bed > 0 else None
        if in_bed_minutes is None and in_bed_start and in_bed_end and in_bed_end >= in_bed_start:
            in_bed_minutes = round((in_bed_end - in_bed_start).total_seconds() / 60)
            derived_fields.append("summary.sleep_window.in_bed_minutes")

        if total_sleep is None:
            missing_fields.append("totalSleep")
        for key, value in (("core", core), ("deep", deep), ("rem", rem), ("awake", awake)):
            if value is None:
                missing_fields.append(key)

        if total_sleep is not None and stage_total is not None and abs(total_sleep - stage_total) > 2:
            quality_flags.append("sleep_stage_totals_do_not_match_total_sleep")
        if sleep_window_minutes is not None and total_sleep is not None and awake is not None:
            unclassified_gap = sleep_window_minutes - total_sleep - awake
            if abs(unclassified_gap) > 15:
                quality_flags.append("sleep_window_contains_unclassified_gap")

        candidate = {
            "sleep_date": wake_date,
            "source": row["source"],
            "session_id": row["session_id"],
            "received_at": _parse_datetime(row["received_at"]),
            "sleep_start": sleep_start,
            "sleep_end": sleep_end,
            "in_bed_start": in_bed_start,
            "in_bed_end": in_bed_end,
            "sleep_window_minutes": sleep_window_minutes,
            "in_bed_minutes": in_bed_minutes,
            "total_sleep_minutes": total_sleep,
            "awake_minutes": awake,
            "unclassified_asleep_minutes": unclassified,
            "core_minutes": core,
            "deep_minutes": deep,
            "rem_minutes": rem,
            "derived_fields": derived_fields,
            "missing_fields": missing_fields,
            "quality_flags": quality_flags,
            "payload": payload,
        }
        existing = by_wake_date.get(wake_date)
        existing_score = (
            int("watch" in str(existing.get("source", "")).lower()),
            existing.get("total_sleep_minutes") or -1,
        ) if existing else (-1, -1)
        candidate_score = (
            int("watch" in str(candidate.get("source", "")).lower()),
            candidate.get("total_sleep_minutes") or -1,
        )
        if existing is None or candidate_score > existing_score:
            by_wake_date[wake_date] = candidate
    return [by_wake_date[key] for key in sorted(by_wake_date)]


def _normalize_percent(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 100 if abs(value) <= 1.5 else value


def _recovery_for_window(
    db_path: Path,
    sleep_start: datetime | None,
    sleep_end: datetime | None,
) -> dict[str, float | None]:
    fields = {
        "sleeping_heart_rate_avg_bpm": None,
        "sleeping_heart_rate_min_bpm": None,
        "respiratory_rate_avg_per_min": None,
        "blood_oxygen_avg_percent": None,
        "blood_oxygen_min_percent": None,
        "hrv_near_wake_ms": None,
        "wrist_temperature_c": None,
    }
    if sleep_start is None or sleep_end is None:
        return fields

    heart = _metric_rows(db_path, "heart_rate", sleep_start, sleep_end)
    heart_avg = [_number(row.get("avg_value")) or _number(row.get("numeric_value")) for row in heart]
    heart_min = [_number(row.get("min_value")) or _number(row.get("numeric_value")) for row in heart]
    fields["sleeping_heart_rate_avg_bpm"] = _round(_mean(heart_avg), 1)
    present_min = [value for value in heart_min if value is not None]
    fields["sleeping_heart_rate_min_bpm"] = _round(min(present_min), 1) if present_min else None

    respiratory = [_row_value(row) for row in _metric_rows(db_path, "respiratory_rate", sleep_start, sleep_end)]
    fields["respiratory_rate_avg_per_min"] = _round(_mean(respiratory), 1)

    oxygen = [
        _normalize_percent(_row_value(row))
        for row in _metric_rows(db_path, "blood_oxygen_saturation", sleep_start, sleep_end)
    ]
    fields["blood_oxygen_avg_percent"] = _round(_mean(oxygen), 1)
    present_oxygen = [value for value in oxygen if value is not None]
    fields["blood_oxygen_min_percent"] = _round(min(present_oxygen), 1) if present_oxygen else None

    hrv_start = sleep_end - timedelta(hours=2)
    hrv_end = sleep_end + timedelta(hours=2)
    hrv = [_row_value(row) for row in _metric_rows(db_path, "heart_rate_variability", hrv_start, hrv_end)]
    fields["hrv_near_wake_ms"] = _round(_mean(hrv), 1)

    temperature_rows = _metric_rows(
        db_path,
        "apple_sleeping_wrist_temperature",
        sleep_end - timedelta(hours=18),
        sleep_end + timedelta(hours=6),
    )
    if temperature_rows:
        closest = min(
            temperature_rows,
            key=lambda row: abs(
                ((_parse_datetime(row["start_time"]) or sleep_end) - sleep_end).total_seconds()
            ),
        )
        fields["wrist_temperature_c"] = _round(_row_value(closest), 2)
    return fields


def _sleep_baseline(sessions: list[dict[str, Any]], target_date: str, limit: int = 14) -> list[dict[str, Any]]:
    return [session for session in sessions if session["sleep_date"] < target_date][-limit:]


def _sleep_comparison(session: dict[str, Any], baseline: list[dict[str, Any]]) -> dict[str, Any]:
    total_baseline = _mean([item.get("total_sleep_minutes") for item in baseline])
    deep_baseline = _mean([item.get("deep_minutes") for item in baseline])
    rem_baseline = _mean([item.get("rem_minutes") for item in baseline])
    return {
        "baseline_nights": len(baseline),
        "baseline_window": "previous_up_to_14_nights",
        "total_sleep_baseline_minutes": round(total_baseline) if total_baseline is not None else None,
        "total_sleep_difference_minutes": (
            round(session["total_sleep_minutes"] - total_baseline)
            if session.get("total_sleep_minutes") is not None and total_baseline is not None
            else None
        ),
        "deep_sleep_difference_minutes": (
            round(session["deep_minutes"] - deep_baseline)
            if session.get("deep_minutes") is not None and deep_baseline is not None
            else None
        ),
        "rem_sleep_difference_minutes": (
            round(session["rem_minutes"] - rem_baseline)
            if session.get("rem_minutes") is not None and rem_baseline is not None
            else None
        ),
        "strength": _strength(len(baseline)),
    }


def _sleep_highlights(session: dict[str, Any], baseline_count: int) -> list[str]:
    highlights: list[str] = []
    total = session.get("total_sleep_minutes")
    deep = session.get("deep_minutes")
    if total is not None:
        highlights.append(f"总睡眠约{total // 60}小时{total % 60}分钟")
    if deep is not None:
        highlights.append(f"深睡约{deep // 60}小时{deep % 60}分钟")
    if baseline_count < 3:
        highlights.append(f"当前仅有{baseline_count + 1}晚可比较数据，暂不足以判断长期趋势")
    return highlights[:3]


def _sleep_summary_payload(
    db_path: Path,
    session: dict[str, Any],
    zone: ZoneInfo,
    all_sessions: list[dict[str, Any]],
    include_recovery_metrics: bool,
    output_format: SleepOutputFormat,
) -> dict[str, Any]:
    total = session.get("total_sleep_minutes")
    stage_values = {
        "rem_minutes": session.get("rem_minutes"),
        "core_minutes": session.get("core_minutes"),
        "deep_minutes": session.get("deep_minutes"),
        "unclassified_asleep_minutes": session.get("unclassified_asleep_minutes"),
    }
    stage_percentages = {
        key.replace("_minutes", ""): _round(value / total * 100, 1) if value is not None and total else None
        for key, value in stage_values.items()
    }
    baseline = _sleep_baseline(all_sessions, session["sleep_date"])
    comparison = _sleep_comparison(session, baseline)
    recovery = (
        _recovery_for_window(db_path, session.get("sleep_start"), session.get("sleep_end"))
        if include_recovery_metrics
        else None
    )
    recovery_missing = [key for key, value in (recovery or {}).items() if value is None]
    complete = not session["missing_fields"] and not session["quality_flags"]
    if include_recovery_metrics and recovery_missing:
        complete = False
    status = "ok" if complete else "partial"
    actual_start = _local_iso(session.get("sleep_start"), zone)
    actual_end = _local_iso(session.get("sleep_end"), zone)
    summary: dict[str, Any] = {
        "sleep_window": {
            "start": actual_start,
            "end": actual_end,
            "window_minutes": session.get("sleep_window_minutes"),
            "in_bed_start": _local_iso(session.get("in_bed_start"), zone),
            "in_bed_end": _local_iso(session.get("in_bed_end"), zone),
            "in_bed_minutes": session.get("in_bed_minutes"),
            "total_sleep_minutes": total,
            "awake_minutes": session.get("awake_minutes"),
        },
        "stages": stage_values,
        "stage_percentages": stage_percentages,
    }
    if include_recovery_metrics:
        summary["recovery"] = recovery
    if output_format == "detailed":
        summary["technical"] = {
            "session_id": session.get("session_id"),
            "original_payload": session.get("payload"),
        }
    return {
        "status": status,
        "sleep_date": session["sleep_date"],
        "timezone": zone.key,
        "requested_range": {"sleep_date": session["sleep_date"]},
        "actual_data_range": {"start": actual_start, "end": actual_end},
        "summary": summary,
        "comparison_to_personal_baseline": comparison,
        "highlights": _sleep_highlights(session, len(baseline)),
        "capabilities": capabilities(db_path),
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_nights": 1,
            "missing_nights": 0,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": [session["source"]] if session.get("source") else [],
            "derived_fields": session["derived_fields"],
            "missing_fields": session["missing_fields"] + recovery_missing,
            "quality_flags": session["quality_flags"],
            "has_stage_timeline": False,
        },
    }


def get_sleep_summary(
    db_path: Path,
    date: str,
    timezone: str = DEFAULT_TIMEZONE,
    include_recovery_metrics: bool = True,
    output_format: SleepOutputFormat = "concise",
) -> dict[str, Any]:
    _validate_output_format(output_format)
    target = _parse_date(date)
    zone = _zone(timezone)
    sessions = _sleep_sessions(db_path, zone)
    session = next((item for item in sessions if item["sleep_date"] == target.isoformat()), None)
    if session is None:
        return {
            "status": "empty",
            "sleep_date": target.isoformat(),
            "timezone": zone.key,
            "requested_range": {"sleep_date": target.isoformat()},
            "actual_data_range": None,
            "summary": {},
            "comparison_to_personal_baseline": {"baseline_nights": 0, "strength": "insufficient_data"},
            "highlights": ["该醒来日期没有已上传的睡眠记录"],
            "capabilities": capabilities(db_path),
            "fetch": {"complete": False},
            "data_quality": {
                "complete": False,
                "valid_nights": 0,
                "missing_nights": 1,
                "latest_ingestion": _latest_ingestion(db_path, zone),
                "sources": [],
                "derived_fields": [],
                "missing_fields": ["sleep_record"],
                "quality_flags": [],
                "has_stage_timeline": False,
            },
        }
    return _sleep_summary_payload(
        db_path,
        session,
        zone,
        sessions,
        include_recovery_metrics,
        output_format,
    )


def get_sleep_trend(
    db_path: Path,
    nights: int = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: SleepOutputFormat = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    if nights not in {7, 14, 30}:
        raise ValueError("nights must be one of 7, 14, or 30")
    zone = _zone(timezone)
    end = _parse_date(end_date) if end_date else datetime.now(zone).date()
    start = end - timedelta(days=nights - 1)
    all_sessions = _sleep_sessions(db_path, zone)
    selected = [item for item in all_sessions if start.isoformat() <= item["sleep_date"] <= end.isoformat()]
    valid = len(selected)
    missing = nights - valid
    totals = [item.get("total_sleep_minutes") for item in selected]
    deep = [item.get("deep_minutes") for item in selected]
    rem = [item.get("rem_minutes") for item in selected]
    strength = _strength(valid)
    nightly = [
        {
            "sleep_date": item["sleep_date"],
            "total_sleep_minutes": item.get("total_sleep_minutes"),
            "awake_minutes": item.get("awake_minutes"),
            "deep_minutes": item.get("deep_minutes"),
            "core_minutes": item.get("core_minutes"),
            "rem_minutes": item.get("rem_minutes"),
            "start": _local_iso(item.get("sleep_start"), zone),
            "end": _local_iso(item.get("sleep_end"), zone),
        }
        for item in selected
    ]
    summary: dict[str, Any] = {
        "requested_nights": nights,
        "valid_nights": valid,
        "missing_nights": missing,
        "average_total_sleep_minutes": round(_mean(totals)) if _mean(totals) is not None else None,
        "median_total_sleep_minutes": round(_median(totals)) if _median(totals) is not None else None,
        "average_deep_minutes": round(_mean(deep)) if _mean(deep) is not None else None,
        "average_rem_minutes": round(_mean(rem)) if _mean(rem) is not None else None,
        "latest_sleep_summary": nightly[-1] if nightly else None,
        "trend_strength": strength,
        "nights": nightly,
    }
    if output_format == "detailed":
        summary["nightly_quality"] = [
            {
                "sleep_date": item["sleep_date"],
                "derived_fields": item["derived_fields"],
                "missing_fields": item["missing_fields"],
                "quality_flags": item["quality_flags"],
            }
            for item in selected
        ]
    actual = (
        {"start_date": selected[0]["sleep_date"], "end_date": selected[-1]["sleep_date"]}
        if selected
        else None
    )
    status = "empty" if not selected else "ok" if missing == 0 else "partial"
    highlights: list[str] = []
    average_total = summary["average_total_sleep_minutes"]
    if average_total is not None:
        highlights.append(f"{valid}晚平均总睡眠约{average_total // 60}小时{average_total % 60}分钟")
    if valid < 3:
        highlights.append(f"请求{nights}晚但只有{valid}晚有效数据，趋势判断为样本不足")
    elif missing:
        highlights.append(f"请求范围内缺少{missing}晚数据，趋势仅基于有效夜晚")
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {"start_date": start.isoformat(), "end_date": end.isoformat(), "nights": nights},
        "actual_data_range": actual,
        "summary": summary,
        "comparison_to_personal_baseline": {
            "baseline_type": "within_requested_period",
            "valid_nights": valid,
            "strength": strength,
        },
        "highlights": highlights[:3],
        "capabilities": capabilities(db_path),
        "fetch": {"complete": missing == 0},
        "data_quality": {
            "complete": missing == 0,
            "valid_nights": valid,
            "missing_nights": missing,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sorted({item["source"] for item in selected if item.get("source")}),
            "derived_fields": sorted({field for item in selected for field in item["derived_fields"]}),
            "missing_fields": [],
            "quality_flags": sorted({flag for item in selected for flag in item["quality_flags"]}),
            "has_stage_timeline": False,
        },
    }


def _daily_rows(db_path: Path, metric_name: str, day: Date, zone: ZoneInfo) -> list[dict[str, Any]]:
    start, end = _day_bounds(day, zone)
    return _metric_rows(db_path, metric_name, start, end)


def _daily_total(db_path: Path, metric_name: str, day: Date, zone: ZoneInfo) -> float | None:
    return _sum_optional([_row_value(row) for row in _daily_rows(db_path, metric_name, day, zone)])


def _daily_average(db_path: Path, metric_name: str, day: Date, zone: ZoneInfo) -> float | None:
    return _mean([_row_value(row) for row in _daily_rows(db_path, metric_name, day, zone)])


def _activity_day_values(db_path: Path, day: Date, zone: ZoneInfo) -> dict[str, Any]:
    steps = _daily_total(db_path, "step_count", day, zone)
    walking_distance = _daily_total(db_path, "walking_running_distance", day, zone)
    cycling_distance = _daily_total(db_path, "cycling_distance", day, zone)
    active_energy_kj = _daily_total(db_path, "active_energy", day, zone)
    exercise = _daily_total(db_path, "apple_exercise_time", day, zone)
    stand = _daily_total(db_path, "apple_stand_hour", day, zone)
    daylight = _daily_total(db_path, "time_in_daylight", day, zone)
    return {
        "date": day.isoformat(),
        "step_count": round(steps) if steps is not None else None,
        "walking_running_distance_km": _round(walking_distance, 3),
        "cycling_distance_km": _round(cycling_distance, 3),
        "active_energy_kcal": _round(active_energy_kj / 4.184, 1) if active_energy_kj is not None else None,
        "exercise_minutes": round(exercise) if exercise is not None else None,
        "stand_hours": round(stand) if stand is not None else None,
        "daylight_minutes": round(daylight) if daylight is not None else None,
    }


def _quantity(payload: dict[str, Any], *keys: str) -> tuple[float | None, str | None]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            quantity = _number(value.get("qty"))
            unit = value.get("units")
            return quantity, str(unit) if unit is not None else None
    return None, None


def _energy_kcal(quantity: float | None, unit: str | None) -> float | None:
    if quantity is None:
        return None
    normalized = (unit or "").strip().lower()
    if normalized == "kj":
        return quantity / 4.184
    if normalized == "j":
        return quantity / 4184
    if normalized in {"kcal", "calorie", "calories"}:
        return quantity
    return None


def _distance_km(quantity: float | None, unit: str | None) -> float | None:
    if quantity is None:
        return None
    normalized = (unit or "").strip().lower()
    if normalized in {"km", "kilometer", "kilometers"}:
        return quantity
    if normalized in {"m", "meter", "meters"}:
        return quantity / 1000
    if normalized in {"mi", "mile", "miles"}:
        return quantity * 1.609344
    return None


def _speed_kmh(quantity: float | None, unit: str | None) -> float | None:
    if quantity is None:
        return None
    normalized = (unit or "").strip().lower()
    if normalized in {"km/hr", "km/h", "kph"}:
        return quantity
    if normalized in {"m/s", "m/sec"}:
        return quantity * 3.6
    if normalized in {"mi/hr", "mph"}:
        return quantity * 1.609344
    return None


def _workout_rows(
    db_path: Path,
    start: datetime | None = None,
    end: datetime | None = None,
    workout_id: str | None = None,
) -> list[dict[str, Any]]:
    initialize_database(db_path)
    clauses = ["he.category COLLATE NOCASE = 'workouts'"]
    params: list[Any] = []
    if start is not None:
        clauses.append("he.start_time >= ?")
        params.append(_utc_iso(start))
    if end is not None:
        clauses.append("he.start_time < ?")
        params.append(_utc_iso(end))
    if workout_id is not None:
        clauses.append(
            "(he.event_uuid = ? OR json_extract(he.payload_json, '$.id') = ? "
            "OR json_extract(he.payload_json, '$.uuid') = ?)"
        )
        params.extend([workout_id, workout_id, workout_id])
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT he.event_uuid, he.event_type, he.start_time, he.end_time, he.source,
                   he.payload_json, i.received_at
            FROM health_events he
            JOIN ingestions i ON i.id = he.ingestion_id
            WHERE {' AND '.join(clauses)}
            ORDER BY he.start_time DESC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _workout_summary(row: dict[str, Any], zone: ZoneInfo, detailed: bool = False) -> dict[str, Any]:
    payload = json.loads(row["payload_json"])
    start = _parse_datetime(row.get("start_time"))
    end = _parse_datetime(row.get("end_time"))
    duration_seconds = _number(payload.get("duration"))
    if duration_seconds is None and start is not None and end is not None:
        duration_seconds = max(0.0, (end - start).total_seconds())

    distance_quantity, distance_unit = _quantity(payload, "distance")
    active_quantity, active_unit = _quantity(payload, "activeEnergyBurned")
    total_quantity, total_unit = _quantity(payload, "totalEnergy")
    average_hr, _ = _quantity(payload, "avgHeartRate")
    minimum_hr = None
    maximum_hr, _ = _quantity(payload, "maxHeartRate")
    heart_rate = payload.get("heartRate")
    if isinstance(heart_rate, dict):
        if isinstance(heart_rate.get("avg"), dict):
            average_hr = _number(heart_rate["avg"].get("qty")) or average_hr
        if isinstance(heart_rate.get("min"), dict):
            minimum_hr = _number(heart_rate["min"].get("qty"))
        if isinstance(heart_rate.get("max"), dict):
            maximum_hr = _number(heart_rate["max"].get("qty")) or maximum_hr

    average_speed_quantity, average_speed_unit = _quantity(payload, "speed", "avgSpeed")
    max_speed_quantity, max_speed_unit = _quantity(payload, "maxSpeed")
    elevation_quantity, elevation_unit = _quantity(payload, "elevationUp")
    distance_km = _distance_km(distance_quantity, distance_unit)
    duration_minutes = duration_seconds / 60 if duration_seconds is not None else None
    average_speed_kmh = _speed_kmh(average_speed_quantity, average_speed_unit)
    derived_fields: list[str] = []
    quality_flags: list[str] = []
    if average_speed_kmh is None and distance_km is not None and duration_seconds:
        average_speed_kmh = distance_km / (duration_seconds / 3600)
        derived_fields.append("average_speed_kmh")
    max_speed_kmh = _speed_kmh(max_speed_quantity, max_speed_unit)
    if max_speed_kmh is None and max_speed_quantity is not None and (max_speed_unit or "").lower() == "km":
        max_speed_kmh = max_speed_quantity
        quality_flags.append("max_speed_unit_normalized_from_export_field")

    activity_name = str(row.get("event_type") or payload.get("name") or "未知锻炼")
    normalized_name = activity_name.casefold()
    pace_minutes_per_km = None
    if distance_km and duration_minutes is not None and any(
        marker in normalized_name for marker in ("步行", "跑", "walk", "run")
    ):
        pace_minutes_per_km = duration_minutes / distance_km
        derived_fields.append("pace_minutes_per_km")

    heart_rate_samples = payload.get("heartRateData")
    route = payload.get("route")
    recovery = payload.get("heartRateRecovery")
    source = row.get("source") or payload.get("source")
    if not source and isinstance(heart_rate_samples, list) and heart_rate_samples:
        first_sample = heart_rate_samples[0]
        if isinstance(first_sample, dict):
            source = first_sample.get("source")

    missing_fields = [
        field
        for field, value in (
            ("workout_id", row.get("event_uuid") or payload.get("id") or payload.get("uuid")),
            ("start", start),
            ("end", end),
            ("duration_minutes", duration_minutes),
        )
        if value is None
    ]
    workout = {
        "workout_id": row.get("event_uuid") or payload.get("id") or payload.get("uuid"),
        "type": activity_name,
        "start": _local_iso(start, zone),
        "end": _local_iso(end, zone),
        "duration_minutes": _round(duration_minutes, 1),
        "is_indoor": payload.get("isIndoor"),
        "location": payload.get("location"),
        "distance_km": _round(distance_km, 3),
        "active_energy_kcal": _round(_energy_kcal(active_quantity, active_unit), 1),
        "total_energy_kcal": _round(_energy_kcal(total_quantity, total_unit), 1),
        "heart_rate_bpm": {
            "average": _round(average_hr, 1),
            "minimum": _round(minimum_hr, 1),
            "maximum": _round(maximum_hr, 1),
        },
        "average_speed_kmh": _round(average_speed_kmh, 2),
        "max_speed_kmh": _round(max_speed_kmh, 2),
        "pace_minutes_per_km": _round(pace_minutes_per_km, 2),
        "elevation_gain_m": (
            _round(elevation_quantity, 1)
            if elevation_quantity is not None and (elevation_unit or "").lower() == "m"
            else None
        ),
        "sensor_coverage": {
            "heart_rate_samples": len(heart_rate_samples) if isinstance(heart_rate_samples, list) else 0,
            "heart_rate_recovery_samples": len(recovery) if isinstance(recovery, list) else 0,
            "route_available": bool(route) if isinstance(route, list) else False,
            "route_points": len(route) if isinstance(route, list) else 0,
        },
        "source": source,
    }
    if detailed:
        workout["reported_units"] = {
            "distance": distance_unit,
            "active_energy": active_unit,
            "total_energy": total_unit,
            "average_speed": average_speed_unit,
            "max_speed": max_speed_unit,
            "elevation_gain": elevation_unit,
        }
        workout["recorded_metrics"] = [
            name
            for name, value in (
                ("duration", duration_minutes),
                ("distance", distance_km),
                ("active_energy", _energy_kcal(active_quantity, active_unit)),
                ("heart_rate", average_hr),
                ("speed", average_speed_kmh),
                ("elevation_gain", elevation_quantity),
                ("route", route if isinstance(route, list) and route else None),
            )
            if value is not None
        ]
    workout["_quality"] = {
        "complete": not missing_fields,
        "missing_fields": missing_fields,
        "derived_fields": derived_fields,
        "quality_flags": quality_flags,
    }
    return workout


def _compact_workout(workout: dict[str, Any]) -> dict[str, Any]:
    return {
        key: workout.get(key)
        for key in (
            "workout_id",
            "type",
            "start",
            "duration_minutes",
            "distance_km",
            "active_energy_kcal",
            "heart_rate_bpm",
            "average_speed_kmh",
            "pace_minutes_per_km",
            "is_indoor",
        )
    }


def _workout_averages(workouts: list[dict[str, Any]]) -> dict[str, float | None]:
    return {
        "duration_minutes": _round(_mean([_number(item.get("duration_minutes")) for item in workouts]), 1),
        "distance_km": _round(_mean([_number(item.get("distance_km")) for item in workouts]), 3),
        "active_energy_kcal": _round(
            _mean([_number(item.get("active_energy_kcal")) for item in workouts]), 1
        ),
        "average_heart_rate_bpm": _round(
            _mean([_number(item.get("heart_rate_bpm", {}).get("average")) for item in workouts]), 1
        ),
        "average_speed_kmh": _round(_mean([_number(item.get("average_speed_kmh")) for item in workouts]), 2),
        "pace_minutes_per_km": _round(
            _mean([_number(item.get("pace_minutes_per_km")) for item in workouts]), 2
        ),
        "elevation_gain_m": _round(_mean([_number(item.get("elevation_gain_m")) for item in workouts]), 1),
    }


def _metric_differences(target: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    target_values = {
        "duration_minutes": _number(target.get("duration_minutes")),
        "distance_km": _number(target.get("distance_km")),
        "active_energy_kcal": _number(target.get("active_energy_kcal")),
        "average_heart_rate_bpm": _number(target.get("heart_rate_bpm", {}).get("average")),
        "average_speed_kmh": _number(target.get("average_speed_kmh")),
        "pace_minutes_per_km": _number(target.get("pace_minutes_per_km")),
        "elevation_gain_m": _number(target.get("elevation_gain_m")),
    }
    result: dict[str, Any] = {}
    for metric, target_value in target_values.items():
        baseline_value = _number(baseline.get(metric))
        difference = target_value - baseline_value if target_value is not None and baseline_value is not None else None
        result[metric] = {
            "target": _round(target_value, 2),
            "baseline": _round(baseline_value, 2),
            "difference": _round(difference, 2),
            "difference_percent": (
                _round(difference / baseline_value * 100, 1)
                if difference is not None and baseline_value not in {None, 0}
                else None
            ),
        }
    return result


def _workout_highlights(workout: dict[str, Any], baseline_count: int | None = None) -> list[str]:
    highlights: list[str] = []
    duration = _number(workout.get("duration_minutes"))
    distance = _number(workout.get("distance_km"))
    if duration is not None:
        text = f"{workout.get('type')}约{round(duration)}分钟"
        if distance is not None:
            text += f"，{distance:.2f}公里"
        highlights.append(text)
    average_hr = _number(workout.get("heart_rate_bpm", {}).get("average"))
    energy = _number(workout.get("active_energy_kcal"))
    if average_hr is not None or energy is not None:
        parts = []
        if average_hr is not None:
            parts.append(f"平均心率约{round(average_hr)}次/分")
        if energy is not None:
            parts.append(f"活动能量约{round(energy)}千卡")
        highlights.append("，".join(parts))
    if baseline_count is not None and baseline_count < 3:
        highlights.append(f"仅有{baseline_count}次同类历史锻炼可比较，个人基线仍不足")
    return highlights[:3]


def list_workouts(
    db_path: Path,
    days: int = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
    workout_type: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    if days not in {7, 14, 30}:
        raise ValueError("days must be one of 7, 14, or 30")
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    zone = _zone(timezone)
    end_day = _parse_date(end_date) if end_date else datetime.now(zone).date()
    start_day = end_day - timedelta(days=days - 1)
    start, _ = _day_bounds(start_day, zone)
    _, end = _day_bounds(end_day, zone)
    workouts = [
        _workout_summary(row, zone, output_format == "detailed")
        for row in _workout_rows(db_path, start, end)
    ]
    if workout_type:
        workouts = [item for item in workouts if str(item.get("type", "")).casefold() == workout_type.casefold()]
    matching_workout_count = len(workouts)
    workouts = workouts[:limit]
    truncated = matching_workout_count > len(workouts)

    previous_start = start - timedelta(days=days)
    previous = [_workout_summary(row, zone) for row in _workout_rows(db_path, previous_start, start)]
    if workout_type:
        previous = [item for item in previous if str(item.get("type", "")).casefold() == workout_type.casefold()]
    current_averages = _workout_averages(workouts)
    previous_averages = _workout_averages(previous)
    sources = sorted({str(item["source"]) for item in workouts if item.get("source")})
    quality = [item.pop("_quality") for item in workouts]
    complete = bool(workouts) and all(item["complete"] for item in quality) and not truncated
    workout_dates = sorted({str(item["start"])[:10] for item in workouts if item.get("start")})
    by_type: dict[str, int] = {}
    for item in workouts:
        name = str(item.get("type") or "未知锻炼")
        by_type[name] = by_type.get(name, 0) + 1
    summary = {
        "workout_count": len(workouts),
        "workout_days": len(workout_dates),
        "totals": {
            "duration_minutes": _round(_sum_optional([_number(item.get("duration_minutes")) for item in workouts]), 1),
            "distance_km": _round(_sum_optional([_number(item.get("distance_km")) for item in workouts]), 3),
            "active_energy_kcal": _round(
                _sum_optional([_number(item.get("active_energy_kcal")) for item in workouts]), 1
            ),
        },
        "by_type": [{"type": name, "count": count} for name, count in sorted(by_type.items())],
        "workouts": workouts if output_format == "detailed" else [_compact_workout(item) for item in workouts],
    }
    status = "empty" if not workouts else "ok" if complete else "partial"
    highlights = (
        [f"请求范围内找到{matching_workout_count}次锻炼，当前返回{len(workouts)}次，覆盖{len(workout_dates)}天"]
        if workouts
        else ["请求范围内没有已上传的锻炼"]
    )
    if by_type:
        highlights.append("锻炼类型：" + "、".join(f"{name}{count}次" for name, count in by_type.items()))
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {
            "start_date": start_day.isoformat(),
            "end_date": end_day.isoformat(),
            "days": days,
            "workout_type": workout_type,
            "limit": limit,
        },
        "actual_data_range": (
            {"start": workouts[-1]["start"], "end": workouts[0].get("end") or workouts[0]["start"]}
            if workouts
            else None
        ),
        "summary": summary,
        "comparison_to_personal_baseline": {
            "baseline_window": "previous_equal_length_period",
            "baseline_workouts": len(previous),
            "current_average": current_averages,
            "baseline_average": previous_averages,
            "strength": _strength(len(previous)),
        },
        "highlights": highlights[:3],
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_days": len(workout_dates),
            "missing_days": None,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sources,
            "derived_fields": sorted({field for item in quality for field in item["derived_fields"]}),
            "missing_fields": sorted({field for item in quality for field in item["missing_fields"]}),
            "quality_flags": sorted({flag for item in quality for flag in item["quality_flags"]}),
            "coverage_note": "没有锻炼的日期不等于缺失数据，因此missing_days为null。",
            "truncated": truncated,
        },
    }


def get_workout_detail(
    db_path: Path,
    workout_id: str,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    _validate_output_format(output_format)
    if not workout_id.strip():
        raise ValueError("workout_id must not be empty; call list_workouts to obtain one")
    zone = _zone(timezone)
    rows = _workout_rows(db_path, workout_id=workout_id)
    if not rows:
        return {
            "status": "empty",
            "timezone": zone.key,
            "requested_range": {"workout_id": workout_id},
            "actual_data_range": None,
            "summary": {},
            "comparison_to_personal_baseline": {"baseline_workouts": 0, "strength": "insufficient_data"},
            "highlights": ["未找到该workout_id；请先调用AppleHealth:list_workouts获取有效ID"],
            "fetch": {"complete": False},
            "data_quality": {
                "complete": False,
                "valid_days": 0,
                "missing_days": None,
                "latest_ingestion": _latest_ingestion(db_path, zone),
                "sources": [],
                "derived_fields": [],
                "missing_fields": ["workout_id"],
            },
        }
    workout = _workout_summary(rows[0], zone, output_format == "detailed")
    quality = workout.pop("_quality")
    target_start = _parse_datetime(rows[0].get("start_time"))
    prior_rows = _workout_rows(db_path, end=target_start) if target_start else []
    prior = [
        _workout_summary(row, zone)
        for row in prior_rows
        if str(row.get("event_type", "")).casefold() == str(workout.get("type", "")).casefold()
    ][:5]
    for item in prior:
        item.pop("_quality", None)
    baseline = _workout_averages(prior)
    complete = quality["complete"]
    return {
        "status": "ok" if complete else "partial",
        "timezone": zone.key,
        "requested_range": {"workout_id": workout_id},
        "actual_data_range": {"start": workout["start"], "end": workout["end"]},
        "summary": {"workout": workout},
        "comparison_to_personal_baseline": {
            "baseline_type": "previous_same_type_workouts",
            "baseline_workouts": len(prior),
            "baseline_average": baseline,
            "metric_comparison": _metric_differences(workout, baseline),
            "strength": _strength(len(prior)),
        },
        "highlights": _workout_highlights(workout, len(prior)),
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_days": 1,
            "missing_days": 0,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": [workout["source"]] if workout.get("source") else [],
            "derived_fields": quality["derived_fields"],
            "missing_fields": quality["missing_fields"],
            "quality_flags": quality["quality_flags"],
            "raw_samples_returned": False,
        },
    }


def compare_workouts(
    db_path: Path,
    workout_id: str,
    comparison_workout_id: str | None = None,
    baseline_count: int = 5,
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    if baseline_count not in {3, 5, 10}:
        raise ValueError("baseline_count must be one of 3, 5, or 10")
    zone = _zone(timezone)
    rows = _workout_rows(db_path, workout_id=workout_id)
    if not rows:
        return get_workout_detail(db_path, workout_id, timezone)
    target = _workout_summary(rows[0], zone)
    target_quality = target.pop("_quality")

    comparison_type = "specific_workout" if comparison_workout_id else "previous_same_type_workouts"
    comparison_workouts: list[dict[str, Any]] = []
    if comparison_workout_id:
        comparison_rows = _workout_rows(db_path, workout_id=comparison_workout_id)
        if not comparison_rows:
            raise ValueError(
                "comparison_workout_id was not found; call list_workouts to obtain a valid workout ID"
            )
        comparison_workouts = [_workout_summary(comparison_rows[0], zone)]
    else:
        target_start = _parse_datetime(rows[0].get("start_time"))
        prior_rows = _workout_rows(db_path, end=target_start) if target_start else []
        comparison_workouts = [
            _workout_summary(row, zone)
            for row in prior_rows
            if str(row.get("event_type", "")).casefold() == str(target.get("type", "")).casefold()
        ][:baseline_count]
    comparison_quality = [item.pop("_quality") for item in comparison_workouts]
    baseline = _workout_averages(comparison_workouts)
    complete = target_quality["complete"] and all(item["complete"] for item in comparison_quality)
    enough_baseline = bool(comparison_workouts) and (comparison_workout_id is not None or len(comparison_workouts) >= 3)
    status = "ok" if complete and enough_baseline else "partial"
    highlights = _workout_highlights(target, None if comparison_workout_id else len(comparison_workouts))
    if comparison_workout_id:
        highlights.append("已与指定的另一场锻炼进行机械指标比较")
    elif comparison_workouts:
        highlights.append(f"已与此前{len(comparison_workouts)}次同类锻炼的个人均值比较")
    else:
        highlights.append("没有更早的同类锻炼可用于比较")
    actual_starts = [item.get("start") for item in [target, *comparison_workouts] if item.get("start")]
    quality_items = [target_quality, *comparison_quality]
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {
            "workout_id": workout_id,
            "comparison_workout_id": comparison_workout_id,
            "baseline_count": baseline_count,
        },
        "actual_data_range": (
            {"start": min(actual_starts), "end": max(actual_starts)} if actual_starts else None
        ),
        "summary": {
            "target_workout": _compact_workout(target),
            "comparison_type": comparison_type,
            "comparison_workouts": [_compact_workout(item) for item in comparison_workouts],
            "metric_comparison": _metric_differences(target, baseline),
        },
        "comparison_to_personal_baseline": {
            "baseline_type": comparison_type,
            "baseline_workouts": len(comparison_workouts),
            "baseline_average": baseline,
            "strength": "direct" if comparison_workout_id else _strength(len(comparison_workouts)),
        },
        "highlights": highlights[:3],
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_days": len({value[:10] for value in actual_starts}),
            "missing_days": None,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sorted(
                {str(item["source"]) for item in [target, *comparison_workouts] if item.get("source")}
            ),
            "derived_fields": sorted({field for item in quality_items for field in item["derived_fields"]}),
            "missing_fields": sorted({field for item in quality_items for field in item["missing_fields"]}),
            "quality_flags": sorted({flag for item in quality_items for flag in item["quality_flags"]}),
            "raw_samples_returned": False,
        },
    }


def _workouts_for_day(db_path: Path, day: Date, zone: ZoneInfo) -> list[dict[str, Any]]:
    start, end = _day_bounds(day, zone)
    workouts = [_workout_summary(row, zone) for row in reversed(_workout_rows(db_path, start, end))]
    for workout in workouts:
        workout.pop("_quality", None)
    return [_compact_workout(workout) for workout in workouts]


def _activity_baseline(
    db_path: Path,
    target: Date,
    zone: ZoneInfo,
    lookback_days: int = 14,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    days = [target - timedelta(days=offset) for offset in range(lookback_days, 0, -1)]
    values = [_activity_day_values(db_path, day, zone) for day in days]
    valid = [item for item in values if any(value is not None for key, value in item.items() if key != "date")]
    baseline = {
        "baseline_days": len(valid),
        "baseline_window": f"previous_{lookback_days}_days",
        "step_count_average": _round(_mean([_number(item["step_count"]) for item in valid]), 0),
        "walking_running_distance_km_average": _round(
            _mean([_number(item["walking_running_distance_km"]) for item in valid]), 2
        ),
        "active_energy_kcal_average": _round(_mean([_number(item["active_energy_kcal"]) for item in valid]), 1),
        "exercise_minutes_average": _round(_mean([_number(item["exercise_minutes"]) for item in valid]), 1),
        "strength": _strength(len(valid)),
    }
    return valid, baseline


def get_daily_activity(
    db_path: Path,
    date: str,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    _validate_output_format(output_format)
    target = _parse_date(date)
    zone = _zone(timezone)
    activity = _activity_day_values(db_path, target, zone)
    workouts = _workouts_for_day(db_path, target, zone)
    valid_fields = [key for key, value in activity.items() if key != "date" and value is not None]
    missing_fields = [key for key, value in activity.items() if key != "date" and value is None]
    _, baseline = _activity_baseline(db_path, target, zone)
    comparison = dict(baseline)
    comparison["step_count_difference"] = (
        round(activity["step_count"] - baseline["step_count_average"])
        if activity["step_count"] is not None and baseline["step_count_average"] is not None
        else None
    )
    comparison["active_energy_difference_kcal"] = (
        _round(activity["active_energy_kcal"] - baseline["active_energy_kcal_average"], 1)
        if activity["active_energy_kcal"] is not None and baseline["active_energy_kcal_average"] is not None
        else None
    )
    summary: dict[str, Any] = {"activity": activity, "workouts": workouts}
    if output_format == "detailed":
        summary["units"] = {
            "walking_running_distance_km": "km",
            "cycling_distance_km": "km",
            "active_energy_kcal": "kcal_converted_from_kJ",
            "exercise_minutes": "min",
            "daylight_minutes": "min",
        }
    workout_capability = capabilities(db_path)["workout_session_summary"]
    complete = bool(valid_fields) and not missing_fields and workout_capability
    status = "empty" if not valid_fields else "ok" if complete else "partial"
    highlights: list[str] = []
    if activity["step_count"] is not None:
        highlights.append(f"当天记录约{activity['step_count']}步")
    if activity["exercise_minutes"] is not None:
        highlights.append(f"锻炼时间约{activity['exercise_minutes']}分钟")
    if not workouts:
        highlights.append("当前未上传可关联的锻炼会话")
    start, end = _day_bounds(target, zone)
    sources = sorted(
        {
            row["source"]
            for metric in (
                "step_count",
                "walking_running_distance",
                "cycling_distance",
                "active_energy",
                "apple_exercise_time",
                "apple_stand_hour",
                "time_in_daylight",
            )
            for row in _metric_rows(db_path, metric, start, end)
            if row.get("source")
        }
    )
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {"date": target.isoformat()},
        "actual_data_range": {"date": target.isoformat()} if valid_fields else None,
        "summary": summary,
        "comparison_to_personal_baseline": comparison,
        "highlights": highlights[:3],
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_days": 1 if valid_fields else 0,
            "missing_days": 0 if valid_fields else 1,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sources,
            "derived_fields": ["summary.activity.active_energy_kcal"],
            "missing_fields": missing_fields + ([] if workouts else ["workouts"]),
        },
    }


def get_activity_trend(
    db_path: Path,
    days: int = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    if days not in {7, 14, 30}:
        raise ValueError("days must be one of 7, 14, or 30")
    zone = _zone(timezone)
    end = _parse_date(end_date) if end_date else datetime.now(zone).date()
    start = end - timedelta(days=days - 1)
    requested_days = _date_range(start, end)
    daily = [_activity_day_values(db_path, day, zone) for day in requested_days]
    valid = [item for item in daily if any(value is not None for key, value in item.items() if key != "date")]
    missing = days - len(valid)
    previous_start = start - timedelta(days=days)
    previous_days = _date_range(previous_start, start - timedelta(days=1))
    previous = [_activity_day_values(db_path, day, zone) for day in previous_days]
    previous_valid = [
        item for item in previous if any(value is not None for key, value in item.items() if key != "date")
    ]

    def averages(items: list[dict[str, Any]]) -> dict[str, float | None]:
        return {
            "step_count": _round(_mean([_number(item["step_count"]) for item in items]), 0),
            "walking_running_distance_km": _round(
                _mean([_number(item["walking_running_distance_km"]) for item in items]), 2
            ),
            "active_energy_kcal": _round(_mean([_number(item["active_energy_kcal"]) for item in items]), 1),
            "exercise_minutes": _round(_mean([_number(item["exercise_minutes"]) for item in items]), 1),
            "stand_hours": _round(_mean([_number(item["stand_hours"]) for item in items]), 1),
            "daylight_minutes": _round(_mean([_number(item["daylight_minutes"]) for item in items]), 1),
        }

    current_average = averages(valid)
    baseline_average = averages(previous_valid)
    summary: dict[str, Any] = {
        "requested_days": days,
        "valid_days": len(valid),
        "missing_days": missing,
        "daily_average": current_average,
        "period_totals": {
            "step_count": round(_sum_optional([_number(item["step_count"]) for item in valid]) or 0)
            if valid
            else None,
            "walking_running_distance_km": _round(
                _sum_optional([_number(item["walking_running_distance_km"]) for item in valid]), 2
            ),
            "active_energy_kcal": _round(
                _sum_optional([_number(item["active_energy_kcal"]) for item in valid]), 1
            ),
            "exercise_minutes": round(_sum_optional([_number(item["exercise_minutes"]) for item in valid]) or 0)
            if valid
            else None,
        },
    }
    if output_format == "detailed":
        summary["days"] = daily
    comparison = {
        "baseline_days": len(previous_valid),
        "baseline_window": "previous_equal_length_period",
        "baseline_daily_average": baseline_average,
        "step_count_average_difference": (
            round(current_average["step_count"] - baseline_average["step_count"])
            if current_average["step_count"] is not None and baseline_average["step_count"] is not None
            else None
        ),
        "strength": _strength(len(previous_valid)),
    }
    status = "empty" if not valid else "ok" if missing == 0 else "partial"
    highlights: list[str] = []
    if current_average["step_count"] is not None:
        highlights.append(f"{len(valid)}个有效日平均约{round(current_average['step_count'])}步")
    if len(valid) < 3:
        highlights.append(f"请求{days}天但只有{len(valid)}天有效数据，趋势判断为样本不足")
    elif missing:
        highlights.append(f"请求范围内缺少{missing}天数据")
    period_start, _ = _day_bounds(start, zone)
    _, period_end = _day_bounds(end, zone)
    sources = sorted(
        {
            row["source"]
            for metric in (
                "step_count",
                "walking_running_distance",
                "cycling_distance",
                "active_energy",
                "apple_exercise_time",
                "apple_stand_hour",
                "time_in_daylight",
            )
            for row in _metric_rows(db_path, metric, period_start, period_end)
            if row.get("source")
        }
    )
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {"start_date": start.isoformat(), "end_date": end.isoformat(), "days": days},
        "actual_data_range": (
            {"start_date": valid[0]["date"], "end_date": valid[-1]["date"]} if valid else None
        ),
        "summary": summary,
        "comparison_to_personal_baseline": comparison,
        "highlights": highlights[:3],
        "fetch": {"complete": missing == 0},
        "data_quality": {
            "complete": missing == 0,
            "valid_days": len(valid),
            "missing_days": missing,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sources,
            "derived_fields": ["summary.daily_average", "summary.period_totals.active_energy_kcal"],
            "missing_fields": [],
        },
    }


def _recovery_values_for_session(
    db_path: Path,
    session: dict[str, Any],
    zone: ZoneInfo,
) -> dict[str, float | None]:
    recovery = _recovery_for_window(db_path, session.get("sleep_start"), session.get("sleep_end"))
    wake_date = _parse_date(session["sleep_date"])
    recovery["resting_heart_rate_bpm"] = _round(
        _daily_average(db_path, "resting_heart_rate", wake_date, zone), 1
    )
    return recovery


def get_recovery_snapshot(
    db_path: Path,
    date: str | None = None,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    _validate_output_format(output_format)
    zone = _zone(timezone)
    sessions = _sleep_sessions(db_path, zone)
    if date:
        target_date = _parse_date(date).isoformat()
    else:
        target_date = sessions[-1]["sleep_date"] if sessions else datetime.now(zone).date().isoformat()
    session = next((item for item in sessions if item["sleep_date"] == target_date), None)
    if session is None:
        return {
            "status": "empty",
            "timezone": zone.key,
            "requested_range": {"date": target_date},
            "actual_data_range": None,
            "summary": {},
            "comparison_to_personal_baseline": {"baseline_nights": 0, "strength": "insufficient_data"},
            "highlights": ["该日期没有可用于恢复快照的睡眠记录"],
            "fetch": {"complete": False},
            "data_quality": {
                "complete": False,
                "valid_days": 0,
                "missing_days": 1,
                "latest_ingestion": _latest_ingestion(db_path, zone),
                "sources": [],
                "derived_fields": [],
                "missing_fields": ["sleep_record"],
            },
        }
    current = _recovery_values_for_session(db_path, session, zone)
    baseline_sessions = _sleep_baseline(sessions, target_date)
    baseline_values = [_recovery_values_for_session(db_path, item, zone) for item in baseline_sessions]
    comparisons: dict[str, Any] = {}
    for key, value in current.items():
        baseline = _mean([_number(item.get(key)) for item in baseline_values])
        comparisons[key] = {
            "current": value,
            "baseline_average": _round(baseline, 1),
            "difference": _round(value - baseline, 1) if value is not None and baseline is not None else None,
        }
    missing_fields = [key for key, value in current.items() if value is None]
    sleep_summary = {
        "sleep_date": target_date,
        "total_sleep_minutes": session.get("total_sleep_minutes"),
        "deep_minutes": session.get("deep_minutes"),
        "rem_minutes": session.get("rem_minutes"),
        "awake_minutes": session.get("awake_minutes"),
    }
    summary: dict[str, Any] = {"sleep": sleep_summary, "recovery_metrics": current}
    if output_format == "detailed":
        summary["metric_windows"] = {
            "sleep_start": _local_iso(session.get("sleep_start"), zone),
            "sleep_end": _local_iso(session.get("sleep_end"), zone),
            "hrv_near_wake_window": "wake_time_minus_2h_to_plus_2h",
            "wrist_temperature_type": "absolute_sleeping_wrist_temperature",
        }
    highlights = _sleep_highlights(session, len(baseline_sessions))[:2]
    if missing_fields:
        highlights.append(f"恢复指标仍缺少{len(missing_fields)}项，结果为部分数据")
    complete = not missing_fields and not session["quality_flags"]
    return {
        "status": "ok" if complete else "partial",
        "timezone": zone.key,
        "requested_range": {"date": target_date},
        "actual_data_range": {
            "start": _local_iso(session.get("sleep_start"), zone),
            "end": _local_iso(session.get("sleep_end"), zone),
        },
        "summary": summary,
        "comparison_to_personal_baseline": {
            "baseline_nights": len(baseline_sessions),
            "strength": _strength(len(baseline_sessions)),
            "metrics": comparisons,
        },
        "highlights": highlights[:3],
        "fetch": {"complete": complete},
        "data_quality": {
            "complete": complete,
            "valid_days": 1,
            "missing_days": 0,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": [session["source"]] if session.get("source") else [],
            "derived_fields": session["derived_fields"],
            "missing_fields": missing_fields,
        },
    }


def _period_stats(
    db_path: Path,
    metric_name: str,
    start: datetime,
    end: datetime,
    zone: ZoneInfo,
    include_total: bool = False,
    include_median: bool = False,
) -> dict[str, Any] | None:
    rows = _metric_rows(db_path, metric_name, start, end)
    if not rows:
        return None
    averages = [_number(row.get("avg_value")) or _number(row.get("numeric_value")) for row in rows]
    minima = [_number(row.get("min_value")) or _number(row.get("numeric_value")) for row in rows]
    maxima = [_number(row.get("max_value")) or _number(row.get("numeric_value")) for row in rows]
    values = [_row_value(row) for row in rows]
    latest = max(rows, key=lambda row: row["start_time"])
    result: dict[str, Any] = {
        "average": _round(_mean(averages), 1),
        "minimum": _round(min([value for value in minima if value is not None]), 1)
        if any(value is not None for value in minima)
        else None,
        "maximum": _round(max([value for value in maxima if value is not None]), 1)
        if any(value is not None for value in maxima)
        else None,
        "latest": _round(_row_value(latest), 1),
        "latest_time": _local_iso(_parse_datetime(latest["start_time"]), zone),
        "unit": latest["unit"],
        "sample_count": len(rows),
    }
    if include_total:
        result["total"] = _round(_sum_optional(values), 1)
    if include_median:
        result["median"] = _round(_median(values), 1)
    return result


def semantic_health_overview(
    db_path: Path,
    days: int = 7,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    end_date: str | None = None,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    if days not in {7, 14, 30}:
        raise ValueError("days must be one of 7, 14, or 30")
    zone = _zone(timezone)
    end_day = _parse_date(end_date) if end_date else datetime.now(zone).date()
    start_day = end_day - timedelta(days=days - 1)
    start, _ = _day_bounds(start_day, zone)
    _, end = _day_bounds(end_day, zone)
    activity = get_activity_trend(db_path, days, timezone, output_format="concise", end_date=end_day.isoformat())
    sleep = get_sleep_trend(db_path, days, timezone, output_format="concise", end_date=end_day.isoformat())
    recovery = get_recovery_snapshot(db_path, timezone=timezone, output_format="concise")
    def select(stats: dict[str, Any] | None, fields: tuple[str, ...]) -> dict[str, Any] | None:
        return {field: stats.get(field) for field in fields} if stats else None

    heart_rate = _period_stats(db_path, "heart_rate", start, end, zone)
    resting_heart_rate = _period_stats(db_path, "resting_heart_rate", start, end, zone)
    hrv = _period_stats(db_path, "heart_rate_variability", start, end, zone, include_median=True)
    walking_heart_rate = _period_stats(db_path, "walking_heart_rate_average", start, end, zone)
    cardiovascular = {
        "heart_rate": select(
            heart_rate, ("average", "minimum", "maximum", "latest", "latest_time", "unit")
        ),
        "resting_heart_rate": select(
            resting_heart_rate, ("average", "latest", "latest_time", "unit")
        ),
        "heart_rate_variability": select(
            hrv, ("average", "median", "latest", "latest_time", "unit")
        ),
        "walking_heart_rate_average": select(
            walking_heart_rate, ("average", "latest", "latest_time", "unit")
        ),
    }
    respiratory_rate = _period_stats(db_path, "respiratory_rate", start, end, zone)
    oxygen = _period_stats(db_path, "blood_oxygen_saturation", start, end, zone)
    oxygen_selected = select(
        oxygen, ("average", "minimum", "maximum", "latest", "latest_time", "unit")
    )
    if oxygen_selected:
        for key in ("average", "minimum", "maximum", "latest"):
            oxygen_selected[key] = _round(_normalize_percent(_number(oxygen_selected[key])), 1)
    respiratory = {
        "respiratory_rate": select(
            respiratory_rate, ("average", "minimum", "maximum", "latest", "latest_time", "unit")
        ),
        "blood_oxygen_saturation": oxygen_selected,
    }
    wrist = _period_stats(db_path, "apple_sleeping_wrist_temperature", start, end, zone)
    audio = _period_stats(db_path, "environmental_audio_exposure", start, end, zone)
    wrist_selected = select(wrist, ("average", "latest", "latest_time", "unit"))
    if wrist_selected:
        wrist_selected["measurement_type"] = "absolute_sleeping_wrist_temperature"
    body_environment = {
        "wrist_temperature": wrist_selected,
        "environmental_audio_exposure": select(audio, ("average", "maximum", "latest_time", "unit")),
    }
    summary: dict[str, Any] = {
        "activity": activity["summary"],
        "cardiovascular": cardiovascular,
        "respiratory": respiratory,
        "sleep": sleep["summary"],
        "body_environment": body_environment,
        "latest_recovery_snapshot": recovery["summary"] if recovery["status"] != "empty" else None,
    }
    if output_format == "detailed":
        summary["coverage_details"] = {
            "activity": activity["data_quality"],
            "sleep": sleep["data_quality"],
            "recovery": recovery["data_quality"],
        }
    valid_days = activity["data_quality"]["valid_days"]
    valid_nights = sleep["data_quality"]["valid_nights"]
    missing_days = days - min(valid_days, days)
    status = "empty" if valid_days == 0 and valid_nights == 0 else "ok" if missing_days == 0 else "partial"
    highlights = (sleep.get("highlights", []) + activity.get("highlights", []))[:5]
    return {
        "status": status,
        "timezone": zone.key,
        "requested_range": {"start_date": start_day.isoformat(), "end_date": end_day.isoformat(), "days": days},
        "actual_data_range": {
            "activity": activity["actual_data_range"],
            "sleep": sleep["actual_data_range"],
        },
        "summary": summary,
        "comparison_to_personal_baseline": {
            "activity": activity["comparison_to_personal_baseline"],
            "sleep": sleep["comparison_to_personal_baseline"],
            "recovery": recovery["comparison_to_personal_baseline"],
        },
        "highlights": highlights,
        "fetch": {"complete": activity["fetch"]["complete"] and sleep["fetch"]["complete"]},
        "data_quality": {
            "complete": activity["fetch"]["complete"] and sleep["fetch"]["complete"],
            "valid_days": valid_days,
            "missing_days": missing_days,
            "valid_nights": valid_nights,
            "missing_nights": sleep["data_quality"]["missing_nights"],
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sorted(set(activity["data_quality"].get("sources", [])) | set(sleep["data_quality"].get("sources", []))),
            "derived_fields": ["activity.active_energy_kcal"],
            "missing_fields": [],
        },
    }


def list_available_data_semantic(
    db_path: Path,
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    zone = _zone(timezone)
    initialize_database(db_path)
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT metric_name, unit, COUNT(*) AS sample_count,
                   MIN(start_time) AS first_time,
                   MAX(COALESCE(end_time, start_time)) AS last_time
            FROM metric_samples
            GROUP BY metric_name, unit
            ORDER BY metric_name COLLATE NOCASE
            """
        ).fetchall()
        events = connection.execute(
            """
            SELECT category, COUNT(*) AS event_count,
                   MIN(start_time) AS first_time,
                   MAX(COALESCE(end_time, start_time)) AS last_time
            FROM health_events
            GROUP BY category ORDER BY category COLLATE NOCASE
            """
        ).fetchall()
    metrics: list[dict[str, Any]] = []
    for row in rows:
        item = {
            "metric_name": row["metric_name"],
            "unit": row["unit"],
            "sample_count": row["sample_count"],
            "last_time": _local_iso(_parse_datetime(row["last_time"]), zone),
        }
        if output_format == "detailed":
            item["first_time"] = _local_iso(_parse_datetime(row["first_time"]), zone)
        metrics.append(item)
    event_categories: list[dict[str, Any]] = []
    for row in events:
        item = {"category": row["category"], "event_count": row["event_count"]}
        if output_format == "detailed":
            item.update(
                {
                    "first_time": _local_iso(_parse_datetime(row["first_time"]), zone),
                    "last_time": _local_iso(_parse_datetime(row["last_time"]), zone),
                }
            )
        event_categories.append(item)
    return {
        "status": "ok" if metrics or event_categories else "empty",
        "timezone": zone.key,
        "role": "fallback_discovery_and_upload_debugging",
        "capabilities": capabilities(db_path),
        "summary": {
            "metric_sample_count": sum(item["sample_count"] for item in metrics),
            "metric_name_count": len(metrics),
            "event_category_count": len(event_categories),
            "metrics": metrics,
            "event_categories": event_categories,
        },
        "latest_ingestion": _latest_ingestion(db_path, zone),
    }


def _query_date(value: str | None, zone: ZoneInfo, default: Date) -> Date:
    if not value:
        return default
    if len(value.strip()) == 10:
        return _parse_date(value.strip())
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError("time must be ISO-8601, for example 2026-07-18 or 2026-07-18T10:00:00+08:00")
    return parsed.astimezone(zone).date()


def query_sleep_fallback(
    db_path: Path,
    metric_names: list[str],
    start_time: str | None = None,
    end_time: str | None = None,
    timezone: str = DEFAULT_TIMEZONE,
    output_format: Literal["concise", "detailed"] = "concise",
    limit: int = 200,
) -> dict[str, Any]:
    _validate_output_format(output_format)
    zone = _zone(timezone)
    requested_lower = {name.lower() for name in metric_names}
    timeline_requested = any(
        token in name
        for name in requested_lower
        for token in ("timeline", "interval", "awaken", "waso", "cycle")
    )
    if timeline_requested:
        return {
            "status": "partial",
            "reason": "sleep_stage_timeline_not_uploaded",
            "requested_range": {"start_time": start_time, "end_time": end_time},
            "actual_data_range": None,
            "summary": {
                "available": ["nightly_sleep_totals", "nightly_stage_totals"],
                "missing": ["stage_intervals"],
            },
            "comparison_to_personal_baseline": {},
            "highlights": ["当前只上传了每晚阶段总量，没有睡眠阶段区间时间线"],
            "capabilities": capabilities(db_path),
            "fetch": {"complete": False},
            "data_quality": {
                "complete": False,
                "valid_nights": 0,
                "missing_nights": None,
                "latest_ingestion": _latest_ingestion(db_path, zone),
                "sources": [],
                "derived_fields": [],
                "missing_fields": ["stage_intervals"],
            },
        }
    today = datetime.now(zone).date()
    start_date = _query_date(start_time, zone, today - timedelta(days=6))
    end_date = _query_date(end_time, zone, today)
    if end_date < start_date:
        raise ValueError("end_time must not be earlier than start_time")
    sessions = [
        item
        for item in _sleep_sessions(db_path, zone)
        if start_date.isoformat() <= item["sleep_date"] <= end_date.isoformat()
    ][-limit:]
    if output_format == "detailed":
        records = [
            {
                "sleep_date": item["sleep_date"],
                "session_id": item.get("session_id"),
                "source": item.get("source"),
                "start": _local_iso(item.get("sleep_start"), zone),
                "end": _local_iso(item.get("sleep_end"), zone),
                "payload": item.get("payload"),
                "derived_metrics": {
                    "total_sleep_minutes": item.get("total_sleep_minutes"),
                    "awake_minutes": item.get("awake_minutes"),
                    "core_minutes": item.get("core_minutes"),
                    "deep_minutes": item.get("deep_minutes"),
                    "rem_minutes": item.get("rem_minutes"),
                    "in_bed_minutes": item.get("in_bed_minutes"),
                },
            }
            for item in sessions
        ]
    else:
        records = [
            {
                "sleep_date": item["sleep_date"],
                "start": _local_iso(item.get("sleep_start"), zone),
                "end": _local_iso(item.get("sleep_end"), zone),
                "total_sleep_minutes": item.get("total_sleep_minutes"),
                "awake_minutes": item.get("awake_minutes"),
                "stages": {
                    "unclassified_asleep_minutes": item.get("unclassified_asleep_minutes"),
                    "core_minutes": item.get("core_minutes"),
                    "deep_minutes": item.get("deep_minutes"),
                    "rem_minutes": item.get("rem_minutes"),
                },
            }
            for item in sessions
        ]
    requested_nights = (end_date - start_date).days + 1
    valid_nights = len(records)
    return {
        "status": "empty" if not records else "ok" if valid_nights == requested_nights else "partial",
        "timezone": zone.key,
        "requested_range": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        "actual_data_range": (
            {"start_date": records[0]["sleep_date"], "end_date": records[-1]["sleep_date"]}
            if records
            else None
        ),
        "summary": {"sleep_nights": records},
        "comparison_to_personal_baseline": {"strength": _strength(valid_nights)},
        "highlights": ([f"返回{valid_nights}晚合并后的睡眠对象"] if records else ["请求范围没有睡眠记录"]),
        "capabilities": capabilities(db_path),
        "fetch": {"complete": valid_nights == requested_nights},
        "data_quality": {
            "complete": valid_nights == requested_nights,
            "valid_nights": valid_nights,
            "missing_nights": requested_nights - valid_nights,
            "latest_ingestion": _latest_ingestion(db_path, zone),
            "sources": sorted({item["source"] for item in sessions if item.get("source")}),
            "derived_fields": ["summary.sleep_nights[].stages"],
            "missing_fields": [],
            "has_stage_timeline": False,
        },
    }


def localize_fallback_metric_result(result: dict[str, Any], timezone: str = DEFAULT_TIMEZONE) -> dict[str, Any]:
    zone = _zone(timezone)
    query = result.get("query", {})
    for key in ("start_time", "end_time"):
        if query.get(key):
            query[key] = _local_iso(_parse_datetime(query[key]), zone)
    for record in result.get("records", []):
        for key in ("start_time", "end_time", "latest_time"):
            if record.get(key):
                record[key] = _local_iso(_parse_datetime(record[key]), zone)
        bucket = record.get("bucket")
        if isinstance(bucket, str) and "T" in bucket:
            record["bucket"] = _local_iso(_parse_datetime(bucket), zone)
    result["timezone"] = zone.key
    result["role"] = "fallback_raw_metric_query"
    return result
