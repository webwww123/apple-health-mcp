from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Asia/Shanghai"
CUMULATIVE_METRICS = frozenset(
    {
        "step_count",
        "walking_running_distance",
        "cycling_distance",
        "active_energy",
        "basal_energy_burned",
        "apple_exercise_time",
        "apple_stand_time",
        "apple_stand_hour",
        "time_in_daylight",
    }
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    session_id TEXT,
    received_at TEXT NOT NULL,
    automation_name TEXT,
    automation_id TEXT,
    automation_aggregation TEXT,
    automation_period TEXT,
    content_sha256 TEXT NOT NULL,
    payload_size INTEGER NOT NULL,
    metric_sample_count INTEGER NOT NULL DEFAULT 0,
    event_count INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingestion_id INTEGER NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
    metric_name TEXT NOT NULL,
    unit TEXT,
    start_time TEXT,
    end_time TEXT,
    source TEXT,
    source_name TEXT,
    source_bundle_id TEXT,
    device_name TEXT,
    numeric_value REAL,
    min_value REAL,
    max_value REAL,
    avg_value REAL,
    sum_value REAL,
    value_text TEXT,
    sample_uuid TEXT,
    original_record_id TEXT,
    fingerprint TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_samples_name_time
    ON metric_samples(metric_name, start_time);
CREATE INDEX IF NOT EXISTS idx_metric_samples_time
    ON metric_samples(start_time);

CREATE TABLE IF NOT EXISTS metric_sample_duplicates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archived_at TEXT NOT NULL,
    archive_reason TEXT NOT NULL,
    survivor_fingerprint TEXT,
    original_metric_sample_id INTEGER NOT NULL,
    ingestion_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    unit TEXT,
    start_time TEXT,
    end_time TEXT,
    source TEXT,
    source_name TEXT,
    source_bundle_id TEXT,
    device_name TEXT,
    numeric_value REAL,
    min_value REAL,
    max_value REAL,
    avg_value REAL,
    sum_value REAL,
    value_text TEXT,
    sample_uuid TEXT,
    original_record_id TEXT,
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(original_metric_sample_id, fingerprint, archive_reason)
);

CREATE INDEX IF NOT EXISTS idx_metric_sample_duplicates_name_time
    ON metric_sample_duplicates(metric_name, start_time);

CREATE TABLE IF NOT EXISTS deduplication_audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingestion_id INTEGER NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    event_type TEXT,
    start_time TEXT,
    end_time TEXT,
    source TEXT,
    value_text TEXT,
    event_uuid TEXT,
    fingerprint TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_health_events_category_time
    ON health_events(category, start_time);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def initialize_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with connect(db_path) as connection:
        connection.executescript(SCHEMA)
        _ensure_schema_migrations(connection)


def _ensure_schema_migrations(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(metric_samples)").fetchall()
    }
    additions = {
        "source_name": "TEXT",
        "source_bundle_id": "TEXT",
        "device_name": "TEXT",
        "original_record_id": "TEXT",
    }
    for name, column_type in additions.items():
        if name not in columns:
            connection.execute(f"ALTER TABLE metric_samples ADD COLUMN {name} {column_type}")
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_metric_samples_original_record
        ON metric_samples(metric_name, original_record_id)
        WHERE original_record_id IS NOT NULL
        """
    )


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: str | bytes) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def _normalize_time(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    try:
        if candidate.endswith("Z"):
            parsed = datetime.fromisoformat(candidate[:-1] + "+00:00")
        else:
            parsed = datetime.fromisoformat(candidate)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    except ValueError:
        return candidate


def _zone(timezone: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Unknown timezone: {timezone}") from error


def normalize_query_time(
    value: str | None,
    *,
    end_of_day: bool = False,
    timezone: str = "UTC",
) -> str | None:
    if value is None or not value.strip():
        return None
    candidate = value.strip()
    if len(candidate) == 10:
        try:
            parsed = datetime.fromisoformat(candidate).replace(tzinfo=_zone(timezone))
        except ValueError as error:
            raise ValueError(
                "Time must be ISO-8601, for example 2026-07-18 or 2026-07-18T10:00:00+02:00"
            ) from error
        if end_of_day:
            parsed += timedelta(days=1)
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    try:
        parsed = datetime.fromisoformat(candidate[:-1] + "+00:00" if candidate.endswith("Z") else candidate)
    except ValueError as error:
        raise ValueError(
            "Time must be ISO-8601, for example 2026-07-18 or 2026-07-18T10:00:00+02:00"
        ) from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def default_time_window(days: int) -> tuple[str, str]:
    end = datetime.now(UTC)
    start = end - timedelta(days=days)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    lowered = {str(key).lower(): value for key, value in mapping.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is not None and value != "":
            return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        try:
            result = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return result if math.isfinite(result) else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return _compact_json(value)


def _canonical_source(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    parts = [part.strip() for part in re.split(r"\|", text) if part.strip()]
    if not parts:
        return None
    unique = sorted(set(parts), key=lambda item: (item.casefold(), item))
    return "|".join(unique)


def _device_name(sample: dict[str, Any]) -> str | None:
    value = _first(sample, "deviceName", "device_name", "device")
    if isinstance(value, dict):
        return _text(_first(value, "name", "model", "localIdentifier", "identifier"))
    return _text(value)


def _identity_number(value: float | None) -> str:
    return "" if value is None else format(value, ".15g")


def _metric_fingerprint(row: dict[str, Any]) -> str:
    metric_name = str(row["metric_name"]).strip().casefold()
    original_record_id = row.get("original_record_id")
    if original_record_id:
        identity = f"original_record_id:{original_record_id}"
    else:
        structured_source_identity = "|".join(
            value
            for value in (
                row.get("source_bundle_id"),
                row.get("device_name"),
            )
            if value
        )
        identity = "|".join(
            (
                f"start:{row.get('start_time') or ''}",
                f"end:{row.get('end_time') or ''}",
                f"unit:{str(row.get('unit') or '').strip().casefold()}",
                f"numeric:{_identity_number(row.get('numeric_value'))}",
                f"min:{_identity_number(row.get('min_value'))}",
                f"max:{_identity_number(row.get('max_value'))}",
                f"avg:{_identity_number(row.get('avg_value'))}",
                f"sum:{_identity_number(row.get('sum_value'))}",
                f"text:{row.get('value_text') or ''}",
                f"structured_source:{structured_source_identity}",
            )
        )
    return _sha256(f"metric:{metric_name}:{identity}")


def _metric_rows(payload: Any) -> Iterable[dict[str, Any]]:
    if not isinstance(payload, dict):
        return
    data = payload.get("data")
    if not isinstance(data, dict):
        return
    metrics = data.get("metrics")
    if not isinstance(metrics, list):
        return

    for index, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            continue
        metric_name = _text(_first(metric, "name", "type", "identifier", "metric")) or f"metric_{index}"
        unit = _text(_first(metric, "units", "unit"))
        samples = metric.get("data")
        if not isinstance(samples, list):
            continue
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            raw = _compact_json(sample)
            sample_uuid = _text(_first(sample, "uuid", "id", "identifier"))
            is_sleep_summary = metric_name.lower() == "sleep_analysis" and any(
                key in sample for key in ("totalSleep", "asleep", "awake", "core", "deep", "rem", "inBed")
            )
            start_time = _normalize_time(
                _first(
                    sample,
                    *("sleepStart", "inBedStart", "date")
                    if is_sleep_summary
                    else ("startDate", "startTime", "start", "date", "timestamp"),
                )
            )
            end_time = _normalize_time(
                _first(
                    sample,
                    *("sleepEnd", "inBedEnd")
                    if is_sleep_summary
                    else ("endDate", "endTime", "end"),
                )
            )
            numeric_value = _number(_first(sample, "qty", "quantity", "numericValue", "value"))
            if numeric_value is None and is_sleep_summary:
                numeric_value = _number(_first(sample, "totalSleep"))
            source_name = _canonical_source(_first(sample, "sourceName", "source"))
            source_bundle_id = _text(
                _first(
                    sample,
                    "sourceBundleIdentifier",
                    "sourceBundleId",
                    "bundleIdentifier",
                    "bundleId",
                )
            )
            device_name = _device_name(sample)
            base_row = {
                "metric_name": metric_name,
                "unit": _text(_first(sample, "units", "unit")) or unit,
                "start_time": start_time,
                "end_time": end_time,
                "source": source_name,
                "source_name": source_name,
                "source_bundle_id": source_bundle_id,
                "device_name": device_name,
                "numeric_value": numeric_value,
                "min_value": _number(_first(sample, "min", "minimum", "minValue")),
                "max_value": _number(_first(sample, "max", "maximum", "maxValue")),
                "avg_value": _number(_first(sample, "avg", "average", "avgValue")),
                "sum_value": _number(_first(sample, "sum", "total", "sumValue")),
                "value_text": _text(_first(sample, "value", "status", "stage")),
                "sample_uuid": sample_uuid,
                "original_record_id": sample_uuid,
                "payload_json": raw,
            }
            base_row["fingerprint"] = _metric_fingerprint(base_row)
            yield base_row

            if is_sleep_summary:
                stage_fields = {
                    "totalSleep": "total_sleep",
                    "asleep": "asleep",
                    "awake": "awake",
                    "core": "core",
                    "deep": "deep",
                    "rem": "rem",
                    "inBed": "in_bed",
                }
                for source_key, suffix in stage_fields.items():
                    stage_value = _number(_first(sample, source_key))
                    if stage_value is None:
                        continue
                    derived_name = f"sleep_analysis.{suffix}"
                    derived_row = dict(base_row)
                    derived_row.update(
                        {
                            "metric_name": derived_name,
                            "numeric_value": stage_value,
                            "min_value": None,
                            "max_value": None,
                            "avg_value": None,
                            "sum_value": None,
                            "value_text": None,
                            "sample_uuid": f"{sample_uuid}:{suffix}" if sample_uuid else None,
                            "original_record_id": (
                                f"{sample_uuid}:{suffix}" if sample_uuid else None
                            ),
                        }
                    )
                    derived_row["fingerprint"] = _metric_fingerprint(derived_row)
                    yield derived_row


def _category_items(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item
        return
    if not isinstance(value, dict):
        return
    nested = value.get("data")
    if isinstance(nested, list):
        for item in nested:
            if isinstance(item, dict):
                merged = {key: val for key, val in value.items() if key != "data"}
                merged.update(item)
                yield merged
    else:
        yield value


def _event_rows(payload: Any) -> Iterable[dict[str, Any]]:
    if not isinstance(payload, dict):
        return
    data = payload.get("data")
    if not isinstance(data, dict):
        return

    for category, value in data.items():
        if category == "metrics" or not isinstance(value, (dict, list)):
            continue
        for item in _category_items(value):
            raw = _compact_json(item)
            event_uuid = _text(_first(item, "uuid", "id", "identifier"))
            event_type = _text(
                _first(
                    item,
                    "workoutActivityType",
                    "activityType",
                    "eventType",
                    "type",
                    "name",
                    "category",
                )
            )
            start_time = _normalize_time(_first(item, "startDate", "startTime", "start", "date", "timestamp"))
            end_time = _normalize_time(_first(item, "endDate", "endTime", "end"))
            source = _text(_first(item, "source", "sourceName"))
            bucket_time = _normalize_time(_first(item, "date"))
            if event_uuid:
                identity = f"uuid:{event_uuid}"
            elif bucket_time:
                identity = f"source:{source or ''}:type:{event_type or ''}:bucket:{bucket_time}"
            elif start_time or end_time:
                identity = (
                    f"source:{source or ''}:type:{event_type or ''}:"
                    f"start:{start_time or ''}:end:{end_time or ''}"
                )
            else:
                identity = f"raw:{_sha256(raw)}"
            yield {
                "category": category,
                "event_type": event_type,
                "start_time": start_time,
                "end_time": end_time,
                "source": source,
                "value_text": _text(_first(item, "value", "status", "classification", "name")),
                "event_uuid": event_uuid,
                "fingerprint": _sha256(f"event:{category}:{identity}"),
                "payload_json": raw,
            }


def _upsert_metric_row(connection: sqlite3.Connection, ingestion_id: int, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO metric_samples (
            ingestion_id, metric_name, unit, start_time, end_time, source,
            source_name, source_bundle_id, device_name,
            numeric_value, min_value, max_value, avg_value, sum_value,
            value_text, sample_uuid, original_record_id, fingerprint, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fingerprint) DO UPDATE SET
            ingestion_id = excluded.ingestion_id,
            unit = excluded.unit,
            start_time = excluded.start_time,
            end_time = excluded.end_time,
            source = excluded.source,
            source_name = excluded.source_name,
            source_bundle_id = excluded.source_bundle_id,
            device_name = excluded.device_name,
            numeric_value = excluded.numeric_value,
            min_value = excluded.min_value,
            max_value = excluded.max_value,
            avg_value = excluded.avg_value,
            sum_value = excluded.sum_value,
            value_text = excluded.value_text,
            sample_uuid = excluded.sample_uuid,
            original_record_id = excluded.original_record_id,
            payload_json = excluded.payload_json
        """,
        (
            ingestion_id,
            row["metric_name"],
            row["unit"],
            row["start_time"],
            row["end_time"],
            row["source"],
            row["source_name"],
            row["source_bundle_id"],
            row["device_name"],
            row["numeric_value"],
            row["min_value"],
            row["max_value"],
            row["avg_value"],
            row["sum_value"],
            row["value_text"],
            row["sample_uuid"],
            row["original_record_id"],
            row["fingerprint"],
            row["payload_json"],
        ),
    )


def _upsert_event_row(connection: sqlite3.Connection, ingestion_id: int, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO health_events (
            ingestion_id, category, event_type, start_time, end_time,
            source, value_text, event_uuid, fingerprint, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fingerprint) DO UPDATE SET
            ingestion_id = excluded.ingestion_id,
            event_type = excluded.event_type,
            start_time = excluded.start_time,
            end_time = excluded.end_time,
            source = excluded.source,
            value_text = excluded.value_text,
            event_uuid = excluded.event_uuid,
            payload_json = excluded.payload_json
        """,
        (
            ingestion_id,
            row["category"],
            row["event_type"],
            row["start_time"],
            row["end_time"],
            row["source"],
            row["value_text"],
            row["event_uuid"],
            row["fingerprint"],
            row["payload_json"],
        ),
    )


def _recount_ingestions(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        UPDATE ingestions
        SET metric_sample_count = (
                SELECT COUNT(*) FROM metric_samples WHERE ingestion_id = ingestions.id
            ),
            event_count = (
                SELECT COUNT(*) FROM health_events WHERE ingestion_id = ingestions.id
            )
        """
    )


def store_ingestion(
    db_path: Path,
    payload: Any,
    raw_json: str,
    request_headers: dict[str, str],
) -> dict[str, Any]:
    initialize_database(db_path)
    content_hash = _sha256(raw_json)
    session_id = request_headers.get("session-id")
    dedupe_key = f"session:{session_id}:sha256:{content_hash}" if session_id else f"sha256:{content_hash}"
    received_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    with connect(db_path) as connection:
        existing = connection.execute(
            "SELECT id, metric_sample_count, event_count FROM ingestions WHERE dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        if existing:
            return {
                "ok": True,
                "duplicate": True,
                "ingestion_id": existing["id"],
                "metric_samples_inserted": existing["metric_sample_count"],
                "events_inserted": existing["event_count"],
            }

        cursor = connection.execute(
            """
            INSERT INTO ingestions (
                dedupe_key, session_id, received_at, automation_name, automation_id,
                automation_aggregation, automation_period, content_sha256,
                payload_size, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                session_id,
                received_at,
                request_headers.get("automation-name"),
                request_headers.get("automation-id"),
                request_headers.get("automation-aggregation"),
                request_headers.get("automation-period"),
                content_hash,
                len(raw_json.encode("utf-8")),
                raw_json,
            ),
        )
        ingestion_id = int(cursor.lastrowid)
        metrics_before = int(
            connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
        )
        events_before = int(
            connection.execute("SELECT COUNT(*) AS count FROM health_events").fetchone()["count"]
        )
        metric_count = 0
        event_count = 0

        for row in _metric_rows(payload):
            _upsert_metric_row(connection, ingestion_id, row)
            metric_count += 1

        for row in _event_rows(payload):
            _upsert_event_row(connection, ingestion_id, row)
            event_count += 1

        _recount_ingestions(connection)
        metrics_after = int(
            connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
        )
        events_after = int(
            connection.execute("SELECT COUNT(*) AS count FROM health_events").fetchone()["count"]
        )

    return {
        "ok": True,
        "duplicate": False,
        "ingestion_id": ingestion_id,
        "metric_samples_inserted": metrics_after - metrics_before,
        "metric_samples_processed": metric_count,
        "events_inserted": events_after - events_before,
        "events_processed": event_count,
    }


def backfill_metric_normalization(db_path: Path) -> dict[str, int]:
    """Rebuild normalized latest-version rows from retained raw payload history."""
    initialize_database(db_path)
    with connect(db_path) as connection:
        metrics_before = connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
        events_before = connection.execute("SELECT COUNT(*) AS count FROM health_events").fetchone()["count"]
        ingestions = connection.execute("SELECT id, raw_json FROM ingestions ORDER BY id").fetchall()
        connection.execute("DELETE FROM metric_samples")
        connection.execute("DELETE FROM health_events")
        payloads_processed = 0
        for ingestion in ingestions:
            try:
                payload = json.loads(ingestion["raw_json"])
            except json.JSONDecodeError:
                continue
            payloads_processed += 1
            for row in _metric_rows(payload):
                _upsert_metric_row(connection, ingestion["id"], row)
            for row in _event_rows(payload):
                _upsert_event_row(connection, ingestion["id"], row)
        _recount_ingestions(connection)
        metrics_after = connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
        events_after = connection.execute("SELECT COUNT(*) AS count FROM health_events").fetchone()["count"]
    return {
        "payloads_processed": payloads_processed,
        "metric_samples_before": metrics_before,
        "metric_samples_after": metrics_after,
        "metric_samples_delta": metrics_after - metrics_before,
        "events_before": events_before,
        "events_after": events_after,
        "events_delta": events_after - events_before,
    }


def _parse_stored_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError:
        return None


def audit_metric_duplicates(
    db_path: Path,
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    initialize_database(db_path)
    zone = _zone(timezone)
    with connect(db_path) as connection:
        raw_counts = {
            row["metric_name"]: int(row["raw_rows"])
            for row in connection.execute(
                "SELECT metric_name, COUNT(*) AS raw_rows FROM metric_samples GROUP BY metric_name"
            ).fetchall()
        }
        groups = connection.execute(
            """
            SELECT metric_name, start_time, COUNT(*) AS factor,
                   COUNT(DISTINCT COALESCE(source, '')) AS source_variants
            FROM metric_samples
            GROUP BY metric_name, start_time, COALESCE(end_time, ''), COALESCE(unit, ''),
                     numeric_value, min_value, max_value, avg_value, sum_value,
                     COALESCE(value_text, '')
            HAVING COUNT(*) > 1
            ORDER BY metric_name, start_time
            """
        ).fetchall()

    metric_stats: dict[str, dict[str, Any]] = {}
    factor_distribution: Counter[int] = Counter()
    for group in groups:
        metric_name = str(group["metric_name"])
        factor = int(group["factor"])
        factor_distribution[factor] += 1
        stats = metric_stats.setdefault(
            metric_name,
            {
                "raw_rows": raw_counts.get(metric_name, 0),
                "duplicate_group_count": 0,
                "duplicate_row_count": 0,
                "duplicates_removed": 0,
                "max_factor": 0,
                "source_variant_groups": 0,
                "affected_dates": set(),
            },
        )
        stats["duplicate_group_count"] += 1
        stats["duplicate_row_count"] += factor
        stats["duplicates_removed"] += factor - 1
        stats["max_factor"] = max(stats["max_factor"], factor)
        if int(group["source_variants"]) > 1:
            stats["source_variant_groups"] += 1
        timestamp = _parse_stored_time(group["start_time"])
        if timestamp is not None:
            stats["affected_dates"].add(timestamp.astimezone(zone).date().isoformat())

    for metric_name, stats in metric_stats.items():
        stats["unique_rows"] = stats["raw_rows"] - stats["duplicates_removed"]
        stats["duplicate_ratio"] = round(
            stats["duplicates_removed"] / stats["raw_rows"], 4
        ) if stats["raw_rows"] else 0.0
        stats["affected_dates"] = sorted(stats["affected_dates"])

    total_raw = sum(raw_counts.values())
    total_removed = sum(stats["duplicates_removed"] for stats in metric_stats.values())
    return {
        "timezone": zone.key,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "candidate_key": (
            "metric_name+start_time+end_time+unit+numeric/min/max/avg/sum/value_text"
        ),
        "total_raw_rows": total_raw,
        "total_unique_rows": total_raw - total_removed,
        "duplicate_group_count": len(groups),
        "duplicate_row_count": sum(int(group["factor"]) for group in groups),
        "duplicates_removed": total_removed,
        "factor_distribution": dict(sorted(factor_distribution.items())),
        "affected_metrics": sorted(metric_stats),
        "metrics": metric_stats,
    }


def _archive_candidate_duplicates(connection: sqlite3.Connection) -> int:
    archived_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    before = int(
        connection.execute("SELECT COUNT(*) AS count FROM metric_sample_duplicates").fetchone()[
            "count"
        ]
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO metric_sample_duplicates (
            archived_at, archive_reason, survivor_fingerprint,
            original_metric_sample_id, ingestion_id, metric_name, unit,
            start_time, end_time, source, source_name, source_bundle_id, device_name,
            numeric_value, min_value, max_value, avg_value, sum_value,
            value_text, sample_uuid, original_record_id, fingerprint, payload_json
        )
        WITH ranked AS (
            SELECT ms.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY metric_name, start_time, COALESCE(end_time, ''),
                                    COALESCE(unit, ''), numeric_value, min_value,
                                    max_value, avg_value, sum_value, COALESCE(value_text, '')
                       ORDER BY (original_record_id IS NOT NULL) DESC,
                                (source_bundle_id IS NOT NULL) DESC,
                                (device_name IS NOT NULL) DESC,
                                id ASC
                   ) AS candidate_rank,
                   FIRST_VALUE(fingerprint) OVER (
                       PARTITION BY metric_name, start_time, COALESCE(end_time, ''),
                                    COALESCE(unit, ''), numeric_value, min_value,
                                    max_value, avg_value, sum_value, COALESCE(value_text, '')
                       ORDER BY (original_record_id IS NOT NULL) DESC,
                                (source_bundle_id IS NOT NULL) DESC,
                                (device_name IS NOT NULL) DESC,
                                id ASC
                   ) AS survivor
            FROM metric_samples ms
        )
        SELECT ?, 'candidate_fingerprint_duplicate', survivor,
               id, ingestion_id, metric_name, unit, start_time, end_time,
               source, source_name, source_bundle_id, device_name,
               numeric_value, min_value, max_value, avg_value, sum_value,
               value_text, sample_uuid, original_record_id, fingerprint, payload_json
        FROM ranked
        WHERE candidate_rank > 1
        """,
        (archived_at,),
    )
    after = int(
        connection.execute("SELECT COUNT(*) AS count FROM metric_sample_duplicates").fetchone()[
            "count"
        ]
    )
    return after - before


def repair_historical_metric_duplicates(
    db_path: Path,
    timezone: str = DEFAULT_TIMEZONE,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    before = audit_metric_duplicates(db_path, timezone)
    if dry_run:
        return {"mode": "dry_run", "before": before}

    with connect(db_path) as connection:
        _ensure_schema_migrations(connection)
        archived_rows = _archive_candidate_duplicates(connection)
    rebuilt = backfill_metric_normalization(db_path)
    after = audit_metric_duplicates(db_path, timezone)
    report = {
        "mode": "historical_repair",
        "archived_rows": archived_rows,
        "before": before,
        "rebuild": rebuilt,
        "after": after,
    }
    with connect(db_path) as connection:
        connection.execute(
            "INSERT INTO deduplication_audits (created_at, mode, report_json) VALUES (?, ?, ?)",
            (
                datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "historical_repair",
                _compact_json(report),
            ),
        )
    return report


def _source_priority(row: dict[str, Any]) -> int:
    source = str(row.get("source_name") or row.get("source") or "").casefold()
    if "watch" in source:
        return 30
    if "iphone" in source:
        return 20
    return 10 if source else 0


def _metric_total_value(row: dict[str, Any]) -> float | None:
    for key in ("sum_value", "numeric_value", "avg_value"):
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _deduplicate_cumulative_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    winners: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("metric_name") or "").casefold(),
            str(row.get("start_time") or ""),
            str(row.get("end_time") or ""),
            str(row.get("unit") or "").casefold(),
        )
        existing = winners.get(key)
        score = (
            _source_priority(row),
            str(row.get("received_at") or ""),
            int(row.get("id") or 0),
        )
        existing_score = (
            _source_priority(existing),
            str(existing.get("received_at") or ""),
            int(existing.get("id") or 0),
        ) if existing else (-1, "", -1)
        if existing is None or score > existing_score:
            winners[key] = row
    return list(winners.values())


def aggregate_cumulative_metric(
    db_path: Path,
    metric_name: str,
    start: datetime,
    end: datetime,
    timezone: str = DEFAULT_TIMEZONE,
) -> dict[str, Any]:
    initialize_database(db_path)
    start_utc = start.astimezone(UTC).isoformat().replace("+00:00", "Z")
    end_utc = end.astimezone(UTC).isoformat().replace("+00:00", "Z")
    with connect(db_path) as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT ms.*, i.received_at
                FROM metric_samples ms
                JOIN ingestions i ON i.id = ms.ingestion_id
                WHERE ms.metric_name COLLATE NOCASE = ?
                  AND ms.start_time >= ? AND ms.start_time < ?
                ORDER BY ms.start_time, ms.id
                """,
                (metric_name, start_utc, end_utc),
            ).fetchall()
        ]
        archived_rows = int(
            connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM metric_sample_duplicates
                WHERE metric_name COLLATE NOCASE = ?
                  AND start_time >= ? AND start_time < ?
                """,
                (metric_name, start_utc, end_utc),
            ).fetchone()["count"]
        )

    winners = _deduplicate_cumulative_rows(rows)
    present_values = [value for row in winners if (value := _metric_total_value(row)) is not None]
    overlap_removed = len(rows) - len(winners)
    duplicates_removed = archived_rows + overlap_removed
    raw_rows = len(rows) + archived_rows
    duplicate_ratio = duplicates_removed / raw_rows if raw_rows else 0.0
    sources = sorted(
        {
            str(row.get("source_name") or row.get("source"))
            for row in winners
            if row.get("source_name") or row.get("source")
        }
    )
    return {
        "metric_name": metric_name,
        "total": sum(present_values) if present_values else None,
        "deduplication": {
            "applied": True,
            "raw_rows": raw_rows,
            "unique_rows": len(winners),
            "duplicates_removed": duplicates_removed,
            "duplicate_ratio": round(duplicate_ratio, 4),
            "method": "stable_record_fingerprint_then_source_priority_overlap",
            "source_priority_applied": overlap_removed > 0,
            "sources": sources,
        },
        "aggregation": {
            "timezone": _zone(timezone).key,
            "method": "deduplicated_exported_interval_sum",
            "authoritative_healthkit_total": None,
            "confidence": "low" if duplicate_ratio > 0.05 else "medium",
        },
        "baseline_eligible": duplicate_ratio <= 0.05,
    }


def ingestion_status(db_path: Path) -> dict[str, Any]:
    initialize_database(db_path)
    with connect(db_path) as connection:
        ingestion = connection.execute(
            "SELECT COUNT(*) AS count, MAX(received_at) AS latest FROM ingestions"
        ).fetchone()
        metrics = connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()
        events = connection.execute("SELECT COUNT(*) AS count FROM health_events").fetchone()
    return {
        "ingestions": ingestion["count"],
        "latest_ingestion": ingestion["latest"],
        "metric_samples": metrics["count"],
        "events": events["count"],
    }


def list_available_data(db_path: Path, output_format: Literal["concise", "detailed"] = "concise") -> dict[str, Any]:
    initialize_database(db_path)
    with connect(db_path) as connection:
        metric_rows = connection.execute(
            """
            SELECT metric_name, unit, COUNT(*) AS sample_count,
                   MIN(start_time) AS first_time,
                   MAX(COALESCE(end_time, start_time)) AS last_time
            FROM metric_samples
            GROUP BY metric_name, unit
            ORDER BY metric_name COLLATE NOCASE
            """
        ).fetchall()
        event_rows = connection.execute(
            """
            SELECT category, COUNT(*) AS event_count,
                   MIN(start_time) AS first_time,
                   MAX(COALESCE(end_time, start_time)) AS last_time
            FROM health_events
            GROUP BY category
            ORDER BY category COLLATE NOCASE
            """
        ).fetchall()
        status = ingestion_status(db_path)

    metrics = [dict(row) for row in metric_rows]
    events = [dict(row) for row in event_rows]
    if output_format == "concise":
        metrics = [
            {
                "metric_name": row["metric_name"],
                "unit": row["unit"],
                "sample_count": row["sample_count"],
                "last_time": row["last_time"],
            }
            for row in metrics
        ]
        events = [{"category": row["category"], "event_count": row["event_count"]} for row in events]
    return {"status": status, "metrics": metrics, "event_categories": events}


def _metric_filters(
    metric_names: list[str] | None,
    start_time: str,
    end_time: str,
) -> tuple[str, list[Any]]:
    clauses = ["start_time >= ?", "start_time < ?"]
    params: list[Any] = [start_time, end_time]
    if metric_names:
        if len(metric_names) > 50:
            raise ValueError("metric_names accepts at most 50 names per query")
        placeholders = ",".join("?" for _ in metric_names)
        clauses.append(f"metric_name COLLATE NOCASE IN ({placeholders})")
        params.extend(metric_names)
    return " AND ".join(clauses), params


def query_health_metrics(
    db_path: Path,
    metric_names: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    aggregation: Literal["raw", "hour", "day"] = "day",
    limit: int = 200,
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = "UTC",
) -> dict[str, Any]:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    if output_format == "detailed" and aggregation != "raw":
        raise ValueError("output_format='detailed' requires aggregation='raw'")
    default_start, default_end = default_time_window(7)
    zone = _zone(timezone)
    start = normalize_query_time(start_time, timezone=zone.key) or default_start
    end = normalize_query_time(end_time, end_of_day=True, timezone=zone.key) or default_end
    where, params = _metric_filters(metric_names, start, end)
    initialize_database(db_path)

    with connect(db_path) as connection:
        matched_active = int(
            connection.execute(
                f"SELECT COUNT(*) AS count FROM metric_samples WHERE {where}", params
            ).fetchone()["count"]
        )
        archived_rows = int(
            connection.execute(
                f"SELECT COUNT(*) AS count FROM metric_sample_duplicates WHERE {where}", params
            ).fetchone()["count"]
        )
        overlap_removed = 0
        if aggregation == "raw":
            fields = """
                metric_name, unit, start_time, end_time, source,
                numeric_value, min_value, max_value, avg_value, sum_value,
                value_text, original_record_id
            """
            if output_format == "detailed":
                fields += ", source_name, source_bundle_id, device_name, fingerprint, payload_json"
            rows = connection.execute(
                f"""
                SELECT {fields}
                FROM metric_samples
                WHERE {where}
                ORDER BY start_time DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            total_result_count = matched_active
        else:
            active_rows = [
                dict(row)
                for row in connection.execute(
                    f"""
                    SELECT ms.id, ms.metric_name, ms.unit, ms.start_time, ms.end_time,
                           ms.source, ms.source_name, ms.numeric_value, ms.min_value,
                           ms.max_value, ms.avg_value, ms.sum_value, ms.value_text,
                           i.received_at
                    FROM metric_samples ms
                    JOIN ingestions i ON i.id = ms.ingestion_id
                    WHERE {where}
                    ORDER BY ms.start_time, ms.id
                    """,
                    params,
                ).fetchall()
            ]
            selected_rows: list[dict[str, Any]] = []
            by_metric: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in active_rows:
                by_metric[str(row["metric_name"]).casefold()].append(row)
            for normalized_name, metric_rows in by_metric.items():
                if normalized_name in CUMULATIVE_METRICS:
                    winners = _deduplicate_cumulative_rows(metric_rows)
                    overlap_removed += len(metric_rows) - len(winners)
                    selected_rows.extend(winners)
                else:
                    selected_rows.extend(metric_rows)

            grouped: dict[tuple[str, str | None, str], list[dict[str, Any]]] = defaultdict(list)
            for row in selected_rows:
                timestamp = _parse_stored_time(row.get("start_time"))
                if timestamp is None:
                    continue
                local = timestamp.astimezone(zone)
                bucket = (
                    local.date().isoformat()
                    if aggregation == "day"
                    else local.replace(minute=0, second=0, microsecond=0).isoformat()
                )
                grouped[(str(row["metric_name"]), row.get("unit"), bucket)].append(row)

            aggregate_records: list[dict[str, Any]] = []
            for (metric_name, unit, bucket), bucket_rows in grouped.items():
                average_values = [
                    value
                    for row in bucket_rows
                    if (value := _first_number(row, "avg_value", "numeric_value", "sum_value"))
                    is not None
                ]
                minimum_values = [
                    value
                    for row in bucket_rows
                    if (
                        value := _first_number(
                            row, "min_value", "numeric_value", "avg_value", "sum_value"
                        )
                    )
                    is not None
                ]
                maximum_values = [
                    value
                    for row in bucket_rows
                    if (
                        value := _first_number(
                            row, "max_value", "numeric_value", "avg_value", "sum_value"
                        )
                    )
                    is not None
                ]
                total_values = [
                    value for row in bucket_rows if (value := _metric_total_value(row)) is not None
                ]
                categorical = sorted(
                    {str(row["value_text"]) for row in bucket_rows if row.get("value_text") is not None}
                )
                aggregate_records.append(
                    {
                        "metric_name": metric_name,
                        "unit": unit,
                        "bucket": bucket,
                        "sample_count": len(bucket_rows),
                        "average": sum(average_values) / len(average_values) if average_values else None,
                        "minimum": min(minimum_values) if minimum_values else None,
                        "maximum": max(maximum_values) if maximum_values else None,
                        "total": sum(total_values) if total_values else None,
                        "categorical_values": ",".join(categorical) if categorical else None,
                    }
                )
            aggregate_records.sort(
                key=lambda row: (str(row["bucket"]), str(row["metric_name"]).casefold()),
                reverse=True,
            )
            total_result_count = len(aggregate_records)
            rows = aggregate_records[:limit]

    records = [dict(row) for row in rows]
    if aggregation == "raw" and output_format == "detailed":
        for record in records:
            record["payload"] = json.loads(record.pop("payload_json"))

    unique_rows = matched_active - overlap_removed
    duplicates_removed = archived_rows + overlap_removed
    raw_rows = matched_active + archived_rows
    duplicate_ratio = duplicates_removed / raw_rows if raw_rows else 0.0
    status = "empty" if unique_rows == 0 else "partial" if duplicate_ratio > 0.05 else "ok"

    return {
        "status": status,
        "query": {
            "metric_names": metric_names or [],
            "start_time": start,
            "end_time": end,
            "aggregation": aggregation,
            "limit": limit,
            "output_format": output_format,
            "timezone": zone.key,
        },
        "records": records,
        "record_count": len(records),
        "fetch": {"complete": total_result_count <= limit},
        "deduplication": {
            "applied": True,
            "raw_rows": raw_rows,
            "unique_rows": unique_rows,
            "duplicates_removed": duplicates_removed,
            "duplicate_ratio": round(duplicate_ratio, 4),
            "method": "stable_record_fingerprint_then_source_priority_overlap",
        },
        "aggregation": {
            "timezone": zone.key,
            "method": (
                "timezone_local_bucket_with_cumulative_source_priority"
                if aggregation != "raw"
                else "raw_unique_records"
            ),
            "confidence": "low" if duplicate_ratio > 0.05 else "medium",
        },
        "data_quality": {
            "complete": status == "ok",
            "baseline_eligible": status == "ok",
        },
        "note": "Cumulative metrics are deduplicated before aggregation; missing values remain null.",
    }


def query_health_events(
    db_path: Path,
    categories: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    output_format: Literal["concise", "detailed"] = "concise",
    timezone: str = "UTC",
) -> dict[str, Any]:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    if categories and len(categories) > 20:
        raise ValueError("categories accepts at most 20 names per query")
    default_start, default_end = default_time_window(30)
    start = normalize_query_time(start_time, timezone=timezone) or default_start
    end = normalize_query_time(end_time, end_of_day=True, timezone=timezone) or default_end
    clauses = ["start_time >= ?", "start_time < ?"]
    params: list[Any] = [start, end]
    if categories:
        placeholders = ",".join("?" for _ in categories)
        clauses.append(f"category COLLATE NOCASE IN ({placeholders})")
        params.extend(categories)
    fields = "category, event_type, start_time, end_time, source, value_text"
    if output_format == "detailed":
        fields += ", payload_json"

    initialize_database(db_path)
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT {fields}
            FROM health_events
            WHERE {' AND '.join(clauses)}
            ORDER BY start_time DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    records = [dict(row) for row in rows]
    if output_format == "detailed":
        for record in records:
            record["payload"] = json.loads(record.pop("payload_json"))
    return {
        "query": {
            "categories": categories or [],
            "start_time": start,
            "end_time": end,
            "limit": limit,
            "output_format": output_format,
            "timezone": _zone(timezone).key,
        },
        "events": records,
        "event_count": len(records),
    }


def health_overview(db_path: Path, days: int = 7) -> dict[str, Any]:
    if not 1 <= days <= 365:
        raise ValueError("days must be between 1 and 365")
    start, end = default_time_window(days)
    initialize_database(db_path)
    with connect(db_path) as connection:
        metrics = connection.execute(
            """
            SELECT metric_name, unit, COUNT(*) AS sample_count,
                   AVG(COALESCE(avg_value, numeric_value, sum_value)) AS average,
                   MIN(COALESCE(min_value, numeric_value, avg_value, sum_value)) AS minimum,
                   MAX(COALESCE(max_value, numeric_value, avg_value, sum_value)) AS maximum,
                   SUM(COALESCE(sum_value, numeric_value, avg_value)) AS total,
                   MAX(start_time) AS latest_time
            FROM metric_samples
            WHERE start_time >= ? AND start_time < ?
            GROUP BY metric_name, unit
            ORDER BY metric_name COLLATE NOCASE
            """,
            (start, end),
        ).fetchall()
        events = connection.execute(
            """
            SELECT category, COUNT(*) AS event_count
            FROM health_events
            WHERE start_time >= ? AND start_time < ?
            GROUP BY category
            ORDER BY category COLLATE NOCASE
            """,
            (start, end),
        ).fetchall()
    return {
        "period": {"days": days, "start_time": start, "end_time": end},
        "metrics": [dict(row) for row in metrics],
        "event_categories": [dict(row) for row in events],
        "note": "This is exported health data, not medical advice or a diagnosis.",
    }
