from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal


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
    numeric_value REAL,
    min_value REAL,
    max_value REAL,
    avg_value REAL,
    sum_value REAL,
    value_text TEXT,
    sample_uuid TEXT,
    fingerprint TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_samples_name_time
    ON metric_samples(metric_name, start_time);
CREATE INDEX IF NOT EXISTS idx_metric_samples_time
    ON metric_samples(start_time);

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


def normalize_query_time(value: str | None, *, end_of_day: bool = False) -> str | None:
    if value is None or not value.strip():
        return None
    candidate = value.strip()
    if len(candidate) == 10:
        try:
            parsed = datetime.fromisoformat(candidate).replace(tzinfo=UTC)
        except ValueError as error:
            raise ValueError(
                "Time must be ISO-8601, for example 2026-07-18 or 2026-07-18T10:00:00+02:00"
            ) from error
        if end_of_day:
            parsed += timedelta(days=1)
            parsed -= timedelta(microseconds=1)
        return parsed.isoformat().replace("+00:00", "Z")
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
            fingerprint_basis = sample_uuid or raw
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
            base_row = {
                "metric_name": metric_name,
                "unit": _text(_first(sample, "units", "unit")) or unit,
                "start_time": start_time,
                "end_time": end_time,
                "source": _text(_first(sample, "source", "sourceName")),
                "numeric_value": numeric_value,
                "min_value": _number(_first(sample, "min", "minimum", "minValue")),
                "max_value": _number(_first(sample, "max", "maximum", "maxValue")),
                "avg_value": _number(_first(sample, "avg", "average", "avgValue")),
                "sum_value": _number(_first(sample, "sum", "total", "sumValue")),
                "value_text": _text(_first(sample, "value", "status", "stage")),
                "sample_uuid": sample_uuid,
                "fingerprint": _sha256(f"metric:{metric_name}:{fingerprint_basis}"),
                "payload_json": raw,
            }
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
                            "fingerprint": _sha256(f"metric:{derived_name}:{fingerprint_basis}"),
                        }
                    )
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
            fingerprint_basis = event_uuid or raw
            yield {
                "category": category,
                "event_type": event_type,
                "start_time": _normalize_time(_first(item, "startDate", "startTime", "start", "date", "timestamp")),
                "end_time": _normalize_time(_first(item, "endDate", "endTime", "end")),
                "source": _text(_first(item, "source", "sourceName")),
                "value_text": _text(_first(item, "value", "status", "classification", "name")),
                "event_uuid": event_uuid,
                "fingerprint": _sha256(f"event:{category}:{fingerprint_basis}"),
                "payload_json": raw,
            }


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
        metric_count = 0
        event_count = 0

        for row in _metric_rows(payload):
            result = connection.execute(
                """
                INSERT OR IGNORE INTO metric_samples (
                    ingestion_id, metric_name, unit, start_time, end_time, source,
                    numeric_value, min_value, max_value, avg_value, sum_value,
                    value_text, sample_uuid, fingerprint, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingestion_id,
                    row["metric_name"],
                    row["unit"],
                    row["start_time"],
                    row["end_time"],
                    row["source"],
                    row["numeric_value"],
                    row["min_value"],
                    row["max_value"],
                    row["avg_value"],
                    row["sum_value"],
                    row["value_text"],
                    row["sample_uuid"],
                    row["fingerprint"],
                    row["payload_json"],
                ),
            )
            metric_count += max(result.rowcount, 0)

        for row in _event_rows(payload):
            result = connection.execute(
                """
                INSERT OR IGNORE INTO health_events (
                    ingestion_id, category, event_type, start_time, end_time,
                    source, value_text, event_uuid, fingerprint, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            event_count += max(result.rowcount, 0)

        connection.execute(
            "UPDATE ingestions SET metric_sample_count = ?, event_count = ? WHERE id = ?",
            (metric_count, event_count, ingestion_id),
        )

    return {
        "ok": True,
        "duplicate": False,
        "ingestion_id": ingestion_id,
        "metric_samples_inserted": metric_count,
        "events_inserted": event_count,
    }


def backfill_metric_normalization(db_path: Path) -> dict[str, int]:
    """Re-run metric normalization from retained raw payloads without duplicating existing samples."""
    initialize_database(db_path)
    with connect(db_path) as connection:
        before = connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
        ingestions = connection.execute("SELECT id, raw_json FROM ingestions ORDER BY id").fetchall()
        payloads_processed = 0
        for ingestion in ingestions:
            try:
                payload = json.loads(ingestion["raw_json"])
            except json.JSONDecodeError:
                continue
            payloads_processed += 1
            for row in _metric_rows(payload):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO metric_samples (
                        ingestion_id, metric_name, unit, start_time, end_time, source,
                        numeric_value, min_value, max_value, avg_value, sum_value,
                        value_text, sample_uuid, fingerprint, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ingestion["id"],
                        row["metric_name"],
                        row["unit"],
                        row["start_time"],
                        row["end_time"],
                        row["source"],
                        row["numeric_value"],
                        row["min_value"],
                        row["max_value"],
                        row["avg_value"],
                        row["sum_value"],
                        row["value_text"],
                        row["sample_uuid"],
                        row["fingerprint"],
                        row["payload_json"],
                    ),
                )
                connection.execute(
                    """
                    UPDATE metric_samples
                    SET numeric_value = COALESCE(numeric_value, ?),
                        start_time = COALESCE(start_time, ?),
                        end_time = COALESCE(end_time, ?),
                        unit = COALESCE(unit, ?),
                        source = COALESCE(source, ?)
                    WHERE fingerprint = ?
                    """,
                    (
                        row["numeric_value"],
                        row["start_time"],
                        row["end_time"],
                        row["unit"],
                        row["source"],
                        row["fingerprint"],
                    ),
                )
        connection.execute(
            """
            UPDATE ingestions
            SET metric_sample_count = (
                SELECT COUNT(*) FROM metric_samples WHERE ingestion_id = ingestions.id
            )
            """
        )
        after = connection.execute("SELECT COUNT(*) AS count FROM metric_samples").fetchone()["count"]
    return {
        "payloads_processed": payloads_processed,
        "metric_samples_before": before,
        "metric_samples_after": after,
        "metric_samples_added": after - before,
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
    clauses = ["start_time >= ?", "start_time <= ?"]
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
) -> dict[str, Any]:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    default_start, default_end = default_time_window(7)
    start = normalize_query_time(start_time) or default_start
    end = normalize_query_time(end_time, end_of_day=True) or default_end
    where, params = _metric_filters(metric_names, start, end)
    initialize_database(db_path)

    with connect(db_path) as connection:
        if aggregation == "raw":
            rows = connection.execute(
                f"""
                SELECT metric_name, unit, start_time, end_time, source,
                       numeric_value, min_value, max_value, avg_value, sum_value,
                       value_text
                FROM metric_samples
                WHERE {where}
                ORDER BY start_time DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        else:
            bucket = "substr(start_time, 1, 13) || ':00:00Z'" if aggregation == "hour" else "substr(start_time, 1, 10)"
            rows = connection.execute(
                f"""
                SELECT metric_name, unit, {bucket} AS bucket,
                       COUNT(*) AS sample_count,
                       AVG(COALESCE(avg_value, numeric_value, sum_value)) AS average,
                       MIN(COALESCE(min_value, numeric_value, avg_value, sum_value)) AS minimum,
                       MAX(COALESCE(max_value, numeric_value, avg_value, sum_value)) AS maximum,
                       SUM(COALESCE(sum_value, numeric_value, avg_value)) AS total,
                       GROUP_CONCAT(DISTINCT value_text) AS categorical_values
                FROM metric_samples
                WHERE {where}
                GROUP BY metric_name, unit, bucket
                ORDER BY bucket DESC, metric_name COLLATE NOCASE
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()

    return {
        "query": {
            "metric_names": metric_names or [],
            "start_time": start,
            "end_time": end,
            "aggregation": aggregation,
            "limit": limit,
        },
        "records": [dict(row) for row in rows],
        "record_count": len(rows),
        "note": "Aggregates are mechanical summaries of exported samples; interpretation depends on the metric and unit.",
    }


def query_health_events(
    db_path: Path,
    categories: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    output_format: Literal["concise", "detailed"] = "concise",
) -> dict[str, Any]:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    if categories and len(categories) > 20:
        raise ValueError("categories accepts at most 20 names per query")
    default_start, default_end = default_time_window(30)
    start = normalize_query_time(start_time) or default_start
    end = normalize_query_time(end_time, end_of_day=True) or default_end
    clauses = ["start_time >= ?", "start_time <= ?"]
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
            WHERE start_time >= ? AND start_time <= ?
            GROUP BY metric_name, unit
            ORDER BY metric_name COLLATE NOCASE
            """,
            (start, end),
        ).fetchall()
        events = connection.execute(
            """
            SELECT category, COUNT(*) AS event_count
            FROM health_events
            WHERE start_time >= ? AND start_time <= ?
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
