import json
import sqlite3
from pathlib import Path

import pytest

from apple_health_mcp.storage import (
    audit_metric_duplicates,
    backfill_metric_normalization,
    health_overview,
    list_available_data,
    query_health_events,
    query_health_metrics,
    repair_historical_metric_duplicates,
    store_ingestion,
)


PAYLOAD = {
    "data": {
        "metrics": [
            {
                "name": "step_count",
                "units": "count",
                "data": [
                    {
                        "source": "iPhone",
                        "qty": 1234,
                        "startDate": "2026-07-17T00:00:00+02:00",
                        "endDate": "2026-07-17T23:59:59+02:00",
                    }
                ],
            },
            {
                "name": "sleep_analysis",
                "units": "count",
                "data": [
                    {
                        "source": "Apple Watch",
                        "qty": 1,
                        "value": "Deep",
                        "startDate": "2026-07-17T23:00:00+02:00",
                        "endDate": "2026-07-17T23:30:00+02:00",
                    },
                    {
                        "source": "Apple Watch",
                        "date": "2026-07-18T00:00:00+02:00",
                        "inBedStart": "2026-07-17T22:45:00+02:00",
                        "inBedEnd": "2026-07-18T07:15:00+02:00",
                        "sleepStart": "2026-07-17T23:00:00+02:00",
                        "sleepEnd": "2026-07-18T07:00:00+02:00",
                        "totalSleep": 7.5,
                        "asleep": 7.5,
                        "awake": 0.5,
                        "core": 4.0,
                        "deep": 1.5,
                        "rem": 2.0,
                        "inBed": 8.0,
                    }
                ],
            },
        ],
        "workouts": [
            {
                "uuid": "workout-1",
                "name": "Walking",
                "source": "Apple Watch",
                "startDate": "2026-07-17T18:00:00+02:00",
                "endDate": "2026-07-17T18:30:00+02:00",
                "duration": 1800,
            }
        ],
    }
}


def test_ingestion_deduplication_and_queries(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    raw = json.dumps(PAYLOAD)
    first = store_ingestion(db_path, PAYLOAD, raw, {"session-id": "session-1"})
    duplicate = store_ingestion(db_path, PAYLOAD, raw, {"session-id": "session-1"})

    assert first["duplicate"] is False
    assert first["metric_samples_inserted"] == 10
    assert first["events_inserted"] == 1
    assert duplicate["duplicate"] is True

    available = list_available_data(db_path, "detailed")
    names = {item["metric_name"] for item in available["metrics"]}
    assert {"step_count", "sleep_analysis", "sleep_analysis.deep", "sleep_analysis.rem"} <= names
    assert available["event_categories"][0]["category"] == "workouts"

    raw_metrics = query_health_metrics(
        db_path,
        metric_names=["sleep_analysis"],
        start_time="2026-07-17",
        end_time="2026-07-18",
        aggregation="raw",
    )
    assert any(record["value_text"] == "Deep" for record in raw_metrics["records"])

    detailed_sleep = query_health_metrics(
        db_path,
        metric_names=["sleep_analysis"],
        start_time="2026-07-17",
        end_time="2026-07-18",
        aggregation="raw",
        output_format="detailed",
    )
    summary = next(record for record in detailed_sleep["records"] if "totalSleep" in record["payload"])
    assert summary["payload"]["inBedStart"] == "2026-07-17T22:45:00+02:00"

    deep_sleep = query_health_metrics(
        db_path,
        metric_names=["sleep_analysis.deep"],
        start_time="2026-07-17",
        end_time="2026-07-18",
        aggregation="raw",
    )
    assert deep_sleep["records"][0]["numeric_value"] == 1.5

    events = query_health_events(
        db_path,
        categories=["workouts"],
        start_time="2026-07-17",
        end_time="2026-07-18",
        output_format="detailed",
    )
    assert events["events"][0]["payload"]["name"] == "Walking"

    overview = health_overview(db_path, days=365)
    assert len(overview["metrics"]) == 9

    with pytest.raises(ValueError, match="ISO-8601"):
        query_health_metrics(db_path, start_time="not-a-date")

    with pytest.raises(ValueError, match="requires aggregation='raw'"):
        query_health_metrics(db_path, output_format="detailed")

    updated_payload = json.loads(raw)
    updated_payload["data"]["metrics"][0]["data"][0]["qty"] = 1500
    updated_raw = json.dumps(updated_payload)
    update = store_ingestion(db_path, updated_payload, updated_raw, {"session-id": "session-2"})
    assert update["duplicate"] is False

    latest_steps = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-16",
        end_time="2026-07-18",
        aggregation="raw",
    )
    assert latest_steps["record_count"] == 2

    latest_step_day = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-17T00:00:00+02:00",
        end_time="2026-07-18T00:00:00+02:00",
        aggregation="day",
        timezone="Europe/Berlin",
    )
    assert latest_step_day["records"][0]["bucket"] == "2026-07-17"
    assert latest_step_day["records"][0]["total"] == 1500
    assert latest_step_day["records"][0]["sample_count"] == 1

    rebuilt = backfill_metric_normalization(db_path)
    assert rebuilt["metric_samples_after"] == 11
    assert rebuilt["events_after"] == 1


def test_source_variant_idempotency_timezone_and_source_priority(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"

    def payload(source: str, *, qty: float = 25, record_id: str | None = None) -> dict:
        sample = {
            "source": source,
            "qty": qty,
            "date": "2026-07-18 13:56:00 +0800",
        }
        if record_id:
            sample["uuid"] = record_id
        return {
            "data": {
                "metrics": [{"name": "step_count", "units": "count", "data": [sample]}]
            }
        }

    inserted_counts = []
    for index, source in enumerate(
        (
            "鹅鹅鹅的Apple Watch|小憨🐻|iPhone",
            "鹅鹅鹅的Apple Watch|iPhone|小憨🐻|iPhone",
            "鹅鹅鹅的Apple Watch|小憨🐻",
        ),
        start=1,
    ):
        current = payload(source)
        result = store_ingestion(
            db_path,
            current,
            json.dumps(current, ensure_ascii=False),
            {"session-id": f"source-variant-{index}"},
        )
        inserted_counts.append(result["metric_samples_inserted"])
    assert inserted_counts == [1, 0, 0]

    raw = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-18T00:00:00+08:00",
        end_time="2026-07-19T00:00:00+08:00",
        aggregation="raw",
        timezone="Asia/Shanghai",
    )
    assert raw["record_count"] == 1
    assert raw["records"][0]["source"] == "小憨🐻|鹅鹅鹅的Apple Watch"

    for index in range(3):
        current = payload("鹅鹅鹅的Apple Watch|小憨🐻")
        store_ingestion(
            db_path,
            current,
            json.dumps(current, ensure_ascii=False),
            {"session-id": f"repeat-file-{index}"},
        )
    with sqlite3.connect(db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM metric_samples WHERE metric_name='step_count'"
        ).fetchone()[0] == 1

    boundary_payload = {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {"uuid": "early", "source": "iPhone", "qty": 10, "date": "2026-07-18 00:01:00 +0800"},
                        {"uuid": "late", "source": "iPhone", "qty": 20, "date": "2026-07-18 23:59:00 +0800"},
                        {"uuid": "boundary", "source": "iPhone", "qty": 99, "date": "2026-07-19 00:00:00 +0800"},
                    ],
                }
            ]
        }
    }
    store_ingestion(
        db_path,
        boundary_payload,
        json.dumps(boundary_payload),
        {"session-id": "timezone-boundary"},
    )
    day = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-18T00:00:00+08:00",
        end_time="2026-07-19T00:00:00+08:00",
        aggregation="day",
        timezone="Asia/Shanghai",
    )
    assert [record["bucket"] for record in day["records"]] == ["2026-07-18"]
    assert day["records"][0]["total"] == 55

    overlap_payload = {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {"uuid": "watch-overlap", "source": "Apple Watch", "qty": 12, "date": "2026-07-18 14:00:00 +0800"},
                        {"uuid": "phone-overlap", "source": "iPhone", "qty": 12, "date": "2026-07-18 14:00:00 +0800"},
                    ],
                }
            ]
        }
    }
    store_ingestion(
        db_path,
        overlap_payload,
        json.dumps(overlap_payload),
        {"session-id": "watch-phone-overlap"},
    )
    overlap_day = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-18T00:00:00+08:00",
        end_time="2026-07-19T00:00:00+08:00",
        aggregation="day",
        timezone="Asia/Shanghai",
    )
    assert overlap_day["records"][0]["total"] == 67
    assert overlap_day["deduplication"]["duplicates_removed"] == 1

    for index, source in enumerate(("Apple Watch|iPhone", "Apple Watch|iPhone|iPhone")):
        heart_payload = {
            "data": {
                "metrics": [
                    {
                        "name": "heart_rate",
                        "units": "count/min",
                        "data": [
                            {
                                "source": source,
                                "date": "2026-07-18 14:30:00 +0800",
                                "Avg": 72,
                                "Min": 68,
                                "Max": 78,
                            }
                        ],
                    }
                ]
            }
        }
        store_ingestion(
            db_path,
            heart_payload,
            json.dumps(heart_payload),
            {"session-id": f"heart-source-variant-{index}"},
        )
    heart = query_health_metrics(
        db_path,
        metric_names=["heart_rate"],
        start_time="2026-07-18T00:00:00+08:00",
        end_time="2026-07-19T00:00:00+08:00",
        aggregation="raw",
        timezone="Asia/Shanghai",
    )
    assert heart["record_count"] == 1
    assert heart["deduplication"]["unique_rows"] == 1


def test_historical_duplicate_archive_and_repair(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    payload = {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {"source": "Apple Watch|iPhone", "qty": 25, "date": "2026-07-18 13:56:00 +0800"}
                    ],
                }
            ]
        }
    }
    store_ingestion(db_path, payload, json.dumps(payload), {"session-id": "legacy-base"})
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO metric_samples (
                ingestion_id, metric_name, unit, start_time, end_time, source,
                source_name, numeric_value, sample_uuid, original_record_id,
                fingerprint, payload_json
            )
            SELECT ingestion_id, metric_name, unit, start_time, end_time,
                   'Apple Watch|iPhone|iPhone', 'Apple Watch|iPhone', numeric_value,
                   sample_uuid, original_record_id, 'legacy-fingerprint', payload_json
            FROM metric_samples
            WHERE metric_name='step_count'
            """
        )
        connection.commit()

    before = audit_metric_duplicates(db_path)
    assert before["metrics"]["step_count"]["duplicates_removed"] == 1
    dry_run = repair_historical_metric_duplicates(db_path, dry_run=True)
    assert dry_run["mode"] == "dry_run"

    repaired = repair_historical_metric_duplicates(db_path, dry_run=False)
    assert repaired["archived_rows"] == 1
    assert repaired["after"]["duplicates_removed"] == 0
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM metric_samples").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM metric_sample_duplicates").fetchone()[0] == 1
