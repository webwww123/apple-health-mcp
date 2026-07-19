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
                    },
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
    assert {
        "step_count",
        "sleep_analysis",
        "sleep_analysis.deep",
        "sleep_analysis.rem",
    } <= names
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


def test_source_variant_idempotency_timezone_and_source_priority(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "health.db"

    def payload(source: str, *, qty: float = 25, record_id: str | None = None) -> dict:
        sample = {
            "source": source,
            "qty": qty,
            "date": "2026-07-18 13:56:00 +0800",
        }
        if record_id:
            sample["uuid"] = record_id
        return {"data": {"metrics": [{"name": "step_count", "units": "count", "data": [sample]}]}}

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
        assert (
            connection.execute("SELECT COUNT(*) FROM metric_samples WHERE metric_name='step_count'").fetchone()[0] == 1
        )

    boundary_payload = {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "uuid": "early",
                            "source": "iPhone",
                            "qty": 10,
                            "date": "2026-07-18 00:01:00 +0800",
                        },
                        {
                            "uuid": "late",
                            "source": "iPhone",
                            "qty": 20,
                            "date": "2026-07-18 23:59:00 +0800",
                        },
                        {
                            "uuid": "boundary",
                            "source": "iPhone",
                            "qty": 99,
                            "date": "2026-07-19 00:00:00 +0800",
                        },
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
                        {
                            "uuid": "watch-overlap",
                            "source": "Apple Watch",
                            "qty": 12,
                            "date": "2026-07-18 14:00:00 +0800",
                        },
                        {
                            "uuid": "phone-overlap",
                            "source": "iPhone",
                            "qty": 12,
                            "date": "2026-07-18 14:00:00 +0800",
                        },
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
                        {
                            "source": "Apple Watch|iPhone",
                            "qty": 25,
                            "date": "2026-07-18 13:56:00 +0800",
                        }
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


def test_half_open_ranges_and_truncation_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    payload = {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "uuid": "step-early",
                            "source": "iPhone",
                            "qty": 10,
                            "date": "2026-07-18 00:01:00 +0800",
                        },
                        {
                            "uuid": "step-late",
                            "source": "iPhone",
                            "qty": 20,
                            "date": "2026-07-18 23:59:00 +0800",
                        },
                        {
                            "uuid": "step-boundary",
                            "source": "iPhone",
                            "qty": 99,
                            "date": "2026-07-19 00:00:00 +0800",
                        },
                    ],
                }
            ],
            "workouts": [
                {
                    "id": "workout-early",
                    "name": "Walking",
                    "start": "2026-07-18 08:00:00 +0800",
                    "end": "2026-07-18 08:30:00 +0800",
                    "duration": 1800,
                },
                {
                    "id": "workout-late",
                    "name": "Walking",
                    "start": "2026-07-18 22:00:00 +0800",
                    "end": "2026-07-18 22:30:00 +0800",
                    "duration": 1800,
                },
                {
                    "id": "workout-boundary",
                    "name": "Walking",
                    "start": "2026-07-19 00:00:00 +0800",
                    "end": "2026-07-19 00:30:00 +0800",
                    "duration": 1800,
                },
            ],
        }
    }
    store_ingestion(db_path, payload, json.dumps(payload), {"session-id": "range-contract"})

    metrics = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-18",
        end_time="2026-07-19",
        aggregation="raw",
        limit=10,
        timezone="Asia/Shanghai",
    )
    assert metrics["status"] == "ok"
    assert metrics["total_result_count"] == 2
    assert {record["numeric_value"] for record in metrics["records"]} == {10, 20}
    assert metrics["fetch"] == {
        "complete": True,
        "truncated": False,
        "matched_results": 2,
        "returned_results": 2,
    }

    truncated_metrics = query_health_metrics(
        db_path,
        metric_names=["step_count"],
        start_time="2026-07-18",
        end_time="2026-07-19",
        aggregation="raw",
        limit=1,
        timezone="Asia/Shanghai",
    )
    assert truncated_metrics["status"] == "partial"
    assert truncated_metrics["fetch"]["complete"] is False
    assert truncated_metrics["fetch"]["truncated"] is True
    assert truncated_metrics["fetch"]["matched_results"] == 2
    assert truncated_metrics["fetch"]["returned_results"] == 1
    assert truncated_metrics["data_quality"]["complete"] is True

    events = query_health_events(
        db_path,
        categories=["workouts"],
        start_time="2026-07-18",
        end_time="2026-07-19",
        limit=10,
        timezone="Asia/Shanghai",
    )
    assert events["status"] == "ok"
    assert events["total_event_count"] == 2
    assert {event["event_type"] for event in events["events"]} == {"Walking"}
    assert events["fetch"]["complete"] is True

    truncated_events = query_health_events(
        db_path,
        categories=["workouts"],
        start_time="2026-07-18",
        end_time="2026-07-19",
        limit=1,
        timezone="Asia/Shanghai",
    )
    assert truncated_events["status"] == "partial"
    assert truncated_events["fetch"] == {
        "complete": False,
        "truncated": True,
        "matched_events": 2,
        "returned_events": 1,
    }

    large_event_payload = {
        "data": {
            "workouts": [
                {
                    "id": "workout-large",
                    "name": "Outdoor Cycling",
                    "start": "2026-07-18 12:00:00 +0800",
                    "end": "2026-07-18 13:00:00 +0800",
                    "duration": 3600,
                    "route": [{"latitude": 31.2, "longitude": 121.5, "index": index} for index in range(2000)],
                }
            ]
        }
    }
    store_ingestion(
        db_path,
        large_event_payload,
        json.dumps(large_event_payload),
        {"session-id": "large-event"},
    )
    bounded_detailed = query_health_events(
        db_path,
        categories=["workouts"],
        start_time="2026-07-18",
        end_time="2026-07-19",
        limit=10,
        output_format="detailed",
        timezone="Asia/Shanghai",
    )
    assert bounded_detailed["status"] == "partial"
    assert bounded_detailed["fetch"]["complete"] is True
    assert bounded_detailed["payloads"]["complete"] is False
    assert bounded_detailed["payloads"]["omitted"] == 1
    omitted = next(event for event in bounded_detailed["events"] if "payload_summary" in event)
    assert omitted["payload_summary"]["reason"] == "bounded_mcp_response"
    assert omitted["payload_summary"]["collection_counts"]["route"] == 2000
    assert len(json.dumps(bounded_detailed, ensure_ascii=False).encode()) < 100000

    empty_events = query_health_events(
        db_path,
        categories=["workouts"],
        start_time="2026-07-20",
        end_time="2026-07-21",
        timezone="Asia/Shanghai",
    )
    assert empty_events["status"] == "empty"
    assert empty_events["fetch"]["complete"] is True

    with pytest.raises(ValueError, match="half-open"):
        query_health_metrics(
            db_path,
            metric_names=["step_count"],
            start_time="2026-07-19",
            end_time="2026-07-18",
            timezone="Asia/Shanghai",
        )
    with pytest.raises(ValueError, match="half-open"):
        query_health_events(
            db_path,
            start_time="2026-07-19",
            end_time="2026-07-18",
            timezone="Asia/Shanghai",
        )
