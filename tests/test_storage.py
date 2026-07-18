from pathlib import Path

import pytest

from apple_health_mcp.storage import (
    health_overview,
    list_available_data,
    query_health_events,
    query_health_metrics,
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
    import json

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
