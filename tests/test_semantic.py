import json
from pathlib import Path

from apple_health_mcp.semantic import (
    compare_workouts,
    get_activity_trend,
    get_daily_activity,
    get_recovery_snapshot,
    get_sleep_summary,
    get_sleep_trend,
    get_workout_detail,
    list_workouts,
    query_sleep_fallback,
    semantic_health_overview,
)
from apple_health_mcp.storage import store_ingestion


def _seed_semantic_data(db_path: Path) -> None:
    payload = {
        "data": {
            "metrics": [
                {
                    "name": "sleep_analysis",
                    "units": "hr",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 00:00:00 +0800",
                            "inBed": 0,
                            "inBedStart": "2026-07-17 00:30:00 +0800",
                            "inBedEnd": "2026-07-17 07:00:00 +0800",
                            "sleepStart": "2026-07-17 00:30:00 +0800",
                            "sleepEnd": "2026-07-17 07:00:00 +0800",
                            "totalSleep": 6.0,
                            "awake": 0.5,
                            "asleep": 0.5,
                            "core": 3.0,
                            "deep": 1.0,
                            "rem": 1.5,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 00:00:00 +0800",
                            "inBed": 0,
                            "inBedStart": "2026-07-18 01:01:41 +0800",
                            "inBedEnd": "2026-07-18 08:27:43 +0800",
                            "sleepStart": "2026-07-18 01:01:41 +0800",
                            "sleepEnd": "2026-07-18 08:27:43 +0800",
                            "totalSleep": 7.23277074240976,
                            "awake": 0.20114628487163122,
                            "asleep": 0,
                            "core": 4.081520953774453,
                            "deep": 1.5421123848358789,
                            "rem": 1.609137403799428,
                        },
                    ],
                },
                {
                    "name": "heart_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 03:00:00 +0800",
                            "Avg": 64,
                            "Min": 55,
                            "Max": 76,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 06:00:00 +0800",
                            "Avg": 62,
                            "Min": 53,
                            "Max": 72,
                        },
                    ],
                },
                {
                    "name": "resting_heart_rate",
                    "units": "count/min",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 08:00:00 +0800", "qty": 78},
                        {"source": "Apple Watch", "date": "2026-07-18 08:00:00 +0800", "qty": 76},
                    ],
                },
                {
                    "name": "heart_rate_variability",
                    "units": "ms",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 07:30:00 +0800", "qty": 42},
                        {"source": "Apple Watch", "date": "2026-07-18 07:30:00 +0800", "qty": 48},
                    ],
                },
                {
                    "name": "respiratory_rate",
                    "units": "count/min",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-18 04:00:00 +0800", "qty": 15.5},
                        {"source": "Apple Watch", "date": "2026-07-18 05:00:00 +0800", "qty": 16.5},
                    ],
                },
                {
                    "name": "blood_oxygen_saturation",
                    "units": "%",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-18 04:30:00 +0800", "qty": 97},
                        {"source": "Apple Watch", "date": "2026-07-18 06:30:00 +0800", "qty": 95},
                    ],
                },
                {
                    "name": "apple_sleeping_wrist_temperature",
                    "units": "degC",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-18 00:30:00 +0800", "qty": 35.8}
                    ],
                },
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {"source": "iPhone", "date": "2026-07-17 10:00:00 +0800", "qty": 3000},
                        {"source": "iPhone", "date": "2026-07-18 10:00:00 +0800", "qty": 5000},
                    ],
                },
                {
                    "name": "walking_running_distance",
                    "units": "km",
                    "data": [
                        {"source": "iPhone", "date": "2026-07-17 10:00:00 +0800", "qty": 2.1},
                        {"source": "iPhone", "date": "2026-07-18 10:00:00 +0800", "qty": 3.5},
                    ],
                },
                {
                    "name": "active_energy",
                    "units": "kJ",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 10:00:00 +0800", "qty": 836.8},
                        {"source": "Apple Watch", "date": "2026-07-18 10:00:00 +0800", "qty": 1255.2},
                    ],
                },
                {
                    "name": "apple_exercise_time",
                    "units": "min",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 10:00:00 +0800", "qty": 20},
                        {"source": "Apple Watch", "date": "2026-07-18 10:00:00 +0800", "qty": 35},
                    ],
                },
                {
                    "name": "apple_stand_hour",
                    "units": "count",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 10:00:00 +0800", "qty": 8},
                        {"source": "Apple Watch", "date": "2026-07-18 10:00:00 +0800", "qty": 10},
                    ],
                },
                {
                    "name": "time_in_daylight",
                    "units": "min",
                    "data": [
                        {"source": "Apple Watch", "date": "2026-07-17 10:00:00 +0800", "qty": 25},
                        {"source": "Apple Watch", "date": "2026-07-18 10:00:00 +0800", "qty": 40},
                    ],
                },
            ],
            "workouts": [
                {
                    "id": f"ride-{day}",
                    "name": "户外 骑行",
                    "start": f"2026-07-{day:02d} 18:00:00 +0800",
                    "end": f"2026-07-{day:02d} 18:{10 + day:02d}:00 +0800",
                    "duration": (10 + day) * 60,
                    "distance": {"qty": 2.0 + day / 10, "units": "km"},
                    "activeEnergyBurned": {"qty": 210 + day, "units": "kJ"},
                    "heartRate": {
                        "avg": {"qty": 100 + day, "units": "count/min"},
                        "min": {"qty": 85 + day, "units": "count/min"},
                        "max": {"qty": 125 + day, "units": "count/min"},
                    },
                    "speed": {"qty": 8 + day / 10, "units": "km/hr"},
                    "maxSpeed": {"qty": 16 + day / 10, "units": "km"},
                    "elevationUp": {"qty": 10 + day, "units": "m"},
                    "isIndoor": False,
                    "location": "户外",
                    "heartRateData": [
                        {
                            "Avg": 100 + day,
                            "source": "Apple Watch",
                            "date": f"2026-07-{day:02d} 18:05:00 +0800",
                        }
                    ],
                    "route": [{"latitude": 0, "longitude": 0}],
                }
                for day in (15, 16, 17, 18)
            ],
        }
    }
    raw = json.dumps(payload)
    store_ingestion(db_path, payload, raw, {"session-id": "semantic-test"})


def test_sleep_semantic_contract(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    result = get_sleep_summary(db_path, "2026-07-18")
    assert result["status"] == "ok"
    assert result["timezone"] == "Asia/Shanghai"
    assert result["summary"]["sleep_window"]["start"].endswith("+08:00")
    assert result["summary"]["sleep_window"]["window_minutes"] == 446
    assert result["summary"]["sleep_window"]["total_sleep_minutes"] == 434
    assert result["summary"]["sleep_window"]["awake_minutes"] == 12
    assert result["summary"]["sleep_window"]["in_bed_minutes"] == 446
    assert result["summary"]["stages"]["deep_minutes"] == 93
    assert result["summary"]["stage_percentages"]["deep"] == 21.4
    assert result["comparison_to_personal_baseline"]["baseline_nights"] == 1
    assert result["comparison_to_personal_baseline"]["strength"] == "insufficient_data"
    assert result["capabilities"]["sleep_stage_timeline"] is False
    assert "summary.sleep_window.in_bed_minutes" in result["data_quality"]["derived_fields"]
    assert "awakenings_count" not in json.dumps(result["summary"])
    assert len(result["highlights"]) <= 3

    empty = get_sleep_summary(db_path, "2026-07-10")
    assert empty["status"] == "empty"
    assert empty["data_quality"]["valid_nights"] == 0
    assert empty["data_quality"]["missing_nights"] == 1

    trend = get_sleep_trend(db_path, nights=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["data_quality"]["valid_nights"] == 2
    assert trend["data_quality"]["missing_nights"] == 5
    assert trend["summary"]["trend_strength"] == "insufficient_data"

    sleep_names = [
        "sleep_analysis",
        "sleep_analysis.asleep",
        "sleep_analysis.awake",
        "sleep_analysis.core",
        "sleep_analysis.deep",
        "sleep_analysis.in_bed",
        "sleep_analysis.rem",
        "sleep_analysis.total_sleep",
    ]
    merged = query_sleep_fallback(
        db_path,
        sleep_names,
        start_time="2026-07-18",
        end_time="2026-07-18",
    )
    assert len(merged["summary"]["sleep_nights"]) == 1
    assert "payload" not in json.dumps(merged)

    detailed = query_sleep_fallback(
        db_path,
        sleep_names,
        start_time="2026-07-18",
        end_time="2026-07-18",
        output_format="detailed",
    )
    assert len(detailed["summary"]["sleep_nights"]) == 1
    assert "payload" in detailed["summary"]["sleep_nights"][0]
    assert detailed["summary"]["sleep_nights"][0]["session_id"] == "semantic-test"

    unavailable = query_sleep_fallback(db_path, ["sleep_stage_timeline"])
    assert unavailable["status"] == "partial"
    assert unavailable["reason"] == "sleep_stage_timeline_not_uploaded"


def test_recovery_activity_and_overview_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    recovery = get_recovery_snapshot(db_path, "2026-07-18")
    assert recovery["status"] == "ok"
    assert recovery["summary"]["recovery_metrics"]["sleeping_heart_rate_avg_bpm"] == 63.0
    assert recovery["summary"]["recovery_metrics"]["blood_oxygen_min_percent"] == 95.0
    assert recovery["summary"]["recovery_metrics"]["wrist_temperature_c"] == 35.8
    assert recovery["comparison_to_personal_baseline"]["strength"] == "insufficient_data"

    activity = get_daily_activity(db_path, "2026-07-18")
    assert activity["summary"]["activity"]["step_count"] == 5000
    assert activity["summary"]["activity"]["active_energy_kcal"] == 300.0
    assert activity["summary"]["activity"]["stand_hours"] == 10

    trend = get_activity_trend(db_path, days=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["data_quality"]["valid_days"] == 2
    assert trend["data_quality"]["missing_days"] == 5

    overview = semantic_health_overview(db_path, days=7, end_date="2026-07-18")
    assert overview["status"] == "partial"
    assert overview["data_quality"]["valid_nights"] == 2
    assert "total" not in overview["summary"]["cardiovascular"]["heart_rate"]
    assert "total" not in overview["summary"]["cardiovascular"]["heart_rate_variability"]
    assert "total" not in overview["summary"]["respiratory"]["blood_oxygen_saturation"]


def test_workout_semantic_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    listed = list_workouts(db_path, days=7, end_date="2026-07-18")
    assert listed["status"] == "ok"
    assert listed["summary"]["workout_count"] == 4
    assert listed["summary"]["by_type"] == [{"type": "户外 骑行", "count": 4}]
    assert listed["data_quality"]["missing_days"] is None
    latest = listed["summary"]["workouts"][0]
    assert latest["workout_id"] == "ride-18"
    assert "route" not in json.dumps(latest)

    detail = get_workout_detail(db_path, "ride-18", output_format="detailed")
    assert detail["status"] == "ok"
    assert detail["summary"]["workout"]["sensor_coverage"]["route_points"] == 1
    assert detail["summary"]["workout"]["active_energy_kcal"] == 54.5
    assert detail["comparison_to_personal_baseline"]["baseline_workouts"] == 3
    assert "latitude" not in json.dumps(detail)

    compared = compare_workouts(db_path, "ride-18")
    assert compared["status"] == "ok"
    assert compared["comparison_to_personal_baseline"]["baseline_workouts"] == 3
    assert compared["summary"]["metric_comparison"]["distance_km"]["difference"] == 0.2

    missing = get_workout_detail(db_path, "not-found")
    assert missing["status"] == "empty"
    assert "AppleHealth:list_workouts" in missing["highlights"][0]
