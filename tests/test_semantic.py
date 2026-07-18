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
                            "inBedStart": "2026-07-17 00:19:40 +0800",
                            "inBedEnd": "2026-07-17 14:51:16 +0800",
                            "sleepStart": "2026-07-17 00:19:40 +0800",
                            "sleepEnd": "2026-07-17 14:51:16 +0800",
                            "totalSleep": 6.159862220221096,
                            "awake": 0.10895330415831672,
                            "asleep": 1.8186422409613927,
                            "core": 2.8159304676122137,
                            "deep": 0.7961757783095043,
                            "rem": 0.7291137333379851,
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
            ]
            + [
                {
                    "id": "indoor-walk",
                    "name": "室内 步行",
                    "start": "2026-07-17 12:28:34 +0800",
                    "end": "2026-07-17 12:56:49 +0800",
                    "duration": 1613.76,
                    "distance": {"qty": 0.08198, "units": "km"},
                    "activeEnergyBurned": {"qty": 116.94, "units": "kJ"},
                    "heartRate": {
                        "avg": {"qty": 72.6, "units": "count/min"},
                        "min": {"qty": 62, "units": "count/min"},
                        "max": {"qty": 89, "units": "count/min"},
                    },
                    "speed": {"qty": 0.18, "units": "km/hr"},
                    "isIndoor": True,
                    "location": "室内",
                },
                {
                    "id": "strength-short",
                    "name": "传统力量训练",
                    "start": "2026-07-18 10:01:10 +0800",
                    "end": "2026-07-18 10:02:44 +0800",
                    "duration": 93.67,
                    "activeEnergyBurned": {"qty": 21.69, "units": "kJ"},
                    "heartRate": {
                        "avg": {"qty": 86.7, "units": "count/min"},
                        "min": {"qty": 85, "units": "count/min"},
                        "max": {"qty": 91, "units": "count/min"},
                    },
                    "heartRateData": [{"Avg": 87, "source": "Apple Watch"}] * 6,
                },
            ],
        }
    }
    raw = json.dumps(payload)
    store_ingestion(db_path, payload, raw, {"session-id": "semantic-test"})


def test_sleep_semantic_contract(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    concise = get_sleep_summary(db_path, "2026-07-18")
    detailed = get_sleep_summary(db_path, "2026-07-18", output_format="detailed")
    assert concise["status"] == "ok"
    assert concise["timezone"] == "Asia/Shanghai"
    assert concise["summary"]["sleep_window"]["start"].endswith("+08:00")
    assert concise["summary"]["sleep_window"]["window_minutes"] == 446
    assert concise["summary"]["sleep_window"]["total_sleep_minutes"] == 434
    assert concise["summary"]["sleep_window"]["awake_minutes"] == 12
    assert concise["summary"]["sleep_window"]["in_bed_minutes"] is None
    assert concise["summary"]["sleep_window"]["sleep_efficiency_percent"] is None
    assert concise["summary"]["stages"]["deep_minutes"] == 93
    assert "stage_percentages" not in concise["summary"]
    assert detailed["summary"]["stage_percentages"]["deep"] == 21.4
    assert concise["comparison_to_personal_baseline"] == {
        "baseline_nights": 1,
        "strength": "insufficient_data",
    }
    assert "capabilities" not in concise
    assert detailed["capabilities"]["sleep_stage_timeline"] is False
    concise_json = json.dumps(concise, ensure_ascii=False)
    detailed_json = json.dumps(detailed, ensure_ascii=False)
    assert len(concise_json.encode()) < len(detailed_json.encode()) * 0.7
    assert len(concise_json.encode()) < 2300
    assert "technical" not in concise_json + detailed_json
    assert "original_payload" not in concise_json + detailed_json
    assert "awakenings_count" not in concise_json
    assert len(concise["highlights"]) <= 3

    anomalous = get_sleep_summary(db_path, "2026-07-17", output_format="detailed")
    assert anomalous["summary"]["sleep_window"]["in_bed_minutes"] is None
    assert anomalous["summary"]["sleep_window"]["sleep_efficiency_percent"] is None
    assert anomalous["summary"]["sleep_window"]["boundary_confidence"] == "low"
    assert anomalous["summary"]["sleep_window"]["window_minutes"] is None
    assert anomalous["summary"]["reported_boundaries"]["reported_span_minutes"] == 872
    assert {"implausible_sleep_window", "large_unclassified_gap"} <= set(
        anomalous["data_quality"]["quality_flags"]
    )
    assert anomalous["summary"]["recovery"]["sleeping_heart_rate_avg_bpm"]["value"] is None
    assert anomalous["session_id"] != concise["session_id"]

    empty = get_sleep_summary(db_path, "2026-07-10")
    assert empty["status"] == "empty"
    assert empty["fetch"]["complete"] is True
    assert empty["coverage"]["valid_nights"] == 0
    assert empty["coverage"]["missing_nights"] == 1

    trend = get_sleep_trend(db_path, nights=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["fetch"]["complete"] is True
    assert trend["coverage"]["complete"] is False
    assert trend["coverage"]["valid_nights"] == 2
    assert trend["coverage"]["missing_nights"] == 5
    assert trend["summary"]["trend_strength"] == "insufficient_data"
    assert trend["summary"]["nights"][0]["session_id"] != trend["summary"]["nights"][1]["session_id"]

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

    raw_detailed = query_sleep_fallback(
        db_path,
        sleep_names,
        start_time="2026-07-18",
        end_time="2026-07-18",
        output_format="detailed",
    )
    assert len(raw_detailed["summary"]["sleep_nights"]) == 1
    assert "payload" in raw_detailed["summary"]["sleep_nights"][0]
    assert raw_detailed["summary"]["sleep_nights"][0]["session_id"].startswith("sleep-")

    unavailable = query_sleep_fallback(db_path, ["sleep_stage_timeline"])
    assert unavailable["status"] == "partial"
    assert unavailable["reason"] == "sleep_stage_timeline_not_uploaded"


def test_recovery_activity_and_overview_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    recovery = get_recovery_snapshot(db_path, "2026-07-18")
    assert recovery["status"] == "partial"
    metrics = recovery["summary"]["recovery_metrics"]
    assert metrics["sleeping_heart_rate_avg_bpm"]["value"] == 63.0
    assert metrics["blood_oxygen_min_percent"]["value"] == 95.0
    assert metrics["wrist_temperature_c"]["value"] == 35.8
    assert metrics["hrv_near_wake_ms"]["sample_count"] == 1
    assert "reported_wake_minus_2h_to_plus_2h" in metrics["hrv_near_wake_ms"]["window"]
    assert recovery["comparison_to_personal_baseline"]["strength"] == "insufficient_data"
    assert "metrics" not in recovery["comparison_to_personal_baseline"]
    assert "个人基线不足，不生成恢复判断" in recovery["highlights"]

    recovery_detailed = get_recovery_snapshot(db_path, "2026-07-18", output_format="detailed")
    assert "metrics" in recovery_detailed["comparison_to_personal_baseline"]
    assert recovery_detailed["data_quality"]["readiness_score_generated"] is False

    activity = get_daily_activity(db_path, "2026-07-18")
    assert activity["summary"]["activity"]["step_count"] == 5000
    assert activity["summary"]["activity"]["active_energy_kcal"] == 300.0
    assert activity["summary"]["activity"]["stand_hours"] == 10
    assert activity["fetch"]["complete"] is True
    assert activity["period"]["is_closed"] is False
    assert activity["period"]["may_still_update"] is True

    trend = get_activity_trend(db_path, days=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["fetch"]["complete"] is True
    assert trend["coverage"]["valid_days"] == 2
    assert trend["coverage"]["missing_days"] == 5

    overview = semantic_health_overview(db_path, days=7, end_date="2026-07-18")
    assert overview["status"] == "partial"
    assert overview["fetch"]["complete"] is True
    assert overview["coverage"]["valid_nights"] == 2
    assert "total" not in overview["summary"]["cardiovascular"]["heart_rate"]
    assert "total" not in overview["summary"]["cardiovascular"]["heart_rate_variability"]
    assert "total" not in overview["summary"]["respiratory"]["blood_oxygen_saturation"]


def test_workout_semantic_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    listed = list_workouts(db_path, days=7, end_date="2026-07-18")
    assert listed["status"] == "partial"
    assert listed["summary"]["workout_count"] == 6
    assert "distance_km" not in listed["summary"]["totals"]
    assert "average_speed_kmh" not in listed["summary"]["totals"]
    by_type = {item["type"]: item for item in listed["summary"]["by_type"]}
    assert by_type["户外 骑行"]["count"] == 4
    assert by_type["室内 步行"]["same_type_averages"]["pace_minutes_per_km"] is None
    assert by_type["传统力量训练"]["same_type_averages"]["average_speed_kmh"] is None
    assert listed["comparison_to_personal_baseline"]["mode"] == "same_type_only"
    assert listed["fetch"]["complete"] is True
    latest = listed["summary"]["workouts"][0]
    assert latest["workout_id"] == "ride-18"
    assert "route" not in json.dumps(latest)

    indoor = next(item for item in listed["summary"]["workouts"] if item["workout_id"] == "indoor-walk")
    assert indoor["pace_minutes_per_km"] is None
    assert "distance_too_low_for_reliable_pace" in indoor["quality_flags"]
    strength = next(item for item in listed["summary"]["workouts"] if item["workout_id"] == "strength-short")
    assert {"very_short_workout", "possible_incomplete_workout"} <= set(strength["quality_flags"])

    detail = get_workout_detail(db_path, "ride-18", output_format="detailed")
    assert detail["status"] == "partial"
    assert detail["summary"]["workout"]["sensor_coverage"]["route_points"] == 1
    assert detail["summary"]["workout"]["active_energy_kcal"] == 54.5
    assert detail["comparison_to_personal_baseline"]["baseline_workouts"] == 3
    assert detail["summary"]["workout"]["max_speed_kmh"] is None
    assert detail["summary"]["workout"]["normalization"]["max_speed"]["confidence"] == "low"
    assert "latitude" not in json.dumps(detail)

    compared = compare_workouts(db_path, "ride-18")
    assert compared["status"] == "partial"
    assert compared["comparison_to_personal_baseline"]["baseline_workouts"] == 3
    assert compared["summary"]["metric_comparison"]["distance_km"]["difference"] == 0.2

    missing = get_workout_detail(db_path, "not-found")
    assert missing["status"] == "empty"
    assert missing["fetch"]["complete"] is True
    assert "AppleHealth:list_workouts" in missing["highlights"][0]
