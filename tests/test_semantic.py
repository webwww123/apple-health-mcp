import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

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
from apple_health_mcp.storage import (
    repair_historical_metric_duplicates,
    store_ingestion,
)


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
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 08:00:00 +0800",
                            "qty": 78,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 08:00:00 +0800",
                            "qty": 76,
                        },
                    ],
                },
                {
                    "name": "heart_rate_variability",
                    "units": "ms",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 07:30:00 +0800",
                            "qty": 42,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 14:00:00 +0800",
                            "qty": 82.1,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 07:30:00 +0800",
                            "qty": 48,
                        },
                    ],
                },
                {
                    "name": "respiratory_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 04:00:00 +0800",
                            "qty": 15.5,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 05:00:00 +0800",
                            "qty": 16.5,
                        },
                    ],
                },
                {
                    "name": "blood_oxygen_saturation",
                    "units": "%",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 04:30:00 +0800",
                            "qty": 97,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 06:30:00 +0800",
                            "qty": 95,
                        },
                    ],
                },
                {
                    "name": "apple_sleeping_wrist_temperature",
                    "units": "degC",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 00:30:00 +0800",
                            "qty": 35.8,
                        }
                    ],
                },
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "source": "iPhone",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 3000,
                        },
                        {
                            "source": "iPhone",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 5000,
                        },
                    ],
                },
                {
                    "name": "walking_running_distance",
                    "units": "km",
                    "data": [
                        {
                            "source": "iPhone",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 2.1,
                        },
                        {
                            "source": "iPhone",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 3.5,
                        },
                    ],
                },
                {
                    "name": "active_energy",
                    "units": "kJ",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 836.8,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 1255.2,
                        },
                    ],
                },
                {
                    "name": "apple_exercise_time",
                    "units": "min",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 20,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 35,
                        },
                    ],
                },
                {
                    "name": "apple_stand_hour",
                    "units": "count",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 8,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 10,
                        },
                    ],
                },
                {
                    "name": "time_in_daylight",
                    "units": "min",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-17 10:00:00 +0800",
                            "qty": 25,
                        },
                        {
                            "source": "Apple Watch",
                            "date": "2026-07-18 10:00:00 +0800",
                            "qty": 40,
                        },
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
    assert concise["summary"]["start"].endswith("+08:00")
    assert concise["summary"]["total_sleep_minutes"] == 434
    assert concise["summary"]["awake_minutes"] == 12
    assert concise["summary"]["deep_minutes"] == 93
    assert concise["recovery"]["sleeping_heart_rate_avg_bpm"] == 63.0
    assert concise["baseline"] == {"nights": 1, "strength": "insufficient_data"}
    assert concise["sleep_session"]["is_closed"] is True
    assert concise["sleep_session"]["end"].endswith("+08:00")
    assert concise["ingestion"]["may_still_update"] is False
    assert detailed["summary"]["stage_percentages"]["deep"] == 21.4
    assert detailed["summary"]["sleep_window"]["in_bed_minutes"] is None
    assert detailed["sleep_session"]["is_closed"] is True
    assert "period" not in detailed
    assert "capabilities" not in concise
    assert detailed["capabilities"]["sleep_stage_timeline"] is False
    concise_json = json.dumps(concise, ensure_ascii=False)
    detailed_json = json.dumps(detailed, ensure_ascii=False)
    assert len(concise_json.encode()) < len(detailed_json.encode()) * 0.5
    assert len(concise_json.encode()) < 2000
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
    assert {"implausible_sleep_window", "large_unclassified_gap"} <= set(anomalous["data_quality"]["quality_flags"])
    assert anomalous["summary"]["recovery"]["sleeping_heart_rate_avg_bpm"]["value"] is None
    assert anomalous["summary"]["recovery"]["hrv_near_wake_ms"]["value"] is None
    assert anomalous["summary"]["recovery"]["hrv_near_wake_ms"]["sample_count"] == 0
    assert anomalous["summary"]["recovery"]["hrv_near_wake_ms"]["window"]["logic"] == (
        "not_queried_due_low_wake_boundary_confidence"
    )
    assert anomalous["diagnostic_candidates"]["reported_wake_based_hrv"]["value"] == 82.1
    assert anomalous["diagnostic_candidates"]["reported_wake_based_hrv"]["excluded_from_analysis"] is True
    anomalous_concise = get_sleep_summary(db_path, "2026-07-17")
    assert "diagnostic_candidates" not in anomalous_concise
    assert anomalous["session_id"] != concise["session_id"]

    empty = get_sleep_summary(db_path, "2026-07-10", output_format="detailed")
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
    assert trend["summary"]["nights"][1]["sleep_session"]["is_closed"] is True
    assert "2026-07-17边界置信度低" in trend["highlights"][1]

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
        end_time="2026-07-19",
    )
    assert len(merged["summary"]["sleep_nights"]) == 1
    assert "payload" not in json.dumps(merged)

    raw_detailed = query_sleep_fallback(
        db_path,
        sleep_names,
        start_time="2026-07-18",
        end_time="2026-07-19",
        output_format="detailed",
    )
    assert len(raw_detailed["summary"]["sleep_nights"]) == 1
    assert "payload" in raw_detailed["summary"]["sleep_nights"][0]
    assert raw_detailed["summary"]["sleep_nights"][0]["session_id"].startswith("sleep-")
    with pytest.raises(ValueError, match="wake-date boundary"):
        query_sleep_fallback(
            db_path,
            sleep_names,
            start_time="2026-07-18T12:00:00+08:00",
            end_time="2026-07-18T13:00:00+08:00",
        )

    unavailable = query_sleep_fallback(db_path, ["sleep_stage_timeline"])
    assert unavailable["status"] == "partial"
    assert unavailable["reason"] == "sleep_stage_timeline_not_uploaded"


def test_recovery_activity_and_overview_contracts(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)

    recovery = get_recovery_snapshot(db_path, "2026-07-18")
    assert recovery["status"] == "ok"
    metrics = recovery["summary"]["recovery"]
    assert metrics["sleeping_heart_rate_avg_bpm"] == 63.0
    assert metrics["blood_oxygen_min_percent"] == 95.0
    assert metrics["wrist_temperature_c"] == 35.8
    assert metrics["hrv_near_wake_ms"] == 48.0
    assert recovery["baseline"]["strength"] == "insufficient_data"
    assert recovery["assessment"] == {
        "status": "insufficient_baseline",
        "baseline_nights": 0,
        "minimum_required_nights": 3,
    }
    assert "个人基线不足，不生成恢复判断" in recovery["highlights"]

    recovery_detailed = get_recovery_snapshot(db_path, "2026-07-18", output_format="detailed")
    detailed_metrics = recovery_detailed["summary"]["recovery_metrics"]
    assert detailed_metrics["hrv_near_wake_ms"]["sample_count"] == 1
    assert "trusted_wake_minus_2h_to_plus_2h" in detailed_metrics["hrv_near_wake_ms"]["window"]["logic"]
    assert "metrics" in recovery_detailed["comparison_to_personal_baseline"]
    assert recovery_detailed["comparison_to_personal_baseline"]["sleep_baseline_nights"] == 1
    assert (
        recovery_detailed["comparison_to_personal_baseline"]["metrics"]["hrv_near_wake_ms"]["baseline_sample_nights"]
        == 0
    )
    assert recovery_detailed["data_quality"]["readiness_score_generated"] is False
    assert (
        len(json.dumps(recovery, ensure_ascii=False).encode())
        < len(json.dumps(recovery_detailed, ensure_ascii=False).encode()) * 0.55
    )

    anomalous_recovery = get_recovery_snapshot(db_path, "2026-07-17", output_format="detailed")
    anomalous_hrv = anomalous_recovery["summary"]["recovery_metrics"]["hrv_near_wake_ms"]
    assert anomalous_recovery["status"] == "partial"
    assert anomalous_hrv["value"] is None
    assert anomalous_hrv["sample_count"] == 0
    assert anomalous_hrv["window"]["logic"] == "not_queried_due_low_wake_boundary_confidence"
    assert anomalous_recovery["diagnostic_candidates"]["reported_wake_based_hrv"]["value"] == 82.1

    activity = get_daily_activity(db_path, "2026-07-18")
    activity_detailed = get_daily_activity(db_path, "2026-07-18", output_format="detailed")
    assert activity["summary"]["activity"]["step_count"] == 5000
    assert activity["summary"]["activity"]["active_energy_kcal"] == 300.0
    assert activity["summary"]["activity"]["stand_hours"] == 10
    assert activity["fetch"]["complete"] is True
    assert activity["period"]["is_closed"] is True
    assert activity["period"]["may_still_update"] is False
    assert len(json.dumps(activity, ensure_ascii=False).encode()) < 2000
    assert len(json.dumps(activity_detailed, ensure_ascii=False).encode()) < 4000

    trend = get_activity_trend(db_path, days=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["fetch"]["complete"] is True
    assert trend["coverage"]["valid_days"] == 2
    assert trend["coverage"]["missing_days"] == 5
    trend_detailed = get_activity_trend(db_path, days=7, end_date="2026-07-18", output_format="detailed")
    assert "by_day" not in trend_detailed["data_quality"]
    assert len(json.dumps(trend_detailed, ensure_ascii=False).encode()) < 12000

    overview = semantic_health_overview(db_path, days=7, end_date="2026-07-18")
    overview_detailed = semantic_health_overview(db_path, days=7, end_date="2026-07-18", output_format="detailed")
    assert overview["status"] == "partial"
    assert overview["fetch"]["complete"] is True
    assert overview["coverage"]["valid_nights"] == 2
    assert overview["summary"]["latest_recovery"]["hrv_near_wake_ms"] == 48.0
    assert overview["summary"]["cardiovascular"]["heart_rate_sample_count"] == 2
    assert overview["summary"]["cardiovascular"]["heart_rate_context"] == (
        "not_a_resting_or_continuous_time_weighted_average"
    )
    assert "recovery_metrics" not in json.dumps(overview)
    assert len(json.dumps(overview, ensure_ascii=False).encode()) < 4000
    assert (
        len(json.dumps(overview, ensure_ascii=False).encode())
        < len(json.dumps(overview_detailed, ensure_ascii=False).encode()) * 0.55
    )
    assert "total" not in overview_detailed["summary"]["cardiovascular"]["heart_rate"]
    assert "total" not in overview_detailed["summary"]["cardiovascular"]["heart_rate_variability"]
    assert "total" not in overview_detailed["summary"]["respiratory"]["blood_oxygen_saturation"]
    assert len(json.dumps(overview_detailed, ensure_ascii=False).encode()) < 10000

    historical = semantic_health_overview(db_path, days=7, end_date="2026-07-17")
    assert historical["summary"]["latest_recovery"]["sleep_date"] == "2026-07-17"
    no_data = semantic_health_overview(db_path, days=7, end_date="2020-01-01")
    assert no_data["status"] == "empty"
    assert no_data["summary"]["latest_recovery"] is None


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
    assert set(by_type["户外 骑行"]) == {
        "type",
        "count",
        "total_duration_minutes",
        "total_distance_km",
        "total_active_energy_kcal",
    }
    assert "comparison_to_personal_baseline" not in listed
    assert listed["fetch"]["complete"] is True
    latest = listed["summary"]["workouts"][0]
    assert latest["workout_id"] == "ride-18"
    assert set(latest) == {
        "workout_id",
        "type",
        "start",
        "duration_minutes",
        "distance_km",
        "active_energy_kcal",
        "average_heart_rate_bpm",
        "quality_flags",
    }
    assert "route" not in json.dumps(latest)

    indoor = next(item for item in listed["summary"]["workouts"] if item["workout_id"] == "indoor-walk")
    assert "distance_too_low_for_reliable_pace" in indoor["quality_flags"]
    strength = next(item for item in listed["summary"]["workouts"] if item["workout_id"] == "strength-short")
    assert "very_short_workout" in strength["quality_flags"]
    assert "possible_incomplete_workout" not in strength["quality_flags"]

    listed_detailed = list_workouts(db_path, days=7, end_date="2026-07-18", output_format="detailed")
    detailed_by_type = {item["type"]: item for item in listed_detailed["summary"]["by_type"]}
    assert detailed_by_type["室内 步行"]["same_type_averages"]["pace_minutes_per_km"] is None
    detailed_indoor = next(
        item for item in listed_detailed["summary"]["workouts"] if item["workout_id"] == "indoor-walk"
    )
    assert detailed_indoor["pace_minutes_per_km"] is None
    assert detailed_indoor["average_speed_kmh"] is None
    assert (
        len(json.dumps(listed, ensure_ascii=False).encode())
        < len(json.dumps(listed_detailed, ensure_ascii=False).encode()) * 0.55
    )

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

    cycling_ids = {
        item["workout_id"]
        for item in list_workouts(db_path, days=7, end_date="2026-07-18", workout_type="cycling")["summary"]["workouts"]
    }
    chinese_cycling_ids = {
        item["workout_id"]
        for item in list_workouts(db_path, days=7, end_date="2026-07-18", workout_type="骑行")["summary"]["workouts"]
    }
    assert cycling_ids == chinese_cycling_ids == {"ride-15", "ride-16", "ride-17", "ride-18"}
    assert (
        list_workouts(db_path, days=7, end_date="2026-07-18", workout_type="walking")["summary"]["workouts"][0][
            "workout_id"
        ]
        == "indoor-walk"
    )
    assert (
        list_workouts(db_path, days=7, end_date="2026-07-18", workout_type="力量")["summary"]["workouts"][0][
            "workout_id"
        ]
        == "strength-short"
    )

    indoor_detail = get_workout_detail(db_path, "indoor-walk", output_format="detailed")
    assert indoor_detail["comparison_to_personal_baseline"]["comparison_performed"] is False
    assert indoor_detail["comparison_to_personal_baseline"]["suppressed_reason"] == ("target_workout_quality")
    short_comparison = compare_workouts(db_path, "strength-short", output_format="detailed")
    assert short_comparison["summary"]["comparison_performed"] is False
    assert short_comparison["summary"]["metric_comparison"] == {}
    with pytest.raises(ValueError, match="same workout type"):
        compare_workouts(db_path, "ride-18", "strength-short")

    missing = get_workout_detail(db_path, "not-found")
    assert missing["status"] == "empty"
    assert missing["fetch"]["complete"] is True
    assert "list_workouts" in missing["highlights"][0]


def test_activity_duplicate_containment_and_baseline_exclusion(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    _seed_semantic_data(db_path)
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
                   sample_uuid, original_record_id, 'legacy-step-duplicate', payload_json
            FROM metric_samples
            WHERE metric_name='step_count' AND start_time='2026-07-18T02:00:00Z'
            """
        )
        connection.commit()
    repaired = repair_historical_metric_duplicates(db_path, dry_run=False)
    assert repaired["archived_rows"] == 1

    daily = get_daily_activity(db_path, "2026-07-18")
    assert daily["status"] == "partial"
    assert daily["summary"]["activity"]["step_count"] == 5000
    assert daily["data_quality"]["baseline_eligible"] is False
    assert daily["data_quality"]["quality_flags"] == ["duplicate_sources"]
    detailed_daily = get_daily_activity(db_path, "2026-07-18", output_format="detailed")
    assert detailed_daily["data_quality"]["deduplication"]["duplicates_removed"] == 1
    assert "活动数据发现重复来源" in daily["highlights"][0]

    trend = get_activity_trend(db_path, days=7, end_date="2026-07-18")
    assert trend["status"] == "partial"
    assert trend["summary"]["valid_days"] == 2
    assert trend["summary"]["trusted_days"] == 1
    assert trend["data_quality"]["baseline_eligible"] is False
    assert trend["data_quality"]["contaminated_dates"] == ["2026-07-18"]

    overview = semantic_health_overview(db_path, days=7, end_date="2026-07-18")
    assert overview["status"] == "partial"
    assert overview["data_quality"]["complete"] is False
    assert "活动数据存在重复来源" in overview["highlights"][0]


def test_current_and_future_activity_lifecycle(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    zone = ZoneInfo("Asia/Shanghai")
    today = datetime.now(zone).date()
    future = today + timedelta(days=1)
    samples = [
        {
            "source": "iPhone",
            "date": f"{(today - timedelta(days=offset)).isoformat()} 10:00:00 +0800",
            "qty": value,
        }
        for offset, value in ((3, 1000), (2, 2000), (1, 3000), (0, 9999))
    ]
    payload = {
        "data": {
            "metrics": [
                {"name": "step_count", "units": "count", "data": samples},
                {
                    "name": "sleep_analysis",
                    "units": "hr",
                    "data": [
                        {
                            "source": "Apple Watch",
                            "date": f"{future.isoformat()} 00:00:00 +0800",
                            "sleepStart": f"{today.isoformat()} 23:00:00 +0800",
                            "sleepEnd": f"{future.isoformat()} 07:00:00 +0800",
                            "totalSleep": 7.5,
                            "awake": 0.5,
                            "core": 4.0,
                            "deep": 1.5,
                            "rem": 2.0,
                        }
                    ],
                },
            ]
        }
    }
    store_ingestion(db_path, payload, json.dumps(payload), {"session-id": "current-period"})

    current = get_daily_activity(db_path, today.isoformat(), output_format="detailed")
    comparison = current["comparison_to_personal_baseline"]
    assert current["status"] == "partial"
    assert current["period"]["state"] == "in_progress"
    assert current["period"]["is_closed"] is False
    assert current["period"]["may_still_update"] is True
    assert comparison["comparison_status"] == "suppressed"
    assert comparison["suppressed_reason"] == "current_period_open"
    assert comparison["step_count_difference"] is None
    assert current["data_quality"]["baseline_eligible"] is False
    assert "current_period_open" in current["data_quality"]["quality_flags"]

    trend = get_activity_trend(db_path, days=7, end_date=today.isoformat())
    assert trend["summary"]["valid_days"] == 4
    assert trend["summary"]["trusted_days"] == 3
    assert trend["summary"]["daily_average"]["step_count"] == 2000
    assert trend["data_quality"]["open_dates"] == [today.isoformat()]
    assert "current_period_open" in trend["data_quality"]["quality_flags"]

    future_activity = get_daily_activity(db_path, future.isoformat())
    assert future_activity["status"] == "empty"
    assert future_activity["period"] == {
        "state": "not_started",
        "has_started": False,
        "is_closed": False,
        "may_still_update": False,
    }
    assert future_activity["comparison_to_personal_baseline"]["suppressed_reason"] == ("future_period")
    future_sleep = get_sleep_summary(db_path, future.isoformat())
    assert future_sleep["status"] == "empty"
    assert future_sleep["period"]["state"] == "not_started"
    assert future_sleep["ingestion"]["may_still_update"] is False
    default_recovery = get_recovery_snapshot(db_path)
    assert default_recovery["status"] == "empty"
    assert default_recovery["requested_range"]["date"] == today.isoformat()
