import importlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from starlette.testclient import TestClient


def test_ingest_auth_and_health(tmp_path, monkeypatch) -> None:
    token = "i" * 48
    monkeypatch.setenv("APPLE_HEALTH_INGEST_TOKEN", token)
    monkeypatch.setenv("APPLE_HEALTH_MCP_TOKEN", "m" * 48)
    monkeypatch.setenv("APPLE_HEALTH_DB_PATH", str(tmp_path / "health.db"))

    module = importlib.import_module("apple_health_mcp.app")
    module.get_settings.cache_clear()

    payload = {
        "data": {
            "metrics": [
                {
                    "name": "heart_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "qty": 65,
                            "source": "Apple Watch",
                            "startDate": "2026-07-18T10:00:00Z",
                            "endDate": "2026-07-18T10:01:00Z",
                        }
                    ],
                }
            ]
        }
    }

    with TestClient(module.app, base_url="https://apps.supxh.xin") as client:
        unauthorized = client.post("/ingest", json=payload)
        assert unauthorized.status_code == 401

        accepted = client.post(
            "/ingest",
            content=json.dumps(payload),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "session-id": "http-test-1",
            },
        )
        assert accepted.status_code == 200
        assert accepted.json()["metric_samples_inserted"] == 1

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["metric_samples"] == 1

        mcp_unauthorized = client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            headers={"Accept": "application/json, text/event-stream"},
        )
        assert mcp_unauthorized.status_code == 401

        mcp_headers = {
            "Authorization": f"Bearer {'m' * 48}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        initialized = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "clientInfo": {"name": "test-client", "version": "1.0"},
                },
            },
            headers=mcp_headers,
        )
        assert initialized.status_code == 200
        assert initialized.json()["result"]["serverInfo"]["name"] == "AppleHealth"

        tools = client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers=mcp_headers,
        )
        assert tools.status_code == 200
        tool_definitions = {tool["name"]: tool for tool in tools.json()["result"]["tools"]}
        assert set(tool_definitions) == {
            "apple_health_guide",
            "apple_health_get_sleep_summary",
            "apple_health_get_sleep_trend",
            "apple_health_get_recovery_snapshot",
            "apple_health_get_daily_activity",
            "apple_health_get_activity_trend",
            "apple_health_list_workouts",
            "apple_health_get_workout_detail",
            "apple_health_compare_workouts",
            "apple_health_list_available_data",
            "apple_health_query_health_metrics",
            "apple_health_query_health_events",
            "apple_health_get_health_overview",
        }
        expected_required = {
            "apple_health_get_sleep_summary": ["date"],
            "apple_health_get_sleep_trend": ["nights"],
            "apple_health_get_recovery_snapshot": [],
            "apple_health_get_daily_activity": ["date"],
            "apple_health_get_activity_trend": ["days"],
            "apple_health_list_workouts": ["days"],
            "apple_health_get_workout_detail": ["workout_id"],
            "apple_health_compare_workouts": ["workout_id"],
            "apple_health_get_health_overview": ["days"],
        }
        for tool_name, required in expected_required.items():
            assert tool_definitions[tool_name]["inputSchema"].get("required", []) == required
        assert tool_definitions["apple_health_query_health_metrics"]["inputSchema"].get("required", []) == ["metric_names"]
        for tool in tool_definitions.values():
            assert tool["inputSchema"]["additionalProperties"] is False
            assert tool["annotations"]["readOnlyHint"] is True
            assert tool["annotations"]["destructiveHint"] is False
            assert tool["annotations"]["idempotentHint"] is True
            assert tool["annotations"]["openWorldHint"] is False
            for property_schema in tool["inputSchema"].get("properties", {}).values():
                assert property_schema.get("description")
        assert "status" in tool_definitions["apple_health_get_sleep_summary"]["outputSchema"]["properties"]
        assert "recommended_sequence" in tool_definitions["apple_health_guide"]["outputSchema"]["properties"]
        assert tool_definitions["apple_health_query_health_events"]["outputSchema"]["properties"]["fetch"]
        assert tool_definitions["apple_health_query_health_events"]["outputSchema"]["properties"]["payloads"]
        sleep_properties = tool_definitions["apple_health_get_sleep_summary"]["inputSchema"]["properties"]
        assert sleep_properties["date"]["pattern"] == r"^\d{4}-\d{2}-\d{2}$"
        assert sleep_properties["timezone"]["default"] == "Asia/Shanghai"
        assert sleep_properties["output_format"]["default"] == "concise"
        assert sleep_properties["include_recovery_metrics"]["default"] is True
        assert tool_definitions["apple_health_get_recovery_snapshot"]["inputSchema"]["properties"]["date"]["default"] is None
        workout_properties = tool_definitions["apple_health_list_workouts"]["inputSchema"]["properties"]
        assert workout_properties["end_date"]["default"] is None
        assert workout_properties["workout_type"]["default"] is None
        assert workout_properties["limit"]["default"] == 20
        assert workout_properties["limit"]["minimum"] == 1
        assert workout_properties["limit"]["maximum"] == 50
        metric_name_schema = tool_definitions["apple_health_query_health_metrics"]["inputSchema"]["properties"]["metric_names"]
        assert metric_name_schema["minItems"] == 1
        assert metric_name_schema["maxItems"] == 10
        compare_properties = tool_definitions["apple_health_compare_workouts"]["inputSchema"]["properties"]
        assert compare_properties["comparison_workout_id"]["default"] is None
        assert compare_properties["baseline_count"]["default"] == 5

        called = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "apple_health_list_available_data", "arguments": {}},
            },
            headers=mcp_headers,
        )
        assert called.status_code == 200
        assert called.json()["result"]["isError"] is False

        guided = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "apple_health_guide", "arguments": {}},
            },
            headers=mcp_headers,
        )
        assert guided.status_code == 200
        assert guided.json()["result"]["isError"] is False
        guide_text = json.dumps(guided.json(), ensure_ascii=False)
        assert '"tool": "apple_health_get_sleep_summary"' in guide_text
        assert '"tool": "apple_health_list_workouts"' in guide_text
        assert '"tool": "apple_health_compare_workouts"' in guide_text
        assert '"tool": "apple_health_get_health_overview"' in guide_text
        assert "AppleHealth:list_available_data / query_health_metrics / query_health_events" not in guide_text
        guide_payload = guided.json()["result"]["structuredContent"]
        assert guide_payload["as_of_date"] == datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        guide_entries = guide_payload["recommended_sequence"]
        assert {entry["tool"] for entry in guide_entries} == set(tool_definitions) - {"apple_health_guide"}
        for entry in guide_entries:
            example = entry.get("arguments_example", {})
            assert set(example) <= set(tool_definitions[entry["tool"]]["inputSchema"].get("properties", {}))

        sleep = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "apple_health_get_sleep_summary",
                    "arguments": {"date": "2026-07-18"},
                },
            },
            headers=mcp_headers,
        )
        assert sleep.status_code == 200
        assert sleep.json()["result"]["isError"] is False

        minimal_calls = [
            (6, "apple_health_get_sleep_trend", {"nights": 7}),
            (7, "apple_health_get_recovery_snapshot", {}),
            (8, "apple_health_get_daily_activity", {"date": "2026-07-18"}),
            (9, "apple_health_get_activity_trend", {"days": 7}),
            (10, "apple_health_list_workouts", {"days": 7}),
            (11, "apple_health_get_health_overview", {"days": 7}),
        ]
        for call_id, tool_name, arguments in minimal_calls:
            response = client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": call_id,
                    "method": "tools/call",
                    "params": {"name": tool_name, "arguments": arguments},
                },
                headers=mcp_headers,
            )
            assert response.status_code == 200
            assert response.json()["result"]["isError"] is False

        typo = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/call",
                "params": {
                    "name": "apple_health_get_daily_activity",
                    "arguments": {"date": "2026-07-18", "time_zone": "UTC"},
                },
            },
            headers=mcp_headers,
        )
        assert typo.status_code == 200
        assert typo.json()["result"]["isError"] is True
        assert "time_zone" in typo.json()["result"]["content"][0]["text"]
        assert "Extra inputs are not permitted" in typo.json()["result"]["content"][0]["text"]

        missing_metric_names = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 13,
                "method": "tools/call",
                "params": {"name": "apple_health_query_health_metrics", "arguments": {}},
            },
            headers=mcp_headers,
        )
        assert missing_metric_names.status_code == 200
        assert missing_metric_names.json()["result"]["isError"] is True
        assert "metric_names" in missing_metric_names.json()["result"]["content"][0]["text"]

        metric_query = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 14,
                "method": "tools/call",
                "params": {
                    "name": "apple_health_query_health_metrics",
                    "arguments": {"metric_names": ["heart_rate"], "limit": 1},
                },
            },
            headers=mcp_headers,
        )
        assert metric_query.status_code == 200
        assert metric_query.json()["result"]["isError"] is False
        metric_structured = metric_query.json()["result"]["structuredContent"]
        assert "events" not in metric_structured
        assert "event_count" not in metric_structured

        sleep_fallback = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 15,
                "method": "tools/call",
                "params": {
                    "name": "apple_health_query_health_metrics",
                    "arguments": {
                        "metric_names": ["sleep_analysis"],
                        "start_time": "2026-07-18",
                        "end_time": "2026-07-19",
                    },
                },
            },
            headers=mcp_headers,
        )
        assert sleep_fallback.status_code == 200
        assert sleep_fallback.json()["result"]["isError"] is False
        assert "summary" in sleep_fallback.json()["result"]["structuredContent"]

        event_fallback = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 16,
                "method": "tools/call",
                "params": {
                    "name": "apple_health_query_health_events",
                    "arguments": {"limit": 1, "output_format": "detailed"},
                },
            },
            headers=mcp_headers,
        )
        assert event_fallback.status_code == 200
        assert event_fallback.json()["result"]["isError"] is False
        assert event_fallback.json()["result"]["structuredContent"]["payloads"]["complete"] is True

        api_unauthorized = client.get("/api/v1/health/capabilities")
        assert api_unauthorized.status_code == 401

        api_headers = {"Authorization": f"Bearer {'m' * 48}"}
        api_capabilities = client.get("/api/v1/health/capabilities", headers=api_headers)
        assert api_capabilities.status_code == 200
        assert api_capabilities.json()["capabilities"]["nightly_sleep_totals"] is True

        api_sleep = client.get(
            "/api/v1/health/sleep/summary?date=2026-07-18",
            headers=api_headers,
        )
        assert api_sleep.status_code == 200
        assert api_sleep.json()["status"] == "empty"

        api_workouts = client.get(
            "/api/v1/health/workouts?days=7&end_date=2026-07-18",
            headers=api_headers,
        )
        assert api_workouts.status_code == 200
        assert api_workouts.json()["status"] == "empty"

        missing_workout_id = client.get(
            "/api/v1/health/workouts/detail",
            headers=api_headers,
        )
        assert missing_workout_id.status_code == 400
