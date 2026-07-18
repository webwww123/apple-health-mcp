import importlib
import json

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
        assert {tool["name"] for tool in tools.json()["result"]["tools"]} == {
            "guide",
            "get_sleep_summary",
            "get_sleep_trend",
            "get_recovery_snapshot",
            "get_daily_activity",
            "get_activity_trend",
            "list_workouts",
            "get_workout_detail",
            "compare_workouts",
            "list_available_data",
            "query_health_metrics",
            "query_health_events",
            "get_health_overview",
        }

        called = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "list_available_data", "arguments": {}},
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
                "params": {"name": "guide", "arguments": {}},
            },
            headers=mcp_headers,
        )
        assert guided.status_code == 200
        assert guided.json()["result"]["isError"] is False
        guide_text = json.dumps(guided.json(), ensure_ascii=False)
        assert "AppleHealth:get_sleep_summary" in guide_text
        assert "AppleHealth:list_workouts" in guide_text
        assert "AppleHealth:compare_workouts" in guide_text
        assert "AppleHealth:get_health_overview" in guide_text

        sleep = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "get_sleep_summary", "arguments": {"date": "2026-07-18"}},
            },
            headers=mcp_headers,
        )
        assert sleep.status_code == 200
        assert sleep.json()["result"]["isError"] is False

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
