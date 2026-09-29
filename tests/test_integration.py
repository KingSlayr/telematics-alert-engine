"""End-to-end test: telemetry through workers to alerts, including ordering.

Runs the real lifespan (queues + workers + notifier) against the test database.
"""

import asyncio

from httpx import ASGITransport, AsyncClient

from app.main import app

BASE = "http://test/api/v1"


async def api(method, path, json=None):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.request(method, f"{BASE}{path}", json=json)
        return response


def payload(vehicle_id, minute, speed, event_id):
    return {
        "event_id": event_id,
        "vehicle_id": vehicle_id,
        "timestamp": f"2026-01-01T10:{minute:02d}:00Z",
        "speed_mph": speed,
        "fuel_level_percent": 50,
        "engine_state": "ON",
        "odometer_miles": 10000,
    }


async def test_end_to_end_alert_flow():
    async with app.router.lifespan_context(app):

        # Create a windowed rule: speed > 70 for 3 consecutive events.
        response = await api("POST", "/rules", json={
            "name": "Persistent speeding",
            "rule_type": "windowed",
            "definition": {
                "field": "speed_mph", "operator": "GT", "value": 70,
                "consecutive_points": 3,
            },
            "enabled": True,
        })
        assert response.status_code == 201
        rule_id = response.json()["id"]

        # VIN-A: arrives out of order (2, 1, 3, 4) but timestamps are 1..4.
        for minute in [2, 1, 3, 4]:
            speeds = {1: 65, 2: 75, 3: 78, 4: 72}
            await api("POST", "/telemetry", json=payload(
                "VIN-A", minute, speeds[minute], f"vin-a-{minute}"
            ))

        # VIN-B: no violation.
        for minute, speed in [(1, 60), (2, 62), (3, 65)]:
            await api("POST", "/telemetry", json=payload(
                "VIN-B", minute, speed, f"vin-b-{minute}"
            ))

        # Wait for the reorder buffer (2s lateness) + workers.
        await asyncio.sleep(4)

        response = await api("GET", "/alerts")
        alerts = response.json()

        vin_a = [a for a in alerts if a["vehicle_id"] == "VIN-A"]
        vin_b = [a for a in alerts if a["vehicle_id"] == "VIN-B"]

        # Exactly one alert for VIN-A, none for VIN-B.
        assert len(vin_a) == 1
        assert vin_a[0]["status"] == "TRIGGERED"
        assert vin_a[0]["details"]["consecutive_count"] == 3
        assert vin_b == []

        # Lifecycle through the API.
        alert_id = vin_a[0]["id"]
        assert (await api("POST", f"/alerts/{alert_id}/acknowledge")).status_code == 200
        assert (await api("POST", f"/alerts/{alert_id}/resolve")).status_code == 200
        # Invalid transition.
        assert (await api("POST", f"/alerts/{alert_id}/acknowledge")).status_code == 409


async def test_time_window_rule_end_to_end():
    async with app.router.lifespan_context(app):

        # Rule: 2 speeding events within any 10-minute span (non-consecutive).
        await api("POST", "/rules", json={
            "name": "Speeding 2 in 10 min",
            "rule_type": "time_window",
            "definition": {
                "field": "speed_mph", "operator": "GT", "value": 70,
                "window_seconds": 600, "min_matches": 2,
            },
            "enabled": True,
        })

        # 75, then a miss, then 72: the miss must NOT reset the count.
        for minute, speed in [(1, 75), (2, 60), (3, 72)]:
            await api("POST", "/telemetry", json=payload(
                "VIN-T", minute, speed, f"vin-t-{minute}"
            ))

        await asyncio.sleep(4)

        response = await api("GET", "/alerts?vehicle_id=VIN-T")
        alerts = response.json()

        assert len(alerts) == 1
        assert alerts[0]["details"]["hits_in_window"] == 2


async def test_late_event_is_skipped():
    async with app.router.lifespan_context(app):

        # Windowed rule with 2 consecutive points.
        await api("POST", "/rules", json={
            "name": "Two-point speeding",
            "rule_type": "windowed",
            "definition": {
                "field": "speed_mph", "operator": "GT", "value": 70,
                "consecutive_points": 2,
            },
            "enabled": True,
        })

        # Event at 10:02 arrives first and is processed.
        await api("POST", "/telemetry", json=payload("VIN-L", 2, 75, "late-1"))
        await asyncio.sleep(3.5)  # past lateness window

        # Event at 10:01 arrives AFTER 10:02 was processed -> late -> skipped.
        await api("POST", "/telemetry", json=payload("VIN-L", 1, 75, "late-2"))
        await asyncio.sleep(3.5)

        # Event at 10:03 is on time. If the late event had (wrongly) been
        # evaluated, the alert would exist with timestamp 10:01.
        await api("POST", "/telemetry", json=payload("VIN-L", 3, 75, "late-3"))
        await asyncio.sleep(3.5)

        response = await api("GET", "/alerts?vehicle_id=VIN-L")
        alerts = response.json()

        assert len(alerts) == 1
        assert alerts[0]["triggered_at"].endswith("10:03:00") or "10:03" in str(
            alerts[0]["triggered_at"]
        )
