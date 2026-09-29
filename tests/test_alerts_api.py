from datetime import datetime

from httpx import ASGITransport, AsyncClient

from app.db import SessionLocal
from app.main import app
from app.models import Alert


async def make_alert(vehicle_id="VIN-A"):
    async with SessionLocal() as db:
        alert = Alert(
            rule_id=1,
            vehicle_id=vehicle_id,
            status="TRIGGERED",
            triggered_at=datetime(2026, 1, 1, 10, 0, 0),
            details={"field": "speed_mph", "actual_value": 75, "threshold": 70},
        )
        db.add(alert)
        await db.commit()
        await db.refresh(alert)
        return alert.id


async def client_call(method, path):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.request(method, path)


async def test_alert_lifecycle():
    alert_id = await make_alert()

    response = await client_call("POST", f"/api/v1/alerts/{alert_id}/acknowledge")
    assert response.status_code == 200
    assert response.json()["status"] == "ACKNOWLEDGED"
    assert response.json()["acknowledged_at"] is not None

    response = await client_call("POST", f"/api/v1/alerts/{alert_id}/resolve")
    assert response.status_code == 200
    assert response.json()["status"] == "RESOLVED"
    assert response.json()["resolved_at"] is not None


async def test_invalid_transition_is_rejected():
    alert_id = await make_alert()

    await client_call("POST", f"/api/v1/alerts/{alert_id}/acknowledge")
    await client_call("POST", f"/api/v1/alerts/{alert_id}/resolve")

    # RESOLVED -> ACKNOWLEDGED is not allowed.
    response = await client_call("POST", f"/api/v1/alerts/{alert_id}/acknowledge")
    assert response.status_code == 409


async def test_resolve_directly_from_triggered_is_allowed():
    alert_id = await make_alert()
    response = await client_call("POST", f"/api/v1/alerts/{alert_id}/resolve")
    assert response.status_code == 200


async def test_list_alerts_filter_by_status():
    await make_alert()
    response = await client_call("GET", "/api/v1/alerts?status=TRIGGERED")
    assert response.status_code == 200
    assert len(response.json()) == 1

    response = await client_call("GET", "/api/v1/alerts?status=RESOLVED")
    assert response.json() == []
