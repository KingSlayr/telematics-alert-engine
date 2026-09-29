import uuid

from sqlalchemy import func, select

from app.api.telemetry import router  # noqa: F401 (ensures module imports cleanly)
from app.db import SessionLocal
from app.main import app
from app.models import Telemetry
from httpx import ASGITransport, AsyncClient


async def post_telemetry(event_id=None, vehicle_id="VIN-A", speed=65, minute=1):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        payload = {
            "event_id": event_id or str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "timestamp": f"2026-01-01T10:0{minute}:00Z",
            "speed_mph": speed,
            "fuel_level_percent": 50,
            "engine_state": "ON",
            "odometer_miles": 10000,
        }
        return await client.post("/api/v1/telemetry", json=payload)


async def telemetry_count():
    async with SessionLocal() as db:
        result = await db.execute(select(func.count()).select_from(Telemetry))
        return result.scalar()


async def test_valid_telemetry_is_accepted():
    response = await post_telemetry()
    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    assert await telemetry_count() == 1


async def test_duplicate_event_id_is_not_stored_twice():
    event_id = str(uuid.uuid4())
    await post_telemetry(event_id=event_id)
    response = await post_telemetry(event_id=event_id)

    assert response.status_code == 202
    assert response.json() == {"status": "duplicate"}
    assert await telemetry_count() == 1  # still one row


async def test_invalid_payload_is_rejected():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        payload = {
            "event_id": str(uuid.uuid4()),
            "vehicle_id": "VIN-A",
            "timestamp": "2026-01-01T10:00:00Z",
            "speed_mph": -5,  # invalid: must be >= 0
            "fuel_level_percent": 50,
            "engine_state": "ON",
            "odometer_miles": 10000,
        }
        response = await client.post("/api/v1/telemetry", json=payload)
    assert response.status_code == 422
