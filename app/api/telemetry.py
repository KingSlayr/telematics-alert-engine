from fastapi import APIRouter

from app.schemas import TelemetryIn
from app.services import telemetry_service

router = APIRouter(tags=["telemetry"])


@router.post("/telemetry", status_code=202)
async def receive_telemetry(data: TelemetryIn):
    result = await telemetry_service.ingest_telemetry(data)
    return {"status": result}
