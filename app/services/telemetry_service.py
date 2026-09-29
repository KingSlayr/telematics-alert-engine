"""Telemetry ingestion: validate (done by the schema), persist, dispatch.

The endpoint must stay fast — no rule evaluation, no history queries here.
"""

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.db import SessionLocal
from app.models import Telemetry
from app.processing import dispatcher


async def ingest_telemetry(data) -> str:
    """Persist the event and hand it to the processing pipeline.

    Returns "accepted" or "duplicate".
    """
    async with SessionLocal() as db:

        telemetry = Telemetry(
            event_id=data.event_id,
            vehicle_id=data.vehicle_id,
            timestamp=data.timestamp,
            speed_mph=data.speed_mph,
            fuel_level_percent=data.fuel_level_percent,
            engine_state=data.engine_state,
            odometer_miles=data.odometer_miles,
        )

        db.add(telemetry)

        try:
            await db.commit()
        except IntegrityError:
            # event_id already exists: the same event was sent twice.
            await db.rollback()
            return "duplicate"

    if not dispatcher.dispatch(data):
        raise HTTPException(
            status_code=503,
            detail="Processing queue is full, try again later",
        )

    return "accepted"
