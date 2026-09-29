"""Alert creation and lifecycle.

An alert is business state: it is saved to the database first.
Notifications are a side effect and happen elsewhere.
"""

from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Alert

# Which status changes are allowed. Everything else is a 409.
VALID_TRANSITIONS = {
    "TRIGGERED": {"ACKNOWLEDGED", "RESOLVED"},
    "ACKNOWLEDGED": {"RESOLVED"},
}


async def create_alert(rule, telemetry, context: dict) -> Alert | None:
    """Save a new alert, or return None if one is already active.

    Backstop against duplicate alerts: only one active (not RESOLVED)
    alert per (rule, vehicle) at a time.
    """
    async with SessionLocal() as db:

        existing = await db.execute(
            select(Alert).where(
                Alert.rule_id == rule.id,
                Alert.vehicle_id == telemetry.vehicle_id,
                Alert.status != "RESOLVED",
            )
        )

        if existing.scalar_one_or_none():
            return None

        alert = Alert(
            rule_id=rule.id,
            vehicle_id=telemetry.vehicle_id,
            status="TRIGGERED",
            triggered_at=telemetry.timestamp,
            details=context,
        )

        db.add(alert)
        await db.commit()
        await db.refresh(alert)

        return alert


async def change_status(alert_id: int, new_status: str) -> Alert:
    """Move an alert through its lifecycle, enforcing valid transitions."""
    async with SessionLocal() as db:

        alert = await db.get(Alert, alert_id)

        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")

        allowed = VALID_TRANSITIONS.get(alert.status, set())

        if new_status not in allowed:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot change alert from {alert.status} to {new_status}",
            )

        alert.status = new_status
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        if new_status == "ACKNOWLEDGED":
            alert.acknowledged_at = now
        elif new_status == "RESOLVED":
            alert.resolved_at = now

        await db.commit()
        await db.refresh(alert)

        return alert
