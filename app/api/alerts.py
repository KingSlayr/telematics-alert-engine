from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Alert
from app.services import alert_service

router = APIRouter(tags=["alerts"])


def alert_to_dict(alert: Alert) -> dict:
    return {
        "id": alert.id,
        "rule_id": alert.rule_id,
        "vehicle_id": alert.vehicle_id,
        "status": alert.status,
        "triggered_at": alert.triggered_at,
        "acknowledged_at": alert.acknowledged_at,
        "resolved_at": alert.resolved_at,
        "details": alert.details,
    }


@router.get("/alerts")
async def list_alerts(status: str | None = None, vehicle_id: str | None = None):

    async with SessionLocal() as db:

        query = select(Alert).order_by(Alert.id)

        if status:
            query = query.where(Alert.status == status)

        if vehicle_id:
            query = query.where(Alert.vehicle_id == vehicle_id)

        result = await db.execute(query)
        alerts = result.scalars().all()

        return [alert_to_dict(alert) for alert in alerts]


@router.get("/alerts/{alert_id}")
async def get_alert(alert_id: int):

    async with SessionLocal() as db:

        alert = await db.get(Alert, alert_id)

        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")

        return alert_to_dict(alert)


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int):
    alert = await alert_service.change_status(alert_id, "ACKNOWLEDGED")
    return alert_to_dict(alert)


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: int):
    alert = await alert_service.change_status(alert_id, "RESOLVED")
    return alert_to_dict(alert)
