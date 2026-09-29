from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base, utcnow


class Alert(Base):

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    rule_id: Mapped[int] = mapped_column(Integer, index=True)
    vehicle_id: Mapped[str] = mapped_column(String(100), index=True)

    # TRIGGERED -> ACKNOWLEDGED -> RESOLVED
    status: Mapped[str] = mapped_column(String(20), default="TRIGGERED")

    triggered_at: Mapped[datetime] = mapped_column(DateTime)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # What exactly triggered the alert: observed value, threshold, window count.
    details: Mapped[dict] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )
