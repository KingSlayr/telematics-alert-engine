from datetime import datetime

from sqlalchemy import DateTime, Float, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base, utcnow


class Telemetry(Base):

    __tablename__ = "telemetry"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Unique event id from the sender gives us idempotency:
    # the same event sent twice is stored and processed once.
    event_id: Mapped[str] = mapped_column(String(64), unique=True)

    vehicle_id: Mapped[str] = mapped_column(String(100))
    timestamp: Mapped[datetime] = mapped_column(DateTime)

    speed_mph: Mapped[float] = mapped_column(Float)
    fuel_level_percent: Mapped[float] = mapped_column(Float)
    engine_state: Mapped[str] = mapped_column(String(20))
    odometer_miles: Mapped[float] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    # Windowed rules look up "recent telemetry for one vehicle",
    # so we index vehicle_id + timestamp together.
    __table_args__ = (
        Index("ix_telemetry_vehicle_timestamp", "vehicle_id", "timestamp"),
    )
