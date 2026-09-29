from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base, utcnow


class Rule(Base):

    __tablename__ = "rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    name: Mapped[str] = mapped_column(String(200))

    # "simple" = check the current event.
    # "windowed" = needs N consecutive matching events.
    rule_type: Mapped[str] = mapped_column(String(20))

    # Rule settings as JSON, so new rules need no code changes.
    definition: Mapped[dict] = mapped_column(JSON)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    # Bumped on every update so consumers can see that a rule changed.
    version: Mapped[int] = mapped_column(Integer, default=1)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )
