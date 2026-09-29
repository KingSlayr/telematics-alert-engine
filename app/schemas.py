from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

TelemetryField = Literal["speed_mph", "fuel_level_percent", "odometer_miles"]
RuleOperator = Literal["GT", "GTE", "LT", "LTE", "EQ", "NEQ"]


class TelemetryIn(BaseModel):

    event_id: str
    vehicle_id: str
    timestamp: datetime

    speed_mph: float = Field(ge=0)
    fuel_level_percent: float = Field(ge=0, le=100)
    engine_state: Literal["ON", "OFF"]
    odometer_miles: float = Field(ge=0)


class SimpleDefinition(BaseModel):

    field: TelemetryField
    operator: RuleOperator
    value: float


class WindowedDefinition(SimpleDefinition):

    consecutive_points: int = Field(ge=1, le=100)
    # Optional: restrict the rule to specific vehicles.
    target_vehicles: list[str] | None = None


class TimeWindowDefinition(BaseModel):

    field: TelemetryField
    operator: RuleOperator
    value: float

    # At least min_matches matching events inside any window of this size.
    window_seconds: float = Field(gt=0, le=86400)
    min_matches: int = Field(ge=1, le=1000)
    target_vehicles: list[str] | None = None


class AggregateDefinition(BaseModel):

    field: TelemetryField
    agg_fn: Literal["AVG", "MAX", "MIN", "SUM"]
    operator: RuleOperator
    value: float

    # Time-based or count-based window: at least one must be set.
    window_seconds: float | None = Field(default=None, gt=0, le=86400)
    window_points: int | None = Field(default=None, ge=1, le=1000)
    target_vehicles: list[str] | None = None


RULE_TYPES = {
    "simple": SimpleDefinition,
    "windowed": WindowedDefinition,
    "time_window": TimeWindowDefinition,
    "aggregate": AggregateDefinition,
}


class RuleCreate(BaseModel):

    name: str
    rule_type: Literal["simple", "windowed", "time_window", "aggregate"]
    definition: dict
    enabled: bool = True


class RuleUpdate(BaseModel):

    name: str | None = None
    rule_type: Literal["simple", "windowed", "time_window", "aggregate"] | None = None
    definition: dict | None = None
    enabled: bool | None = None


def validate_definition(rule_type: str, definition: dict) -> dict:
    """Check a definition dict against its rule type. Raises ValueError if invalid."""
    model = RULE_TYPES.get(rule_type)

    if model is None:
        raise ValueError(f"Unknown rule_type: {rule_type}")

    validated = model(**definition).model_dump()

    if rule_type == "aggregate" and not (
        validated["window_seconds"] or validated["window_points"]
    ):
        raise ValueError("aggregate rule needs window_seconds or window_points")

    return validated
