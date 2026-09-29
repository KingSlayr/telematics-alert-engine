"""Tests for time-windowed and aggregate rules (non-consecutive window semantics)."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.processing import window_state
from app.services.rule_engine import evaluate_rule

BASE = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def make_event(speed, seconds):
    return SimpleNamespace(
        vehicle_id="VIN-T",
        timestamp=BASE + timedelta(seconds=seconds),
        speed_mph=speed,
        fuel_level_percent=50,
        odometer_miles=10000,
    )


def evaluate_sequence(rule, events):
    return [evaluate_rule(rule, make_event(speed, seconds))[0] for seconds, speed in events]


@pytest.fixture(autouse=True)
def reset_state():
    window_state.window_states.clear()
    yield
    window_state.window_states.clear()


def make_time_rule():
    return SimpleNamespace(
        id=1,
        rule_type="time_window",
        definition={
            "field": "speed_mph", "operator": "GT", "value": 70,
            "window_seconds": 600, "min_matches": 3,
        },
    )


def make_agg_rule(agg_fn="AVG", operator="GT", value=65, **window):
    return SimpleNamespace(
        id=2,
        rule_type="aggregate",
        definition={
            "field": "speed_mph", "agg_fn": agg_fn,
            "operator": operator, "value": value, **window,
        },
    )


# -------------------------
# Time-windowed rules
# -------------------------


def test_time_window_fires_on_kth_hit_within_span():
    rule = make_time_rule()
    # 3 matches inside 10 minutes, a miss in between does NOT reset anything.
    events = [(120, 75), (240, 68), (420, 72), (540, 73)]
    assert evaluate_sequence(rule, events) == [False, False, False, True]


def test_time_window_rearms_when_hits_age_out():
    rule = make_time_rule()
    events = [
        (120, 75), (240, 68), (420, 72), (540, 73),  # fires at 540
        (600, 71),   # still 4 hits in window: quiet
        (780, 75),   # 75@120 aged out, 4 hits remain: quiet
        (1200, 65),  # only 2 hits left: re-arm
        (1260, 60),  # 1 hit
    ]
    assert evaluate_sequence(rule, events) == [False, False, False, True, False, False, False, False]


def test_time_window_non_consecutive_hits():
    rule = make_time_rule()
    # A streak counter would reset at the 40 mph miss; the window does not.
    events = [(60, 75), (120, 40), (180, 72), (240, 73)]
    assert evaluate_sequence(rule, events) == [False, False, False, True]


def test_time_window_fires_again_after_quiet_period():
    rule = make_time_rule()
    first = evaluate_sequence(rule, [(60, 75), (120, 72), (180, 73)])
    gap = evaluate_sequence(rule, [(3000, 60)])
    second = evaluate_sequence(rule, [(3060, 75), (3120, 72), (3180, 73)])
    assert first == [False, False, True]
    assert gap == [False]      # old hits aged out, re-armed
    assert second == [False, False, True]


# -------------------------
# Aggregate rules
# -------------------------


def test_avg_over_time_window():
    rule = make_agg_rule("AVG", "GT", 65, window_seconds=60)
    events = [(0, 60), (20, 70), (40, 72), (60, 30), (80, 90), (100, 95)]
    # avg: 60 -> 65 -> 67.3 (FIRE) -> 57.3 (re-arm) -> 64.0 -> 71.7 (FIRE again)
    assert evaluate_sequence(rule, events) == [False, False, True, False, False, True]


def test_max_over_last_n_points():
    rule = make_agg_rule("MAX", "GT", 85, window_points=5)
    events = [(1, 60), (2, 65), (3, 90), (4, 50), (5, 10)]
    # MAX stays above 85 for the whole window once 90 enters it.
    assert evaluate_sequence(rule, events) == [False, False, True, False, False]


def test_avg_does_not_fire_below_threshold():
    rule = make_agg_rule("AVG", "GT", 90, window_seconds=600)
    events = [(10, 60), (20, 70), (30, 80)]
    assert evaluate_sequence(rule, events) == [False, False, False]
