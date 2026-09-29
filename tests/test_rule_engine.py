from datetime import datetime, timezone

import pytest
from types import SimpleNamespace

from app.processing import window_state
from app.services.rule_engine import evaluate_rule


def make_rule(rule_id=1, rule_type="simple", **definition):
    return SimpleNamespace(id=rule_id, rule_type=rule_type, definition=definition)


def make_event(vehicle_id="VIN-A", speed=65.0, minute=1):
    return SimpleNamespace(
        vehicle_id=vehicle_id,
        timestamp=datetime(2026, 1, 1, 10, minute % 60, 0, tzinfo=timezone.utc),
        speed_mph=speed,
        fuel_level_percent=50,
        odometer_miles=10000,
    )


@pytest.fixture(autouse=True)
def reset_window_state():
    window_state.window_states.clear()
    yield
    window_state.window_states.clear()


# -------------------------
# Simple rules
# -------------------------


def test_simple_rule_match():
    rule = make_rule(field="speed_mph", operator="GT", value=70)
    matched, _ = evaluate_rule(rule, make_event(speed=75))
    assert matched is True


def test_simple_rule_no_match():
    rule = make_rule(field="speed_mph", operator="GT", value=70)
    matched, _ = evaluate_rule(rule, make_event(speed=65))
    assert matched is False


def test_operators():
    event = make_event(speed=70)

    for operator, expected in [
        ("GT", False),
        ("GTE", True),
        ("LT", False),
        ("LTE", True),
        ("EQ", True),
        ("NEQ", False),
    ]:
        rule = make_rule(field="speed_mph", operator=operator, value=70)
        matched, _ = evaluate_rule(rule, event)
        assert matched is expected, f"operator {operator} failed"


# -------------------------
# Windowed rules
# -------------------------


def make_window_rule(rule_id=1, points=3):
    return make_rule(
        rule_id=rule_id,
        rule_type="windowed",
        field="speed_mph",
        operator="GT",
        value=70,
        consecutive_points=points,
    )


def test_window_fires_on_third_consecutive():
    rule = make_window_rule()
    results = [
        evaluate_rule(rule, make_event(speed=speed, minute=m))[0]
        for m, speed in [(1, 65), (2, 75), (3, 78), (4, 72)]
    ]
    assert results == [False, False, False, True]


def test_window_reset_on_miss():
    rule = make_window_rule()
    # 75, 78, 65, 72: the miss breaks the streak, so 3-consecutive never fires.
    results = [
        evaluate_rule(rule, make_event(speed=speed, minute=m))[0]
        for m, speed in [(1, 75), (2, 78), (3, 65), (4, 72)]
    ]
    assert results == [False, False, False, False]


def test_no_duplicate_alerts_within_streak():
    rule = make_window_rule()
    speeds = [75, 78, 72, 73, 76, 80]
    results = [
        evaluate_rule(rule, make_event(speed=speed, minute=m))[0]
        for m, speed in enumerate(speeds, start=1)
    ]
    # Fires on the 3rd point only, even though all points violate.
    assert results == [False, False, True, False, False, False]


def test_window_rearms_after_miss():
    rule = make_window_rule()
    # First streak fires, miss re-arms, second streak fires again.
    first = [evaluate_rule(rule, make_event(speed=s, minute=m))[0] for m, s in
             [(1, 75), (2, 78), (3, 72)]]
    between = evaluate_rule(rule, make_event(speed=60, minute=4))[0]
    second = [evaluate_rule(rule, make_event(speed=s, minute=m))[0] for m, s in
              [(5, 75), (6, 78), (7, 72)]]
    assert first == [False, False, True]
    assert between is False
    assert second == [False, False, True]


def test_independent_vehicle_state():
    rule = make_window_rule()
    # VIN-A builds a streak of 2, VIN-B is fresh.
    evaluate_rule(rule, make_event(vehicle_id="VIN-A", speed=75, minute=1))
    evaluate_rule(rule, make_event(vehicle_id="VIN-A", speed=78, minute=2))

    matched_b, _ = evaluate_rule(rule, make_event(vehicle_id="VIN-B", speed=72, minute=1))
    assert matched_b is False  # VIN-B starts at count 0, not 2


def test_rule_update_clears_state():
    rule = make_window_rule()
    evaluate_rule(rule, make_event(speed=75, minute=1))
    evaluate_rule(rule, make_event(speed=78, minute=2))  # count = 2

    # Simulates the API clearing state after an update.
    window_state.clear_rule_state(rule.id)

    matched, _ = evaluate_rule(rule, make_event(speed=72, minute=3))
    assert matched is False  # streak restarts at 0 -> count is 1, not 3
