"""Pure rule evaluation: no database, no I/O. Easy to unit test.

Four rule types share one window-state shape (see window_state.py):

  simple       current event only (no state)
  windowed     all of the last N events must match (consecutive)
  time_window  at least K matching events within any T-second span
  aggregate    AVG/MAX/MIN/SUM over a window, compared to a threshold
"""

from datetime import timedelta

from app.processing.window_state import get_state

OPERATORS = {
    "GT": lambda value, threshold: value > threshold,
    "GTE": lambda value, threshold: value >= threshold,
    "LT": lambda value, threshold: value < threshold,
    "LTE": lambda value, threshold: value <= threshold,
    "EQ": lambda value, threshold: value == threshold,
    "NEQ": lambda value, threshold: value != threshold,
}

AGG_FNS = {
    "AVG": lambda values: sum(values) / len(values),
    "MAX": max,
    "MIN": min,
    "SUM": sum,
}


def _base_context(definition, actual_value):
    return {
        "field": definition["field"],
        "operator": definition["operator"],
        "threshold": definition["value"],
        "actual_value": actual_value,
    }


def evaluate_rule(rule, telemetry) -> tuple[bool, dict]:
    """Evaluate one rule against one event.

    Returns (matched, context). `context` describes the match and is
    stored on the created alert.
    """
    if rule.rule_type == "simple":
        return evaluate_simple(rule, telemetry)

    state = get_state(rule.id, telemetry.vehicle_id)

    evaluator = {
        "windowed": evaluate_consecutive,
        "time_window": evaluate_time_window,
        "aggregate": evaluate_aggregate,
    }[rule.rule_type]

    return evaluator(rule, telemetry, state)


def evaluate_simple(rule, telemetry):
    definition = rule.definition

    actual = getattr(telemetry, definition["field"])
    matched = OPERATORS[definition["operator"]](actual, definition["value"])

    return matched, _base_context(definition, actual)


def evaluate_consecutive(rule, telemetry, state):
    definition = rule.definition
    points = definition["consecutive_points"]
    actual = getattr(telemetry, definition["field"])

    # Every event enters the window, matches and misses alike.
    state.values.append((telemetry.timestamp, actual))
    state.values = state.values[-points:]

    context = _base_context(definition, actual)

    all_match = len(state.values) == points and all(
        OPERATORS[definition["operator"]](v, definition["value"])
        for _, v in state.values
    )

    if all_match and not state.triggered:
        # Fire once per streak, then stay quiet until a miss re-arms it.
        state.triggered = True
        context["consecutive_count"] = points
        context["consecutive_points"] = points
        return True, context

    if not all_match:
        state.triggered = False  # streak broken, alert re-armed

    return False, context


def evaluate_time_window(rule, telemetry, state):
    definition = rule.definition
    ts = telemetry.timestamp
    actual = getattr(telemetry, definition["field"])

    state.values.append((ts, actual))

    # Event-time eviction: keep only values inside the newest event's window.
    # Correct because the reorder buffer guarantees event-time order.
    window = timedelta(seconds=definition["window_seconds"])
    state.values = [
        (t, v) for (t, v) in state.values if t > ts - window
    ]

    hits = sum(
        1 for _, v in state.values
        if OPERATORS[definition["operator"]](v, definition["value"])
    )

    context = _base_context(definition, actual)
    context["hits_in_window"] = hits

    if hits >= definition["min_matches"]:
        if not state.triggered:
            state.triggered = True
            context["min_matches"] = definition["min_matches"]
            context["window_seconds"] = definition["window_seconds"]
            return True, context
        return False, context  # still violating, already alerted

    # Fewer than K hits in the window: re-arm for the next period.
    state.triggered = False
    return False, context


def evaluate_aggregate(rule, telemetry, state):
    definition = rule.definition
    ts = telemetry.timestamp
    actual = getattr(telemetry, definition["field"])

    state.values.append((ts, actual))

    if definition.get("window_seconds"):
        # Time-based eviction.
        window = timedelta(seconds=definition["window_seconds"])
        state.values = [
            (t, v) for (t, v) in state.values if t > ts - window
        ]
    else:
        # Count-based eviction.
        state.values = state.values[-definition["window_points"]:]

    values = [v for _, v in state.values]
    agg = AGG_FNS[definition["agg_fn"]](values)

    context = _base_context(definition, actual)
    context["agg_fn"] = definition["agg_fn"]
    context["agg_value"] = agg

    over = OPERATORS[definition["operator"]](agg, definition["value"])

    if over and not state.triggered:
        state.triggered = True
        context["window"] = definition.get("window_seconds") or definition["window_points"]
        return True, context

    if not over:
        state.triggered = False  # aggregate dropped back, re-arm

    return False, context
