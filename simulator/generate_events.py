"""Generate a large telemetry dataset and the alerts the server SHOULD raise.

Writes dataset/dataset.json containing:
  rules    - rule definitions to create through the API
  events   - telemetry events, in the order they should be sent
  expected - one expected alert per (rule, vehicle) pair that fires:
             {"rule", "vehicle_id", "triggered_at"}

Why exactly one alert per firing pair: alerts stay active until resolved,
and the server suppresses duplicates per (rule, vehicle). The replay never
resolves alerts, so the first fire of each pair is the only one stored.

The event order is shuffled across vehicles (so concurrent batches really
interleave) but per-vehicle disorder stays inside small chunks, which the
server's reorder buffer is designed to absorb.

Usage:
  python simulator/generate_events.py --vehicles 20 --events-per-vehicle 150

Vehicle scenarios round-robin, so the rule matrix is covered:
  v % 4 == 0  calm          -> no rule fires
  v % 4 == 1  burst driver  -> fires all four rules
  v % 4 == 2  oscillating   -> fires only the time-window rule
  v % 4 == 3  steady ramp   -> fires only the aggregate rule
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUTPUT = Path(__file__).resolve().parents[1] / "dataset" / "dataset.json"

RULES = [
    {"name": "Simple overspeed", "rule_type": "simple",
     "definition": {"field": "speed_mph", "operator": "GT", "value": 85}},
    {"name": "Consecutive overspeed", "rule_type": "windowed",
     "definition": {"field": "speed_mph", "operator": "GT", "value": 80,
                    "consecutive_points": 3}},
    {"name": "Speeding 5 in 10 min", "rule_type": "time_window",
     "definition": {"field": "speed_mph", "operator": "GT", "value": 75,
                    "window_seconds": 600, "min_matches": 5}},
    {"name": "Sustained speed", "rule_type": "aggregate",
     "definition": {"field": "speed_mph", "agg_fn": "AVG", "operator": "GT",
                    "value": 65, "window_seconds": 120}},
]

OPERATORS = {
    "GT": lambda a, b: a > b,
    "GTE": lambda a, b: a >= b,
    "LT": lambda a, b: a < b,
    "LTE": lambda a, b: a <= b,
    "EQ": lambda a, b: a == b,
    "NEQ": lambda a, b: a != b,
}


def speed_for(scenario: int, k: int) -> float:
    """Speed of the k-th event (1s event-time cadence) for a scenario."""
    kind = scenario % 4

    if kind == 0:                                       # calm: nothing fires
        return 30 + (k % 7) * 3 + (k * 7 % 11)          # 30..58

    if kind == 1:                                       # burst driver
        return [55, 82, 84, 86, 83, 60, 58, 87, 56, 57][k % 10]

    if kind == 2:                                       # oscillating: spikes, low avg
        return [76, 50, 78, 50, 79, 50, 77, 50, 76, 50][k % 10]

    return min(50 + k // 2, 75)                         # steady ramp to 75 plateau


def build_events(vehicles: int, per_vehicle: int, shuffle_window: int, seed: int):
    base = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    per_vehicle_lists = []
    for v in range(vehicles):
        vehicle_id = f"VIN-{v:03d}"
        events = []
        for k in range(per_vehicle):
            events.append({
                "event_id": f"{vehicle_id}-{k:06d}",
                "vehicle_id": vehicle_id,
                "timestamp": (base + timedelta(seconds=k)).isoformat(),
                "speed_mph": speed_for(v, k),
                "fuel_level_percent": max(5.0, 100 - k * 0.05),
                "engine_state": "ON",
                "odometer_miles": 10000 + v * 137 + k * 0.5,
            })
        per_vehicle_lists.append(events)

    # Interleave by rounds (one event per vehicle per round), vehicles shuffled
    # within each round, then shuffle inside small chunks so that a vehicle's
    # events can arrive slightly out of order — bounded disorder that the
    # server's reorder buffer absorbs.
    rng = random.Random(seed)

    rounds = []
    for k in range(per_vehicle):
        round_events = [per_vehicle_lists[v][k] for v in range(vehicles)]
        rng.shuffle(round_events)
        rounds.append(round_events)

    flat = [event for round_events in rounds for event in round_events]

    if shuffle_window > 1:
        for i in range(0, len(flat), shuffle_window):
            chunk = flat[i:i + shuffle_window]
            rng.shuffle(chunk)
            flat[i:i + shuffle_window] = chunk

    return flat


def _op(name, a, b):
    return OPERATORS[name](a, b)


def first_fire(rule: dict, events: list):
    """Return the first event that triggers the rule, or None.

    Independent simulation of the engine semantics (app/services/rule_engine.py).
    Only the FIRST fire matters: later fires are suppressed because the alert
    stays active (the replay never resolves anything).
    """
    rule_type = rule["rule_type"]
    d = rule["definition"]
    field, operator, value = d["field"], d["operator"], d["value"]

    values = []  # (timestamp, value) — mirrors WindowState

    for event in sorted(events, key=lambda e: e["timestamp"]):
        ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        current = event[field]

        if rule_type == "simple":
            if _op(operator, current, value):
                return event
            continue

        values.append((ts, current))

        if rule_type == "windowed":
            points = d["consecutive_points"]
            values = values[-points:]
            cond = len(values) == points and all(
                _op(operator, v, value) for _, v in values
            )

        elif rule_type == "time_window":
            window = timedelta(seconds=d["window_seconds"])
            values = [(t, v) for (t, v) in values if t > ts - window]
            hits = sum(1 for _, v in values if _op(operator, v, value))
            cond = hits >= d["min_matches"]

        else:  # aggregate
            if d.get("window_seconds"):
                window = timedelta(seconds=d["window_seconds"])
                values = [(t, v) for (t, v) in values if t > ts - window]
            else:
                values = values[-d["window_points"]:]
            agg_values = [v for _, v in values]
            agg = {"AVG": lambda vs: sum(vs) / len(vs),
                   "MAX": max, "MIN": min, "SUM": sum}[d["agg_fn"]](agg_values)
            cond = _op(operator, agg, value)

        if cond:
            return event  # first fire = the expected alert

    return None


def build_expected(rules, events_by_vehicle):
    expected = []
    for rule in rules:
        for vehicle_id, events in sorted(events_by_vehicle.items()):
            fired = first_fire(rule, events)
            if fired:
                expected.append({
                    "rule": rule["name"],
                    "vehicle_id": vehicle_id,
                    "triggered_at": fired["timestamp"],
                })
    return expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vehicles", type=int, default=20)
    parser.add_argument("--events-per-vehicle", type=int, default=150)
    parser.add_argument("--shuffle-window", type=int, default=None,
                        help="events per shuffled chunk (default: 2 x vehicles)")
    parser.add_argument("--no-shuffle", action="store_true",
                        help="send in strict event-time order (no reordering test)")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    shuffle_window = 0 if args.no_shuffle else (
        args.shuffle_window or 2 * args.vehicles
    )

    events = build_events(
        args.vehicles, args.events_per_vehicle, shuffle_window, args.seed
    )

    events_by_vehicle = {}
    for event in events:
        events_by_vehicle.setdefault(event["vehicle_id"], []).append(event)

    expected = build_expected(RULES, events_by_vehicle)

    dataset = {
        "rules": RULES,
        "events": events,
        "expected": expected,
        "meta": {
            "vehicles": args.vehicles,
            "events_per_vehicle": args.events_per_vehicle,
            "shuffle_window": shuffle_window,
            "seed": args.seed,
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(dataset))

    print(f"Wrote {OUTPUT}")
    print(f"  events : {len(events)} "
          f"({args.vehicles} vehicles x {args.events_per_vehicle})")
    print(f"  rules  : {len(RULES)}")
    print(f"  expected alerts: {len(expected)}")
    by_rule = {}
    for e in expected:
        by_rule[e["rule"]] = by_rule.get(e["rule"], 0) + 1
    for name, count in by_rule.items():
        print(f"    {name}: {count}")


if __name__ == "__main__":
    main()
