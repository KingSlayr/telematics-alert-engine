"""In-memory window state for window-style rules.

Key: (rule_id, vehicle_id) — every rule/vehicle pair has its own state,
so one vehicle's window never mixes with another's.

The state stores bounded (timestamp, value) pairs instead of just a count:
- windowed    (all of last N)     reads it as "do all stored values match?"
- time_window (K matches in T s)  reads it as "how many stored values match?"
- aggregate   (AVG/MAX/...)       reads it as "aggregate of the stored values"

Every event is appended (matches and misses alike); each rule type decides
what to evict and which predicate to apply.
"""

from dataclasses import dataclass, field


@dataclass
class WindowState:

    # (event timestamp, field value) pairs, oldest first.
    values: list = field(default_factory=list)

    # Duplicate guard: True means this violation period already fired an alert.
    triggered: bool = False


# (rule_id, vehicle_id) -> WindowState
window_states: dict[tuple[int, str], WindowState] = {}


def get_state(rule_id: int, vehicle_id: str) -> WindowState:
    key = (rule_id, vehicle_id)

    if key not in window_states:
        window_states[key] = WindowState()

    return window_states[key]


def clear_rule_state(rule_id: int) -> None:
    """Forget all state for one rule.

    Called when a rule is updated, disabled, or deleted: the stored window
    belongs to the old definition, so the next event starts fresh.
    """
    for key in [k for k in window_states if k[0] == rule_id]:
        del window_states[key]
