"""Bounded reorder buffer.

HTTP requests for one vehicle can arrive out of order (network delays),
so each worker holds events for a short time (allowed lateness) and then
releases them sorted by event timestamp, per vehicle.

Release is per VEHICLE, not per event: a vehicle's whole backlog is
released together, once the OLDEST of its held events has waited the full
lateness (a per-vehicle watermark). That guarantees every event arriving
within the lateness window is evaluated in event-time order, even when
out-of-order arrivals land in different flush cycles.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from app.config import ALLOWED_LATENESS_SECONDS


class ReorderBuffer:

    def __init__(self, allowed_lateness_seconds: float = ALLOWED_LATENESS_SECONDS):
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        # vehicle_id -> [(arrival_time, event), ...]
        self.pending: dict[str, list] = defaultdict(list)

    def add(self, event) -> None:
        self.pending[event.vehicle_id].append(
            (datetime.now(timezone.utc), event)
        )

    def flush_due(self) -> list:
        """Release each vehicle whose oldest held event has waited long enough.

        Releasing per event instead would let two out-of-order events of the
        same vehicle expire in different flush cycles and be evaluated in
        arrival order — the newer one first, which would get the older one
        skipped as late. Events arriving after their vehicle's watermark has
        fired are handled by the worker's late-event check.
        """
        now = datetime.now(timezone.utc)
        ready = []
        released_vehicles = []

        for vehicle_id, items in self.pending.items():
            oldest_arrival = items[0][0]

            if now - oldest_arrival < self.allowed_lateness:
                continue

            released_vehicles.append(vehicle_id)

            # Event-time order within the vehicle.
            items.sort(key=lambda pair: pair[1].timestamp)
            for _, event in items:
                ready.append((vehicle_id, event.timestamp, event))

        for vehicle_id in released_vehicles:
            del self.pending[vehicle_id]

        # Vehicle first, then event time within the vehicle.
        ready.sort(key=lambda item: (item[0], item[1]))

        return [event for _, _, event in ready]
