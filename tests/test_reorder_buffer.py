import asyncio
from datetime import datetime, timedelta, timezone

from app.processing.reorder_buffer import ReorderBuffer


def make_event(vehicle_id, minute, speed=70):
    return SimpleNamespace(
        vehicle_id=vehicle_id,
        timestamp=datetime(2026, 1, 1, 10, minute, 0, tzinfo=timezone.utc),
        speed_mph=speed,
    )


from types import SimpleNamespace


async def wait_for_flush(buffer, expected_count, timeout=2.0):
    """Helper: flush repeatedly until the buffer releases the expected events."""
    released = []
    deadline = asyncio.get_event_loop().time() + timeout
    while len(released) < expected_count and asyncio.get_event_loop().time() < deadline:
        released.extend(buffer.flush_due())
        await asyncio.sleep(0.05)
    return released


async def test_out_of_order_events_are_released_in_event_time_order():
    buffer = ReorderBuffer(allowed_lateness_seconds=0.1)

    # Arrive as A2, A1, A3.
    buffer.add(make_event("VIN-A", 2))
    await asyncio.sleep(0.02)
    buffer.add(make_event("VIN-A", 1))
    await asyncio.sleep(0.02)
    buffer.add(make_event("VIN-A", 3))

    released = await wait_for_flush(buffer, 3)

    minutes = [event.timestamp.minute for event in released]
    assert minutes == [1, 2, 3]


async def test_vehicles_are_independent():
    buffer = ReorderBuffer(allowed_lateness_seconds=0.1)

    # Interleaved arrivals for two vehicles.
    buffer.add(make_event("VIN-A", 1))
    buffer.add(make_event("VIN-B", 1))
    buffer.add(make_event("VIN-A", 2))
    buffer.add(make_event("VIN-B", 2))

    released = await wait_for_flush(buffer, 4)

    order = [(event.vehicle_id, event.timestamp.minute) for event in released]
    assert order == [("VIN-A", 1), ("VIN-A", 2), ("VIN-B", 1), ("VIN-B", 2)]


async def test_events_not_yet_due_stay_pending():
    buffer = ReorderBuffer(allowed_lateness_seconds=60)

    buffer.add(make_event("VIN-A", 1))
    released = buffer.flush_due()

    assert released == []  # still inside the lateness window


async def test_older_event_arriving_late_joins_the_same_release():
    # The load-test bug: an older event arriving AFTER the watermark would
    # previously be released in a later cycle than the newer one, get
    # evaluated out of order, and be skipped as late. It must instead be
    # released together with the newer event, in event-time order.
    buffer = ReorderBuffer(allowed_lateness_seconds=0.2)

    buffer.add(make_event("VIN-A", 2))
    await asyncio.sleep(0.3)          # watermark for VIN-A is now due
    buffer.add(make_event("VIN-A", 1))  # older event, arrives late

    released = buffer.flush_due()

    minutes = [event.timestamp.minute for event in released]
    assert minutes == [1, 2]          # both, in event-time order
    assert buffer.pending == {}


async def test_event_arriving_after_release_starts_a_new_backlog():
    # Past the watermark, an older event can no longer be saved: it is
    # released on its own (and the worker's late-event check will skip it).
    buffer = ReorderBuffer(allowed_lateness_seconds=0.2)

    buffer.add(make_event("VIN-A", 2))
    await asyncio.sleep(0.3)
    assert [event.timestamp.minute for event in buffer.flush_due()] == [2]

    buffer.add(make_event("VIN-A", 1))
    assert buffer.flush_due() == []   # its own lateness wait starts now

    await asyncio.sleep(0.3)
    assert [event.timestamp.minute for event in buffer.flush_due()] == [1]
