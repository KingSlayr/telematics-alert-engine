"""Partition workers: queue -> reorder buffer -> rule evaluation -> alerts."""

import asyncio
from datetime import datetime

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Rule
from app.notifications import notification_queue
from app.processing.reorder_buffer import ReorderBuffer
from app.services.alert_service import create_alert
from app.services.rule_engine import evaluate_rule

# How often each worker checks its buffer for events ready to release,
# even when no new events arrive.
FLUSH_INTERVAL_SECONDS = 0.5


async def process_event(event, last_processed: dict) -> None:
    """Evaluate all enabled rules against one event."""

    # Late event: we already processed a newer event for this vehicle.
    # Re-evaluating would corrupt the window streak, so we skip it.
    last_timestamp = last_processed.get(event.vehicle_id)

    if last_timestamp is not None and event.timestamp <= last_timestamp:
        print(
            f"LATE EVENT skipped: vehicle {event.vehicle_id} "
            f"timestamp {event.timestamp}"
        )
        return

    async with SessionLocal() as db:

        result = await db.execute(select(Rule).where(Rule.enabled == True))  # noqa: E712
        rules = result.scalars().all()

        for rule in rules:

            # Optional rule targeting.
            targets = rule.definition.get("target_vehicles")
            if targets and event.vehicle_id not in targets:
                continue

            matched, context = evaluate_rule(rule, event)

            if not matched:
                continue

            alert = await create_alert(rule, event, context)

            if alert:
                # Alert is already saved in the database at this point,
                # so a notification failure cannot lose it.
                await notification_queue.put(alert)
            else:
                print(
                    f"Alert suppressed: an active alert already exists "
                    f"for rule {rule.id}, vehicle {event.vehicle_id}"
                )

    last_processed[event.vehicle_id] = event.timestamp


async def worker_loop(worker_index: int, queue: asyncio.Queue, buffer: ReorderBuffer):
    """Consume one partition queue sequentially."""

    last_processed: dict[str, datetime] = {}
    print(f"Worker {worker_index} started")

    while True:

        # Wait for the next event, but no longer than FLUSH_INTERVAL_SECONDS,
        # so buffered events get released even when the queue goes quiet.
        try:
            event = await asyncio.wait_for(
                queue.get(), timeout=FLUSH_INTERVAL_SECONDS
            )
            buffer.add(event)
            queue.task_done()
        except asyncio.TimeoutError:
            pass

        for ready_event in buffer.flush_due():
            try:
                await process_event(ready_event, last_processed)
            except Exception as error:
                print(f"Error processing event: {error}")


def start_workers(num_partitions: int, queues: list[asyncio.Queue]) -> list:
    tasks = []

    for index in range(num_partitions):
        buffer = ReorderBuffer()
        task = asyncio.create_task(worker_loop(index, queues[index], buffer))
        tasks.append(task)

    return tasks
