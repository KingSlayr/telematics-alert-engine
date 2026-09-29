"""Fixed partition dispatcher.

Same vehicle -> same partition -> same queue -> same worker -> sequential
processing. Different vehicles can share a queue and be processed
concurrently across partitions. The number of queues never grows with the
number of vehicles.
"""

import asyncio
import hashlib

# Created once at startup. Fixed size forever.
queues: list[asyncio.Queue] = []


def setup_queues(num_partitions: int, maxsize: int) -> list[asyncio.Queue]:
    global queues
    queues = [asyncio.Queue(maxsize=maxsize) for _ in range(num_partitions)]
    return queues


def get_partition(vehicle_id: str, num_partitions: int) -> int:
    # md5 is deterministic across processes (unlike Python's hash()).
    digest = hashlib.md5(vehicle_id.encode()).hexdigest()
    return int(digest, 16) % num_partitions


def dispatch(event) -> bool:
    """Put an event on its partition queue.

    Returns False if the queue is full (backpressure: the API turns this
    into a 503). The event is already saved in the database, so nothing
    is lost permanently.
    """
    partition = get_partition(event.vehicle_id, len(queues))

    try:
        queues[partition].put_nowait(event)
        return True
    except asyncio.QueueFull:
        return False
