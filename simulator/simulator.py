"""Simulates several vehicles sending telemetry at the same time.

Scenarios:
  VIN-A: 65 -> 75 -> 78 -> 72   (window rule fires; events ARRIVE out of order
                                 on purpose to demonstrate the reorder buffer)
  VIN-B: 60 -> 62 -> 65         (never fires)
  VIN-C: 75 -> 78 -> 72         (fires; arrives already in order)

Run the server first, then:  python simulator/simulator.py
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = "http://127.0.0.1:8000/api/v1"
BASE_TIME = datetime.now(timezone.utc)


async def send_event(client, vehicle_id, timestamp, speed):
    payload = {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": timestamp.isoformat(),
        "speed_mph": speed,
        "fuel_level_percent": 50,
        "engine_state": "ON",
        "odometer_miles": 12000,
    }
    response = await client.post(f"{BASE_URL}/telemetry", json=payload)
    print(f"  {vehicle_id} speed={speed} -> {response.status_code} {response.json()}")


async def main():
    async with httpx.AsyncClient() as client:

        # 1. Create the windowed rule used by the demo.
        rule = {
            "name": "Persistent speeding",
            "rule_type": "windowed",
            "definition": {
                "field": "speed_mph",
                "operator": "GT",
                "value": 70,
                "consecutive_points": 3,
            },
            "enabled": True,
        }
        response = await client.post(f"{BASE_URL}/rules", json=rule)
        print("CREATE RULE:", response.status_code, response.json())

        # 2. VIN-A: event timestamps are 10:01..10:04, but they ARRIVE out of
        # order (A2 first). The reorder buffer must sort them before evaluation.
        print("\nVIN-A (out-of-order arrivals):")
        speeds = {1: 65, 2: 75, 3: 78, 4: 72}
        for minute in [2, 1, 3, 4]:
            await send_event(
                client,
                "VIN-A",
                BASE_TIME + timedelta(minutes=minute),
                speeds[minute],
            )
            await asyncio.sleep(0.2)

        # 3. VIN-B: normal driving, no alert expected.
        print("\nVIN-B (no violation):")
        for minute, speed in [(1, 60), (2, 62), (3, 65)]:
            await send_event(
                client, "VIN-B", BASE_TIME + timedelta(minutes=minute), speed
            )
            await asyncio.sleep(0.2)

        # 4. VIN-C: violation arriving in order, alert on the 3rd point.
        print("\nVIN-C (in order, fires on 3rd point):")
        for minute, speed in [(1, 75), (2, 78), (3, 72)]:
            await send_event(
                client, "VIN-C", BASE_TIME + timedelta(minutes=minute), speed
            )
            await asyncio.sleep(0.2)

        # 5. Wait for the reorder buffer (2s lateness) + workers, then check.
        print("\nWaiting for processing...")
        await asyncio.sleep(5)

        response = await client.get(f"{BASE_URL}/alerts")
        print("\nALERTS:")
        for alert in response.json():
            print(
                f"  #{alert['id']} vehicle={alert['vehicle_id']} "
                f"status={alert['status']} details={alert['details']}"
            )


asyncio.run(main())
