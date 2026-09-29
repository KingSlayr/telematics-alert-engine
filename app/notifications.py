"""Notifications are a side effect of alert creation, never part of it.

The alert row is committed before anything is put on this queue, so a
notification failure can never lose an alert. Swap the console print
for email/SMS/webhook in production.
"""

import asyncio

from app.models import Alert

# Alerts are pushed here after being saved to the database.
notification_queue: asyncio.Queue = asyncio.Queue()


async def run_notifier():
    """Console notification consumer."""
    while True:
        alert: Alert = await notification_queue.get()
        details = alert.details or {}
        print(
            f"🚨 ALERT #{alert.id} | vehicle {alert.vehicle_id} | "
            f"rule {alert.rule_id} | "
            f"{details.get('field')} {details.get('operator')} "
            f"{details.get('threshold')} | observed {details.get('actual_value')}"
        )
