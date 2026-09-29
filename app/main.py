import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import alerts, rules, telemetry
from app.config import NUM_PARTITIONS, QUEUE_MAXSIZE
from app.db import init_db
from app.notifications import run_notifier
from app.processing import dispatcher
from app.processing.partition_worker import start_workers


@asynccontextmanager
async def lifespan(app: FastAPI):

    await init_db()

    queues = dispatcher.setup_queues(NUM_PARTITIONS, QUEUE_MAXSIZE)
    tasks = start_workers(NUM_PARTITIONS, queues)
    tasks.append(asyncio.create_task(run_notifier()))

    print("Telematics Alert Engine started")

    yield

    for task in tasks:
        task.cancel()


app = FastAPI(title="Telematics Alert Engine", lifespan=lifespan)

app.include_router(telemetry.router, prefix="/api/v1")
app.include_router(rules.router, prefix="/api/v1")
app.include_router(alerts.router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok"}
