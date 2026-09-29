import os

# Database. Swap to PostgreSQL by changing this value, e.g.:
#   postgresql+asyncpg://user:password@localhost/telematics
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./telematics.db")

# Fixed number of processing partitions. Queues = workers, always.
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "4"))

# Max events waiting in one partition queue before the API returns 503.
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "1000"))

# How long the reorder buffer waits for earlier events before releasing.
ALLOWED_LATENESS_SECONDS = float(os.getenv("ALLOWED_LATENESS_SECONDS", "2.0"))
