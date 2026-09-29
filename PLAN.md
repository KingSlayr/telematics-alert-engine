# Telematics Alert Engine — Implementation Plan (locked)

## 1. Goal

Interview-level telematics alert engine: ingest telemetry from many vehicles concurrently,
evaluate configurable rules asynchronously, manage alert lifecycle.
Simple enough for a 2-hour assignment, production-aware in its boundaries.

Stack: Python 3.12, FastAPI (async), SQLAlchemy + SQLite (Postgres-swappable via `DATABASE_URL`),
fixed in-memory `asyncio.Queue` partitions, pytest.

No Kafka / Redis / Celery / RabbitMQ / Kubernetes / Docker / auth / SMTP.

## 2. Locked decisions

| Decision | Rationale |
|---|---|
| `NUM_PARTITIONS = 4`, created once at startup | Queue count never grows with vehicle count |
| `md5(vehicle_id) % NUM_PARTITIONS` | Deterministic across processes (not built-in `hash()`) |
| One worker per queue, exactly one consumer | Same vehicle → sequential processing, consistent state |
| Allowed lateness = 2.0s (configurable) | Reorder buffer waits briefly for earlier events, then releases in event-time order |
| Late event policy: log + skip, never re-evaluate | No replay machinery; window state stays consistent |
| Window state: consecutive counter per `(rule_id, vehicle_id)` with `triggered` flag | Fires once per violation streak; a miss resets and re-arms |
| Rule update/delete/disable clears that rule's window state | Simple, deterministic; next event starts from count 0 |
| Existing alerts are never auto-deleted on rule change | Alerts are historical business records, independent of rule lifecycle |
| Duplicate-alert backstop: max one active (non-RESOLVED) alert per `(rule_id, vehicle_id)` | Covers simple rules; windowed `triggered` flag governs re-arming |
| Telemetry persisted with `event_id` UNIQUE | Idempotency: duplicate events are not stored or processed twice |
| Rules loaded per event from DB | Simplest correct approach at assignment volume |
| Backpressure: `maxsize=1000`, API returns 503 when full | Telemetry is already durable in DB; nothing is silently lost |
| SQLite via aiosqlite, `DATABASE_URL` env-overridable | Zero setup; Postgres is a config swap, not a code change |
| Alert first, then notification | Notification failure can never lose an alert |

## 3. Project structure

```text
app/
├── main.py                  # FastAPI app, lifespan: create_all → start workers + notifier
├── config.py                # DATABASE_URL, NUM_PARTITIONS, QUEUE_MAXSIZE, ALLOWED_LATENESS_SECONDS
├── db.py                    # async engine, session, get_db, init_db
├── schemas.py               # Pydantic request models + rule definition validation
├── notifications.py         # notification queue + console notifier consumer
├── api/
│   ├── telemetry.py         # POST /api/v1/telemetry
│   ├── rules.py             # rule CRUD
│   └── alerts.py            # alerts list / get / acknowledge / resolve
├── models/
│   ├── telemetry.py
│   ├── rule.py
│   └── alert.py
├── services/
│   ├── telemetry_service.py # idempotent persist + dispatch
│   ├── rule_engine.py       # operators + simple/windowed evaluation
│   └── alert_service.py     # create (dedupe), acknowledge, resolve
└── processing/
    ├── dispatcher.py        # vehicle_id → partition, queue registry, backpressure
    ├── partition_worker.py  # queue → reorder buffer → evaluate → alert → notify
    ├── reorder_buffer.py    # bounded event-time ordering per vehicle
    └── window_state.py      # WindowState dataclass + (rule_id, vehicle_id) registry

simulator/
└── simulator.py             # concurrent vehicle producers + scripted scenarios

tests/
├── conftest.py
├── test_rule_engine.py
├── test_reorder_buffer.py
├── test_telemetry_api.py
├── test_alerts_api.py
└── test_integration.py
```

## 4. Data model

**telemetry** — id, event_id (UNIQUE), vehicle_id, timestamp, speed_mph, fuel_level_percent,
engine_state, odometer_miles, created_at. Index `(vehicle_id, timestamp)`.

**rules** — id, name, rule_type (`simple` | `windowed`), definition (JSON), enabled, version
(bumped on update), created_at, updated_at.

Definition shapes:

```json
{"field": "speed_mph", "operator": "GT", "value": 70}
{"field": "speed_mph", "operator": "GT", "value": 70, "consecutive_points": 3}
{"field": "speed_mph", "operator": "GT", "value": 70, "consecutive_points": 3, "target_vehicles": ["VIN001"]}
```

field ∈ speed_mph | fuel_level_percent | odometer_miles
operator ∈ GT | GTE | LT | LTE | EQ | NEQ

**alerts** — id, rule_id (indexed), vehicle_id, status (TRIGGERED | ACKNOWLEDGED | RESOLVED),
triggered_at, acknowledged_at, resolved_at, details (JSON), created_at, updated_at.

Transitions: TRIGGERED→ACKNOWLEDGED, TRIGGERED→RESOLVED, ACKNOWLEDGED→RESOLVED. Else 409.

## 5. Processing pipeline

```text
POST /api/v1/telemetry
  → validate
  → persist (duplicate event_id → 202 {"status": "duplicate"}, no second row)
  → dispatcher: md5(vehicle_id) % 4 → queues[p].put_nowait()   (full → 503)
  → 202 {"status": "accepted"}

Worker (one asyncio task per queue, own reorder buffer):
  wait up to 0.5s for an event → add to buffer
  flush events held ≥ ALLOWED_LATENESS, sorted by event time per vehicle
  for each event:
      skip if event.timestamp <= last_processed[vehicle]   (late event: log + skip)
      load enabled rules
      for each rule: target check → evaluate → matched → alert_service.create → notification queue
      last_processed[vehicle] = event.timestamp
```

Window semantics: match → count += 1; count reaches `consecutive_points` and not yet
`triggered` → fire once, set `triggered`; miss → count = 0, triggered = False (re-arms).

Notification: alert row committed first, then message on a queue; one consumer prints
the 🚨 ALERT line. Swap point for email/SMS/webhook.

## 6. Crash story

Durable: telemetry, rules, alerts. Volatile: queues, reorder buffers, window states.
A crash loses counters, never data. Production note: externalize state to a shared
store/broker; the partitioning model stays the same.

## 7. Phases

1. Database models + `create_all`
2. Rule CRUD (+ window-state clearing on update/delete/disable)
3. Rule engine (operators, simple, windowed) — pure, unit tested
4. Telemetry API (idempotent persist + dispatch)
5. Partitioning: dispatcher + workers + backpressure
6. Reorder buffer: bounded lateness, late-event policy
7. Alert service (create + dedupe + transitions) wired into workers + console notifier — first end-to-end run
8. Simulator with scripted scenarios incl. deliberate out-of-order arrivals
9. Tests + README

## 8. Test checklist

duplicate event · simple match/non-match · consecutive window fires on 3rd · window reset ·
no duplicate alerts · independent vehicle state · rule update resets state · disabled rule ignored ·
A2/A1/A3 reordered · late event skipped · alert lifecycle + 409s · vehicles share a partition ·
same vehicle processed sequentially.

## 9. Explicit non-goals

Kafka, Redis, Celery, RabbitMQ, Kubernetes, Docker, authentication, SMTP, replay of late
events, historical rule versions, distributed locks, exactly-once processing. Each gets a
one-line "where it would go" note in the README.

---

## Addendum — extended rule families (implemented after lock)

Window state was generalized from a streak counter to a bounded list of
`(timestamp, value)` pairs per `(rule_id, vehicle_id)`. One state shape now
supports four rule families:

| rule_type | semantics | window |
|---|---|---|
| `simple` | current event matches | none |
| `windowed` | all of the last N events match | count-based (N) |
| `time_window` | ≥ K matching events within any T-second span | time-based, event-time eviction |
| `aggregate` | AVG/MAX/MIN/SUM over the window matches threshold | time- or count-based |

Dependencies and invariants unchanged: eviction runs in event time (guaranteed
by the reorder buffer), state is cleared on rule update/disable/delete, the
`triggered` flag gives one alert per violation period, and the active-alert
backstop in `alert_service` still applies. Zero DB migration (definitions are
JSON; `rule_type` is a string).
