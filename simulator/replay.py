"""Replay a generated dataset against a running server, then verify.

Phases (all logged live to the terminal):
  1. Cleanup + rules   — deletes rules from previous runs, creates this
                         run's rules, remembers their server-assigned ids.
  2. Send              — sends events with several concurrent senders in
                         batches of DIFFERENT sizes; logs every batch and
                         counts accepted vs duplicate responses.
  3. Processing        — waits out the reorder window, then polls the alert
                         stream, printing the count as it grows, until it is
                         stable.
  4. Verification      — compares actual alerts with the expectation
                         precomputed by generate_events.py:
                           * every expected (rule, vehicle) fires exactly one
                             TRIGGERED alert at the exact expected timestamp;
                           * no unexpected alerts for this run's rules.
                         The timestamp check is the ordering check: windowed
                         rules fire at a precise event, so any mis-ordered
                         evaluation shifts the timestamp.

Two send orders:

  --order stream (default)
    Models production: every vehicle is an independent ~1 event/second
    stream that arrives in event-time order. Vehicles are spread across
    concurrent senders, so HTTP handling, queues and workers are all under
    real concurrency, but no same-vehicle disorder is injected — the only
    reordering is what our own pipeline does. Runs on the default 2s
    ALLOWED_LATENESS_SECONDS.

  --order shuffled
    Stress test: batches of mixed size are sent concurrently and chunks of
    the event list are shuffled, so the same vehicle's events arrive
    seconds apart in the wrong order. Deliberately exceeds the default
    lateness — start the server with ALLOWED_LATENESS_SECONDS=30 and pass
    --lateness 30, otherwise events are correctly dropped as late and the
    verification fails.

Re-running the same dataset sends duplicate event_ids — the server must
report them as duplicates and no alert may change (idempotency at scale).

Usage:
  python simulator/replay.py                                  # stream mode
  python simulator/replay.py --batch-sizes 250,50,400,10,150  # custom sizes
  python simulator/replay.py --order shuffled --lateness 30   # stress mode
"""

import argparse
import asyncio
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import httpx

DATASET = Path(__file__).resolve().parents[1] / "dataset" / "dataset.json"


def parse_ts(value: str) -> datetime:
    """Normalize timestamps for comparison (server returns naive UTC)."""
    value = value.replace("Z", "")
    if "+" in value:
        value = value.split("+")[0]
    return datetime.fromisoformat(value)


def chunk_events(events, sizes):
    batches, i, si = [], 0, 0
    while i < len(events):
        size = sizes[si % len(sizes)]
        si += 1
        batches.append(events[i:i + size])
        i += size
    return batches


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument("--file", default=str(DATASET))
    parser.add_argument("--batch-sizes", default="250,50,400,10,150",
                        help="comma-separated, cycled over the event list")
    parser.add_argument("--concurrency", type=int, default=4,
                        help="number of concurrent batch senders")
    parser.add_argument("--order", choices=["stream", "shuffled"], default="stream",
                        help="stream: per-vehicle in-order (production model); "
                             "shuffled: scramble arrival to stress the reorder "
                             "buffer (needs a bigger server lateness)")
    parser.add_argument("--wait-timeout", type=int, default=300,
                        help="max seconds to wait for processing to settle")
    parser.add_argument("--lateness", type=float, default=2,
                        help="ALLOWED_LATENESS_SECONDS the server was started "
                             "with (used to time the polling)")
    parser.add_argument("--resend-first-batch", action="store_true",
                        help="idempotency check: resend batch 0 before verifying")
    args = parser.parse_args()

    dataset = json.loads(Path(args.file).read_text())
    events = dataset["events"]
    sizes = [int(s) for s in args.batch_sizes.split(",")]

    meta = dataset.get("meta", {})
    print("╔══════════════════════════════════════════════════════════════╗")
    print(f"║ dataset : {len(events)} events "
          f"({meta.get('vehicles', '?')} vehicles x "
          f"{meta.get('events_per_vehicle', '?')})")
    print(f"║ plan    : {args.concurrency} concurrent senders, "
          f"batch sizes cycled from {sizes}, order={args.order}")
    print(f"║ server  : {args.url} (lateness {args.lateness}s)")
    print("╚══════════════════════════════════════════════════════════════╝")

    if args.order == "shuffled" and args.lateness <= 5:
        print("\n⚠️  shuffled mode usually exceeds the default 2s lateness — "
              "start the server with ALLOWED_LATENESS_SECONDS=30 and pass "
              "--lateness 30, or some events will be (correctly) dropped "
              "as late and verification will fail.")

    sent = accepted = duplicates = 0
    errors = []
    started = time.perf_counter()

    async with httpx.AsyncClient(base_url=args.url, timeout=30) as client:

        # ── Phase 1: cleanup + rules ─────────────────────────────────────
        existing = (await client.get("/rules")).json()
        if existing:
            print(f"\n── Cleanup: deleting {len(existing)} rule(s) from "
                  f"previous runs ──")
            for rule in existing:
                await client.delete(f"/rules/{rule['id']}")
                print(f"  deleted rule {rule['id']} '{rule['name']}'")

        print(f"\n── Creating this run's {len(dataset['rules'])} rules ──")
        rule_ids = {}
        for rule in dataset["rules"]:
            response = await client.post("/rules", json=rule)
            rule_id = response.json()["id"]
            rule_ids[rule["name"]] = rule_id
            print(f"  rule {rule_id}: '{rule['name']}' "
                  f"[{rule['rule_type']}] {rule['definition']}")
        rule_id_set = set(rule_ids.values())
        name_by_id = {rid: name for name, rid in rule_ids.items()}

        # ── Phase 2: send ────────────────────────────────────────────────
        print(f"\n── Sending {len(events)} events ({args.order} order) ──")

        async def post_event(event) -> None:
            nonlocal sent, accepted, duplicates
            response = await client.post("/telemetry", json=event)
            sent += 1
            if response.status_code != 202:
                errors.append((event["event_id"], response.status_code))
            elif response.json().get("status") == "duplicate":
                duplicates += 1
            else:
                accepted += 1

        if args.order == "stream":
            # Regroup into per-vehicle event-time-ordered streams and hand
            # whole vehicles to senders round-robin: every vehicle stays
            # in-order (like a real 1 event/s tracker) while senders run
            # concurrently — the production shape of the load.
            streams = {}
            for event in events:
                streams.setdefault(event["vehicle_id"], []).append(event)
            for vehicle_events in streams.values():
                vehicle_events.sort(key=lambda e: e["timestamp"])

            assigned = [[] for _ in range(args.concurrency)]
            for index, vehicle_id in enumerate(sorted(streams)):
                assigned[index % args.concurrency].append(streams[vehicle_id])
            totals = [sum(len(s) for s in mine) for mine in assigned]

            async def sender(worker_id: int):
                my_streams = assigned[worker_id]
                pointers = [0] * len(my_streams)
                mine_sent, size_index = 0, 0
                while mine_sent < totals[worker_id]:
                    size = sizes[size_index % len(sizes)]
                    size_index += 1
                    batch, batch_start = [], time.perf_counter()
                    while (len(batch) < size
                           and mine_sent + len(batch) < totals[worker_id]):
                        for j, vehicle_events in enumerate(my_streams):
                            if len(batch) >= size:
                                break
                            if pointers[j] < len(vehicle_events):
                                batch.append(vehicle_events[pointers[j]])
                                pointers[j] += 1
                    for event in batch:
                        await post_event(event)
                    mine_sent += len(batch)
                    took = time.perf_counter() - batch_start
                    print(f"  [sender {worker_id}] batch {size_index:>3} "
                          f"({len(batch):>4} events) in {took:5.2f}s — "
                          f"{sent}/{len(events)} sent")

        else:  # shuffled
            batches = chunk_events(events, sizes)
            batch_queue = asyncio.Queue()
            for index, batch in enumerate(batches):
                batch_queue.put_nowait((index, batch))

            async def sender(worker_id: int):
                while True:
                    index, batch = await batch_queue.get()
                    if batch is None:
                        return
                    batch_start = time.perf_counter()
                    for event in batch:
                        await post_event(event)
                    took = time.perf_counter() - batch_start
                    print(f"  [sender {worker_id}] batch {index + 1:>3}/"
                          f"{len(batches)} ({len(batch):>4} events) in "
                          f"{took:5.2f}s — {sent}/{len(events)} sent")
                    batch_queue.task_done()

        senders = [asyncio.create_task(sender(i)) for i in range(args.concurrency)]
        if args.order == "shuffled":
            await batch_queue.join()
            for _ in senders:
                batch_queue.put_nowait(None)
        await asyncio.gather(*senders)

        send_elapsed = time.perf_counter() - started
        print(f"\n  send complete: {accepted} accepted, {duplicates} duplicates, "
              f"{len(errors)} errors — {send_elapsed:.1f}s "
              f"({sent / max(send_elapsed, 0.001):.0f} events/s)")
        if errors:
            print(f"  transport errors: {errors[:5]}")

        if args.resend_first_batch:
            re_dup = 0
            for event in events[:sizes[0]]:
                response = await client.post("/telemetry", json=event)
                if response.json().get("status") == "duplicate":
                    re_dup += 1
            print(f"\n  idempotency check: resent {sizes[0]} events — "
                  f"{re_dup} reported as duplicates")

        # ── Phase 3: processing (wait out watermarks, poll until quiet) ──
        async def our_alerts():
            alerts = (await client.get("/alerts")).json()
            return [a for a in alerts if a["rule_id"] in rule_id_set]

        expected_total = len(dataset["expected"])
        min_wait = started + send_elapsed + args.lateness + 10
        print(f"\n── Processing: polling starts after the reorder window "
              f"({args.lateness}s), then waits for a quiet alert stream ──")

        deadline = time.perf_counter() + args.wait_timeout
        last_count, stable_since, polls, last_marker = -1, None, 0, None
        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < min_wait:
                marker = int((min_wait - now) // 5)
                if marker != last_marker:
                    last_marker = marker
                    print(f"  reorder window: {min_wait - now:4.0f}s left")
                await asyncio.sleep(0.5)
                continue

            count = len(await our_alerts())
            polls += 1
            if count != last_count:
                print(f"  poll {polls:>3}: {count}/{expected_total} alerts so far")
                last_count, stable_since = count, now
            else:
                stable_for = time.perf_counter() - stable_since
                if (count >= expected_total and stable_for >= 3.0) \
                        or stable_for >= 12.0:
                    print(f"  quiet for {stable_for:.0f}s after {polls} polls "
                          f"— processing done")
                    break
            await asyncio.sleep(0.5)

        total_elapsed = time.perf_counter() - started

        # ── Phase 4: verification ────────────────────────────────────────
        print(f"\n── Verification ──")
        alerts = await our_alerts()
        actual = {}
        for alert in alerts:
            actual.setdefault((alert["rule_id"], alert["vehicle_id"]), []).append(alert)

        expected_by_rule = Counter(e["rule"] for e in dataset["expected"])
        actual_by_rule = Counter(name_by_id[a["rule_id"]] for a in alerts)

        print(f"  expected (rule, vehicle) alerts: {len(dataset['expected'])}")
        print(f"  actual alerts for this run's rules: {len(alerts)}")

        print("\n  per rule:")
        for name in rule_ids:
            expected_n = expected_by_rule.get(name, 0)
            actual_n = actual_by_rule.get(name, 0)
            mark = "✓" if expected_n == actual_n else "✗"
            print(f"    {mark} {name:<26} expected {expected_n:>3}   "
                  f"actual {actual_n:>3}")

        print(f"\n  actual alerts ({len(alerts)}):")
        ordered = sorted(alerts, key=lambda a: (
            a["rule_id"], a["vehicle_id"], a["triggered_at"]))
        for alert in ordered[:40]:
            print(f"    #{alert['id']:<5} {name_by_id.get(alert['rule_id'], '?'):<26} "
                  f"{alert['vehicle_id']}  at {alert['triggered_at']}  "
                  f"[{alert['status']}]")
        if len(ordered) > 40:
            print(f"    ... and {len(ordered) - 40} more")

        problems = []

        for expected in dataset["expected"]:
            rule_id = rule_ids[expected["rule"]]
            vehicle_id = expected["vehicle_id"]
            found = actual.get((rule_id, vehicle_id), [])

            if not found:
                problems.append(
                    f"MISSING   rule '{expected['rule']}' vehicle {vehicle_id}"
                )
            elif len(found) > 1:
                problems.append(
                    f"DUPLICATE rule '{expected['rule']}' vehicle {vehicle_id}: "
                    f"{len(found)} alerts"
                )
            else:
                alert = found[0]
                if alert["status"] != "TRIGGERED":
                    problems.append(
                        f"STATUS    rule '{expected['rule']}' vehicle {vehicle_id}: "
                        f"{alert['status']}"
                    )
                if parse_ts(alert["triggered_at"]) != parse_ts(expected["triggered_at"]):
                    problems.append(
                        f"TIMESTAMP rule '{expected['rule']}' vehicle {vehicle_id}: "
                        f"expected {expected['triggered_at']}, "
                        f"got {alert['triggered_at']}"
                    )

        expected_keys = {
            (rule_ids[e["rule"]], e["vehicle_id"]) for e in dataset["expected"]
        }
        for rule_id, vehicle_id in actual:
            if (rule_id, vehicle_id) not in expected_keys:
                problems.append(
                    f"UNEXPECTED rule {name_by_id.get(rule_id)} vehicle {vehicle_id}"
                )

        # ── Report ───────────────────────────────────────────────────────
        print(f"\n  timing: send {send_elapsed:.1f}s · process "
              f"{total_elapsed - send_elapsed:.1f}s · total {total_elapsed:.1f}s")

        if errors:
            print(f"\n  transport errors: {len(errors)} (first 5: {errors[:5]})")

        if problems:
            print(f"\n❌ FAIL — {len(problems)} problem(s):")
            for problem in problems[:30]:
                print(f"  {problem}")
            if len(problems) > 30:
                print(f"  ... and {len(problems) - 30} more")
            raise SystemExit(1)

        print(f"\n✅ PASS — every expected alert fired exactly once, "
              f"at the exact expected event time; no unexpected alerts.")


asyncio.run(main())
