"""Runs the simulator.

Dry run (no --bootstrap):
    uv run python simulator/producer.py --sessions 10000 --seed 1 --summary

Stream to Kafka in real time (Ctrl+C to stop):
    uv run python simulator/producer.py --bootstrap localhost:19092 --rate 5
"""
import argparse
import heapq
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from events import SessionGenerator

TOPIC = "clickstream.events"


def dry_run(args: argparse.Namespace) -> None:
    gen = SessionGenerator(seed=args.seed)
    start = datetime.now(timezone.utc)
    records = []
    for i in range(args.sessions):
        # spread session starts over time: ~1 new session every 2 seconds
        records.extend(gen.session(start + timedelta(seconds=2 * i)))
    records.sort(key=lambda r: r[0])        # the order they'd actually be sent in

    if not args.summary:
        for _, payload, _ in records:
            print(payload)
        return
    tags = Counter(tag for _, _, tag in records)
    print(f"records sent: {len(records)}")
    for tag, n in tags.most_common():
        print(f"  {tag:<10} {n:>7}  ({n / len(records):.1%})")


def partition_key(payload: str) -> bytes | None:
    """Key = anonymous_id, so all events from one device go to the same partition
    (Kafka only guarantees order WITHIN a partition). Unparseable events get no key."""
    try:
        anon = json.loads(payload).get("anonymous_id")
        return anon.encode() if anon else None
    except json.JSONDecodeError:
        return None


def stream(args: argparse.Namespace) -> None:
    from confluent_kafka import Producer   # imported here so dry runs don't need Kafka

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "acks": "all",                  # wait until the broker has stored it
        "enable.idempotence": True,     # broker drops the producer's OWN retries (no dupes from us)
        "linger.ms": 20,                # wait up to 20ms to batch messages -> higher throughput
        "compression.type": "lz4",
    })
    failed = 0

    def on_delivery(err, _msg) -> None:
        nonlocal failed
        if err is not None:
            failed += 1

    gen = SessionGenerator(seed=args.seed)
    queue: list[tuple[float, int, str, str]] = []   # min-heap ordered by send time
    seq = 0                                          # tie-breaker for equal send times
    stats: Counter = Counter()
    started = last_report = next_session = time.time()
    print(f"Streaming to {args.bootstrap} topic={TOPIC} at {args.rate} sessions/sec. Ctrl+C to stop.")

    try:
        while args.duration is None or time.time() - started < args.duration:
            now = time.time()
            # 1. start new sessions at the requested rate
            while next_session <= now:
                for send_at, payload, tag in gen.session(datetime.now(timezone.utc)):
                    heapq.heappush(queue, (send_at.timestamp(), seq, payload, tag))
                    seq += 1
                next_session += 1 / args.rate
            # 2. send every event whose time has come (late events wait in the heap)
            while queue and queue[0][0] <= now:
                _, _, payload, tag = heapq.heappop(queue)
                producer.produce(TOPIC, value=payload.encode(), key=partition_key(payload),
                                 on_delivery=on_delivery)
                stats[tag] += 1
            producer.poll(0)                # serve delivery callbacks
            # 3. progress report every 10s
            if now - last_report >= 10:
                print(f"sent={sum(stats.values()):>8}  waiting={len(queue):>6}  failed={failed}  "
                      + "  ".join(f"{k}={v}" for k, v in stats.most_common()))
                last_report = now
            time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        print("Flushing...")
        producer.flush(10)
        print(f"Done. sent={sum(stats.values())} failed={failed} still-waiting(late)={len(queue)}")


def main() -> None:
    p = argparse.ArgumentParser(description="Clickstream simulator")
    p.add_argument("--sessions", type=int, default=3, help="dry run: how many sessions")
    p.add_argument("--seed", type=int, default=None, help="fix randomness for repeatable output")
    p.add_argument("--summary", action="store_true", help="dry run: print stats instead of events")
    p.add_argument("--bootstrap", help="Kafka address, e.g. localhost:19092. If set, streams to Kafka")
    p.add_argument("--rate", type=float, default=5, help="stream: new sessions per second")
    p.add_argument("--duration", type=float, default=None, help="stream: stop after N seconds")
    args = p.parse_args()

    if args.bootstrap:
        stream(args)
    else:
        dry_run(args)


if __name__ == "__main__":
    main()