"""Unit tests for the clickstream simulator: run with `uv run pytest simulator/tests -q`."""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # so `import events` works

from events import ChaosConfig, SessionGenerator  # noqa: E402

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def generate(n_sessions=500, seed=1, chaos=None):
    gen = SessionGenerator(seed=seed, chaos=chaos)
    return [rec for _ in range(n_sessions) for rec in gen.session(START)]


def test_same_seed_gives_identical_data():
    assert generate(seed=42) == generate(seed=42)


def test_without_chaos_every_event_is_valid_and_unique():
    clean = ChaosConfig(duplicate_rate=0, late_rate=0, malformed_rate=0,
                        bot_session_rate=0, cookie_reset_rate=0)
    events = [json.loads(payload) for _, payload, _ in generate(chaos=clean)]
    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))
    assert all(e["anonymous_id"] for e in events)


def test_device_type_never_changes_for_a_device():
    seen = defaultdict(set)
    for _, payload, _ in generate(n_sessions=2000):
        try:
            e = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if e.get("anonymous_id"):
            seen[e["anonymous_id"]].add(e["device"])
    assert all(len(devices) == 1 for devices in seen.values())


def test_chaos_rates_are_roughly_as_configured():
    records = generate(n_sessions=3000)
    tags = [tag for _, _, tag in records]
    total = len(tags)
    assert 0.01 < tags.count("duplicate") / total < 0.03
    assert 0.002 < tags.count("malformed") / total < 0.01


def test_purchases_always_have_order_and_positive_price():
    clean = ChaosConfig(malformed_rate=0)
    for _, payload, _ in generate(n_sessions=2000, chaos=clean):
        e = json.loads(payload)
        if e["event_type"] == "purchase":
            assert e["order_id"] and e["price"] > 0