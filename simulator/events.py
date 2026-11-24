"""Generates realistic e-commerce clickstream sessions, including real-world mess.

A session = one visit by one user. It walks a shopping funnel:
home -> (search) -> product views -> add to cart -> checkout -> purchase
with drop-off at each stage.

On top of clean sessions, ChaosConfig injects the problems real pipelines must handle:
  * duplicates     - the app retries a send, the same event arrives twice
  * late events    - a phone was offline; events arrive minutes/hours after they happened
  * malformed      - truncated JSON, missing ids, typo'd event types, bad timestamps
  * bots           - crawlers firing hundreds of page views very fast
  * cookie resets  - user cleared cookies, so they get a brand-new anonymous_id
  * schema drift   - newer app versions (schema_version 2) send an extra field
"""
from __future__ import annotations

import json
import random
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

DEVICE_TYPES = ["mobile", "mobile", "mobile", "desktop", "tablet"]  # mobile-heavy, like Indian e-com
USER_AGENTS = {             # simplified client labels (real trackers send full browser strings)
    "mobile": "android-app",
    "desktop": "windows-chrome",
    "tablet": "ipad-safari",
    "bot": "crawler",
}
COUNTRIES = ["IN", "IN", "IN", "IN", "US", "GB", "AE", "SG"]
SEARCHES = ["running shoes", "wireless earbuds", "kurta", "smartwatch", "backpack",
            "air fryer", "phone case", "jeans", "sunscreen", "office chair"]
CAMPAIGNS = [(None, None), (None, None), ("google", "brand_search"),
             ("meta", "diwali_sale"), ("instagram", "influencer_q4"), ("email", "weekly_digest")]


@dataclass
class ChaosConfig:
    duplicate_rate: float = 0.02      # 2% of events are sent twice
    late_rate: float = 0.03           # 3% of events arrive late...
    late_max_minutes: int = 90        # ...by up to 90 minutes
    malformed_rate: float = 0.005     # 0.5% of events are invalid
    bot_session_rate: float = 0.03    # 3% of sessions are bots
    cookie_reset_rate: float = 0.05   # 5% of sessions start with a fresh anonymous_id


def iso(ts: datetime) -> str:
    """2026-11-23T12:30:05.123Z — the timestamp format most trackers send."""
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stable_pick(key: str, options: list):
    """Same key -> same option, every run. Used for device-level attributes."""
    return options[zlib.crc32(key.encode()) % len(options)]


class SessionGenerator:
    def __init__(self, seed: int | None = None, n_users: int = 20_000,
                 n_products: int = 500, chaos: ChaosConfig | None = None):
        self.rng = random.Random(seed)          # seed => same output every run (testable)
        self.n_users = n_users
        self.chaos = chaos or ChaosConfig()
        self.products = [f"P{i:05d}" for i in range(n_products)]
        # long-tailed prices, rounded like real price tags (x99)
        self.prices = {p: max(49.0, round(self.rng.lognormvariate(6.5, 0.9), -1) - 1)
                       for p in self.products}
        # Zipf-like popularity: product i gets weight 1/(i+1)^0.9 -> bestsellers + long tail
        weights = [1 / (i + 1) ** 0.9 for i in range(n_products)]
        self.cum_weights = [sum(weights[: i + 1]) for i in range(n_products)]

    # ------------------------------------------------------------------ public
    def session(self, start: datetime) -> list[tuple[datetime, str, str]]:
        """One session as (send_at, payload, tag) tuples, sorted by send time.

        send_at = when the producer should send it. For late events this is long after
        the event's own event_ts — exactly how late data happens in real life.
        tag = what kind of record it is (ok/duplicate/late/malformed/bot), for stats only.
        """
        is_bot = self.rng.random() < self.chaos.bot_session_rate
        events = self._bot_session(start) if is_bot else self._human_session(start)

        out: list[tuple[datetime, str, str]] = []
        for ev in events:
            ts = datetime.fromisoformat(ev["event_ts"].replace("Z", "+00:00"))
            send_at, tag = ts, "bot" if is_bot else "ok"
            if self.rng.random() < self.chaos.late_rate:
                send_at = ts + timedelta(minutes=self.rng.uniform(3, self.chaos.late_max_minutes))
                tag = "late"
            payload, invalid = self._apply_data_issues(ev)
            if invalid:
                tag = "malformed"
            out.append((send_at, payload, tag))
            if self.rng.random() < self.chaos.duplicate_rate:
                retry_at = send_at + timedelta(milliseconds=self.rng.randint(50, 3000))
                out.append((retry_at, payload, "duplicate"))
        out.sort(key=lambda x: x[0])
        return out

    # ------------------------------------------------------------------ identity
    def _visitor(self) -> dict:
        r = self.rng
        user_n = r.randint(1, self.n_users)
        device_slot = r.randint(0, 2)                       # each person has up to 3 devices
        device_key = f"{user_n:06d}-{device_slot}"
        anonymous_id = f"anon-{device_key}"
        if r.random() < self.chaos.cookie_reset_rate:       # cleared cookies -> new id
            anonymous_id = f"anon-{device_key}-r{r.getrandbits(24):06x}"
        # device-level attributes are derived from the device, so they never flip
        device = stable_pick(device_key, DEVICE_TYPES)
        schema_version = stable_pick(device_key + "v", [1, 2])   # half the devices updated the app
        utm_source, utm_campaign = r.choice(CAMPAIGNS)
        return {
            "schema_version": schema_version,
            "user_id": f"U{user_n:06d}" if r.random() < 0.55 else None,  # 45% logged out
            "anonymous_id": anonymous_id,
            "device": device,
            "user_agent": USER_AGENTS[device],
            "country": r.choice(COUNTRIES),
            "utm_source": utm_source,
            "utm_campaign": utm_campaign,
            **({"app_version": stable_pick(device_key + "a", ["5.12.0", "5.13.1", "5.14.0"])}
               if schema_version == 2 else {}),
        }

    def _event(self, ts: datetime, event_type: str, ctx: dict, **fields) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_type": event_type,
            "event_ts": iso(ts),
            **ctx,
            **fields,
        }

    def _pick_product(self) -> tuple[str, float]:
        pid = self.rng.choices(self.products, cum_weights=self.cum_weights)[0]
        return pid, self.prices[pid]

    # ------------------------------------------------------------------ sessions
    def _human_session(self, start: datetime) -> list[dict]:
        r, ctx, ts, events = self.rng, self._visitor(), start, []

        def emit(event_type: str, **fields) -> None:
            nonlocal ts
            ts += timedelta(seconds=r.expovariate(1 / 25))   # ~25s between clicks
            events.append(self._event(ts, event_type, ctx, **fields))

        # 1. land on home page; 35% bounce immediately
        emit("page_view", page_type="home", page_url="/")
        if r.random() < 0.35:
            return events

        # 2. browse: searches, product views, occasional add-to-cart
        cart: list[tuple[str, float, int]] = []
        for _ in range(r.randint(1, 4)):
            if r.random() < 0.5:
                q = r.choice(SEARCHES)
                emit("search", page_type="search", page_url=f"/search?q={q.replace(' ', '+')}",
                     search_query=q)
            for _ in range(r.randint(1, 5)):
                pid, price = self._pick_product()
                emit("product_view", page_type="product", page_url=f"/p/{pid}",
                     product_id=pid, price=price)
                if r.random() < 0.10:
                    qty = 1 if r.random() < 0.85 else r.randint(2, 3)
                    emit("add_to_cart", page_type="product", page_url=f"/p/{pid}",
                         product_id=pid, price=price, quantity=qty)
                    cart.append((pid, price, qty))

        # 3. sometimes remove an item
        if cart and r.random() < 0.15:
            pid, price, qty = cart.pop(r.randrange(len(cart)))
            emit("remove_from_cart", page_type="cart", page_url="/cart",
                 product_id=pid, price=price, quantity=qty)

        # 4. checkout -> purchase, with drop-off at each stage
        if not cart or r.random() > 0.55:
            return events
        emit("checkout_start", page_type="checkout", page_url="/checkout")
        if r.random() > 0.6:
            return events
        order_id = f"O{r.getrandbits(40):010x}"
        for pid, price, qty in cart:                       # one purchase event per item
            emit("purchase", page_type="confirmation", page_url="/order/confirmed",
                 order_id=order_id, product_id=pid, price=price, quantity=qty)
        return events

    def _bot_session(self, start: datetime) -> list[dict]:
        """Crawler: logged out, 40-150 product page views, < 1.5s apart."""
        ctx = self._visitor()
        ctx.update(user_id=None, device="bot", user_agent=USER_AGENTS["bot"],
                   anonymous_id=f"bot-{self.rng.getrandbits(32):08x}",  # crawlers don't keep cookies
                   utm_source=None, utm_campaign=None)
        ctx.pop("app_version", None)
        ctx["schema_version"] = 1
        ts, events = start, []
        for _ in range(self.rng.randint(40, 150)):
            ts += timedelta(milliseconds=self.rng.randint(200, 1500))
            pid, _ = self._pick_product()
            events.append(self._event(ts, "page_view", ctx, page_type="product",
                                      page_url=f"/p/{pid}"))
        return events

    # ------------------------------------------------------------------ chaos
    def _apply_data_issues(self, ev: dict) -> tuple[str, bool]:
        """Returns (json_payload, is_invalid)."""
        if self.rng.random() >= self.chaos.malformed_rate:
            return json.dumps(ev), False
        bad = dict(ev)
        kind = self.rng.choice(["cut_short", "no_identity", "bad_type", "bad_ts"])
        if kind == "cut_short":                      # connection dropped mid-send
            return json.dumps(bad)[: self.rng.randint(10, 60)], True
        if kind == "no_identity":                    # SDK bug: no ids at all
            bad["user_id"] = bad["anonymous_id"] = None
        elif kind == "bad_type":                     # old SDK sends a typo'd event name
            bad["event_type"] = "pageview"
        else:                                        # invalid client clock/format
            bad["event_ts"] = "yesterday-ish"
        return json.dumps(bad), True
