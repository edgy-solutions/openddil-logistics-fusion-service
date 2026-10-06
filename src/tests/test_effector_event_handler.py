"""Handler-level tests for `AssetLogistics.on_effector_event`.

Uses the same stub ObjectContext as `test_removal_unknown_asset.py` (that
file's docstring explains why a minimal stand-in is enough: the handlers
only touch `ctx.get`/`ctx.set`/`ctx.run`/`ctx.object_send`/`ctx.key`/
`ctx.time`). Duplicated locally rather than imported, matching that file's
own convention.

Two of the effector-supply unit tests live here because
they exercise the admission/dedup gate inside the handler itself, not the
pure `fusion/rules.py` evaluator or the pure `fusion/effector_supply.py`
helpers (covered in tests/test_rules.py and this file's pure-function
cases respectively):
  - a replayed event_urn leaves `expended` unchanged
  - an unknown launcher (no prior AssetLogistics state) is refused
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from prometheus_client import REGISTRY

from fusion import effector_supply
from fusion.thresholds import Thresholds
from metrics import (
    fusion_effector_detonation_seen_total,
    fusion_effector_refused_total,
    fusion_effector_replayed_total,
)
from workflows import asset_logistics
from workflows.asset_logistics import on_effector_event, on_proprietary_update


# ---------------------------------------------------------------------------
# Stub Restate ObjectContext (mirrors tests/test_removal_unknown_asset.py::StubCtx,
# itself mirroring openddil-cm-service/src/tests/test_asset_cm.py::StubCtx)
# ---------------------------------------------------------------------------
class StubCtx:
    def __init__(self, key: str, now_ns: int):
        self._key = key
        self._now_ns = now_ns
        self._state: dict[str, object] = {}
        self.runs: list[tuple[str, object]] = []
        self.scheduled: list[dict] = []

    def key(self) -> str:
        return self._key

    def time(self):
        return datetime.fromtimestamp(self._now_ns / 1_000_000_000, tz=timezone.utc)

    async def get(self, name: str, type_hint=None):
        return self._state.get(name)

    def set(self, name: str, value) -> None:
        self._state[name] = value

    def clear(self, name: str) -> None:
        self._state.pop(name, None)

    async def run(self, label: str, fn):
        result = fn()
        self.runs.append((label, result))
        return result

    def object_send(self, handler, *, key, arg, send_delay=None):
        self.scheduled.append({
            "handler": getattr(handler, "__name__", str(handler)),
            "key": key,
            "arg": arg,
            "send_delay_s": send_delay.total_seconds() if send_delay else 0,
        })


@pytest.fixture(autouse=True)
def install_thresholds_and_publisher():
    asset_logistics.set_thresholds(Thresholds())
    asset_logistics.set_declared_load({"asset": {}, "variant": {}})
    published: list[tuple[str, str, bytes]] = []

    def stub_publish(topic: str, key: str, value: bytes) -> None:
        published.append((topic, key, value))

    asset_logistics.set_kafka_publisher(stub_publish)
    yield published


def _now_ns(iso: str = "2026-10-06T12:00:00Z") -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
               * 1_000_000_000)


def _counter(counter, **labels) -> float:
    c = counter.labels(**labels) if labels else counter
    return c._value.get()


def _telemetry_record(asset_id: str) -> dict:
    return {
        "eventId": "evt-1",
        "asset": {"assetId": asset_id, "platformVariant": "AH-64E-V6"},
        "sustainment": {"fluids": {"fuelRemaining": {"value": 80.0, "unit": "%"}}},
        "provenance": {"sourceProtocol": "sim-a"},
    }


def _fire_record(*, event_urn: str, quantity: int = 1) -> dict:
    return {
        "pdu_type": "fire",
        "event_urn": event_urn,
        "launcher_urn": "dis:1:1:5001",
        "target_urn": "dis:1:1:6001",
        "munition_type": {"kind": 2, "domain": 9, "country": 225,
                            "category": 2, "subcategory": 1, "specific": 1,
                            "extra": 0},
        "quantity": quantity,
        "detonation_result": None,
        "ingest_timestamp": "2026-10-06T12:00:00Z",
        "provenance": {"edge_id": "edge-01", "region_id": "region-east"},
    }


_MUNITION_KEY = "2.9.225.2.1.1.0"


# ---------------------------------------------------------------------------
# (1 of 6 mandated tests) unknown launcher refused
# ---------------------------------------------------------------------------
def test_unknown_launcher_fire_is_refused():
    asset_id = "dis:1:1:5001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())
    before = _counter(fusion_effector_refused_total, reason="unknown_launcher")

    asyncio.run(on_effector_event(ctx, _fire_record(event_urn="dis-event:1:1:1")))

    assert ctx._state == {}, "no state may be created for a never-admitted launcher"
    assert ctx.runs == [], "no recompute/publish for a refused Fire"
    assert ctx.scheduled == [], "no timer scheduled for a refused Fire"
    assert _counter(fusion_effector_refused_total, reason="unknown_launcher") == before + 1


# ---------------------------------------------------------------------------
# (2 of 6 mandated tests) replay leaves expended unchanged
# ---------------------------------------------------------------------------
def test_replayed_event_urn_leaves_expended_unchanged():
    asset_id = "dis:1:1:5001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # Admit the launcher first (an Entity State PDU, same path as DIS-only
    # launchers B/C): establishes _KEY_TELEMETRY so _has_known_state is True.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))

    fire = _fire_record(event_urn="dis-event:1:1:1", quantity=1)
    asyncio.run(on_effector_event(ctx, fire))
    expended_after_first = dict(ctx._state["effector_expended_dict"])
    assert expended_after_first == {_MUNITION_KEY: 1}

    before_replay_count = _counter(fusion_effector_replayed_total)

    # Same event_urn arrives again (Restate retry / Kafka redelivery).
    asyncio.run(on_effector_event(ctx, fire))

    assert ctx._state["effector_expended_dict"] == expended_after_first, \
        "a replayed event_urn must not add quantity a second time"
    assert _counter(fusion_effector_replayed_total) == before_replay_count + 1


# ---------------------------------------------------------------------------
# Supporting coverage (not mandated, but cheap and directly adjacent):
# a counted Fire DOES change state, and detonation never refuses / never
# touches expended.
# ---------------------------------------------------------------------------
def test_counted_fire_adds_quantity_and_reaches_recompute_path():
    asset_id = "dis:1:1:5001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))
    scheduled_before = len(ctx.scheduled)

    # Advance the clock past the emit cadence first, same as
    # test_removal_unknown_asset.py does, so the reschedule below is not
    # debounced against the timer `on_proprietary_update` already set.
    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000

    asyncio.run(on_effector_event(ctx, _fire_record(event_urn="dis-event:1:1:1", quantity=2)))

    assert ctx._state["effector_expended_dict"] == {_MUNITION_KEY: 2}
    # No declared load is configured for this asset in this test, so the
    # effector factor stays absent and severity stays OK -- nothing new to
    # publish. `_recompute_and_maybe_emit` still runs to that conclusion
    # (rather than short-circuiting), which `_schedule_next_timer` always
    # following it demonstrates.
    assert len(ctx.scheduled) > scheduled_before, \
        "a counted Fire reaches _recompute_and_maybe_emit + _schedule_next_timer"


def test_detonation_never_refused_no_supply_effect():
    asset_id = "dis:1:1:5001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())  # never admitted
    before = _counter(fusion_effector_detonation_seen_total)

    detonation = _fire_record(event_urn="dis-event:1:1:1")
    detonation["pdu_type"] = "detonation"
    asyncio.run(on_effector_event(ctx, detonation))

    assert ctx._state == {}, "detonation never writes state"
    assert _counter(fusion_effector_detonation_seen_total) == before + 1
