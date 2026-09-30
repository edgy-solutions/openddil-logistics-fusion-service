"""The upstream kind gate that used to keep a Remove Entity PDU away from an
asset_id AssetLogistics had never seen is becoming stateless itself (every
removal now passes by PDU type alone), so this VO must refuse to create
state for a never-seen asset_id off the back of a removal claim.

Uses a stub ObjectContext, the same mechanism `openddil-cm-service`'s
`test_asset_cm.py` uses to exercise its Virtual Object's handlers without a
live Restate runtime: the handlers only touch `ctx.get`/`ctx.set`/`ctx.run`/
`ctx.object_send`/`ctx.key`/`ctx.time`, so a minimal stand-in that records
what it was asked to do is enough to assert on.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from prometheus_client import REGISTRY

from fusion.thresholds import Thresholds
from workflows import asset_logistics
from workflows.asset_logistics import on_proprietary_update

_COUNTER_NAME = "logistics_removal_unknown_asset_dropped_total"


# ---------------------------------------------------------------------------
# Stub Restate ObjectContext (mirrors openddil-cm-service/src/tests/test_asset_cm.py::StubCtx)
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def install_thresholds_and_publisher():
    asset_logistics.set_thresholds(Thresholds())
    published: list[tuple[str, str, bytes]] = []

    def stub_publish(topic: str, key: str, value: bytes) -> None:
        published.append((topic, key, value))

    asset_logistics.set_kafka_publisher(stub_publish)
    yield published


def _now_ns(iso: str = "2026-09-30T12:00:00Z") -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
               * 1_000_000_000)


def _counter_value() -> float:
    return REGISTRY.get_sample_value(_COUNTER_NAME) or 0.0


def _removal_record(asset_id: str = "dis:1:1:9999") -> dict:
    """Shape a Remove Entity PDU takes once decoded: no kinematics, no
    sustainment, no platform_variant — see telemetry.proto's
    OperationalStatus doc comment (ADR-0044 §A, OPERATIONAL_STATUS_REMOVED).
    Field name and enum value as `MessageToDict(..., preserving_proto_field_
    name=False)` (the default `_decode_telemetry_event` uses for real proto
    bytes) renders them: camelCase field, enum NAME string.
    """
    return {
        "eventId": "evt-remove-1",
        "asset": {"assetId": asset_id},
        "operationalState": {"operationalStatus": "OPERATIONAL_STATUS_REMOVED"},
        "provenance": {"sourceProtocol": "DIS-SIM-MGR"},
    }


def _telemetry_record(asset_id: str = "dis:1:1:4001") -> dict:
    """An ordinary Silver record that carries sustainment, so it is admitted
    to `_KEY_TELEMETRY` (`_carries_sustainment`)."""
    return {
        "eventId": "evt-1",
        "asset": {"assetId": asset_id, "platformVariant": "M1A2-SEPv3"},
        "sustainment": {"fluids": {"fuelRemaining": {"value": 80.0, "unit": "%"}}},
        "provenance": {"sourceProtocol": "sim-a"},
    }


# ---------------------------------------------------------------------------
# (a) removal + no prior state -> dropped
# ---------------------------------------------------------------------------
def test_removal_for_unknown_asset_is_dropped():
    ctx = StubCtx(key="dis:1:1:9999", now_ns=_now_ns())
    before = _counter_value()

    asyncio.run(on_proprietary_update(ctx, _removal_record("dis:1:1:9999")))

    assert ctx._state == {}, "no state may be created for a never-seen asset_id"
    assert ctx.runs == [], "no Kafka publish (ctx.run) for a dropped removal"
    assert ctx.scheduled == [], "no on_timer scheduled for a dropped removal"
    assert _counter_value() == before + 1


# ---------------------------------------------------------------------------
# (b) removal + existing state -> existing (pre-patch) behaviour, unchanged
# ---------------------------------------------------------------------------
def test_removal_for_known_asset_follows_existing_path():
    asset_id = "dis:1:1:4001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # First, an ordinary event establishes state for this asset_id.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))
    assert ctx._state.get("latest_telemetry_dict") is not None
    runs_after_first = len(ctx.runs)
    scheduled_after_first = len(ctx.scheduled)
    before = _counter_value()

    # Advance the clock past the emit cadence so the timer reschedule below
    # is not debounced, then send a removal for the SAME (now known) asset.
    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000

    asyncio.run(on_proprietary_update(ctx, _removal_record(asset_id)))

    # Not counted as an unknown-asset drop.
    assert _counter_value() == before
    # Reached _schedule_next_timer, same as any other proprietary update —
    # the new gate did not intercept it.
    assert len(ctx.scheduled) > scheduled_after_first
    # The earlier sustainment-bearing record is still the stored telemetry:
    # the removal itself carries no sustainment, so it never overwrites it
    # (this is `_carries_sustainment`'s existing, unchanged behaviour).
    assert ctx._state["latest_telemetry_dict"]["eventId"] == "evt-1"


# ---------------------------------------------------------------------------
# (c) non-removal, first-seen event -> unchanged
# ---------------------------------------------------------------------------
def test_non_removal_first_seen_event_is_unchanged():
    asset_id = "dis:1:1:7001"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())
    before = _counter_value()

    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))

    assert _counter_value() == before, "gate must not fire for a non-removal record"
    assert ctx._state.get("latest_telemetry_dict") is not None
    assert ctx.runs, "first-ever event still emits (is_initial)"
    assert ctx.scheduled, "first-ever event still schedules its on_timer"
