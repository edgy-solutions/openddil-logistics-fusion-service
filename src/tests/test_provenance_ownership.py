"""Tests for the one-writer-per-asset-at-every-fusion rule, derived from
record provenance: `_held_for_other_stack`, the guard shared by
`_recompute_and_maybe_emit` (suppress the publish) and `_schedule_next_timer`
(end the timer chain), and its OTHER_STACK_IDS config (`Thresholds.
other_stack_ids`).

Replaces `test_tier_ownership.py` (the registry-owner rework this module
superseded): there is no registry involvement left in this rule --
origin(asset) is `_KEY_ORIGIN`'s edge_id, kept current by `_refresh_origin`
on every inbound event, nested `provenance.edge_id` or top-level `edge_id`
on cm-state.

Uses the same stub ObjectContext as `test_registry_event_handler.py` /
`test_effector_event_handler.py` (see either file's docstring for why a
minimal stand-in is enough). Duplicated locally rather than imported,
matching those files' own convention.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from fusion.thresholds import Thresholds
from metrics import fusion_publish_suppressed_other_stack_total
from workflows import asset_logistics
from workflows.asset_logistics import (
    _KEY_LAST_SEVERITY,
    _KEY_ORIGIN,
    _KEY_REGISTRY_PLATFORM_VARIANT,
    on_proprietary_update,
    on_registry_event,
)


# ---------------------------------------------------------------------------
# Stub Restate ObjectContext (mirrors test_registry_event_handler.py::StubCtx)
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


@pytest.fixture
def published():
    out: list[tuple[str, str, bytes]] = []

    def stub_publish(topic: str, key: str, value: bytes) -> None:
        out.append((topic, key, value))

    asset_logistics.set_kafka_publisher(stub_publish)
    asset_logistics.set_declared_load({"asset": {}, "variant": {}})
    yield out


def _now_ns(iso: str = "2026-10-06T12:00:00Z") -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
               * 1_000_000_000)


def _counter(counter, **labels) -> float:
    c = counter.labels(**labels) if labels else counter
    return c._value.get()


def _telemetry_record(asset_id: str, *, edge_id: str = "") -> dict:
    """A raw-sensor-stream record. `edge_id` lands in nested provenance --
    the same shape `_extract_origin` reads for every proto-derived event.
    Omitted (empty string) models a record that carries no origin at all,
    e.g. a feed that never stamps it."""
    prov: dict = {"sourceProtocol": "sim-a"}
    if edge_id:
        prov["edge_id"] = edge_id
    return {
        "eventId": "evt-1",
        "asset": {"assetId": asset_id, "platformVariant": "AH-64E-V6"},
        "sustainment": {"fluids": {"fuelRemaining": {"value": 80.0, "unit": "%"}}},
        "provenance": prov,
    }


def _registry_record(
    asset_id: str,
    *,
    edge_id: str = "",
    observed_edge_id: str = "",
    assignment_source: str = "",
) -> dict:
    """Full asset-registry-events shape -- used only to prove on_registry_event
    ignores all of this except platform_variant (ADR-0028); the one-writer
    rule no longer reads this topic at all."""
    return {
        "asset_id": asset_id,
        "edge_id": edge_id,
        "region_id": "",
        "assignment_source": assignment_source,
        "observed_edge_id": observed_edge_id,
        "divergent": False,
        "platform_variant": "",
    }


# ---------------------------------------------------------------------------
# origin in OTHER_STACK_IDS -> held (no publish, no timer re-arm)
# ---------------------------------------------------------------------------
def test_origin_in_other_stack_holds_and_ends_timer(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:1"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    before = _counter(fusion_publish_suppressed_other_stack_total)
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-02")))
    after = _counter(fusion_publish_suppressed_other_stack_total)

    assert published == [], "origin is another node's own stack -- this fusion must not publish"
    assert ctx.scheduled == [], "origin is another node's own stack -- timer chain must end"
    assert _KEY_LAST_SEVERITY not in ctx._state
    assert after == before + 1


# ---------------------------------------------------------------------------
# origin == self (this fusion's own edge, not in the set) -> publishes
# ---------------------------------------------------------------------------
def test_origin_self_not_in_set_publishes(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:2"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # edge-01 is this fusion's own ingest, not a member of OTHER_STACK_IDS.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-01")))

    assert published, "origin is this fusion's own edge -- it is the writer for this asset"
    assert ctx.scheduled


# ---------------------------------------------------------------------------
# origin is an edge with no stack of its own -> publishes
# ---------------------------------------------------------------------------
def test_origin_not_a_stack_publishes(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:3"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # edge-09 runs no fusion of its own and is not in OTHER_STACK_IDS --
    # whatever ingest produced this record, this fusion is the writer.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-09")))

    assert published, "origin is not any other node's stack -- this fusion derives for it"
    assert ctx.scheduled


# ---------------------------------------------------------------------------
# unknown origin + non-empty set -> HOLD
# ---------------------------------------------------------------------------
def test_unknown_origin_holds_when_set_nonempty(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:4"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # No edge_id anywhere on the record -- origin is never decided.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))

    assert published == [], "origin undecided -- HOLD, do not emit"
    assert ctx.scheduled == [], "origin undecided -- HOLD, do not schedule"


# ---------------------------------------------------------------------------
# empty OTHER_STACK_IDS -> today's behaviour exactly
# ---------------------------------------------------------------------------
def test_empty_set_behaves_as_today(published):
    asset_logistics.set_thresholds(Thresholds())  # other_stack_ids default: empty
    asset_id = "dis:1:1:5"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # No origin at all -- with no other stacks, this fusion still derives.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))

    assert published, "empty OTHER_STACK_IDS -- today's behaviour, always derive"
    assert ctx.scheduled


# ---------------------------------------------------------------------------
# Refresh-before-guard: the FIRST input for a brand-new asset must have its
# own provenance decide the guard in the SAME invocation, not a stale/unset
# origin from a previous one.
# ---------------------------------------------------------------------------
def test_first_input_with_bridged_origin_never_publishes(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:6"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # This asset_id has never been seen before -- is_initial would normally
    # force a publish. _refresh_origin must still run first so the guard
    # sees "edge-02" on this very call, not an unset origin.
    assert ctx._state == {}
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-02")))

    assert ctx._state[_KEY_ORIGIN]["edge_id"] == "edge-02", "origin was refreshed"
    assert published == [], "first input already carried a bridged origin -- never publish"
    assert ctx.scheduled == []


# ---------------------------------------------------------------------------
# Origin changing from a stack edge to a non-stack edge publishes on the
# very next input (no stale suppression once ownership resolves).
# ---------------------------------------------------------------------------
def test_origin_change_to_non_stack_publishes_on_next_input(published):
    asset_logistics.set_thresholds(Thresholds(other_stack_ids=frozenset({"edge-02"})))
    asset_id = "dis:1:1:7"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-02")))
    assert published == [], "still bridged from edge-02 at this point"

    # The next input for the same asset carries a different origin -- no
    # longer a bridge from another stack.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id, edge_id="edge-09")))

    assert published, "origin is no longer another stack's -- the next input must publish"
    assert ctx.scheduled


# ---------------------------------------------------------------------------
# on_registry_event: no owner key written, variant behaviour unchanged.
# The full registry-events shape (edge_id/observed_edge_id/assignment_source)
# is fed in to prove none of it is read any more -- only platform_variant.
# ---------------------------------------------------------------------------
def test_registry_event_writes_only_platform_variant():
    asset_id = "dis:1:1:8"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    event = _registry_record(
        asset_id, edge_id="edge-02", observed_edge_id="edge-05",
        assignment_source="static",
    )
    event["platform_variant"] = "AH-64E-V6"
    asyncio.run(on_registry_event(ctx, event))

    assert ctx._state == {_KEY_REGISTRY_PLATFORM_VARIANT: "AH-64E-V6"}, (
        "on_registry_event must store nothing beyond platform_variant -- "
        "no owner/ownership key of any kind"
    )
    assert ctx.runs == []
    assert ctx.scheduled == []
