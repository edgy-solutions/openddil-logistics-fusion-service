"""Handler-level tests for `AssetLogistics.on_registry_event` and the
platform_variant resolution order it feeds into `_recompute_and_maybe_emit`.

Uses the same stub ObjectContext as `test_effector_event_handler.py` /
`test_removal_unknown_asset.py` (see either file's docstring for why a
minimal stand-in is enough: the handlers only touch
`ctx.get`/`ctx.set`/`ctx.run`/`ctx.object_send`/`ctx.key`/`ctx.time`).
Duplicated locally rather than imported, matching those files' own
convention.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from fusion.thresholds import Thresholds
from openddil.logistics.v1 import logistics_status_pb2 as ls
from workflows import asset_logistics
from workflows.asset_logistics import (
    _KEY_REGISTRY_PLATFORM_VARIANT,
    on_effector_event,
    on_proprietary_update,
    on_registry_event,
    on_telemetry_window,
    on_timer,
)


# ---------------------------------------------------------------------------
# Stub Restate ObjectContext (mirrors test_effector_event_handler.py::StubCtx)
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


class TrackingStubCtx(StubCtx):
    """Same stub, plus a record of every `ctx.set` call -- needed to assert
    an unchanged variant performs no write at all, not just that the state
    ends up the same."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_calls: list[tuple[str, object]] = []

    def set(self, name: str, value) -> None:
        self.set_calls.append((name, value))
        super().set(name, value)


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


def _telemetry_record(asset_id: str, *, platform_variant: str | None = None) -> dict:
    asset = {"assetId": asset_id}
    if platform_variant is not None:
        asset["platformVariant"] = platform_variant
    return {
        "eventId": "evt-1",
        "asset": asset,
        "sustainment": {"fluids": {"fuelRemaining": {"value": 80.0, "unit": "%"}}},
        "provenance": {"sourceProtocol": "dis"},
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


def _registry_record(asset_id: str, platform_variant: str) -> dict:
    return {"asset_id": asset_id, "platform_variant": platform_variant}


def _last_status_update(published: list[tuple[str, str, bytes]]):
    """`published` can also carry a `tactical-events` entry on a severity
    transition (see `_recompute_and_maybe_emit`'s tail) -- filter down to
    the `asset-logistics-status` topic before parsing as
    AssetLogisticsStatusUpdate, or a transition's tactical-event record
    fails to parse as one."""
    status_entries = [p for p in published if p[0] == "asset-logistics-status"]
    assert status_entries, "no asset-logistics-status publish found"
    update = ls.AssetLogisticsStatusUpdate()
    update.ParseFromString(status_entries[-1][2])
    return update


# ---------------------------------------------------------------------------
# on_registry_event: store/ignore/never-clear/no-op-on-unchanged
# ---------------------------------------------------------------------------
def test_stores_nonempty_variant():
    asset_id = "dis:1:1:5002"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "AH-64E-V6")))

    assert ctx._state[_KEY_REGISTRY_PLATFORM_VARIANT] == "AH-64E-V6"
    assert ctx.runs == [], "on_registry_event never recomputes/publishes"
    assert ctx.scheduled == [], "on_registry_event never schedules a timer"


def test_ignores_empty_variant():
    asset_id = "dis:1:1:5002"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "")))

    assert _KEY_REGISTRY_PLATFORM_VARIANT not in ctx._state


def test_empty_observation_does_not_clear_existing_variant():
    asset_id = "dis:1:1:5002"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())
    ctx.set(_KEY_REGISTRY_PLATFORM_VARIANT, "AH-64E-V6")

    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "")))

    assert ctx._state[_KEY_REGISTRY_PLATFORM_VARIANT] == "AH-64E-V6"


def test_noop_when_variant_unchanged():
    asset_id = "dis:1:1:5002"
    ctx = TrackingStubCtx(key=asset_id, now_ns=_now_ns())
    ctx.set(_KEY_REGISTRY_PLATFORM_VARIANT, "AH-64E-V6")
    ctx.set_calls.clear()

    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "AH-64E-V6")))

    assert ctx.set_calls == [], "same value in -- no redundant write"


# ---------------------------------------------------------------------------
# Resolution order: registry state wins over telemetry_proto.asset
# ---------------------------------------------------------------------------
def test_registry_variant_wins_over_telemetry(install_thresholds_and_publisher):
    published = install_thresholds_and_publisher
    asset_id = "dis:1:1:5003"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    asyncio.run(on_proprietary_update(
        ctx, _telemetry_record(asset_id, platform_variant="TELEMETRY-VARIANT")
    ))
    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "REGISTRY-VARIANT")))

    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000
    asyncio.run(on_timer(ctx, None))

    assert published, "on_timer(force_emit=True) always publishes"
    update = _last_status_update(published)
    assert update.status.platform_variant == "REGISTRY-VARIANT"


def test_telemetry_wins_when_registry_empty(install_thresholds_and_publisher):
    published = install_thresholds_and_publisher
    asset_id = "dis:1:1:5005"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    asyncio.run(on_proprietary_update(
        ctx, _telemetry_record(asset_id, platform_variant="TELEMETRY-VARIANT")
    ))
    asyncio.run(on_telemetry_window(
        ctx, {"platformVariant": "WINDOWS-VARIANT"}
    ))
    # No on_registry_event call -- the registry has nothing recorded for
    # this asset, so the record (telemetry) is the next fallback, ahead of
    # the window.

    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000
    asyncio.run(on_timer(ctx, None))

    update = _last_status_update(published)
    assert update.status.platform_variant == "TELEMETRY-VARIANT"


def test_windows_last_when_registry_and_telemetry_empty(install_thresholds_and_publisher):
    published = install_thresholds_and_publisher
    asset_id = "dis:1:1:5006"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # No on_proprietary_update, no on_registry_event -- the window is the
    # only source that ever carried a variant for this asset.
    asyncio.run(on_telemetry_window(ctx, {"platformVariant": "WINDOWS-VARIANT"}))

    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000
    asyncio.run(on_timer(ctx, None))

    update = _last_status_update(published)
    assert update.status.platform_variant == "WINDOWS-VARIANT"


# ---------------------------------------------------------------------------
# Effector factor appears for a DIS-only asset once the registry records
# the variant that unlocks a variant-keyed declared-load entry.
# ---------------------------------------------------------------------------
def test_effector_factor_appears_once_registry_variant_set(
    install_thresholds_and_publisher,
):
    published = install_thresholds_and_publisher
    asset_logistics.set_declared_load({
        "asset": {},
        "variant": {"REGISTRY-VARIANT": {_MUNITION_KEY: 1}},
    })
    asset_id = "dis:1:1:5004"
    ctx = StubCtx(key=asset_id, now_ns=_now_ns())

    # DIS-only admission -- no platform_variant anywhere yet, so the
    # variant-keyed declared-load entry above cannot resolve.
    asyncio.run(on_proprietary_update(ctx, _telemetry_record(asset_id)))
    asyncio.run(on_effector_event(ctx, _fire_record(event_urn="dis-event:1:1:9", quantity=1)))
    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000
    asyncio.run(on_timer(ctx, None))

    pre = _last_status_update(published)
    assert not any(
        f.factor_id == f"effector.{_MUNITION_KEY}"
        for f in pre.status.constraining_factors
    ), "no variant recorded anywhere yet -- the variant-keyed entry can't resolve"

    asyncio.run(on_registry_event(ctx, _registry_record(asset_id, "REGISTRY-VARIANT")))
    ctx._now_ns += (Thresholds().emit_interval_seconds + 5) * 1_000_000_000
    asyncio.run(on_timer(ctx, None))

    post = _last_status_update(published)
    assert post.status.platform_variant == "REGISTRY-VARIANT"
    assert any(
        f.factor_id == f"effector.{_MUNITION_KEY}"
        for f in post.status.constraining_factors
    ), "declared=1, expended=1 -> 0% remaining, now resolvable via the registry variant"
