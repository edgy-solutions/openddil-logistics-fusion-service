"""
AssetLogistics — Restate Virtual Object that owns per-asset logistics state.

ADR-0014: one Virtual Object per `asset_id`. State is durable across restarts
(verified by Phase 3 — `restate state clear` is required to wipe state, Kafka
topic purge alone is insufficient).

Inputs (delivered by Restate Kafka subscriptions):
  on_telemetry_window(WindowedTelemetry-bytes)
    - source topic: asset-telemetry-windows
    - rolling-window aggregates from Faust edge agent
  on_cm_state_change(AsMaintainedConfiguration-bytes)
    - source topic: asset-cm-state
    - cm-service emits these whenever per-asset CM state changes
  on_proprietary_update(EntityTelemetryEvent-bytes)
    - source topic: raw-sensor-stream
    - filtered by sustainment-presence in the handler (DIS lacks sustainment;
      sim-a and proprietary feeds carry it)
  on_timer()
    - scheduled callback, fires every EMIT_INTERVAL_SECONDS

Output:
  AssetLogisticsStatusUpdate -> asset-logistics-status (compacted, keyed by asset_id)

Durable state keys per asset:
  latest_telemetry_dict      — last EntityTelemetryEvent (as dict) with sustainment
  latest_windows_dict        — last WindowedTelemetry (as dict)
  cm_state_dict              — last AsMaintainedConfiguration (as dict)
  last_emitted_severity      — int (LogisticsSeverity enum value)
  status_revision            — uint64, increments per emission
  next_timer_ns              — int, unix ns of the next scheduled on_timer
"""
from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any

import restate
from google.protobuf.json_format import MessageToDict, Parse

from openddil.configuration.v1 import as_maintained_pb2 as cm
from openddil.logistics.v1 import logistics_status_pb2 as ls
from openddil.logistics.v1 import windowed_telemetry_pb2 as win
from openddil.telemetry.v1 import telemetry_pb2 as tel

from fusion.rules import FusionInputs, compute_logistics_status
from fusion.thresholds import Thresholds

logger = logging.getLogger("logistics.asset_logistics")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_THRESHOLDS: Thresholds | None = None


def set_thresholds(t: Thresholds) -> None:
    """Install the Thresholds instance (main.py wires this at startup so it
    reads env once and reuses for every handler invocation)."""
    global _THRESHOLDS
    _THRESHOLDS = t


def _thresholds() -> Thresholds:
    if _THRESHOLDS is None:
        raise RuntimeError(
            "Thresholds not initialized — call set_thresholds() before "
            "serving the AssetLogistics object"
        )
    return _THRESHOLDS


# Kafka publisher hook (installed by main.py).
def _extract_origin(event_dict: dict | None) -> tuple[str, str]:
    """Pull origin-node provenance from any of the four inbound shapes.

    Phase 6b §A: cm-state events carry edge_id/region_id at the JSON
    envelope's top level (cm-service stamps top-level snake_case via
    dataclasses.asdict). Telemetry / windowed / derived events carry it
    in nested provenance, encoded via MessageToDict(preserving_proto_
    field_name=False) → camelCase. Helper handles all combinations so
    per-handler call-sites stay one-liner."""
    if not event_dict:
        return "", ""
    # cm-state envelope: top-level snake_case
    top_edge = event_dict.get("edge_id")
    if top_edge:
        return top_edge, event_dict.get("region_id", "") or ""
    # Proto-derived events: nested provenance, EITHER snake_case OR
    # camelCase depending on the decoder used.
    prov = event_dict.get("provenance") or {}
    edge = prov.get("edgeId") or prov.get("edge_id") or ""
    region = prov.get("regionId") or prov.get("region_id") or ""
    return edge, region


async def _refresh_origin(ctx, event_dict: dict | None) -> None:
    """Update the per-asset _KEY_ORIGIN from the inbound event. Stored so
    emissions from on_timer (no fresh inbound event) inherit the asset's
    last-known edge attribution."""
    edge_id, region_id = _extract_origin(event_dict)
    if edge_id or region_id:
        existing = await ctx.get(_KEY_ORIGIN, type_hint=dict) or {}
        ctx.set(_KEY_ORIGIN, {
            "edge_id":   edge_id or existing.get("edge_id", ""),
            "region_id": region_id or existing.get("region_id", ""),
        })


_publish_kafka_fn = None


def set_kafka_publisher(fn) -> None:
    """Install Kafka publish callable. Signature: fn(topic, key, value_bytes)."""
    global _publish_kafka_fn
    _publish_kafka_fn = fn


def _publish_kafka(*, topic: str, key: str, value: bytes) -> None:
    if _publish_kafka_fn is None:
        raise RuntimeError("Kafka publisher not installed")
    _publish_kafka_fn(topic, key, value)


# Restate state keys
_KEY_ORIGIN = "origin_node"
_KEY_TELEMETRY = "latest_telemetry_dict"
# Phase 5 step 2: latest derived-sustainment event for this asset. Separate
# from `_KEY_TELEMETRY` so the measured/derived merge stays explicit:
# `_recompute_and_maybe_emit` uses the measured telemetry when present and
# falls back to derived only when measured is absent (the typical DIS-only
# asset case). No per-component merging in Phase 5 — the engine sees one or
# the other.
_KEY_DERIVED_TELEMETRY = "latest_derived_telemetry_dict"
_KEY_WINDOWS = "latest_windows_dict"
_KEY_CM_STATE = "cm_state_dict"
_KEY_LAST_SEVERITY = "last_emitted_severity"
_KEY_REVISION = "status_revision"
_KEY_NEXT_TIMER = "next_timer_ns"


# ---------------------------------------------------------------------------
# Virtual Object declaration
# ---------------------------------------------------------------------------
asset_logistics = restate.VirtualObject("AssetLogistics")


@asset_logistics.handler(
    "on_telemetry_window",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_telemetry_window(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a windowed-telemetry record. Refresh state, recompute,
    emit if the severity transitioned."""
    asset_id = ctx.key()
    record_dict = _decode_windowed_telemetry(raw)
    if not record_dict:
        return

    ctx.set(_KEY_WINDOWS, record_dict)
    await _refresh_origin(ctx, record_dict)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="windowed_telemetry")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_cm_state_change",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_cm_state_change(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a cm-state update (cm-service publishes JSON, not proto, today
    — accept both via a small bridge)."""
    asset_id = ctx.key()
    state_dict = _decode_cm_state(raw)
    if not state_dict:
        return

    ctx.set(_KEY_CM_STATE, state_dict)
    await _refresh_origin(ctx, state_dict)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="cm_state_change")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_proprietary_update",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_proprietary_update(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a Silver raw-sensor-stream event. Skips DIS-sourced events
    (DIS carries no sustainment; treating them as logistics input would
    flood with zero-value telemetry per ADR-0010)."""
    asset_id = ctx.key()
    record_dict = _decode_telemetry_event(raw)
    if not record_dict:
        return

    # Skip DIS-sourced events — they have no sustainment fields.
    src = (record_dict.get("provenance") or {}).get("sourceProtocol", "")
    if "DIS" in src.upper():
        return

    ctx.set(_KEY_TELEMETRY, record_dict)
    await _refresh_origin(ctx, record_dict)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="telemetry")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_derived_sustainment",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_derived_sustainment(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a Phase 5 prognostics derived-sustainment event.

    The engine (ADR-0020) emits an EntityTelemetryEvent on the
    `derived-sustainment` topic carrying derived `sustainment.*` values
    (typically `wear.components`) with `value_provenance["*"] = DERIVED`.
    For DIS-only assets — which have no measured sustainment — this is the
    only path that surfaces wear-driven logistics severity.

    Stored under a separate state key so the merge with measured stays
    explicit: `_recompute_and_maybe_emit` prefers measured when present
    and falls back to derived otherwise. Per Phase 5 mechanism scope,
    NOT per-component merged; that arrives with the validation phase."""
    asset_id = ctx.key()
    record_dict = _decode_telemetry_event(raw)
    if not record_dict:
        return

    ctx.set(_KEY_DERIVED_TELEMETRY, record_dict)
    await _refresh_origin(ctx, record_dict)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="derived_sustainment")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler("on_timer")
async def on_timer(ctx: restate.ObjectContext, _: dict | None = None) -> None:
    """Scheduled tick. Recompute (some factors are time-dependent — staleness,
    MTBF projection) and emit a cadenced update even if severity didn't change.
    """
    asset_id = ctx.key()
    # Always emit on the cadence, regardless of transition.
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="timer",
                                     force_emit=True)
    await _schedule_next_timer(ctx, asset_id)


# ---------------------------------------------------------------------------
# Core recompute path
# ---------------------------------------------------------------------------
async def _recompute_and_maybe_emit(
    ctx: restate.ObjectContext,
    asset_id: str,
    *,
    trigger: str,
    force_emit: bool = False,
) -> None:
    """Pull latest state, run pure rules, emit if transitioning or forced."""
    telemetry_dict = await ctx.get(_KEY_TELEMETRY, type_hint=dict)
    derived_telemetry_dict = await ctx.get(_KEY_DERIVED_TELEMETRY, type_hint=dict)
    windows_dict = await ctx.get(_KEY_WINDOWS, type_hint=dict)
    cm_dict = await ctx.get(_KEY_CM_STATE, type_hint=dict)

    # Phase 5 merge rule: measured wins, derived fills the DIS-asset gap.
    # `on_proprietary_update` already drops DIS-sourced events (no
    # sustainment), so `_KEY_TELEMETRY` is empty for DIS-only assets;
    # `_KEY_DERIVED_TELEMETRY` is the only source of wear for them.
    chosen_telemetry_dict = telemetry_dict or derived_telemetry_dict
    telemetry_proto = (_dict_to_telemetry(chosen_telemetry_dict)
                       if chosen_telemetry_dict else None)
    windows_proto = _dict_to_windows(windows_dict) if windows_dict else None
    cm_proto = _dict_to_cm_state(cm_dict) if cm_dict else None

    platform_variant = ""
    if telemetry_proto is not None and telemetry_proto.asset.platform_variant:
        platform_variant = telemetry_proto.asset.platform_variant
    elif windows_proto is not None and windows_proto.platform_variant:
        platform_variant = windows_proto.platform_variant

    inputs = FusionInputs(
        asset_id=asset_id,
        platform_variant=platform_variant,
        latest_telemetry=telemetry_proto,
        telemetry_windows=windows_proto,
        cm_state=cm_proto,
    )
    now_ns = _now_ns(ctx)
    status = compute_logistics_status(inputs, _thresholds(), now_ns)

    prev_sev = await ctx.get(_KEY_LAST_SEVERITY, type_hint=int)
    is_initial = prev_sev is None
    is_transition = (not is_initial) and prev_sev != status.overall_severity

    if not (is_initial or is_transition or force_emit):
        return  # quiet update, no emission

    revision = await ctx.get(_KEY_REVISION, type_hint=int) or 0
    revision += 1
    status.status_revision = revision

    update = ls.AssetLogisticsStatusUpdate(
        status=status,
        previous_severity=prev_sev or ls.LOGISTICS_SEVERITY_UNSPECIFIED,
        is_transition=is_transition,
        is_initial=is_initial,
    )

    # Phase 6b §A: stamp origin-node provenance from the asset's stored
    # _KEY_ORIGIN (inherited from inbound events via _refresh_origin).
    # The projector logistics_status handler will read this and write it
    # to the per-asset row's edge_id / region_id columns.
    origin = await ctx.get(_KEY_ORIGIN, type_hint=dict) or {}
    if origin.get("edge_id"):
        update.provenance.edge_id = origin["edge_id"]
    if origin.get("region_id"):
        update.provenance.region_id = origin["region_id"]
    update.provenance.producer_id = "logistics-fusion-service"

    await ctx.run(
        "publish-asset-logistics-status",
        lambda: _publish_kafka(
            topic="asset-logistics-status",
            key=asset_id,
            value=update.SerializeToString(),
        ),
    )
    ctx.set(_KEY_LAST_SEVERITY, status.overall_severity)
    ctx.set(_KEY_REVISION, revision)

    logger.info(
        "Emitted %s for %s: %s -> %s (rev=%d, trigger=%s, factors=%d)",
        "initial" if is_initial else ("transition" if is_transition else "cadenced"),
        asset_id,
        _sev_name(prev_sev or ls.LOGISTICS_SEVERITY_UNSPECIFIED),
        _sev_name(status.overall_severity),
        revision, trigger, len(status.constraining_factors),
    )


async def _schedule_next_timer(ctx: restate.ObjectContext, asset_id: str) -> None:
    """Schedule the next on_timer firing EMIT_INTERVAL_SECONDS from now.

    Debounce: if a timer is already scheduled within the cadence window,
    don't double-schedule (avoids piling up timer events for chatty assets).
    """
    now_ns = _now_ns(ctx)
    cadence_ns = _thresholds().emit_interval_seconds * 1_000_000_000
    target_ns = now_ns + cadence_ns

    existing = await ctx.get(_KEY_NEXT_TIMER, type_hint=int)
    if existing and existing > now_ns and (existing - now_ns) <= cadence_ns:
        return  # already scheduled within the window

    ctx.set(_KEY_NEXT_TIMER, target_ns)
    ctx.object_send(
        on_timer, key=asset_id, arg={},
        send_delay=timedelta(seconds=_thresholds().emit_interval_seconds),
    )


# ---------------------------------------------------------------------------
# Bytes -> dict decoders (Restate state stores JSON)
# ---------------------------------------------------------------------------
def _decode_telemetry_event(raw: bytes | dict | None) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        evt = tel.EntityTelemetryEvent()
        evt.ParseFromString(raw if isinstance(raw, (bytes, bytearray)) else bytes(raw))
        return MessageToDict(evt, preserving_proto_field_name=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to decode telemetry event (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


def _decode_windowed_telemetry(raw: bytes | dict | None) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        w = win.WindowedTelemetry()
        w.ParseFromString(raw if isinstance(raw, (bytes, bytearray)) else bytes(raw))
        return MessageToDict(w, preserving_proto_field_name=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to decode WindowedTelemetry (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


def _decode_cm_state(raw: bytes | dict | None) -> dict:
    """cm-service publishes asset-cm-state as JSON (dataclass-derived).
    Tolerate proto-binary too."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    # Try JSON first (cm-service current format).
    try:
        text = raw.decode("utf-8")
        return json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    # Fallback: proto binary.
    try:
        state = cm.AsMaintainedConfiguration()
        state.ParseFromString(raw if isinstance(raw, (bytes, bytearray)) else bytes(raw))
        return MessageToDict(state, preserving_proto_field_name=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to decode cm-state payload (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


# ---------------------------------------------------------------------------
# Dict -> proto bridges. The rules engine wants protos; state holds dicts.
# ---------------------------------------------------------------------------
def _dict_to_telemetry(d: dict) -> tel.EntityTelemetryEvent | None:
    if not d:
        return None
    try:
        return Parse(json.dumps(d), tel.EntityTelemetryEvent(),
                     ignore_unknown_fields=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("dict->EntityTelemetryEvent parse failed: %s", exc)
        return None


def _dict_to_windows(d: dict) -> win.WindowedTelemetry | None:
    if not d:
        return None
    try:
        return Parse(json.dumps(d), win.WindowedTelemetry(),
                     ignore_unknown_fields=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("dict->WindowedTelemetry parse failed: %s", exc)
        return None


def _dict_to_cm_state(d: dict) -> cm.AsMaintainedConfiguration | None:
    """cm-service emits dataclass-derived JSON whose key names mostly match
    the proto camelCase. Use ignore_unknown_fields to tolerate the dataclass-
    only fields (e.g., `last_alerted_status`)."""
    if not d:
        return None
    try:
        return Parse(json.dumps(d), cm.AsMaintainedConfiguration(),
                     ignore_unknown_fields=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("dict->AsMaintainedConfiguration parse failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _now_ns(ctx: restate.ObjectContext) -> int:
    try:
        return int(ctx.time().timestamp() * 1_000_000_000)
    except Exception:  # noqa: BLE001
        from datetime import datetime, timezone
        return int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)


def _sev_name(s: int) -> str:
    try:
        return ls.LogisticsSeverity.Name(s)
    except ValueError:
        return f"UNKNOWN({s})"
