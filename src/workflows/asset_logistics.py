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
  on_derived_sustainment(EntityTelemetryEvent-bytes)
    - source topic: derived-sustainment
    - Phase 5 prognostics engine; wear values stamped ORIGIN_DERIVED
  on_capability_snapshot(AssetCapabilitySnapshot-JSON)
    - source topic: asset-capability-snapshot
    - weapons-capability feed (source-specific messages decomposed
      into the canonical shape at ingress); drives the engagement-
      worthiness evaluator (Sub-phase F)
  on_effector_event(Fire/Detonation-JSON)
    - source topic: effector-events, keyed by launcher_urn
    - Fire accumulates expended[munition_key] (deduped by event_urn) and
      drives the effector-supply evaluator; Detonation is counted only
  on_registry_event(asset-registry-events-JSON)
    - source topic: asset-registry-events, keyed by asset_id
    - asset-registry-service's own record of this asset (ADR-0028); the
      only field this object reads from it is platform_variant. Records
      a non-empty value when it differs from what's stored; never emits,
      never schedules a timer — it's an enrichment input, not a trigger
  on_timer()
    - scheduled callback, fires every EMIT_INTERVAL_SECONDS

Output:
  AssetLogisticsStatusUpdate -> asset-logistics-status (compacted, keyed by asset_id)

Durable state keys per asset:
  latest_telemetry_dict          — last EntityTelemetryEvent (as dict) with sustainment
  latest_derived_telemetry_dict  — last derived-sustainment EntityTelemetryEvent
  latest_windows_dict            — last WindowedTelemetry (as dict)
  cm_state_dict                  — last AsMaintainedConfiguration (as dict)
  latest_capability_dict         — last AssetCapabilitySnapshot (as dict)
  effector_expended_dict         — Σ quantity fired per munition_key
  effector_counted_urns_list     — dedup set (ordered list) of counted Fire event_urns
  registry_platform_variant      — platform_variant as last recorded by asset_registry
  last_emitted_severity          — int (LogisticsSeverity enum value)
  status_revision                — uint64, increments per emission
  next_timer_ns                  — int, unix ns of the next scheduled on_timer
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import restate
from google.protobuf.json_format import MessageToDict, Parse

from openddil.configuration.v1 import as_maintained_pb2 as cm
from openddil.logistics.v1 import logistics_status_pb2 as ls
from openddil.logistics.v1 import windowed_telemetry_pb2 as win
from openddil.telemetry.v1 import telemetry_pb2 as tel

from fusion import effector_supply
from fusion.rules import FusionInputs, compute_logistics_status
from fusion.thresholds import Thresholds
from metrics import (
    fusion_effector_detonation_seen_total,
    fusion_effector_refused_total,
    fusion_effector_replayed_total,
    fusion_publish_suppressed_other_stack_total,
    removal_unknown_asset_dropped_total,
)

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


# Declared-load table (fusion.effector_supply.load_declared_load's return
# shape): {"asset": {id: {munition_key: declared}}, "variant": {...}}.
# Defaults to empty so a service that never calls set_declared_load (e.g. a
# unit test that only exercises other handlers) resolves "nothing declared"
# rather than raising, matching load_declared_load's own no-file default.
_DECLARED_LOAD_TABLE: dict = {"asset": {}, "variant": {}}


def set_declared_load(table: dict) -> None:
    """Install the parsed declared-load table. main.py calls this at
    startup with fusion.effector_supply.load_declared_load()'s result, the
    same pattern set_thresholds uses."""
    global _DECLARED_LOAD_TABLE
    _DECLARED_LOAD_TABLE = table


def _declared_load_table() -> dict:
    return _DECLARED_LOAD_TABLE


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


def _extract_releasability(event_dict: dict | None) -> tuple[str, list[str]]:
    """Pull ADR-0029 releasability labels from any of the four inbound shapes.

    Deliberately mirrors _extract_origin above, including the cm-state
    top-level case and the camelCase/snake_case ambiguity that comes from
    two different MessageToDict settings being live in this system. Written
    as a sibling rather than folded into _extract_origin because the two
    answer different questions and one of them is allowed to be absent
    routinely — origin falls back to an env default, a MISSING LABEL MUST
    NOT.

    Returns ("", []) when the event carries no label. Empty nation is the
    unlabelled signal; an empty releasable_to list beside a real nation is a
    legitimate and common posture, not an absence."""
    if not event_dict:
        return "", []
    # cm-state envelope: top-level snake_case (ADR-0018 JSON shape).
    top = event_dict.get("originator_nation")
    if top:
        return top, list(event_dict.get("releasable_to") or [])
    prov = event_dict.get("provenance") or {}
    nation = prov.get("originatorNation") or prov.get("originator_nation") or ""
    releasable = prov.get("releasableTo") or prov.get("releasable_to") or []
    return nation, list(releasable)


async def _refresh_releasability(ctx, event_dict: dict | None) -> None:
    """Store the asset's labels so emissions from on_timer — which have no
    fresh inbound event — still carry them.

    THE STICKY BEHAVIOUR IS DELIBERATE AND IS THE SAME CHOICE _refresh_origin
    MADE: once an asset's nation is known, a later event that omits it does
    not clear it. An asset does not change nationality because one message
    was thin, and clearing on absence would make the completeness gate
    flicker with feed hiccups. Relabelling requires a positive statement."""
    nation, releasable = _extract_releasability(event_dict)
    if nation:
        ctx.set(_KEY_RELEASABILITY,
                {"originator_nation": nation, "releasable_to": releasable})


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


_OPERATIONAL_AXES = ("health_state", "power_state", "functional_mode")


def _apply_operational_axes(telemetry_proto, axes: dict) -> None:
    """Write the remembered axes onto the telemetry the rules will read.

    Only axes an actual source SET are written. An axis nobody has spoken to
    stays at its proto default, which is the UNSPECIFIED the absence
    convention requires — this must never manufacture a claim to fill a gap.
    """
    op = telemetry_proto.operational_state
    for axis, entry in (axes or {}).items():
        value = (entry or {}).get("value")
        if not value or axis not in _OPERATIONAL_AXES:
            continue
        field = op.DESCRIPTOR.fields_by_name.get(axis)
        if field is None or field.enum_type is None:
            continue
        enum_value = field.enum_type.values_by_name.get(value)
        if enum_value is None:
            # A source said something this build's enum does not know. Not a
            # claim we can act on, and not one to guess at.
            logger.warning("operational axis %s carried unknown value %r; "
                            "leaving the axis unset", axis, value)
            continue
        setattr(op, axis, enum_value.number)


# The sustainment sub-messages an evaluator can actually USE. `health` is
# excluded on purpose: a DIS record carries `sustainment.health` (an empty
# submessage) and nothing else, and the wear/fuel/ammo evaluators read none
# of it.
_SUSTAINMENT_PAYLOAD_FIELDS = ("wear", "fluids", "consumables", "thermal", "power")


def _carries_sustainment(record_dict: dict | None) -> bool:
    """Does this record carry sustainment DATA an evaluator can use?

    ⚠ THE FIRST VERSION OF THIS ASKED `bool(record["sustainment"])` AND WAS
    WRONG IN THE SAME WAY THE GUARD IT REPLACED WAS WRONG.

    A DIS Silver record carries `sustainment = {"health": {}}` — present, and
    empty. Proto submessage presence is asserted by writing ANY field, so the
    container exists while carrying nothing an evaluator reads (GD-12's
    submessage addendum: for scalars the question is "how do I say nothing?",
    for submessages it is "how do I avoid accidentally saying something?").

    The consequence, measured on the lab: every DIS record was admitted to
    `_KEY_TELEMETRY`, which `_recompute` prefers over
    `_KEY_DERIVED_TELEMETRY`, so the derived record holding the actual wear
    components stopped being chosen and the fleet lost EVERY wear factor.
    The RCV-M sitting at 100% consumed track read OK.

    So: test the payload, not the container. I replaced a source-name
    correlate with a key-presence correlate, which is the same mistake at a
    shorter distance — the property is "carries usable sustainment data", and
    that is what this now asks.
    """
    sust = (record_dict or {}).get("sustainment") or {}
    if not isinstance(sust, dict):
        return False
    for field in _SUSTAINMENT_PAYLOAD_FIELDS:
        if sust.get(field):
            return True
    return False


def _is_removal(record_dict: dict | None) -> bool:
    """Does this record claim OPERATIONAL_STATUS_REMOVED (ADR-0044 §A)?

    A Remove Entity PDU decoded by `_decode_telemetry_event` carries this on
    `operational_state.operational_status`. Two shapes reach this function:

      * The production path — proto bytes through `MessageToDict(...,
        preserving_proto_field_name=False)` — renders the field name as
        `operationalStatus` and the enum VALUE as its NAME STRING (the
        default is `use_integers_for_enums=False`, and nothing here passes
        that kwarg), so this is `"OPERATIONAL_STATUS_REMOVED"`.
      * `_decode_telemetry_event`'s dict-passthrough branch (tests, and any
        future JSON-native producer) — shape not otherwise constrained, so
        both the proto snake_case field name and an int enum value (4, the
        wire value — see `tel.OPERATIONAL_STATUS_REMOVED`) are also
        accepted, mirroring how `_absorb_operational_state` already checks
        both `operationalState`/`operational_state` for the container.
    """
    op = ((record_dict or {}).get("operationalState")
          or (record_dict or {}).get("operational_state") or {})
    if not isinstance(op, dict):
        return False
    status = op.get("operationalStatus")
    if status is None:
        status = op.get("operational_status")
    return status in ("OPERATIONAL_STATUS_REMOVED", tel.OPERATIONAL_STATUS_REMOVED)


# ADR-0044 lifecycle gate: the state keys any inbound handler can set BEFORE
# its own first `_recompute_and_maybe_emit` call in the SAME invocation.
# Checking all five, rather than relying on `_KEY_LAST_SEVERITY` alone, is
# the minimal set that stays reliable if a future handler changes order:
# today every one of the other four is written by a handler that always
# reaches `_recompute_and_maybe_emit` immediately afterward, and that first
# call always emits (`is_initial` is true whenever `_KEY_LAST_SEVERITY` is
# still None) — so in the CURRENT control flow, `_KEY_LAST_SEVERITY` alone
# would already answer "has this asset_id been seen before?" correctly.
# But that is a fact about today's call order in each handler, not a
# contract those four keys promise, and this gate exists specifically to
# stop a never-seen asset_id from acquiring state — it should not depend on
# every future handler continuing to recompute-and-emit in the same
# invocation as its first write. `_KEY_WINDOWS` and `_KEY_CAPABILITY` are
# left out on purpose: they are not in the five candidates this gate is
# scoped to (windowed-telemetry and capability-snapshot assets already
# satisfy "known" via `_KEY_LAST_SEVERITY`, set on their own first event by
# the same first-invocation-always-emits rule above).
#
# (`_KNOWN_ASSET_KEYS` / `_has_known_state` are defined further down, once
# the `_KEY_*` state-key constants they reference exist.)


async def _absorb_operational_state(ctx, record_dict: dict | None) -> None:
    """Merge whatever operational axes this record carries, per axis.

    Last-writer-wins PER AXIS rather than per record: a source that speaks to
    one axis must not blank the two it says nothing about. Each axis keeps the
    source and sample time that set it, so a later disagreement between two
    sources about the same axis is visible rather than silently resolved by
    arrival order.
    """
    op = (record_dict or {}).get("operationalState") or          (record_dict or {}).get("operational_state") or {}
    if not op:
        return
    prov = (record_dict or {}).get("provenance") or {}
    source = prov.get("sourceProtocol") or prov.get("source_protocol") or "unknown"
    sample_time = (record_dict or {}).get("sampleTime") or                   (record_dict or {}).get("sample_time") or ""

    current = (await ctx.get(_KEY_OPERATIONAL_STATE, type_hint=dict)) or {}
    changed = False
    for axis in _OPERATIONAL_AXES:
        value = op.get(axis) or op.get(_snake_to_camel(axis))
        if not value:
            continue                       # this source says nothing on this axis
        current[axis] = {"value": value, "source": source,
                         "sample_time": str(sample_time)}
        changed = True
    if changed:
        ctx.set(_KEY_OPERATIONAL_STATE, current)


def _snake_to_camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(p.title() for p in rest)


async def _refresh_provenance(ctx, event_dict: dict | None) -> None:
    """Refresh EVERYTHING an emission inherits from an inbound event.

    Exists so there is exactly ONE call to add at an inbound handler, not a
    pair that must be kept together. There are five inbound shapes; a second
    call site that someone forgets to add beside the first would mean labels
    never propagate for assets that only ever arrive on that path — and the
    symptom would be a §7 gate failure attributed to ingress rather than to
    the handler that dropped the label. Cheap to prevent, expensive to
    diagnose."""
    await _refresh_origin(ctx, event_dict)
    await _refresh_releasability(ctx, event_dict)


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
# One writer per asset, from record provenance: this key's edge_id is also
# the origin(asset) the guard in `_held_for_other_stack` reads -- no
# separate key, no registry, no new header. See that function and the
# module docstring.
_KEY_ORIGIN = "origin_node"
# ADR-0029 §3. Held per-asset for the same reason as _KEY_ORIGIN: a derived
# row emitted on a timer has no inbound event to read labels from, and a
# derived row without labels is a leak by omission — the severity of an asset
# is as national as the asset.
_KEY_RELEASABILITY = "releasability"
_KEY_TELEMETRY = "latest_telemetry_dict"
# Phase 5 step 2: latest derived-sustainment event for this asset. Separate
# from `_KEY_TELEMETRY` so the measured/derived merge stays explicit:
# `_recompute_and_maybe_emit` uses the measured telemetry when present and
# falls back to derived only when measured is absent (the typical DIS-only
# asset case). No per-component merging in Phase 5 — the engine sees one or
# the other.
_KEY_DERIVED_TELEMETRY = "latest_derived_telemetry_dict"
# UD-13. The latest OPERATIONAL state per axis, regardless of which source
# supplied it. Separate from `_KEY_TELEMETRY` because the two answer
# different questions: that key holds the record wear is computed from, this
# one holds what any source last said about health / power / mode.
#
# Shape: {axis: {"value": str, "source": str, "sample_time": str}}
#
# PER-AXIS, and stamped, because a DIS record carries health (decoded from
# appearance bits) while a richer feed carries all three — so two sources
# speak to the same asset at different cadences, which is ADR-0041's cadence
# asymmetry arriving inside a single asset. Last-writer-wins per axis is the
# rule; the stamps are what make a cross-source disagreement (DIS says FAULT,
# another feed says NOMINAL, minutes apart) DETECTABLE later. The detector is
# not built — the stamps only make it possible.
_KEY_OPERATIONAL_STATE = "operational_state_axes"
_KEY_WINDOWS = "latest_windows_dict"
_KEY_CM_STATE = "cm_state_dict"
# Sub-phase F: latest weapons-capability snapshot for this asset
# (Silver topic asset-capability-snapshot, decomposed from a source-
# specific producer at ingress). Drives the engagement-worthiness
# evaluator (`_eval_inventory`).
_KEY_CAPABILITY = "latest_capability_dict"
# Effector supply: Σ quantity fired per munition_key, from fusion's own
# `effector-events` subscription (on_effector_event), never from telemetry
# or the projector's table. `_KEY_EFFECTOR_COUNTED_URNS` is the dedup set
# (as an ordered list — Restate state must be JSON-serializable) that
# `fusion.effector_supply.apply_fire` uses to recognise a replayed Fire.
# Both keys live on THIS object, so `restate state clear AssetLogistics/
# <asset_id>` (the existing reset path — see clear_asset_logistics_state in
# the hero-test helpers) clears them along with every other key here; no
# separate reset path exists or is needed for either.
_KEY_EFFECTOR_EXPENDED = "effector_expended_dict"
_KEY_EFFECTOR_COUNTED_URNS = "effector_counted_urns_list"
# ADR-0028: asset_registry's own platform_variant, as last published on
# asset-registry-events. Separate from the capability/telemetry-derived
# guesses `_recompute_and_maybe_emit` falls back to -- this one is RECORDED
# by the registry, not read off whichever event happened to carry it, so it
# is preferred over them (see the resolution order where platform_variant
# is computed).
_KEY_REGISTRY_PLATFORM_VARIANT = "registry_platform_variant"
_KEY_LAST_SEVERITY = "last_emitted_severity"
_KEY_REVISION = "status_revision"
_KEY_NEXT_TIMER = "next_timer_ns"

# ADR-0044 lifecycle gate (removal-unknown-key): the "known key" candidates
# — see the comment above `_is_removal` for why these five and not fewer.
_KNOWN_ASSET_KEYS = (
    _KEY_LAST_SEVERITY, _KEY_TELEMETRY, _KEY_DERIVED_TELEMETRY,
    _KEY_OPERATIONAL_STATE, _KEY_CM_STATE,
)


async def _has_known_state(ctx) -> bool:
    """True if this Virtual Object instance has ever recorded state for this
    asset_id, per `_KNOWN_ASSET_KEYS` above."""
    for key in _KNOWN_ASSET_KEYS:
        if (await ctx.get(key)) is not None:
            return True
    return False


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
    await _refresh_provenance(ctx, record_dict)
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
    await _refresh_provenance(ctx, state_dict)
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

    # Removal-unknown-key gate: the upstream kind gate that used to keep a
    # stateless removal from reaching an asset_id this VO has never heard of
    # is becoming stateless itself (every removal now passes by PDU type),
    # so a Remove Entity for a never-seen asset_id must be dropped HERE,
    # before any state is created for it — not absorbed, not emitted, no
    # timer scheduled. A removal for an asset_id this VO already knows
    # follows the unchanged path below.
    if _is_removal(record_dict) and not await _has_known_state(ctx):
        removal_unknown_asset_dropped_total.inc()
        return

    # UD-13: ADMITTED BY CONTENT, NEVER BY SOURCE NAME.
    #
    # This used to read `if "DIS" in src.upper(): return`, justified as "they
    # have no sustainment fields". That was true when written and false from
    # 2026-08-19, when the appearance-bits mapping gave DIS records an
    # operational_state. The guard tested a CORRELATE of the property it
    # cared about, and the correlate expired while the guard did not.
    #
    # What it was protecting is real and is preserved below: `_recompute`
    # prefers `_KEY_TELEMETRY` over `_KEY_DERIVED_TELEMETRY`, so admitting a
    # sustainment-less record there would blank wear for exactly the fleet
    # this helps. So the question is now asked of the RECORD — does it carry
    # sustainment? — which is true forever, rather than of its source name,
    # which was true until someone improved DIS.
    await _absorb_operational_state(ctx, record_dict)
    if _carries_sustainment(record_dict):
        ctx.set(_KEY_TELEMETRY, record_dict)
    await _refresh_provenance(ctx, record_dict)
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
    await _refresh_provenance(ctx, record_dict)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="derived_sustainment")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_capability_snapshot",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_capability_snapshot(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a weapons-capability snapshot (Sub-phase F).

    Source topic: `asset-capability-snapshot` (Silver, JSON). Source-
    specific weapons-capability feeds land here via their respective connect
    overlay; each message is the current per-store Ammo state for one
    asset. Drives the engagement-worthiness evaluator (`_eval_inventory`)
    — `AMMO_LOW` / `AMMO_EXHAUSTED` ConstrainingFactors carried on the
    existing `asset-logistics-status` output, no new topic.

    A capability-only asset has no measured telemetry; the snapshot is its
    sole input. `_recompute_and_maybe_emit` runs the full rule set against
    whatever state is present, so this path needs no special-casing."""
    asset_id = ctx.key()
    snapshot = _decode_capability_snapshot(raw)
    if not snapshot:
        return

    ctx.set(_KEY_CAPABILITY, snapshot)
    await _refresh_provenance(ctx, snapshot)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="capability_snapshot")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_effector_event",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_effector_event(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume a Fire or Detonation record from `effector-events` (keyed by
    `launcher_urn`, which is this object's key). Registered the same way
    `on_capability_snapshot` is.

    Fire: admitted the same test as a removal (`_has_known_state`), not a
    literal check of `_KEY_TELEMETRY` — this topic carries no sustainment
    either, so a DIS-only launcher admitted solely via Entity State would
    never populate `_KEY_TELEMETRY` and would read as permanently unknown
    under a literal check. An admitted Fire adds `quantity` to
    `expended[munition_key]`, deduped by `event_urn`
    (`fusion.effector_supply.apply_fire`); a Fire whose urn is already
    counted changes nothing and is counted as a replay, never a refusal. A
    counted (non-replay) Fire recomputes and publishes immediately, the
    same as `on_capability_snapshot`, so status reflects it without waiting
    for the next telemetry or timer.

    Detonation has no supply effect — expended is counted at Fire — and is
    never refused; it is only counted as seen."""
    asset_id = ctx.key()
    event = _decode_effector_event(raw)
    if not event:
        return

    pdu_type = event.get("pdu_type")
    if pdu_type == "detonation":
        fusion_effector_detonation_seen_total.inc()
        return
    if pdu_type != "fire":
        return

    if not await _has_known_state(ctx):
        fusion_effector_refused_total.labels(reason="unknown_launcher").inc()
        return

    event_urn = event.get("event_urn")
    if not event_urn:
        return

    munition_key = effector_supply.munition_type_key(event.get("munition_type"))
    quantity = int(event.get("quantity", 0) or 0)

    expended = await ctx.get(_KEY_EFFECTOR_EXPENDED, type_hint=dict) or {}
    counted_urns = await ctx.get(_KEY_EFFECTOR_COUNTED_URNS, type_hint=list) or []

    new_expended, new_counted, was_replay = effector_supply.apply_fire(
        expended, counted_urns,
        event_urn=event_urn, munition_key=munition_key, quantity=quantity,
    )
    if was_replay:
        fusion_effector_replayed_total.inc()
        return

    ctx.set(_KEY_EFFECTOR_EXPENDED, new_expended)
    ctx.set(_KEY_EFFECTOR_COUNTED_URNS, new_counted)
    await _refresh_provenance(ctx, event)
    await _recompute_and_maybe_emit(ctx, asset_id, trigger="effector_event")
    await _schedule_next_timer(ctx, asset_id)


@asset_logistics.handler(
    "on_registry_event",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def on_registry_event(ctx: restate.ObjectContext, raw: bytes) -> None:
    """Consume asset-registry-service's own record for this asset
    (`asset-registry-events`, keyed by `asset_id` — the same key this
    Virtual Object is keyed by). The only field read is
    `platform_variant`; everything else in the payload (edge_id,
    region_id, assignment_source, divergent, ...) belongs to a
    different concern and is ignored here.

    Mirrors `on_capability_snapshot`'s JSON handling, but stores rather
    than drives: a non-empty variant that differs from what's already
    recorded is written to `_KEY_REGISTRY_PLATFORM_VARIANT`; an empty
    or unchanged one is a no-op. Unlike every other handler in this
    module, this one never recomputes, never emits, and never schedules
    a timer — the registry's variant is read lazily, from state, the
    next time `_recompute_and_maybe_emit` runs for any other reason.
    Recording it is not itself news."""
    event = _decode_registry_event(raw)
    if not event:
        return

    variant = event.get("platform_variant") or ""
    if not variant:
        return

    current = await ctx.get(_KEY_REGISTRY_PLATFORM_VARIANT, type_hint=str)
    if variant != current:
        ctx.set(_KEY_REGISTRY_PLATFORM_VARIANT, variant)


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
async def _held_for_other_stack(ctx: restate.ObjectContext) -> bool:
    """One writer per asset at EVERY fusion, from record provenance: true
    when THIS fusion must NOT act (emit, or (re)arm its own timer) for
    this asset because the asset's own ingest belongs to some OTHER node
    that runs its own fusion stack.

    origin(asset) is `_KEY_ORIGIN`'s edge_id -- already kept current by
    `_refresh_origin` for every inbound event (nested provenance.edge_id,
    or top-level edge_id on cm-state), which runs before this guard is
    ever evaluated. No registry, no new header, no bridge change.

    False whenever OTHER_STACK_IDS is empty -- no other node has its own
    stack, so today's behaviour holds and this fusion derives for every
    asset. Otherwise true when origin is unknown/empty (no input has
    decided it yet -- HOLD rather than risk being a second writer for
    the few seconds between a restart and the first input that carries
    provenance) OR origin is one of OTHER_STACK_IDS (that node's own
    fusion is the one writer for this asset; what arrived here crossed a
    bridge and is held, not re-derived).

    Shared by `_recompute_and_maybe_emit` (suppress the publish) and
    `_schedule_next_timer` (end the timer chain instead of re-arming it),
    so the two can never disagree about whether this fusion owns the
    asset. Root and every tier run this same function against their own
    OTHER_STACK_IDS -- same rule everywhere.
    """
    other_stack_ids = _thresholds().other_stack_ids
    if not other_stack_ids:
        return False
    origin = await ctx.get(_KEY_ORIGIN, type_hint=dict) or {}
    edge_id = origin.get("edge_id") or ""
    return (not edge_id) or (edge_id in other_stack_ids)


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
    capability_dict = await ctx.get(_KEY_CAPABILITY, type_hint=dict)

    operational_axes = await ctx.get(_KEY_OPERATIONAL_STATE, type_hint=dict)

    # Phase 5 merge rule: measured wins, derived fills the DIS-asset gap.
    # `_KEY_TELEMETRY` now admits by CONTENT (carries sustainment) rather
    # than by source name, so it is still empty for a DIS-only asset and
    # `_KEY_DERIVED_TELEMETRY` is still the only source of wear for them —
    # the property the old guard protected, protected by the right question.
    # CHOSEN BY WHAT IT CARRIES, not merely by being present.
    #
    # The admission test on the write side stops NEW payload-less records
    # entering `_KEY_TELEMETRY`. It cannot evict one already stored — Restate
    # object state is durable, so a record admitted under yesterday's bug
    # keeps winning this choice forever. Measured: after the write-side fix
    # deployed and was confirmed in the running container, the fleet still
    # reported zero wear factors, because every asset's `_KEY_TELEMETRY` still
    # held the empty-sustainment DIS record from before.
    #
    # So the question is asked again HERE, where the record is used. That
    # makes historical state self-heal and is the better invariant anyway:
    # the chooser should pick the record that can answer the question being
    # asked of it, not the one that arrived on a particular topic.
    chosen_telemetry_dict = (telemetry_dict
                             if _carries_sustainment(telemetry_dict)
                             else derived_telemetry_dict)
    telemetry_proto = (_dict_to_telemetry(chosen_telemetry_dict)
                       if chosen_telemetry_dict else None)

    # UD-13: the operational axes are applied OVER the chosen record, from
    # whichever source last spoke to each one. Without this a DIS-sourced
    # fault reached the read model (the projector writes it from the same
    # Silver message) and never reached severity — three signals said the
    # path was live and it was not.
    if telemetry_proto is not None and operational_axes:
        _apply_operational_axes(telemetry_proto, operational_axes)
    windows_proto = _dict_to_windows(windows_dict) if windows_dict else None
    cm_proto = _dict_to_cm_state(cm_dict) if cm_dict else None

    # Registry first: asset_registry RECORDS platform_variant (ADR-0028),
    # so once it has one it's the asset's answer, not a per-event guess.
    # telemetry/windows are what's left for a DIS-only asset the registry
    # hasn't recorded a variant for yet -- whichever event happened to
    # carry it, which is why they're the fallback and not the source.
    registry_variant = await ctx.get(_KEY_REGISTRY_PLATFORM_VARIANT, type_hint=str)
    platform_variant = ""
    if registry_variant:
        platform_variant = registry_variant
    elif telemetry_proto is not None and telemetry_proto.asset.platform_variant:
        platform_variant = telemetry_proto.asset.platform_variant
    elif windows_proto is not None and windows_proto.platform_variant:
        platform_variant = windows_proto.platform_variant

    effector_expended = await ctx.get(_KEY_EFFECTOR_EXPENDED, type_hint=dict) or {}
    effector_declared = effector_supply.resolve_declared(
        _declared_load_table(), asset_id=asset_id, platform_variant=platform_variant,
    )

    inputs = FusionInputs(
        asset_id=asset_id,
        platform_variant=platform_variant,
        latest_telemetry=telemetry_proto,
        telemetry_windows=windows_proto,
        cm_state=cm_proto,
        capability_snapshot=capability_dict or None,
        effector_expended=effector_expended,
        effector_declared=effector_declared,
    )
    now_ns = _now_ns(ctx)
    status = compute_logistics_status(inputs, _thresholds(), now_ns)

    prev_sev = await ctx.get(_KEY_LAST_SEVERITY, type_hint=int)
    is_initial = prev_sev is None
    is_transition = (not is_initial) and prev_sev != status.overall_severity

    if not (is_initial or is_transition or force_emit):
        return  # quiet update, no emission

    # One writer per asset, from record provenance (see
    # `_held_for_other_stack`): this would otherwise be a publish, so
    # suppress it here rather than earlier -- a quiet update above already
    # returns without touching anything, and counting THOSE as suppressed
    # would overcount a metric meant to answer "how often would this
    # fusion have been a second writer."
    if await _held_for_other_stack(ctx):
        fusion_publish_suppressed_other_stack_total.inc()
        return  # no publish, no revision/last-severity change

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

    # ADR-0029 §3: PROPAGATE, do not derive. Fusion has no releasability
    # declaration and must never acquire one — the label is stamped once at
    # ingress and carried. What fusion contributes is the join: it knows
    # which asset this derived row is about, so it knows which labels the row
    # inherits.
    #
    # No else-branch and no default. An asset whose telemetry never carried a
    # label emits a derived row with no label, which the §7 gate then counts.
    # That is the correct outcome: the fix belongs at the ingress that failed
    # to declare the asset, not here, and inventing a value here would hide
    # the very thing the gate exists to surface.
    rel = await ctx.get(_KEY_RELEASABILITY, type_hint=dict) or {}
    if rel.get("originator_nation"):
        update.provenance.originator_nation = rel["originator_nation"]
        update.provenance.releasable_to.extend(rel.get("releasable_to") or [])

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

    # Maintainer attention flow (2026-06-30): on TRANSITION into
    # CRITICAL or NON_OPERATIONAL, emit a CloudEvent to tactical-events
    # so the maintainer view's AlertFeed surfaces the change even if
    # the operator's focus is on a different asset. The projector's
    # tactical_events handler decodes JSON CloudEvents (not protobuf)
    # and writes to the tactical_events table; ElectricSQL streams
    # the row to the frontend, where AlertFeed renders cross-asset
    # rows with a chevron + click-to-switch affordance. Only fires
    # on the UPWARD transition into the alerting tiers -- a recovery
    # transition out of CRITICAL doesn't need an alert (the posture
    # pill and 3D tiles already reflect it). Initial emissions don't
    # fire either; only confirmed transitions from a lower severity.
    if is_transition and status.overall_severity in _ALERTING_SEVERITIES:
        await _publish_tactical_event(
            ctx,
            asset_id=asset_id,
            new_severity=status.overall_severity,
            prev_severity=prev_sev or ls.LOGISTICS_SEVERITY_UNSPECIFIED,
            revision=revision,
            # THE ASSET'S LABELS TRAVEL WITH THE EVENT. Read from the same
            # state key that stamps asset_logistics_status, so the alert and
            # the status row cannot disagree about who may see this asset.
            # Carried, never derived — the projector may not invent them and
            # neither may this.
            releasability=(await ctx.get(_KEY_RELEASABILITY)) or {},
            constraining_factors=list(status.constraining_factors),
            origin=origin,
        )

    logger.info(
        "Emitted %s for %s: %s -> %s (rev=%d, trigger=%s, factors=%d)",
        "initial" if is_initial else ("transition" if is_transition else "cadenced"),
        asset_id,
        _sev_name(prev_sev or ls.LOGISTICS_SEVERITY_UNSPECIFIED),
        _sev_name(status.overall_severity),
        revision, trigger, len(status.constraining_factors),
    )


# Severity tiers that warrant a maintainer-facing tactical event. Tracking
# only the upward transitions (OK/UNSPECIFIED/DEGRADED -> CRITICAL or
# NON_OPERATIONAL); recovery transitions are visible through the existing
# asset-logistics-status path without needing an alert row.
_ALERTING_SEVERITIES = frozenset({
    ls.LOGISTICS_SEVERITY_CRITICAL,
    ls.LOGISTICS_SEVERITY_NON_OPERATIONAL,
})


async def _publish_tactical_event(
    ctx: restate.ObjectContext,
    *,
    asset_id: str,
    new_severity: int,
    prev_severity: int,
    revision: int,
    constraining_factors: list,
    origin: dict,
    releasability: dict | None = None,
) -> None:
    """Build + publish a JSON CloudEvent to the tactical-events topic.

    Shape matches what the projector's tactical_events handler expects:
    a top-level dict with `id`, `source`, `type`, `subject`, `time`,
    `data` keys. `severity` is nested in `data` so the projector's
    _extract_severity helper picks it up via the `severity` priority
    key. Provenance (edge_id, region_id) is stamped into `data` too --
    the handler reads them via resolve_provenance_from_top_level for
    per-asset row attribution. `originator_nation` /
    `releasable_to` ride in `data` beside them, from this object's own
    state -- the alert must not be visible to anyone the asset is not. Top constraining factor (if any) lands
    in the description-ish slot so the AlertFeed row reads as
    "logistics.transition --- thermal_overload_imminent" rather than
    just "logistics.transition" with the operator wondering what
    transitioned why.

    The publish is wrapped in ctx.run so Restate journals it -- a
    crash mid-publish replays into the same idempotent write. uuid4
    for event_id; Restate replays cache the result on retry.
    """
    event_id = str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).isoformat()
    top_factor = ""
    if constraining_factors:
        # Best-effort -- the proto has factor.code / factor.detail. Pick
        # the first; severity-ordering already happened upstream.
        f = constraining_factors[0]
        top_factor = getattr(f, "code", "") or getattr(f, "factor_code", "") or ""

    sev_name = _sev_name(new_severity)
    prev_sev_name = _sev_name(prev_severity)

    envelope: dict[str, Any] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "openddil/logistics-fusion-service",
        "type": "openddil.logistics.v1.severity-transition",
        "subject": asset_id,
        "time": event_time,
        "datacontenttype": "application/json",
        "data": {
            "severity": sev_name,
            "previous_severity": prev_sev_name,
            "status_revision": revision,
            "top_factor": top_factor,
            "edge_id": origin.get("edge_id", ""),
            "region_id": origin.get("region_id", ""),
            # ADR-0029 §3. A tactical event is ABOUT an asset, so it is as
            # releasable as that asset and no more. Emitted only when the
            # labels are known: an absent key means unlabelled, which
            # deny-unlabeled reads as releasable to nobody. Emitting empty
            # values instead would be a label asserting "no nation", which
            # is a claim rather than a silence.
            **{k: v for k, v in (releasability or {}).items() if v},
        },
    }
    payload = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    await ctx.run(
        "publish-tactical-event",
        lambda: _publish_kafka(
            topic="tactical-events",
            key=asset_id,
            value=payload,
        ),
    )
    logger.info(
        "Tactical event emitted for %s: %s -> %s (rev=%d, factor=%s)",
        asset_id, prev_sev_name, sev_name, revision, top_factor or "<none>",
    )


async def _schedule_next_timer(ctx: restate.ObjectContext, asset_id: str) -> None:
    """Schedule the next on_timer firing EMIT_INTERVAL_SECONDS from now.

    Debounce: if a timer is already scheduled within the cadence window,
    don't double-schedule (avoids piling up timer events for chatty assets).

    One writer per asset, from record provenance (see
    `_held_for_other_stack`): for an asset whose origin is another node's
    own stack -- or whose origin isn't decided yet -- the timer chain
    ENDS here instead of re-arming. Re-arming this fusion's own cadence
    for an asset it must not derive for is exactly the race that
    recreates the second writer a few seconds after every restart, just
    on a longer period.
    """
    if await _held_for_other_stack(ctx):
        return  # no timer chain for an asset this fusion does not own

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


def _decode_capability_snapshot(raw: bytes | dict | None) -> dict:
    """asset-capability-snapshot is a plain-JSON Silver shape — the
    source-specific weapons-capability Bloblang produces JSON, not proto. No proto
    fallback (unlike cm-state): this topic is JSON-only by contract."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        text = (raw.decode("utf-8")
                if isinstance(raw, (bytes, bytearray)) else str(raw))
        decoded = json.loads(text)
        return decoded if isinstance(decoded, dict) else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Failed to decode capability snapshot (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


def _decode_effector_event(raw: bytes | dict | None) -> dict:
    """`effector-events` is JSON-only, same contract as
    `asset-capability-snapshot` — no proto fallback."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        text = (raw.decode("utf-8")
                if isinstance(raw, (bytes, bytearray)) else str(raw))
        decoded = json.loads(text)
        return decoded if isinstance(decoded, dict) else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Failed to decode effector event (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


def _decode_registry_event(raw: bytes | dict | None) -> dict:
    """`asset-registry-events` is JSON-only, same contract as
    `asset-capability-snapshot` and `effector-events` — no proto fallback."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        text = (raw.decode("utf-8")
                if isinstance(raw, (bytes, bytearray)) else str(raw))
        decoded = json.loads(text)
        return decoded if isinstance(decoded, dict) else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Failed to decode registry event (len=%d): %s",
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
