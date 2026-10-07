"""Prometheus counters for the logistics-fusion service.

Kept as its own module (rather than inline in `workflows/asset_logistics.py`)
so the metric can be imported by the workflow while the HTTP exposition
server (`prometheus_client.start_http_server`) is started once, by
`main.py`, at process startup — one owner for the endpoint, any number of
importers for the counters it serves.
"""
from __future__ import annotations

from prometheus_client import Counter

# Counts HANDLER INVOCATIONS that took the unknown-asset-removal drop path,
# not distinct Remove Entity PDUs: a Restate retry re-runs
# `on_proprietary_update` from the top, so a retried invocation increments
# this again for what is, from the topic's point of view, the same record.
removal_unknown_asset_dropped_total = Counter(
    "logistics_removal_unknown_asset_dropped_total",
    "Remove Entity claims for an asset_id with no AssetLogistics state, dropped",
)

# effector-events (Fire/Detonation) handling, AssetLogistics.on_effector_event.
# Labeled by reason so "unknown_launcher" (Fire, no prior AssetLogistics
# state) is distinguishable from any other future refusal reason.
fusion_effector_refused_total = Counter(
    "fusion_effector_refused_total",
    "Fire/Detonation events refused by AssetLogistics.on_effector_event",
    ["reason"],
)

# A Fire whose event_urn was already counted -- changes nothing, counted
# here rather than as a refusal (it is not malformed or unadmitted, it is
# the same record arriving again).
fusion_effector_replayed_total = Counter(
    "fusion_effector_replayed_total",
    "Fire events whose event_urn was already counted (no state change)",
)

# Detonation has no supply effect (expended is counted at Fire) and is
# never refused -- this counts that it was seen, nothing more.
fusion_effector_detonation_seen_total = Counter(
    "fusion_effector_detonation_seen_total",
    "Detonation events observed by AssetLogistics.on_effector_event",
)

# One writer per asset at EVERY fusion, from record provenance:
# `_recompute_and_maybe_emit` / `_schedule_next_timer` suppress this
# fusion's own publish + timer re-arm for an asset whose origin edge_id
# (see `_KEY_ORIGIN` / `_refresh_origin`) is an OTHER node's own stack (or
# not yet known, while OTHER_STACK_IDS is non-empty) -- that node is the
# writer for that asset, or origin hasn't been decided yet. Counts each
# suppressed publish, i.e. each recompute that would otherwise have
# emitted.
fusion_publish_suppressed_other_stack_total = Counter(
    "fusion_publish_suppressed_other_stack_total",
    "Recomputes that would have published but were suppressed because the "
    "asset's origin is another node's own fusion stack, or its origin is "
    "not yet known",
)
