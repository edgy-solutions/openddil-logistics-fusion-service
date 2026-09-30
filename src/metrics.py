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
