"""
Logistics-fusion thresholds. Env-driven, no separate YAML schema.

Each threshold has a sensible default baked in here; deployment overrides
via env vars. The fusion engine reads `Thresholds.from_env()` once at
service start.

Subsystem-health-string → severity mapping is also here, with a JSON env
override (SUBSYSTEM_HEALTH_MAP) for deployments whose proprietary feed uses
non-standard tokens.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

from openddil.logistics.v1 import logistics_status_pb2 as ls

logger = logging.getLogger("fusion.thresholds")


@dataclass(frozen=True)
class Thresholds:
    # Fuel — % of capacity; CRITICAL is the more severe band.
    fuel_pct_critical: float = 15.0
    fuel_pct_degraded: float = 30.0

    # Ammunition — % of capacity per slot.
    ammo_pct_critical: float = 10.0
    ammo_pct_degraded: float = 25.0

    # Engagement-worthiness (Sub-phase F) — the customer capability
    # snapshot reports an absolute Ammo count per loaded store, not a
    # percent (the feed carries no per-store capacity). A store at or
    # below this count is AMMO_LOW (DEGRADED); a store at zero is
    # AMMO_EXHAUSTED (CRITICAL).
    ammo_low_count: int = 5

    # Component wear — % of useful-life consumed.
    wear_pct_critical: float = 90.0
    wear_pct_degraded: float = 75.0

    # Predicted hours until next maintenance event.
    mtbf_hours_critical: float = 2.0
    mtbf_hours_degraded: float = 8.0

    # Restate workflow cadence + staleness.
    emit_interval_seconds: int = 30
    stale_input_seconds: int = 300

    # Subsystem health string → LogisticsSeverity. Values are the
    # LogisticsSeverity enum *int* values (not names) so the dict is
    # framework-free.
    subsystem_health_map: dict[str, int] = field(default_factory=dict)

    # One writer per asset at EVERY fusion, from record provenance: the ids
    # of every OTHER node that runs its own fusion stack. This fusion must
    # not be a second writer for an asset whose origin (see `_KEY_ORIGIN` /
    # `_refresh_origin`) is one of them — see `workflows.asset_logistics`'s
    # module docstring and `_recompute_and_maybe_emit` / `_schedule_next_
    # timer`. Empty means no other stacks exist yet, so this fusion derives
    # for every asset (today's behaviour).
    other_stack_ids: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_env(cls) -> Thresholds:
        # subsystem_health_map: JSON object string in SUBSYSTEM_HEALTH_MAP env
        # var. Values are LogisticsSeverity enum names; we look up the int.
        raw = os.getenv("SUBSYSTEM_HEALTH_MAP")
        if raw:
            try:
                parsed: dict[str, str] = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "SUBSYSTEM_HEALTH_MAP env var is not valid JSON; "
                    "falling back to defaults (%s)", exc,
                )
                parsed = {}
        else:
            parsed = {}

        if not parsed:
            # Sensible default tokens. Real customer feeds may have richer
            # vocabularies; override via SUBSYSTEM_HEALTH_MAP.
            parsed = {
                "OPERATIONAL":  "LOGISTICS_SEVERITY_OK",
                "NOMINAL":      "LOGISTICS_SEVERITY_OK",
                "DEGRADED":     "LOGISTICS_SEVERITY_DEGRADED",
                "WARN":         "LOGISTICS_SEVERITY_DEGRADED",
                "WARNING":      "LOGISTICS_SEVERITY_DEGRADED",
                "FAULT":        "LOGISTICS_SEVERITY_CRITICAL",
                "CRITICAL":     "LOGISTICS_SEVERITY_CRITICAL",
                "INOPERATIVE":  "LOGISTICS_SEVERITY_NON_OPERATIONAL",
                "FAILED":       "LOGISTICS_SEVERITY_NON_OPERATIONAL",
            }

        # Resolve enum names to ints; ignore unknown names with a warning.
        sev_map: dict[str, int] = {}
        for token, sev_name in parsed.items():
            try:
                sev_map[token.upper()] = ls.LogisticsSeverity.Value(sev_name)
            except ValueError:
                logger.warning(
                    "SUBSYSTEM_HEALTH_MAP value %r for token %r is not a "
                    "known LogisticsSeverity; dropping", sev_name, token,
                )

        # OTHER_STACK_IDS: comma-separated node ids, default empty (same
        # one-parse-at-config-load discipline as every other field here).
        other_stack_ids = frozenset(
            t.strip() for t in os.getenv("OTHER_STACK_IDS", "").split(",")
            if t.strip()
        )

        return cls(
            fuel_pct_critical    = float(os.getenv("FUEL_PCT_CRITICAL",    "15")),
            fuel_pct_degraded    = float(os.getenv("FUEL_PCT_DEGRADED",    "30")),
            ammo_pct_critical    = float(os.getenv("AMMO_PCT_CRITICAL",    "10")),
            ammo_pct_degraded    = float(os.getenv("AMMO_PCT_DEGRADED",    "25")),
            ammo_low_count       = int(os.getenv("AMMO_LOW_COUNT",        "5")),
            wear_pct_critical    = float(os.getenv("WEAR_PCT_CRITICAL",    "90")),
            wear_pct_degraded    = float(os.getenv("WEAR_PCT_DEGRADED",    "75")),
            mtbf_hours_critical  = float(os.getenv("MTBF_HOURS_CRITICAL",  "2")),
            mtbf_hours_degraded  = float(os.getenv("MTBF_HOURS_DEGRADED",  "8")),
            emit_interval_seconds = int(os.getenv("EMIT_INTERVAL_SECONDS",  "30")),
            stale_input_seconds  = int(os.getenv("STALE_INPUT_SECONDS",   "300")),
            subsystem_health_map = sev_map,
            other_stack_ids      = other_stack_ids,
        )
