"""
Pure-Python logistics fusion rules.

No Faust imports, no Restate imports, no Kafka imports, no RabbitMQ imports.
Inputs:
  - FusionInputs (latest telemetry + windowed trends + CM state + metadata)
  - Thresholds  (env-driven dataclass)
  - now_ns      (monotonic clock — passed in so this stays a pure function)

Output:
  - openddil.logistics.v1.AssetLogisticsStatus

Each evaluator (`_eval_*`) is independently unit-testable and returns
`ConstrainingFactor | None`. The overall severity is the max across all
factors.

Per ADR-0006: all framework integration (Faust streaming, Restate workflow,
Kafka producer) lives outside this module. This file imports only Protobuf
generated code and pint.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Iterable

from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2
from pint import UnitRegistry, Quantity as PQ

from openddil.common.v1 import quantity_pb2 as qpb
from openddil.logistics.v1 import logistics_status_pb2 as ls
from openddil.telemetry.v1 import telemetry_pb2 as tel
from openddil.logistics.v1 import windowed_telemetry_pb2 as win
from openddil.configuration.v1 import as_maintained_pb2 as cm
from openddil.configuration.v1 import discrepancy_pb2 as disc

from fusion.thresholds import Thresholds

logger = logging.getLogger("fusion.rules")

# Single pint registry. UCUM-to-pint dialect shims kept in sync with
# openddil-tactical-agents/edge/detection/units.py.
ureg = UnitRegistry()
ureg.default_format = "~"
_UCUM_TO_PINT = {
    "[degF]": "degF",
    "[kn_i]": "knot",
    "gal_us": "gallon",
    "1": "dimensionless",
}


# ---------------------------------------------------------------------------
# Fuel-capacity lookup table
#
# Source of truth: openddil-contracts/ontology/platform_reference.yaml.
#   Domain-authoritative reference data (the M1A2 SEPv3 fuel capacity is
#   a property of the platform itself, not a deployment choice). Curated
#   under PR review by domain experts. Same pattern as
#   asset_identity_aliases.yaml.
#
# Deployment escape hatch (operational tuning only):
#   FUEL_CAPACITY_BY_VARIANT env var, JSON-encoded
#   {"VARIANT": {"value": 500, "unit": "gal_us"}}. When set, OVERRIDES
#   the matching ontology entries; non-overridden variants still come
#   from the ontology. Use sparingly — operations should generally fix
#   the ontology, not the env.
# ---------------------------------------------------------------------------
ONTOLOGY_DIR = os.getenv("ONTOLOGY_DIR", "/ontology")


def _load_fuel_capacity_table() -> dict[str, PQ]:
    """Merge platform_reference.yaml + FUEL_CAPACITY_BY_VARIANT env override.

    Ontology is the base; env wins for any variant present in both.
    """
    out: dict[str, PQ] = {}

    # 1. Base: ontology file.
    try:
        import yaml  # type: ignore
        path = os.path.join(ONTOLOGY_DIR, "platform_reference.yaml")
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        platforms = doc.get("platforms") or {}
        for variant, spec in platforms.items():
            cap = (spec or {}).get("fuel_capacity")
            if not cap:
                continue
            try:
                v = float(cap["value"])
                u = _UCUM_TO_PINT.get(cap["unit"], cap["unit"])
                out[variant] = ureg.Quantity(v, u)
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "platform_reference.yaml entry for %r malformed: %s; skipping",
                    variant, exc,
                )
    except FileNotFoundError:
        logger.warning(
            "platform_reference.yaml not found at %s — fuel%% evaluation "
            "will rely solely on FUEL_CAPACITY_BY_VARIANT env (if set).",
            ONTOLOGY_DIR,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed loading platform_reference.yaml: %s", exc)

    # 2. Override: env var.
    import json
    raw = os.getenv("FUEL_CAPACITY_BY_VARIANT")
    if raw:
        try:
            parsed: dict[str, dict] = json.loads(raw)
            for variant, spec in parsed.items():
                try:
                    v = float(spec["value"])
                    u = _UCUM_TO_PINT.get(spec["unit"], spec["unit"])
                    out[variant] = ureg.Quantity(v, u)
                except (KeyError, TypeError, ValueError) as exc:
                    logger.warning(
                        "FUEL_CAPACITY_BY_VARIANT entry for %r malformed: %s; skipping",
                        variant, exc,
                    )
        except json.JSONDecodeError as exc:
            logger.warning(
                "FUEL_CAPACITY_BY_VARIANT not valid JSON; ignoring (%s)", exc,
            )
    return out


_FUEL_CAPACITY: dict[str, PQ] = _load_fuel_capacity_table()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _quantity_to_pint(q: qpb.Quantity) -> PQ | None:
    if q is None or (q.value == 0.0 and not q.unit):
        return None
    unit = _UCUM_TO_PINT.get(q.unit, q.unit)
    try:
        return ureg.Quantity(q.value, unit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not parse Quantity(value=%s, unit=%s): %s",
                        q.value, q.unit, exc)
        return None


def _ucum_quantity(value: float, ucum_unit: str) -> qpb.Quantity:
    return qpb.Quantity(value=value, unit=ucum_unit)


def _duration_from_hours(hours: float) -> duration_pb2.Duration:
    d = duration_pb2.Duration()
    d.FromSeconds(max(0, int(hours * 3600)))
    return d


def _max_severity(factors: Iterable[ls.ConstrainingFactor]) -> int:
    items = list(factors)
    if not items:
        return ls.LOGISTICS_SEVERITY_OK
    return max(f.severity for f in items)


# ---------------------------------------------------------------------------
# Public input container
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FusionInputs:
    asset_id: str
    platform_variant: str
    latest_telemetry: tel.EntityTelemetryEvent | None
    telemetry_windows: win.WindowedTelemetry | None
    cm_state: cm.AsMaintainedConfiguration | None
    # Sub-phase F: the customer-overlay capability snapshot (Silver
    # `asset-capability-snapshot` — a JSON dict, not a proto). Optional and
    # last so every existing positional construction site keeps working.
    capability_snapshot: dict | None = None


# ---------------------------------------------------------------------------
# Evaluators — pure functions, ConstrainingFactor | None
# ---------------------------------------------------------------------------
def _eval_fuel(inputs: FusionInputs,
                thresholds: Thresholds) -> ls.ConstrainingFactor | None:
    if inputs.latest_telemetry is None:
        return None
    fuel = inputs.latest_telemetry.sustainment.fluids.fuel_remaining
    if not fuel.unit and fuel.value == 0.0:
        return None  # field unset

    if fuel.unit == "%":
        pct = float(fuel.value)
        current_qty = _ucum_quantity(pct, "%")
    else:
        # Convert via pint to a common unit, then ratio against the
        # platform's fuel capacity.
        capacity = _FUEL_CAPACITY.get(inputs.platform_variant)
        fuel_pq = _quantity_to_pint(fuel)
        if capacity is None or fuel_pq is None:
            logger.debug(
                "Fuel evaluation skipped: variant=%s capacity=%s fuel=%s/%s",
                inputs.platform_variant, capacity, fuel.value, fuel.unit,
            )
            return None
        try:
            ratio = fuel_pq.to(capacity.units) / capacity
            pct = float(ratio.magnitude) * 100.0
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fuel unit conversion failed: %s", exc)
            return None
        current_qty = _ucum_quantity(pct, "%")

    if pct <= thresholds.fuel_pct_critical:
        sev = ls.LOGISTICS_SEVERITY_CRITICAL
        threshold = thresholds.fuel_pct_critical
    elif pct <= thresholds.fuel_pct_degraded:
        sev = ls.LOGISTICS_SEVERITY_DEGRADED
        threshold = thresholds.fuel_pct_degraded
    else:
        return None

    return ls.ConstrainingFactor(
        factor_id="fuel",
        severity=sev,
        description=f"Fuel at {pct:.1f}% (threshold {threshold:.0f}%)",
        current_value=current_qty,
        threshold=_ucum_quantity(threshold, "%"),
    )


def _eval_ammo(inputs: FusionInputs,
                thresholds: Thresholds) -> list[ls.ConstrainingFactor]:
    """Returns one factor per slot below threshold (may be empty)."""
    if inputs.latest_telemetry is None:
        return []
    factors: list[ls.ConstrainingFactor] = []
    items = inputs.latest_telemetry.sustainment.consumables.items
    for slot, state in items.items():
        if state.quantity_capacity == 0:
            continue
        pct = state.quantity_remaining * 100.0 / state.quantity_capacity
        if pct <= thresholds.ammo_pct_critical:
            sev = ls.LOGISTICS_SEVERITY_CRITICAL
            threshold = thresholds.ammo_pct_critical
        elif pct <= thresholds.ammo_pct_degraded:
            sev = ls.LOGISTICS_SEVERITY_DEGRADED
            threshold = thresholds.ammo_pct_degraded
        else:
            continue
        factors.append(ls.ConstrainingFactor(
            factor_id=f"ammo.{slot}",
            severity=sev,
            description=(
                f"Ammunition {slot} at {pct:.1f}% "
                f"({state.quantity_remaining}/{state.quantity_capacity})"
            ),
            current_value=_ucum_quantity(pct, "%"),
            threshold=_ucum_quantity(threshold, "%"),
        ))
    return factors


def _factor_origin(sustainment: tel.SustainmentMetrics) -> int:
    """Read the Phase 5 message-level provenance wildcard, default UNSPECIFIED.

    ADR-0020: derived sustainment carries `value_provenance["*"]` with
    `origin = ORIGIN_DERIVED`. Sustainment from measured paths has no such
    entry today; reading it would auto-create one on the proto map (Python
    proto3 quirk), so guard with explicit `in` check."""
    if "*" in sustainment.value_provenance:
        return sustainment.value_provenance["*"].origin
    return tel.ORIGIN_UNSPECIFIED


def _eval_wear(inputs: FusionInputs,
                thresholds: Thresholds) -> list[ls.ConstrainingFactor]:
    """One factor per component whose wear% crosses a threshold.

    Phase 5 step 2: handles the engine's emit contract — `hours_in_service`
    / `cycles` in natural units, `remaining_useful_life` in `"%"`. The two
    branches below cover (a) the legacy time-units shape that measured
    feeds emit and (b) the percent-shape that the prognostics engine
    emits. Provenance is read from the sustainment's value_provenance
    wildcard and stamped on each emitted factor."""
    if inputs.latest_telemetry is None:
        return []
    sustainment = inputs.latest_telemetry.sustainment
    origin = _factor_origin(sustainment)
    factors: list[ls.ConstrainingFactor] = []
    components = sustainment.wear.components
    for name, state in components.items():
        rul = _quantity_to_pint(state.remaining_useful_life)
        if rul is None:
            continue

        # Branch on rul unit: percent (engine-emit contract) vs time
        # (legacy measured-emit contract).
        if state.remaining_useful_life.unit == "%":
            # Engine contract: remaining_useful_life is percent-remaining
            # directly. pct_consumed = 100 - rul. No need to read
            # hours_in_service for the ratio (it's a natural-unit raw
            # measure, not part of the ratio).
            pct = max(0.0, 100.0 - float(state.remaining_useful_life.value))
        else:
            # Legacy: hours_in_service + rul both in time units, ratio.
            hours = _quantity_to_pint(state.hours_in_service)
            if hours is None:
                continue
            try:
                hours_h = hours.to("h").magnitude
                rul_h = rul.to("h").magnitude
            except Exception as exc:  # noqa: BLE001
                logger.warning("Wear unit conversion failed for %s: %s", name, exc)
                continue
            total = hours_h + rul_h
            if total <= 0:
                continue
            pct = hours_h * 100.0 / total

        if pct >= thresholds.wear_pct_critical:
            sev = ls.LOGISTICS_SEVERITY_CRITICAL
            threshold = thresholds.wear_pct_critical
        elif pct >= thresholds.wear_pct_degraded:
            sev = ls.LOGISTICS_SEVERITY_DEGRADED
            threshold = thresholds.wear_pct_degraded
        else:
            continue

        # Description string stays clean — provenance is structural.
        natural_unit = state.hours_in_service.unit or state.cycles.unit or ""
        natural_value = (state.hours_in_service.value
                         if state.hours_in_service.unit else state.cycles.value)
        factors.append(ls.ConstrainingFactor(
            factor_id=f"wear.{name}",
            severity=sev,
            description=(
                f"Component {name} wear at {pct:.1f}% "
                f"({natural_value:.1f}{natural_unit} in service)"
            ),
            current_value=_ucum_quantity(pct, "%"),
            threshold=_ucum_quantity(threshold, "%"),
            origin=origin,
            # Phase 5: confidence is meaningfully 0.0 for DERIVED until the
            # validation phase. For UNSPECIFIED-origin factors the field is
            # proto3-default 0.0 anyway. See ADR-0020.
            confidence=0.0,
        ))
    return factors


def _eval_inventory(inputs: FusionInputs,
                     thresholds: Thresholds) -> list[ls.ConstrainingFactor]:
    """Engagement-worthiness from the customer capability snapshot.

    Sub-phase F. The customer's StrikeCapabilityMessage feed (Silver topic
    `asset-capability-snapshot`) carries the current Ammo count for every
    loaded store on an asset. A store at zero Ammo cannot engage
    (AMMO_EXHAUSTED → CRITICAL); a store running low is a DEGRADED warning
    (AMMO_LOW). The snapshot reports an absolute count, not a percent — the
    feed has no per-store capacity — so this evaluator bands on
    `thresholds.ammo_low_count` rather than a percentage. (Distinct from
    `_eval_ammo`, which bands on percent-of-capacity from the *telemetry*
    sustainment block; the two never see the same asset in practice.)

    Both factors are stamped `origin = ORIGIN_DERIVED`, the same contract
    as `_eval_wear`: the Ammo count is a customer-fed measurement, but the
    *engagement-worthiness assessment* ("can this asset still engage?") is
    an OpenDDIL-derived conclusion. `confidence` stays 0.0 — no oracle,
    per ADR-0020."""
    snapshot = inputs.capability_snapshot
    if not snapshot:
        return []
    factors: list[ls.ConstrainingFactor] = []
    for store in snapshot.get("capabilities") or []:
        if not isinstance(store, dict):
            continue
        ammo = store.get("ammo")
        if not isinstance(ammo, (int, float)):
            continue  # field absent or non-numeric — no claim
        ammo = int(ammo)
        cap_id = store.get("capability_id") or ""
        store_loc = store.get("store_location")
        cap_key = cap_id or (f"store-{store_loc}" if store_loc is not None
                             else "store")

        if ammo <= 0:
            sev = ls.LOGISTICS_SEVERITY_CRITICAL
            state = "AMMO_EXHAUSTED"
            threshold = 0
        elif ammo <= thresholds.ammo_low_count:
            sev = ls.LOGISTICS_SEVERITY_DEGRADED
            state = "AMMO_LOW"
            threshold = thresholds.ammo_low_count
        else:
            continue

        factors.append(ls.ConstrainingFactor(
            factor_id=f"inventory.{cap_key}",
            severity=sev,
            description=(
                f"{state}: {cap_key} has {ammo} round(s) loaded "
                f"(store {store_loc})"
            ),
            current_value=_ucum_quantity(float(ammo), "{round}"),
            threshold=_ucum_quantity(float(threshold), "{round}"),
            origin=tel.ORIGIN_DERIVED,
            confidence=0.0,
        ))
    return factors


def _eval_mtbf(inputs: FusionInputs,
                thresholds: Thresholds) -> ls.ConstrainingFactor | None:
    """Soonest projected component failure from windowed RUL slopes.

    For each ComponentWearTrend in the windows, if remaining_useful_life is
    dropping (slope < 0), project hours-to-zero from the latest RUL value.
    Take the minimum across all components.
    """
    if inputs.telemetry_windows is None:
        return None
    soonest_h: float | None = None
    soonest_component: str | None = None
    for trend in inputs.telemetry_windows.wear_trends:
        latest = _quantity_to_pint(trend.remaining_useful_life.latest)
        slope  = _quantity_to_pint(trend.remaining_useful_life.slope)
        if latest is None or slope is None:
            continue
        try:
            latest_h = latest.to("h").magnitude
            slope_h_per_h = slope.to("h/h").magnitude  # rate of life loss
        except Exception:  # noqa: BLE001
            continue
        if slope_h_per_h >= 0:
            # Life is stable or improving; not a constraint.
            continue
        hours_to_zero = latest_h / (-slope_h_per_h)
        if soonest_h is None or hours_to_zero < soonest_h:
            soonest_h = hours_to_zero
            soonest_component = trend.component_key

    if soonest_h is None or soonest_component is None:
        return None

    if soonest_h <= thresholds.mtbf_hours_critical:
        sev = ls.LOGISTICS_SEVERITY_CRITICAL
        threshold = thresholds.mtbf_hours_critical
    elif soonest_h <= thresholds.mtbf_hours_degraded:
        sev = ls.LOGISTICS_SEVERITY_DEGRADED
        threshold = thresholds.mtbf_hours_degraded
    else:
        return None

    return ls.ConstrainingFactor(
        factor_id=f"mtbf.{soonest_component}",
        severity=sev,
        description=(
            f"Projected {soonest_component} time-to-failure {soonest_h:.1f}h "
            f"(threshold {threshold:.0f}h)"
        ),
        current_value=_ucum_quantity(soonest_h, "h"),
        threshold=_ucum_quantity(threshold, "h"),
        projected_time_to_worse=_duration_from_hours(soonest_h),
    )


def _eval_subsystems(inputs: FusionInputs,
                      thresholds: Thresholds) -> list[ls.ConstrainingFactor]:
    """One factor per active subsystem fault that maps to >= DEGRADED."""
    if inputs.latest_telemetry is None:
        return []
    factors: list[ls.ConstrainingFactor] = []
    for code in inputs.latest_telemetry.sustainment.health.active_fault_codes:
        # Format expected: "SUBSYS:HEALTH" (e.g., "POWERPLANT:DEGRADED").
        if ":" in code:
            subsys, health = code.split(":", 1)
        else:
            subsys, health = "unknown", code
        sev = thresholds.subsystem_health_map.get(
            health.strip().upper(), ls.LOGISTICS_SEVERITY_UNSPECIFIED,
        )
        if sev in (ls.LOGISTICS_SEVERITY_UNSPECIFIED,
                    ls.LOGISTICS_SEVERITY_OK):
            continue
        factors.append(ls.ConstrainingFactor(
            factor_id=f"subsystem.{subsys.strip()}",
            severity=sev,
            description=f"Subsystem {subsys.strip()} reports {health.strip()}",
        ))
    return factors


def _eval_cm_state(inputs: FusionInputs,
                    thresholds: Thresholds) -> ls.ConstrainingFactor | None:
    if inputs.cm_state is None:
        return None
    overall = inputs.cm_state.overall_status
    # Map ConfigurationStatus → LogisticsSeverity.
    # Values come from openddil.configuration.v1.ConfigurationStatus.
    # Re-derive numeric values via enum lookup so a renumbering in the proto
    # surfaces here rather than silently mis-mapping.
    cs = inputs.cm_state.DESCRIPTOR.fields_by_name["overall_status"].enum_type
    name_to_sev: dict[str, int | None] = {
        "CONFIG_STATUS_UNSPECIFIED":          None,
        "CONFIG_STATUS_IN_COMPLIANCE":        None,
        "CONFIG_STATUS_MINOR_DISCREPANCY":    None,  # advisory; not a constraint
        "CONFIG_STATUS_MAJOR_DISCREPANCY":    ls.LOGISTICS_SEVERITY_DEGRADED,
        "CONFIG_STATUS_NOT_MISSION_CAPABLE":  ls.LOGISTICS_SEVERITY_NON_OPERATIONAL,
    }
    try:
        status_name = cs.values_by_number[overall].name
    except KeyError:
        return None
    sev = name_to_sev.get(status_name)
    if sev is None:
        return None
    return ls.ConstrainingFactor(
        factor_id="cm.overall_status",
        severity=sev,
        description=f"As-Maintained configuration status: {status_name}",
    )


def _eval_staleness(inputs: FusionInputs,
                     thresholds: Thresholds,
                     now_ns: int) -> ls.ConstrainingFactor | None:
    if inputs.latest_telemetry is None:
        # A capability-only asset (customer overlay — no DIS / sustainment
        # telemetry, only the StrikeCapabilityMessage feed) IS being
        # observed; the capability snapshot is its input. Don't false-flag
        # it "no telemetry". Staleness of the capability feed itself is out
        # of Sub-phase F scope.
        if inputs.capability_snapshot:
            return None
        return ls.ConstrainingFactor(
            factor_id="stale_inputs",
            severity=ls.LOGISTICS_SEVERITY_DEGRADED,
            description="No telemetry observed for this asset yet",
        )
    sample_ts = inputs.latest_telemetry.provenance.sample_time
    sample_ns = sample_ts.ToNanoseconds() if sample_ts.seconds or sample_ts.nanos else 0
    if sample_ns == 0:
        return None  # producer didn't set sample_time; can't judge staleness
    age_s = (now_ns - sample_ns) / 1_000_000_000
    if age_s < thresholds.stale_input_seconds:
        return None
    return ls.ConstrainingFactor(
        factor_id="stale_inputs",
        severity=ls.LOGISTICS_SEVERITY_DEGRADED,
        description=(
            f"Telemetry stale ({age_s:.0f}s; threshold "
            f"{thresholds.stale_input_seconds}s)"
        ),
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def compute_logistics_status(
    inputs: FusionInputs,
    thresholds: Thresholds,
    now_ns: int,
) -> ls.AssetLogisticsStatus:
    """Pure function. Combine telemetry + windows + CM into AssetLogisticsStatus."""
    factors: list[ls.ConstrainingFactor] = []

    if (f := _eval_fuel(inputs, thresholds)):
        factors.append(f)
    factors.extend(_eval_ammo(inputs, thresholds))
    factors.extend(_eval_wear(inputs, thresholds))
    factors.extend(_eval_inventory(inputs, thresholds))
    if (f := _eval_mtbf(inputs, thresholds)):
        factors.append(f)
    factors.extend(_eval_subsystems(inputs, thresholds))
    if (f := _eval_cm_state(inputs, thresholds)):
        factors.append(f)
    if (f := _eval_staleness(inputs, thresholds, now_ns)):
        factors.append(f)

    overall = _max_severity(factors)

    status = ls.AssetLogisticsStatus(
        asset_id=inputs.asset_id,
        platform_variant=inputs.platform_variant,
        overall_severity=overall,
        constraining_factors=factors,
    )

    # Earliest non-critical factor's projected_time_to_worse becomes the
    # "projected mission capable remaining" — heuristic, but transparent.
    # If we're already CRITICAL/NON_OPERATIONAL, it's 0.
    if overall in (ls.LOGISTICS_SEVERITY_CRITICAL,
                    ls.LOGISTICS_SEVERITY_NON_OPERATIONAL):
        status.projected_mission_capable_remaining.FromSeconds(0)
    else:
        soonest_s = None
        for f in factors:
            if f.projected_time_to_worse.seconds or f.projected_time_to_worse.nanos:
                s = (f.projected_time_to_worse.seconds
                     + f.projected_time_to_worse.nanos / 1e9)
                if soonest_s is None or s < soonest_s:
                    soonest_s = s
        if soonest_s is not None:
            status.projected_mission_capable_remaining.FromSeconds(int(soonest_s))

    # Audit timestamps
    status.computed_at.FromNanoseconds(now_ns)
    if inputs.latest_telemetry is not None:
        st = inputs.latest_telemetry.provenance.sample_time
        if st.seconds or st.nanos:
            status.latest_telemetry_sample_time.CopyFrom(st)

    if inputs.cm_state is not None:
        status.cm_baseline_id = inputs.cm_state.baseline_id

    return status
