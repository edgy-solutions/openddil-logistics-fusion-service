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


# ---------------------------------------------------------------------------
# Deduplicated warning for discarded input (ADR-0036 clause 1)
# ---------------------------------------------------------------------------
_warned_unmapped: set[str] = set()


def _warn_unmapped_once(kind: str, value: str, hint: str) -> None:
    """Warn ONCE per distinct unrecognised value, at WARNING.

    Both properties are load-bearing, and both come from clause 1:

    * WARNING, not DEBUG — a signal suppressed at the default level is not
      a signal. That is how the buffer probe stayed invisible for months.
    * Deduplicated — these arrive per telemetry message, so an unmapped
      value would log at feed rate and be filtered within a day, which
      trains readers to ignore it exactly as a permanent zero trained them
      to trust it.

    The message names the likely cause rather than the symptom, so the log
    line is a diagnosis: someone reading it should not have to open this
    file to learn what to do.
    """
    key = f"{kind}:{value}"
    if key in _warned_unmapped:
        return
    _warned_unmapped.add(key)
    logger.warning(
        "Unrecognised %s %r — DISCARDED, contributes nothing to severity. %s",
        kind, value, hint,
    )


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


# ---------------------------------------------------------------------------
# WEAR-COMPONENT MANIFEST (GD-14) — does this platform HAVE this component?
# ---------------------------------------------------------------------------
# Until 2026-09-06 every wear axis was derived for every asset that moved,
# which produced four helicopters reporting 100.0% TRACK wear. The axis was
# applicable because the model could compute it, not because the platform had
# the part.
#
# The declaration is per PLATFORM CLASS, not per asset: whether an AH-64E has
# tracks is the same for every AH-64E. That is deliberately NOT the CM
# baseline, which is a per-asset authorized-configuration fact — measured
# coverage 3 of 14 assets, no baseline declaring `track` at all, and slot
# names (`engine-left`) that do not match axis names (`engine`). Two declared
# vocabularies that do not align are usually not fixed by a mapping between
# them but by a third declaration at the level the fact actually lives.
#
# THREE OUTCOMES. The middle one is the whole point:
#   declared present -> evaluate
#   declared absent  -> NOT APPLICABLE: no factor, and explicitly not
#                       "unknown". The component does not exist, so nothing
#                       about it is unknown. Reporting a helicopter's track
#                       wear as UNKNOWN would claim we are missing data about
#                       a part that is not there.
#   no manifest      -> UNKNOWN: we have not been told what this platform
#                       carries, which is a real gap and must not be silently
#                       treated as "has everything".
def _load_wear_manifest() -> dict[str, set[str]]:
    """variant -> declared component set. Absent file => empty mapping, which
    makes every variant UNKNOWN rather than every variant fully-equipped."""
    out: dict[str, set[str]] = {}
    try:
        import yaml  # type: ignore
        path = os.path.join(ONTOLOGY_DIR, "wear_component_manifest.yaml")
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        for variant, spec in (doc.get("platforms") or {}).items():
            comps = (spec or {}).get("components")
            if not isinstance(comps, list):
                logger.warning(
                    "wear_component_manifest.yaml: %r has no component list; "
                    "treating as UNDECLARED rather than as empty", variant,
                )
                continue
            out[str(variant)] = {str(c) for c in comps}
    except FileNotFoundError:
        logger.warning(
            "wear_component_manifest.yaml not found in %s — every wear axis "
            "will report UNKNOWN. This is the honest default: without a "
            "manifest we do not know which components a platform carries, "
            "and assuming it carries all of them is how helicopters came to "
            "have track wear.", ONTOLOGY_DIR,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("wear_component_manifest.yaml unreadable (%s); "
                        "every wear axis will report UNKNOWN", exc)
    return out


_WEAR_MANIFEST: dict[str, set[str]] = _load_wear_manifest()


def set_wear_manifest_for_test(mapping: dict[str, set[str]] | None) -> None:
    """TEST SEAM. The manifest is loaded once at import, which is right for a
    service and wrong for a test: without it every test ran against an empty
    manifest and therefore against the UNKNOWN branch, which is exactly one
    of the three behaviours under test. Injecting keeps all three reachable."""
    global _WEAR_MANIFEST
    _WEAR_MANIFEST = {} if mapping is None else dict(mapping)


def wear_axis_applicability(variant: str, component: str) -> str:
    """'evaluate' | 'not_applicable' | 'unknown'."""
    declared = _WEAR_MANIFEST.get(variant)
    if declared is None:
        return "unknown"
    return "evaluate" if component in declared else "not_applicable"


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
        # GATE FIRST, before reading any value: whether the platform HAS this
        # component is prior to what the sensor says about it.
        applies = wear_axis_applicability(inputs.platform_variant, name)
        if applies == "not_applicable":
            # The platform does not carry this component. No factor, and
            # deliberately no UNKNOWN either — see the manifest comment.
            logger.debug("wear axis %r not applicable to %s", name,
                         inputs.platform_variant)
            continue
        if applies == "unknown":
            # No manifest entry for this variant. We do not know whether the
            # component exists, so we decline to assert wear on it. Logged at
            # INFO because an undeclared platform is a gap someone should
            # close, not a routine condition.
            logger.info(
                "wear axis %r for %s: UNDECLARED platform, no factor emitted "
                "(add it to wear_component_manifest.yaml)",
                name, inputs.platform_variant,
            )
            continue

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
    """Engagement-worthiness from the weapons-capability snapshot.

    Sub-phase F. The weapons-capability feed (source-specific messages
    decomposed by their respective Bloblang into the Silver topic
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


# Confidence for the mtbf projection. ASSERTED, NOT COMPUTED — and the
# distinction is the point, because a computed-looking number here would be
# the confabulation shape: a real-seeming answer where a refusal or an
# honest assertion belongs.
#
# There is no computation available today. Linear extrapolation of a single
# trend line carries no intrinsic uncertainty estimate, and the wear
# accumulators keep (mean, count) — sufficient to merge a mean across tiers,
# insufficient for dispersion. So there is nothing to derive a fit quality
# from even in principle.
#
# Low because the projection sits on ADR-0020's authored placeholder
# coefficients, which are explicitly unvalidated. The value is a floor on
# trust, not a measurement of it.
#
# See ADR-0020 §Confidence staircase for the path to a real number, and the
# rule that a computed confidence must declare its KIND alongside its value —
# fit quality and calibrated probability are different quantities and must
# never share this field silently.
_MTBF_ASSERTED_CONFIDENCE = 0.2


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
        # ADR-0035 IH-5. This is the most-derived value the engine produces —
        # a projection built on an extrapolation of authored coefficients —
        # and until this stamp it was the ONLY factor claiming nothing about
        # its own origin, while less-derived siblings (_eval_inventory, the
        # derived-sustainment evaluator) stamped correctly. The provenance
        # discipline had landed everywhere except its most load-bearing
        # point, and proto3's honest zero-defaults are what made that
        # invisible.
        #
        # Consumers read `origin` to decide whether a value needs a
        # modelled-not-measured treatment on screen (ADR-0035 class 1), so
        # this stamp is what makes an honest render possible at all — the
        # alternative is hardcoding the marker per field.
        origin=tel.ORIGIN_DERIVED,
        confidence=_MTBF_ASSERTED_CONFIDENCE,
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
        # UNSPECIFIED and OK arrive here together and mean different things:
        # "we do not recognise this string" versus "this subsystem is fine".
        # Both are skipped — that is deliberate, since neither constrains the
        # asset — but only the first is a DISCARD, and a discard that leaves
        # no trace is indistinguishable from a producer that said nothing.
        # The asset DID report a fault code; we simply had no mapping for it.
        if sev == ls.LOGISTICS_SEVERITY_UNSPECIFIED:
            _warn_unmapped_once(
                "subsystem health", health.strip(),
                f"Add it to subsystem_health_map (subsystem={subsys.strip()!r}) "
                "or confirm with the producer that this vocabulary is expected.",
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


def _eval_operational_state(inputs: FusionInputs,
                              thresholds: Thresholds) -> list[ls.ConstrainingFactor]:
    """Map OperationalState's 3 axes -> ConstrainingFactor entries.

    OperationalState is a generic 3-axis posture model (power × mode ×
    health, plus discrete RX/TX cues) populated by any source that carries
    entity operational state — proprietary sensor feeds, DIS EmissionSystem
    + EntityState appearance, AFSim sensor/platform state, VRForces damage
    states, etc. See openddil.telemetry.v1.OperationalState for the proto.

    Severity mapping per axis (this evaluator is the policy embodiment of
    the table in the proto comments + the operational-state ADR):

      power_state == OFF          -> CRITICAL, factor_id="operational.offline"
      power_state == SHUTTING_DOWN -> CRITICAL, factor_id="operational.shutdown"
      power_state == MAINTENANCE  -> DEGRADED, factor_id="operational.maintenance"
      health_state == FAILED      -> CRITICAL, factor_id="operational.failed"
      health_state == FAULT       -> CRITICAL, factor_id="operational.fault"
      health_state == DEGRADED    -> DEGRADED, factor_id="operational.degraded"
      otherwise                   -> no factor (healthy posture)

    Multiple axes can each contribute a factor (e.g. an entity reporting
    POWER_STATE_MAINTENANCE + HEALTH_STATE_DEGRADED gets both factors).
    Distinct from `_eval_subsystems` above which reads per-component fault
    codes from `sustainment.health.active_fault_codes` (mobile-platform
    BIT); the `operational.*` factor_id namespace stays semantically
    separate from `subsystem.<NAME>`.
    """
    if inputs.latest_telemetry is None:
        return []
    op = inputs.latest_telemetry.operational_state
    factors: list[ls.ConstrainingFactor] = []

    # ---- PowerState axis ----
    if op.power_state == tel.POWER_STATE_OFF:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.offline",
            severity=ls.LOGISTICS_SEVERITY_CRITICAL,
            description="Entity is powered off",
        ))
    elif op.power_state == tel.POWER_STATE_SHUTTING_DOWN:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.shutdown",
            severity=ls.LOGISTICS_SEVERITY_CRITICAL,
            description="Entity is executing or completed shutdown",
        ))
    elif op.power_state == tel.POWER_STATE_MAINTENANCE:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.maintenance",
            severity=ls.LOGISTICS_SEVERITY_DEGRADED,
            description="Entity is in a scheduled maintenance window",
        ))

    # ---- HealthState axis (orthogonal — fires alongside any power state) ----
    if op.health_state == tel.HEALTH_STATE_FAILED:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.failed",
            severity=ls.LOGISTICS_SEVERITY_CRITICAL,
            description="Entity has hard-failed and requires service",
        ))
    elif op.health_state == tel.HEALTH_STATE_FAULT:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.fault",
            severity=ls.LOGISTICS_SEVERITY_CRITICAL,
            description="Entity has an active fault requiring attention",
        ))
    elif op.health_state == tel.HEALTH_STATE_DEGRADED:
        factors.append(ls.ConstrainingFactor(
            factor_id="operational.degraded",
            severity=ls.LOGISTICS_SEVERITY_DEGRADED,
            description="Entity has a non-critical anomaly limiting capability",
        ))

    # FunctionalMode is informational only — it does NOT drive severity by
    # itself (IDLE vs ACTIVE vs RECEIVE_ONLY are operator postures, not
    # faults). Visible to consumers via the raw OperationalState block.

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
    #
    # 2026-07-14: CM contribution CAPPED at DEGRADED per ADR-0026 (CM and
    # operational state are orthogonal axes; a records-compliance judgment
    # must not dominate a rollup that also carries functional severity).
    # Previously CONFIG_STATUS_NOT_MISSION_CAPABLE mapped to
    # LOGISTICS_SEVERITY_NON_OPERATIONAL, and because overall_severity is
    # a strict worst-of across all factors, a CM-only red asset with all
    # operational factors green would still render NON_OPERATIONAL to
    # every consumer (map ring, LogisticsStatusCard badge, regional/HQ
    # rollups). Stakeholders correctly read that as an incoherent
    # display: functionally-up radar labeled cannot-perform-mission.
    #
    # A CM deviation on a functioning asset IS a real degradation of the
    # logistics posture (operating with deferred maintenance risk) but
    # it is not "cannot perform mission" territory. Cap at DEGRADED so
    # the factor still surfaces as a ConstrainingFactor in the drill-in
    # (visibility preserved) and still contributes to overall_severity
    # via worst-of, but never solo-drives it past DEGRADED.
    #
    # A CM state that ever genuinely means "do not operate this asset"
    # (safety-of-use notice, grounding directive) is a distinct input
    # and would arrive as its own factor, not through this mapping. See
    # follow-up: explicit OPERATING_WITH_CM_WAIVER enum (B4) for the
    # first-class rendering of the CM-red/ops-green quadrant.
    cs = inputs.cm_state.DESCRIPTOR.fields_by_name["overall_status"].enum_type
    name_to_sev: dict[str, int | None] = {
        "CONFIG_STATUS_UNSPECIFIED":          None,
        "CONFIG_STATUS_IN_COMPLIANCE":        None,
        "CONFIG_STATUS_MINOR_DISCREPANCY":    None,  # advisory; not a constraint
        "CONFIG_STATUS_MAJOR_DISCREPANCY":    ls.LOGISTICS_SEVERITY_DEGRADED,
        "CONFIG_STATUS_NOT_MISSION_CAPABLE":  ls.LOGISTICS_SEVERITY_DEGRADED,
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
        # telemetry, only the weapons-capability feed) IS being observed;
        # the capability snapshot is its input. Don't false-flag
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
    factors.extend(_eval_operational_state(inputs, thresholds))
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
