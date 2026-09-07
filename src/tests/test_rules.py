"""Unit tests for the pure-Python fusion rules.

These tests are framework-free: no Faust, no Restate, no Kafka. They
construct Protobuf inputs in-memory, call `compute_logistics_status`, and
assert on the returned AssetLogisticsStatus.

Target: ≥90% coverage on fusion/rules.py.
"""
from __future__ import annotations

import logging

import time

import pytest

from fusion import rules

from openddil.common.v1 import quantity_pb2 as qpb
from openddil.logistics.v1 import logistics_status_pb2 as ls
from openddil.telemetry.v1 import telemetry_pb2 as tel
from openddil.logistics.v1 import windowed_telemetry_pb2 as win
from openddil.configuration.v1 import as_maintained_pb2 as cm

from fusion.rules import (
    FusionInputs,
    compute_logistics_status,
    _eval_fuel,
    _eval_ammo,
    _eval_wear,
    _eval_inventory,
    _eval_mtbf,
    _eval_subsystems,
    _eval_operational_state,
    _eval_cm_state,
    _eval_staleness,
)
from fusion.thresholds import Thresholds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ASSET_ID = "USA-ARMY-1HBCT-M1A2-4773"
VARIANT = "M1A2-SEPv3"


# The wear-component manifest is loaded once at import from /ontology, which
# does not exist in a test environment — so without this every test would run
# against the UNDECLARED branch and silently stop exercising wear at all.
# Declaring the test variant restores the pre-manifest behaviour for the
# existing cases; the three states get their own tests below.
@pytest.fixture(autouse=True)
def _declare_test_variant():
    rules.set_wear_manifest_for_test(
        {VARIANT: {"track", "engine", "barrel", "suspension", "transmission"}}
    )
    yield
    rules.set_wear_manifest_for_test(None)


def _thr(**overrides) -> Thresholds:
    base = Thresholds.from_env()
    # Build a fresh frozen instance with any overrides applied.
    fields = {
        "fuel_pct_critical":    base.fuel_pct_critical,
        "fuel_pct_degraded":    base.fuel_pct_degraded,
        "ammo_pct_critical":    base.ammo_pct_critical,
        "ammo_pct_degraded":    base.ammo_pct_degraded,
        "wear_pct_critical":    base.wear_pct_critical,
        "wear_pct_degraded":    base.wear_pct_degraded,
        "mtbf_hours_critical":  base.mtbf_hours_critical,
        "mtbf_hours_degraded":  base.mtbf_hours_degraded,
        "ammo_low_count":       base.ammo_low_count,
        "emit_interval_seconds": base.emit_interval_seconds,
        "stale_input_seconds":  base.stale_input_seconds,
        "subsystem_health_map": dict(base.subsystem_health_map),
    }
    fields.update(overrides)
    return Thresholds(**fields)


def _now_ns() -> int:
    return int(time.time() * 1_000_000_000)


def _telemetry(
    *,
    fuel_value: float | None = None,
    fuel_unit: str = "%",
    consumables: dict[str, tuple[int, int]] | None = None,
    wear: dict[str, tuple[float, float]] | None = None,
    faults: list[str] | None = None,
    sample_time_ns: int | None = None,
) -> tel.EntityTelemetryEvent:
    evt = tel.EntityTelemetryEvent()
    evt.asset.asset_id = ASSET_ID
    evt.asset.platform_variant = VARIANT

    if fuel_value is not None:
        evt.sustainment.fluids.fuel_remaining.value = fuel_value
        evt.sustainment.fluids.fuel_remaining.unit = fuel_unit

    if consumables:
        for slot, (remaining, capacity) in consumables.items():
            cs = evt.sustainment.consumables.items[slot]
            cs.quantity_remaining = remaining
            cs.quantity_capacity = capacity

    if wear:
        for comp, (hours_in_service, rul) in wear.items():
            ws = evt.sustainment.wear.components[comp]
            ws.hours_in_service.value = hours_in_service
            ws.hours_in_service.unit = "h"
            ws.remaining_useful_life.value = rul
            ws.remaining_useful_life.unit = "h"

    if faults:
        evt.sustainment.health.active_fault_codes.extend(faults)

    if sample_time_ns is not None:
        evt.provenance.sample_time.FromNanoseconds(sample_time_ns)
    return evt


def _cm_state(*, status_name: str | None) -> cm.AsMaintainedConfiguration:
    state = cm.AsMaintainedConfiguration()
    state.asset_id = ASSET_ID
    state.baseline_id = "M1A2-SEPv3-Baseline-2024.2"
    if status_name is not None:
        state.overall_status = cm.ConfigurationStatus.Value(status_name)
    return state


def _windows(
    *,
    wear_trends: list[tuple[str, float, float]] | None = None,
) -> win.WindowedTelemetry:
    """wear_trends: [(component, latest_rul_h, slope_h_per_h), ...]"""
    w = win.WindowedTelemetry()
    w.asset_id = ASSET_ID
    w.platform_variant = VARIANT
    if wear_trends:
        for comp, latest_h, slope in wear_trends:
            t = w.wear_trends.add()
            t.component_key = comp
            t.remaining_useful_life.latest.value = latest_h
            t.remaining_useful_life.latest.unit = "h"
            t.remaining_useful_life.slope.value = slope
            t.remaining_useful_life.slope.unit = "h/h"
    return w


def _store(capability_id: str, ammo: int, *, store_location: int = 1,
            store_category: str = "AIR") -> dict:
    """One per-store entry of the asset-capability-snapshot Silver shape."""
    return {
        "capability_id": capability_id,
        "store_location": store_location,
        "store_category": store_category,
        "ammo": ammo,
        "simulated": True,
    }


def _capability_snapshot(*stores: dict) -> dict:
    """The asset-capability-snapshot Silver shape (JSON) produced by any
    source-specific weapons-capability Bloblang. Only the fields
    `_eval_inventory` reads are required."""
    return {
        "schema_revision": 1,
        "asset_id": ASSET_ID,
        "schema_version": "002.3",
        "mode": "SIMULATION",
        "capabilities": list(stores),
    }


# ---------------------------------------------------------------------------
# Healthy case
# ---------------------------------------------------------------------------
def test_healthy_asset_is_ok_with_no_factors():
    inputs = FusionInputs(
        asset_id=ASSET_ID,
        platform_variant=VARIANT,
        latest_telemetry=_telemetry(
            fuel_value=85.0,
            consumables={"main_gun": (40, 40)},
            wear={"engine": (200.0, 1800.0)},  # 10% wear
            sample_time_ns=_now_ns() - 30_000_000_000,  # 30s ago, not stale
        ),
        telemetry_windows=_windows(),
        cm_state=_cm_state(status_name="CONFIG_STATUS_IN_COMPLIANCE"),
    )
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_OK
    assert len(status.constraining_factors) == 0
    assert status.asset_id == ASSET_ID
    assert status.platform_variant == VARIANT
    assert status.cm_baseline_id == "M1A2-SEPv3-Baseline-2024.2"


# ---------------------------------------------------------------------------
# Fuel evaluator
# ---------------------------------------------------------------------------
def test_fuel_degraded_pct():
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(fuel_value=25.0, fuel_unit="%"),
                      None, None),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert factor.current_value.unit == "%"
    assert factor.current_value.value == pytest.approx(25.0)


def test_fuel_critical_pct():
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(fuel_value=12.0, fuel_unit="%"),
                      None, None),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_fuel_above_threshold_returns_none():
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(fuel_value=80.0, fuel_unit="%"),
                      None, None),
        _thr(),
    )
    assert factor is None


def test_fuel_volume_converts_via_pint_then_pct():
    """504 gal capacity, 100 gal remaining ≈ 19.8% → CRITICAL band."""
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(fuel_value=100.0, fuel_unit="gal_us"),
                      None, None),
        _thr(),
    )
    assert factor is not None
    # 100 gal / 504.4 gal = 19.83% — between critical (15) and degraded (30)
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_fuel_unit_mismatch_liters_vs_gallons():
    """Same physical 100 gal expressed as ~378.5 L should compute the same %."""
    # 100 gal_us ≈ 378.541 L. With 504.4 gal capacity in the env table,
    # pint converts L → gal correctly.
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(fuel_value=378.541, fuel_unit="L"),
                      None, None),
        _thr(),
    )
    assert factor is not None
    # Should land in the same band as the gallons-input test.
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_fuel_unknown_variant_skips():
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, "UNOBTAINIUM-99",
                      _telemetry(fuel_value=100.0, fuel_unit="gal_us"),
                      None, None),
        _thr(),
    )
    assert factor is None


def test_fuel_no_telemetry_skips():
    assert _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT, None, None, None),
        _thr(),
    ) is None


def test_fuel_unset_field_skips():
    factor = _eval_fuel(
        FusionInputs(ASSET_ID, VARIANT, _telemetry(), None, None),
        _thr(),
    )
    assert factor is None


# ---------------------------------------------------------------------------
# Ammunition evaluator
# ---------------------------------------------------------------------------
def test_ammo_critical_when_one_slot_low():
    factors = _eval_ammo(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(consumables={
                          "main_gun": (3, 40),   # 7.5% → CRITICAL
                          "coax":     (1800, 2000),  # 90% → fine
                      }), None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "ammo.main_gun"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_ammo_multiple_below_threshold():
    factors = _eval_ammo(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(consumables={
                          "main_gun": (8, 40),     # 20% → DEGRADED
                          "smoke":    (1, 8),      # 12.5% → DEGRADED
                      }), None, None),
        _thr(),
    )
    assert len(factors) == 2
    assert all(f.severity == ls.LOGISTICS_SEVERITY_DEGRADED for f in factors)


def test_ammo_zero_capacity_skips():
    factors = _eval_ammo(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(consumables={"weird_slot": (0, 0)}),
                      None, None),
        _thr(),
    )
    assert factors == []


# ---------------------------------------------------------------------------
# Wear evaluator
# ---------------------------------------------------------------------------
def test_wear_critical():
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(wear={"transmission": (950.0, 50.0)}),
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL
    assert factors[0].factor_id == "wear.transmission"


def test_wear_degraded():
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(wear={"engine": (800.0, 200.0)}),  # 80%
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_wear_unset_quantities_skip():
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(wear={"phantom": (0.0, 0.0)}),
                      None, None),
        _thr(),
    )
    assert factors == []


# ---------------------------------------------------------------------------
# Inventory / engagement-worthiness evaluator (Sub-phase F)
# ---------------------------------------------------------------------------
def test_inventory_exhausted_is_critical():
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 0))),
        _thr(),
    )
    assert len(factors) == 1
    f = factors[0]
    assert f.factor_id == "inventory.MRAD_Interceptor"
    assert f.severity == ls.LOGISTICS_SEVERITY_CRITICAL
    assert "AMMO_EXHAUSTED" in f.description
    assert f.origin == tel.ORIGIN_DERIVED


def test_inventory_low_is_degraded():
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 3))),  # <= ammo_low_count
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert "AMMO_LOW" in factors[0].description


def test_inventory_at_threshold_is_degraded():
    # Boundary: ammo == ammo_low_count is inclusive (still AMMO_LOW).
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 5))),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_inventory_above_threshold_no_factor():
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 40))),
        _thr(),
    )
    assert factors == []


def test_inventory_multiple_stores_one_constrained():
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("Interceptor_A", 0, store_location=1),
                          _store("Interceptor_B", 50, store_location=2))),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "inventory.Interceptor_A"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_inventory_no_snapshot_skips():
    assert _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=None),
        _thr(),
    ) == []


def test_inventory_missing_ammo_field_skips():
    # A store entry with no numeric `ammo` makes no claim — skipped, not 0.
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          {"capability_id": "X", "store_location": 1})),
        _thr(),
    )
    assert factors == []


def test_inventory_fallback_factor_id_when_no_capability_id():
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("", 0, store_location=7))),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "inventory.store-7"


def test_inventory_custom_low_threshold():
    # AMMO_LOW_COUNT override flows through _thr into the banding.
    factors = _eval_inventory(
        FusionInputs(ASSET_ID, VARIANT, None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 8))),
        _thr(ammo_low_count=10),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_capability_only_asset_surfaces_factor_without_stale_flag():
    # A capability-only asset (no telemetry/windows/cm) must NOT get a
    # spurious "no telemetry observed" stale_inputs factor — the capability
    # snapshot is a real input. compute_logistics_status integration check.
    status = compute_logistics_status(
        FusionInputs(ASSET_ID, "", None, None, None,
                      capability_snapshot=_capability_snapshot(
                          _store("MRAD_Interceptor", 0))),
        _thr(), _now_ns(),
    )
    factor_ids = {f.factor_id for f in status.constraining_factors}
    assert "inventory.MRAD_Interceptor" in factor_ids
    assert "stale_inputs" not in factor_ids
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_CRITICAL


# ---------------------------------------------------------------------------
# MTBF evaluator
# ---------------------------------------------------------------------------
def test_mtbf_critical_from_negative_slope():
    """RUL dropping at -10 h/h, latest 5 h → 0.5 h to failure (critical)."""
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None,
                      _windows(wear_trends=[("transmission", 5.0, -10.0)]),
                      None),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_CRITICAL
    assert factor.factor_id == "mtbf.transmission"


def test_mtbf_factor_stamps_derived_provenance():
    """ADR-0035 IH-5. The mtbf projection is the most-derived value the
    engine produces and must say so: a projection built on an extrapolation
    of authored coefficients.

    This guard was run RED against the unstamped evaluator before being
    trusted (ADR-0037 §3) — it fails with ORIGIN_UNSPECIFIED / 0.0, which is
    exactly the state it exists to prevent recurring.

    `origin` is asserted by identity, not by truthiness: ORIGIN_UNSPECIFIED
    is 0, so `assert factor.origin` would pass for ORIGIN_MEASURED and fail
    open on the case that matters.
    """
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None,
                      _windows(wear_trends=[("engine", 6.0, -1.0)]),
                      None),
        _thr(),
    )
    assert factor is not None
    assert factor.origin == tel.ORIGIN_DERIVED
    # Asserted, not computed — see rules._MTBF_ASSERTED_CONFIDENCE. Pinned
    # as a non-zero low value: 0.0 was the pre-IH-5 state and reads as "no
    # claim", which is the thing being corrected.
    assert 0.0 < factor.confidence < 0.5


def test_mtbf_degraded_from_slow_decline():
    """RUL dropping at -1 h/h, latest 6 h → 6 h to failure (degraded)."""
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None,
                      _windows(wear_trends=[("engine", 6.0, -1.0)]),
                      None),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_mtbf_positive_slope_skips():
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None,
                      _windows(wear_trends=[("battery", 100.0, 0.5)]),
                      None),
        _thr(),
    )
    assert factor is None


def test_mtbf_no_windows_skips():
    assert _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None, None, None), _thr(),
    ) is None


# ---------------------------------------------------------------------------
# Subsystem health evaluator
# ---------------------------------------------------------------------------
def test_subsystem_inoperative_non_operational():
    factors = _eval_subsystems(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(faults=["POWERPLANT:INOPERATIVE"]),
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_NON_OPERATIONAL


def test_subsystem_degraded():
    factors = _eval_subsystems(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(faults=["TRANSMISSION:DEGRADED"]),
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_subsystem_unknown_health_skipped():
    factors = _eval_subsystems(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(faults=["WEIRD:PARTIALLY-MAGICAL"]),
                      None, None),
        _thr(),
    )
    assert factors == []


def test_subsystem_token_without_colon_handled():
    factors = _eval_subsystems(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(faults=["INOPERATIVE"]),
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "subsystem.unknown"


def test_subsystem_unknown_health_warns_and_dedupes(caplog):
    """The skip is correct; the SILENCE was not.

    An unmapped fault code is still discarded — that is deliberate, since an
    unrecognised string does not constrain the asset. But the asset DID
    report something, and before this the discard left no trace anywhere:
    no factor, no log, nothing to distinguish it from a producer that said
    nothing at all.

    Both halves are asserted because both are load-bearing (ADR-0036
    clause 1): WARNING level, because a signal suppressed at the default
    level is not a signal; and deduplicated, because fault codes arrive per
    message and an unmapped one would otherwise log at feed rate until
    someone filters it.
    """
    import fusion.rules as R
    R._warned_unmapped.clear()   # module singleton — do not inherit state

    with caplog.at_level(logging.WARNING, logger="fusion.rules"):
        for _ in range(3):
            factors = _eval_subsystems(
                FusionInputs(ASSET_ID, VARIANT,
                              _telemetry(faults=["POWERPLANT:THERMAL_LIMITED"]),
                              None, None),
                _thr(),
            )

    # Behaviour is unchanged: still discarded, still no factor.
    assert factors == []

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected exactly one warning, got {len(warnings)}"
    msg = warnings[0].getMessage()
    assert "THERMAL_LIMITED" in msg          # names the value it could not map
    assert "DISCARDED" in msg                # says what happened to it
    assert "subsystem_health_map" in msg     # names the fix, not just the symptom


def test_subsystem_mapped_health_does_not_warn(caplog):
    """A recognised code must stay silent — otherwise the warning is noise
    and gets filtered, which is the failure mode it was written to avoid."""
    import fusion.rules as R
    R._warned_unmapped.clear()

    with caplog.at_level(logging.WARNING, logger="fusion.rules"):
        factors = _eval_subsystems(
            FusionInputs(ASSET_ID, VARIANT,
                          _telemetry(faults=["POWERPLANT:DEGRADED"]),
                          None, None),
            _thr(),
        )

    assert len(factors) == 1
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


# ---------------------------------------------------------------------------
# OperationalState evaluator (ADR-0026)
# ---------------------------------------------------------------------------
# These tests pin the policy table in _eval_operational_state — the
# mapping from proto enum values to ConstrainingFactor entries. The
# policy is the visible contract for "what severity does each posture
# axis produce" and the most-trafficked code path in the customer-overlay
# pipeline (every sensor heartbeat goes through it). Without these,
# any well-intentioned refactor that touches the if/elif ladder
# silently changes ring colors across the entire deployment.

def _op_state_telemetry(
    *,
    power_state: int | None = None,
    health_state: int | None = None,
    functional_mode: int | None = None,
    actively_receiving: bool | None = None,
    actively_transmitting: bool | None = None,
) -> tel.EntityTelemetryEvent:
    """Helper: build an EntityTelemetryEvent with operational_state populated.
    Fields default to the proto's UNSPECIFIED (= 0) when omitted."""
    evt = tel.EntityTelemetryEvent()
    evt.asset.asset_id = ASSET_ID
    evt.asset.platform_variant = VARIANT
    if power_state is not None:
        evt.operational_state.power_state = power_state
    if health_state is not None:
        evt.operational_state.health_state = health_state
    if functional_mode is not None:
        evt.operational_state.functional_mode = functional_mode
    if actively_receiving is not None:
        evt.operational_state.actively_receiving = actively_receiving
    if actively_transmitting is not None:
        evt.operational_state.actively_transmitting = actively_transmitting
    return evt


def _op_inputs(evt: tel.EntityTelemetryEvent) -> FusionInputs:
    return FusionInputs(ASSET_ID, VARIANT, evt, None, None)


# -- PowerState axis: 3 non-OK transitions ----------------------------------

def test_operational_state_power_off_is_critical():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(power_state=tel.POWER_STATE_OFF)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.offline"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL
    assert "powered off" in factors[0].description.lower()


def test_operational_state_power_shutting_down_is_critical():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(power_state=tel.POWER_STATE_SHUTTING_DOWN)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.shutdown"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_operational_state_power_maintenance_is_degraded():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(power_state=tel.POWER_STATE_MAINTENANCE)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.maintenance"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


# -- HealthState axis: 3 non-NOMINAL transitions ----------------------------

def test_operational_state_health_failed_is_critical():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(health_state=tel.HEALTH_STATE_FAILED)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.failed"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_operational_state_health_fault_is_critical():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(health_state=tel.HEALTH_STATE_FAULT)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.fault"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_operational_state_health_degraded_is_degraded():
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(health_state=tel.HEALTH_STATE_DEGRADED)),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].factor_id == "operational.degraded"
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED


# -- Axes are orthogonal: both can fire on one event ------------------------

def test_operational_state_power_and_health_both_fire():
    """Per the ADR-0026 policy comment in rules.py: multiple axes can each
    contribute a factor (e.g. POWER_MAINTENANCE + HEALTH_DEGRADED gets
    both). The overall severity is the max across factors — DEGRADED here
    since both axes are DEGRADED-tier."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(
            power_state=tel.POWER_STATE_MAINTENANCE,
            health_state=tel.HEALTH_STATE_DEGRADED,
        )),
        _thr(),
    )
    factor_ids = {f.factor_id for f in factors}
    assert factor_ids == {"operational.maintenance", "operational.degraded"}
    assert all(f.severity == ls.LOGISTICS_SEVERITY_DEGRADED for f in factors)


def test_operational_state_power_critical_plus_health_degraded():
    """Cross-tier orthogonality: POWER_OFF (CRITICAL) + HEALTH_DEGRADED
    (DEGRADED) both fire, max severity is CRITICAL."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(
            power_state=tel.POWER_STATE_OFF,
            health_state=tel.HEALTH_STATE_DEGRADED,
        )),
        _thr(),
    )
    factor_ids = {f.factor_id for f in factors}
    assert factor_ids == {"operational.offline", "operational.degraded"}
    sevs_by_id = {f.factor_id: f.severity for f in factors}
    assert sevs_by_id["operational.offline"] == ls.LOGISTICS_SEVERITY_CRITICAL
    assert sevs_by_id["operational.degraded"] == ls.LOGISTICS_SEVERITY_DEGRADED


# -- Healthy / unset paths produce no factors -------------------------------

def test_operational_state_power_on_health_nominal_no_factors():
    """Per ADR-0026: POWER_ON + HEALTH_NOMINAL is the happy path, no
    factors emitted. The asset reads as OK from this evaluator (severity
    defaults to OK in _max_severity when factor list is empty)."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(
            power_state=tel.POWER_STATE_ON,
            health_state=tel.HEALTH_STATE_NOMINAL,
        )),
        _thr(),
    )
    assert factors == []


def test_operational_state_standby_no_factors():
    """POWER_STANDBY is "initialized, ready, no claim of activity" — not
    a fault, no factor. Distinct from OFF (CRITICAL) and MAINTENANCE
    (DEGRADED)."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(power_state=tel.POWER_STATE_STANDBY)),
        _thr(),
    )
    assert factors == []


def test_operational_state_all_unspecified_no_factors():
    """No operational_state filled at all -> no factors (proto-default
    UNSPECIFIED on every axis). This is the legacy-DIS / capability-only
    case — schematic falls back to nominal-vs-degraded driven by other
    factors (fuel, ammo, etc.) instead."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry()),  # all axes default
        _thr(),
    )
    assert factors == []


# -- FunctionalMode is informational only -----------------------------------

def test_operational_state_functional_mode_does_not_drive_severity():
    """Per the proto comment + ADR-0026 + the rules.py implementation
    comment: FunctionalMode is informational only. IDLE / ACTIVE /
    RECEIVE_ONLY / TRANSMIT_ONLY / SCAN / TRACK are operator postures,
    not faults, and must NOT contribute a ConstrainingFactor on their
    own."""
    for mode in (
        tel.FUNCTIONAL_MODE_IDLE,
        tel.FUNCTIONAL_MODE_ACTIVE,
        tel.FUNCTIONAL_MODE_RECEIVE_ONLY,
        tel.FUNCTIONAL_MODE_TRANSMIT_ONLY,
        tel.FUNCTIONAL_MODE_SCAN,
        tel.FUNCTIONAL_MODE_TRACK,
    ):
        factors = _eval_operational_state(
            _op_inputs(_op_state_telemetry(functional_mode=mode)),
            _thr(),
        )
        assert factors == [], (
            f"FunctionalMode {tel.FunctionalMode.Name(mode)} unexpectedly produced "
            f"a ConstrainingFactor — mode is informational only per ADR-0026."
        )


# -- Activity flags don't drive severity ------------------------------------

def test_operational_state_activity_flags_dont_drive_severity():
    """actively_receiving / actively_transmitting are discrete activity
    cues for the SPA's activity-dot display. They never produce factors —
    being inactive isn't a fault."""
    factors = _eval_operational_state(
        _op_inputs(_op_state_telemetry(
            actively_receiving=False,
            actively_transmitting=False,
        )),
        _thr(),
    )
    assert factors == []


# -- Absent-telemetry guard -------------------------------------------------

def test_operational_state_no_telemetry_returns_empty():
    """Defensive: with latest_telemetry=None the evaluator must not
    blow up reading op state from a non-existent message."""
    inputs = FusionInputs(ASSET_ID, VARIANT,
                          latest_telemetry=None,
                          telemetry_windows=None,
                          cm_state=None)
    factors = _eval_operational_state(inputs, _thr())
    assert factors == []


# -- compute_logistics_status integration: op_state factors flow through ---

def test_compute_status_picks_up_operational_state_factor():
    """End-to-end (within rules.py): an op_state-only event with no
    sustainment / cm / windows still produces an AssetLogisticsStatus
    whose overall_severity reflects the op_state-derived factor."""
    inputs = _op_inputs(_op_state_telemetry(
        power_state=tel.POWER_STATE_MAINTENANCE,
    ))
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_DEGRADED
    factor_ids = {f.factor_id for f in status.constraining_factors}
    assert "operational.maintenance" in factor_ids


# ---------------------------------------------------------------------------
# CM state evaluator
# ---------------------------------------------------------------------------
def test_cm_not_mission_capable_capped_at_degraded():
    # ADR-0026: CM and operational state are orthogonal axes. Fusion
    # must not fold CM state into a severity that dominates a rollup
    # also carrying functional signal. NOT_MISSION_CAPABLE is capped
    # at DEGRADED (same as MAJOR_DISCREPANCY) so a CM-only failure on
    # a functionally-green asset never renders NON_OPERATIONAL to
    # downstream consumers. The factor is still emitted so the CM
    # constraint remains visible in drill-in views.
    factor = _eval_cm_state(
        FusionInputs(ASSET_ID, VARIANT, None, None,
                      _cm_state(status_name="CONFIG_STATUS_NOT_MISSION_CAPABLE")),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert factor.factor_id == "cm.overall_status"


def test_cm_major_discrepancy_is_degraded():
    factor = _eval_cm_state(
        FusionInputs(ASSET_ID, VARIANT, None, None,
                      _cm_state(status_name="CONFIG_STATUS_MAJOR_DISCREPANCY")),
        _thr(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_cm_in_compliance_no_factor():
    factor = _eval_cm_state(
        FusionInputs(ASSET_ID, VARIANT, None, None,
                      _cm_state(status_name="CONFIG_STATUS_IN_COMPLIANCE")),
        _thr(),
    )
    assert factor is None


def test_cm_no_state_skips():
    assert _eval_cm_state(
        FusionInputs(ASSET_ID, VARIANT, None, None, None), _thr(),
    ) is None


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------
def test_no_telemetry_is_degraded_staleness():
    factor = _eval_staleness(
        FusionInputs(ASSET_ID, VARIANT, None, None, None),
        _thr(), _now_ns(),
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert factor.factor_id == "stale_inputs"


def test_old_telemetry_is_stale():
    now = _now_ns()
    factor = _eval_staleness(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(sample_time_ns=now - 600_000_000_000),  # 10 min
                      None, None),
        _thr(), now,
    )
    assert factor is not None
    assert factor.severity == ls.LOGISTICS_SEVERITY_DEGRADED


def test_fresh_telemetry_not_stale():
    now = _now_ns()
    factor = _eval_staleness(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(sample_time_ns=now - 10_000_000_000),  # 10 s
                      None, None),
        _thr(), now,
    )
    assert factor is None


def test_telemetry_without_sample_time_no_staleness_factor():
    factor = _eval_staleness(
        FusionInputs(ASSET_ID, VARIANT, _telemetry(), None, None),
        _thr(), _now_ns(),
    )
    assert factor is None


# ---------------------------------------------------------------------------
# Composition — overall severity is max across factors
# ---------------------------------------------------------------------------
def test_multiple_amber_factors_overall_degraded():
    inputs = FusionInputs(
        asset_id=ASSET_ID,
        platform_variant=VARIANT,
        latest_telemetry=_telemetry(
            fuel_value=25.0,
            consumables={"main_gun": (8, 40)},  # 20% — DEGRADED
            sample_time_ns=_now_ns(),
        ),
        telemetry_windows=_windows(),
        cm_state=_cm_state(status_name="CONFIG_STATUS_IN_COMPLIANCE"),
    )
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert len(status.constraining_factors) >= 2


def test_one_red_among_ambers_overall_critical():
    inputs = FusionInputs(
        asset_id=ASSET_ID,
        platform_variant=VARIANT,
        latest_telemetry=_telemetry(
            fuel_value=25.0,                       # DEGRADED
            consumables={"main_gun": (3, 40)},     # 7.5% — CRITICAL
            sample_time_ns=_now_ns(),
        ),
        telemetry_windows=_windows(),
        cm_state=_cm_state(status_name="CONFIG_STATUS_IN_COMPLIANCE"),
    )
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_CRITICAL
    # Projected MC remaining should be 0 when already critical.
    assert status.projected_mission_capable_remaining.seconds == 0


def test_cm_not_mission_capable_does_not_dominate_functional_green():
    # ADR-0026 compliance: a CM-only failure on a functionally-green
    # asset must NOT drag overall_severity past DEGRADED. Previously
    # this test asserted the opposite (worst-of pulled overall to
    # NON_OPERATIONAL solely from CM); that behavior violated the
    # orthogonality declared in ADR-0026 and produced the confusing
    # "functionally-green radar labeled cannot-perform-mission"
    # rendering that stakeholders correctly flagged.
    inputs = FusionInputs(
        asset_id=ASSET_ID,
        platform_variant=VARIANT,
        latest_telemetry=_telemetry(fuel_value=85.0, sample_time_ns=_now_ns()),
        telemetry_windows=_windows(),
        cm_state=_cm_state(status_name="CONFIG_STATUS_NOT_MISSION_CAPABLE"),
    )
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_DEGRADED
    # The CM factor is still emitted -- drill-in visibility preserved.
    factor_ids = {f.factor_id for f in status.constraining_factors}
    assert "cm.overall_status" in factor_ids


def test_completely_empty_inputs_get_staleness_factor():
    inputs = FusionInputs(ASSET_ID, VARIANT, None, None, None)
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert any(f.factor_id == "stale_inputs" for f in status.constraining_factors)


# ---------------------------------------------------------------------------
# Edge-case coverage targets — bring branches and config helpers above 90%.
# ---------------------------------------------------------------------------
from fusion.rules import (  # noqa: E402
    _load_fuel_capacity_table,
    _quantity_to_pint,
)


def test_load_fuel_capacity_table_env_override(monkeypatch):
    monkeypatch.setenv(
        "FUEL_CAPACITY_BY_VARIANT",
        '{"BRADLEY-A4": {"value": 175.0, "unit": "gal_us"}}',
    )
    table = _load_fuel_capacity_table()
    assert "BRADLEY-A4" in table
    assert pytest.approx(table["BRADLEY-A4"].to("gallon").magnitude) == 175.0


def test_load_fuel_capacity_table_invalid_json(monkeypatch):
    """Invalid JSON env override is logged and ignored; ontology base persists."""
    monkeypatch.setenv("FUEL_CAPACITY_BY_VARIANT", "{not json")
    table = _load_fuel_capacity_table()
    # Ontology base still contributes M1A2-SEPv3; env-only entries don't.
    assert "M1A2-SEPv3" in table


def test_load_fuel_capacity_table_ontology_base():
    """Without any env override, the ontology file populates the table."""
    table = _load_fuel_capacity_table()
    assert "M1A2-SEPv3" in table
    assert pytest.approx(table["M1A2-SEPv3"].to("gallon").magnitude) == 504.4
    assert "AH-64E" in table


def test_load_fuel_capacity_table_malformed_entry(monkeypatch):
    monkeypatch.setenv(
        "FUEL_CAPACITY_BY_VARIANT",
        '{"GOOD": {"value": 100, "unit": "gal_us"}, "MISSING_UNIT": {"value": 42}}',
    )
    table = _load_fuel_capacity_table()
    assert "GOOD" in table
    assert "MISSING_UNIT" not in table


def test_quantity_to_pint_invalid_unit_returns_none():
    bad = qpb.Quantity(value=5.0, unit="totally-not-a-unit")
    assert _quantity_to_pint(bad) is None


def test_fuel_unit_incompatible_with_capacity_skips():
    """Fuel in mass (kg) cannot be converted to volume (gal) without density."""
    # Patch the module-level capacity table so the test is self-contained
    # (without env-var coupling).
    import fusion.rules
    original = fusion.rules._FUEL_CAPACITY
    fusion.rules._FUEL_CAPACITY = {VARIANT: __import__("pint").UnitRegistry()
                                        .Quantity(500.0, "gallon")}
    try:
        factor = _eval_fuel(
            FusionInputs(ASSET_ID, VARIANT,
                          _telemetry(fuel_value=50.0, fuel_unit="kg"),
                          None, None),
            _thr(),
        )
        assert factor is None
    finally:
        fusion.rules._FUEL_CAPACITY = original


def test_wear_unit_conversion_failure_skips():
    evt = tel.EntityTelemetryEvent()
    ws = evt.sustainment.wear.components["weird"]
    ws.hours_in_service.value = 100.0
    ws.hours_in_service.unit = "kg"  # not a time unit
    ws.remaining_useful_life.value = 50.0
    ws.remaining_useful_life.unit = "h"
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT, evt, None, None), _thr(),
    )
    assert factors == []


def test_mtbf_unit_conversion_failure_skips():
    w = win.WindowedTelemetry()
    t = w.wear_trends.add()
    t.component_key = "weird"
    t.remaining_useful_life.latest.value = 10.0
    t.remaining_useful_life.latest.unit = "kg"  # not a time unit
    t.remaining_useful_life.slope.value = -1.0
    t.remaining_useful_life.slope.unit = "kg/h"
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None, w, None), _thr(),
    )
    assert factor is None


def test_mtbf_far_future_returns_none():
    """Slope so small that projected hours-to-failure exceeds threshold."""
    factor = _eval_mtbf(
        FusionInputs(ASSET_ID, VARIANT, None,
                      _windows(wear_trends=[("engine", 1000.0, -0.001)]),
                      None),
        _thr(),
    )
    # 1000 h / 0.001 h/h = 1,000,000 h to failure → far above degraded (8 h)
    assert factor is None


def test_cm_state_unknown_enum_value_returns_none():
    state = cm.AsMaintainedConfiguration()
    state.asset_id = ASSET_ID
    # Force a value outside the enum range by going through SetInParent + manual
    # field manipulation. Setting an int directly works because protobuf 6.x
    # tolerates unknown enum ints on the wire.
    state.overall_status = 99  # not in ConfigurationStatus
    factor = _eval_cm_state(
        FusionInputs(ASSET_ID, VARIANT, None, None, state), _thr(),
    )
    assert factor is None


def test_projected_mc_remaining_propagates_from_factor():
    """A DEGRADED MTBF factor carries projected_time_to_worse; the composed
    status should reflect that as projected_mission_capable_remaining."""
    inputs = FusionInputs(
        asset_id=ASSET_ID,
        platform_variant=VARIANT,
        latest_telemetry=_telemetry(fuel_value=80.0, sample_time_ns=_now_ns()),
        telemetry_windows=_windows(wear_trends=[("engine", 6.0, -1.0)]),  # 6h to fail
        cm_state=_cm_state(status_name="CONFIG_STATUS_IN_COMPLIANCE"),
    )
    status = compute_logistics_status(inputs, _thr(), _now_ns())
    assert status.overall_severity == ls.LOGISTICS_SEVERITY_DEGRADED
    # Should be ~6h = 21600s; allow some slack for the integer truncation.
    assert 21500 <= status.projected_mission_capable_remaining.seconds <= 21700


# ---------------------------------------------------------------------------
# Phase 5 step 2: percent-aware wear branch + provenance read-through
# ---------------------------------------------------------------------------
def _derived_wear_telemetry(
    *, component: str, natural_value: float, natural_unit: str,
    rul_percent: float,
) -> tel.EntityTelemetryEvent:
    """Build telemetry shaped like the prognostics engine's emit contract:
    natural-unit raw measure in `hours_in_service`, percent-remaining in
    `remaining_useful_life`, and `value_provenance['*']` carrying
    ORIGIN_DERIVED."""
    evt = tel.EntityTelemetryEvent()
    evt.asset.asset_id = ASSET_ID
    evt.asset.platform_variant = VARIANT
    ws = evt.sustainment.wear.components[component]
    ws.hours_in_service.value = natural_value
    ws.hours_in_service.unit = natural_unit
    ws.remaining_useful_life.value = rul_percent
    ws.remaining_useful_life.unit = "%"
    evt.sustainment.value_provenance["*"].origin = tel.ORIGIN_DERIVED
    evt.sustainment.value_provenance["*"].confidence = 0.0
    return evt


def test_wear_percent_branch_degraded():
    """rul.unit == '%' triggers the percent-aware branch; pct_consumed =
    100 - rul.value. 20% remaining -> 80% consumed -> DEGRADED band."""
    factor = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _derived_wear_telemetry(
                          component="track", natural_value=8.0,
                          natural_unit="km", rul_percent=20.0,
                      ),
                      None, None),
        _thr(),
    )
    # _eval_wear returns a list (one factor per crossed-threshold component).
    assert len(factor) == 1
    assert factor[0].factor_id == "wear.track"
    assert factor[0].severity == ls.LOGISTICS_SEVERITY_DEGRADED
    assert factor[0].current_value.unit == "%"
    assert factor[0].current_value.value == pytest.approx(80.0)


def test_wear_percent_branch_critical():
    """5% remaining -> 95% consumed -> CRITICAL band."""
    factor = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _derived_wear_telemetry(
                          component="suspension", natural_value=95.0,
                          natural_unit="deg.km", rul_percent=5.0,
                      ),
                      None, None),
        _thr(),
    )
    assert len(factor) == 1
    assert factor[0].factor_id == "wear.suspension"
    assert factor[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


def test_wear_factor_stamps_origin_from_value_provenance():
    """Provenance read-through: when sustainment.value_provenance['*'] is
    ORIGIN_DERIVED, the emitted factor's origin matches. Phase 5 step 2
    structural distinction — replaces the description-string `(derived)`
    tag idea."""
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _derived_wear_telemetry(
                          component="track", natural_value=8.0,
                          natural_unit="km", rul_percent=20.0,
                      ),
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].origin == tel.ORIGIN_DERIVED
    assert factors[0].confidence == 0.0  # accurate value, not a placeholder


def test_wear_factor_origin_unspecified_for_legacy_measured_path():
    """Sustainment with NO value_provenance entry — the existing time-units
    measured path — gets origin=ORIGIN_UNSPECIFIED via proto3 default. The
    `'*' in value_provenance` guard prevents the proto-map autocreate
    quirk."""
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(wear={"transmission": (900.0, 100.0)}),  # 90%
                      None, None),
        _thr(),
    )
    assert len(factors) == 1
    assert factors[0].origin == tel.ORIGIN_UNSPECIFIED
    # And the legacy time-units path must still produce the correct severity.
    assert factors[0].severity == ls.LOGISTICS_SEVERITY_CRITICAL


# ===========================================================================
# Wear-component manifest — three states, and the middle one is the point
# ===========================================================================
# GD-14: every wear axis was derived for every asset that moved, so four
# helicopters reported 100.0% TRACK wear. The axis was applicable because
# the model could compute it, not because the platform had the part.
def test_declared_present_evaluates():
    rules.set_wear_manifest_for_test({"M1A2-SEPv3": {"track"}})
    assert rules.wear_axis_applicability("M1A2-SEPv3", "track") == "evaluate"


def test_declared_absent_is_not_applicable_not_unknown():
    """The distinction this whole manifest exists for.

    A helicopter has no tracks. Reporting that as UNKNOWN would claim the
    system is missing data about a component that is not there — an absence
    of a claim rendered as a claim of absence of data. Not-applicable says
    the question does not arise."""
    rules.set_wear_manifest_for_test({"UH-60M": {"engine"}})
    assert rules.wear_axis_applicability("UH-60M", "track") == "not_applicable"
    assert rules.wear_axis_applicability("UH-60M", "engine") == "evaluate"


def test_no_manifest_entry_is_unknown_not_fully_equipped():
    """An undeclared platform must not default to having everything — that
    default is precisely how helicopters came to have track wear."""
    rules.set_wear_manifest_for_test({"M1A2-SEPv3": {"track"}})
    assert rules.wear_axis_applicability("SOMETHING-UNDECLARED", "track") == "unknown"


def test_empty_manifest_makes_everything_unknown():
    """The file being absent or unreadable must fail toward UNKNOWN, never
    toward evaluating every axis for every platform."""
    rules.set_wear_manifest_for_test(None)
    for comp in ("track", "engine", "barrel", "suspension"):
        assert rules.wear_axis_applicability("M1A2-SEPv3", comp) == "unknown"


def test_undeclared_platform_emits_no_wear_factor():
    """End to end: the gate suppresses the factor rather than the value."""
    rules.set_wear_manifest_for_test({})
    factors = _eval_wear(
        FusionInputs(ASSET_ID, VARIANT,
                      _telemetry(wear={"track": (4900.0, 100.0)}),
                      None, None),
        Thresholds(),
    )
    assert factors == []
