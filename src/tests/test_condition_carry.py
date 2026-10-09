"""The DIS condition must travel from the record to the rules.

A DIS record carries no sustainment, so it never lands in the stored
telemetry; what reaches the rules from it is the remembered operational
state. The condition rides the same path under its own key, with per-source
last-writer-wins semantics. These tests go through the real handler and the
real rules, spying on the inputs the rules were handed.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import pytest

from fusion.thresholds import Thresholds
from workflows import asset_logistics
from workflows.asset_logistics import on_proprietary_update

_ASSET = "dis:1:1:5001"
_NOW_NS = int(datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc).timestamp()
              * 1_000_000_000)


class StubCtx:
    def __init__(self, key: str, now_ns: int):
        self._key = key
        self._now_ns = now_ns
        self._state: dict[str, object] = {}

    def key(self) -> str:
        return self._key

    def time(self):
        return datetime.fromtimestamp(self._now_ns / 1_000_000_000,
                                      tz=timezone.utc)

    async def get(self, name, type_hint=None):
        return self._state.get(name)

    def set(self, name, value) -> None:
        self._state[name] = value

    def clear(self, name) -> None:
        self._state.pop(name, None)

    async def run(self, label, fn):
        return fn()

    def object_send(self, handler, *, key, arg, send_delay=None):
        pass


@pytest.fixture(autouse=True)
def _wiring():
    asset_logistics.set_thresholds(Thresholds())
    asset_logistics.set_kafka_publisher(lambda topic, key, value: None)
    asset_logistics._condition_parse_warned.clear()
    yield


@pytest.fixture
def seen(monkeypatch):
    """(inputs, status) for every real rules call the handler makes."""
    calls: list[tuple[object, object]] = []
    real = asset_logistics.compute_logistics_status

    def spy(inputs, thresholds, now_ns):
        status = real(inputs, thresholds, now_ns)
        calls.append((inputs, status))
        return status

    monkeypatch.setattr(asset_logistics, "compute_logistics_status", spy)
    return calls


def _ctx() -> StubCtx:
    ctx = StubCtx(_ASSET, _NOW_NS)
    # A DIS-only asset has a telemetry_proto only through the derived record.
    ctx._state[asset_logistics._KEY_DERIVED_TELEMETRY] = {
        "eventId": "derived-1",
        "asset": {"assetId": _ASSET, "platformVariant": "M1A2-SEPv3"},
        "sustainment": {"fluids": {"fuelRemaining": {"value": 80.0,
                                                     "unit": "%"}}},
        "provenance": {"sourceProtocol": "derived"},
    }
    return ctx


def _dis(source="DIS-SIM", condition=None, level="CONDITION_LEVEL_CRITICAL"):
    op = {"healthState": "HEALTH_STATE_NOMINAL"}
    if condition is None and condition is not False:
        condition = {"claims": [{"source": "CONDITION_SOURCE_DATA_HEALTH",
                                 "level": level}]}
    if condition:
        op["condition"] = condition
    return {
        "eventId": "evt",
        "asset": {"assetId": _ASSET},
        "operationalState": op,
        "sustainment": {"health": {}},
        "provenance": {"sourceProtocol": source},
    }


def _send(ctx, record):
    asyncio.run(on_proprietary_update(ctx, record))


def _cond_factors(status):
    return [f for f in status.constraining_factors
            if f.factor_id.startswith("condition.")]


def test_condition_reaches_rules_and_raises_factor(seen):
    ctx = _ctx()
    _send(ctx, _dis())
    inputs, status = seen[-1]
    claims = inputs.latest_telemetry.operational_state.condition.claims
    assert len(claims) == 1
    factors = _cond_factors(status)
    assert [f.factor_id for f in factors] == ["condition.data_health"]
    assert factors[0].severity == asset_logistics.ls.LOGISTICS_SEVERITY_CRITICAL


def test_same_source_without_condition_clears_it(seen):
    ctx = _ctx()
    _send(ctx, _dis())
    assert asset_logistics._KEY_CONDITION in ctx._state
    _send(ctx, _dis(condition=False))
    assert asset_logistics._KEY_CONDITION not in ctx._state
    inputs, status = seen[-1]
    assert not inputs.latest_telemetry.operational_state.HasField("condition")
    assert _cond_factors(status) == []


def test_other_source_without_condition_keeps_it(seen):
    ctx = _ctx()
    _send(ctx, _dis(source="DIS-SIM"))
    _send(ctx, _dis(source="other-feed", condition=False))
    assert ctx._state[asset_logistics._KEY_CONDITION]["source"] == "DIS-SIM"
    _, status = seen[-1]
    assert [f.factor_id for f in _cond_factors(status)] == [
        "condition.data_health"]


def test_malformed_condition_does_not_crash_and_warns_once(seen, caplog):
    ctx = _ctx()
    bad = {"claims": [{"source": "CONDITION_SOURCE_DATA_HEALTH",
                       "level": "CONDITION_LEVEL_NOT_A_LEVEL"}]}
    with caplog.at_level(logging.WARNING, logger="logistics.asset_logistics"):
        _send(ctx, _dis(condition=bad))
        _send(ctx, _dis(condition=bad))
    _, status = seen[-1]
    assert _cond_factors(status) == []
    warned = [r for r in caplog.records if "condition" in r.getMessage()
              and "did not parse" in r.getMessage()]
    assert len(warned) == 1
