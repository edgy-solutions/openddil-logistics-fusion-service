"""Tests for the tactical-event alert rule and the cause carried on it.

`_is_alerting_transition` is the feed's only gate: an upward rank change
into CRITICAL / NON_OPERATIONAL. `_top_factor` picks the worst factor (not
the first appended). `_publish_tactical_event` is driven with a fake ctx and
a captured Kafka publish.
"""
from __future__ import annotations

import asyncio
import json

from openddil.logistics.v1 import logistics_status_pb2 as ls
from workflows import asset_logistics
from workflows.asset_logistics import _is_alerting_transition, _top_factor

OK = ls.LOGISTICS_SEVERITY_OK
DEG = ls.LOGISTICS_SEVERITY_DEGRADED
CRIT = ls.LOGISTICS_SEVERITY_CRITICAL
NONOP = ls.LOGISTICS_SEVERITY_NON_OPERATIONAL


def _factor(fid: str, sev: int, desc: str = "") -> ls.ConstrainingFactor:
    return ls.ConstrainingFactor(factor_id=fid, severity=sev, description=desc)


def test_alerting_upward_transitions():
    assert _is_alerting_transition(DEG, CRIT) is True
    assert _is_alerting_transition(OK, NONOP) is True
    assert _is_alerting_transition(CRIT, NONOP) is True


def test_non_alerting_transitions():
    assert _is_alerting_transition(NONOP, CRIT) is False
    assert _is_alerting_transition(CRIT, DEG) is False
    assert _is_alerting_transition(DEG, DEG) is False
    assert _is_alerting_transition(None, CRIT) is False
    assert _is_alerting_transition(OK, DEG) is False


def test_top_factor_picks_highest_severity_earliest_on_tie():
    factors = [
        _factor("cm.overall_status", DEG, "cm degraded"),
        _factor("operational.fault", CRIT, "fault: pump seized"),
        _factor("condition.appearance_damage", CRIT, "damage moderate"),
    ]
    assert _top_factor(factors) == ("operational.fault", "fault: pump seized")


def test_top_factor_empty():
    assert _top_factor([]) == ("", "")


def test_top_factor_description_not_cut_by_helper_but_event_cuts_to_160():
    long = "x" * 300
    assert _top_factor([_factor("a", CRIT, long)])[1] == long


class _Ctx:
    async def run(self, label, fn):
        return fn()


def _drive(monkeypatch, factors, prev=DEG, new=CRIT) -> dict:
    captured: dict = {}

    def fake_publish(*, topic, key, value):
        captured["topic"] = topic
        captured["key"] = key
        captured["payload"] = json.loads(value)

    monkeypatch.setattr(asset_logistics, "_publish_kafka", fake_publish)
    asyncio.run(
        asset_logistics._publish_tactical_event(
            _Ctx(),
            asset_id="dis:9:9:1",
            new_severity=new,
            prev_severity=prev,
            revision=7,
            constraining_factors=factors,
            origin={"edge_id": "e1", "region_id": "r1"},
        )
    )
    return captured


def test_publish_carries_cause_and_previous(monkeypatch):
    factors = [
        _factor("cm.overall_status", DEG, "cm degraded"),
        _factor("operational.fault", CRIT, "y" * 300),
    ]
    got = _drive(monkeypatch, factors)
    data = got["payload"]["data"]
    assert got["topic"] == "tactical-events"
    assert data["top_factor"] == "operational.fault"
    assert data["top_factor_description"] == "y" * 160
    assert data["previous_severity"] == "LOGISTICS_SEVERITY_DEGRADED"
    assert data["severity"] == "LOGISTICS_SEVERITY_CRITICAL"
