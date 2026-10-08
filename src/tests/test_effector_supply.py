"""Pure tests for `fusion.effector_supply.apply_resupply`."""
from __future__ import annotations

from fusion import effector_supply
from fusion.effector_supply import apply_fire, apply_resupply

KEY = "2.9.225.2.1.1.0"
OTHER = "2.9.225.2.1.2.0"
MT = {"kind": 2, "domain": 9, "country": 225, "category": 2,
      "subcategory": 1, "specific": 1, "extra": 0}
MT_OTHER = dict(MT, specific=2)


def _supply(mt, quantity):
    return {"munition_type": mt, "quantity": quantity}


def test_partial_refill_lowers_expended():
    exp, counted, replay = apply_resupply(
        {KEY: 5}, [], event_urn="dis-resupply:1:1:5:10", supplies=[_supply(MT, 2)],
    )
    assert exp == {KEY: 3}
    assert counted == ["dis-resupply:1:1:5:10"]
    assert replay is False


def test_over_refill_clamps_at_zero():
    exp, _, _ = apply_resupply(
        {KEY: 3}, [], event_urn="u1", supplies=[_supply(MT, 50)],
    )
    assert exp == {KEY: 0}


def test_replay_is_a_noop():
    start = {KEY: 5}
    exp, counted, replay = apply_resupply(
        start, ["u1"], event_urn="u1", supplies=[_supply(MT, 2)],
    )
    assert replay is True
    assert exp == {KEY: 5}
    assert counted == ["u1"]


def test_never_fired_key_stays_absent_or_zero():
    exp, _, replay = apply_resupply(
        {KEY: 1}, [], event_urn="u1", supplies=[_supply(MT_OTHER, 4)],
    )
    assert replay is False
    assert exp.get(OTHER, 0) == 0
    assert exp[KEY] == 1


def test_multiple_supplies_in_one_pdu():
    exp, counted, _ = apply_resupply(
        {KEY: 4, OTHER: 2}, [], event_urn="u1",
        supplies=[_supply(MT, 3), _supply(MT_OTHER, 9)],
    )
    assert exp == {KEY: 1, OTHER: 0}
    assert counted == ["u1"]


def test_input_not_mutated():
    start, urns = {KEY: 5}, ["a"]
    apply_resupply(start, urns, event_urn="u1", supplies=[_supply(MT, 2)])
    assert start == {KEY: 5} and urns == ["a"]


def test_counted_urns_bounded_like_fire():
    full = [f"u{i}" for i in range(effector_supply.MAX_COUNTED_URNS)]
    _, counted, _ = apply_resupply(
        {}, full, event_urn="new", supplies=[_supply(MT, 1)],
    )
    assert len(counted) == effector_supply.MAX_COUNTED_URNS
    assert counted[-1] == "new" and "u0" not in counted


def test_fire_resupply_fire_sequence_remaining():
    declared = 8
    exp, counted = {}, []
    exp, counted, _ = apply_fire(
        exp, counted, event_urn="f1", munition_key=KEY, quantity=3)
    exp, counted, _ = apply_resupply(
        exp, counted, event_urn="r1", supplies=[_supply(MT, 5)])
    exp, counted, _ = apply_fire(
        exp, counted, event_urn="f2", munition_key=KEY, quantity=2)
    # 8 - 3 = 5, refill 5 clamps at declared (8), minus 2 fired = 6.
    assert declared - exp[KEY] == 6
