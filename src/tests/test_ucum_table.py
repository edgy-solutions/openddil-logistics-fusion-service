"""Fusion reads the one shared UCUM->pint table (openddil.common.v1.ucum).

One test per consumer in fusion.rules: the capacity table (platform_reference
ontology), the spec table (FUEL_CAPACITY_BY_VARIANT env) and
_quantity_to_pint. Each names what that consumer did with "Cel" before the
import: its own copy had no such entry and pint has no unit called "Cel".
"""
from __future__ import annotations

import json

import pytest
from openddil.common.v1 import quantity_pb2 as qpb
from openddil.common.v1 import ucum

from fusion import rules


def _q(value, unit):
    q = qpb.Quantity()
    q.value = value
    q.unit = unit
    return q


def _kelvin(value):
    return rules.ureg.Quantity(value, "degC").to("kelvin").magnitude


# Identity: before the change rules held its own dict, so `is` failed.
def test_rules_uses_the_shared_table_object():
    assert rules._UCUM_TO_PINT is ucum.UCUM_TO_PINT


# Every shared code resolves, through rules' own table, to a unit in fusion's
# registry. Before the change the copy had no "Cel" entry.
@pytest.mark.parametrize("code", sorted(ucum.UCUM_TO_PINT))
def test_every_table_value_parses_in_fusion_ureg(code):
    rules.ureg.Unit(rules._UCUM_TO_PINT[code])


# _quantity_to_pint: with "Cel" it warned and returned None.
def test_quantity_to_pint_reads_cel_as_degc():
    pq = rules._quantity_to_pint(_q(92.0, "Cel"))
    assert pq is not None
    assert pq.to("kelvin").magnitude == _kelvin(92.0)
    assert pq.to("kelvin").magnitude == pytest.approx(365.15)


def _ontology(tmp_path, monkeypatch, variant, value, unit):
    (tmp_path / "platform_reference.yaml").write_text(
        f"platforms:\n  {variant}:\n    fuel_capacity:\n"
        f"      value: {value}\n      unit: \"{unit}\"\n", encoding="utf-8")
    monkeypatch.setattr(rules, "ONTOLOGY_DIR", str(tmp_path))
    monkeypatch.delenv("FUEL_CAPACITY_BY_VARIANT", raising=False)


# Capacity table (ontology path): a "Cel" capacity was logged as malformed and
# dropped, so the variant was absent.
def test_capacity_table_from_ontology_reads_cel(tmp_path, monkeypatch):
    _ontology(tmp_path, monkeypatch, "V1", 92.0, "Cel")
    out = rules._load_fuel_capacity_table()
    assert out["V1"].to("kelvin").magnitude == _kelvin(92.0)


# Spec table (FUEL_CAPACITY_BY_VARIANT env path): a "Cel" entry raised out of
# the loader.
def test_spec_table_from_env_reads_cel(tmp_path, monkeypatch):
    monkeypatch.setattr(rules, "ONTOLOGY_DIR", str(tmp_path))  # no yaml file
    monkeypatch.setenv("FUEL_CAPACITY_BY_VARIANT",
                       json.dumps({"V2": {"value": 92.0, "unit": "Cel"}}))
    out = rules._load_fuel_capacity_table()
    assert out["V2"].to("kelvin").magnitude == _kelvin(92.0)


# Old versus new, per consumer. RED-EXEMPT: the old 4-entry table is frozen
# here verbatim and old and new code agree on it, so these pass on both. "Cel"
# is not in the old table and is covered by the tests above.
_OLD_TABLE = {
    "[degF]": "degF",
    "[kn_i]": "knot",
    "gal_us": "gallon",
    "1": "dimensionless",
}
_MAGNITUDES = [0.0, -40.0, 1.5, 92.0, 1234.5]


def _same(new, code, value):
    old = rules.ureg.Quantity(value, _OLD_TABLE.get(code, code)).to_base_units()
    new = new.to_base_units()
    assert new.magnitude == old.magnitude
    assert new.units == old.units


@pytest.mark.parametrize("value", _MAGNITUDES)
@pytest.mark.parametrize("code", sorted(_OLD_TABLE))
def test_old_codes_as_before_through_quantity_to_pint(code, value):
    # (0.0, "1") is a legal quantity: only an empty unit means unset.
    _same(rules._quantity_to_pint(_q(value, code)), code, value)


@pytest.mark.parametrize("value", _MAGNITUDES)
@pytest.mark.parametrize("code", sorted(_OLD_TABLE))
def test_old_codes_as_before_through_capacity_table(code, value, tmp_path,
                                                    monkeypatch):
    _ontology(tmp_path, monkeypatch, "V1", value, code)
    _same(rules._load_fuel_capacity_table()["V1"], code, value)


@pytest.mark.parametrize("value", _MAGNITUDES)
@pytest.mark.parametrize("code", sorted(_OLD_TABLE))
def test_old_codes_as_before_through_spec_table(code, value, tmp_path,
                                                monkeypatch):
    monkeypatch.setattr(rules, "ONTOLOGY_DIR", str(tmp_path))
    monkeypatch.setenv("FUEL_CAPACITY_BY_VARIANT",
                       json.dumps({"V2": {"value": value, "unit": code}}))
    _same(rules._load_fuel_capacity_table()["V2"], code, value)
