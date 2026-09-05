"""Fusion propagates releasability labels; it never derives them.

ADR-0029 §3 stamps labels ONCE at ingress. Fusion's contribution is the
JOIN — it knows which asset a derived row is about, so it knows which
labels that row inherits. What it must never acquire is a declaration of
its own: two places that can decide a label are two answers to a question
that must have exactly one.

The extractor is tested against ALL FOUR inbound shapes because this system
has two different MessageToDict settings live at once, so the same field
arrives as `originator_nation` or `originatorNation` depending on the path.
`_extract_origin` already carries that scar; these tests keep the sibling
from acquiring it silently.
"""
from __future__ import annotations

from workflows.asset_logistics import _extract_releasability


def test_cm_state_envelope_top_level_snake_case():
    """asset-cm-state is JSON not proto (ADR-0018); cm-service stamps at the
    envelope's top level via dataclasses.asdict."""
    assert _extract_releasability(
        {"originator_nation": "BDR", "releasable_to": ["ATL"]}
    ) == ("BDR", ["ATL"])


def test_proto_derived_camel_case():
    """MessageToDict(preserving_proto_field_name=False)."""
    assert _extract_releasability(
        {"provenance": {"originatorNation": "ATL", "releasableTo": ["BDR"]}}
    ) == ("ATL", ["BDR"])


def test_proto_derived_snake_case():
    """MessageToDict(preserving_proto_field_name=True). Both decoders are
    live in this system; a reader that handled only one would work for some
    inbound topics and silently drop labels on others."""
    assert _extract_releasability(
        {"provenance": {"originator_nation": "ATL", "releasable_to": []}}
    ) == ("ATL", [])


def test_declared_nation_with_empty_release_is_not_absence():
    """An empty releasable_to beside a real nation is the common coalition
    posture, not a missing label. Conflating them would drop the label of
    every asset released to nobody — which in the demo is every asset."""
    nation, releasable = _extract_releasability(
        {"provenance": {"originator_nation": "ATL", "releasable_to": []}}
    )
    assert nation == "ATL"
    assert releasable == []


def test_unlabelled_event_yields_no_label():
    """The absence path. Fusion must NOT invent a label for an asset whose
    telemetry never carried one — the §7 gate is supposed to catch that, and
    a default here would hide the exact thing the gate exists to surface.
    The fix belongs at the ingress that failed to declare the asset."""
    assert _extract_releasability({"provenance": {"producer_id": "x"}}) == ("", [])
    assert _extract_releasability({}) == ("", [])
    assert _extract_releasability(None) == ("", [])


def test_extractor_does_not_confuse_origin_with_releasability():
    """Guard against the obvious refactor: folding this into _extract_origin.
    An event carrying edge attribution but no nation is UNLABELLED, and an
    origin fallback must not become a label fallback — origin legitimately
    defaults to an env value, a label never may."""
    assert _extract_releasability(
        {"edge_id": "edge-01", "region_id": "region-east"}
    ) == ("", [])
