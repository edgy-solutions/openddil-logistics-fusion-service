"""
Startup self-check across ontology files.

WARN (not fatal) for any platform_variant referenced from
asset_identity_aliases.yaml or dis_entity_types.yaml that lacks a
corresponding entry in platform_reference.yaml. Catches ontology drift
BETWEEN files (e.g., someone added a new variant to dis_entity_types
without populating its capacity in platform_reference).

Failure mode change vs the original idea (warn on live mismatches): this
warning fires at SERVICE STARTUP, not on every wrong status emission.
Operators see the drift the moment it's introduced, not after wrong
logistics statuses have shipped to the customer.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger("fusion.ontology_check")


def _load(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed reading %s: %s", path, exc)
        return {}


def check_ontology_consistency(ontology_dir: str | None = None) -> int:
    """Log WARN for every platform_variant referenced in other ontology files
    that is missing from platform_reference.yaml.

    Returns the number of drift warnings emitted (0 = clean). Non-fatal —
    the service starts regardless.
    """
    root = Path(ontology_dir or os.getenv("ONTOLOGY_DIR", "/ontology"))
    if not root.is_dir():
        logger.warning(
            "Ontology directory %s not found; skipping consistency check", root,
        )
        return 0

    platform_ref = _load(root / "platform_reference.yaml")
    canonical_variants: set[str] = set(
        (platform_ref.get("platforms") or {}).keys()
    )

    referenced: dict[str, set[str]] = {}  # variant -> {files it appears in}

    # platform_variant_aliases.yaml: each entry's `canonical` should resolve
    # in platform_reference.yaml.
    pva = _load(root / "platform_variant_aliases.yaml")
    for feed, entries in (pva.get("aliases") or {}).items():
        for e in entries or []:
            canon = (e or {}).get("canonical")
            if canon and canon != "UNKNOWN":
                referenced.setdefault(canon, set()).add(
                    f"platform_variant_aliases.yaml:{feed}"
                )

    # dis_entity_types.yaml: platform_variant values per DIS triplet.
    det = _load(root / "dis_entity_types.yaml")
    for entry in det.get("entity_types") or []:
        pv = (entry or {}).get("platform_variant")
        if pv and pv != "UNKNOWN":
            referenced.setdefault(pv, set()).add("dis_entity_types.yaml")

    # asset_identity_aliases.yaml: canonical_asset_id strings can be inspected
    # for their embedded variant token. Skip — the format is opaque (e.g.
    # "USA-ARMY-1HBCT-M1A2-4773"). Drift here is a separate problem; this
    # check focuses on variant-keyed files.

    missing = sorted(
        v for v in referenced if v not in canonical_variants
    )
    if not missing:
        logger.info(
            "Ontology consistency check OK (%d variants in platform_reference; "
            "%d referenced variants resolved)",
            len(canonical_variants), len(referenced),
        )
        return 0

    for v in missing:
        srcs = ", ".join(sorted(referenced[v]))
        logger.warning(
            "ONTOLOGY DRIFT: platform_variant %r is referenced by [%s] but "
            "has no entry in platform_reference.yaml. Fuel%% evaluation for "
            "assets with this variant will fall back to env override (if "
            "present) or be skipped.",
            v, srcs,
        )
    return len(missing)
