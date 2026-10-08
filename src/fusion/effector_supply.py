"""Startup loader + pure helpers for fusion's own copy of the declared-load
table.

Fusion gets its launch counts from its own Restate subscription to
`effector-events` (mirroring `on_capability_snapshot`), keyed by launcher
asset id -- it does not read the projector's `effector_launch` table. But
the declared-load FILE is the same file the projector loads
(`EFFECTOR_DECLARED_LOAD_PATH`), because declared load is a property of the
launcher asset/variant, not of which service admits its Fire.

The two services share no library for this today, so the validation rules
below are copied from `openddil-projector/src/effector_declared_load.py`'s
`_validate_and_flatten` BYTE-FOR-BYTE (same regex, same error messages,
same fatal-at-startup discipline). Keep any change to one mirrored in the
other. What differs here is only the destination: the projector replaces a
Postgres table; fusion needs the parsed rows in-process, as a nested dict
`resolve_declared` can look up synchronously inside a Restate handler.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
import re

import yaml

log = logging.getLogger("fusion.effector_supply")

ENV_PATH = "EFFECTOR_DECLARED_LOAD_PATH"

# The wire/schema munition key: "k.d.c.cat.sub.spec.extra", seven dot-joined
# non-negative integers -- same shape `munition_type_key` below builds from
# the decoded DIS 7-tuple, and the same pattern the projector's loader uses.
_MUNITION_KEY_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+$")

_KEY_KINDS = ("asset", "variant")

# Bound on the per-launcher counted-Fire-urn set (`apply_fire` below). One
# compose or hero-test exercise fires single digits to low tens of rounds
# through any one launcher; 4096 is several orders of magnitude beyond
# anything a demo run or a hero scenario drives through one launcher object,
# so eviction here is a safety bound against an unbounded object, never a
# path this exercise is expected to reach.
MAX_COUNTED_URNS = 4096


class DeclaredLoadConfigError(Exception):
    """A readable EFFECTOR_DECLARED_LOAD_PATH file failed to validate.
    Fatal at startup -- same discipline as the projector's loader of the
    same file: a bad config is fatal, not a per-row skip discovered later
    as a silently-wrong remaining count."""


def _validate_and_flatten(raw: dict[str, Any]) -> list[tuple[str, str, str, int]]:
    """`{"asset": {...}, "variant": {...}}` -> [(load_key, key_kind,
    munition_type, declared), ...]. Raises `DeclaredLoadConfigError` naming
    the offending entry on the first problem found."""
    rows: list[tuple[str, str, str, int]] = []
    if not isinstance(raw, dict):
        raise DeclaredLoadConfigError(
            f"declared-load file must be a mapping at the top level, got "
            f"{type(raw).__name__}"
        )
    for key_kind in _KEY_KINDS:
        block = raw.get(key_kind)
        if block is None:
            continue
        if not isinstance(block, dict):
            raise DeclaredLoadConfigError(
                f"declared-load '{key_kind}' block must be a mapping, got "
                f"{type(block).__name__}"
            )
        for load_key, munitions in block.items():
            if not isinstance(munitions, dict):
                raise DeclaredLoadConfigError(
                    f"declared-load entry '{key_kind}.{load_key}' must map "
                    f"munition keys to counts, got {type(munitions).__name__}"
                )
            for munition_key, declared in munitions.items():
                entry_name = f"{key_kind}.{load_key}.{munition_key}"
                if not _MUNITION_KEY_RE.match(str(munition_key)):
                    raise DeclaredLoadConfigError(
                        f"declared-load entry '{entry_name}' has a malformed "
                        "munition key (want int.int.int.int.int.int.int)"
                    )
                if isinstance(declared, bool) or not isinstance(declared, int) or declared < 0:
                    raise DeclaredLoadConfigError(
                        f"declared-load entry '{entry_name}' has a negative "
                        f"or non-integer declared count: {declared!r}"
                    )
                rows.append((str(load_key), key_kind, str(munition_key), declared))
    return rows


def _rows_to_table(
    rows: list[tuple[str, str, str, int]],
) -> dict[str, dict[str, dict[str, int]]]:
    table: dict[str, dict[str, dict[str, int]]] = {"asset": {}, "variant": {}}
    for load_key, key_kind, munition_key, declared in rows:
        table[key_kind].setdefault(load_key, {})[munition_key] = declared
    return table


def load_declared_load(path: str | None = None) -> dict[str, dict[str, dict[str, int]]]:
    """Read EFFECTOR_DECLARED_LOAD_PATH (or `path`, for tests) into the
    in-memory `{"asset": {id: {munition_key: declared}}, "variant": {...}}`
    table `resolve_declared` looks up.

    No file, or the env var unset -- the table is emptied, so every lookup
    falls through to "nothing declared" (not "zero declared"). Same
    non-fatal default as the projector's loader for this file. A malformed
    entry raises `DeclaredLoadConfigError`; the caller (main.py) treats that
    as fatal at startup, same as the projector does.
    """
    if path is None:
        path = os.getenv(ENV_PATH, "").strip()
    rows: list[tuple[str, str, str, int]] = []
    if not path:
        log.info(
            "%s not set -- effector declared load table will be empty "
            "(every launcher's effector factor reads 'nothing declared')",
            ENV_PATH,
        )
    else:
        file = Path(path)
        if not file.is_file():
            log.warning(
                "%s=%s does not name a readable file -- effector declared "
                "load table will be empty", ENV_PATH, path,
            )
        else:
            raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
            rows = _validate_and_flatten(raw)
    table = _rows_to_table(rows)
    log.info("effector declared load: loaded %d row(s) from %s",
              len(rows), path or "(none configured)")
    return table


def resolve_declared(
    table: dict[str, dict[str, dict[str, int]]],
    *, asset_id: str, platform_variant: str,
) -> dict[str, int]:
    """Lookup order: an asset-keyed entry for `asset_id` wins; else a
    variant-keyed entry for `platform_variant`; else nothing declared
    (empty dict, not a dict of zeros)."""
    by_asset = table.get("asset") or {}
    if asset_id in by_asset:
        return dict(by_asset[asset_id])
    by_variant = table.get("variant") or {}
    if platform_variant and platform_variant in by_variant:
        return dict(by_variant[platform_variant])
    return {}


def munition_type_key(munition_type: dict[str, Any] | None) -> str:
    """The DIS 7-tuple dict (same shape `effector-events` records carry) ->
    "k.d.c.cat.sub.spec.extra". Byte-for-byte the same function as
    `openddil-projector/src/handlers/effector_launch.py`'s
    `munition_type_key` -- the two services must agree on this string, or a
    declared-load key written for one would silently never resolve for the
    other."""
    d = munition_type or {}
    parts = (
        d.get("kind", 0), d.get("domain", 0), d.get("country", 0),
        d.get("category", 0), d.get("subcategory", 0), d.get("specific", 0),
        d.get("extra", 0),
    )
    return ".".join(str(int(p)) for p in parts)


def apply_fire(
    expended: dict[str, int],
    counted_urns: list[str],
    *, event_urn: str, munition_key: str, quantity: int,
) -> tuple[dict[str, int], list[str], bool]:
    """Pure dedup + accumulate for a single Fire. Returns (new_expended,
    new_counted_urns, was_replay); never touches Restate state itself -- the
    handler owns ctx.get/ctx.set and decides what to do with the result.

    A urn already in `counted_urns` changes nothing (`was_replay=True`).
    Otherwise `quantity` is added to `expended[munition_key]` and the urn is
    appended, evicting the oldest urn once the set exceeds
    `MAX_COUNTED_URNS` (see its comment for why one exercise will not reach
    that bound)."""
    if event_urn in counted_urns:
        return dict(expended), list(counted_urns), True
    new_expended = dict(expended)
    new_expended[munition_key] = new_expended.get(munition_key, 0) + int(quantity)
    new_counted = list(counted_urns) + [event_urn]
    if len(new_counted) > MAX_COUNTED_URNS:
        new_counted = new_counted[-MAX_COUNTED_URNS:]
    return new_expended, new_counted, False


def apply_resupply(
    expended: dict[str, int],
    counted_urns: list[str],
    *, event_urn: str, supplies: list[dict[str, Any]],
) -> tuple[dict[str, int], list[str], bool]:
    """Pure dedup + refill for one Resupply Received. Same contract and
    same bounded `counted_urns` list as `apply_fire`.

    Each supply lowers `expended[munition_key]` by its quantity, floored at
    0, so remaining (declared - expended) never exceeds the declared load.
    A key that was never fired is left as it was (absent stays absent)."""
    if event_urn in counted_urns:
        return dict(expended), list(counted_urns), True
    new_expended = dict(expended)
    for supply in supplies:
        key = munition_type_key(supply.get("munition_type"))
        if key not in new_expended:
            continue
        new_expended[key] = max(0, new_expended[key] - int(supply.get("quantity", 0) or 0))
    new_counted = list(counted_urns) + [event_urn]
    if len(new_counted) > MAX_COUNTED_URNS:
        new_counted = new_counted[-MAX_COUNTED_URNS:]
    return new_expended, new_counted, False
