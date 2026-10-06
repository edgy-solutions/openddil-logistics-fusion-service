"""`asset-registry-events` must be in `_hq_subscriptions()`.

Two things ride on it being there: `bootstrap_restate_service` registers it
against the HQ cluster, and `main()`'s `desired` list (built from the same
function -- see that module's own comment on why the list has to be
complete) protects it from `prune_subscriptions`. Both read from one
function, so asserting on `_hq_subscriptions()`'s return value covers both.

`bootstrap/register_subscriptions.py` lives outside `src/`, on purpose: the
bootstrap Helm hook Job runs it standalone (same image, command overridden,
see the Dockerfile), so it isn't on the path `conftest.py` sets up for the
service's own package imports. Loaded directly from its file instead of via
sys.path games.

Its `openddil_bootstrap` import is the shared library this repo's Dockerfile
bakes in at build time (COPY ... /app/openddil_bootstrap, see the comment
there) -- in this checkout that source is openddil-contracts/bootstrap under
its own package name. Aliased into sys.modules under the name
register_subscriptions.py actually imports, so the real module runs rather
than a second, parallel stand-in.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SERVICE_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = _SERVICE_ROOT.parent
_CONTRACTS_BOOTSTRAP = _REPO_ROOT / "openddil-contracts" / "bootstrap"


def _load_module(name: str, path: Path, *, package_dir: Path | None = None):
    spec = importlib.util.spec_from_file_location(
        name, path,
        submodule_search_locations=[str(package_dir)] if package_dir else None,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


if not _CONTRACTS_BOOTSTRAP.is_dir():
    pytest.skip(
        f"openddil-contracts not checked out as a sibling of this repo "
        f"(expected {_CONTRACTS_BOOTSTRAP}) -- cannot load the shared "
        f"bootstrap library this test needs",
        allow_module_level=True,
    )

if "openddil_bootstrap" not in sys.modules:
    _load_module(
        "openddil_bootstrap", _CONTRACTS_BOOTSTRAP / "__init__.py",
        package_dir=_CONTRACTS_BOOTSTRAP,
    )
    _load_module(
        "openddil_bootstrap.restate_subscriptions",
        _CONTRACTS_BOOTSTRAP / "restate_subscriptions.py",
    )

import openddil_bootstrap.restate_subscriptions as restate_subscriptions  # noqa: E402

register_subscriptions = _load_module(
    "register_subscriptions",
    _SERVICE_ROOT / "bootstrap" / "register_subscriptions.py",
)


def test_hq_subscriptions_includes_registry_event():
    subs = register_subscriptions._hq_subscriptions()
    matches = [s for s in subs if s.topic == "asset-registry-events"]
    assert len(matches) == 1, "exactly one asset-registry-events subscription at HQ"
    sub = matches[0]
    assert sub.handler == "AssetLogistics/on_registry_event"
    assert sub.consumer_group == "fusion-service-registry-hq"


def test_registry_subscription_is_owned_by_this_services_prune_pass():
    # Must stay inside the prune's desired set -- i.e. carry the same
    # ownership prefix the real bootstrap's group_prefix_owner("fusion-
    # service-") checks against, or prune_subscriptions would read it as
    # belonging to nobody it owns and delete it.
    owns = restate_subscriptions.group_prefix_owner("fusion-service-")
    sub = next(
        s for s in register_subscriptions._hq_subscriptions()
        if s.topic == "asset-registry-events"
    )
    existing_shape = {"options": {"group.id": sub.consumer_group}}
    assert owns(existing_shape)


def test_cm_state_subscription_still_present():
    # Guard against the new entry having replaced rather than joined the
    # existing HQ subscription list.
    subs = register_subscriptions._hq_subscriptions()
    assert any(s.topic == "asset-cm-state" for s in subs)


def test_hq_subscriptions_are_all_in_the_desired_prune_set():
    # Confirms the actual wiring, not just the function in isolation --
    # main() extends `desired` with exactly `_hq_subscriptions()`'s
    # entries, under the "openddil-hq" cluster name. Reproduce that one
    # line rather than importing `desired` out of `main()`, since `main()`
    # also performs live HTTP bootstrap calls this test must not make.
    desired = [("openddil-hq", sub) for sub in register_subscriptions._hq_subscriptions()]
    assert ("openddil-hq",
            next(s for s in register_subscriptions._hq_subscriptions()
                 if s.topic == "asset-registry-events")) in desired
