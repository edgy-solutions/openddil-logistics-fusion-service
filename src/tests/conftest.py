"""
Test environment setup.

`fusion.rules` reads `ONTOLOGY_DIR` at module import time to load
`platform_reference.yaml`. In the container, this is `/ontology` (mounted
volume). On the host during pytest, point it at the repo's ontology dir
so fuel%-from-volume tests have a capacity table to work against.

This file is imported by pytest BEFORE any test module, so the env var
takes effect ahead of the `from fusion.rules import ...` at the top of
test_rules.py.
"""
from __future__ import annotations

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_ONTOLOGY = _REPO_ROOT / "openddil-contracts" / "ontology"

if _ONTOLOGY.is_dir():
    os.environ.setdefault("ONTOLOGY_DIR", str(_ONTOLOGY))
