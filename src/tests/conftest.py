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
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_ONTOLOGY = _REPO_ROOT / "openddil-contracts" / "ontology"

if _ONTOLOGY.is_dir():
    os.environ.setdefault("ONTOLOGY_DIR", str(_ONTOLOGY))

# The generated protobuf bindings are not installed as a package; they are
# produced into openddil-contracts/gen/python and imported from there. In the
# container they arrive on PYTHONPATH; on a host running pytest, nothing puts
# them there, so `from openddil.common.v1 import quantity_pb2` fails AT
# COLLECTION and pytest aborts the whole run.
#
# cm-service solved this by repeating the same two sys.path.insert calls at
# the top of every test module. Same mechanism here, once, in the file that
# already exists to fix an import-time environment assumption.
#
# Kept as a sibling-repo-relative path deliberately: it is the same layout
# assumption `_ONTOLOGY` above already makes, so if the workspace is
# rearranged both break together and visibly, rather than one silently.
_GEN = _REPO_ROOT / "openddil-contracts" / "gen" / "python"
if _GEN.is_dir():
    sys.path.insert(0, str(_GEN))

# `fusion` itself is imported as a top-level package by the tests.
_SRC = Path(__file__).resolve().parents[1]
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))
