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

# ---------------------------------------------------------------------------
# THE STUBS MUST EXIST, AND THEIR ABSENCE MUST SAY SO
# ---------------------------------------------------------------------------
# `if _GEN.is_dir(): sys.path.insert(...)` was silent when the directory was
# absent, and silence is the whole problem: what the developer then saw was
# either `ModuleNotFoundError: No module named 'openddil'` at collection —
# which aborts pytest before any test runs, so it is NO SIGNAL rather than a
# set of failures — or, worse, a suite that ran and failed one test with
# `assert '2' == 'LIFECYCLE_ACTIVE'`, which reads like a real defect in the
# handler and is not.
#
# Neither message names the cause. This does, and it names the command.
#
# The check is on the GENERATED MODULES, not on the directory: `gen/python`
# exists and is empty after a fresh checkout of openddil-contracts, because
# `gen/` is gitignored there and built on demand. A directory test passes in
# exactly the situation this guard exists to catch.
_stub = _GEN / "openddil" / "telemetry" / "v1" / "telemetry_pb2.py"
if not _stub.exists():
    raise RuntimeError(
        "the generated protobuf bindings are missing, so this suite cannot "
        "collect.\n\n"
        "  expected: " + str(_stub) + "\n\n"
        "  generate them with:\n"
        "    cd " + str(_GEN.parents[1]) + "\n"
        "    python -m pip install grpcio-tools\n"
        "    mkdir -p gen/python\n"
        "    python -m grpc_tools.protoc --proto_path=proto "
        "--python_out=gen/python $(find proto -name '*.proto')\n\n"
        "`gen/` is gitignored in openddil-contracts and built on demand, so a "
        "fresh checkout has an EMPTY tree there. This is GD-13: the generated "
        "code is a contract artifact consumed across repository boundaries "
        "and is not published as a package, so every consumer reproduces this "
        "step."
    )

# `fusion` itself is imported as a top-level package by the tests.
_SRC = Path(__file__).resolve().parents[1]
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))
