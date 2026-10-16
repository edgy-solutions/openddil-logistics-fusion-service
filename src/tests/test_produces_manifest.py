"""The code must produce exactly the topics produces.yaml lists.

AST scan of the shipped code under src/ (tests excluded). bootstrap/ is an
operator/hook tool, not the service workload, and is not scanned.

Producer call sites: `.send(`, `.send_and_wait(`, `.produce(`, and this repo's
own publish wrapper `_publish_kafka(` (workflows/asset_logistics.py). The one transport
that receives the topic as a parameter, `publish` in main.py, is the sink the
wrapper feeds; it is skipped by name, so the wrapper's callers carry the topic.
Each remaining site's topic must resolve statically (literal, module constant,
os.environ.get / os.getenv) or carry `# produces: <manifest entry>`.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import produces_manifest as pm  # noqa: E402

MANIFEST = pm.load(ROOT / "produces.yaml")
METHODS = {"send", "send_and_wait", "produce"}
WRAPPERS = {"_publish_kafka"}
TRANSPORTS = {("src/main.py", "publish")}  # (file, function) taking topic as a parameter
ANNOTATION = re.compile(r"#\s*produces:\s*(.+?)\s*$")
ENV = {}  # fixture env: every code default applies


def _parents(tree):
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node


def _fn(node):
    while node is not None and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        node = getattr(node, "parent", None)
    return node


def _env_template(call):
    """os.environ.get("X", "d") / os.getenv("X", "d") -> '${X:-d}', else None."""
    f = call.func
    ok = isinstance(f, ast.Attribute) and (
        (f.attr == "getenv" and isinstance(f.value, ast.Name) and f.value.id == "os")
        or (f.attr == "get" and isinstance(f.value, ast.Attribute) and f.value.attr == "environ"))
    if not ok or not call.args or not isinstance(call.args[0], ast.Constant):
        return None
    var = call.args[0].value
    if len(call.args) > 1 and isinstance(call.args[1], ast.Constant) and isinstance(call.args[1].value, str):
        return f"${{{var}:-{call.args[1].value}}}"
    return f"${{{var}}}"


def _scan():
    templates, bad, sites = set(), [], {}
    for path in sorted((ROOT / "src").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if "/tests/" in rel or path.name.startswith("test_"):
            continue
        src = path.read_text(encoding="utf-8")
        lines = src.splitlines()
        tree = ast.parse(src)
        _parents(tree)
        consts = {}
        for n in tree.body:
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                if isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
                    consts[n.targets[0].id] = n.value.value
                elif isinstance(n.value, ast.Call) and _env_template(n.value):
                    consts[n.targets[0].id] = _env_template(n.value)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not ((isinstance(f, ast.Attribute) and f.attr in METHODS)
                    or (isinstance(f, ast.Name) and f.id in WRAPPERS)
                    or (isinstance(f, ast.Attribute) and f.attr in WRAPPERS)):
                continue
            fn = _fn(node)
            if fn is not None and (rel, fn.name) in TRANSPORTS:
                continue
            sites[rel] = sites.get(rel, 0) + 1
            span = lines[max(node.lineno - 2, 0):node.end_lineno]
            note = next((m.group(1).strip("\"'") for m in map(ANNOTATION.search, span) if m), None)
            if note:
                templates.add(note)
                continue
            kw = {k.arg: k.value for k in node.keywords}
            arg = kw.get("topic") or (node.args[0] if node.args else None)
            got = None
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                got = arg.value
            elif isinstance(arg, ast.Name):
                got = consts.get(arg.id)
            elif isinstance(arg, ast.Call):
                got = _env_template(arg)
            if got:
                templates.add(got)
            else:
                bad.append(f"{rel}:{node.lineno}")
    return templates, bad, sites


def test_code_produces_exactly_the_manifest():
    templates, bad, sites = _scan()
    assert sites, "no producer call sites found: the scan is not looking at the code"
    assert not bad, ("unresolvable producer call sites (annotate with '# produces: <entry>'): "
                     + ", ".join(bad))
    got = {pm.expand(t, ENV) for t in templates}
    declared = {pm.expand(t, ENV) for w in MANIFEST.values() for t in w["topics"]}
    msg = [f"manifest lists a topic the code does not produce: {t}" for t in sorted(declared - got)]
    msg += [f"code produces a topic the manifest does not list: {t}" for t in sorted(got - declared)]
    assert not msg, "\n".join(msg)


def test_manifest_is_exactly_the_fusion_image():
    assert set(MANIFEST) == {"fusion"}
    assert {w["image"] for w in MANIFEST.values()} == {"openddil/logistics-fusion-service"}
