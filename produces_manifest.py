"""Expand produces.yaml topic templates. Stdlib + PyYAML only; self-contained so
other checkers can vendor it. Grammar: ${VAR}, ${VAR:-default} (default may nest
${...}), ${VAR|us} ('-' becomes '_'). An unset variable with no default raises."""
from __future__ import annotations

import yaml


def load(path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)["workloads"]


def _close(s: str, i: int) -> int:
    """Index of the '}' closing the '${' whose body starts at i."""
    depth = 1
    while i < len(s):
        if s.startswith("${", i):
            depth += 1
            i += 2
            continue
        if s[i] == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError(f"unterminated ${{ in {s!r}")


def expand(template: str, env: dict) -> str:
    out, i = [], 0
    while i < len(template):
        if not template.startswith("${", i):
            out.append(template[i])
            i += 1
            continue
        end = _close(template, i + 2)
        body = template[i + 2:end]
        i = end + 1
        name, sep, default = body.partition(":-")
        if sep:
            out.append(env[name] if env.get(name) else expand(default, env))
            continue
        name, bar, flt = body.partition("|")
        if bar and flt != "us":
            raise ValueError(f"unknown filter {flt!r} in {template!r}")
        if not env.get(name):
            raise KeyError(f"undefined ${{{name}}} with no default in {template!r}")
        out.append(env[name].replace("-", "_") if bar else env[name])
    return "".join(out)
