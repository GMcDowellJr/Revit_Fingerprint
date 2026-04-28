# -*- coding: utf-8 -*-
"""
Standalone Dynamo Python-node helper to purge selected entries from sys.modules
between runs, so edited project modules are re-imported without restarting Revit.

Intended for testing/development only.

Dynamo inputs (optional):
- IN[0]: prefixes to purge (string "a,b" or list). Default: ["runner", "core", "domains"]
- IN[1]: exact module names to purge (string "a,b" or list). Default: []
- IN[2]: dry run flag (True/False). Default: False

Dynamo output:
- OUT: dict with removed modules and counts
"""

import sys


# Conservative denylist: do not touch these roots.
_PROTECTED_ROOTS = set([
    "sys",
    "os",
    "json",
    "math",
    "clr",
    "Autodesk",
    "Revit",
    "RevitServices",
    "RevitNodes",
    "ProtoGeometry",
    "__main__",
])


def _to_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    try:
        if isinstance(value, (int, float)):
            return bool(int(value))
    except Exception:
        pass
    s = str(value).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return True
    if s in ("0", "false", "f", "no", "n", "off"):
        return False
    return default


def _is_protected(module_name):
    root = module_name.split(".", 1)[0]
    return root in _PROTECTED_ROOTS


def purge_modules(prefixes, exact_names, dry_run=False):
    prefixes = tuple(prefixes)
    exact_names = set(exact_names)

    to_remove = []
    for name in list(sys.modules.keys()):
        if not name or _is_protected(name):
            continue

        matched = (
            name in exact_names
            or any(name == p or name.startswith(p + ".") for p in prefixes)
        )
        if matched:
            to_remove.append(name)

    # Remove deep modules first for cleaner dependency reloads.
    to_remove.sort(key=lambda n: (-n.count("."), n))

    removed = []
    errors = {}
    if not dry_run:
        for name in to_remove:
            try:
                sys.modules.pop(name, None)
                removed.append(name)
            except Exception as exc:
                errors[name] = str(exc)

    return {
        "status": "dry_run" if dry_run else "ok",
        "prefixes": list(prefixes),
        "exact_names": sorted(list(exact_names)),
        "matched_count": len(to_remove),
        "removed_count": 0 if dry_run else len(removed),
        "error_count": len(errors),
        "removed_modules": [] if dry_run else removed,
        "matched_modules": to_remove if dry_run else [],
        "errors": errors,
        "note": "Development helper only. Avoid in production graphs.",
    }


_default_prefixes = ["runner", "core", "domains"]

try:
    _in = IN if "IN" in globals() else []
    _prefixes = _to_list(_in[0]) if len(_in) > 0 else []
    _exact = _to_list(_in[1]) if len(_in) > 1 else []
    _dry_run = _to_bool(_in[2], default=False) if len(_in) > 2 else False

    if not _prefixes:
        _prefixes = _default_prefixes

    OUT = purge_modules(_prefixes, _exact, dry_run=_dry_run)
except Exception as exc:
    OUT = {
        "status": "error",
        "error": str(exc),
        "note": "Check IN values and retry.",
    }
