# -*- coding: utf-8 -*-
"""
Deterministic JSON comparison helpers (pure CPython).

Policy:
- Dict key order is normalized (sorted) for hashing/pretty output.
- List order is treated as meaningful (no sorting), because many domain
  outputs rely on stable ordering semantics.
- Diff output is bounded to avoid huge console spam.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple


def _canon_obj(obj: Any) -> Any:
    """Return an object that is stable under json.dumps(sort_keys=True)."""
    if isinstance(obj, dict):
        # recurse; don't sort here—json.dumps(sort_keys=True) handles ordering
        return {str(k): _canon_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_canon_obj(v) for v in obj]
    return obj


def canonical_json_bytes(obj: Any) -> bytes:
    """Canonical JSON encoding used for hashing and deterministic file writes."""
    canon = _canon_obj(obj)
    s = json.dumps(canon, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return s.encode("utf-8")


def sha256_of_json(obj: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(obj)).hexdigest()


def pretty_json(obj: Any) -> str:
    canon = _canon_obj(obj)
    return json.dumps(canon, sort_keys=True, ensure_ascii=False, indent=2)


def diff_paths(a: Any, b: Any, *, max_diffs: int = 200) -> List[Dict[str, Any]]:
    """
    Structural diff returning a bounded list of differing paths.

    Each diff record:
      { "path": "$.foo[0].bar", "a_type": "...", "b_type": "...", "a": <preview>, "b": <preview> }
    """
    out: List[Dict[str, Any]] = []

    def _preview(x: Any) -> Any:
        # Keep previews small and JSON-friendly
        try:
            if isinstance(x, (str, int, float, bool)) or x is None:
                return x
            if isinstance(x, list):
                return {"_list_len": len(x)}
            if isinstance(x, dict):
                return {"_dict_keys": sorted(list(x.keys()))[:20], "_dict_len": len(x)}
            return str(x)
        except Exception:
            return "<unpreviewable>"

    def _walk(x: Any, y: Any, path: str) -> None:
        if len(out) >= max_diffs:
            return

        if type(x) is not type(y):
            out.append(
                {
                    "path": path,
                    "a_type": type(x).__name__,
                    "b_type": type(y).__name__,
                    "a": _preview(x),
                    "b": _preview(y),
                }
            )
            return

        if isinstance(x, dict):
            xk = set(x.keys())
            yk = set(y.keys())
            for k in sorted(xk - yk):
                if len(out) >= max_diffs:
                    return
                out.append(
                    {"path": f"{path}.{k}", "a_type": type(x[k]).__name__, "b_type": "<missing>", "a": _preview(x[k]), "b": "<missing>"}
                )
            for k in sorted(yk - xk):
                if len(out) >= max_diffs:
                    return
                out.append(
                    {"path": f"{path}.{k}", "a_type": "<missing>", "b_type": type(y[k]).__name__, "a": "<missing>", "b": _preview(y[k])}
                )
            for k in sorted(xk & yk):
                _walk(x[k], y[k], f"{path}.{k}")
            return

        if isinstance(x, list):
            if len(x) != len(y):
                out.append(
                    {
                        "path": path,
                        "a_type": "list",
                        "b_type": "list",
                        "a": {"len": len(x)},
                        "b": {"len": len(y)},
                    }
                )
                if len(out) >= max_diffs:
                    return
            n = min(len(x), len(y))
            for i in range(n):
                _walk(x[i], y[i], f"{path}[{i}]")
                if len(out) >= max_diffs:
                    return
            return

        # primitives / fallback compare
        if x != y:
            out.append(
                {
                    "path": path,
                    "a_type": type(x).__name__,
                    "b_type": type(y).__name__,
                    "a": _preview(x),
                    "b": _preview(y),
                }
            )

    _walk(a, b, "$")
    return out


def compare_json(a: Any, b: Any, *, max_diffs: int = 200) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (equal, summary)
    summary includes stable hashes and bounded diffs.
    """
    ha = sha256_of_json(a)
    hb = sha256_of_json(b)
    if ha == hb:
        return True, {"equal": True, "sha256_a": ha, "sha256_b": hb, "diffs": []}

    diffs = diff_paths(a, b, max_diffs=max_diffs)
    return False, {"equal": False, "sha256_a": ha, "sha256_b": hb, "diffs": diffs, "diff_count": len(diffs)}
