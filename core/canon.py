# -*- coding: utf-8 -*-
"""
Core canonicalization and formatting utilities (pure Python, no Revit API).

PR3 scope (behavior-changing):
- Single source of truth for canonical primitives.
- Single set of sentinel strings:
    <MISSING>, <UNREADABLE>, <NOT_APPLICABLE>

Policy:
- Domains must emit canonicalized primitives (strings) and/or the declared sentinels.
- Do not introduce new angle-bracket sentinels in domains.
"""

from __future__ import annotations

from typing import Any, Optional

from .hashing import safe_str


# =========================
# Sentinel strings (SoT)
# =========================

S_MISSING = "<MISSING>"
S_UNREADABLE = "<UNREADABLE>"
S_NOT_APPLICABLE = "<NOT_APPLICABLE>"


def is_sentinel(v: Any) -> bool:
    return v in (S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE)


# =========================
# Canonical primitives
# =========================

def canon_str(v: Any) -> str:
    """Canonicalize string-like values.

    Rules:
    - None -> <MISSING>
    - str(...) conversion failure -> <UNREADABLE>
    - Strip leading/trailing whitespace
    - Empty-after-strip -> <MISSING>
    - Normalize legacy tokens: <None> -> <MISSING>, <Unreadable> -> <UNREADABLE>
    """
    if v is None:
        return S_MISSING
    try:
        s = safe_str(v)
    except Exception:
        return S_UNREADABLE

    if s is None:
        return S_UNREADABLE

    s2 = s.strip()
    if not s2:
        return S_MISSING

    if s2 == "<None>":
        return S_MISSING
    if s2 == "<Unreadable>":
        return S_UNREADABLE

    return s2


def canon_bool(v: Any) -> str:
    """Canonicalize booleans.

    - None -> <MISSING>
    - bool -> "true"/"false"
    - int-like 0/1 -> "false"/"true"
    - Anything else -> <UNREADABLE>
    """
    if v is None:
        return S_MISSING
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        if v == 0:
            return "false"
        if v == 1:
            return "true"
    return S_UNREADABLE


def canon_num(v: Any, nd: int = 9) -> str:
    """Canonicalize numbers to a fixed decimal string.

    - None -> <MISSING>
    - Conversion failure -> <UNREADABLE>
    """
    if v is None:
        return S_MISSING
    try:
        f = float(v)
        return format(f, f".{int(nd)}f")
    except Exception:
        return S_UNREADABLE


def canon_id(v: Any) -> str:
    """Canonicalize Revit ElementId-like values to a decimal string.

    Accepts:
    - ElementId (IntegerValue)
    - int
    - numeric string

    - None -> <MISSING>
    - Anything else / failure -> <UNREADABLE>
    """
    if v is None:
        return S_MISSING

    try:
        iv = getattr(v, "IntegerValue", None)
        if iv is not None:
            return str(int(iv))
    except Exception:
        return S_UNREADABLE

    try:
        if isinstance(v, int):
            return str(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return S_MISSING
            return str(int(s))
        return str(int(v))
    except Exception:
        return S_UNREADABLE


# =========================
# Back-compat helpers (deprecated; kept to reduce churn)
# =========================

def fnum(v: Any, nd: int) -> str:
    """Legacy alias for canon_num."""
    return canon_num(v, nd=nd)


def rgb_sig_from_color(col: Any) -> str:
    """Create RGB signature string from a Revit Color object."""
    if col is None:
        return S_MISSING
    try:
        return "{},{},{}".format(int(col.Red), int(col.Green), int(col.Blue))
    except Exception:
        return S_UNREADABLE


def rgb_dict_from_color(col: Any) -> Optional[dict]:
    """Create RGB dict from a Revit Color object.

    Returns None on missing/unreadable (dict path stays non-sentinel).
    """
    if col is None:
        return None
    try:
        return {"r": int(col.Red), "g": int(col.Green), "b": int(col.Blue)}
    except Exception:
        return None
