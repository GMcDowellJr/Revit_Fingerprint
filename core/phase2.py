# -*- coding: utf-8 -*-
"""
Phase-2 instrumentation helpers (export-time, additive-only).

Hard rules:
- No heuristics.
- No inference.
- Never emit legacy sentinel literals into IdentityItem.v.
- Keep behavior deterministic (sorted items, stable join-hash).
"""

from core.hashing import make_hash, safe_str
from core.canon import S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
from core.record_v2 import (
    canonicalize_str,
    canonicalize_str_allow_empty,
    serialize_identity_items,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
)

from core.record_v2 import (
    canonicalize_bool,
    canonicalize_int,
    ITEM_Q_UNREADABLE,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
)

def phase2_sorted_items(items):
    """Return IdentityItem-like dicts sorted by key 'k'."""
    return sorted(items or [], key=lambda it: it.get("k", ""))


def phase2_qv_from_legacy_sentinel_str(v, *, allow_empty=False):
    """
    Map legacy sentinel strings to record.v2-safe (v,q) without emitting sentinel literals.

    - S_MISSING -> (None, missing)
    - S_UNREADABLE -> (None, unreadable)
    - S_NOT_APPLICABLE -> (None, unsupported_not_applicable)
    - Otherwise: canonicalize via canonicalize_str / canonicalize_str_allow_empty
    """
    if v is None:
        return None, ITEM_Q_MISSING

    try:
        sv = safe_str(v)
    except Exception:
        return None, ITEM_Q_UNREADABLE

    if sv == S_MISSING:
        return None, ITEM_Q_MISSING
    if sv == S_UNREADABLE:
        return None, ITEM_Q_UNREADABLE
    if sv == S_NOT_APPLICABLE:
        return None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE

    if allow_empty:
        return canonicalize_str_allow_empty(sv)
    return canonicalize_str(sv)


def phase2_join_hash(identity_items):
    """
    Deterministic join-hash for Phase-2 joins.
    Expects caller to have already sorted items if desired; we sort defensively anyway.
    """
    items = phase2_sorted_items(identity_items)
    preimage = serialize_identity_items(items)
    return make_hash(preimage) if preimage is not None else None
