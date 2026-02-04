# -*- coding: utf-8 -*-
"""core/record_v2.py

Shared utilities for constructing record.v2 artifacts.

Hard constraints (project-level):
  - Pure Python (no Revit API).
  - Deterministic.
  - Explicit failure signaling (no silent sentinel injection).

This module is intended to be imported by domains in later PRs.
It must NOT change existing domain outputs by itself.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.hashing import safe_str


# =========================
# Contract constants (defaults)
# =========================

SCHEMA_VERSION_RECORD_V2 = "record.v2"
IDENTITY_ITEM_SCHEMA_V1 = "identity_items.v1"

ITEM_Q_OK = "ok"
ITEM_Q_MISSING = "missing"
ITEM_Q_UNREADABLE = "unreadable"
ITEM_Q_UNSUPPORTED = "unsupported"

# "unsupported" needs subtyping to distinguish exporter gaps vs valid N/A.
# These strings remain within IdentityItem.q to keep the meaning local to the item.
ITEM_Q_UNSUPPORTED_NOT_APPLICABLE = "unsupported.not_applicable"
ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED = "unsupported.not_implemented"

VALID_ITEM_QS = {
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED,
}

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_BLOCKED = "blocked"
VALID_STATUSES = {STATUS_OK, STATUS_DEGRADED, STATUS_BLOCKED}

IDENTITY_QUALITY_COMPLETE = "complete"
IDENTITY_QUALITY_INCOMPLETE_MISSING = "incomplete_missing"
IDENTITY_QUALITY_INCOMPLETE_UNREADABLE = "incomplete_unreadable"
IDENTITY_QUALITY_INCOMPLETE_UNSUPPORTED = "incomplete_unsupported"
IDENTITY_QUALITY_NONE_BLOCKED = "none_blocked"

DEFAULT_IDENTITY_QUALITY_DOMINANCE_ORDER: Tuple[str, ...] = (
    IDENTITY_QUALITY_NONE_BLOCKED,
    IDENTITY_QUALITY_INCOMPLETE_UNREADABLE,
    IDENTITY_QUALITY_INCOMPLETE_UNSUPPORTED,
    IDENTITY_QUALITY_INCOMPLETE_MISSING,
    IDENTITY_QUALITY_COMPLETE,
)

# NOTE: Defaults match contracts/domain_identity_keys_v2.json. Callers may override.
DEFAULT_BANNED_IDENTITY_VALUE_SUBSTRINGS: Tuple[str, ...] = (
    "<MISSING>",
    "<UNREADABLE>",
    "<NOT_APPLICABLE>",
    "<LP:UNMAPPED>",
)


# =========================
# Canonicalization helpers
# =========================

def canonicalize_str(v: Any) -> Tuple[Optional[str], str]:
    """Canonicalize a string-like value for IdentityItem.v.

    Returns:
        (value_or_none, q)

    Rules:
      - None -> (None, "missing")
      - Conversion error -> (None, "unreadable")
      - Strip whitespace
      - Empty-after-strip -> (None, "missing")
    """
    if v is None:
        return None, ITEM_Q_MISSING

    try:
        s = str(v)
    except Exception:
        return None, ITEM_Q_UNREADABLE

    s2 = s.strip()
    if not s2:
        return None, ITEM_Q_MISSING

    return s2, ITEM_Q_OK

def canonicalize_str_allow_empty(v: Any) -> Tuple[Optional[str], str]:
    """Canonicalize a string-like value, but preserve empty string as a valid value.

    Returns:
        (value_or_none, q)

    Rules:
      - None -> (None, "missing")
      - Conversion error -> (None, "unreadable")
      - Strip whitespace
      - Empty-after-strip -> ("", "ok")
    """
    if v is None:
        return None, ITEM_Q_MISSING

    try:
        s = str(v)
    except Exception:
        return None, ITEM_Q_UNREADABLE

    s2 = s.strip()
    if s2 == "":
        return "", ITEM_Q_OK
    return s2, ITEM_Q_OK


def canonicalize_int(v: Any) -> Tuple[Optional[str], str]:
    """Canonicalize an integer-like value for IdentityItem.v.

    Returns:
        (value_or_none, q)

    Rules:
      - None -> (None, "missing")
      - bool -> (None, "unreadable")  # avoid implicit True==1
      - int -> decimal string
      - float -> accepted only if finite and integral
      - str -> accepted only if strip()+int(...) succeeds
      - otherwise -> unreadable
    """
    if v is None:
        return None, ITEM_Q_MISSING

    if isinstance(v, bool):
        return None, ITEM_Q_UNREADABLE

    try:
        if isinstance(v, int):
            return str(v), ITEM_Q_OK

        if isinstance(v, float):
            if not math.isfinite(v):
                return None, ITEM_Q_UNREADABLE
            if float(v).is_integer():
                return str(int(v)), ITEM_Q_OK
            return None, ITEM_Q_UNREADABLE

        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None, ITEM_Q_MISSING
            return str(int(s)), ITEM_Q_OK

        # last resort: try int(...) directly
        return str(int(v)), ITEM_Q_OK
    except Exception:
        return None, ITEM_Q_UNREADABLE


def canonicalize_float(v: Any, *, nd: int = 9) -> Tuple[Optional[str], str]:
    """Canonicalize a float-like value for IdentityItem.v.

    Returns:
        (value_or_none, q)

    Rules:
      - None -> (None, "missing")
      - bool -> (None, "unreadable")
      - finite float conversion -> fixed decimal string with nd places
      - nan/inf -> unreadable
    """
    if v is None:
        return None, ITEM_Q_MISSING

    if isinstance(v, bool):
        return None, ITEM_Q_UNREADABLE

    try:
        f = float(v)
        if not math.isfinite(f):
            return None, ITEM_Q_UNREADABLE
        return format(f, f".{int(nd)}f"), ITEM_Q_OK
    except Exception:
        return None, ITEM_Q_UNREADABLE


def canonicalize_bool(v: Any) -> Tuple[Optional[str], str]:
    """Canonicalize boolean values for IdentityItem.v.

    Returns:
        ("true"|"false"|None, q)
    """
    if v is None:
        return None, ITEM_Q_MISSING

    if isinstance(v, bool):
        return ("true" if v else "false"), ITEM_Q_OK

    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if v == 0:
            return "false", ITEM_Q_OK
        if v == 1:
            return "true", ITEM_Q_OK
        return None, ITEM_Q_UNREADABLE

    if isinstance(v, str):
        s = v.strip().lower()
        if not s:
            return None, ITEM_Q_MISSING
        if s in {"true", "t", "yes", "y", "1"}:
            return "true", ITEM_Q_OK
        if s in {"false", "f", "no", "n", "0"}:
            return "false", ITEM_Q_OK
        return None, ITEM_Q_UNREADABLE

    return None, ITEM_Q_UNREADABLE


def canonicalize_enum(v: Any) -> Tuple[Optional[str], str]:
    """Canonicalize enum-like values for IdentityItem.v.

    This is intentionally conservative:
      - None -> missing
      - Python Enum -> name
      - otherwise -> str(v) (strip) if non-empty, else missing
    """
    if v is None:
        return None, ITEM_Q_MISSING

    # Late import to avoid cost if unused.
    try:
        import enum

        if isinstance(v, enum.Enum):
            name = getattr(v, "name", None)
            if isinstance(name, str) and name:
                return name, ITEM_Q_OK
            return None, ITEM_Q_UNREADABLE
    except Exception:
        # If enum module import fails (unlikely), fall back to str conversion.
        pass

    return canonicalize_str(v)


# =========================
# Record ID helpers
# =========================

def make_record_id_from_element(elem: Any) -> Optional[Tuple[str, str]]:
    """Create a stable record_id from a Revit element.

    Priority:
      1) UniqueId -> "uid:<UniqueId>"
      2) ElementId.IntegerValue -> "eid:<int>"

    Returns:
        (record_id, record_id_alg) or None if unavailable.
    """
    if elem is None:
        return None

    uid_raw = None
    try:
        uid_raw = getattr(elem, "UniqueId", None)
    except Exception:
        uid_raw = None
    uid_v, uid_q = canonicalize_str(uid_raw)
    if uid_q == ITEM_Q_OK and uid_v:
        return f"uid:{uid_v}", "revit_uniqueid_v1"

    eid_raw = None
    try:
        eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
    except Exception:
        eid_raw = None
    eid_v, eid_q = canonicalize_int(eid_raw)
    if eid_q == ITEM_Q_OK and eid_v:
        return f"eid:{eid_v}", "revit_elementid_v1"

    return None


def _canonical_structural_value(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, bool)):
        return v

    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError("non-finite float in structural fields")
        return format(v, ".9f")

    if isinstance(v, (list, tuple)):
        return [_canonical_structural_value(x) for x in v]

    if isinstance(v, (set, frozenset)):
        canon = [_canonical_structural_value(x) for x in v]
        return sorted(canon, key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=True))

    if isinstance(v, dict):
        out: Dict[str, Any] = {}
        for k in sorted(v.keys(), key=lambda x: str(x)):
            out[str(k)] = _canonical_structural_value(v[k])
        return out

    raise ValueError(f"unsupported structural field type: {type(v)}")


def canonical_structural_fields(fields: Dict[str, Any]) -> str:
    """Canonicalize structural fields into a deterministic JSON string."""
    if not isinstance(fields, dict):
        raise TypeError("structural fields must be a dict")
    canon = _canonical_structural_value(fields)
    return json.dumps(canon, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def make_record_id_structural(structural_fields: Dict[str, Any]) -> Tuple[str, str, str]:
    """Create a structural-hash record_id base and canonical preimage."""
    canon = canonical_structural_fields(structural_fields)
    digest = hashlib.md5(canon.encode("utf-8")).hexdigest()
    return f"sh1:{digest}", "structural_hash_v1", canon


def _default_record_id_secondary_key(rec: Dict[str, Any]) -> str:
    items = rec.get("identity_items")
    preimage = serialize_identity_items(items) if isinstance(items, (list, tuple)) else []
    label = rec.get("label", {}) if isinstance(rec.get("label"), dict) else {}
    label_display = safe_str(label.get("display", ""))
    return canonical_structural_fields(
        {
            "label_display": label_display,
            "identity_preimage": preimage,
            "status": safe_str(rec.get("status", "")),
            "status_reasons": sorted([safe_str(x) for x in rec.get("status_reasons", []) if x]),
        }
    )


def finalize_record_ids_for_domain(records: List[Dict[str, Any]]) -> None:
    """Assign dup_index for structural record_id groups deterministically.

    Mutates records in-place. Expects structural candidates to include:
      - record_id_alg == "structural_hash_v1"
      - record_id_base
      - record_id_sort_key (string)
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        if rec.get("record_id_alg") == "structural_hash_v1":
            base_id = rec.get("record_id_base") or rec.get("record_id")
            if base_id:
                rec["record_id_base"] = base_id
                groups.setdefault(base_id, []).append(rec)

    for base_id, group in groups.items():
        if len(group) == 1:
            group[0]["record_id"] = base_id
            continue

        for rec in group:
            if not isinstance(rec.get("record_id_sort_key"), str):
                rec["record_id_sort_key"] = None
            if not isinstance(rec.get("record_id_sort_key_secondary"), str):
                rec["record_id_sort_key_secondary"] = _default_record_id_secondary_key(rec)

        keys = [
            (rec.get("record_id_sort_key"), rec.get("record_id_sort_key_secondary"))
            for rec in group
        ]
        if any(k[0] is None or k[1] is None for k in keys):
            for rec in group:
                _block_record_for_unstable_id(rec)
            continue

        if len(set(keys)) < len(group):
            for rec in group:
                _block_record_for_unstable_id(rec)
            continue

        group_sorted = sorted(group, key=lambda r: (r["record_id_sort_key"], r["record_id_sort_key_secondary"]))
        for idx, rec in enumerate(group_sorted):
            rec["record_id"] = f"{base_id}:{idx:03d}"


def _block_record_for_unstable_id(rec: Dict[str, Any]) -> None:
    reasons = set([safe_str(x) for x in rec.get("status_reasons", []) if x])
    reasons.add("unstable_record_id_no_structural_key")
    rec["status"] = STATUS_BLOCKED
    rec["status_reasons"] = sorted(reasons)
    rec["sig_hash"] = None


# =========================
# Identity item construction + serialization
# =========================

def make_identity_item(
    k: str,
    v: Optional[str],
    q: str,
    *,
    banned_substrings: Sequence[str] = DEFAULT_BANNED_IDENTITY_VALUE_SUBSTRINGS,
) -> Dict[str, Any]:
    """Construct an IdentityItem with a banned-substring guard.

    Contract invariant:
      - Sentinel literals MUST NOT appear in identity values.

    Raises:
        ValueError on invalid k/q or banned substring in v.
    """
    if not isinstance(k, str) or not k.strip():
        raise ValueError("IdentityItem.k must be a non-empty string")

    if q not in VALID_ITEM_QS:
        raise ValueError(f"IdentityItem.q invalid: {q!r}")

    vv: Optional[str]
    if v is None:
        vv = None
    else:
        if not isinstance(v, str):
            raise ValueError("IdentityItem.v must be a string or None")
        vv = v.strip()
        # IMPORTANT: empty string may be a valid semantic value for some keys (e.g. prefix/suffix).
        # Do NOT collapse "" to None or rewrite q; the caller controls q via canonicalizers/policy.
        # (If you want "blank means missing" semantics, use canonicalize_str(), not this constructor.)

    if isinstance(vv, str):
        for b in banned_substrings or []:
            if b and b in vv:
                raise ValueError(f"IdentityItem.v contains banned substring: {b!r}")

    return {"k": k.strip(), "v": vv, "q": q}


def serialize_identity_items(items: Sequence[Dict[str, Any]]) -> List[str]:
    """Serialize items into the authoritative preimage strings.

    Determinism:
      - Always sorts by k (lexicographically) before serialization.
    """
    if not isinstance(items, (list, tuple)):
        raise TypeError("items must be a sequence")

    def _k(it: Dict[str, Any]) -> str:
        try:
            return str(it.get("k", ""))
        except Exception:
            return ""

    out: List[str] = []
    for it in sorted(items, key=_k):
        k = it.get("k", "")
        q = it.get("q", "")
        v = it.get("v", None)
        v_or_empty = "" if v is None else v
        out.append(f"k={k}|q={q}|v={v_or_empty}")
    return out


def compute_identity_quality(
    status: str,
    required_qs: Iterable[str],
    *,
    dominance_order: Sequence[str] = DEFAULT_IDENTITY_QUALITY_DOMINANCE_ORDER,
) -> str:
    """Compute record.v2 identity_quality from required key qualities.

    Args:
        status: record status
        required_qs: q values for required keys only
        dominance_order: worst->best ordering of identity_quality values

    Returns:
        identity_quality
    """
    if status == STATUS_BLOCKED:
        return IDENTITY_QUALITY_NONE_BLOCKED

    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status!r}")

    qs = list(required_qs)

    present: List[str] = [IDENTITY_QUALITY_COMPLETE]
    if any(q == ITEM_Q_MISSING for q in qs):
        present.append(IDENTITY_QUALITY_INCOMPLETE_MISSING)
    if any(q == ITEM_Q_UNREADABLE for q in qs):
        present.append(IDENTITY_QUALITY_INCOMPLETE_UNREADABLE)
    if any(q == ITEM_Q_UNSUPPORTED for q in qs):
        present.append(IDENTITY_QUALITY_INCOMPLETE_UNSUPPORTED)

    # Choose the *worst* present, using the provided dominance order.
    dom = list(dominance_order)
    if not dom:
        raise ValueError("dominance_order must be non-empty")

    idx: Dict[str, int] = {name: i for i, name in enumerate(dom)}
    missing = [x for x in present if x not in idx]
    if missing:
        raise ValueError(f"dominance_order missing identity_quality values: {missing!r}")

    worst = min(present, key=lambda x: idx[x])
    return worst


# =========================
# Record construction
# =========================

def build_record_v2(
    *,
    domain: str,
    record_id: str,
    record_id_alg: str = "legacy_unspecified_v1",
    record_id_scope: str = "file_local",
    status: str,
    status_reasons: Sequence[str],
    sig_hash: Optional[str],
    identity_items: Sequence[Dict[str, Any]],
    required_qs: Sequence[str],
    label: Dict[str, Any],
    hash_alg: str = "md5_utf8_join_pipe",
    item_schema: str = IDENTITY_ITEM_SCHEMA_V1,
    schema_version: str = SCHEMA_VERSION_RECORD_V2,
    dominance_order: Sequence[str] = DEFAULT_IDENTITY_QUALITY_DOMINANCE_ORDER,
    debug: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble a record.v2 structure.

    This helper does not compute sig_hash; callers supply sig_hash.
    It does, however, compute identity_quality from required_qs and status.
    """
    if schema_version != SCHEMA_VERSION_RECORD_V2:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION_RECORD_V2!r}")

    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status!r}")

    # Enforce sig_hash nullability rule.
    if status == STATUS_BLOCKED:
        if sig_hash is not None:
            raise ValueError("blocked records must have sig_hash=None")
    else:
        if sig_hash is None:
            raise ValueError("non-blocked records must have sig_hash")

    identity_quality = compute_identity_quality(
        status,
        required_qs,
        dominance_order=dominance_order,
    )

    rec: Dict[str, Any] = {
        "schema_version": schema_version,
        "domain": str(domain),
        "record_id": str(record_id),
        "record_id_alg": str(record_id_alg),
        "record_id_scope": str(record_id_scope),
        "status": status,
        "status_reasons": [str(x) for x in status_reasons],
        "sig_hash": sig_hash,
        "identity_basis": {
            "hash_alg": str(hash_alg),
            "item_schema": str(item_schema),
            "items": list(identity_items),
        },
        "identity_quality": identity_quality,
        "label": dict(label) if isinstance(label, dict) else {},
    }

    if debug is not None:
        if not isinstance(debug, dict):
            raise TypeError("debug must be a dict if provided")
        rec["debug"] = debug

    return rec


def block_record_v2(
    *,
    domain: str,
    record_id: str,
    status_reasons: Sequence[str],
    identity_items: Sequence[Dict[str, Any]],
    label: Dict[str, Any],
    hash_alg: str = "md5_utf8_join_pipe",
    item_schema: str = IDENTITY_ITEM_SCHEMA_V1,
    schema_version: str = SCHEMA_VERSION_RECORD_V2,
    debug: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Convenience helper to build a blocked record.v2.

    Sets:
      - status = blocked
      - sig_hash = None
      - identity_quality = none_blocked
    """
    return build_record_v2(
        domain=domain,
        record_id=record_id,
        status=STATUS_BLOCKED,
        status_reasons=status_reasons,
        sig_hash=None,
        identity_items=identity_items,
        required_qs=(),
        label=label,
        hash_alg=hash_alg,
        item_schema=item_schema,
        schema_version=schema_version,
        dominance_order=DEFAULT_IDENTITY_QUALITY_DOMINANCE_ORDER,
        debug=debug,
    )
