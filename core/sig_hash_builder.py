# -*- coding: utf-8 -*-
"""Policy-driven sig_hash creation for record.v2 records."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.hashing import make_hash
from core.record_v2 import (
    ITEM_Q_OK,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    serialize_identity_items,
)


def _items_to_map(items: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for it in items or []:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if isinstance(k, str) and k not in out:
            out[k] = it
    return out


def _key_allowed(k: str, allowed: Sequence[str], prefixes: Sequence[str]) -> bool:
    if k in set(allowed or []):
        return True
    for p in prefixes or []:
        if isinstance(p, str) and p and k.startswith(p):
            return True
    return False


def build_sig_hash_from_policy(
    *,
    domain_policy: Dict[str, Any],
    items: Optional[Sequence[Dict[str, Any]]] = None,
    identity_items: Optional[Sequence[Dict[str, Any]]] = None,
    status_reasons: Optional[Sequence[str]] = None,
) -> Tuple[Optional[str], str, List[str], List[Dict[str, Any]]]:
    """Return (sig_hash, status, status_reasons, hash_items).

    The builder hashes every emitted identity item allowed by policy. Required
    items control block/degrade semantics; they are not the full hash selector.
    """
    pol = domain_policy or {}
    allowed = list(pol.get("allowed_items") or [])
    prefixes = list(pol.get("allowed_item_prefixes") or [])
    required = list(pol.get("required_items") or [])
    minima = pol.get("minima") if isinstance(pol.get("minima"), dict) else {}
    block_if_any_required_not_ok = bool(minima.get("block_if_any_required_not_ok", True))

    src_items = items if items is not None else (identity_items or [])
    reasons = sorted({str(x) for x in (status_reasons or []) if str(x)})
    kmap = _items_to_map(src_items or [])

    hash_items: List[Dict[str, Any]] = []
    for it in src_items or []:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if isinstance(k, str) and _key_allowed(k, allowed, prefixes):
            hash_items.append({"k": k, "q": it.get("q"), "v": it.get("v")})

    required_qs: List[str] = []
    required_not_ok: List[str] = []
    for k in required:
        it = kmap.get(k)
        q = it.get("q") if isinstance(it, dict) else None
        required_qs.append(str(q) if q is not None else "missing")
        if q != ITEM_Q_OK:
            required_not_ok.append(k)
            reasons.append("identity.incomplete:required_not_ok:%s" % k)

    if required_not_ok and block_if_any_required_not_ok:
        preimage = serialize_identity_items(hash_items)
        blocked_hash = make_hash(preimage) if hash_items else None
        return blocked_hash, STATUS_BLOCKED, sorted(set(reasons)), hash_items

    if required_not_ok:
        status = STATUS_DEGRADED
    else:
        status = STATUS_OK

    preimage = serialize_identity_items(hash_items)
    return make_hash(preimage), status, sorted(set(reasons)), hash_items


def apply_sig_hash_policy_to_record(record: Dict[str, Any], domain_policy: Dict[str, Any]) -> Dict[str, Any]:
    """Mutate and return a canonical record dict with policy-generated sig_hash/status."""
    if not isinstance(record, dict):
        return record
    items = record.get("items") if isinstance(record.get("items"), list) else []
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
        domain_policy=domain_policy,
        items=items,
        status_reasons=record.get("status_reasons") if isinstance(record.get("status_reasons"), list) else [],
    )
    record["status"] = status
    record["status_reasons"] = reasons
    record["sig_hash"] = sig_hash
    record["sig_basis"] = {
        "schema": str(domain_policy.get("sig_hash_schema") or ""),
        "keys_used": sorted([it.get("k") for it in hash_items if isinstance(it.get("k"), str)]),
        "hash_alg": str(domain_policy.get("hash_alg") or "md5_utf8_join_pipe"),
    }
    return record
