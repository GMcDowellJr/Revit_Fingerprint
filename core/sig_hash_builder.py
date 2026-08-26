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


def _shape_gated_hash_keys(
    shape_gating: Optional[Dict[str, Any]],
    kmap: Dict[str, Dict[str, Any]],
) -> Tuple[frozenset, frozenset]:
    """Resolve (gated_keys, owned_keys) for a domain policy's `shape_gating` block.

    `gated_keys` is the union of every shape's `additional_required` +
    `additional_optional` across the whole `shape_requirements` map --
    i.e. every key whose hash participation depends on which shape this
    specific record is. `owned_keys` is the subset of those actually owned
    by *this* record's own shape (resolved from `kmap` via
    `discriminator_key`); empty if the shape doesn't match any entry
    (`default_shape_behavior: "common_only"` is the only mode this builder
    understands -- "block" is not implemented here since it's out of scope
    for D-049's fix).

    Only consulted when the policy opts in via
    `shape_gating.applies_to_sig_hash: true` (see D-049) -- this mirrors,
    for the analysis-side hash reconstruction, the same per-record
    bucket-ownership filter `domains/arrowheads.py`'s inline extractor
    already applies via its own `hash_bucket_keys`. Domains whose policy
    doesn't opt in (e.g. `identity`, whose `shape_gating` remains
    informational/discovery-only per its own policy notes) are completely
    unaffected -- this function returns `(frozenset(), frozenset())` for
    them, a no-op against `_key_allowed()`'s existing behavior.
    """
    if not isinstance(shape_gating, dict) or not shape_gating.get("applies_to_sig_hash"):
        return frozenset(), frozenset()

    shape_requirements = shape_gating.get("shape_requirements")
    if not isinstance(shape_requirements, dict):
        return frozenset(), frozenset()

    gated_keys = set()
    for req in shape_requirements.values():
        if not isinstance(req, dict):
            continue
        gated_keys.update(k for k in (req.get("additional_required") or []) if isinstance(k, str))
        gated_keys.update(k for k in (req.get("additional_optional") or []) if isinstance(k, str))

    owned_keys = set()
    disc_key = shape_gating.get("discriminator_key")
    if isinstance(disc_key, str):
        disc_item = kmap.get(disc_key)
        disc_v = disc_item.get("v") if isinstance(disc_item, dict) else None
        disc_q = disc_item.get("q") if isinstance(disc_item, dict) else None
        if disc_q == ITEM_Q_OK and disc_v is not None:
            shape_req = shape_requirements.get(str(disc_v).strip())
            if isinstance(shape_req, dict):
                owned_keys.update(k for k in (shape_req.get("additional_required") or []) if isinstance(k, str))
                owned_keys.update(k for k in (shape_req.get("additional_optional") or []) if isinstance(k, str))

    return frozenset(gated_keys), frozenset(owned_keys)


def build_sig_hash_from_policy(
    *,
    domain_policy: Dict[str, Any],
    items: Optional[Sequence[Dict[str, Any]]] = None,
    identity_items: Optional[Sequence[Dict[str, Any]]] = None,
    status_reasons: Optional[Sequence[str]] = None,
) -> Tuple[Optional[str], str, List[str], List[Dict[str, Any]]]:
    """Return (sig_hash, status, status_reasons, hash_items).

    The builder hashes every emitted identity item allowed by policy.
    Required items control block semantics (and degrade semantics on their
    own); any non-required item that is part of the hash preimage but is not
    q=ok also degrades status (never blocks) -- an incomplete item that
    contributes to the hash must not be silently invisible to the record's
    reported status.

    If the policy's `shape_gating` block sets `applies_to_sig_hash: true`,
    an `allowed_items` key that's also one of shape_gating's per-shape
    `additional_required`/`additional_optional` keys only feeds the hash
    when it's owned by *this* record's own shape (see
    `_shape_gated_hash_keys()`). This exists so a domain like `arrowheads`
    -- whose extractor now emits every style-specific field unconditionally
    into `identity_basis.items` (D-049), regardless of style bucket -- gets
    the same per-record bucket-gating applied here that the inline
    extractor already applies to its own hash, keeping this analysis-side
    reconstruction consistent with the extractor's inline `sig_hash` value.
    Policies that don't set this flag (the default) are unaffected.
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

    # D-049: a policy that opts in via shape_gating.applies_to_sig_hash further
    # restricts which allowed_items actually feed the hash for THIS record,
    # based on its own discriminator value -- see _shape_gated_hash_keys().
    # A key in gated_keys but not in owned_keys is allowed_items-eligible but
    # not owned by this record's shape, so it's excluded from hash_items
    # entirely (not merely hashed with its q, since it was never meant to
    # participate for this shape at all).
    gated_keys, owned_keys = _shape_gated_hash_keys(pol.get("shape_gating"), kmap)

    hash_items: List[Dict[str, Any]] = []
    for it in src_items or []:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not isinstance(k, str) or not _key_allowed(k, allowed, prefixes):
            continue
        if k in gated_keys and k not in owned_keys:
            continue
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

    optional_not_ok: List[str] = []
    for it in hash_items:
        k = it.get("k")
        q = it.get("q")
        if isinstance(k, str) and k not in required and q != ITEM_Q_OK:
            optional_not_ok.append(k)
            reasons.append("identity.incomplete:optional_not_ok:%s" % k)

    if required_not_ok or optional_not_ok:
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
