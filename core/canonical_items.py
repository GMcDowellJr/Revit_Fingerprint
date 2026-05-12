# -*- coding: utf-8 -*-
"""Canonical flat-item helpers.

These helpers support migration from legacy bucketed phase2 payloads to
canonical flat extracted records:

    {"items": [{"k": ..., "v": ..., "q": ...}]}

Extractors emit facts only; role assignment is resolved at runtime from policy.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


LEGACY_BUCKET_KEYS: Tuple[str, ...] = (
    "semantic_items",
    "cosmetic_items",
    "coordination_items",
    "unknown_items",
)


def _normalize_item(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    k = item.get("k")
    if not isinstance(k, str) or not k:
        return None
    return {"k": k, "v": item.get("v"), "q": item.get("q")}


def build_flat_items(*item_groups: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build canonical flat items preserving first-seen key authority.

    Single-pass merge over inputs:
    - keeps first item per key
    - drops malformed/non-dict/non-key items
    - output sorted by key for deterministic serialization
    """
    out: List[Dict[str, Any]] = []
    seen = set()
    for group in item_groups:
        for raw in group or []:
            item = _normalize_item(raw)
            if item is None:
                continue
            k = item["k"]
            if k in seen:
                continue
            seen.add(k)
            out.append(item)
    return sorted(out, key=lambda it: it["k"])


def merge_legacy_buckets(phase2_payload: Mapping[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Convert legacy phase2 bucket payload into canonical flat items envelope."""
    payload = phase2_payload if isinstance(phase2_payload, Mapping) else {}
    existing_items = payload.get("items")
    groups: List[Iterable[Dict[str, Any]]] = []
    if isinstance(existing_items, list):
        groups.append(existing_items)
    for key in LEGACY_BUCKET_KEYS:
        val = payload.get(key)
        if isinstance(val, list):
            groups.append(val)
    return {"items": build_flat_items(*groups)}


def compile_role_policy(policy: Mapping[str, Any], *, domain: Optional[str] = None) -> Dict[str, str]:
    """Compile policy to key->role lookup map.

    Supports either:
    - domain-scoped role->list[k] shape, or
    - direct k->role mapping (already compiled).
    """
    src = policy if isinstance(policy, Mapping) else {}
    if domain:
        if isinstance(src.get(domain), Mapping):
            src = src.get(domain)  # type: ignore[assignment]
        elif isinstance(src.get("domains"), Mapping) and isinstance(src.get("domains", {}).get(domain), Mapping):
            src = src.get("domains", {}).get(domain)  # type: ignore[assignment]

    # Precompiled shape: {"item.k": "identity"}
    if all(isinstance(k, str) and isinstance(v, str) for k, v in src.items()):
        return {str(k): str(v) for k, v in src.items()}

    out: Dict[str, str] = {}
    for role, keys in src.items():
        if not isinstance(role, str) or isinstance(keys, str) or not isinstance(keys, Sequence):
            continue
        for k in keys:
            if isinstance(k, str) and k and k not in out:
                out[k] = role
    return out


def resolve_item_roles(items: Sequence[Mapping[str, Any]], role_lookup: Mapping[str, str]) -> Dict[str, List[Dict[str, Any]]]:
    """Resolve roles from item.k via runtime lookup.

    Returns grouped items without mutating input items and without adding a role
    field into canonical extracted JSON.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {
        "identity": [],
        "cosmetic": [],
        "coordination": [],
        "unknown": [],
    }
    for it in items or []:
        k = it.get("k") if isinstance(it, Mapping) else None
        if not isinstance(k, str):
            continue
        role = role_lookup.get(k, "unknown")
        if role not in grouped:
            role = "unknown"
        grouped[role].append({"k": k, "v": it.get("v"), "q": it.get("q")})
    return grouped


def canonicalize_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Canonicalize a record to flat `items` shape and remove legacy/derived keys."""
    out = dict(record) if isinstance(record, Mapping) else {}
    existing_items = out.get("items") if isinstance(out.get("items"), list) else []
    ib = out.get("identity_basis") if isinstance(out.get("identity_basis"), Mapping) else {}
    identity_items = ib.get("items") if isinstance(ib.get("items"), list) else []
    phase2 = out.get("phase2") if isinstance(out.get("phase2"), Mapping) else {}
    out["items"] = build_flat_items(
        existing_items,
        identity_items,
        phase2.get("semantic_items", []) if isinstance(phase2.get("semantic_items"), list) else [],
        phase2.get("lineage_items", []) if isinstance(phase2.get("lineage_items"), list) else [],
        phase2.get("cosmetic_items", []) if isinstance(phase2.get("cosmetic_items"), list) else [],
        phase2.get("coordination_items", []) if isinstance(phase2.get("coordination_items"), list) else [],
        phase2.get("unknown_items", []) if isinstance(phase2.get("unknown_items"), list) else [],
    )
    for it in out.get("items", []):
        if isinstance(it, dict):
            it.pop("role", None)
    for k in ("identity_basis", "phase2", "join_key", "sig_hash", "sig_basis", "identity_quality", "record_id_alg", "record_id_scope", "schema_version"):
        out.pop(k, None)
    return out
