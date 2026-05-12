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
    groups: List[Iterable[Dict[str, Any]]] = []
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
    if domain and isinstance(src.get(domain), Mapping):
        src = src.get(domain)  # type: ignore[assignment]

    # Precompiled shape: {"item.k": "identity"}
    if all(isinstance(k, str) and isinstance(v, str) for k, v in src.items()):
        return {str(k): str(v) for k, v in src.items()}

    out: Dict[str, str] = {}
    for role, keys in src.items():
        if not isinstance(role, str) or not isinstance(keys, Sequence):
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
