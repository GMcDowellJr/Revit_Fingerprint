# -*- coding: utf-8 -*-
"""
core/stratum_features.py

Discovery-only surface for comparison strata / groups.

Design constraints:
  - Deterministic (stable ordering, no volatile ids).
  - Domain-agnostic: consumes identity_basis.items (k/q/v triples).
  - Non-authoritative: for Phase-2 exploration and diagnostics only.
  - Does NOT modify join keys or signature hashing behavior.

Export location:
  record.v2.debug.stratum_features
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


STRATUM_FEATURES_SCHEMA_V1 = "stratum_features.v1"


def _s(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x)
    except Exception:
        return None
    s = s.strip()
    return s if s else None


def _first_value_for_keys(
    identity_items: Sequence[Dict[str, Any]],
    keys: Sequence[str],
) -> Optional[str]:
    keyset = set(keys)
    for it in identity_items:
        k = _s(it.get("k"))
        if not k or k not in keyset:
            continue
        return _s(it.get("v"))
    return None


def _select_discriminators(identity_items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Conservative discriminator picker based on key suffixes.
    This is intentionally minimal: it should not become a shadow join key.
    """
    allow_suffixes: Tuple[str, ...] = (
        ".family",
        ".shape",
        ".kind",
        ".type_class",
        ".category",
        ".subcategory",
        ".view_type",
        ".discipline",
        ".spec_type",
        ".unit_system",
        ".origin",
    )

    out: List[Dict[str, Any]] = []
    for it in identity_items:
        k = _s(it.get("k"))
        if not k or not k.endswith(allow_suffixes):
            continue
        q = _s(it.get("q")) or "ok"
        v = _s(it.get("v"))
        out.append({"k": k, "q": q, "v": v})

    out.sort(key=lambda d: d.get("k") or "")
    return out


def build_stratum_features_v1(
    *,
    domain: str,
    identity_items: Sequence[Dict[str, Any]],
    group: Optional[str] = None,
    shape: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a universal stratum_features payload.

    group/shape inference rules (domain-agnostic):
      - group: prefer explicit arg; else first of keys [*.family, *.group, *.category]
      - shape: prefer explicit arg; else first of keys [*.shape, *.view_type]
    """
    dom = _s(domain) or ""

    inferred_group = _first_value_for_keys(
        identity_items,
        keys=(
            "dim_type.family",
            "record.group",
            "record.category",
        ),
    )

    inferred_shape = _first_value_for_keys(
        identity_items,
        keys=(
            "dim_type.shape",
            "view_template.view_type",
        ),
    )

    grp = _s(group) or inferred_group
    shp = _s(shape) or inferred_shape

    return {
        "schema_version": STRATUM_FEATURES_SCHEMA_V1,
        "domain": dom,
        "group": grp,
        "shape": shp,
        "discriminators": _select_discriminators(identity_items),
    }