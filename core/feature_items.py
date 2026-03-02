# -*- coding: utf-8 -*-
"""
core/feature_items.py

Typed, deterministic feature evidence for Phase-2 discovery.

Exporter responsibilities:
  - Emit cleaned values (typed).
  - Mark missing/unreadable/unsupported deterministically.
  - Avoid volatile identifiers (ElementId/UniqueId) in this surface.

Phase-2 responsibilities:
  - Compute distinct counts, presence, selectivity, Pareto, etc.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


_VALID_T = {"b", "i", "f", "s", "ref"}
_VALID_Q_PREFIXES = ("ok", "missing", "unreadable", "unsupported")


def make_feature_item(
    k: str,
    t: str,
    v: Any,
    q: str,
    *,
    ref_label: Optional[str] = None,
) -> Dict[str, Any]:
    if not isinstance(k, str) or not k.strip():
        raise ValueError("FeatureItem.k must be a non-empty string")

    if t not in _VALID_T:
        raise ValueError(f"FeatureItem.t invalid: {t!r}")

    if not isinstance(q, str) or not q:
        raise ValueError("FeatureItem.q must be a non-empty string")
    if not q.startswith(_VALID_Q_PREFIXES):
        raise ValueError(f"FeatureItem.q invalid prefix: {q!r}")

    item: Dict[str, Any] = {"k": k.strip(), "t": t, "q": q}

    # Keep v typed; allow None.
    item["v"] = v

    if t == "ref":
        # refs must not carry volatile ids; allow only stable label-ish info
        if ref_label is not None:
            if not isinstance(ref_label, str):
                raise ValueError("ref_label must be a string or None")
            item["ref_label"] = ref_label.strip() if ref_label.strip() else None

    return item