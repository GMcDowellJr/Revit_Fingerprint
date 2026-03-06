# -*- coding: utf-8 -*-
"""core/vg_sig.py

Shared helpers for view_templates domain extractors.

These functions are extracted from the core extraction loop in view_templates.py
and shared across the view-type-partitioned domain files. All helpers are pure
Python - no Revit API dependency.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.record_v2 import (
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    canonicalize_int,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
)
from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)


def _phase2_items_from_def_signature(def_signature):
    """Convert legacy def_signature entries ('k=v') into IdentityItems safely.

    Hard rule: do not emit legacy sentinel literals into IdentityItem.v.
    """
    out = []
    for s in (def_signature or []):
        try:
            ss = safe_str(s)
        except Exception:
            continue

        if "=" not in ss:
            k = "view_template.sig.{}".format(ss)
            out.append(make_identity_item(k, None, "missing"))
            continue

        left, right = ss.split("=", 1)
        k = "view_template.sig.{}".format(safe_str(left).strip())

        rr = safe_str(right).strip()
        # Mechanical unwrapping: some legacy signatures may serialize sentinels with quotes.
        if len(rr) >= 2 and ((rr[0] == rr[-1] == "'") or (rr[0] == rr[-1] == '"')):
            rr = rr[1:-1].strip()

        # If RHS is a packed k=v|k=v|... payload (e.g., vg lines), expand into sub-items so
        # legacy sentinel literals never appear as substrings inside IdentityItem.v.
        if ("|" in rr) and ("=" in rr):
            parts = [p.strip() for p in rr.split("|") if p.strip()]
            for p in parts:
                if "=" not in p:
                    sk = "{}.part".format(k)
                    out.append(make_identity_item(sk, None, "missing"))
                    continue

                subk_raw, subv_raw = p.split("=", 1)
                subk = safe_str(subk_raw).strip()
                subv = safe_str(subv_raw).strip()
                if len(subv) >= 2 and ((subv[0] == subv[-1] == "'") or (subv[0] == subv[-1] == '"')):
                    subv = subv[1:-1].strip()

                sv, sq = phase2_qv_from_legacy_sentinel_str(subv, allow_empty=True)
                out.append(make_identity_item("{}.{}".format(k, subk), sv, sq))
        else:
            v, q = phase2_qv_from_legacy_sentinel_str(rr, allow_empty=True)
            out.append(make_identity_item(k, v, q))

    return phase2_sorted_items(out)


def _canonical_identity_items_from_signature(def_hash, sig_final, override_stack_hash=None):
    """Canonical evidence superset for view_templates.

    Pilot rule: identity_basis.items is the single source of k/q/v evidence.
    Join and semantic surfaces should point at this superset via key selectors.

    Args:
        def_hash: MD5 hex string of the sorted signature list
        sig_final: sorted list of 'k=v' strings representing the signature
        override_stack_hash: optional MD5 hex string of category override items

    Returns:
        Sorted list of IdentityItem dicts
    """
    items = [make_identity_item("view_template.def_hash", def_hash, ITEM_Q_OK)]
    if override_stack_hash:
        items.append(
            make_identity_item("view_template.category_overrides_def_hash", override_stack_hash, ITEM_Q_OK)
        )
    items.extend(_phase2_items_from_def_signature(sig_final))
    return phase2_sorted_items(items)


def _semantic_keys_from_identity_items(identity_items):
    """Semantic selector list over canonical evidence.

    Keep join-key material separate from sig_hash basis for strict join/identity separation.
    Excludes 'view_template.def_hash' which is the root hash, not a semantic key.

    Args:
        identity_items: list of IdentityItem dicts (each with 'k', 'q', 'v')

    Returns:
        Sorted list of key strings (excluding view_template.def_hash)
    """
    keys = sorted(
        {
            safe_str(it.get("k", ""))
            for it in (identity_items or [])
            if isinstance(it.get("k"), str)
            and safe_str(it.get("k", ""))
            and safe_str(it.get("k", "")) != "view_template.def_hash"
        }
    )
    return [k for k in keys if k]


def _traceability_unknown_items(elem):
    """Build traceability unknown_items for a view element (metadata only).

    Args:
        elem: Revit View element

    Returns:
        Sorted list of IdentityItem dicts for element_id and unique_id traceability
    """
    items = []
    try:
        _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
        _eid_v, _eid_q = canonicalize_int(_eid_raw)
    except Exception:
        _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
    try:
        _uid_raw = getattr(elem, "UniqueId", None)
        _uid_v, _uid_q = canonicalize_str(_uid_raw)
    except Exception:
        _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
    items.append({"k": "vt.source_element_id", "q": _eid_q, "v": _eid_v})
    items.append({"k": "vt.source_unique_id", "q": _uid_q, "v": _uid_v})
    return phase2_sorted_items(items)


def _compute_delta_items(override_items, baseline_record):
    """Compare override to baseline, return only changed properties.

    Same logic as view_category_overrides domain.

    Args:
        override_items: list of IdentityItem dicts for the category override
        baseline_record: baseline record dict from object_styles domain

    Returns:
        List of IdentityItem dicts for items that differ from baseline
    """
    delta_items = []

    baseline_items = (baseline_record or {}).get("identity_basis", {}).get("items", []) or []
    baseline_map = {item.get("k"): item.get("v") for item in baseline_items}

    for override_item in (override_items or []):
        override_key = safe_str(override_item.get("k", ""))
        if not override_key:
            continue

        baseline_key = override_key.replace("vco.", "obj_style.")

        baseline_value = baseline_map.get(baseline_key)
        override_value = override_item.get("v")

        if override_value != baseline_value:
            delta_items.append(override_item)

    return delta_items
