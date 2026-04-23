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

try:
    from Autodesk.Revit.DB import BuiltInParameter
except Exception:
    BuiltInParameter = None


_BUILTIN_PARAM_SPECS = [
    {
        "key": "detail_level",
        "include_bip": "VIEW_DETAIL_LEVEL",
        "value_bip": "VIEW_DETAIL_LEVEL",
        "storage": "int",
        "partitions": None,
    },
    {
        "key": "discipline",
        "include_bip": "VIEW_DISCIPLINE",
        "value_bip": "VIEW_DISCIPLINE",
        "storage": "int",
        "partitions": None,
    },
    {
        "key": "display_model",
        "include_bip": "VIEW_MODEL_DISPLAY_MODE",
        "value_bip": "VIEW_MODEL_DISPLAY_MODE",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
            "view_templates_elevations_sections_detail",
        ],
    },
    {
        "key": "parts_visibility",
        "include_bip": "VIEW_PARTS_VISIBILITY",
        "value_bip": "VIEW_PARTS_VISIBILITY",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
            "view_templates_elevations_sections_detail",
        ],
    },
    {
        "key": "show_hidden_lines",
        "include_bip": "VIEW_SHOW_HIDDEN_LINES",
        "value_bip": "VIEW_SHOW_HIDDEN_LINES",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
            "view_templates_elevations_sections_detail",
        ],
    },
    {
        "key": "visual_style",
        "include_bip": "MODEL_GRAPHICS_STYLE_ANON_DRAFT",
        "value_bip": "MODEL_GRAPHICS_STYLE_ANON_DRAFT",
        "storage": "int",
        "partitions": [
            "view_templates_renderings_drafting",
        ],
    },
    {
        "key": "view_scale",
        "include_bip": "VIEW_SCALE",
        "value_bip": "VIEW_SCALE",
        "storage": "int",
        "partitions": None,
        "debug_note": "include uses VIEW_SCALE as canonical indicator for reads",
    },
    {
        "key": "orientation",
        "include_bip": "PLAN_VIEW_NORTH",
        "value_bip": "PLAN_VIEW_NORTH",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
        ],
    },
    {
        "key": "underlay_orientation",
        "include_bip": "VIEW_UNDERLAY_ORIENTATION",
        "value_bip": "VIEW_UNDERLAY_ORIENTATION",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
        ],
    },
    {
        "key": "depth_clipping",
        "include_bip": "VIEW_BACK_CLIPPING",
        "value_bip": "VIEW_BACK_CLIPPING",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
        ],
    },
    {
        "key": "color_scheme_location",
        "include_bip": "COLOR_SCHEME_LOCATION",
        "value_bip": "COLOR_SCHEME_LOCATION",
        "storage": "int",
        "partitions": [
            "view_templates_floor_structural_area_plans",
            "view_templates_elevations_sections_detail",
        ],
    },
]


def _read_bip_int(v, bip_enum_name, tpl_bips, debug_counters=None, storage="int"):
    """Read a BuiltInParameter integer value from a view template element.

    Returns (include_flag: bool, value_str: str, readable: bool).
    """
    if BuiltInParameter is None:
        return (False, "<UNREADABLE>", False)

    try:
        bip_int = int(getattr(BuiltInParameter, bip_enum_name))
    except Exception:
        return (False, "<UNREADABLE>", False)

    include_flag = bip_int in (tpl_bips or set())

    try:
        p = v.get_Parameter(getattr(BuiltInParameter, bip_enum_name))
        if p is None:
            return (include_flag, "<UNREADABLE>", False)

        if storage == "double":
            try:
                val = p.AsDouble()
                return (include_flag, "{:.6f}".format(float(val)), True)
            except Exception:
                pass
        else:
            try:
                val = p.AsInteger()
                return (include_flag, safe_str(val), True)
            except Exception:
                pass

        try:
            eid = p.AsElementId()
            return (include_flag, safe_str(getattr(eid, "IntegerValue", None)), True)
        except Exception:
            return (include_flag, "<UNREADABLE>", False)
    except Exception:
        return (include_flag, "<UNREADABLE>", False)


def emit_builtin_params(v, domain_name, tpl_bips, non_ctrl_bips, sig, sig_v2, debug_counters=None):
    """Emit include-flag + value items for built-in params for a domain."""
    for spec in _BUILTIN_PARAM_SPECS:
        key = spec.get("key")
        if not key:
            continue

        partitions = spec.get("partitions")
        if partitions is not None and domain_name not in partitions:
            continue

        include_flag = False
        include_names = spec.get("include_bip_any_of")
        if include_names is None:
            include_names = [spec.get("include_bip")]
        include_bip_ints = []

        if BuiltInParameter is None:
            include_bip_ints = []
        else:
            for include_name in include_names:
                if not include_name:
                    continue
                try:
                    include_bip_ints.append(int(getattr(BuiltInParameter, include_name)))
                except Exception:
                    continue

        if not include_bip_ints:
            include_flag = False
            if debug_counters is not None:
                k = "debug_bip_unresolved_{}".format(key)
                debug_counters[k] = debug_counters.get(k, 0) + 1
        else:
            if not non_ctrl_bips:
                include_flag = False
            else:
                include_flag = any(bip_int not in non_ctrl_bips for bip_int in include_bip_ints)

        _, value_str, _ = _read_bip_int(
            v,
            spec.get("value_bip"),
            tpl_bips,
            debug_counters=debug_counters,
            storage=spec.get("storage", "int"),
        )

        include_entry = "include_{}={}".format(key, include_flag)
        value_entry = "{}={}".format(key, value_str)

        sig.append(include_entry)
        sig.append(value_entry)
        sig_v2.append(include_entry)
        sig_v2.append(value_entry)


def emit_shared_params_stub(v, domain_name, tpl_param_ids, sig, sig_v2, debug_counters=None):
    """Stub for shared/project parameter extraction.

    tpl_param_ids: full list of ParameterId objects from
                   v.GetTemplateParameterIds() (not filtered to BIPs only).
    """
    # sun_path: VIEW_SUNPATH_DISPLAYED not confirmed as template BIP.
    # Investigate via View property surface rather than parameter API.
    pass


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
    include_map = {}
    for it in (identity_items or []):
        try:
            k = safe_str(it.get("k", ""))
        except Exception:
            continue
        if not k.startswith("view_template.sig.include_"):
            continue
        base = k.replace("view_template.sig.include_", "", 1)
        try:
            raw_v = safe_str(it.get("v", "")).strip().lower()
        except Exception:
            raw_v = ""
        include_map[base] = raw_v == "true"

    keys = set()
    for it in (identity_items or []):
        if not isinstance(it.get("k"), str):
            continue
        key = safe_str(it.get("k", ""))
        if (not key) or key == "view_template.def_hash":
            continue

        if key.startswith("view_template.sig.include_"):
            keys.add(key)
            continue

        if key.startswith("view_template.sig."):
            sig_key = key.replace("view_template.sig.", "", 1)
            if sig_key in include_map and not include_map.get(sig_key, False):
                continue

        keys.add(key)

    return sorted(keys)


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
