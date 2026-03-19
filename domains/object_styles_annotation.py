# -*- coding: utf-8 -*-
"""
Object Styles (Annotation) domain extractor.

Domain family: object_styles
Contains: CategoryType.Annotation categories and subcategories

Per-record identity: sig_hash (UID-free) derived from identity_items.
Ordering: order-insensitive (sorted before hashing)

Cut weight is NOT applicable to Annotation categories — do not emit it.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)
from core.graphic_overrides import (
    extract_projection_graphics,
)
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    canonicalize_str,
    canonicalize_int,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.deps import require_domain, Blocked

try:
    from Autodesk.Revit.DB import GraphicsStyleType, CategoryType
except ImportError:
    GraphicsStyleType = None
    CategoryType = None

DOMAIN_NAME = "object_styles_annotation"

# Semantic keys for sig_hash preimage — no weight.cut for Annotation
SEMANTIC_KEYS = sorted([
    "obj_style.color.rgb",
    "obj_style.pattern_ref.sig_hash",
    "obj_style.weight.projection",
])


def extract(doc, ctx=None):
    """
    Extract Object Styles (Annotation) fingerprint from document.

    Includes CategoryType.Annotation categories and subcategories only.
    Does NOT emit obj_style.weight.cut (not applicable to Annotation).
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "signature_hashes_v2": [],
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
        "debug_total_categories": 0,
        "debug_skipped_wrong_type": 0,
        "debug_skipped_no_name": 0,
        "debug_fail_record_build": 0,
    }

    if GraphicsStyleType is None or CategoryType is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"revit_api_unavailable": True}
        return info

    try:
        require_domain((ctx or {}).get("_domains", {}), "line_patterns")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": "line_patterns: {}".format(b.reasons)}
        return info

    lp_uid_to_sig_hash_v2 = None
    try:
        lp_uid_to_sig_hash_v2 = (ctx or {}).get("line_pattern_uid_to_hash", None) if ctx is not None else None
        if not isinstance(lp_uid_to_sig_hash_v2, dict) or not lp_uid_to_sig_hash_v2:
            lp_uid_to_sig_hash_v2 = None
    except Exception:
        lp_uid_to_sig_hash_v2 = None

    def _rgb_sig(c):
        try:
            return "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
        except Exception:
            return None

    try:
        cats = doc.Settings.Categories
    except Exception:
        return info

    v2_records = []
    v2_sig_hashes = []
    v2_any_blocked = False

    for cat in list(cats or []):
        info["debug_total_categories"] += 1

        try:
            ct = cat.CategoryType
            if ct != CategoryType.Annotation:
                info["debug_skipped_wrong_type"] += 1
                continue
        except Exception:
            info["debug_skipped_wrong_type"] += 1
            continue

        try:
            parent_name = canon_str(getattr(cat, "Name", None))
        except Exception:
            parent_name = None

        if not parent_name:
            info["debug_skipped_no_name"] += 1
            continue

        rows = [("self", cat)]
        try:
            for sc in list(getattr(cat, "SubCategories", []) or []):
                try:
                    rn = canon_str(getattr(sc, "Name", None))
                except Exception:
                    rn = None
                if rn:
                    rows.append((rn, sc))
        except Exception:
            pass

        for row_name, cat_obj in rows:
            try:
                row_key = "{}|{}".format(parent_name, row_name)

                status_reasons = []
                status_v2 = STATUS_OK
                identity_items = []

                rk_v, rk_q = canonicalize_str(row_key)
                identity_items.append(make_identity_item("obj_style.row_key", rk_v, rk_q))
                required_qs = [rk_q]

                proj_items = extract_projection_graphics(doc, cat_obj, ctx, key_prefix="obj_style.projection")
                proj_items_by_key = {it.get("k"): it for it in (proj_items or [])}

                # projection weight
                proj_weight_item = proj_items_by_key.get("obj_style.projection.line_weight", {}) or {}
                wproj_v = proj_weight_item.get("v", None)
                wproj_q = proj_weight_item.get("q", ITEM_Q_MISSING)
                if wproj_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("weight_projection_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.weight.projection", wproj_v, wproj_q))

                # color
                proj_color_item = proj_items_by_key.get("obj_style.projection.color.rgb", {}) or {}
                rgb_v = proj_color_item.get("v", None)
                rgb_q = proj_color_item.get("q", ITEM_Q_MISSING)
                if rgb_q != ITEM_Q_OK:
                    try:
                        rgb_sig = _rgb_sig(cat_obj.LineColor)
                        if rgb_sig:
                            rgb_v, rgb_q = canonicalize_str(rgb_sig)
                        else:
                            rgb_v, rgb_q = None, ITEM_Q_MISSING
                    except Exception:
                        rgb_v, rgb_q = None, ITEM_Q_MISSING
                if rgb_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("color_rgb_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.color.rgb", rgb_v, rgb_q))

                # pattern reference (optional)
                lp_sig_hash_v = None
                lp_sig_hash_q = ITEM_Q_MISSING

                lp_id_v2 = None
                try:
                    lp_id_v2 = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
                except Exception:
                    lp_id_v2 = None
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("get_line_pattern_id_failed")

                if lp_id_v2 and getattr(lp_id_v2, "IntegerValue", 0) > 0:
                    if lp_uid_to_sig_hash_v2 is None:
                        status_v2 = STATUS_DEGRADED
                        status_reasons.append("dependency_missing_line_patterns_v2_sig_hash")
                    else:
                        try:
                            lp_elem = doc.GetElement(lp_id_v2)
                            lp_uid = canon_str(getattr(lp_elem, "UniqueId", None)) if lp_elem else None
                        except Exception:
                            lp_uid = None
                            status_v2 = STATUS_DEGRADED
                            status_reasons.append("get_line_pattern_element_failed")

                        if lp_uid:
                            lp_sig_hash_v = lp_uid_to_sig_hash_v2.get(lp_uid, None)
                            if lp_sig_hash_v:
                                lp_sig_hash_q = ITEM_Q_OK
                            else:
                                status_v2 = STATUS_DEGRADED
                                status_reasons.append("dependency_unmapped_line_pattern_v2_sig_hash")

                identity_items.append(make_identity_item("obj_style.pattern_ref.sig_hash", lp_sig_hash_v, lp_sig_hash_q))

                if any(q != ITEM_Q_OK for q in required_qs):
                    status_v2 = STATUS_BLOCKED
                    status_reasons.append("required_identity_not_ok")

                identity_items_sorted = sorted(identity_items, key=lambda d: str(d.get("k", "")))
                semantic_items = [
                    it for it in identity_items_sorted
                    if safe_str(it.get("k", "")) in set(SEMANTIC_KEYS)
                ]
                preimage_v2 = serialize_identity_items(semantic_items)
                sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(preimage_v2)

                rec_v2 = build_record_v2(
                    domain=DOMAIN_NAME,
                    record_id=safe_str(row_key),
                    status=status_v2,
                    status_reasons=sorted(set(status_reasons)),
                    sig_hash=sig_hash_v2,
                    identity_items=identity_items_sorted,
                    required_qs=required_qs,
                    label={
                        "display": safe_str(row_key),
                        "quality": "human",
                        "provenance": "computed.path",
                        "components": {"row_key": safe_str(row_key)},
                    },
                )

                pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
                rec_v2["join_key"], _missing = build_join_key_from_policy(
                    domain_policy=pol,
                    identity_items=identity_items_sorted,
                    include_optional_items=False,
                    emit_keys_used=True,
                    hash_optional_items=False,
                    emit_items=False,
                    emit_selectors=True,
                )

                # coordination_items
                _CATEGORY_TYPE_LABELS = {1: "Model", 2: "Annotation", 3: "AnalyticalModel", 4: "Imported"}
                try:
                    cat_type_int = int(cat_obj.CategoryType)
                    cat_type_label = _CATEGORY_TYPE_LABELS.get(cat_type_int, safe_str(cat_type_int))
                    ct_v, ct_q = canonicalize_str(cat_type_label)
                except Exception:
                    ct_v, ct_q = (None, ITEM_Q_UNREADABLE)
                try:
                    _is_sub = row_name != "self"
                    is_sub_v, is_sub_q = ("true" if _is_sub else "false"), ITEM_Q_OK
                except Exception:
                    is_sub_v, is_sub_q = None, ITEM_Q_UNREADABLE

                coordination_items = [
                    make_identity_item("obj_style.domain_family", "object_styles", ITEM_Q_OK),
                    make_identity_item("obj_style.category_type", ct_v, ct_q),
                    make_identity_item("obj_style.is_subcategory", is_sub_v, is_sub_q),
                ]

                unknown_items = []
                try:
                    _eid_raw = getattr(getattr(cat_obj, "Id", None), "IntegerValue", None)
                    _eid_v, _eid_q = canonicalize_int(_eid_raw)
                except Exception:
                    _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
                try:
                    _uid_raw = getattr(cat_obj, "UniqueId", None)
                    _uid_v, _uid_q = canonicalize_str(_uid_raw)
                    if _uid_raw is None:
                        _uid_v, _uid_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
                except Exception:
                    _uid_v, _uid_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
                unknown_items.append(make_identity_item("obj_style.source_element_id", _eid_v, _eid_q))
                unknown_items.append(make_identity_item("obj_style.source_unique_id", _uid_v, _uid_q))

                rec_v2["phase2"] = {
                    "schema": "phase2.{}.v1".format(DOMAIN_NAME),
                    "grouping_basis": "phase2.hypothesis",
                    "cosmetic_items": phase2_sorted_items([
                        make_identity_item("obj_style.row_key", rk_v, rk_q),
                    ]),
                    "coordination_items": phase2_sorted_items(coordination_items),
                    "unknown_items": phase2_sorted_items(unknown_items),
                }
                rec_v2["sig_basis"] = {
                    "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
                    "keys_used": SEMANTIC_KEYS,
                }

                v2_records.append(rec_v2)
                if sig_hash_v2:
                    v2_sig_hashes.append(sig_hash_v2)
                if status_v2 == STATUS_BLOCKED:
                    v2_any_blocked = True

            except Exception:
                info["debug_fail_record_build"] += 1
                continue

    info["count"] = len(v2_records)
    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)

    if v2_any_blocked:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"one_or_more_records_blocked": True}

    if (not v2_any_blocked) and info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        if (not v2_any_blocked) and (not info["signature_hashes_v2"]):
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"] = {"no_v2_records": True}
        info["hash_v2"] = None

    # Export baseline map for downstream domains
    if ctx is not None:
        category_to_sig_hash = {}
        for rec in (info.get("records") or []):
            for item in (rec.get("identity_basis", {}).get("items", []) or []):
                if item.get("k") == "obj_style.row_key":
                    key = item.get("v")
                    sig_hash = rec.get("sig_hash")
                    if key and sig_hash:
                        category_to_sig_hash[key] = sig_hash
                    break
        existing = ctx.get("object_styles_category_to_sig_hash", {})
        existing.update(category_to_sig_hash)
        ctx["object_styles_category_to_sig_hash"] = existing

    info["record_rows"] = [
        {"record_key": safe_str(r.get("record_id", "")), "sig_hash": r.get("sig_hash", None)}
        for r in info["records"]
    ]

    return info
