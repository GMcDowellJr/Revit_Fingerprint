# -*- coding: utf-8 -*-
"""Object Styles domain family extractor."""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import canon_str
from core.graphic_overrides import extract_projection_graphics, extract_cut_graphics
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
from core.phase2 import phase2_sorted_items
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.collect import purge_lookup
from core.deps import require_domain, Blocked

try:
    from Autodesk.Revit.DB import GraphicsStyleType, CategoryType
except ImportError:
    GraphicsStyleType = None
    CategoryType = None

_CTX_CATEGORIES_CACHE_KEY = "_object_styles_categories_cache"
_EXCLUDED_TOP_LEVEL_CATEGORIES = frozenset(["Lines"])
_MODEL_SEMANTIC_KEYS = sorted([
    "obj_style.color.rgb",
    "obj_style.material_sig_hash",
    "obj_style.pattern_ref.sig_hash",
    "obj_style.weight.cut",
    "obj_style.weight.projection",
])
_NON_MODEL_SEMANTIC_KEYS = sorted([
    "obj_style.color.rgb",
    "obj_style.pattern_ref.sig_hash",
    "obj_style.weight.projection",
])


def _is_model_category_int0(cat):
    try:
        return int(getattr(cat, "CategoryType", -1)) == 0
    except Exception:
        try:
            return getattr(cat, "CategoryType", None) == getattr(CategoryType, "Model", None)
        except Exception:
            return False


def _collect_categories(doc, ctx, kind=None):
    cache_key = _CTX_CATEGORIES_CACHE_KEY if kind is None else "{}::{}".format(_CTX_CATEGORIES_CACHE_KEY, safe_str(kind))
    if ctx is not None and cache_key in ctx:
        return ctx[cache_key]
    result = []
    excluded_top_level_names = set()
    excluded_parent_ids = set()
    try:
        for cat in doc.Settings.Categories:
            is_excluded_top_level = False
            try:
                cat_name = canon_str(getattr(cat, "Name", None))
            except Exception:
                cat_name = None
            if kind == "model" and _is_model_category_int0(cat) and cat_name in _EXCLUDED_TOP_LEVEL_CATEGORIES:
                is_excluded_top_level = True
                if cat_name:
                    excluded_top_level_names.add(cat_name)
                try:
                    excluded_parent_ids.add(int(getattr(getattr(cat, "Id", None), "IntegerValue", 0)))
                except Exception:
                    pass

            if is_excluded_top_level:
                continue

            result.append((cat, False, None))
            try:
                for sub in list(getattr(cat, "SubCategories", []) or []):
                    if kind == "model":
                        try:
                            parent_id = int(getattr(getattr(cat, "Id", None), "IntegerValue", 0))
                        except Exception:
                            parent_id = 0
                        if parent_id in excluded_parent_ids:
                            continue
                    result.append((sub, True, cat))
            except Exception:
                pass
    except Exception:
        pass
    if ctx is not None:
        ctx[cache_key] = result
        if kind == "model":
            ctx["{}_excluded_top_level_names".format(cache_key)] = sorted(excluded_top_level_names)
    return result


def _is_analytical_category_type(ct):
    try:
        return "analytical" in safe_str(ct).strip().lower()
    except Exception:
        return False


def _matches_category_type(cat, kind):
    try:
        ct = cat.CategoryType
    except Exception:
        return False
    if kind == "model":
        return ct == CategoryType.Model
    if kind == "annotation":
        return ct == CategoryType.Annotation
    if kind == "analytical":
        return _is_analytical_category_type(ct)
    if kind == "imported":
        return ct == getattr(CategoryType, "ImportInstance", getattr(CategoryType, "Imported", None))
    return False


def _rgb_sig(c):
    try:
        return "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
    except Exception:
        return None


def _material_ref_item(doc, cat_obj):
    try:
        mid_or_mat = getattr(cat_obj, "Material", None)
    except Exception:
        mid_or_mat = None

    mat = None
    if mid_or_mat is not None and hasattr(mid_or_mat, "MaterialClass"):
        mat = mid_or_mat
    else:
        mid = mid_or_mat
        if mid is None:
            try:
                mid = getattr(cat_obj, "MaterialId", None)
            except Exception:
                mid = None

        if mid is None:
            return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_MISSING)

        try:
            if getattr(mid, "IntegerValue", 0) <= 0:
                return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_MISSING)
        except Exception:
            return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_UNREADABLE)

        try:
            mat = doc.GetElement(mid)
        except Exception:
            return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_UNREADABLE)

    if mat is None:
        return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_MISSING)

    try:
        name = canon_str(getattr(mat, "Name", None))
        mat_class = canon_str(getattr(mat, "MaterialClass", None))
        material_sig = make_hash([safe_str(name), safe_str(mat_class)])
        return make_identity_item("obj_style.material_sig_hash", material_sig, ITEM_Q_OK)
    except Exception:
        return make_identity_item("obj_style.material_sig_hash", None, ITEM_Q_UNREADABLE)


def _build_info():
    return {
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


def _extract_object_styles(doc, ctx, *, domain_name, kind, include_cut_weight, zero_records_valid, ctx_export):
    info = _build_info()

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

    try:
        lp_uid_to_sig_hash_v2 = (ctx or {}).get("line_pattern_uid_to_hash", None) if ctx is not None else None
        if not isinstance(lp_uid_to_sig_hash_v2, dict) or not lp_uid_to_sig_hash_v2:
            lp_uid_to_sig_hash_v2 = None
    except Exception:
        lp_uid_to_sig_hash_v2 = None
    try:
        lp_id_to_value = (ctx or {}).get("line_pattern_id_to_value", {}) if ctx is not None else {}
        if not isinstance(lp_id_to_value, dict):
            lp_id_to_value = {}
    except Exception:
        lp_id_to_value = {}
    try:
        lp_special_values = (ctx or {}).get("line_pattern_special_values", {}) if ctx is not None else {}
        if not isinstance(lp_special_values, dict):
            lp_special_values = {}
    except Exception:
        lp_special_values = {}

    try:
        cats = _collect_categories(doc, ctx, kind=kind)
    except Exception:
        return info

    if kind == "model":
        excluded_names = []
        if ctx is not None:
            try:
                excluded_names = ctx.get("{}::{}_excluded_top_level_names".format(_CTX_CATEGORIES_CACHE_KEY, safe_str(kind)), []) or []
            except Exception:
                excluded_names = []
        if "Lines" in set(excluded_names):
            print("[object_styles_model] excluded_top_level_category name='Lines' reason='covered_by_line_styles'")

    semantic_keys = _MODEL_SEMANTIC_KEYS if include_cut_weight else _NON_MODEL_SEMANTIC_KEYS
    v2_records = []
    v2_sig_hashes = []
    v2_any_blocked = False

    for cat, is_subcategory, parent in list(cats or []):
        info["debug_total_categories"] += 1
        if not _matches_category_type(cat, kind):
            info["debug_skipped_wrong_type"] += 1
            continue

        try:
            parent_name = canon_str(getattr(parent if is_subcategory else cat, "Name", None))
        except Exception:
            parent_name = None
        if not parent_name:
            info["debug_skipped_no_name"] += 1
            continue

        row_name = "self"
        if is_subcategory:
            try:
                row_name = canon_str(getattr(cat, "Name", None))
            except Exception:
                row_name = None
        if not row_name:
            info["debug_skipped_no_name"] += 1
            continue

        cat_obj = cat
        row_key = "{}|{}".format(parent_name, row_name)
        try:
            status_reasons = []
            status_v2 = STATUS_OK
            identity_items = []

            rk_v, rk_q = canonicalize_str(row_key)
            identity_items.append(make_identity_item("obj_style.row_key", rk_v, rk_q))
            required_qs = [rk_q]

            proj_items = extract_projection_graphics(doc, cat_obj, ctx, key_prefix="obj_style.projection")
            proj_items_by_key = {it.get("k"): it for it in (proj_items or [])}
            cut_items_by_key = {}
            if include_cut_weight:
                cut_items = extract_cut_graphics(doc, cat_obj, ctx, key_prefix="obj_style.cut")
                cut_items_by_key = {it.get("k"): it for it in (cut_items or [])}

            proj_weight_item = proj_items_by_key.get("obj_style.projection.line_weight", {}) or {}
            wproj_v = proj_weight_item.get("v", None)
            wproj_q = proj_weight_item.get("q", ITEM_Q_MISSING)
            if wproj_q != ITEM_Q_OK:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("weight_projection_missing_or_unreadable")
            identity_items.append(make_identity_item("obj_style.weight.projection", wproj_v, wproj_q))

            if include_cut_weight:
                try:
                    w_cut_legacy = cat_obj.GetLineWeight(GraphicsStyleType.Cut)
                except Exception:
                    w_cut_legacy = None
                if w_cut_legacy is None:
                    wcut_v, wcut_q = None, ITEM_Q_UNSUPPORTED
                else:
                    cut_weight_item = cut_items_by_key.get("obj_style.cut.line_weight", {}) or {}
                    wcut_v = cut_weight_item.get("v", None)
                    wcut_q = cut_weight_item.get("q", ITEM_Q_MISSING)
                identity_items.append(make_identity_item("obj_style.weight.cut", wcut_v, wcut_q))

            proj_color_item = proj_items_by_key.get("obj_style.projection.color.rgb", {}) or {}
            rgb_v = proj_color_item.get("v", None)
            rgb_q = proj_color_item.get("q", ITEM_Q_MISSING)
            if rgb_q != ITEM_Q_OK:
                try:
                    rgb_sig = _rgb_sig(cat_obj.LineColor)
                    rgb_v, rgb_q = canonicalize_str(rgb_sig) if rgb_sig else (None, ITEM_Q_MISSING)
                except Exception:
                    rgb_v, rgb_q = None, ITEM_Q_MISSING
            if rgb_q != ITEM_Q_OK:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("color_rgb_missing_or_unreadable")
            identity_items.append(make_identity_item("obj_style.color.rgb", rgb_v, rgb_q))

            lp_sig_hash_v = None
            lp_sig_hash_q = ITEM_Q_MISSING
            lp_id_read_failed = False
            try:
                lp_id_v2 = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
            except Exception:
                lp_id_v2 = None
                lp_id_read_failed = True
                status_v2 = STATUS_DEGRADED
                status_reasons.append("get_line_pattern_id_failed")

            if lp_id_v2 and getattr(lp_id_v2, "IntegerValue", 0) > 0:
                pid_key = safe_str(getattr(lp_id_v2, "IntegerValue", ""))
                if pid_key in lp_id_to_value:
                    lp_sig_hash_v, lp_sig_hash_q = canonicalize_str(lp_id_to_value.get(pid_key))
                elif lp_uid_to_sig_hash_v2 is None:
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
            elif (not lp_id_read_failed) and lp_id_v2 is not None and getattr(lp_id_v2, "IntegerValue", 0) <= 0:
                lp_sig_hash_v, lp_sig_hash_q = canonicalize_str(lp_special_values.get("solid", None))
            identity_items.append(make_identity_item("obj_style.pattern_ref.sig_hash", lp_sig_hash_v, lp_sig_hash_q))

            if kind == "model":
                identity_items.append(_material_ref_item(doc, cat_obj))

            if any(q != ITEM_Q_OK for q in required_qs):
                status_v2 = STATUS_BLOCKED
                status_reasons.append("required_identity_not_ok")

            identity_items_sorted = sorted(identity_items, key=lambda d: str(d.get("k", "")))
            semantic_items = [it for it in identity_items_sorted if safe_str(it.get("k", "")) in set(semantic_keys)]
            preimage_v2 = serialize_identity_items(semantic_items)
            sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(preimage_v2)

            rec_v2 = build_record_v2(
                domain=domain_name,
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
            if row_name != "self":
                _ip, _ip_q = purge_lookup(getattr(getattr(cat_obj, "Id", None), "IntegerValue", None), ctx)
            else:
                _ip, _ip_q = None, "unsupported_not_applicable"
            rec_v2["is_purgeable"] = _ip
            rec_v2["is_purgeable_q"] = _ip_q

            pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), domain_name)
            rec_v2["join_key"], _missing = build_join_key_from_policy(
                domain_policy=pol,
                identity_items=identity_items_sorted,
                include_optional_items=False,
                emit_keys_used=True,
                hash_optional_items=False,
                emit_items=False,
                emit_selectors=True,
            )

            category_type_labels = {1: "Model", 2: "Annotation", 3: "AnalyticalModel", 4: "Imported"}
            try:
                cat_type_int = int(cat_obj.CategoryType)
                ct_v, ct_q = canonicalize_str(category_type_labels.get(cat_type_int, safe_str(cat_type_int)))
            except Exception:
                ct_v, ct_q = (None, ITEM_Q_UNREADABLE)
            coordination_items = [
                make_identity_item("obj_style.domain_family", "object_styles", ITEM_Q_OK),
                make_identity_item("obj_style.category_type", ct_v, ct_q),
                make_identity_item("obj_style.is_subcategory", "true" if row_name != "self" else "false", ITEM_Q_OK),
            ]

            unknown_items = []
            try:
                _eid_v, _eid_q = canonicalize_int(getattr(getattr(cat_obj, "Id", None), "IntegerValue", None))
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
                "schema": "phase2.{}.v1".format(domain_name),
                "grouping_basis": "phase2.hypothesis",
                "cosmetic_items": phase2_sorted_items([make_identity_item("obj_style.row_key", rk_v, rk_q)]),
                "coordination_items": phase2_sorted_items(coordination_items),
                "unknown_items": phase2_sorted_items(unknown_items),
            }
            rec_v2["sig_basis"] = {"schema": "{}.sig_basis.v1".format(domain_name), "keys_used": semantic_keys}

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
        info["hash_v2"] = None
    elif info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    elif not zero_records_valid:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"no_v2_records": True}
        info["hash_v2"] = None

    if ctx is not None:
        row_key_to_sig = {}
        records_by_sig = {}
        for rec in info.get("records", []):
            items = {it["k"]: it for it in rec.get("identity_basis", {}).get("items", [])}
            rk = items.get("obj_style.row_key", {})
            if rk.get("q") == ITEM_Q_OK and rk.get("v"):
                sig = rec.get("sig_hash")
                if sig:
                    row_key_to_sig[rk["v"]] = sig
                    records_by_sig[sig] = rec
        if ctx_export == "model":
            existing = ctx.get("object_styles_category_to_sig_hash", {})
            existing.update(row_key_to_sig)
            ctx["object_styles_category_to_sig_hash"] = existing
            ctx["object_style_row_key_to_sig_hash"] = dict(existing)
        elif ctx_export == "annotation":
            existing = ctx.get("object_styles_category_to_sig_hash", {})
            existing.update(row_key_to_sig)
            ctx["object_styles_category_to_sig_hash"] = existing
            ctx["object_style_annotation_row_key_to_sig_hash"] = dict(row_key_to_sig)
        elif ctx_export == "analytical":
            ctx["object_style_analytical_row_key_to_sig_hash"] = row_key_to_sig
            ctx["object_styles_analytical_records"] = records_by_sig
        elif ctx_export == "imported":
            ctx["object_style_imported_row_key_to_sig_hash"] = row_key_to_sig
            ctx["object_styles_imported_records"] = records_by_sig

    info["record_rows"] = [{"record_key": safe_str(r.get("record_id", "")), "sig_hash": r.get("sig_hash", None)} for r in info["records"]]
    return info


def extract_model(doc, ctx=None):
    return _extract_object_styles(doc, ctx, domain_name="object_styles_model", kind="model", include_cut_weight=True, zero_records_valid=False, ctx_export="model")


def extract_annotation(doc, ctx=None):
    return _extract_object_styles(doc, ctx, domain_name="object_styles_annotation", kind="annotation", include_cut_weight=False, zero_records_valid=False, ctx_export="annotation")


def extract_analytical(doc, ctx=None):
    return _extract_object_styles(doc, ctx, domain_name="object_styles_analytical", kind="analytical", include_cut_weight=False, zero_records_valid=True, ctx_export="analytical")


def extract_imported(doc, ctx=None):
    return _extract_object_styles(doc, ctx, domain_name="object_styles_imported", kind="imported", include_cut_weight=False, zero_records_valid=True, ctx_export="imported")
