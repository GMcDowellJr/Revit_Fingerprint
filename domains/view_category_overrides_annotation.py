# -*- coding: utf-8 -*-
"""Annotation partition for view category overrides."""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
    canonicalize_str,
    canonicalize_int,
    canonicalize_bool,
    ITEM_Q_UNSUPPORTED,
)
from core.phase2 import phase2_sorted_items
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)
from core.collect import collect_instances
from domains.view_templates import _VIEW_INSTANCES_CACHE_KEY

try:
    from Autodesk.Revit.DB import View, OverrideGraphicSettings
except ImportError:
    View = None
    OverrideGraphicSettings = None

DOMAIN_NAME = "view_category_overrides_annotation"


def _phase2_partition_items(items):
    semantic = []
    cosmetic = []
    unknown = []

    for it in (items or []):
        k = safe_str(it.get("k", ""))
        if k in ("vco.baseline_category_path", "vco.baseline_sig_hash", "vco.override_properties_hash", "vco.category_hidden"):
            semantic.append(it)
        elif (k.startswith("vco.projection.") or k.startswith("vco.cut.") or k in ("vco.halftone", "vco.transparency")):
            cosmetic.append(it)
        else:
            unknown.append(it)

    return (phase2_sorted_items(semantic), phase2_sorted_items(cosmetic), phase2_sorted_items(unknown))


def _safe_bool(fn, default=False):
    try:
        return bool(fn())
    except Exception:
        return default


def _category_hidden_item(template, cat):
    try:
        if hasattr(template, "GetCategoryHidden"):
            val, q = canonicalize_bool(bool(template.GetCategoryHidden(cat.Id)))
            return make_identity_item("vco.category_hidden", val, q)
        return make_identity_item("vco.category_hidden", None, ITEM_Q_UNSUPPORTED)
    except Exception:
        return make_identity_item("vco.category_hidden", None, ITEM_Q_UNREADABLE)


def extract(doc, ctx=None):
    info = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_templates_processed": 0,
        "debug_categories_checked": 0,
        "debug_overrides_found": 0,
        "debug_no_baseline": 0,
        "debug_no_change": 0,
        "debug_v2_blocked": False,
    }

    if View is None or OverrideGraphicSettings is None:
        info["debug_v2_blocked"] = True
        return info

    baseline_sig_map = (ctx or {}).get("object_style_annotation_row_key_to_sig_hash") or {}
    if not baseline_sig_map:
        info["debug_no_baseline_map"] = True

    all_views = []
    try:
        all_views = list(
            collect_instances(
                doc,
                of_class=View,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
            )
        )
    except Exception:
        pass

    templates = [v for v in all_views if _safe_bool(lambda: v.IsTemplate)]
    info["debug_templates_processed"] = len(templates)

    all_cats = []
    try:
        cat_root = doc.Settings.Categories
        for cat in cat_root:
            try:
                all_cats.append((cat, False, None))
                try:
                    for sub in cat.SubCategories:
                        all_cats.append((sub, True, cat))
                except Exception:
                    pass
            except Exception:
                pass
    except Exception:
        pass

    dflt = OverrideGraphicSettings()
    dflt_proj = extract_projection_graphics(doc, dflt, ctx, "vco.projection")
    dflt_cut = extract_cut_graphics(doc, dflt, ctx, "vco.cut")
    dflt_halftone = extract_halftone(dflt, "vco.halftone")
    dflt_trans = extract_transparency(dflt, "vco.transparency")
    dflt_map = {it.get("k"): it.get("v") for it in dflt_proj + dflt_cut + dflt_halftone + dflt_trans}

    v2_records = []
    signature_hashes_v2 = []
    v2_any_blocked = False

    for template in templates:
        tpl_uid = None
        try:
            tpl_uid = safe_str(template.UniqueId)
        except Exception:
            pass
        tpl_eid = None
        try:
            tpl_eid = int(template.Id.IntegerValue)
        except Exception:
            pass

        for (cat, is_sub, parent) in all_cats:
            try:
                cat_type_int = int(cat.CategoryType)
            except Exception:
                continue
            if cat_type_int != 2:
                continue

            info["debug_categories_checked"] += 1

            try:
                ogs = template.GetCategoryOverrides(cat.Id)
            except Exception:
                continue
            if ogs is None:
                continue

            proj_items = extract_projection_graphics(doc, ogs, ctx, "vco.projection")
            cut_items = extract_cut_graphics(doc, ogs, ctx, "vco.cut")
            halftone_items = extract_halftone(ogs, "vco.halftone")
            trans_items = extract_transparency(ogs, "vco.transparency")
            all_ogs_items = proj_items + cut_items + halftone_items + trans_items
            hidden_item = _category_hidden_item(template, cat)
            hidden_is_true = hidden_item.get("q") == ITEM_Q_OK and hidden_item.get("v") == "true"

            actual_map = {it.get("k"): it.get("v") for it in all_ogs_items}
            has_graphic_override = any(actual_map.get(k) != dflt_map.get(k) for k in actual_map)
            has_override = has_graphic_override or hidden_is_true
            if not has_override:
                info["debug_no_change"] += 1
                continue

            info["debug_overrides_found"] += 1
            info["raw_count"] += 1

            cat_name = safe_str(getattr(cat, "Name", None) or "")
            if is_sub and parent is not None:
                parent_name = safe_str(getattr(parent, "Name", None) or "")
                row_key = "{}|{}".format(parent_name, cat_name)
            else:
                row_key = "{}|self".format(cat_name)

            baseline_sig = baseline_sig_map.get(row_key) if baseline_sig_map else None
            if not baseline_sig:
                info["debug_no_baseline"] += 1

            rk_v, rk_q = canonicalize_str(row_key)
            bs_v, bs_q = canonicalize_str(baseline_sig) if baseline_sig else (None, ITEM_Q_MISSING)

            non_dflt_items = [it for it in all_ogs_items if actual_map.get(it.get("k")) != dflt_map.get(it.get("k"))]
            if hidden_is_true:
                non_dflt_items.append(hidden_item)
            non_dflt_sorted = sorted(non_dflt_items, key=lambda x: x.get("k", ""))
            oph_preimage = serialize_identity_items(non_dflt_sorted) if non_dflt_sorted else "|empty|"
            override_props_hash = make_hash(oph_preimage)
            oph_v, oph_q = canonicalize_str(override_props_hash)

            identity_items = [
                make_identity_item("vco.baseline_category_path", rk_v, rk_q),
                make_identity_item("vco.baseline_sig_hash", bs_v, bs_q),
                make_identity_item("vco.override_properties_hash", oph_v, oph_q),
            ] + sorted(all_ogs_items + [hidden_item], key=lambda x: x.get("k", ""))
            identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))

            preimage = serialize_identity_items(identity_items_sorted)
            sig_hash = make_hash(preimage) if preimage else None

            required_qs = [rk_q, bs_q, oph_q]
            blocked = any(q != ITEM_Q_OK for q in required_qs) or sig_hash is None
            any_incomplete = any(it.get("q") != ITEM_Q_OK for it in identity_items_sorted)
            status_reasons = []
            for it in identity_items_sorted:
                if it.get("q") != ITEM_Q_OK:
                    status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))

            record_id = "vco_{}_{}".format(safe_str(row_key), safe_str(sig_hash or "blocked")[:8])
            label = {
                "display": row_key,
                "quality": "human",
                "provenance": "computed.override",
                "components": {
                    "category_path": safe_str(row_key),
                    "template_uid": safe_str(tpl_uid or ""),
                },
            }

            if blocked:
                v2_any_blocked = True
                rec = build_record_v2(
                    domain=DOMAIN_NAME,
                    record_id=record_id,
                    status=STATUS_BLOCKED,
                    status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                    sig_hash=None,
                    identity_items=identity_items_sorted,
                    required_qs=(),
                    label=label,
                )
            else:
                status = STATUS_DEGRADED if any_incomplete else STATUS_OK
                rec = build_record_v2(
                    domain=DOMAIN_NAME,
                    record_id=record_id,
                    status=status,
                    status_reasons=sorted(set(status_reasons)),
                    sig_hash=sig_hash,
                    identity_items=identity_items_sorted,
                    required_qs=required_qs,
                    label=label,
                )
                signature_hashes_v2.append(sig_hash)

            p2_semantic, p2_cosmetic, p2_unknown = _phase2_partition_items(identity_items_sorted)
            coordination = [
                make_identity_item("vco.vg_tab", "Annotation", ITEM_Q_OK),
                make_identity_item("vco.context_type", "template", ITEM_Q_OK),
            ]

            cosmetic = []
            try:
                tpl_name_raw = getattr(template, "Name", None)
                if tpl_name_raw:
                    tpl_nm_v, tpl_nm_q = canonicalize_str(safe_str(tpl_name_raw))
                    cosmetic.append(make_identity_item("vco.template_name", tpl_nm_v, tpl_nm_q))
            except Exception:
                pass

            unknown_extra = []
            try:
                eid_v, eid_q = canonicalize_int(tpl_eid)
                unknown_extra.append(make_identity_item("vco.template_element_id", eid_v, eid_q))
            except Exception:
                unknown_extra.append(make_identity_item("vco.template_element_id", None, ITEM_Q_UNREADABLE))
            try:
                uid_v, uid_q = canonicalize_str(tpl_uid)
                unknown_extra.append(make_identity_item("vco.template_unique_id", uid_v, uid_q))
            except Exception:
                unknown_extra.append(make_identity_item("vco.template_unique_id", None, ITEM_Q_UNREADABLE))

            rec["phase2"] = {
                "schema": "phase2.{}.v1".format(DOMAIN_NAME),
                "grouping_basis": "join_key.join_hash",
                "cosmetic_items": phase2_sorted_items(p2_cosmetic + cosmetic),
                "coordination_items": phase2_sorted_items(coordination),
                "unknown_items": phase2_sorted_items(p2_unknown + unknown_extra),
            }

            pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
            rec["join_key"], _missing = build_join_key_from_policy(
                domain_policy=pol,
                identity_items=identity_items_sorted,
                include_optional_items=False,
                emit_keys_used=True,
                hash_optional_items=False,
                emit_items=False,
                emit_selectors=True,
            )

            rec["sig_basis"] = {
                "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
                "keys_used": sorted({safe_str(it.get("k", "")) for it in identity_items_sorted}),
            }

            v2_records.append(rec)

    info["records"] = sorted(v2_records, key=lambda r: safe_str(r.get("record_id", "")))
    info["count"] = len(v2_records)
    info["signature_hashes_v2"] = sorted(signature_hashes_v2)

    if v2_any_blocked:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"one_or_more_records_blocked": True}
        info["hash_v2"] = None
    elif signature_hashes_v2:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    if ctx is not None:
        ctx["view_category_overrides_sig_hash"] = {}

    return info
