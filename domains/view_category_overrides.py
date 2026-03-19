# -*- coding: utf-8 -*-
"""
View Category Overrides domain extractor.

Captures per-category graphics override records for view templates:
  - Category 1: Template-controlled overrides (V/G checkbox checked)
  - Category 2: Latent overrides (V/G checkbox unchecked, override set)

Both categories share the same record schema. BI filters by
coordination_items.vco.include_controlled to isolate the governance
population (Category 1).

Category 3 (view-local overrides on non-template views) is deferred.
Hooks: vco.context_type will be "view_local" for that population.

Dependencies:
  Upstream:  object_styles_model  (for row_key → sig_hash baseline map)
  Downstream: none (view_template_* domains do NOT depend on VCO)
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import canon_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
    canonicalize_str,
    canonicalize_int,
)
from core.phase2 import phase2_sorted_items
from core.deps import require_domain, Blocked
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)

try:
    from Autodesk.Revit.DB import (
        View,
        OverrideGraphicSettings,
        BuiltInParameter,
    )
except ImportError:
    View = None
    OverrideGraphicSettings = None
    BuiltInParameter = None

from core.collect import collect_instances


# ---------------------------------------------------------------------------
# V/G include-controlled BIP map.
# When a BIP integer is present in GetTemplateParameterIds(), the corresponding
# V/G tab's "Include" checkbox is checked for that template.
# ---------------------------------------------------------------------------
def _build_vg_include_bips():
    if BuiltInParameter is None:
        return {}
    bips = {}
    try:
        bips["Model"] = int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES)
    except Exception:
        pass
    try:
        bips["Annotation"] = int(BuiltInParameter.VIS_GRAPHICS_ANNOTATION_OVERRIDES)
    except Exception:
        pass
    try:
        bips["AnalyticalModel"] = int(BuiltInParameter.VIS_GRAPHICS_ANALYTICAL_MODEL_OVERRIDES)
    except Exception:
        pass
    try:
        bips["Imported"] = int(BuiltInParameter.VIS_GRAPHICS_IMPORT_OVERRIDES)
    except Exception:
        pass
    return bips


_CAT_TYPE_LABELS = {1: "Model", 2: "Annotation", 3: "AnalyticalModel", 4: "Imported"}


def _compute_override_properties_hash(identity_items):
    """
    Compute canonical hash of ALL override properties as a behavioural unit.

    Excludes baseline reference items (keys starting with 'vco.baseline').
    Deterministic: sorts by key before hashing.

    Kept for backwards compatibility with test_join_key_migration.py and any
    external callers.  Internal extraction uses the non-default field approach;
    this helper is the canonical public surface for the hash algorithm.
    """
    import hashlib

    override_items = [
        item for item in (identity_items or [])
        if not item.get("k", "").startswith("vco.baseline")
           and item.get("k", "").startswith("vco.")
    ]
    sorted_items = sorted(override_items, key=lambda x: x.get("k", ""))

    parts = []
    for item in sorted_items:
        k = item.get("k", "")
        q = item.get("q", "")
        v = item.get("v")
        v_str = "" if v is None else str(v)
        parts.append("{}={}:{}".format(k, q, v_str))

    signature = "|".join(parts)
    return hashlib.md5(signature.encode("utf-8")).hexdigest()


def _phase2_partition_items(items):
    """Partition identity_basis items into semantic/cosmetic/unknown buckets."""
    semantic = []
    cosmetic = []
    unknown = []

    for it in (items or []):
        k = safe_str(it.get("k", ""))
        if k in ("vco.baseline_category_path",
                 "vco.baseline_sig_hash",
                 "vco.override_properties_hash"):
            semantic.append(it)
        elif (k.startswith("vco.projection.")
              or k.startswith("vco.cut.")
              or k in ("vco.halftone", "vco.transparency")):
            cosmetic.append(it)
        else:
            unknown.append(it)

    return (
        phase2_sorted_items(semantic),
        phase2_sorted_items(cosmetic),
        phase2_sorted_items(unknown),
    )


def extract(doc, ctx=None):
    """
    Extract view category override records from all view templates.

    Args:
        doc: Revit Document
        ctx: context dict; must contain object_style_row_key_to_sig_hash
             (from object_styles_model) and optionally
             object_style_annotation_row_key_to_sig_hash
             (from object_styles_annotation) for annotation category baselines

    Returns:
        dict with records, hash_v2, count, raw_count, and debug counters.
    """
    info = {
        "count": 0,
        "raw_count": 0,      # (template × category) pairs with any override
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

    try:
        require_domain((ctx or {}).get("_domains", {}), "object_styles_model")
    except Blocked:
        info["debug_v2_blocked"] = True
        return info

    # Merge model and annotation baseline maps.
    # Annotation categories (Grids, Revision Clouds, etc.) live in the annotation map.
    baseline_sig_map = {}
    baseline_sig_map.update(
        (ctx or {}).get("object_style_row_key_to_sig_hash", {}) or {}
    )
    baseline_sig_map.update(
        (ctx or {}).get("object_styles_category_to_sig_hash", {}) or {}
    )
    baseline_sig_map.update(
        (ctx or {}).get("object_style_annotation_row_key_to_sig_hash", {}) or {}
    )

    # Build V/G include-controlled BIP set once
    vg_include_bips = _build_vg_include_bips()

    # -----------------------------------------------------------------------
    # Collect view templates via collect_instances (wraps FEC internally).
    # collect_types is for ElementType subclasses; View instances are not
    # types, so collect_instances is the appropriate API.
    # -----------------------------------------------------------------------
    all_views = []
    try:
        all_views = list(
            collect_instances(doc, of_class=View, where_key="view_category_overrides.views")
        )
    except Exception:
        pass

    templates = [v for v in all_views if _safe_bool(lambda: v.IsTemplate)]
    info["debug_templates_processed"] = len(templates)

    # -----------------------------------------------------------------------
    # Collect categories (top-level + subcategories).
    # -----------------------------------------------------------------------
    all_cats = []  # [(cat_obj, is_sub, parent_or_None)]
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

    # Default OGS used to detect non-default fields
    dflt = OverrideGraphicSettings()

    v2_records = []
    signature_hashes_v2 = []
    v2_any_blocked = False

    for template in templates:
        # --- V/G include-controlled state ---
        tpl_param_ids = set()
        try:
            for p in (template.GetTemplateParameterIds() or []):
                try:
                    tpl_param_ids.add(int(p.IntegerValue))
                except Exception:
                    pass
        except Exception:
            pass

        # Template traceability
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
            # Only govern known category types
            try:
                cat_type_int = int(cat.CategoryType)
            except Exception:
                continue
            if cat_type_int not in _CAT_TYPE_LABELS:
                continue

            info["debug_categories_checked"] += 1

            # Read override from this template for this category
            try:
                ogs = template.GetCategoryOverrides(cat.Id)
            except Exception:
                continue
            if ogs is None:
                continue

            # Extract override items
            proj_items = extract_projection_graphics(doc, ogs, ctx, "vco.projection")
            cut_items = extract_cut_graphics(doc, ogs, ctx, "vco.cut")
            halftone_items = extract_halftone(ogs, "vco.halftone")
            trans_items = extract_transparency(ogs, "vco.transparency")
            all_ogs_items = proj_items + cut_items + halftone_items + trans_items

            # Compare to default OGS to detect any override
            dflt_proj = extract_projection_graphics(doc, dflt, ctx, "vco.projection")
            dflt_cut = extract_cut_graphics(doc, dflt, ctx, "vco.cut")
            dflt_halftone = extract_halftone(dflt, "vco.halftone")
            dflt_trans = extract_transparency(dflt, "vco.transparency")
            dflt_map = {it.get("k"): it.get("v")
                        for it in dflt_proj + dflt_cut + dflt_halftone + dflt_trans}

            actual_map = {it.get("k"): it.get("v") for it in all_ogs_items}
            has_override = any(actual_map.get(k) != dflt_map.get(k) for k in actual_map)

            if not has_override:
                info["debug_no_change"] += 1
                continue

            info["debug_overrides_found"] += 1
            info["raw_count"] += 1

            # --- Build row_key to look up baseline sig_hash ---
            cat_name = safe_str(getattr(cat, "Name", None) or "")
            if is_sub and parent is not None:
                parent_name = safe_str(getattr(parent, "Name", None) or "")
                row_key = "{}|{}".format(parent_name, cat_name)
            else:
                row_key = "{}|self".format(cat_name)

            baseline_sig = baseline_sig_map.get(row_key) if baseline_sig_map else None
            if not baseline_sig:
                info["debug_no_baseline"] += 1

            # --- V/G include_controlled state ---
            cat_type_label = _CAT_TYPE_LABELS.get(cat_type_int, safe_str(cat_type_int))
            vg_bip = vg_include_bips.get(cat_type_label)
            include_controlled = vg_bip is not None and vg_bip in tpl_param_ids

            # --- Identity items ---
            rk_v, rk_q = canonicalize_str(row_key)
            bs_v, bs_q = canonicalize_str(baseline_sig) if baseline_sig else (None, ITEM_Q_MISSING)

            # override_properties_hash = hash of non-default field items only
            non_dflt_items = [
                it for it in all_ogs_items
                if actual_map.get(it.get("k")) != dflt_map.get(it.get("k"))
            ]
            non_dflt_sorted = sorted(non_dflt_items, key=lambda x: x.get("k", ""))
            oph_preimage = serialize_identity_items(non_dflt_sorted) if non_dflt_sorted else "|empty|"
            override_props_hash = make_hash(oph_preimage)
            oph_v, oph_q = canonicalize_str(override_props_hash)

            identity_items = (
                [
                    make_identity_item("vco.baseline_category_path", rk_v, rk_q),
                    make_identity_item("vco.baseline_sig_hash", bs_v, bs_q),
                    make_identity_item("vco.override_properties_hash", oph_v, oph_q),
                ]
                + sorted(all_ogs_items, key=lambda x: x.get("k", ""))
            )
            identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))

            # sig_hash
            preimage = serialize_identity_items(identity_items_sorted)
            sig_hash = make_hash(preimage) if preimage else None

            # Status computation
            required_qs = [rk_q, bs_q, oph_q]
            blocked = any(q != ITEM_Q_OK for q in required_qs) or sig_hash is None
            any_incomplete = any(it.get("q") != ITEM_Q_OK for it in identity_items_sorted)
            status_reasons = []
            for it in identity_items_sorted:
                if it.get("q") != ITEM_Q_OK:
                    status_reasons.append("identity.incomplete:{}:{}".format(
                        it.get("q"), it.get("k")))

            record_id = "vco_{}_{}".format(
                safe_str(row_key),
                safe_str(sig_hash or "blocked")[:8],
            )
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
                    domain="view_category_overrides",
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
                    domain="view_category_overrides",
                    record_id=record_id,
                    status=status,
                    status_reasons=sorted(set(status_reasons)),
                    sig_hash=sig_hash,
                    identity_items=identity_items_sorted,
                    required_qs=required_qs,
                    label=label,
                )
                signature_hashes_v2.append(sig_hash)

            # --- Phase 2 partitioning ---
            p2_semantic, p2_cosmetic, p2_unknown = _phase2_partition_items(identity_items_sorted)

            coordination = [
                make_identity_item(
                    "vco.include_controlled",
                    "true" if include_controlled else "false",
                    ITEM_Q_OK,
                ),
                make_identity_item("vco.vg_category_type", cat_type_label, ITEM_Q_OK),
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
                unknown_extra.append(
                    make_identity_item("vco.template_element_id", None, ITEM_Q_UNREADABLE))
            try:
                uid_v, uid_q = canonicalize_str(tpl_uid)
                unknown_extra.append(make_identity_item("vco.template_unique_id", uid_v, uid_q))
            except Exception:
                unknown_extra.append(
                    make_identity_item("vco.template_unique_id", None, ITEM_Q_UNREADABLE))

            rec["phase2"] = {
                "schema": "phase2.view_category_overrides.v1",
                "grouping_basis": "join_key.join_hash",
                "cosmetic_items": phase2_sorted_items(p2_cosmetic + cosmetic),
                "coordination_items": phase2_sorted_items(coordination),
                "unknown_items": phase2_sorted_items(p2_unknown + unknown_extra),
            }

            pol = get_domain_join_key_policy(
                (ctx or {}).get("join_key_policies"), "view_category_overrides")
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
                "schema": "view_category_overrides.sig_basis.v1",
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
    # else: 0 override records is valid — no overrides in this model
    # hash_v2 stays None, debug_v2_blocked stays False

    # Keep legacy ctx key for backwards compatibility
    if ctx is not None:
        ctx["view_category_overrides_sig_hash"] = {}

    return info


def _safe_bool(fn, default=False):
    """Evaluate a zero-arg callable, returning default on any exception."""
    try:
        return bool(fn())
    except Exception:
        return default
