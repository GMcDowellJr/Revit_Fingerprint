# -*- coding: utf-8 -*-
"""
Object Styles domain extractor.

Fingerprints Category graphics (non-import categories) including:
- Parent category + subcategories
- Projection/cut lineweights
- Line colors
- Line patterns
- Materials

Per-row identity: parent_name|row_name (name-based, not UniqueId)
Ordering: order-insensitive (sorted before hashing)
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    rgb_sig_from_color,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)
from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
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
from core.feature_items import make_feature_item
from core.stratum_features import build_stratum_features_v1
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


# Canonical semantic selector for object_styles sig_hash.
# Canonical evidence source remains identity_basis.items; selector lists avoid
# duplicating k/q/v payloads into additional evidence arrays.
OBJECT_STYLE_SEMANTIC_KEYS = sorted([
    "obj_style.color.rgb",
    "obj_style.pattern_ref.kind",
    "obj_style.pattern_ref.sig_hash",
    "obj_style.weight.cut",
    "obj_style.weight.projection",
])

def extract(doc, ctx=None):
    """
    Extract Object Styles fingerprint from document.

    record.v2 surfaces:
      - info["hash_v2"], info["records"] (record.v2 dicts), info["signature_hashes_v2"]

    Pattern references:
      - Uses line_patterns record.v2 sig_hash via ctx["line_pattern_uid_to_hash"].
      - No sentinel literals are injected into identity items.
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        # record.v2
        "records": [],
        "signature_hashes_v2": [],
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},

        # debug counters
        "debug_total_categories": 0,
        "debug_skipped_import": 0,
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
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    # Dependency map: LinePatternElement.UniqueId -> line_patterns record.v2 sig_hash
    lp_uid_to_sig_hash_v2 = None
    try:
        lp_uid_to_sig_hash_v2 = (ctx or {}).get("line_pattern_uid_to_hash", None) if ctx is not None else None
        if not isinstance(lp_uid_to_sig_hash_v2, dict) or not lp_uid_to_sig_hash_v2:
            lp_uid_to_sig_hash_v2 = None
    except Exception:
        lp_uid_to_sig_hash_v2 = None

    def rgb_sig_from_color(c):
        try:
            return "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
        except Exception:
            return S_MISSING

    try:
        cats = doc.Settings.Categories
    except Exception:
        return info

    names = []
    v2_records = []
    v2_sig_hashes = []
    v2_any_blocked = False
    v2_block_reasons = {}

    # Parent + children iteration
    for cat in list(cats or []):
        info["debug_total_categories"] += 1

        try:
            # Skip import categories
            if cat.CategoryType == CategoryType.Import:
                info["debug_skipped_import"] += 1
                continue
        except Exception:
            # If CategoryType can't be read, treat as non-import (fail open for legacy parity)
            pass

        try:
            parent_name = canon_str(getattr(cat, "Name", None))
        except Exception:
            parent_name = None

        if not parent_name:
            info["debug_skipped_no_name"] += 1
            continue

        # Determine a stable cat_type string for legacy signature
        try:
            cat_type = safe_str(getattr(cat, "CategoryType", None))
        except Exception:
            cat_type = S_MISSING

        # Rows: parent "self" then each subcategory
        rows = [("self", cat)]
        try:
            subs = list(getattr(cat, "SubCategories", []) or [])
            for sc in subs:
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
                names.append(row_key)

                try:
                    w_proj_legacy = cat_obj.GetLineWeight(GraphicsStyleType.Projection)
                except Exception:
                    w_proj_legacy = None
                try:
                    w_cut_legacy = cat_obj.GetLineWeight(GraphicsStyleType.Cut)
                except Exception:
                    w_cut_legacy = None

                try:
                    rgb_sig_legacy = rgb_sig_from_color(cat_obj.LineColor)
                except Exception:
                    rgb_sig_legacy = S_MISSING

                # -------------------------
                # record.v2 identity + sig_hash (NO sentinel literals; q marks missing/unreadable)
                # -------------------------
                status_reasons = []
                status_v2 = STATUS_OK

                identity_items = []

                # Required: row key
                rk_v, rk_q = canonicalize_str(row_key)
                identity_items.append(make_identity_item("obj_style.row_key", rk_v, rk_q))
                required_qs = [rk_q]

                # Extract graphics using shared helpers (category surface)
                proj_items = extract_projection_graphics(
                    doc,
                    cat_obj,
                    ctx,
                    key_prefix="obj_style.projection",
                )
                cut_items = extract_cut_graphics(
                    doc,
                    cat_obj,
                    ctx,
                    key_prefix="obj_style.cut",
                )
                # Keep halftone extraction centralized (not part of legacy identity set).
                _halftone_items = extract_halftone(cat_obj, key_prefix="obj_style.halftone")

                proj_items_by_key = {it.get("k"): it for it in (proj_items or [])}
                cut_items_by_key = {it.get("k"): it for it in (cut_items or [])}

                # Optional: projection weight
                proj_weight_item = proj_items_by_key.get("obj_style.projection.line_weight", {}) or {}
                wproj_v = proj_weight_item.get("v", None)
                wproj_q = proj_weight_item.get("q", ITEM_Q_MISSING)
                if w_proj_legacy is None and wproj_q != ITEM_Q_OK:
                    wproj_v, wproj_q = None, ITEM_Q_MISSING
                if wproj_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("weight_projection_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.weight.projection", wproj_v, wproj_q))

                # Optional: cut weight (many categories legitimately lack cut -> treat as unsupported)
                if w_cut_legacy is None:
                    wcut_v, wcut_q = None, ITEM_Q_UNSUPPORTED
                else:
                    cut_weight_item = cut_items_by_key.get("obj_style.cut.line_weight", {}) or {}
                    wcut_v = cut_weight_item.get("v", None)
                    wcut_q = cut_weight_item.get("q", ITEM_Q_MISSING)
                identity_items.append(make_identity_item("obj_style.weight.cut", wcut_v, wcut_q))

                # Optional: color
                proj_color_item = proj_items_by_key.get("obj_style.projection.color.rgb", {}) or {}
                rgb_v = proj_color_item.get("v", None)
                rgb_q = proj_color_item.get("q", ITEM_Q_MISSING)
                if rgb_sig_legacy in {S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE}:
                    rgb_v, rgb_q = None, ITEM_Q_MISSING
                if rgb_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("color_rgb_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.color.rgb", rgb_v, rgb_q))

                # Optional: pattern reference (sig_hash from line_patterns record.v2)
                lp_kind_v = None
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
                    lp_kind_v = "ref"
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
                        else:
                            status_v2 = STATUS_DEGRADED
                            status_reasons.append("line_pattern_uid_missing")
                else:
                    lp_kind_v = "solid"

                kind_v, kind_q = canonicalize_str(lp_kind_v)
                if kind_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("pattern_kind_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.pattern_ref.kind", kind_v, kind_q))

                if lp_sig_hash_q == ITEM_Q_OK:
                    identity_items.append(make_identity_item("obj_style.pattern_ref.sig_hash", lp_sig_hash_v, lp_sig_hash_q))

                # Enforce minima: required not-ok => blocked
                if any(q != ITEM_Q_OK for q in required_qs):
                    status_v2 = STATUS_BLOCKED
                    status_reasons.append("required_identity_not_ok")

                # Canonical evidence source for this domain is identity_basis.items.
                # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used)
                # define hash subsets without duplicating k/q/v payloads.
                identity_items_sorted = sorted(identity_items, key=lambda d: str(d.get("k", "")))
                semantic_items = [
                    it for it in identity_items_sorted
                    if safe_str(it.get("k", "")) in set(OBJECT_STYLE_SEMANTIC_KEYS)
                ]
                preimage_v2 = serialize_identity_items(semantic_items)
                sig_hash_v2 = make_hash(preimage_v2)

                # ---------------------------
                # Discovery feature surface (Phase-2 computes stats / classification)
                # ---------------------------
                def _pick_identity_item(items, key):
                    for it in (items or []):
                        if it.get("k") == key:
                            return it
                    return None

                features_items = []
                for _k, _t in (
                    ("object_style.tab", "s"),
                    ("object_style.category_type", "s"),
                    ("object_style.is_subcategory", "b"),
                    ("object_style.can_add_subcategory", "b"),
                    ("object_style.is_cuttable", "b"),
                    ("object_style.allows_visibility_control", "b"),
                    ("object_style.has_material_quantities", "b"),
                    ("object_style.color_rgb", "s"),
                    ("object_style.weight_projection", "i"),
                    ("object_style.pattern_name", "s"),
                ):
                    _it = _pick_identity_item(identity_items_sorted, _k)
                    if _it is not None:
                        features_items.append(make_feature_item(_k, _t, _it.get("v"), _it.get("q")))
                        
                rec_v2 = build_record_v2(
                    domain="object_styles",
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
                    features_items=features_items,
                    debug={
                        "stratum_features": build_stratum_features_v1(
                            domain="object_styles",
                            identity_items=identity_items_sorted,
                        ),
                    },
                )

                # -------------------------
                # Phase-2 additions (additive, explanatory, reversible)
                # -------------------------
                pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "object_styles")
                rec_v2["join_key"], _missing = build_join_key_from_policy(
                    domain_policy=pol,
                    identity_items=identity_items_sorted,
                    include_optional_items=False,
                    emit_keys_used=True,
                    hash_optional_items=False,
                    emit_items=False,
                    emit_selectors=True,
                )

                cosmetic_items = []
                unknown_items = []

                # Graphics are behavioral (semantic), category name is cosmetic.
                for it in (identity_items_sorted or []):
                    k = safe_str(it.get("k", ""))
                    if k == "obj_style.row_key":
                        cosmetic_items.append(it)
                    else:
                        unknown_items.append(it)

                # Add CategoryType as unknown context (not part of identity_basis)
                ct_v, ct_q = phase2_qv_from_legacy_sentinel_str(cat_type, allow_empty=False)
                unknown_items.append(make_identity_item("obj_style.category_type", ct_v, ct_q))

                # Traceability fields (metadata only — never in hash/sig/join)
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
                    "schema": "phase2.object_styles.v1",
                    "grouping_basis": "phase2.hypothesis",
                    # Semantic selector references identity_basis.items.
                    # Deprecated direction: do not duplicate semantic k/q/v evidence here.
                    "cosmetic_items": phase2_sorted_items(cosmetic_items),
                    "coordination_items": phase2_sorted_items([]),
                    "unknown_items": phase2_sorted_items(unknown_items),
                }
                rec_v2["sig_basis"] = {
                    "schema": "object_styles.sig_basis.v1",
                    "keys_used": OBJECT_STYLE_SEMANTIC_KEYS,
                }

                v2_records.append(rec_v2)

                v2_sig_hashes.append(sig_hash_v2)
                if status_v2 == STATUS_BLOCKED:
                    v2_any_blocked = True

            except Exception:
                info["debug_fail_record_build"] += 1
                continue

    info["names"] = sorted(set(names))
    info["count"] = len(v2_records)
    info["raw_count"] = info["debug_total_categories"]

    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)

    if v2_any_blocked:
        info["debug_v2_blocked"] = True
        v2_block_reasons["one_or_more_records_blocked"] = True

    if (not v2_any_blocked) and info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        if (not v2_any_blocked) and (not info["signature_hashes_v2"]):
            info["debug_v2_blocked"] = True
            v2_block_reasons["no_v2_records"] = True
        info["hash_v2"] = None

    if v2_block_reasons:
        info["debug_v2_block_reasons"] = v2_block_reasons

    info["record_rows"] = [
        {"record_key": safe_str(r.get("record_id", "")), "sig_hash": r.get("sig_hash", None)}
        for r in info["records"]
    ]

    # Export baseline map for downstream domains (view_category_overrides, view_templates)
    if ctx is not None:
        category_to_sig_hash = {}
        category_records = {}  # Full records for detailed comparison

        records = info.get("records") or []
        for rec in records:
            row_key = rec.get("identity_basis", {}).get("items", [])
            # Find row_key value (obj_style.row_key)
            for item in row_key:
                if item.get("k") == "obj_style.row_key":
                    key = item.get("v")
                    sig_hash = rec.get("sig_hash")

                    if key and sig_hash:
                        category_to_sig_hash[key] = sig_hash
                        category_records[sig_hash] = rec
                    break

        # Export to context
        ctx["object_styles_category_to_sig_hash"] = category_to_sig_hash
        ctx["object_styles_records"] = category_records

        # Debug info
        info["debug_exported_baseline_count"] = len(category_to_sig_hash)

    return info
