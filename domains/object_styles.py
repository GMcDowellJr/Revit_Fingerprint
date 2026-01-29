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

from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
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

try:
    from Autodesk.Revit.DB import GraphicsStyleType, CategoryType
except ImportError:
    GraphicsStyleType = None
    CategoryType = None

def extract(doc, ctx=None):
    """
    Extract Object Styles fingerprint from document.

    Legacy surfaces:
      - info["hash"], info["legacy_records"], info["signature_hashes"]

    record.v2 surfaces:
      - info["hash_v2"], info["records"] (record.v2 dicts), info["signature_hashes_v2"]

    Pattern references:
      - Uses line_patterns record.v2 sig_hash via ctx["line_pattern_uid_to_hash_v2"].
      - No sentinel literals are injected into identity items.
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "legacy_records": [],
        "signature_hashes": [],
        "hash": None,

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

    # Dependency map: LinePatternElement.UniqueId -> line_patterns record.v2 sig_hash
    lp_uid_to_sig_hash_v2 = None
    try:
        lp_uid_to_sig_hash_v2 = (ctx or {}).get("line_pattern_uid_to_hash_v2", None) if ctx is not None else None
        if not isinstance(lp_uid_to_sig_hash_v2, dict) or not lp_uid_to_sig_hash_v2:
            lp_uid_to_sig_hash_v2 = None
    except Exception:
        lp_uid_to_sig_hash_v2 = None

    # Legacy map (def_hash) used for legacy signature only
    lp_uid_to_hash_legacy = {}
    try:
        lp_uid_to_hash_legacy = (ctx or {}).get("line_pattern_uid_to_hash", {}) if ctx is not None else {}
        if not isinstance(lp_uid_to_hash_legacy, dict):
            lp_uid_to_hash_legacy = {}
    except Exception:
        lp_uid_to_hash_legacy = {}

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
    legacy_records = []
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

                # -------------------------
                # Legacy signature (UNCHANGED behavior)
                # -------------------------
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

                # Line pattern (legacy): prefer def_hash via ctx map; else S_UNREADABLE (unmapped pattern)
                try:
                    lp_id = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
                    lp_val = S_MISSING
                    if lp_id and getattr(lp_id, "IntegerValue", 0) > 0:
                        lp_e = doc.GetElement(lp_id)
                        lp_uid = canon_str(getattr(lp_e, "UniqueId", None)) if lp_e else None
                        lp_val = lp_uid_to_hash_legacy.get(lp_uid) or S_UNREADABLE
                except Exception:
                    lp_val = S_MISSING

                legacy_sig = "|".join([
                    safe_str(parent_name),
                    safe_str(row_name),
                    safe_str(cat_type),
                    safe_str(w_proj_legacy),
                    safe_str(w_cut_legacy),
                    safe_str(rgb_sig_legacy),
                    safe_str(lp_val),
                ])
                legacy_records.append(legacy_sig)

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

                # Optional: projection weight
                wproj_v, wproj_q = canonicalize_int(w_proj_legacy)
                if wproj_q != ITEM_Q_OK:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("weight_projection_missing_or_unreadable")
                identity_items.append(make_identity_item("obj_style.weight.projection", wproj_v, wproj_q))

                # Optional: cut weight (many categories legitimately lack cut -> treat as unsupported)
                if w_cut_legacy is None:
                    wcut_v, wcut_q = None, ITEM_Q_UNSUPPORTED
                else:
                    wcut_v, wcut_q = canonicalize_int(w_cut_legacy)
                identity_items.append(make_identity_item("obj_style.weight.cut", wcut_v, wcut_q))

                # Optional: color
                rgb_v, rgb_q = canonicalize_str(None if rgb_sig_legacy in {S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE} else rgb_sig_legacy)
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

                identity_items_sorted = sorted(identity_items, key=lambda d: str(d.get("k", "")))
                preimage_v2 = serialize_identity_items(identity_items_sorted)
                sig_hash_v2 = make_hash(preimage_v2)

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
                )

                # -------------------------
                # Phase-2 additions (additive, explanatory, reversible)
                # -------------------------
                pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "object_styles")
                rec_v2["join_key"], _missing = build_join_key_from_policy(
                    domain_policy=pol,
                    identity_items=identity_items_sorted,
                )

                semantic_items = []
                cosmetic_items = []
                unknown_items = []

                # Hypothesis: Object Styles are graphics-driven; treat extracted attributes as cosmetic.
                for it in (identity_items_sorted or []):
                    k = safe_str(it.get("k", ""))
                    if k == "obj_style.row_key":
                        unknown_items.append(it)
                    else:
                        cosmetic_items.append(it)

                # Add CategoryType as unknown context (not part of identity_basis)
                ct_v, ct_q = phase2_qv_from_legacy_sentinel_str(cat_type, allow_empty=False)
                unknown_items.append(make_identity_item("obj_style.category_type", ct_v, ct_q))

                rec_v2["phase2"] = {
                    "schema": "phase2.object_styles.v1",
                    "grouping_basis": "phase2.hypothesis",
                    "semantic_items": phase2_sorted_items(semantic_items),
                    "cosmetic_items": phase2_sorted_items(cosmetic_items),
                    "unknown_items": phase2_sorted_items(unknown_items),
                }

                v2_records.append(rec_v2)

                v2_sig_hashes.append(sig_hash_v2)
                if status_v2 == STATUS_BLOCKED:
                    v2_any_blocked = True

            except Exception:
                info["debug_fail_record_build"] += 1
                continue

    legacy_records_sorted = sorted(legacy_records)
    info["legacy_records"] = legacy_records_sorted
    info["names"] = sorted(set(names))
    info["count"] = len(legacy_records_sorted)
    info["raw_count"] = info["debug_total_categories"]

    info["signature_hashes"] = [make_hash([r]) for r in legacy_records_sorted] if legacy_records_sorted else []
    info["hash"] = make_hash(legacy_records_sorted) if legacy_records_sorted else None

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

    return info
