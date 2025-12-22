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

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from hashing import make_hash, safe_str
from canon import canon_str, rgb_sig_from_color

try:
    from Autodesk.Revit.DB import GraphicsStyleType, CategoryType
except ImportError:
    GraphicsStyleType = None
    CategoryType = None

def extract(doc, ctx=None):
    """
    Extract Object Styles fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (used for v2 mappings + optional output controls)

    Returns:
        Dictionary with count, hash, signature_hashes, category_hashes,
        records, record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "hash": None,
        "signature_hashes": [],
        "category_hashes": {},
        "records": [],

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},

        # debug counters
        "debug_total_categories": 0,
        "debug_rows_emitted": 0,
        "debug_skipped_import": 0,
        "debug_skipped_cad_layers": 0,
        "debug_fail_row": 0
    }

    # Output controls (default preserve legacy payload)
    emit_records = False
    include_cad_layer_categories = False
    try:
        if ctx is not None:
            emit_records = bool(ctx.get("emit_records", True))
            include_cad_layer_categories = bool(ctx.get("include_cad_layer_categories", True))
    except:
        emit_records = True
        include_cad_layer_categories = True

    row_pairs = []

    try:
        cats = doc.Settings.Categories
    except:
        return info

    # v2 build state (domain-level block; no partial coverage semantics)
    v2_records = []
    v2_blocked = False
    v2_reasons = {}

    # Dependency: line_patterns semantic_v2 must provide UID -> def_hash_v2 map
    lp_map_v2 = None
    try:
        lp_map_v2 = (ctx or {}).get("line_pattern_uid_to_hash_v2", None) if ctx is not None else None
        if not isinstance(lp_map_v2, dict) or not lp_map_v2:
            lp_map_v2 = None
    except:
        lp_map_v2 = None

    def _looks_like_cad_import_category(name_str):
        if not name_str:
            return False
        n = name_str.strip().lower()
        return n.endswith(".dwg") or n.endswith(".dxf") or n.endswith(".dwf")

    def row_sig_legacy(cat_obj, parent_name, row_name, cat_type):
        # Projection / cut lineweights
        try:
            w_proj = cat_obj.GetLineWeight(GraphicsStyleType.Projection)
        except:
            w_proj = None

        try:
            w_cut = cat_obj.GetLineWeight(GraphicsStyleType.Cut)
        except:
            w_cut = None

        # Line color
        try:
            rgb_sig = rgb_sig_from_color(cat_obj.LineColor)
        except:
            rgb_sig = "<None>"

        # Line pattern (Object Styles has ONE pattern, not proj/cut)
        # Hash surface contract: no UniqueId/GUID/+ElementId
        # Prefer line_patterns def_hash via ctx map; else deterministic sentinel
        try:
            lp_id = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
            lp_val = "<None>"
            if lp_id and lp_id.IntegerValue > 0:
                lp_e = doc.GetElement(lp_id)
                lp_uid = canon_str(getattr(lp_e, "UniqueId", None)) if lp_e else None
                lp_map = (ctx or {}).get("line_pattern_uid_to_hash", {}) if ctx is not None else {}
                lp_val = lp_map.get(lp_uid) or "<LP:UNMAPPED>"
        except:
            lp_val = "<None>"

        sig = "|".join([
            safe_str(parent_name),
            safe_str(row_name),
            safe_str(cat_type),
            safe_str(w_proj),
            safe_str(w_cut),
            safe_str(rgb_sig),
            safe_str(lp_val),
        ])
        return sig, w_proj, w_cut, rgb_sig

    def row_sig_v2(cat_obj, parent_name, row_name, cat_type, w_proj, w_cut, rgb_sig):
        """
        v2 signature (contract semantic hash)

        Exception (explicit): object_styles uses names as part of identity/definition.
        Rationale: category/subcategory stable ids are not available or reliable cross-file.
        """
        # Block on unreadables / sentinel-like values (no partial coverage semantics)
        if w_proj is None or w_cut is None:
            return None, "unreadable_line_weight"

        if rgb_sig == "<None>":
            return None, "unreadable_line_color"

        # Line pattern mapping requirement (upstream v2 dependency)
        try:
            lp_id_v2 = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
        except:
            lp_id_v2 = None

        if lp_id_v2 is None:
            return None, "unreadable_line_pattern_id"

        try:
            is_real = bool(lp_id_v2 and lp_id_v2.IntegerValue > 0)
        except:
            is_real = False

        if is_real:
            if lp_map_v2 is None:
                return None, "dependency_missing_line_patterns_v2"

            try:
                lp_elem_v2 = doc.GetElement(lp_id_v2)
                lp_uid_v2 = getattr(lp_elem_v2, "UniqueId", None) if lp_elem_v2 else None
            except:
                lp_uid_v2 = None

            lp_hash_v2 = lp_map_v2.get(lp_uid_v2) if lp_uid_v2 else None
            if not lp_hash_v2:
                return None, "unmapped_line_pattern_v2"
        else:
            # Legacy would emit "<None>" — in v2 we block rather than hash sentinels.
            return None, "no_line_pattern"

        sig_v2 = "|".join([
            safe_str(parent_name),
            safe_str(row_name),
            safe_str(cat_type),
            safe_str(w_proj),
            safe_str(w_cut),
            safe_str(rgb_sig),
            safe_str(lp_hash_v2),
        ])
        return sig_v2, None

    names = []
    records = []
    row_hashes = []
    per_parent_hashes = {}  # parent_name -> list(sig_hashes)

    for cat in cats:
        if cat is None:
            continue

        info["debug_total_categories"] += 1

        # Skip import categories
        try:
            if cat.CategoryType == CategoryType.Import:
                info["debug_skipped_import"] += 1
                continue
        except:
            pass

        # Parent name
        try:
            parent_name = canon_str(cat.Name)
        except:
            continue

        if (not include_cad_layer_categories) and _looks_like_cad_import_category(parent_name):
            info["debug_skipped_cad_layers"] += 1
            continue

        # Category type
        try:
            cat_type = safe_str(cat.CategoryType)
        except:
            cat_type = "<unknown>"

        # Emit the parent row ("<self>")
        try:
            sig, w_proj, w_cut, rgb_sig = row_sig_legacy(cat, parent_name, "<self>", cat_type)
            row_key = "{}|{}".format(parent_name, "<self>")
            names.append(row_key)
            h = make_hash([sig])  # stable, deterministic
            records.append(sig)
            row_hashes.append(h)
            row_pairs.append((sig, h))
            per_parent_hashes.setdefault(parent_name, []).append(h)
            info["debug_rows_emitted"] += 1

            if not v2_blocked:
                sig_v2, reason = row_sig_v2(cat, parent_name, "<self>", cat_type, w_proj, w_cut, rgb_sig)
                if sig_v2 is None:
                    v2_blocked = True
                    v2_reasons[reason] = True
                else:
                    v2_records.append(sig_v2)
        except:
            info["debug_fail_row"] += 1

        # Emit each subcategory row
        subs = []
        try:
            subs = list(cat.SubCategories)
        except:
            subs = []

        for sub in subs:
            try:
                sub_name = canon_str(getattr(sub, "Name", None))
                if not sub_name:
                    continue

                sig, w_proj, w_cut, rgb_sig = row_sig_legacy(sub, parent_name, sub_name, cat_type)
                row_key = "{}|{}".format(parent_name, sub_name)
                names.append(row_key)
                h = make_hash([sig])
                records.append(sig)
                row_hashes.append(h)
                per_parent_hashes.setdefault(parent_name, []).append(h)
                info["debug_rows_emitted"] += 1

                if not v2_blocked:
                    sig_v2, reason = row_sig_v2(sub, parent_name, sub_name, cat_type, w_proj, w_cut, rgb_sig)
                    if sig_v2 is None:
                        v2_blocked = True
                        v2_reasons[reason] = True
                    else:
                        v2_records.append(sig_v2)
            except:
                info["debug_fail_row"] += 1
                continue

    records_sorted = sorted(records)
    row_hashes_sorted = sorted(row_hashes)

    info["raw_count"] = len(names)
    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = records_sorted
    info["signature_hashes"] = row_hashes_sorted
    info["count"] = len(records_sorted)
    info["hash"] = make_hash(row_hashes_sorted) if row_hashes_sorted else None
    info["record_rows"] = []
    if row_pairs:
        row_pairs_sorted = sorted(row_pairs, key=lambda t: t[0])
        info["record_rows"] = [{"record": s, "sig_hash": h} for (s, h) in row_pairs_sorted]

    # Per-parent rollups
    cat_hashes = {}
    for pname, hs in per_parent_hashes.items():
        hs_sorted = sorted(hs)
        cat_hashes[pname] = make_hash(hs_sorted) if hs_sorted else None
    info["category_hashes"] = cat_hashes

    # v2 hash (domain-level block; no partial coverage semantics)
    info["debug_v2_blocked"] = bool(v2_blocked)
    info["debug_v2_block_reasons"] = v2_reasons if v2_blocked else {}
    if (not v2_blocked) and v2_records:
        info["hash_v2"] = make_hash(sorted(v2_records))
    else:
        info["hash_v2"] = None

    # Optional payload suppression (does not affect hash)
    if not emit_records:
        info["records"] = []
        info["record_rows"] = []

    return info
