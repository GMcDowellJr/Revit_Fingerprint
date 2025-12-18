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
        ctx: Context dictionary (unused for this domain)

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
        # debug counters
        "debug_total_categories": 0,
        "debug_rows_emitted": 0,
        "debug_skipped_import": 0,
        "debug_fail_row": 0
    }

    row_pairs = []

    try:
        cats = doc.Settings.Categories
    except:
        return info

    def row_sig(cat_obj, parent_name, row_name, cat_type):
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
            col = cat_obj.LineColor
            rgb_sig = rgb_sig_from_color(col)
        except:
            rgb_sig = "<None>"

        # Line pattern (Object Styles has ONE pattern, not proj/cut)
        try:
            lp_id = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
            lp_val = "<None>"
            if lp_id and lp_id.IntegerValue > 0:
                lp_e = doc.GetElement(lp_id)
                lp_val = canon_str(getattr(lp_e, "UniqueId", None)) or "<None>"
        except:
            lp_val = "<None>"

        # Category material Id
        # Material (UID for stability)
        try:
            mat_id = cat_obj.Material
            mat_val = "<None>"
            if mat_id and mat_id.IntegerValue > 0:
                m = doc.GetElement(mat_id)
                mat_val = canon_str(getattr(m, "UniqueId", None)) or "<None>"
        except:
            mat_val = "<None>"

        # Deterministic row signature
        return "|".join([
            parent_name,
            row_name,
            cat_type,
            safe_str(w_proj),
            safe_str(w_cut),
            rgb_sig,
            lp_val,
            mat_val
        ])

    records = []
    row_hashes = []
    names = []
    per_parent_hashes = {}  # parent_name -> [row_hash,...]

    for cat in cats:
        info["debug_total_categories"] += 1
        if cat is None:
            continue

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

        # Category type
        try:
            cat_type = safe_str(cat.CategoryType)
        except:
            cat_type = "<unknown>"

        # Emit the parent row ("<self>")
        try:
            sig = row_sig(cat, parent_name, "<self>", cat_type)
            row_key = "{}|{}".format(parent_name, "<self>")
            names.append(row_key)
            h = make_hash([sig])  # stable, deterministic
            records.append(sig)
            row_hashes.append(h)
            row_pairs.append((sig, h))
            per_parent_hashes.setdefault(parent_name, []).append(h)
            info["debug_rows_emitted"] += 1
        except:
            info["debug_fail_row"] += 1

        # Emit each subcategory row
        try:
            subs = cat.SubCategories
        except:
            subs = None

        if subs:
            for sub in subs:
                try:
                    sub_name = canon_str(sub.Name)
                    row_key = "{}|{}".format(parent_name, sub_name)
                    names.append(row_key)
                    sig = row_sig(sub, parent_name, sub_name, cat_type)
                    h = make_hash([sig])
                    records.append(sig)
                    row_hashes.append(h)
                    per_parent_hashes.setdefault(parent_name, []).append(h)
                    info["debug_rows_emitted"] += 1
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

    return info
