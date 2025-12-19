# -*- coding: utf-8 -*-
"""
Line Styles domain extractor.

Fingerprints Line Styles (subcategories under Lines category) including:
- Projection/cut lineweights
- Line color
- Line pattern reference

Per-record identity: line style name (name-based, not UniqueId)
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
from canon import canon_str

try:
    from Autodesk.Revit.DB import Category, BuiltInCategory, GraphicsStyleType, ElementId
except ImportError:
    Category = None
    BuiltInCategory = None
    GraphicsStyleType = None
    ElementId = None


def extract(doc, ctx=None):
    """
    Extract Line Styles fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with count, hash, records, record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "records": [],
        "signature_hashes": [],
        "hash": None,

        # (optional) small, domain-local debug counters
        "debug_fail_get_lines_cat": 0,
        "debug_fail_subcats": 0,
        "debug_skipped_no_name": 0,
        "debug_fail_record_build": 0,
    }

    # Only "Lines" category contains actual Line Styles (subcategories)
    try:
        lines_cat = Category.GetCategory(doc, BuiltInCategory.OST_Lines)
    except:
        info["debug_fail_get_lines_cat"] += 1
        lines_cat = None

    if not lines_cat:
        return info

    try:
        subs = list(lines_cat.SubCategories)
    except:
        info["debug_fail_subcats"] += 1
        subs = []

    info["raw_count"] = len(subs)

    records = []
    names = []

    for sc in subs:
        try:
            sc_name = canon_str(getattr(sc, "Name", None))
            if not sc_name:
                info["debug_skipped_no_name"] += 1
                continue
            names.append(sc_name)

            # weights
            try: w_proj = sc.GetLineWeight(GraphicsStyleType.Projection)
            except: w_proj = None
            try: w_cut  = sc.GetLineWeight(GraphicsStyleType.Cut)
            except: w_cut = None

            # color
            try:
                c = sc.LineColor
                rgb_sig = "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
            except:
                rgb_sig = "<None>"

            # Line pattern reference in hash surface:
            # - Must not include UniqueId/GUID/+ElementId
            # - Prefer line_patterns def_hash via ctx map; else deterministic sentinel
            lp_val = "<None>"
            try:
                lp_id = sc.GetLinePatternId(GraphicsStyleType.Projection)
                if lp_id and lp_id != ElementId.InvalidElementId:
                    lp_elem = doc.GetElement(lp_id)
                    lp_uid = getattr(lp_elem, "UniqueId", None) if lp_elem else None
                    lp_map = (ctx or {}).get("line_pattern_uid_to_hash", {}) if ctx is not None else {}
                    lp_val = lp_map.get(lp_uid) or "<LP:UNMAPPED>"
            except:
                lp_val = "<None>"

            # record signature (names ARE identity here by your locked semantics)
            records.append("|".join([
                safe_str(sc_name),
                safe_str(w_proj),
                safe_str(w_cut),   # kept for now (pending decision)
                safe_str(rgb_sig),
                safe_str(lp_val),
            ]))
        except:
            info["debug_fail_record_build"] += 1
            continue

    records_sorted = sorted(records)
    info["records"] = records_sorted
    info["names"] = sorted(set(names))
    info["count"] = len(records_sorted)

    # Per-row signature hashes (metadata; NOT used in global hash)
    info["signature_hashes"] = [make_hash([r]) for r in records_sorted] if records_sorted else []

    info["record_rows"] = []
    if records_sorted:
        sigs = info.get("signature_hashes") or []
        # Defensive: if something ever goes out of sync, fail-soft by pairing "<None>"
        for i, r in enumerate(records_sorted):
            sh = sigs[i] if i < len(sigs) else "<None>"
            info["record_rows"].append({
                "record": r,
                "sig_hash": sh,
            })

    # GLOBAL hash stays EXACTLY the same semantic as before
    info["hash"] = make_hash(records_sorted) if records_sorted else None
    return info
