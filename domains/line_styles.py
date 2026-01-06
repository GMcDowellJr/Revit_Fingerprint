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
        ctx: Context dictionary (used for v2 line pattern hash mapping)

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

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},

        # (optional) small, domain-local debug counters
        "debug_fail_get_lines_cat": 0,
        "debug_fail_subcats": 0,
        "debug_skipped_no_name": 0,
        "debug_fail_record_build": 0,
    }

    # Only "Lines" category contains actual Line Styles (subcategories)
    try:
        lines_cat = Category.GetCategory(doc, BuiltInCategory.OST_Lines)
    except Exception as e:
        info["debug_fail_get_lines_cat"] += 1
        lines_cat = None

    if not lines_cat:
        return info

    try:
        subs = list(lines_cat.SubCategories)
    except Exception as e:
        info["debug_fail_subcats"] += 1
        subs = []

    info["raw_count"] = len(subs)

    records = []
    names = []

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
    except Exception as e:
        lp_map_v2 = None

    for sc in subs:
        try:
            sc_name = canon_str(getattr(sc, "Name", None))
            if not sc_name:
                info["debug_skipped_no_name"] += 1
                continue
            names.append(sc_name)

            # weights
            try: w_proj = sc.GetLineWeight(GraphicsStyleType.Projection)
            except Exception as e:
                w_proj = None
            try: w_cut  = sc.GetLineWeight(GraphicsStyleType.Cut)
            except Exception as e:
                w_cut = None

            # color
            try:
                c = sc.LineColor
                rgb_sig = "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
            except Exception as e:
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
            except Exception as e:
                lp_val = "<None>"

            # record signature (names ARE identity here by your locked semantics)
            records.append("|".join([
                safe_str(sc_name),
                safe_str(w_proj),
                safe_str(w_cut),   # kept for now (pending decision)
                safe_str(rgb_sig),
                safe_str(lp_val),
            ]))

            # ---------------------------
            # v2 signature (contract semantic hash)
            # ---------------------------
            # Exception (explicit): line style name is part of the definition/identity for this domain.
            # Rationale: Revit does not provide a stable built-in id for line style subcategories.
            if not v2_blocked:
                # block on any unreadable / sentinel-like values (no partial coverage semantics)
                if w_proj is None or w_cut is None:
                    v2_blocked = True
                    v2_reasons["unreadable_line_weight"] = True

                if rgb_sig == "<None>":
                    v2_blocked = True
                    v2_reasons["unreadable_line_color"] = True

                # line pattern mapping requirement (upstream v2 dependency)
                try:
                    lp_id_v2 = sc.GetLinePatternId(GraphicsStyleType.Projection)
                except Exception as e:
                    lp_id_v2 = None

                if lp_id_v2 is None or lp_id_v2 == ElementId.InvalidElementId:
                    v2_blocked = True
                    v2_reasons["unreadable_line_pattern_id"] = True
                else:
                    if lp_map_v2 is None:
                        v2_blocked = True
                        v2_reasons["dependency_missing_line_patterns_v2"] = True
                    else:
                        try:
                            lp_elem_v2 = doc.GetElement(lp_id_v2)
                            lp_uid_v2 = getattr(lp_elem_v2, "UniqueId", None) if lp_elem_v2 else None
                        except Exception as e:
                            lp_uid_v2 = None

                        lp_hash_v2 = lp_map_v2.get(lp_uid_v2) if lp_uid_v2 else None
                        if not lp_hash_v2:
                            v2_blocked = True
                            v2_reasons["unmapped_line_pattern_v2"] = True
                        else:
                            v2_records.append("|".join([
                                safe_str(sc_name),
                                safe_str(w_proj),
                                safe_str(w_cut),
                                safe_str(rgb_sig),
                                safe_str(lp_hash_v2),
                            ]))
        except Exception as e:
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

    # v2 hash (domain-level block; no partial coverage semantics)
    info["debug_v2_blocked"] = bool(v2_blocked)
    info["debug_v2_block_reasons"] = v2_reasons if v2_blocked else {}

    if (not v2_blocked) and v2_records:
        info["hash_v2"] = make_hash(sorted(v2_records))
    else:
        # If v2 wasn't explicitly blocked but we produced no v2 records, make the null explainable.
        if (not v2_blocked) and (not v2_records):
            v2_blocked = True
            v2_reasons["no_v2_records"] = True
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"] = v2_reasons
        info["hash_v2"] = None

    return info
