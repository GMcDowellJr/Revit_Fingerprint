# -*- coding: utf-8 -*-
"""
Dimension Types domain extractor.

Fingerprints dimension types including:
- Text font, size
- Line weight, color
- Tick mark (arrowhead)
- Witness line control

Per-record identity: UniqueId
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
from canon import canon_str, sig_val, fnum
from rows import (
    first_param, _as_string, _as_double, _as_int,
    format_len_inches, try_get_color_rgb_from_elem,
    get_type_display_name, get_element_display_name
)

try:
    from Autodesk.Revit.DB import FilteredElementCollector, DimensionType
except ImportError:
    FilteredElementCollector = None
    DimensionType = None


def extract(doc, ctx=None):
    """
    Extract Dimension Types fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "names": [],
        "hash": None,

        # new
        "records": [],
        "signature_hashes": [],
        "raw_count": 0,
        "debug_missing_name": 0
    }

    types = list(FilteredElementCollector(doc).OfClass(DimensionType))
    info["raw_count"] = len(types)

    names = []
    missing = 0
    records = []
    sig_hashes = []

    for d in types:
        type_name = get_type_display_name(d)
        if type_name:
            type_name = canon_str(type_name)
            if type_name:
                names.append(type_name)
            else:
                missing += 1
                continue
        else:
            missing += 1
            continue

        # --- minimal dim-style signature (text + graphics + ticks) ---
        text_font = _as_string(first_param(d, ui_names=["Text Font"]))
        text_font = canon_str(text_font)

        text_size_ft = _as_double(first_param(d, ui_names=["Text Size"]))
        text_size_in = fnum(format_len_inches(text_size_ft), 6)

        lw = _as_int(first_param(d, ui_names=["Line Weight"]))
        color_int, color_rgb = try_get_color_rgb_from_elem(d)

        # Tick Mark (arrowhead) – store UniqueId metadata + include NAME in signature (more stable than ids)

        tick_name = _as_string(first_param(d, ui_names=["Tick Mark"]))
        tick_uid = None
        try:
            p_tick = first_param(d, ui_names=["Tick Mark"])
            if p_tick and p_tick.HasValue:
                tid = p_tick.AsElementId()
                if tid and tid.IntegerValue > 0:
                    te = doc.GetElement(tid)
                    if te:
                        tick_uid = te.UniqueId
                        # prefer element.Name where available
                        try:
                            tick_name = tick_name or get_element_display_name(te)
                            if tick_name is not None:
                                tick_name = canon_str(tick_name)
                        except:
                            pass
        except:
            pass

        # Witness line control is common; keep as metadata + optional signature
        witness = _as_string(first_param(d, ui_names=["Witness Line Control"]))
        witness = canon_str(witness)

        tick_name = canon_str(tick_name)

        signature_tuple = [
            "text_font={}".format(sig_val(text_font)),
            "text_size_in={}".format(sig_val(text_size_in)),
            "line_weight={}".format(sig_val(lw)),
            "color_int={}".format(sig_val(color_int)),
            "tick_mark={}".format(sig_val(tick_name)),
            "witness_ctrl={}".format(sig_val(witness)),
        ]

        sig_hash = make_hash(signature_tuple)

        rec = {
            "type_id": safe_str(d.Id.IntegerValue),
            "type_uid": getattr(d, "UniqueId", "") or "",
            "type_name": type_name,

            "text_font": text_font,
            "text_size_ft": text_size_ft,
            "text_size_in": text_size_in,

            "line_weight": lw,
            "color_int": color_int,
            "color_rgb": color_rgb,

            "tick_mark_name": tick_name,
            "tick_mark_uid": tick_uid,
            "witness_line_control": witness,

            "signature_tuple": signature_tuple,
            "signature_hash": sig_hash
        }

        records.append(rec)
        sig_hashes.append(sig_hash)

    info["debug_missing_name"] = missing

    names_sorted = sorted(set(names))
    info["count"] = len(names_sorted)
    info["names"] = names_sorted

    info["records"] = sorted(records, key=lambda r: (r.get("type_name",""), r.get("type_id","")))
    info["signature_hashes"] = sorted(sig_hashes)
    info["hash"] = make_hash(sorted(sig_hashes)) if sig_hashes else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("type_uid", "")),
            "sig_hash":  safe_str(r.get("signature_hash", "")),
            "name":      safe_str(r.get("type_name", "")),   # optional metadata
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
