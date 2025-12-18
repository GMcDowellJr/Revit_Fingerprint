# -*- coding: utf-8 -*-
"""
Text Types domain extractor.

Fingerprints text note types including:
- Font, size, width factor
- Background, border, tab settings
- Line weight, color
- Bold, italic, underline

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
    first_param, _as_string, _as_double, _as_int, _as_bool_from_param,
    format_len_inches, try_get_color_rgb_from_elem,
    get_type_display_name, get_element_display_name
)

try:
    from Autodesk.Revit.DB import FilteredElementCollector, TextNoteType
except ImportError:
    FilteredElementCollector = None
    TextNoteType = None


def extract(doc, ctx=None):
    """
    Extract Text Types fingerprint from document.

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

    types = list(FilteredElementCollector(doc).OfClass(TextNoteType))
    info["raw_count"] = len(types)

    names = []
    missing = 0
    records = []
    sig_hashes = []

    for t in types:
        type_name = get_type_display_name(t)
        if type_name:
            type_name = canon_str(type_name)
            names.append(type_name)
        else:
            missing += 1
            type_name = "<unnamed>"

        # --- core fields ---
        font = _as_string(first_param(t, bip_names=["TEXT_FONT"], ui_names=["Text Font"]))
        size_ft = _as_double(first_param(t, bip_names=["TEXT_SIZE"], ui_names=["Text Size"]))
        size_in = fnum(format_len_inches(size_ft), 6)

        font = canon_str(font)

        width_factor = _as_double(first_param(t, bip_names=["TEXT_WIDTH_SCALE"], ui_names=["Width Factor"]))
        width_factor_n = fnum(width_factor, 6)

        background_i = _as_int(first_param(t, bip_names=["TEXT_BACKGROUND"], ui_names=["Background"]))

        # Graphics
        p_lw = first_param(t, bip_names=["TEXT_LINE_WEIGHT", "LINE_PEN"], ui_names=["Line Weight"])
        line_weight = _as_int(p_lw)

        color_int, color_rgb = try_get_color_rgb_from_elem(t)

        # Border / tabs / styles
        show_border = _as_bool_from_param(first_param(t, ui_names=["Show Border", "Show border"]))
        leader_border_offset_ft = _as_double(first_param(t, ui_names=["Leader/Border Offset", "Leader / Border Offset"]))
        leader_border_offset_in = fnum(format_len_inches(leader_border_offset_ft), 6)

        tab_size_ft = _as_double(first_param(t, ui_names=["Tab Size", "Tab size"]))
        tab_size_in = fnum(format_len_inches(tab_size_ft), 6)

        bold = _as_bool_from_param(first_param(t, ui_names=["Bold"]))
        italic = _as_bool_from_param(first_param(t, ui_names=["Italic"]))
        underline = _as_bool_from_param(first_param(t, ui_names=["Underline"]))

        # Leader Arrowhead (metadata only; do NOT put in core signature)
        leader_arrow_uid = None
        leader_arrow_name = None
        try:
            p_arrow = first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])
            if p_arrow and p_arrow.HasValue:
                ah_eid = p_arrow.AsElementId()
                if ah_eid and ah_eid.IntegerValue > 0:
                    ah = doc.GetElement(ah_eid)
                    if ah:
                        leader_arrow_uid = ah.UniqueId
                        try:
                            leader_arrow_name = get_element_display_name(ah)
                        except:
                            leader_arrow_name = None
        except:
            pass

        # --- signature tuple (core) ---
        signature_tuple = [
            "font={}".format(sig_val(font)),
            "size_in={}".format(sig_val(size_in)),
            "width_factor={}".format(sig_val(width_factor_n)),
            "background={}".format(sig_val(background_i)),
            "line_weight={}".format(sig_val(line_weight)),
            "color_int={}".format(sig_val(color_int)),

            "show_border={}".format(sig_val(show_border)),
            "leader_border_offset_in={}".format(sig_val(leader_border_offset_in)),
            "tab_size_in={}".format(sig_val(tab_size_in)),
            "bold={}".format(sig_val(bold)),
            "italic={}".format(sig_val(italic)),
            "underline={}".format(sig_val(underline)),
        ]
        sig_hash = make_hash(signature_tuple)

        rec = {
            "type_id": safe_str(t.Id.IntegerValue),
            "type_uid": getattr(t, "UniqueId", "") or "",
            "type_name": type_name,

            "font": font,
            "text_size_ft": size_ft,
            "text_size_in": size_in,
            "width_factor": width_factor_n,
            "background_raw": background_i,
            "line_weight": line_weight,

            "color_int": color_int,
            "color_rgb": color_rgb,

            "show_border": show_border,
            "leader_border_offset_in": leader_border_offset_in,
            "tab_size_in": tab_size_in,
            "bold": bold,
            "italic": italic,
            "underline": underline,

            "leader_arrowhead_uid": leader_arrow_uid,
            "leader_arrowhead_name": leader_arrow_name,

            "signature_tuple": signature_tuple,
            "signature_hash": sig_hash
        }

        records.append(rec)
        sig_hashes.append(sig_hash)

    info["debug_missing_name"] = missing

    names_sorted = sorted(set(names))
    info["count"] = len(names_sorted)
    info["names"] = names_sorted

    # new: records + signature-based hash
    info["records"] = sorted(records, key=lambda r: (r.get("type_name",""), r.get("type_id","")))
    info["signature_hashes"] = sorted(sig_hashes)
    info["hash"] = make_hash(sorted(sig_hashes)) if sig_hashes else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("type_uid", "")) or safe_str(r.get("uid", "")),
            "sig_hash":  safe_str(r.get("signature_hash", "")),
            "name":      safe_str(r.get("type_name", "")),   # optional metadata
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
