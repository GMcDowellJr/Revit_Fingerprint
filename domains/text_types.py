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

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    sig_val,
    fnum,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.rows import (
    first_param, _as_string, _as_double, _as_int, _as_bool_from_param,
    format_len_inches, try_get_color_rgb_from_elem,
    get_type_display_name, get_element_display_name
)

try:
    from Autodesk.Revit.DB import TextNoteType
except ImportError:
    TextNoteType = None

def extract(doc, ctx=None):
    """
    Extract Text Types fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused here; present for extractor parity)

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
    }

    types = []
    try:
        types = list(
            collect_types(
                doc,
                of_class=TextNoteType,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="text_types:TextNoteType:types",
            )
        )
    except Exception as e:
        types = []

    info["raw_count"] = len(types)

    names = []
    missing = 0
    records = []
    sig_hashes = []

    # v2 build state (domain-level block; no partial coverage semantics)
    v2_records = []
    v2_blocked = False
    v2_reasons = {}

    def _v2_block(reason_key):
        nonlocal v2_blocked
        if not v2_blocked:
            v2_blocked = True
        v2_reasons[reason_key] = True

    for t in types:
        type_name = get_type_display_name(t)
        if type_name:
            type_name = canon_str(type_name)
            names.append(type_name)
        else:
            missing += 1
            type_name = S_MISSING

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
            if p_arrow:
                arrow_id = p_arrow.AsElementId()
                if arrow_id and arrow_id.IntegerValue > 0:
                    arrow = doc.GetElement(arrow_id)
                    if arrow:
                        leader_arrow_uid = getattr(arrow, "UniqueId", None)
                        leader_arrow_name = get_type_display_name(arrow) or getattr(arrow, "Name", None)
                        leader_arrow_name = canon_str(leader_arrow_name)
        except Exception as e:
            leader_arrow_uid = None
            leader_arrow_name = None

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

        # ---------------------------
        # v2 signature (contract semantic hash)
        # ---------------------------
        # Exception (explicit): text type name is part of identity/definition in this domain.
        # Leader Arrowhead is excluded from v2 by policy decision.
        if not v2_blocked:
            # Block on unreadables / sentinel-like values (no partial coverage semantics)
            if not font:
                _v2_block("unreadable_font")

            # numeric fields must be non-None and must not be sentinel strings
            if size_ft is None:
                _v2_block("unreadable_text_size")
            if width_factor is None:
                _v2_block("unreadable_width_factor")
            if background_i is None:
                _v2_block("unreadable_background")
            if line_weight is None:
                _v2_block("unreadable_line_weight")

            # color must be readable; use RGB in v2 (avoid element ids / GUIDs)
            if canon_str(color_rgb) in (S_MISSING, S_UNREADABLE):
                 _v2_block("unreadable_color_rgb")

            # boolean-ish fields must be non-None
            if show_border is None:
                _v2_block("unreadable_show_border")
            if bold is None:
                _v2_block("unreadable_bold")
            if italic is None:
                _v2_block("unreadable_italic")
            if underline is None:
                _v2_block("unreadable_underline")

            # offsets/tab sizes must be readable doubles
            if leader_border_offset_ft is None:
                _v2_block("unreadable_leader_border_offset")
            if tab_size_ft is None:
                _v2_block("unreadable_tab_size")

            if not v2_blocked:
                # Use the already-normalized representations where applicable
                v2_records.append("|".join([
                    "name={}".format(safe_str(type_name)),
                    "font={}".format(safe_str(font)),
                    "size_in={}".format(safe_str(size_in)),
                    "width_factor={}".format(safe_str(width_factor_n)),
                    "background={}".format(safe_str(background_i)),
                    "line_weight={}".format(safe_str(line_weight)),
                    "color_rgb={}".format(safe_str(color_rgb)),

                    "show_border={}".format(safe_str(show_border)),
                    "leader_border_offset_in={}".format(safe_str(leader_border_offset_in)),
                    "tab_size_in={}".format(safe_str(tab_size_in)),
                    "bold={}".format(safe_str(bold)),
                    "italic={}".format(safe_str(italic)),
                    "underline={}".format(safe_str(underline)),
                ]))

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

    info["names"] = sorted(names)
    info["count"] = len(info["names"])

    info["records"] = sorted(records, key=lambda r: safe_str(r.get("type_name", "")))
    info["signature_hashes"] = sorted(sig_hashes)
    info["hash"] = make_hash(sorted(sig_hashes)) if sig_hashes else None

    # v2 hash (domain-level block; no partial coverage semantics)
    info["debug_v2_blocked"] = bool(v2_blocked)
    info["debug_v2_block_reasons"] = v2_reasons if v2_blocked else {}
    if (not v2_blocked) and v2_records:
        info["hash_v2"] = make_hash(sorted(v2_records))
    else:
        info["hash_v2"] = None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("type_uid", "")) or safe_str(r.get("uid", "")),
            "sig_hash":  safe_str(r.get("signature_hash", "")),
            "name":      safe_str(r.get("type_name", "")),   # optional metadata
        } for r in recs]
    except Exception as e:
        info["record_rows"] = []

    return info
