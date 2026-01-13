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
    first_param,
    _as_string,
    _as_double,
    _as_int,
    format_len_inches,
    try_get_color_rgb_from_elem,
    get_element_display_name,
    get_type_display_name,
)

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None


# --- v2 helpers: Units / Alternate Units FormatOptions ---

def _as_string(v):
    """
    Defensive conversion to a stable string.

    Handles:
      - None
      - Revit Parameter-like objects (AsString / AsValueString)
      - Any other object via str()
    """
    if v is None:
        return ""

    # Revit DB.Parameter has AsString/AsValueString.
    try:
        if hasattr(v, "AsString"):
            s = v.AsString()
            if s is not None:
                return str(s)
    except Exception:
        pass

    try:
        if hasattr(v, "AsValueString"):
            s = v.AsValueString()
            if s is not None:
                return str(s)
    except Exception:
        pass

    try:
        return str(v)
    except Exception:
        return ""

def get_type_display_name(elem_type):
    """
    Deterministic, defensive type name extraction.

    Preference order:
      1) FamilyName + ":" + Name (common for many types)
      2) Name
      3) ElementId string as last resort

    This intentionally avoids localized UI display names.
    """
    if elem_type is None:
        return S_MISSING

    # Avoid raising if the element is a proxy or partially invalid.
    family = None
    name = None

    try:
        family = getattr(elem_type, "FamilyName", None)
    except Exception:
        family = None

    try:
        name = getattr(elem_type, "Name", None)
    except Exception:
        name = None

    if family and name:
        return "{0}:{1}".format(str(family), str(name))
    if name:
        return str(name)

    try:
        eid = getattr(elem_type, "Id", None)
        if eid is not None:
            return "id:{0}".format(str(eid))
    except Exception:
        pass

    return S_MISSING

def _fmt_in_from_ft(ft, places=6):
    if ft is None:
        return None
    try:
        inches = float(ft) * 12.0
        return format(inches, ".{}f".format(int(places)))
    except Exception as e:
        return None

def _fmt_float(x, places=12):
    if x is None:
        return None
    try:
        return format(float(x), ".{}g".format(int(places)))
    except Exception as e:
        return None

def _fmt_in_from_ft(ft, places=6):
    if ft is None:
        return None
    try:
        inches = float(ft) * 12.0
        return format(inches, ".{}f".format(int(places)))
    except Exception as e:
        return None

def _format_options_to_kv(fo):
    """
    Serialize Autodesk.Revit.DB.FormatOptions to a stable, hashable dict.
    Only include semantically relevant fields; stringify enums.
    """
    if fo is None:
        return None

    out = {}
    try:
        out["use_default"] = bool(getattr(fo, "UseDefault", False))
    except Exception as e:
        out["use_default"] = False

    # If using project default, do NOT serialize overrides
    if out["use_default"]:
        return out

    keys = [
        "Accuracy",
        "RoundingMethod",
        "UseDigitGrouping",
        "SuppressLeadingZeros",
        "SuppressTrailingZeros",
        "SuppressSpaces",
        "SuppressZeroFeet",
        "SuppressZeroInches",
        "UsePlusPrefix",
    ]

    for k in keys:
        try:
            if not hasattr(fo, k):
                continue

            v = getattr(fo, k)

            if k == "Accuracy":
                out["accuracy_in"] = _fmt_in_from_ft(v)
            else:
                out[k.lower()] = safe_str(v)

        except Exception as e:
            continue

    return out

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
        "debug_missing_name": 0,

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    types = list(
        collect_types(
            doc,
            of_class=DimensionType,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key="dimension_types:DimensionType:types",
        )
    )

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
                        except Exception as e:
                            pass
        except Exception as e:
            pass

        # Witness line control is common; keep as metadata + optional signature
        witness = _as_string(first_param(d, ui_names=["Witness Line Control"]))
        witness = canon_str(witness)

        # --- additional likely-visible parameters (optional; will be S_MISSING if absent) ---
        def _p(ui_name):
            return first_param(d, ui_names=[ui_name])

        # Text formatting / placement
        text_bg = canon_str(_as_string(_p("Text Background")))
        width_factor = _as_double(_p("Width Factor"))
        text_offset = _as_double(_p("Text Offset"))

        bold = _as_int(_p("Bold"))
        italic = _as_int(_p("Italic"))
        underline = _as_int(_p("Underline"))
        suppress_spaces = _as_int(_p("Suppress Spaces"))
        read_conv = canon_str(_as_string(_p("Read Convention")))

        # Leaders (dims + spots vary; optional)
        leader_type = canon_str(_as_string(_p("Leader Type")))
        show_leader_when_text_moves = _as_int(_p("Show Leader When Text Moves"))
        leader_tick_mark = canon_str(_as_string(_p("Leader Tick Mark")))

        # Tick / line weights
        tick_lw = _as_int(_p("Tick Mark Line Weight"))

        # Common dim line + witness line settings (mostly linear/angular; optional)
        dim_line_ext = _as_double(_p("Dimension Line Extension"))
        flipped_dim_line_ext = _as_double(_p("Flipped Dimension Line Extension"))
        snap_dist = _as_double(_p("Dimension Line Snap Distance"))

        witness_ext = _as_double(_p("Witness Line Extension"))
        witness_gap = _as_double(_p("Witness Line Gap to Element"))
        witness_len = _as_double(_p("Witness Line Length"))

        # Center marks (radial/diameter; optional)
        center_marks = _as_int(_p("Center Marks"))
        center_mark_size = _as_double(_p("Center Mark Size"))

        # Units formatting via FormatOptions (NOT parameters)
        units_fmt = None
        alt_units_fmt = None
        try:
            fo = d.GetUnitsFormatOptions()
            units_fmt = fo  # keep raw; stringify below
        except Exception as e:
            units_fmt = None

        try:
            afo = d.GetAlternateUnitsFormatOptions()
            alt_units_fmt = afo
        except Exception as e:
            alt_units_fmt = None

        # --- Units formatting (v2 only; NOT parameters) ---
        units_fmt = None
        alt_units_fmt = None

        try:
            fo = d.GetUnitsFormatOptions()
            units_fmt = _format_options_to_kv(fo)
        except Exception as e:
            units_fmt = None

        try:
            afo = d.GetAlternateUnitsFormatOptions()
            alt_units_fmt = _format_options_to_kv(afo)
        except Exception as e:
            alt_units_fmt = None

        tick_name = canon_str(tick_name)

        signature_tuple = [
            "text_font={}".format(sig_val(text_font)),
            "text_size_in={}".format(sig_val(text_size_in)),
            "line_weight={}".format(sig_val(lw)),
            "color_int={}".format(sig_val(color_int)),
            "tick_mark={}".format(sig_val(tick_name)),
            "witness_ctrl={}".format(sig_val(witness)),

            # expanded signature (optional fields)
            "text_bg={}".format(sig_val(text_bg)),
            "width_factor={}".format(sig_val(width_factor)),
            "text_offset_in={}".format(sig_val(_fmt_in_from_ft(text_offset))),
            "bold={}".format(sig_val(bold)),
            "italic={}".format(sig_val(italic)),
            "underline={}".format(sig_val(underline)),
            "suppress_spaces={}".format(sig_val(suppress_spaces)),
            "read_convention={}".format(sig_val(read_conv)),

            "leader_type={}".format(sig_val(leader_type)),
            "show_leader_when_text_moves={}".format(sig_val(show_leader_when_text_moves)),
            "leader_tick_mark={}".format(sig_val(leader_tick_mark)),

            "tick_mark_line_weight={}".format(sig_val(tick_lw)),

            "dim_line_ext_in={}".format(sig_val(_fmt_in_from_ft(dim_line_ext))),
            "flipped_dim_line_ext_in={}".format(sig_val(_fmt_in_from_ft(flipped_dim_line_ext))),
            "snap_dist_in={}".format(sig_val(_fmt_in_from_ft(snap_dist))),

            "witness_ext_in={}".format(sig_val(_fmt_in_from_ft(witness_ext))),
            "witness_gap_in={}".format(sig_val(_fmt_in_from_ft(witness_gap))),
            "witness_len_in={}".format(sig_val(_fmt_in_from_ft(witness_len))),

            "center_marks={}".format(sig_val(center_marks)),
            "center_mark_size_in={}".format(sig_val(_fmt_in_from_ft(center_mark_size))),

            # FormatOptions stringify (captures UseDefault + overrides without pretending it's a Parameter)
            "units_fmt={}".format(sig_val(safe_str(units_fmt) if units_fmt is not None else None)),
            "alt_units_fmt={}".format(sig_val(safe_str(alt_units_fmt) if alt_units_fmt is not None else None)),
        ]

        sig_hash = make_hash(signature_tuple)


        # ---------------------------
        # v2 signature (contract semantic hash)
        # ---------------------------
        # Exception (explicit): dimension type name is part of identity/definition in this domain.
        # Policy: tick mark is represented by its display name (no ids/guids).
        if not v2_blocked:
            type_name = canon_str(type_name)
            if type_name in (S_MISSING, S_UNREADABLE):
                _v2_block("unreadable_type_name")

            if not text_font:
                _v2_block("unreadable_text_font")

            if text_size_ft is None:
                _v2_block("unreadable_text_size")

            #if lw is None:
            #    _v2_block("unreadable_line_weight")

            # color must be readable; use RGB in v2 (avoid element ids / GUIDs)
            if canon_str(color_rgb) in (S_MISSING, S_UNREADABLE):
                _v2_block("unreadable_color_rgb")

            #if not tick_name:
            #    _v2_block("unreadable_tick_mark_name")

            # witness_line_control is not reliably readable across dimension families;
            # do not block v2 on it, and do not include it in the v2 record.
            if not v2_blocked:
                v2_records.append("|".join([
                    "name={}".format(safe_str(type_name)),
                    "text_font={}".format(safe_str(text_font)),
                    "text_size_in={}".format(safe_str(text_size_in)),
                    "line_weight={}".format(safe_str(lw)),
                    "color_rgb={}".format(safe_str(color_rgb)),
                    "tick_mark={}".format(safe_str(tick_name)),
                ]))

        rec = {
            "type_id": safe_str(d.Id.IntegerValue),
            "type_uid": getattr(d, "UniqueId", "") or "",
            "type_name": type_name,
            
            # v2-only metadata (not part of legacy signature)
            "units_format_options": units_fmt,
            "alternate_units_format_options": alt_units_fmt,

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
    except Exception as e:
        info["record_rows"] = []

    # v2 hash (domain-level block; no partial coverage semantics)
    info["debug_v2_blocked"] = bool(v2_blocked)
    info["debug_v2_block_reasons"] = v2_reasons if v2_blocked else {}
    if (not v2_blocked) and v2_records:
        info["hash_v2"] = make_hash(sorted(v2_records))
    else:
        info["hash_v2"] = None

    return info
