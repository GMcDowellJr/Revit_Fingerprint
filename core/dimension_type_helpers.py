# -*- coding: utf-8 -*-
"""
core/dimension_type_helpers.py

Shared helpers for the dimension_types_* domain extractors.

Provides:
  - Shape detection constants and _get_dimension_shape()
  - _format_options_to_kv() for FormatOptions serialization
  - _fmt_in_from_ft() and _fmt_float() unit conversion helpers
  - get_type_display_name() for DimensionType display names
  - _build_text_appearance_items() for text/appearance identity items
  - _read_tick_mark_sig_hash() for tick mark arrowhead sig hash lookup
  - _read_unit_format_info() for UnitsFormatOptions reading

Pure-Python and Revit-agnostic except where guarded by try/except ImportError.
No domain imports.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import canon_str, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
from core.rows import (
    first_param,
    _as_string,
    _as_double,
    _as_int,
    format_len_inches,
    try_get_color_rgb_from_elem,
    get_element_display_name,
)
from core.record_v2 import (
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_int,
    canonicalize_float,
    canonicalize_bool,
    canonicalize_enum,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    make_identity_item,
)


# ---------------------------------------------------------------------------
# Shape Detection Constants
# ---------------------------------------------------------------------------

# Canonical shape names (normalized from DimensionStyleType enum)
SHAPE_LINEAR = "Linear"
SHAPE_ANGULAR = "Angular"
SHAPE_RADIAL = "Radial"
SHAPE_DIAMETER = "Diameter"
SHAPE_ARC_LENGTH = "ArcLength"
SHAPE_SPOT_ELEVATION = "SpotElevation"
SHAPE_SPOT_COORDINATE = "SpotCoordinate"
SHAPE_SPOT_SLOPE = "SpotSlope"
SHAPE_LINEAR_FIXED = "LinearFixed"
SHAPE_SPOT_ELEVATION_FIXED = "SpotElevationFixed"
SHAPE_DIAMETER_LINKED = "DiameterLinked"
SHAPE_ALIGNMENT_STATION_LABEL = "AlignmentStationLabel"
SHAPE_UNKNOWN = "Unknown"

# Shape family constants for property gating
FAMILY_LINEAR = "linear"
FAMILY_RADIAL = "radial"
FAMILY_ANGULAR = "angular"
FAMILY_SPOT = "spot"
FAMILY_UNKNOWN = "unknown"

# Map canonical shape names to shape families
SHAPE_TO_FAMILY = {
    SHAPE_LINEAR: FAMILY_LINEAR,
    SHAPE_LINEAR_FIXED: FAMILY_LINEAR,
    SHAPE_RADIAL: FAMILY_RADIAL,
    SHAPE_DIAMETER: FAMILY_RADIAL,
    SHAPE_DIAMETER_LINKED: FAMILY_RADIAL,
    SHAPE_ANGULAR: FAMILY_ANGULAR,
    SHAPE_ARC_LENGTH: FAMILY_ANGULAR,
    SHAPE_SPOT_ELEVATION: FAMILY_SPOT,
    SHAPE_SPOT_COORDINATE: FAMILY_SPOT,
    SHAPE_SPOT_SLOPE: FAMILY_SPOT,
    SHAPE_SPOT_ELEVATION_FIXED: FAMILY_SPOT,
    SHAPE_ALIGNMENT_STATION_LABEL: FAMILY_SPOT,
    SHAPE_UNKNOWN: FAMILY_UNKNOWN,
}

# Map integer enum values to canonical shape names (fallback for non-enum access)
SHAPE_INT_TO_NAME = {
    0: SHAPE_LINEAR,
    1: SHAPE_ANGULAR,
    2: SHAPE_RADIAL,
    3: SHAPE_DIAMETER,
    4: SHAPE_ARC_LENGTH,
    5: SHAPE_SPOT_ELEVATION,
    6: SHAPE_SPOT_COORDINATE,
    7: SHAPE_SPOT_SLOPE,
    8: SHAPE_LINEAR_FIXED,
    9: SHAPE_SPOT_ELEVATION_FIXED,
    10: SHAPE_DIAMETER_LINKED,
    11: SHAPE_ALIGNMENT_STATION_LABEL,
}


# ---------------------------------------------------------------------------
# Shape Detection Helper
# ---------------------------------------------------------------------------

def _get_dimension_shape(dim_type):
    """
    Detect dimension shape from a Revit DimensionType object.

    Revit exposes shape via multiple API paths depending on version:
      - DimensionType.StyleType (preferred, returns DimensionStyleType enum)
      - DimensionType.Shape (some versions)
      - DimensionType.DimensionShape (legacy)
      - DimensionType.DimensionStyleType (redundant accessor)

    Returns:
        tuple: (shape_name, shape_family, quality)
            - shape_name: str - Canonical shape name (e.g., "Linear", "Radial")
            - shape_family: str - Shape family for property gating (e.g., "linear", "radial")
            - quality: str - ITEM_Q_OK, ITEM_Q_MISSING, or ITEM_Q_UNREADABLE

    Fail-soft behavior:
        - If shape cannot be read, returns (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)
        - If shape is None/empty, returns (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)
        - Unknown enum values return (str(value), FAMILY_UNKNOWN, ITEM_Q_OK)
    """
    if dim_type is None:
        return (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)

    # Try multiple API paths in order of preference
    shape_raw = None
    read_exception = None

    for attr_name in ("StyleType", "Shape", "DimensionShape", "DimensionStyleType"):
        try:
            if hasattr(dim_type, attr_name):
                val = getattr(dim_type, attr_name, None)
                if val is not None:
                    shape_raw = val
                    break
        except Exception as ex:
            if read_exception is None:
                read_exception = ex
            continue

    # Handle missing shape
    if shape_raw is None:
        if read_exception is not None:
            return (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)
        return (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)

    # Extract canonical shape name from enum or value
    shape_name = None

    # Try 1: Enum with .name attribute (preferred - gives string like "Linear")
    try:
        enum_name = getattr(shape_raw, "name", None)
        if isinstance(enum_name, str) and enum_name.strip():
            shape_name = enum_name.strip()
    except Exception:
        pass

    # Try 2: Enum with .Name attribute (some .NET enums use PascalCase)
    if shape_name is None:
        try:
            enum_name = getattr(shape_raw, "Name", None)
            if isinstance(enum_name, str) and enum_name.strip():
                shape_name = enum_name.strip()
        except Exception:
            pass

    # Try 3: Integer value lookup
    if shape_name is None:
        try:
            int_val = None
            for int_attr in ("value", "Value", "value__", "__int__"):
                try:
                    if int_attr == "__int__":
                        int_val = int(shape_raw)
                    elif hasattr(shape_raw, int_attr):
                        int_val = getattr(shape_raw, int_attr)
                        if callable(int_val):
                            int_val = int_val()
                    if int_val is not None:
                        break
                except Exception:
                    continue

            if int_val is not None and int_val in SHAPE_INT_TO_NAME:
                shape_name = SHAPE_INT_TO_NAME[int_val]
        except Exception:
            pass

    # Try 4: String conversion fallback
    if shape_name is None:
        try:
            str_val = str(shape_raw).strip()
            if str_val:
                for known_name in SHAPE_TO_FAMILY.keys():
                    if str_val.lower() == known_name.lower():
                        shape_name = known_name
                        break
                if shape_name is None:
                    shape_name = str_val
        except Exception:
            pass

    # Explicit handling for AlignmentStationLabel (spot-like)
    try:
        _sn = safe_str(shape_name).lower().replace(" ", "")
        if _sn == "alignmentstationlabel":
            return (SHAPE_ALIGNMENT_STATION_LABEL, FAMILY_SPOT, ITEM_Q_OK)
    except Exception:
        pass

    # Final fallback
    if shape_name is None:
        return (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)

    # Determine shape family
    shape_family = SHAPE_TO_FAMILY.get(shape_name, FAMILY_UNKNOWN)

    return (shape_name, shape_family, ITEM_Q_OK)


# ---------------------------------------------------------------------------
# Unit conversion helpers
# ---------------------------------------------------------------------------

def _fmt_in_from_ft(ft, places=6):
    """Convert feet to inches and format as string with given decimal places."""
    if ft is None:
        return None
    try:
        inches = float(ft) * 12.0
        return format(inches, ".{}f".format(int(places)))
    except Exception:
        return None


def _fmt_float(x, places=12):
    """Format a float with given significant digits."""
    if x is None:
        return None
    try:
        return format(float(x), ".{}g".format(int(places)))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# FormatOptions serialization
# ---------------------------------------------------------------------------

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
    except Exception:
        out["use_default"] = False

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
        except Exception:
            continue

    return out


# ---------------------------------------------------------------------------
# Display name helper
# ---------------------------------------------------------------------------

def get_type_display_name(elem_type):
    """
    Deterministic, defensive type name extraction for DimensionType.

    IMPORTANT:
    - DimensionType.Name may throw TypeError and MUST NOT be relied on.
    - UI-visible names are exposed via parameters:
        SYMBOL_FAMILY_NAME_PARAM (-1002002)
        SYMBOL_NAME_PARAM        (-1002001)

    Preference order:
      1) Family Name + ":" + Type Name   (matches Revit UI grouping)
      2) Type Name
      3) Family Name
      4) id:ElementId
    """
    if elem_type is None:
        return S_MISSING

    fam = None
    typ = None

    # Family Name
    try:
        p_fam = first_param(
            elem_type,
            bip_names=["SYMBOL_FAMILY_NAME_PARAM"],
            ui_names=["Family Name"],
        )
        fam = canon_str(_as_string(p_fam))
        if fam in (S_MISSING, S_UNREADABLE, "", None):
            fam = None
    except Exception:
        fam = None

    # Type Name
    try:
        p_typ = first_param(
            elem_type,
            bip_names=["SYMBOL_NAME_PARAM", "ALL_MODEL_TYPE_NAME"],
            ui_names=["Type Name", "Name"],
        )
        typ = canon_str(_as_string(p_typ))
        if typ in (S_MISSING, S_UNREADABLE, "", None):
            typ = None
    except Exception:
        typ = None

    if fam and typ:
        return "{}:{}".format(fam, typ)
    if typ:
        return typ
    if fam:
        try:
            eid = getattr(elem_type, "Id", None)
            if eid is not None:
                return "{}:id:{}".format(fam, safe_str(getattr(eid, "IntegerValue", eid)))
        except Exception:
            pass
        return fam

    try:
        eid = getattr(elem_type, "Id", None)
        if eid is not None:
            return "id:{}".format(str(eid))
    except Exception:
        pass

    return S_MISSING


# ---------------------------------------------------------------------------
# Text/Appearance Identity Items Builder
# ---------------------------------------------------------------------------

def _build_text_appearance_items(d):
    """
    Extract text/appearance identity items common to all dimension type shapes.

    Returns a list of identity item dicts for:
      - dim_type.text_font
      - dim_type.text_size_in
      - dim_type.text_bold
      - dim_type.text_italic
      - dim_type.text_underline
      - dim_type.text_width_factor
      - dim_type.text_background
      - dim_type.color_rgb
      - dim_type.line_weight

    These items are always included regardless of shape.
    """
    items = []

    # text_font
    try:
        p_font = first_param(
            d,
            bip_names=["TEXT_FONT", "DIM_TEXT_FONT", "SPOT_ELEV_TEXT_FONT", "SPOT_COORDINATE_TEXT_FONT"],
            ui_names=["Text Font"],
        )
        font_raw = _as_string(p_font) if p_font is not None else None
        font_v, font_q = canonicalize_str(font_raw)
    except Exception:
        font_v, font_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_font", font_v, font_q))

    # text_size_in (stored as feet, converted to inches)
    try:
        p_size = first_param(
            d,
            bip_names=["TEXT_SIZE", "DIM_TEXT_SIZE", "SPOT_ELEV_TEXT_SIZE", "SPOT_COORDINATE_TEXT_SIZE"],
            ui_names=["Text Size"],
        )
        size_ft = _as_double(p_size) if p_size is not None else None
        if size_ft is not None:
            size_in_str = _fmt_in_from_ft(size_ft)
            size_v, size_q = canonicalize_float(size_in_str)
        else:
            size_v, size_q = (None, ITEM_Q_MISSING)
    except Exception:
        size_v, size_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_size_in", size_v, size_q))

    # text_bold
    try:
        p_bold = first_param(d, ui_names=["Bold"])
        bold_int = _as_int(p_bold) if p_bold is not None else None
        bold_v, bold_q = canonicalize_bool(bold_int)
    except Exception:
        bold_v, bold_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_bold", bold_v, bold_q))

    # text_italic
    try:
        p_italic = first_param(d, ui_names=["Italic"])
        italic_int = _as_int(p_italic) if p_italic is not None else None
        italic_v, italic_q = canonicalize_bool(italic_int)
    except Exception:
        italic_v, italic_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_italic", italic_v, italic_q))

    # text_underline
    try:
        p_underline = first_param(d, ui_names=["Underline"])
        underline_int = _as_int(p_underline) if p_underline is not None else None
        underline_v, underline_q = canonicalize_bool(underline_int)
    except Exception:
        underline_v, underline_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_underline", underline_v, underline_q))

    # text_width_factor
    try:
        p_wf = first_param(d, ui_names=["Width Factor"])
        wf_raw = _as_double(p_wf) if p_wf is not None else None
        wf_v, wf_q = canonicalize_float(wf_raw)
    except Exception:
        wf_v, wf_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_width_factor", wf_v, wf_q))

    # text_background
    try:
        p_bg = first_param(d, ui_names=["Text Background"])
        bg_raw = _as_string(p_bg) if p_bg is not None else None
        bg_v, bg_q = canonicalize_str_allow_empty(bg_raw)
    except Exception:
        bg_v, bg_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_background", bg_v, bg_q))

    # color_rgb
    try:
        _color_int, color_rgb_str = try_get_color_rgb_from_elem(d)
        if color_rgb_str is not None:
            color_v, color_q = canonicalize_str(safe_str(color_rgb_str))
        else:
            color_v, color_q = (None, ITEM_Q_MISSING)
    except Exception:
        color_v, color_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.color_rgb", color_v, color_q))

    # line_weight
    try:
        p_lw = first_param(
            d,
            bip_names=["LINE_WEIGHT", "DIM_LINE_WEIGHT"],
            ui_names=["Line Weight"],
        )
        lw_raw = _as_int(p_lw) if p_lw is not None else None
        lw_v, lw_q = canonicalize_int(lw_raw)
    except Exception:
        lw_v, lw_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.line_weight", lw_v, lw_q))

    return items


# ---------------------------------------------------------------------------
# Tick Mark Sig Hash Reader
# ---------------------------------------------------------------------------

def _read_tick_mark_sig_hash(d, ctx, doc=None):
    """
    Read the tick mark parameter and return (sig_hash_v, sig_hash_q) using
    the ctx arrowheads_by_type_id map.

    Returns:
        (sig_hash_v, sig_hash_q) where:
          - sig_hash_v: str hash or None
          - sig_hash_q: ITEM_Q_OK if found, ITEM_Q_MISSING if not found/none
    """
    tick_sig_hash = None

    try:
        p_tick = first_param(
            d,
            bip_names=["DIM_LEADER_ARROWHEAD", "TICK_MARK", "DIM_TICK_MARK"],
            ui_names=["Tick Mark"],
        )

        if p_tick is not None and getattr(p_tick, "HasValue", False):
            tid = None
            try:
                tid = p_tick.AsElementId()
            except Exception:
                tid = None

            if tid is not None and getattr(tid, "IntegerValue", 0) > 0:
                # Try ctx lookup first (preferred - UID-free)
                try:
                    ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
                    k = safe_str(getattr(tid, "IntegerValue", None))
                    if k and isinstance(ah_map, dict) and k in ah_map:
                        tick_sig_hash = ah_map.get(k, {}).get("sig_hash", None)
                except Exception:
                    tick_sig_hash = None

    except Exception:
        tick_sig_hash = None

    if tick_sig_hash:
        return (safe_str(tick_sig_hash), ITEM_Q_OK)
    else:
        return (None, ITEM_Q_MISSING)


# ---------------------------------------------------------------------------
# Unit Format Info Reader
# ---------------------------------------------------------------------------

def _read_unit_format_info(d):
    """
    Read UnitsFormatOptions from a DimensionType and return a tuple of
    (unit_format_id_v, unit_format_id_q, rounding_v, rounding_q, accuracy_v, accuracy_q).

    Handles UseDefault by returning ("use_default", ITEM_Q_OK) for all three.
    Handles unsupported (e.g., SpotSlope) by returning (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE).
    """

    def _units_fo_not_applicable(ex):
        msg = safe_str(getattr(ex, "Message", None) or ex)
        tname = safe_str(getattr(type(ex), "__name__", "")).lower()
        msg_l = msg.lower()
        return (
            "notsupported" in tname
            or "invalidoperation" in tname
            or "not supported" in msg_l
            or "not applicable" in msg_l
            or "unsupported" in msg_l
        )

    unit_format_id_v = None
    unit_format_id_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
    rounding_v = None
    rounding_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
    accuracy_v = None
    accuracy_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE

    fo = None
    fo_exc = None
    try:
        fo = d.GetUnitsFormatOptions()
    except Exception as ex:
        fo_exc = ex

    if fo is None:
        if fo_exc is not None and (not _units_fo_not_applicable(fo_exc)):
            unit_format_id_q = ITEM_Q_UNREADABLE
            rounding_q = ITEM_Q_UNREADABLE
            accuracy_q = ITEM_Q_UNREADABLE
        # else: leave as UNSUPPORTED_NOT_APPLICABLE
    else:
        use_default = getattr(fo, "UseDefault", None)
        if use_default is True:
            unit_format_id_v, unit_format_id_q = ("use_default", ITEM_Q_OK)
            rounding_v, rounding_q = ("use_default", ITEM_Q_OK)
            accuracy_v, accuracy_q = ("use_default", ITEM_Q_OK)
        else:
            try:
                unit_format_id_v, unit_format_id_q = canonicalize_str(safe_str(fo.GetUnitTypeId()))
            except Exception:
                unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNREADABLE)

            try:
                rounding_v, rounding_q = canonicalize_enum(getattr(fo, "RoundingMethod", None))
            except Exception:
                rounding_v, rounding_q = (None, ITEM_Q_UNREADABLE)

            try:
                accuracy_v, accuracy_q = canonicalize_float(_fmt_in_from_ft(getattr(fo, "Accuracy", None)))
            except Exception:
                accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)

    return (unit_format_id_v, unit_format_id_q, rounding_v, rounding_q, accuracy_v, accuracy_q)


# ---------------------------------------------------------------------------
# Prefix/Suffix Reader
# ---------------------------------------------------------------------------

def _read_prefix_suffix(d):
    """
    Read Prefix and Suffix properties from a DimensionType.

    Returns:
        (prefix_v, prefix_q, suffix_v, suffix_q)
    """
    prefix_v, prefix_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
    suffix_v, suffix_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)

    if hasattr(d, "Prefix"):
        try:
            raw = getattr(d, "Prefix", "")
            if raw is None:
                raw = ""
            prefix_v, prefix_q = (safe_str(raw), ITEM_Q_OK)
        except Exception:
            prefix_v, prefix_q = (None, ITEM_Q_UNREADABLE)

    if hasattr(d, "Suffix"):
        try:
            raw = getattr(d, "Suffix", "")
            if raw is None:
                raw = ""
            suffix_v, suffix_q = (safe_str(raw), ITEM_Q_OK)
        except Exception:
            suffix_v, suffix_q = (None, ITEM_Q_UNREADABLE)

    return (prefix_v, prefix_q, suffix_v, suffix_q)
