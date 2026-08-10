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
  - _read_unit_format_info() for primary/alternate UnitsFormatOptions reading
  - _read_leader_arrowhead() for the spot-family Leader Arrowhead cluster
    (Area 7 §1) -- shared by the 3 spot dimension_types partitions so the
    text_types.py-derived resolve pattern isn't duplicated 3 times
  - _read_arrowhead_ref_sig_hash() -- generic ElementId-param -> arrowheads
    sig_hash resolver for the other tick-mark-family fields (Leader Tick
    Mark, Centerline Tick Mark, Interior Tick Mark, Witness Line Tick Mark)
  - _read_element_ref_name() -- generic ElementId-param -> referenced
    element's display name resolver (Centerline Pattern/Symbol; no ctx
    sig_hash map coverage confirmed for these, unlike the arrowhead family)
  - _build_alternate_units_items() for the Alternate Units cluster (Area 7 §5)

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
    _as_value_string,
    _as_double,
    _as_int,
    format_len_inches,
    try_get_color_rgb_from_elem,
    get_element_display_name,
    _canon_rgb,
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

    # text_background (storage=Integer/enum — use AsValueString; probe shows display='Opaque')
    try:
        p_bg = first_param(d, ui_names=["Text Background"])
        bg_raw = _as_value_string(p_bg) if p_bg is not None else None
        bg_v, bg_q = canonicalize_str(bg_raw)
    except Exception:
        bg_v, bg_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.text_background", bg_v, bg_q))

    # color_rgb — canonicalize dict to "r-g-b" string before storing
    try:
        _color_int, color_rgb_raw = try_get_color_rgb_from_elem(d)
        color_rgb_str = _canon_rgb(color_rgb_raw)
        if color_rgb_str is not None:
            color_v, color_q = canonicalize_str(color_rgb_str)
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

def _read_unit_format_info(d, alternate=False):
    """
    Read UnitsFormatOptions (or, when alternate=True, AlternateUnitsFormatOptions)
    from a DimensionType and return a tuple of
    (unit_format_id_v, unit_format_id_q, rounding_v, rounding_q, accuracy_v, accuracy_q,
     suppress_spaces_v, suppress_spaces_q).

    Handles UseDefault by returning ("use_default", ITEM_Q_OK) for all four.
    Handles unsupported (e.g., SpotSlope, or a shape with no alternate-units tab)
    by returning (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE).

    suppress_spaces (Area 7 §6) is read off the same FormatOptions object as
    rounding/accuracy -- same FormatOptions-boolean-flag gap Area 8 documented
    for units.py, just on DimensionType.GetUnitsFormatOptions() instead of the
    doc-level Units object. Only meaningful for the primary (alternate=False)
    call per Area 7 §6 scope; callers reading the alternate cluster should
    ignore this element of the tuple.

    GetAlternateUnitsFormatOptions() (Area 7 §5) is a best-effort mirror of
    GetUnitsFormatOptions() -- its exact name is not independently confirmed
    against a live Revit API in this pass, so any AttributeError/exception
    calling it fails soft to UNSUPPORTED_NOT_APPLICABLE via the same
    _units_fo_not_applicable()-style handling as the primary path, never a
    hard failure.
    """

    def _units_fo_not_applicable(ex):
        msg = safe_str(getattr(ex, "Message", None) or ex)
        tname = safe_str(getattr(type(ex), "__name__", "")).lower()
        msg_l = msg.lower()
        return (
            "notsupported" in tname
            or "invalidoperation" in tname
            or "attributeerror" in tname
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
    suppress_spaces_v = None
    suppress_spaces_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE

    fo = None
    fo_exc = None
    try:
        if alternate:
            fo = d.GetAlternateUnitsFormatOptions()
        else:
            fo = d.GetUnitsFormatOptions()
    except Exception as ex:
        fo_exc = ex

    if fo is None:
        if fo_exc is not None and (not _units_fo_not_applicable(fo_exc)):
            unit_format_id_q = ITEM_Q_UNREADABLE
            rounding_q = ITEM_Q_UNREADABLE
            accuracy_q = ITEM_Q_UNREADABLE
            suppress_spaces_q = ITEM_Q_UNREADABLE
        # else: leave as UNSUPPORTED_NOT_APPLICABLE
    else:
        use_default = getattr(fo, "UseDefault", None)
        if use_default is True:
            unit_format_id_v, unit_format_id_q = ("use_default", ITEM_Q_OK)
            rounding_v, rounding_q = ("use_default", ITEM_Q_OK)
            accuracy_v, accuracy_q = ("use_default", ITEM_Q_OK)
            suppress_spaces_v, suppress_spaces_q = ("use_default", ITEM_Q_OK)
        else:
            try:
                forge_type_id_obj = fo.GetUnitTypeId()
                uid_str = getattr(forge_type_id_obj, "TypeId", None)
                if uid_str is None:
                    uid_str = forge_type_id_obj.ToString()
                unit_format_id_v, unit_format_id_q = canonicalize_str(str(uid_str))
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

            try:
                suppress_spaces_v, suppress_spaces_q = canonicalize_bool(getattr(fo, "SuppressSpaces", None))
            except Exception:
                suppress_spaces_v, suppress_spaces_q = (None, ITEM_Q_UNREADABLE)

    return (
        unit_format_id_v, unit_format_id_q,
        rounding_v, rounding_q,
        accuracy_v, accuracy_q,
        suppress_spaces_v, suppress_spaces_q,
    )


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


# ---------------------------------------------------------------------------
# Leader Arrowhead Reader (Area 7 §1 -- spot-family leader/arrowhead cluster)
# ---------------------------------------------------------------------------

def _read_leader_arrowhead(d, ctx, doc):
    """
    Read the Leader Arrowhead parameter (BuiltInParameter.LEADER_ARROWHEAD)
    and resolve it to uid/name/sig_hash, mirroring the working pattern
    already shipped in domains/text_types.py (same field, read for the
    text_types domain). Shared here so the 3 spot dimension_types partitions
    (extract_spot_elevation/_spot_coordinate/_spot_slope) don't each
    duplicate the read-and-resolve logic.

    Returns:
        (uid_v, uid_q, name_v, name_q, sig_hash_v, sig_hash_q)
    """
    uid_v, uid_q = (None, ITEM_Q_MISSING)
    name_v, name_q = (None, ITEM_Q_MISSING)
    sig_hash_v, sig_hash_q = (None, ITEM_Q_MISSING)

    try:
        p_arrow = first_param(d, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])
        if p_arrow is not None and getattr(p_arrow, "HasValue", False):
            arrow_id = None
            try:
                arrow_id = p_arrow.AsElementId()
            except Exception:
                arrow_id = None

            if arrow_id is not None and getattr(arrow_id, "IntegerValue", 0) > 0:
                arrow = None
                try:
                    arrow = doc.GetElement(arrow_id) if doc is not None else None
                except Exception:
                    arrow = None

                if arrow is not None:
                    try:
                        arrow_uid = getattr(arrow, "UniqueId", None)
                        uid_v, uid_q = canonicalize_str(arrow_uid) if arrow_uid else (None, ITEM_Q_MISSING)
                    except Exception:
                        uid_v, uid_q = (None, ITEM_Q_UNREADABLE)

                    try:
                        arrow_name = get_type_display_name(arrow)
                        if arrow_name in (None, S_MISSING, S_UNREADABLE):
                            arrow_name = getattr(arrow, "Name", None)
                        name_v, name_q = canonicalize_str(arrow_name)
                    except Exception:
                        name_v, name_q = (None, ITEM_Q_UNREADABLE)

                    try:
                        ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
                        k = safe_str(getattr(arrow_id, "IntegerValue", None))
                        if k and isinstance(ah_map, dict) and k in ah_map:
                            sh = ah_map.get(k, {}).get("sig_hash", None)
                            if sh:
                                sig_hash_v, sig_hash_q = (safe_str(sh), ITEM_Q_OK)
                    except Exception:
                        pass
    except Exception:
        pass

    return (uid_v, uid_q, name_v, name_q, sig_hash_v, sig_hash_q)


# ---------------------------------------------------------------------------
# Generic tick-mark-family ElementId -> arrowheads sig_hash resolver
# ---------------------------------------------------------------------------

def _read_arrowhead_ref_sig_hash(d, ctx, bip_names=None, ui_names=None):
    """
    Generic ElementId-parameter -> ctx["arrowheads_by_type_id"] sig_hash
    resolver, generalizing the pattern already used by _read_tick_mark_sig_hash
    (kept separate/unchanged to avoid touching its established Tick Mark
    call sites). Shared by the other tick-mark-style fields added in
    Area 7: Leader Tick Mark (§2), Centerline Tick Mark, Interior Tick Mark
    (§4), and Witness Line Tick Mark (§3) -- all reference an
    arrowhead/tick-mark-style element the same way the existing Tick Mark
    field does.

    Returns:
        (sig_hash_v, sig_hash_q) where sig_hash_q is ITEM_Q_OK if resolved,
        ITEM_Q_MISSING otherwise (no reference, or a negative/built-in id
        not present in the arrowheads collector -- e.g. a "None" selection).
    """
    sig_hash = None

    try:
        p = first_param(d, bip_names=bip_names, ui_names=ui_names)
        if p is not None and getattr(p, "HasValue", False):
            eid = None
            try:
                eid = p.AsElementId()
            except Exception:
                eid = None

            if eid is not None and getattr(eid, "IntegerValue", 0) > 0:
                try:
                    ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
                    k = safe_str(getattr(eid, "IntegerValue", None))
                    if k and isinstance(ah_map, dict) and k in ah_map:
                        sig_hash = ah_map.get(k, {}).get("sig_hash", None)
                except Exception:
                    sig_hash = None
    except Exception:
        sig_hash = None

    if sig_hash:
        return (safe_str(sig_hash), ITEM_Q_OK)
    return (None, ITEM_Q_MISSING)


# ---------------------------------------------------------------------------
# Generic named-element-reference resolver (no ctx sig_hash map coverage)
# ---------------------------------------------------------------------------

def _read_element_ref_name(d, doc, ui_names):
    """
    Generic ElementId-parameter -> referenced element's display name
    resolver, for fields that reference a named element but are NOT known
    to be covered by ctx["arrowheads_by_type_id"] (e.g. Centerline Pattern,
    a line-pattern reference, and Centerline Symbol, which the probe's
    sample value ("ANG-Centerline") suggests is a line-style/annotation
    symbol name rather than an arrowhead -- see Area 7 §4 open question).
    Resolves by name only, not sig_hash, to avoid asserting an unconfirmed
    ctx map lookup.

    Unlike _read_arrowhead_ref_sig_hash (which treats a negative ElementId
    as "no reference" -- built-in tick-mark constants like -2 mean "None"),
    this treats only IntegerValue == 0/None as "no reference": negative
    built-in ids here (e.g. -3000010 for a built-in line pattern such as
    "Solid") are real, resolvable references, not a "none selected" state.
    """
    try:
        p = first_param(d, ui_names=ui_names)
        if p is None or not getattr(p, "HasValue", False):
            return (None, ITEM_Q_MISSING)

        eid = None
        try:
            eid = p.AsElementId()
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if eid is None or getattr(eid, "IntegerValue", 0) == 0:
            return (None, ITEM_Q_MISSING)

        elem = None
        try:
            elem = doc.GetElement(eid) if doc is not None else None
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if elem is None:
            return (None, ITEM_Q_MISSING)

        name = None
        try:
            name = get_type_display_name(elem)
            if name in (None, S_MISSING, S_UNREADABLE):
                name = getattr(elem, "Name", None)
        except Exception:
            try:
                name = getattr(elem, "Name", None)
            except Exception:
                name = None

        if name:
            return canonicalize_str(name)
        return (None, ITEM_Q_MISSING)
    except Exception:
        return (None, ITEM_Q_UNREADABLE)


# ---------------------------------------------------------------------------
# Alternate Units Cluster (Area 7 §5)
# ---------------------------------------------------------------------------

def _build_alternate_units_items(d):
    """
    Read the Alternate Units cluster: master toggle, format id, prefix, suffix.

    Per Greg's correction (Area 7 §5), Revit's UI repeats the Alternate Units
    parameter set across all dimension-type families -- observed on every
    shape in probe data (linear/angular/radial/diameter/all 3 spot families),
    not just Linear. Presence/absence is captured as real per-type signal,
    not gated to a subset of shapes the way Witness Lines/Centerline/Equality
    are.

    Returns a list of identity item dicts:
      - dim_type.alternate_units
      - dim_type.alternate_units_format_id
      - dim_type.alternate_units_prefix
      - dim_type.alternate_units_suffix
    """
    items = []

    # alternate_units (master toggle)
    try:
        p_au = first_param(d, ui_names=["Alternate Units"])
        au_int = _as_int(p_au) if p_au is not None else None
        au_v, au_q = canonicalize_bool(au_int)
    except Exception:
        au_v, au_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.alternate_units", au_v, au_q))

    # alternate_units_format_id (same shape as dim_type.unit_format_id, off
    # the alternate FormatOptions object; rounding/accuracy intentionally
    # not surfaced here -- not part of Area 7 §5's requested field set)
    try:
        _alt_fo = _read_unit_format_info(d, alternate=True)
        alt_fmt_v, alt_fmt_q = _alt_fo[0], _alt_fo[1]
    except Exception:
        alt_fmt_v, alt_fmt_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.alternate_units_format_id", alt_fmt_v, alt_fmt_q))

    # alternate_units_prefix
    try:
        p_pfx = first_param(d, ui_names=["Alternate Units Prefix"])
        pfx_raw = _as_string(p_pfx) if p_pfx is not None else None
        pfx_v, pfx_q = canonicalize_str_allow_empty(pfx_raw)
    except Exception:
        pfx_v, pfx_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.alternate_units_prefix", pfx_v, pfx_q))

    # alternate_units_suffix
    try:
        p_sfx = first_param(d, ui_names=["Alternate Units Suffix"])
        sfx_raw = _as_string(p_sfx) if p_sfx is not None else None
        sfx_v, sfx_q = canonicalize_str_allow_empty(sfx_raw)
    except Exception:
        sfx_v, sfx_q = (None, ITEM_Q_UNREADABLE)
    items.append(make_identity_item("dim_type.alternate_units_suffix", sfx_v, sfx_q))

    return items
