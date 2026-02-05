# -*- coding: utf-8 -*-
"""
Dimension Types domain extractor.

Fingerprints dimension types including:
- Text font, size
- Line weight, color
- Tick mark (arrowhead)
- Witness line control

Per-record identity: sig_hash (UID-free) derived from identity_items.
Ordering: order-insensitive (identity_items sorted before hashing)
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

from core.record_v2 import (
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_enum,
    canonicalize_float,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED,
    build_record_v2,
    make_identity_item,
    serialize_identity_items,
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)

from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None


# ---------------------------------------------------------------------------
# Shape Detection Constants and Helper
# ---------------------------------------------------------------------------
#
# Revit DimensionStyleType enum values (API 2020+):
#   Linear = 0
#   Angular = 1
#   Radial = 2
#   Diameter = 3
#   ArcLength = 4
#   SpotElevation = 5
#   SpotCoordinate = 6
#   SpotSlope = 7
#   LinearFixed = 8
#   SpotElevationFixed = 9
#   (DiameterLinked = 10 in some versions)
#
# Shape families for property gating:
#   LINEAR_FAMILY: Linear, LinearFixed - have witness lines
#   RADIAL_FAMILY: Radial, Diameter, DiameterLinked - have center marks
#   ANGULAR_FAMILY: Angular, ArcLength - angle measurements
#   SPOT_FAMILY: SpotElevation, SpotCoordinate, SpotSlope, SpotElevationFixed
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
}


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

    Example:
        >>> shape_name, shape_family, quality = _get_dimension_shape(dim_type)
        >>> if shape_family == FAMILY_LINEAR:
        ...     # Export witness line properties
        ...     pass
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
            # Record first exception for diagnostics but continue trying
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
            # Get integer value from enum
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
                # Check if it matches a known shape name
                for known_name in SHAPE_TO_FAMILY.keys():
                    if str_val.lower() == known_name.lower():
                        shape_name = known_name
                        break
                # If not matched, use the string value as-is
                if shape_name is None:
                    shape_name = str_val
        except Exception:
            pass

    # Final fallback
    if shape_name is None:
        return (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)

    # Determine shape family
    shape_family = SHAPE_TO_FAMILY.get(shape_name, FAMILY_UNKNOWN)

    return (shape_name, shape_family, ITEM_Q_OK)


def _is_linear_family(shape_family):
    """Check if shape family supports witness line properties."""
    return shape_family == FAMILY_LINEAR


def _is_radial_family(shape_family):
    """Check if shape family supports center mark properties."""
    return shape_family == FAMILY_RADIAL


def _is_angular_family(shape_family):
    """Check if shape family supports angular-specific properties."""
    return shape_family == FAMILY_ANGULAR


def _is_spot_family(shape_family):
    """Check if shape family is a spot dimension type."""
    return shape_family == FAMILY_SPOT


# ---------------------------------------------------------------------------
# Shape-Gated Identity Item Builders
# ---------------------------------------------------------------------------
#
# These functions build identity_items based on shape family.
# Properties are organized into:
#   - COMMON: Exported for all shapes (shape, unit formatting, prefix/suffix, tick mark)
#   - LINEAR_SPECIFIC: witness_line_control (only for linear family)
#   - RADIAL_SPECIFIC: center_marks, center_mark_size (only for radial family)
#   - ANGULAR_SPECIFIC: (currently none unique - uses common properties)
#
# The shape-gating approach:
#   - Never export properties with ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
#   - Only include properties applicable to the detected shape
#   - This results in cleaner identity_items that vary by shape
# ---------------------------------------------------------------------------


def _build_common_identity_items(
    shape_v,
    shape_q,
    unit_format_id_v,
    unit_format_id_q,
    rounding_v,
    rounding_q,
    accuracy_v,
    accuracy_q,
    prefix_v,
    prefix_q,
    suffix_v,
    suffix_q,
    tick_sig_hash,
):
    """
    Build identity items common to ALL dimension shapes.

    Common properties:
      - dim_type.shape: Shape discriminator (always first)
      - dim_type.unit_format_id: Unit format type ID
      - dim_type.rounding: Rounding method
      - dim_type.accuracy: Accuracy setting
      - dim_type.prefix: Prefix string
      - dim_type.suffix: Suffix string
      - dim_type.tick_mark_sig_hash: Tick mark signature hash

    Returns:
        list: List of identity item dicts
    """
    items = [
        make_identity_item("dim_type.shape", shape_v, shape_q),
        make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
        make_identity_item("dim_type.rounding", rounding_v, rounding_q),
        make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
        make_identity_item("dim_type.prefix", prefix_v, prefix_q),
        make_identity_item("dim_type.suffix", suffix_v, suffix_q),
        make_identity_item(
            "dim_type.tick_mark_sig_hash",
            safe_str(tick_sig_hash) if tick_sig_hash else None,
            ITEM_Q_OK if tick_sig_hash else ITEM_Q_MISSING,
        ),
    ]
    return items


def _build_linear_identity_items(witness_v, witness_q):
    """
    Build identity items specific to LINEAR dimension shapes.

    Linear-specific properties (Linear, LinearFixed):
      - dim_type.witness_line_control: Witness line behavior setting

    Note: Witness lines are ONLY applicable to linear dimensions.
    Other shapes do not have this parameter.

    Returns:
        list: List of identity item dicts for linear shapes
    """
    items = [
        make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
    ]
    return items


def _build_radial_identity_items(center_marks_v, center_marks_q, center_mark_size_v, center_mark_size_q):
    """
    Build identity items specific to RADIAL dimension shapes.

    Radial-specific properties (Radial, Diameter, DiameterLinked):
      - dim_type.center_marks: Center marks enabled/disabled
      - dim_type.center_mark_size: Size of center marks

    Note: Center marks are ONLY applicable to radial/diameter dimensions.
    Linear and angular shapes do not have these parameters.

    Returns:
        list: List of identity item dicts for radial shapes
    """
    items = [
        make_identity_item("dim_type.center_marks", center_marks_v, center_marks_q),
        make_identity_item("dim_type.center_mark_size", center_mark_size_v, center_mark_size_q),
    ]
    return items


def _build_angular_identity_items():
    """
    Build identity items specific to ANGULAR dimension shapes.

    Angular-specific properties (Angular, ArcLength):
      - Currently no unique properties beyond common ones

    Returns:
        list: Empty list (angular uses only common properties)
    """
    return []


def _build_spot_identity_items():
    """
    Build identity items specific to SPOT dimension shapes.

    Spot-specific properties (SpotElevation, SpotCoordinate, SpotSlope, etc.):
      - Currently no unique properties beyond common ones

    Returns:
        list: Empty list (spot uses only common properties)
    """
    return []


def _build_identity_items(
    shape_family,
    shape_v,
    shape_q,
    unit_format_id_v,
    unit_format_id_q,
    rounding_v,
    rounding_q,
    accuracy_v,
    accuracy_q,
    prefix_v,
    prefix_q,
    suffix_v,
    suffix_q,
    tick_sig_hash,
    witness_v=None,
    witness_q=None,
    center_marks_v=None,
    center_marks_q=None,
    center_mark_size_v=None,
    center_mark_size_q=None,
):
    """
    Build complete identity_items list with shape-gated property inclusion.

    This function implements shape-gating:
      1. Always includes common properties for all shapes
      2. Conditionally includes shape-specific properties based on shape_family
      3. Never includes properties that would be UNSUPPORTED_NOT_APPLICABLE

    Args:
        shape_family: Shape family constant (FAMILY_LINEAR, FAMILY_RADIAL, etc.)
        shape_v, shape_q: Shape value and quality
        unit_format_id_v/q, rounding_v/q, accuracy_v/q: Unit format properties
        prefix_v/q, suffix_v/q: Prefix/suffix properties
        tick_sig_hash: Tick mark signature hash (or None)
        witness_v/q: Witness line control (only used for LINEAR)
        center_marks_v/q, center_mark_size_v/q: Center mark properties (only used for RADIAL)

    Returns:
        tuple: (identity_items, required_qualities)
            - identity_items: Sorted list of identity item dicts
            - required_qualities: List of quality values to check for blocking
    """
    # 1. Start with common properties
    items = _build_common_identity_items(
        shape_v=shape_v,
        shape_q=shape_q,
        unit_format_id_v=unit_format_id_v,
        unit_format_id_q=unit_format_id_q,
        rounding_v=rounding_v,
        rounding_q=rounding_q,
        accuracy_v=accuracy_v,
        accuracy_q=accuracy_q,
        prefix_v=prefix_v,
        prefix_q=prefix_q,
        suffix_v=suffix_v,
        suffix_q=suffix_q,
        tick_sig_hash=tick_sig_hash,
    )

    # Track required qualities for blocking determination
    # Common required qualities (shape, unit formatting, prefix/suffix)
    required_qs = [
        shape_q,
        unit_format_id_q,
        rounding_q,
        accuracy_q,
        prefix_q,
        suffix_q,
    ]

    # 2. Add shape-specific properties
    if _is_linear_family(shape_family):
        items.extend(_build_linear_identity_items(witness_v, witness_q))
        # witness_line_control is required for linear shapes
        if witness_q is not None:
            required_qs.append(witness_q)

    elif _is_radial_family(shape_family):
        items.extend(_build_radial_identity_items(
            center_marks_v, center_marks_q,
            center_mark_size_v, center_mark_size_q,
        ))
        # center mark properties are required for radial shapes
        if center_marks_q is not None:
            required_qs.append(center_marks_q)
        if center_mark_size_q is not None:
            required_qs.append(center_mark_size_q)

    elif _is_angular_family(shape_family):
        items.extend(_build_angular_identity_items())
        # No additional required qualities for angular

    elif _is_spot_family(shape_family):
        items.extend(_build_spot_identity_items())
        # No additional required qualities for spot

    # Unknown shape family: only common properties, but mark as degraded
    # (required_qs already contains common qualities)

    # 3. Sort items by key for deterministic ordering
    items = sorted(items, key=lambda it: it.get("k", ""))

    return items, required_qs


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
        # canon_str("") may return sentinel strings like "<MISSING>".
        # Treat sentinels as absent for display-name composition.
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
        # canon_str("") may return sentinel strings like "<MISSING>".
        # Treat sentinels as absent for display-name composition.
        if typ in (S_MISSING, S_UNREADABLE, "", None):
            typ = None
    except Exception:
        typ = None

    if fam and typ:
        return "{}:{}".format(fam, typ)
    if typ:
        return typ
    if fam:
        # Type Name exists but is blank for some unused/system DimensionTypes.
        # Provide deterministic fallback to avoid "<MISSING>" strings.
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

def _phase2_build_join_key_items(doc, d, shape_v, shape_q, witness_v, witness_q):
    """
    Phase-2 join key (coarse) for DimensionType records.

    Hard rules:
      - No heuristics.
      - Explicit missing/unsupported/unreadable.
      - Do not emit legacy sentinel literals in IdentityItem.v.
    """
    # Family Name (best-effort)
    fam_v, fam_q = (None, ITEM_Q_MISSING)
    try:
        p_fam = first_param(
            d,
            bip_names=["SYMBOL_FAMILY_NAME_PARAM"],
            ui_names=["Family Name"],
        )
        fam_v, fam_q = canonicalize_str(_as_string(p_fam) if p_fam is not None else None)
    except Exception:
        fam_v, fam_q = (None, ITEM_Q_UNREADABLE)

    # UnitTypeId from UnitsFormatOptions (best-effort; not applicable is explicit)
    unit_type_v, unit_type_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)

    fo = None
    fo_exc = None
    try:
        fo = d.GetUnitsFormatOptions()
    except Exception as ex:
        fo_exc = ex

    if fo is None:
        if fo_exc is not None:
            try:
                msg = safe_str(getattr(fo_exc, "Message", None) or fo_exc)
                tname = safe_str(getattr(type(fo_exc), "__name__", "")).lower()
                msg_l = msg.lower()
                if (
                    "notsupported" in tname
                    or "invalidoperation" in tname
                    or "not supported" in msg_l
                    or "not applicable" in msg_l
                    or "unsupported" in msg_l
                ):
                    unit_type_v, unit_type_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
                else:
                    unit_type_v, unit_type_q = (None, ITEM_Q_UNREADABLE)
            except Exception:
                unit_type_v, unit_type_q = (None, ITEM_Q_UNREADABLE)
    else:
        try:
            unit_type_v, unit_type_q = canonicalize_str(safe_str(fo.GetUnitTypeId()))
        except Exception:
            unit_type_v, unit_type_q = (None, ITEM_Q_UNREADABLE)

    items = [
        make_identity_item("dim_join.family_name", fam_v, fam_q),
        make_identity_item("dim_join.shape", shape_v, shape_q),
        make_identity_item("dim_join.unit_type_id", unit_type_v, unit_type_q),
        make_identity_item("dim_join.witness_line_control", witness_v, witness_q),
    ]
    return phase2_sorted_items(items)

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

    # record.v2 build state
    v2_records = []
    v2_sig_hashes = []  # non-null only
    v2_block_reasons = {}

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
        # Prefer BuiltInParameter when possible to avoid localized UI name dependence.
        text_font = _as_string(
            first_param(
                d,
                bip_names=[
                    "TEXT_FONT",
                    "DIM_TEXT_FONT",
                    "SPOT_ELEV_TEXT_FONT",
                    "SPOT_COORDINATE_TEXT_FONT",
                ],
                ui_names=["Text Font"],
            )
        )
        text_font = canon_str(text_font)

        text_size_ft = _as_double(
            first_param(
                d,
                bip_names=[
                    "TEXT_SIZE",
                    "DIM_TEXT_SIZE",
                    "SPOT_ELEV_TEXT_SIZE",
                    "SPOT_COORDINATE_TEXT_SIZE",
                ],
                ui_names=["Text Size"],
            )
        )
        text_size_in = fnum(format_len_inches(text_size_ft), 6)

        lw = _as_int(
            first_param(
                d,
                bip_names=["LINE_WEIGHT", "DIM_LINE_WEIGHT"],
                ui_names=["Line Weight"],
            )
        )
        color_int, color_rgb = try_get_color_rgb_from_elem(d)

        # Tick Mark (arrowhead) – store UniqueId metadata + include NAME in signature (more stable than ids)
        tick_param_present = False
        tick_name = ""
        tick_uid = None
        tick_exc = None
        tick_sig_hash = None

        try:
            p_tick = first_param(
                d,
                bip_names=["DIM_LEADER_ARROWHEAD", "TICK_MARK", "DIM_TICK_MARK"],
                ui_names=["Tick Mark"],
            )
            tick_param_present = p_tick is not None

            if p_tick is not None:
                # ValueString is stable for legacy signature purposes (even if no ElementId)
                try:
                    tick_name = _as_string(p_tick)
                except Exception:
                    tick_name = ""

                if getattr(p_tick, "HasValue", False):
                    tid = None
                    try:
                        tid = p_tick.AsElementId()
                    except Exception:
                        tid = None

                    if tid and getattr(tid, "IntegerValue", 0) > 0:
                        te = None
                        try:
                            te = doc.GetElement(tid)
                        except Exception:
                            te = None

                        if te is not None:
                            tick_uid = getattr(te, "UniqueId", None)

                            try:
                                # Prefer definition-based arrowhead signature via ctx (no UID).
                                ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
                                k = safe_str(getattr(tid, "IntegerValue", None))
                                if k and isinstance(ah_map, dict) and k in ah_map:
                                    tick_sig_hash = ah_map.get(k, {}).get("sig_hash", None)
                            except Exception:
                                tick_sig_hash = None

                            # Prefer a stable display name for legacy signature when available
                            try:
                                tick_name = tick_name or get_element_display_name(te) or tick_name
                            except Exception:
                                pass

            # Canonicalize legacy signature value, but preserve explicit blank as blank
            if tick_name:
                tick_name = canon_str(tick_name)
        except Exception as ex:
            tick_exc = ex
            tick_name = S_UNREADABLE

        # Witness line control is family-conditional; absence must not be treated as missing.
        witness_param_present = False
        witness_raw = ""
        try:
            p_wit = first_param(d, ui_names=["Witness Line Control"])
            witness_param_present = p_wit is not None
            if p_wit is None:
                witness = S_NOT_APPLICABLE
            else:
                witness_raw = _as_string(p_wit)
                # Preserve explicit blank as blank for signature purposes
                witness = canon_str(witness_raw) if witness_raw else ""
        except Exception:
            witness = S_UNREADABLE

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
            units_fmt = _format_options_to_kv(fo)
        except Exception:
            units_fmt = None

        try:
            afo = d.GetAlternateUnitsFormatOptions()
            alt_units_fmt = _format_options_to_kv(afo)
        except Exception:
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
        # record.v2 per-record emission (identity lives in sig_hash)
        # ---------------------------

        # Required: dim_type.uid
        try:
            uid_raw = getattr(d, "UniqueId", None)
        except Exception:
            uid_raw = None
        uid_v, uid_q = canonicalize_str(uid_raw)

        # Required: dim_type.shape
        # Use shape detection helper for consistent shape detection and family classification
        shape_v, shape_family, shape_q = _get_dimension_shape(d)

        # Optional: dim_type.text_type_uid (ElementId -> element.UniqueId)
        text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
        try:
            p_tt = first_param(d, bip_names=["TEXT_TYPE"], ui_names=["Text Type"])  # best-effort
            if p_tt is None:
                text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
            else:
                try:
                    eid = p_tt.AsElementId()
                except Exception:
                    eid = None
                if eid is None:
                    text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)
                else:
                    try:
                        el = doc.GetElement(eid)
                    except Exception:
                        el = None
                    if el is None:
                        text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
                    else:
                        try:
                            text_type_uid_v, text_type_uid_q = canonicalize_str(getattr(el, "UniqueId", None))
                        except Exception:
                            text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)
        except Exception:
            text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)

        # Optional: dim_type.tick_mark_uid
        # - If the parameter is absent for this family: unsupported.not_applicable
        # - If present but explicitly none/invalid: ok with empty string
        # - If present and resolvable: ok with referenced element UniqueId
        tick_uid_v, tick_uid_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
        if tick_param_present:
            if tick_uid:
                tick_uid_v, tick_uid_q = canonicalize_str(tick_uid)
            else:
                tick_uid_v, tick_uid_q = ("", ITEM_Q_OK)

        # Shape-gated: dim_type.witness_line_control (LINEAR_FAMILY only)
        # - Only extracted for linear shapes (Linear, LinearFixed)
        # - If parameter is absent: missing (not applicable doesn't apply since we only extract for linear)
        # - If present: allow empty-as-ok
        witness_v, witness_q = (None, ITEM_Q_MISSING)
        if _is_linear_family(shape_family) and witness_param_present:
            try:
                witness_v, witness_q = canonicalize_str_allow_empty(witness_raw)
            except Exception:
                witness_v, witness_q = (None, ITEM_Q_UNREADABLE)

        # Shape-gated: dim_type.center_marks and dim_type.center_mark_size (RADIAL_FAMILY only)
        # - Only extracted for radial shapes (Radial, Diameter, DiameterLinked)
        # - Uses integer (center_marks) and float (center_mark_size) already read above
        center_marks_v, center_marks_q = (None, ITEM_Q_MISSING)
        center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)

        if _is_radial_family(shape_family):
            # center_marks is already extracted as _as_int(_p("Center Marks"))
            if center_marks is not None:
                center_marks_v, center_marks_q = canonicalize_str(safe_str(center_marks))
                if center_marks_v is None:
                    center_marks_q = ITEM_Q_UNREADABLE
            # center_mark_size is already extracted as _as_double(_p("Center Mark Size"))
            if center_mark_size is not None:
                try:
                    center_mark_size_v, center_mark_size_q = canonicalize_float(_fmt_in_from_ft(center_mark_size))
                except Exception:
                    center_mark_size_v, center_mark_size_q = (None, ITEM_Q_UNREADABLE)

        # Optional: unit format identity fields from UnitsFormatOptions
        # If the dimension type uses project defaults (no per-type override), treat these as valid N/A
        # instead of "missing"/"unreadable" to avoid spurious degradation.
        unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
        rounding_v, rounding_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
        accuracy_v, accuracy_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)

        # Prefix/Suffix are DimensionType properties.
        # Contract: if property exists and is readable, blank is OK and MUST NOT collapse to missing.
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

        fo = None
        fo_exc = None
        try:
            fo = d.GetUnitsFormatOptions()
        except Exception as ex:
            fo_exc = ex

        if fo is None:
            if fo_exc is not None and (not _units_fo_not_applicable(fo_exc)):
                unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNREADABLE)
                rounding_v, rounding_q = (None, ITEM_Q_UNREADABLE)
                accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)
        else:
            use_default = getattr(fo, "UseDefault", None)

            # If the type uses project defaults, treat that as deterministic identity
            # (do NOT mark as unsupported/not_applicable, which blocks sig_hash).
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
                    # UnitsFormatOptions.Accuracy is stored as feet; identity key expects a string, so use inches string.
                    accuracy_v, accuracy_q = canonicalize_float(_fmt_in_from_ft(getattr(fo, "Accuracy", None)))
                except Exception:
                    accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)

        # ---------------------------
        # Phase-2 instrumentation (additive, non-normative)
        # ---------------------------

        # Phase-2 attribute bags (explicit grouping is a hypothesis marker, not a standard)
        # Important: do not emit legacy sentinel literals in IdentityItem.v.
        type_name_v, type_name_q = phase2_qv_from_legacy_sentinel_str(type_name, allow_empty=False)
        text_font_v, text_font_q = phase2_qv_from_legacy_sentinel_str(text_font, allow_empty=False)
        tick_name_v, tick_name_q = phase2_qv_from_legacy_sentinel_str(tick_name, allow_empty=True)

        text_size_in_v, text_size_in_q = canonicalize_str(_fmt_in_from_ft(text_size_ft))
        lw_v, lw_q = canonicalize_str(lw)
        color_int_v, color_int_q = canonicalize_str(color_int)

        phase2_unknown_items = phase2_sorted_items([
            make_identity_item("dim_attr.prefix", prefix_v, prefix_q),
            make_identity_item("dim_attr.suffix", suffix_v, suffix_q),
        ])

        phase2_cosmetic_items = phase2_sorted_items([
            make_identity_item("dim_attr.type_name", type_name_v, type_name_q),
            make_identity_item("dim_type.name", type_name_v, type_name_q),
            make_identity_item("dim_attr.text_font", text_font_v, text_font_q),
            make_identity_item("dim_attr.text_size_in", text_size_in_v, text_size_in_q),
            make_identity_item("dim_attr.line_weight", lw_v, lw_q),
            make_identity_item("dim_attr.color_int", color_int_v, color_int_q),
            make_identity_item("dim_attr.tick_mark_name", tick_name_v, tick_name_q),
            make_identity_item("dim_attr.tick_mark_uid", tick_uid_v, tick_uid_q),
            make_identity_item("dim_attr.tick_mark_sig_hash", safe_str(tick_sig_hash) if tick_sig_hash else "", ITEM_Q_OK),
        ])


        # ---------------------------
        # Shape-gated identity_items (Phase-2 compliant, definition-based)
        # ---------------------------
        # tick_mark_uid excluded per Phase 2 architecture (UIDs are file-local, not semantic)
        #
        # Shape-gating approach:
        #   - Common properties are included for ALL shapes
        #   - Shape-specific properties are only included for applicable shapes
        #   - No ITEM_Q_UNSUPPORTED_NOT_APPLICABLE for shape-gated properties
        #   - required_qs varies by shape (only includes qualities for included properties)
        identity_items, required_qs = _build_identity_items(
            shape_family=shape_family,
            shape_v=shape_v,
            shape_q=shape_q,
            unit_format_id_v=unit_format_id_v,
            unit_format_id_q=unit_format_id_q,
            rounding_v=rounding_v,
            rounding_q=rounding_q,
            accuracy_v=accuracy_v,
            accuracy_q=accuracy_q,
            prefix_v=prefix_v,
            prefix_q=prefix_q,
            suffix_v=suffix_v,
            suffix_q=suffix_q,
            tick_sig_hash=tick_sig_hash,
            # Linear-specific (only used if shape_family == FAMILY_LINEAR)
            witness_v=witness_v,
            witness_q=witness_q,
            # Radial-specific (only used if shape_family == FAMILY_RADIAL)
            center_marks_v=center_marks_v,
            center_marks_q=center_marks_q,
            center_mark_size_v=center_mark_size_v,
            center_mark_size_q=center_mark_size_q,
        )

        # Canonical evidence source for this domain is identity_basis.items.
        # Selectors (join_key.keys_used, phase2.semantic_keys) define subsets without duplicating k/q/v evidence.
        semantic_keys = sorted([it.get("k", "") for it in identity_items if isinstance(it.get("k"), str) and it.get("k")])

        # Policy-driven join_key is built from v2 identity_items only (no uid/name/id fallback)
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "dimension_types")
        join_key_policy, _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
        )

        # Block if any required authoritative identity field is not OK.
        # Shape-specific required qualities are determined by _build_identity_items based on shape_family.
        blocked = any(q != ITEM_Q_OK for q in required_qs)

        status_reasons = []
        any_incomplete = False
        for it in identity_items:
            q = it.get("q")
            k = it.get("k", "")
            # With shape-gating, we no longer use ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
            # for shape-specific properties - they simply aren't included.
            # ITEM_Q_MISSING is acceptable for tick_mark_sig_hash (not all types have tick marks)
            if q == ITEM_Q_OK:
                continue
            if q == ITEM_Q_MISSING and k == "dim_type.tick_mark_sig_hash":
                # tick_mark_sig_hash missing is acceptable
                continue
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(q, k))

        # Label
        label_quality = "human"
        label_display = safe_str(type_name) if (type_name not in (S_MISSING, S_UNREADABLE) and safe_str(type_name).strip()) else "Dimension Type"
        if blocked:
            # Required identity missing/unreadable: mark placeholder to avoid implying a human label is authoritative.
            label_quality = "placeholder_unreadable" if (uid_q == ITEM_Q_UNREADABLE or shape_q == ITEM_Q_UNREADABLE) else "placeholder_missing"

        label = {
            "display": label_display,
            "quality": label_quality,
            "provenance": "revit.Name",
            "components": {
                "type_id": safe_str(getattr(getattr(d, "Id", None), "IntegerValue", "")),
                "type_name": safe_str(type_name),
            },
        }

        record_id = uid_v if (uid_q == ITEM_Q_OK and uid_v) else "dim_type_id:{}".format(safe_str(getattr(getattr(d, "Id", None), "IntegerValue", "")))

        if blocked:
            v2_block_reasons["blocked_required"] = int(v2_block_reasons.get("blocked_required", 0)) + 1
            rec_v2 = build_record_v2(
                domain="dimension_types",
                record_id=record_id,
                status=STATUS_BLOCKED,
                status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                sig_hash=None,
                identity_items=identity_items,
                required_qs=(),
                label=label,
            )
            # Phase-2 additive fields (do not affect record.v2 invariants)
            rec_v2["join_key"] = join_key_policy

            rec_v2["phase2"] = {
                "schema": "phase2.dimension_types.v1",
                "grouping_basis": "phase2.hypothesis",
                # Deprecated direction: semantic selectors should reference canonical identity_basis.items.
                "semantic_keys": semantic_keys,
                "cosmetic_items": phase2_cosmetic_items,
                "coordination_items": phase2_sorted_items([]),
                "unknown_items": phase2_unknown_items,
            }
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            preimage = serialize_identity_items(identity_items)
            sig_hash_v2 = make_hash(preimage)
            v2_sig_hashes.append(sig_hash_v2)
            rec_v2 = build_record_v2(
                domain="dimension_types",
                record_id=record_id,
                status=status,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash_v2,
                identity_items=identity_items,
                required_qs=required_qs,
                label=label,
            )
            # Phase-2 additive fields (do not affect record.v2 invariants)
            rec_v2["join_key"] = join_key_policy

            rec_v2["phase2"] = {
                "schema": "phase2.dimension_types.v1",
                "grouping_basis": "phase2.hypothesis",
                # Deprecated direction: semantic selectors should reference canonical identity_basis.items.
                "semantic_keys": semantic_keys,
                "cosmetic_items": phase2_cosmetic_items,
                "coordination_items": phase2_sorted_items([]),
                "unknown_items": phase2_unknown_items,
            }

        v2_records.append(rec_v2)
        sig_hashes.append(sig_hash)

        # Legacy (v1) record emission was never completed in this file.
        # Preserve existing keys and compute only domain-level aggregates.
        # If you later want per-type v1 rows, add them explicitly and deterministically.

    # --- finalize legacy (v1) aggregates ---
    info["count"] = len(names)
    info["names"] = sorted(names)
    info["signature_hashes"] = sorted(sig_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else make_hash([])

    info["debug_missing_name"] = int(missing)

    # --- finalize v2 aggregates ---
    info["records"] = v2_records

    v2_sig_hashes = sorted([h for h in v2_sig_hashes if h])
    if v2_sig_hashes:
        info["hash_v2"] = make_hash(v2_sig_hashes)
        info["debug_v2_blocked"] = False
    else:
        # If we saw types but none could produce a v2 signature, force blocked signal.
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True

    info["debug_v2_block_reasons"] = dict(v2_block_reasons)

    return info
