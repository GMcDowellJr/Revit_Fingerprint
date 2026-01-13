# -*- coding: utf-8 -*-
"""
Core row/parameter utilities (Revit-aware).

Provides helpers for reading parameters, element names, and unit conversion.
These utilities are Revit-aware but still considered "core" because they're
used across multiple domains.
"""

try:
    from Autodesk.Revit.DB import BuiltInParameter, UnitUtils, UnitTypeId
except ImportError:
    # Allow import in non-Revit environments for testing
    BuiltInParameter = None
    UnitUtils = None
    UnitTypeId = None

from core.canon import canon_str, safe_str


def _param(elem, bip):
    """
    Safely get a parameter by BuiltInParameter enum.

    Args:
        elem: Revit element
        bip: BuiltInParameter enum value

    Returns:
        Parameter object or None if not found/accessible
    """
    try:
        return elem.get_Parameter(bip)
    except Exception as e:
        return None


def _as_string(p):
    """
    Extract string value from a parameter.

    Args:
        p: Parameter object

    Returns:
        String value or None if not available
    """
    try:
        if p and p.HasValue:
            s = p.AsString()
            if s is not None:
                return safe_str(s)
    except Exception as e:
        pass
    return None


def _as_double(p):
    """
    Extract double value from a parameter.

    Args:
        p: Parameter object

    Returns:
        Float value or None if not available
    """
    try:
        if p and p.HasValue:
            return p.AsDouble()
    except Exception as e:
        pass
    return None


def _as_int(p):
    """
    Extract integer value from a parameter.

    Args:
        p: Parameter object

    Returns:
        Integer value or None if not available
    """
    try:
        if p and p.HasValue:
            return p.AsInteger()
    except Exception as e:
        pass
    return None


def _as_bool_from_param(p):
    """
    Extract boolean value from a parameter (via integer).

    Args:
        p: Parameter object

    Returns:
        True/False or None if not available
    """
    v = _as_int(p)
    if v is None:
        return None
    return True if v != 0 else False


def first_param(elem, bip_names=None, ui_names=None):
    """
    Find first available parameter by trying BuiltInParameter names,
    then UI names as fallback.

    Args:
        elem: Revit element
        bip_names: List of BuiltInParameter attribute names (e.g., ["TEXT_COLOR"])
        ui_names: List of UI parameter names (e.g., ["Color"])

    Returns:
        First available Parameter object with a value, or None
    """
    # Try BuiltInParameter by NAME safely (no AttributeError)
    for bip_name in (bip_names or []):
        try:
            bip = getattr(BuiltInParameter, bip_name, None)
        except Exception as e:
            bip = None
        if bip is None:
            continue
        try:
            p = elem.get_Parameter(bip)
            if p and p.HasValue:
                return p
        except Exception as e:
            pass

    # UI-name fallback (English UI labels)
    for nm in (ui_names or []):
        try:
            p = elem.LookupParameter(nm)
            if p and p.HasValue:
                return p
        except Exception as e:
            pass

    return None


def format_len_inches(feet_val):
    """
    Convert internal Revit units (feet) to inches.

    Args:
        feet_val: Length value in feet (Revit internal units)

    Returns:
        Length in inches or None if conversion fails
    """
    if feet_val is None:
        return None
    try:
        return UnitUtils.ConvertFromInternalUnits(feet_val, UnitTypeId.Inches)
    except Exception as e:
        try:
            # Fallback for older API versions
            return float(feet_val) * 12.0
        except Exception as e:
            return None


def try_get_color_rgb_from_elem(elem):
    """
    Extract color from an element as both integer and RGB dict.

    Canonical color representation for all styles.

    Args:
        elem: Revit element

    Returns:
        Tuple of (color_int, color_rgb_dict)
        color_int: Integer color value or None
        color_rgb_dict: {"r": int, "g": int, "b": int} or None
    """
    p = first_param(elem, bip_names=["TEXT_COLOR", "LINE_COLOR"], ui_names=["Color"])
    color_int = _as_int(p)

    if color_int is None:
        return None, None

    try:
        r = (color_int      ) & 0xFF
        g = (color_int >>  8) & 0xFF
        b = (color_int >> 16) & 0xFF
        return color_int, {"r": r, "g": g, "b": b}
    except Exception as e:
        return color_int, None


def get_element_display_name(elem):
    """
    Get the best display name for an element.

    Tries .Name property first, then common name parameters.

    Args:
        elem: Revit element

    Returns:
        Canonical string name or None
    """
    if elem is None:
        return None

    # 1) .Name property
    try:
        nm = getattr(elem, "Name", None)
        nm_c = canon_str(nm)
        if nm_c:
            return nm_c
    except Exception as e:
        pass

    # 2) Common name parameters
    for bip_name in ["SYMBOL_NAME_PARAM", "ALL_MODEL_TYPE_NAME", "ALL_MODEL_INSTANCE_COMMENTS"]:
        bip = getattr(BuiltInParameter, bip_name, None) if BuiltInParameter else None
        if bip is None:
            continue
        try:
            p = elem.get_Parameter(bip)
            if p and p.HasValue:
                s = p.AsString()
                s_c = canon_str(s)
                if s_c:
                    return s_c
        except Exception as e:
            pass

    return None


def get_type_display_name(elem):
    """
    Get the type name as shown in the Type selector.

    Tries SYMBOL_NAME_PARAM first, then .Name property.

    Args:
        elem: Revit element (typically an ElementType)

    Returns:
        Canonical string type name or None
    """
    # 1) Type Name parameter
    try:
        p = elem.get_Parameter(BuiltInParameter.SYMBOL_NAME_PARAM) if BuiltInParameter else None
        if p and p.HasValue:
            nm = p.AsString()
            nm_c = canon_str(nm)
            if nm_c:
                return nm_c
    except Exception as e:
        pass

    # 2) Fallback to .Name
    try:
        nm = getattr(elem, "Name", None)
        nm_c = canon_str(nm)
        if nm_c:
            return nm_c
    except Exception as e:
        pass

    return None
