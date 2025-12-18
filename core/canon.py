# -*- coding: utf-8 -*-
"""
Core canonicalization and formatting utilities (pure Python, no Revit API).

Provides consistent string normalization and value formatting for
deterministic fingerprinting.
"""

from hashing import safe_str


def canon_str(s):
    """
    Canonicalize a string value by stripping whitespace.

    Args:
        s: String value to canonicalize (may be None)

    Returns:
        Stripped string, or None if input is None or conversion fails
    """
    if s is None:
        return None
    try:
        s2 = safe_str(s)
        return s2.strip()
    except:
        return None


def sig_val(v):
    """
    Format a value for signature inclusion with fail-soft handling.

    Converts None or empty strings to "<None>" marker to ensure
    distinct states remain distinct in signatures.

    Args:
        v: Value to format (any type)

    Returns:
        String representation or "<None>" marker
    """
    if v is None:
        return "<None>"
    s = safe_str(v).strip()
    return s if s else "<None>"


def fnum(v, nd):
    """
    Format a numeric value to a fixed number of decimal places.

    Args:
        v: Numeric value (may be None)
        nd: Number of decimal places

    Returns:
        Formatted float or None if input is None
    """
    return None if v is None else float(format(float(v), ".{}f".format(nd)))


def rgb_sig_from_color(col):
    """
    Create RGB signature string from a Revit Color object.

    Args:
        col: Revit Color object

    Returns:
        "R,G,B" string or "<None>" if color cannot be read
    """
    try:
        return "{},{},{}".format(int(col.Red), int(col.Green), int(col.Blue))
    except:
        return "<None>"


def rgb_dict_from_color(col):
    """
    Create RGB dictionary from a Revit Color object.

    Args:
        col: Revit Color object

    Returns:
        {"r": int, "g": int, "b": int} or None if color cannot be read
    """
    try:
        return {"r": int(col.Red), "g": int(col.Green), "b": int(col.Blue)}
    except:
        return None
