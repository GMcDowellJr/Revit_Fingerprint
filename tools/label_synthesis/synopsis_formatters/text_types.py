"""
tools/label_synthesis/synopsis_formatters/text_types.py

Behavioral synopsis formatter for the text_types domain.

Produces labels like:
  "Arial | 3/32" | Regular"
  "Arial | 1/8" | Bold Italic"
  "Courier New | 3/32" | Bold | Border"
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    kv = {
        item["k"]: item["v"]
        for item in identity_items
        if isinstance(item, dict)
        and item.get("q") == "ok"
        and item.get("v") not in (None, "", "__missing__", "__na__", "__not_applicable__")
    }

    font = kv.get("text_type.font", "")
    if not font:
        return None

    parts = [font]

    # Size
    size_raw = kv.get("text_type.size_in")
    if size_raw:
        frac = _inches_to_fraction(size_raw)
        parts.append(frac if frac else f'{size_raw}"')

    # Style flags
    bold = kv.get("text_type.bold") == "true"
    italic = kv.get("text_type.italic") == "true"
    underline = kv.get("text_type.underline") == "true"

    if bold and italic:
        parts.append("Bold Italic")
    elif bold:
        parts.append("Bold")
    elif italic:
        parts.append("Italic")
    elif underline:
        parts.append("Underline")
    else:
        parts.append("Regular")

    # Border
    show_border = kv.get("text_type.show_border") == "true"
    if show_border:
        parts.append("Border")

    # Background — 1=opaque, 0=transparent (Revit encoding)
    bg = kv.get("text_type.background")
    if bg == "1":
        parts.append("Opaque")

    return " | ".join(parts)


def _inches_to_fraction(val_str: str) -> Optional[str]:
    try:
        val = float(val_str)
    except (ValueError, TypeError):
        return None
    _MAP = {
        1/64: '1/64"', 1/32: '1/32"', 3/64: '3/64"',
        1/16: '1/16"', 5/64: '5/64"', 3/32: '3/32"',
        7/64: '7/64"', 1/8:  '1/8"',  9/64: '9/64"',
        5/32: '5/32"', 3/16: '3/16"', 1/4:  '1/4"',
    }
    for k, label in _MAP.items():
        if abs(val - k) < 1e-5:
            return label
    return None