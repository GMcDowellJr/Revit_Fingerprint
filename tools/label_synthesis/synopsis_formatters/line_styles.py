"""
tools/label_synthesis/synopsis_formatters/line_styles.py

Behavioral synopsis formatter for the line_styles domain.

Produces labels like:
  "LW1 | Black | Solid"
  "LW3 | Red | Dashed"
  "LW5 | Blue | Dash-Dot"

Identity items available:
  line_style.weight.projection      — integer line weight
  line_style.color.rgb              — "R-G-B" string
  line_style.pattern_ref.sig_hash   — opaque hash of referenced line pattern
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


# Common color approximations — R-G-B format
_COLOR_NAMES = {
    "0-0-0":       "Black",
    "255-255-255": "White",
    "255-0-0":     "Red",
    "0-255-0":     "Green",
    "0-0-255":     "Blue",
    "128-0-0":     "Dark Red",
    "0-128-0":     "Dark Green",
    "0-0-128":     "Dark Blue",
    "255-255-0":   "Yellow",
    "0-255-255":   "Cyan",
    "255-0-255":   "Magenta",
    "128-128-128": "Gray",
    "192-192-192": "Light Gray",
    "64-64-64":    "Dark Gray",
}


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    kv = {
        item["k"]: item["v"]
        for item in identity_items
        if isinstance(item, dict)
        and item.get("q") == "ok"
        and item.get("v") not in (None, "", "__missing__", "__na__", "__not_applicable__")
    }

    weight = kv.get("line_style.weight.projection")
    if not weight:
        return None

    parts = [f"LW{weight}"]

    # Color
    color_rgb = kv.get("line_style.color.rgb") or kv.get("line_style.color_rgb")
    if color_rgb:
        color_name = _COLOR_NAMES.get(color_rgb)
        if color_name:
            parts.append(color_name)
        else:
            # Normalize RGB string for display
            parts.append(_format_rgb(color_rgb))

    # Pattern — we only have the sig_hash, which is opaque
    # But its presence vs absence tells us solid vs patterned
    pattern_hash = kv.get("line_style.pattern_ref.sig_hash")
    pattern_kind = kv.get("line_style.pattern_ref.kind")
    if pattern_kind == "Solid" or pattern_hash is None:
        parts.append("Solid")
    else:
        parts.append("Patterned")

    return " | ".join(parts)


def _format_rgb(rgb_str: str) -> str:
    """Convert 'R-G-B' to '#RRGGBB' hex."""
    try:
        parts = rgb_str.split("-")
        if len(parts) == 3:
            r, g, b = int(parts[0]), int(parts[1]), int(parts[2])
            return f"#{r:02X}{g:02X}{b:02X}"
    except (ValueError, TypeError):
        pass
    return rgb_str