"""
tools/label_synthesis/synopsis_formatters/arrowheads.py

Behavioral synopsis formatter for the arrowheads domain.

Produces labels like:
  "Arrow | 45° | Filled | Closed"
  "Tick | 1/8" | Centered | LW2"
  "Dot | 3/32""
  "Diagonal | 1/8""
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

    style = kv.get("arrowhead.style", "")
    if not style:
        return None

    parts = [style]

    # Tick size — convert decimal inches to fraction
    tick_raw = kv.get("arrowhead.tick_size_in")
    if tick_raw:
        frac = _inches_to_fraction(tick_raw)
        parts.append(frac if frac else f'{tick_raw}"')

    # Arrow-specific
    if style in ("Arrow",):
        angle = kv.get("arrowhead.width_angle_deg")
        if angle:
            try:
                parts.append(f"{float(angle):.0f}°")
            except (ValueError, TypeError):
                pass

        fill = kv.get("arrowhead.fill_tick", "")
        if fill == "true":
            parts.append("Filled")

        closed = kv.get("arrowhead.arrow_closed", "")
        if closed == "true":
            parts.append("Closed")

    # Tick/Dot/Other-specific
    elif style in ("Tick", "Dot", "Other"):
        centered = kv.get("arrowhead.tick_mark_centered", "")
        if centered == "true":
            parts.append("Centered")

        pen = kv.get("arrowhead.heavy_end_pen_weight")
        if pen:
            parts.append(f"LW{pen}")

    return " | ".join(parts)


def _inches_to_fraction(val_str: str) -> Optional[str]:
    try:
        val = float(val_str)
    except (ValueError, TypeError):
        return None
    _MAP = {
        1/64: '1/64"', 1/32: '1/32"', 1/16: '1/16"',
        1/8:  '1/8"',  3/16: '3/16"', 1/4:  '1/4"',
        3/8:  '3/8"',  1/2:  '1/2"',  3/4:  '3/4"',
        1.0:  '1"',
    }
    for k, label in _MAP.items():
        if abs(val - k) < 1e-5:
            return label
    # For small arrowhead sizes, show as decimal if no fraction match
    return f'{val:.4f}"'.rstrip("0").rstrip(".")  + '"'
