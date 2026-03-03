"""
tools/label_synthesis/synopsis_formatters/fill_patterns.py

Behavioral synopsis formatter for the fill_patterns domain.

Produces labels like:
  "Solid | Drafting"
  "Solid | Model"
  "2-grid | Drafting"
  "3-grid | Model"
  "1-grid | Drafting"

Identity items available:
  fill_pattern.is_solid     — "true" / "false"
  fill_pattern.target_id    — 1=Drafting, 2=Model (integer string)
  fill_pattern.grid_count   — integer string
  fill_pattern.grids_def_hash — opaque hash of full grid definition
  fill_pattern.grid[N].*    — per-grid geometry (angle, offset, shift, origin)
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

_TARGET_NAMES = {
    "1": "Drafting",
    "2": "Model",
}


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    kv = {
        item["k"]: item["v"]
        for item in identity_items
        if isinstance(item, dict)
        and item.get("q") == "ok"
        and item.get("v") not in (None, "", "__missing__", "__na__", "__not_applicable__")
    }

    is_solid = kv.get("fill_pattern.is_solid")
    target_id = kv.get("fill_pattern.target_id")
    grid_count = kv.get("fill_pattern.grid_count")

    target_name = _TARGET_NAMES.get(str(target_id), f"Target{target_id}") if target_id else None

    parts = []

    if is_solid == "true":
        parts.append("Solid")
    elif grid_count:
        try:
            n = int(grid_count)
            parts.append(f"{n}-grid")
        except (ValueError, TypeError):
            parts.append("Patterned")
    else:
        parts.append("Patterned")

    if target_name:
        parts.append(target_name)

    if not parts:
        return None

    return " | ".join(parts)