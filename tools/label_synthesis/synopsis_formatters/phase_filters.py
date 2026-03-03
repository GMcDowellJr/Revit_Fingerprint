"""
tools/label_synthesis/synopsis_formatters/phase_filters.py

Behavioral synopsis formatter for the phase_filters domain.

Produces labels like:
  "Show All | Show All | Show All | Show All"
  "Show Previous + Demo | Show All | Show All | Not Displayed"
  
Presentation IDs map to Revit phase status presentation styles:
  1 = Not Displayed
  2 = Overridden (uses override graphics)
  3 = By Category (normal display)
  4 = Not Applicable (shouldn't appear in normal filters)

The four statuses are: New | Existing | Demolished | Temporary

Identity items available:
  phase_filter.new.presentation_id         — integer
  phase_filter.existing.presentation_id    — integer
  phase_filter.demolished.presentation_id  — integer
  phase_filter.temporary.presentation_id   — integer
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

# Observed Revit presentation_id values
_PRES_NAMES = {
    "1": "Hidden",
    "2": "Overridden",
    "3": "Shown",
    "4": "N/A",
}

_STATUS_KEYS = [
    ("phase_filter.new.presentation_id",        "New"),
    ("phase_filter.existing.presentation_id",   "Exist"),
    ("phase_filter.demolished.presentation_id", "Demo"),
    ("phase_filter.temporary.presentation_id",  "Temp"),
]


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    kv = {
        item["k"]: item["v"]
        for item in identity_items
        if isinstance(item, dict)
        and item.get("q") == "ok"
        and item.get("v") not in (None, "", "__missing__", "__na__", "__not_applicable__")
    }

    parts = []
    found_any = False
    for key, label in _STATUS_KEYS:
        val = kv.get(key)
        if val is not None:
            found_any = True
            pres = _PRES_NAMES.get(str(val), f"P{val}")
            parts.append(f"{label}:{pres}")

    if not found_any:
        return None

    return " | ".join(parts)