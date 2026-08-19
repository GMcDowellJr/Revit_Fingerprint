"""
tools/label_synthesis/synopsis_formatters/object_styles_annotation.py

Behavioral synopsis formatter for the object_styles_annotation domain.

See synopsis_formatters/object_styles_model.py's module docstring for the full
root-cause explanation: object_styles_annotation is also a ROW_KEY_DOMAINS
member with no synopsis formatter previously registered, so bundle-analysis
scope derivation (derive_scope_key, tools/bundle_analysis/common.py) never
received a label containing the real "{category}|{subcategory}" prefix.

Produces labels like:
  "Doors | Panel | LW3 | Black"
  "Grids | LW2 | Dark Gray"

Note the label's first "|"-delimited segment is the TOP-LEVEL category only
("Doors", "Grids") -- derive_scope_key (tools/bundle_analysis/common.py)
splits on the first "|" alone, so that segment is what becomes scope_key.
Subcategory (when present and not "self") is carried as the next segment,
visible for review but not part of the scope key.

Identity items available (domains/object_styles.py, _NON_MODEL_SEMANTIC_KEYS
plus the non-semantic row_key item carried on every record). No cut-weight or
material items exist for annotation categories (include_cut_weight=False):
  obj_style.row_key                 -- "{parent_name}|{row_name}", REQUIRED (not part of sig_hash basis)
  obj_style.weight.projection       -- integer line weight
  obj_style.color.rgb               -- "R-G-B" string
  obj_style.pattern_ref.sig_hash    -- opaque hash of referenced line pattern
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional


# Duplicated per-formatter, matching the existing convention in
# synopsis_formatters/line_styles.py and object_styles_model.py.
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

    row_key = kv.get("obj_style.row_key")
    if not row_key:
        return None

    # See object_styles_model.py's format_synopsis for why the split happens
    # here rather than passing row_key through verbatim: derive_scope_key
    # only splits on the first "|", so the top-level category must lead.
    top_level, _, subcategory = row_key.partition("|")
    parts = [top_level]
    if subcategory and subcategory != "self":
        parts.append(subcategory)

    weight_proj = kv.get("obj_style.weight.projection")
    if weight_proj:
        parts.append(f"LW{weight_proj}")

    color_rgb = kv.get("obj_style.color.rgb")
    if color_rgb:
        parts.append(_COLOR_NAMES.get(color_rgb, _format_rgb(color_rgb)))

    return " | ".join(parts)


def _format_rgb(rgb_str: str) -> str:
    """Convert 'R-G-B' to '#RRGGBB' hex."""
    try:
        r, g, b = (int(p) for p in rgb_str.split("-"))
        return f"#{r:02X}{g:02X}{b:02X}"
    except (ValueError, TypeError):
        return rgb_str
