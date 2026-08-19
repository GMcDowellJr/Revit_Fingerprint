"""
tools/label_synthesis/synopsis_formatters/object_styles_model.py

Behavioral synopsis formatter for the object_styles_model domain.

object_styles_model is one of ROW_KEY_DOMAINS (tools/bundle_analysis/common.py) --
bundle analysis derives scope_key per element/category by splitting
pattern_label_human on the first "|" (derive_scope_key). Before this formatter
existed, no synopsis_formatters/object_styles_model.py module was present, so
label resolution's Layer 2 (behavioral synopsis) always failed its dynamic
import and silently fell through to Layer 3/4/5 -- none of which reproduce the
"{category}|{subcategory}" prefix the row-key split requires. This is the fix:
lead every label with obj_style.row_key verbatim so the split is always
correct, regardless of which downstream layer would otherwise have won.

Produces labels like:
  "Doors | Panel | LW3 | Black | Cut LW5"
  "Walls | LW1 | Dark Gray | Cut LW1 | Material"

Note the label's first "|"-delimited segment is the TOP-LEVEL category only
("Doors", "Walls") -- derive_scope_key (tools/bundle_analysis/common.py)
splits on the first "|" alone, so that segment is what becomes scope_key.
Subcategory (when present and not "self") is carried as the next segment,
visible for review but not part of the scope key.

Identity items available (domains/object_styles.py, _MODEL_SEMANTIC_KEYS plus
the non-semantic row_key/parent_name items carried on every record):
  obj_style.row_key                 -- "{parent_name}|{row_name}", REQUIRED (not part of sig_hash basis)
  obj_style.weight.projection       -- integer line weight
  obj_style.weight.cut              -- integer line weight (model only)
  obj_style.color.rgb               -- "R-G-B" string
  obj_style.pattern_ref.sig_hash    -- opaque hash of referenced line pattern (None/solid vs patterned)
  obj_style.material_sig_hash       -- opaque hash of assigned material (model only)
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional


# Common color approximations -- R-G-B format. Duplicated per-formatter
# rather than shared, matching the existing convention in
# synopsis_formatters/line_styles.py (no shared util module in this package).
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

    # row_key is a required identity item (domains/object_styles.py's
    # required_qs) for any record that reached a real pattern cluster, so
    # this should always be present. If it's ever missing, fall through to
    # the next resolution layer rather than emit a scope-key-breaking label.
    row_key = kv.get("obj_style.row_key")
    if not row_key:
        return None

    # derive_scope_key (tools/bundle_analysis/common.py) splits on the FIRST
    # "|" only -- it takes everything before it as the scope. row_key itself
    # is "{parent_name}|{row_name}" (e.g. "Doors|Panel"), so the top-level
    # category must be the very first token, not the full row_key -- putting
    # "Doors|Panel" first would derive a scope of "Doors" anyway (single
    # split), silently discarding the subcategory rather than scoping by it.
    # Surface the subcategory as a second, non-scope-bearing segment instead
    # so it's still visible in the label for review, just not load-bearing.
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

    weight_cut = kv.get("obj_style.weight.cut")
    if weight_cut:
        parts.append(f"Cut LW{weight_cut}")

    if kv.get("obj_style.material_sig_hash"):
        parts.append("Material")

    return " | ".join(parts)


def _format_rgb(rgb_str: str) -> str:
    """Convert 'R-G-B' to '#RRGGBB' hex."""
    try:
        r, g, b = (int(p) for p in rgb_str.split("-"))
        return f"#{r:02X}{g:02X}{b:02X}"
    except (ValueError, TypeError):
        return rgb_str
