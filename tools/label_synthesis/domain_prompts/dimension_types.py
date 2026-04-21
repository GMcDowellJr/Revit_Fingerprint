"""
tools/label_synthesis/domain_prompts/dimension_types.py

LLM system prompt and prompt builder for all dimension_types partition domains:
  dimension_types_linear, dimension_types_angular, dimension_types_radial,
  dimension_types_diameter, dimension_types_spot_elevation,
  dimension_types_spot_coordinate, dimension_types_spot_slope

Loaded via the base-name fallback in synthesize_fragmented_labels.py:
  dimension_types_linear → tries dimension_types_linear, then dimension_types (this file)

Routing within build_prompt is driven by dim_type.shape from identity_items,
not from the domain name passed at the call site.

Invoke via synthesize_fragmented_labels.py, not at emit time.

NOTE: _PARAM_LABELS keys must stay in sync with domain_identity_keys_v2.json.
      If a key is renamed in the extractor, it will silently stop appearing in prompts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming dimension type configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

DOMAIN CONTEXT — REVIT DIMENSION TYPES
=======================================
A Dimension Type in Revit controls how dimensions look and behave on drawings.
Every dimension on a sheet is governed by a Dimension Type. Projects inherit these
from templates, but teams often rename or duplicate them — which is why the same
underlying configuration often carries different names across projects.

You are seeing a configuration that multiple project teams implemented identically
but named differently. Your job is to produce the canonical name those teams
could have or might have used — not to prescribe what they should have used.
Name is excluded from the join key; fragmentation is purely a naming problem.

SHAPE FAMILIES — FOUR GROUPS
==============================
Dimension types split into four behaviorally distinct families. The shape value
in the parameters below tells you which family you are working with.

MEASURED FAMILY (Linear, LinearFixed, Angular, ArcLength)
  Primary discriminators: accuracy, tick_mark_sig_hash
  Linear/LinearFixed also: witness_line_control
  Angular/ArcLength: accuracy in degrees, no witness line

RADIAL/DIAMETER FAMILY (Radial, Diameter, DiameterLinked)
  Primary discriminators: accuracy, center_marks, center_mark_size, symbol fields
  Radial has radius_symbol_location/text; Diameter has diameter_symbol_location/text

SPOT ELEVATION FAMILY (SpotElevation, SpotElevationFixed)
  Completely different parameters — no accuracy, no tick mark
  Primary discriminators: elevation_indicator, top/bottom indicators,
    text_orientation, text_location, symbol_name

SPOT COORDINATE FAMILY (SpotCoordinate)
  Primary discriminators: top/bottom coordinates, N/S and E/W indicators,
    include_elevation, text_orientation, text_location, symbol_name

SPOT SLOPE FAMILY (SpotSlope)
  Primary discriminators: slope_direction, leader_line_length

KEY PARAMETERS — MEASURED DIMENSIONS
======================================

dim_type.shape
  Always present. The primary classifier — always lead with it in the name.
  Values: Linear, LinearFixed, Radial, Diameter, DiameterLinked,
          Angular, ArcLength, SpotElevation, SpotElevationFixed,
          SpotCoordinate, SpotSlope

dim_type.accuracy
  Precision of the readout, stored in decimal inches (or degrees for angular).
  Common values:
    0.25    = 1/4"   (very coarse, schematic)
    0.125   = 1/8"   (standard coarse — most common)
    0.0625  = 1/16"  (medium)
    0.03125 = 1/32"  (fine, detail work)
    0.001   = 0.001" (very fine, structural/MEP)
  Include accuracy in the name only when it differentiates patterns in the corpus.

dim_type.witness_line_control   [Linear/LinearFixed only]
  Controls the gap between element and witness line.
  Values: "Gap to Element" (current API), "Gap and Line" (older Revit exports),
          "Gap Only", "No Gap"
  These describe the same setting; "Gap to Element" and "Gap and Line" are equivalent.

dim_type.center_marks           [Radial/Diameter only]
  Whether a center mark cross appears at the arc/circle center.

dim_type.center_mark_size       [Radial/Diameter only]
  Physical size of the center mark.

dim_type.radius_symbol_location / dim_type.radius_symbol_text   [Radial only]
  Location and text of the radius prefix/suffix symbol (e.g., "R").

dim_type.diameter_symbol_location / dim_type.diameter_symbol_text   [Diameter only]
  Location and text of the diameter prefix/suffix symbol (e.g., "Ø").

dim_type.tick_mark_sig_hash
  Opaque behavioral hash of the arrowhead/tick mark style.
  Present = consistent arrowhead configuration across the pattern.
  Treat as context, not a naming driver.

dim_type.unit_format_id
  "Default" = uses project unit setting.
  Non-default = explicit override (e.g., decimal feet on a fractional-inch project).
  Include in name only when non-default.

dim_type.prefix / dim_type.suffix
  Text prepended/appended to readout. E.g., suffix "TYP", prefix "~".

KEY PARAMETERS — SPOT DIMENSIONS
===================================

dim_type.elevation_indicator    [SpotElevation]
  The indicator symbol shown with the elevation value (e.g., "EL.", "+", "").

dim_type.top_indicator / dim_type.bottom_indicator   [SpotElevation]
  Indicators for top and bottom of element (used for dual-value spots).

dim_type.top_indicator_as_prefix_suffix / dim_type.bottom_indicator_as_prefix_suffix
  Whether indicators appear as prefix or suffix.

dim_type.elevation_indicator_as_prefix_suffix   [SpotElevation]
  Whether the elevation indicator appears as prefix or suffix.

dim_type.text_orientation       [Spot types]
  How text orients on the drawing: "Horizontal", "Aligned with Element", etc.

dim_type.text_location          [Spot types]
  Where text appears relative to the symbol.

dim_type.symbol_name            [Spot types]
  The annotation symbol family used. This is the primary differentiator
  between spot elevation type families at many firms.

dim_type.top_coordinate / dim_type.bottom_coordinate   [SpotCoordinate]
  Whether to show top/bottom coordinate values.

dim_type.north_south_indicator / dim_type.east_west_indicator   [SpotCoordinate]
  Indicator text for N/S and E/W coordinate values.

dim_type.include_elevation      [SpotCoordinate]
  Whether elevation is included in coordinate callout.

dim_type.slope_direction        [SpotSlope]
  The direction arrow convention for slope indication.

dim_type.leader_line_length     [SpotSlope]
  The length of the slope leader line.

TEXT APPEARANCE PARAMETERS (secondary — rarely drive the name)
  dim_type.text_font, dim_type.text_size_in, dim_type.text_bold,
  dim_type.text_italic, dim_type.text_width_factor, dim_type.text_background,
  dim_type.color_rgb, dim_type.line_weight
  These appear in identity items. Note unusual values in rationale but do not
  include them in candidate names unless they are the primary differentiator.

NAMING CONVENTIONS at engineering firms
=========================================
Linear/Angular:
  "Linear Standard", "Standard", "Standard Dim"
  "Fine", "Detail", "1/32 Detail"
  "Coarse", "Schematic", "1/4 Schematic"
  "Angular", "Angle"
  Discipline prefixes: "Structural Linear", "Civil Linear", "Arch Standard"

Radial/Diameter:
  "Radial Standard", "Radius", "Radial Fine"
  "Diameter", "Diameter Standard"

Spot Elevation:
  "Spot Elevation", "Spot Elev", "SE Standard"
  Symbol-based: "Spot - Filled", "Spot - Arrow", "Spot - Triangle"
  Role-based: "Spot Elev - Top/Bottom", "Spot - Slab"

Spot Coordinate:
  "Spot Coordinate", "Coordinate", "North Coordinate"

Spot Slope:
  "Spot Slope", "Slope Arrow", "Slope - Percent"

NAMING RULES
============
1. Always start with the shape family name (Linear, Radial, Spot Elev, etc.)
2. Include accuracy when it differentiates — if all patterns share the same
   accuracy, omit it; if multiple accuracies exist, always include it
3. For spot types: symbol_name is the strongest differentiator after shape
4. Keep names under 40 characters for BI slicer legibility
5. If observed names converge in intent despite different formatting
   (e.g., "Dim-Standard", "DIM_STD", "Standard Dim"), synthesize the
   cleanest canonical form
6. If observed names span disciplines or offices, pick the most transferable name
   and note the discipline spread in rationale
"""


# ---------------------------------------------------------------------------
# Shape family routing
# ---------------------------------------------------------------------------

_SPOT_ELEVATION_SHAPES = frozenset({"SpotElevation", "SpotElevationFixed"})
_SPOT_COORDINATE_SHAPES = frozenset({"SpotCoordinate"})
_SPOT_SLOPE_SHAPES = frozenset({"SpotSlope"})
_SPOT_SHAPES = _SPOT_ELEVATION_SHAPES | _SPOT_COORDINATE_SHAPES | _SPOT_SLOPE_SHAPES
_RADIAL_SHAPES = frozenset({"Radial", "Diameter", "DiameterLinked"})
_LINEAR_SHAPES = frozenset({"Linear", "LinearFixed"})
_ANGULAR_SHAPES = frozenset({"Angular", "ArcLength"})


def _get_shape(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    for item in identity_items:
        if item.get("k") == "dim_type.shape" and item.get("q") == "ok":
            return str(item.get("v", "")).strip()
    return None


def _shape_context_note(shape: Optional[str]) -> Optional[str]:
    """Return a brief shape-specific note to insert before the parameters."""
    if shape in _LINEAR_SHAPES:
        return (
            f"Shape family: Linear ({shape}). "
            "Key discriminators: accuracy, witness_line_control, tick_mark_sig_hash."
        )
    if shape in _ANGULAR_SHAPES:
        return (
            f"Shape family: Angular ({shape}). "
            "Key discriminators: accuracy (in degrees), tick_mark_sig_hash. "
            "No witness line control — angular dimensions use common parameters only."
        )
    if shape in _RADIAL_SHAPES:
        return (
            f"Shape family: Radial/Diameter ({shape}). "
            "Key discriminators: accuracy, center_marks, center_mark_size, symbol fields."
        )
    if shape in _SPOT_ELEVATION_SHAPES:
        return (
            f"Shape family: Spot Elevation ({shape}). "
            "No accuracy or tick mark — symbol_name and indicator fields are the primary discriminators."
        )
    if shape in _SPOT_COORDINATE_SHAPES:
        return (
            "Shape family: Spot Coordinate. "
            "No accuracy or tick mark — coordinate indicators and symbol_name are primary."
        )
    if shape in _SPOT_SLOPE_SHAPES:
        return (
            "Shape family: Spot Slope. "
            "Primary discriminators: slope_direction and leader_line_length."
        )
    if shape:
        return f"Shape: {shape}."
    return None


# ---------------------------------------------------------------------------
# User prompt builder
# ---------------------------------------------------------------------------

def build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines = []
    shape = _get_shape(identity_items)

    # --- Observed names ---
    lines.append("OBSERVED NAMES IN THIS CORPUS")
    lines.append("(All these names refer to the same behavioral configuration)")
    if observed_labels:
        total_files = sum(int(r.get("files_count", 0)) for r in observed_labels)
        for row in observed_labels[:10]:
            label = str(row.get("label_v", "")).split(":", 1)[-1].strip()
            count = int(row.get("files_count", 0))
            pct = (count / total_files * 100) if total_files else 0
            lines.append(f'  "{label}"  ({count} files, {pct:.0f}%)')
    else:
        lines.append("  (no names observed — all files used unnamed/default configuration)")
    lines.append("")

    # --- Shape family note ---
    note = _shape_context_note(shape)
    if note:
        lines.append(f"SHAPE CONTEXT: {note}")
        lines.append("")

    # --- Behavioral parameters ---
    lines.append("BEHAVIORAL PARAMETERS (what this configuration actually does)")
    kv_lines = _format_identity_items(identity_items, shape=shape)
    if kv_lines:
        lines.extend(f"  {l}" for l in kv_lines)
    else:
        lines.append("  (no readable parameters available)")
    lines.append("")

    # --- Corpus context ---
    if corpus_context:
        total = corpus_context.get("total_files_in_corpus")
        pattern_count = corpus_context.get("domain_pattern_count")
        if total:
            lines.append(f"CORPUS CONTEXT: {total}-file corpus.")
        if pattern_count:
            lines.append(f"There are {pattern_count} distinct dimension type patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this dimension type pattern. Names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Start with the shape family (Linear, Radial, Spot Elev, etc.)\n"
        "  - Reflect what the configuration actually does\n"
        "  - Be short enough for a Power BI slicer (under 40 characters)\n"
        "  - Prefer the most common observed name if appropriate\n"
        "  - Converge messy naming variants toward a clean canonical form"
    )
    lines.append("")
    lines.append(
        "Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:\n"
        "{\n"
        '  "candidates": ["name1", "name2", "name3"],\n'
        '  "recommended": "name1",\n'
        '  "rationale": "One sentence explaining the recommended name"\n'
        "}"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parameter formatter
# ---------------------------------------------------------------------------

_PARAM_LABELS = {
    # Common
    "dim_type.shape":                           "Shape",
    "dim_type.accuracy":                        "Accuracy (decimal inches)",
    "dim_type.unit_format_id":                  "Unit format",
    "dim_type.rounding":                        "Rounding method",
    "dim_type.prefix":                          "Prefix text",
    "dim_type.suffix":                          "Suffix text",
    "dim_type.tick_mark_sig_hash":              "Tick mark/arrowhead config (opaque hash)",
    "dim_type.leader_arrowhead_sig_hash":       "Leader arrowhead config (opaque hash) [spot types]",
    # Linear
    "dim_type.witness_line_control":            "Witness line control [linear only]",
    # Radial/Diameter
    "dim_type.center_marks":                    "Center marks enabled [radial/diameter]",
    "dim_type.center_mark_size":                "Center mark size [radial/diameter]",
    "dim_type.radius_symbol_location":          "Radius symbol location [radial]",
    "dim_type.radius_symbol_text":              "Radius symbol text [radial]",
    "dim_type.diameter_symbol_location":        "Diameter symbol location [diameter]",
    "dim_type.diameter_symbol_text":            "Diameter symbol text [diameter]",
    # Spot elevation
    "dim_type.elevation_indicator":             "Elevation indicator [spot elev]",
    "dim_type.elevation_indicator_as_prefix_suffix": "Elevation indicator position [spot elev]",
    "dim_type.top_indicator":                   "Top indicator [spot elev]",
    "dim_type.bottom_indicator":                "Bottom indicator [spot elev]",
    "dim_type.top_indicator_as_prefix_suffix":  "Top indicator position [spot elev]",
    "dim_type.bottom_indicator_as_prefix_suffix": "Bottom indicator position [spot elev]",
    # Spot coordinate
    "dim_type.top_coordinate":                  "Show top coordinate [spot coord]",
    "dim_type.bottom_coordinate":               "Show bottom coordinate [spot coord]",
    "dim_type.north_south_indicator":           "N/S indicator [spot coord]",
    "dim_type.east_west_indicator":             "E/W indicator [spot coord]",
    "dim_type.include_elevation":               "Include elevation [spot coord]",
    "dim_type.indicator_as_prefix_suffix":      "Indicator position [spot coord]",
    # Spot slope
    "dim_type.slope_direction":                 "Slope direction [spot slope]",
    "dim_type.leader_line_length":              "Leader line length [spot slope]",
    # Spot shared
    "dim_type.text_orientation":                "Text orientation [spot types]",
    "dim_type.text_location":                   "Text location [spot types]",
    "dim_type.symbol_name":                     "Annotation symbol family [spot types]",
    # Text appearance (secondary)
    "dim_type.text_font":                       "Text font",
    "dim_type.text_size_in":                    "Text size (inches)",
    "dim_type.text_bold":                       "Text bold",
    "dim_type.text_italic":                     "Text italic",
    "dim_type.text_underline":                  "Text underline",
    "dim_type.text_width_factor":               "Text width factor",
    "dim_type.text_background":                 "Text background",
    "dim_type.color_rgb":                       "Color (R-G-B)",
    "dim_type.line_weight":                     "Line weight",
}

# Keys excluded from join policy — omit from prompt
_SKIP_KEYS = {
    "dim_type.name",
    "dim_type.tick_mark_uid",
    "dim_attr.tick_mark_uid",
}

# Opaque hash keys — show presence note, not value
_OPAQUE_KEYS = {
    "dim_type.tick_mark_sig_hash",
    "dim_type.leader_arrowhead_sig_hash",
}

# Text appearance keys — show only when non-default or differentiating
_TEXT_APPEARANCE_KEYS = {
    "dim_type.text_font",
    "dim_type.text_size_in",
    "dim_type.text_bold",
    "dim_type.text_italic",
    "dim_type.text_underline",
    "dim_type.text_width_factor",
    "dim_type.text_background",
    "dim_type.color_rgb",
    "dim_type.line_weight",
}

_TEXT_DEFAULTS = {
    "dim_type.text_bold": "false",
    "dim_type.text_italic": "false",
    "dim_type.text_underline": "false",
    "dim_type.text_width_factor": "1.0",
    "dim_type.text_background": "0",
    "dim_type.color_rgb": "0-0-0",
}

_ACCURACY_MAP = {
    0.25:     '1/4"',
    0.125:    '1/8"',
    0.0625:   '1/16"',
    0.03125:  '1/32"',
    0.015625: '1/64"',
    0.001:    '0.001"',
    1.0:      '1°',
    0.5:      '0.5°',
    0.1:      '0.1°',
    0.01:     '0.01°',
}

_WITNESS_EQUIVALENTS = {
    "gap and line": "Gap to Element (standard)",
    "gap to element": "Gap to Element (standard)",
    "gap only": "Gap Only",
    "no gap": "No Gap",
}


def _fmt_accuracy(val_str: str) -> str:
    try:
        v = float(val_str)
        for k, label in _ACCURACY_MAP.items():
            if abs(v - k) < 1e-6:
                return f"{val_str} ({label})"
        return val_str
    except (ValueError, TypeError):
        return val_str


def _fmt_witness(val_str: str) -> str:
    return _WITNESS_EQUIVALENTS.get(val_str.lower().strip(), val_str)


def _format_identity_items(
    identity_items: List[Dict[str, Any]],
    shape: Optional[str] = None,
) -> List[str]:
    lines = []
    seen = set()

    # Collect all readable values first
    kv: Dict[str, str] = {}
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        k = str(item.get("k", ""))
        q = str(item.get("q", "ok"))
        v = item.get("v")
        if not k or k in seen or k in _SKIP_KEYS:
            continue
        seen.add(k)
        if q != "ok" or v is None:
            continue
        if str(v).strip() in ("", "__missing__", "__na__", "__not_applicable__"):
            continue
        kv[k] = str(v)

    # Emit in a logical order: shape first, then primary discriminators, then secondary
    priority_order = [
        "dim_type.shape",
        "dim_type.accuracy",
        "dim_type.witness_line_control",
        "dim_type.center_marks",
        "dim_type.center_mark_size",
        "dim_type.radius_symbol_location",
        "dim_type.radius_symbol_text",
        "dim_type.diameter_symbol_location",
        "dim_type.diameter_symbol_text",
        "dim_type.elevation_indicator",
        "dim_type.elevation_indicator_as_prefix_suffix",
        "dim_type.top_indicator",
        "dim_type.bottom_indicator",
        "dim_type.top_indicator_as_prefix_suffix",
        "dim_type.bottom_indicator_as_prefix_suffix",
        "dim_type.top_coordinate",
        "dim_type.bottom_coordinate",
        "dim_type.north_south_indicator",
        "dim_type.east_west_indicator",
        "dim_type.include_elevation",
        "dim_type.indicator_as_prefix_suffix",
        "dim_type.slope_direction",
        "dim_type.leader_line_length",
        "dim_type.text_orientation",
        "dim_type.text_location",
        "dim_type.symbol_name",
        "dim_type.unit_format_id",
        "dim_type.prefix",
        "dim_type.suffix",
        "dim_type.rounding",
        "dim_type.tick_mark_sig_hash",
        "dim_type.leader_arrowhead_sig_hash",
    ]

    emitted = set()
    for k in priority_order:
        if k not in kv:
            continue
        emitted.add(k)
        label = _PARAM_LABELS.get(k, k)
        v = kv[k]

        if k in _OPAQUE_KEYS:
            lines.append(f"{label}: [present — consistent configuration]")
            continue
        if k == "dim_type.accuracy":
            lines.append(f"{label}: {_fmt_accuracy(v)}")
            continue
        if k == "dim_type.witness_line_control":
            lines.append(f"{label}: {_fmt_witness(v)}")
            continue
        if k == "dim_type.unit_format_id" and v.lower() in ("default", ""):
            continue  # default unit format is not worth surfacing
        lines.append(f"{label}: {v}")

    # Text appearance — emit only non-default values, grouped at end
    text_lines = []
    for k in sorted(_TEXT_APPEARANCE_KEYS):
        if k not in kv or k in emitted:
            continue
        v = kv[k]
        default = _TEXT_DEFAULTS.get(k)
        if default is not None and v.lower().strip() == default:
            continue  # suppress default values
        label = _PARAM_LABELS.get(k, k)
        text_lines.append(f"{label}: {v}  [text appearance — secondary]")

    if text_lines:
        lines.append("")
        lines.extend(text_lines)

    # Any remaining keys not in priority_order and not text appearance
    for k, v in sorted(kv.items()):
        if k in emitted or k in _TEXT_APPEARANCE_KEYS or k in _SKIP_KEYS or k in _OPAQUE_KEYS:
            continue
        label = _PARAM_LABELS.get(k, k)
        lines.append(f"{label}: {v}")

    return lines