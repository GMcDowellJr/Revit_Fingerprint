"""
tools/label_synthesis/domain_prompts/text_types.py

LLM system prompt and prompt builder for text_types pattern name synthesis.

Invoke via synthesize_fragmented_labels.py, not at emit time.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming text type configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

DOMAIN CONTEXT — REVIT TEXT TYPES
===================================
A Text Type in Revit controls the appearance of annotation text notes on drawings.
Every annotation text element is governed by a Text Type. Projects inherit these from
templates, but teams routinely rename or duplicate them — which is why the same
underlying configuration often carries different names across projects.

You are seeing a configuration that multiple project teams implemented identically
but named differently. Your job is to produce the canonical name those teams
could have or might have used — not to prescribe what they should have used.
Name is excluded from the join key; fragmentation is purely a naming problem.

KEY PARAMETERS you will see:

text_type.font
  The typeface name. Common values: Arial, Romans, Calibri, Verdana, RomanS, Tahoma.
  Font is a strong identity signal — Arial vs Romans is governance-significant.
  Include font in the name only when the corpus contains multiple font families;
  if one font is universal, omit it.

text_type.size_in
  Text height in inches (annotation space). All types in this domain are annotation
  space — there are no model-space text types here.
  Common sizes and their typical annotation roles:
    3/64" (0.046875")  — very small, reference tags, room schedule callouts
    1/16" (0.0625")    — small reference, keynote tags
    3/32" (0.09375")   — standard body notes, most common annotation size
    1/8"  (0.125")     — secondary body or small heading
    5/32" (0.15625")   — medium heading
    3/16" (0.1875")    — large heading, room names
    1/4"  (0.25")      — title block text, major headings
  Size is important but not always in the name — include it when multiple patterns
  share the same role at different sizes.

text_type.bold
  true/false — bold is the primary discriminator between heading and body roles.
  A bold 1/8" type and a regular 1/8" type serve different governance roles.

text_type.italic
  true/false — italic is relatively rare; include "Italic" in the name when true.

text_type.show_border
  true/false — bordered text = callout box or keynote balloon style.
  When true, include "Bordered", "Box", or "Callout" in the canonical name.

text_type.color_rgb
  RGB values as "R-G-B", e.g. "0-0-0" (black), "255-0-0" (red).
  Non-black = special annotation role: red = demo/revision, cyan/blue = coordination.
  Include a color descriptor in the name when color is non-black (not the RGB values).

text_type.background
  0 = transparent (default), 1 = opaque.
  Opaque masks elements behind the text — only relevant when non-default.

text_type.width_factor
  Font width scaling. Default 1.0.
  Non-default values (e.g. 0.8 condensed) are secondary — note in rationale but
  rarely drive the canonical name.

text_type.tab_size_in / text_type.leader_border_offset_in
  Secondary parameters. Usually at default values. When non-default they may help
  distinguish patterns but rarely drive the name.

text_type.leader_arrowhead_sig_hash
  Opaque hash of the leader arrowhead configuration. Consistent presence indicates
  a consistent arrowhead style — treat as context, not a naming driver.

NAMING CONVENTIONS at engineering firms
========================================
Role-based (preferred when the role is clear):
  "Notes", "Body Notes", "General Notes"
  "Heading", "Sub-Heading", "Title"
  "Room Tag Text", "Room Name"
  "Revision Notes", "Coordination Text"
  "Keynote Tag", "Note Block"

Size-and-font-based (when role is ambiguous):
  "Arial 3/32", "3/32 Text", "Romans 1/8"

Combined (common in large firms):
  "Notes 3/32", "Heading 3/16 Bold"

NAMING RULES
============
1. Prefer role-based names. Include size when multiple patterns share the same role.
2. Include font only when the corpus contains multiple font families.
3. Bold = heading role; include "Bold" or a heading descriptor — not both.
4. Bordered/show_border = callout role; reflect it in the name.
5. Non-black color = special role; describe the role (e.g., "Revision Text"), not the RGB value.
6. Keep names under 40 characters for BI slicer legibility.
7. Converge messy variants toward the cleanest canonical form
   (e.g. "TXT-NOTES", "Notes-3/32", "GENL NOTES" → "Notes 3/32").
"""


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

    # --- Observed names ---
    lines.append("OBSERVED NAMES IN THIS CORPUS")
    lines.append("(All these names refer to the same behavioral configuration)")
    if observed_labels:
        total_files = sum(int(r.get("files_count", 0)) for r in observed_labels)
        for row in observed_labels[:10]:
            label = row.get("label_v", "").strip()
            count = int(row.get("files_count", 0))
            pct = (count / total_files * 100) if total_files else 0
            lines.append(f'  "{label}"  ({count} files, {pct:.0f}%)')
    else:
        lines.append("  (no names observed)")
    lines.append("")

    # --- Behavioral parameters ---
    lines.append("BEHAVIORAL PARAMETERS")
    param_lines = _format_identity_items(identity_items)
    if param_lines:
        lines.extend(f"  {l}" for l in param_lines)
    else:
        lines.append("  (no readable parameters)")
    lines.append("")

    # --- Corpus context ---
    if corpus_context:
        total = corpus_context.get("total_files_in_corpus")
        pattern_count = corpus_context.get("domain_pattern_count")
        if total:
            lines.append(f"CORPUS CONTEXT: {total}-file corpus.")
        if pattern_count:
            lines.append(f"There are {pattern_count} distinct text type patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this text type pattern. Names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Reflect the annotation role this configuration serves\n"
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
    "text_type.font":                    "Font",
    "text_type.size_in":                 "Size (inches)",
    "text_type.bold":                    "Bold",
    "text_type.italic":                  "Italic",
    "text_type.underline":               "Underline",
    "text_type.color_rgb":               "Color (R-G-B)",
    "text_type.width_factor":            "Width factor",
    "text_type.show_border":             "Show border",
    "text_type.background":              "Background (0=transparent, 1=opaque)",
    "text_type.line_weight":             "Line weight",
    "text_type.tab_size_in":             "Tab size (inches)",
    "text_type.leader_border_offset_in": "Leader/border offset (inches)",
    "text_type.leader_arrowhead_sig_hash": "Leader arrowhead config (opaque hash)",
}

_OPAQUE_KEYS = {"text_type.leader_arrowhead_sig_hash"}

# Keys to skip entirely — cosmetic/label fields excluded from join key
_SKIP_KEYS = {
    "text_type.name",
    "text_type.type_id",
    "text_type.type_uid",
    "text_type.leader_arrowhead_uid",
    "text_type.leader_arrowhead_name",
    "text_type.color_int",
}

_SIZE_MAP = {
    0.046875: '3/64"', 0.0625: '1/16"', 0.078125: '5/64"',
    0.09375: '3/32"',  0.109375: '7/64"', 0.125: '1/8"',
    0.15625: '5/32"',  0.1875: '3/16"',   0.21875: '7/32"',
    0.25: '1/4"',      0.3125: '5/16"',   0.375: '3/8"',
    0.5: '1/2"',       0.75: '3/4"',      1.0: '1"',
}

_COLOR_NAMES = {
    "0-0-0":       "Black (standard)",
    "255-0-0":     "Red",
    "0-0-255":     "Blue",
    "0-255-255":   "Cyan",
    "255-0-255":   "Magenta",
    "128-128-128": "Gray",
    "0-128-0":     "Dark Green",
}


def _fmt_size(val_str: str) -> str:
    try:
        v = float(val_str)
        for k, label in _SIZE_MAP.items():
            if abs(v - k) < 0.002:
                return label
        return f'{v:.4g}"'
    except (ValueError, TypeError):
        return val_str


def _fmt_color(val_str: str) -> str:
    return _COLOR_NAMES.get(val_str, val_str)


def _format_identity_items(identity_items: List[Dict[str, Any]]) -> List[str]:
    out = []
    for item in identity_items:
        k = str(item.get("k", ""))
        q = str(item.get("q", ""))
        v = item.get("v")

        if k in _SKIP_KEYS:
            continue
        if q != "ok" or v is None:
            continue

        label = _PARAM_LABELS.get(k, k)

        if k in _OPAQUE_KEYS:
            out.append(f"{label}: [present — consistent configuration]")
            continue

        # Human-readable value formatting
        if k == "text_type.size_in":
            display = _fmt_size(str(v))
        elif k == "text_type.color_rgb":
            display = _fmt_color(str(v))
        elif k == "text_type.background":
            display = "transparent" if str(v) == "0" else "opaque"
        else:
            display = str(v)

        out.append(f"{label}: {display}")

    return out
