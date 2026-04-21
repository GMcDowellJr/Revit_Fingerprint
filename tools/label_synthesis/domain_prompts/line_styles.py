"""
tools/label_synthesis/domain_prompts/line_styles.py

LLM system prompt and prompt builder for line_styles pattern name synthesis.

Invoke via synthesize_fragmented_labels.py, not at emit time.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming line style configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

# DOMAIN CONTEXT — REVIT LINE STYLES

A Line Style in Revit combines a pen weight, a line color, and a line pattern
reference. Line styles are applied to model elements via object styles and directly
to detail/drafting lines. They appear as subcategories under the "Lines" category
in Revit's Object Styles dialog.

Projects inherit line styles from templates, but teams often rename or duplicate
them — which is why the same underlying configuration often carries different names
across projects.

You are seeing a configuration that multiple project teams implemented identically
but named differently. Your job is to produce the canonical name those teams
could have or might have used — not to prescribe what they should have used.
Name is excluded from the join key; fragmentation is purely a naming problem.

# KEY PARAMETERS

line_style.weight.projection
  Integer pen weight (line weight index). Common values and their approximate roles:
    1  — hairline, very fine detail lines
    2  — fine lines, typical annotation, leaders
    3  — light, general linework
    4  — medium-light
    5  — medium, visible edges
    6  — medium-heavy
    7  — heavy, section cuts, major boundaries
    8+ — very heavy, major structural elements

line_style.color.rgb
  Line color as "R-G-B", e.g. "0-0-0" (black).
  Non-black color is a strong semantic signal:
    Red (255-0-0):         demolished elements, revision markup
    Gray (128-128-128):    existing/existing-to-remain
    Cyan (0-255-255):      coordination, clash detection
    Blue (0-0-255):        discipline-specific (often MEP or structural)
    Yellow (255-255-0):    specialty, coordination
  Include a color descriptor in the name when color is non-black.

line_style.pattern_ref.kind
  "Solid" — the line is continuous (no dashes).
  "ref"   — the line references a non-solid pattern (dashed, dotted, dash-dot, etc.),
            but the specific pattern geometry is not available here. Use the observed
            names' pattern descriptor words (Hidden, Dashed, Center, Overhead) as the
            source of pattern character.

line_style.pattern_ref.sig_hash
  Opaque hash of the referenced line pattern. Cannot be interpreted directly.
  When kind="ref", rely on observed names to identify the pattern type.

line_style.path
  Subcategory path, e.g. "Lines|Thin Lines" or "Lines|Hidden".
  The "Lines|" prefix is the parent category and should be stripped — the meaningful
  part is the subcategory name after the pipe.
  "Lines|<self>" or where the subcategory mirrors the parent = the parent-level row.

# NAMING CONVENTIONS AT ENGINEERING FIRMS

Weight-based (common in large firms with pen table systems):
  "LW1", "LW2", "LW3" — pen weight designation
  "LW2 Dashed", "LW3 Hidden" — weight + pattern role

Role-based:
  "Hidden", "Hidden Lines", "Beyond"
  "Center", "Centerline"
  "Overhead", "Overhead Lines"
  "Demolition", "Demo", "Existing"
  "Detail Lines", "Fine Lines", "Medium Lines", "Heavy Lines"

Discipline-prefixed:
  "A-Hidden", "S-Center", "M-Overhead" — discipline + role

Color in name:
  "Red Dashed", "Gray Solid", "Cyan Lines"

# CLUSTERING LOGIC

First cluster observed names by fuzzy naming intent.

A fuzzy naming cluster groups labels that share the same core intent after normalizing:

* punctuation and spacing differences (LW-2-DASH vs LW2 Dashed vs lw2 dashed)
* capitalization differences
* discipline prefixes (A-, S-, M-) when the underlying role is the same
* minor wording variants (Hidden vs Hidden Lines vs Hid)

Identify the core intent term first:

* weight core (LW1, LW2, LW3, Thin, Medium, Heavy)
* role core (Hidden, Center, Overhead, Demo, Demolition, Beyond)
* color core for non-black lines (Red, Gray, Cyan)

Treat discipline prefixes as weak qualifiers unless multiple labels consistently
use the same discipline prefix — in that case preserve it.

If observed names reflect genuinely different drawing conventions (e.g., some say
"Hidden" and others say "Demo" for the same weight+pattern combo), do not collapse
them. Emit one canonical name per meaningful cluster.

# SPARSE-EVIDENCE RULE

When observed labels are few or weak (1–2 labels), prefer merging by shared core
naming intent rather than splitting on qualifiers.

Treat these as weak qualifiers by default:

* discipline prefixes (A-, S-, M-, C-) when only one discipline is evident
* weight number variants (LW2 vs LW-02 vs LineWeight2)
* redundant category words (Lines, Line, Linework) when the core role is clear

Do not create separate clusters from weak qualifiers alone. In sparse cases,
optimize for consolidation over fragmentation.

# CANONICAL NAMING RULES

For each cluster, synthesize the shortest clear canonical label.

Normalization rules:

1. Color is the strongest discriminator — always include color for non-black lines
2. For solid black lines: lead with weight role or drawing role
3. For non-solid black lines: include the pattern character (Hidden, Dashed, Center)
4. Strip redundant category words when the core intent remains clear
5. Use the subcategory path (after stripping "Lines|") as a secondary anchor when
   observed names are messy but the path is clear
6. Keep names under 40 characters for BI slicer legibility

Examples:

* "LW-2-DASH", "lw2 dashed", "LW2-Hidden" → LW2 Hidden
* "A-Hidden", "S-HIDDEN", "Hidden Lines" → Hidden (or preserve discipline if consistent)
* "Red Dashed Lines", "RED-DEMO", "Demolition-Red" → Red Dashed
* "Fine", "FINE LINES", "Fine-01" → Fine Lines

# YOUR TASK

Suggest 2-3 canonical names for this line style pattern. The names should:

* Be recognizable to a Revit standards manager
* Reflect the drawing convention this line style represents
* Synthesize clean canonical labels from fuzzy naming clusters
* Emit one canonical name per meaningful cluster, ordered by support
* Be short enough for a Power BI slicer (under 40 characters)

# OUTPUT RULE

Return the canonical names as a pipe-delimited string in support order.
If only one meaningful cluster exists, recommended may be a single name.

Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:
{
  "candidates": ["name1", "name2", "name3"],
  "recommended": "name1 | name2",
  "rationale": "One sentence explaining why the output reflects the clustered core naming intents."
}
"""


# ---------------------------------------------------------------------------
# User prompt builder
# ---------------------------------------------------------------------------

_PARENT_PREFIX = "Lines|"

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


def _strip_lines_prefix(path: str) -> Optional[str]:
    """Strip 'Lines|' parent prefix; return None for self-referential paths."""
    if not path:
        return None
    stripped = path
    if stripped.startswith(_PARENT_PREFIX):
        stripped = stripped[len(_PARENT_PREFIX):]
    # Self-referential: subcategory name mirrors parent, or literally "<self>"
    lower = stripped.lower().strip()
    if lower in ("lines", "<self>", "self", ""):
        return None
    return stripped


def _fmt_color(val_str: str) -> str:
    name = _COLOR_NAMES.get(val_str)
    if name:
        return name
    # Try to format as hex if not in map
    try:
        parts = val_str.split("-")
        if len(parts) == 3:
            r, g, b = int(parts[0]), int(parts[1]), int(parts[2])
            return f"#{r:02X}{g:02X}{b:02X}"
    except (ValueError, TypeError):
        pass
    return val_str


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
            lines.append(f"There are {pattern_count} distinct line style patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this line style pattern. Names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Reflect the drawing convention this line style represents\n"
        "  - Synthesize clean canonical labels from fuzzy naming clusters\n"
        "  - Emit one canonical name per meaningful cluster, ordered by support\n"
        "  - Be short enough for a Power BI slicer (under 40 characters)"
    )
    lines.append("")
    lines.append(
        "Return the canonical names as a pipe-delimited string in support order.\n"
        "If only one meaningful cluster exists, recommended may be a single name.\n"
        "Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:\n"
        "{\n"
        '  "candidates": ["name1", "name2", "name3"],\n'
        '  "recommended": "name1 | name2",\n'
        '  "rationale": "One sentence explaining why the output reflects the clustered core naming intents."\n'
        "}"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parameter formatter
# ---------------------------------------------------------------------------

_PARAM_LABELS = {
    "line_style.weight.projection":    "Pen weight",
    "line_style.color.rgb":            "Color",
    "line_style.pattern_ref.kind":     "Pattern type",
    "line_style.pattern_ref.sig_hash": "Pattern definition (opaque hash)",
    "line_style.path":                 "Subcategory path",
}

_SKIP_KEYS = {
    "line_style.source_element_id",
    "line_style.source_unique_id",
}

_OPAQUE_KEYS = {"line_style.pattern_ref.sig_hash"}

_WEIGHT_ROLES = {
    1: "LW1 (hairline)",
    2: "LW2 (fine)",
    3: "LW3 (light)",
    4: "LW4 (medium-light)",
    5: "LW5 (medium)",
    6: "LW6 (medium-heavy)",
    7: "LW7 (heavy)",
    8: "LW8 (very heavy)",
}


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
            out.append(f"{label}: [present — pattern geometry not directly readable; use observed names for pattern character]")
            continue

        if k == "line_style.weight.projection":
            try:
                w = int(v)
                display = _WEIGHT_ROLES.get(w, f"LW{w}")
            except (ValueError, TypeError):
                display = str(v)
            out.append(f"{label}: {display}")

        elif k == "line_style.color.rgb":
            color_str = _fmt_color(str(v))
            is_black = str(v) in ("0-0-0", "0,0,0")
            note = "" if is_black else "  ← non-black, indicates special annotation role"
            out.append(f"{label}: {color_str}{note}")

        elif k == "line_style.pattern_ref.kind":
            if str(v) == "Solid":
                out.append(f"{label}: Solid (continuous line — no dashes)")
            else:
                out.append(
                    f"{label}: {v} (non-solid — pattern geometry opaque; "
                    f"use observed names to identify Hidden/Center/Dashed/etc.)"
                )

        elif k == "line_style.path":
            stripped = _strip_lines_prefix(str(v))
            if stripped:
                out.append(f"Subcategory name (path stripped): {stripped}")
            # If self-referential, skip — not useful context

        else:
            out.append(f"{label}: {v}")

    return out
