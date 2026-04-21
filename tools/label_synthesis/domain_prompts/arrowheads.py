"""
tools/label_synthesis/domain_prompts/arrowheads.py

LLM system prompt and prompt builder for arrowheads pattern name synthesis.

Invoke via synthesize_fragmented_labels.py, not at emit time.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming arrowhead type configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

# DOMAIN CONTEXT — REVIT ARROWHEAD TYPES

Arrowhead types in Revit are shared terminator symbols used across dimension types,
annotation leaders, and detail components. The same configuration may be referenced
by dozens of dimension types across a project. Projects inherit these from templates,
but teams often rename or duplicate them — which is why the same underlying
configuration often carries different names across projects.

You are seeing a configuration that multiple project teams implemented identically
but named differently. Your job is to produce the canonical name those teams
could have or might have used — not to prescribe what they should have used.
Name is excluded from the join key; fragmentation is purely a naming problem.

# RECORD CLASSES — THREE TYPES

Arrowheads fall into three record classes based on their style. The identity items
you see depend entirely on the record class. Sparse items are NOT missing data —
they are the complete and correct identity for that class.

CLASS 1 — Arrow
  Style: "Arrow"
  Identity items: style + tick_size_in + width_angle_deg + fill_tick + arrow_closed
  These are true geometric arrows. Naming is style-first, then fill state and angle
  when they differentiate.

CLASS 2 — Heavy end tick mark
  Style: "Heavy end tick mark"
  Identity items: style + tick_size_in + tick_mark_centered + heavy_end_pen_weight
  A bold diagonal tick used in structural dimension conventions.
  Naming: "Tick", "Heavy Tick", "Structural Tick", or with size (e.g. "Tick 1/8").

CLASS 3 — SizeOnly
  Styles: "Dot", "Diagonal", "Box", "Loop", "Elevation Target", "Datum triangle"
  Identity items: style + tick_size_in ONLY — no other fields are applicable.
  These styles have no fill/angle/centered properties in Revit's UI.
  The join key for each is fully defined by style + size. Two Dots at different sizes
  are distinct patterns; two Dots at the same size with different names are the same
  governance pattern. Name by style + size.

# KEY PARAMETERS

arrowhead.style
  The style enum display string. This is the primary classifier — always use it
  to anchor the name.
  Values: Arrow, Heavy end tick mark, Dot, Diagonal, Box, Loop,
          Elevation Target, Datum triangle

arrowhead.tick_size_in
  Physical size in inches. Common values:
    1/16" — small, detail work, tight spaces
    3/32" — standard small
    1/8"  — standard, most common
    3/16" — medium
    1/4"  — large
  Include size in the name when multiple sizes of the same style exist in the corpus.

arrowhead.width_angle_deg  [Arrow class only]
  The opening angle of the arrowhead in degrees. Common values: 15°, 30°, 45°, 60°.
  Narrow angles (15°–20°) are slender/precise; wide angles (45°+) are bold.
  Include angle only when it differentiates patterns in the corpus.

arrowhead.fill_tick  [Arrow class only]
  true = filled (solid) arrow, false = open arrow.
  This is a primary discriminator for arrow class: "Filled Arrow" vs "Open Arrow".

arrowhead.arrow_closed  [Arrow class only]
  true = closed outline, false = open sides.
  Usually secondary to fill_tick.

arrowhead.tick_mark_centered  [Heavy end tick mark class only]
  true = centered on element, false = offset.

arrowhead.heavy_end_pen_weight  [Heavy end tick mark class only]
  Integer pen weight for the thick end of the tick.

# NAMING CONVENTIONS AT ENGINEERING FIRMS

Arrow class:
  "Filled Arrow", "Arrow Filled", "Solid Arrow"
  "Open Arrow", "Arrow Open"
  With size: "Filled Arrow 1/8", "Open Arrow 3/32"

Heavy end tick mark class:
  "Tick", "Heavy Tick", "Structural Tick"
  With size: "Tick 1/8", "Heavy Tick 3/16"

SizeOnly class (Dot, Diagonal, Box, etc.):
  Style name alone: "Dot", "Diagonal", "Box", "Loop"
  With size when differentiation needed: "Dot 1/8", "Diagonal 3/32"
  Special cases: "Elevation Target", "Datum Triangle" (named by role)

# CLUSTERING LOGIC

First cluster observed names by fuzzy naming intent.

A fuzzy naming cluster groups labels that share the same core intent after normalizing:

* punctuation and spacing differences
* capitalization differences
* firm-specific prefixes and abbreviations
* minor wording variants (Arrow Filled vs Filled Arrow vs Solid Arrow)

Identify the core intent term first — style class is always the anchor:

* Arrow class core: fill state (Filled/Open) + style
* Tick class core: Tick or Heavy Tick
* SizeOnly class core: style name

If observed names contain multiple materially different fuzzy naming groups
(e.g., some labels say "Filled Arrow" and others say "Open Arrow" for what appears
to be the same pattern), do not collapse them. Emit one canonical name per
meaningful cluster.

# SPARSE-EVIDENCE RULE

When observed labels are few or weak (1–2 labels), prefer merging by shared core
naming intent rather than splitting on qualifiers.

Treat these as weak qualifiers by default:

* size descriptors (SM, Small, LG, Large) when only one size exists in corpus
* angle descriptors when only one angle exists in corpus
* firm-specific prefixes (STN-, A-, DIM-)
* punctuation/spacing/capitalization variants

Do not create separate clusters from weak qualifiers alone when the core style
intent is shared. In sparse cases, optimize for consolidation over fragmentation.

# CANONICAL NAMING RULES

For each cluster, synthesize the shortest clear canonical label.

Normalization rules:

1. Always lead with the style class (Arrow, Tick, Dot, Diagonal, etc.)
2. For Arrow class: fill state (Filled/Open) is the primary qualifier after style
3. Strip firm-specific prefixes and normalize capitalization
4. Include size only when multiple sizes of the same style exist in corpus
5. Include angle only when it is a real differentiator in corpus
6. Remove redundant words that restate the style (e.g., "Arrow" in "Arrow Arrow Filled")
7. Keep names under 40 characters for BI slicer legibility

Examples:

* "Arrow Filled", "ARROW-SM", "Standard Arrow", "Filled" → Filled Arrow
* "Arrow-SM 1/8", "Filled Arrow Small", "Arrowhead F" → Filled Arrow 1/8
* "Open Arrow", "Arrow Open", "ARROWOPEN" → Open Arrow
* "Tick Mark", "Heavy Tick", "TICK-STD" → Tick (or Heavy Tick if pen weight is heavy)
* "Dot", "DOT-STD", "Dot Mark 1/8" → Dot 1/8

# GEOMETRY FALLBACK

If observed names are all opaque fallbacks or firm codes with no readable style
intent, infer from the behavioral parameters (style enum + size) and set confidence
low in rationale.

# YOUR TASK

Suggest 2-3 canonical names for this arrowhead pattern. The names should:

* Be recognizable to a Revit standards manager
* Lead with the style type
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

def build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines = []

    # Determine record class from identity items for context
    record_class = _detect_record_class(identity_items)

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

    # --- Record class note ---
    if record_class == "SizeOnly":
        lines.append(
            "NOTE: This is a SizeOnly arrowhead style (Dot, Diagonal, Box, Loop, etc.).\n"
            "Only style and size appear as identity items — this is complete and correct,\n"
            "not missing data. No fill/angle/centered fields apply to this style."
        )
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
            lines.append(f"There are {pattern_count} distinct arrowhead patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this arrowhead pattern. Names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Lead with the style type\n"
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
    "arrowhead.style":              "Style",
    "arrowhead.tick_size_in":       "Size (inches)",
    "arrowhead.width_angle_deg":    "Arrow width angle (degrees) [Arrow class]",
    "arrowhead.fill_tick":          "Filled [Arrow class]",
    "arrowhead.arrow_closed":       "Closed outline [Arrow class]",
    "arrowhead.tick_mark_centered": "Centered [Heavy tick class]",
    "arrowhead.heavy_end_pen_weight": "Heavy end pen weight [Heavy tick class]",
}

_SKIP_KEYS = {
    "arrowhead.name",
    "arrowhead.type_id",
    "arrowhead.arrow_style_raw_int",
    "arrowhead.arrow_style_display",
    "arrowhead.source_element_id",
    "arrowhead.source_unique_id",
}

_SIZE_MAP = {
    1/64: '1/64"', 1/32: '1/32"', 1/16: '1/16"', 3/32: '3/32"',
    1/8: '1/8"',   3/16: '3/16"', 1/4: '1/4"',   3/8: '3/8"',
    1/2: '1/2"',   3/4: '3/4"',   1.0: '1"',
}

_SIZEONLY_STYLES = frozenset({
    "Dot", "Diagonal", "Box", "Loop", "Elevation Target", "Datum triangle"
})


def _detect_record_class(identity_items: List[Dict[str, Any]]) -> str:
    kv = {
        item.get("k"): item.get("v")
        for item in identity_items
        if item.get("q") == "ok"
    }
    style = kv.get("arrowhead.style", "")
    if style == "Arrow":
        return "Arrow"
    if style == "Heavy end tick mark":
        return "Tick"
    if style in _SIZEONLY_STYLES:
        return "SizeOnly"
    return "Unknown"


def _fmt_size(val_str: str) -> str:
    try:
        v = float(val_str)
        for k, label in _SIZE_MAP.items():
            if abs(v - k) < 1e-5:
                return label
        return f'{v:.4g}"'
    except (ValueError, TypeError):
        return val_str


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

        if k == "arrowhead.tick_size_in":
            display = _fmt_size(str(v))
        elif k == "arrowhead.width_angle_deg":
            try:
                display = f"{float(v):.0f}°"
            except (ValueError, TypeError):
                display = str(v)
        else:
            display = str(v)

        out.append(f"{label}: {display}")

    return out
