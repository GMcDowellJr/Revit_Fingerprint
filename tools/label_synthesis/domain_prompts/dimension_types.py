"""
tools/label_synthesis/domain_prompts/dimension_types.py

LLM system prompt and prompt builder for dimension_types pattern name synthesis.

The system prompt is the most important lever for label quality — it gives the
model the domain vocabulary needed to produce names that standards managers will
recognize, rather than generic AI labels.

Invoke via synthesize_fragmented_labels.py, not at emit time.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt — loaded once per synthesis session
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming dimension type configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

DOMAIN CONTEXT — REVIT DIMENSION TYPES
=======================================
A Dimension Type in Revit controls how dimensions look and behave on drawings.
Every dimension on a sheet is governed by a Dimension Type. Projects inherit these
from templates, but teams often customize them — which is why the same underlying
configuration often has different names across projects.

KEY PARAMETERS you will see:

dim_type.shape
  The geometric class of dimension this type is used for.
  Values: Linear, LinearFixed, Radial, Diameter, Angular, ArcLength,
          SpotElevation, SpotCoordinate, SpotSlope
  Shape is the primary classifier — always lead with it in the name.

dim_type.accuracy
  The precision of the dimension readout, stored in decimal inches.
  Common values and their meaning:
    0.125    = 1/8 inch (standard coarse)
    0.0625   = 1/16 inch (medium)
    0.03125  = 1/32 inch (fine, detail work)
    0.25     = 1/4 inch (very coarse, schematic)
    0.001    = 0.001 inch (extremely fine, structural/MEP)
  This is one of the most important discriminators between patterns.

dim_type.witness_line_control   [LINEAR only]
  Controls the gap between the element being dimensioned and the witness line.
  Values: "Gap and Line" (standard), "Gap Only", "No Gap"
  Affects code compliance and drawing legibility in different disciplines.

dim_type.center_marks / dim_type.center_mark_size   [RADIAL/DIAMETER only]
  Whether a center mark cross appears at the center of arcs/circles.
  Important for mechanical and structural drawings.

dim_type.unit_format_id
  The unit system override. "Default" means the project unit setting is used.
  Non-default values indicate an explicit override (e.g., millimeters on an
  imperial project, or decimal feet where fractional inches are the standard).

dim_type.prefix / dim_type.suffix
  Text prepended/appended to every dimension readout.
  E.g., suffix "mm" or "TYP" or prefix "~" (approximately).

dim_type.tick_sig_hash
  A behavioral hash of the arrowhead style. You cannot read this directly,
  but if it is present and consistent, treat it as indicating a consistent
  arrowhead configuration across the pattern.

NAMING CONVENTIONS at engineering firms
========================================
- "Standard" or "Standard Linear" — the primary workhorse dimension type
- "Fine" / "Detail" — high-precision types for detail drawings
- "Coarse" / "Schematic" — low-precision types for design/early phases
- "Radial Standard" / "Radius" — for arcs and circles
- "Angular" / "Angle" — for angular dimensions
- "Spot Elevation" — for spot elevation callouts
- Discipline prefixes like "Structural Linear", "Civil Spot", "Arch Standard"
  appear when firms have discipline-specific templates

NAMING RULES
============
1. Always start with the shape type (Linear, Radial, Angular, Spot Elev, etc.)
2. Include accuracy only when it differentiates — if 1/8" is universal, omit it;
   if multiple accuracies exist in the corpus, always include it
3. Keep names under 40 characters for BI slicer legibility
4. Prefer terms practitioners already use — avoid technical parameter names
5. If observed names are messy but consistent in intent (e.g., "Dim-Standard",
   "DIM_STD", "Standard Dim"), synthesize the canonical form they were trying to use
6. If observed names are from different disciplines or offices, note this in rationale
   and pick the most transferable name
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
    """
    Build the user-turn prompt for LLM name synthesis.

    Args:
        join_hash:       The join_hash being named (for reference only)
        observed_labels: Rows from joinhash_label_population — [{label_v, files_count}, ...]
                         Sorted by files_count desc before passing in.
        identity_items:  List of {k, v, q} identity items from a representative record
        corpus_context:  Optional dict with corpus-level stats, e.g. total_files, domain_pattern_count
    """
    lines = []

    # --- Observed names section ---
    lines.append("OBSERVED NAMES IN THIS CORPUS")
    lines.append("(All these names refer to the same behavioral configuration)")
    if observed_labels:
        total_files = sum(int(r.get("files_count", 0)) for r in observed_labels)
        for row in observed_labels:
            label = row.get("label_v", "").strip()
            count = int(row.get("files_count", 0))
            pct = (count / total_files * 100) if total_files else 0
            lines.append(f'  "{label}"  ({count} files, {pct:.0f}%)')
    else:
        lines.append("  (no names observed — all files used unnamed/default configuration)")
    lines.append("")

    # --- Behavioral parameters section ---
    lines.append("BEHAVIORAL PARAMETERS (what this configuration actually does)")
    kv_lines = _format_identity_items(identity_items)
    if kv_lines:
        lines.extend(f"  {l}" for l in kv_lines)
    else:
        lines.append("  (no readable parameters available)")
    lines.append("")

    # --- Corpus context (optional) ---
    if corpus_context:
        total = corpus_context.get("total_files_in_corpus", None)
        pattern_count = corpus_context.get("domain_pattern_count", None)
        if total:
            lines.append(f"CORPUS CONTEXT: This pattern appears in a {total}-file corpus.")
        if pattern_count:
            lines.append(f"There are {pattern_count} distinct dimension type patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this pattern. The names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Reflect what the configuration actually does (not just what teams called it)\n"
        "  - Be short enough for a Power BI slicer (under 40 characters ideally)\n"
        "  - Prefer the most common observed name if it is appropriate\n"
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
# Parameter formatter — makes identity_items readable in the prompt
# ---------------------------------------------------------------------------

_PARAM_LABELS = {
    "dim_type.shape":                "Shape",
    "dim_type.accuracy":             "Accuracy (decimal inches)",
    "dim_type.unit_format_id":       "Unit format",
    "dim_type.rounding":             "Rounding method",
    "dim_type.prefix":               "Prefix text",
    "dim_type.suffix":               "Suffix text",
    "dim_type.witness_line_control": "Witness line control [linear only]",
    "dim_type.center_marks":         "Center marks enabled [radial only]",
    "dim_type.center_mark_size":     "Center mark size inches [radial only]",
    "dim_type.tick_sig_hash":        "Arrowhead config hash (consistent arrowhead across pattern)",
}

_SKIP_KEYS = {
    # Opaque hashes — mention presence but not value
}

_OPAQUE_KEYS = {"dim_type.tick_sig_hash"}


def _format_identity_items(identity_items: List[Dict[str, Any]]) -> List[str]:
    """Convert identity_items to readable parameter lines for the prompt."""
    lines = []
    seen_keys = set()
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        k = item.get("k", "")
        q = item.get("q", "ok")
        v = item.get("v", None)
        if not k or k in seen_keys:
            continue
        seen_keys.add(k)
        if q != "ok":
            continue

        label = _PARAM_LABELS.get(k, k)

        if k in _OPAQUE_KEYS:
            lines.append(f"{label}: [consistent across pattern]")
            continue

        if v is None or str(v).strip() in ("", "__missing__", "__na__", "__not_applicable__"):
            continue

        # Special formatting for accuracy — add fraction equivalent
        if k == "dim_type.accuracy":
            frac = _inches_to_fraction(v)
            display = f"{v} ({frac})" if frac else str(v)
            lines.append(f"{label}: {display}")
            continue

        lines.append(f"{label}: {v}")

    return lines


def _inches_to_fraction(val_str: str) -> Optional[str]:
    """Convert decimal inches string to fraction label for display in prompt."""
    try:
        val = float(val_str)
    except (ValueError, TypeError):
        return None
    _MAP = {
        1/64: "1/64\"", 1/32: "1/32\"", 1/16: "1/16\"",
        1/8:  "1/8\"",  3/16: "3/16\"", 1/4:  "1/4\"",
        3/8:  "3/8\"",  1/2:  "1/2\"",  3/4:  "3/4\"",
        1.0:  "1\"",    2.0:  "2\"",
    }
    for k, label in _MAP.items():
        if abs(val - k) < 1e-6:
            return label
    return None
