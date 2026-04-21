"""
tools/label_synthesis/domain_prompts/fill_patterns.py

LLM system prompt and prompt builder for fill pattern name synthesis.

Handles both fill_patterns_drafting and fill_patterns_model domains.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


SYSTEM_PROMPT = """\
You are a Revit standards specialist naming fill pattern configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

# DOMAIN CONTEXT — REVIT FILL PATTERNS

Fill patterns are hatch graphics applied to cut sections and surfaces of elements.
Revit separates them by target type because they are governed at different scales:

* Drafting patterns are defined in sheet/paper space units and are view-scale independent
* Model patterns are defined in model space units and scale with the view

Target context is resolved at call time and applied downstream. Do not append \
"(Drafting)" or "(Model)" to output names.

# THREE VALID NAMING CONVENTIONS

All of these are valid naming styles. Preserve the observed naming intent.

1. Material-based names
   Examples: Concrete, Earth, Sand, Gravel, Steel, Wood, Brick,
   AR-CONC, AR-BRSTD, AR-SAND, AR-HBONE,
   ANSI31, ANSI32, ANSI33, ANSI37,
   Concrete Block, CMU, Batt Insulation, Rigid Insulation,
   Concrete Small, Concrete Dense, Sand 2mm,
   Concrete (Cut), Concrete (Surface)

2. Geometry-based names
   Examples: Diagonal, Diagonal Lines, 45 Degree Lines,
   Crosshatch, Cross Hatch, Diagonal Crosshatch,
   Horizontal, Vertical, Net, Grid, Dots, Circles

3. Role/application-based names
   Examples: Hidden, Overhead, Beyond

# CRITICAL

* Do not translate names across categories.
* If observed names are geometry-based, converge to a geometry-based canonical name, \
not a material name.
* Do not invent material names from geometry alone.
* Prefer synthesized canonical labels over copying the longest or most common raw \
label verbatim.

# KNOWN NAMING PATTERNS TO RECOGNIZE

* AR- prefix = AutoCAD architectural hatch families
  (AR-CONC=concrete, AR-BRSTD=brick, AR-SAND=sand)
* ANSI prefix = ANSI standard material hatches
  (ANSI31=steel/iron, ANSI37=aluminium)
* Scale suffixes (Small, Dense, 2mm, 4mm) indicate density variant, not a new type
* (Cut) / (Surface) suffixes indicate application context variant
* .dwg in the name usually indicates CAD import artifact; treat as low-signal naming

# CLUSTERING LOGIC

First cluster observed names by fuzzy naming intent.

A fuzzy naming cluster groups labels that share the same core intent after normalizing:

* punctuation and spacing differences
* capitalization differences
* prefixes/suffixes and import artifacts
* minor wording variants
* codes/spec suffixes that are not the core identity

Identify the core intent term first:

* material core (e.g. Sand, Concrete, Gypsum-Plaster, Brick)
* geometry core (e.g. Crosshatch, Diagonal, Grid, Horizontal)
* role/application core only if no stronger material/geometry core exists

If observed names contain multiple materially different fuzzy naming groups,
do not collapse them to one name. Emit one canonical name per meaningful cluster.

# SPARSE-EVIDENCE RULE

When observed labels are few or weak, especially only 1-2 labels, prefer merging \
by shared core naming intent rather than splitting on qualifiers.

Treat these as weak qualifiers by default:

* application/context words (e.g. Valve, Wall, Floor, Ceiling, Finish)
* size/density words (e.g. Small, Extra Small, Dense, Tight, Heavy)
* codes/spec identifiers (e.g. CPT-4, D312000)
* punctuation/spacing/formatting variants

Do not create separate clusters from weak qualifiers alone when the core material \
or geometry term is shared.

Preserve a qualifier in the canonical label or as a separate cluster only when \
there is positive evidence it represents a real distinct standard family, such as:

* multiple labels consistently repeating the same qualifier
* the qualifier materially changes the naming intent
* several files support that distinction, not just a sparse one-off example

In sparse cases, optimize for consolidation over fragmentation.

# CANONICAL NAMING RULES

For each cluster, synthesize the shortest clear canonical label rather than \
copying the most common raw label verbatim.

Normalization rules:

1. Remove redundant target context like "(Drafting)" or "(Model)"
2. Strip firm-specific prefixes and normalize capitalization
3. Remove punctuation noise and formatting artifacts
4. Remove generic category words when core intent remains clear
5. Remove codes/spec suffixes unless needed to distinguish otherwise identical clusters
6. Preserve meaningful material/application distinctions
7. Preserve modifiers like Dense, Small, Tight only when necessary to distinguish \
a real separate cluster
8. Keep names recognizable to a Revit standards manager
9. Keep names short enough for a Power BI slicer, ideally under 40 characters

Examples:

* "Sand - Dense", "D 312000 Sand", "Sand 1/2" -> Sand
* "Gypsum-Plaster", "+Gypsum - Plaster" -> Gypsum-Plaster
* "Finish-Carpet (CPT-4)", "Finish_CPT-4" -> Carpet
* "Valve Crosshatch", "Cross hatch extra small" -> Crosshatch

# GEOMETRY FALLBACK

If observed names are all opaque fallbacks, infer from geometry and use a \
geometry-based name with lower confidence in rationale. Do not infer a material \
from geometry alone.

# YOUR TASK

Suggest 2-3 canonical names for this pattern. The names should:

* Be recognizable to a Revit standards manager
* Reflect observed naming intent first, geometry second
* Be short enough for a Power BI slicer
* Synthesize clean canonical labels from fuzzy naming clusters
* Emit one canonical name per meaningful cluster
* Order clusters by support

# OUTPUT RULE

Return the canonical names as a pipe-delimited string in support order.

Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:
{
  "candidates": ["name1", "name2", "name3"],
  "recommended": "name1 | name2 | name3",
  "rationale": "One sentence explaining why the output reflects the clustered core naming intents."
}
"""


_PARAM_LABELS = {
    "fill_pattern.target": "Target",
    "fill_pattern.grid_count": "Grid count",
    "fill_pattern.grid[N].angle": "Angle (degrees)",
    "fill_pattern.grid[N].offset": "Offset (line spacing)",
    "fill_pattern.grid[N].shift": "Shift (stagger)",
}


def build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines: List[str] = []

    target = _get_identity_value(identity_items, "fill_pattern.target")
    if str(target).strip().lower() == "model":
        domain = "fill_patterns_model"
        scale_text = "Model — model space units, scales with view"
        units_label = "model units"
    else:
        domain = "fill_patterns_drafting"
        scale_text = "Drafting — sheet/paper space units, view-scale independent"
        units_label = "sheet units"

    lines.append(f"DOMAIN: {domain}")
    lines.append(f"SCALE: {scale_text}")
    lines.append("")

    lines.append("OBSERVED NAMES (primary signal — favour these):")
    sorted_labels = sorted(
        observed_labels or [],
        key=lambda r: int(r.get("files_count", 0) or 0),
        reverse=True,
    )
    top_labels = sorted_labels[:8]

    if top_labels:
        for row in top_labels:
            label = str(row.get("label_v", "") or "").strip()
            count = int(row.get("files_count", 0) or 0)
            lines.append(f'  "{label}" ({count} files)')
        if all(_is_opaque_fallback(str(row.get("label_v", "") or "")) for row in top_labels):
            lines.append("  [No meaningful names available — infer from geometry below]")
    else:
        lines.append("  [No meaningful names available — infer from geometry below]")
    lines.append("")

    lines.append("GEOMETRY (use only if names above are insufficient):")
    grid_count_raw = _get_identity_value(identity_items, "fill_pattern.grid_count")
    try:
        grid_count = int(str(grid_count_raw))
    except (TypeError, ValueError):
        grid_count = 0

    lines.append(f"  Grid count: {grid_count if grid_count else 'unknown'}")

    grids = _extract_grid_geometry(identity_items)
    grid_angles: List[float] = []
    for i, grid in enumerate(grids):
        angle_str = grid.get("angle")
        offset_str = grid.get("offset")
        shift_str = grid.get("shift")

        angle_display = "unknown"
        if angle_str is not None:
            try:
                angle_val = float(str(angle_str))
                angle_display = f"{angle_val:.1f}°"
                grid_angles.append(angle_val)
            except (TypeError, ValueError):
                angle_display = str(angle_str)

        parts = [f"angle={angle_display}"]
        if offset_str is not None and str(offset_str).strip() != "":
            parts.append(f"offset={offset_str} {units_label}")

        include_shift = False
        if shift_str is not None and str(shift_str).strip() != "":
            try:
                include_shift = abs(float(str(shift_str))) > 1e-9
            except (TypeError, ValueError):
                include_shift = True
        if include_shift:
            parts.append(f"shift={shift_str}")

        lines.append(f"  Grid {i}: " + ", ".join(parts))

    lines.append(f"  → Structural description: {_infer_geometry_description(grid_count, grid_angles)}")
    lines.append("")

    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this pattern. The names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Reflect observed naming intent first (material/geometry/role)\n"
        "  - Synthesize clean canonical labels from fuzzy naming clusters\n"
        "  - Emit one canonical name per meaningful cluster, ordered by support\n"
        "  - Use geometry as fallback when names are opaque\n"
        "  - Be short enough for a Power BI slicer (under 40 characters ideally)"
    )
    lines.append("")
    lines.append(
        "Return the canonical names as a pipe-delimited string in support order.\n"
        "Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:\n"
        "{\n"
        '  "candidates": ["name1", "name2", "name3"],\n'
        '  "recommended": "name1 | name2 | name3",\n'
        '  "rationale": "One sentence explaining why the output reflects the clustered core naming intents."\n'
        "}"
    )

    return "\n".join(lines)


def _is_opaque_fallback(label_v: str) -> bool:
    label = (label_v or "").strip()
    if not label:
        return True
    patterns = ["join_key.v1", "Variant", ".dwg", "__"]
    return any(p in label for p in patterns)


def _normalise_angle(deg: float) -> float:
    return deg % 180.0


def _is_angle_close(value: float, target: float, tol: float = 5.0) -> bool:
    value_n = _normalise_angle(value)
    target_n = _normalise_angle(target)
    diff = abs(value_n - target_n)
    circular_diff = min(diff, 180.0 - diff)
    return circular_diff <= tol


def _infer_geometry_description(grid_count: int, grid_angles: List[float]) -> str:
    if grid_count == 1:
        if not grid_angles:
            return "1-grid pattern"
        a0 = _normalise_angle(grid_angles[0])
        if _is_angle_close(a0, 0) or _is_angle_close(a0, 180):
            return "1-grid horizontal lines"
        if _is_angle_close(a0, 90):
            return "1-grid vertical lines"
        if _is_angle_close(a0, 45):
            return "1-grid diagonal lines (45°)"
        if _is_angle_close(a0, -45) or _is_angle_close(a0, 135):
            return "1-grid diagonal lines (-45°)"
        return f"1-grid lines at {a0:.1f}°"

    if grid_count == 2:
        if len(grid_angles) < 2:
            return "2-grid pattern"
        a0 = _normalise_angle(grid_angles[0])
        a1 = _normalise_angle(grid_angles[1])
        pair = [a0, a1]

        diag_pair = (
            (_is_angle_close(pair[0], 45) and (_is_angle_close(pair[1], 135) or _is_angle_close(pair[1], -45)))
            or (_is_angle_close(pair[1], 45) and (_is_angle_close(pair[0], 135) or _is_angle_close(pair[0], -45)))
        )
        if diag_pair:
            return "2-grid crosshatch (opposing diagonals)"

        hv_pair = (
            (_is_angle_close(pair[0], 0) and _is_angle_close(pair[1], 90))
            or (_is_angle_close(pair[1], 0) and _is_angle_close(pair[0], 90))
        )
        if hv_pair:
            return "2-grid net (horizontal + vertical)"

        return f"2-grid pattern ({a0:.1f}° + {a1:.1f}°)"

    if grid_count == 3:
        return "3-grid complex pattern"

    if grid_count > 3:
        return f"complex pattern ({grid_count} grids)"

    return f"complex pattern ({grid_count if grid_count else 'unknown'} grids)"


def _extract_grid_geometry(identity_items: List[Dict[str, Any]]) -> List[Dict[str, Optional[str]]]:
    grouped: Dict[int, Dict[str, Optional[str]]] = {}
    pattern = re.compile(r"^fill_pattern\.grid\[(\d+)\]\.(angle|offset|shift)$")

    for item in identity_items:
        if not isinstance(item, dict):
            continue
        if item.get("q", "ok") != "ok":
            continue

        k = str(item.get("k", "") or "")
        m = pattern.match(k)
        if not m:
            continue

        idx = int(m.group(1))
        field = m.group(2)
        grouped.setdefault(idx, {"angle": None, "offset": None, "shift": None})

        v = item.get("v")
        grouped[idx][field] = None if v is None else str(v)

    return [grouped[i] for i in sorted(grouped.keys())]


def _get_identity_value(identity_items: List[Dict[str, Any]], key: str) -> Optional[str]:
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        if item.get("k") != key:
            continue
        if item.get("q", "ok") != "ok":
            continue
        val = item.get("v")
        return None if val is None else str(val)
    return None