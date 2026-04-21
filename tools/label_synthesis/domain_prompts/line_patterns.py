"""
tools/label_synthesis/domain_prompts/line_patterns.py

LLM system prompt and prompt builder for line_patterns pattern name synthesis.

Invoke via synthesize_fragmented_labels.py, not at emit time.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming line pattern configuration patterns \
for use in a cross-project standards analytics dashboard at a large engineering firm.

DOMAIN CONTEXT — REVIT LINE PATTERNS
======================================
A Line Pattern in Revit defines the dash/dot/space rhythm of a line — its segment
structure. Line patterns are referenced by Line Styles and Object Styles, which add
pen weight and color on top. The pattern itself is pure geometry: segment types and
relative proportions.

Projects inherit line patterns from templates, but teams often duplicate or rename
them — which is why the same structural pattern often carries different names across
projects.

You are seeing a configuration that multiple project teams implemented identically
but named differently. Your job is to produce the canonical name those teams
could have or might have used — not to prescribe what they should have used.
Name is excluded from the join key; fragmentation is purely a naming problem.

SCALE INVARIANCE — IMPORTANT
==============================
The join key for line patterns uses a scale-normalised hash (segments_norm_hash)
that collapses patterns differing only in absolute dash/dot lengths into the same
pattern. This means the observed names on a single join_hash may describe
geometrically similar but not pixel-identical patterns — for example, a "Hidden"
pattern at 3/32" dash spacing and a "Hidden" pattern at 1/8" dash spacing will
hash the same. "Hidden" is still the correct canonical name even if the names
show slight variation. Focus on the structural type (the rhythm), not absolute scale.

IMPORT-DERIVED PATTERNS
========================
Some line patterns originate from CAD/DWG imports. These are identified by names
starting with "IMPORT-" or similar. When you see such names:
- Strip the "IMPORT-" prefix and treat the remainder as a normal observed name.
  E.g. "IMPORT-Hidden" → treat as "Hidden".
- If the remaining name still carries no semantic meaning (numeric codes, layer IDs,
  garbled strings like "A_354782" or "XREF-23-B"), assign the recommended name as
  "unresolved-import" and set confidence low in your rationale.

KEY PARAMETERS you will see:

line_pattern.segment_count
  Total number of segments (dashes + spaces + dots combined).
  A single-segment pattern is a solid line (no actual dashes).
  Common structures:
    2 segments: Dash-Space → simple hidden line
    4 segments: Dash-Space-Dash-Space → hidden line variant
    4 segments: Dash-Space-Dot-Space → centerline
    6 segments: Dash-Space-Dot-Space-Dot-Space → phantom line
    4 segments: Dot-Space-Dot-Space → dot pattern

line_pattern.seg[NNN].kind
  Segment type: 0=Dash, 1=Space, 2=Dot
  Segments are ordered — the sequence defines the pattern's visual rhythm.

line_pattern.seg[NNN].length
  Segment length in inches (absolute, not normalised). Dots report 0.0.
  Because scale is normalised for identity, focus on the kind sequence, not lengths.

line_pattern.segments_norm_hash / line_pattern.segments_def_hash
  Opaque hashes. Present for reference only — do not try to interpret them.

line_pattern.lp.is_import
  "true" if the pattern is likely import-derived (name-based heuristic).
  When true, apply the import name stripping guidance above.

STANDARD PATTERN FAMILIES
===========================
  Solid (0 or 1 segments, no dashes)
  Hidden line (Dash-Space or Dash-Space-Dash-Space): dashed, below-cut elements
  Centerline (Dash-Space-Dot-Space or Dash-Dot-Space): grid lines, centerlines
  Phantom (Dash-Space-Dot-Space-Dot-Space): long dash with two dots
  Dot pattern (Dot-Space sequences): property lines, setback lines
  Custom/complex (6+ varied segments): specialty or import-derived

NAMING CONVENTIONS at engineering firms
========================================
  "Hidden", "Hidden Lines", "Dashed"
  "Center", "Centerline", "CL"
  "Dash Dot", "Dash-Dot", "Dash Dot Dot"
  "Phantom", "Long Dash Short Dash"
  "Dot", "Dotted", "Property Line"
  "Overhead", "Beyond"  ← role-based names for specific conventions
  Revit built-in names are canonical and should be preserved when observed:
    "Hidden", "Center", "Dash dot", "Overhead"

NAMING RULES
============
1. Name by structural type (the rhythm), not by absolute scale.
2. Prefer observed names when they are semantic (not garbled/numeric).
3. Preserve Revit built-in canonical names when present in observed labels.
4. For import-derived patterns with semantic names: strip prefix, use remainder.
5. For import-derived patterns with opaque names: recommend "unresolved-import".
6. Keep names under 40 characters for BI slicer legibility.
"""


# ---------------------------------------------------------------------------
# User prompt builder
# ---------------------------------------------------------------------------

_IMPORT_PREFIXES = ("IMPORT-", "IMPORT ", "IMPORT_")
_OPAQUE_NAME_RE = re.compile(r'^[A-Z0-9_\-\.]{1,20}$')  # all-caps/numeric, no spaces


def _strip_import_prefix(name: str) -> str:
    upper = name.upper()
    for prefix in _IMPORT_PREFIXES:
        if upper.startswith(prefix):
            return name[len(prefix):].strip()
    return name


def _is_opaque_name(name: str) -> bool:
    """Heuristic: name is opaque if it's all-caps/numeric with no spaces and
    doesn't match any known pattern family keyword."""
    _SEMANTIC_WORDS = {
        "hidden", "center", "dash", "dot", "dashed", "centerline",
        "phantom", "overhead", "beyond", "solid", "property", "line",
        "long", "short", "space",
    }
    stripped = _strip_import_prefix(name)
    words = re.split(r'[\s\-_]+', stripped.lower())
    return not any(w in _SEMANTIC_WORDS for w in words if w)


def build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines = []

    # Pre-process observed labels: strip import prefixes, flag opaque names
    processed_labels = []
    has_import = False
    has_opaque = False
    for row in observed_labels:
        raw = row.get("label_v", "").strip()
        stripped = _strip_import_prefix(raw)
        is_import = stripped != raw
        is_opaque = _is_opaque_name(raw)
        if is_import:
            has_import = True
        if is_opaque:
            has_opaque = True
        processed_labels.append({
            "raw": raw,
            "display": stripped if is_import else raw,
            "is_import": is_import,
            "is_opaque": is_opaque,
            "files_count": row.get("files_count", 0),
        })

    # --- Observed names ---
    lines.append("OBSERVED NAMES IN THIS CORPUS")
    lines.append("(All these names refer to the same structural pattern — possibly at different scales)")
    if processed_labels:
        total_files = sum(int(r.get("files_count", 0)) for r in processed_labels)
        for row in processed_labels[:10]:
            label = row["display"]
            raw = row["raw"]
            count = int(row.get("files_count", 0))
            pct = (count / total_files * 100) if total_files else 0
            suffix = ""
            if row["is_import"]:
                suffix = "  [import-derived, prefix stripped]"
            elif row["is_opaque"]:
                suffix = "  [opaque/garbled name]"
            if label != raw:
                lines.append(f'  "{label}"  (was: "{raw}", {count} files, {pct:.0f}%){suffix}')
            else:
                lines.append(f'  "{label}"  ({count} files, {pct:.0f}%){suffix}')
    else:
        lines.append("  (no names observed)")
    lines.append("")

    # --- Import/opaque warnings ---
    if has_opaque:
        lines.append(
            "WARNING: Some observed names appear opaque or garbled (numeric codes, "
            "CAD layer IDs). If no semantic name is available after stripping, "
            'use "unresolved-import" as the recommended name.'
        )
        lines.append("")

    # --- Behavioral parameters ---
    lines.append("BEHAVIORAL PARAMETERS (segment structure)")
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
            lines.append(f"There are {pattern_count} distinct line pattern patterns total.")
        lines.append("")

    # --- Task ---
    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this line pattern. Names should:\n"
        "  - Describe the structural rhythm (Dash-Dot, Hidden, Center, etc.)\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Be short enough for a Power BI slicer (under 40 characters)\n"
        "  - Prefer Revit built-in canonical names when observed\n"
        '  - Use "unresolved-import" for truly opaque/garbled import names'
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

_SEG_KEY_RE = re.compile(r'^line_pattern\.seg\[(\d+)\]\.(kind|length)$')
_KIND_NAME = {0: "Dash", 1: "Space", 2: "Dot"}

_SKIP_KEYS = {
    "line_pattern.uid",
    "line_pattern.name",
    "line_pattern.element_id",
    "line_pattern.source_element_id",
    "line_pattern.source_unique_id",
    "line_pattern.segments_def_hash",
    "line_pattern.segments_norm_hash",
}


def _format_identity_items(identity_items: List[Dict[str, Any]]) -> List[str]:
    kv = {}
    for item in identity_items:
        k = str(item.get("k", ""))
        q = str(item.get("q", ""))
        v = item.get("v")
        if q == "ok" and v is not None and k not in _SKIP_KEYS:
            kv[k] = v

    out = []

    # Segment count first
    seg_count = kv.get("line_pattern.segment_count")
    if seg_count is not None:
        out.append(f"Segment count: {seg_count}")

    # Parse per-segment items
    segments: Dict[int, Dict[str, Any]] = {}
    for k, v in kv.items():
        m = _SEG_KEY_RE.match(k)
        if not m:
            continue
        idx = int(m.group(1))
        field = m.group(2)
        segments.setdefault(idx, {})
        try:
            if field == "kind":
                segments[idx]["kind"] = int(v)
            elif field == "length":
                segments[idx]["length"] = float(v)
        except (ValueError, TypeError):
            pass

    if segments:
        ordered = sorted(segments.items())
        kind_seq = []
        for _, seg_data in ordered:
            kind_id = seg_data.get("kind")
            if kind_id is not None:
                kind_seq.append(_KIND_NAME.get(kind_id, f"k{kind_id}"))

        if kind_seq:
            out.append(f"Segment sequence: {'-'.join(kind_seq)}")

        # Show representative lengths for dashes (not spaces/dots — dots are always 0)
        dash_lengths = []
        for _, seg_data in ordered:
            if seg_data.get("kind") == 0:  # Dash
                length = seg_data.get("length")
                if length is not None and length > 0:
                    dash_lengths.append(f"{length:.4f}\"")
        if dash_lengths:
            out.append(f"Dash lengths (absolute, scale may vary): {', '.join(dash_lengths[:3])}")

    # is_import flag
    is_import = kv.get("lp.is_import")
    if is_import == "true":
        out.append("Import-derived: yes (name-based detection)")

    return out
