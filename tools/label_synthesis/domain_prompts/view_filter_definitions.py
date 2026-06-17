# -*- coding: utf-8 -*-
"""
tools/label_synthesis/domain_prompts/view_filter_definitions.py

LLM system prompt and prompt builder for view_filter_definitions name synthesis.

VFD synthesis strategy:
  1. Filter name is the primary signal — most filters have descriptive names
     encoding discipline, element type, and condition. Names come from
     observed_labels (the label population), NOT from identity_items —
     the view_filter_definitions extractor does not include the filter
     name in identity_basis.items (name is excluded from the hashable
     identity surface).
  2. Rule structure (categories, parameter conditions, operators), read from
     identity_items, is secondary confirmation — used to disambiguate or to
     fall back on when names are opaque.
  3. Group by: discipline prefix + element type + condition theme.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Revit standards specialist naming view filter definitions \
for use in a cross-project standards analytics dashboard at a large engineering firm.

# DOMAIN CONTEXT — REVIT VIEW FILTER DEFINITIONS (ParameterFilterElement)

A View Filter Definition is a reusable rule set (category scope + parameter
conditions) that view filters apply to control visibility and graphic
overrides. Filters are named by BIM authors to describe their purpose, and
that name is almost always self-describing.

Names typically follow a convention:
  [Discipline] - [Element Type] - [Condition]
Examples:
  "Structural - Walls - by Phase"
  "MEP - Mechanical - Hidden"
  "Arch - Rooms - Workset Visibility"

# PRIMARY SIGNAL: THE FILTER NAME

Treat the observed filter name as the primary signal. The canonical label is
a normalized form of the most common name across projects, with consistent
discipline abbreviation, separator, and capitalization.

# CLUSTERING LOGIC

Cluster by semantic purpose, not name variation. Filters named
"Structural - Walls - Phase", "STR - Walls - by Phase", and "S - Wall Phase"
are the same governance unit. Produce one canonical label that represents
all observed variants, normalizing:

* discipline abbreviations (Structural/STR/S, Architectural/Arch/A, MEP/M)
* punctuation and spacing differences
* capitalization differences
* minor wording variants (Hidden vs Hide vs Not Visible)

# RULE STRUCTURE: CONFIRMATION, NOT LABELING INPUT

Rule details (category scope, parameter names, operator types) confirm the
filter's purpose when names are ambiguous or opaque. They do not override a
clear, descriptive name.

* A filter with an opaque or generic name (e.g. "Filter 3", "VF_001",
  a bare GUID-like or numeric token) should be labeled from its category
  scope and rule themes instead of the literal name.
* When categories + rule themes contradict an otherwise clear name, prefer
  the name but note the discrepancy in rationale.

# SPARSE-EVIDENCE RULE

When observed labels are few or weak (1-2 labels), prefer merging by shared
core naming intent rather than splitting on discipline/qualifier noise alone.

# CANONICAL NAMING RULES

1. Preserve the "[Discipline] - [Element Type] - [Condition]" structure when
   the observed names support it; otherwise use the clearest available
   structure.
2. Normalize discipline prefixes to one of: Arch, Structural, MEP, Civil,
   (or the discipline evident in the corpus) — pick the most transferable
   form when names disagree only on abbreviation.
3. Strip firm-specific codes/prefixes that carry no governance meaning
   (e.g. project numbers, ad hoc revision tags).
4. Keep names recognizable to a Revit standards manager and short enough
   for a Power BI slicer (under 50 characters).
5. If the most common name is opaque, construct the label from the
   category scope and the dominant rule condition theme (e.g. phase,
   workset, parameter value), and note lower confidence in rationale.

# YOUR TASK

Suggest 2-3 canonical names for this view filter definition. Names should:
  - Be recognizable to a Revit standards manager
  - Reflect the observed naming convention first, rule structure second
  - Synthesize clean canonical labels from fuzzy naming clusters
  - Emit one canonical name per meaningful cluster, ordered by support
  - Fall back to category/rule-based naming only when names are opaque
  - Be short enough for a Power BI slicer (under 50 characters)

# OUTPUT RULE

Return the canonical names as a pipe-delimited string in support order.

Respond with ONLY valid JSON, no markdown, no explanation outside the JSON:
{
  "candidates": ["name1", "name2", "name3"],
  "recommended": "name1 | name2 | name3",
  "rationale": "One sentence explaining why the output reflects the clustered core naming intents."
}
"""


# ---------------------------------------------------------------------------
# Opaque-name detection
# ---------------------------------------------------------------------------

_OPAQUE_PATTERNS = [
    re.compile(r"^\d"),                       # starts with a number
    re.compile(r"^[A-Z0-9_\-]{1,12}$"),        # all-caps/numeric code
    re.compile(r"^filter[\s_]*\d+$", re.I),    # "Filter 3", "Filter_03"
    re.compile(r"^vf[_\-]?\d+$", re.I),        # "VF_001"
    re.compile(r"^\{?[0-9a-fA-F\-]{8,}\}?$"),  # GUID-like token
]


def _is_opaque_name(name: str) -> bool:
    name = (name or "").strip()
    if not name:
        return True
    return any(p.match(name) for p in _OPAQUE_PATTERNS)


# ---------------------------------------------------------------------------
# identity_items readers
# ---------------------------------------------------------------------------

_RULE_FIELD_RE = re.compile(r"^vf\.rule\[(\d+)\]\.(kind|value|op|prefix|param_ref\.kind|param_ref\.id|sig)$")


def _get_value(identity_items: List[Dict[str, Any]], key: str) -> Optional[str]:
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        if item.get("k") != key:
            continue
        if item.get("q", "ok") != "ok":
            return None
        v = item.get("v")
        return None if v is None else str(v)
    return None


def _collect_rules(identity_items: List[Dict[str, Any]]) -> List[Dict[str, Optional[str]]]:
    grouped: Dict[int, Dict[str, Optional[str]]] = {}
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        k = str(item.get("k", "") or "")
        m = _RULE_FIELD_RE.match(k)
        if not m:
            continue
        idx = int(m.group(1))
        field = m.group(2)
        grouped.setdefault(idx, {"kind": None, "value": None, "op": None, "prefix": None,
                                  "param_ref.kind": None, "param_ref.id": None, "sig": None})
        if item.get("q", "ok") != "ok":
            continue
        v = item.get("v")
        grouped[idx][field] = None if v is None else str(v)
    return [grouped[i] for i in sorted(grouped.keys())]


def _op_short(op: Optional[str]) -> str:
    if not op:
        return "?"
    tail = op.rsplit(".", 1)[-1]
    return tail.replace("FilterRule", "").replace("Evaluator", "") or tail


def _format_rule_summary(rules: List[Dict[str, Optional[str]]]) -> List[str]:
    lines = []
    for i, r in enumerate(rules[:10]):
        prefix = r.get("prefix") or ""
        op = _op_short(r.get("op"))
        kind = r.get("kind") or "?"
        value = r.get("value")
        param_kind = r.get("param_ref.kind") or "?"
        param_id = r.get("param_ref.id") or "?"
        value_part = f" = {value}" if value is not None else ""
        lines.append(
            f"  Rule {i}: {prefix}param({param_kind}:{param_id}) {op} [{kind}]{value_part}"
        )
    if len(rules) > 10:
        lines.append(f"  ... and {len(rules) - 10} more rules")
    return lines


# ---------------------------------------------------------------------------
# User prompt builder
# ---------------------------------------------------------------------------

def build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines: List[str] = []

    sorted_labels = sorted(
        observed_labels or [],
        key=lambda r: int(r.get("files_count", 0) or 0),
        reverse=True,
    )
    top_labels = sorted_labels[:10]

    lines.append("OBSERVED FILTER NAMES (primary signal — these refer to the same rule definition)")
    if top_labels:
        total_files = sum(int(r.get("files_count", 0) or 0) for r in top_labels)
        for row in top_labels:
            label = str(row.get("label_v", "") or "").strip()
            count = int(row.get("files_count", 0) or 0)
            pct = (count / total_files * 100) if total_files else 0
            lines.append(f'  "{label}"  ({count} files, {pct:.0f}%)')
        top_name = str(top_labels[0].get("label_v", "") or "").strip()
        if _is_opaque_name(top_name):
            lines.append(
                "  [Most common name appears opaque/non-descriptive — "
                "derive the label from rule structure below instead]"
            )
    else:
        lines.append("  (no names observed — derive the label from rule structure below)")
    lines.append("")

    lines.append("RULE STRUCTURE (confirmation signal — use to disambiguate or as fallback)")
    categories = _get_value(identity_items, "vf.categories")
    logic_root = _get_value(identity_items, "vf.logic_root")
    rule_count = _get_value(identity_items, "vf.rule_count")

    lines.append(f"  Category scope (negative BuiltInCategory ids): {categories or 'unknown'}")
    lines.append(f"  Logic root: {logic_root or 'unknown'}")
    lines.append(f"  Rule count: {rule_count or '0'}")

    rules = _collect_rules(identity_items)
    if rules:
        lines.append("  Rules:")
        lines.extend(_format_rule_summary(rules))
    else:
        lines.append("  (no readable rules — selection filter or unreadable rule tree)")
    lines.append("")

    if corpus_context:
        total = corpus_context.get("total_files_in_corpus")
        pattern_count = corpus_context.get("domain_pattern_count")
        if total:
            lines.append(f"CORPUS CONTEXT: {total}-file corpus.")
        if pattern_count:
            lines.append(f"There are {pattern_count} distinct view filter definitions total.")
        lines.append("")

    lines.append("YOUR TASK")
    lines.append(
        "Suggest 2-3 canonical names for this view filter definition. Names should:\n"
        "  - Be recognizable to a Revit standards manager\n"
        "  - Reflect the observed naming convention first, rule structure second\n"
        "  - Follow the [Discipline] - [Element Type] - [Condition] convention when supported\n"
        "  - Synthesize clean canonical labels from fuzzy naming clusters\n"
        "  - Emit one canonical name per meaningful cluster, ordered by support\n"
        "  - Fall back to category/rule-based naming only when names are opaque\n"
        "  - Be short enough for a Power BI slicer (under 50 characters)"
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
