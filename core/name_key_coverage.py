# -*- coding: utf-8 -*-
"""Coverage-class registry for the Canonical Name Identity Projection (PR1/PR2).

Single source of truth for the Native / Widened / Excluded classification carried
forward from PR1's Step-0-within-PR1 audit (audit_results/audit_6_name_key_step0_within_pr1.md)
and restated in this PR's brief. Any code that reports "domains covered" under the
name-identity projection (`join_key_name_identity`) must read this registry rather than
re-deriving or assuming the list -- see tools/generate_name_key_patterns.py.

The Native/Widened distinction is not cosmetic: Widened domains' name key is built from a
value pulled from a phase2 bucket or raw `label.display`/`label.components` at the name-key
call site only -- it is NOT a subset of `identity_items`/`identity_basis.items` the way a
Native domain's name key is. A "disagreement" between `join_key_name_identity` and
`join_hash` is not directly comparable in kind between the two classes.

`phases` is Native but also carries the `.redundant` marker: its name-identity value is
expected to be near-tautologically identical to its configuration `join_hash` (both key off
`phase.name`, D-010). Downstream reporting must propagate this marker rather than treating
`phases` as a genuinely independent correspondence data point.
"""

from __future__ import annotations

from typing import Dict, FrozenSet

COVERAGE_NATIVE = "native"
COVERAGE_WIDENED = "widened"
COVERAGE_PHASES_REDUNDANT = "phases_redundant"
COVERAGE_EXCLUDED = "excluded"

# Native (7): own-name item already present in identity_items -- same evidence surface as
# join_hash/sig_hash. `phases` is native but reported as COVERAGE_PHASES_REDUNDANT (see below).
NATIVE_DOMAINS: FrozenSet[str] = frozenset({
    "phases",
    "materials",
    "text_types",
    "wall_types",
    "floor_types",
    "roof_types",
    "ceiling_types",
})

# Widened (18): name pulled from a phase2 bucket or raw label.display/label.components at the
# name-key call site only -- not a subset of identity_items/identity_basis.items.
WIDENED_DOMAINS: FrozenSet[str] = frozenset({
    "identity",
    "phase_filters",
    "line_patterns",
    "fill_patterns_drafting",
    "fill_patterns_model",
    "arrowheads",
    "loaded_family_types",
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
    "view_filter_definitions",
    "dimension_types_linear",
    "dimension_types_angular",
    "dimension_types_radial",
    "dimension_types_diameter",
    "dimension_types_spot_slope",
})

# Excluded (12): no usable own-name evidence -- reason string carried per-domain for explicit
# reporting (never a silent absence).
EXCLUDED_DOMAINS: Dict[str, str] = {
    "units": "no_name_like_key",
    "line_styles": "no_name_like_key",
    "object_styles_analytical": "no_name_like_key",
    "object_styles_annotation": "no_name_like_key",
    "object_styles_imported": "no_name_like_key",
    "object_styles_model": "no_name_like_key",
    "view_category_overrides": "no_name_like_key",
    "view_category_overrides_annotation": "no_name_like_key",
    "view_category_overrides_model": "no_name_like_key",
    "dimension_types_spot_coordinate": "referenced_element_name_not_own_label",
    "dimension_types_spot_elevation": "referenced_element_name_not_own_label",
    "view_filter_applications_view_templates": "uid_shaped_only_candidate",
}

ELIGIBLE_DOMAINS: FrozenSet[str] = frozenset(NATIVE_DOMAINS | WIDENED_DOMAINS)

assert not (ELIGIBLE_DOMAINS & set(EXCLUDED_DOMAINS)), "eligible/excluded domain sets must be disjoint"
assert len(NATIVE_DOMAINS) == 7, "Native domain count drifted from PR1 findings"
assert len(WIDENED_DOMAINS) == 18, "Widened domain count drifted from PR1 findings"
assert len(EXCLUDED_DOMAINS) == 12, "Excluded domain count drifted from PR1 findings"


def coverage_class(domain: str) -> str:
    """Classify a domain for the name-identity projection.

    Returns one of COVERAGE_PHASES_REDUNDANT / COVERAGE_NATIVE / COVERAGE_WIDENED /
    COVERAGE_EXCLUDED. A domain that is neither eligible nor in the excluded registry
    (i.e. not part of PR1's traced surface at all) also reports as COVERAGE_EXCLUDED with
    reason "not_traced" via `exclusion_reason`.
    """
    if domain == "phases":
        return COVERAGE_PHASES_REDUNDANT
    if domain in NATIVE_DOMAINS:
        return COVERAGE_NATIVE
    if domain in WIDENED_DOMAINS:
        return COVERAGE_WIDENED
    return COVERAGE_EXCLUDED


def exclusion_reason(domain: str) -> str:
    """Reason string for an excluded (or untraced) domain; "" for an eligible domain."""
    if domain in ELIGIBLE_DOMAINS:
        return ""
    return EXCLUDED_DOMAINS.get(domain, "not_traced")


def is_eligible(domain: str) -> bool:
    return domain in ELIGIBLE_DOMAINS
