# -*- coding: utf-8 -*-
"""Item 0 (PR2 brief): verify inline vs. analysis-side agreement for join_key_name_identity.

PR1 shipped two independent computations of the name-identity join key: one inline at
export time (domains/*.py), one analysis-side, read-time, against already-exported
*.details.json (tools/apply_name_key_policy.py / core/name_key_builder.py). They are
expected to agree by construction -- both read the same underlying identity_basis.items /
phase2 bucket / label data, just at different pipeline stages -- but PR1 shipped without
verifying that. This test is that verification, run before any of PR2's pattern-generation
code is written (see DECISIONS.md D-037 for the
narrative report of these results).

No Revit-extracted corpus exists in this environment (extraction requires the Revit API,
which is unavailable outside Dynamo). Per every domain's own "Canonical Name Identity
Projection (PR1)" code comment (read directly from domains/*.py for this test), the inline
computation is always:

    build_join_key_from_policy(
        domain_policy=<name-key policy for this domain>,
        identity_items=<domain's identity_items> [+ one make_identity_item(...)-wrapped
                                                     widened value, for Widened domains],
        include_optional_items=False, emit_keys_used=True, hash_optional_items=False,
        emit_items=False, emit_selectors=True,
    )

This test reconstructs that exact call per domain (the "inline-equivalent" value) from a
synthetic record's raw source value, independently of core/name_key_builder.py, and asserts
it matches what core.name_key_builder.build_name_key_for_record() reconstructs from the
same record's *exported* shape (identity_basis.items + phase2 buckets + label). Because the
two sides are built from independent code paths reading the same underlying value, this is
a genuine two-path agreement check, not a tautology.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.name_key_builder import build_name_key_for_record
from core.name_key_coverage import ELIGIBLE_DOMAINS
from core.record_v2 import canonicalize_str, make_identity_item

POLICY_PATH = "policies/domain_name_key_policies.json"


def _inline_equivalent(pol: Dict[str, Any], identity_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Mirrors the exact build_join_key_from_policy call every domains/*.py inline call
    site makes for its Canonical Name Identity Projection (PR1) -- kwargs copied verbatim
    from domains/materials.py, domains/phases.py, domains/identity.py, etc."""
    join_key, _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    return join_key


# Each case: (domain, exported_record, base_identity_items, widened_key, raw_widened_value)
# widened_key/raw_widened_value are None for Native domains (no widening -- the required
# item is already a member of identity_items).
CASES: List[Tuple[str, Dict[str, Any], List[Dict[str, Any]], Any, Any]] = []


def _native_case(domain: str, item_key: str, value: str) -> None:
    base_items = [{"k": item_key, "v": value, "q": "ok"}]
    rec = {
        "domain": domain,
        "label": {"display": value},
        "identity_basis": {"items": base_items},
    }
    CASES.append((domain, rec, base_items, None, None))


def _bucket_widened_case(domain: str, item_key: str, value: str, bucket: str) -> None:
    base_items = [{"k": f"{domain}.other", "v": "x", "q": "ok"}]
    rec = {
        "domain": domain,
        "label": {"display": value},
        "identity_basis": {"items": base_items},
        "phase2": {bucket: [{"k": item_key, "v": value, "q": "ok"}]},
    }
    CASES.append((domain, rec, base_items, item_key, value))


def _label_only_case(domain: str, item_key: str, value: str, component: str = None) -> None:
    base_items = [{"k": f"{domain}.other", "v": "x", "q": "ok"}]
    label: Dict[str, Any] = {"display": value}
    if component:
        label["components"] = {component: value}
    rec = {
        "domain": domain,
        "label": label,
        "identity_basis": {"items": base_items},
    }
    CASES.append((domain, rec, base_items, item_key, value))


# --- Native (7): own-name item already present in identity_basis.items ---
_native_case("phases", "phase.name", "Existing")
_native_case("materials", "material.name", "Concrete, Cast-in-Place")
_native_case("text_types", "text_type.name", "Arial 3/32in")
_native_case("wall_types", "wt.type_name", "Generic - 200mm")
_native_case("floor_types", "ft.type_name", "Generic 300mm")
_native_case("roof_types", "rt.type_name", "Generic - 400mm")
_native_case("ceiling_types", "ct.type_name", "Compound Ceiling")

# --- Widened via phase2 bucket (own-name value already present in a phase2 bucket) ---
_bucket_widened_case("identity", "identity.project_title", "My Project", "unknown_items")
_bucket_widened_case("phase_filters", "phase_filter.name", "Existing", "coordination_items")
_bucket_widened_case("line_patterns", "line_pattern.name", "Dash Dot", "cosmetic_items")
_bucket_widened_case("fill_patterns_drafting", "fill_pattern.name", "Diagonal Crosshatch", "cosmetic_items")
_bucket_widened_case("fill_patterns_model", "fill_pattern.name", "Sand - Dense", "cosmetic_items")

# --- Widened via label only (no phase2 bucket item exists; value lives only in label) ---
_label_only_case("arrowheads", "arrowhead.name", "Arrow Filled 15deg", component="type_name")
_label_only_case("loaded_family_types", "lft.family_name", "Single-Flush", component="family_name")
_label_only_case("view_filter_definitions", "vf.name", "Foo", component="name")
_label_only_case("view_templates_floor_structural_area_plans", "view_template.name", "Floor Plan - Arch")
_label_only_case("view_templates_ceiling_plans", "view_template.name", "Ceiling Plan - RCP")
_label_only_case("view_templates_elevations_sections_detail", "view_template.name", "Elevation - Exterior")
_label_only_case("view_templates_renderings_drafting", "view_template.name", "Rendering - Exterior")
_label_only_case("view_templates_schedules", "view_template.name", "Door Schedule")
_label_only_case("dimension_types_linear", "dim_type.name", "Linear - 3/32in Arial")
_label_only_case("dimension_types_angular", "dim_type.name", "Angular - 3/32in Arial")
_label_only_case("dimension_types_radial", "dim_type.name", "Radial - 3/32in Arial")
_label_only_case("dimension_types_diameter", "dim_type.name", "Diameter - 3/32in Arial")
_label_only_case("dimension_types_spot_slope", "dim_type.name", "Spot Slope - Arial")

assert {c[0] for c in CASES} == ELIGIBLE_DOMAINS, (
    "agreement-check sample must cover exactly the 25 eligible domains (7 native + 18 widened)"
)
assert len(CASES) == 25


@pytest.fixture(scope="module")
def name_key_policies():
    return load_join_key_policies(POLICY_PATH)


@pytest.mark.parametrize("domain,rec,base_items,widened_key,widened_value", CASES, ids=[c[0] for c in CASES])
def test_inline_equivalent_matches_analysis_side(domain, rec, base_items, widened_key, widened_value, name_key_policies):
    pol = get_domain_join_key_policy(name_key_policies, domain)
    assert pol is not None, f"{domain} must have a name-key policy entry"

    if widened_key is None:
        inline_items = base_items
    else:
        v, q = canonicalize_str(widened_value)
        inline_items = base_items + [make_identity_item(widened_key, v, q)]

    expected = _inline_equivalent(pol, inline_items)
    actual = build_name_key_for_record(rec, domain, name_key_policies)

    assert actual is not None
    assert actual["join_hash"] == expected["join_hash"], (
        f"{domain}: analysis-side reconstruction diverged from inline-equivalent computation"
    )
    assert actual["status"] == "ok"


def test_agreement_sample_size_and_match_rate(name_key_policies):
    """Reports the concrete sample size / match rate this file's parametrized cases give,
    per the PR2 brief's requirement that item 0 report "a concrete number, not 'as
    expected.'" 25 domains sampled, 1 record each = N=25, match_rate=1.0 (all pass, or
    this test fails loudly with the count that didn't)."""
    total = 0
    matched = 0
    for domain, rec, base_items, widened_key, widened_value in CASES:
        pol = get_domain_join_key_policy(name_key_policies, domain)
        if widened_key is None:
            inline_items = base_items
        else:
            v, q = canonicalize_str(widened_value)
            inline_items = base_items + [make_identity_item(widened_key, v, q)]
        expected = _inline_equivalent(pol, inline_items)
        actual = build_name_key_for_record(rec, domain, name_key_policies)
        total += 1
        if actual is not None and actual.get("join_hash") == expected.get("join_hash"):
            matched += 1

    assert total == 25
    match_rate = matched / total
    assert match_rate == 1.0, f"N={total}, matched={matched}, match_rate={match_rate:.4f}"
