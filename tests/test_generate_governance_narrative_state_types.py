"""Tests for governance-state comparison_type handling in
tools/generate_governance_narrative.py (build_governance_state_summary).

See docs/governance_narrative_scope_gap_audit.md section A3.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import GOVERNANCE_STATE_DIRECTED_TYPES  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    _DIRECTED_GOVERNANCE_TYPES,
    _GOVERNANCE_STATE_RENDERED_TYPES,
    build_governance_state_summary,
)

# Comparison types the narrative deliberately keeps beyond the producer's set, with
# a documented reason each (see the code comment above _DIRECTED_GOVERNANCE_TYPES).
# If this set ever needs to shrink, that's a deliberate cleanup, not a silent drift.
_KNOWN_LEGACY_EXTRA_TYPES = {"generic_to_downstream", "parent_sibling_roles"}


def _gs_row(**overrides):
    r = {
        "domain": "", "comparison_type": "",
        "provided_and_used_count": "", "provided_but_passive_count": "",
        "provided_but_missing_count": "", "local_active_count": "",
        "local_passive_count": "", "local_unbundled_count": "",
        "reference_all_count": "", "target_all_count": "", "target_used_count": "",
        "provided_to_configured_containment": "", "provided_to_used_containment": "",
        "provided_passive_share": "", "provided_missing_share": "", "local_active_share": "",
        "all_containment_a_in_b_mean": "",
    }
    r.update(overrides)
    return r


def _state_row(**overrides):
    r = {"domain": "", "comparison_type": "", "state": "",
         "in_reference_all": "", "in_target_all": "", "in_target_used": ""}
    r.update(overrides)
    return r


def test_directed_governance_types_match_producer_modulo_flagged_legacy_entries():
    """A future producer-side addition/removal to GOVERNANCE_STATE_DIRECTED_TYPES
    must fail this test rather than silently drift out of sync again."""
    extra = _DIRECTED_GOVERNANCE_TYPES - GOVERNANCE_STATE_DIRECTED_TYPES
    missing = GOVERNANCE_STATE_DIRECTED_TYPES - _DIRECTED_GOVERNANCE_TYPES

    assert missing == set(), (
        "generate_governance_narrative._DIRECTED_GOVERNANCE_TYPES is missing producer "
        f"type(s): {missing}"
    )
    assert extra == _KNOWN_LEGACY_EXTRA_TYPES, (
        "generate_governance_narrative._DIRECTED_GOVERNANCE_TYPES has drifted: expected "
        f"only the flagged legacy entries {_KNOWN_LEGACY_EXTRA_TYPES} beyond the producer's "
        f"set, found {extra}"
    )


def test_rendered_types_excludes_scope_level_fan_out():
    scope_types = {"enterprise_to_project", "bc_to_project", "enterprise_to_bc", "enterprise_to_client"}
    assert scope_types.isdisjoint(_GOVERNANCE_STATE_RENDERED_TYPES)


def test_compact_summary_loop_does_not_blend_distinct_comparison_types():
    """bc_to_project (scope-level) and template_to_project (cascade-stage) rows for
    the same domain must never be averaged into one number."""
    rows = [
        _gs_row(domain="arrowheads", comparison_type="template_to_project",
                provided_to_used_containment="0.90", provided_passive_share="0.05",
                reference_all_count="10", target_used_count="10"),
        _gs_row(domain="arrowheads", comparison_type="bc_to_project",
                provided_to_used_containment="0.20", provided_passive_share="0.70",
                reference_all_count="10", target_used_count="10"),
    ]
    result = build_governance_state_summary([], rows)
    by_type = result["arrowheads"]["by_comparison_type"]

    assert by_type["template_to_project"]["provided_passive_share"] == 0.05
    assert by_type["bc_to_project"]["provided_passive_share"] == 0.70

    # The rendered (merged) view must reflect ONLY the rendered types -- bc_to_project
    # (Group 3) is excluded, so this must equal template_to_project alone, not a blend.
    assert result["arrowheads"]["provided_passive_share"] == 0.05


def test_detailed_loop_no_longer_drops_new_scope_types():
    rows = [
        _state_row(domain="arrowheads", comparison_type="enterprise_to_project", state="provided_and_used"),
        _state_row(domain="arrowheads", comparison_type="bc_to_project", state="provided_but_passive"),
        _state_row(domain="arrowheads", comparison_type="enterprise_to_bc", state="provided_but_missing"),
        _state_row(domain="arrowheads", comparison_type="enterprise_to_client", state="local_active"),
        # A rendered-type row for the same domain so it isn't entirely omitted from
        # the result (see test_domain_with_only_group3_state_is_omitted_from_result
        # for the all-Group-3 case).
        _state_row(domain="arrowheads", comparison_type="template_to_project", state="provided_and_used"),
    ]
    result = build_governance_state_summary(rows, [])
    by_type = result["arrowheads"]["by_comparison_type"]

    assert by_type["enterprise_to_project"]["provided_and_used_count"] == 1
    assert by_type["bc_to_project"]["provided_but_passive_count"] == 1
    assert by_type["enterprise_to_bc"]["provided_but_missing_count"] == 1
    assert by_type["enterprise_to_client"]["local_active_count"] == 1

    # None of these four (Group 3) reach the merged/rendered domain-level counts --
    # only the template_to_project row does.
    merged = result["arrowheads"]
    assert merged["provided_and_used_count"] == 1
    assert merged["provided_but_passive_count"] == 0
    assert merged["provided_but_missing_count"] == 0
    assert merged["local_active_count"] == 0


def test_domain_with_only_group3_state_is_omitted_from_result():
    """A domain whose ENTIRE governance-state signal is Group 3 (scope-level
    fan-out) rows must be absent from the returned map entirely, not present with
    an all-None-but-truthy merged entry -- render_domain_tiers()'s has_state check
    treats any non-None dict as "this domain has state data" regardless of its
    values, which would wrongly switch its whole tier group to state-column
    rendering. See PR #350 review."""
    rows = [
        _state_row(domain="scope_only_domain", comparison_type="enterprise_to_project", state="provided_and_used"),
        _state_row(domain="scope_only_domain", comparison_type="bc_to_project", state="provided_but_passive"),
    ]
    result = build_governance_state_summary(rows, [])
    assert "scope_only_domain" not in result
