from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.governance_manifest import (
    build_governance_populations,
    compute_scope_key,
    normalize_business_center_label,
)


def _row(export_run_id, role, client_label, bc_label, unit="imperial", discipline="architectural"):
    return {
        "export_run_id": export_run_id,
        "unit_system": unit,
        "governance_role": role,
        "discipline_label": discipline,
        "client_label": client_label,
        "business_center_label": bc_label,
    }


def _find(manifest_rows, **kwargs):
    matches = [
        r for r in manifest_rows
        if all(r.get(k) == v for k, v in kwargs.items())
    ]
    assert len(matches) == 1, f"expected exactly one match for {kwargs}, got {len(matches)}: {matches}"
    return matches[0]


def _members(governance_id, membership_rows):
    return sorted(r["export_run_id"] for r in membership_rows if r["governance_id"] == governance_id)


# ---------------------------------------------------------------------------
# normalize_business_center_label / compute_scope_key unit-level coverage
# ---------------------------------------------------------------------------

def test_normalize_strips_bc_prefix_case_insensitive():
    assert normalize_business_center_label("BC_2014") == ("2014", False)
    assert normalize_business_center_label("bc_2014") == ("2014", False)
    assert normalize_business_center_label("2014") == ("2014", False)


def test_normalize_recognizes_enterprise_tokens_before_prefix_strip():
    assert normalize_business_center_label("0000") == ("0000", True)
    assert normalize_business_center_label("BC_0000") == ("BC_0000", True)
    assert normalize_business_center_label("bc_0000") == ("bc_0000", True)


def test_scope_key_requires_both_conditions_for_enterprise():
    # Internal client but real bc -> bc scope, not enterprise.
    key, level, _, bc = compute_scope_key("Stantec", "2014")
    assert level == "bc" and key == "bc:2014" and bc == "2014"
    # External client but enterprise-bookkeeping bc -> client scope, not enterprise.
    key, level, _, _ = compute_scope_key("Sutter", "0000")
    assert level == "client" and key == "client:Sutter"
    # Both conditions -> enterprise.
    key, level, _, _ = compute_scope_key("Stantec", "0000")
    assert level == "enterprise" and key == "enterprise"
    # Neither condition -> named project.
    key, level, _, _ = compute_scope_key("Sutter", "2270")
    assert level == "project" and key == "project:Sutter:2270"


# ---------------------------------------------------------------------------
# build_governance_populations
# ---------------------------------------------------------------------------

def test_stantec_and_0000_collapse_to_one_enterprise_population():
    rows = [
        _row("e1", "Container", "Stantec", "0000"),
        _row("e2", "Container", "Stantec", "0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    pop = _find(manifest_rows, scope_key="enterprise", governance_role="Container")
    assert pop["file_count"] == "2"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2"]


def test_legacy_bc_prefixed_and_bare_numeric_collapse_to_one_population():
    rows = [
        _row("e1", "Container", "Stantec", "BC_2014"),
        _row("e2", "Container", "Stantec", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    assert len(manifest_rows) == 1
    pop = manifest_rows[0]
    assert pop["scope_key"] == "bc:2014"
    assert pop["business_center_label"] == "2014"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2"]


def test_generic_role_gets_no_scope_key():
    rows = [
        _row("e1", "Generic", "Stantec", "0000"),
        _row("e2", "Generic", "Stantec", "0000", unit="metric"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    for pop in manifest_rows:
        assert pop["scope_key"] == ""
        assert pop["scope_level"] == "generic"
    assert len(manifest_rows) == 2  # split by unit_system


def test_generic_host_role_treated_same_as_generic():
    rows = [_row("e1", "Generic-Host", "Stantec", "0000")]
    manifest_rows, _membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    assert manifest_rows[0]["scope_level"] == "generic"


def test_case_variant_role_merges_into_canonical_population():
    # A manual-edit case variant ("container") must merge with "Container",
    # not fragment into its own governance_id — otherwise downstream pair
    # discovery's exact-string role checks silently drop it.
    rows = [
        _row("e1", "Container", "Stantec", "2014"),
        _row("e2", "container", "Stantec", "2014"),
        _row("e3", "CONTAINER", "Stantec", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    assert len(manifest_rows) == 1
    pop = manifest_rows[0]
    assert pop["governance_role"] == "Container"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2", "e3"]


def test_generic_host_case_variant_folds_to_generic_role_label():
    rows = [
        _row("e1", "Generic", "Stantec", "0000"),
        _row("e2", "generic-host", "Stantec", "0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["governance_role"] == "Generic"
    assert _members(manifest_rows[0]["governance_id"], membership_rows) == ["e1", "e2"]


def test_unrecognized_role_excluded_with_loud_report(capsys):
    rows = [
        _row("e1", "Container", "Stantec", "0000"),
        _row("e2", "Mystery", "Stantec", "0000"),
    ]
    manifest_rows, _membership_rows, excluded = build_governance_populations(rows)
    assert len(excluded) == 1
    assert excluded[0]["export_run_id"] == "e2"
    # The good row still resolves into a population despite the bad one existing.
    assert any(r["file_count"] == "1" for r in manifest_rows)
    captured = capsys.readouterr()
    assert "e2" in captured.err
    assert "excluded" in captured.err.lower()


def test_blank_client_label_raises_defense_in_depth():
    rows = [_row("e1", "Container", "", "2014")]
    with pytest.raises(SystemExit):
        build_governance_populations(rows)


def test_na_business_center_label_raises_defense_in_depth():
    rows = [_row("e1", "Container", "Stantec", "N/A")]
    with pytest.raises(SystemExit):
        build_governance_populations(rows)


def test_fully_populated_rows_build_without_raising():
    rows = [_row("e1", "Container", "Stantec", "2014")]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    assert len(manifest_rows) == 1
    assert len(membership_rows) == 1
