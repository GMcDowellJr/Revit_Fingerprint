from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.enterprise_policy import load_enterprise_policy

POLICY = load_enterprise_policy()

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


def test_normalize_zero_pads_short_numeric_values():
    # PR #425 review finding: this module reads file_metadata.csv directly
    # and independently of build_segment_manifest.py's own zero-pad fix, so
    # it needs the same fix applied here too -- otherwise a business_center_
    # label Excel collapsed from "0000"/"0796" to "0"/"796" (column not
    # imported as Text) fragments governance populations that
    # build_segment_manifest.py's segment lattice would already have merged.
    assert normalize_business_center_label("796") == ("0796", False)
    assert normalize_business_center_label("14") == ("0014", False)
    assert normalize_business_center_label("0796") == ("0796", False)


def test_normalize_zero_pad_recognizes_collapsed_enterprise_token():
    # A collapsed "0" must still be recognized as the "0000" enterprise
    # token, not treated as a real 1-digit business center.
    assert normalize_business_center_label("0") == ("0000", True)


def test_normalize_zero_pad_does_not_affect_bc_prefixed_values():
    # "BC_14" is not purely numeric (contains letters) -- zero-padding must
    # not fire here; only the existing BC_-prefix-strip behavior applies.
    assert normalize_business_center_label("BC_14") == ("14", False)


def test_scope_key_requires_both_conditions_for_enterprise():
    # Internal client but real bc -> bc scope, not enterprise.
    key, level, _, bc = compute_scope_key("InternalEnterprise", "2014", POLICY)
    assert level == "bc" and key == "bc:2014" and bc == "2014"
    # External client but enterprise-bookkeeping bc -> client scope, not enterprise.
    key, level, _, _ = compute_scope_key("ClientBeta", "0000", POLICY)
    assert level == "client" and key == "client:ClientBeta"
    # Both conditions -> enterprise.
    key, level, _, _ = compute_scope_key("InternalEnterprise", "0000", POLICY)
    assert level == "enterprise" and key == "enterprise"
    # Neither condition -> named project.
    key, level, _, _ = compute_scope_key("ClientBeta", "2270", POLICY)
    assert level == "project" and key == "project:ClientBeta:2270"


# ---------------------------------------------------------------------------
# build_governance_populations
# ---------------------------------------------------------------------------

def test_internalenterprise_and_0000_collapse_to_one_enterprise_population():
    rows = [
        _row("e1", "Container", "InternalEnterprise", "0000"),
        _row("e2", "Container", "InternalEnterprise", "0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    pop = _find(manifest_rows, scope_key="enterprise", governance_role="Container")
    assert pop["file_count"] == "2"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2"]


def test_legacy_bc_prefixed_and_bare_numeric_collapse_to_one_population():
    rows = [
        _row("e1", "Container", "InternalEnterprise", "BC_2014"),
        _row("e2", "Container", "InternalEnterprise", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    pop = manifest_rows[0]
    assert pop["scope_key"] == "bc:2014"
    assert pop["business_center_label"] == "2014"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2"]


def test_excel_collapsed_and_correctly_formatted_bc_collapse_to_one_population():
    # PR #425 review finding, integration-level: some rows still say "0796"
    # (never opened in Excel without a Text-typed column import), others got
    # collapsed to "796" -- both must fold into the SAME governance
    # population rather than fragmenting into two.
    rows = [
        _row("e1", "Container", "InternalEnterprise", "0796"),
        _row("e2", "Container", "InternalEnterprise", "796"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    pop = manifest_rows[0]
    assert pop["scope_key"] == "bc:0796"
    assert pop["business_center_label"] == "0796"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2"]


def test_generic_role_gets_no_scope_key():
    rows = [
        _row("e1", "Generic", "InternalEnterprise", "0000"),
        _row("e2", "Generic", "InternalEnterprise", "0000", unit="metric"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    for pop in manifest_rows:
        assert pop["scope_key"] == ""
        assert pop["scope_level"] == "generic"
    assert len(manifest_rows) == 2  # split by unit_system


def test_generic_host_role_treated_same_as_generic():
    rows = [_row("e1", "Generic-Host", "InternalEnterprise", "0000")]
    manifest_rows, _membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert manifest_rows[0]["scope_level"] == "generic"


def test_case_variant_role_merges_into_canonical_population():
    # A manual-edit case variant ("container") must merge with "Container",
    # not fragment into its own governance_id — otherwise downstream pair
    # discovery's exact-string role checks silently drop it.
    rows = [
        _row("e1", "Container", "InternalEnterprise", "2014"),
        _row("e2", "container", "InternalEnterprise", "2014"),
        _row("e3", "CONTAINER", "InternalEnterprise", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    pop = manifest_rows[0]
    assert pop["governance_role"] == "Container"
    assert _members(pop["governance_id"], membership_rows) == ["e1", "e2", "e3"]


def test_generic_host_case_variant_folds_to_generic_role_label():
    rows = [
        _row("e1", "Generic", "InternalEnterprise", "0000"),
        _row("e2", "generic-host", "InternalEnterprise", "0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["governance_role"] == "Generic"
    assert _members(manifest_rows[0]["governance_id"], membership_rows) == ["e1", "e2"]


def test_unit_system_case_variants_merge_to_lowercase():
    rows = [
        _row("e1", "Container", "InternalEnterprise", "2014", unit="imperial"),
        _row("e2", "Container", "InternalEnterprise", "2014", unit="Imperial"),
        _row("e3", "Container", "InternalEnterprise", "2014", unit="IMPERIAL"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["unit_system"] == "imperial"
    assert _members(manifest_rows[0]["governance_id"], membership_rows) == ["e1", "e2", "e3"]


def test_client_label_case_variants_merge_to_first_seen_casing():
    rows = [
        _row("e1", "Container", "Acme", "2014"),
        _row("e2", "Container", "acme", "2014"),
        _row("e3", "Container", "ACME", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["client_label"] == "Acme"  # first occurrence wins
    assert manifest_rows[0]["scope_key"] == "project:Acme:2014"
    assert _members(manifest_rows[0]["governance_id"], membership_rows) == ["e1", "e2", "e3"]


def test_discipline_label_case_variants_merge():
    rows = [
        _row("e1", "Container", "InternalEnterprise", "2014", discipline="architectural"),
        _row("e2", "Container", "InternalEnterprise", "2014", discipline="Architectural"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["discipline_label"] == "architectural"


def test_business_center_label_case_variants_merge_after_prefix_strip():
    # "Page" is a real, non-numeric legacy business_center_label in current
    # data (no special-casing per the "no Page-specific branch" rule) -- its
    # casing must fold the same way a numeric BC's would.
    rows = [
        _row("e1", "Container", "InternalEnterprise", "Page"),
        _row("e2", "Container", "InternalEnterprise", "page"),
        _row("e3", "Container", "InternalEnterprise", "PAGE"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["business_center_label"] == "Page"  # first occurrence wins


def test_enterprise_bookkeeping_casing_still_recognized_after_normalization():
    rows = [
        _row("e1", "Container", "InternalEnterprise", "0000"),
        _row("e2", "Container", "InternalEnterprise", "BC_0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["scope_level"] == "enterprise"
    assert _members(manifest_rows[0]["governance_id"], membership_rows) == ["e1", "e2"]


def test_unrecognized_role_excluded_with_loud_report(capsys):
    rows = [
        _row("e1", "Container", "InternalEnterprise", "0000"),
        _row("e2", "Mystery", "InternalEnterprise", "0000"),
    ]
    manifest_rows, _membership_rows, excluded = build_governance_populations(rows, POLICY)
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
        build_governance_populations(rows, POLICY)


def test_na_business_center_label_raises_defense_in_depth():
    rows = [_row("e1", "Container", "InternalEnterprise", "N/A")]
    with pytest.raises(SystemExit):
        build_governance_populations(rows, POLICY)


def test_fully_populated_rows_build_without_raising():
    rows = [_row("e1", "Container", "InternalEnterprise", "2014")]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows, POLICY)
    assert not excluded
    assert len(manifest_rows) == 1
    assert len(membership_rows) == 1


def test_compute_scope_key_accepts_case_insensitive_deployment_override():
    key, level, _, bc = compute_scope_key("Deployment Enterprise", "0000", load_enterprise_policy(enterprise_label="deployment enterprise"))
    assert (key, level, bc) == ("enterprise", "enterprise", "")
    key, level, _, bc = compute_scope_key("DEPLOYMENT ENTERPRISE", "2270", load_enterprise_policy(enterprise_label="deployment enterprise"))
    assert (key, level, bc) == ("bc:2270", "bc", "2270")


def test_cross_segment_and_governance_manifest_share_policy_classification():
    from tools.compare_cross_segment import _scope_level
    from tools.enterprise_policy import load_enterprise_policy

    custom = load_enterprise_policy(enterprise_label="My Enterprise")
    matrix = [
        ("My Enterprise", "0000", "enterprise", "enterprise"),
        ("MY ENTERPRISE", "2270", "bc", "business_center"),
        ("External", "2270", "project", "client_business_center"),
        ("External", "0000", "client", None),
    ]
    for client, bc, governance_level, cross_level in matrix:
        assert compute_scope_key(client, bc, custom)[1] == governance_level
        assert _scope_level({"client_label": client, "business_center_label": bc}, custom) == cross_level
