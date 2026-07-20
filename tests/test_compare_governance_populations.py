from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.governance_manifest import build_governance_populations
from tools.compare_governance_populations import (
    discover_same_role_peer_pairs,
    discover_directed_tc_to_project_pairs,
    discover_generic_pairs,
    run_comparisons,
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


def _synthetic_manifest():
    """A manifest shaped to exercise every comparison_type in one shot:
    enterprise/bc/bc/client/client for Container (same-role peer coverage),
    a matching bc/enterprise/enterprise pairing on the Project side (directed
    coverage), and one Generic population (unconditional-pairing coverage)."""
    rows = [
        _row("ent1", "Container", "Stantec", "0000"),
        _row("bc2014_1", "Container", "Stantec", "2014"),
        _row("bc2270_1", "Container", "Stantec", "2270"),
        _row("cl_sutter1", "Container", "Sutter", "0000"),
        _row("cl_renown1", "Container", "Renown", "0000"),
        _row("proj_bc2014", "Project", "Stantec", "2014"),
        _row("proj_sutter", "Project", "Sutter", "9999"),
        _row("gen1", "Generic", "Stantec", "0000"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    return manifest_rows, membership_rows


def _records_rows(jh_map, domain="object_styles_model"):
    rows = []
    for eid, jhs in jh_map.items():
        for jh in jhs:
            rows.append({"export_run_id": eid, "domain": domain, "join_hash": jh})
    return rows


_JH_MAP = {
    "ent1": {"h1", "h2"},
    "bc2014_1": {"h1", "h3"},
    "bc2270_1": {"h4"},
    "cl_sutter1": {"h1", "h5"},
    "cl_renown1": {"h6"},
    "proj_bc2014": {"h1", "h3", "h7"},
    "proj_sutter": {"h5", "h8"},
    "gen1": {"h1", "h9"},
}


def test_same_role_peer_produces_expected_comparison_type_set():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_same_role_peer_pairs(manifest_rows)
    types = {t for _a, _b, t in pairs}
    assert types == {"bc_to_bc", "bc_to_client", "client_to_client", "enterprise_to_bc", "enterprise_to_client"}


def test_same_role_peer_excludes_project_and_generic():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_same_role_peer_pairs(manifest_rows)
    for pop_a, pop_b, _t in pairs:
        assert pop_a["governance_role"] != "Project"
        assert pop_b["governance_role"] != "Project"
        assert pop_a["scope_level"] != "generic"
        assert pop_b["scope_level"] != "generic"


def test_same_role_peer_excludes_project_scoped_template_or_container():
    # Regression: a Template/Container population can itself land at
    # scope_level "project" (real external client + real bc together, e.g.
    # a Template scoped to one specific client+bc combination -- this shape
    # occurs in production data). Peer-comparing it against a "bc"-scoped
    # sibling of the same role would produce "bc_to_project", colliding with
    # the directed Template/Container -> Project containment comparison's
    # identical type name.
    rows = [
        _row("t_proj", "Template", "Sutter", "2014"),  # scope_level "project"
        _row("t_bc", "Template", "Stantec", "2014"),   # scope_level "bc"
    ]
    manifest_rows, _membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    pairs = discover_same_role_peer_pairs(manifest_rows)
    assert pairs == []


def test_comparison_type_still_unambiguous_with_project_scoped_template():
    rows = [
        _row("t_proj", "Template", "Sutter", "2014"),
        _row("t_bc", "Template", "Stantec", "2014"),
        _row("p1", "Project", "Stantec", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    records_rows = _records_rows({
        "t_proj": {"h1"}, "t_bc": {"h1"}, "p1": {"h1"},
    })
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)
    shape_by_type = {}
    for row in out_rows:
        has_jaccard = bool(row.get("all_pairwise_jaccard_mean"))
        ctype = row["comparison_type"]
        assert shape_by_type.setdefault(ctype, has_jaccard) == has_jaccard, (
            f"comparison_type {ctype!r} mixes symmetric and directed rows"
        )


def test_generic_pairs_unconditionally_against_every_tc_project_population():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_generic_pairs(manifest_rows)
    target_populations = [r for r in manifest_rows if r["governance_role"] in ("Template", "Container", "Project")]
    assert len(pairs) == len(target_populations)
    paired_targets = {b["governance_id"] for _a, b, _t in pairs}
    assert paired_targets == {r["governance_id"] for r in target_populations}


def test_directed_enterprise_to_project_is_unconditional_on_scope():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_directed_tc_to_project_pairs(manifest_rows)
    enterprise_pairs = [(a, b, t) for a, b, t in pairs if a["scope_level"] == "enterprise"]
    project_pops = [r for r in manifest_rows if r["governance_role"] == "Project"]
    # Enterprise Container pairs against every Project population, regardless
    # of the project's own scope_key.
    assert len(enterprise_pairs) == len(project_pops)
    for _a, _b, t in enterprise_pairs:
        assert t == "enterprise_to_project"


def test_directed_bc_to_project_matches_by_business_center_label_alone():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_directed_tc_to_project_pairs(manifest_rows)
    bc_pairs = [(a, b, t) for a, b, t in pairs if a["scope_level"] == "bc"]
    for a, b, t in bc_pairs:
        assert a["business_center_label"] == b["business_center_label"]
        assert t == "bc_to_project"
    # bc:2014 Container has a matching-bc Project population in this fixture
    # (proj_bc2014 also resolves to business_center_label "2014"); bc:2270
    # does not have a matching Project population, so it produces no pair.
    assert len(bc_pairs) == 1


def test_directed_bc_to_project_matches_regardless_of_differing_client():
    # Regression: a bc-scoped Container (Stantec-internal, real bc) must
    # still pair with an external-client Project that shares that bc — an
    # exact scope_key match would wrongly exclude this, since the Container's
    # scope_key is "bc:2014" while an external-client project in that same
    # bc is "project:Acme:2014". Mirrors compare_cross_segment.py's
    # discover_governance_chain() bc_standards loop, which matches Project
    # rows by _bc_of() alone, ignoring client_label entirely.
    rows = [
        _row("bc1", "Container", "Stantec", "2014"),
        _row("proj_ext", "Project", "Acme", "2014"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    pairs = discover_directed_tc_to_project_pairs(manifest_rows)
    types = {t for _a, _b, t in pairs}
    assert "bc_to_project" in types
    bc_pair = next((a, b, t) for a, b, t in pairs if t == "bc_to_project")
    assert bc_pair[0]["scope_key"] == "bc:2014"
    assert bc_pair[1]["scope_key"] == "project:Acme:2014"


def test_directed_client_to_project_matches_by_client_label_alone():
    # Symmetric case to the bc fix above: a client-scoped Container/Template
    # (external client, no real bc — enterprise-bookkeeping tag) should pair
    # with any Project sharing that client, regardless of the project's own
    # business_center_label.
    rows = [
        _row("cl1", "Template", "Acme", "0000"),
        _row("proj_bc", "Project", "Acme", "2270"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    pairs = discover_directed_tc_to_project_pairs(manifest_rows)
    assert len(pairs) == 1
    a, b, t = pairs[0]
    assert t == "client_to_project"
    assert a["scope_key"] == "client:Acme"
    assert b["scope_key"] == "project:Acme:2270"


def test_files_with_no_inventory_for_domain_are_excluded_not_zero_padded():
    # Regression: a file with zero records for a domain must not be counted
    # in n_files/n_pairs or contribute a spurious zero-overlap pair that
    # drags the Jaccard/containment mean toward 0.
    rows = [
        _row("a1", "Container", "Stantec", "2014"),
        _row("a2", "Container", "Stantec", "2014"),
        _row("b1", "Container", "Stantec", "2270"),
        _row("b2", "Container", "Stantec", "2270"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded

    records_rows = [
        {"export_run_id": "a1", "domain": "domain_x", "join_hash": "h1"},
        # a2 has no records for domain_x at all.
        {"export_run_id": "b1", "domain": "domain_x", "join_hash": "h1"},
        {"export_run_id": "b2", "domain": "domain_x", "join_hash": "h1"},
    ]
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)
    bc_bc_rows = [r for r in out_rows if r["comparison_type"] == "bc_to_bc"]
    assert len(bc_bc_rows) == 1
    row = bc_bc_rows[0]
    assert row["n_files_a"] == "1"
    assert float(row["all_pairwise_jaccard_mean"]) == 1.0


def test_zero_inventory_domain_produces_no_row():
    # If neither population has any file with inventory for a domain, no row
    # should be emitted for that (pair, domain) at all -- a row of blank/
    # zero aggregates would just be noise.
    rows = [
        _row("a1", "Container", "Stantec", "2014"),
        _row("b1", "Container", "Stantec", "2270"),
    ]
    manifest_rows, membership_rows, excluded = build_governance_populations(rows)
    assert not excluded
    records_rows = [
        {"export_run_id": "a1", "domain": "domain_y", "join_hash": "h1"},
        # b1 has no records in any domain at all.
    ]
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)
    assert out_rows == []


def test_comparison_type_never_mixes_symmetric_and_directed_metric_shape():
    manifest_rows, membership_rows = _synthetic_manifest()
    records_rows = _records_rows(_JH_MAP)
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)

    shape_by_type = {}
    for row in out_rows:
        has_jaccard = bool(row.get("all_pairwise_jaccard_mean"))
        ctype = row["comparison_type"]
        if ctype not in shape_by_type:
            shape_by_type[ctype] = has_jaccard
        else:
            assert shape_by_type[ctype] == has_jaccard, (
                f"comparison_type {ctype!r} mixes symmetric and directed rows"
            )


def test_run_comparisons_end_to_end_type_coverage():
    manifest_rows, membership_rows = _synthetic_manifest()
    records_rows = _records_rows(_JH_MAP)
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)
    types = {row["comparison_type"] for row in out_rows}
    expected_minimum = {
        "bc_to_bc", "bc_to_client", "client_to_client",
        "enterprise_to_bc", "enterprise_to_client",
        "enterprise_to_project", "bc_to_project",
        "generic_to_container", "generic_to_project",
    }
    assert expected_minimum <= types
    for row in out_rows:
        assert row["n_files_a"] != ""
        assert row["n_files_b"] != ""
