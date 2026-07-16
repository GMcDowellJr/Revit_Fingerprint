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


def test_directed_bc_to_project_requires_matching_scope_key():
    manifest_rows, _membership_rows = _synthetic_manifest()
    pairs = discover_directed_tc_to_project_pairs(manifest_rows)
    bc_pairs = [(a, b, t) for a, b, t in pairs if a["scope_level"] == "bc"]
    for a, b, t in bc_pairs:
        assert a["scope_key"] == b["scope_key"]
        assert t == "bc_to_project"
    # bc:2014 Container has a matching-scope Project population in this
    # fixture (proj_bc2014 also resolves to scope_key "bc:2014"); bc:2270
    # does not have a matching Project population, so it produces no pair.
    assert len(bc_pairs) == 1


def test_comparison_type_never_mixes_symmetric_and_directed_metric_shape():
    manifest_rows, membership_rows = _synthetic_manifest()
    records_rows = _records_rows(_JH_MAP)
    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)

    shape_by_type = {}
    for row in out_rows:
        has_jaccard = bool(row.get("all_jaccard_mean"))
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
