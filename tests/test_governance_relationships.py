from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.governance_relationships import (
    build_relationships_rows,
    build_bc_client_matrix_rows,
    build_client_bc_matrix_rows,
)


def _row(export_run_id, client, bc, project_label, role="Project", discipline="architectural", unit="imperial"):
    return {
        "export_run_id": export_run_id,
        "client_label": client,
        "business_center_label": bc,
        "project_label": project_label,
        "governance_role": role,
        "discipline_label": discipline,
        "unit_system": unit,
    }


def _find(rows, **kwargs):
    matches = [r for r in rows if all(r.get(k) == v for k, v in kwargs.items())]
    assert len(matches) == 1, f"expected exactly one match for {kwargs}, got {len(matches)}: {matches}"
    return matches[0]


# ---------------------------------------------------------------------------
# build_relationships_rows
# ---------------------------------------------------------------------------

def test_non_project_roles_excluded():
    rows = [
        _row("f1", "Sutter", "2014", "Alpha", role="Project"),
        _row("f2", "Stantec", "2014", "n/a", role="Template"),
        _row("f3", "Stantec", "2014", "n/a", role="Container"),
    ]
    out, warnings = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["client_label"] == "Sutter"
    assert warnings == []


def test_lowercase_governance_role_still_counted_as_project():
    # Accepted manual-entry variant (matches governance_manifest.py's own
    # case-insensitive _governance_role_key() convention) -- must not be
    # silently dropped from the roster.
    rows = [_row("f1", "Sutter", "2014", "Alpha", role="project")]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["project_file_count"] == "1"


def test_client_label_casing_variants_fold_to_one_project():
    rows = [
        _row("f1", "Sutter", "2014", "Alpha", role="Project"),
        _row("f2", "sutter", "2014", "Alpha", role="Project"),
    ]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["client_label"] == "Sutter"  # first-seen casing
    assert out[0]["project_file_count"] == "2"


def test_bc_prefix_variant_folds_to_same_bc_identity():
    # "BC_2014" vs "2014" must collapse to one business_center_label, or the
    # same real BC would fragment into two separate BC/client matrix rows.
    rows = [
        _row("f1", "Sutter", "BC_2014", "Alpha", role="Project"),
        _row("f2", "Sutter", "2014", "Alpha", role="Project"),
    ]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["business_center_label"] == "2014"
    assert out[0]["project_file_count"] == "2"


def test_excel_collapsed_bc_folds_to_same_bc_identity():
    # PR #425 follow-up: "796" (Excel-collapsed from "0796") must fold
    # together with correctly-formatted "0796" rows via governance_manifest.
    # py's shared normalize_business_center_label(), the same as the
    # existing BC_-prefix case above.
    rows = [
        _row("f1", "Sutter", "0796", "Alpha", role="Project"),
        _row("f2", "Sutter", "796", "Alpha", role="Project"),
    ]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["business_center_label"] == "0796"
    assert out[0]["project_file_count"] == "2"


def test_same_project_label_different_client_stays_distinct():
    # Real production case: "MPMC" appears under two unrelated clients in the
    # same BC. Identity must be (client, bc, project_label), not project_label
    # alone, or these would silently merge into one project's file count.
    rows = [
        _row("f1", "Sutter", "2014", "MPMC"),
        _row("f2", "Sutter", "2014", "MPMC"),
        _row("f3", "Duke LifePoint", "2014", "MPMC"),
    ]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 2
    sutter_mpmc = _find(out, client_label="Sutter", business_center_label="2014")
    duke_mpmc = _find(out, client_label="Duke LifePoint", business_center_label="2014")
    assert sutter_mpmc["project_name"] == "MPMC"
    assert sutter_mpmc["project_file_count"] == "2"
    assert duke_mpmc["project_name"] == "MPMC"
    assert duke_mpmc["project_file_count"] == "1"
    assert sutter_mpmc["project_id"] != duke_mpmc["project_id"]


def test_blank_project_label_falls_back_to_export_run_id_per_file():
    rows = [
        _row("f1", "Sutter", "2014", "__NOT_APPLICABLE__"),
        _row("f2", "Sutter", "2014", ""),
    ]
    out, _ = build_relationships_rows(rows)
    # Each unlabeled file becomes its own single-file project (mirrors
    # compare_cross_segment.py's _project_label_for_file() convention) --
    # not pooled together under one blank bucket.
    assert len(out) == 2
    names = {r["project_name"] for r in out}
    assert names == {"f1", "f2"}
    for r in out:
        assert r["project_name_is_fallback"] == "true"
        assert r["project_file_count"] == "1"


def test_multi_discipline_project_collects_sorted_discipline_list():
    rows = [
        _row("f1", "Sutter", "2014", "Alpha", discipline="architectural"),
        _row("f2", "Sutter", "2014", "Alpha", discipline="structural"),
        _row("f3", "Sutter", "2014", "Alpha", discipline="electrical"),
    ]
    out, _ = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["discipline_labels"] == "architectural|electrical|structural"
    assert out[0]["project_file_count"] == "3"


def test_enterprise_bookkeeping_bc_token_blanked_not_carried_as_fake_bc():
    # governance_manifest.py's compute_scope_key() treats "0000"/"BC_0000" as
    # "no real business center," regardless of client -- this must not leak a
    # literal "0000" business center into governance_relationships.csv.
    rows = [
        _row("f1", "Sutter", "0000", "Alpha", role="Project"),
        _row("f2", "Kaiser", "BC_0000", "Beta", role="Project"),
    ]
    out, warnings = build_relationships_rows(rows)
    assert len(out) == 2
    for r in out:
        assert r["business_center_label"] == ""
    assert any("enterprise-bookkeeping" in w for w in warnings)


def test_enterprise_bookkeeping_project_excluded_from_bc_client_matrix():
    rows = [
        _row("f1", "Sutter", "0000", "Alpha", role="Project"),
        _row("f2", "Sutter", "2014", "Beta", role="Project"),
    ]
    relationship_rows, _ = build_relationships_rows(rows)
    matrix = build_bc_client_matrix_rows(relationship_rows)
    # Only the real-BC project appears; the blank-BC one is excluded entirely,
    # not rendered as a fake "" business center row.
    assert len(matrix) == 1
    assert matrix[0]["business_center_label"] == "2014"
    assert matrix[0]["percentage_of_bc"] == "1.000000"
    assert matrix[0]["percentage_of_client"] == "1.000000"


def test_inconsistent_unit_system_within_one_project_warns_not_raises():
    rows = [
        _row("f1", "Sutter", "2014", "Alpha", unit="imperial"),
        _row("f2", "Sutter", "2014", "Alpha", unit="metric"),
    ]
    out, warnings = build_relationships_rows(rows)
    assert len(out) == 1
    assert out[0]["project_file_count"] == "2"
    assert len(warnings) == 1
    assert "Alpha" in warnings[0]


# ---------------------------------------------------------------------------
# build_bc_client_matrix_rows / build_client_bc_matrix_rows
# ---------------------------------------------------------------------------

def _rel_row(client, bc, n_files):
    return {
        "project_id": f"proj_{client}_{bc}_{n_files}",
        "client_label": client,
        "business_center_label": bc,
        "project_file_count": str(n_files),
    }


def test_percentage_of_bc_and_client_single_bc_per_client():
    relationship_rows = [
        _rel_row("Sutter", "2014", 62),
        _rel_row("Renown", "2014", 41),
    ]
    matrix = build_bc_client_matrix_rows(relationship_rows)
    sutter = _find(matrix, client_label="Sutter")
    assert sutter["percentage_of_bc"] == f"{62/103:.6f}"
    assert sutter["percentage_of_client"] == "1.000000"


def test_percentage_of_client_sums_to_one_across_multiple_bcs():
    # Synthetic multi-BC client: real corpus supplied for this pass has no
    # client spanning more than one BC (client_cross_bc = 0 rows), so this
    # exercises the aggregation path the real data doesn't reach.
    relationship_rows = [
        _rel_row("Sutter", "2014", 60),
        _rel_row("Sutter", "1779", 40),
        _rel_row("Renown", "2014", 41),
    ]
    matrix = build_bc_client_matrix_rows(relationship_rows)
    sutter_rows = [r for r in matrix if r["client_label"] == "Sutter"]
    assert len(sutter_rows) == 2
    total_pct = sum(float(r["percentage_of_client"]) for r in sutter_rows)
    assert abs(total_pct - 1.0) < 1e-9

    client_matrix = build_client_bc_matrix_rows(matrix)
    sutter = _find(client_matrix, client_label="Sutter")
    assert sutter["business_center_count"] == "2"
    # Ordered by percentage_of_client descending -- 2014 (60/100) before 1779 (40/100).
    assert sutter["business_centers"] == "2014|1779"
    assert sutter["project_file_count"] == "100"


def test_client_bc_matrix_never_recomputes_percentage_it_only_sums_counts():
    relationship_rows = [_rel_row("Sutter", "2014", 62), _rel_row("Renown", "2014", 41)]
    matrix = build_bc_client_matrix_rows(relationship_rows)
    client_matrix = build_client_bc_matrix_rows(matrix)
    keys_present = {k for r in client_matrix for k in r.keys()}
    assert "percentage_of_bc" not in keys_present
    assert "percentage_of_client" not in keys_present
