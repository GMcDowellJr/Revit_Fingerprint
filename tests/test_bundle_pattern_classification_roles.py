from __future__ import annotations

import csv
from pathlib import Path

from tools.bundle_analysis.step5_classify_patterns import emit_stub


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating(tmp_path: Path) -> None:
    domain = "units"
    domain_dir = tmp_path / domain

    _write_csv(
        domain_dir / "bundle_membership.csv",
        ["schema_version", "analysis_run_id", "scope_key", "bundle_id", "pattern_id"],
        [
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "root", "pattern_id": "p_foundation"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "mid", "pattern_id": "p_foundation"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "root", "pattern_id": "p_diff"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "mid", "pattern_id": "p_diff"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "leaf", "pattern_id": "p_diff"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "leaf2", "pattern_id": "p_other"},
            {"schema_version": "1", "analysis_run_id": "run-1", "scope_key": "s1", "bundle_id": "mid", "pattern_id": "p_intermediate"},
        ],
    )
    _write_csv(
        domain_dir / "bundle_dag_nodes.csv",
        ["scope_key", "bundle_id", "is_root", "is_leaf"],
        [
            {"scope_key": "s1", "bundle_id": "root", "is_root": "true", "is_leaf": "false"},
            {"scope_key": "s1", "bundle_id": "mid", "is_root": "false", "is_leaf": "false"},
            {"scope_key": "s1", "bundle_id": "leaf", "is_root": "false", "is_leaf": "true"},
            {"scope_key": "s1", "bundle_id": "leaf2", "is_root": "false", "is_leaf": "true"},
        ],
    )
    _write_csv(
        domain_dir / "bundle_dag_edges.csv",
        ["scope_key", "parent_bundle_id", "child_bundle_id"],
        [
            {"scope_key": "s1", "parent_bundle_id": "root", "child_bundle_id": "mid"},
            {"scope_key": "s1", "parent_bundle_id": "mid", "child_bundle_id": "leaf"},
            {"scope_key": "s1", "parent_bundle_id": "mid", "child_bundle_id": "leaf2"},
        ],
    )
    _write_csv(
        domain_dir / "membership_matrix.csv",
        ["scope_key", "pattern_id", "export_run_id"],
        [
            {"scope_key": "s1", "pattern_id": "p_foundation", "export_run_id": "f1"},
            {"scope_key": "s1", "pattern_id": "p_diff", "export_run_id": "f1"},
            {"scope_key": "s1", "pattern_id": "p_intermediate", "export_run_id": "f1"},
        ],
    )
    _write_csv(
        domain_dir / "scope_registry.csv",
        ["scope_key", "files_in_scope"],
        [{"scope_key": "s1", "files_in_scope": "1"}],
    )

    emit_stub(tmp_path, domain)

    with (domain_dir / "pattern_bundle_classification.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    by_pattern = {row["pattern_id"]: row["bundle_role"] for row in rows}
    assert by_pattern["p_foundation"] == "foundation"
    assert by_pattern["p_diff"] == "differentiating"
    assert by_pattern["p_intermediate"] == "intermediate"
