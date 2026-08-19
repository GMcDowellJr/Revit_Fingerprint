"""Tests for D-032's comparison-registry input-completeness note:
build_comparison_completeness(), the --comparison-registry CLI wiring, and
governance_package_health.json's comparison_registry present/absent +
comparison_completeness gating. See DECISIONS.md D-032 and
docs/governance_generator_cross_compare_coverage.md.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import (  # noqa: E402
    SUMMARY_FIELDS, POOLED_FIELDS, COMPARISON_REGISTRY_FIELDS, GOVERNANCE_STATE_SUMMARY_FIELDS,
)
import generate_governance_narrative as g  # noqa: E402


def _summary_row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _pooled_row(**overrides):
    r = {f: "" for f in POOLED_FIELDS}
    r.update(overrides)
    return r


def _registry_row(**overrides):
    r = {f: "" for f in COMPARISON_REGISTRY_FIELDS}
    r.update(overrides)
    return r


def _gov_state_summary_row(**overrides):
    r = {f: "" for f in GOVERNANCE_STATE_SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _write_csv(path: Path, fields: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _minimal_fixture(tmp_path: Path) -> tuple[Path, Path]:
    summary_rows = [
        _summary_row(
            comparison_run_id="run1", segment_id_a="imperial|Template",
            segment_id_b="imperial|Project|acme", governance_role_a="Template",
            governance_role_b="Project", client_label_b="acme",
            comparison_type="template_to_project", domain="line_styles",
            all_pairwise_containment_a_in_b_mean="0.6", all_pairwise_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(
            comparison_run_id="run1", segment_id="imperial|Project|acme",
            client_label="acme", governance_role="Project", pool_scope="parent_sibling",
            domain="line_styles", n_files_focal="10", n_files_pool="30",
            executed_utc="2026-07-16T00:00:00Z",
        ),
    ]
    summary_path = tmp_path / "cross_segment_summary.csv"
    pooled_path = tmp_path / "cross_segment_pooled.csv"
    _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)
    return summary_path, pooled_path


def _run_main(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["generate_governance_narrative.py"] + argv)
    g.main()


# ---------------------------------------------------------------------------
# build_comparison_completeness()
# ---------------------------------------------------------------------------

def test_completeness_present_when_registry_row_matches():
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                  domain="line_styles", executed_utc="2026-08-01T00:00:00Z")]
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness(summary_rows, registry_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 0}


def test_completeness_missing_when_no_matching_registry_row():
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                  domain="line_styles", executed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness(summary_rows, [])
    assert completeness["line_styles"] == {"total": 1, "present": 0, "missing": 1, "stale": 0}


def test_completeness_stale_when_registry_computed_utc_predates_summary_executed_utc():
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                  domain="line_styles", executed_utc="2026-08-10T00:00:00Z")]
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="line_styles", computed_utc="2026-07-01T00:00:00Z")]
    completeness = g.build_comparison_completeness(summary_rows, registry_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 1}


def test_completeness_not_stale_when_registry_computed_utc_is_current():
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                  domain="line_styles", executed_utc="2026-08-10T00:00:00Z")]
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="line_styles", computed_utc="2026-08-10T00:00:00Z")]
    completeness = g.build_comparison_completeness(summary_rows, registry_rows)
    assert completeness["line_styles"]["stale"] == 0


def test_completeness_counts_registry_only_entry_as_present_and_stale():
    """PR review finding: a registry stamp with no matching summary row (the
    current summary snapshot doesn't reflect it, e.g. a domain-scoped run
    that didn't recompute everything a broader prior run did) must not be
    invisible -- it's counted as present (the registry did stamp it) and
    stale (out of sync with the current evidence)."""
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness([], registry_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 1}


def test_completeness_registry_only_entry_uses_registry_own_domain_for_grouping():
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="materials", computed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness([], registry_rows)
    assert "materials" in completeness
    assert "line_styles" not in completeness


def test_completeness_registry_only_entry_with_matching_state_evidence_is_not_stale():
    """PR review finding: compare_cross_segment.py legitimately stamps
    comparison_registry.csv for directed work below --min-patterns that
    produced governance-state output but no cross_segment_summary.csv row.
    A registry-only entry matching governance-state evidence (segment_id_
    reference/_target mapped to segment_id_a/b) must not be flagged stale."""
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="generic_to_template",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z")]
    state_summary_rows = [_gov_state_summary_row(segment_id_reference="a", segment_id_target="b",
                                                  comparison_type="generic_to_template", domain="line_styles")]
    completeness = g.build_comparison_completeness([], registry_rows, None, state_summary_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 0}


def test_completeness_registry_only_entry_without_state_evidence_is_still_stale():
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="generic_to_template",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness([], registry_rows, None, None)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 1}


def test_completeness_registry_only_entry_with_state_evidence_is_stale_when_registry_predates_state():
    """PR review finding: an independently supplied registry and state CSV
    can come from different runs. A registry-only entry matching
    governance-state evidence must still be checked for recency against
    that state row's own executed_utc, not unconditionally treated as
    current just because a matching state key exists."""
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="generic_to_template",
                                    domain="line_styles", computed_utc="2026-07-01T00:00:00Z")]
    state_summary_rows = [_gov_state_summary_row(segment_id_reference="a", segment_id_target="b",
                                                  comparison_type="generic_to_template", domain="line_styles",
                                                  executed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness([], registry_rows, None, state_summary_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 1}


def test_completeness_registry_only_entry_with_state_evidence_not_stale_when_registry_is_current():
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="generic_to_template",
                                    domain="line_styles", computed_utc="2026-08-10T00:00:00Z")]
    state_summary_rows = [_gov_state_summary_row(segment_id_reference="a", segment_id_target="b",
                                                  comparison_type="generic_to_template", domain="line_styles",
                                                  executed_utc="2026-08-01T00:00:00Z")]
    completeness = g.build_comparison_completeness([], registry_rows, None, state_summary_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 1, "missing": 0, "stale": 0}


def test_completeness_state_evidence_from_detailed_rows_also_prevents_stale():
    from compare_cross_segment import GOVERNANCE_STATE_FIELDS

    def _gov_state_row(**overrides):
        r = {f: "" for f in GOVERNANCE_STATE_FIELDS}
        r.update(overrides)
        return r

    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="generic_to_template",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z")]
    state_rows = [_gov_state_row(segment_id_reference="a", segment_id_target="b",
                                  comparison_type="generic_to_template", domain="line_styles")]
    completeness = g.build_comparison_completeness([], registry_rows, state_rows, None)
    assert completeness["line_styles"]["stale"] == 0


def test_completeness_state_only_key_with_no_registry_or_summary_row_is_counted_missing():
    """PR review finding: a directed comparison that produced governance-state
    evidence but has no cross_segment_summary.csv row and no
    comparison_registry.csv stamp either (e.g. build_comparison_registry_rows()
    excludes it because a segment wasn't marked complete) was previously
    invisible to this function -- neither registry_index nor summary_index
    had its key, so the loop never visited it at all. It must be counted
    missing like any other unstamped work item, not silently dropped."""
    state_summary_rows = [_gov_state_summary_row(segment_id_reference="a", segment_id_target="b",
                                                  comparison_type="generic_to_template", domain="line_styles")]
    completeness = g.build_comparison_completeness([], [], None, state_summary_rows)
    assert completeness["line_styles"] == {"total": 1, "present": 0, "missing": 1, "stale": 0}


def test_completeness_ignores_rows_with_no_domain():
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project", domain="")]
    completeness = g.build_comparison_completeness(summary_rows, [])
    assert completeness == {}


def test_completeness_registry_content_never_appears_in_result():
    """Only derived counts, never registry row content (D-032: never
    embedded/reproduced)."""
    summary_rows = [_summary_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                  domain="line_styles", executed_utc="2026-08-01T00:00:00Z")]
    registry_rows = [_registry_row(segment_id_a="a", segment_id_b="b", comparison_type="template_to_project",
                                    domain="line_styles", computed_utc="2026-08-01T00:00:00Z",
                                    population_hash_a="SECRET_HASH_VALUE")]
    completeness = g.build_comparison_completeness(summary_rows, registry_rows)
    assert "SECRET_HASH_VALUE" not in json.dumps(completeness)


# ---------------------------------------------------------------------------
# CLI / main() wiring, governance_package_health.json
# ---------------------------------------------------------------------------

def test_health_reports_comparison_registry_absent_when_not_supplied(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["optional_inputs"]["comparison_registry"] is False
    assert "comparison_completeness" not in health


def test_health_reports_comparison_registry_present_and_completeness_when_supplied(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "my_registry.csv"
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [
        _registry_row(segment_id_a="imperial|Template", segment_id_b="imperial|Project|acme",
                      comparison_type="template_to_project", domain="line_styles",
                      computed_utc="2026-07-16T00:00:00Z"),
    ])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["optional_inputs"]["comparison_registry"] is True
    assert "comparison_completeness" in health
    assert health["comparison_completeness"]["line_styles"]["present"] == 1


def test_health_overall_status_degrades_when_comparison_completeness_has_gaps(tmp_path, monkeypatch):
    """PR review finding: overall_status was computed before
    comparison_completeness was attached to the health dict, so a package
    with missing/stale comparison-registry entries still reported
    'complete' -- a consumer following the health-first flow would wrongly
    conclude nothing limits interpretation."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "my_registry.csv"
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [])  # no rows -> the summary pair is "missing"
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["overall_status"] == "degraded"
    assert any(w["condition"] == "comparison_registry_gaps" for w in health["warnings"])


def test_health_overall_status_stays_complete_when_comparison_completeness_has_no_gaps(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "my_registry.csv"
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [
        _registry_row(segment_id_a="imperial|Template", segment_id_b="imperial|Project|acme",
                      comparison_type="template_to_project", domain="line_styles",
                      computed_utc="2026-07-16T00:00:00Z"),
    ])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["overall_status"] == "complete"
    assert not any(w["condition"] == "comparison_registry_gaps" for w in health["warnings"])


def test_evidence_map_comparison_registry_path_matches_explicit_flag(tmp_path, monkeypatch):
    """The evidence-map artifact and the health/manifest input tracking must
    resolve to the SAME path when --comparison-registry is explicitly
    supplied, not silently diverge."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "elsewhere" / "my_registry.csv"
    registry_path.parent.mkdir()
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [_registry_row()])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    ev_entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "comparison_registry")
    manifest_entry = next(i for i in manifest["inputs"] if i["artifact_id"] == "comparison_registry")
    assert ev_entry["present"] is True
    assert manifest_entry["present"] is True
    assert Path(ev_entry["path"]) == registry_path
    assert Path(manifest_entry["path"]) == registry_path


def test_narrative_omits_completeness_note_when_not_supplied(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    narrative = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "Input Completeness / Staleness" not in narrative


def test_narrative_includes_completeness_note_when_supplied_with_gap(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "my_registry.csv"
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [])  # no rows -> the one summary pair is "missing"
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    narrative = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "Input Completeness / Staleness" in narrative
    assert "line_styles" in narrative.split("Input Completeness / Staleness")[1][:1200]


def test_registry_content_never_reproduced_in_output_package(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    registry_path = tmp_path / "my_registry.csv"
    _write_csv(registry_path, COMPARISON_REGISTRY_FIELDS, [
        _registry_row(segment_id_a="imperial|Template", segment_id_b="imperial|Project|acme",
                      comparison_type="template_to_project", domain="line_styles",
                      computed_utc="2026-07-16T00:00:00Z", population_hash_a="SECRET_HASH_VALUE"),
    ])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--comparison-registry", str(registry_path)])
    for out_file in tmp_path.glob("*.json"):
        assert "SECRET_HASH_VALUE" not in out_file.read_text(encoding="utf-8"), out_file
    for out_file in tmp_path.glob("*.md"):
        assert "SECRET_HASH_VALUE" not in out_file.read_text(encoding="utf-8"), out_file
    for out_file in tmp_path.glob("*.csv"):
        if out_file.name == "my_registry.csv":
            continue
        assert "SECRET_HASH_VALUE" not in out_file.read_text(encoding="utf-8"), out_file
