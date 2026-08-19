"""Tests for D-034's static-doc copy-into-out behavior: main() copies the
four docs/governance/*.md reference docs into --out when present, and keeps
that copy in sync with source presence / --emit-evidence-package across
repeated runs over the same --out directory. See DECISIONS.md D-034.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS, POOLED_FIELDS  # noqa: E402
import generate_governance_narrative as g  # noqa: E402


def _summary_row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _pooled_row(**overrides):
    r = {f: "" for f in POOLED_FIELDS}
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


_DOC_CONSTANTS = (
    "INTERPRETATION_GUIDE_PATH", "QUESTION_ROUTES_PATH",
    "READING_ORDER_PATH", "CLASSIFICATION_RULES_PATH",
)


_MINIMAL_CORPUS = {
    "total": 1, "Template": 0, "Container": 0, "Project": 1,
    "disciplines": set(), "clients": set(),
}


def test_render_header_points_at_out_directory_copy_when_guide_will_be_copied():
    """PR review finding: the 'How to Read the Analysis' pointer named the
    guide by bare basename unconditionally, but the guide is only actually
    copied alongside the narrative inside main()'s emit_evidence_package
    branch -- with --no-emit-evidence-package the pointer named a file that
    would not exist beside the output. When the guide WILL be copied this
    run, the pointer should say so."""
    header = g.render_header("2026-08-19", _MINIMAL_CORPUS, False, False,
                              interpretation_guide_will_be_copied=True)
    assert "in this run's output directory" in header
    assert "docs/governance/" not in header


def test_render_header_points_at_repo_path_when_guide_will_not_be_copied():
    header = g.render_header("2026-08-19", _MINIMAL_CORPUS, False, False,
                              interpretation_guide_will_be_copied=False)
    assert "docs/governance/governance_interpretation_guide.md" in header
    assert "not included alongside this run's output" in header


def test_authority_header_points_at_out_directory_copy_when_guides_present():
    """PR review finding: render_evidence_authority_header() (the second
    renderer with these pointers) unconditionally named the guide/question-
    routes/reading-order docs by bare basename, on the theory that they're
    static repo docs that always exist -- but a reader of this document may
    only have --out, where the copy only happens inside main()'s
    emit_evidence_package branch and only for a source doc actually present
    on disk (same gap render_header() had)."""
    header = g.render_evidence_authority_header("record.v1", "test-generator", True, True)
    assert f"`{g.INTERPRETATION_GUIDE_PATH.name}`" in header
    assert f"`{g.QUESTION_ROUTES_PATH.name}`" in header
    assert f"`{g.READING_ORDER_PATH.name}`" in header
    assert "docs/governance/" not in header


def test_authority_header_points_at_repo_path_when_guides_absent(monkeypatch):
    missing_dir = Path("/does/not/exist")
    monkeypatch.setattr(g, "INTERPRETATION_GUIDE_PATH", missing_dir / "governance_interpretation_guide.md")
    monkeypatch.setattr(g, "QUESTION_ROUTES_PATH", missing_dir / "governance_question_routes.md")
    monkeypatch.setattr(g, "READING_ORDER_PATH", missing_dir / "governance_reading_order.md")
    header = g.render_evidence_authority_header("record.v1", "test-generator", True, True)
    assert "docs/governance/governance_interpretation_guide.md" in header
    assert "docs/governance/governance_question_routes.md" in header
    assert "docs/governance/governance_reading_order.md" in header
    assert "the source doc was not found on disk" in header


def test_authority_header_points_at_repo_path_when_evidence_package_disabled():
    header = g.render_evidence_authority_header("record.v1", "test-generator", False, True)
    assert "this run used --no-emit-evidence-package" in header


def test_default_run_copies_all_four_static_docs_into_out(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    for const_name in _DOC_CONSTANTS:
        src = getattr(g, const_name)
        assert (tmp_path / src.name).exists(), src.name


def test_narrative_pointer_matches_actual_guide_presence_end_to_end(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    narrative = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "in this run's output directory" in narrative
    assert (tmp_path / g.INTERPRETATION_GUIDE_PATH.name).exists()

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    narrative = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "not included alongside this run's output" in narrative
    assert not (tmp_path / g.INTERPRETATION_GUIDE_PATH.name).exists()


def test_health_degrades_end_to_end_when_interpretation_guide_source_absent(tmp_path, monkeypatch):
    """PR review finding: health had no signal for a missing
    required_before_conclusions artifact. Wired through main()'s
    build_package_health() call via INTERPRETATION_GUIDE_PATH.exists()."""
    missing_src = tmp_path / "does_not_exist" / "governance_interpretation_guide.md"
    monkeypatch.setattr(g, "INTERPRETATION_GUIDE_PATH", missing_src)
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    import json
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["overall_status"] == "degraded"
    assert any(w["condition"] == "reasoning_prerequisite_absent" for w in health["warnings"])


def test_no_emit_evidence_package_removes_previously_copied_docs(tmp_path, monkeypatch):
    """A prior run (default flags) copies the four docs in; a later run over
    the same --out with --no-emit-evidence-package must not leave them
    behind alongside a package that claims no evidence-package output."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    for const_name in _DOC_CONSTANTS:
        assert (tmp_path / getattr(g, const_name).name).exists()

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    for const_name in _DOC_CONSTANTS:
        assert not (tmp_path / getattr(g, const_name).name).exists(), const_name


def test_no_emit_evidence_package_does_not_delete_source_docs_when_out_is_docs_governance(tmp_path, monkeypatch):
    """P1 PR review finding: if --out resolves to the same directory the four
    static docs actually live in (e.g. --out docs/governance), the
    --no-emit-evidence-package stale-file cleanup must not delete the
    checked-in source documents -- those names only mean 'stale copy' when
    the destination is NOT the source itself."""
    fake_docs_dir = tmp_path / "docs_governance"
    fake_docs_dir.mkdir()
    real_guide_text = "# Real checked-in interpretation guide\n"
    for const_name in _DOC_CONSTANTS:
        (fake_docs_dir / getattr(g, const_name).name).write_text(real_guide_text, encoding="utf-8")
        monkeypatch.setattr(g, const_name, fake_docs_dir / getattr(g, const_name).name)

    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(fake_docs_dir), "--no-emit-evidence-package"])
    for const_name in _DOC_CONSTANTS:
        doc_path = fake_docs_dir / getattr(g, const_name).name
        assert doc_path.exists(), const_name
        assert doc_path.read_text(encoding="utf-8") == real_guide_text, const_name


def test_copy_removed_when_source_doc_absent_but_stale_copy_exists(tmp_path, monkeypatch):
    """A stripped-down deployment without docs/ (source absent) must not
    leave a stale copy from an earlier run, when a source WAS present, sitting
    in --out alongside this run's freshly-written narrative/CSVs."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    guide_dst = tmp_path / g.INTERPRETATION_GUIDE_PATH.name
    assert guide_dst.exists()

    missing_src = tmp_path / "does_not_exist" / "governance_interpretation_guide.md"
    monkeypatch.setattr(g, "INTERPRETATION_GUIDE_PATH", missing_src)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert not guide_dst.exists()
