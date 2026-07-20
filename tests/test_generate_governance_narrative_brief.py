"""Tests for PR4 (interpretation & routing split): render_governance_brief(),
the --emit-interpretation-layer CLI wiring, and the three new evidence-map
artifacts (governance_brief, governance_interpretation_guide,
governance_question_routes). See DECISIONS.md D-022 and
docs/governance_evidence_package.md.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS, POOLED_FIELDS  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    INTERPRETATION_GUIDE_PATH,
    QUESTION_ROUTES_PATH,
    INTERPRETATION_GUIDE_VERSION,
    QUESTION_ROUTES_VERSION,
    main,
    render_evidence_authority_header,
    render_governance_brief,
)


def _finding(finding_type, subject_id="line_styles", subject_type="domain", summary=None, status="supported"):
    return {
        "finding_id": "GF-000",
        "subject": {"type": subject_type, "id": subject_id},
        "finding_type": finding_type,
        "status": status,
        "origin": "deterministic_computation",
        "fidelity": "exact",
        "authority_level": "controlled_interpretation",
        "summary": summary or f"{subject_id} matched {finding_type}.",
        "support": [],
        "rule_ids": ["GOV-TEST"],
        "limits": [],
    }


_HEALTH_OK = {"overall_status": "complete", "warnings": []}
_CORPUS = {"Project": 42, "Template": 3, "Container": 2}


# ---------------------------------------------------------------------------
# render_governance_brief() -- pure unit tests
# ---------------------------------------------------------------------------

def test_brief_states_its_own_convenience_summary_role():
    md = render_governance_brief([], _HEALTH_OK, _CORPUS, "1.0")
    assert "Convenience summary" in md
    assert "not a new source of evidence" in md


def test_brief_points_at_interpretation_guide_and_question_routes():
    md = render_governance_brief([], _HEALTH_OK, _CORPUS, "1.0")
    assert INTERPRETATION_GUIDE_PATH.name in md
    assert QUESTION_ROUTES_PATH.name in md
    assert INTERPRETATION_GUIDE_VERSION in md
    assert QUESTION_ROUTES_VERSION in md


def test_brief_reports_package_health_and_corpus_counts():
    md = render_governance_brief([], _HEALTH_OK, _CORPUS, "1.0")
    assert "**complete**" in md
    assert "**42**" in md  # Project count


def test_brief_groups_findings_by_type_with_domain_label():
    findings = [_finding("strong_baseline_candidate", subject_id="line_styles")]
    md = render_governance_brief(findings, _HEALTH_OK, _CORPUS, "1.0")
    assert "## Strong baseline candidates (1)" in md
    assert "Line Styles" in md  # DOMAIN_LABELS.get("line_styles") -> "Line Styles"


def test_brief_omits_empty_sections():
    md = render_governance_brief([], _HEALTH_OK, _CORPUS, "1.0")
    assert "Strong baseline candidates" not in md
    assert "High-fragmentation" not in md


def test_brief_caps_long_lists_and_points_to_findings_json():
    findings = [
        _finding("high_fragmentation", subject_id=f"domain_{i}") for i in range(20)
    ]
    md = render_governance_brief(findings, _HEALTH_OK, _CORPUS, "1.0")
    assert "## High-fragmentation domains (20)" in md
    assert "...and 5 more -- see `governance_findings.json`." in md


def test_brief_lists_leadership_questions_as_numbered_list_not_findings():
    findings = [
        _finding("leadership_question", subject_id="governance_evidence_package",
                 subject_type="package", summary="Which baseline candidates should enter ratification review?",
                 status="question_not_claim"),
    ]
    md = render_governance_brief(findings, _HEALTH_OK, _CORPUS, "1.0")
    assert "## Leadership questions" in md
    assert "1. Which baseline candidates should enter ratification review?" in md


def test_brief_includes_low_client_coherence_section():
    findings = [_finding("low_client_coherence", subject_id="Acme", subject_type="client")]
    md = render_governance_brief(findings, _HEALTH_OK, _CORPUS, "1.0")
    assert "## Clients with low internal coherence (1)" in md
    assert "Acme" in md


def test_brief_does_not_recompute_only_consumes_passed_findings():
    """The brief must reflect exactly the findings list it was given -- no
    hidden recomputation from cascade/client_rows (which it isn't even
    passed), matching PR2's "consume, not recompute" discipline."""
    findings = [_finding("strong_baseline_candidate", subject_id="only_this_domain")]
    md = render_governance_brief(findings, _HEALTH_OK, _CORPUS, "1.0")
    assert "only_this_domain" in md
    assert "Structured findings this run: **1**" in md


# ---------------------------------------------------------------------------
# render_evidence_authority_header() pointer gating
# ---------------------------------------------------------------------------

def test_authority_header_points_to_brief_when_interpretation_layer_on():
    md = render_evidence_authority_header("1.0", "generate_governance_narrative.py",
                                          emit_evidence_package=True, emit_interpretation_layer=True)
    assert "governance_brief.md" in md


def test_authority_header_omits_brief_pointer_when_interpretation_layer_off():
    md = render_evidence_authority_header("1.0", "generate_governance_narrative.py",
                                          emit_evidence_package=True, emit_interpretation_layer=False)
    assert "governance_brief.md" not in md


def test_authority_header_always_points_to_static_docs():
    """Interpretation guide / question routes are static repo docs, not
    per-run outputs -- their pointers must appear regardless of either flag."""
    for emit_pkg in (True, False):
        for emit_interp in (True, False):
            md = render_evidence_authority_header("1.0", "generate_governance_narrative.py",
                                                  emit_evidence_package=emit_pkg,
                                                  emit_interpretation_layer=emit_interp)
            assert INTERPRETATION_GUIDE_PATH.name in md
            assert QUESTION_ROUTES_PATH.name in md


# ---------------------------------------------------------------------------
# Static docs exist on disk
# ---------------------------------------------------------------------------

def test_interpretation_guide_and_question_routes_docs_exist():
    assert INTERPRETATION_GUIDE_PATH.exists()
    assert QUESTION_ROUTES_PATH.exists()
    assert INTERPRETATION_GUIDE_PATH.read_text(encoding="utf-8").startswith("# Governance Interpretation Guide")
    assert QUESTION_ROUTES_PATH.read_text(encoding="utf-8").startswith("# Governance Question Routes")


# ---------------------------------------------------------------------------
# CLI / main() wiring
# ---------------------------------------------------------------------------

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
            all_pairwise_containment_a_in_b_mean="0.95", all_pairwise_jaccard_mean="0.5",
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
    main()


def test_default_invocation_writes_governance_brief(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_brief.md").exists()


def test_no_emit_interpretation_layer_suppresses_brief_but_not_findings(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    assert not (tmp_path / "governance_brief.md").exists()
    assert (tmp_path / "governance_findings.json").exists()
    assert (tmp_path / "governance_package_health.json").exists()


def test_no_emit_evidence_package_also_suppresses_brief(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_brief.md").exists()


def test_stale_brief_removed_when_interpretation_layer_turned_off_between_runs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_brief.md").exists()
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    assert not (tmp_path / "governance_brief.md").exists()


def test_stale_brief_removed_when_evidence_package_turned_off_between_runs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_brief.md").exists()
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_brief.md").exists()


def test_manifest_records_governance_brief_output(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
    assert by_id["governance_brief"]["present"] is True
    assert by_id["governance_brief"]["size_bytes"] > 0
    assert by_id["governance_brief"]["authority_level"] == "convenience_summary"


def test_manifest_omits_governance_brief_output_when_layer_off(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    ids = {o["artifact_id"] for o in manifest["outputs"]}
    assert "governance_brief" not in ids


def test_evidence_map_governance_brief_present_true_by_default(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    em = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["governance_brief"]["present"] is True
    assert by_id["governance_brief"]["path"] is not None


def test_evidence_map_governance_brief_absent_when_layer_off(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    em = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["governance_brief"]["present"] is False
    assert by_id["governance_brief"]["path"] is None


def test_evidence_map_static_docs_present_regardless_of_interpretation_layer_flag(tmp_path, monkeypatch):
    """The interpretation guide / question routes docs ship with the repo --
    their presence in the evidence map must not depend on
    --emit-interpretation-layer (that flag only controls the per-run brief)."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    em = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["governance_interpretation_guide"]["present"] is True
    assert by_id["governance_question_routes"]["present"] is True
    assert by_id["governance_interpretation_guide"]["authority_level"] == "controlled_interpretation"
    assert by_id["governance_question_routes"]["authority_level"] == "convenience_summary"


def test_evidence_map_related_artifacts_reference_valid_ids_including_pr4(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    em = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    ids = {a["artifact_id"] for a in em["artifacts"]}
    for a in em["artifacts"]:
        for related in a["related_artifacts"]:
            assert related in ids, f"{a['artifact_id']}.related_artifacts references unknown id {related!r}"
