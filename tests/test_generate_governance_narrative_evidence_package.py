"""Tests for the PR1 governance evidence-package layer wired into
tools/generate_governance_narrative.py: the corrected producer-identity
footer, the new evidence-authority narrative header, the comparison_type
coverage hook, and the governance_package_manifest.json /
governance_package_health.json / governance_evidence_map.json outputs.

See docs/governance_evidence_package.md and
docs/governance_narrative_scope_gap_audit.md.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS, POOLED_FIELDS  # noqa: E402
from governance_evidence_package import GENERATOR_IDENTITY  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    CASCADE_GROUP1_TYPES,
    CASCADE_GROUP2_TYPES,
    CASCADE_GROUP3_TYPES,
    CASCADE_GROUP4_EXCLUDED_TYPES,
    _comparison_type_coverage,
    _DIRECTED_GOVERNANCE_TYPES,
    main,
    render_evidence_authority_header,
    render_limitations,
)


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
            all_containment_a_in_b_mean="0.8", all_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10", data_sufficient="true",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(
            comparison_run_id="run1", segment_id="imperial|Project|acme",
            client_label="acme", governance_role="Project", pool_scope="parent_sibling",
            domain="line_styles", n_files_focal="10", n_files_pool="30",
            data_sufficient="true", executed_utc="2026-07-16T00:00:00Z",
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


# ---------------------------------------------------------------------------
# Footer / producer identity
# ---------------------------------------------------------------------------

def test_footer_references_real_generator_identity_not_stale_filename():
    md = render_limitations({"Project": 10})
    assert "generate_governance_narrative_dod_aligned_v2.py" not in md
    assert f"`{GENERATOR_IDENTITY}`" in md
    assert GENERATOR_IDENTITY == "generate_governance_narrative.py"


# ---------------------------------------------------------------------------
# Authority header
# ---------------------------------------------------------------------------

def test_authority_header_states_controlled_interpretation_and_no_llm():
    md = render_evidence_authority_header("1.0", GENERATOR_IDENTITY)
    assert "controlled_interpretation" in md
    assert "no LLM is involved" in md
    assert "governance_package_health.json" in md
    assert "governance_evidence_map.json" in md


def test_authority_header_inserted_between_header_and_state_model(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    idx_header = md.index("## Executive Summary")
    idx_authority = md.index("**Artifact role:**")
    idx_state_model = md.index("## Governance State Model") if "## Governance State Model" in md else len(md)
    assert idx_header < idx_authority
    assert idx_authority < idx_state_model


# ---------------------------------------------------------------------------
# comparison_type coverage -- backward-compatible refactor
# ---------------------------------------------------------------------------

def test_comparison_type_coverage_matches_known_cascade_groups():
    known = CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
    cov = _comparison_type_coverage({"template_to_project", "bogus_type"}, known,
                                     intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()))
    assert cov["unrecognized"] == ["bogus_type"]
    assert "template_to_project" in cov["recognized"]


def test_comparison_type_coverage_governance_state_uses_directed_types():
    cov = _comparison_type_coverage({"generic_to_template"}, _DIRECTED_GOVERNANCE_TYPES)
    assert cov["unrecognized"] == []


def test_unrecognized_comparison_type_still_warns_to_stderr(capsys, tmp_path, monkeypatch):
    """Locks in that the _warn_unrecognized_comparison_types refactor (adding a
    return value) did not remove its existing stderr side effect."""
    summary_rows = [
        _summary_row(
            comparison_run_id="run1", segment_id_a="imperial|Template",
            segment_id_b="imperial|Project|acme", governance_role_a="Template",
            governance_role_b="Project", client_label_b="acme",
            comparison_type="totally_bogus_comparison_type", domain="line_styles",
            all_containment_a_in_b_mean="0.8", all_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10", data_sufficient="true",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(comparison_run_id="run1", segment_id="imperial|Project|acme",
                    client_label="acme", governance_role="Project", pool_scope="parent_sibling",
                    domain="line_styles", n_files_focal="10", n_files_pool="30",
                    data_sufficient="true", executed_utc="2026-07-16T00:00:00Z"),
    ]
    summary_path = tmp_path / "cross_segment_summary.csv"
    pooled_path = tmp_path / "cross_segment_pooled.csv"
    _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    captured = capsys.readouterr()
    assert "unrecognized comparison_type" in captured.err
    assert "totally_bogus_comparison_type" in captured.err

    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert "totally_bogus_comparison_type" in health["comparison_type_coverage"]["build_cascade"]["unrecognized"]


# ---------------------------------------------------------------------------
# Regression lock: PR1 must not change classification output
# ---------------------------------------------------------------------------

_EXPECTED_DOMAIN_COLUMNS = [
    "domain", "domain_label", "governance_tier", "score_reliability",
    "cascade_generic_to_template", "cascade_generic_to_container", "cascade_generic_to_project",
    "template_to_container", "container_to_project", "template_to_project",
    "cross_client_convergence", "within_project_all", "within_project_p10", "within_project_p90",
    "within_project_spread", "within_project_architectural", "within_project_mechanical_plumbing",
    "within_project_electrical", "within_project_structural", "bundle_schema",
    "template_to_project_used", "bundle_share_all", "bundle_share_used",
    "passive_inheritance_indicator", "passive_indicator_method", "passive_inheritance_risk",
    "generic_to_template", "generic_to_container", "generic_to_project",
    "provided_to_configured_containment", "provided_to_used_containment", "provided_passive_share",
    "provided_missing_share", "local_active_share", "provided_and_used_count",
    "provided_but_passive_count", "provided_but_missing_count", "local_active_count",
    "local_passive_count", "local_unbundled_count", "primary_governance_read", "notable_anomalies",
]

_EXPECTED_CLIENT_COLUMNS = [
    "client", "n_project_files", "alignment_tier", "cross_client_similarity_mean",
    "within_project_coherence", "confidence_note", "most_aligned_domains", "least_aligned_domains",
    "onboarding_internal_read", "onboarding_portability_read", "onboarding_common_base",
    "onboarding_variant_burden", "onboarding_operating_implication",
]


def test_domain_csv_column_set_unchanged(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    with open(tmp_path / "governance_domain_summary.csv", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == _EXPECTED_DOMAIN_COLUMNS


def test_client_csv_column_set_unchanged(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    with open(tmp_path / "governance_client_summary.csv", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == _EXPECTED_CLIENT_COLUMNS


# ---------------------------------------------------------------------------
# Evidence-package emission (default on)
# ---------------------------------------------------------------------------

def test_emit_evidence_package_default_writes_three_json_files(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_package_manifest.json").exists()
    assert (tmp_path / "governance_package_health.json").exists()
    assert (tmp_path / "governance_evidence_map.json").exists()


def test_no_emit_evidence_package_suppresses_json_but_not_existing_outputs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_package_manifest.json").exists()
    assert not (tmp_path / "governance_package_health.json").exists()
    assert not (tmp_path / "governance_evidence_map.json").exists()
    assert (tmp_path / "governance_domain_summary.csv").exists()
    assert (tmp_path / "governance_client_summary.csv").exists()
    assert (tmp_path / "governance_narrative_context.md").exists()


def test_emit_and_no_emit_produce_identical_csv_and_md_content(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    out_a.mkdir()
    out_b.mkdir()
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(out_a)])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(out_b), "--no-emit-evidence-package"])
    for name in ("governance_domain_summary.csv", "governance_client_summary.csv", "governance_narrative_context.md"):
        assert (out_a / name).read_bytes() == (out_b / name).read_bytes(), name


def test_package_manifest_records_inputs_and_outputs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
    assert by_id["summary"]["present"] is True
    assert by_id["union_inventory"]["present"] is False
    out_by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
    assert out_by_id["domain_summary_csv"]["size_bytes"] > 0
    assert manifest["generator"]["name"] == GENERATOR_IDENTITY
    assert manifest["package_status"] == "complete"


def test_package_manifest_reports_sibling_json_outputs_as_present_with_real_sizes(tmp_path, monkeypatch):
    """Regression test for a PR review finding: the manifest is built (and
    stats its output_paths) after governance_package_health.json and
    governance_evidence_map.json are already written to disk, so it must not
    report them as present=False/size_bytes=None. The manifest also does not
    describe its own file (see build_package_manifest's manifest_output_paths
    exclusion in main()) -- self-description is governance_evidence_map.json's
    job, not the manifest's."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    out_by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
    assert "package_manifest_json" not in out_by_id
    for artifact_id in ("package_health_json", "evidence_map_json"):
        assert out_by_id[artifact_id]["present"] is True, artifact_id
        assert out_by_id[artifact_id]["size_bytes"] > 0, artifact_id
        expected_path = tmp_path / {
            "package_health_json": "governance_package_health.json",
            "evidence_map_json": "governance_evidence_map.json",
        }[artifact_id]
        assert out_by_id[artifact_id]["size_bytes"] == expected_path.stat().st_size


def test_package_manifest_records_comparison_run_ids(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert manifest["corpus_scope"]["comparison_run_ids"] == ["run1"]
    assert manifest["corpus_scope"]["source_executed_utc"] == ["2026-07-16T00:00:00Z"]


def test_package_health_schema_detection_dual_for_dual_view_rows(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["schema_detection"] == "dual"
    assert health["used_view_fallback"] is False
    assert health["overall_status"] == "complete"


def test_package_health_optional_inputs_present_reflects_cli_flags(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    file_meta_path = tmp_path / "file_metadata.csv"
    _write_csv(file_meta_path, ["governance_role", "client_label", "discipline_label"], [])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--file-meta", str(file_meta_path)])
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["optional_inputs"]["file_meta"] is True
    assert health["optional_inputs"]["union_inventory"] is False


def test_evidence_map_lists_eighteen_artifacts_with_required_fields(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    ids = [a["artifact_id"] for a in evidence_map["artifacts"]]
    assert len(ids) == 18
    assert len(ids) == len(set(ids))
    narrative = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_narrative_context")
    assert narrative["authority_level"] != "authoritative_deterministic_evidence"


def test_cli_accepts_policy_dir_and_package_schema_version_as_inert(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    policy_dir = tmp_path / "some_policy_dir"
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--policy-dir", str(policy_dir),
                            "--package-schema-version", "2.0"])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert manifest["policy_profiles"]["policy_dir"] == str(policy_dir)
    assert manifest["package_schema_version"] == "2.0"
    # No crash, and the domain/client CSV outputs are still produced normally.
    assert (tmp_path / "governance_domain_summary.csv").exists()
