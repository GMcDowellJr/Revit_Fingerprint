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

from compare_cross_segment import (  # noqa: E402
    SUMMARY_FIELDS, POOLED_FIELDS, DELTA_FIELDS, GOVERNANCE_STATE_SUMMARY_FIELDS,
    COMPARISON_REGISTRY_FIELDS, REUSE_SUMMARY_FIELDS, MATRIX_OUTPUT_FIELDS,
)
from governance_evidence_package import GENERATOR_IDENTITY  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    CASCADE_GROUP1_TYPES,
    CASCADE_GROUP2_TYPES,
    CASCADE_GROUP3_TYPES,
    CASCADE_GROUP3B_TYPES,
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


def _delta_row(**overrides):
    r = {f: "" for f in DELTA_FIELDS}
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
            all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
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
    known = (
        CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
        | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
    )
    cov = _comparison_type_coverage({"template_to_project", "bogus_type"}, known,
                                     intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()))
    assert cov["unrecognized"] == ["bogus_type"]
    assert "template_to_project" in cov["recognized"]


def test_bc_to_bc_and_client_cross_bc_are_registered_not_unrecognized():
    """Regression for a PR #373 review finding: compare_cross_segment.py's
    new bc_to_bc/client_cross_bc comparison types must be in the known set
    (like sibling_templates/sibling_containers), or a default run where
    they're emitted surfaces as unrecognized-comparison-type coverage
    degradation even though the producer intentionally emitted the rows.
    bc_to_bc has since moved from Group 4 (excluded) to Group 3b (captured
    into build_cascade(), still not rendered) -- see CASCADE_GROUP3B_TYPES;
    client_cross_bc remains Group-4-excluded."""
    known = (
        CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
        | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
    )
    cov = _comparison_type_coverage({"bc_to_bc", "client_cross_bc"}, known,
                                     intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()))
    assert cov["unrecognized"] == []
    assert "bc_to_bc" in cov["recognized"]
    assert "bc_to_bc" in CASCADE_GROUP3B_TYPES
    assert "bc_to_bc" not in CASCADE_GROUP4_EXCLUDED_TYPES
    assert "client_cross_bc" in cov["intentionally_excluded"]
    assert "client_cross_bc" in CASCADE_GROUP4_EXCLUDED_TYPES


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
            all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(comparison_run_id="run1", segment_id="imperial|Project|acme",
                    client_label="acme", governance_role="Project", pool_scope="parent_sibling",
                    domain="line_styles", n_files_focal="10", n_files_pool="30",
                    executed_utc="2026-07-16T00:00:00Z"),
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
    "template_to_container", "container_to_project",
    "container_to_project_scoped", "container_to_project_scoped_pair",
    "template_to_project",
    "cross_client_convergence", "cross_client_convergence_all_view",
    "within_project_all", "within_project_p10", "within_project_p90",
    "within_project_reliability_source",
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
    "cross_client_similarity_mean_all_view",
    "within_project_coherence", "within_project_coherence_all_view",
    "confidence_note", "most_aligned_domains", "least_aligned_domains",
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


def test_no_emit_removes_stale_evidence_package_files_from_prior_run(tmp_path, monkeypatch):
    """Regression test for a PR review finding: rerunning with
    --no-emit-evidence-package over an --out directory that already has
    package JSONs from an earlier default (emit-on) run must not leave those
    stale files in place -- the narrative just rendered says no package
    health/evidence-map file exists for this run, so leaving old ones would
    let a downstream reader pick up out-of-date provenance/health data."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_package_manifest.json").exists()
    assert (tmp_path / "governance_package_health.json").exists()
    assert (tmp_path / "governance_evidence_map.json").exists()

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_package_manifest.json").exists()
    assert not (tmp_path / "governance_package_health.json").exists()
    assert not (tmp_path / "governance_evidence_map.json").exists()
    assert (tmp_path / "governance_domain_summary.csv").exists()


def test_emit_and_no_emit_produce_identical_csvs(tmp_path, monkeypatch):
    """The two CSV outputs are unaffected by --emit-evidence-package -- only
    the narrative's authority-header package-pointer section differs (see
    test_no_emit_narrative_does_not_point_at_missing_package_files)."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    out_a.mkdir()
    out_b.mkdir()
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(out_a)])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(out_b), "--no-emit-evidence-package"])
    for name in ("governance_domain_summary.csv", "governance_client_summary.csv"):
        assert (out_a / name).read_bytes() == (out_b / name).read_bytes(), name


def test_no_emit_narrative_does_not_point_at_missing_package_files(tmp_path, monkeypatch):
    """Regression test for a PR review finding: the narrative's authority
    header unconditionally referenced governance_package_health.json/
    governance_findings.json/governance_evidence_map.json even when
    --no-emit-evidence-package means those files are never written."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "governance_package_health.json" not in md
    assert "governance_findings.json" not in md
    assert "governance_evidence_map.json" not in md
    assert "--no-emit-evidence-package" in md
    # The rest of the narrative (findings section, footer) is unaffected.
    assert "## Key Findings and Governance Questions" in md
    assert f"`{GENERATOR_IDENTITY}`" in md


def test_emit_narrative_points_at_package_files(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
    assert "governance_package_health.json" in md
    assert "governance_findings.json" in md
    assert "governance_evidence_map.json" in md


def test_package_manifest_records_inputs_and_outputs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
    assert by_id["cross_segment_summary"]["present"] is True
    assert by_id["cross_segment_union_inventory"]["present"] is False
    out_by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
    assert out_by_id["governance_domain_summary"]["size_bytes"] > 0
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
    assert "governance_package_manifest" not in out_by_id
    for artifact_id in ("governance_package_health", "governance_evidence_map"):
        assert out_by_id[artifact_id]["present"] is True, artifact_id
        assert out_by_id[artifact_id]["size_bytes"] > 0, artifact_id
        expected_path = tmp_path / {
            "governance_package_health": "governance_package_health.json",
            "governance_evidence_map": "governance_evidence_map.json",
        }[artifact_id]
        assert out_by_id[artifact_id]["size_bytes"] == expected_path.stat().st_size


def test_package_manifest_records_comparison_run_ids(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert manifest["corpus_scope"]["comparison_run_ids"] == ["run1"]
    assert manifest["corpus_scope"]["source_executed_utc"] == ["2026-07-16T00:00:00Z"]


def test_package_manifest_comparison_run_ids_include_pooled_only_values(tmp_path, monkeypatch):
    """If --summary and --pooled are accidentally taken from different runs,
    the manifest's provenance sets must surface both run ids / timestamps,
    not just summary's -- otherwise a mixed-run package looks single-run."""
    summary_rows = [
        _summary_row(
            comparison_run_id="run1", segment_id_a="imperial|Template",
            segment_id_b="imperial|Project|acme", governance_role_a="Template",
            governance_role_b="Project", client_label_b="acme",
            comparison_type="template_to_project", domain="line_styles",
            all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(
            comparison_run_id="run2", segment_id="imperial|Project|acme",
            client_label="acme", governance_role="Project", pool_scope="parent_sibling",
            domain="line_styles", n_files_focal="10", n_files_pool="30",
            executed_utc="2026-07-15T00:00:00Z",
        ),
    ]
    summary_path = tmp_path / "cross_segment_summary.csv"
    pooled_path = tmp_path / "cross_segment_pooled.csv"
    _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert manifest["corpus_scope"]["comparison_run_ids"] == ["run1", "run2"]
    assert manifest["corpus_scope"]["source_executed_utc"] == ["2026-07-15T00:00:00Z", "2026-07-16T00:00:00Z"]


def test_package_manifest_comparison_run_ids_include_optional_evidence_values(tmp_path, monkeypatch):
    """Regression test for a PR review finding: --governance-state-summary/
    --delta rows are parsed and can drive the narrative/findings, and they
    carry their own comparison_run_id/executed_utc (compare_cross_segment.py's
    GOVERNANCE_STATE_SUMMARY_FIELDS/DELTA_FIELDS), but the manifest's
    corpus_scope used to report only summary_rows/pooled_rows -- so a package
    built from a --delta or --governance-state-summary file taken from a
    different comparison run than --summary/--pooled would silently look like
    a single reproducible run."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)

    delta_rows = [_delta_row(comparison_run_id="run-delta", domain="line_styles",
                              executed_utc="2026-06-01T00:00:00Z")]
    delta_path = tmp_path / "cross_segment_delta.csv"
    _write_csv(delta_path, DELTA_FIELDS, delta_rows)

    state_summary_rows = [_gov_state_summary_row(
        comparison_run_id="run-state", domain="line_styles", comparison_type="template_to_project",
        executed_utc="2026-06-02T00:00:00Z",
    )]
    state_summary_path = tmp_path / "cross_segment_governance_state_summary.csv"
    _write_csv(state_summary_path, GOVERNANCE_STATE_SUMMARY_FIELDS, state_summary_rows)

    _run_main(monkeypatch, [
        "--summary", str(summary_path), "--pooled", str(pooled_path),
        "--delta", str(delta_path), "--governance-state-summary", str(state_summary_path),
        "--out", str(tmp_path),
    ])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert manifest["corpus_scope"]["comparison_run_ids"] == ["run-delta", "run-state", "run1"]
    assert manifest["corpus_scope"]["source_executed_utc"] == [
        "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", "2026-07-16T00:00:00Z",
    ]


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
    assert health["optional_inputs"]["file_metadata"] is True
    assert health["optional_inputs"]["cross_segment_union_inventory"] is False


def test_segment_manifest_recorded_in_evidence_package_when_supplied(tmp_path, monkeypatch):
    """Regression test for a PR #381 review finding: --segment-manifest changes
    cascade/governance_domain_summary.csv (the within_project_reliability_source
    resolved-segment fallback) but was not being recorded anywhere in the
    evidence package, so a run using the fallback would misleadingly claim it
    was built without the manifest input that actually affected the scores."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    manifest_path = tmp_path / "segment_manifest.csv"
    _write_csv(manifest_path, ["segment_id", "run_type", "notes"], [
        {"segment_id": "imperial|Project", "run_type": "registration",
         "notes": "redundant_single_child:imperial|Project|acme"},
        {"segment_id": "imperial|Project|acme", "run_type": "bundle", "notes": ""},
    ])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--segment-manifest", str(manifest_path)])

    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    inputs_by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
    assert inputs_by_id["segment_manifest"]["present"] is True
    assert inputs_by_id["segment_manifest"]["path"] == str(manifest_path)

    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["optional_inputs"]["segment_manifest"] is True

    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    ids = [a["artifact_id"] for a in evidence_map["artifacts"]]
    assert "segment_manifest" in ids


def test_segment_manifest_absent_from_evidence_package_when_not_supplied(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])

    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    inputs_by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
    assert inputs_by_id["segment_manifest"]["present"] is False
    assert inputs_by_id["segment_manifest"]["path"] is None

    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    assert health["optional_inputs"]["segment_manifest"] is False


def test_evidence_map_lists_thirty_five_artifacts_with_required_fields(tmp_path, monkeypatch):
    # 29 (pre-relationship-layer) + governance_bc_client_matrix +
    # governance_client_bc_matrix + governance_relationships + governance_file_inventory (D-023)
    # + pattern_reuse_summary_by_domain + project_mean_file_pair_jaccard_matrix (D-024).
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    ids = [a["artifact_id"] for a in evidence_map["artifacts"]]
    assert len(ids) == 35
    assert len(ids) == len(set(ids))
    assert "governance_findings" in ids
    assert "segment_manifest" in ids
    assert "governance_bc_client_matrix" in ids
    assert "governance_client_bc_matrix" in ids
    assert "governance_relationships" in ids
    assert "governance_file_inventory" in ids
    assert "pattern_reuse_summary_by_domain" in ids
    assert "project_mean_file_pair_jaccard_matrix" in ids
    narrative = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_narrative_context")
    assert narrative["authority_level"] != "authoritative_deterministic_evidence"


def test_governance_relationships_resolved_beside_supplied_matrix_not_summary_dir(tmp_path, monkeypatch):
    """Regression test for a PR review finding: tools/governance_relationships.py's
    --out-dir is independent of --summary's directory, but this generator used to
    hard-code governance_relationships.csv's sibling path relative to --summary,
    so a caller pointing --governance-bc-client-matrix at a different directory
    got a permanently-absent governance_relationships evidence-map entry even
    though the real file existed right beside the matrix it did supply."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)

    matrix_dir = tmp_path / "relationship_layer_output"
    matrix_dir.mkdir()
    _write_csv(
        matrix_dir / "governance_bc_client_matrix.csv",
        ["business_center_label", "client_label", "project_count", "project_file_count",
         "percentage_of_bc", "percentage_of_client"],
        [{"business_center_label": "2014", "client_label": "Sutter", "project_count": "16",
          "project_file_count": "62", "percentage_of_bc": "0.446043", "percentage_of_client": "1.000000"}],
    )
    relationships_path = matrix_dir / "governance_relationships.csv"
    _write_csv(
        relationships_path,
        ["project_id", "project_name", "project_name_is_fallback", "client_label",
         "business_center_label", "discipline_labels", "unit_system",
         "project_file_count", "export_run_ids"],
        [{"project_id": "proj_abc123", "project_name": "Alpha", "project_name_is_fallback": "false",
          "client_label": "Sutter", "business_center_label": "2014", "discipline_labels": "architectural",
          "unit_system": "imperial", "project_file_count": "1", "export_run_ids": "f1"}],
    )
    # governance_relationships.csv does NOT exist beside --summary -- only in matrix_dir.
    assert not (tmp_path / "governance_relationships.csv").exists()

    _run_main(monkeypatch, [
        "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
        "--governance-bc-client-matrix", str(matrix_dir / "governance_bc_client_matrix.csv"),
    ])

    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    rel_artifact = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_relationships")
    assert rel_artifact["present"] is True
    assert rel_artifact["path"] == str(relationships_path)


def test_pattern_reuse_summary_by_domain_resolved_beside_supplied_reuse_by_client_not_summary_dir(tmp_path, monkeypatch):
    """Regression test for a PR review finding (D-024): pattern_reuse_summary_by_domain.csv
    is written by compare_cross_segment.py's main() to the SAME --out-dir as
    pattern_reuse_summary_by_client.csv/pattern_reuse_distribution.csv, but this
    generator used to hard-code its sibling path relative to --summary's
    directory -- so a caller pointing --reuse-by-client at a different
    directory got a permanently-absent evidence-map entry for this file even
    though it sits right beside the reuse input actually supplied."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)

    reuse_dir = tmp_path / "reuse_output"
    reuse_dir.mkdir()
    _write_csv(
        reuse_dir / "pattern_reuse_summary_by_client.csv", REUSE_SUMMARY_FIELDS,
        [{f: "" for f in REUSE_SUMMARY_FIELDS}],
    )
    domain_path = reuse_dir / "pattern_reuse_summary_by_domain.csv"
    _write_csv(domain_path, REUSE_SUMMARY_FIELDS, [{f: "" for f in REUSE_SUMMARY_FIELDS}] * 3)
    # pattern_reuse_summary_by_domain.csv does NOT exist beside --summary -- only in reuse_dir.
    assert not (tmp_path / "pattern_reuse_summary_by_domain.csv").exists()

    _run_main(monkeypatch, [
        "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
        "--reuse-by-client", str(reuse_dir / "pattern_reuse_summary_by_client.csv"),
    ])

    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "pattern_reuse_summary_by_domain")
    assert entry["present"] is True
    assert entry["path"] == str(domain_path)
    assert entry["row_count"] == 3


def test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_fragmentation_diagnostic_not_summary_dir(tmp_path, monkeypatch):
    """Regression test for a PR review finding (D-024): project_mean_file_pair_jaccard_matrix.csv
    is written by compare_cross_segment.py's main() to the SAME --out-dir as
    project_fragmentation_diagnostic.csv and the other project_* matrices, but
    this generator used to hard-code its sibling path relative to --summary's
    directory -- so a caller pointing --project-fragmentation-diagnostic at a
    different directory got a permanently-absent evidence-map entry for this
    file even though it sits right beside the matrix input actually supplied."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)

    matrix_dir = tmp_path / "project_matrix_output"
    matrix_dir.mkdir()
    frag_fields = ["matrix_name", "row_id", "column_id", "view_scope", "domain",
                   "footprint_similarity", "exact_identity_overlap", "fragmentation_diagnostic",
                   "value_status", "interpretation", "executed_utc"]
    _write_csv(
        matrix_dir / "project_fragmentation_diagnostic.csv", frag_fields,
        [{f: "" for f in frag_fields}],
    )
    matrix_path = matrix_dir / "project_mean_file_pair_jaccard_matrix.csv"
    _write_csv(matrix_path, MATRIX_OUTPUT_FIELDS, [{f: "" for f in MATRIX_OUTPUT_FIELDS}] * 5)
    # project_mean_file_pair_jaccard_matrix.csv does NOT exist beside --summary -- only in matrix_dir.
    assert not (tmp_path / "project_mean_file_pair_jaccard_matrix.csv").exists()

    _run_main(monkeypatch, [
        "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
        "--project-fragmentation-diagnostic", str(matrix_dir / "project_fragmentation_diagnostic.csv"),
    ])

    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "project_mean_file_pair_jaccard_matrix")
    assert entry["present"] is True
    assert entry["path"] == str(matrix_path)
    assert entry["row_count"] == 5


def test_evidence_map_findings_entry_has_a_real_path(tmp_path, monkeypatch):
    """Regression test for a PR review finding: build_evidence_map() looked up
    output_paths["findings_json"], but main() writes that entry under the key
    "governance_findings" -- the evidence-map entry for governance_findings
    reported path: null and present: true simultaneously, since the .get()
    silently returned nothing for the mismatched key. path must resolve to
    the real governance_findings.json file that was actually written."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_findings")
    assert entry["path"] is not None
    assert Path(entry["path"]).name == "governance_findings.json"
    assert Path(entry["path"]).exists()


def test_manifest_output_artifact_ids_match_evidence_map_artifact_ids(tmp_path, monkeypatch):
    """Regression test for a PR review finding: governance_package_manifest.json's
    outputs[].artifact_id values (e.g. "domain_summary_csv") used a different
    vocabulary than governance_evidence_map.json's artifacts[].artifact_id
    values (e.g. "governance_domain_summary") for the exact same files, so a
    consumer joining provenance/size data from the manifest to navigation
    metadata in the evidence map by artifact_id could not resolve them."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    manifest_output_ids = {o["artifact_id"] for o in manifest["outputs"]}
    evidence_map_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
    assert manifest_output_ids <= evidence_map_ids, manifest_output_ids - evidence_map_ids


def test_manifest_input_artifact_ids_match_evidence_map_artifact_ids(tmp_path, monkeypatch):
    """Regression test for a PR review finding: governance_package_manifest.json's
    inputs[].artifact_id values used short CLI-flag-derived names ("summary",
    "pooled", "union_inventory", etc.) while governance_evidence_map.json uses
    the canonical artifact_id for the same source CSVs ("cross_segment_summary",
    "cross_segment_pooled", "cross_segment_union_inventory", etc.), so a
    consumer joining manifest input provenance to evidence-map navigation
    metadata by artifact_id could not resolve them, even though the output
    side had already been made canonical."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    manifest_input_ids = {i["artifact_id"] for i in manifest["inputs"]}
    evidence_map_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
    assert manifest_input_ids <= evidence_map_ids, manifest_input_ids - evidence_map_ids


def test_evidence_map_related_artifacts_use_artifact_ids_not_filenames(tmp_path, monkeypatch):
    """Regression test for a PR review finding: related_artifacts entries
    hard-coded filenames-with-extension (e.g. 'cross_segment_pooled.csv')
    instead of the corresponding artifact_id ('cross_segment_pooled'), so a
    consumer traversing the evidence map by artifact_id could not resolve
    the links."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
    for a in evidence_map["artifacts"]:
        for related in a["related_artifacts"]:
            assert related in ids, f"{a['artifact_id']}.related_artifacts references unknown id {related!r}"
            assert not related.endswith((".csv", ".json", ".md")), (
                f"{a['artifact_id']}.related_artifacts contains a filename, not an artifact_id: {related!r}"
            )


# ---------------------------------------------------------------------------
# D-023: governance_file_inventory.json (live file-availability inventory)
# ---------------------------------------------------------------------------

def test_file_inventory_written_and_registered_in_manifest_and_evidence_map(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_file_inventory.json").exists()
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    manifest_ids = {o["artifact_id"] for o in manifest["outputs"]}
    assert "governance_file_inventory" in manifest_ids
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    em_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
    assert "governance_file_inventory" in em_ids


def test_file_inventory_is_empty_when_no_undiscovered_files_present(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    assert fi["files"] == []
    assert fi["file_count"] == 0


def test_file_inventory_surfaces_an_undiscovered_sibling_csv(tmp_path, monkeypatch):
    """The motivating scenario: a real pipeline export sits beside
    cross_segment_summary.csv but has no artifact_id registered anywhere in
    the evidence-package layer yet -- the live scan must surface it with real
    header/row-count, computed fresh from disk, not from a hand-maintained
    list. Uses a fictitious filename: pattern_reuse_summary_by_domain.csv
    (this scenario's original example) was promoted to its own
    governance_evidence_map.json artifact by D-024, so it is no longer a
    valid stand-in for "undiscovered"."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _write_csv(
        tmp_path / "some_future_pipeline_export.csv",
        ["domain", "reuse_bucket", "n_patterns"],
        [{"domain": "line_styles", "reuse_bucket": "corpus_wide", "n_patterns": "5"}],
    )
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    assert fi["file_count"] == 1
    entry = fi["files"][0]
    assert entry["filename"] == "some_future_pipeline_export.csv"
    assert entry["row_count"] == 1
    assert [c["name"] for c in entry["columns"]] == ["domain", "reuse_bucket", "n_patterns"]
    assert entry["columns"][2]["inferred_dtype"] == "integer"
    assert "narrative" in entry and entry["narrative"]


def test_file_inventory_never_flags_this_runs_own_outputs_as_undiscovered(tmp_path, monkeypatch):
    """--out defaults to the same directory as --summary in these fixtures --
    the scan must exclude this generator's own CSV/JSON/MD outputs (already
    tracked via input_paths/output_paths/sibling_paths), not just the two
    required input CSVs."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    flagged = {f["filename"] for f in fi["files"]}
    assert "governance_domain_summary.csv" not in flagged
    assert "governance_client_summary.csv" not in flagged
    assert "governance_bc_summary.csv" not in flagged


def test_file_inventory_borrows_interpretation_from_matrix_output_manifest(tmp_path, monkeypatch):
    """When a discovered file's name matches a matrix_name already documented
    in matrix_output_manifest.csv, the narrative must reuse that row's own
    interpretation text rather than falling back to a generic sentence --
    the 'interpretation field pattern already used in the matrix CSVs'.

    Uses a fictitious matrix filename: project_mean_file_pair_jaccard_matrix.csv
    (this scenario's original example) was promoted to its own
    governance_evidence_map.json artifact by D-024, so it is no longer picked
    up by the generic undiscovered-file scan this test exercises."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    matrix_manifest_path = tmp_path / "matrix_output_manifest.csv"
    _write_csv(
        matrix_manifest_path,
        ["matrix_name", "governance_role", "view_scope", "source_file", "source_grain",
         "metric", "identity_unit", "aggregation_method", "interpretation",
         "known_limitations", "executed_utc"],
        [{
            "matrix_name": "project_hypothetical_future_matrix.csv", "governance_role": "Project",
            "view_scope": "all,used", "source_file": "cross_segment_summary.csv",
            "source_grain": "segment_pair/domain", "metric": "mean_file_pair_jaccard",
            "identity_unit": "file join_hash set",
            "aggregation_method": "Mean of pairwise file Jaccard comparisons",
            "interpretation": "Are individual files typically similar across project groups?",
            "known_limitations": "Not equivalent to union_jaccard.",
            "executed_utc": "2026-07-16T00:00:00Z",
        }],
    )
    _write_csv(
        tmp_path / "project_hypothetical_future_matrix.csv",
        ["matrix_name", "row_id", "column_id", "view_scope", "domain", "metric",
         "value", "value_status", "self_comparison", "interpretation", "executed_utc"],
        [{
            "matrix_name": "project_hypothetical_future_matrix.csv", "row_id": "proj_a",
            "column_id": "proj_b", "view_scope": "all", "domain": "ALL_DOMAINS",
            "metric": "mean_file_pair_jaccard", "value": "0.5", "value_status": "ok",
            "self_comparison": "false", "interpretation": "x", "executed_utc": "2026-07-16T00:00:00Z",
        }],
    )
    _run_main(monkeypatch, [
        "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
        "--matrix-manifest", str(matrix_manifest_path),
    ])
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    entry = next(f for f in fi["files"] if f["filename"] == "project_hypothetical_future_matrix.csv")
    assert "Are individual files typically similar across project groups?" in entry["narrative"]


def test_no_emit_evidence_package_suppresses_file_inventory(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_file_inventory.json").exists()


def test_stale_file_inventory_removed_when_evidence_package_turned_off_between_runs(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
    assert (tmp_path / "governance_file_inventory.json").exists()
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-evidence-package"])
    assert not (tmp_path / "governance_file_inventory.json").exists()


def test_file_inventory_surfaces_regardless_of_interpretation_layer_flag(tmp_path, monkeypatch):
    """governance_file_inventory.json is gated by --emit-evidence-package only,
    not --emit-interpretation-layer (that flag controls governance_brief.md's
    section, a separate rendering of the same already-scanned data).

    Uses a fictitious filename (see test_file_inventory_surfaces_an_undiscovered_sibling_csv
    for why pattern_reuse_summary_by_domain.csv no longer qualifies as "undiscovered")."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _write_csv(
        tmp_path / "some_future_pipeline_export.csv",
        ["domain", "reuse_bucket", "n_patterns"],
        [{"domain": "line_styles", "reuse_bucket": "corpus_wide", "n_patterns": "5"}],
    )
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--no-emit-interpretation-layer"])
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    assert fi["file_count"] == 1


# ---------------------------------------------------------------------------
# D-024: escalation-target files (the large cross_segment_* siblings this
# generator never parses, named in docs/governance_interpretation_guide.md's
# escalation section) get their own governance_evidence_map.json artifact
# with real header/row_count, instead of only the generic file-inventory
# scan bucket.
# ---------------------------------------------------------------------------

def test_escalation_target_files_get_real_shape_in_evidence_map_not_generic_inventory(tmp_path, monkeypatch):
    """The four files this generator's own module docstring lists as "not yet
    consumed directly" -- comparison_registry.csv, cross_segment_file_pairs.csv,
    pattern_reuse_summary_by_domain.csv, project_mean_file_pair_jaccard_matrix.csv
    -- must each resolve in governance_evidence_map.json with the real column
    header and row count read straight off disk, and must NOT also appear in
    governance_file_inventory.json's generic undiscovered-file bucket (that
    would be a second, redundant narrative layer for the same file)."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _write_csv(
        tmp_path / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS,
        [{f: "" for f in COMPARISON_REGISTRY_FIELDS}],
    )
    _write_csv(
        tmp_path / "cross_segment_file_pairs.csv",
        ["segment_id_a", "segment_id_b", "domain", "join_hash"],
        [{"segment_id_a": "imperial|A", "segment_id_b": "imperial|B",
          "domain": "line_styles", "join_hash": "abc123"}] * 3,
    )
    _write_csv(
        tmp_path / "pattern_reuse_summary_by_domain.csv", REUSE_SUMMARY_FIELDS,
        [{f: "" for f in REUSE_SUMMARY_FIELDS}] * 2,
    )
    _write_csv(
        tmp_path / "project_mean_file_pair_jaccard_matrix.csv", MATRIX_OUTPUT_FIELDS,
        [{f: "" for f in MATRIX_OUTPUT_FIELDS}] * 4,
    )

    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])

    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in evidence_map["artifacts"]}

    fp = by_id["cross_segment_file_pairs"]
    assert fp["present"] is True
    assert fp["row_count"] == 3
    assert [c["name"] for c in fp["columns"]] == ["segment_id_a", "segment_id_b", "domain", "join_hash"]

    cr = by_id["comparison_registry"]
    assert cr["present"] is True
    assert cr["row_count"] == 1
    assert [c["name"] for c in cr["columns"]] == COMPARISON_REGISTRY_FIELDS

    prsd = by_id["pattern_reuse_summary_by_domain"]
    assert prsd["present"] is True
    assert prsd["row_count"] == 2
    assert [c["name"] for c in prsd["columns"]] == REUSE_SUMMARY_FIELDS
    assert "can_answer" in prsd and prsd["can_answer"]
    assert "cannot_answer" in prsd and prsd["cannot_answer"]

    pmfp = by_id["project_mean_file_pair_jaccard_matrix"]
    assert pmfp["present"] is True
    assert pmfp["row_count"] == 4
    assert [c["name"] for c in pmfp["columns"]] == MATRIX_OUTPUT_FIELDS
    assert "can_answer" in pmfp and pmfp["can_answer"]
    assert "cannot_answer" in pmfp and pmfp["cannot_answer"]

    # Not duplicated into the generic file-inventory scan bucket.
    fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
    flagged = {f["filename"] for f in fi["files"]}
    assert flagged.isdisjoint({
        "comparison_registry.csv", "cross_segment_file_pairs.csv",
        "pattern_reuse_summary_by_domain.csv", "project_mean_file_pair_jaccard_matrix.csv",
    })


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


def test_package_schema_version_override_is_consistent_across_manifest_health_and_evidence_map(tmp_path, monkeypatch):
    """Regression test for a PR review finding: --package-schema-version was
    reflected in governance_package_manifest.json/_health.json's own top-level
    schema fields, but governance_evidence_map.json's entries describing those
    two files hard-coded the module default (PACKAGE_SCHEMA_VERSION) instead
    of the actual runtime override -- so a consumer following the evidence
    map to select a schema contract would pick the wrong one."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--package-schema-version", "2.0"])
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
    evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in evidence_map["artifacts"]}

    assert manifest["package_schema_version"] == "2.0"
    assert health["schema_version"] == "2.0"
    assert by_id["governance_package_manifest"]["schema_version"] == "2.0"
    assert by_id["governance_package_health"]["schema_version"] == "2.0"
    # governance_evidence_map.json's own schema (EVIDENCE_MAP_SCHEMA_VERSION) is a
    # separate versioning axis with no CLI override -- it must stay at its default.
    assert evidence_map["schema_version"] == "1.0"
    assert by_id["governance_evidence_map"]["schema_version"] == "1.0"
