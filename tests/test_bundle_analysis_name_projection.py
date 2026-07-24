# -*- coding: utf-8 -*-
"""Tests for PR3 (Name-Projection Bundle Support).

Covers the PR3 brief's acceptance criteria:
  - config-target passthrough is unchanged (same function, same args, no path nesting)
  - USED view / --compute-share-profile / --compare are blocked explicitly for name/both
  - the adapter's staged analysis_dir is actually consumable by the *unmodified*
    step1/step2 pipeline and produces a real bundle from name-projection input
  - every bundle produced under comparison_target=name carries comparison_target,
    coverage_class, and the provenance note
  - excluded-class domains are stated explicitly, never silently absent
  - determinism: re-running produces identical staged input and provenance output
"""
from __future__ import annotations

import csv
import filecmp
from pathlib import Path

import pytest

from core.name_key_coverage import COVERAGE_NATIVE
from tools.generate_name_key_patterns import emit_name_patterns
from tools.bundle_analysis.name_projection_adapter import (
    DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
    PROVENANCE_NOTE_NAME_TARGET,
    _normalize_export_run_id,
    emit_name_target_provenance,
    stage_name_projection_analysis_dir,
)
from tools.bundle_analysis.run_bundle_analysis import (
    VALID_COMPARISON_TARGETS,
    _validate_name_target_constraints,
    run_bundle_analysis_for_target,
)
from tools.bundle_analysis.step1_membership_matrix import build_membership_matrix
from tools.bundle_analysis.step2_find_bundles import find_bundles_for_domain
from tools.bundle_analysis.common import read_csv_rows


NAME_KEY_FIELDS = [
    "export_file", "domain", "record_id", "label_display",
    "join_key_schema", "join_hash", "status", "missing_required",
]


def _write_csv(path: Path, fieldnames, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _materials_name_key_rows():
    """3 files, domain=materials (Native), 2 co-occurring records (Concrete/Steel) present
    identically in all 3 files -- enough for a 2-pattern bundle at min_support=3."""
    rows = []
    for f in ("f1.details.json", "f2.details.json", "f3.details.json"):
        rows.append({
            "export_file": f, "domain": "materials", "record_id": "uid:concrete",
            "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
        })
        rows.append({
            "export_file": f, "domain": "materials", "record_id": "uid:steel",
            "label_display": "Steel", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashSteel", "status": "ok", "missing_required": "",
        })
    return rows


def _build_pr2_name_patterns_dir(tmp_path: Path) -> Path:
    name_key_csv = tmp_path / "name_key_results.csv"
    _write_csv(name_key_csv, NAME_KEY_FIELDS, _materials_name_key_rows())
    name_patterns_dir = tmp_path / "Results_v21" / "name_key" / "patterns" / "name"
    emit_name_patterns(name_key_csv, name_patterns_dir)
    return name_patterns_dir


class TestValidationBlocksUnsupportedFeatures:
    @pytest.mark.parametrize("comparison_target", ["name", "both"])
    def test_used_view_blocked(self, comparison_target):
        with pytest.raises(SystemExit, match="purge-view all"):
            _validate_name_target_constraints(comparison_target, "used", False, False)

    @pytest.mark.parametrize("comparison_target", ["name", "both"])
    def test_both_purge_view_blocked(self, comparison_target):
        with pytest.raises(SystemExit, match="purge-view all"):
            _validate_name_target_constraints(comparison_target, "both", False, False)

    @pytest.mark.parametrize("comparison_target", ["name", "both"])
    def test_share_profile_blocked(self, comparison_target):
        with pytest.raises(SystemExit, match="compute-share-profile"):
            _validate_name_target_constraints(comparison_target, "all", True, False)

    @pytest.mark.parametrize("comparison_target", ["name", "both"])
    def test_compare_blocked(self, comparison_target):
        with pytest.raises(SystemExit, match="compare"):
            _validate_name_target_constraints(comparison_target, "all", False, True)

    def test_config_target_never_blocked(self):
        # config target must remain fully unrestricted regardless of these flags.
        _validate_name_target_constraints("config", "used", True, True)

    def test_name_target_all_view_no_extras_passes(self):
        _validate_name_target_constraints("name", "all", False, False)


class TestConfigPassthroughUnchanged:
    def test_config_target_calls_run_bundle_analysis_with_unchanged_out_dir(self, tmp_path, monkeypatch):
        import tools.bundle_analysis.run_bundle_analysis as rba_module

        captured = {}

        def _fake_run_bundle_analysis(**kwargs):
            captured.update(kwargs)
            return {"domains_processed": 0}

        monkeypatch.setattr(rba_module, "run_bundle_analysis", _fake_run_bundle_analysis)

        analysis_dir = tmp_path / "Results_v21" / "analysis_v21"
        out_dir = tmp_path / "out"
        results = run_bundle_analysis_for_target(
            analysis_dir=analysis_dir, out_dir=out_dir, comparison_target="config",
        )

        assert captured["analysis_dir"] == analysis_dir
        assert captured["out_dir"] == out_dir  # unchanged -- no nesting for config-only runs
        assert "config" in results

    def test_both_target_nests_config_output_under_config_subdir(self, tmp_path, monkeypatch):
        import tools.bundle_analysis.run_bundle_analysis as rba_module

        captured_calls = []

        def _fake_run_bundle_analysis(**kwargs):
            captured_calls.append(kwargs)
            return {"domains_processed": 0}

        monkeypatch.setattr(rba_module, "run_bundle_analysis", _fake_run_bundle_analysis)
        monkeypatch.setattr(
            rba_module, "stage_name_projection_analysis_dir",
            lambda **kwargs: {"patterns": 0, "presence_rows": 0, "domains": 0, "coverage_rows": 0},
        )
        monkeypatch.setattr(
            rba_module, "emit_name_target_provenance",
            lambda **kwargs: {"bundles_annotated": 0, "excluded_domains": 0},
        )

        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "analysis", out_dir=out_dir,
            comparison_target="both", purge_view="all",
        )

        by_out_dir = {c["out_dir"] for c in captured_calls}
        assert out_dir / "config" in by_out_dir
        assert out_dir / "name" in by_out_dir
        assert out_dir not in by_out_dir  # never written directly -- always namespaced


class TestSplitExportFileIdNormalization:
    """PR #389 review: tools/apply_name_key_policy.py records `export_file` as the
    *.details.json name (CLAUDE.md's input-format priority), while tools/extractor.py's
    emit_records() stamps export_run_id/file_metadata.csv from the *.index.json name for a
    split-export pair (_iter_export_files(): the index file is always `primary` when one
    exists). Copying export_file verbatim would silently break --roles filtering and
    cross-target file alignment for any split-export corpus."""

    def test_details_filename_normalized_to_index_filename(self):
        assert _normalize_export_run_id("model_a.details.json") == "model_a.index.json"

    def test_details_filename_normalization_is_case_insensitive_on_suffix(self):
        assert _normalize_export_run_id("model_a.DETAILS.JSON") == "model_a.index.json"

    def test_index_filename_left_unchanged(self):
        assert _normalize_export_run_id("model_a.index.json") == "model_a.index.json"

    def test_plain_filename_left_unchanged(self):
        assert _normalize_export_run_id("model_a.json") == "model_a.json"

    def test_staged_presence_rows_use_index_export_run_id_for_split_export(self, tmp_path):
        rows = []
        for f in ("model_a.details.json", "model_b.details.json"):
            rows.append({
                "export_file": f, "domain": "materials", "record_id": "uid:concrete",
                "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
                "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
            })
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, rows)
        name_patterns_dir = tmp_path / "patterns" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)

        staging_dir = tmp_path / "staging"
        stage_name_projection_analysis_dir(name_patterns_dir, staging_dir)

        presence_rows = read_csv_rows(staging_dir / "pattern_presence_file.csv")
        export_run_ids = {r["export_run_id"] for r in presence_rows}
        assert export_run_ids == {"model_a.index.json", "model_b.index.json"}
        assert not any(eid.endswith(".details.json") for eid in export_run_ids)


class TestNameProjectionAdapterProducesConsumableInput:
    def test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle(self, tmp_path):
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        staging_dir = tmp_path / "staging"
        stats = stage_name_projection_analysis_dir(name_patterns_dir, staging_dir)

        assert stats["domains"] == 1
        assert stats["presence_rows"] == 6  # 3 files x 2 patterns

        # Staged domain_patterns.csv must satisfy resolve_analysis_run_id's single-value
        # invariant and derive_scope_key's column expectations, unmodified.
        staged_patterns = read_csv_rows(staging_dir / "domain_patterns.csv")
        assert {r["analysis_run_id"] for r in staged_patterns} == {DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID}
        assert all(r["is_cad_import"] == "" for r in staged_patterns)  # documented degradation

        work_out_dir = tmp_path / "bundle_out"
        build_membership_matrix(staging_dir, work_out_dir, "materials", DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)
        find_bundles_for_domain(work_out_dir, "materials", min_support_count=2)

        bundles = read_csv_rows(work_out_dir / "materials" / "bundles.csv")
        assert len(bundles) == 1
        assert bundles[0]["pattern_count"] == "2"
        assert bundles[0]["files_present"] == "3"

    def test_staging_is_deterministic(self, tmp_path):
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        staging_a = tmp_path / "staging_a"
        staging_b = tmp_path / "staging_b"
        stage_name_projection_analysis_dir(name_patterns_dir, staging_a)
        stage_name_projection_analysis_dir(name_patterns_dir, staging_b)

        assert filecmp.cmp(staging_a / "domain_patterns.csv", staging_b / "domain_patterns.csv", shallow=False)
        assert filecmp.cmp(staging_a / "pattern_presence_file.csv", staging_b / "pattern_presence_file.csv", shallow=False)


class TestBundleProvenance:
    def _run_pipeline(self, tmp_path: Path):
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        staging_dir = tmp_path / "staging"
        stage_name_projection_analysis_dir(name_patterns_dir, staging_dir)
        work_out_dir = tmp_path / "bundle_out" / "name"
        build_membership_matrix(staging_dir, work_out_dir, "materials", DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)
        find_bundles_for_domain(work_out_dir, "materials", min_support_count=2)
        return name_patterns_dir, work_out_dir

    def test_every_bundle_declares_comparison_target_and_coverage_class(self, tmp_path):
        name_patterns_dir, work_out_dir = self._run_pipeline(tmp_path)
        stats = emit_name_target_provenance(work_out_dir, name_patterns_dir, DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)
        assert stats["bundles_annotated"] == 1

        rows = read_csv_rows(work_out_dir / "bundle_provenance.csv")
        assert len(rows) == 1
        row = rows[0]
        assert row["comparison_target"] == "name"
        assert row["domain"] == "materials"
        assert row["coverage_class"] == COVERAGE_NATIVE
        assert PROVENANCE_NOTE_NAME_TARGET == row["provenance_note"]
        assert "analysis-side-reconstructed" in row["provenance_note"]

    def test_excluded_domains_stated_explicitly_in_readme_and_coverage_csv(self, tmp_path):
        name_patterns_dir, work_out_dir = self._run_pipeline(tmp_path)
        emit_name_target_provenance(work_out_dir, name_patterns_dir, DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)

        coverage_rows = read_csv_rows(work_out_dir / "domain_coverage.csv")
        excluded = {r["domain"] for r in coverage_rows if r["included"].lower() != "true"}
        assert "units" in excluded  # a known Excluded-class domain (core/name_key_coverage.py)
        assert len(excluded) == 12

        readme = (work_out_dir / "README.md").read_text(encoding="utf-8")
        assert "units" in readme
        assert "Excluded domains" in readme

    def test_determinism_of_provenance_output(self, tmp_path):
        name_patterns_dir, work_out_dir = self._run_pipeline(tmp_path)
        emit_name_target_provenance(work_out_dir, name_patterns_dir, DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)
        first = (work_out_dir / "bundle_provenance.csv").read_text(encoding="utf-8")
        emit_name_target_provenance(work_out_dir, name_patterns_dir, DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID)
        second = (work_out_dir / "bundle_provenance.csv").read_text(encoding="utf-8")
        assert first == second
