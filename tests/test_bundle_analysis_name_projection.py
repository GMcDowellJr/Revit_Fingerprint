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
    NAME_TARGET_COMBINED_FILES,
    PROVENANCE_NOTE_NAME_TARGET,
    annotate_name_target_combined_files,
    normalize_export_run_id,
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
from tools.bundle_analysis.common import read_csv_rows, retry_fs_op


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
        assert captured["purge_view"] == "both"  # unset default unchanged for config target
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


class TestPurgeViewDefaultIsTargetAware:
    """PR #389 review: the flat "both" default (inherited from config target) made
    --comparison-target name/both fail out of the box even when the caller never asked
    for anything but ALL view. Only an *explicit* used/both request should still error."""

    def _capture(self, monkeypatch):
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
        return captured_calls

    def test_name_target_without_explicit_purge_view_does_not_raise(self, tmp_path, monkeypatch):
        captured_calls = self._capture(monkeypatch)
        # No purge_view kwarg at all -- reproduces the bare CLI invocation the review
        # comment flagged (--comparison-target name with no --purge-view).
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "analysis", out_dir=tmp_path / "out",
            comparison_target="name",
        )
        assert captured_calls[0]["purge_view"] == "all"

    def test_both_target_without_explicit_purge_view_does_not_raise(self, tmp_path, monkeypatch):
        captured_calls = self._capture(monkeypatch)
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "analysis", out_dir=tmp_path / "out",
            comparison_target="both",
        )
        purge_views = {c["purge_view"] for c in captured_calls}
        assert purge_views == {"all"}

    def test_name_target_with_explicit_used_still_raises(self, tmp_path, monkeypatch):
        self._capture(monkeypatch)
        with pytest.raises(SystemExit, match="purge-view all"):
            run_bundle_analysis_for_target(
                analysis_dir=tmp_path / "analysis", out_dir=tmp_path / "out",
                comparison_target="name", purge_view="used",
            )

    def test_config_target_without_explicit_purge_view_still_defaults_to_both(self, tmp_path, monkeypatch):
        captured_calls = self._capture(monkeypatch)
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "analysis", out_dir=tmp_path / "out",
            comparison_target="config",
        )
        assert captured_calls[0]["purge_view"] == "both"

    def test_cli_purge_view_default_is_none(self):
        import tools.bundle_analysis.run_bundle_analysis as rba_module

        args = rba_module._parse_args([
            "--analysis-dir", "x", "--out-dir", "y", "--comparison-target", "name",
        ])
        assert args.purge_view is None


class TestSplitExportFileIdNormalization:
    """PR #389 review: tools/apply_name_key_policy.py records `export_file` as the
    *.details.json name (CLAUDE.md's input-format priority), while tools/extractor.py's
    emit_records() stamps export_run_id/file_metadata.csv from the *.index.json name for a
    split-export pair (_iter_export_files(): the index file is always `primary` when one
    exists). Copying export_file verbatim would silently break --roles filtering and
    cross-target file alignment for any split-export corpus."""

    def test_details_filename_normalized_to_index_filename(self):
        assert normalize_export_run_id("model_a.details.json") == "model_a.index.json"

    def test_details_filename_normalization_is_case_insensitive_on_suffix(self):
        assert normalize_export_run_id("model_a.DETAILS.JSON") == "model_a.index.json"

    def test_index_filename_left_unchanged(self):
        assert normalize_export_run_id("model_a.index.json") == "model_a.index.json"

    def test_plain_filename_left_unchanged(self):
        assert normalize_export_run_id("model_a.json") == "model_a.json"

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


class TestNormalizeExportRunIdWithKnownIds:
    """PR #390 review, fourth round: a details-only export (no sibling *.index.json) keeps
    its *.details.json name as its canonical export_run_id -- blind normalization can't
    tell that apart from a split-export file's raw name by string shape alone. known_ids
    (e.g. file_metadata.csv's real export_run_id set) resolves the ambiguity."""

    def test_split_export_resolves_to_normalized_form(self):
        known_ids = {"model_a.index.json"}
        assert normalize_export_run_id("model_a.details.json", known_ids=known_ids) == "model_a.index.json"

    def test_details_only_export_resolves_to_raw_form(self):
        known_ids = {"model_c.details.json"}
        assert normalize_export_run_id("model_c.details.json", known_ids=known_ids) == "model_c.details.json"

    def test_neither_form_known_falls_back_to_normalized_guess(self):
        known_ids = {"some_other_file.index.json"}
        assert normalize_export_run_id("model_z.details.json", known_ids=known_ids) == "model_z.index.json"

    def test_no_known_ids_is_unchanged_blind_rewrite(self):
        # Backward compatible: omitting known_ids behaves exactly as before this
        # parameter existed.
        assert normalize_export_run_id("model_a.details.json") == "model_a.index.json"

    def test_index_and_plain_names_unaffected_by_known_ids(self):
        assert normalize_export_run_id("model_a.index.json", known_ids=set()) == "model_a.index.json"
        assert normalize_export_run_id("model_a.json", known_ids=set()) == "model_a.json"


class TestStageWithKnownExportRunIds:
    def test_details_only_export_stages_with_raw_id_when_known(self, tmp_path):
        rows = [{
            "export_file": "model_c.details.json", "domain": "materials", "record_id": "uid:concrete",
            "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
        }]
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, rows)
        name_patterns_dir = tmp_path / "patterns" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)

        staging_dir = tmp_path / "staging"
        stage_name_projection_analysis_dir(
            name_patterns_dir, staging_dir,
            known_export_run_ids={"model_c.details.json"},
        )

        presence_rows = read_csv_rows(staging_dir / "pattern_presence_file.csv")
        assert {r["export_run_id"] for r in presence_rows} == {"model_c.details.json"}

    def test_without_known_export_run_ids_details_only_export_is_wrongly_normalized(self, tmp_path):
        # Contrast case demonstrating the bug this fix closes: without known ids, staging
        # falls back to the blind rewrite, producing a nonexistent id.
        rows = [{
            "export_file": "model_c.details.json", "domain": "materials", "record_id": "uid:concrete",
            "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
        }]
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, rows)
        name_patterns_dir = tmp_path / "patterns" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)

        staging_dir = tmp_path / "staging"
        stage_name_projection_analysis_dir(name_patterns_dir, staging_dir)

        presence_rows = read_csv_rows(staging_dir / "pattern_presence_file.csv")
        assert {r["export_run_id"] for r in presence_rows} == {"model_c.index.json"}  # wrong, no known_ids given


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


class TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile:
    """PR #390 review, fourth round: run_bundle_analysis_for_target() must supply
    known_export_run_ids from --metadata-file automatically, end to end, not just as an
    available parameter nobody calls."""

    def test_metadata_file_resolves_details_only_export_correctly(self, tmp_path):
        rows = [{
            "export_file": "model_c.details.json", "domain": "materials", "record_id": "uid:concrete",
            "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
        }]
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, rows)
        name_patterns_dir = tmp_path / "patterns" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)

        metadata_file = tmp_path / "file_metadata.csv"
        _write_csv(
            metadata_file, ["export_run_id", "governance_role"],
            [{"export_run_id": "model_c.details.json", "governance_role": "Project"}],
        )

        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused",
            out_dir=out_dir,
            comparison_target="name",
            name_key_patterns_dir=name_patterns_dir,
            metadata_file=metadata_file,
            discover_populations_flag=False,
        )

        presence_rows = read_csv_rows(out_dir / "name" / "_staging_analysis_input" / "pattern_presence_file.csv")
        assert {r["export_run_id"] for r in presence_rows} == {"model_c.details.json"}

    def test_without_metadata_file_still_falls_back_to_blind_normalize(self, tmp_path):
        rows = [{
            "export_file": "model_c.details.json", "domain": "materials", "record_id": "uid:concrete",
            "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1",
            "join_hash": "hashConcrete", "status": "ok", "missing_required": "",
        }]
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, rows)
        name_patterns_dir = tmp_path / "patterns" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)

        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused",
            out_dir=out_dir,
            comparison_target="name",
            name_key_patterns_dir=name_patterns_dir,
            discover_populations_flag=False,
        )

        presence_rows = read_csv_rows(out_dir / "name" / "_staging_analysis_input" / "pattern_presence_file.csv")
        assert {r["export_run_id"] for r in presence_rows} == {"model_c.index.json"}


class TestNameAllOutputLocation:
    """PR3 BI-output-compatibility follow-up: the name leg's BI-facing output must land at
    a flat out_dir/name_all -- a single path segment, matching the Power BI model's
    pPurgeView folder-splice convention (<segment>\\results\\bundle_analysis\\<pPurgeView>\\
    *_combined.csv) -- not the two-level out_dir/name/all this function builds internally
    for its own staging/namespacing needs."""

    def _run(self, tmp_path: Path) -> Path:
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused",
            out_dir=out_dir,
            comparison_target="name",
            name_key_patterns_dir=name_patterns_dir,
            min_support_count=2,
            discover_populations_flag=False,
        )
        return out_dir

    def test_name_all_is_flat_single_segment_under_out_dir(self, tmp_path):
        out_dir = self._run(tmp_path)
        assert (out_dir / "name_all").is_dir()
        # Never a nested out_dir/name/all -- that path must no longer exist once the
        # relocation step has moved it to the flat location.
        assert not (out_dir / "name" / "all").exists()

    def test_provenance_and_coverage_and_readme_relocated_alongside_bundle_output(self, tmp_path):
        out_dir = self._run(tmp_path)
        assert (out_dir / "name_all" / "bundle_provenance.csv").is_file()
        assert (out_dir / "name_all" / "domain_coverage.csv").is_file()
        assert (out_dir / "name_all" / "README.md").is_file()
        assert (out_dir / "name_all" / "materials" / "bundles.csv").is_file()

    def test_staging_input_remains_under_internal_name_dir_not_relocated(self, tmp_path):
        out_dir = self._run(tmp_path)
        assert (out_dir / "name" / "_staging_analysis_input" / "domain_patterns.csv").is_file()
        assert not (out_dir / "name_all" / "_staging_analysis_input").exists()

    def test_rerun_against_same_out_dir_self_clears_stale_name_all(self, tmp_path):
        # No external pre-clean between the two calls -- the relocation step itself must
        # not leave a previous run's name_all/ output mixed in with a fresh (empty) run.
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused", out_dir=out_dir, comparison_target="name",
            name_key_patterns_dir=name_patterns_dir, min_support_count=2,
            discover_populations_flag=False,
        )
        assert (out_dir / "name_all" / "materials").is_dir()

        empty_name_key_csv = tmp_path / "empty_name_key_results.csv"
        _write_csv(empty_name_key_csv, NAME_KEY_FIELDS, [])
        empty_patterns_dir = tmp_path / "empty_patterns" / "name"
        emit_name_patterns(empty_name_key_csv, empty_patterns_dir)
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused", out_dir=out_dir, comparison_target="name",
            name_key_patterns_dir=empty_patterns_dir, min_support_count=2,
            discover_populations_flag=False,
        )
        assert not (out_dir / "name_all" / "materials").exists()

    def test_config_target_output_untouched_by_relocation(self, tmp_path):
        # Guards the "never mixed into or overwriting config-target output" acceptance
        # criterion: comparison_target=config must never produce a name_all/ directory,
        # and out_dir itself (not out_dir/config) is used exactly as before this PR.
        analysis_dir = tmp_path / "Results_v21" / "analysis_v21"
        _write_csv(analysis_dir / "pattern_presence_file.csv", ["schema_version", "analysis_run_id", "domain", "pattern_id"], [])
        _write_csv(analysis_dir / "domain_patterns.csv", ["schema_version", "analysis_run_id", "domain", "pattern_id"], [])
        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=analysis_dir, out_dir=out_dir, comparison_target="config",
            analysis_run_id="test_run", purge_view="all", discover_populations_flag=False,
        )
        assert not (out_dir / "name_all").exists()
        assert not (out_dir / "name").exists()


class TestStaleNameAllClearedBeforeRegenerationEvenOnFailure:
    """PR review (chatgpt-codex-connector, #391): a failure during staging/mining/
    provenance generation happens before the relocation step at the end of the name
    branch ever runs -- without an upfront clear, a prior successful run's name_all/
    would survive completely untouched, and Power BI would keep reading it as current
    output even though this run is marked failed upstream. name_all/ must be cleared
    before regeneration starts, not only after a fresh tree is produced."""

    def test_stale_name_all_removed_even_when_mining_raises(self, tmp_path, monkeypatch):
        import tools.bundle_analysis.run_bundle_analysis as rba_module

        out_dir = tmp_path / "out"
        stale_name_all = out_dir / "name_all"
        (stale_name_all / "materials").mkdir(parents=True)
        (stale_name_all / "bundle_provenance.csv").write_text("stale", encoding="utf-8")
        (stale_name_all / "bundles_combined.csv").write_text("stale", encoding="utf-8")

        def _raises(**kwargs):
            raise RuntimeError("boom during mining")

        monkeypatch.setattr(rba_module, "run_bundle_analysis", _raises)

        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        with pytest.raises(RuntimeError, match="boom during mining"):
            run_bundle_analysis_for_target(
                analysis_dir=tmp_path / "unused",
                out_dir=out_dir,
                comparison_target="name",
                name_key_patterns_dir=name_patterns_dir,
                discover_populations_flag=False,
            )

        assert not stale_name_all.exists()

    def test_stale_name_all_removed_even_when_staging_raises(self, tmp_path, monkeypatch):
        import tools.bundle_analysis.run_bundle_analysis as rba_module

        out_dir = tmp_path / "out"
        stale_name_all = out_dir / "name_all"
        stale_name_all.mkdir(parents=True)
        (stale_name_all / "bundle_provenance.csv").write_text("stale", encoding="utf-8")

        def _raises(**kwargs):
            raise RuntimeError("boom during staging")

        monkeypatch.setattr(rba_module, "stage_name_projection_analysis_dir", _raises)

        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        with pytest.raises(RuntimeError, match="boom during staging"):
            run_bundle_analysis_for_target(
                analysis_dir=tmp_path / "unused",
                out_dir=out_dir,
                comparison_target="name",
                name_key_patterns_dir=name_patterns_dir,
                discover_populations_flag=False,
            )

        assert not stale_name_all.exists()

    def test_successful_run_still_repopulates_name_all_normally(self, tmp_path):
        # Guards against an overzealous fix that clears name_all/ and never lets a
        # successful run repopulate it.
        name_patterns_dir = _build_pr2_name_patterns_dir(tmp_path)
        out_dir = tmp_path / "out"
        run_bundle_analysis_for_target(
            analysis_dir=tmp_path / "unused",
            out_dir=out_dir,
            comparison_target="name",
            name_key_patterns_dir=name_patterns_dir,
            min_support_count=2,
            discover_populations_flag=False,
        )
        assert (out_dir / "name_all" / "bundle_provenance.csv").is_file()
        assert (out_dir / "name_all" / "materials" / "bundles.csv").is_file()


class TestAnnotateNameTargetCombinedFiles:
    """PR3 BI-output-compatibility follow-up's 'Column-shape constraint': every
    *_combined.csv under name_all/ must additionally carry comparison_target/
    coverage_class/provenance_note, strictly appended after the existing typed columns
    (never inserted/renamed/reordered) so Table.TransformColumnTypes keeps working."""

    def test_adds_three_columns_after_existing_header_and_looks_up_coverage_class(self, tmp_path):
        bundle_dir = tmp_path / "name_all"
        _write_csv(
            bundle_dir / "bundles_combined.csv",
            ["schema_version", "analysis_run_id", "domain", "scope_key", "bundle_id"],
            [{"schema_version": "2.1", "analysis_run_id": "name_projection", "domain": "materials", "scope_key": "", "bundle_id": "bnd_x"}],
        )

        stats = annotate_name_target_combined_files(bundle_dir)
        assert stats["bundles.csv"] == 1

        rows = read_csv_rows(bundle_dir / "bundles_combined.csv")
        assert len(rows) == 1
        row = rows[0]
        # Existing typed columns are untouched.
        assert row["schema_version"] == "2.1"
        assert row["domain"] == "materials"
        assert row["bundle_id"] == "bnd_x"
        # New columns are additive.
        assert row["comparison_target"] == "name"
        assert row["coverage_class"] == COVERAGE_NATIVE
        assert row["provenance_note"] == PROVENANCE_NOTE_NAME_TARGET

        with (bundle_dir / "bundles_combined.csv").open(encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh))
        assert header == [
            "schema_version", "analysis_run_id", "domain", "scope_key", "bundle_id",
            "comparison_target", "coverage_class", "provenance_note",
        ]

    def test_missing_files_are_skipped_without_error(self, tmp_path):
        bundle_dir = tmp_path / "name_all"
        bundle_dir.mkdir(parents=True)
        stats = annotate_name_target_combined_files(bundle_dir)
        assert stats == {}

    def test_idempotent_second_call_leaves_already_annotated_file_unchanged(self, tmp_path):
        bundle_dir = tmp_path / "name_all"
        _write_csv(
            bundle_dir / "scope_registry_combined.csv",
            ["schema_version", "analysis_run_id", "domain", "scope_key", "files_in_scope", "patterns_in_scope"],
            [{"schema_version": "2.1", "analysis_run_id": "name_projection", "domain": "materials", "scope_key": "", "files_in_scope": "3", "patterns_in_scope": "2"}],
        )
        first_stats = annotate_name_target_combined_files(bundle_dir)
        assert first_stats["scope_registry.csv"] == 1
        first_text = (bundle_dir / "scope_registry_combined.csv").read_text(encoding="utf-8")

        second_stats = annotate_name_target_combined_files(bundle_dir)
        assert second_stats == {}
        second_text = (bundle_dir / "scope_registry_combined.csv").read_text(encoding="utf-8")
        assert first_text == second_text

    def test_excluded_domain_row_still_annotated_with_its_own_coverage_class(self, tmp_path):
        bundle_dir = tmp_path / "name_all"
        _write_csv(
            bundle_dir / "bundles_combined.csv",
            ["schema_version", "analysis_run_id", "domain", "scope_key", "bundle_id"],
            [{"schema_version": "2.1", "analysis_run_id": "name_projection", "domain": "units", "scope_key": "", "bundle_id": "bnd_y"}],
        )
        annotate_name_target_combined_files(bundle_dir)
        rows = read_csv_rows(bundle_dir / "bundles_combined.csv")
        assert rows[0]["coverage_class"] == "excluded"

    def test_covers_all_ten_bi_merge_filenames(self):
        # NAME_TARGET_COMBINED_FILES must mirror run_segment_orchestrator.BI_MERGE_FILES
        # exactly -- a drift here would silently leave some *_combined.csv files
        # unannotated.
        import tools.run_segment_orchestrator as orchestrator_module

        assert set(NAME_TARGET_COMBINED_FILES) == set(orchestrator_module.BI_MERGE_FILES)


class TestRetryFsOp:
    """A cloud-synced segments root (OneDrive, etc.) can transiently lock a file/folder
    the name-projection bundle leg just finished writing, producing a Windows
    PermissionError ([WinError 5] Access is denied) on an otherwise-correct
    shutil.move/rmtree. retry_fs_op() is the shared mitigation used by both
    run_bundle_analysis_for_target()'s out_dir/name_all relocation and
    run_segment_orchestrator.py's stale-output pre-clean."""

    def test_succeeds_on_first_try_without_retry(self):
        calls = []
        retry_fs_op(lambda: calls.append(1), delay_seconds=0)
        assert calls == [1]

    def test_recovers_after_transient_failures(self):
        state = {"remaining_failures": 2, "calls": 0}

        def flaky():
            state["calls"] += 1
            if state["remaining_failures"] > 0:
                state["remaining_failures"] -= 1
                raise PermissionError("[WinError 5] Access is denied")

        retry_fs_op(flaky, attempts=5, delay_seconds=0)
        assert state["calls"] == 3

    def test_reraises_after_exhausting_attempts(self):
        def always_fails():
            raise PermissionError("[WinError 5] Access is denied")

        with pytest.raises(PermissionError):
            retry_fs_op(always_fails, attempts=3, delay_seconds=0)

    def test_passes_through_positional_args(self, tmp_path):
        target = tmp_path / "some_dir"
        target.mkdir()
        retry_fs_op(lambda p: p.rmdir(), target, delay_seconds=0)
        assert not target.exists()

    def test_non_os_error_is_not_retried(self):
        calls = []

        def raises_value_error():
            calls.append(1)
            raise ValueError("not a filesystem error")

        with pytest.raises(ValueError):
            retry_fs_op(raises_value_error, attempts=5, delay_seconds=0)
        assert calls == [1]
