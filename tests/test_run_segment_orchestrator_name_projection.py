# -*- coding: utf-8 -*-
"""Tests for PR4 (Segment-Orchestrator Name-Projection Support).

Covers:
  - _filter_name_key_csv_to_segment() filters by normalized export id (split-export
    files use export_run_id.txt's canonical *.index.json id, not the raw
    *.details.json name apply_name_key_policy.py recorded)
  - _active_domains_from_name_patterns() reads the name-target domain_patterns.csv shape
  - --comparison-target config (default) is unchanged: dry-run output has no name-leg lines
  - --comparison-target name adds the expected step 2b/3b commands to dry-run output
  - --name-key-results-csv is required when --comparison-target is name/both
"""
from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from run_segment_orchestrator import (  # noqa: E402
    _active_domains_from_name_patterns,
    _filter_name_key_csv_to_segment,
    _segment_has_name_leg_output,
    merge_bi_outputs,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ORCHESTRATOR = _REPO_ROOT / "tools" / "run_segment_orchestrator.py"

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


def _read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


class TestFilterNameKeyCsvToSegment:
    def test_filters_by_export_run_id_membership(self, tmp_path):
        corpus_csv = tmp_path / "name_key_results.csv"
        _write_csv(corpus_csv, NAME_KEY_FIELDS, [
            {"export_file": "f1.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
            {"export_file": "f2.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
            {"export_file": "f3.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
        ])
        out_csv = tmp_path / "segment" / "name_key_results.csv"
        n = _filter_name_key_csv_to_segment(corpus_csv, out_csv, {"f1.json", "f2.json"})

        assert n == 2
        rows = _read_csv(out_csv)
        assert {r["export_file"] for r in rows} == {"f1.json", "f2.json"}

    def test_matches_split_export_details_rows_against_canonical_index_id(self, tmp_path):
        # Regression for the same class of bug PR #389 review caught: a segment's
        # export_run_ids.txt carries the canonical *.index.json id, but
        # apply_name_key_policy.py recorded export_file as the *.details.json name.
        # The filter must normalize before comparing, or every split-export row in this
        # segment silently disappears.
        corpus_csv = tmp_path / "name_key_results.csv"
        _write_csv(corpus_csv, NAME_KEY_FIELDS, [
            {"export_file": "model_a.details.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
            {"export_file": "model_b.details.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
        ])
        out_csv = tmp_path / "segment" / "name_key_results.csv"
        # Segment membership (export_run_ids.txt) uses the canonical index name.
        n = _filter_name_key_csv_to_segment(corpus_csv, out_csv, {"model_a.index.json"})

        assert n == 1
        rows = _read_csv(out_csv)
        assert rows[0]["export_file"] == "model_a.details.json"  # left unmodified

    def test_raises_on_missing_corpus_csv(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _filter_name_key_csv_to_segment(tmp_path / "missing.csv", tmp_path / "out.csv", {"f1.json"})

    def test_preserves_details_only_export_with_no_index_sibling(self, tmp_path):
        # PR #390 review: for a details-only export (no matching *.index.json),
        # tools/extractor.py's _iter_export_files() keeps the *.details.json name itself
        # as the canonical export_run_id -- there is no *.index.json to rewrite to.
        # Blindly normalizing every *.details.json row would make this row's normalized
        # id ("model_c.index.json") never match the segment's real membership
        # ("model_c.details.json"), silently dropping it.
        corpus_csv = tmp_path / "name_key_results.csv"
        _write_csv(corpus_csv, NAME_KEY_FIELDS, [
            {"export_file": "model_c.details.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
        ])
        out_csv = tmp_path / "segment" / "name_key_results.csv"
        # Segment membership carries the raw details name -- there was never an index file.
        n = _filter_name_key_csv_to_segment(corpus_csv, out_csv, {"model_c.details.json"})

        assert n == 1
        rows = _read_csv(out_csv)
        assert rows[0]["export_file"] == "model_c.details.json"

    def test_split_export_and_details_only_export_coexist_correctly(self, tmp_path):
        # Both id shapes present in the same segment must each match their own real id,
        # not each other's.
        corpus_csv = tmp_path / "name_key_results.csv"
        _write_csv(corpus_csv, NAME_KEY_FIELDS, [
            {"export_file": "model_a.details.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
            {"export_file": "model_c.details.json", "domain": "materials", "record_id": "r1", "label_display": "Concrete", "join_key_schema": "s", "join_hash": "h1", "status": "ok", "missing_required": ""},
        ])
        out_csv = tmp_path / "segment" / "name_key_results.csv"
        n = _filter_name_key_csv_to_segment(
            corpus_csv, out_csv, {"model_a.index.json", "model_c.details.json"}
        )
        assert n == 2


class TestActiveDomainsFromNamePatterns:
    def test_reads_domain_column(self, tmp_path):
        patterns_dir = tmp_path / "name"
        _write_csv(
            patterns_dir / "domain_patterns.csv",
            ["domain", "coverage_class", "pattern_id"],
            [
                {"domain": "materials", "coverage_class": "native", "pattern_id": "pat_1"},
                {"domain": "phases", "coverage_class": "phases_redundant", "pattern_id": "pat_2"},
            ],
        )
        assert _active_domains_from_name_patterns(patterns_dir) == frozenset({"materials", "phases"})

    def test_returns_none_when_missing(self, tmp_path):
        assert _active_domains_from_name_patterns(tmp_path / "does_not_exist") is None

    def test_returns_empty_frozenset_when_present_but_empty(self, tmp_path):
        # PR #390 review: an empty-but-present domain_patterns.csv is a legitimate outcome
        # for the name projection (no eligible domains intersect this segment), not a
        # signal to fall back to "unfiltered" -- returning None here would make
        # merge_bi_outputs() merge in stale per-domain folders left over from a previous,
        # larger population for this segment.
        patterns_dir = tmp_path / "name"
        _write_csv(patterns_dir / "domain_patterns.csv", ["domain"], [])
        result = _active_domains_from_name_patterns(patterns_dir)
        assert result == frozenset()
        assert result is not None


class TestMergeBiOutputsExcludesStaleDomainsForEmptySegment:
    def test_empty_active_domains_merges_nothing_even_with_stale_folder_present(self, tmp_path):
        bundle_dir = tmp_path / "name_all"
        _write_csv(
            bundle_dir / "stale_domain" / "bundles.csv",
            ["schema_version", "analysis_run_id", "domain", "bundle_id"],
            [{"schema_version": "2.1", "analysis_run_id": "r1", "domain": "stale_domain", "bundle_id": "bnd_x"}],
        )

        result = merge_bi_outputs(bundle_dir, active_domains=frozenset())

        assert result == {}
        assert not (bundle_dir / "bundles_combined.csv").exists()

    def test_none_active_domains_merges_everything_found_unfiltered(self, tmp_path):
        # Contrast case: None genuinely means "no filtering info available" (e.g. the
        # patterns step never ran), which intentionally still merges whatever is on disk.
        bundle_dir = tmp_path / "name_all"
        _write_csv(
            bundle_dir / "some_domain" / "bundles.csv",
            ["schema_version", "analysis_run_id", "domain", "bundle_id"],
            [{"schema_version": "2.1", "analysis_run_id": "r1", "domain": "some_domain", "bundle_id": "bnd_x"}],
        )

        result = merge_bi_outputs(bundle_dir, active_domains=None)

        assert result["bundles.csv"]["files_merged"] == 1
        assert (bundle_dir / "bundles_combined.csv").exists()

    def test_stale_combined_csv_from_previous_run_is_deleted_on_empty_rerun(self, tmp_path):
        # PR #390 review, second round: a segment that previously had real bundles and is
        # rerun with zero current candidates must not leave the old *_combined.csv in
        # place -- Power BI would keep reading it as if it were this run's result.
        bundle_dir = tmp_path / "name_all"
        stale_combined = bundle_dir / "bundles_combined.csv"
        _write_csv(
            stale_combined,
            ["schema_version", "analysis_run_id", "domain", "bundle_id"],
            [{"schema_version": "2.1", "analysis_run_id": "r1", "domain": "old_domain", "bundle_id": "bnd_old"}],
        )
        assert stale_combined.exists()

        # No per-domain source folders at all this run (e.g. domain_patterns.csv came back
        # empty) -- candidates end up empty regardless of active_domains' value.
        result = merge_bi_outputs(bundle_dir, active_domains=frozenset())

        assert result == {}
        assert not stale_combined.exists()

    def test_stale_combined_csv_deleted_when_all_candidates_are_headerless(self, tmp_path):
        # The second "continue without writing" path (candidates exist but every one is a
        # truly empty/headerless file) must clean up stale output the same way.
        bundle_dir = tmp_path / "name_all"
        stale_combined = bundle_dir / "bundles_combined.csv"
        _write_csv(
            stale_combined,
            ["schema_version", "domain"],
            [{"schema_version": "2.1", "domain": "old_domain"}],
        )
        (bundle_dir / "some_domain").mkdir(parents=True)
        (bundle_dir / "some_domain" / "bundles.csv").write_text("", encoding="utf-8")

        result = merge_bi_outputs(bundle_dir, active_domains=frozenset({"some_domain"}))

        assert result == {}
        assert not stale_combined.exists()


class TestSegmentHasNameLegOutput:
    def test_false_when_no_provenance_file(self, tmp_path):
        assert _segment_has_name_leg_output(tmp_path) is False

    def test_true_when_provenance_file_present(self, tmp_path):
        provenance = tmp_path / "results" / "bundle_analysis" / "name_all" / "bundle_provenance.csv"
        provenance.parent.mkdir(parents=True)
        provenance.write_text("analysis_run_id,comparison_target\n", encoding="utf-8")
        assert _segment_has_name_leg_output(tmp_path) is True


class TestCLIComparisonTarget:
    def _build_fixture(self, tmp_path: Path) -> dict:
        records_dir = tmp_path / "records"
        records_dir.mkdir(parents=True)
        (records_dir / "file_metadata.csv").write_text(
            "export_run_id,governance_role\nf1.json,Project\n", encoding="utf-8"
        )

        manifest_file = tmp_path / "segment_manifest.csv"
        _write_csv(manifest_file, ["segment_id", "segment_level", "file_count", "population_hash"], [
            {"segment_id": "seg1", "segment_level": "0", "file_count": "1", "population_hash": ""},
        ])
        registry_file = tmp_path / "run_registry.csv"
        _write_csv(registry_file, ["segment_id", "run_type", "output_folder", "status", "notes", "last_run_utc"], [
            {"segment_id": "seg1", "run_type": "bundle", "output_folder": "seg1", "status": "", "notes": "", "last_run_utc": ""},
        ])
        membership_file = tmp_path / "segment_membership.csv"
        _write_csv(membership_file, ["segment_id", "export_run_id"], [
            {"segment_id": "seg1", "export_run_id": "f1.json"},
        ])
        return {
            "manifest_file": manifest_file,
            "registry_file": registry_file,
            "membership_file": membership_file,
            "records_dir": records_dir,
            "segments_root": tmp_path / "segments",
        }

    def _base_args(self, fx: dict, tmp_path: Path) -> list:
        return [
            sys.executable, str(_ORCHESTRATOR),
            "--manifest-file", str(fx["manifest_file"]),
            "--registry-file", str(fx["registry_file"]),
            "--membership-file", str(fx["membership_file"]),
            "--records-dir", str(fx["records_dir"]),
            "--exports-dir", str(tmp_path / "exports"),
            "--segments-root", str(fx["segments_root"]),
            "--repo-root", str(_REPO_ROOT),
            "--join-policy", str(_REPO_ROOT / "policies" / "domain_join_key_policies.json"),
            "--dry-run",
        ]

    def test_name_target_requires_name_key_results_csv(self, tmp_path):
        fx = self._build_fixture(tmp_path)
        result = subprocess.run(
            self._base_args(fx, tmp_path) + ["--comparison-target", "name"],
            capture_output=True, text=True,
        )
        assert result.returncode != 0
        assert "--name-key-results-csv is required" in result.stderr

    def test_config_target_dry_run_has_no_name_leg_lines(self, tmp_path):
        fx = self._build_fixture(tmp_path)
        result = subprocess.run(self._base_args(fx, tmp_path), capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert "step 2b" not in result.stdout
        assert "step 3b" not in result.stdout
        assert "--comparison-target" not in result.stdout

    def test_name_target_dry_run_includes_name_leg_commands(self, tmp_path):
        fx = self._build_fixture(tmp_path)
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [])
        result = subprocess.run(
            self._base_args(fx, tmp_path) + [
                "--comparison-target", "name",
                "--name-key-results-csv", str(name_key_csv),
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "step 2b:" in result.stdout
        assert "generate_name_key_patterns.py" in result.stdout
        assert "step 3b:" in result.stdout
        assert "--comparison-target name" in result.stdout
        assert "--name-key-patterns-dir" in result.stdout


class TestCompleteSegmentSkipHonorsNameTarget:
    """PR #390 review: a segment already marked complete under a prior config-only run
    was being skipped outright for --comparison-target name/both, silently producing no
    name-projection output for it unless the operator also passed --force (which would
    needlessly redo the config leg too)."""

    def _build_fixture(self, tmp_path: Path, *, with_name_leg_output: bool, run_type: str = "bundle") -> dict:
        records_dir = tmp_path / "records"
        records_dir.mkdir(parents=True)
        (records_dir / "file_metadata.csv").write_text(
            "export_run_id,governance_role\nf1.json,Project\n", encoding="utf-8"
        )

        manifest_file = tmp_path / "segment_manifest.csv"
        _write_csv(manifest_file, ["segment_id", "segment_level", "file_count", "population_hash"], [
            {"segment_id": "seg1", "segment_level": "0", "file_count": "1", "population_hash": ""},
        ])
        registry_file = tmp_path / "run_registry.csv"
        _write_csv(registry_file, ["segment_id", "run_type", "output_folder", "status", "notes", "last_run_utc"], [
            {"segment_id": "seg1", "run_type": run_type, "output_folder": "seg1", "status": "complete", "notes": "", "last_run_utc": "2026-01-01T00:00:00Z"},
        ])
        membership_file = tmp_path / "segment_membership.csv"
        _write_csv(membership_file, ["segment_id", "export_run_id"], [
            {"segment_id": "seg1", "export_run_id": "f1.json"},
        ])

        segments_root = tmp_path / "segments"
        if with_name_leg_output:
            provenance = segments_root / "seg1" / "results" / "bundle_analysis" / "name_all" / "bundle_provenance.csv"
            provenance.parent.mkdir(parents=True)
            provenance.write_text("analysis_run_id,comparison_target\n", encoding="utf-8")

        return {
            "manifest_file": manifest_file,
            "registry_file": registry_file,
            "membership_file": membership_file,
            "records_dir": records_dir,
            "segments_root": segments_root,
        }

    def _base_args(self, fx: dict, tmp_path: Path) -> list:
        return [
            sys.executable, str(_ORCHESTRATOR),
            "--manifest-file", str(fx["manifest_file"]),
            "--registry-file", str(fx["registry_file"]),
            "--membership-file", str(fx["membership_file"]),
            "--records-dir", str(fx["records_dir"]),
            "--exports-dir", str(tmp_path / "exports"),
            "--segments-root", str(fx["segments_root"]),
            "--repo-root", str(_REPO_ROOT),
            "--join-policy", str(_REPO_ROOT / "policies" / "domain_join_key_policies.json"),
            "--dry-run",
        ]

    def test_config_target_still_skips_complete_segment(self, tmp_path):
        fx = self._build_fixture(tmp_path, with_name_leg_output=False)
        result = subprocess.run(self._base_args(fx, tmp_path), capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert "skipped — already complete" in result.stdout

    def test_name_target_does_not_skip_complete_segment_missing_name_leg(self, tmp_path):
        fx = self._build_fixture(tmp_path, with_name_leg_output=False)
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [])
        result = subprocess.run(
            self._base_args(fx, tmp_path) + [
                "--comparison-target", "name",
                "--name-key-results-csv", str(name_key_csv),
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "skipped — already complete" not in result.stdout
        assert "step 2b:" in result.stdout
        assert "step 3b:" in result.stdout

    def test_name_target_still_skips_complete_segment_with_existing_name_leg_output(self, tmp_path):
        fx = self._build_fixture(tmp_path, with_name_leg_output=True)
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [])
        result = subprocess.run(
            self._base_args(fx, tmp_path) + [
                "--comparison-target", "name",
                "--name-key-results-csv", str(name_key_csv),
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "skipped — already complete" in result.stdout

    def test_name_target_still_skips_complete_reference_row_missing_name_leg(self, tmp_path):
        # PR #390 review, fourth round: step 3/3b (both legs) are gated on
        # run_type == "bundle", so a "reference" row can never produce a name-leg marker
        # regardless of comparison_target. Without also checking run_type in the skip
        # logic, a complete reference row would never be recognized as satisfied under
        # name/both and would be needlessly reprocessed on every run.
        fx = self._build_fixture(tmp_path, with_name_leg_output=False, run_type="reference")
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [])
        result = subprocess.run(
            self._base_args(fx, tmp_path) + [
                "--comparison-target", "name",
                "--name-key-results-csv", str(name_key_csv),
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "skipped — already complete" in result.stdout


class TestStaleNameBundleOutputClearedBeforeRerun:
    """PR #390 review, third round: run_bundle_analysis.py only ever writes per-domain
    folders for domains present in the *current* pattern set -- it never deletes a stale
    <domain>/ folder left over from a prior run whose population included a domain this one
    doesn't. Left in place, emit_name_target_provenance()'s rglob("bundles.csv") (run
    automatically inside run_bundle_analysis.py --comparison-target name) picks up those
    stale files and reports them in a fresh bundle_provenance.csv even for a run that finds
    zero active domains. merge_bi_outputs()'s *_combined.csv cleanup (round 2's fix) doesn't
    cover this, since provenance is built independently. These tests exercise the real CLI
    (not mocked). The original gap is now closed as a byproduct of the PR3 BI-output-
    compatibility follow-up's move-based relocation of out_dir/name/all into the flat
    out_dir/name_all (see the first test below) -- the second test additionally confirms
    _run_one_segment()'s own step 3b pre-clean of out_dir/name still works as
    belt-and-suspenders."""

    _RUN_BUNDLE_ANALYSIS = _REPO_ROOT / "tools" / "bundle_analysis" / "run_bundle_analysis.py"

    def _materials_name_key_rows(self):
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

    def _build_populated_name_patterns_dir(self, tmp_path: Path) -> Path:
        from tools.generate_name_key_patterns import emit_name_patterns

        name_key_csv = tmp_path / "populated_name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, self._materials_name_key_rows())
        name_patterns_dir = tmp_path / "patterns_populated" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)
        return name_patterns_dir

    def _build_empty_name_patterns_dir(self, tmp_path: Path) -> Path:
        from tools.generate_name_key_patterns import emit_name_patterns

        name_key_csv = tmp_path / "empty_name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [])
        name_patterns_dir = tmp_path / "patterns_empty" / "name"
        emit_name_patterns(name_key_csv, name_patterns_dir)
        return name_patterns_dir

    def _run_name_bundle_analysis(self, out_dir: Path, name_patterns_dir: Path) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                sys.executable, str(self._RUN_BUNDLE_ANALYSIS),
                "--analysis-dir", str(out_dir / "unused"),
                "--out-dir", str(out_dir),
                "--comparison-target", "name",
                "--name-key-patterns-dir", str(name_patterns_dir),
                "--no-discover-populations",
                "--min-support-count", "2",
            ],
            capture_output=True, text=True,
        )

    def test_reusing_the_same_out_dir_without_clearing_no_longer_leaves_stale_provenance(self, tmp_path):
        # PR3 BI-output-compatibility follow-up: run_bundle_analysis_for_target() now
        # *moves* (not copies) out_dir/name/all into the flat out_dir/name_all BI-facing
        # location, self-clearing out_dir/name_all before each move. A side effect: the
        # source directory (out_dir/name/all) no longer persists between invocations of
        # this script either, so the specific staleness gap this class exercises (a stale
        # per-domain folder surviving in the *final* output across reruns without an
        # external pre-clean) is closed as a byproduct -- even calling run_bundle_analysis.py
        # directly, bypassing the orchestrator's own step-3b pre-clean entirely.
        out_dir = tmp_path / "bundle_out"
        populated = self._build_populated_name_patterns_dir(tmp_path)
        first = self._run_name_bundle_analysis(out_dir, populated)
        assert first.returncode == 0, first.stderr
        provenance = _read_csv(out_dir / "name_all" / "bundle_provenance.csv")
        assert any(r["domain"] == "materials" for r in provenance)

        # Rerun against a name-key CSV that now yields zero domains, WITHOUT clearing
        # out_dir first.
        empty = self._build_empty_name_patterns_dir(tmp_path)
        second = self._run_name_bundle_analysis(out_dir, empty)
        assert second.returncode == 0, second.stderr

        fresh_provenance = _read_csv(out_dir / "name_all" / "bundle_provenance.csv")
        assert not any(r["domain"] == "materials" for r in fresh_provenance), (
            "the move-based relocation to name_all/ should have reset the source "
            "directory, so no stale materials bundle should survive into this rerun's "
            "output even without an external pre-clean step"
        )

    def test_clearing_out_dir_before_rerun_removes_stale_provenance(self, tmp_path):
        import shutil

        out_dir = tmp_path / "bundle_out"
        populated = self._build_populated_name_patterns_dir(tmp_path)
        first = self._run_name_bundle_analysis(out_dir, populated)
        assert first.returncode == 0, first.stderr
        assert any(
            r["domain"] == "materials"
            for r in _read_csv(out_dir / "name_all" / "bundle_provenance.csv")
        )

        # This is what _run_one_segment()'s step 3b now does before invoking
        # run_bundle_analysis.py: clear the name-leg output directory first.
        name_dir = out_dir / "name"
        if name_dir.is_dir():
            shutil.rmtree(name_dir)

        empty = self._build_empty_name_patterns_dir(tmp_path)
        second = self._run_name_bundle_analysis(out_dir, empty)
        assert second.returncode == 0, second.stderr

        fresh_provenance = _read_csv(out_dir / "name_all" / "bundle_provenance.csv")
        assert fresh_provenance == []
        assert not any((out_dir / "name_all").rglob("bundles.csv"))
