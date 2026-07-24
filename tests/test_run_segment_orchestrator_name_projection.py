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

    def test_returns_none_when_empty(self, tmp_path):
        patterns_dir = tmp_path / "name"
        _write_csv(patterns_dir / "domain_patterns.csv", ["domain"], [])
        assert _active_domains_from_name_patterns(patterns_dir) is None


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
