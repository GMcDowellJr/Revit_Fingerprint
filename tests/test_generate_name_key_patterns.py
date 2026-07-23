# -*- coding: utf-8 -*-
"""Tests for PR2's parameterized pattern generation (tools/generate_name_key_patterns.py).

Covers the PR2 brief's four required test areas:
  - config-path regression: byte-identical output
  - name-path coverage-class tagging
  - excluded-domain explicit-absence behavior
  - both-mode non-collision of pattern IDs
"""
from __future__ import annotations

import csv
import filecmp
from pathlib import Path

from tools.generate_name_key_patterns import (
    build_domain_coverage,
    build_name_membership,
    build_name_patterns,
    emit_config_patterns,
    emit_name_patterns,
)
from tools.pattern_id_utils import stable_pattern_id
from core.name_key_coverage import (
    COVERAGE_EXCLUDED,
    COVERAGE_NATIVE,
    COVERAGE_PHASES_REDUNDANT,
    COVERAGE_WIDENED,
    ELIGIBLE_DOMAINS,
    EXCLUDED_DOMAINS,
)


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


NAME_KEY_FIELDS = [
    "export_file", "domain", "record_id", "label_display",
    "join_key_schema", "join_hash", "status", "missing_required",
]


class TestConfigPathRegression:
    def test_config_output_is_byte_identical_to_production_source(self, tmp_path: Path):
        # Simulates "current production output" for a fixed test corpus: a hand-written
        # domain_patterns.csv exactly as tools/extractor.py's emit_analysis would produce.
        prod_csv = tmp_path / "Results_v21" / "analysis_v21" / "domain_patterns.csv"
        _write_csv(prod_csv, ["schema_version", "domain", "pattern_id", "pattern_label"], [
            {"schema_version": "2.1.0", "domain": "materials", "pattern_id": "pat_abc123", "pattern_label": "x — Variant 1 of 1"},
        ])
        out_dir = tmp_path / "out" / "config"
        dest = emit_config_patterns(prod_csv, out_dir)

        assert dest.exists()
        assert filecmp.cmp(prod_csv, dest, shallow=False), "config target must be byte-identical to production output"

    def test_config_target_never_writes_to_the_source_path(self, tmp_path: Path):
        prod_csv = tmp_path / "Results_v21" / "analysis_v21" / "domain_patterns.csv"
        _write_csv(prod_csv, ["domain"], [{"domain": "materials"}])
        original_mtime = prod_csv.stat().st_mtime_ns
        emit_config_patterns(prod_csv, tmp_path / "out" / "config")
        assert prod_csv.stat().st_mtime_ns == original_mtime


class TestNamePathCoverageClassTagging:
    def _sample_rows(self):
        return [
            # native
            {"export_file": "f1.details.json", "domain": "materials", "record_id": "uid:1", "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1", "join_hash": "hashA", "status": "ok", "missing_required": ""},
            {"export_file": "f2.details.json", "domain": "materials", "record_id": "uid:2", "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1", "join_hash": "hashA", "status": "ok", "missing_required": ""},
            # widened
            {"export_file": "f1.details.json", "domain": "phase_filters", "record_id": "Existing", "label_display": "Existing", "join_key_schema": "name_identity.join_key.v1", "join_hash": "hashB", "status": "ok", "missing_required": ""},
            # phases (redundant marker)
            {"export_file": "f1.details.json", "domain": "phases", "record_id": "Existing", "label_display": "Existing", "join_key_schema": "phases.name_identity.join_key.v1.redundant", "join_hash": "hashC", "status": "ok", "missing_required": ""},
            # non-ok status must not form a pattern
            {"export_file": "f1.details.json", "domain": "materials", "record_id": "uid:3", "label_display": "", "join_key_schema": "name_identity.join_key.v1", "join_hash": "hashD", "status": "missing_required", "missing_required": "material.name"},
        ]

    def test_pattern_rows_tagged_with_coverage_class(self):
        rows = build_name_patterns(self._sample_rows())
        by_domain = {r["domain"]: r for r in rows}
        assert by_domain["materials"]["coverage_class"] == COVERAGE_NATIVE
        assert by_domain["phase_filters"]["coverage_class"] == COVERAGE_WIDENED
        assert by_domain["phases"]["coverage_class"] == COVERAGE_PHASES_REDUNDANT

    def test_materials_cluster_spans_both_files(self):
        rows = build_name_patterns(self._sample_rows())
        materials_row = next(r for r in rows if r["domain"] == "materials")
        assert materials_row["pattern_size_records"] == 2
        assert materials_row["pattern_size_files"] == 2

    def test_non_ok_status_rows_excluded_from_patterns(self):
        rows = build_name_patterns(self._sample_rows())
        # Only one materials pattern (hashA) -- hashD (missing_required) must not appear.
        materials_rows = [r for r in rows if r["domain"] == "materials"]
        assert len(materials_rows) == 1
        assert materials_rows[0]["join_hash"] == "hashA"

    def test_membership_rows_link_records_to_pattern_ids(self):
        sample = self._sample_rows()
        pattern_rows = build_name_patterns(sample)
        membership = build_name_membership(sample, pattern_rows)
        # membership rows carry record_id/export_file, not join_hash directly -- verify via pattern_id join
        pid_for_hashA = next(r["pattern_id"] for r in pattern_rows if r["domain"] == "materials")
        linked = [m for m in membership if m["domain"] == "materials" and m["pattern_id"] == pid_for_hashA]
        assert {m["record_id"] for m in linked} == {"uid:1", "uid:2"}

    def test_end_to_end_emit_name_patterns(self, tmp_path: Path):
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, self._sample_rows())
        out_dir = tmp_path / "out" / "name"
        patterns_path = emit_name_patterns(name_key_csv, out_dir)

        patterns = _read_csv(patterns_path)
        assert {"materials", "phase_filters", "phases"} <= {r["domain"] for r in patterns}

        membership = _read_csv(out_dir / "pattern_membership.csv")
        # All 5 eligible-domain rows get a membership row (uid:1/uid:2/uid:3 materials,
        # Existing phase_filters, Existing phases) -- uid:3 (missing_required, no pattern)
        # still gets a row with a blank pattern_id, mirroring the production convention
        # that a record without a usable join_hash is never silently dropped.
        assert len(membership) == 5
        uid3_row = next(m for m in membership if m["record_id"] == "uid:3")
        assert uid3_row["pattern_id"] == ""


class TestExcludedDomainExplicitAbsence:
    def test_domain_coverage_lists_all_37_traced_domains(self):
        rows = build_domain_coverage()
        domains = {r["domain"] for r in rows}
        assert domains == (ELIGIBLE_DOMAINS | set(EXCLUDED_DOMAINS))
        assert len(rows) == 37

    def test_excluded_domains_marked_not_included_with_reason(self):
        rows = build_domain_coverage()
        by_domain = {r["domain"]: r for r in rows}
        for domain in EXCLUDED_DOMAINS:
            assert by_domain[domain]["included"] == "false"
            assert by_domain[domain]["coverage_class"] == COVERAGE_EXCLUDED
            assert by_domain[domain]["reason"], f"{domain} must carry an explicit exclusion reason"

    def test_eligible_domains_marked_included(self):
        rows = build_domain_coverage()
        by_domain = {r["domain"]: r for r in rows}
        for domain in ELIGIBLE_DOMAINS:
            assert by_domain[domain]["included"] == "true"

    def test_excluded_domain_rows_in_name_key_csv_produce_no_pattern(self):
        # Even if an excluded domain somehow appears in the name-key CSV (e.g. stale data),
        # pattern generation must not surface it -- absence is enforced structurally, not
        # by trusting the upstream CSV to already be filtered.
        rows = [
            {"export_file": "f1.details.json", "domain": "units", "record_id": "u1", "label_display": "Units (Area)", "join_key_schema": "bogus", "join_hash": "hashX", "status": "ok", "missing_required": ""},
        ]
        pattern_rows = build_name_patterns(rows)
        assert pattern_rows == []

    def test_untraced_input_domain_reported_not_traced_not_silently_dropped(self):
        # A domain outside both the eligible and excluded registries (schema drift, a
        # stale input, or a new domain the registry hasn't caught up with) must still
        # surface as an explicit not_traced exclusion row, not vanish without a trace.
        rows = build_domain_coverage(["some_future_domain", "materials"])
        by_domain = {r["domain"]: r for r in rows}
        assert by_domain["some_future_domain"]["included"] == "false"
        assert by_domain["some_future_domain"]["coverage_class"] == COVERAGE_EXCLUDED
        assert by_domain["some_future_domain"]["reason"] == "not_traced"
        # An already-known domain passed in as "observed" must not be duplicated.
        assert sum(1 for r in rows if r["domain"] == "materials") == 1
        assert len(rows) == 38  # 37 traced + 1 untraced


class TestBothModeNonCollision:
    def test_pattern_id_formula_differs_by_schema_even_for_same_domain_and_hash(self):
        # Config and name projections for the same domain always use different
        # join_key_schema strings, so pattern_id (which hashes in the schema) cannot
        # collide even in the pathological case of an identical join_hash string.
        taken_a: set = set()
        taken_b: set = set()
        pid_config = stable_pattern_id("materials", "materials.join_key.v3", "deadbeef", taken_a)
        pid_name = stable_pattern_id("materials", "name_identity.join_key.v1", "deadbeef", taken_b)
        assert pid_config != pid_name

    def test_end_to_end_both_target_no_collision(self, tmp_path: Path):
        prod_csv = tmp_path / "Results_v21" / "analysis_v21" / "domain_patterns.csv"
        _write_csv(prod_csv, ["domain", "pattern_id", "join_key_schema", "join_hash"], [
            {"domain": "materials", "pattern_id": "pat_configvariant", "join_key_schema": "materials.join_key.v3", "join_hash": "deadbeef"},
        ])
        name_key_csv = tmp_path / "name_key_results.csv"
        _write_csv(name_key_csv, NAME_KEY_FIELDS, [
            {"export_file": "f1.details.json", "domain": "materials", "record_id": "uid:1", "label_display": "Concrete", "join_key_schema": "name_identity.join_key.v1", "join_hash": "deadbeef", "status": "ok", "missing_required": ""},
        ])

        from tools.generate_name_key_patterns import _assert_no_pattern_id_collision

        out_root = tmp_path / "out"
        config_out = emit_config_patterns(prod_csv, out_root / "config")
        name_out = emit_name_patterns(name_key_csv, out_root / "name")
        # Must not raise.
        _assert_no_pattern_id_collision(config_out, name_out)

        config_ids = {r["pattern_id"] for r in _read_csv(config_out)}
        name_ids = {r["pattern_id"] for r in _read_csv(name_out)}
        assert not (config_ids & name_ids)
