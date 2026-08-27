import csv
from pathlib import Path
from typing import Dict, List, Optional, Set

import pytest

from tools.bundle_analysis.step_compare import run_compare_for_domain


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _setup_run(
    tmp_path: Path,
    domain: str,
    membership: Dict[str, Set[str]],
    run_id: str = "run1",
) -> Path:
    """Write minimal pattern_presence_file.csv + membership_matrix.csv fixtures.

    `membership` maps export_run_id -> set of pattern_ids present for that
    file in `domain`. Every key becomes both a presence row and membership
    rows (an export_run_id with an empty set still gets a presence row, so
    it is treated as a validly-observed, actually-empty target).
    """
    analysis_dir = tmp_path / "analysis"
    out_dir = tmp_path / "out"

    presence_rows = [
        {"analysis_run_id": run_id, "domain": domain, "export_run_id": export_run_id}
        for export_run_id in membership
    ]
    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id"],
        presence_rows,
    )

    membership_rows = [
        {"analysis_run_id": run_id, "export_run_id": export_run_id, "pattern_id": pattern_id}
        for export_run_id, pattern_ids in membership.items()
        for pattern_id in pattern_ids
    ]
    _write_csv(
        out_dir / domain / "membership_matrix.csv",
        ["analysis_run_id", "export_run_id", "pattern_id"],
        membership_rows,
    )
    return analysis_dir, out_dir


def _reference(domain: str, pattern_ids: List[str], seed_export_run_id: str = "seed_file") -> Dict[str, object]:
    return {
        "reference_bundle_id": "ref-2026-08-27",
        "effective_date": "2026-08-27",
        "seed_export_run_id": seed_export_run_id,
        "domains": {domain: list(pattern_ids)},
    }


def _row_for(rows: List[Dict[str, str]], export_run_id: str) -> Dict[str, str]:
    matches = [r for r in rows if r.get("export_run_id") == export_run_id]
    assert len(matches) == 1, f"expected exactly one row for {export_run_id}, found {matches}"
    return matches[0]


def test_exact_match(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"A", "B"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["shared_count"] == "2"
    assert row["reference_only_count"] == "0"
    assert row["target_only_count"] == "0"
    assert row["union_count"] == "2"
    assert row["jaccard"] == "1.000000"
    assert row["reference_coverage_pct"] == "1.000000"
    assert row["coverage_pct"] == "1.000000"
    assert row["coverage_status"] == "full"
    assert row["shared_pattern_ids"] == "A|B"
    assert row["reference_only_pattern_ids"] == ""
    assert row["target_only_pattern_ids"] == ""


def test_target_subset(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"A", "B"}})
    reference = _reference(domain, ["A", "B", "C"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["shared_count"] == "2"
    assert row["reference_only_count"] == "1"
    assert row["target_only_count"] == "0"
    assert row["union_count"] == "3"
    assert row["reference_only_pattern_ids"] == "C"
    assert row["gap_pattern_ids"] == "C"
    assert row["coverage_status"] == "partial"
    assert row["jaccard"] == pytest.approx(2 / 3, abs=1e-6) or row["jaccard"] == f"{2/3:.6f}"


def test_target_superset_does_not_lose_target_only_patterns(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"A", "B", "C"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    # Full reference coverage...
    assert row["coverage_status"] == "full"
    assert row["coverage_pct"] == "1.000000"
    assert row["reference_coverage_pct"] == "1.000000"
    # ...but C must not disappear just because reference coverage is 100%.
    assert row["target_only_count"] == "1"
    assert row["target_only_pattern_ids"] == "C"
    assert row["union_count"] == "3"
    assert row["jaccard"] == f"{2/3:.6f}"

    detail_rows = _read_csv(out_dir.parent / "compare" / "file_gap_detail.csv")
    classes = {(r["pattern_id"], r["comparison_class"]) for r in detail_rows if r["export_run_id"] == "f1"}
    assert classes == {("A", "shared"), ("B", "shared"), ("C", "target_only")}


def test_disjoint_sets(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"C", "D"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["shared_count"] == "0"
    assert row["reference_only_count"] == "2"
    assert row["target_only_count"] == "2"
    assert row["union_count"] == "4"
    assert row["jaccard"] == "0.000000"
    assert row["coverage_status"] == "none"
    assert row["reference_only_pattern_ids"] == "A|B"
    assert row["target_only_pattern_ids"] == "C|D"


def test_empty_target_domain_is_represented_as_actual_empty_set(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": set()})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["target_pattern_count"] == "0"
    assert row["shared_count"] == "0"
    assert row["reference_only_count"] == "2"
    assert row["target_only_count"] == "0"
    assert row["coverage_status"] == "none"
    assert row["jaccard"] == "0.000000"


def test_no_reference_defined_for_domain(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"A"}})
    reference = _reference("some_other_domain", ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["coverage_status"] == "NO_REFERENCE_DEFINED"
    assert row["coverage_pct"] == ""
    assert row["reference_coverage_pct"] == ""
    assert row["jaccard"] == ""
    assert row["reference_pattern_count"] == "0"
    assert row["target_pattern_count"] == "0"
    assert row["shared_count"] == "0"
    assert row["target_only_count"] == "0"

    detail_rows = _read_csv(out_dir.parent / "compare" / "file_gap_detail.csv")
    assert detail_rows == []


def test_seed_export_run_id_excluded_from_comparison(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(
        tmp_path, domain, {"seed_file": {"A", "B"}, "f1": {"A", "B"}}
    )
    reference = _reference(domain, ["A", "B"], seed_export_run_id="seed_file")

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    gap_rows = _read_csv(out_dir.parent / "compare" / "file_gap_report.csv")
    export_ids = {r["export_run_id"] for r in gap_rows}
    assert "seed_file" not in export_ids
    assert export_ids == {"f1"}

    detail_rows = _read_csv(out_dir.parent / "compare" / "file_gap_detail.csv")
    assert all(r["export_run_id"] != "seed_file" for r in detail_rows)


def test_deterministic_pattern_id_and_row_ordering(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(
        tmp_path,
        domain,
        {"f_zzz": {"C", "A", "B"}, "f_aaa": {"B", "A"}},
    )
    reference = _reference(domain, ["C", "A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    gap_rows = _read_csv(out_dir.parent / "compare" / "file_gap_report.csv")
    assert [r["export_run_id"] for r in gap_rows] == ["f_aaa", "f_zzz"]
    for row in gap_rows:
        assert row["shared_pattern_ids"] == "A|B" or row["shared_pattern_ids"] == "A|B|C"

    detail_rows = _read_csv(out_dir.parent / "compare" / "file_gap_detail.csv")
    f_zzz_rows = [r["pattern_id"] for r in detail_rows if r["export_run_id"] == "f_zzz"]
    assert f_zzz_rows == sorted(f_zzz_rows)
    export_order = [r["export_run_id"] for r in detail_rows]
    assert export_order == sorted(export_order)


def test_existing_reference_coverage_values_preserved(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": {"A"}})
    reference = _reference(domain, ["A", "B", "C", "D"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["patterns_required"] == "4"
    assert row["patterns_present"] == "1"
    assert row["patterns_missing"] == "3"
    assert row["gap_pattern_ids"] == "B|C|D"
    assert row["coverage_pct"] == f"{0.25:.6f}"
    assert row["coverage_status"] == "partial"


def test_zero_denominator_metrics_use_no_reference_defined_semantics(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run(tmp_path, domain, {"f1": set()})
    reference = _reference("other_domain", ["A"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    # No NaN/Infinity; zero-denominator ratios serialize as an explicit empty string.
    assert row["coverage_pct"] == ""
    assert row["reference_coverage_pct"] == ""
    assert row["jaccard"] == ""
    for value in row.values():
        assert value.lower() not in ("nan", "inf", "-inf", "infinity")
