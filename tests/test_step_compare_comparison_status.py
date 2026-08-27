# tests/test_step_compare_comparison_status.py
#
# PR2: explicit comparison status / reason-code semantics for reference-vs-target
# comparison. These tests are additive to tests/test_step_compare.py (PR1's
# symmetric-comparison tests), which must keep passing unchanged -- see that
# file's fixtures for the "no pattern-level granularity" backward-compatible
# path this PR preserves exactly.

import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Set

import pytest

from tools.bundle_analysis.comparison_status import (
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_DEGRADED,
    COMPARISON_STATUS_BLOCKED,
    REASON_REFERENCE_DOMAIN_UNDEFINED,
    REASON_TARGET_DOMAIN_UNAVAILABLE,
    REASON_TARGET_DOMAIN_DEGRADED,
    REASON_TARGET_IDENTITY_INVALID,
    REASON_COMPARISON_INPUT_INVALID,
    REASON_REFERENCE_INVALID,
    REASON_SCHEMA_INCOMPATIBLE,
    aggregate_comparison_status,
    join_reason_codes,
)
from tools.bundle_analysis.reference_bundle import (
    load_and_validate,
    ReferenceBundleError,
    ReferenceBundleMissingError,
    ReferenceBundleInvalidError,
    ReferenceBundleSchemaMismatchError,
)
from tools.bundle_analysis.run_bundle_analysis import _write_compare_run_outputs, _blocked_compare_summary, run_bundle_analysis
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


def _setup_run_v2(
    tmp_path: Path,
    domain: str,
    known: Dict[str, Set[str]],
    unknown_only: Optional[Set[str]] = None,
    partial_unknown: Optional[Dict[str, float]] = None,
    run_id: str = "run1",
):
    """Write production-shaped pattern_presence_file.csv + membership_matrix.csv.

    `known` maps export_run_id -> set of pattern_ids the file was assigned
    (mirrors real membership: only records with a valid join_hash get a
    pattern_id and a membership row). `unknown_only` names export_run_ids
    whose *entire* domain evidence for this file is the UNKNOWN bucket
    (pattern_id="") -- i.e. every record present had unassignable identity.
    `partial_unknown` maps export_run_id -> unknown_share for files that have
    both a known subset (from `known`) and a nonzero UNKNOWN bucket.

    Unlike tests/test_step_compare.py's `_setup_run` (which deliberately omits
    the pattern_id/pattern_share_pct columns to exercise the pre-PR2,
    granularity-free path), this fixture always includes them, matching
    tools/extractor.py's real pattern_presence_file.csv schema.
    """
    unknown_only = unknown_only or set()
    partial_unknown = partial_unknown or {}
    analysis_dir = tmp_path / "analysis"
    out_dir = tmp_path / "out"

    presence_rows: List[Dict[str, str]] = []
    membership_rows: List[Dict[str, str]] = []

    for export_run_id, pattern_ids in known.items():
        for pattern_id in sorted(pattern_ids):
            presence_rows.append(
                {
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "pattern_share_pct": f"{(1.0 / len(pattern_ids)):.6f}" if pattern_ids else "0.000000",
                }
            )
            membership_rows.append(
                {"analysis_run_id": run_id, "export_run_id": export_run_id, "pattern_id": pattern_id}
            )
        if export_run_id in partial_unknown:
            presence_rows.append(
                {
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "export_run_id": export_run_id,
                    "pattern_id": "",
                    "pattern_share_pct": f"{partial_unknown[export_run_id]:.6f}",
                }
            )

    for export_run_id in unknown_only:
        presence_rows.append(
            {
                "analysis_run_id": run_id,
                "domain": domain,
                "export_run_id": export_run_id,
                "pattern_id": "",
                "pattern_share_pct": "1.000000",
            }
        )

    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
        presence_rows,
    )
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


# ---------------------------------------------------------------------------
# 1-3: valid comparisons (exact/subset/superset) -> ok, with full granularity.
# ---------------------------------------------------------------------------

def test_exact_match_is_comparison_status_ok(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A", "B"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert row["comparison_reason_codes"] == ""
    assert row["shared_count"] == "2"
    assert row["coverage_status"] == "full"


def test_target_subset_is_comparison_status_ok(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A", "B"}})
    reference = _reference(domain, ["A", "B", "C"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert row["reference_only_count"] == "1"


def test_target_superset_is_comparison_status_ok(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A", "B", "C"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert row["target_only_count"] == "1"


# ---------------------------------------------------------------------------
# 4: valid empty target is not automatically blocked (backward-compatible,
#    granularity-free presence schema -- the exact PR1 fixture shape).
# ---------------------------------------------------------------------------

def test_valid_empty_target_without_pattern_granularity_is_ok(tmp_path):
    domain = "object_styles_model"
    analysis_dir = tmp_path / "analysis"
    out_dir = tmp_path / "out"
    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id"],
        [{"analysis_run_id": "run1", "domain": domain, "export_run_id": "f1"}],
    )
    _write_csv(out_dir / domain / "membership_matrix.csv", ["analysis_run_id", "export_run_id", "pattern_id"], [])
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert row["target_pattern_count"] == "0"
    assert row["coverage_status"] == "none"


# ---------------------------------------------------------------------------
# 5: undefined reference domain retains explicit, distinct semantics.
# ---------------------------------------------------------------------------

def test_undefined_reference_domain_is_ok_with_distinct_reason(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A"}})
    reference = _reference("some_other_domain", ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["coverage_status"] == "NO_REFERENCE_DEFINED"
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert row["comparison_reason_codes"] == REASON_REFERENCE_DOMAIN_UNDEFINED
    # Not conflated with an actual empty reference set / target failure.
    assert row["comparison_reason_codes"] != REASON_TARGET_DOMAIN_UNAVAILABLE


# ---------------------------------------------------------------------------
# 6-8: reference sidecar missing / invalid / schema mismatch -> blocked/fails
#      explicitly, with distinguishable reason codes.
# ---------------------------------------------------------------------------

def test_missing_reference_sidecar_raises_with_reference_invalid_reason(tmp_path):
    with pytest.raises(ReferenceBundleMissingError) as exc_info:
        load_and_validate(tmp_path, "v21")
    assert isinstance(exc_info.value, ValueError)  # backward-compatible catch site
    assert isinstance(exc_info.value, ReferenceBundleError)
    assert exc_info.value.reason_code == REASON_REFERENCE_INVALID


def test_invalid_reference_sidecar_json_raises_with_reference_invalid_reason(tmp_path):
    (tmp_path / "reference_bundle.json").write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ReferenceBundleInvalidError) as exc_info:
        load_and_validate(tmp_path, "v21")
    assert exc_info.value.reason_code == REASON_REFERENCE_INVALID


def test_invalid_reference_sidecar_missing_fields_raises(tmp_path):
    (tmp_path / "reference_bundle.json").write_text(json.dumps({"reference_bundle_id": "x"}), encoding="utf-8")
    with pytest.raises(ReferenceBundleInvalidError) as exc_info:
        load_and_validate(tmp_path, "v21")
    assert exc_info.value.reason_code == REASON_REFERENCE_INVALID


def test_reference_sidecar_schema_mismatch_raises_with_schema_incompatible_reason(tmp_path):
    (tmp_path / "reference_bundle.json").write_text(
        json.dumps(
            {
                "reference_bundle_id": "seed-2026-04-07",
                "effective_date": "2026-04-07",
                "extractor_schema_version": "v20",
                "seed_export_run_id": "seed",
                "domains": {"A": ["P1"]},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ReferenceBundleSchemaMismatchError) as exc_info:
        load_and_validate(tmp_path, "v21")
    assert exc_info.value.reason_code == REASON_SCHEMA_INCOMPATIBLE
    assert isinstance(exc_info.value, ValueError)


# ---------------------------------------------------------------------------
# 9: target domain extraction unavailable -> blocked, not an ordinary gap.
# ---------------------------------------------------------------------------

def test_target_domain_unavailable_is_blocked_not_a_reference_only_gap(tmp_path):
    domain = "object_styles_model"
    # "f_missing" has zero presence rows at all for this domain -- e.g. a
    # role/population filter widened the eligible set to include a file whose
    # domain extraction never produced any evidence.
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A"}})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(
        analysis_dir, out_dir, reference, domain, eligible_export_run_ids={"f1", "f_missing"}
    )

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f_missing")
    assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert REASON_TARGET_DOMAIN_UNAVAILABLE in row["comparison_reason_codes"].split("|")
    # Must not look like "target legitimately has zero of the two required patterns".
    assert row["coverage_status"] == ""
    assert row["reference_only_count"] == ""
    assert row["target_pattern_count"] == ""
    assert row["coverage_pct"] == ""
    assert row["jaccard"] == ""

    detail_rows = _read_csv(out_dir.parent / "compare" / "file_gap_detail.csv")
    assert all(r["export_run_id"] != "f_missing" for r in detail_rows)

    # The genuinely-observed sibling file is unaffected.
    ok_row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert ok_row["comparison_status"] == COMPARISON_STATUS_OK


# ---------------------------------------------------------------------------
# 10: degraded target evidence -> degraded, preserving a reliable partial
#     comparison over the assignable (known) subset.
# ---------------------------------------------------------------------------

def test_partially_unreadable_target_is_degraded_with_partial_comparison(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(
        tmp_path, domain, {"f1": {"A"}}, partial_unknown={"f1": 0.5}
    )
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_DEGRADED
    assert REASON_TARGET_DOMAIN_DEGRADED in row["comparison_reason_codes"].split("|")
    assert "identity_unknown_share=0.500000" == row["comparison_detail"]
    # Reliable partial result over the known subset is preserved, not blanked.
    assert row["shared_count"] == "1"
    assert row["reference_only_count"] == "1"
    assert row["coverage_status"] == "partial"


# ---------------------------------------------------------------------------
# 11: missing/invalid required comparison identity (100% unassignable) ->
#     blocked, not a guessed empty-set comparison.
# ---------------------------------------------------------------------------

def test_fully_invalid_target_identity_is_blocked(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {}, unknown_only={"f1"})
    reference = _reference(domain, ["A"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert REASON_TARGET_IDENTITY_INVALID in row["comparison_reason_codes"].split("|")
    assert row["target_pattern_count"] == ""
    assert row["reference_only_count"] == ""


# ---------------------------------------------------------------------------
# 12: reason-code ordering is deterministic.
# ---------------------------------------------------------------------------

def test_reason_code_join_is_deterministic_regardless_of_input_order():
    a = join_reason_codes([REASON_TARGET_DOMAIN_DEGRADED, REASON_TARGET_DOMAIN_UNAVAILABLE, REASON_TARGET_DOMAIN_DEGRADED])
    b = join_reason_codes([REASON_TARGET_DOMAIN_UNAVAILABLE, REASON_TARGET_DOMAIN_DEGRADED])
    assert a == b == "TARGET_DOMAIN_DEGRADED|TARGET_DOMAIN_UNAVAILABLE"
    assert join_reason_codes([]) == ""
    assert join_reason_codes(["", None or ""]) == ""


# ---------------------------------------------------------------------------
# 13: run-level status aggregation is deterministic ("blocked beats degraded
#     beats ok").
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "statuses,expected",
    [
        ([], COMPARISON_STATUS_OK),
        ([COMPARISON_STATUS_OK, COMPARISON_STATUS_OK], COMPARISON_STATUS_OK),
        ([COMPARISON_STATUS_OK, COMPARISON_STATUS_DEGRADED], COMPARISON_STATUS_DEGRADED),
        ([COMPARISON_STATUS_DEGRADED, COMPARISON_STATUS_BLOCKED, COMPARISON_STATUS_OK], COMPARISON_STATUS_BLOCKED),
        ([COMPARISON_STATUS_BLOCKED], COMPARISON_STATUS_BLOCKED),
    ],
)
def test_aggregate_comparison_status_is_deterministic(statuses, expected):
    assert aggregate_comparison_status(statuses) == expected
    # Order must not matter.
    assert aggregate_comparison_status(list(reversed(statuses))) == expected


def test_write_compare_run_outputs_rolls_up_run_level_status(tmp_path):
    compare_out_dir = tmp_path / "compare_all"
    compare_rows = [
        {"analysis_run_id": "run1", "domain": "d1", "population_id": "", "comparison_status": COMPARISON_STATUS_OK, "comparison_reason_codes": ""},
        {"analysis_run_id": "run1", "domain": "d2", "population_id": "", "comparison_status": COMPARISON_STATUS_BLOCKED, "comparison_reason_codes": REASON_TARGET_DOMAIN_UNAVAILABLE},
    ]
    _write_compare_run_outputs(compare_out_dir, "run1", compare_rows)

    status_rows = _read_csv(compare_out_dir / "compare_run_status.csv")
    assert len(status_rows) == 1
    assert status_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert status_rows[0]["comparison_reason_codes"] == REASON_TARGET_DOMAIN_UNAVAILABLE
    assert status_rows[0]["domains_total"] == "2"
    assert status_rows[0]["domains_blocked"] == "1"
    assert status_rows[0]["domains_ok"] == "1"

    summary_rows = _read_csv(compare_out_dir / "compare_run_summary.csv")
    assert len(summary_rows) == 2


# ---------------------------------------------------------------------------
# 14: blocked metrics never contain misleading zero/100% values.
# ---------------------------------------------------------------------------

def test_blocked_rows_never_contain_plausible_looking_placeholder_metrics(tmp_path):
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {}, unknown_only={"f1"})
    reference = _reference(domain, ["A", "B"])

    run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    row = _row_for(_read_csv(out_dir.parent / "compare" / "file_gap_report.csv"), "f1")
    assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
    blanked_fields = [
        "patterns_present", "patterns_missing", "gap_pattern_ids", "coverage_pct", "coverage_status",
        "target_pattern_count", "shared_count", "reference_only_count", "target_only_count", "union_count",
        "shared_pattern_ids", "reference_only_pattern_ids", "target_only_pattern_ids",
        "reference_coverage_pct", "jaccard",
    ]
    for field_name in blanked_fields:
        assert row[field_name] == "", f"{field_name} should be blank on a blocked row, got {row[field_name]!r}"
    # Reference-side facts remain visible (unaffected by target reliability).
    assert row["patterns_required"] == "2"
    assert row["reference_pattern_count"] == "2"


# ---------------------------------------------------------------------------
# 15: comparison exceptions are recorded (blocked summary) rather than
#     console-only / silently swallowed.
# ---------------------------------------------------------------------------

def test_missing_membership_matrix_is_recorded_as_blocked_not_raised(tmp_path):
    domain = "object_styles_model"
    analysis_dir = tmp_path / "analysis"
    out_dir = tmp_path / "out"
    # pattern_presence_file.csv exists, but step1 never ran for this domain --
    # membership_matrix.csv is entirely absent.
    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
        [{"analysis_run_id": "run1", "domain": domain, "export_run_id": "f1", "pattern_id": "A", "pattern_share_pct": "1.000000"}],
    )
    reference = _reference(domain, ["A"])

    summary = run_compare_for_domain(analysis_dir, out_dir, reference, domain)

    assert summary["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert REASON_COMPARISON_INPUT_INVALID in summary["comparison_reason_codes"].split("|")
    assert summary["analysis_run_id"] == "run1"
    # A single domain-level blocked placeholder is recorded (export_run_id=""
    # marks "no per-file breakdown available") -- not fabricated per-file
    # metrics, and not silence that would let a stale prior-run row stand.
    gap_rows = _read_csv(out_dir.parent / "compare" / "file_gap_report.csv")
    assert len(gap_rows) == 1
    assert gap_rows[0]["export_run_id"] == ""
    assert gap_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert gap_rows[0]["coverage_status"] == ""


# ---------------------------------------------------------------------------
# Regressions from automated review (Codex) on the PR2 implementation itself.
# ---------------------------------------------------------------------------

def test_write_compare_run_outputs_blocks_when_a_requested_domain_produced_no_summary(tmp_path):
    # A domain's own pipeline (step0/step1/discovery) can raise before
    # run_compare_for_domain ever appends a summary -- caught only by
    # run_bundle_analysis.py's pre-existing per-domain `except Exception:
    # print(...)` handlers. An entirely empty compare_rows must not
    # aggregate to "ok": that would falsely certify a comparison run where
    # every requested domain silently failed upstream of comparison.
    compare_out_dir = tmp_path / "compare_all"
    _write_compare_run_outputs(compare_out_dir, "run1", [], expected_domains=["d1", "d2"])

    status_rows = _read_csv(compare_out_dir / "compare_run_status.csv")
    assert status_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert status_rows[0]["comparison_reason_codes"] == REASON_COMPARISON_INPUT_INVALID
    assert status_rows[0]["domains_total"] == "2"
    assert status_rows[0]["domains_blocked"] == "2"

    summary_rows = _read_csv(compare_out_dir / "compare_run_summary.csv")
    assert {r["domain"] for r in summary_rows} == {"d1", "d2"}
    for row in summary_rows:
        assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
        assert row["comparison_reason_codes"] == REASON_COMPARISON_INPUT_INVALID


def test_write_compare_run_outputs_only_synthesizes_rows_for_domains_missing_entirely(tmp_path):
    # A domain that DID produce a summary (even a blocked one, or one for
    # only some of its populations) is left alone -- only domains with zero
    # rows at all get a synthesized placeholder.
    compare_out_dir = tmp_path / "compare_all"
    compare_rows = [
        {"analysis_run_id": "run1", "domain": "d1", "population_id": "", "comparison_status": COMPARISON_STATUS_OK, "comparison_reason_codes": ""},
    ]
    _write_compare_run_outputs(compare_out_dir, "run1", compare_rows, expected_domains=["d1", "d2"])

    summary_rows = _read_csv(compare_out_dir / "compare_run_summary.csv")
    assert len(summary_rows) == 2
    d1_row = next(r for r in summary_rows if r["domain"] == "d1")
    d2_row = next(r for r in summary_rows if r["domain"] == "d2")
    assert d1_row["comparison_status"] == COMPARISON_STATUS_OK
    assert d2_row["comparison_status"] == COMPARISON_STATUS_BLOCKED


def test_reference_sidecar_non_utf8_bytes_raises_invalid_error(tmp_path):
    # Raw non-UTF-8 bytes raise UnicodeDecodeError on read, before json.loads
    # ever runs -- must still be classified REFERENCE_INVALID, not escape as
    # an unclassified exception.
    (tmp_path / "reference_bundle.json").write_bytes(b"\xff\xfe\x00{not even close to json")
    with pytest.raises(ReferenceBundleInvalidError) as exc_info:
        load_and_validate(tmp_path, "v21")
    assert exc_info.value.reason_code == REASON_REFERENCE_INVALID


def test_run_bundle_analysis_blocked_reference_status_preserves_run_id(tmp_path):
    analysis_dir = tmp_path / "analysis"
    out_dir = tmp_path / "out"
    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id"],
        [{"analysis_run_id": "run1", "domain": "d1", "export_run_id": "f1"}],
    )
    # No reference_bundle.json written -- load_and_validate raises Missing.

    with pytest.raises(ReferenceBundleMissingError):
        run_bundle_analysis(analysis_dir, out_dir, compare=True, discover_populations_flag=False, purge_view="all")

    # Written under the same compare_<view>/ path a successful run would use
    # (out_dir/compare_all/), not a one-off top-level location a consumer
    # monitoring the normal per-view outputs would never check.
    status_rows = _read_csv(out_dir / "compare_all" / "compare_run_status.csv")
    assert len(status_rows) == 1
    assert status_rows[0]["analysis_run_id"] == "run1"
    assert status_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert status_rows[0]["comparison_reason_codes"] == REASON_REFERENCE_INVALID
    assert status_rows[0]["domains_total"] == "1"
    assert status_rows[0]["domains_blocked"] == "1"

    summary_rows = _read_csv(out_dir / "compare_all" / "compare_run_summary.csv")
    assert len(summary_rows) == 1
    assert summary_rows[0]["domain"] == "d1"
    assert summary_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert summary_rows[0]["comparison_reason_codes"] == REASON_REFERENCE_INVALID


def test_run_compare_for_domain_replaces_stale_ok_rows_on_later_input_failure(tmp_path):
    # First run: inputs are valid, comparison succeeds and writes an "ok" row.
    domain = "object_styles_model"
    analysis_dir, out_dir = _setup_run_v2(tmp_path, domain, {"f1": {"A"}})
    reference = _reference(domain, ["A"])
    run_compare_for_domain(analysis_dir, out_dir, reference, domain)
    ok_rows = _read_csv(out_dir.parent / "compare" / "file_gap_report.csv")
    assert _row_for(ok_rows, "f1")["comparison_status"] == COMPARISON_STATUS_OK

    # Second run reuses the same compare output directory, but step1's output
    # is now missing (e.g. deleted between runs). The stale "ok" row for f1
    # must not be left standing next to a blocked summary -- a downstream
    # reader of file_gap_report.csv would otherwise see old, no-longer-true
    # metrics with no indication this run couldn't reproduce them.
    (out_dir / domain / "membership_matrix.csv").unlink()
    summary = run_compare_for_domain(analysis_dir, out_dir, reference, domain)
    assert summary["comparison_status"] == COMPARISON_STATUS_BLOCKED

    gap_rows = _read_csv(out_dir.parent / "compare" / "file_gap_report.csv")
    assert len(gap_rows) == 1
    assert gap_rows[0]["export_run_id"] == ""
    assert gap_rows[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert not any(r.get("export_run_id") == "f1" for r in gap_rows)


def test_write_compare_run_outputs_counts_distinct_domains_not_population_rows(tmp_path):
    # Population-aware mode: compare_rows has one row per (domain,
    # population_id). A domain with several successful population rows must
    # still count as ONE domain in domains_total/domains_ok, not one per row.
    compare_out_dir = tmp_path / "compare_all"
    compare_rows = [
        {"analysis_run_id": "run1", "domain": "d1", "population_id": "p1", "comparison_status": COMPARISON_STATUS_OK, "comparison_reason_codes": ""},
        {"analysis_run_id": "run1", "domain": "d1", "population_id": "p2", "comparison_status": COMPARISON_STATUS_OK, "comparison_reason_codes": ""},
        {"analysis_run_id": "run1", "domain": "d1", "population_id": "p3", "comparison_status": COMPARISON_STATUS_DEGRADED, "comparison_reason_codes": REASON_TARGET_DOMAIN_DEGRADED},
        {"analysis_run_id": "run1", "domain": "d2", "population_id": "p1", "comparison_status": COMPARISON_STATUS_OK, "comparison_reason_codes": ""},
    ]
    _write_compare_run_outputs(compare_out_dir, "run1", compare_rows)

    status_rows = _read_csv(compare_out_dir / "compare_run_status.csv")
    assert status_rows[0]["domains_total"] == "2"
    # d1's three populations roll up to one domain-level status: degraded
    # beats ok, so d1 counts as degraded, not ok.
    assert status_rows[0]["domains_ok"] == "1"
    assert status_rows[0]["domains_degraded"] == "1"
    assert status_rows[0]["comparison_status"] == COMPARISON_STATUS_DEGRADED

    # The underlying per-population rows are untouched (4 rows, not collapsed).
    summary_rows = _read_csv(compare_out_dir / "compare_run_summary.csv")
    assert len(summary_rows) == 4


def test_blocked_compare_summary_shape_matches_run_compare_for_domain_contract(tmp_path):
    reference = _reference("d1", ["A"])
    row = _blocked_compare_summary(reference, "run1", "d1", "p1", REASON_COMPARISON_INPUT_INVALID, "boom")
    assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert row["comparison_reason_codes"] == REASON_COMPARISON_INPUT_INVALID
    assert row["domain"] == "d1"
    assert row["population_id"] == "p1"
    assert row["analysis_run_id"] == "run1"
    assert set(row.keys()) == {
        "reference_bundle_id", "effective_date", "analysis_run_id", "domain", "population_id",
        "files_scored", "full_count", "partial_count", "none_count", "no_reference_count",
        "comparison_status", "comparison_reason_codes",
        "comparison_ok_count", "comparison_degraded_count", "comparison_blocked_count",
    }
