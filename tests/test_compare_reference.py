# tests/test_compare_reference.py
#
# PR3: orchestration/output-surface tests for tools/compare_reference.py.
#
# This module implements no comparison mathematics of its own (see its module
# docstring) -- it stages inputs for, and shells out to, run_extract_all.py
# and run_bundle_analysis.py, then reshapes their already-tested outputs
# (tests/test_step_compare.py, tests/test_step_compare_comparison_status.py)
# into the PR3 consumable package. These tests therefore:
#
#   - exercise the pure staging/validation helpers directly (no subprocess);
#   - exercise `assemble_outputs()` against real compare_all/ output produced
#     by tools.bundle_analysis.step_compare.run_compare_for_domain (the same
#     fixture style as test_step_compare_comparison_status.py), so the
#     reshaping logic is checked against the actual authoritative contract,
#     not a hand-rolled approximation of it;
#   - exercise main()'s orchestration/exit-code wiring with the two
#     subprocess calls stubbed out (compare_reference._execute), since
#     driving the full extraction pipeline end-to-end is out of scope for an
#     orchestration-layer test suite (tools/run_extract_all.py and
#     tools/bundle_analysis/ already have their own test coverage).
#
# Use synthetic fixtures only. No Revit dependency.

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for _candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import compare_reference as cr  # noqa: E402
from tools.bundle_analysis.comparison_status import (  # noqa: E402
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_DEGRADED,
    COMPARISON_STATUS_BLOCKED,
    REASON_TARGET_DOMAIN_UNAVAILABLE,
    REASON_TARGET_DOMAIN_DEGRADED,
    REASON_REFERENCE_INVALID,
)
from tools.bundle_analysis.step_compare import run_compare_for_domain


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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


def _reference_dict(domain: str, pattern_ids: List[str], seed_export_run_id: str = "seed_file") -> Dict[str, object]:
    return {
        "reference_bundle_id": "ref-2026-08-27",
        "effective_date": "2026-08-27",
        "seed_export_run_id": seed_export_run_id,
        "domains": {domain: list(pattern_ids)},
    }


def _setup_compare_all(
    tmp_path: Path,
    domain: str,
    known: Dict[str, Set[str]],
    reference_pattern_ids: List[str],
    unknown_only: Set[str] = frozenset(),
    partial_unknown: Dict[str, float] = None,
    run_id: str = "run1",
) -> Path:
    """Produce a real compare_all/ directory via the authoritative comparator
    (tools.bundle_analysis.step_compare.run_compare_for_domain), matching
    test_step_compare_comparison_status.py's fixture shape. Returns the
    bundle_out_dir such that compare_all/ sits directly under it.
    """
    partial_unknown = partial_unknown or {}
    analysis_dir = tmp_path / "analysis"
    bundle_out_dir = tmp_path / "bundle_out"

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
            membership_rows.append({"analysis_run_id": run_id, "export_run_id": export_run_id, "pattern_id": pattern_id})
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
            {"analysis_run_id": run_id, "domain": domain, "export_run_id": export_run_id, "pattern_id": "", "pattern_share_pct": "1.000000"}
        )

    _write_csv(
        analysis_dir / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
        presence_rows,
    )
    _write_csv(bundle_out_dir / domain / "membership_matrix.csv", ["analysis_run_id", "export_run_id", "pattern_id"], membership_rows)

    reference = _reference_dict(domain, reference_pattern_ids)
    run_compare_for_domain(analysis_dir, bundle_out_dir, reference, domain, compare_out_dir=bundle_out_dir / "compare_all")

    # compare_run_summary.csv / compare_run_status.csv are normally written by
    # run_bundle_analysis.py's own wrapper around run_compare_for_domain --
    # reproduce that thin step here so assemble_outputs() has real inputs.
    from tools.bundle_analysis.run_bundle_analysis import _write_compare_run_outputs

    gap_rows = _read_csv(bundle_out_dir / "compare_all" / "file_gap_report.csv")
    _write_compare_run_outputs(bundle_out_dir / "compare_all", run_id, [
        {
            "reference_bundle_id": reference["reference_bundle_id"],
            "effective_date": reference["effective_date"],
            "analysis_run_id": run_id,
            "domain": domain,
            "population_id": "",
            "files_scored": str(len(gap_rows)),
            "full_count": "0",
            "partial_count": "0",
            "none_count": "0",
            "no_reference_count": "0",
            "comparison_status": max(
                (r.get("comparison_status", COMPARISON_STATUS_OK) for r in gap_rows),
                key=lambda s: {COMPARISON_STATUS_OK: 0, COMPARISON_STATUS_DEGRADED: 1, COMPARISON_STATUS_BLOCKED: 2}.get(s, 0),
                default=COMPARISON_STATUS_OK,
            ),
            "comparison_reason_codes": "|".join(sorted({c for r in gap_rows for c in (r.get("comparison_reason_codes") or "").split("|") if c})),
            "comparison_ok_count": str(sum(1 for r in gap_rows if r.get("comparison_status") == COMPARISON_STATUS_OK)),
            "comparison_degraded_count": str(sum(1 for r in gap_rows if r.get("comparison_status") == COMPARISON_STATUS_DEGRADED)),
            "comparison_blocked_count": str(sum(1 for r in gap_rows if r.get("comparison_status") == COMPARISON_STATUS_BLOCKED)),
        }
    ])
    return bundle_out_dir


def _assemble(tmp_path: Path, bundle_out_dir: Path, reference_file: Path = None) -> Path:
    out_dir = tmp_path / "final_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    cr.assemble_outputs(
        bundle_out_dir,
        "all",
        out_dir,
        reference_file or (tmp_path / "ref.json"),
        {"reference_files": [], "target_files": [], "target_file_count": 1},
        ["fake", "extract", "cmd"],
        ["fake", "bundle", "cmd"],
    )
    return out_dir


# ---------------------------------------------------------------------------
# 1. One reference vs one target -- successful run.
# ---------------------------------------------------------------------------


def test_one_reference_one_target_success(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A", "B"}}, ["A", "B"])
    out_dir = _assemble(tmp_path, bundle_out_dir)

    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert len(summary) == 1
    assert summary[0]["target_export_run_id"] == "t1"
    assert summary[0]["comparison_status"] == COMPARISON_STATUS_OK
    assert summary[0]["shared_count"] == "2"


# ---------------------------------------------------------------------------
# 2. One reference vs multiple targets.
# ---------------------------------------------------------------------------


def test_one_reference_multiple_targets(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(
        tmp_path, domain, {"t1": {"A", "B"}, "t2": {"A"}, "t3": {"A", "B", "C"}}, ["A", "B"]
    )
    out_dir = _assemble(tmp_path, bundle_out_dir)

    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert {r["target_export_run_id"] for r in summary} == {"t1", "t2", "t3"}
    assert len(summary) == 3


# ---------------------------------------------------------------------------
# 3. Reference export excluded from target results.
# ---------------------------------------------------------------------------


def test_reference_export_excluded_from_targets(tmp_path):
    domain = "object_styles_model"
    # run_compare_for_domain (the authoritative comparator) already excludes
    # reference["seed_export_run_id"] from the eligible target set -- this
    # test asserts that guarantee survives assemble_outputs() unchanged.
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A"}}, ["A"])
    out_dir = _assemble(tmp_path, bundle_out_dir)

    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert "seed_file" not in {r["target_export_run_id"] for r in summary}


# ---------------------------------------------------------------------------
# 4 / 5. Deterministic summary / detail output.
# ---------------------------------------------------------------------------


def test_summary_output_is_deterministic_across_runs(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(
        tmp_path, domain, {"t3": {"A"}, "t1": {"A", "B"}, "t2": {"B"}}, ["A", "B"]
    )
    out_dir_1 = _assemble(tmp_path / "run1_final", bundle_out_dir)
    out_dir_2 = _assemble(tmp_path / "run2_final", bundle_out_dir)

    assert (out_dir_1 / cr.SUMMARY_FILENAME).read_text() == (out_dir_2 / cr.SUMMARY_FILENAME).read_text()
    rows = _read_csv(out_dir_1 / cr.SUMMARY_FILENAME)
    assert [r["target_export_run_id"] for r in rows] == ["t1", "t2", "t3"]


def test_detail_output_is_deterministic_and_covers_all_classes(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A", "C"}}, ["A", "B"])
    out_dir = _assemble(tmp_path, bundle_out_dir)

    detail = _read_csv(out_dir / cr.DETAIL_FILENAME)
    classes = {(r["pattern_id"], r["comparison_class"]) for r in detail}
    assert classes == {("A", "shared"), ("B", "reference_only"), ("C", "target_only")}
    assert list(detail) == sorted(detail, key=lambda r: (r["domain"], r["population_id"], r["target_export_run_id"], r["pattern_id"]))


# ---------------------------------------------------------------------------
# 6 / 7. Diagnostics for blocked target/domain; degraded status survives.
# ---------------------------------------------------------------------------


def test_diagnostics_emitted_for_blocked_target(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {}, ["A"], unknown_only={"t_bad"})
    out_dir = _assemble(tmp_path, bundle_out_dir)

    diagnostics = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diagnostics["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    target_diag = diagnostics["target_diagnostics"]
    assert len(target_diag) == 1
    assert target_diag[0]["target_export_run_id"] == "t_bad"
    assert target_diag[0]["domain"] == domain
    assert target_diag[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED

    # Not scrapeable-from-stdout-only: it's a file on disk.
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert summary[0]["shared_count"] == ""  # never a fabricated zero


def test_degraded_status_survives_into_final_outputs(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A"}}, ["A", "B"], partial_unknown={"t1": 0.5})
    out_dir = _assemble(tmp_path, bundle_out_dir)

    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["comparison_status"] == COMPARISON_STATUS_DEGRADED
    assert REASON_TARGET_DOMAIN_DEGRADED in summary[0]["comparison_reason_codes"].split("|")
    # A degraded row is a real partial result, not blanked.
    assert summary[0]["shared_count"] == "1"

    diagnostics = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diagnostics["run_comparison_status"] == COMPARISON_STATUS_DEGRADED
    assert diagnostics["target_diagnostics"][0]["comparison_status"] == COMPARISON_STATUS_DEGRADED


# ---------------------------------------------------------------------------
# 8 / 9. Missing / malformed reference input fails explicitly (pre-flight,
#         before any subprocess is ever invoked).
# ---------------------------------------------------------------------------


def test_missing_reference_input_fails_explicitly(tmp_path):
    with pytest.raises(cr.CompareReferenceError, match="not found"):
        cr._validate_export_path("--reference", tmp_path / "does_not_exist.json")


def test_malformed_reference_json_fails_explicitly(tmp_path):
    bad = tmp_path / "bad_ref.json"
    bad.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(cr.CompareReferenceError, match="not valid JSON"):
        cr._validate_export_path("--reference", bad)


def test_legacy_export_rejected_explicitly(tmp_path):
    legacy = tmp_path / "foo.legacy.json"
    _write_json(legacy, {"ok": True})
    with pytest.raises(cr.CompareReferenceError, match="legacy"):
        cr._validate_export_path("--reference", legacy)


def test_main_exits_nonzero_and_does_not_invoke_subprocess_on_missing_reference(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(cr, "_execute", lambda cmd: calls.append(cmd))
    target = tmp_path / "t1.details.json"
    _write_json(target, {"ok": True})

    rc = cr.main(
        [
            "--reference", str(tmp_path / "missing_ref.json"),
            "--target", str(target),
            "--out-dir", str(tmp_path / "out"),
        ]
    )
    assert rc != 0
    assert calls == []  # no subprocess was ever invoked


# ---------------------------------------------------------------------------
# 10. Schema-incompatible reference blocks explicitly, propagated through
#     main()'s orchestration (run_bundle_analysis.py subprocess simulated as
#     failing after recording a blocked compare_run_status.csv, exactly as
#     the real script does per tools/bundle_analysis/README.md).
# ---------------------------------------------------------------------------


def test_main_propagates_blocked_reference_failure_and_writes_diagnostics(tmp_path, monkeypatch):
    reference = tmp_path / "ref.details.json"
    target = tmp_path / "t1.details.json"
    _write_json(reference, {"ok": True})
    _write_json(target, {"ok": True})
    out_dir = tmp_path / "out"

    def fake_execute(cmd):
        cmd = list(cmd)
        if "run_bundle_analysis.py" in cmd[1]:
            bundle_out_dir = Path(cmd[cmd.index("--out-dir") + 1])
            compare_dir = bundle_out_dir / "compare_all"
            _write_csv(
                compare_dir / "file_gap_report.csv",
                ["reference_bundle_id", "analysis_run_id", "domain", "population_id", "export_run_id", "comparison_status", "comparison_reason_codes"],
                [],
            )
            _write_csv(
                compare_dir / "compare_run_status.csv",
                ["analysis_run_id", "comparison_status", "comparison_reason_codes", "comparison_detail", "domains_total", "domains_ok", "domains_degraded", "domains_blocked"],
                [
                    {
                        "analysis_run_id": "run1",
                        "comparison_status": COMPARISON_STATUS_BLOCKED,
                        "comparison_reason_codes": REASON_REFERENCE_INVALID,
                        "comparison_detail": "Missing reference bundle sidecar",
                        "domains_total": "1",
                        "domains_ok": "0",
                        "domains_degraded": "0",
                        "domains_blocked": "1",
                    }
                ],
            )
            raise subprocess.CalledProcessError(1, cmd)
        return None

    monkeypatch.setattr(cr, "_execute", fake_execute)
    rc = cr.main(["--reference", str(reference), "--target", str(target), "--out-dir", str(out_dir)])

    assert rc != 0
    diagnostics = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diagnostics["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert REASON_REFERENCE_INVALID in diagnostics["run_comparison_reason_codes"]


# ---------------------------------------------------------------------------
# 11. Stale prior-run rows cannot be confused with current results.
# ---------------------------------------------------------------------------


def test_stale_prior_run_rows_are_not_confused_with_current_results(tmp_path):
    domain = "object_styles_model"
    out_dir = tmp_path / "final_out"
    out_dir.mkdir()

    bundle_out_dir_1 = _setup_compare_all(tmp_path / "run1", domain, {"t1": {"A"}}, ["A"])
    cr.assemble_outputs(
        bundle_out_dir_1, "all", out_dir, tmp_path / "ref1.json",
        {"reference_files": [], "target_files": [], "target_file_count": 1}, ["cmd1"], ["cmd1"],
    )
    first_summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert {r["target_export_run_id"] for r in first_summary} == {"t1"}

    # A second, unrelated comparison run reuses the same out_dir (as
    # --overwrite/prepare_out_dir would allow) -- its outputs must fully
    # replace the first run's, not merge with them.
    bundle_out_dir_2 = _setup_compare_all(tmp_path / "run2", domain, {"t2": {"B"}}, ["B"])
    cr.assemble_outputs(
        bundle_out_dir_2, "all", out_dir, tmp_path / "ref2.json",
        {"reference_files": [], "target_files": [], "target_file_count": 1}, ["cmd2"], ["cmd2"],
    )
    second_summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert {r["target_export_run_id"] for r in second_summary} == {"t2"}
    assert "t1" not in {r["target_export_run_id"] for r in second_summary}


def test_prepare_out_dir_refuses_foreign_directory_without_overwrite(tmp_path):
    out_dir = tmp_path / "existing"
    out_dir.mkdir()
    (out_dir / "unrelated_user_file.txt").write_text("do not delete me", encoding="utf-8")

    with pytest.raises(cr.CompareReferenceError, match="was not produced by a prior run"):
        cr.prepare_out_dir(out_dir, overwrite=False)

    # File must survive the refused call.
    assert (out_dir / "unrelated_user_file.txt").is_file()

    # --overwrite explicitly allows clearing it.
    cr.prepare_out_dir(out_dir, overwrite=True)
    assert not (out_dir / "unrelated_user_file.txt").exists()


def test_prepare_out_dir_allows_reuse_of_its_own_prior_output(tmp_path):
    out_dir = tmp_path / "owned"
    out_dir.mkdir()
    (out_dir / cr.MANIFEST_FILENAME).write_text("{}", encoding="utf-8")
    (out_dir / "stale.csv").write_text("stale", encoding="utf-8")

    cr.prepare_out_dir(out_dir, overwrite=False)
    assert not (out_dir / "stale.csv").exists()


# ---------------------------------------------------------------------------
# 12. Filenames / output locations are deterministic.
# ---------------------------------------------------------------------------


def test_output_filenames_and_locations_are_fixed(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A"}}, ["A"])
    out_dir = _assemble(tmp_path, bundle_out_dir)

    assert (out_dir / "reference_comparison_summary.csv").is_file()
    assert (out_dir / "reference_comparison_detail.csv").is_file()
    assert (out_dir / "reference_comparison_diagnostics.json").is_file()


# ---------------------------------------------------------------------------
# 13. Command exit behavior matches documented status behavior.
# ---------------------------------------------------------------------------


def test_main_returns_zero_on_success(tmp_path, monkeypatch):
    domain = "object_styles_model"
    reference = tmp_path / "ref.details.json"
    target = tmp_path / "t1.details.json"
    _write_json(reference, {"ok": True})
    _write_json(target, {"ok": True})
    out_dir = tmp_path / "out"

    def fake_execute(cmd):
        cmd = list(cmd)
        if "run_bundle_analysis.py" in cmd[1]:
            bundle_out_dir = Path(cmd[cmd.index("--out-dir") + 1])
            bundle_out_dir_bundle = _setup_compare_all(tmp_path / "fixture", domain, {"t1": {"A"}}, ["A"])
            import shutil as _sh

            if bundle_out_dir.exists():
                _sh.rmtree(bundle_out_dir)
            _sh.copytree(bundle_out_dir_bundle, bundle_out_dir)
        return None

    monkeypatch.setattr(cr, "_execute", fake_execute)
    rc = cr.main(["--reference", str(reference), "--target", str(target), "--out-dir", str(out_dir)])
    assert rc == 0
    manifest = json.loads((out_dir / cr.MANIFEST_FILENAME).read_text())
    assert manifest["aggregate_comparison_status"] == COMPARISON_STATUS_OK


def test_main_returns_two_on_validation_error(tmp_path):
    rc = cr.main(["--reference", str(tmp_path / "nope.json"), "--target", str(tmp_path / "also_nope.json"), "--out-dir", str(tmp_path / "out")])
    assert rc == 2


def test_main_requires_target_or_target_dir(tmp_path, capsys):
    with pytest.raises(SystemExit):
        cr.main(["--reference", str(tmp_path / "ref.json"), "--out-dir", str(tmp_path / "out")])


# ---------------------------------------------------------------------------
# 14. Reference and target provenance present in every required artifact.
# ---------------------------------------------------------------------------


def test_provenance_present_in_every_artifact(tmp_path):
    domain = "object_styles_model"
    bundle_out_dir = _setup_compare_all(tmp_path, domain, {"t1": {"A"}}, ["A"])
    ref_file = tmp_path / "ref.details.json"
    _write_json(ref_file, {"ok": True})
    out_dir = tmp_path / "final_out"
    out_dir.mkdir()
    cr.assemble_outputs(
        bundle_out_dir, "all", out_dir, ref_file,
        {"reference_files": [str(ref_file)], "target_files": ["t1"], "target_file_count": 1},
        ["extract", "cmd"], ["bundle", "cmd"],
    )

    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["reference_bundle_id"] == "ref-2026-08-27"
    assert summary[0]["target_export_run_id"] == "t1"

    detail = _read_csv(out_dir / cr.DETAIL_FILENAME)
    assert detail[0]["reference_bundle_id"] == "ref-2026-08-27"
    assert detail[0]["target_export_run_id"] == "t1"

    diagnostics = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diagnostics["reference_bundle_id"] == "ref-2026-08-27"
    assert diagnostics["analysis_run_id"] == "run1"

    manifest = json.loads((out_dir / cr.MANIFEST_FILENAME).read_text())
    assert manifest["reference_export"] == str(ref_file)
    assert manifest["reference_bundle_id"] == "ref-2026-08-27"
    assert manifest["target_scope"]["target_file_count"] == 1


# ---------------------------------------------------------------------------
# Staging / discovery helper tests (pure filesystem logic, no subprocess).
# ---------------------------------------------------------------------------


def test_stage_comparison_inputs_one_target(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    target = src_dir / "t1.details.json"
    _write_json(reference, {"a": 1})
    _write_json(target, {"a": 2})

    staging_dir = tmp_path / "staged"
    info = cr.stage_comparison_inputs(reference, [target], None, staging_dir)

    assert (staging_dir / "ref.details.json").is_file()
    assert (staging_dir / "t1.details.json").is_file()
    assert info["target_file_count"] == 1


def test_stage_comparison_inputs_multiple_targets_via_target_dir(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    _write_json(reference, {"a": 1})
    corpus_dir = tmp_path / "corpus"
    for name in ("t1.details.json", "t2.details.json", "t3.details.json"):
        _write_json(corpus_dir / name, {"a": name})

    staging_dir = tmp_path / "staged"
    info = cr.stage_comparison_inputs(reference, [], corpus_dir, staging_dir)

    assert info["target_file_count"] == 3
    for name in ("t1.details.json", "t2.details.json", "t3.details.json"):
        assert (staging_dir / name).is_file()


def test_stage_comparison_inputs_excludes_reference_copy_from_target_dir(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    _write_json(reference, {"a": 1})
    corpus_dir = tmp_path / "corpus"
    _write_json(corpus_dir / "ref.details.json", {"a": 1})  # accidental duplicate of the reference
    _write_json(corpus_dir / "t1.details.json", {"a": 2})

    staging_dir = tmp_path / "staged"
    info = cr.stage_comparison_inputs(reference, [], corpus_dir, staging_dir)

    assert info["target_file_count"] == 1
    assert info["target_files"] == [str(staging_dir / "t1.details.json")]


def test_stage_comparison_inputs_stages_split_export_siblings(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    _write_json(reference, {"a": 1})
    _write_json(src_dir / "ref.index.json", {"a": "index"})
    target = src_dir / "t1.details.json"
    _write_json(target, {"a": 2})
    _write_json(src_dir / "t1.index.json", {"a": "index"})

    staging_dir = tmp_path / "staged"
    cr.stage_comparison_inputs(reference, [target], None, staging_dir)

    assert (staging_dir / "ref.index.json").is_file()
    assert (staging_dir / "t1.index.json").is_file()


def test_stage_comparison_inputs_never_pulls_in_legacy_sibling(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    _write_json(reference, {"a": 1})
    _write_json(src_dir / "ref.legacy.json", {"a": "legacy"})
    target = src_dir / "t1.details.json"
    _write_json(target, {"a": 2})

    staging_dir = tmp_path / "staged"
    cr.stage_comparison_inputs(reference, [target], None, staging_dir)

    assert not (staging_dir / "ref.legacy.json").exists()


def test_stage_comparison_inputs_raises_on_no_targets_after_exclusion(tmp_path):
    src_dir = tmp_path / "src"
    reference = src_dir / "ref.details.json"
    _write_json(reference, {"a": 1})
    corpus_dir = tmp_path / "corpus"
    _write_json(corpus_dir / "ref.details.json", {"a": 1})  # only the reference itself

    with pytest.raises(cr.CompareReferenceError, match="nothing to compare"):
        cr.stage_comparison_inputs(reference, [], corpus_dir, tmp_path / "staged")


def test_pick_primary_export_prefers_fingerprint_then_details_then_plain(tmp_path):
    a = tmp_path / "x.details.json"
    b = tmp_path / "x__fingerprint.json"
    c = tmp_path / "x.index.json"
    for p in (a, b, c):
        p.touch()
    assert cr._pick_primary_export([a, b, c]) == b
    assert cr._pick_primary_export([a, c]) == a
    assert cr._pick_primary_export([c]) == c
