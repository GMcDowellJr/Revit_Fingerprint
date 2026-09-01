# tests/test_compare_reference_multi.py
#
# Regression tests for the additive fan-out driver tools/compare_reference_multi.py.
#
# This driver never imports or modifies tools/compare_reference.py -- every
# (reference_segment, reference, target_segment) combo is run as its own real
# `python tools/compare_reference.py` subprocess. The end-to-end test below
# calls compare_reference_multi.main() in-process (same convention
# tests/test_compare_reference.py uses for compare_reference.py itself), which
# is sufficient to exercise the real subprocess fan-out: main() internally
# spawns real child processes via ProcessPoolExecutor + subprocess.run --
# nothing about the subprocess boundary is mocked or stubbed here.
#
# Use synthetic fixtures only. No Revit dependency, no access to a real corpus.

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Sequence

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for _candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import compare_reference_multi as crm  # noqa: E402


def _write_csv(path: Path, fieldnames: Sequence[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


DOMAIN = "object_styles_model"
RUN_ID = "run1"

_REGISTRY_FIELDNAMES = [
    "segment_id", "parent_segment_id", "run_type", "population_hash",
    "conformance_reference_mode", "output_folder", "status", "last_run_utc",
    "notes", "segment_purpose", "segment_label",
]


def _registry_row(segment_id: str, status: str = "complete") -> Dict[str, str]:
    return {
        "segment_id": segment_id, "parent_segment_id": "", "run_type": "bundle",
        "population_hash": "", "conformance_reference_mode": "", "output_folder": segment_id,
        "status": status, "last_run_utc": "", "notes": "", "segment_purpose": "", "segment_label": "",
    }


def _build_synthetic_corpus(tmp_path: Path) -> Dict[str, Path]:
    """Build a 4-segment synthetic corpus sharing one run_registry.csv,
    mirroring the minimal per-segment shape tests/test_compare_reference.py's
    own _build_segment() helper already proves drives compare_reference.py
    end-to-end (registry row + file_metadata/records/pattern_presence/
    domain_patterns/membership_matrix), confirmed exact in Step 0
    (audit_results/compare_reference_multi_step0_findings.md).

    Two "reference" segments:
      - synthetic_template_a (imperial), export synthetic_ref_export_alpha
        with patterns [p1, p2].
      - synthetic_template_b (metric), export synthetic_ref_export_beta with
        pattern [p1].
    Two "target" segments, both imperial:
      - synthetic_container_a: two exports with overlapping/differing
        pattern evidence.
      - synthetic_project_a: one export.

    template_b's metric unit_system deliberately mismatches both target
    segments' imperial unit_system, so every (template_b, *) combo blocks at
    the CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH pre-flight gate (returncode 2) --
    giving a 2x2 grid with 2 ok combos and 2 cleanly-blocked combos, so the
    aggregator's zero-row-for-blocked-combo handling is actually exercised,
    not just the happy path.
    """
    segments_root = tmp_path / "segments"
    corpus_records_dir = tmp_path / "corpus" / "records"
    registry_file = corpus_records_dir / "run_registry.csv"
    registry_rows: List[Dict[str, str]] = []

    def add_segment(segment_id: str, export_patterns: Dict[str, List[str]], unit_system: str) -> None:
        seg_root = segments_root / segment_id
        seg_records = seg_root / "results" / "records"
        seg_analysis = seg_root / "results" / "analysis"
        seg_bundle = seg_root / "results" / "bundle_analysis"

        registry_rows.append(_registry_row(segment_id))

        all_ids = sorted(export_patterns.keys())
        _write_csv(
            seg_records / "file_metadata.csv",
            ["export_run_id", "central_path", "governance_role", "unit_system"],
            [
                {"export_run_id": eid, "central_path": f"/x/{segment_id}/{eid}", "governance_role": "Project", "unit_system": unit_system}
                for eid in all_ids
            ],
        )
        _write_csv(
            seg_records / "records.csv",
            ["export_run_id", "domain", "join_key_schema", "join_key_policy_id", "join_key_policy_version"],
            [
                {"export_run_id": eid, "domain": DOMAIN, "join_key_schema": f"{DOMAIN}.join_key.v1", "join_key_policy_id": "p1", "join_key_policy_version": "1"}
                for eid in all_ids
            ],
        )

        presence_rows: List[Dict[str, str]] = []
        membership_rows: List[Dict[str, str]] = []
        for eid, pattern_ids in export_patterns.items():
            for pid in pattern_ids:
                presence_rows.append(
                    {"analysis_run_id": RUN_ID, "domain": DOMAIN, "export_run_id": eid, "pattern_id": pid, "pattern_share_pct": "1.000000"}
                )
                membership_rows.append({"analysis_run_id": RUN_ID, "export_run_id": eid, "pattern_id": pid})
        _write_csv(
            seg_analysis / "pattern_presence_file.csv",
            ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
            presence_rows,
        )
        _write_csv(seg_analysis / "corpus_manifest.csv", ["schema_version"], [{"schema_version": "2.1.0"}])

        distinct_pattern_ids = sorted({pid for pids in export_patterns.values() for pid in pids})
        _write_csv(
            seg_analysis / "domain_patterns.csv",
            ["schema_version", "analysis_run_id", "domain", "pattern_id", "source_cluster_id"],
            [
                {"schema_version": "2.1.0", "analysis_run_id": RUN_ID, "domain": DOMAIN, "pattern_id": pid, "source_cluster_id": f"{DOMAIN}|{pid}"}
                for pid in distinct_pattern_ids
            ],
        )
        for view in ("all", "used"):
            _write_csv(
                seg_bundle / view / DOMAIN / "membership_matrix.csv",
                ["analysis_run_id", "export_run_id", "pattern_id"],
                membership_rows,
            )

    add_segment("synthetic_template_a", {"synthetic_ref_export_alpha": ["p1", "p2"]}, "imperial")
    add_segment("synthetic_template_b", {"synthetic_ref_export_beta": ["p1"]}, "metric")
    add_segment(
        "synthetic_container_a",
        {"container_export_1": ["p1", "p2"], "container_export_2": ["p2"]},
        "imperial",
    )
    add_segment("synthetic_project_a", {"project_export_1": ["p1"]}, "imperial")

    _write_csv(registry_file, _REGISTRY_FIELDNAMES, registry_rows)
    return {"segments_root": segments_root, "registry_file": registry_file}


def _write_references_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    _write_csv(path, ["reference_segment", "reference"], rows)


# ---------------------------------------------------------------------------
# Input validation (unit-level, no subprocess needed)
# ---------------------------------------------------------------------------


def test_read_references_csv_valid(tmp_path):
    path = tmp_path / "references.csv"
    _write_references_csv(
        path,
        [
            {"reference_segment": "seg_a", "reference": "ref1"},
            {"reference_segment": "seg_b", "reference": "ref2"},
        ],
    )
    rows = crm.read_references_csv(path)
    assert rows == [
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
        crm.ReferenceRow(reference_segment="seg_b", reference="ref2"),
    ]


def test_read_references_csv_missing_file(tmp_path):
    with pytest.raises(SystemExit) as excinfo:
        crm.read_references_csv(tmp_path / "does_not_exist.csv")
    assert "does_not_exist.csv" in str(excinfo.value)


def test_read_references_csv_malformed_header(tmp_path):
    path = tmp_path / "references.csv"
    path.write_text("reference,reference_segment\nref1,seg_a\n", encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        crm.read_references_csv(path)
    message = str(excinfo.value)
    assert str(path) in message
    assert "reference_segment,reference" in message


def test_read_references_csv_extra_column_header(tmp_path):
    path = tmp_path / "references.csv"
    path.write_text("reference_segment,reference,extra\nseg_a,ref1,x\n", encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        crm.read_references_csv(path)
    assert "reference_segment,reference" in str(excinfo.value)


def test_read_references_csv_zero_data_rows(tmp_path):
    path = tmp_path / "references.csv"
    path.write_text("reference_segment,reference\n", encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        crm.read_references_csv(path)
    assert "zero data rows" in str(excinfo.value)


def test_read_references_csv_blank_value_row(tmp_path):
    path = tmp_path / "references.csv"
    path.write_text("reference_segment,reference\nseg_a,\n", encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        crm.read_references_csv(path)
    assert "blank" in str(excinfo.value)


def test_read_references_csv_duplicate_row_deduped(tmp_path):
    path = tmp_path / "references.csv"
    _write_references_csv(
        path,
        [
            {"reference_segment": "seg_a", "reference": "ref1"},
            {"reference_segment": "seg_b", "reference": "ref2"},
            {"reference_segment": "seg_a", "reference": "ref1"},  # exact duplicate of row 1
        ],
    )
    rows = crm.read_references_csv(path)
    assert rows == [
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
        crm.ReferenceRow(reference_segment="seg_b", reference="ref2"),
    ]


def test_read_target_segments_from_file(tmp_path):
    path = tmp_path / "targets.txt"
    path.write_text("# comment\nseg_1\n\nseg_2\n", encoding="utf-8")
    assert crm.read_target_segments(str(path)) == ["seg_1", "seg_2"]


def test_read_target_segments_from_file_deduped(tmp_path):
    path = tmp_path / "targets.txt"
    path.write_text("seg_1\nseg_2\nseg_1\n", encoding="utf-8")
    assert crm.read_target_segments(str(path)) == ["seg_1", "seg_2"]


def test_read_target_segments_inline_list():
    assert crm.read_target_segments("seg_1, seg_2 ,seg_3") == ["seg_1", "seg_2", "seg_3"]


def test_read_target_segments_inline_list_deduped():
    assert crm.read_target_segments("seg_1,seg_2,seg_1") == ["seg_1", "seg_2"]


def test_read_target_segments_empty_inline_list():
    with pytest.raises(SystemExit) as excinfo:
        crm.read_target_segments("   ,  ,")
    assert "zero target" in str(excinfo.value)


def test_sanitize_path_component_collapses_unsafe_characters():
    assert crm._sanitize_path_component("Central File (v3).rvt") == "Central_File_v3_.rvt"
    assert crm._sanitize_path_component("a/b\\c") == "a_b_c"
    assert crm._sanitize_path_component("") == "_"


def test_sanitize_path_component_bounds_long_values():
    long_value = "x" * 400
    sanitized = crm._sanitize_path_component(long_value)
    assert len(sanitized) == crm._MAX_COMPONENT_LENGTH + 1 + 8  # truncated + "_" + 8-hex digest
    assert sanitized.startswith("x" * crm._MAX_COMPONENT_LENGTH)

    # two different long values sharing a long common prefix must not collide
    long_value_b = ("x" * 300) + "DIFFERENT_TAIL"
    sanitized_b = crm._sanitize_path_component(long_value_b)
    assert sanitized != sanitized_b


def test_build_combos_rejects_direct_duplicate(tmp_path):
    # build_combos() itself must reject an exact duplicate combo even if
    # called directly with un-deduplicated input (belt-and-suspenders on top
    # of read_references_csv()/read_target_segments()'s own dedup).
    references = [
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
    ]
    with pytest.raises(SystemExit) as excinfo:
        crm.build_combos(references, ["tgt1"], tmp_path / "out")
    assert "duplicate combo" in str(excinfo.value)


def test_build_combos_cross_product(tmp_path):
    references = [
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
        crm.ReferenceRow(reference_segment="seg_b", reference="ref2"),
    ]
    combos = crm.build_combos(references, ["tgt1", "tgt2"], tmp_path / "out")
    assert len(combos) == 4
    assert {c.combo_key for c in combos} == {
        "seg_a::ref1::tgt1", "seg_a::ref1::tgt2", "seg_b::ref2::tgt1", "seg_b::ref2::tgt2",
    }
    # every combo gets a distinct, deterministic out_dir under out_root
    assert len({c.out_dir for c in combos}) == 4
    for c in combos:
        assert c.out_dir.parent == tmp_path / "out"


def test_build_combos_collision_raises(tmp_path):
    # Two references whose sanitized names collapse to the same directory
    # component must not silently overwrite each other's output.
    references = [
        crm.ReferenceRow(reference_segment="seg_a", reference="ref/1"),
        crm.ReferenceRow(reference_segment="seg_a", reference="ref!1"),
    ]
    with pytest.raises(SystemExit) as excinfo:
        crm.build_combos(references, ["tgt1"], tmp_path / "out")
    assert "same --out-dir" in str(excinfo.value)


def test_build_combos_collision_raises_case_insensitive(tmp_path):
    # "Ref1" and "ref1" sanitize to two distinct (case-differing) strings,
    # but resolve to the same directory on a case-insensitive filesystem
    # (Windows, default macOS) -- must still be rejected, not just an exact
    # raw-string match.
    references = [
        crm.ReferenceRow(reference_segment="seg_a", reference="Ref1"),
        crm.ReferenceRow(reference_segment="seg_a", reference="ref1"),
    ]
    with pytest.raises(SystemExit) as excinfo:
        crm.build_combos(references, ["tgt1"], tmp_path / "out")
    assert "same --out-dir" in str(excinfo.value)


def test_classify_combo_status():
    assert crm.classify_combo_status(0) == crm.COMBO_STATUS_OK
    assert crm.classify_combo_status(2) == crm.COMBO_STATUS_BLOCKED
    assert crm.classify_combo_status(1) == crm.COMBO_STATUS_CRASHED
    assert crm.classify_combo_status(-1) == crm.COMBO_STATUS_CRASHED


def test_clear_stale_combo_output_removes_existing_directory(tmp_path):
    out_dir = tmp_path / "combo_out"
    (out_dir / "sub").mkdir(parents=True)
    (out_dir / crm.CHILD_SUMMARY_FILENAME).write_text("stale content", encoding="utf-8")
    assert out_dir.exists()

    crm.clear_stale_combo_output(out_dir)
    assert not out_dir.exists()


def test_clear_stale_combo_output_noop_when_absent(tmp_path):
    out_dir = tmp_path / "does_not_exist"
    crm.clear_stale_combo_output(out_dir)  # must not raise
    assert not out_dir.exists()


def test_aggregate_summaries_no_data_returns_none_not_error(tmp_path):
    """Every combo either crashed or was cleanly blocked before writing any
    output at all (e.g. compare_reference.py's own pre-flight out-dir-safety
    refusal, which writes no summary/manifest -- see the Step 0 findings
    doc's finding #2). This must not be treated as an aggregation failure:
    the driver's own exit code depends only on main()'s combos_crashed
    count, not on whether aggregate_summaries() found any data.
    """
    out_root = tmp_path / "out_root"
    combo = crm.Combo(reference_segment="seg_a", reference="ref1", target_segment="tgt1", out_dir=out_root / "combo_a")
    # combo.out_dir deliberately never created -- no summary file anywhere.
    summary_path, rows_written, combos_included, combos_skipped = crm.aggregate_summaries(
        [combo], [{"combo_key": combo.combo_key, "reference_segment_id": "seg_a"}], out_root
    )
    assert summary_path is None
    assert rows_written == 0
    assert combos_included == 0
    assert combos_skipped == [combo.combo_key]


def test_aggregate_summaries_reads_current_data(tmp_path):
    out_root = tmp_path / "out_root"
    combo = crm.Combo(reference_segment="seg_b", reference="ref2", target_segment="tgt2", out_dir=out_root / "combo_fresh")
    fieldnames = ["segment_id", "purge_view", "domain"]
    combo.out_dir.mkdir(parents=True)
    _write_csv(
        combo.out_dir / crm.CHILD_SUMMARY_FILENAME,
        fieldnames,
        [{"segment_id": "seg_fresh_target", "purge_view": "used", "domain": "object_styles_model"}],
    )
    summary_path, rows_written, combos_included, combos_skipped = crm.aggregate_summaries(
        [combo], [{"combo_key": combo.combo_key, "reference_segment_id": "seg_b"}], out_root
    )
    assert summary_path is not None
    assert rows_written == 1
    assert combos_included == 1
    assert combos_skipped == []
    rows = _read_csv(summary_path)
    assert rows[0]["segment_id"] == "seg_fresh_target"
    assert rows[0]["reference"] == "ref2"
    assert rows[0]["reference_segment_id"] == "seg_b"


# ---------------------------------------------------------------------------
# End-to-end: real subprocess fan-out against the synthetic fixture corpus
# ---------------------------------------------------------------------------


def test_end_to_end_grid(tmp_path):
    corpus = _build_synthetic_corpus(tmp_path)

    references_csv = tmp_path / "references.csv"
    _write_references_csv(
        references_csv,
        [
            {"reference_segment": "synthetic_template_a", "reference": "synthetic_ref_export_alpha"},
            {"reference_segment": "synthetic_template_b", "reference": "synthetic_ref_export_beta"},
        ],
    )
    target_segments_txt = tmp_path / "targets.txt"
    target_segments_txt.write_text("synthetic_container_a\nsynthetic_project_a\n", encoding="utf-8")

    out_root = tmp_path / "multi_out"

    rc = crm.main(
        [
            "--segments-root", str(corpus["segments_root"]),
            "--registry-file", str(corpus["registry_file"]),
            "--references", str(references_csv),
            "--target-segments", str(target_segments_txt),
            "--out-root", str(out_root),
            "--workers", "2",
        ]
    )

    # No combo should crash: 2 combos succeed (template_a x each target),
    # 2 combos cleanly block on CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH
    # (template_b x each target) -- neither outcome is a driver-level crash.
    assert rc == 0

    # --- run report ---
    run_report_path = out_root / crm.RUN_REPORT_FILENAME
    assert run_report_path.is_file()
    run_report = json.loads(run_report_path.read_text(encoding="utf-8"))
    assert run_report["combos_total"] == 4
    assert run_report["combos_ok"] == 2
    assert run_report["combos_blocked"] == 2
    assert run_report["combos_crashed"] == 0

    combos_by_key = {c["combo_key"]: c for c in run_report["combos"]}
    assert len(combos_by_key) == 4

    ok_keys = [
        "synthetic_template_a::synthetic_ref_export_alpha::synthetic_container_a",
        "synthetic_template_a::synthetic_ref_export_alpha::synthetic_project_a",
    ]
    blocked_keys = [
        "synthetic_template_b::synthetic_ref_export_beta::synthetic_container_a",
        "synthetic_template_b::synthetic_ref_export_beta::synthetic_project_a",
    ]
    for key in ok_keys:
        entry = combos_by_key[key]
        assert entry["returncode"] == 0
        assert entry["combo_status"] == crm.COMBO_STATUS_OK
        assert entry["reference_segment_id"] == "synthetic_template_a"
        assert entry["aggregate_comparison_status"] in ("ok", "degraded")
        assert Path(entry["out_dir"]).is_dir()
        assert (Path(entry["out_dir"]) / crm.CHILD_SUMMARY_FILENAME).is_file()

    for key in blocked_keys:
        entry = combos_by_key[key]
        assert entry["returncode"] == 2
        assert entry["combo_status"] == crm.COMBO_STATUS_BLOCKED
        assert entry["reference_segment_id"] == "synthetic_template_b"
        assert entry["aggregate_comparison_status"] == "blocked"
        # a blocked combo still writes a header-only summary CSV -- not missing
        summary_path = Path(entry["out_dir"]) / crm.CHILD_SUMMARY_FILENAME
        assert summary_path.is_file()
        assert _read_csv(summary_path) == []

    # --- aggregated summary ---
    multi_summary_path = out_root / crm.MULTI_SUMMARY_FILENAME
    assert multi_summary_path.is_file()
    rows = _read_csv(multi_summary_path)
    assert len(rows) > 0
    # every row comes from an ok combo (blocked combos contributed 0 rows)
    seen_references = {row["reference"] for row in rows}
    assert seen_references == {"synthetic_ref_export_alpha"}
    for row in rows:
        assert row["reference_segment_id"] == "synthetic_template_a"
        assert row["segment_id"] in ("synthetic_container_a", "synthetic_project_a")
        assert row["domain"] == DOMAIN
        # column ordering: reference/reference_segment_id lead, followed by
        # the child summary CSV's own columns (segment_id first among those)
        assert list(row.keys())[0] == "reference"
        assert list(row.keys())[1] == "reference_segment_id"


def test_end_to_end_grid_inline_target_segments(tmp_path):
    """Same corpus, but --target-segments passed as an inline comma-separated
    list instead of a file, exercising that alternate input form end-to-end.
    """
    corpus = _build_synthetic_corpus(tmp_path)

    references_csv = tmp_path / "references.csv"
    _write_references_csv(
        references_csv,
        [{"reference_segment": "synthetic_template_a", "reference": "synthetic_ref_export_alpha"}],
    )
    out_root = tmp_path / "multi_out"

    rc = crm.main(
        [
            "--segments-root", str(corpus["segments_root"]),
            "--registry-file", str(corpus["registry_file"]),
            "--references", str(references_csv),
            "--target-segments", "synthetic_container_a,synthetic_project_a",
            "--out-root", str(out_root),
            "--workers", "2",
        ]
    )
    assert rc == 0
    run_report = json.loads((out_root / crm.RUN_REPORT_FILENAME).read_text(encoding="utf-8"))
    assert run_report["combos_total"] == 2
    assert run_report["combos_ok"] == 2

    rows = _read_csv(out_root / crm.MULTI_SUMMARY_FILENAME)
    assert len(rows) > 0
    assert {row["segment_id"] for row in rows} == {"synthetic_container_a", "synthetic_project_a"}


def test_end_to_end_rerun_does_not_leak_stale_summary(tmp_path):
    """A combo's out_dir pre-populated with foreign/stale content (as if left
    behind by an earlier, since-interrupted invocation into this same
    --out-root) must not leak into the aggregate, and must not cause the
    child to block on compare_reference.py's own foreign-directory refusal
    either -- clear_stale_combo_output() removes it before the child is ever
    submitted.
    """
    corpus = _build_synthetic_corpus(tmp_path)
    references_csv = tmp_path / "references.csv"
    _write_references_csv(
        references_csv,
        [{"reference_segment": "synthetic_template_a", "reference": "synthetic_ref_export_alpha"}],
    )
    target_segments_txt = tmp_path / "targets.txt"
    target_segments_txt.write_text("synthetic_container_a\n", encoding="utf-8")
    out_root = tmp_path / "multi_out"

    # Seed the combo's deterministic out_dir with foreign content (no
    # manifest -- compare_reference.py's own prepare_out_dir() would refuse
    # to clear this without --overwrite, absent the driver's own pre-clear).
    references = crm.read_references_csv(references_csv)
    target_segments = crm.read_target_segments(str(target_segments_txt))
    combos = crm.build_combos(references, target_segments, out_root)
    assert len(combos) == 1
    stale_out_dir = combos[0].out_dir
    stale_out_dir.mkdir(parents=True)
    _write_csv(
        stale_out_dir / crm.CHILD_SUMMARY_FILENAME,
        ["segment_id", "purge_view", "domain"],
        [{"segment_id": "STALE_FROM_PRIOR_RUN", "purge_view": "used", "domain": "object_styles_model"}],
    )

    rc = crm.main(
        [
            "--segments-root", str(corpus["segments_root"]),
            "--registry-file", str(corpus["registry_file"]),
            "--references", str(references_csv),
            "--target-segments", str(target_segments_txt),
            "--out-root", str(out_root),
            "--workers", "1",
        ]
    )
    assert rc == 0
    run_report = json.loads((out_root / crm.RUN_REPORT_FILENAME).read_text(encoding="utf-8"))
    assert run_report["combos_ok"] == 1
    assert run_report["combos_crashed"] == 0

    rows = _read_csv(out_root / crm.MULTI_SUMMARY_FILENAME)
    assert len(rows) > 0
    assert all(row["segment_id"] != "STALE_FROM_PRIOR_RUN" for row in rows)
    assert all(row["segment_id"] == "synthetic_container_a" for row in rows)
