# tests/test_compare_reference_name_overlap.py
#
# Regression tests for tools/compare_reference.py's Step 1 Part B addition:
# --include-name-overlap / compute_name_overlap_rows() / reference_comparison_name_overlap.csv.
#
# Unlike compute_semantic_changes_rows() (same-segment-only, string-matched), this
# classifies the SET relationship between the reference side's and target side's name-key
# join_hash values for each pattern identity, and is sound in both same-segment and
# cross-segment mode. See docs/namekey_crosssegment_step0_findings.md and
# tools/compare_reference.py's own NAME_SETS_*/NAME_EVIDENCE_* constants for the full design
# rationale (B1-B4).
#
# Use synthetic fixtures only. No Revit dependency.

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List, Sequence

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for _candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import compare_reference as cr  # noqa: E402


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


def _write_side(
    seg_root: Path,
    records_rows: List[Dict[str, str]],
    domain_patterns_rows: List[Dict[str, str]],
    name_key_rows: List[Dict[str, str]] = None,
) -> None:
    _write_csv(
        seg_root / "results" / "records" / "records.csv",
        ["export_run_id", "domain", "record_id", "join_hash"],
        records_rows,
    )
    _write_csv(
        seg_root / "results" / "analysis" / "domain_patterns.csv",
        ["domain", "pattern_id", "source_cluster_id"],
        domain_patterns_rows,
    )
    if name_key_rows is not None:
        _write_csv(
            seg_root / "results" / "name_key" / "name_key_results.csv",
            ["export_file", "domain", "record_id", "label_display", "join_hash", "status"],
            name_key_rows,
        )


# ---------------------------------------------------------------------------
# compute_name_overlap_rows() -- direct unit tests (no CLI/segment-resolution
# scaffolding; exercises the classification/join logic in isolation).
# ---------------------------------------------------------------------------


def _detail_row(domain: str, pattern_id: str, comparison_class: str, target_export_run_id: str = "t1.details.json") -> Dict[str, str]:
    return {
        "purge_view": "all", "domain": domain, "population_id": "p1",
        "target_export_run_id": target_export_run_id, "pattern_id": pattern_id,
        "comparison_class": comparison_class, "segment_id": "tgt",
        "reference_bundle_id": "rb1", "analysis_run_id": "run1",
    }


def test_cross_segment_overlap_and_target_only_and_excluded(tmp_path):
    ref_root = tmp_path / "ref"
    tgt_root = tmp_path / "tgt"

    _write_side(
        ref_root,
        records_rows=[{"export_run_id": "r1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:1", "join_hash": "cfgA"}],
        domain_patterns_rows=[
            {"domain": "arrowheads", "pattern_id": "pat_ref1", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
            {"domain": "line_styles", "pattern_id": "pat_refLS", "source_cluster_id": "line_styles|cfg.schema.v1|cfgLS"},
        ],
        name_key_rows=[
            {"export_file": "r1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:1", "label_display": "Arrow 15deg", "join_hash": "nameX", "status": "ok"},
        ],
    )
    _write_side(
        tgt_root,
        records_rows=[
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:10", "join_hash": "cfgA"},
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:11", "join_hash": "cfgA"},
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:20", "join_hash": "cfgB"},
        ],
        domain_patterns_rows=[
            {"domain": "arrowheads", "pattern_id": "pat_tgt1", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
            {"domain": "arrowheads", "pattern_id": "pat_tgt2", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgB"},
            {"domain": "line_styles", "pattern_id": "pat_tgtLS", "source_cluster_id": "line_styles|cfg.schema.v1|cfgLS"},
        ],
        name_key_rows=[
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:10", "label_display": "Arrow 15deg", "join_hash": "nameX", "status": "ok"},
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:11", "label_display": "Arrow 15deg (2)", "join_hash": "nameY", "status": "ok"},
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:20", "label_display": "Zigzag", "join_hash": "nameZ", "status": "ok"},
        ],
    )

    # Cross-segment mode: all_detail_rows["pattern_id"] already holds join_hash directly
    # (resolve_cross_segment_pattern_identity() translates both sides before
    # run_compare_for_domain ever runs -- see that function's docstring).
    all_detail_rows = [
        _detail_row("arrowheads", "cfgA", "shared"),
        _detail_row("arrowheads", "cfgB", "target_only"),
        _detail_row("line_styles", "cfgLS", "shared"),
    ]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads", "line_styles"], ref_root, tgt_root, same_segment=False,
        reference_export_run_id="r1.details.json",
    )
    by_pattern = {(r["domain"], r["pattern_id"]): r for r in rows}

    shared_row = by_pattern[("arrowheads", "cfgA")]
    assert shared_row["name_set_classification"] == cr.NAME_SETS_OVERLAP
    assert shared_row["reference_name_hashes"] == "nameX"
    assert shared_row["target_name_hashes"] == "nameX|nameY"
    assert shared_row["reference_name_hash_count"] == "1"
    assert shared_row["target_name_hash_count"] == "2"
    assert shared_row["shared_name_hash_count"] == "1"

    target_only_row = by_pattern[("arrowheads", "cfgB")]
    assert target_only_row["name_set_classification"] == cr.NAME_EVIDENCE_MISSING
    assert target_only_row["reference_name_hashes"] == ""
    assert target_only_row["target_name_hashes"] == "nameZ"

    excluded_row = by_pattern[("line_styles", "cfgLS")]
    assert excluded_row["name_set_classification"] == cr.NAME_EVIDENCE_EXCLUDED
    assert excluded_row["exclusion_reason"] == "no_name_like_key"
    assert excluded_row["reference_name_hashes"] == ""
    assert excluded_row["target_name_hashes"] == ""


def test_same_segment_identical_sets_translates_pattern_id_via_domain_patterns(tmp_path):
    seg_root = tmp_path / "seg"
    _write_side(
        seg_root,
        records_rows=[
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:10", "join_hash": "cfgA"},
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:11", "join_hash": "cfgA"},
        ],
        domain_patterns_rows=[
            {"domain": "arrowheads", "pattern_id": "pat_x", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
        ],
        name_key_rows=[
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:10", "label_display": "Arrow 15deg", "join_hash": "nameX", "status": "ok"},
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:11", "label_display": "Arrow 15deg (2)", "join_hash": "nameY", "status": "ok"},
        ],
    )
    all_detail_rows = [_detail_row("arrowheads", "pat_x", "shared")]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads"], seg_root, seg_root, same_segment=True,
        reference_export_run_id="t1.details.json",
    )
    assert len(rows) == 1
    assert rows[0]["name_set_classification"] == cr.NAME_SETS_IDENTICAL
    assert rows[0]["reference_name_hashes"] == rows[0]["target_name_hashes"] == "nameX|nameY"


def test_same_segment_two_different_files_are_not_falsely_identical(tmp_path):
    """Regression for PR #476 review (P1): comparing two DIFFERENT files within the same
    segment must scope each side to its own file's names, never the segment-wide aggregate --
    otherwise, since reference_facets and target_facets are the same object in same-segment
    mode, every same-segment comparison would collapse to name_sets_identical regardless of
    what the two files actually contain."""
    seg_root = tmp_path / "seg"
    _write_side(
        seg_root,
        records_rows=[
            {"export_run_id": "old.details.json", "domain": "arrowheads", "record_id": "a:1", "join_hash": "cfgA"},
            {"export_run_id": "new.details.json", "domain": "arrowheads", "record_id": "a:2", "join_hash": "cfgA"},
        ],
        domain_patterns_rows=[
            {"domain": "arrowheads", "pattern_id": "pat_x", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
        ],
        name_key_rows=[
            {"export_file": "old.details.json", "domain": "arrowheads", "record_id": "a:1", "label_display": "Old Name", "join_hash": "nameOld", "status": "ok"},
            {"export_file": "new.details.json", "domain": "arrowheads", "record_id": "a:2", "label_display": "New Name", "join_hash": "nameNew", "status": "ok"},
        ],
    )
    all_detail_rows = [_detail_row("arrowheads", "pat_x", "shared", target_export_run_id="new.details.json")]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads"], seg_root, seg_root, same_segment=True,
        reference_export_run_id="old.details.json",
    )
    assert len(rows) == 1
    assert rows[0]["name_set_classification"] == cr.NAME_SETS_DISJOINT
    assert rows[0]["reference_name_hashes"] == "nameOld"
    assert rows[0]["target_name_hashes"] == "nameNew"


def test_cross_segment_two_target_files_do_not_contaminate_each_other(tmp_path):
    """Regression for PR #476 review (P1): when the target segment has multiple files under
    the same config pattern with different names, each all_detail_rows row (one per target
    file) must see only that file's own names, not the segment-wide union."""
    ref_root = tmp_path / "ref"
    tgt_root = tmp_path / "tgt"
    _write_side(
        ref_root,
        records_rows=[{"export_run_id": "r1.details.json", "domain": "arrowheads", "record_id": "a:1", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "arrowheads", "pattern_id": "pat_r", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"}],
        name_key_rows=[{"export_file": "r1.details.json", "domain": "arrowheads", "record_id": "a:1", "label_display": "T1Name", "join_hash": "nameT1", "status": "ok"}],
    )
    _write_side(
        tgt_root,
        records_rows=[
            {"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "a:10", "join_hash": "cfgA"},
            {"export_run_id": "t2.details.json", "domain": "arrowheads", "record_id": "a:20", "join_hash": "cfgA"},
        ],
        domain_patterns_rows=[{"domain": "arrowheads", "pattern_id": "pat_t", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"}],
        name_key_rows=[
            {"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "a:10", "label_display": "T1Name", "join_hash": "nameT1", "status": "ok"},
            {"export_file": "t2.details.json", "domain": "arrowheads", "record_id": "a:20", "label_display": "T2Name", "join_hash": "nameT2", "status": "ok"},
        ],
    )
    all_detail_rows = [
        _detail_row("arrowheads", "cfgA", "shared", target_export_run_id="t1.details.json"),
        _detail_row("arrowheads", "cfgA", "shared", target_export_run_id="t2.details.json"),
    ]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads"], ref_root, tgt_root, same_segment=False,
        reference_export_run_id="r1.details.json",
    )
    by_target = {r["target_export_run_id"]: r for r in rows}

    t1_row = by_target["t1.details.json"]
    assert t1_row["target_name_hashes"] == "nameT1"
    assert t1_row["name_set_classification"] == cr.NAME_SETS_IDENTICAL

    t2_row = by_target["t2.details.json"]
    assert t2_row["target_name_hashes"] == "nameT2"
    assert t2_row["name_set_classification"] == cr.NAME_SETS_DISJOINT


def test_disjoint_name_sets_for_same_config_identity(tmp_path):
    ref_root = tmp_path / "ref"
    tgt_root = tmp_path / "tgt"
    _write_side(
        ref_root,
        records_rows=[{"export_run_id": "r1.details.json", "domain": "text_types", "record_id": "tt:1", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "text_types", "pattern_id": "pat_r", "source_cluster_id": "text_types|cfg.schema.v1|cfgA"}],
        name_key_rows=[{"export_file": "r1.details.json", "domain": "text_types", "record_id": "tt:1", "label_display": "Standard", "join_hash": "nameOld", "status": "ok"}],
    )
    _write_side(
        tgt_root,
        records_rows=[{"export_run_id": "t1.details.json", "domain": "text_types", "record_id": "tt:9", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "text_types", "pattern_id": "pat_t", "source_cluster_id": "text_types|cfg.schema.v1|cfgA"}],
        name_key_rows=[{"export_file": "t1.details.json", "domain": "text_types", "record_id": "tt:9", "label_display": "Standard - Renamed", "join_hash": "nameNew", "status": "ok"}],
    )
    all_detail_rows = [_detail_row("text_types", "cfgA", "shared")]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["text_types"], ref_root, tgt_root, same_segment=False,
        reference_export_run_id="r1.details.json",
    )
    assert rows[0]["name_set_classification"] == cr.NAME_SETS_DISJOINT
    assert rows[0]["shared_name_hash_count"] == "0"


def test_fail_soft_reference_name_key_not_materialized(tmp_path):
    """B2: a segment missing name-key materialization degrades that side's patterns to
    name_evidence_missing -- it must never raise or block the whole comparison."""
    ref_root = tmp_path / "ref"
    tgt_root = tmp_path / "tgt"
    _write_side(
        ref_root,
        records_rows=[{"export_run_id": "r1.details.json", "domain": "arrowheads", "record_id": "a:1", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "arrowheads", "pattern_id": "pat_r", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"}],
        name_key_rows=None,  # -NameKey never run for this segment
    )
    _write_side(
        tgt_root,
        records_rows=[{"export_run_id": "t1.details.json", "domain": "arrowheads", "record_id": "a:9", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "arrowheads", "pattern_id": "pat_t", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"}],
        name_key_rows=[{"export_file": "t1.details.json", "domain": "arrowheads", "record_id": "a:9", "label_display": "Arrow", "join_hash": "nameX", "status": "ok"}],
    )
    all_detail_rows = [_detail_row("arrowheads", "cfgA", "shared")]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads"], ref_root, tgt_root, same_segment=False,
        reference_export_run_id="r1.details.json",
    )
    assert rows[0]["name_set_classification"] == cr.NAME_EVIDENCE_MISSING
    assert rows[0]["reference_name_key_status"] == cr.NAME_KEY_STATUS_NOT_MATERIALIZED
    assert rows[0]["target_name_key_status"] == cr.NAME_KEY_STATUS_OK


def test_split_export_normalization_reused_not_reimplemented(tmp_path):
    """B4/Step 0 finding B.4: records.csv's export_run_id is the .index.json name for a
    split-export pair, while name_key_results.csv's export_file is the .details.json name
    for the same model -- normalize_export_run_id() must bridge them."""
    seg_root = tmp_path / "seg"
    _write_side(
        seg_root,
        records_rows=[{"export_run_id": "model.index.json", "domain": "arrowheads", "record_id": "a:1", "join_hash": "cfgA"}],
        domain_patterns_rows=[{"domain": "arrowheads", "pattern_id": "pat_x", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"}],
        name_key_rows=[{"export_file": "model.details.json", "domain": "arrowheads", "record_id": "a:1", "label_display": "Arrow", "join_hash": "nameX", "status": "ok"}],
    )
    all_detail_rows = [_detail_row("arrowheads", "pat_x", "shared", target_export_run_id="model.index.json")]
    rows = cr.compute_name_overlap_rows(
        all_detail_rows, ["arrowheads"], seg_root, seg_root, same_segment=True,
        reference_export_run_id="model.index.json",
    )
    assert rows[0]["name_set_classification"] == cr.NAME_SETS_IDENTICAL
    assert rows[0]["reference_name_hashes"] == "nameX"


# ---------------------------------------------------------------------------
# End-to-end: --include-name-overlap wired through main() correctly.
# ---------------------------------------------------------------------------


_REGISTRY_FIELDNAMES = [
    "segment_id", "parent_segment_id", "run_type", "population_hash",
    "conformance_reference_mode", "output_folder", "status", "last_run_utc",
    "notes", "segment_purpose", "segment_label",
]


def _registry_row(segment_id: str) -> Dict[str, str]:
    return {
        "segment_id": segment_id, "parent_segment_id": "", "run_type": "bundle",
        "population_hash": "", "conformance_reference_mode": "", "output_folder": segment_id,
        "status": "complete", "last_run_utc": "", "notes": "", "segment_purpose": "", "segment_label": "",
    }


def test_end_to_end_include_name_overlap_flag_writes_sidecar(tmp_path):
    domain = "arrowheads"  # must be name-key ELIGIBLE (object_styles_model, used elsewhere in
    # this repo's compare_reference fixtures, is explicitly excluded -- no_name_like_key)
    segments_root = tmp_path / "segments"
    seg_root = segments_root / "seg_a"
    corpus_records_dir = tmp_path / "corpus" / "records"
    registry_file = corpus_records_dir / "run_registry.csv"
    _write_csv(registry_file, _REGISTRY_FIELDNAMES, [_registry_row("seg_a")])

    _write_csv(
        seg_root / "results" / "records" / "file_metadata.csv",
        ["export_run_id", "central_path", "governance_role", "unit_system"],
        [
            {"export_run_id": eid, "central_path": f"/x/{eid}", "governance_role": "Project", "unit_system": "imperial"}
            for eid in ("ref.json", "target.json")
        ],
    )
    _write_csv(
        seg_root / "results" / "records" / "records.csv",
        ["export_run_id", "domain", "join_key_schema", "join_key_policy_id", "join_key_policy_version", "record_id", "join_hash"],
        [
            {"export_run_id": eid, "domain": domain, "join_key_schema": f"{domain}.join_key.v1",
             "join_key_policy_id": "p1", "join_key_policy_version": "1", "record_id": f"rec_{eid}", "join_hash": "A"}
            for eid in ("ref.json", "target.json")
        ],
    )
    presence_rows = [
        {"analysis_run_id": "run1", "domain": domain, "export_run_id": "ref.json", "pattern_id": "A", "pattern_share_pct": "1.000000"},
        {"analysis_run_id": "run1", "domain": domain, "export_run_id": "target.json", "pattern_id": "A", "pattern_share_pct": "1.000000"},
    ]
    _write_csv(seg_root / "results" / "analysis" / "pattern_presence_file.csv",
               ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"], presence_rows)
    _write_csv(seg_root / "results" / "analysis" / "corpus_manifest.csv", ["schema_version"], [{"schema_version": "2.1.0"}])
    _write_csv(
        seg_root / "results" / "analysis" / "domain_patterns.csv",
        ["schema_version", "analysis_run_id", "domain", "pattern_id", "source_cluster_id"],
        [{"schema_version": "2.1.0", "analysis_run_id": "run1", "domain": domain, "pattern_id": "A", "source_cluster_id": f"{domain}|A"}],
    )
    membership_rows = [
        {"analysis_run_id": "run1", "export_run_id": "ref.json", "pattern_id": "A"},
        {"analysis_run_id": "run1", "export_run_id": "target.json", "pattern_id": "A"},
    ]
    for view in ("all", "used"):
        _write_csv(seg_root / "results" / "bundle_analysis" / view / domain / "membership_matrix.csv",
                   ["analysis_run_id", "export_run_id", "pattern_id"], membership_rows)
    _write_csv(
        seg_root / "results" / "name_key" / "name_key_results.csv",
        ["export_file", "domain", "record_id", "label_display", "join_hash", "status"],
        [
            {"export_file": "ref.json", "domain": domain, "record_id": "rec_ref.json", "label_display": "Standard", "join_hash": "N1", "status": "ok"},
            {"export_file": "target.json", "domain": domain, "record_id": "rec_target.json", "label_display": "Standard", "join_hash": "N1", "status": "ok"},
        ],
    )

    out_dir = tmp_path / "out"
    argv = [
        "--segments-root", str(segments_root),
        "--registry-file", str(registry_file),
        "--reference-segment", "seg_a",
        "--reference", "ref.json",
        "--out-dir", str(out_dir),
        "--purge-view", "all",
        "--include-name-overlap",
    ]
    rc = cr.main(argv)
    assert rc == 0

    overlap_path = out_dir / cr.NAME_OVERLAP_FILENAME
    assert overlap_path.is_file()
    rows = _read_csv(overlap_path)
    assert len(rows) == 1
    assert rows[0]["name_set_classification"] == cr.NAME_SETS_IDENTICAL
    assert rows[0]["domain"] == domain

    manifest = __import__("json").loads((out_dir / cr.MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert manifest["name_overlap_included"] is True
    assert cr.NAME_OVERLAP_FILENAME in manifest["output_files"]
    assert manifest["name_overlap_known_gaps"] == [cr.REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED]


def test_name_overlap_omitted_by_default(tmp_path):
    """Opt-in flag: without --include-name-overlap, no sidecar file and no manifest churn."""
    domain = "object_styles_model"
    segments_root = tmp_path / "segments"
    seg_root = segments_root / "seg_a"
    corpus_records_dir = tmp_path / "corpus" / "records"
    registry_file = corpus_records_dir / "run_registry.csv"
    _write_csv(registry_file, _REGISTRY_FIELDNAMES, [_registry_row("seg_a")])
    _write_csv(
        seg_root / "results" / "records" / "file_metadata.csv",
        ["export_run_id", "central_path", "governance_role", "unit_system"],
        [{"export_run_id": eid, "central_path": f"/x/{eid}", "governance_role": "Project", "unit_system": "imperial"} for eid in ("ref.json", "target.json")],
    )
    _write_csv(
        seg_root / "results" / "records" / "records.csv",
        ["export_run_id", "domain", "join_key_schema", "join_key_policy_id", "join_key_policy_version"],
        [{"export_run_id": eid, "domain": domain, "join_key_schema": f"{domain}.join_key.v1", "join_key_policy_id": "p1", "join_key_policy_version": "1"} for eid in ("ref.json", "target.json")],
    )
    presence_rows = [
        {"analysis_run_id": "run1", "domain": domain, "export_run_id": "ref.json", "pattern_id": "A", "pattern_share_pct": "1.000000"},
        {"analysis_run_id": "run1", "domain": domain, "export_run_id": "target.json", "pattern_id": "A", "pattern_share_pct": "1.000000"},
    ]
    _write_csv(seg_root / "results" / "analysis" / "pattern_presence_file.csv",
               ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"], presence_rows)
    _write_csv(seg_root / "results" / "analysis" / "corpus_manifest.csv", ["schema_version"], [{"schema_version": "2.1.0"}])
    membership_rows = [
        {"analysis_run_id": "run1", "export_run_id": "ref.json", "pattern_id": "A"},
        {"analysis_run_id": "run1", "export_run_id": "target.json", "pattern_id": "A"},
    ]
    for view in ("all", "used"):
        _write_csv(seg_root / "results" / "bundle_analysis" / view / domain / "membership_matrix.csv",
                   ["analysis_run_id", "export_run_id", "pattern_id"], membership_rows)

    out_dir = tmp_path / "out"
    argv = [
        "--segments-root", str(segments_root),
        "--registry-file", str(registry_file),
        "--reference-segment", "seg_a",
        "--reference", "ref.json",
        "--out-dir", str(out_dir),
    ]
    rc = cr.main(argv)
    assert rc == 0
    assert not (out_dir / cr.NAME_OVERLAP_FILENAME).is_file()
    manifest = __import__("json").loads((out_dir / cr.MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert manifest["name_overlap_included"] is False
    assert manifest["name_overlap_known_gaps"] == []
