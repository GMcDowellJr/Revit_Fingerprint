"""Tests for tools/build_segment_manifest.py."""
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

# Allow running without installing; resolve to repo root.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from build_segment_manifest import _build_segments, _build_registry, _population_hash, main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _meta_row(export_run_id, unit_system, client_label, governance_role):
    return {
        "export_run_id": export_run_id,
        "unit_system": unit_system,
        "client_label": client_label,
        "governance_role": governance_role,
    }


def _read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

ROWS = [
    _meta_row("r01", "imperial", "Kaiser", "Project"),
    _meta_row("r02", "imperial", "Kaiser", "Project"),
    _meta_row("r03", "imperial", "Kaiser", "Project"),
    _meta_row("r04", "imperial", "Kaiser", "Template"),
    _meta_row("r05", "imperial", "Renown", "Project"),
    _meta_row("r06", "imperial", "Renown", "Project"),
    _meta_row("r07", "imperial", "Renown", "Project"),
    _meta_row("r08", "metric",   "Global",  "Project"),
    _meta_row("r09", "metric",   "Global",  "Container"),
    _meta_row("r10", "",        "Unknown",  "Project"),   # blank unit_system — excluded
]


def test_population_hash_deterministic():
    ids = ["r03", "r01", "r02"]
    h1 = _population_hash(ids)
    h2 = _population_hash(["r01", "r02", "r03"])  # different order
    assert h1 == h2
    expected = hashlib.sha1(b"r01|r02|r03").hexdigest()
    assert h1 == expected


def test_blank_unit_system_excluded():
    segs = _build_segments(ROWS, min_files=3)
    all_ids = "|".join(r["export_run_ids"] for r in segs)
    assert "r10" not in all_ids


def test_level1_segments_present():
    segs = _build_segments(ROWS, min_files=3)
    l1 = [r for r in segs if r["segment_level"] == "1"]
    ids = {r["segment_id"] for r in l1}
    assert ids == {"imperial", "metric"}


def test_level1_file_counts():
    segs = _build_segments(ROWS, min_files=3)
    l1 = {r["segment_id"]: int(r["file_count"]) for r in segs if r["segment_level"] == "1"}
    assert l1["imperial"] == 7   # r01-r07 (r10 excluded)
    assert l1["metric"] == 2     # r08, r09


def test_level2_segments_present():
    segs = _build_segments(ROWS, min_files=3)
    l2 = [r for r in segs if r["segment_level"] == "2"]
    seg_ids = {r["segment_id"] for r in l2}
    assert "imperial|Kaiser" in seg_ids
    assert "imperial|Renown" in seg_ids
    assert "metric|Global" in seg_ids


def test_level2_run_type_below_min():
    segs = _build_segments(ROWS, min_files=3)
    metric_global = next(r for r in segs if r["segment_id"] == "metric|Global")
    assert metric_global["run_type"] == "skip"
    assert "below_min_files" in metric_global["notes"]


def test_level2_run_type_at_min():
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    assert kaiser["run_type"] == "bundle"


def test_seed_detection_level2():
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    assert kaiser["has_seed_file"] == "true"
    assert "r04" in kaiser["seed_export_run_ids"].split("|")


def test_seed_detection_renown_no_seed():
    segs = _build_segments(ROWS, min_files=3)
    renown = next(r for r in segs if r["segment_id"] == "imperial|Renown")
    assert renown["has_seed_file"] == "false"
    assert renown["seed_export_run_ids"] == ""


def test_seed_detection_container_role():
    segs = _build_segments(ROWS, min_files=3)
    global_seg = next(r for r in segs if r["segment_id"] == "metric|Global")
    assert global_seg["has_seed_file"] == "true"
    assert "r09" in global_seg["seed_export_run_ids"].split("|")


def test_level1_parent_is_empty():
    segs = _build_segments(ROWS, min_files=3)
    for r in segs:
        if r["segment_level"] == "1":
            assert r["parent_segment_id"] == ""


def test_level2_parent_is_unit_system():
    segs = _build_segments(ROWS, min_files=3)
    for r in segs:
        if r["segment_level"] == "2":
            assert r["parent_segment_id"] == r["unit_system"]


def test_sort_order_level1_before_level2():
    segs = _build_segments(ROWS, min_files=3)
    levels = [int(r["segment_level"]) for r in segs]
    assert levels == sorted(levels)


def test_sort_order_within_level_alphabetical():
    segs = _build_segments(ROWS, min_files=3)
    l1_ids = [r["segment_id"] for r in segs if r["segment_level"] == "1"]
    assert l1_ids == sorted(l1_ids)
    l2_ids = [r["segment_id"] for r in segs if r["segment_level"] == "2"]
    assert l2_ids == sorted(l2_ids)


def test_export_run_ids_sorted_pipe_delimited():
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    ids = kaiser["export_run_ids"].split("|")
    assert ids == sorted(ids)


def test_population_hash_in_manifest():
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    expected = _population_hash(kaiser["export_run_ids"].split("|"))
    assert kaiser["population_hash"] == expected


def test_registry_excludes_skip_segments():
    segs = _build_segments(ROWS, min_files=3)
    reg = _build_registry(segs)
    reg_ids = {r["segment_id"] for r in reg}
    assert "metric|Global" not in reg_ids


def test_registry_output_folder_sanitized():
    segs = _build_segments(ROWS, min_files=3)
    reg = _build_registry(segs)
    kaiser_reg = next(r for r in reg if r["segment_id"] == "imperial|Kaiser")
    assert kaiser_reg["output_folder"] == "imperial_kaiser"


def test_registry_output_folders_globally_unique_with_suffix_collision():
    # Reproduce the case where a generated suffix collides with another
    # segment's natural sanitized name:
    #   imperial|kaiser   → imperial_kaiser (natural)
    #   imperial|Kaiser   → imperial_kaiser (collision → imperial_kaiser_2)
    #   imperial|kaiser_2 → imperial_kaiser_2 (natural — collides with the suffix!)
    # The registry must still produce three distinct output_folder values.
    rows = (
        [_meta_row(f"a{i:02d}", "imperial", "kaiser", "Project") for i in range(3)]
        + [_meta_row(f"b{i:02d}", "imperial", "Kaiser", "Project") for i in range(3)]
        + [_meta_row(f"c{i:02d}", "imperial", "kaiser_2", "Project") for i in range(3)]
    )
    segs = _build_segments(rows, min_files=1)
    reg = _build_registry(segs)
    folders = [r["output_folder"] for r in reg]
    assert len(folders) == len(set(folders)), f"Duplicate output_folder values: {folders}"


def test_registry_initial_status_pending():
    segs = _build_segments(ROWS, min_files=3)
    reg = _build_registry(segs)
    for r in reg:
        assert r["status"] == "pending"
        assert r["last_run_utc"] == ""


# ---------------------------------------------------------------------------
# Integration test — end-to-end via main()
# ---------------------------------------------------------------------------

def test_main_writes_files(tmp_path):
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in ROWS:
            w.writerow(row)

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
    assert rc == 0

    manifest_path = out_dir / "segment_manifest.csv"
    registry_path = out_dir / "run_registry.csv"
    assert manifest_path.is_file()
    assert registry_path.is_file()

    manifest_rows = _read_csv(manifest_path)
    seg_ids = {r["segment_id"] for r in manifest_rows}
    assert "imperial" in seg_ids
    assert "metric" in seg_ids
    assert "imperial|Kaiser" in seg_ids

    reg_rows = _read_csv(registry_path)
    assert all(r["status"] == "pending" for r in reg_rows)
    assert not any(r["segment_id"] == "metric|Global" for r in reg_rows)


def test_seed_only_note_not_set_for_generic_only_segment():
    # A segment whose files are all Generic (no Project AND no Template/Container)
    # must NOT be flagged seed_only — it has no actual seed files.
    rows = [_meta_row(f"r{i:02d}", "imperial", "GenericClient", "Generic") for i in range(3)]
    segs = _build_segments(rows, min_files=1)
    l2 = next(r for r in segs if r["segment_level"] == "2")
    assert "seed_only" not in (l2.get("notes") or "")
    assert l2["has_seed_file"] == "false"


def test_seed_only_note_set_when_segment_has_seeds_no_project():
    # Template/Container files with no Project files → seed_only is correct.
    rows = [
        _meta_row("s01", "imperial", "SeedOrg", "Template"),
        _meta_row("s02", "imperial", "SeedOrg", "Container"),
        _meta_row("s03", "imperial", "SeedOrg", "Template"),
    ]
    segs = _build_segments(rows, min_files=1)
    l2 = next(r for r in segs if r["segment_level"] == "2")
    assert "seed_only" in (l2.get("notes") or "")
    assert l2["has_seed_file"] == "true"


def test_registry_output_folders_unique_across_case_variants():
    # "imperial|Kaiser" and "imperial|kaiser" both sanitize to "imperial_kaiser";
    # the registry must still assign each a distinct output_folder.
    rows = (
        [_meta_row(f"r{i:02d}", "imperial", "Kaiser", "Project") for i in range(3)]
        + [_meta_row(f"r{i:02d}", "imperial", "kaiser", "Project") for i in range(10, 13)]
    )
    segs = _build_segments(rows, min_files=1)
    reg = _build_registry(segs)
    folders = [r["output_folder"] for r in reg]
    assert len(folders) == len(set(folders)), f"Duplicate output_folder values: {folders}"


def test_blank_client_label_level2_id_distinct_from_level1():
    # When client_label is blank the level-2 segment_id must be "imperial|", not "imperial",
    # so it never collides with the level-1 segment_id for the same unit_system.
    rows = [_meta_row(f"r{i:02d}", "imperial", "", "Project") for i in range(3)]
    segs = _build_segments(rows, min_files=1)
    l1_ids = {r["segment_id"] for r in segs if r["segment_level"] == "1"}
    l2_ids = {r["segment_id"] for r in segs if r["segment_level"] == "2"}
    assert l1_ids.isdisjoint(l2_ids), f"Level-1 and level-2 IDs overlap: {l1_ids & l2_ids}"
    assert "imperial|" in l2_ids


def test_main_missing_metadata_file(tmp_path):
    rc = main(["--metadata-file", str(tmp_path / "missing.csv"), "--out-dir", str(tmp_path / "out")])
    assert rc == 1


def test_main_fails_on_missing_required_columns(tmp_path):
    # CSV is present and non-empty but lacks governance_role — tool must exit 1
    # and write no output files (silently dropping every row would be worse).
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["export_run_id", "unit_system", "client_label"])
        w.writeheader()
        w.writerow({"export_run_id": "r01", "unit_system": "imperial", "client_label": "Acme"})

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
    assert rc == 1
    assert not (out_dir / "segment_manifest.csv").exists()


def test_main_fails_when_export_run_id_column_absent(tmp_path):
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["unit_system", "client_label", "governance_role"])
        w.writeheader()
        w.writerow({"unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
    assert rc == 1
