"""Tests for tools/build_segment_manifest.py."""
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

# Allow running without installing; resolve to repo root.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from build_segment_manifest import (
    _build_segments,
    _build_registry,
    _population_hash,
    _normalize_rows,
    _build_membership_rows,
    _membership_by_segment,
    MANIFEST_FIELDNAMES,
    REGISTRY_FIELDNAMES,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _meta_row(export_run_id, unit_system, client_label, governance_role, discipline_label=""):
    return {
        "export_run_id": export_run_id,
        "unit_system": unit_system,
        "client_label": client_label,
        "governance_role": governance_role,
        "discipline_label": discipline_label,
    }


def _read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _membership_ids(out_dir: Path, segment_id: str) -> set:
    """Read segment_membership.csv and return the export_run_id set for one segment_id."""
    rows = _read_csv(out_dir / "segment_membership.csv")
    return {r["export_run_id"] for r in rows if r["segment_id"] == segment_id}


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


def test_level1_run_type_skip_when_below_min_files():
    # Level-1 unit populations with children are registration-only parents.
    rows = [_meta_row("x01", "metric", "Tiny", "Project"), _meta_row("x02", "metric", "Tiny", "Project")]
    segs = _build_segments(rows, min_files=3)
    metric = next(r for r in segs if r["segment_id"] == "metric" and r["segment_level"] == "1")
    assert metric["run_type"] == "registration"


def test_level1_run_type_bundle_at_min_files():
    rows = [_meta_row(f"r{i:02d}", "imperial", "Acme", "Project") for i in range(3)]
    segs = _build_segments(rows, min_files=3)
    imp = next(r for r in segs if r["segment_id"] == "imperial" and r["segment_level"] == "1")
    assert imp["run_type"] == "registration"


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
    assert metric_global["run_type"] == "registration"


def test_level2_run_type_at_min():
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    assert kaiser["run_type"] == "registration"


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
    # File membership now lives in segment_membership.csv, not an inline
    # pipe-delimited manifest column (which blew past spreadsheet cell limits
    # for large populations). Rows are sorted (segment_id, export_run_id).
    segs = _build_segments(ROWS, min_files=3)
    membership = _build_membership_rows(segs)
    kaiser_ids = [r["export_run_id"] for r in membership if r["segment_id"] == "imperial|Kaiser"]
    assert kaiser_ids == sorted(kaiser_ids)
    assert kaiser_ids  # non-empty for this fixture


def test_membership_rows_no_pipe_delimited_values():
    # Regression guard for the original bug: export_run_id/is_seed must never
    # be a pipe-joined list (segment_id legitimately contains "|" as its own
    # hierarchical separator, e.g. "imperial|Kaiser" — that's unrelated).
    segs = _build_segments(ROWS, min_files=3)
    membership = _build_membership_rows(segs)
    for row in membership:
        assert "|" not in row["export_run_id"]
        assert "|" not in row["is_seed"]


def test_manifest_and_registry_have_no_list_columns():
    # Regression guard: segment_manifest.csv / run_registry.csv must only ever
    # carry scalar summary fields — file membership belongs in
    # segment_membership.csv exclusively.
    assert "export_run_ids" not in MANIFEST_FIELDNAMES
    assert "seed_export_run_ids" not in MANIFEST_FIELDNAMES
    assert "export_run_ids" not in REGISTRY_FIELDNAMES
    assert "seed_export_run_ids" not in REGISTRY_FIELDNAMES


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
    kaiser_reg = next(r for r in reg if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser_reg["output_folder"] == "imperial_project_kaiser"


def test_sanitize_folder_strips_path_separators():
    from build_segment_manifest import _sanitize_folder
    assert "/" not in _sanitize_folder("imperial/west|Client")
    assert "\\" not in _sanitize_folder("imperial\\east|Client")
    # Result should be a flat name, not a path
    result = _sanitize_folder("us/west|Acme Corp")
    assert "/" not in result and "\\" not in result
    assert result == result.lower()


def test_registry_output_folders_globally_unique_with_suffix_collision():
    # Reproduce the case where a generated suffix collides with another
    # segment's natural sanitized name. Uses distinct literal client_label
    # strings (not case variants — those now merge upstream in
    # _build_segments() via _normalize_rows(), so they can no longer produce
    # two different segment_ids to collide in the first place):
    #   imperial|west/coast   → imperial_west_coast (natural)
    #   imperial|west_coast   → imperial_west_coast (collision → imperial_west_coast_2)
    #   imperial|west_coast_2 → imperial_west_coast_2 (natural — collides with the suffix!)
    # The registry must still produce three distinct output_folder values.
    rows = (
        [_meta_row(f"a{i:02d}", "imperial", "west/coast", "Project") for i in range(3)]
        + [_meta_row(f"b{i:02d}", "imperial", "west_coast", "Project") for i in range(3)]
        + [_meta_row(f"c{i:02d}", "imperial", "west_coast_2", "Project") for i in range(3)]
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
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
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


def test_seed_only_note_not_suppressed_by_blank_eid_project_row():
    # A malformed row with blank export_run_id and governance_role=Project must NOT
    # suppress seed_only — it is excluded from membership so it should not influence
    # the no_project predicate either.
    rows = [
        _meta_row("s01", "imperial", "SeedOrg", "Template"),
        _meta_row("s02", "imperial", "SeedOrg", "Template"),
        _meta_row("s03", "imperial", "SeedOrg", "Template"),
        _meta_row("",    "imperial", "SeedOrg", "Project"),   # blank eid — excluded member
    ]
    segs = _build_segments(rows, min_files=1)
    l2 = next(r for r in segs if r["segment_level"] == "2" and r["unit_system"] == "imperial")
    assert "seed_only" in (l2.get("notes") or ""), (
        "Blank-eid Project row should not suppress seed_only"
    )
    assert l2["has_seed_file"] == "true"
    # The blank-eid row must not appear in export_run_ids
    assert "" not in l2["export_run_ids"].split("|")


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


def test_registry_folder_merges_for_client_label_case_variants():
    # "Kaiser" and "kaiser" are case variants of the same client, not two
    # clients — _normalize_rows() folds them together (first-seen casing)
    # before segment_id construction, so this must produce ONE registry row
    # / output_folder, not two. (Previously this scenario produced two
    # distinct segment_ids that both sanitized to "imperial_kaiser" and had
    # to be disambiguated with a suffix — that was the bug this fix closes.)
    rows = (
        [_meta_row(f"r{i:02d}", "imperial", "Kaiser", "Project") for i in range(3)]
        + [_meta_row(f"r{i:02d}", "imperial", "kaiser", "Project") for i in range(10, 13)]
    )
    segs = _build_segments(rows, min_files=1)
    reg = _build_registry(segs)
    kaiser_rows = [r for r in reg if r["segment_id"] == "imperial|Project|Kaiser"]
    assert len(kaiser_rows) == 1
    assert not any(r["segment_id"] == "imperial|Project|kaiser" for r in reg)
    assert kaiser_rows[0]["output_folder"] == "imperial_project_kaiser"


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


def test_main_warns_on_blank_export_run_id(tmp_path, capsys):
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerow({"export_run_id": "r01", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})
        w.writerow({"export_run_id": "r02", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})
        w.writerow({"export_run_id": "r03", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})
        # Malformed row — blank export_run_id, valid unit_system
        w.writerow({"export_run_id": "", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})

    import io, contextlib
    stderr_buf = io.StringIO()
    with contextlib.redirect_stderr(stderr_buf):
        rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
    assert rc == 0
    assert "blank export_run_id" in stderr_buf.getvalue()


def test_main_fails_on_missing_columns_even_with_no_data_rows(tmp_path):
    # Header-only file missing governance_role must still fail, not silently succeed.
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["export_run_id", "unit_system", "client_label"])
        w.writeheader()
        # No data rows — previously validation was skipped in this branch.

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
    assert rc == 1
    assert not (out_dir / "segment_manifest.csv").exists()

def test_level2_project_bundle_with_parent_bundle_runs_enabled():
    rows = (
        [_meta_row(f"k{i:02d}", "imperial", "Kaiser", "Project") for i in range(1, 4)]
        + [_meta_row(f"r{i:02d}", "imperial", "Renown", "Project") for i in range(1, 4)]
    )
    segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
    parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
    assert parent["run_type"] == "bundle"


def test_level2_project_registration_without_flag():
    rows = (
        [_meta_row(f"k{i:02d}", "imperial", "Kaiser", "Project") for i in range(1, 4)]
        + [_meta_row(f"r{i:02d}", "imperial", "Renown", "Project") for i in range(1, 4)]
    )
    segs = _build_segments(rows, min_files=3)
    parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
    assert parent["run_type"] == "registration"


def test_mixed_role_client_segment_stays_reference():
    rows = [
        _meta_row("s01", "imperial", "Sutter", "Project"),
        _meta_row("s02", "imperial", "Sutter", "Project"),
        _meta_row("s03", "imperial", "Sutter", "Project"),
        _meta_row("s04", "imperial", "Sutter", "Template"),
        _meta_row("s05", "imperial", "Sutter", "Template"),
        _meta_row("s06", "imperial", "Sutter", "Template"),
    ]
    segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
    mixed = next(r for r in segs if r["segment_id"] == "imperial|Sutter")
    assert mixed["governance_role"] == ""
    assert mixed["run_type"] == "registration"


def test_single_child_suppression_still_fires():
    rows = [_meta_row(f"k{i:02d}", "imperial", "Kaiser", "Project") for i in range(1, 4)]
    segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
    parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
    assert parent["run_type"] == "registration"
    assert "redundant_single_child" in (parent.get("notes") or "")


# ---------------------------------------------------------------------------
# Discipline-cut dimension tests
# ---------------------------------------------------------------------------

def _disc_rows():
    """Multi-client, multi-discipline Container corpus for discipline tests."""
    return (
        [_meta_row(f"ka{i:02d}", "imperial", "Kaiser", "Container", "Architectural") for i in range(4)]
        + [_meta_row(f"ke{i:02d}", "imperial", "Kaiser", "Container", "Electrical") for i in range(3)]
        + [_meta_row(f"ra{i:02d}", "imperial", "Renown", "Container", "Architectural") for i in range(3)]
        # rows with no discipline_label — must not generate discipline cuts
        + [_meta_row(f"nx{i:02d}", "imperial", "Kaiser", "Project") for i in range(3)]
    )


def test_discipline_cut_level3_segment_generated():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg_ids = {r["segment_id"] for r in segs}
    assert "imperial|Container|Architectural" in seg_ids
    assert "imperial|Container|Electrical" in seg_ids


def test_discipline_cut_level4_segment_generated():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg_ids = {r["segment_id"] for r in segs}
    assert "imperial|Container|Kaiser|Architectural" in seg_ids
    assert "imperial|Container|Kaiser|Electrical" in seg_ids


def test_discipline_cut_extra_dimensions_populated():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
    assert seg["extra_dimensions"] == "discipline_label=Architectural"
    assert seg["client_label"] == ""
    assert seg["discipline_label"] == "Architectural"


def test_discipline_label_top_level_field_blank_for_non_discipline_segments():
    segs = _build_segments(_disc_rows(), min_files=3)
    # A pure governance segment has no discipline cut — field must be blank, not absent.
    container = next(r for r in segs if r["segment_id"] == "imperial|Container")
    assert container["discipline_label"] == ""
    # A client-only cut also has no discipline.
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    assert kaiser["discipline_label"] == ""


def test_discipline_label_top_level_field_populated_in_mixed_cut():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Kaiser|Architectural")
    assert seg["discipline_label"] == "Architectural"
    assert seg["client_label"] == "Kaiser"


def test_discipline_cut_level3_purpose():
    # With two clients contributing, the discipline-only level-3 segment should NOT be
    # redundant_single_child — it has two distinct child populations (Kaiser + Renown).
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
    assert seg["segment_purpose"] == "discipline_coordination"


def test_discipline_cut_level3_label():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
    assert seg["segment_label"] == "Architectural coordination files"


def test_blank_discipline_does_not_generate_discipline_cut():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg_ids = {r["segment_id"] for r in segs}
    # Rows with blank discipline contribute to governance and client cuts only
    assert "imperial|Project" in seg_ids
    # No discipline cut that includes blank discipline
    disc_segs = [r for r in segs if "discipline_label=" in r.get("extra_dimensions", "")]
    for s in disc_segs:
        assert s["extra_dimensions"] != "discipline_label="


def test_no_discipline_column_rows_not_broken():
    # Rows lacking discipline_label entirely must not generate discipline cuts.
    rows = [
        {"export_run_id": f"r{i:02d}", "unit_system": "imperial",
         "client_label": "Acme", "governance_role": "Container"}
        for i in range(3)
    ]
    segs = _build_segments(rows, min_files=3)
    disc_segs = [r for r in segs if "discipline_label=" in r.get("extra_dimensions", "")]
    assert disc_segs == []


def test_discipline_cut_not_required_column(tmp_path):
    # A metadata file without discipline_label must succeed (not exit 1).
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in ROWS:
            w.writerow({k: row.get(k, "") for k in fieldnames})
    rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "3"])
    assert rc == 0


# ---------------------------------------------------------------------------
# Bug 2: level-3+ governance-role segments must not be demoted by "has children"
# ---------------------------------------------------------------------------

def test_discipline_cut_level3_bundle_not_demoted_by_children():
    # imperial|Container|Architectural has two client children (Kaiser + Renown).
    # The "has children → registration" logic must not fire for level-3 governance-role segments.
    segs = _build_segments(_disc_rows(), min_files=3)
    arch = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
    assert arch["run_type"] == "bundle", (
        f"Expected bundle, got {arch['run_type']}; "
        "level-3 scoped segments must not be demoted by child presence"
    )


def test_discipline_cut_level4_bundle_not_affected():
    # Level-4 combined client+discipline segments have no children and must be bundle.
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Kaiser|Architectural")
    assert seg["run_type"] == "bundle"


# ---------------------------------------------------------------------------
# Bug 3: redundant_single_child must not fire when a parent has multiple children
# ---------------------------------------------------------------------------

def test_multi_child_parent_not_demoted_redundant_single_child():
    # imperial|Container|Kaiser has both Architectural and Electrical children.
    # redundant_single_child must NOT fire.
    segs = _build_segments(_disc_rows(), min_files=3)
    kaiser_container = next(r for r in segs if r["segment_id"] == "imperial|Container|Kaiser")
    assert "redundant_single_child" not in (kaiser_container.get("notes") or ""), (
        "Multi-child parent must not be flagged redundant_single_child"
    )
    assert kaiser_container["run_type"] != "registration" or "redundant_single_child" not in (kaiser_container.get("notes") or "")


def test_single_child_same_hash_still_demoted():
    # imperial|Container|Electrical has only one child (Kaiser|Electrical) with the same population.
    # redundant_single_child SHOULD fire here.
    segs = _build_segments(_disc_rows(), min_files=3)
    elec = next(r for r in segs if r["segment_id"] == "imperial|Container|Electrical")
    assert "redundant_single_child" in (elec.get("notes") or ""), (
        "Single child with same population_hash must still trigger redundant_single_child"
    )


def test_matching_child_demotes_parent_even_with_other_nonmatching_children():
    # Real-world shape: a business_center-scoped Container pool where every
    # row also happens to have a blank client_label (so the client=""-twin
    # child is a byte-identical duplicate of the parent), AND a subset of
    # those rows also carry a discipline_label (so a second, non-matching
    # discipline-cut child also exists as a sibling). The old "exactly one
    # child" gate saw two children and skipped the redundancy check
    # entirely, leaving the parent — a true duplicate of its client=""-twin
    # — independently runnable alongside that twin. It must now demote
    # regardless of the extra non-matching sibling.
    rows = (
        [{"export_run_id": f"s{i:02d}", "unit_system": "imperial", "governance_role": "Container",
          "client_label": "", "business_center_label": "Shared", "discipline_label": "architectural"}
         for i in range(2)]
        + [{"export_run_id": f"s{i:02d}", "unit_system": "imperial", "governance_role": "Container",
            "client_label": "", "business_center_label": "Shared"}
           for i in range(2, 5)]
    )
    segs = _build_segments(rows, min_files=3)
    parent = next(r for r in segs if r["segment_id"] == "imperial|Container|Shared")
    twin = next(r for r in segs if r["segment_id"] == "imperial|Container||Shared")
    disc_child = next(r for r in segs if r["segment_id"] == "imperial|Container|architectural|Shared")

    assert parent["file_count"] == "5"
    assert twin["file_count"] == "5"
    assert disc_child["file_count"] == "2"

    assert parent["run_type"] == "registration", (
        "Parent with a byte-identical child (the client=\"\" twin) must demote "
        "even though it also has a second, non-matching discipline-cut child"
    )
    assert "redundant_single_child" in (parent.get("notes") or "")
    # The pointer must name the matching twin, not the non-matching sibling.
    assert "imperial|Container||Shared" in parent["notes"]
    assert disc_child["file_count"] != parent["file_count"]


# ---------------------------------------------------------------------------
# Level-4 client+discipline leaf segment purpose and label
# ---------------------------------------------------------------------------

def test_client_discipline_leaf_purpose_container():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Kaiser|Architectural")
    assert seg["segment_purpose"] == "client_discipline_coordination"


def test_client_discipline_leaf_label_container():
    segs = _build_segments(_disc_rows(), min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Kaiser|Architectural")
    assert seg["segment_label"] == "Kaiser Architectural coordination files"


def test_client_discipline_leaf_purpose_template():
    rows = (
        [_meta_row(f"t{i:02d}", "imperial", "Kaiser", "Template", "Architectural") for i in range(3)]
        + [_meta_row(f"u{i:02d}", "imperial", "Renown", "Template", "Architectural") for i in range(3)]
    )
    segs = _build_segments(rows, min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Template|Kaiser|Architectural")
    assert seg["segment_purpose"] == "client_discipline_standard_anchor"
    assert seg["segment_label"] == "Kaiser Architectural templates — standards as authored"


def test_client_discipline_leaf_purpose_project():
    rows = (
        [_meta_row(f"p{i:02d}", "imperial", "Kaiser", "Project", "Architectural") for i in range(3)]
        + [_meta_row(f"q{i:02d}", "imperial", "Renown", "Project", "Architectural") for i in range(3)]
    )
    segs = _build_segments(rows, min_files=3)
    seg = next(r for r in segs if r["segment_id"] == "imperial|Project|Kaiser|Architectural")
    assert seg["segment_purpose"] == "client_discipline_practice"
    assert seg["segment_label"] == "Kaiser Architectural projects — standards as practiced"


# ---------------------------------------------------------------------------
# Registry stability + population-hash-based status preservation
# ---------------------------------------------------------------------------

def test_registry_first_run_no_existing_file_unaffected():
    # Regression guard: calling _build_registry with no existing_registry (or
    # existing_registry=None explicitly) must be byte-for-byte identical to
    # the pre-change behavior.
    segs = _build_segments(ROWS, min_files=3)
    reg_default = _build_registry(segs)
    reg_explicit_none = _build_registry(segs, existing_registry=None)
    assert reg_default == reg_explicit_none
    for r in reg_default:
        assert r["status"] == "pending"
        assert r["last_run_utc"] == ""
    kaiser_reg = next(r for r in reg_default if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser_reg["output_folder"] == "imperial_project_kaiser"


def test_registry_preserves_output_folder_across_runs_when_unchanged():
    segs = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs)
    reg2 = _build_registry(segs, existing_registry=reg1)
    folders1 = {r["segment_id"]: r["output_folder"] for r in reg1}
    folders2 = {r["segment_id"]: r["output_folder"] for r in reg2}
    assert folders1 == folders2


def test_registry_preserves_status_when_population_hash_unchanged():
    segs = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs)
    for r in reg1:
        if r["segment_id"] == "imperial|Project|Kaiser":
            r["status"] = "complete"
            r["last_run_utc"] = "2026-01-01T00:00:00Z"

    reg2 = _build_registry(segs, existing_registry=reg1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["status"] == "complete"
    assert kaiser2["last_run_utc"] == "2026-01-01T00:00:00Z"
    assert kaiser2["output_folder"] == "imperial_project_kaiser"


def test_registry_resets_status_when_population_hash_changes():
    segs1 = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs1)
    for r in reg1:
        if r["segment_id"] == "imperial|Project|Kaiser":
            r["status"] = "complete"
            r["last_run_utc"] = "2026-01-01T00:00:00Z"

    rows2 = ROWS + [_meta_row("r11", "imperial", "Kaiser", "Project")]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")

    kaiser1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["population_hash"] != kaiser1["population_hash"]
    assert kaiser2["status"] == "pending"
    assert kaiser2["last_run_utc"] == ""
    assert "population_changed" in kaiser2["notes"]
    # Folder name must remain stable even though status reset.
    assert kaiser2["output_folder"] == kaiser1["output_folder"]


def test_registry_new_segment_gets_unique_folder_not_colliding_with_carryover():
    # First run: two distinct clients whose sanitized names collide — not
    # case variants (those merge upstream in _normalize_rows() and can no
    # longer produce two segment_ids to collide in the first place; see
    # test_registry_folder_merges_for_client_label_case_variants).
    # "west/coast" and "west_coast" both sanitize to "imperial_..._west_coast";
    # the second gets suffixed -> imperial_project_west_coast_2.
    rows_a = [_meta_row(f"a{i:02d}", "imperial", "west/coast", "Project") for i in range(3)]
    rows_b = [_meta_row(f"b{i:02d}", "imperial", "west_coast", "Project") for i in range(3)]
    segs1 = _build_segments(rows_a + rows_b, min_files=1)
    reg1 = _build_registry(segs1)

    folder_a_1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|west/coast")["output_folder"]
    folder_b_1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|west_coast")["output_folder"]
    assert folder_a_1 != folder_b_1

    # Second run: a brand-new client "west_coast_2" is added — its natural
    # sanitized name collides with whatever suffix the first run picked for "b".
    rows_c = [_meta_row(f"c{i:02d}", "imperial", "west_coast_2", "Project") for i in range(3)]
    segs2 = _build_segments(rows_a + rows_b + rows_c, min_files=1)
    reg2 = _build_registry(segs2, existing_registry=reg1)

    folder_a_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west/coast")["output_folder"]
    folder_b_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west_coast")["output_folder"]
    folder_new_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west_coast_2")["output_folder"]

    # Pre-existing segments keep their prior folder names untouched.
    assert folder_a_2 == folder_a_1
    assert folder_b_2 == folder_b_1
    # All three are distinct.
    assert len({folder_a_2, folder_b_2, folder_new_2}) == 3


def _manifest_row(segment_id, run_type="bundle", population_hash="h1", parent="", notes="", purpose="", label=""):
    """Hand-craft a manifest-row-shaped dict for testing _build_registry() in
    isolation, without routing through _build_segments()."""
    return {
        "segment_id": segment_id, "parent_segment_id": parent, "run_type": run_type,
        "population_hash": population_hash, "notes": notes,
        "segment_purpose": purpose, "segment_label": label,
    }


def test_registry_resets_status_when_run_type_changes():
    # population_hash alone must not be the only staleness signal — a
    # run_type change (e.g. lowering --min-files turns a "reference" segment
    # into a "bundle" for the same file population) must also reset status,
    # otherwise the orchestrator keeps skipping a segment that now needs a
    # different analysis to be produced.
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser["run_type"] == "bundle"

    reg1 = _build_registry(segs)
    for r in reg1:
        if r["segment_id"] == "imperial|Project|Kaiser":
            r["status"] = "complete"
            r["last_run_utc"] = "2026-01-01T00:00:00Z"

    segs2 = [dict(r) for r in segs]
    for r in segs2:
        if r["segment_id"] == "imperial|Project|Kaiser":
            r["run_type"] = "reference"  # same population_hash, different run_type

    reg2 = _build_registry(segs2, existing_registry=reg1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["population_hash"] == kaiser["population_hash"]
    assert kaiser2["status"] == "pending"
    assert kaiser2["last_run_utc"] == ""
    assert "run_type_changed" in kaiser2["notes"]
    # Folder name must remain stable even though status reset.
    kaiser1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["output_folder"] == kaiser1["output_folder"]


def test_registry_reserves_dropped_segment_folder_from_new_reuse():
    # A dropped segment's directory under segments/ still holds its old
    # records/markers/analysis output (the caller is only warned to review it
    # for manual cleanup, not to delete it) — a new segment must never be
    # silently handed that same folder name.
    old_manifest = [_manifest_row("imperial|Project|OldClient", population_hash="h1")]
    reg1 = _build_registry(old_manifest)
    old_row = next(r for r in reg1 if r["segment_id"] == "imperial|Project|OldClient")
    assert old_row["output_folder"] == "imperial_project_oldclient"

    # OldClient is dropped entirely; a different, unrelated new segment
    # happens to sanitize to the exact same folder base (distinct separator
    # characters both collapse to "_" under _sanitize_folder).
    new_manifest = [_manifest_row("imperial|Project OldClient", population_hash="h2")]
    reg2 = _build_registry(new_manifest, existing_registry=reg1)

    assert not any(r["segment_id"] == "imperial|Project|OldClient" for r in reg2)
    new_row = next(r for r in reg2 if r["segment_id"] == "imperial|Project OldClient")
    assert new_row["output_folder"] != "imperial_project_oldclient"


def test_registry_drops_removed_segment_ids_with_warning(capsys):
    rows_full = ROWS  # includes both imperial|Kaiser and imperial|Renown
    segs1 = _build_segments(rows_full, min_files=3)
    reg1 = _build_registry(segs1)
    assert any(r["segment_id"] == "imperial|Project|Renown" for r in reg1)

    rows_dropped = [r for r in rows_full if r.get("client_label") != "Renown"]
    segs2 = _build_segments(rows_dropped, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1)

    reg2_ids = {r["segment_id"] for r in reg2}
    assert "imperial|Project|Renown" not in reg2_ids
    assert "imperial|Renown" not in reg2_ids

    captured = capsys.readouterr()
    assert "imperial|Project|Renown" in captured.err or "imperial|Renown" in captured.err


def test_client_discipline_leaf_no_empty_purpose():
    # No level-4 client+discipline segment should have an empty segment_purpose.
    segs = _build_segments(_disc_rows(), min_files=3)
    l4 = [r for r in segs if r["segment_level"] == "4" and r["client_label"] and r["discipline_label"]]
    assert l4, "Expected level-4 client+discipline segments in _disc_rows fixture"
    for r in l4:
        assert r["segment_purpose"], (
            f"segment_purpose is empty for level-4 segment {r['segment_id']}"
        )
        assert r["segment_label"] != r["segment_id"], (
            f"segment_label fell back to raw ID for {r['segment_id']}"
        )


# ---------------------------------------------------------------------------
# collection_label segment-id collision guard
# ---------------------------------------------------------------------------

def _collision_rows():
    rows = []
    for i in range(3):
        rows.append({
            "export_run_id": f"bc{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "business_center_label": "Shared", "discipline_label": "",
        })
    for i in range(3):
        rows.append({
            "export_run_id": f"coll{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "collection_label": "Shared", "discipline_label": "",
        })
    return rows


def test_collection_label_value_does_not_collide_with_other_dimension_value():
    # A business-center-scoped segment and a collection-scoped segment that
    # happen to share the same literal value ("Shared") must not produce the
    # same segment_id, or one overwrites the other in any downstream loader
    # that keys segment_manifest.csv by segment_id, and their memberships mix
    # in segment_membership.csv.
    segs = _build_segments(_collision_rows(), min_files=3)
    ids = [r["segment_id"] for r in segs]
    assert len(ids) == len(set(ids)), f"duplicate segment_id values: {ids}"

    bc_scoped = [r for r in segs if r.get("business_center_label") == "Shared" and not r.get("collection_label")]
    coll_scoped = [r for r in segs if r.get("collection_label") == "Shared" and not r.get("business_center_label")]
    assert bc_scoped and coll_scoped
    bc_ids = {r["segment_id"] for r in bc_scoped}
    coll_ids = {r["segment_id"] for r in coll_scoped}
    assert bc_ids.isdisjoint(coll_ids), (
        f"business-center-scoped and collection-scoped segment_ids collide: {bc_ids & coll_ids}"
    )


def test_collection_namespace_prefix_itself_cannot_be_forged_by_other_fields():
    # A business_center_label whose raw value literally IS the reserved
    # "collection:Shared" token must not collide with a real
    # collection_label="Shared" row's namespaced "collection:Shared" token.
    # Without escaping the raw value's own colon, both would render as the
    # identical segment_id substring "collection:Shared".
    rows = []
    for i in range(3):
        rows.append({
            "export_run_id": f"forge{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "business_center_label": "collection:Shared", "discipline_label": "",
        })
    for i in range(3):
        rows.append({
            "export_run_id": f"real{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "collection_label": "Shared", "discipline_label": "",
        })
    segs = _build_segments(rows, min_files=3)
    ids = [r["segment_id"] for r in segs]
    assert len(ids) == len(set(ids)), f"duplicate segment_id values: {ids}"

    forged = next(r for r in segs if r.get("business_center_label") == "collection:Shared" and r["segment_level"] == "4")
    real = next(r for r in segs if r.get("collection_label") == "Shared" and r["segment_level"] == "4")
    assert forged["segment_id"] != real["segment_id"]
    assert forged["segment_id"] == "imperial|Template||collection::Shared"
    assert real["segment_id"] == "imperial|Template||collection:Shared"


def test_colon_in_non_collection_value_not_forging_prefix_is_left_untouched():
    # A client_label/discipline_label/business_center_label containing a
    # colon that does NOT start with the reserved "collection:" prefix
    # can never be confused with a namespaced token, so its segment_id
    # rendering must stay byte-for-byte identical to before namespacing was
    # introduced. Escaping it anyway would silently change segment_id for
    # an already-stable segment on the next rebuild with unchanged source
    # data, breaking _build_registry()'s folder-stability guarantee for it.
    rows = [{
        "export_run_id": f"r{i}", "unit_system": "imperial", "governance_role": "Project",
        "client_label": "A:B", "discipline_label": "",
    } for i in range(3)]
    segs = _build_segments(rows, min_files=3)
    leaf = next(r for r in segs if r["client_label"] == "A:B" and r["segment_level"] == "3")
    assert leaf["segment_id"] == "imperial|Project|A:B"


def test_collection_label_own_leading_colon_does_not_collide_with_forged_escape():
    # collection_label=":Shared" and business_center_label="collection:Shared"
    # must not render to the identical segment_id substring. Without also
    # escaping collection_label's own value, "collection:" + ":Shared" and
    # the escaped forgery "collection:Shared".replace(":","::") both produce
    # "collection::Shared".
    rows = []
    for i in range(3):
        rows.append({
            "export_run_id": f"lead{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "collection_label": ":Shared", "discipline_label": "",
        })
    for i in range(3):
        rows.append({
            "export_run_id": f"forge{i}", "unit_system": "imperial", "governance_role": "Template",
            "client_label": "", "business_center_label": "collection:Shared", "discipline_label": "",
        })
    segs = _build_segments(rows, min_files=3)
    ids = [r["segment_id"] for r in segs]
    assert len(ids) == len(set(ids)), f"duplicate segment_id values: {ids}"

    coll_leaf = next(r for r in segs if r.get("collection_label") == ":Shared" and r["segment_level"] == "4")
    forged_leaf = next(r for r in segs if r.get("business_center_label") == "collection:Shared" and r["segment_level"] == "4")
    assert coll_leaf["segment_id"] != forged_leaf["segment_id"]


def test_collection_label_segment_id_namespaced_in_output():
    segs = _build_segments(_collision_rows(), min_files=3)
    coll_leaf = next(
        r for r in segs
        if r.get("collection_label") == "Shared" and r["governance_role"] == "Template" and not r.get("business_center_label")
    )
    assert "collection:Shared" in coll_leaf["segment_id"]


def test_non_collection_segment_ids_unaffected_by_namespacing():
    # The pre-existing dimensions (client/discipline/business_center) must
    # keep their exact prior segment_id format — only collection_label gets
    # namespaced.
    segs = _build_segments(_collision_rows(), min_files=3)
    bc_leaf = next(
        r for r in segs
        if r.get("business_center_label") == "Shared" and r["governance_role"] == "Template"
        and not r.get("collection_label") and r["segment_level"] == "3"
    )
    assert bc_leaf["segment_id"] == "imperial|Template|Shared"


# ---------------------------------------------------------------------------
# client_label + collection_label leaf purposes
# ---------------------------------------------------------------------------

def _client_collection_rows():
    """Mirrors real Sutter-shaped data: a client's Container/Template rows
    are all tagged with that client's own collection_label, but its Project
    rows carry no collection_label at all — so the level-3 client-alone
    Container segment has population-identical children (the discipline-cut
    child and the client+collection leaf), and pass5 can demote the level-3
    parent, making the client+collection leaf the actual runnable segment."""
    rows = []
    for i in range(4):
        rows.append({
            "export_run_id": f"sc{i}", "unit_system": "imperial", "governance_role": "Container",
            "client_label": "Sutter", "collection_label": "Sutter Standards",
        })
    for i in range(4):
        rows.append({
            "export_run_id": f"sp{i}", "unit_system": "imperial", "governance_role": "Project",
            "client_label": "Sutter",
        })
    return rows


def test_client_collection_leaf_gets_purpose_and_label():
    segs = _build_segments(_client_collection_rows(), min_files=3)
    leaf = next(
        r for r in segs
        if r["client_label"] == "Sutter" and r.get("collection_label") == "Sutter Standards"
        and r["governance_role"] == "Container"
    )
    assert leaf["run_type"] in ("bundle", "reference"), (
        "expected the client+collection leaf to be runnable in this fixture"
    )
    assert leaf["segment_purpose"] == "client_collection_coordination"
    assert leaf["segment_label"] == "Sutter — Sutter Standards coordination files"
    assert leaf["segment_label"] != leaf["segment_id"]


def test_client_collection_discipline_leaf_gets_purpose_and_label():
    rows = [dict(r, discipline_label="architectural") for r in _client_collection_rows() if r["governance_role"] == "Container"]
    segs = _build_segments(rows, min_files=3)
    leaf = next(
        r for r in segs
        if r["client_label"] == "Sutter" and r.get("collection_label") == "Sutter Standards"
        and r["discipline_label"] == "architectural" and r["governance_role"] == "Container"
    )
    assert leaf["segment_purpose"] == "client_collection_discipline_coordination"
    assert leaf["segment_label"] != leaf["segment_id"]


# ---------------------------------------------------------------------------
# Case normalization for segment dimension fields
# ---------------------------------------------------------------------------

def test_unit_system_case_variants_merge_into_single_segment():
    # "imperial" and "Imperial" are the same unit system typed inconsistently
    # during manual file_metadata.csv editing — they must merge into one
    # level-1 segment, not fragment into two shadow populations.
    rows = (
        [_meta_row(f"r{i:02d}", "imperial", "Acme", "Project") for i in range(3)]
        + [_meta_row(f"r{i:02d}", "Imperial", "Acme", "Project") for i in range(10, 13)]
    )
    segs = _build_segments(rows, min_files=1)
    l1_ids = {r["segment_id"] for r in segs if r["segment_level"] == "1"}
    assert l1_ids == {"imperial"}
    l1 = next(r for r in segs if r["segment_level"] == "1")
    assert l1["file_count"] == "6"


def test_governance_role_case_variants_merge_and_no_false_warning(tmp_path, capsys):
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    rows = (
        [{"export_run_id": f"a{i:02d}", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Container"} for i in range(3)]
        + [{"export_run_id": f"b{i:02d}", "unit_system": "imperial", "client_label": "Acme", "governance_role": "container"} for i in range(3)]
    )
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
    assert rc == 0

    captured = capsys.readouterr()
    assert "Unrecognised governance_role" not in captured.err

    ids = _membership_ids(out_dir, "imperial|Container|Acme")
    assert ids == {f"a{i:02d}" for i in range(3)} | {f"b{i:02d}" for i in range(3)}


def test_unknown_governance_role_still_warns_after_normalization_added(tmp_path, capsys):
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i in range(3):
            w.writerow({
                "export_run_id": f"r{i:02d}", "unit_system": "imperial",
                "client_label": "Acme", "governance_role": "Contractor",
            })

    rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Unrecognised governance_role value in metadata: 'Contractor'" in captured.err


def test_client_label_first_seen_casing_is_canonical():
    # "Stantec" appears first in row order — all case variants fold to it,
    # not to an arbitrary or alphabetically-chosen casing.
    rows = [
        _meta_row("s01", "imperial", "Stantec", "Container"),
        _meta_row("s02", "imperial", "stantec", "Container"),
        _meta_row("s03", "imperial", "STANTEC", "Container"),
    ]
    segs = _build_segments(rows, min_files=1)
    seg_ids = {r["segment_id"] for r in segs}
    assert "imperial|Container|Stantec" in seg_ids
    assert "imperial|Container|stantec" not in seg_ids
    assert "imperial|Container|STANTEC" not in seg_ids
    merged = next(r for r in segs if r["segment_id"] == "imperial|Container|Stantec")
    assert merged["client_label"] == "Stantec"
    assert set(merged["export_run_ids"].split("|")) == {"s01", "s02", "s03"}


def test_normalization_warning_emitted_with_aggregate_count(tmp_path, capsys):
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    meta = tmp_path / "file_metadata.csv"
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i in range(16):
            w.writerow({
                "export_run_id": f"r{i:02d}", "unit_system": "Imperial",
                "client_label": "Acme", "governance_role": "Project",
            })

    rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
    assert rc == 0
    captured = capsys.readouterr()
    warn_lines = [ln for ln in captured.err.splitlines() if "Normalized unit_system" in ln]
    assert len(warn_lines) == 1, f"Expected one aggregated warning line, got: {warn_lines}"
    assert "'Imperial' -> 'imperial'" in warn_lines[0]
    assert "(16 row(s))" in warn_lines[0]


def test_clean_corpus_unaffected_by_normalization():
    # Regression guard: consistently-cased fixtures produce zero normalization
    # changes, and _build_segments() output is unaffected.
    _, changes_rows = _normalize_rows(ROWS)
    assert changes_rows == []
    _, changes_disc = _normalize_rows(_disc_rows())
    assert changes_disc == []

    segs = _build_segments(ROWS, min_files=3)
    seg_ids = {r["segment_id"] for r in segs}
    assert "imperial" in seg_ids
    assert "metric" in seg_ids
    assert "imperial|Kaiser" in seg_ids
    assert "imperial|Renown" in seg_ids
    assert "metric|Global" in seg_ids


# ---------------------------------------------------------------------------
# Staleness reasons + conformance_reference_mode
# ---------------------------------------------------------------------------

def test_conformance_reference_mode_defaults_to_latest_for_new_segment():
    segs = _build_segments(ROWS, min_files=3)
    reg = _build_registry(segs)
    kaiser = next(r for r in reg if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser["conformance_reference_mode"] == "latest"


def test_conformance_reference_mode_carried_over_across_runs():
    segs = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs)
    reg2 = _build_registry(segs, existing_registry=reg1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["conformance_reference_mode"] == "latest"


def test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field():
    # Simulate a registry written before this field existed: DictReader on an
    # older CSV yields no "conformance_reference_mode" key at all.
    segs = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs)
    for r in reg1:
        r.pop("conformance_reference_mode", None)
    reg2 = _build_registry(segs, existing_registry=reg1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")
    assert kaiser2["conformance_reference_mode"] == "latest"


def test_registry_no_longer_carries_export_run_ids():
    # run_registry.csv dropped its inline export_run_ids column (moved to
    # segment_membership.csv) — the in-memory row built by _build_registry()
    # must not carry the key either, since REGISTRY_FIELDNAMES no longer
    # includes it.
    segs = _build_segments(ROWS, min_files=3)
    reg = _build_registry(segs)
    kaiser = next(r for r in reg if r["segment_id"] == "imperial|Project|Kaiser")
    assert "export_run_ids" not in kaiser


def test_registry_new_files_reason_when_file_added():
    segs1 = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs1)
    membership1 = _membership_by_segment(_build_membership_rows(segs1))

    rows2 = ROWS + [_meta_row("r11", "imperial", "Kaiser", "Project")]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")

    assert "population_changed" in kaiser2["notes"]
    assert "new_files:1" in kaiser2["notes"]
    assert "removed_files" not in kaiser2["notes"]


def test_registry_removed_files_reason_when_file_removed():
    # Kaiser needs more than min_files here so removing one file doesn't also
    # cross the min_files threshold and flip run_type to "skip" (which would
    # drop the segment from the registry entirely rather than mark it stale).
    rows1 = ROWS + [_meta_row("r12", "imperial", "Kaiser", "Project")]
    segs1 = _build_segments(rows1, min_files=3)
    reg1 = _build_registry(segs1)
    membership1 = _membership_by_segment(_build_membership_rows(segs1))

    rows2 = [r for r in rows1 if r["export_run_id"] != "r03"]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")

    assert "population_changed" in kaiser2["notes"]
    assert "removed_files:1" in kaiser2["notes"]
    assert "new_files" not in kaiser2["notes"]


def test_registry_both_new_and_removed_files_reasons_when_combined_change():
    rows1 = ROWS + [_meta_row("r12", "imperial", "Kaiser", "Project")]
    segs1 = _build_segments(rows1, min_files=3)
    reg1 = _build_registry(segs1)
    membership1 = _membership_by_segment(_build_membership_rows(segs1))

    # Swap r03 out for a new file r11 in the same segment in one run.
    rows2 = [r for r in rows1 if r["export_run_id"] != "r03"] + [
        _meta_row("r11", "imperial", "Kaiser", "Project")
    ]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
    kaiser2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Kaiser")

    assert "population_changed" in kaiser2["notes"]
    assert "new_files:1" in kaiser2["notes"]
    assert "removed_files:1" in kaiser2["notes"]


def test_registry_new_files_reason_does_not_cause_false_removal_warnings(capsys):
    # Regression guard: diffing export_run_ids inside the population_changed
    # branch must not clobber the outer new_ids (segment_id set) used later
    # for dropped_ids — otherwise a plain file add to one segment would make
    # every other still-present segment look "removed" and trigger a false
    # cleanup warning.
    segs1 = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs1)
    membership1 = _membership_by_segment(_build_membership_rows(segs1))

    rows2 = ROWS + [_meta_row("r11", "imperial", "Kaiser", "Project")]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)

    reg2_ids = {r["segment_id"] for r in reg2}
    reg1_ids = {r["segment_id"] for r in reg1}
    assert reg1_ids <= reg2_ids, "no segment should appear removed when only a file was added"

    captured = capsys.readouterr()
    assert "removed from registry" not in captured.err


def test_registry_no_reason_notes_for_brand_new_segment():
    # A segment that didn't exist in the prior registry is "new", not "stale" —
    # it must not carry population_changed/new_files/removed_files reasons.
    segs1 = _build_segments(ROWS, min_files=3)
    reg1 = _build_registry(segs1)

    rows2 = ROWS + [_meta_row(f"z{i:02d}", "imperial", "Zenith", "Project") for i in range(3)]
    segs2 = _build_segments(rows2, min_files=3)
    reg2 = _build_registry(segs2, existing_registry=reg1)
    zenith = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Zenith")

    assert "population_changed" not in zenith["notes"]
    assert "new_files" not in zenith["notes"]
    assert "removed_files" not in zenith["notes"]


# ---------------------------------------------------------------------------
# segment_membership.csv — normalized join table (replaces inline
# export_run_ids / seed_export_run_ids pipe-delimited columns)
# ---------------------------------------------------------------------------

def test_segment_membership_round_trip_reconstructs_in_memory_sets(tmp_path):
    # Build segments -> write segment_membership.csv -> reconstruct per-segment
    # export_run_ids/seed sets by filtering the membership CSV by segment_id ->
    # assert equality with the in-memory sets used to compute
    # file_count/has_seed_file/population_hash.
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in ROWS:
            w.writerow(row)

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
    assert rc == 0

    segs = _build_segments(ROWS, min_files=3)
    membership_rows = _read_csv(out_dir / "segment_membership.csv")

    for seg in segs:
        sid = seg["segment_id"]
        expected_eids = {x for x in seg.get("export_run_ids", "").split("|") if x}
        expected_seeds = {x for x in seg.get("seed_export_run_ids", "").split("|") if x}

        seg_rows = [r for r in membership_rows if r["segment_id"] == sid]
        reconstructed_eids = {r["export_run_id"] for r in seg_rows}
        reconstructed_seeds = {r["export_run_id"] for r in seg_rows if r["is_seed"] == "true"}

        assert reconstructed_eids == expected_eids, f"segment {sid}: export_run_id mismatch"
        assert reconstructed_seeds == expected_seeds, f"segment {sid}: is_seed mismatch"
        assert str(len(reconstructed_eids)) == seg["file_count"]
        assert ("true" if reconstructed_seeds else "false") == seg["has_seed_file"]


def test_segment_membership_join_keys_present_in_manifest_and_metadata(tmp_path):
    # segment_id joins back to segment_manifest.csv; export_run_id joins back
    # to file_metadata.csv (definition grain, unchanged by this migration).
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in ROWS:
            w.writerow(row)

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
    assert rc == 0

    manifest_ids = {r["segment_id"] for r in _read_csv(out_dir / "segment_manifest.csv")}
    metadata_ids = {r["export_run_id"] for r in ROWS if r["export_run_id"]}
    membership_rows = _read_csv(out_dir / "segment_membership.csv")

    for row in membership_rows:
        assert row["segment_id"] in manifest_ids
        assert row["export_run_id"] in metadata_ids


def test_population_hash_unchanged_by_membership_storage_migration():
    # population_hash must byte-for-byte match prior runs given the same file
    # population — it's load-bearing for skip-logic/staleness comparisons.
    # Confirmed here by hand-tracing: it is still computed from the in-memory
    # eids list, not by re-reading any CSV (segment_membership.csv included).
    segs = _build_segments(ROWS, min_files=3)
    kaiser = next(r for r in segs if r["segment_id"] == "imperial|Kaiser")
    eids = [x for x in kaiser["export_run_ids"].split("|") if x]
    assert kaiser["population_hash"] == hashlib.sha1("|".join(sorted(eids)).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Field-length regression guard — the original bug this migration fixes was a
# manifest/registry cell exceeding spreadsheet limits (Excel ~32,767 chars,
# Google Sheets ~50,000 chars) and desyncing downstream CSV parsers.
# ---------------------------------------------------------------------------

_MAX_SANE_FIELD_LEN = 10_000


def test_manifest_and_registry_fields_stay_under_size_threshold(tmp_path):
    # Large population: enough files that the old inline export_run_ids column
    # would have blown past the threshold (each id here is ~30 chars; 500 files
    # -> ~15,000 chars, comfortably over the 10,000-char guard).
    rows = [
        _meta_row(f"export-run-id-{i:06d}-looooong-suffix", "imperial", "BigClient", "Project")
        for i in range(500)
    ]
    meta = tmp_path / "file_metadata.csv"
    fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
    with meta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)

    out_dir = tmp_path / "out"
    rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
    assert rc == 0

    for csv_name in ("segment_manifest.csv", "run_registry.csv"):
        for row in _read_csv(out_dir / csv_name):
            for field, value in row.items():
                assert len(value) < _MAX_SANE_FIELD_LEN, (
                    f"{csv_name} field '{field}' on segment_id={row.get('segment_id')} "
                    f"is {len(value)} chars — file membership must live in "
                    f"segment_membership.csv, not an inline manifest/registry column"
                )

    # segment_membership.csv rows themselves must also stay well under the
    # threshold (each row is one segment_id/export_run_id/is_seed triple).
    for row in _read_csv(out_dir / "segment_membership.csv"):
        for field, value in row.items():
            assert len(value) < _MAX_SANE_FIELD_LEN
