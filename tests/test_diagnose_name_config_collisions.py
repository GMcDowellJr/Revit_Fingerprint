# tests/test_diagnose_name_config_collisions.py
#
# Regression tests for tools/diagnose_name_config_collisions.py's segment-root resolution:
# a positional argument may be a segment root itself, or a container directory holding many
# segment roots (e.g. Fingerprint_Data/segments/<segment_id>/results/...), which must be
# auto-discovered rather than requiring the caller to enumerate every segment_id by hand.
#
# Regression for a real-world usage report: `diagnose_name_config_collisions.py
# .../Fingerprint_Data/segments --detail` (the parent container, not a segment root) reported
# "name-key status=not_materialized -- skipping" because results/records/records.csv lives
# one level down, under segments/<segment_id>/, not directly under segments/.
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

import diagnose_name_config_collisions as dncc  # noqa: E402


def _write_csv(path: Path, fieldnames: Sequence[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_segment(seg_root: Path) -> None:
    _write_csv(
        seg_root / "results" / "records" / "records.csv",
        ["export_run_id", "domain", "record_id", "join_hash"],
        [
            {"export_run_id": "m1.details.json", "domain": "arrowheads", "record_id": "a:1", "join_hash": "cfgA"},
            {"export_run_id": "m1.details.json", "domain": "arrowheads", "record_id": "a:2", "join_hash": "cfgB"},
        ],
    )
    _write_csv(
        seg_root / "results" / "analysis" / "domain_patterns.csv",
        ["domain", "pattern_id", "source_cluster_id"],
        [
            {"domain": "arrowheads", "pattern_id": "p1", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
            {"domain": "arrowheads", "pattern_id": "p2", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgB"},
        ],
    )
    _write_csv(
        seg_root / "results" / "name_key" / "name_key_results.csv",
        ["export_file", "domain", "record_id", "label_display", "join_hash", "status"],
        [
            {"export_file": "m1.details.json", "domain": "arrowheads", "record_id": "a:1", "label_display": "Standard Arrow", "join_hash": "nameX", "status": "ok"},
            {"export_file": "m1.details.json", "domain": "arrowheads", "record_id": "a:2", "label_display": "Standard Arrow", "join_hash": "nameX", "status": "ok"},
        ],
    )


def test_segment_root_passed_directly_is_used_as_is(tmp_path):
    seg_root = tmp_path / "imperial_container"
    _write_segment(seg_root)
    assert dncc._resolve_segment_roots(seg_root) == [seg_root]


def test_container_directory_auto_discovers_nested_segment_roots(tmp_path):
    """The real-world bug report: passing the parent `segments/` directory (which itself has
    no results/records/records.csv) must discover the segment root(s) nested beneath it,
    rather than being treated as a single (not-materialized) segment."""
    segments_dir = tmp_path / "segments"
    seg_root = segments_dir / "imperial_container"
    _write_segment(seg_root)

    resolved = dncc._resolve_segment_roots(segments_dir)
    assert resolved == [seg_root]


def test_container_with_multiple_segments_discovers_all(tmp_path):
    segments_dir = tmp_path / "segments"
    seg_a = segments_dir / "imperial_container"
    seg_b = segments_dir / "metric_container"
    _write_segment(seg_a)
    _write_segment(seg_b)

    resolved = dncc._resolve_segment_roots(segments_dir)
    assert resolved == sorted([seg_a, seg_b])


def test_container_with_no_segments_resolves_empty_not_not_materialized(tmp_path):
    empty_dir = tmp_path / "no_segments_here"
    empty_dir.mkdir()
    assert dncc._resolve_segment_roots(empty_dir) == []


def test_main_end_to_end_against_container_directory(tmp_path, capsys):
    segments_dir = tmp_path / "segments"
    seg_root = segments_dir / "imperial_container"
    _write_segment(seg_root)

    rc = dncc.main([str(segments_dir), "--detail"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "container directory -- found 1 segment(s)" in out
    assert "arrowheads" in out
    assert "map to >1 config" in out
    assert "nameX" in out  # --detail spot-check row


def test_main_returns_nonzero_when_every_input_is_invalid(tmp_path, capsys):
    """PR #482 review: a scan that never ran (every supplied path missing/mistyped or
    resolving to zero segments) must be distinguishable from a legitimate empty scan --
    scripts invoking this tool need a nonzero exit code for the former."""
    empty_dir = tmp_path / "no_segments_here"
    empty_dir.mkdir()
    rc = dncc.main([str(empty_dir)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not a segment root and not a container of any" in err
    assert "nothing was scanned" in err


def test_main_returns_nonzero_for_a_missing_path(tmp_path, capsys):
    missing = tmp_path / "does_not_exist"
    rc = dncc.main([str(missing)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not a directory" in err


def test_main_still_succeeds_when_only_some_inputs_are_invalid(tmp_path, capsys):
    """A mix of one valid segment and one bad path should still scan the valid one and
    return success -- the nonzero exit is reserved for 'nothing was scanned at all'."""
    seg_root = tmp_path / "seg_ok"
    _write_segment(seg_root)
    missing = tmp_path / "does_not_exist"

    rc = dncc.main([str(seg_root), str(missing)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "arrowheads" in out


def test_main_dedupes_overlapping_segment_roots(tmp_path, capsys):
    """PR #482 review: passing a container directory AND one of its own segments together
    must not double-count that segment's names in the totals."""
    segments_dir = tmp_path / "segments"
    seg_root = segments_dir / "imperial_container"
    _write_segment(seg_root)

    rc = dncc.main([str(segments_dir), str(seg_root)])
    assert rc == 0
    out = capsys.readouterr().out
    # Exactly one scanned segment, not two -- the dedup keeps the arrowheads row's
    # distinct_name_count from doubling (would be 2 instead of 1 if double-counted).
    assert "TOTAL [widened] across 1 segment(s): 1 distinct names scanned" in out


def test_main_reports_coverage_class_and_separates_headline_totals(tmp_path, capsys):
    seg_root = tmp_path / "seg"
    _write_segment(seg_root)  # arrowheads is a WIDENED domain
    rc = dncc.main([str(seg_root)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "native" in out or "widened" in out
    assert "TOTAL [widened]" in out
    # No pooled cross-class line -- each coverage class gets its own TOTAL line.
    assert "TOTAL across" not in out
