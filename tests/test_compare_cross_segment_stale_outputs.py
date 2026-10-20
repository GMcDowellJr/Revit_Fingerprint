"""Regression test: outputs a run produces no rows for must not survive from a prior run.

compare_cross_segment.py only writes most outputs when they have rows. A rerun into
the same --out-dir that produces none of a category (here: every pair falls below
--min-patterns) previously left the earlier run's files in place, where
generate_governance_narrative.py and Power BI would read them as current.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from compare_cross_segment import MATRIX_OUTPUT_FILENAMES, main as compare_main  # noqa: E402
from test_compare_cross_segment_streamed_pairs import _build_sibling_fixture  # noqa: E402

CONDITIONAL_OUTPUTS = (
    "cross_segment_summary.csv",
    "cross_segment_file_pairs.csv",
    "cross_segment_governance_states.csv",
    "cross_segment_governance_state_summary.csv",
    "cross_segment_delta.csv",
    "cross_segment_pooled.csv",
    "cross_segment_union_inventory.csv",
    "pattern_reuse_distribution.csv",
    "pattern_reuse_summary_by_domain.csv",
    "pattern_reuse_summary_by_client.csv",
    "project_fragmentation_diagnostic.csv",
    "matrix_output_manifest.csv",
) + MATRIX_OUTPUT_FILENAMES


def _argv(segments_root, records_dir, out_dir, domain, min_patterns):
    return [
        "compare_cross_segment.py",
        "--segments-root", str(segments_root),
        "--records-dir", str(records_dir),
        "--out-dir", str(out_dir),
        "--sibling-segments",
        "--domain", domain,
        "--min-patterns", str(min_patterns),
        "--workers", "1",
        "--no-delta",
    ]


def test_rerun_with_no_rows_removes_previous_outputs(tmp_path, monkeypatch):
    domain = "line_patterns"
    records_dir, segments_root, out_dir = _build_sibling_fixture(tmp_path, domain)

    # First run produces real output.
    monkeypatch.setattr(sys, "argv", _argv(segments_root, records_dir, out_dir, domain, 1))
    assert compare_main() == 0
    assert (out_dir / "cross_segment_summary.csv").is_file()
    assert (out_dir / "cross_segment_file_pairs.csv").is_file()

    # Plant a previous-run copy of every conditional output, including ones the
    # first run didn't write, so each removal branch is exercised.
    for name in CONDITIONAL_OUTPUTS:
        (out_dir / name).write_text("stale\n", encoding="utf-8")

    # Second run: every (segment, domain) falls below --min-patterns -> no rows.
    monkeypatch.setattr(sys, "argv", _argv(segments_root, records_dir, out_dir, domain, 999))
    assert compare_main() == 0

    # Each planted file is either removed (no rows this run) or rewritten with this
    # run's rows; none may survive with the previous run's content.
    survivors = sorted(
        name for name in CONDITIONAL_OUTPUTS
        if (out_dir / name).exists() and (out_dir / name).read_text(encoding="utf-8") == "stale\n"
    )
    assert survivors == []
    # The pair-level outputs genuinely produced no rows, so they must be gone.
    for name in ("cross_segment_summary.csv", "cross_segment_file_pairs.csv", "cross_segment_pooled.csv"):
        assert not (out_dir / name).exists(), name
    # comparison_registry.csv is always rewritten as this run's snapshot.
    assert (out_dir / "comparison_registry.csv").is_file()


def test_outputs_with_rows_are_still_written(tmp_path, monkeypatch):
    domain = "line_patterns"
    records_dir, segments_root, out_dir = _build_sibling_fixture(tmp_path, domain)

    monkeypatch.setattr(sys, "argv", _argv(segments_root, records_dir, out_dir, domain, 1))
    assert compare_main() == 0
    monkeypatch.setattr(sys, "argv", _argv(segments_root, records_dir, out_dir, domain, 1))
    assert compare_main() == 0

    assert (out_dir / "cross_segment_summary.csv").stat().st_size > 0
    assert (out_dir / "cross_segment_file_pairs.csv").is_file()
