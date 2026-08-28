# tests/test_bundle_analysis_common.py
#
# Regression test for tools/bundle_analysis/common.py::read_csv_rows -- must
# not crash on a field larger than Python csv's default 131072-byte limit.
#
# Reproduces the exact traceback a large pipe-joined pattern_id-list cell in
# file_gap_report.csv (tools/bundle_analysis/step_compare.py's
# *_pattern_ids columns) could trigger on re-read before this fix:
#   _csv.Error: field larger than field limit (131072)
# common.py now raises csv.field_size_limit() at import time, mirroring the
# same Windows-safe pattern already used in tools/run_extract_all.py and
# tools/run_segment_orchestrator.py.

from __future__ import annotations

import csv
from pathlib import Path

from tools.bundle_analysis.common import read_csv_rows


def test_read_csv_rows_handles_field_larger_than_default_csv_limit(tmp_path):
    path = tmp_path / "big_field.csv"
    huge_value = "A" * 200_000  # comfortably past the 131072-byte default limit
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "big"])
        writer.writeheader()
        writer.writerow({"id": "1", "big": huge_value})

    rows = read_csv_rows(path)
    assert len(rows) == 1
    assert rows[0]["big"] == huge_value
