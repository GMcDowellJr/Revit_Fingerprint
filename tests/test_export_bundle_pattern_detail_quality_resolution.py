# -*- coding: utf-8 -*-
"""Regression test for tools/export_bundle_pattern_detail.py::_iter_identity_csv's
quality-column resolution.

The v2.1 identity-item shard schema (tools/extractor.py's per-domain
identity_items_by_domain/*.csv writer) writes item quality into
`item_value_type`; `item_role` is a separate, unrelated tag -- always blank for
a normal row, but populated with a non-quality marker like "synthetic" for
tools/run_extract_all.py's synthetic line_pattern.segments_norm_hash row (see
that function's use of the v2.1 schema's item_role column as its "synthetic"
marker slot). `_iter_identity_csv` must always resolve `q` from
`item_value_type` on this schema and never from `item_role` -- an earlier
version of this fix incorrectly preferred `item_role` whenever it was
non-empty, which broke exactly the synthetic-row case (q would resolve to the
literal string "synthetic" instead of the row's real "ok"/"missing" quality).
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from export_bundle_pattern_detail import _iter_identity_csv


def _write_csv(path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_v21_schema_reads_quality_from_item_value_type_when_role_blank(tmp_path):
    path = tmp_path / "line_patterns.csv"
    _write_csv(
        path,
        ["schema_version", "export_run_id", "domain", "record_pk", "item_key", "item_value", "item_value_type", "item_role"],
        [{
            "schema_version": "record.v2", "export_run_id": "run1", "domain": "line_patterns",
            "record_pk": "pk1", "item_key": "line_pattern.segment_count", "item_value": "2",
            "item_value_type": "ok", "item_role": "",
        }],
    )
    rows = list(_iter_identity_csv(path))
    assert len(rows) == 1
    assert rows[0]["k"] == "line_pattern.segment_count"
    assert rows[0]["v"] == "2"
    assert rows[0]["q"] == "ok"


def test_v21_schema_ignores_item_role_even_when_populated(tmp_path):
    # Regression for the synthetic-row case: tools/run_extract_all.py writes
    # item_role="synthetic" (a provenance marker) alongside the real quality in
    # item_value_type for line_pattern.segments_norm_hash. q must come from
    # item_value_type regardless of what item_role holds.
    path = tmp_path / "line_patterns.csv"
    _write_csv(
        path,
        ["schema_version", "export_run_id", "domain", "record_pk", "item_key", "item_value", "item_value_type", "item_role"],
        [{
            "schema_version": "record.v2", "export_run_id": "run1", "domain": "line_patterns",
            "record_pk": "pk1", "item_key": "line_pattern.segments_norm_hash", "item_value": "deadbeef",
            "item_value_type": "ok", "item_role": "synthetic",
        }],
    )
    rows = list(_iter_identity_csv(path))
    assert rows[0]["q"] == "ok"
    assert rows[0]["v"] == "deadbeef"


def test_legacy_kvq_schema_unaffected(tmp_path):
    path = tmp_path / "line_patterns.csv"
    _write_csv(
        path,
        ["k", "v", "q"],
        [{"k": "line_pattern.segment_count", "v": "2", "q": "ok"}],
    )
    rows = list(_iter_identity_csv(path))
    assert rows[0]["k"] == "line_pattern.segment_count"
    assert rows[0]["v"] == "2"
    assert rows[0]["q"] == "ok"
