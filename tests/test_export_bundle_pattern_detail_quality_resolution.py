# -*- coding: utf-8 -*-
"""Regression test for tools/export_bundle_pattern_detail.py::_iter_identity_csv's
quality-column resolution.

The v2.1 identity-item shard schema (tools/extractor.py's per-domain
identity_items_by_domain/*.csv writer) always emits an `item_role` column, but
leaves it blank and writes the actual item quality into `item_value_type`
instead (see tools/extractor.py's shard-row construction). `_iter_identity_csv`
must fall back to `item_value_type` whenever `item_role` is blank -- a plain
`dict.get("item_role", fallback)` does not do this, since the key is present
(just empty), so `.get()`'s default never fires. This previously left every
`q` column in pattern_settings.csv blank for real v2.1 exports, blocking every
requested join_hash downstream in mapping/line_pattern_reconstruction.py.
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


def test_v21_schema_falls_back_to_item_value_type_when_item_role_blank(tmp_path):
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


def test_v21_schema_prefers_item_role_when_populated(tmp_path):
    path = tmp_path / "line_patterns.csv"
    _write_csv(
        path,
        ["schema_version", "export_run_id", "domain", "record_pk", "item_key", "item_value", "item_value_type", "item_role"],
        [{
            "schema_version": "record.v2", "export_run_id": "run1", "domain": "line_patterns",
            "record_pk": "pk1", "item_key": "line_pattern.segment_count", "item_value": "2",
            "item_value_type": "unreadable", "item_role": "ok",
        }],
    )
    rows = list(_iter_identity_csv(path))
    assert rows[0]["q"] == "ok"


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
