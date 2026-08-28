# tests/test_generate_pattern_name_fragmentation.py
#
# Regression tests for tools/generate_pattern_name_fragmentation.py (Step 1 Part A) and
# its shared join/rollup helper, tools/name_key_rollup.py.
#
# Use synthetic fixtures only. No Revit dependency.

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for _candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import generate_pattern_name_fragmentation as gpnf  # noqa: E402
import name_key_rollup as nkr  # noqa: E402


RECORDS_ROWS = [
    {"export_run_id": "model1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:100", "join_hash": "cfgA"},
    {"export_run_id": "model1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:101", "join_hash": "cfgA"},
    # split-export pair: this model's canonical export_run_id is the .index.json name.
    {"export_run_id": "model2.index.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:200", "join_hash": "cfgA"},
]

DOMAIN_PATTERNS_ROWS = [
    {"domain": "arrowheads", "pattern_id": "pat_AAA", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
    {"domain": "arrowheads", "pattern_id": "pat_BBB", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgB"},
    {"domain": "line_styles", "pattern_id": "pat_LS1", "source_cluster_id": "line_styles|cfg.schema.v1|cfgLS1"},
]

NAME_KEY_ROWS = [
    {"export_file": "model1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:100", "label_display": "Arrow 15deg", "join_hash": "nameX", "status": "ok"},
    {"export_file": "model1.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:101", "label_display": "Arrow 15deg (2)", "join_hash": "nameY", "status": "ok"},
    # model2's raw export_file is the .details.json name even though records.csv's
    # export_run_id for the same model is model2.index.json -- exercises
    # normalize_export_run_id() rather than a literal string-equality join.
    {"export_file": "model2.details.json", "domain": "arrowheads", "record_id": "arrowhead_type_id:200", "label_display": "Arrow 15deg", "join_hash": "nameX", "status": "ok"},
]


def test_multiplicity_detected_with_representative_labels_and_counts():
    detail_rows, summary_rows = gpnf.build_fragmentation_rows(RECORDS_ROWS, DOMAIN_PATTERNS_ROWS, NAME_KEY_ROWS)

    aaa_rows = [r for r in detail_rows if r["pattern_id"] == "pat_AAA"]
    assert {r["name_hash"] for r in aaa_rows} == {"nameX", "nameY"}
    assert all(r["distinct_name_count"] == 2 for r in aaa_rows)
    assert all(r["status"] == gpnf.STATUS_OK for r in aaa_rows)

    by_hash = {r["name_hash"]: r for r in aaa_rows}
    assert by_hash["nameX"]["record_count_for_this_name"] == 2  # model1 record 100 + model2 record 200
    assert by_hash["nameX"]["representative_label"] == "Arrow 15deg"
    assert by_hash["nameY"]["record_count_for_this_name"] == 1
    assert by_hash["nameY"]["representative_label"] == "Arrow 15deg (2)"

    summary_by_domain = {r["domain"]: r for r in summary_rows}
    arrow_summary = summary_by_domain["arrowheads"]
    assert arrow_summary["total_patterns"] == 2  # pat_AAA + pat_BBB
    assert arrow_summary["patterns_with_multiplicity"] == 1
    assert arrow_summary["multiplicity_pct"] == "50.00"
    assert arrow_summary["max_distinct_names_observed"] == 2


def test_pattern_with_no_resolved_name_evidence_is_explicit_not_absent():
    detail_rows, summary_rows = gpnf.build_fragmentation_rows(RECORDS_ROWS, DOMAIN_PATTERNS_ROWS, NAME_KEY_ROWS)
    bbb_rows = [r for r in detail_rows if r["pattern_id"] == "pat_BBB"]
    assert len(bbb_rows) == 1
    assert bbb_rows[0]["status"] == gpnf.STATUS_NO_EVIDENCE
    assert bbb_rows[0]["distinct_name_count"] == 0

    arrow_summary = next(r for r in summary_rows if r["domain"] == "arrowheads")
    assert arrow_summary["patterns_with_no_name_evidence"] == 1


def test_excluded_domain_appears_explicitly_with_reason():
    detail_rows, summary_rows = gpnf.build_fragmentation_rows(RECORDS_ROWS, DOMAIN_PATTERNS_ROWS, NAME_KEY_ROWS)
    ls_rows = [r for r in detail_rows if r["domain"] == "line_styles"]
    assert len(ls_rows) == 1
    assert ls_rows[0]["status"] == gpnf.STATUS_EXCLUDED
    assert ls_rows[0]["pattern_id"] == "pat_LS1"

    ls_summary = next(r for r in summary_rows if r["domain"] == "line_styles")
    assert ls_summary["status"] == gpnf.STATUS_EXCLUDED
    assert ls_summary["total_patterns"] == 1
    assert ls_summary["patterns_with_multiplicity"] == 0


def test_no_domain_is_ever_silently_absent():
    detail_rows, summary_rows = gpnf.build_fragmentation_rows(RECORDS_ROWS, DOMAIN_PATTERNS_ROWS, NAME_KEY_ROWS)
    assert {r["domain"] for r in summary_rows} == {"arrowheads", "line_styles"}
    assert {(r["domain"], r["pattern_id"]) for r in detail_rows} == {
        ("arrowheads", "pat_AAA"), ("arrowheads", "pat_BBB"), ("line_styles", "pat_LS1"),
    }


def test_representative_label_ties_break_lexicographically():
    from collections import Counter
    counts = Counter({"B name": 2, "A name": 2})
    assert nkr.representative_label(counts) == "A name"


def test_parse_source_cluster_id_matches_last_segment_convention():
    # Matches tools/compare_reference.py::load_domain_pattern_join_hash_map's
    # scid.split("|")[-1] convention, including the 2-part shorthand used by that file's
    # own test fixtures (domain|pattern_id, join_hash==pattern_id).
    assert nkr.parse_source_cluster_id("arrowheads|schema.v1|abc123") == ("arrowheads", "schema.v1", "abc123")
    assert nkr.parse_source_cluster_id("arrowheads|abc123") == ("arrowheads", "", "abc123")
    assert nkr.parse_source_cluster_id("") is None
