# -*- coding: utf-8 -*-
"""Pure-Python tests for mapping/line_pattern_reconstruction.py.

No Revit dependency -- covers evidence validation/blocking, segment
reconstruction, hash reconstruction (including a cross-check against
tools/run_extract_all.py's live segments_norm_hash algorithm), and
deterministic naming.
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from core.record_v2 import STATUS_BLOCKED, STATUS_DEGRADED, STATUS_OK

from mapping.line_pattern_reconstruction import (
    MappingOutcome,
    ReconstructedPattern,
    build_mapping_name_candidates,
    build_report_rows,
    compute_join_hash_for_segments,
    compute_run_status,
    compute_segments_def_hash,
    compute_segments_norm_hash,
    dominant_status,
    group_requested_join_hashes,
    resolve_observed_name,
    sanitize_revit_name,
    select_observed_name,
    reconstruct_pattern,
    short_join_hash,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seg_rows(segments, *, join_hash="", segment_count=None, extra=None):
    """Build pattern_settings.csv-shaped rows for a segment list
    [(idx, kind, length), ...]."""
    rows = []
    sc = len(segments) if segment_count is None else segment_count
    rows.append({"domain": "line_patterns", "join_hash": join_hash, "k": "line_pattern.segment_count", "v": str(sc), "q": "ok"})
    for idx, kind, length in segments:
        rows.append({
            "domain": "line_patterns", "join_hash": join_hash,
            "k": "line_pattern.seg[{:03d}].kind".format(idx), "v": str(kind), "q": "ok",
        })
        rows.append({
            "domain": "line_patterns", "join_hash": join_hash,
            "k": "line_pattern.seg[{:03d}].length".format(idx), "v": "{:.9f}".format(length), "q": "ok",
        })
    if extra:
        rows.extend(extra)
    return rows


def _requested_join_hash_for(segments):
    jh, _jk, _missing = compute_join_hash_for_segments(segments)
    return jh


DASH_DOT_SEGMENTS = [(0, 0, 1.25), (1, 2, 0.0)]  # Dash, Dot


# ---------------------------------------------------------------------------
# Reconstruction: happy path + Dot-length normalization
# ---------------------------------------------------------------------------

def test_reconstruct_ok_with_full_evidence():
    jh = _requested_join_hash_for(DASH_DOT_SEGMENTS)
    def_hash = compute_segments_def_hash(DASH_DOT_SEGMENTS)
    norm_hash = compute_segments_norm_hash(DASH_DOT_SEGMENTS)
    extra = [
        {"domain": "line_patterns", "join_hash": jh, "k": "line_pattern.segments_def_hash", "v": def_hash, "q": "ok"},
        {"domain": "line_patterns", "join_hash": jh, "k": "line_pattern.segments_norm_hash", "v": norm_hash, "q": "ok"},
    ]
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash=jh, extra=extra)

    rp = reconstruct_pattern(jh, rows)

    assert rp.status == STATUS_OK
    assert rp.reasons == []
    assert rp.segments == DASH_DOT_SEGMENTS
    assert rp.reconstructed_join_hash == jh


def test_reconstruct_degraded_when_forensic_evidence_absent():
    jh = _requested_join_hash_for(DASH_DOT_SEGMENTS)
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash=jh)  # no def_hash/norm_hash evidence rows

    rp = reconstruct_pattern(jh, rows)

    assert rp.status == STATUS_DEGRADED
    assert "segments_def_hash_evidence_unavailable" in rp.reasons
    assert "segments_norm_hash_evidence_unavailable" in rp.reasons
    assert rp.segments == DASH_DOT_SEGMENTS


def test_dot_length_normalization_forces_zero_and_degrades():
    # A tampered/stale CSV records a non-zero length for a Dot segment.
    segments_raw = [(0, 0, 1.25), (1, 2, 0.0)]
    jh = _requested_join_hash_for(segments_raw)  # requested identity uses the *normalized* (0.0) length
    rows = _seg_rows(segments_raw, join_hash=jh)
    # Overwrite the Dot segment's length row with a non-zero value.
    for row in rows:
        if row["k"] == "line_pattern.seg[001].length":
            row["v"] = "5.000000000"

    rp = reconstruct_pattern(jh, rows)

    assert rp.status == STATUS_DEGRADED
    assert "dot_length_not_normalized:001" in rp.reasons
    assert rp.segments[1] == (1, 2, 0.0)  # normalized back to 0.0
    assert rp.reconstructed_join_hash == jh  # still reproduces the requested identity


# ---------------------------------------------------------------------------
# Blocking rules
# ---------------------------------------------------------------------------

def test_block_settings_absent():
    rp = reconstruct_pattern("deadbeef", [])
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["settings_absent"]
    assert rp.segments is None


def test_block_no_items_marker():
    rows = [{"domain": "line_patterns", "join_hash": "deadbeef", "k": "__no_items__", "v": "", "q": "missing"}]
    rp = reconstruct_pattern("deadbeef", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["no_items_marker"]


def test_block_duplicate_segment_key():
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash="jh")
    # Duplicate one of the raw settings keys (malformed export).
    rows.append({"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.seg[000].kind", "v": "0", "q": "ok"})

    rp = reconstruct_pattern("jh", rows)

    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["duplicate_settings_key:line_pattern.seg[000].kind"]


def test_block_segment_count_mismatch():
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash="jh", segment_count=3)  # declares 3, only 2 present
    rp = reconstruct_pattern("jh", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segment_count_mismatch:declared=3:found=2"]


def test_block_segment_indices_non_contiguous():
    # idx 0 and idx 2, skipping idx 1.
    rows = [
        {"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.segment_count", "v": "2", "q": "ok"},
        {"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.seg[000].kind", "v": "0", "q": "ok"},
        {"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.seg[000].length", "v": "1.000000000", "q": "ok"},
        {"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.seg[002].kind", "v": "0", "q": "ok"},
        {"domain": "line_patterns", "join_hash": "jh", "k": "line_pattern.seg[002].length", "v": "1.000000000", "q": "ok"},
    ]
    rp = reconstruct_pattern("jh", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segment_indices_non_contiguous"]


def test_block_quality_not_ok():
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash="jh")
    for row in rows:
        if row["k"] == "line_pattern.seg[000].length":
            row["q"] = "unreadable"

    rp = reconstruct_pattern("jh", rows)

    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segment_length_quality:000:unreadable"]


def test_block_segment_kind_unmapped():
    rows = _seg_rows([(0, 9, 1.0)], join_hash="jh")  # kind 9 has no Dash/Space/Dot mapping
    rp = reconstruct_pattern("jh", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segment_kind_unmapped:000:9"]


def test_block_non_positive_length_for_non_dot_segment():
    rows = _seg_rows([(0, 0, 0.0)], join_hash="jh")  # Dash with zero length
    rp = reconstruct_pattern("jh", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segment_length_non_positive:000"]


def test_block_segments_def_hash_mismatch():
    jh = _requested_join_hash_for(DASH_DOT_SEGMENTS)
    extra = [{"domain": "line_patterns", "join_hash": jh, "k": "line_pattern.segments_def_hash", "v": "0" * 32, "q": "ok"}]
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash=jh, extra=extra)

    rp = reconstruct_pattern(jh, rows)

    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["segments_def_hash_mismatch"]


def test_block_reconstructed_join_hash_mismatch():
    # Evidence internally consistent, but the caller asked for the wrong join_hash
    # (e.g. stale/corrupted CSV row association).
    rows = _seg_rows(DASH_DOT_SEGMENTS, join_hash="not-the-real-hash")
    rp = reconstruct_pattern("not-the-real-hash", rows)
    assert rp.status == STATUS_BLOCKED
    assert rp.reasons == ["reconstructed_join_hash_mismatch"]


# ---------------------------------------------------------------------------
# segments_norm_hash cross-check against tools/run_extract_all.py
# ---------------------------------------------------------------------------

def _reference_norm_hash_via_run_extract_all(tmp_path, segments):
    import run_extract_all  # imported lazily so pure unit tests don't pay this cost

    items_csv = tmp_path / "identity_items.csv"
    fieldnames = [
        "schema_version", "export_run_id", "file_id", "domain", "record_id",
        "record_ordinal", "record_pk", "item_index", "k", "q", "v",
    ]
    rows = [{
        "schema_version": "record.v2", "export_run_id": "run1", "file_id": "f1",
        "domain": "line_patterns", "record_id": "1", "record_ordinal": "0", "record_pk": "pk1",
        "item_index": "0", "k": "line_pattern.segment_count", "q": "ok", "v": str(len(segments)),
    }]
    for idx, kind, length in segments:
        rows.append({
            "schema_version": "record.v2", "export_run_id": "run1", "file_id": "f1",
            "domain": "line_patterns", "record_id": "1", "record_ordinal": "0", "record_pk": "pk1",
            "item_index": str(idx * 2 + 1), "k": "line_pattern.seg[{:03d}].kind".format(idx), "q": "ok", "v": str(kind),
        })
        rows.append({
            "schema_version": "record.v2", "export_run_id": "run1", "file_id": "f1",
            "domain": "line_patterns", "record_id": "1", "record_ordinal": "0", "record_pk": "pk1",
            "item_index": str(idx * 2 + 2), "k": "line_pattern.seg[{:03d}].length".format(idx), "q": "ok", "v": "{:.9f}".format(length),
        })

    with items_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    run_extract_all._append_line_pattern_synthetic_norm_hash(items_csv)

    with items_csv.open("r", encoding="utf-8-sig", newline="") as f:
        out_rows = list(csv.DictReader(f))
    norm_rows = [r for r in out_rows if r["k"] == "line_pattern.segments_norm_hash" and r["record_pk"] == "pk1"]
    assert len(norm_rows) == 1
    assert norm_rows[0]["q"] == "ok"
    return norm_rows[0]["v"]


def test_segments_norm_hash_matches_run_extract_all_reference(tmp_path):
    cases = [
        [(0, 0, 1.0)],
        [(0, 0, 1.25), (1, 2, 0.0)],
        [(0, 1, 0.5), (1, 0, 1.5), (2, 2, 0.0)],
        [(0, 2, 0.0), (1, 2, 0.0)],  # all-Dot
        [(0, 0, 0.125), (1, 1, 0.0625), (2, 0, 0.0625), (3, 1, 0.0625), (4, 2, 0.0)],
    ]
    for i, segments in enumerate(cases):
        case_dir = tmp_path / "case{}".format(i)
        case_dir.mkdir()
        ours = compute_segments_norm_hash(segments)
        reference = _reference_norm_hash_via_run_extract_all(case_dir, segments)
        assert ours == reference, "mismatch for segments={}".format(segments)


# ---------------------------------------------------------------------------
# Naming: observed_name selection, tie-breaking, synthetic fallback, collisions
# ---------------------------------------------------------------------------

def test_select_observed_name_highest_files_count_wins():
    rows = [
        {"label_v": "Hidden 1/8", "label_q": "ok", "files_count": "3"},
        {"label_v": "Hidden Line", "label_q": "ok", "files_count": "10"},
        {"label_v": "Fence Line", "label_q": "ok", "files_count": "10"},  # ties with above on files_count
    ]
    name, reasons = select_observed_name(rows)
    # Tie-break is lexical ascending: "Fence Line" < "Hidden Line"
    assert name == "Fence Line"
    assert reasons == []


def test_select_observed_name_ignores_non_ok_and_empty():
    rows = [
        {"label_v": "", "label_q": "ok", "files_count": "99"},
        {"label_v": "Junk", "label_q": "missing", "files_count": "50"},
        {"label_v": "Real Name", "label_q": "ok", "files_count": "1"},
    ]
    name, reasons = select_observed_name(rows)
    assert name == "Real Name"
    assert reasons == []


def test_select_observed_name_none_when_no_acceptable_rows():
    rows = [{"label_v": "", "label_q": "missing", "files_count": ""}]
    name, reasons = select_observed_name(rows)
    assert name is None
    assert reasons == ["no_acceptable_observed_name"]


def test_resolve_observed_name_synthetic_fallback_is_deterministic():
    jh = "abcdef0123456789abcdef0123456789"
    name1, is_synth1, reasons1 = resolve_observed_name([], jh)
    name2, is_synth2, reasons2 = resolve_observed_name([], jh)
    assert name1 == name2 == "unnamed_{}".format(short_join_hash(jh))
    assert is_synth1 is True and is_synth2 is True
    assert "synthetic_name_used" in reasons1


def test_build_mapping_name_candidates_deterministic_collision_name():
    jh = "abcdef0123456789abcdef0123456789"
    primary, collision = build_mapping_name_candidates("Hidden Line", jh)
    assert primary == "MAP__Hidden Line"
    assert collision == "MAP__Hidden Line__{}".format(short_join_hash(jh))
    # Deterministic: same inputs -> same outputs
    primary2, collision2 = build_mapping_name_candidates("Hidden Line", jh)
    assert (primary, collision) == (primary2, collision2)


def test_sanitize_revit_name_replaces_illegal_characters():
    assert sanitize_revit_name('Hidden:Line|1/8"') == "Hidden_Line_1/8_"
    assert sanitize_revit_name(None) == "unnamed"
    assert sanitize_revit_name("   ") == "unnamed"


# ---------------------------------------------------------------------------
# Bundle-membership deduplication
# ---------------------------------------------------------------------------

def test_group_requested_join_hashes_dedupes_and_preserves_bundle_associations():
    inventory_rows = [
        {"domain": "line_patterns", "segment_id": "seg1", "join_hash": "jh1", "bundle_id": "b1", "pattern_id": "p1"},
        {"domain": "line_patterns", "segment_id": "seg1", "join_hash": "jh1", "bundle_id": "b2", "pattern_id": "p2"},
        {"domain": "line_patterns", "segment_id": "seg1", "join_hash": "jh2", "bundle_id": "b1", "pattern_id": "p3"},
        {"domain": "fill_patterns", "segment_id": "seg1", "join_hash": "jhX", "bundle_id": "bX", "pattern_id": "pX"},
        {"domain": "line_patterns", "segment_id": "seg1", "join_hash": "", "bundle_id": "b3", "pattern_id": "p4"},
    ]
    requested, skipped = group_requested_join_hashes(inventory_rows)

    assert set(requested.keys()) == {"jh1", "jh2"}
    assert requested["jh1"]["bundle_ids"] == {"b1", "b2"}
    assert requested["jh1"]["pattern_ids"] == {"p1", "p2"}
    assert requested["jh2"]["bundle_ids"] == {"b1"}

    assert len(skipped) == 1
    assert skipped[0].pattern_id == "p4"
    assert skipped[0].bundle_ids == ["b3"]
    assert skipped[0].reason == "join_hash_missing"


# ---------------------------------------------------------------------------
# Report building / run-status dominance
# ---------------------------------------------------------------------------

def test_dominant_status_ordering():
    assert dominant_status([STATUS_OK, STATUS_OK]) == STATUS_OK
    assert dominant_status([STATUS_OK, STATUS_DEGRADED]) == STATUS_DEGRADED
    assert dominant_status([STATUS_DEGRADED, STATUS_BLOCKED, STATUS_OK]) == STATUS_BLOCKED
    assert dominant_status([]) == STATUS_OK


def test_build_report_rows_deterministic_ordering_and_dedup_of_reasons():
    outcomes = [
        MappingOutcome(join_hash="jh2", segment_id="seg1", status=STATUS_OK, action="created", reasons=["a"]),
        MappingOutcome(join_hash="jh1", segment_id="seg1", status=STATUS_BLOCKED, action="blocked", reasons=["b", "b", "a"]),
    ]
    rows = build_report_rows(outcomes)
    assert [r["join_hash"] for r in rows] == ["jh1", "jh2"]
    assert rows[0]["status_reason"] == "a;b"


def test_compute_run_status_dominance_over_outcomes():
    ok_only = [MappingOutcome(join_hash="jh1", status=STATUS_OK)]
    assert compute_run_status(ok_only) == STATUS_OK

    with_blocked = [
        MappingOutcome(join_hash="jh1", status=STATUS_OK),
        MappingOutcome(join_hash="jh2", status=STATUS_BLOCKED),
    ]
    assert compute_run_status(with_blocked) == STATUS_BLOCKED

    assert compute_run_status([]) == STATUS_OK
