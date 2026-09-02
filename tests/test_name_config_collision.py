# tests/test_name_config_collision.py
#
# Regression tests for tools/name_config_collision.py -- the Same-Name-Different-Config
# Collision Detection prototype (Step 1 of the investigation described in the PR: given the
# same name, does the config agree?). This is the inverse question to
# tools/compare_reference.py's --include-name-overlap (given the same config, do the names
# agree?).
#
# Use synthetic fixtures only. No Revit dependency. Fixture-construction style (helper
# functions writing CSV rows under a segment_root, matching the exact records.csv/
# domain_patterns.csv/name_key_results.csv column sets) follows
# tests/test_compare_reference_name_overlap.py's convention.

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

import name_config_collision as ncc  # noqa: E402
from name_key_rollup import build_domain_name_hash_facets  # noqa: E402


def _write_csv(path: Path, fieldnames: Sequence[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


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


_DOMAIN_PATTERNS_ROWS = [
    {"domain": "arrowheads", "pattern_id": "pat_A", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgA"},
    {"domain": "arrowheads", "pattern_id": "pat_B", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgB"},
    {"domain": "arrowheads", "pattern_id": "pat_C", "source_cluster_id": "arrowheads|cfg.schema.v1|cfgC"},
]


def _row(domain, export_run_id, record_id, config_hash):
    return {"export_run_id": export_run_id, "domain": domain, "record_id": record_id, "join_hash": config_hash}


def _nk_row(domain, export_file, record_id, label, name_hash, status="ok"):
    return {"export_file": export_file, "domain": domain, "record_id": record_id, "label_display": label, "join_hash": name_hash, "status": status}


# ---------------------------------------------------------------------------
# invert_domain_name_hash_facets() -- pure structural inversion.
# ---------------------------------------------------------------------------


def test_invert_is_a_mechanical_mirror_of_the_forward_index():
    records_rows = [
        _row("arrowheads", "e1.details.json", "a:1", "cfgA"),
        _row("arrowheads", "e1.details.json", "a:2", "cfgB"),
    ]
    name_key_rows = [
        _nk_row("arrowheads", "e1.details.json", "a:1", "Arrow", "nameX"),
        _nk_row("arrowheads", "e1.details.json", "a:2", "Arrow", "nameX"),
    ]
    forward = build_domain_name_hash_facets(records_rows, _DOMAIN_PATTERNS_ROWS, name_key_rows)
    reverse = ncc.invert_domain_name_hash_facets(forward)

    # Forward: cfgA -> {nameX}, cfgB -> {nameX}. Reverse: nameX -> {cfgA, cfgB}.
    assert set(forward.name_hashes_for("arrowheads", "cfgA").keys()) == {"nameX"}
    assert set(forward.name_hashes_for("arrowheads", "cfgB").keys()) == {"nameX"}
    assert set(reverse.config_hashes_for("arrowheads", "nameX").keys()) == {"cfgA", "cfgB"}
    entry = reverse.config_hashes_for("arrowheads", "nameX")["cfgA"]
    assert entry["record_count"] == 1
    assert entry["label_counts"]["Arrow"] == 1


# ---------------------------------------------------------------------------
# find_within_side_name_ambiguities() -- single-side scan (1b's primitive).
# ---------------------------------------------------------------------------


def test_within_side_ambiguity_detected_on_single_segment():
    records_rows = [
        _row("arrowheads", "e1.details.json", "a:1", "cfgA"),
        _row("arrowheads", "e1.details.json", "a:2", "cfgB"),
    ]
    name_key_rows = [
        _nk_row("arrowheads", "e1.details.json", "a:1", "Standard Arrow", "nameX"),
        _nk_row("arrowheads", "e1.details.json", "a:2", "Standard Arrow", "nameX"),
    ]
    facets = ncc.build_domain_config_collision_facets(records_rows, _DOMAIN_PATTERNS_ROWS, name_key_rows)
    rows = ncc.find_within_side_name_ambiguities(facets)
    assert len(rows) == 1
    assert rows[0]["domain"] == "arrowheads"
    assert rows[0]["name_hash"] == "nameX"
    assert rows[0]["distinct_config_count"] == 2
    assert rows[0]["is_ambiguous"] is True
    assert rows[0]["config_hashes"] == ["cfgA", "cfgB"]


def test_within_side_no_ambiguity_when_name_maps_to_one_config():
    records_rows = [_row("arrowheads", "e1.details.json", "a:1", "cfgA")]
    name_key_rows = [_nk_row("arrowheads", "e1.details.json", "a:1", "Standard Arrow", "nameX")]
    facets = ncc.build_domain_config_collision_facets(records_rows, _DOMAIN_PATTERNS_ROWS, name_key_rows)
    rows = ncc.find_within_side_name_ambiguities(facets)
    assert len(rows) == 1
    assert rows[0]["is_ambiguous"] is False


def test_within_side_scan_excludes_ineligible_domains():
    records_rows = [_row("line_styles", "e1.details.json", "a:1", "cfgLS")]
    domain_patterns_rows = [{"domain": "line_styles", "pattern_id": "pat_LS", "source_cluster_id": "line_styles|cfg.schema.v1|cfgLS"}]
    name_key_rows = [_nk_row("line_styles", "e1.details.json", "a:1", "Continuous", "nameLS")]
    facets = ncc.build_domain_config_collision_facets(records_rows, domain_patterns_rows, name_key_rows)
    rows = ncc.find_within_side_name_ambiguities(facets)
    assert rows == []


# ---------------------------------------------------------------------------
# classify_name_config_collisions() -- reference vs target, per-export scoped.
# ---------------------------------------------------------------------------


def _classify_single_domain(ref_root, tgt_root, same_segment, ref_export, tgt_export):
    return ncc.classify_name_config_collisions(
        ["arrowheads"], ref_root, tgt_root, same_segment=same_segment,
        reference_export_run_id=ref_export, target_export_run_id=tgt_export,
    )


def test_clean_match_same_name_same_config_both_sides(tmp_path):
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[_row("arrowheads", "r1.details.json", "a:1", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "r1.details.json", "a:1", "Arrow", "nameX")])
    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Arrow", "nameX")])

    out_rows, config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    assert len(out_rows) == 1
    row = out_rows[0]
    assert row["name_hash"] == "nameX"
    assert row["name_config_classification"] == ncc.CONFIG_SETS_IDENTICAL
    assert row["reference_config_hash_count"] == "1"
    assert row["target_config_hash_count"] == "1"
    assert row["shared_config_hash_count"] == "1"
    assert {(r["side"], r["config_hash"]) for r in config_rows} == {("reference", "cfgA"), ("target", "cfgA")}


def test_the_hazard_same_name_different_config_each_side(tmp_path):
    """The case this module exists to catch: name X resolves to config A on the reference
    side and config B on the target side -- a naive name-only comparison would call these
    the same governance object; they are not."""
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[_row("arrowheads", "r1.details.json", "a:1", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "r1.details.json", "a:1", "Arrow 15deg", "nameX")])
    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgB")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Arrow 15deg", "nameX")])

    out_rows, _config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    assert len(out_rows) == 1
    assert out_rows[0]["name_config_classification"] == ncc.CONFIG_SETS_DISJOINT
    assert out_rows[0]["shared_config_hash_count"] == "0"


def test_within_side_ambiguity_flagged_before_cross_side_comparison(tmp_path):
    """name X maps to configs A and B within the reference side alone; the target side has
    no evidence for this name at all, so no cross-side set comparison ever executes --
    the within-side signal is reported directly instead of collapsing to a generic
    name_evidence_missing."""
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[
            _row("arrowheads", "r1.details.json", "a:1", "cfgA"),
            _row("arrowheads", "r1.details.json", "a:2", "cfgB"),
        ],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[
            _nk_row("arrowheads", "r1.details.json", "a:1", "Arrow 15deg", "nameX"),
            _nk_row("arrowheads", "r1.details.json", "a:2", "Arrow 15deg (2)", "nameX"),
        ])
    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgC")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Unrelated Name", "nameZ")])

    out_rows, config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    by_name = {r["name_hash"]: r for r in out_rows}

    assert by_name["nameX"]["name_config_classification"] == ncc.NAME_AMBIGUOUS_WITHIN_SIDE
    assert by_name["nameX"]["reference_config_hash_count"] == "2"
    assert by_name["nameX"]["target_config_hash_count"] == "0"
    assert {r["config_hash"] for r in config_rows if r["name_hash"] == "nameX" and r["side"] == "reference"} == {"cfgA", "cfgB"}

    # nameZ only exists on target with exactly one config -- ordinary "missing", not ambiguous.
    assert by_name["nameZ"]["name_config_classification"] == ncc.NAME_EVIDENCE_MISSING


def test_partial_overlap_both_sides_multi_valued(tmp_path):
    """name X -> {A, B} on reference, {B, C} on target: both sides are individually
    'ambiguous' (>1 config), but since BOTH sides have evidence, the cross-side set
    relationship (overlap, since B is shared but the sets differ) takes precedence over the
    within-side-ambiguity classification."""
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[
            _row("arrowheads", "r1.details.json", "a:1", "cfgA"),
            _row("arrowheads", "r1.details.json", "a:2", "cfgB"),
        ],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[
            _nk_row("arrowheads", "r1.details.json", "a:1", "Arrow 15deg", "nameX"),
            _nk_row("arrowheads", "r1.details.json", "a:2", "Arrow 15deg (2)", "nameX"),
        ])
    _write_side(tgt_root,
        records_rows=[
            _row("arrowheads", "t1.details.json", "a:8", "cfgB"),
            _row("arrowheads", "t1.details.json", "a:9", "cfgC"),
        ],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[
            _nk_row("arrowheads", "t1.details.json", "a:8", "Arrow 15deg (2)", "nameX"),
            _nk_row("arrowheads", "t1.details.json", "a:9", "Arrow 15deg (3)", "nameX"),
        ])

    out_rows, _config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    assert len(out_rows) == 1
    assert out_rows[0]["name_config_classification"] == ncc.CONFIG_SETS_OVERLAP
    assert out_rows[0]["reference_config_hash_count"] == "2"
    assert out_rows[0]["target_config_hash_count"] == "2"
    assert out_rows[0]["shared_config_hash_count"] == "1"


def test_ineligible_domain_is_excluded(tmp_path):
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    domain_patterns_rows = [{"domain": "line_styles", "pattern_id": "pat_LS", "source_cluster_id": "line_styles|cfg.schema.v1|cfgLS"}]
    _write_side(ref_root,
        records_rows=[_row("line_styles", "r1.details.json", "a:1", "cfgLS")],
        domain_patterns_rows=domain_patterns_rows,
        name_key_rows=[_nk_row("line_styles", "r1.details.json", "a:1", "Continuous", "nameLS")])
    _write_side(tgt_root,
        records_rows=[_row("line_styles", "t1.details.json", "a:9", "cfgLS")],
        domain_patterns_rows=domain_patterns_rows,
        name_key_rows=[_nk_row("line_styles", "t1.details.json", "a:9", "Continuous", "nameLS")])

    out_rows, config_rows = ncc.classify_name_config_collisions(
        ["line_styles"], ref_root, tgt_root, same_segment=False,
        reference_export_run_id="r1.details.json", target_export_run_id="t1.details.json",
    )
    assert len(out_rows) == 1
    assert out_rows[0]["name_config_classification"] == ncc.NAME_EVIDENCE_EXCLUDED
    assert out_rows[0]["exclusion_reason"] == "no_name_like_key"
    assert config_rows == []


def test_unresolvable_config_is_skipped_not_errored(tmp_path):
    """A name_key row whose record has no matching config join_hash in records.csv (e.g. the
    record is join-key-blocked) must be skipped the same way build_domain_name_hash_facets()
    already skips it -- never surfaced as a false single-config entry, never an error."""
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[],  # a:1 never appears in records.csv -- unresolvable config
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "r1.details.json", "a:1", "Orphaned", "nameOrphan")])
    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Arrow", "nameX")])

    out_rows, _config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    names_seen = {r["name_hash"] for r in out_rows if r["name_hash"]}
    assert "nameOrphan" not in names_seen
    # target's nameX still resolves normally (reference has zero configs for it -> missing).
    by_name = {r["name_hash"]: r for r in out_rows if r["name_hash"]}
    assert by_name["nameX"]["name_config_classification"] == ncc.NAME_EVIDENCE_MISSING


def test_export_id_normalization_split_export_pair(tmp_path):
    """records.csv's export_run_id is the .index.json name for a split-export pair, while
    name_key_results.csv's export_file is the sibling .details.json name for the same model
    -- normalize_export_run_id() (reused inside build_domain_name_hash_facets(), never
    reimplemented here) must bridge them."""
    seg_root = tmp_path / "seg"
    _write_side(seg_root,
        records_rows=[_row("arrowheads", "model.index.json", "a:1", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "model.details.json", "a:1", "Arrow", "nameX")])

    out_rows, config_rows = ncc.classify_name_config_collisions(
        ["arrowheads"], seg_root, seg_root, same_segment=True,
        reference_export_run_id="model.index.json", target_export_run_id="model.index.json",
    )
    assert len(out_rows) == 1
    assert out_rows[0]["name_hash"] == "nameX"
    assert out_rows[0]["name_config_classification"] == ncc.CONFIG_SETS_IDENTICAL
    assert {r["side"] for r in config_rows} == {"reference", "target"}


def test_stale_name_key_gate_degrades_that_side_not_the_whole_run(tmp_path):
    """B2-equivalent fail-soft gate: a segment whose name_key_results.csv predates its own
    records.csv is STALE, not OK -- that side's facets must come back empty rather than
    stale/inconsistent data being used, and the run must still complete (never raise)."""
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[_row("arrowheads", "r1.details.json", "a:1", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "r1.details.json", "a:1", "Arrow", "nameX")])
    # Make name_key_results.csv older than records.csv to force STALE.
    records_csv = ref_root / "results" / "records" / "records.csv"
    name_key_csv = ref_root / "results" / "name_key" / "name_key_results.csv"
    import os
    import time
    now = time.time()
    os.utime(name_key_csv, (now - 1000, now - 1000))
    os.utime(records_csv, (now, now))

    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Arrow", "nameX")])

    out_rows, _config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    assert len(out_rows) == 1
    assert out_rows[0]["reference_name_key_status"] == ncc.NAME_KEY_STATUS_STALE
    assert out_rows[0]["target_name_key_status"] == ncc.NAME_KEY_STATUS_OK
    assert out_rows[0]["name_config_classification"] == ncc.NAME_EVIDENCE_MISSING


def test_not_materialized_name_key_gate(tmp_path):
    ref_root, tgt_root = tmp_path / "ref", tmp_path / "tgt"
    _write_side(ref_root,
        records_rows=[_row("arrowheads", "r1.details.json", "a:1", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=None)  # -NameKey never run for this segment
    _write_side(tgt_root,
        records_rows=[_row("arrowheads", "t1.details.json", "a:9", "cfgA")],
        domain_patterns_rows=_DOMAIN_PATTERNS_ROWS,
        name_key_rows=[_nk_row("arrowheads", "t1.details.json", "a:9", "Arrow", "nameX")])

    out_rows, _config_rows = _classify_single_domain(ref_root, tgt_root, False, "r1.details.json", "t1.details.json")
    assert len(out_rows) == 1
    assert out_rows[0]["reference_name_key_status"] == ncc.NAME_KEY_STATUS_NOT_MATERIALIZED
    assert out_rows[0]["name_config_classification"] == ncc.NAME_EVIDENCE_MISSING
