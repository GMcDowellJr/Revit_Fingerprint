# -*- coding: utf-8 -*-
"""Pure-Python tests for mapping/fill_pattern_reconstruction.py.

No Revit dependency -- covers evidence validation/blocking, grid
reconstruction (including a cross-check against an independent
reimplementation of domains/fill_patterns.py's own grids_def_hash token
algorithm), hash reconstruction/join_hash computation, and deterministic
naming. Mirrors tests/test_line_pattern_mapping_reconstruction.py's structure.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    canonicalize_float,
    canonicalize_int,
    canonicalize_str,
)
from core.hashing import make_hash, safe_str

from mapping.fill_pattern_reconstruction import (
    DOMAIN_DRAFTING,
    DOMAIN_MODEL,
    GRIDS_DEF_HASH_KEY,
    GRID_COUNT_KEY,
    MappingOutcome,
    ReconstructedGrid,
    TARGET_KEY,
    TARGET_NAME_BY_DOMAIN,
    build_mapping_name_candidates,
    build_report_rows,
    compute_grids_def_hash,
    compute_join_hash_for_grids,
    compute_run_status,
    dominant_status,
    group_requested_join_hashes,
    resolve_observed_name,
    reconstruct_pattern,
    sanitize_revit_name,
    select_observed_name,
    short_join_hash,
)


# ---------------------------------------------------------------------------
# Independent reference reimplementation of domains/fill_patterns.py's own
# grids_def_hash computation (deliberately NOT imported from that module --
# same "independent reimplementation, not an import" precedent
# tools/pattern_id_utils.py and mapping/line_pattern_reconstruction.py's
# compute_segments_norm_hash already follow). This hand-copies the exact
# insertion-order/token-format logic domains/fill_patterns.py itself uses
# (grid_count, then per grid: angle, origin.kind, origin.u/v OR origin.x/y,
# offset, shift -- unsorted, safe_str(None) == "None").
# ---------------------------------------------------------------------------

def _reference_grids_def_hash(grid_count, grids):
    """grids: list of dicts with keys idx, angle, origin_kind ('uv'/'xy'), a, b, offset, shift."""
    items = []
    gc_v, gc_q = canonicalize_int(grid_count)
    items.append({"k": GRID_COUNT_KEY, "v": gc_v, "q": gc_q})
    for g in grids:
        idx = "{:03d}".format(g["idx"])
        ang_v, ang_q = canonicalize_float(g["angle"])
        items.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": ang_v, "q": ang_q})
        kind_v, kind_q = canonicalize_str(g["origin_kind"])
        items.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": kind_v, "q": kind_q})
        if g["origin_kind"] == "uv":
            a_v, a_q = canonicalize_float(g["a"])
            b_v, b_q = canonicalize_float(g["b"])
            items.append({"k": "fill_pattern.grid[{}].origin.u".format(idx), "v": a_v, "q": a_q})
            items.append({"k": "fill_pattern.grid[{}].origin.v".format(idx), "v": b_v, "q": b_q})
        elif g["origin_kind"] == "xy":
            a_v, a_q = canonicalize_float(g["a"])
            b_v, b_q = canonicalize_float(g["b"])
            items.append({"k": "fill_pattern.grid[{}].origin.x".format(idx), "v": a_v, "q": a_q})
            items.append({"k": "fill_pattern.grid[{}].origin.y".format(idx), "v": b_v, "q": b_q})
        off_v, off_q = canonicalize_float(g["offset"])
        items.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": off_v, "q": off_q})
        sh_v, sh_q = canonicalize_float(g["shift"])
        items.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": sh_v, "q": sh_q})

    grid_like = [
        "k={}|q={}|v={}".format(safe_str(it["k"]), safe_str(it["q"]), safe_str(it["v"]))
        for it in items
    ]
    return make_hash(grid_like)


def _rg(idx, angle, kind, a, b, offset, shift):
    return ReconstructedGrid(idx=idx, angle=angle, origin_kind=kind, origin_a=a, origin_b=b, offset=offset, shift=shift)


SAMPLE_GRIDS = [
    _rg(0, 0.0, "uv", 0.0, 0.0, 0.125, 0.0625),
    _rg(1, 1.5707963267948966, "uv", 0.5, -0.25, 0.25, 0.0),
]


def _sample_grid_dicts():
    return [
        {"idx": 0, "angle": 0.0, "origin_kind": "uv", "a": 0.0, "b": 0.0, "offset": 0.125, "shift": 0.0625},
        {"idx": 1, "angle": 1.5707963267948966, "origin_kind": "uv", "a": 0.5, "b": -0.25, "offset": 0.25, "shift": 0.0},
    ]


# ---------------------------------------------------------------------------
# compute_grids_def_hash cross-check against the independent reference
# ---------------------------------------------------------------------------

def test_grids_def_hash_matches_reference_implementation():
    expected = _reference_grids_def_hash(2, _sample_grid_dicts())
    actual = compute_grids_def_hash(2, SAMPLE_GRIDS)
    assert actual == expected


def test_grids_def_hash_matches_reference_for_xy_origin():
    grids = [_rg(0, 0.3, "xy", 1.0, 2.0, 0.1, 0.0)]
    dicts = [{"idx": 0, "angle": 0.3, "origin_kind": "xy", "a": 1.0, "b": 2.0, "offset": 0.1, "shift": 0.0}]
    assert compute_grids_def_hash(1, grids) == _reference_grids_def_hash(1, dicts)


def test_grids_def_hash_changes_with_grid_order():
    reversed_grids = [SAMPLE_GRIDS[1], SAMPLE_GRIDS[0]]
    # Re-index to preserve idx==position, mirroring how the domain always
    # indexes grids by their position in GetFillGrids().
    reindexed = [
        _rg(0, reversed_grids[0].angle, reversed_grids[0].origin_kind, reversed_grids[0].origin_a,
            reversed_grids[0].origin_b, reversed_grids[0].offset, reversed_grids[0].shift),
        _rg(1, reversed_grids[1].angle, reversed_grids[1].origin_kind, reversed_grids[1].origin_a,
            reversed_grids[1].origin_b, reversed_grids[1].offset, reversed_grids[1].shift),
    ]
    assert compute_grids_def_hash(2, SAMPLE_GRIDS) != compute_grids_def_hash(2, reindexed)


# ---------------------------------------------------------------------------
# join_hash computation: sanity that it does NOT equal grids_def_hash alone
# (3 required items -> phase2_join_hash path, not the single-def_hash
# passthrough shortcut line_patterns' single-required-item policy hits)
# ---------------------------------------------------------------------------

def test_join_hash_differs_from_bare_grids_def_hash():
    grids_def_hash = compute_grids_def_hash(2, SAMPLE_GRIDS)
    jh, join_key, missing = compute_join_hash_for_grids(DOMAIN_DRAFTING, "Drafting", 2, grids_def_hash)
    assert not missing
    assert jh is not None
    assert jh != grids_def_hash
    assert join_key["keys_used"] == sorted([TARGET_KEY, GRID_COUNT_KEY, GRIDS_DEF_HASH_KEY])


def test_join_hash_differs_between_drafting_and_model_for_same_grids():
    grids_def_hash = compute_grids_def_hash(2, SAMPLE_GRIDS)
    jh_drafting, _, _ = compute_join_hash_for_grids(DOMAIN_DRAFTING, "Drafting", 2, grids_def_hash)
    jh_model, _, _ = compute_join_hash_for_grids(DOMAIN_MODEL, "Model", 2, grids_def_hash)
    assert jh_drafting != jh_model


# ---------------------------------------------------------------------------
# Helpers for building pattern_settings.csv-shaped rows
# ---------------------------------------------------------------------------

def _settings_rows_for(domain_name, join_hash, grid_dicts, *, include_target=True, include_grids_def_hash=True):
    rows = []
    target_name = TARGET_NAME_BY_DOMAIN[domain_name]
    if include_target:
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": TARGET_KEY, "v": target_name, "q": ITEM_Q_OK})
    rows.append({
        "domain": domain_name, "join_hash": join_hash, "k": GRID_COUNT_KEY,
        "v": str(len(grid_dicts)), "q": ITEM_Q_OK,
    })
    for g in grid_dicts:
        idx = "{:03d}".format(g["idx"])
        pfx = "fill_pattern.grid[{}].".format(idx)
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "angle",
                      "v": "{:.9f}".format(g["angle"]), "q": ITEM_Q_OK})
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "origin.kind",
                      "v": g["origin_kind"], "q": ITEM_Q_OK})
        if g["origin_kind"] == "uv":
            rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "origin.u",
                          "v": "{:.9f}".format(g["a"]), "q": ITEM_Q_OK})
            rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "origin.v",
                          "v": "{:.9f}".format(g["b"]), "q": ITEM_Q_OK})
        elif g["origin_kind"] == "xy":
            rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "origin.x",
                          "v": "{:.9f}".format(g["a"]), "q": ITEM_Q_OK})
            rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "origin.y",
                          "v": "{:.9f}".format(g["b"]), "q": ITEM_Q_OK})
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "offset",
                      "v": "{:.9f}".format(g["offset"]), "q": ITEM_Q_OK})
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": pfx + "shift",
                      "v": "{:.9f}".format(g["shift"]), "q": ITEM_Q_OK})
    if include_grids_def_hash:
        gdh = _reference_grids_def_hash(len(grid_dicts), grid_dicts)
        rows.append({"domain": domain_name, "join_hash": join_hash, "k": GRIDS_DEF_HASH_KEY, "v": gdh, "q": ITEM_Q_OK})
    return rows


def _requested_join_hash_for(domain_name, grid_dicts):
    gdh = _reference_grids_def_hash(len(grid_dicts), grid_dicts)
    jh, _, _ = compute_join_hash_for_grids(domain_name, TARGET_NAME_BY_DOMAIN[domain_name], len(grid_dicts), gdh)
    return jh


# ---------------------------------------------------------------------------
# Reconstruction: happy path
# ---------------------------------------------------------------------------

def test_reconstruct_ok_with_full_evidence():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)

    assert result.status == STATUS_OK
    assert result.reasons == []
    assert result.grid_count == 2
    assert len(result.grids) == 2
    assert result.reconstructed_join_hash == jh


def test_block_xy_origin_not_creatable():
    # Revit's real FillGrid.Origin is exclusively UV-typed -- "xy" evidence
    # (domains/fill_patterns.py's defensive fallback for a runtime Origin shape
    # that has never been observed) can never be constructed or verified
    # against a live FillPatternElement, so reconstruction blocks it rather
    # than guessing x/y are interchangeable with u/v.
    grid_dicts = [{"idx": 0, "angle": 0.1, "origin_kind": "xy", "a": 3.0, "b": -1.5, "offset": 0.2, "shift": 0.0}]
    jh = _requested_join_hash_for(DOMAIN_MODEL, grid_dicts)
    rows = _settings_rows_for(DOMAIN_MODEL, jh, grid_dicts)

    result = reconstruct_pattern(DOMAIN_MODEL, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_origin_kind_not_creatable:000:xy"]


def test_reconstruct_degraded_when_grids_def_hash_evidence_absent():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts, include_grids_def_hash=False)

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_DEGRADED
    assert "grids_def_hash_evidence_unavailable" in result.reasons


def test_reconstruct_degraded_when_target_evidence_absent():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts, include_target=False)

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_DEGRADED
    assert "target_evidence_unavailable" in result.reasons


# ---------------------------------------------------------------------------
# Reconstruction: blocking conditions
# ---------------------------------------------------------------------------

def test_block_settings_absent():
    result = reconstruct_pattern(DOMAIN_DRAFTING, "deadbeef", [])
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["settings_absent"]


def test_block_no_items_marker():
    rows = [{"domain": DOMAIN_DRAFTING, "join_hash": "jh1", "k": "__no_items__", "v": "", "q": "missing"}]
    result = reconstruct_pattern(DOMAIN_DRAFTING, "jh1", rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["no_items_marker"]


def test_block_duplicate_settings_key():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)
    rows.append(dict(rows[1]))  # duplicate the grid_count row

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert any(r.startswith("duplicate_settings_key:") for r in result.reasons)


def test_block_target_mismatch():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)
    for r in rows:
        if r["k"] == TARGET_KEY:
            r["v"] = "Model"

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert any(r.startswith("target_mismatch:") for r in result.reasons)


def test_block_grid_count_missing():
    rows = [{"domain": DOMAIN_DRAFTING, "join_hash": "jh1", "k": TARGET_KEY, "v": "Drafting", "q": ITEM_Q_OK}]
    result = reconstruct_pattern(DOMAIN_DRAFTING, "jh1", rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_count_missing"]


def test_block_grid_count_not_creatable():
    rows = [
        {"domain": DOMAIN_DRAFTING, "join_hash": "jh1", "k": TARGET_KEY, "v": "Drafting", "q": ITEM_Q_OK},
        {"domain": DOMAIN_DRAFTING, "join_hash": "jh1", "k": GRID_COUNT_KEY, "v": "0", "q": ITEM_Q_OK},
    ]
    result = reconstruct_pattern(DOMAIN_DRAFTING, "jh1", rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_count_not_creatable:0"]


def test_block_grid_field_quality_not_ok():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)
    for r in rows:
        if r["k"] == "fill_pattern.grid[000].angle":
            r["q"] = ITEM_Q_UNREADABLE
            r["v"] = ""

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_field_quality:000:angle:unreadable"]


def test_block_grid_origin_kind_unmapped():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)
    for r in rows:
        if r["k"] == "fill_pattern.grid[000].origin.kind":
            r["v"] = "polar"

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_origin_kind_unmapped:000:polar"]


def test_block_grid_incomplete_missing_offset():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = [r for r in _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts) if r["k"] != "fill_pattern.grid[000].offset"]

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grid_incomplete:000:offset"]


def test_block_grids_def_hash_mismatch():
    grid_dicts = _sample_grid_dicts()
    jh = _requested_join_hash_for(DOMAIN_DRAFTING, grid_dicts)
    rows = _settings_rows_for(DOMAIN_DRAFTING, jh, grid_dicts)
    for r in rows:
        if r["k"] == GRIDS_DEF_HASH_KEY:
            r["v"] = "0" * 32

    result = reconstruct_pattern(DOMAIN_DRAFTING, jh, rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["grids_def_hash_mismatch"]


def test_block_reconstructed_join_hash_mismatch():
    grid_dicts = _sample_grid_dicts()
    rows = _settings_rows_for(DOMAIN_DRAFTING, "wrong_requested_hash", grid_dicts)

    result = reconstruct_pattern(DOMAIN_DRAFTING, "wrong_requested_hash", rows)
    assert result.status == STATUS_BLOCKED
    assert result.reasons == ["reconstructed_join_hash_mismatch"]
    assert result.reconstructed_join_hash is not None
    assert result.reconstructed_join_hash != "wrong_requested_hash"


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

def test_select_observed_name_highest_files_count_wins():
    rows = [
        {"label_v": "Brick", "label_q": ITEM_Q_OK, "files_count": "2"},
        {"label_v": "Masonry", "label_q": ITEM_Q_OK, "files_count": "9"},
    ]
    name, reasons = select_observed_name(rows)
    assert name == "Masonry"
    assert reasons == []


def test_select_observed_name_ignores_non_ok_and_empty():
    rows = [
        {"label_v": "", "label_q": ITEM_Q_OK, "files_count": "9"},
        {"label_v": "Brick", "label_q": "missing", "files_count": "9"},
        {"label_v": "Sand", "label_q": ITEM_Q_OK, "files_count": "1"},
    ]
    name, reasons = select_observed_name(rows)
    assert name == "Sand"


def test_resolve_observed_name_synthetic_fallback_is_deterministic():
    name1, is_synth1, reasons1 = resolve_observed_name([], "abc123def456")
    name2, is_synth2, reasons2 = resolve_observed_name([], "abc123def456")
    assert name1 == name2 == "unnamed_abc123def456"
    assert is_synth1 is True and is_synth2 is True
    assert "synthetic_name_used" in reasons1


def test_build_mapping_name_candidates_deterministic_collision_name():
    primary, collision = build_mapping_name_candidates("Brick, Common", "abcdef0123456789")
    assert primary == "MAP__Brick, Common"
    assert collision == "MAP__Brick, Common__abcdef012345"


def test_sanitize_revit_name_replaces_illegal_characters():
    assert sanitize_revit_name('a\\b:c{d}e[f]g|h;i<j>k?l\'m~n"o') == "a_b_c_d_e_f_g_h_i_j_k_l_m_n_o"
    assert sanitize_revit_name(None) == "unnamed"
    assert sanitize_revit_name("   ") == "unnamed"


# ---------------------------------------------------------------------------
# Grouping / report / status dominance
# ---------------------------------------------------------------------------

def test_group_requested_join_hashes_filters_by_domain_and_dedupes():
    rows = [
        {"domain": DOMAIN_DRAFTING, "segment_id": "seg1", "bundle_id": "b1", "pattern_id": "p1", "join_hash": "jh1"},
        {"domain": DOMAIN_DRAFTING, "segment_id": "seg1", "bundle_id": "b2", "pattern_id": "p1", "join_hash": "jh1"},
        {"domain": DOMAIN_MODEL, "segment_id": "seg1", "bundle_id": "b3", "pattern_id": "p2", "join_hash": "jh2"},
        {"domain": DOMAIN_DRAFTING, "segment_id": "seg1", "bundle_id": "b4", "pattern_id": "p3", "join_hash": ""},
    ]
    requested, skipped = group_requested_join_hashes(rows, DOMAIN_DRAFTING)
    assert set(requested.keys()) == {"jh1"}
    assert requested["jh1"]["bundle_ids"] == {"b1", "b2"}
    assert len(skipped) == 1
    assert skipped[0].domain == DOMAIN_DRAFTING
    assert skipped[0].reason == "join_hash_missing"


def test_dominant_status_ordering():
    assert dominant_status([STATUS_OK, STATUS_OK]) == STATUS_OK
    assert dominant_status([STATUS_OK, STATUS_DEGRADED]) == STATUS_DEGRADED
    assert dominant_status([STATUS_DEGRADED, STATUS_BLOCKED, STATUS_OK]) == STATUS_BLOCKED
    assert dominant_status([]) == STATUS_OK


def test_build_report_rows_ordering_by_domain_then_join_hash():
    outcomes = [
        MappingOutcome(domain=DOMAIN_MODEL, join_hash="a"),
        MappingOutcome(domain=DOMAIN_DRAFTING, join_hash="b"),
        MappingOutcome(domain=DOMAIN_DRAFTING, join_hash="a"),
    ]
    rows = build_report_rows(outcomes)
    assert [(r["domain"], r["join_hash"]) for r in rows] == [
        (DOMAIN_DRAFTING, "a"),
        (DOMAIN_DRAFTING, "b"),
        (DOMAIN_MODEL, "a"),
    ]


def test_compute_run_status_dominance_over_outcomes():
    outcomes = [
        MappingOutcome(domain=DOMAIN_DRAFTING, join_hash="a", status=STATUS_OK),
        MappingOutcome(domain=DOMAIN_DRAFTING, join_hash="b", status=STATUS_DEGRADED),
    ]
    assert compute_run_status(outcomes) == STATUS_DEGRADED
    outcomes.append(MappingOutcome(domain=DOMAIN_MODEL, join_hash="c", status=STATUS_BLOCKED))
    assert compute_run_status(outcomes) == STATUS_BLOCKED
    assert compute_run_status([]) == STATUS_OK
