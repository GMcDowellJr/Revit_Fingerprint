"""Tests for the structural_ancestor / population_containment lineage model
(D-027) in tools/compare_cross_segment.py: _build_ancestor_map()'s full
lattice transitive closure, _compute_containment_thresholds()/
_population_containment_map()'s Jenks-gated empirical containment relation,
and discover_sibling_segments()'s use of both as pair-discovery guards.

Background: prior to D-027, _build_ancestor_map() walked only
parent_segment_id (a single primary-parent chain), under-reporting ancestors
for any segment with more than one non-root dimension present (D-026's
dimensional-powerset lattice). A live corpus audit found discover_sibling_
segments() had no lineage guard at all and emitted 101 real population-
containment violations via its redundant_single_child resolution mechanism
grouping a structural ancestor and its own descendant as if they were
unrelated siblings. See DECISIONS.md D-026/D-027/D-028.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

import csv

from compare_cross_segment import (  # noqa: E402
    _build_ancestor_map,
    _compute_containment_thresholds,
    _is_lineage_related,
    _is_population_contained,
    _population_containment_map,
    detect_stale_ancestor_encoding,
    discover_sibling_segments,
    main as compare_main,
    validate_membership_against_manifest,
)


# ---------------------------------------------------------------------------
# (b) _build_ancestor_map lattice completeness
# ---------------------------------------------------------------------------

def _lattice_manifest():
    """Synthetic 3-non-root-field lattice (governance x client x business_
    center, discipline absent), fully populated with every one-field-drop
    ancestor at every depth -- exactly what build_segment_manifest.py's
    _build_segments() emits for a real multi-dimension-cut corpus. Segment
    "imperial|Container|InternalEnterprise|0000" (3 non-root fields present) has 2^3 - 1
    = 7 true structural ancestors at the root/1-field/2-field depths, but its
    OWN ancestor_segment_ids field (the one-field-drop adjacency list
    build_segment_manifest.py writes) lists only the 3 immediate ones -- the
    other 4 (including the root) are only reachable by recursively unioning
    each immediate parent's own ancestor set, which is exactly what
    _build_ancestor_map()'s transitive closure must do.
    """
    def row(parent, ancestor_ids):
        return {"parent_segment_id": parent, "ancestor_segment_ids": ";".join(ancestor_ids)}

    return {
        "imperial": row("", []),
        "imperial|Container": row("imperial", ["imperial"]),
        "imperial|InternalEnterprise": row("imperial", ["imperial"]),
        "imperial|0000": row("imperial", ["imperial"]),
        "imperial|Container|InternalEnterprise": row("imperial|Container", ["imperial|Container", "imperial|InternalEnterprise"]),
        "imperial|Container|0000": row("imperial|Container", ["imperial|0000", "imperial|Container"]),
        "imperial|InternalEnterprise|0000": row("imperial|InternalEnterprise", ["imperial|0000", "imperial|InternalEnterprise"]),
        "imperial|Container|InternalEnterprise|0000": row(
            "imperial|Container|InternalEnterprise",
            ["imperial|Container|0000", "imperial|Container|InternalEnterprise", "imperial|InternalEnterprise|0000"],
        ),
    }


def test_build_ancestor_map_full_lattice_closure():
    manifest = _lattice_manifest()
    ancestors = _build_ancestor_map(manifest)

    leaf = "imperial|Container|InternalEnterprise|0000"
    expected = {
        "imperial",
        "imperial|Container", "imperial|InternalEnterprise", "imperial|0000",
        "imperial|Container|InternalEnterprise", "imperial|Container|0000", "imperial|InternalEnterprise|0000",
    }
    assert ancestors[leaf] == expected, (
        "must include the root and every 1-field/2-field ancestor, not just "
        "the 3 immediate one-field-drop parents ancestor_segment_ids itself lists"
    )


def test_build_ancestor_map_superset_of_single_parent_chain():
    # The old implementation only ever recovered the parent_segment_id chain
    # (imperial|Container|InternalEnterprise|0000 -> imperial|Container|InternalEnterprise ->
    # imperial|Container -> imperial). The new closure must be a strict
    # superset of that, never a subset.
    manifest = _lattice_manifest()
    ancestors = _build_ancestor_map(manifest)
    old_chain = {"imperial|Container|InternalEnterprise", "imperial|Container", "imperial"}
    assert old_chain <= ancestors["imperial|Container|InternalEnterprise|0000"]
    assert len(ancestors["imperial|Container|InternalEnterprise|0000"]) > len(old_chain)


def test_is_lineage_related_symmetric_across_full_closure():
    manifest = _lattice_manifest()
    ancestors = _build_ancestor_map(manifest)
    # A 2-level-removed ancestor (root) must be caught, not just the direct parent.
    assert _is_lineage_related(ancestors, "imperial", "imperial|Container|InternalEnterprise|0000")
    assert _is_lineage_related(ancestors, "imperial|Container|InternalEnterprise|0000", "imperial")
    # Two segments with no subset relationship at all (siblings, e.g. two
    # different 1-field cuts) must not be flagged.
    assert not _is_lineage_related(ancestors, "imperial|Container", "imperial|InternalEnterprise")


def test_build_ancestor_map_cycle_detection_still_fires():
    # Adapted cycle guard: a self-referencing ancestor_segment_ids entry must
    # still be blocked, same as the old parent_segment_id-chain guard was.
    manifest = {
        "a": {"parent_segment_id": "", "ancestor_segment_ids": "b"},
        "b": {"parent_segment_id": "", "ancestor_segment_ids": "a"},
    }
    import pytest
    with pytest.raises(SystemExit):
        _build_ancestor_map(manifest)


# ---------------------------------------------------------------------------
# (c) population_containment threshold behavior
# ---------------------------------------------------------------------------

def _pop(prefix, n):
    return {f"{prefix}{i:03d}" for i in range(n)}


def _pop_hash(eids):
    return hashlib.sha1("|".join(sorted(eids)).encode()).hexdigest()


def test_population_containment_above_and_below_materiality_bar():
    # No structural relationships in this fixture -- containment must be
    # derived purely from real membership. A handful of tiny noise pairs are
    # included alongside the real signal so jenks_breaks() sees a genuine
    # bimodal distribution (mirroring the real corpus's "many tiny
    # coincidental subsets, few large real ones" shape) rather than a
    # degenerate 2-point input, whose small-N fallback in jenks_breaks()
    # behaves differently from the true Fisher-Jenks split. Two segments
    # share the "large, near-total" signal cluster (180/190 of 200) so the
    # natural-breaks boundary falls strictly below the larger one -- the
    # break itself always lands on the smaller member of the upper class,
    # which is intentionally excluded by the strict "at or below the break
    # is noise" rule (see the module-level comment), so this test only
    # asserts on the unambiguous side of that edge.
    manifest = {f"s{i}": {} for i in range(9)}
    ancestor_map = {}  # nothing structurally related

    big_pop = _pop("f", 200)
    signal_190_pop = set(list(big_pop)[:190])
    signal_180_pop = set(list(big_pop)[:180])
    noise_pops = [set(list(big_pop)[:n]) for n in (1, 2, 3, 2, 1)]
    unrelated_pop = _pop("g", 200)  # same size as big, but disjoint -- not a subset at all

    membership = {"big": big_pop, "signal_190": signal_190_pop, "signal_180": signal_180_pop,
                  "unrelated": unrelated_pop}
    for i, pop in enumerate(noise_pops):
        membership[f"noise{i}"] = pop

    thresholds = _compute_containment_thresholds(manifest | {k: {} for k in membership}, membership, ancestor_map)
    containment = _population_containment_map(manifest | {k: {} for k in membership}, membership, thresholds)

    assert _is_population_contained(containment, "big", "signal_190"), (
        "large, near-total subset must clear the materiality bar"
    )
    assert not _is_population_contained(containment, "big", "noise0"), (
        "tiny subset must be treated as small-sample coincidence, not real containment"
    )
    assert not _is_population_contained(containment, "big", "unrelated"), (
        "same-size disjoint populations are not a subset relationship at all"
    )


def test_population_containment_excludes_structural_pairs_from_threshold_fit_but_still_flags_them():
    # A structural pair (per ancestor_map) that is ALSO a large, near-total
    # subset must not corrupt the non-structural threshold fit, but the
    # resulting containment map still flags it (population_containment is
    # evaluated over every pair with membership data, not just non-structural
    # ones -- see the module-level comment in compare_cross_segment.py).
    # Filler noise pairs again keep jenks_breaks() out of its small-N
    # fallback branch; two non-structural signal pairs put the natural-breaks
    # boundary strictly below the larger one, same as the test above.
    manifest = {"parent": {}, "child": {}, "peer_a": {}, "peer_b_190": {}, "peer_b_180": {}}
    ancestor_map = {"child": {"parent"}, "parent": set()}

    parent_pop = _pop("f", 200)
    child_pop = set(list(parent_pop)[:195])  # structural, large, near-total -- same scale as the peer signal below

    peer_a_pop = _pop("g", 200)
    peer_b_190_pop = set(list(peer_a_pop)[:190])
    peer_b_180_pop = set(list(peer_a_pop)[:180])
    noise_pops = [set(list(peer_a_pop)[:n]) for n in (1, 2, 3, 2, 1)]

    membership = {
        "parent": parent_pop, "child": child_pop,
        "peer_a": peer_a_pop, "peer_b_190": peer_b_190_pop, "peer_b_180": peer_b_180_pop,
    }
    for i, pop in enumerate(noise_pops):
        membership[f"noise{i}"] = pop

    full_manifest = manifest | {k: {} for k in membership}
    thresholds = _compute_containment_thresholds(full_manifest, membership, ancestor_map)
    containment = _population_containment_map(full_manifest, membership, thresholds)

    assert _is_population_contained(containment, "parent", "child")
    assert _is_population_contained(containment, "peer_a", "peer_b_190")


def test_population_containment_identical_populations_always_contained():
    # Byte-identical populations are the strongest possible form of the
    # subset relationship population_containment exists to catch -- must be
    # unconditionally flagged, bypassing both materiality thresholds
    # entirely (build_segment_manifest.py only warns, never blocks, on a
    # duplicate bundle population_hash, so this is a real possible state,
    # not just a hypothetical one -- PR #423 review finding).
    manifest = {"a": {}, "b": {}, "noise0": {}, "noise1": {}, "big": {}, "small": {}}
    ancestor_map = {}
    shared_pop = _pop("f", 50)
    membership = {
        "a": shared_pop, "b": set(shared_pop),  # identical, not the same object
        "noise0": set(list(shared_pop)[:1]), "noise1": set(list(shared_pop)[:2]),
        "big": _pop("g", 100), "small": set(list(_pop("g", 100))[:90]),
    }
    thresholds = _compute_containment_thresholds(manifest, membership, ancestor_map)
    containment = _population_containment_map(manifest, membership, thresholds)
    assert _is_population_contained(containment, "a", "b")


def test_population_containment_boundary_value_included_not_excluded():
    # A pair sitting exactly AT the Jenks break must clear the size floor
    # (jenks_breaks()'s own docstring: "values below break_0 are class 1
    # (lowest)" -- the break itself belongs to the upper/signal class), not
    # be treated as noise by a stray strict "greater than" (PR #423 review
    # finding -- confirmed against the real corpus, where 21 of 1,806
    # non-structural pairs sit exactly at the size break).
    manifest = {f"s{i}": {} for i in range(8)}
    ancestor_map = {}
    # Two pairs share the exact same (smaller) size -- both should become
    # class-2 members once jenks correctly separates {noise} from {signal},
    # and the break itself (== that shared size) must not exclude them.
    big_pop = _pop("f", 200)
    signal_a_pop = set(list(big_pop)[:150])
    signal_b_pop = set(list(_pop("g", 200))[:150])
    noise_pops = [set(list(big_pop)[:n]) for n in (1, 2, 3, 2, 1)]
    membership = {
        "big": big_pop, "other": _pop("g", 200),
        "signal_a": signal_a_pop, "signal_b": signal_b_pop,
    }
    for i, pop in enumerate(noise_pops):
        membership[f"noise{i}"] = pop
    full_manifest = manifest | {k: {} for k in membership}

    thresholds = _compute_containment_thresholds(full_manifest, membership, ancestor_map)
    assert thresholds["min_population_for_containment"] == 150.0
    containment = _population_containment_map(full_manifest, membership, thresholds)
    assert _is_population_contained(containment, "big", "signal_a")
    assert _is_population_contained(containment, "other", "signal_b")


# ---------------------------------------------------------------------------
# (e) Jenks threshold computation: deterministic + degenerate-input behavior
# ---------------------------------------------------------------------------

def test_compute_containment_thresholds_deterministic():
    manifest = {f"s{i}": {} for i in range(6)}
    ancestor_map = {}
    membership = {
        "s0": _pop("f", 100), "s1": set(list(_pop("f", 100))[:90]),
        "s2": _pop("g", 100), "s3": set(list(_pop("g", 100))[:20]),
        "s4": _pop("h", 100), "s5": set(list(_pop("h", 100))[:70]),
    }
    first = _compute_containment_thresholds(manifest, membership, ancestor_map)
    second = _compute_containment_thresholds(manifest, membership, ancestor_map)
    assert first["min_population_for_containment"] == second["min_population_for_containment"]
    assert first["min_containment_ratio"] == second["min_containment_ratio"]


def test_compute_containment_thresholds_no_non_structural_pairs():
    # Degenerate input: every real subset pair is structurally explained (or
    # there are no subset pairs at all) -- jenks_breaks() is handed an empty
    # list. Must not crash; must return sane (zero) thresholds rather than
    # excluding everything by an undefined comparison.
    manifest = {"a": {}, "b": {}}
    ancestor_map = {"b": {"a"}, "a": set()}
    membership = {"a": _pop("f", 50), "b": set(list(_pop("f", 50))[:40])}
    thresholds = _compute_containment_thresholds(manifest, membership, ancestor_map)
    assert thresholds["min_population_for_containment"] == 0.0
    assert thresholds["min_containment_ratio"] == 0.0


def test_compute_containment_thresholds_empty_membership():
    manifest = {"a": {}, "b": {}}
    thresholds = _compute_containment_thresholds(manifest, {}, {})
    assert thresholds["min_population_for_containment"] == 0.0
    assert thresholds["min_containment_ratio"] == 0.0


# ---------------------------------------------------------------------------
# (d) discover_sibling_segments regression: real-corpus-shaped violation,
# pre-fix (no guard) vs post-fix (structural guard active by default)
# ---------------------------------------------------------------------------

def _sibling_row(parent, role, client="", bc="", ancestor_ids=(), run_type="bundle", notes=""):
    return {
        "parent_segment_id": parent,
        "governance_role": role,
        "unit_system": "imperial",
        "client_label": client,
        "business_center_label": bc,
        "discipline_label": "",
        "run_type": run_type,
        "notes": notes,
        "ancestor_segment_ids": ";".join(ancestor_ids),
    }


def _real_corpus_shaped_manifest():
    """Reproduces the exact shape of a real, corpus-verified
    discover_sibling_segments() violation (D-027 finding 5): a client-wide
    Container rollup ("imperial|Container|InternalEnterprise") shares its parent
    ("imperial|Container") with a business-center-only Container segment
    ("imperial|Container|0000") that build_segment_manifest.py's
    redundant_single_child pass demoted (population-identical to its
    narrower "imperial|Container|InternalEnterprise|0000" child). Both resolve into the
    SAME sibling group even though "imperial|Container|InternalEnterprise" is a real
    structural ancestor of "imperial|Container|InternalEnterprise|0000" (dropping
    business_center_label from the latter's key recovers the former's).
    """
    return {
        "imperial|Container": _sibling_row("imperial", "Container"),
        "imperial|Container|InternalEnterprise": _sibling_row(
            "imperial|Container", "Container", client="InternalEnterprise",
            ancestor_ids=["imperial|Container"],
        ),
        "imperial|Container|0000": _sibling_row(
            "imperial|Container", "Container", bc="0000",
            run_type="registration",
            notes="redundant_single_child:imperial|Container|InternalEnterprise|0000",
            ancestor_ids=["imperial|Container"],
        ),
        "imperial|Container|InternalEnterprise|0000": _sibling_row(
            "imperial|Container|InternalEnterprise", "Container", client="InternalEnterprise", bc="0000",
            ancestor_ids=["imperial|Container", "imperial|Container|InternalEnterprise"],
        ),
    }


def test_discover_sibling_segments_pre_fix_reproduces_violation():
    # Simulate pre-D-027 behavior: no lineage guard at all (empty ancestor_map,
    # no containment_map). The real violation must reproduce.
    manifest = _real_corpus_shaped_manifest()
    pairs = discover_sibling_segments(manifest, ancestor_map={}, containment_map=None)
    assert ("imperial|Container|InternalEnterprise", "imperial|Container|InternalEnterprise|0000", "sibling_containers") in pairs


def test_discover_sibling_segments_post_fix_excludes_violation():
    # Default behavior (no args): ancestor_map is now auto-built from the
    # manifest's own ancestor_segment_ids field, and the structural guard
    # excludes the real ancestor/descendant pair.
    manifest = _real_corpus_shaped_manifest()
    pairs = discover_sibling_segments(manifest)
    assert ("imperial|Container|InternalEnterprise", "imperial|Container|InternalEnterprise|0000", "sibling_containers") not in pairs
    assert not any(
        {a, b} == {"imperial|Container|InternalEnterprise", "imperial|Container|InternalEnterprise|0000"}
        for a, b, _ in pairs
    )


def test_discover_sibling_segments_unrelated_siblings_still_pair():
    # A genuine, non-structural sibling pair in the same group must still be
    # emitted -- the guard must not over-exclude.
    manifest = _real_corpus_shaped_manifest()
    manifest["imperial|Container|ClientBeta"] = _sibling_row(
        "imperial|Container", "Container", client="ClientBeta",
        ancestor_ids=["imperial|Container"],
    )
    pairs = discover_sibling_segments(manifest)
    assert any(
        {a, b} == {"imperial|Container|InternalEnterprise", "imperial|Container|ClientBeta"}
        for a, b, _ in pairs
    )


def test_discover_sibling_segments_backward_compatible_without_ancestor_data():
    # A hand-built fixture with no ancestor_segment_ids populated at all
    # (the pre-D-027 test-fixture convention used throughout the rest of the
    # test suite) must behave exactly as before: no exclusion, since
    # _build_ancestor_map() derives an empty ancestor set when the field is
    # blank/absent.
    manifest = {
        "p": {"parent_segment_id": "", "governance_role": "", "unit_system": ""},
        "a": {"parent_segment_id": "p", "governance_role": "Container", "unit_system": "imperial", "run_type": "bundle"},
        "b": {"parent_segment_id": "p", "governance_role": "Container", "unit_system": "imperial", "run_type": "bundle"},
    }
    pairs = discover_sibling_segments(manifest)
    assert ("a", "b", "sibling_containers") in pairs


# ---------------------------------------------------------------------------
# validate_membership_against_manifest (PR #423 review finding: guard against
# a stale/mismatched segment_membership.csv silently driving
# population_containment off a population that disagrees with what
# segment_manifest.csv itself records for that segment_id)
# ---------------------------------------------------------------------------

def test_validate_membership_against_manifest_agreement_no_errors():
    manifest = {"a": {"file_count": "3", "population_hash": _pop_hash({"e1", "e2", "e3"})}}
    membership = {"a": {"e1", "e2", "e3"}}
    assert validate_membership_against_manifest(manifest, membership) == []


def test_validate_membership_against_manifest_file_count_mismatch():
    manifest = {"a": {"file_count": "3", "population_hash": _pop_hash({"e1", "e2", "e3"})}}
    membership = {"a": {"e1", "e2"}}  # stale: only 2, manifest says 3
    errors = validate_membership_against_manifest(manifest, membership)
    assert len(errors) == 1
    assert "a" in errors[0] and "file_count" in errors[0]


def test_validate_membership_against_manifest_population_hash_mismatch():
    manifest = {"a": {"file_count": "3", "population_hash": _pop_hash({"e1", "e2", "e3"})}}
    membership = {"a": {"e1", "e2", "e4"}}  # same count, different actual members
    errors = validate_membership_against_manifest(manifest, membership)
    assert len(errors) == 1
    assert "population_hash" in errors[0]


def test_validate_membership_against_manifest_unknown_segment_ignored():
    # A membership segment_id absent from manifest is out of scope for this
    # check (nothing to compare against) -- not itself an error here.
    manifest = {}
    membership = {"ghost": {"e1"}}
    assert validate_membership_against_manifest(manifest, membership) == []


def test_validate_membership_against_manifest_entirely_missing_segment():
    # PR #423 review finding: a truncated/partially-written segment_
    # membership.csv that omits an entire manifest segment (not just
    # under-counts it) must also be flagged -- the first pass alone (looping
    # over membership.items()) can never see an omission, since there's no
    # entry to iterate.
    manifest = {
        "a": {"file_count": "3", "population_hash": _pop_hash({"e1", "e2", "e3"})},
        "b": {"file_count": "2", "population_hash": _pop_hash({"e4", "e5"})},
    }
    membership = {"a": {"e1", "e2", "e3"}}  # "b" entirely absent from the sidecar
    errors = validate_membership_against_manifest(manifest, membership)
    assert len(errors) == 1
    assert "b" in errors[0] and "no export_run_id rows" in errors[0]


def test_validate_membership_against_manifest_zero_file_count_segment_not_flagged():
    # A manifest segment with file_count=0 (or blank) legitimately has no
    # membership rows -- not a truncation signal.
    manifest = {"a": {"file_count": "0", "population_hash": ""}}
    membership = {}
    assert validate_membership_against_manifest(manifest, membership) == []


# ---------------------------------------------------------------------------
# detect_stale_ancestor_encoding (PR #423 review finding: a pre-D-028
# pipe-joined ancestor_segment_ids value should be flagged, not silently
# degrade lineage exclusion back to the parent_segment_id-only fallback)
# ---------------------------------------------------------------------------

def test_detect_stale_ancestor_encoding_flags_pipe_joined_blob():
    manifest = {
        "imperial|Container|InternalEnterprise|0000": {
            "governance_role": "Container", "client_label": "InternalEnterprise", "business_center_label": "0000",
            # pre-D-028 shape: "|".join(["imperial|0000", "imperial|Container|InternalEnterprise"])
            "ancestor_segment_ids": "imperial|0000|imperial|Container|InternalEnterprise",
        },
    }
    warnings = detect_stale_ancestor_encoding(manifest)
    assert len(warnings) == 1
    assert "imperial|Container|InternalEnterprise|0000" in warnings[0]


def test_detect_stale_ancestor_encoding_does_not_flag_wellformed_semicolon_data():
    manifest = _lattice_manifest()
    assert detect_stale_ancestor_encoding(manifest) == []


def test_detect_stale_ancestor_encoding_does_not_flag_genuine_single_ancestor():
    # Only one non-root field present -> at most one immediate ancestor is
    # possible at all; a single, unremarkable segment_id (few "|"s) here is
    # normal, not a stale-format signal.
    manifest = {
        "imperial|Container": {"governance_role": "Container", "ancestor_segment_ids": "imperial"},
    }
    assert detect_stale_ancestor_encoding(manifest) == []


def test_validate_membership_against_manifest_completely_empty_sidecar():
    # PR #423 review finding: segment_membership.csv present on disk but
    # header-only/all-invalid-rows loads as membership={} -- indistinguishable
    # from "file absent" by dict truthiness alone. The second pass over
    # manifest.items() must still flag every eligible segment against a
    # totally empty membership dict (main()'s fix is to call this function
    # whenever the file EXISTS, not only when the loaded dict is non-empty --
    # this test proves the function itself behaves correctly once called).
    manifest = {
        "a": {"file_count": "3", "population_hash": _pop_hash({"e1", "e2", "e3"})},
        "b": {"file_count": "2", "population_hash": _pop_hash({"e4", "e5"})},
    }
    errors = validate_membership_against_manifest(manifest, {})
    assert len(errors) == 2
    assert any("a" in e for e in errors)
    assert any("b" in e for e in errors)


# ---------------------------------------------------------------------------
# Stale population_containment_thresholds.csv cleanup (PR #423 review
# finding: a prior run's thresholds file must not be left in --out-dir
# looking current when THIS run has population_containment disabled)
# ---------------------------------------------------------------------------

def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_main_removes_stale_thresholds_when_containment_disabled(tmp_path, monkeypatch):
    records_dir = tmp_path / "records"
    segments_root = tmp_path / "segments"
    out_dir = tmp_path / "out"
    records_dir.mkdir()

    _write_csv(records_dir / "segment_manifest.csv", [{
        "segment_id": "proj_a", "segment_label": "Project A", "governance_role": "Project",
        "client_label": "Acme", "discipline_label": "Arch", "unit_system": "imperial",
        "run_type": "bundle", "segment_level": "2", "parent_segment_id": "imperial",
    }])
    _write_csv(records_dir / "run_registry.csv", [
        {"segment_id": "proj_a", "output_folder": "proj_a", "run_type": "bundle"},
    ])
    _write_csv(records_dir / "file_metadata.csv", [{"export_run_id": "fa1", "project_label": ""}])
    # Deliberately no segment_membership.csv -- population_containment must
    # be disabled for this run.

    out_dir.mkdir(parents=True)
    stale_path = out_dir / "population_containment_thresholds.csv"
    stale_path.write_text("stage,algorithm\nsize_noise_filter,jenks_breaks\n", encoding="utf-8")
    assert stale_path.exists()

    monkeypatch.setattr(
        sys, "argv",
        [
            "compare_cross_segment.py",
            "--segments-root", str(segments_root),
            "--records-dir", str(records_dir),
            "--out-dir", str(out_dir),
            "--sibling-segments",
            "--workers", "1",
            "--no-delta",
        ],
    )

    assert compare_main() == 0
    assert not stale_path.exists(), "stale thresholds file from a prior run must be removed when containment is disabled"
