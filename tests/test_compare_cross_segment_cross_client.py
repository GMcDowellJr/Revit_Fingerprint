"""Tests for discover_cross_client() / _is_client_only_project_segment() in
tools/compare_cross_segment.py.

Background: cross_client is a purpose-built client-vs-client comparison type
(governance pipeline gap fix). Before this, the only thing feeding the
cross_client_convergence / cross_client_similarity_mean columns downstream in
generate_governance_narrative.py was "sibling_projects" -- an accidental
overload that only pairs Project segments sharing an immediate
parent_segment_id, which a corpus need not ever satisfy for two different
clients. discover_cross_client() pairs each client's own broadest
(client-only-scoped) Project population against every other client's, within
the same unit_system, independent of segment lineage.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import (  # noqa: E402
    _is_client_only_project_segment,
    discover_cross_client,
    drop_legacy_sibling_projects_covered_by_cross_client,
)


def _seg(role, client="", unit="imperial", discipline="", bc="", collection="", run_type="bundle"):
    return {
        "governance_role": role,
        "client_label": client,
        "unit_system": unit,
        "discipline_label": discipline,
        "business_center_label": bc,
        "collection_label": collection,
        "run_type": run_type,
    }


# ---------------------------------------------------------------------------
# _is_client_only_project_segment()
# ---------------------------------------------------------------------------

def test_is_client_only_project_segment_true_for_bare_client_scope():
    assert _is_client_only_project_segment(_seg("Project", client="Kaiser")) is True


def test_is_client_only_project_segment_false_for_non_project_role():
    assert _is_client_only_project_segment(_seg("Container", client="Kaiser")) is False


def test_is_client_only_project_segment_false_when_client_blank():
    assert _is_client_only_project_segment(_seg("Project", client="")) is False
    assert _is_client_only_project_segment(_seg("Project", client="n/a")) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_discipline():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", discipline="architectural")
    ) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_bc():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", bc="BC_1")
    ) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_collection():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", collection="2014")
    ) is False


# ---------------------------------------------------------------------------
# discover_cross_client()
# ---------------------------------------------------------------------------

def test_discover_cross_client_pairs_distinct_clients_same_unit():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser", "p_sutter", "cross_client") in pairs
    assert len(pairs) == 1


def test_discover_cross_client_no_pair_across_different_unit_systems():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", unit="imperial"),
        "p_sutter": _seg("Project", client="Sutter", unit="metric"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_excludes_discipline_scoped_project_segments():
    """A client's discipline-scoped Project child must not participate --
    only the client's own broadest (client-only) segment is a peer of another
    client's broadest segment; mixing grains would compare a narrower
    population against a broader one."""
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser"),
        "p_kaiser_arch": _seg("Project", client="Kaiser", discipline="architectural"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser", "p_sutter", "cross_client") in pairs
    assert not any("p_kaiser_arch" in (a, b) for a, b, _ in pairs)


def test_discover_cross_client_excludes_non_project_roles():
    manifest = {
        "t_kaiser": _seg("Template", client="Kaiser"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_excludes_registration_only_segments():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", run_type="registration"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_three_clients_produces_all_pairs():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
        "p_c": _seg("Project", client="C"),
    }
    pairs = {(a, b) for a, b, ctype in discover_cross_client(manifest) if ctype == "cross_client"}
    assert pairs == {("p_a", "p_b"), ("p_a", "p_c"), ("p_b", "p_c")}


def test_discover_cross_client_no_self_pair_or_reverse_duplicate():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_b", "p_a", "cross_client") not in pairs
    assert ("p_a", "p_a", "cross_client") not in pairs


# ---------------------------------------------------------------------------
# drop_legacy_sibling_projects_covered_by_cross_client()
# ---------------------------------------------------------------------------

def test_drops_sibling_projects_when_same_pair_covered_by_cross_client():
    """Regression for a Codex review finding on PR #370: when
    discover_sibling_segments() and discover_cross_client() both fire for the
    same two client-only Project segments (they share an immediate parent),
    keeping both double-counts that pair in xc/xc_by_client and collides on
    comparison_run_id. cross_client wins; the sibling_projects entry for that
    exact pair is dropped."""
    pairs = [
        ("p_kaiser", "p_sutter", "sibling_projects"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_sibling_projects_covered_by_cross_client(pairs)
    assert result == [("p_kaiser", "p_sutter", "cross_client")]


def test_drop_legacy_sibling_projects_is_order_independent():
    pairs = [
        ("p_sutter", "p_kaiser", "sibling_projects"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_sibling_projects_covered_by_cross_client(pairs)
    assert result == [("p_kaiser", "p_sutter", "cross_client")]


def test_drop_legacy_sibling_projects_leaves_uncovered_pairs_untouched():
    """A sibling_projects pair with no matching cross_client entry (e.g. two
    discipline-scoped Project siblings, which discover_cross_client() never
    emits) must be preserved."""
    pairs = [
        ("p_kaiser_arch", "p_kaiser_elec", "sibling_projects"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_sibling_projects_covered_by_cross_client(pairs)
    assert ("p_kaiser_arch", "p_kaiser_elec", "sibling_projects") in result
    assert ("p_kaiser", "p_sutter", "cross_client") in result
    assert len(result) == 2


def test_drop_legacy_sibling_projects_leaves_other_types_untouched():
    """Only sibling_projects is special-cased against cross_client -- other
    comparison_types for the exact same pair (a real, expected scenario per
    deduplicate_pairs()'s own docstring) must not be touched by this function."""
    pairs = [
        ("p_kaiser", "p_sutter", "template_to_project"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_sibling_projects_covered_by_cross_client(pairs)
    assert set(result) == set(pairs)


def test_drop_legacy_sibling_projects_noop_when_no_cross_client_rows():
    pairs = [("p_kaiser", "p_sutter", "sibling_projects")]
    assert drop_legacy_sibling_projects_covered_by_cross_client(pairs) == pairs


def test_segment_filter_before_drop_preserves_reversed_orientation_pair():
    """Regression for a fifth Codex review finding on PR #370: main() must
    apply --segment-a/--segment-b filtering BEFORE calling
    drop_legacy_sibling_projects_covered_by_cross_client() -- not after.

    discover_sibling_segments() orders its pair by sorted segment ID, while
    discover_cross_client() orders by sorted client label -- for two segments
    whose ID order doesn't match their client-label order, the surviving
    cross_client pair can be the reverse (b, a) of the sibling_projects pair
    it replaces. The position-sensitive --segment-a/--segment-b filters
    (`a == args.segment_a`, `b == args.segment_b`) would then reject that
    reversed row too, leaving a scoped run with zero pairs for segments that
    do have a comparison -- unless filtering happens first, so the drop only
    ever operates on whichever orientation actually survived the requested
    scope.

    Here segment_id sorts "p_zclient" < "p_akiser" is false alphabetically
    (z > a), so pick IDs that invert the client-label sort order directly:
    client "Akiser" (segment "p_zsutter") vs client "Zsutter" (segment
    "p_akiser") -- sibling_projects (segment-ID order) emits
    ("p_akiser", "p_zsutter"); cross_client (client-label order) emits
    ("p_zsutter", "p_akiser"), the reverse.
    """
    pairs = [
        ("p_akiser", "p_zsutter", "sibling_projects"),  # segment-ID sorted order
        ("p_zsutter", "p_akiser", "cross_client"),       # client-label sorted order (reversed)
    ]
    # Simulate `--segment-a p_akiser --segment-b p_zsutter`, applied BEFORE the
    # drop (the fixed order in main()).
    segment_a, segment_b = "p_akiser", "p_zsutter"
    filtered = [(a, b, ct) for a, b, ct in pairs if a == segment_a]
    filtered = [(a, b, ct) for a, b, ct in filtered if b == segment_b]
    result = drop_legacy_sibling_projects_covered_by_cross_client(filtered)
    # Only the correctly-oriented sibling_projects row matches the filter
    # (the reversed cross_client row doesn't survive it at all), so the drop
    # sees no cross_client entry for this pair and must not remove it.
    assert result == [("p_akiser", "p_zsutter", "sibling_projects")]
