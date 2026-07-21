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
    _redundant_child_segment_id,
    _resolve_runnable_segment,
    discover_cross_client,
    discover_parent_siblings,
    discover_sibling_segments,
    drop_legacy_siblings_covered_by_peer_comparisons,
)


def _seg(
    role,
    client="",
    unit="imperial",
    discipline="",
    bc="",
    collection="",
    run_type="bundle",
    notes="",
    parent="",
    segment_level="",
):
    return {
        "governance_role": role,
        "client_label": client,
        "unit_system": unit,
        "discipline_label": discipline,
        "business_center_label": bc,
        "collection_label": collection,
        "run_type": run_type,
        "notes": notes,
        "parent_segment_id": parent,
        "segment_level": segment_level,
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


def test_is_client_only_project_segment_true_when_further_scoped_by_discipline():
    """discipline_label is a grouping dimension for discover_cross_client(),
    not a disqualifier -- a client's per-discipline roll-up is still a valid
    client-only population as long as bc/collection aren't also cut."""
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", discipline="architectural")
    ) is True


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


def test_discover_cross_client_discipline_scoped_segment_does_not_mix_with_broader_grain():
    """A client's discipline-scoped Project child is its own distinct
    population -- it does not pair against another client's blank-discipline
    (broader) portfolio segment; mixing grains would compare a narrower
    population against a broader one."""
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser"),
        "p_kaiser_arch": _seg("Project", client="Kaiser", discipline="architectural"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser", "p_sutter", "cross_client") in pairs
    assert not any("p_kaiser_arch" in (a, b) for a, b, _ in pairs)


def test_discover_cross_client_matching_discipline_peers_do_pair():
    """Two clients each with the SAME discipline-scoped Project population
    pair on that discipline grain -- discipline is a grouping dimension now,
    not an exclusion."""
    manifest = {
        "p_kaiser_arch": _seg("Project", client="Kaiser", discipline="architectural"),
        "p_sutter_arch": _seg("Project", client="Sutter", discipline="architectural"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser_arch", "p_sutter_arch", "cross_client") in pairs
    assert len(pairs) == 1


def test_discover_cross_client_discipline_mismatch_produces_no_pair():
    """Matching unit_system alone is not sufficient -- differing
    discipline_label values must not produce a cross_client pair."""
    manifest = {
        "p_kaiser_arch": _seg("Project", client="Kaiser", discipline="architectural"),
        "p_sutter_elec": _seg("Project", client="Sutter", discipline="electrical"),
    }
    assert discover_cross_client(manifest) == []


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
# drop_legacy_siblings_covered_by_peer_comparisons()
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
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("p_kaiser", "p_sutter", "cross_client")]


def test_drop_legacy_sibling_projects_is_order_independent():
    pairs = [
        ("p_sutter", "p_kaiser", "sibling_projects"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("p_kaiser", "p_sutter", "cross_client")]


def test_drop_legacy_sibling_projects_leaves_uncovered_pairs_untouched():
    """A sibling_projects pair with no matching cross_client entry (e.g. two
    discipline-scoped Project siblings, which discover_cross_client() never
    emits) must be preserved."""
    pairs = [
        ("p_kaiser_arch", "p_kaiser_elec", "sibling_projects"),
        ("p_kaiser", "p_sutter", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
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
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert set(result) == set(pairs)


def test_drop_legacy_sibling_projects_noop_when_no_cross_client_rows():
    pairs = [("p_kaiser", "p_sutter", "sibling_projects")]
    assert drop_legacy_siblings_covered_by_peer_comparisons(pairs) == pairs


def test_segment_filter_before_drop_preserves_reversed_orientation_pair():
    """Regression for a fifth Codex review finding on PR #370: main() must
    apply --segment-a/--segment-b filtering BEFORE calling
    drop_legacy_siblings_covered_by_peer_comparisons() -- not after.

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
    result = drop_legacy_siblings_covered_by_peer_comparisons(filtered)
    # Only the correctly-oriented sibling_projects row matches the filter
    # (the reversed cross_client row doesn't survive it at all), so the drop
    # sees no cross_client entry for this pair and must not remove it.
    assert result == [("p_akiser", "p_zsutter", "sibling_projects")]


def test_drops_sibling_templates_when_same_pair_covered_by_bc_to_bc():
    """Regression for a PR #373 review finding: when two BC-scoped Template
    segments discover_governance_chain()'s bc_to_bc fan-out already pairs
    also happen to share an immediate parent_segment_id,
    discover_sibling_segments() re-pairs them as sibling_templates for the
    exact same (seg_a, seg_b) -- keeping both would collide on
    comparison_run_id and double-count the pair downstream. bc_to_bc wins."""
    pairs = [
        ("bc1_t", "bc2_t", "sibling_templates"),
        ("bc1_t", "bc2_t", "bc_to_bc"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("bc1_t", "bc2_t", "bc_to_bc")]


def test_drops_sibling_containers_when_same_pair_covered_by_bc_to_bc():
    pairs = [
        ("bc1_c", "bc2_c", "sibling_containers"),
        ("bc1_c", "bc2_c", "bc_to_bc"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("bc1_c", "bc2_c", "bc_to_bc")]


def test_drops_sibling_projects_when_same_pair_covered_by_client_cross_bc():
    """Regression for a PR #373 review finding: when client+BC-scoped Project
    segments for the same client discover_client_cross_bc() already pairs
    also happen to share a natural client parent,
    discover_sibling_segments() re-pairs them as sibling_projects for the
    exact same (seg_a, seg_b) -- client_cross_bc wins."""
    pairs = [
        ("acme_bc1", "acme_bc2", "sibling_projects"),
        ("acme_bc1", "acme_bc2", "client_cross_bc"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("acme_bc1", "acme_bc2", "client_cross_bc")]


def test_drop_leaves_sibling_generic_and_sibling_segments_untouched_when_uncovered():
    pairs = [
        ("g1", "g2", "sibling_generic"),
        ("x1", "x2", "sibling_segments"),
    ]
    assert drop_legacy_siblings_covered_by_peer_comparisons(pairs) == pairs


# ---------------------------------------------------------------------------
# _redundant_child_segment_id() / _resolve_runnable_segment()
#
# Regression coverage for the cross-client/bc-promotion starvation bug: once
# business_center_label became a real cut dimension (peer to client_label/
# discipline_label), a client whose Project files all sit in a single
# business center makes its client-only Project rollup byte-identical to
# that business-center-scoped child. build_segment_manifest.py's
# redundant_single_child pass correctly demotes the rollup to
# run_type="registration" (avoiding running the same population twice) and
# records a "redundant_single_child:<segment_id>" note -- but that silently
# dropped the client from discover_cross_client()/discover_sibling_segments()
# entirely, since both require run_type in (bundle, reference).
# ---------------------------------------------------------------------------

def test_redundant_child_segment_id_extracts_pointer():
    row = _seg("Project", client="Sutter", run_type="registration",
                notes="redundant_single_child:imperial|Project|Sutter|BC_C")
    assert _redundant_child_segment_id(row) == "imperial|Project|Sutter|BC_C"


def test_redundant_child_segment_id_survives_pipe_in_segment_id_and_prior_notes():
    """segment_id uses "|" as its own separator, and redundant_single_child
    may not be the only note -- but pass5 always appends it last, so
    everything after the marker (pipes included) belongs to the segment_id."""
    row = _seg("Project", client="Sutter", run_type="registration",
                notes="below_min_files|redundant_single_child:imperial|Project|Sutter|BC_C|architectural")
    assert _redundant_child_segment_id(row) == "imperial|Project|Sutter|BC_C|architectural"


def test_redundant_child_segment_id_none_when_no_marker():
    assert _redundant_child_segment_id(_seg("Project", client="Sutter", run_type="registration")) is None


def test_resolve_runnable_segment_returns_self_when_already_eligible():
    manifest = {"p_kaiser": _seg("Project", client="Kaiser", run_type="bundle")}
    assert _resolve_runnable_segment(manifest, "p_kaiser") == "p_kaiser"


def test_resolve_runnable_segment_follows_single_hop():
    manifest = {
        "p_sutter": _seg("Project", client="Sutter", run_type="registration",
                          notes="redundant_single_child:p_sutter_bc_c"),
        "p_sutter_bc_c": _seg("Project", client="Sutter", bc="BC_C", run_type="bundle"),
    }
    assert _resolve_runnable_segment(manifest, "p_sutter") == "p_sutter_bc_c"


def test_resolve_runnable_segment_follows_multi_hop_chain():
    """redundant_single_child can chain: a segment can be redundant to a
    child that is itself redundant to a grandchild. A single-hop lookup
    would wrongly stop at the still-ineligible intermediate row."""
    manifest = {
        "a": _seg("Template", run_type="registration", notes="redundant_single_child:b"),
        "b": _seg("Template", run_type="registration", notes="redundant_single_child:c"),
        "c": _seg("Template", run_type="bundle"),
    }
    assert _resolve_runnable_segment(manifest, "a") == "c"


def test_resolve_runnable_segment_none_when_chain_dead_ends():
    manifest = {
        "a": _seg("Project", run_type="registration", notes="redundant_single_child:missing"),
    }
    assert _resolve_runnable_segment(manifest, "a") is None


def test_resolve_runnable_segment_none_when_no_pointer_and_ineligible():
    manifest = {"p_kaiser": _seg("Project", client="Kaiser", run_type="registration")}
    assert _resolve_runnable_segment(manifest, "p_kaiser") is None


def test_resolve_runnable_segment_guards_against_cycle():
    manifest = {
        "a": _seg("Project", run_type="registration", notes="redundant_single_child:b"),
        "b": _seg("Project", run_type="registration", notes="redundant_single_child:a"),
    }
    assert _resolve_runnable_segment(manifest, "a") is None


# ---------------------------------------------------------------------------
# discover_cross_client() bc-scoped fallback
# ---------------------------------------------------------------------------

def test_discover_cross_client_rescues_single_bc_client_via_redundant_pointer():
    """Sutter's client-only Project rollup was demoted to "registration"
    because all of Sutter's files sit in BC_C (single-BC client); it must
    still pair against Kaiser (a healthy, multi-BC client whose rollup was
    never demoted) using the population-identical BC_C-scoped segment."""
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", run_type="bundle"),
        "p_sutter": _seg("Project", client="Sutter", run_type="registration",
                          notes="redundant_single_child:p_sutter_bc_c"),
        "p_sutter_bc_c": _seg("Project", client="Sutter", bc="BC_C", run_type="bundle"),
    }
    pairs = discover_cross_client(manifest)
    assert pairs == [("p_kaiser", "p_sutter_bc_c", "cross_client")]


def test_discover_cross_client_no_rescue_when_pointed_to_child_also_ineligible():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", run_type="bundle"),
        "p_sutter": _seg("Project", client="Sutter", run_type="registration",
                          notes="redundant_single_child:p_sutter_bc_c"),
        "p_sutter_bc_c": _seg("Project", client="Sutter", bc="BC_C", run_type="registration"),
    }
    assert discover_cross_client(manifest) == []


# ---------------------------------------------------------------------------
# discover_sibling_segments() bc-scoped fallback
# ---------------------------------------------------------------------------

def test_discover_sibling_segments_rescues_single_bc_client_under_shared_parent():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", run_type="bundle",
                          parent="p_root", segment_level="3"),
        "p_sutter": _seg("Project", client="Sutter", run_type="registration",
                          notes="redundant_single_child:p_sutter_bc_c",
                          parent="p_root", segment_level="3"),
        "p_sutter_bc_c": _seg("Project", client="Sutter", bc="BC_C", run_type="bundle",
                               parent="p_sutter", segment_level="4"),
    }
    pairs = discover_sibling_segments(manifest)
    assert ("p_kaiser", "p_sutter_bc_c", "sibling_projects") in pairs
    assert len(pairs) == 1


# ---------------------------------------------------------------------------
# discover_parent_siblings() bc-scoped fallback
# ---------------------------------------------------------------------------

def test_discover_parent_siblings_rescues_single_bc_template_rollup():
    manifest = {
        "l2_template": _seg("Template", run_type="registration",
                             notes="redundant_single_child:l2_template_bc",
                             parent="root", segment_level="2"),
        "l2_template_bc": _seg("Template", bc="BC_STD", run_type="bundle",
                                parent="l2_template", segment_level="3"),
        "l2_project": _seg("Project", run_type="bundle", parent="root", segment_level="2"),
    }
    pairs = discover_parent_siblings(manifest)
    assert pairs == [("l2_template_bc", "l2_project", "parent_sibling_roles")]


def test_discover_parent_siblings_does_not_misclassify_blank_role_rollup():
    """Regression: a blank-role, client-only rollup (pools every governance_role
    for that client) can itself be redundant_single_child to a role-scoped
    descendant (e.g. a client with no non-Project files, so its "all roles"
    rollup and its Project rollup are byte-identical). That descendant's OWN
    governance_role is "Project", but the blank-role original was never
    scoped to Project specifically -- it must not be classified into
    `projects` (and must not, therefore, produce a parent_sibling_roles pair
    it was never a legitimate input to)."""
    manifest = {
        # blank-role client rollup, demoted because Kaiser has no non-Project files
        "l2_kaiser": _seg("", client="Kaiser", run_type="registration",
                           notes="redundant_single_child:l3_project_kaiser",
                           parent="root", segment_level="2"),
        "l3_project_kaiser": _seg("Project", client="Kaiser", run_type="bundle",
                                   parent="l2_project", segment_level="3"),
        "l2_template": _seg("Template", run_type="bundle", parent="root", segment_level="2"),
    }
    assert discover_parent_siblings(manifest) == []
