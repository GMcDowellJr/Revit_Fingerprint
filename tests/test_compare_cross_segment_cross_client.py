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

from enterprise_policy import load_enterprise_policy  # noqa: E402
POLICY = load_enterprise_policy()

from compare_cross_segment import (  # noqa: E402
    _build_summary_row,
    _is_client_only_project_segment,
    _redundant_child_segment_id,
    _resolve_runnable_segment,
    _scope_override_key,
    discover_cross_client,
    discover_parent_siblings,
    discover_sibling_segments,
    drop_legacy_siblings_covered_by_peer_comparisons,
)


def _summary_row(seg_a, seg_b, ctype, manifest):
    """Thin wrapper over _build_summary_row() supplying the required
    bundle/metric arguments with inert defaults -- these tests only care
    about the scope-metadata columns it derives from `manifest`."""
    return _build_summary_row(
        POLICY, "crid", seg_a, seg_b, ctype, "some_domain", manifest, {},
        n_patterns_a=1, n_patterns_b=1, n_unique_patterns_a=1, n_unique_patterns_b=1,
        all_has_bundles_a="false", all_has_bundles_b="false",
        all_n_shared_bundle_both=0, all_n_shared_bundle_a_only=0, all_n_shared_bundle_b_only=0,
        used_has_bundles_a="false", used_has_bundles_b="false",
        used_n_shared_bundle_both=0, used_n_shared_bundle_a_only=0, used_n_shared_bundle_b_only=0,
        executed_utc="2026-01-01T00:00:00Z",
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
    assert _is_client_only_project_segment(_seg("Project", client="ClientAlpha")) is True


def test_is_client_only_project_segment_false_for_non_project_role():
    assert _is_client_only_project_segment(_seg("Container", client="ClientAlpha")) is False


def test_is_client_only_project_segment_false_when_client_blank():
    assert _is_client_only_project_segment(_seg("Project", client="")) is False
    assert _is_client_only_project_segment(_seg("Project", client="n/a")) is False


def test_is_client_only_project_segment_true_when_further_scoped_by_discipline():
    """discipline_label is a grouping dimension for discover_cross_client(),
    not a disqualifier -- a client's per-discipline roll-up is still a valid
    client-only population as long as bc/collection aren't also cut."""
    assert _is_client_only_project_segment(
        _seg("Project", client="ClientAlpha", discipline="architectural")
    ) is True


def test_is_client_only_project_segment_false_when_further_scoped_by_bc():
    assert _is_client_only_project_segment(
        _seg("Project", client="ClientAlpha", bc="BC_1")
    ) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_collection():
    assert _is_client_only_project_segment(
        _seg("Project", client="ClientAlpha", collection="2014")
    ) is False


# ---------------------------------------------------------------------------
# discover_cross_client()
# ---------------------------------------------------------------------------

def test_discover_cross_client_pairs_distinct_clients_same_unit():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha"),
        "p_clientbeta": _seg("Project", client="ClientBeta"),
    }
    pairs = discover_cross_client(POLICY, manifest)
    assert ("p_clientalpha", "p_clientbeta", "cross_client") in pairs
    assert len(pairs) == 1


def test_discover_cross_client_no_pair_across_different_unit_systems():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", unit="imperial"),
        "p_clientbeta": _seg("Project", client="ClientBeta", unit="metric"),
    }
    assert discover_cross_client(POLICY, manifest) == []


def test_discover_cross_client_discipline_scoped_segment_does_not_mix_with_broader_grain():
    """A client's discipline-scoped Project child is its own distinct
    population -- it does not pair against another client's blank-discipline
    (broader) portfolio segment; mixing grains would compare a narrower
    population against a broader one."""
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha"),
        "p_clientalpha_arch": _seg("Project", client="ClientAlpha", discipline="architectural"),
        "p_clientbeta": _seg("Project", client="ClientBeta"),
    }
    pairs = discover_cross_client(POLICY, manifest)
    assert ("p_clientalpha", "p_clientbeta", "cross_client") in pairs
    assert not any("p_clientalpha_arch" in (a, b) for a, b, _ in pairs)


def test_discover_cross_client_matching_discipline_peers_do_pair():
    """Two clients each with the SAME discipline-scoped Project population
    pair on that discipline grain -- discipline is a grouping dimension now,
    not an exclusion."""
    manifest = {
        "p_clientalpha_arch": _seg("Project", client="ClientAlpha", discipline="architectural"),
        "p_clientbeta_arch": _seg("Project", client="ClientBeta", discipline="architectural"),
    }
    pairs = discover_cross_client(POLICY, manifest)
    assert ("p_clientalpha_arch", "p_clientbeta_arch", "cross_client") in pairs
    assert len(pairs) == 1


def test_discover_cross_client_discipline_mismatch_produces_no_pair():
    """Matching unit_system alone is not sufficient -- differing
    discipline_label values must not produce a cross_client pair."""
    manifest = {
        "p_clientalpha_arch": _seg("Project", client="ClientAlpha", discipline="architectural"),
        "p_clientbeta_elec": _seg("Project", client="ClientBeta", discipline="electrical"),
    }
    assert discover_cross_client(POLICY, manifest) == []


def test_discover_cross_client_excludes_non_project_roles():
    manifest = {
        "t_clientalpha": _seg("Template", client="ClientAlpha"),
        "p_clientbeta": _seg("Project", client="ClientBeta"),
    }
    assert discover_cross_client(POLICY, manifest) == []


def test_discover_cross_client_excludes_registration_only_segments():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="registration"),
        "p_clientbeta": _seg("Project", client="ClientBeta"),
    }
    assert discover_cross_client(POLICY, manifest) == []


def test_discover_cross_client_three_clients_produces_all_pairs():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
        "p_c": _seg("Project", client="C"),
    }
    pairs = {(a, b) for a, b, ctype in discover_cross_client(POLICY, manifest) if ctype == "cross_client"}
    assert pairs == {("p_a", "p_b"), ("p_a", "p_c"), ("p_b", "p_c")}


def test_discover_cross_client_no_self_pair_or_reverse_duplicate():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
    }
    pairs = discover_cross_client(POLICY, manifest)
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
        ("p_clientalpha", "p_clientbeta", "sibling_projects"),
        ("p_clientalpha", "p_clientbeta", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("p_clientalpha", "p_clientbeta", "cross_client")]


def test_drop_legacy_sibling_projects_is_order_independent():
    pairs = [
        ("p_clientbeta", "p_clientalpha", "sibling_projects"),
        ("p_clientalpha", "p_clientbeta", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert result == [("p_clientalpha", "p_clientbeta", "cross_client")]


def test_drop_legacy_sibling_projects_leaves_uncovered_pairs_untouched():
    """A sibling_projects pair with no matching cross_client entry (e.g. two
    discipline-scoped Project siblings, which discover_cross_client() never
    emits) must be preserved."""
    pairs = [
        ("p_clientalpha_arch", "p_clientalpha_elec", "sibling_projects"),
        ("p_clientalpha", "p_clientbeta", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert ("p_clientalpha_arch", "p_clientalpha_elec", "sibling_projects") in result
    assert ("p_clientalpha", "p_clientbeta", "cross_client") in result
    assert len(result) == 2


def test_drop_legacy_sibling_projects_leaves_other_types_untouched():
    """Only sibling_projects is special-cased against cross_client -- other
    comparison_types for the exact same pair (a real, expected scenario per
    deduplicate_pairs()'s own docstring) must not be touched by this function."""
    pairs = [
        ("p_clientalpha", "p_clientbeta", "template_to_project"),
        ("p_clientalpha", "p_clientbeta", "cross_client"),
    ]
    result = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
    assert set(result) == set(pairs)


def test_drop_legacy_sibling_projects_noop_when_no_cross_client_rows():
    pairs = [("p_clientalpha", "p_clientbeta", "sibling_projects")]
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
    client "Akiser" (segment "p_zclientbeta") vs client "Zclientbeta" (segment
    "p_akiser") -- sibling_projects (segment-ID order) emits
    ("p_akiser", "p_zclientbeta"); cross_client (client-label order) emits
    ("p_zclientbeta", "p_akiser"), the reverse.
    """
    pairs = [
        ("p_akiser", "p_zclientbeta", "sibling_projects"),  # segment-ID sorted order
        ("p_zclientbeta", "p_akiser", "cross_client"),       # client-label sorted order (reversed)
    ]
    # Simulate `--segment-a p_akiser --segment-b p_zclientbeta`, applied BEFORE the
    # drop (the fixed order in main()).
    segment_a, segment_b = "p_akiser", "p_zclientbeta"
    filtered = [(a, b, ct) for a, b, ct in pairs if a == segment_a]
    filtered = [(a, b, ct) for a, b, ct in filtered if b == segment_b]
    result = drop_legacy_siblings_covered_by_peer_comparisons(filtered)
    # Only the correctly-oriented sibling_projects row matches the filter
    # (the reversed cross_client row doesn't survive it at all), so the drop
    # sees no cross_client entry for this pair and must not remove it.
    assert result == [("p_akiser", "p_zclientbeta", "sibling_projects")]


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
    row = _seg("Project", client="ClientBeta", run_type="registration",
                notes="redundant_single_child:imperial|Project|ClientBeta|BC_C")
    assert _redundant_child_segment_id(row) == "imperial|Project|ClientBeta|BC_C"


def test_redundant_child_segment_id_survives_pipe_in_segment_id_and_prior_notes():
    """segment_id uses "|" as its own separator, and redundant_single_child
    may not be the only note -- but pass5 always appends it last, so
    everything after the marker (pipes included) belongs to the segment_id."""
    row = _seg("Project", client="ClientBeta", run_type="registration",
                notes="below_min_files|redundant_single_child:imperial|Project|ClientBeta|BC_C|architectural")
    assert _redundant_child_segment_id(row) == "imperial|Project|ClientBeta|BC_C|architectural"


def test_redundant_child_segment_id_none_when_no_marker():
    assert _redundant_child_segment_id(_seg("Project", client="ClientBeta", run_type="registration")) is None


def test_resolve_runnable_segment_returns_self_when_already_eligible():
    manifest = {"p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle")}
    assert _resolve_runnable_segment(manifest, "p_clientalpha") == "p_clientalpha"


def test_resolve_runnable_segment_follows_single_hop():
    manifest = {
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle"),
    }
    assert _resolve_runnable_segment(manifest, "p_clientbeta") == "p_clientbeta_bc_c"


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
    manifest = {"p_clientalpha": _seg("Project", client="ClientAlpha", run_type="registration")}
    assert _resolve_runnable_segment(manifest, "p_clientalpha") is None


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
    """ClientBeta's client-only Project rollup was demoted to "registration"
    because all of ClientBeta's files sit in BC_C (single-BC client); it must
    still pair against ClientAlpha (a healthy, multi-BC client whose rollup was
    never demoted) using the population-identical BC_C-scoped segment."""
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle"),
    }
    pairs = discover_cross_client(POLICY, manifest)
    assert pairs == [("p_clientalpha", "p_clientbeta_bc_c", "cross_client")]


def test_discover_cross_client_no_rescue_when_pointed_to_child_also_ineligible():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="registration"),
    }
    assert discover_cross_client(POLICY, manifest) == []


# ---------------------------------------------------------------------------
# discover_sibling_segments() bc-scoped fallback
# ---------------------------------------------------------------------------

def test_discover_sibling_segments_rescues_single_bc_client_under_shared_parent():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle",
                          parent="p_root", segment_level="3"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c",
                          parent="p_root", segment_level="3"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle",
                               parent="p_clientbeta", segment_level="4"),
    }
    pairs = discover_sibling_segments(POLICY, manifest)
    assert ("p_clientalpha", "p_clientbeta_bc_c", "sibling_projects") in pairs
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
        # blank-role client rollup, demoted because ClientAlpha has no non-Project files
        "l2_clientalpha": _seg("", client="ClientAlpha", run_type="registration",
                           notes="redundant_single_child:l3_project_clientalpha",
                           parent="root", segment_level="2"),
        "l3_project_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle",
                                   parent="l2_project", segment_level="3"),
        "l2_template": _seg("Template", run_type="bundle", parent="root", segment_level="2"),
    }
    assert discover_parent_siblings(manifest) == []


# ---------------------------------------------------------------------------
# Scope-override metadata (Codex review finding on PR #380): a rescued pair's
# segment_id must stay the resolved descendant (the only segment with real
# on-disk data), but _build_summary_row() derives business_center_label_a/_b,
# discipline_label_a/_b, and scope_level_a/_b straight from that segment's own
# manifest row -- which, for a resolved descendant, is its own narrower
# identity rather than the broader population the pair was actually matched
# under. cross_client/sibling_projects have no consumer that re-derives scope
# from segment_id, so overriding these display columns to the original,
# broader row's values is safe and fixes the mislabeling. parent_sibling_roles
# is deliberately NOT overridden -- see discover_parent_siblings()'s own
# comment on why (_is_unscoped_segment() re-derives shape from segment_id
# itself, which no column override can satisfy).
# ---------------------------------------------------------------------------

def test_discover_cross_client_rescued_pair_reports_original_blank_scope():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle"),
    }
    pairs = discover_cross_client(POLICY, manifest)
    seg_a, seg_b, ctype = pairs[0]
    assert seg_b == "p_clientbeta_bc_c"  # segment_id stays the resolved (data-bearing) descendant

    row = _summary_row(seg_a, seg_b, ctype, manifest)
    assert row["segment_id_b"] == "p_clientbeta_bc_c"
    assert row["business_center_label_b"] == ""
    assert row["scope_level_b"] == ""


def test_discover_cross_client_override_does_not_leak_into_other_comparison_types():
    """The SAME resolved segment can legitimately appear under its own true
    (bc-scoped) identity in a different comparison_type (e.g.
    discover_client_cross_bc()) -- the override, namespaced by comparison_type,
    must not affect that reading."""
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle"),
    }
    discover_cross_client(POLICY, manifest)  # populates the cross_client-scoped override as a side effect

    row = _summary_row("p_clientbeta_bc_c", "p_clientbeta_bc_c", "client_cross_bc", manifest)
    assert row["business_center_label_a"] == "BC_C"


def test_discover_cross_client_no_override_when_no_resolution_needed():
    """A client-only segment that was already eligible (never demoted) must
    not carry a scope override at all -- its own row is already correct."""
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="bundle"),
    }
    discover_cross_client(POLICY, manifest)
    assert _scope_override_key("cross_client") not in manifest["p_clientalpha"]
    assert _scope_override_key("cross_client") not in manifest["p_clientbeta"]


def test_discover_sibling_segments_rescued_pair_reports_original_scope():
    manifest = {
        "p_clientalpha": _seg("Project", client="ClientAlpha", run_type="bundle",
                          parent="p_root", segment_level="3"),
        "p_clientbeta": _seg("Project", client="ClientBeta", run_type="registration",
                          notes="redundant_single_child:p_clientbeta_bc_c",
                          parent="p_root", segment_level="3"),
        "p_clientbeta_bc_c": _seg("Project", client="ClientBeta", bc="BC_C", run_type="bundle",
                               parent="p_clientbeta", segment_level="4"),
    }
    pairs = discover_sibling_segments(POLICY, manifest)
    seg_a, seg_b, ctype = pairs[0]
    assert ctype == "sibling_projects"

    row = _summary_row(seg_a, seg_b, ctype, manifest)
    resolved_side = "business_center_label_a" if seg_a == "p_clientbeta_bc_c" else "business_center_label_b"
    assert row[resolved_side] == ""


def test_discover_parent_siblings_rescued_pair_reports_resolved_descendants_true_scope():
    """Deliberately the OPPOSITE of cross_client/sibling_projects: no override
    is applied here, so the rescued Template side reports its resolved
    descendant's real (non-blank) business_center_label -- internally
    consistent with segment_id_a, which _is_unscoped_segment() in
    generate_governance_narrative.py re-derives its own classification from
    and which cannot be overridden without breaking on-disk data lookup."""
    manifest = {
        "l2_template": _seg("Template", run_type="registration",
                             notes="redundant_single_child:l2_template_bc",
                             parent="root", segment_level="2"),
        "l2_template_bc": _seg("Template", bc="BC_STD", run_type="bundle",
                                parent="l2_template", segment_level="3"),
        "l2_project": _seg("Project", run_type="bundle", parent="root", segment_level="2"),
    }
    pairs = discover_parent_siblings(manifest)
    seg_a, seg_b, ctype = pairs[0]
    assert seg_a == "l2_template_bc"

    row = _summary_row(seg_a, seg_b, ctype, manifest)
    assert row["business_center_label_a"] == "BC_STD"
    assert _scope_override_key("parent_sibling_roles") not in manifest["l2_template_bc"]
