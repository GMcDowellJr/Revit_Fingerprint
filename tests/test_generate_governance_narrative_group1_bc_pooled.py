"""Tests for the Group 1 (tc/cp/tp) bc-pooled fallback in
tools/generate_governance_narrative.py's build_cascade()/assign_tier().

Background: since business_center_label became a real segmentation cut
dimension, almost no segment is fully unscoped anymore, so tc/cp/tp are None
for effectively every domain and assign_tier() always falls to
TIER_INSUFFICIENT regardless of real bc-pooled evidence sitting unused in
cross_segment_summary.csv. This mirrors Group 2's Option C precedent
(gt_by_scope/gc_by_scope/gp_by_scope, tests/test_generate_governance_narrative_scope_breakdown.py):
tc/cp/tp themselves stay gated to the single "enterprise::enterprise" pair
(unchanged), while tc_by_scope/cp_by_scope/tp_by_scope capture every other
(scope_a, scope_b) pair instead of discarding it. A same-bc-both-sides
("bc::bc") pooled value gives assign_tier() a new, explicitly-named fallback
tier (TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE) instead of blending into the
existing enterprise-only `primary` -- see
docs/governance_narrative_group1_scope_gap_investigation.md (Q2/Q3).
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    TIER_INSUFFICIENT,
    TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE,
    TIER_ORDER,
    _has_group1_bc_pooled_evidence,
    assign_tier,
    build_cascade,
    detect_anomalies,
    normalise_summary_schema,
    render_group1_scope_section,
)


def _row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


# ---------------------------------------------------------------------------
# build_cascade(): tc_by_scope/cp_by_scope/tp_by_scope population
# ---------------------------------------------------------------------------

def test_tc_enterprise_slice_unchanged_by_bc_scoped_rows():
    """tc (the rendered headline number) must equal exactly the
    enterprise::enterprise-scope mean, regardless of how many bc-scoped rows
    exist alongside it -- Option A is preserved even though Option C now
    captures the rest. This is the byte-for-byte-unchanged guarantee the
    investigation's fix explicitly requires."""
    rows = [
        _row(segment_id_a="imperial|Template", segment_id_b="imperial|Container",
             governance_role_a="Template", governance_role_b="Container",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.80", n_files_a="3", n_files_b="5"),
        _row(segment_id_a="imperial|Template|BC_1", segment_id_b="imperial|Container|BC_1",
             governance_role_a="Template", governance_role_b="Container",
             business_center_label_a="BC_1", business_center_label_b="BC_1",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.40", n_files_a="2", n_files_b="4"),
        _row(segment_id_a="imperial|Template|BC_2", segment_id_b="imperial|Container|BC_2",
             governance_role_a="Template", governance_role_b="Container",
             business_center_label_a="BC_2", business_center_label_b="BC_2",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.60", n_files_a="2", n_files_b="4"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]

    assert d["tc"] == 0.80
    assert d["tc_by_scope"] == {
        "enterprise::enterprise": 0.80,
        "bc::bc": 0.50,
    }


def test_tp_by_scope_absent_when_no_group1_rows():
    rows = [
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template",
             governance_role_a="Generic", governance_role_b="Template",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.9", n_files_a="1", n_files_b="3"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    assert d["tp"] is None
    assert d["tp_by_scope"] == {}


def test_tp_bc_pooled_when_both_sides_same_bc_no_enterprise_pair():
    """The core gap scenario: no fully-unscoped pair exists at all for this
    domain (tp/cp stay None), but two bc-scoped-both-sides pairs exist and are
    captured under the "bc::bc" key instead of being discarded."""
    rows = [
        _row(segment_id_a="imperial|Template|BC_1", segment_id_b="imperial|Project|BC_1",
             governance_role_a="Template", governance_role_b="Project",
             business_center_label_a="BC_1", business_center_label_b="BC_1",
             comparison_type="template_to_project", domain="materials",
             all_containment_a_in_b_mean="0.18", n_files_a="2", n_files_b="6"),
        _row(segment_id_a="imperial|Template|BC_2", segment_id_b="imperial|Project|BC_2",
             governance_role_a="Template", governance_role_b="Project",
             business_center_label_a="BC_2", business_center_label_b="BC_2",
             comparison_type="template_to_project", domain="materials",
             all_containment_a_in_b_mean="0.995", n_files_a="2", n_files_b="6"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["materials"]

    assert d["tp"] is None
    assert d["tp_by_scope"] == {"bc::bc": (0.18 + 0.995) / 2}
    assert d["tp_by_scope_spread"] == {"bc::bc": (0.18, 0.995)}


def test_group1_scope_pair_uses_both_sides_unlike_group2():
    """Unlike Group 2 (only the target/b side is classified, since the
    reference/a side is always gated to enterprise-only), Group 1 has no
    fixed-role side -- a client-scoped 'a' paired with a bc-scoped 'b' must
    produce a "client::bc" key, not silently collapse to one side's label."""
    rows = [
        _row(segment_id_a="imperial|Template|Kaiser", segment_id_b="imperial|Container|BC_1",
             governance_role_a="Template", governance_role_b="Container",
             client_label_a="Kaiser", business_center_label_b="BC_1",
             comparison_type="template_to_container", domain="ghost_domain",
             all_containment_a_in_b_mean="0.33", n_files_a="2", n_files_b="3"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["ghost_domain"]
    assert d["tc_by_scope"] == {"client::bc": 0.33}
    assert d["tc"] is None


def test_scope_pair_separator_does_not_collide_across_multi_dimension_labels():
    """_target_scope_label() already joins multi-dimension side labels with "_"
    (e.g. "bc_discipline", "client_bc"), so joining scope_a/scope_b with the
    SAME character would be ambiguous: ("client", "bc_discipline") and
    ("client_bc", "discipline") both concatenate to the literal string
    "client_bc_discipline" under a bare "_" join -- confirmed to actually occur
    in real cross_segment_summary.csv data (Container/Project rows scoped by
    client+business-center on one side, discipline-only on the other, vs.
    client-only on one side, business-center+discipline on the other). The "::"
    separator (a token _target_scope_label() never produces on its own) keeps
    these two semantically distinct pairs in separate buckets."""
    rows = [
        _row(segment_id_a="imperial|Container|Kaiser|BC_1", segment_id_b="imperial|Project|architectural",
             governance_role_a="Container", governance_role_b="Project",
             client_label_a="Kaiser", business_center_label_a="BC_1",
             discipline_label_b="architectural",
             comparison_type="container_to_project", domain="collision_domain",
             all_containment_a_in_b_mean="0.20", n_files_a="2", n_files_b="3"),
        _row(segment_id_a="imperial|Container|Kaiser", segment_id_b="imperial|Project|BC_1|architectural",
             governance_role_a="Container", governance_role_b="Project",
             client_label_a="Kaiser",
             business_center_label_b="BC_1", discipline_label_b="architectural",
             comparison_type="container_to_project", domain="collision_domain",
             all_containment_a_in_b_mean="0.90", n_files_a="2", n_files_b="3"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["collision_domain"]
    assert d["cp_by_scope"] == {
        "client_bc::discipline": 0.20,
        "client::bc_discipline": 0.90,
    }


def test_domain_with_no_group1_rows_at_all_absent():
    """A domain with no template_to_container/container_to_project/
    template_to_project/parent_sibling_roles rows at all must not appear in
    cascade purely because of this fix (mirrors Group 2's equivalent test)."""
    rows = [
        _row(segment_id_a="imperial|Project", segment_id_b="imperial|Project",
             governance_role_a="Project", governance_role_b="Project",
             comparison_type="within_project", domain="only_wp_domain",
             all_jaccard_mean="0.5", n_files_a="10"),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    d = cascade["only_wp_domain"]
    assert d["tc_by_scope"] == {}
    assert d["cp_by_scope"] == {}
    assert d["tp_by_scope"] == {}


# ---------------------------------------------------------------------------
# assign_tier(): new fallback tier
# ---------------------------------------------------------------------------

def _bc_pooled_dict(tp_by_scope=None, cp_by_scope=None, tc=None, cp=None, tp=None):
    """Minimal dict shaped like a build_cascade() per-domain result, for
    assign_tier()/_has_group1_bc_pooled_evidence() unit tests that don't need
    a full build_cascade() round trip."""
    return {
        "tc": tc, "cp": cp, "tp": tp,
        "tp_by_scope": tp_by_scope or {},
        "cp_by_scope": cp_by_scope or {},
        "wp_p10": None, "wp_p90": None, "wp_all": None,
    }


def test_has_group1_bc_pooled_evidence_true_for_tp_bc_bc():
    d = _bc_pooled_dict(tp_by_scope={"bc::bc": 0.5})
    assert _has_group1_bc_pooled_evidence(d) is True


def test_has_group1_bc_pooled_evidence_true_for_cp_bc_bc():
    d = _bc_pooled_dict(cp_by_scope={"bc::bc": 0.5})
    assert _has_group1_bc_pooled_evidence(d) is True


def test_has_group1_bc_pooled_evidence_false_when_only_other_scope_pairs():
    """A "client::bc" or "client::client" bucket is not "same-bc-both-sides" --
    only the literal "bc::bc" key counts."""
    d = _bc_pooled_dict(tp_by_scope={"client::bc": 0.5, "client::client": 0.9})
    assert _has_group1_bc_pooled_evidence(d) is False


def test_has_group1_bc_pooled_evidence_false_when_empty():
    d = _bc_pooled_dict()
    assert _has_group1_bc_pooled_evidence(d) is False


def test_assign_tier_returns_bc_evidence_tier_when_primary_none_and_bc_pooled_present():
    d = _bc_pooled_dict(tp_by_scope={"bc::bc": 0.6})
    assert assign_tier(d) == TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE


def test_assign_tier_still_returns_insufficient_when_no_bc_pooled_evidence():
    """The genuinely-nothing case (e.g. materials/dimension_types_spot_coordinate
    per the investigation) must still fall to plain TIER_INSUFFICIENT."""
    d = _bc_pooled_dict()
    assert assign_tier(d) == TIER_INSUFFICIENT


def test_assign_tier_enterprise_primary_path_unaffected_by_bc_data():
    """When an enterprise tp/cp DOES exist, assign_tier() must use it exactly
    as before -- bc-pooled data present alongside it must not change the
    outcome. (Uses tp=0.95, cp=0.95 -> TIER_STRONG_BASELINE-eligible path,
    with no state exceptions.)"""
    d = _bc_pooled_dict(tc=0.90, cp=0.95, tp=0.95, tp_by_scope={"enterprise::enterprise": 0.95, "bc::bc": 0.10})
    tier = assign_tier(d)
    assert tier != TIER_INSUFFICIENT
    assert tier != TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE


def test_tier_order_places_new_tier_between_high_fragmentation_and_insufficient():
    assert TIER_ORDER[TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE] == TIER_ORDER[TIER_INSUFFICIENT] - 1


# ---------------------------------------------------------------------------
# detect_anomalies(): Group 1 bc-pooled intra-bucket divergence note
# ---------------------------------------------------------------------------

def _group1_rows(bc1_val, bc2_val, ctype="template_to_project", domain="materials"):
    return [
        _row(segment_id_a="imperial|Template|BC_1", segment_id_b="imperial|Project|BC_1",
             governance_role_a="Template", governance_role_b="Project",
             business_center_label_a="BC_1", business_center_label_b="BC_1",
             comparison_type=ctype, domain=domain,
             all_containment_a_in_b_mean=str(bc1_val), n_files_a="2", n_files_b="6"),
        _row(segment_id_a="imperial|Template|BC_2", segment_id_b="imperial|Project|BC_2",
             governance_role_a="Template", governance_role_b="Project",
             business_center_label_a="BC_2", business_center_label_b="BC_2",
             comparison_type=ctype, domain=domain,
             all_containment_a_in_b_mean=str(bc2_val), n_files_a="2", n_files_b="6"),
    ]


def test_detect_anomalies_flags_material_bc_divergence():
    rows = _group1_rows(0.18, 0.995)
    normalise_summary_schema(rows)
    d = build_cascade(rows)["materials"]
    notes = detect_anomalies("materials", d, None)
    matches = [n for n in notes if "bc::bc" in n and "scope level is not a single converged reading" in n]
    assert matches, f"expected a bc-pooled divergence note, got: {notes}"
    assert "18%" in matches[0] and "100%" in matches[0]


def test_detect_anomalies_silent_when_bc_pairs_agree():
    rows = _group1_rows(0.85, 0.90)
    normalise_summary_schema(rows)
    d = build_cascade(rows)["materials"]
    notes = detect_anomalies("materials", d, None)
    assert not any("scope level is not a single converged reading" in n for n in notes)


def test_detect_anomalies_silent_when_only_one_bc_pair():
    """A single bc pair has no spread to compare (len(v) > 1 gate on
    *_by_scope_spread) -- must not fire the note."""
    rows = [
        _row(segment_id_a="imperial|Template|BC_1", segment_id_b="imperial|Project|BC_1",
             governance_role_a="Template", governance_role_b="Project",
             business_center_label_a="BC_1", business_center_label_b="BC_1",
             comparison_type="template_to_project", domain="materials",
             all_containment_a_in_b_mean="0.5", n_files_a="2", n_files_b="6"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["materials"]
    assert d["tp_by_scope_spread"] == {}
    notes = detect_anomalies("materials", d, None)
    assert not any("scope level is not a single converged reading" in n for n in notes)


# ---------------------------------------------------------------------------
# render_group1_scope_section()
# ---------------------------------------------------------------------------

def test_render_group1_scope_section_includes_bc_bc_row():
    rows = _group1_rows(0.18, 0.995)
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    section = render_group1_scope_section(cascade)
    assert "## Group 1 Propagation by Scope" in section
    assert "| Materials | bc::bc | — | 0.588 | — |" in section  # mean of 0.18/0.995


def test_render_group1_scope_section_empty_when_only_enterprise_pair():
    rows = [
        _row(segment_id_a="imperial|Template", segment_id_b="imperial|Container",
             governance_role_a="Template", governance_role_b="Container",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.8", n_files_a="3", n_files_b="5"),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    section = render_group1_scope_section(cascade)
    assert "| Arrowheads | enterprise::enterprise |" in section


def test_render_group1_scope_section_empty_when_no_group1_data():
    rows = [
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template",
             governance_role_a="Generic", governance_role_b="Template",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.9", n_files_a="1", n_files_b="3"),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    assert render_group1_scope_section(cascade) == ""
