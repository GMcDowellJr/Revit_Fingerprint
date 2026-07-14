"""Tests for gt/gc/gp per-target-scope-level breakdown (Option C) in
tools/generate_governance_narrative.py's build_cascade().

Background: compare_cross_segment.py intentionally emits generic_to_template/
_container/_project rows for client-/bc-/discipline-scoped targets, not only the
single broadest ("enterprise") one -- real baseline-propagation evidence that was
previously discarded by gating both sides of the comparison to the broadest
population. gt/gc/gp themselves stay the "enterprise" slice only (Option A,
decided in PR #350 review); every other target scope level is now captured in
gt_by_scope/gc_by_scope/gp_by_scope instead.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    _target_scope_label,
    build_cascade,
    detect_anomalies,
    normalise_summary_schema,
    render_generic_baseline_scope_section,
)


def _row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def test_target_scope_label_enterprise():
    row = _row(governance_role_b="Template", segment_id_b="imperial|Template")
    assert _target_scope_label(row, "b") == "enterprise"


def test_target_scope_label_single_dimensions():
    client_row = _row(governance_role_b="Template", client_label_b="Kaiser",
                       segment_id_b="imperial|Template|Kaiser")
    assert _target_scope_label(client_row, "b") == "client"

    bc_row = _row(governance_role_b="Template", business_center_label_b="BC_2270",
                  segment_id_b="imperial|Template|BC_2270")
    assert _target_scope_label(bc_row, "b") == "bc"

    disc_row = _row(governance_role_b="Template", discipline_label_b="architectural",
                     segment_id_b="imperial|Template|architectural")
    assert _target_scope_label(disc_row, "b") == "discipline"


def test_target_scope_label_combined_dimensions():
    row = _row(governance_role_b="Template", client_label_b="Kaiser",
               discipline_label_b="architectural",
               segment_id_b="imperial|Template|Kaiser|architectural")
    assert _target_scope_label(row, "b") == "client_discipline"


def test_target_scope_label_collection_only_is_other_scoped():
    """collection_label is not yet a SUMMARY_FIELDS column (residual B6 gap) -- a
    segment scoped only by collection must not be silently mislabeled
    "enterprise" just because client/bc/discipline all read blank."""
    row = _row(governance_role_b="Template", segment_id_b="imperial|Template|collection:Shared")
    assert _target_scope_label(row, "b") == "other_scoped"


def test_gt_enterprise_slice_unchanged_by_scoped_rows():
    """gt (the rendered headline number) must equal exactly the enterprise-scope
    mean, regardless of how many other-scoped rows exist alongside it -- Option A
    is preserved even though Option C now captures the rest."""
    rows = [
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template",
             governance_role_a="Generic", governance_role_b="Template",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.90", n_files_a="1", n_files_b="3"),
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template|Kaiser",
             governance_role_a="Generic", governance_role_b="Template",
             client_label_b="Kaiser",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.40", n_files_a="1", n_files_b="4"),
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template|architectural",
             governance_role_a="Generic", governance_role_b="Template",
             discipline_label_b="architectural",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.60", n_files_a="1", n_files_b="5"),
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template|BC_2270",
             governance_role_a="Generic", governance_role_b="Template",
             business_center_label_b="BC_2270",
             comparison_type="generic_to_template", domain="arrowheads",
             all_containment_a_in_b_mean="0.55", n_files_a="1", n_files_b="6"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]

    assert d["gt"] == 0.90
    assert d["gt_by_scope"] == {
        "enterprise": 0.90,
        "client": 0.40,
        "discipline": 0.60,
        "bc": 0.55,
    }


def test_gt_by_scope_absent_when_no_generic_to_template_rows():
    rows = [
        _row(segment_id_a="imperial|Template", segment_id_b="imperial|Container",
             governance_role_a="Template", governance_role_b="Container",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.8", n_files_a="3", n_files_b="5"),
    ]
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    assert d["gt"] is None
    assert d["gt_by_scope"] == {}


def test_generic_side_still_gated_to_unscoped_reference():
    """The Generic (reference) side must still be the one canonical enterprise-
    wide Generic population -- a scoped Generic reference is not a valid
    baseline source and must not contribute to any scope bucket. With no other
    signal for this domain, it must not appear in the result at all."""
    rows = [
        _row(segment_id_a="imperial|Generic|architectural", segment_id_b="imperial|Template",
             governance_role_a="Generic", governance_role_b="Template",
             discipline_label_a="architectural",
             comparison_type="generic_to_template", domain="ghost_domain",
             all_containment_a_in_b_mean="0.99", n_files_a="1", n_files_b="3"),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    assert "ghost_domain" not in cascade


# ---------------------------------------------------------------------------
# Rendering / anomaly-detection for the Option C breakdown
# ---------------------------------------------------------------------------

def _generic_to_template_rows(enterprise_val, client_val, domain="arrowheads"):
    rows = [
        _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template",
             governance_role_a="Generic", governance_role_b="Template",
             comparison_type="generic_to_template", domain=domain,
             all_containment_a_in_b_mean=str(enterprise_val), n_files_a="1", n_files_b="3"),
    ]
    if client_val is not None:
        rows.append(
            _row(segment_id_a="imperial|Generic", segment_id_b="imperial|Template|Kaiser",
                 governance_role_a="Generic", governance_role_b="Template",
                 client_label_b="Kaiser",
                 comparison_type="generic_to_template", domain=domain,
                 all_containment_a_in_b_mean=str(client_val), n_files_a="1", n_files_b="4")
        )
    return rows


def test_detect_anomalies_flags_material_scope_divergence():
    rows = _generic_to_template_rows(enterprise_val=0.90, client_val=0.40)
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    notes = detect_anomalies("arrowheads", d, None)
    matches = [n for n in notes if "Generic→Template propagation is weaker into scoped targets" in n]
    assert matches, f"expected a scope-divergence note, got: {notes}"
    assert "90%" in matches[0] and "40%" in matches[0] and "client=40%" in matches[0]


def test_detect_anomalies_silent_when_no_material_divergence():
    """A small gap (< 0.25) between enterprise and scoped must not fire the note."""
    rows = _generic_to_template_rows(enterprise_val=0.90, client_val=0.80)
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    notes = detect_anomalies("arrowheads", d, None)
    assert not any("propagation is" in n and "scoped targets" in n for n in notes)


def test_detect_anomalies_silent_when_only_enterprise_scope_exists():
    """No scoped rows at all (only "enterprise") must not fire the divergence note --
    there's nothing to compare against."""
    rows = _generic_to_template_rows(enterprise_val=0.90, client_val=None)
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    notes = detect_anomalies("arrowheads", d, None)
    assert not any("scoped targets" in n for n in notes)


def test_render_generic_baseline_scope_section_includes_all_scopes():
    rows = _generic_to_template_rows(enterprise_val=0.90, client_val=0.40)
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    section = render_generic_baseline_scope_section(cascade)
    assert "## Generic Baseline Propagation by Scope" in section
    assert "| Arrowheads | enterprise | 0.900 | — | — |" in section
    assert "| Arrowheads | client | 0.400 | — | — |" in section


def test_render_generic_baseline_scope_section_empty_when_no_data():
    rows = [
        _row(segment_id_a="imperial|Template", segment_id_b="imperial|Container",
             governance_role_a="Template", governance_role_b="Container",
             comparison_type="template_to_container", domain="arrowheads",
             all_containment_a_in_b_mean="0.8", n_files_a="3", n_files_b="5"),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    assert render_generic_baseline_scope_section(cascade) == ""
