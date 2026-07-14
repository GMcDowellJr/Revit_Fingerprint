"""Tests for discipline-vocabulary and client-sector classification in
tools/generate_governance_narrative.py.

See docs/governance_narrative_scope_gap_audit.md sections C7/C8.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS, POOLED_FIELDS  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    _disc_label,
    build_cascade,
    build_client_summary,
    load_client_sectors,
    normalise_summary_schema,
    render_discipline_section,
)


def _summary_row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _pooled_row(**overrides):
    r = {f: "" for f in POOLED_FIELDS}
    r.update(overrides)
    return r


# ---------------------------------------------------------------------------
# Fix A: discipline vocabulary from data
# ---------------------------------------------------------------------------

def test_disc_label_uses_override_for_known_discipline():
    assert _disc_label("mechanical_plumbing") == "Mechanical/Plumbing"


def test_disc_label_humanizes_unknown_discipline():
    assert _disc_label("medical_equipment") == "Medical Equipment"
    assert _disc_label("lighting") == "Lighting"
    assert _disc_label("security") == "Security"


def test_render_discipline_section_includes_disciplines_beyond_disc_labels():
    """lighting/medical_equipment/security are real disciplines not in DISC_LABELS'
    7 hardcoded entries -- render_discipline_section must not silently skip them."""
    rows = []
    for disc in ("lighting", "medical_equipment", "security", "architectural"):
        rows.append(_summary_row(
            segment_id_a=f"imperial|Project|{disc}", segment_id_b=f"imperial|Project|{disc}",
            governance_role_a="Project", governance_role_b="Project",
            discipline_label_a=disc, discipline_label_b=disc,
            comparison_type="within_project", domain="arrowheads",
            all_jaccard_mean="0.5", n_files_a="3", n_files_b="3",
        ))
    normalise_summary_schema(rows)
    cascade = build_cascade(rows)
    md = render_discipline_section(cascade, rows)

    assert "### Lighting" in md
    assert "### Medical Equipment" in md
    assert "### Security" in md
    assert "### Architectural" in md


# ---------------------------------------------------------------------------
# Fix B: client-sector classification as external data
# ---------------------------------------------------------------------------

def test_load_client_sectors_empty_when_absent():
    assert load_client_sectors(None) == {}
    assert load_client_sectors([]) == {}


def test_load_client_sectors_builds_map():
    rows = [
        {"client_label": "Kaiser", "sector": "healthcare"},
        {"client_label": "Intel", "sector": "semiconductor"},
    ]
    assert load_client_sectors(rows) == {"Kaiser": "healthcare", "Intel": "semiconductor"}


def _client_fixture(client_names):
    summary_rows = []
    pooled_rows = []
    for c in client_names:
        summary_rows.append(_summary_row(
            segment_id_a=f"imperial|Project|{c}", segment_id_b=f"imperial|Project|{c}",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a=c, client_label_b=c,
            comparison_type="within_project", domain="arrowheads",
            all_jaccard_mean="0.6", n_files_a="12", n_files_b="12",
        ))
        pooled_rows.append(_pooled_row(
            segment_id=f"imperial|Project|{c}", governance_role="Project",
            client_label=c, unit_system="imperial", domain="arrowheads",
            pool_scope="parent_sibling", n_files_focal="12",
        ))
    normalise_summary_schema(summary_rows)
    return summary_rows, pooled_rows


def test_known_healthcare_client_is_flagged_healthcare():
    summary_rows, pooled_rows = _client_fixture(["Kaiser"])
    sector_map = {"Kaiser": "healthcare"}
    rows = build_client_summary(summary_rows, pooled_rows, sector_map)
    kaiser = next(r for r in rows if r["client"] == "Kaiser")
    assert kaiser["is_healthcare"] is True
    assert kaiser["tier"] != "Non-comparable (different sector)"


def test_known_non_healthcare_sector_gets_non_comparable_tier():
    summary_rows, pooled_rows = _client_fixture(["Intel"])
    sector_map = {"Intel": "semiconductor"}
    rows = build_client_summary(summary_rows, pooled_rows, sector_map)
    intel = next(r for r in rows if r["client"] == "Intel")
    assert intel["is_healthcare"] is False
    assert intel["tier"] == "Non-comparable (different sector)"


def test_unclassified_client_falls_through_to_normal_tiering():
    """A client absent from sector_map (or sector_map entirely absent) must NOT
    get the non-comparable tier -- only a client with a KNOWN non-healthcare
    sector does."""
    summary_rows, pooled_rows = _client_fixture(["Unclassified"])
    rows_with_empty_map = build_client_summary(summary_rows, pooled_rows, {})
    rows_with_no_map = build_client_summary(summary_rows, pooled_rows, None)
    for rows in (rows_with_empty_map, rows_with_no_map):
        r = next(row for row in rows if row["client"] == "Unclassified")
        assert r["is_healthcare"] is False
        assert r["tier"] != "Non-comparable (different sector)"


def test_cascade_cross_client_jaccard_uses_sector_map():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|Kaiser", segment_id_b="imperial|Project|Sutter",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="Kaiser", client_label_b="Sutter",
            comparison_type="sibling_projects", domain="arrowheads",
            all_jaccard_mean="0.5", n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Project|Kaiser", segment_id_b="imperial|Project|Intel",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="Kaiser", client_label_b="Intel",
            comparison_type="sibling_projects", domain="arrowheads",
            all_jaccard_mean="0.9", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"Kaiser": "healthcare", "Sutter": "healthcare", "Intel": "semiconductor"}
    cascade = build_cascade(rows, sector_map)
    # Only the Kaiser/Sutter (both healthcare) pair should count toward xc.
    assert cascade["arrowheads"]["xc"] == 0.5
