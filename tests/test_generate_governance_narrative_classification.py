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
    _DEFAULT_CLIENT_SECTOR_PATH,
    _client_onboarding_profile,
    _disc_label,
    build_cascade,
    build_client_summary,
    EXCLUDED_FROM_SCORING,
    load_client_sectors,
    normalise_summary_schema,
    read_csv,
    render_client_section,
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
            used_union_jaccard="0.5", n_files_a="3", n_files_b="3",
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
        {"client_label": "ClientAlpha", "sector": "healthcare"},
        {"client_label": "Intel", "sector": "semiconductor"},
    ]
    assert load_client_sectors(rows) == {"ClientAlpha": "healthcare", "Intel": "semiconductor"}


def _client_fixture(client_names):
    summary_rows = []
    pooled_rows = []
    for c in client_names:
        summary_rows.append(_summary_row(
            segment_id_a=f"imperial|Project|{c}", segment_id_b=f"imperial|Project|{c}",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a=c, client_label_b=c,
            comparison_type="within_project", domain="arrowheads",
            used_union_jaccard="0.6", n_files_a="12", n_files_b="12",
        ))
        pooled_rows.append(_pooled_row(
            segment_id=f"imperial|Project|{c}", governance_role="Project",
            client_label=c, unit_system="imperial", domain="arrowheads",
            pool_scope="parent_sibling", n_files_focal="12",
        ))
    normalise_summary_schema(summary_rows)
    return summary_rows, pooled_rows


def test_known_healthcare_client_is_flagged_healthcare():
    summary_rows, pooled_rows = _client_fixture(["ClientAlpha"])
    sector_map = {"ClientAlpha": "healthcare"}
    rows = build_client_summary(summary_rows, pooled_rows, sector_map)
    clientalpha = next(r for r in rows if r["client"] == "ClientAlpha")
    assert clientalpha["is_healthcare"] is True
    assert clientalpha["tier"] != "Non-comparable (different sector)"


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
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.5", n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|Intel",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="Intel",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.9", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare", "Intel": "semiconductor"}
    cascade = build_cascade(rows, sector_map)
    # Only the ClientAlpha/ClientBeta (both healthcare) pair should count toward xc.
    assert cascade["arrowheads"]["xc"] == 0.5


# ---------------------------------------------------------------------------
# PR #350 review round 4: default --client-sector path + unknown-vs-non-healthcare
# ---------------------------------------------------------------------------

def test_default_client_sector_path_exists_and_loads():
    """Existing invocations that don't pass --client-sector must still pick up
    the shipped classification, or cross-client convergence (xc) silently goes
    to None for every domain -- not just the sector/tier fields."""
    assert _DEFAULT_CLIENT_SECTOR_PATH.exists()
    sector_map = load_client_sectors(read_csv(_DEFAULT_CLIENT_SECTOR_PATH))
    assert sector_map.get("ClientBeta") == "healthcare"
    assert sector_map.get("ClientEpsilon") not in ("healthcare", None, "")


def test_unclassified_client_not_treated_as_confirmed_non_healthcare():
    """is_healthcare=False alone can't distinguish 'known different sector' from
    'we don't know' -- only a client with a KNOWN non-healthcare sector should
    get the different-sector operating implication / render note."""
    profile_unknown = _client_onboarding_profile(
        {"sector": "unknown", "is_healthcare": False, "xc_mean": 0.4, "wp_mean": 0.6, "n_files": 20}
    )
    profile_known_non_healthcare = _client_onboarding_profile(
        {"sector": "semiconductor", "is_healthcare": False, "xc_mean": 0.4, "wp_mean": 0.6, "n_files": 20}
    )
    assert "healthcare baseline assumptions" not in profile_unknown["operating_implication"]
    assert "healthcare baseline assumptions" in profile_known_non_healthcare["operating_implication"]

    client_rows = [
        {"client": "Unknown Co", "sector": "unknown", "is_healthcare": False, "tier": "Insufficient Data",
         "xc_mean": None, "wp_mean": None, "n_files": 5, "confidence_note": "", "strongest": [], "weakest": []},
        {"client": "Intel", "sector": "semiconductor", "is_healthcare": False,
         "tier": "Non-comparable (different sector)",
         "xc_mean": None, "wp_mean": None, "n_files": 5, "confidence_note": "", "strongest": [], "weakest": []},
    ]
    md = render_client_section(client_rows)
    unknown_section = md.split("### Unknown Co")[1].split("### Intel")[0]
    intel_section = md.split("### Intel")[1]
    assert "_Non-healthcare sector" not in unknown_section
    assert "_Non-healthcare sector" in intel_section


# ---------------------------------------------------------------------------
# Post-merge review round: within-client sibling comparisons; non-Project
# within_project fallback rows
# ---------------------------------------------------------------------------

def test_within_client_sibling_projects_excluded_from_cross_client_xc():
    """discover_sibling_segments() groups purely by (parent_segment_id,
    governance_role, unit_system), so two differently-scoped Project segments
    under the SAME client (e.g. ClientAlpha's discipline-scoped siblings) can pair as
    sibling_projects with client_label_a == client_label_b -- a within-client
    comparison, not cross-client convergence, and must not count toward xc."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha|architectural", segment_id_b="imperial|Project|ClientAlpha|electrical",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientAlpha",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.95", n_files_a="5", n_files_b="5",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.5", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    cascade = build_cascade(rows, sector_map)
    assert cascade["arrowheads"]["xc"] == 0.5


# ---------------------------------------------------------------------------
# cross_client comparison type (governance pipeline gap fix)
# ---------------------------------------------------------------------------

def test_cascade_cross_client_requires_both_healthcare_like_sibling_projects():
    """cross_client's contribution to xc is gated to both-healthcare pairs,
    the same as sibling_projects's existing gate -- xc is a healthcare-cohort
    metric (see build_cascade()'s own docstring and the client-tier
    "Non-comparable (different sector)" logic downstream); a pair with an
    unclassified/non-healthcare side must not feed it, regardless of source
    comparison_type. discover_cross_client() itself is unaffected -- it still
    computes and emits every client pair into cross_segment_summary.csv
    regardless of sector; this is purely a rollup-consumer filter."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|Intel", segment_id_b="imperial|Project|Acme",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="Intel", client_label_b="Acme",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.7", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    # No sector_map at all -- neither client classified as healthcare.
    cascade = build_cascade(rows)
    assert "arrowheads" not in cascade


def test_cascade_cross_client_feeds_xc_when_both_sides_healthcare():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.7", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    cascade = build_cascade(rows, sector_map)
    assert cascade["arrowheads"]["xc"] == 0.7


def test_cascade_cross_client_and_sibling_projects_both_feed_xc():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.5", n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|Acme",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="Acme",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.9", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare", "Acme": "healthcare"}
    cascade = build_cascade(rows, sector_map)
    # sibling_projects (ClientAlpha/ClientBeta, both healthcare) = 0.5, cross_client
    # (ClientAlpha/Acme, both healthcare) = 0.9 -- both land in the same xc bucket.
    assert cascade["arrowheads"]["xc"] == (0.5 + 0.9) / 2


def test_cascade_cross_client_excludes_pair_with_one_non_healthcare_side():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.5", n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|Intel",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="Intel",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.9", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    # Intel is unclassified (not in sector_map) -- ClientAlpha/Intel must be excluded.
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    cascade = build_cascade(rows, sector_map)
    assert cascade["arrowheads"]["xc"] == 0.5


def test_build_client_summary_xc_mean_uses_cross_client_rows():
    """governance_client_summary.csv's cross_client_similarity_mean must be
    populated from cross_client rows even when no sibling_projects rows exist
    for these clients at all."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.6", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    clientbeta = next(r for r in client_rows if r["client"] == "ClientBeta")
    assert clientalpha["xc_mean"] == 0.6
    assert clientbeta["xc_mean"] == 0.6


def test_build_client_summary_xc_mean_uses_client_label_not_segment_id_shape():
    """Regression for a Codex review finding on PR #370: xc_by_client/
    xc_dom_by_client used to positionally parse segment_id ("len(pa) == 3")
    to find the client name, which only holds for the unit|role|client-shaped
    IDs build_segment_manifest.py happens to emit -- discover_cross_client()
    places no such constraint on segment_id shape. A cross_client row with
    non-standard segment_ids must still populate xc_mean via client_label_a/b."""
    rows = [
        _summary_row(
            segment_id_a="p_clientalpha", segment_id_b="p_clientbeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.6", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    clientbeta = next(r for r in client_rows if r["client"] == "ClientBeta")
    assert clientalpha["xc_mean"] == 0.6
    assert clientbeta["xc_mean"] == 0.6


def test_build_client_summary_backfills_n_files_for_cross_client_only_clients():
    """Regression for a second Codex review finding on PR #370: a client can
    now be discovered purely from a cross_client row (no pooled/within_project/
    sibling_projects rows at all). client_files must backfill from the
    cross_client row's own n_files_a/b, or such a client falsely reports
    n_project_files=0 / a low-confidence note despite the row carrying real
    counts."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.6", n_files_a="42", n_files_b="17",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    clientbeta = next(r for r in client_rows if r["client"] == "ClientBeta")
    assert clientalpha["n_files"] == 42
    assert clientbeta["n_files"] == 17


def test_within_client_cross_client_like_pair_excluded_from_xc_mean():
    """ca != cb must still exclude a within-client pair from xc_by_client/
    xc_dom_by_client even though the segment_id-shape check that used to
    incidentally enforce this (a 4-part discipline-scoped segment_id) is gone
    now that client_label_a/b is read directly."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha|architectural", segment_id_b="imperial|Project|ClientAlpha|electrical",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientAlpha",
            comparison_type="sibling_projects", domain="arrowheads",
            used_union_jaccard="0.95", n_files_a="5", n_files_b="5",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.5", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    # Only the ClientAlpha/ClientBeta cross_client pair (0.5) should count -- the
    # within-client 0.95 sibling_projects pair must not blend in.
    assert clientalpha["xc_mean"] == 0.5


def test_build_client_summary_excludes_confirmed_non_healthcare_partner_from_xc_mean():
    """Regression for a fourth Codex review finding on PR #370: cross_client
    being default-on and pairing every client regardless of sector means a
    healthcare client's xc_mean/tier could be driven by a comparison against a
    client whose OWN row is separately (and correctly) marked
    Non-comparable (different sector) -- xc_by_client/xc_dom_by_client had no
    defense against this for either source comparison_type. A pair with a
    CONFIRMED (not merely unclassified) non-healthcare side must not feed
    either client's xc_mean."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|Intel",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="Intel",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.9", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "Intel": "semiconductor"}
    client_rows = build_client_summary(rows, [], sector_map)
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    intel = next(r for r in client_rows if r["client"] == "Intel")
    assert clientalpha["xc_mean"] is None
    assert intel["tier"] == "Non-comparable (different sector)"


def test_build_client_summary_unclassified_partner_still_feeds_xc_mean():
    """An UNCLASSIFIED client (absent from sector_map, not a KNOWN different
    sector) must still count -- only a CONFIRMED non-healthcare sector is
    excluded, matching this function's own tier definition of "comparable"
    (sector in ("unknown", "healthcare"))."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|NewClient",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="NewClient",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.8", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare"}  # NewClient absent -- unclassified
    client_rows = build_client_summary(rows, [], sector_map)
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    assert clientalpha["xc_mean"] == 0.8


def test_build_client_summary_excludes_policy_excluded_domain_from_xc_mean():
    """Regression for a seventh Codex review finding on PR #370: xc_by_client
    had no EXCLUDED_FROM_SCORING gate at all, unlike xc_dom_by_client right
    below it and build_cascade()'s own per-domain xc accumulation -- a
    cross_client row for a domain the governance policy excludes from scoring
    (e.g. view_templates_renderings_drafting) could still classify a client
    as highly aligned, disagreeing with the rest of the scoring policy.
    cross_client being default-on and pairing every client for every domain
    makes this routinely reachable."""
    excluded_domain = next(iter(EXCLUDED_FROM_SCORING))
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain=excluded_domain,
            used_union_jaccard="0.95", n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            used_union_jaccard="0.3", n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    client_rows = build_client_summary(rows, [], sector_map)
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    # Only the non-excluded arrowheads row (0.3) should count -- the excluded
    # domain's 0.95 must not pull xc_mean up.
    assert clientalpha["xc_mean"] == 0.3


def test_non_project_within_project_rows_excluded_from_client_summary():
    """discover_within_project() can emit within_project rows for any non-skip/
    non-registration segment, not just Project-role ones (e.g. a client-scoped
    Template library). The Client Analysis section is specifically about the
    client's PROJECT portfolio -- a Template-only client must not appear with
    project file counts/coherence sourced from Template data."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Template|GhostClient", segment_id_b="imperial|Template|GhostClient",
            governance_role_a="Template", governance_role_b="Template",
            client_label_a="GhostClient", client_label_b="GhostClient",
            comparison_type="within_project", domain="arrowheads",
            used_union_jaccard="0.99", n_files_a="50", n_files_b="50",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    assert "GhostClient" not in {r["client"] for r in client_rows}


# ---------------------------------------------------------------------------
# PR B1: xc/wp/xc_dom/disc_domain_wp union-metric adoption (v/vu read sites)
# ---------------------------------------------------------------------------
#
# _recommended_primary_view() in compare_cross_segment.py declares used-view
# primary ("active practice") for cross_client/sibling_projects/within_project
# (Project role) rows -- xc_mean/wp_mean/d["xc"] must therefore read
# used_union_jaccard, with all_union_jaccard exposed as the secondary "_all"/
# "xc_all" context value. Stale all_pairwise_jaccard_mean/all_jaccard_mean
# values are deliberately set alongside the union fields in these tests and
# must be ignored -- proving the read sites were actually swapped, not just
# aliased through to the old pairwise family under a new name.

def test_xc_mean_reads_used_union_jaccard_not_pairwise_mean():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            # Stale pairwise fields present with a DIFFERENT value than the
            # union fields -- if the read site still resolved to these, the
            # assertion below on xc_mean would fail.
            all_pairwise_jaccard_mean="0.10", all_jaccard_mean="0.10",
            all_union_jaccard="0.40", used_union_jaccard="0.75",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    client_rows = build_client_summary(rows, [], sector_map)
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    assert clientalpha["xc_mean"] == 0.75
    assert clientalpha["xc_mean_all"] == 0.40
    assert clientalpha["xc_mean"] != clientalpha["xc_mean_all"]


def test_wp_mean_reads_used_union_jaccard_and_exposes_all_view_companion():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientAlpha",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha",
            comparison_type="within_project", domain="arrowheads",
            all_pairwise_jaccard_mean="0.10",
            all_union_jaccard="0.30", used_union_jaccard="0.65",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    assert clientalpha["wp_mean"] == 0.65
    assert clientalpha["wp_mean_all"] == 0.30
    assert clientalpha["wp_mean"] != clientalpha["wp_mean_all"]


def test_cascade_xc_reads_used_union_jaccard_with_distinct_all_view_companion():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            all_pairwise_jaccard_mean="0.10",
            all_union_jaccard="0.20", used_union_jaccard="0.85",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    cascade = build_cascade(rows, sector_map=sector_map)
    d = cascade["arrowheads"]
    assert d["xc"] == 0.85
    assert d["xc_all"] == 0.20
    assert d["xc"] != d["xc_all"]


def test_cascade_wp_all_and_wp_used_stay_a_true_all_used_pair_not_flipped():
    """Unlike xc (no prior used companion), wp_all/wp_used were already a
    genuine all-view/used-view pair before this PR -- the metric family swaps
    to union, but which side is "all" and which is "used" must not flip, or
    the passive_indicator (all - used) delta downstream would silently invert."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientAlpha",
            governance_role_a="Project", governance_role_b="Project",
            comparison_type="within_project", domain="arrowheads",
            all_union_jaccard="0.90", used_union_jaccard="0.20",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows, sector_map={})
    d = cascade["arrowheads"]
    assert d["wp_all"] == 0.90
    assert d["wp_used"] == 0.20


# ---------------------------------------------------------------------------
# PR #376 review fix: within_project rows never carry all_union_*/used_union_*
# from the real producer (compare_cross_segment.py's dedicated within-project
# branch returns before the normal path's _union_similarity() call) -- these
# tests use the REAL producer row shape (pairwise fields populated, union
# fields blank/absent) rather than setting union fields directly, to prove
# the fallback actually engages and isn't just a no-op against a synthetic
# fixture that happens to always supply both.
# ---------------------------------------------------------------------------

def test_cascade_wp_falls_back_to_pairwise_when_union_blank_real_producer_shape():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientAlpha",
            governance_role_a="Project", governance_role_b="Project",
            comparison_type="within_project", domain="arrowheads",
            all_pairwise_jaccard_mean="0.55", used_pairwise_jaccard_mean="0.35",
            # all_union_jaccard/used_union_jaccard deliberately left blank --
            # this is the real shape a within_project row has today.
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    cascade = build_cascade(rows, sector_map={})
    d = cascade["arrowheads"]
    assert d["wp_all"] == 0.55
    assert d["wp_used"] == 0.35


def test_wp_by_client_falls_back_to_pairwise_when_union_blank_real_producer_shape():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientAlpha",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha",
            comparison_type="within_project", domain="arrowheads",
            all_pairwise_jaccard_mean="0.60", used_pairwise_jaccard_mean="0.40",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    client_rows = build_client_summary(rows, [], {})
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    assert clientalpha["wp_mean"] == 0.40
    assert clientalpha["wp_mean_all"] == 0.60


def test_disc_domain_wp_falls_back_to_pairwise_when_union_blank_real_producer_shape():
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|arch", segment_id_b="imperial|Project|arch",
            governance_role_a="Project", governance_role_b="Project",
            discipline_label_a="architectural",
            comparison_type="within_project", domain="arrowheads",
            all_pairwise_jaccard_mean="0.70", used_pairwise_jaccard_mean="0.45",
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    section = render_discipline_section({}, rows)
    assert "45" in section
    assert "70" in section


def test_xc_does_not_fall_back_to_pairwise_when_union_blank():
    """The fallback is within_project-only. cross_client/sibling_projects
    always populate union fields when they have real data (they hit the
    normal path's else branch, unlike within_project) -- xc must never
    silently fall back to the stale pairwise family, or a genuine producer
    regression on those types would be masked instead of surfaced as blank."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientBeta",
            governance_role_a="Project", governance_role_b="Project",
            client_label_a="ClientAlpha", client_label_b="ClientBeta",
            comparison_type="cross_client", domain="arrowheads",
            all_pairwise_jaccard_mean="0.60",
            # all_union_jaccard/used_union_jaccard deliberately blank.
            n_files_a="10", n_files_b="10",
        ),
    ]
    normalise_summary_schema(rows)
    sector_map = {"ClientAlpha": "healthcare", "ClientBeta": "healthcare"}
    client_rows = build_client_summary(rows, [], sector_map)
    clientalpha = next(r for r in client_rows if r["client"] == "ClientAlpha")
    assert clientalpha["xc_mean"] is None


def test_disc_domain_wp_keeps_all_view_primary_for_non_project_within_project_rows():
    """PR #376 P2 review finding: discover_within_project() emits within_project
    rows for discipline-scoped Template/Container/Generic standards segments
    too, not just Project ones (unlike wp_by_client in build_client_summary(),
    which gates to governance_role_a == "Project" and drops non-Project rows
    entirely, this section must keep showing them). used-view is not
    meaningful/primary outside Project targets per _recommended_primary_view()
    -- a Template segment with no used-view membership at all (used blank,
    only all-view populated) must still render its real all-view coherence,
    not silently drop to "no data" because the read switched to a used-view
    field that was never meaningful for this role in the first place."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Template|arch", segment_id_b="imperial|Template|arch",
            governance_role_a="Template", governance_role_b="Template",
            discipline_label_a="architectural",
            comparison_type="within_project", domain="arrowheads",
            all_union_jaccard="0.88",
            # used_union_jaccard/used_pairwise_jaccard_mean deliberately blank
            # -- this Template standards segment has no used-view membership.
            n_files_a="4", n_files_b="4",
        ),
    ]
    normalise_summary_schema(rows)
    section = render_discipline_section({}, rows)
    assert "88" in section


def test_disc_domain_wp_labels_non_project_discipline_as_all_view_not_active_practice():
    """PR #376 review, second P2 finding: the rendered sentence must not claim
    'used-view, active practice' for a discipline whose domain_means value is
    actually all-view (a Template/Container/Generic-only discipline) -- that
    would misstate configured standards evidence as active usage."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Template|arch", segment_id_b="imperial|Template|arch",
            governance_role_a="Template", governance_role_b="Template",
            discipline_label_a="architectural",
            comparison_type="within_project", domain="arrowheads",
            all_union_jaccard="0.77",
            n_files_a="4", n_files_b="4",
        ),
    ]
    normalise_summary_schema(rows)
    section = render_discipline_section({}, rows)
    assert "all-view, configured standards" in section
    assert "used-view, active practice" not in section


def test_disc_domain_wp_labels_mixed_project_and_non_project_discipline():
    """A discipline fed by both a Project (used-view) row and a Template
    (all-view) row must get the neutral mixed label, not silently claim
    'used-view, active practice' for the whole aggregate."""
    rows = [
        _summary_row(
            segment_id_a="imperial|Project|ClientAlpha", segment_id_b="imperial|Project|ClientAlpha",
            governance_role_a="Project", governance_role_b="Project",
            discipline_label_a="architectural",
            comparison_type="within_project", domain="arrowheads",
            used_union_jaccard="0.65", all_union_jaccard="0.80",
            n_files_a="10", n_files_b="10",
        ),
        _summary_row(
            segment_id_a="imperial|Template|arch", segment_id_b="imperial|Template|arch",
            governance_role_a="Template", governance_role_b="Template",
            discipline_label_a="architectural",
            comparison_type="within_project", domain="fill_patterns_drafting",
            all_union_jaccard="0.90",
            n_files_a="4", n_files_b="4",
        ),
    ]
    normalise_summary_schema(rows)
    section = render_discipline_section({}, rows)
    assert "mixed used-view (Project rows) / all-view (standards rows)" in section
