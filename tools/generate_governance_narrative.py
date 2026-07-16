"""
generate_governance_narrative.py

Deterministic governance narrative renderer for the Revit Fingerprint System.
Produces governance_narrative_context.md from pipeline CSV outputs.
No LLM in the loop — all text is assembled from templates filled by computed values.

Required inputs (all produced by compare_cross_segment.py / bundle pipeline):
  --summary      cross_segment_summary.csv
  --pooled       cross_segment_pooled.csv

Optional inputs (enrich state, delta, and pattern sections when available):
  --governance-state-summary cross_segment_governance_state_summary.csv
  --governance-states        cross_segment_governance_states.csv
  --delta                    cross_segment_delta.csv
  --run-registry             run_registry.csv          (for corpus metadata)
  --file-meta                file_metadata.csv         (for file counts by role/client/discipline)
  --client-sector            client_sector.csv         (client_label,sector -- classifies
                                                          cross-client convergence and
                                                          non-comparable-sector tiering;
                                                          absent = every client unclassified)
  --union-inventory         cross_segment_union_inventory.csv
  --reuse-distribution      pattern_reuse_distribution.csv
  --matrix-manifest         matrix_output_manifest.csv

Not yet consumed directly; see docs/governance_generator_cross_compare_coverage.md
for recommended integration points:
  comparison_registry.csv, cross_segment_file_pairs.csv,
  pattern_reuse_summary_by_domain.csv, pattern_reuse_summary_by_client.csv,
  project_*_matrix.csv, and project_fragmentation_diagnostic.csv

Output:
  --out          governance_narrative_context.md  (default)

Usage:
  python generate_governance_narrative.py \\
      --summary cross_segment_summary.csv \\
      --pooled  cross_segment_pooled.csv \\
      [--delta  cross_segment_delta.csv] \\
      [--file-meta file_metadata.csv] \\
      [--client-sector policies/client_sector.csv] \\
      --out governance_narrative_context.md
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

# compare_cross_segment.py lives in this same directory and is side-effect-free on
# import (its pipeline logic is gated behind `if __name__ == "__main__":`), so its
# GOVERNANCE_STATE_DIRECTED_TYPES is imported directly rather than hand-copied --
# see _DIRECTED_GOVERNANCE_TYPES below for why a hand-copy drifted before.
from compare_cross_segment import GOVERNANCE_STATE_DIRECTED_TYPES

# governance_evidence_package.py is a sibling module (same side-effect-free-on-
# import convention) providing the package manifest/health/evidence-map layer
# added around this generator's existing deterministic outputs. See
# docs/governance_evidence_package.md.
from governance_evidence_package import (
    GENERATOR_IDENTITY,
    GENERATOR_ROLE,
    PACKAGE_SCHEMA_VERSION,
    EVIDENCE_MAP_SCHEMA_VERSION,
    FINDINGS_SCHEMA_VERSION,
    AUTHORITY_CONTROLLED_INTERPRETATION,
    AUTHORITY_CONVENIENCE_SUMMARY,
    FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
    FINDING_FIDELITY_EXACT,
    FINDING_STATUS_SUPPORTED,
    FINDING_STATUS_QUESTION_NOT_CLAIM,
    build_evidence_map,
    build_findings_document,
    build_package_health,
    build_package_manifest,
    comparison_type_coverage as _comparison_type_coverage,
    write_json,
)


# ── helpers ────────────────────────────────────────────────────────────────────

def pf(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pct(v: Optional[float], decimals: int = 0) -> str:
    if v is None:
        return "—"
    return f"{v * 100:.{decimals}f}%"


def fmt(v: Optional[float], decimals: int = 3) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"


def _warn_unrecognized_comparison_types(seen: set, known: set, context: str) -> None:
    """Warn once, to stderr, for any comparison_type not accounted for by name.

    Shared by build_cascade() and build_governance_state_summary() so an
    unrecognized/drifted comparison_type is never silently swallowed in either
    place -- see docs/governance_narrative_scope_gap_audit.md A1/A3.
    """
    unrecognized = seen - known
    if unrecognized:
        print(
            f"[warn] {context}: unrecognized comparison_type value(s) not in any "
            f"known group, excluded: {sorted(unrecognized)}",
            file=sys.stderr,
        )


def read_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


DOMAIN_LABELS = {
    "arrowheads": "Arrowheads",
    "ceiling_types": "Ceiling Types",
    "dimension_types_angular": "Dimension Types — Angular",
    "dimension_types_diameter": "Dimension Types — Diameter",
    "dimension_types_linear": "Dimension Types — Linear",
    "dimension_types_radial": "Dimension Types — Radial",
    "dimension_types_spot_coordinate": "Dimension Types — Spot Coordinate",
    "dimension_types_spot_elevation": "Dimension Types — Spot Elevation",
    "dimension_types_spot_slope": "Dimension Types — Spot Slope",
    "fill_patterns_drafting": "Fill Patterns — Drafting",
    "fill_patterns_model": "Fill Patterns — Model",
    "floor_types": "Floor Types",
    "line_patterns": "Line Patterns",
    "line_styles": "Line Styles",
    "loaded_family_types": "Loaded Family Types",
    "materials": "Materials",
    "object_styles_analytical": "Object Styles — Analytical",
    "object_styles_annotation": "Object Styles — Annotation",
    "object_styles_model": "Object Styles — Model",
    "phase_filters": "Phase Filters",
    "phases": "Phases",
    "roof_types": "Roof Types",
    "text_types": "Text Types",
    "units": "Units",
    "view_category_overrides_annotation": "View Category Overrides — Annotation",
    "view_category_overrides_model": "View Category Overrides — Model",
    "view_filter_applications_view_templates": "View Filter Applications",
    "view_filter_definitions": "View Filter Definitions",
    "view_templates_ceiling_plans": "View Templates — Ceiling Plans",
    "view_templates_elevations_sections_detail": "View Templates — Elevations/Sections",
    "view_templates_floor_structural_area_plans": "View Templates — Floor/Structural Plans",
    "view_templates_renderings_drafting": "View Templates — Renderings/Drafting",
    "view_templates_schedules": "View Templates — Schedules",
}

# Domains excluded from aggregate governance scoring (structurally anomalous)
EXCLUDED_FROM_SCORING = {"view_templates_renderings_drafting"}

DISC_LABELS = {
    "architectural": "Architectural",
    "electrical": "Electrical",
    "mechanical_plumbing": "Mechanical/Plumbing",
    "structural": "Structural",
    "fire_protection": "Fire Protection",
    "low_voltage": "Low Voltage",
    "water": "Water",
}


def _disc_label(disc: str) -> str:
    """Display name for a discipline_label value. DISC_LABELS is an optional
    override for known disciplines' display casing/punctuation (e.g.
    "Mechanical/Plumbing" instead of a plain title-case render) -- it is NOT the
    source of which disciplines exist. A discipline outside DISC_LABELS still
    renders humanely (e.g. "medical_equipment" -> "Medical Equipment") rather
    than crashing or being silently dropped from any disc-keyed section. See
    docs/governance_narrative_scope_gap_audit.md C7.
    """
    return DISC_LABELS.get(disc, disc.replace("_", " ").title())

# Domains where passive inheritance is most likely to inflate all-view scores.
# These domains are often fully inherited from templates but rarely customised.
PASSIVE_INHERITANCE_RISK_DOMAINS = {
    "arrowheads", "fill_patterns_drafting", "fill_patterns_model",
    "line_patterns", "dimension_types_diameter", "dimension_types_radial",
    "dimension_types_spot_coordinate", "dimension_types_spot_elevation",
    "dimension_types_spot_slope", "object_styles_analytical",
}


def detect_bundle_schema(rows: list) -> str:
    """
    Returns which bundle annotation schema is present:
      'dual'   -- all_n_shared_bundle_both AND used_n_shared_bundle_both present
      'single' -- only n_shared_bundle_both (pre-dual-view schema)
      'none'   -- no bundle columns present
    """
    if not rows:
        return "none"
    sample = rows[0]
    if "all_n_shared_bundle_both" in sample and "used_n_shared_bundle_both" in sample:
        return "dual"
    if "n_shared_bundle_both" in sample:
        return "single"
    return "none"



# ── schema normalisation ─────────────────────────────────────────────────────────────────

# Maps canonical renderer column names to actual CSV columns for both
# pre-dual-view schema (bare names) and dual-view schema (all_/used_ prefixes).
_SUMMARY_COL_ALIASES: dict = {}


def normalise_summary_schema(rows: list) -> None:
    """Inspect first row and build _SUMMARY_COL_ALIASES. Called once at startup."""
    global _SUMMARY_COL_ALIASES
    if not rows:
        return
    cols = set(rows[0].keys())

    def alias(canonical: str, dual_name: str, legacy_name: str) -> None:
        if dual_name in cols:
            _SUMMARY_COL_ALIASES[canonical] = dual_name
        elif legacy_name in cols:
            _SUMMARY_COL_ALIASES[canonical] = legacy_name

    # Jaccard
    alias("jaccard_mean",            "all_jaccard_mean",            "jaccard_mean")
    alias("jaccard_p10",             "all_jaccard_p10",             "jaccard_p10")
    alias("jaccard_p90",             "all_jaccard_p90",             "jaccard_p90")
    alias("used_jaccard_mean",       "used_jaccard_mean",           "jaccard_mean")
    alias("used_jaccard_p10",        "used_jaccard_p10",            "jaccard_p10")
    alias("used_jaccard_p90",        "used_jaccard_p90",            "jaccard_p90")
    # Containment
    alias("containment_a_in_b_mean",      "all_containment_a_in_b_mean",      "containment_a_in_b_mean")
    alias("containment_b_in_a_mean",      "all_containment_b_in_a_mean",      "containment_b_in_a_mean")
    alias("used_containment_a_in_b_mean", "used_containment_a_in_b_mean",     "containment_a_in_b_mean")
    alias("used_containment_b_in_a_mean", "used_containment_b_in_a_mean",     "containment_b_in_a_mean")
    # Shared counts
    alias("n_shared_join_hash",      "n_shared_join_hash",      "n_shared_join_hash")
    alias("used_n_shared_join_hash", "used_n_shared_join_hash", "n_shared_join_hash")
    # Bundle columns
    alias("all_n_shared_bundle_both",   "all_n_shared_bundle_both",   "n_shared_bundle_both")
    alias("all_n_shared_bundle_a_only", "all_n_shared_bundle_a_only", "n_shared_bundle_a_only")
    alias("all_n_shared_bundle_b_only", "all_n_shared_bundle_b_only", "n_shared_bundle_b_only")
    alias("used_n_shared_bundle_both",  "used_n_shared_bundle_both",  "n_shared_bundle_both")
    # has_bundles
    alias("has_bundles_a", "all_has_bundles_a", "has_bundles_a")
    alias("has_bundles_b", "all_has_bundles_b", "has_bundles_b")
    # Signal ambiguity
    alias("signal_spread",        "signal_spread",        "signal_spread")
    alias("score_ambiguity_band", "score_ambiguity_band", "score_ambiguity_band")


def _col(row: dict, canonical: str) -> str:
    """Read a summary row column using canonical renderer name."""
    actual = _SUMMARY_COL_ALIASES.get(canonical, canonical)
    return row.get(actual, "")


def used_view_falls_back_to_legacy() -> bool:
    """True when canonical used-view columns resolved to legacy all-view names."""
    legacy_pairs = {
        "used_jaccard_mean": "jaccard_mean",
        "used_jaccard_p10": "jaccard_p10",
        "used_jaccard_p90": "jaccard_p90",
        "used_containment_a_in_b_mean": "containment_a_in_b_mean",
        "used_containment_b_in_a_mean": "containment_b_in_a_mean",
        "used_n_shared_join_hash": "n_shared_join_hash",
        "used_n_shared_bundle_both": "n_shared_bundle_both",
    }
    return any(_SUMMARY_COL_ALIASES.get(k) == v for k, v in legacy_pairs.items())


# ── data loading ───────────────────────────────────────────────────────────────

def _is_unscoped_segment(row: dict, suffix: str) -> bool:
    """True when a segment is the broadest (client/discipline-unscoped) population
    for its governance role — the condition the old is_generic() tried to detect
    via "segment_id has exactly 2 pipe-separated parts" (unit_system + role only).

    A blank governance_role is NOT this condition — it is a scope rollup with no
    role filter at all (e.g. a business-center-wide rollup like "imperial|BC_2014"),
    which also happens to produce a 2-part segment_id and was therefore
    misclassified as "generic" by the old part-count check. See
    docs/governance_narrative_scope_gap_audit.md B5.

    business_center_label / collection_label are not yet columns on SUMMARY_FIELDS
    (see B6), so a row with role set and client_label/discipline_label both blank
    could still be a business-center- or collection-scoped standard (e.g.
    "imperial|Template|BC_1234" or "imperial|Template|collection:Shared") that
    these three columns alone can't reveal. Once client_label/discipline_label are
    confirmed blank, any EXTRA NON-EMPTY pipe-separated part in segment_id can only
    have come from business_center_label/collection_label (per
    build_segment_manifest.py's fixed field order) and must be rejected — but an
    extra part that is itself EMPTY is not hidden data: build_segment_manifest.py's
    _subset_to_id() emits a literal empty token for a client_label/discipline_label
    dimension that IS selected as part of the segment's key but happens to have a
    blank value (e.g. "imperial|Template||Shared" for a blank client_label
    alongside a real business_center_label "Shared" -- see the comment in
    _subset_to_id() itself), as distinct from a dimension simply absent from the
    key (which contributes no token at all, e.g. "imperial|Template"). A segment
    like "imperial|Generic|" (trailing blank client token, nothing else) is
    therefore still genuinely unscoped and must not be rejected just because it
    has more than 2 raw pipe-separated parts. This is a structural completeness
    check on segment_id, not the positional-parsing anti-pattern removed elsewhere
    in this file: it never reads a VALUE out of segment_id, it only confirms any
    extra part is blank rather than a hidden scope token.
    """
    role = row.get(f"governance_role_{suffix}", "")
    client = row.get(f"client_label_{suffix}", "")
    disc = row.get(f"discipline_label_{suffix}", "")
    if not role or client or disc:
        return False
    seg_id = row.get(f"segment_id_{suffix}", "")
    if not seg_id:
        return True
    return all(p == "" for p in seg_id.split("|")[2:])


def _target_scope_label(row: dict, suffix: str) -> str:
    """Classify a generic_to_*'s TARGET side (Template/Container/Project) into a
    scope-level bucket for gt/gc/gp's per-scope breakdown, using real columns
    (client_label, business_center_label -- now on SUMMARY_FIELDS per B6,
    discipline_label) rather than segment_id parsing.

    "enterprise" reuses _is_unscoped_segment()'s own definition of the broadest
    population. Otherwise the bucket names every populated dimension
    (client/bc/discipline), e.g. "client", "client_discipline". collection_label
    is still not a SUMMARY_FIELDS column (residual B6 gap) -- a segment scoped
    only by collection would have all three known dimensions blank yet still
    fail _is_unscoped_segment()'s segment_id structural check, landing in
    "other_scoped" rather than being silently mislabeled "enterprise".
    """
    if _is_unscoped_segment(row, suffix):
        return "enterprise"
    parts = []
    if row.get(f"client_label_{suffix}", ""):
        parts.append("client")
    if row.get(f"business_center_label_{suffix}", ""):
        parts.append("bc")
    if row.get(f"discipline_label_{suffix}", ""):
        parts.append("discipline")
    return "_".join(parts) if parts else "other_scoped"


# Maps a _target_scope_label() shape to the field(s) that make it up, so
# _group1_scope_pair() can verify VALUE equality (not just shape equality)
# when both sides land on the same shape. "enterprise" and "other_scoped"
# are intentionally absent: "enterprise" means every one of these fields is
# blank on both sides (nothing to compare), and "other_scoped" is the
# collection-only-scoped catch-all -- collection_label is still not a
# SUMMARY_FIELDS column (residual B6 gap noted in _target_scope_label()'s own
# docstring), so its value can't be verified with the columns available today.
_SCOPE_DIMENSION_FIELDS = {
    "client": ("client_label",),
    "bc": ("business_center_label",),
    "discipline": ("discipline_label",),
    "client_bc": ("client_label", "business_center_label"),
    "client_discipline": ("client_label", "discipline_label"),
    "bc_discipline": ("business_center_label", "discipline_label"),
    "client_bc_discipline": ("client_label", "business_center_label", "discipline_label"),
}


def _group1_scope_pair(row: dict) -> tuple[str, str, str]:
    """Classify a Group 1 row's two sides into (scope_a, scope_b, scope_pair_key).

    Reuses _target_scope_label() for each side's SHAPE (which dimensions are
    populated) -- unlike Group 2, BOTH sides matter here, since neither side
    of a Group 1 pair is gated to a fixed role population. But
    _target_scope_label() alone only tells you the shape, not the VALUE: two
    segments that are both e.g. "bc"-shaped (business_center_label populated,
    client/discipline blank) could have DIFFERENT business_center_label
    values -- discover_within_segment() in compare_cross_segment.py pairs
    same-parent, same-unit Template/Container/Project segments without
    checking that scope label values match, so a BC_1-scoped segment paired
    against a BC_2-scoped segment is a real, producer-side-reachable shape,
    not just a hypothetical. Silently bucketing that under "bc::bc" would
    corrupt _has_group1_bc_pooled_evidence()'s "same business center" check
    (compares mismatched-value rows as if they were one converged reading)
    and mislabel the same disagreement as "business-center" evidence in
    render_group1_scope_section()/detect_anomalies() when it's actually a
    cross-value comparison.

    When both sides share an identical shape AND every field making up that
    shape has an equal value on both sides, the pair is genuine same-value
    evidence and gets the normal f"{scope_a}::{scope_b}" key (e.g. "bc::bc").
    When the shapes match but any field's value differs, the pair is
    captured under a distinct f"{scope_a}!cross::{scope_b}!cross" key instead
    -- never discarded (this file's fail-soft-in-narrative posture), but
    never conflated with same-value pooled evidence either. "!cross" cannot
    collide with any of _target_scope_label()'s own outputs or the plain
    "::"-joined keys (verified: it never appears in any of the 9 possible
    _target_scope_label() return values).
    """
    scope_a = _target_scope_label(row, "a")
    scope_b = _target_scope_label(row, "b")
    fields = _SCOPE_DIMENSION_FIELDS.get(scope_a) if scope_a == scope_b else None
    if fields and any(row.get(f"{f}_a", "") != row.get(f"{f}_b", "") for f in fields):
        return scope_a, scope_b, f"{scope_a}!cross::{scope_b}!cross"
    return scope_a, scope_b, f"{scope_a}::{scope_b}"


# Default location for the optional client_sector.csv, resolved relative to this
# script's own directory (tools/) rather than the CWD -- so existing invocations
# that don't pass --client-sector still pick up the shipped classification and
# keep today's healthcare cross-client convergence signal, without requiring
# every caller to learn a new flag. Passing --client-sector explicitly (a real
# path or a nonexistent one) always overrides this default.
_DEFAULT_CLIENT_SECTOR_PATH = Path(__file__).resolve().parent.parent / "policies" / "client_sector.csv"


def load_client_sectors(client_sector_rows: Optional[list[dict]]) -> dict:
    """Build a {client_label: sector} map from an optional client_sector.csv
    (--client-sector). Sector membership is a real business fact that cannot be
    derived from the fingerprint pipeline's own data -- nothing in
    segment_manifest.csv/file_metadata.csv encodes "sector" -- so it lives in an
    editable data file instead of a Python literal. See
    docs/governance_narrative_scope_gap_audit.md C7.

    Absent input (file not supplied) returns an empty map: every client is then
    "unknown" (not healthcare, not non-comparable) and falls through to normal
    cross-client alignment tiering -- there is no special-cased client name.
    """
    if not client_sector_rows:
        return {}
    sector_map = {}
    for row in client_sector_rows:
        client = _pick(row, "client_label")
        sector = _pick(row, "sector").strip().lower()
        if client and sector:
            sector_map[client] = sector
    return sector_map


def load_corpus_counts(
    summary_rows: list[dict],
    file_meta_rows: Optional[list[dict]],
) -> dict:
    """Return counts of files by role, and disciplines/clients present."""
    counts = {"Template": 0, "Container": 0, "Project": 0, "total": 0}
    disciplines = set()
    clients = set()

    if file_meta_rows:
        for r in file_meta_rows:
            role = r.get("governance_role", "")
            if role in counts:
                counts[role] += 1
            d = r.get("discipline_label", "")
            if d:
                disciplines.add(d)
            c = r.get("client_label", "")
            if c:
                clients.add(c)
        counts["total"] = counts["Template"] + counts["Container"] + counts["Project"]
    else:
        # Infer from within_project rows
        seen = {}
        for r in summary_rows:
            if r["comparison_type"] == "within_project" and r["segment_id_a"] == r["segment_id_b"]:
                seg = r["segment_id_a"]
                role = seg.split("|")[1] if "|" in seg else ""
                if role in counts:
                    n = int(r["n_files_a"]) if r["n_files_a"] else 0
                    # take largest (most inclusive) count per role
                    if role not in seen or n > seen[role]:
                        seen[role] = n
        for role, n in seen.items():
            if role in counts:
                counts[role] = n
        counts["total"] = counts["Template"] + counts["Container"] + counts["Project"]
        # Disciplines and clients from segment IDs
        for r in summary_rows:
            d = _pick(r, "discipline_label_a", "discipline_label_b")
            if d:
                disciplines.add(d)
            c = _pick(r, "client_label_a", "client_label_b")
            if c:
                clients.add(c)

    counts["disciplines"] = disciplines
    counts["clients"] = clients
    return counts


# ── build_cascade comparison_type coverage ──────────────────────────────────────
# The full comparison_type vocabulary compare_cross_segment.py can emit splits into
# four groups that need different treatment (see docs/governance_narrative_scope_gap_audit.md
# A1). Every comparison_type value build_cascade can see must appear in exactly one
# of these — the coverage check at the end of the main loop below warns on anything
# that doesn't, so a future producer addition is never silently invisible again.

# Group 1 — already handled by the explicit branches in the main loop; directed
# cross-role cascade stages (Template<->Container<->Project) plus the two
# self/peer-comparison shapes (sibling_projects -> xc, within_project -> wp_*).
CASCADE_GROUP1_TYPES = {
    "template_to_container", "container_to_project", "template_to_project",
    "parent_sibling_roles", "sibling_projects", "within_project",
}

# Group 2 — one level up the cascade: Generic/Generic-Host (out-of-box Revit stock
# content) into Template/Container/Project. This is the literal top rung of the
# "Governance Cascade" diagram already printed in render_header() ("Generic /
# Enterprise Baseline down-arrow [generic -> template/container/project
# containment]") — an existing promise in the narrative's own output that was
# never implemented before this pass.
#
# Scope decision (PR #350 review, revised -- Option C): compare_cross_segment.py
# intentionally emits generic_to_template/_container/_project rows for client-/
# discipline-/bc-scoped targets too, not only the single broadest one -- those
# scoped rows are real baseline-propagation evidence. gt/gc/gp keep the SAME
# single-broadest-pair semantics as tc/cp/tp (the GENERIC/reference side must
# still pass _is_unscoped_segment -- one canonical enterprise-wide Generic
# population -- and gt/gc/gp themselves are populated only from target rows whose
# OWN scope is "enterprise"), avoiding the blend-distinct-scope-grains anti-
# pattern this audit already fixed elsewhere (A2's pool_scope filter, A3's
# governance-state blending). But the target (Template/Container/Project) side is
# no longer gated to broadest-only: every other target scope level is captured,
# not discarded, in gt_by_scope/gc_by_scope/gp_by_scope (see _target_scope_label()
# and build_cascade()'s docstring) -- Option C from the original PR #350 review
# discussion, implemented as its own follow-up once the tradeoff was accepted.
# collection_label is still not a SUMMARY_FIELDS column (B6 residual gap after the
# business_center_label addition); a collection-only-scoped target lands in the
# "other_scoped" bucket rather than being silently mislabeled "enterprise".
CASCADE_GROUP2_TYPES = {
    "generic_to_template", "generic_to_container", "generic_to_project",
}

# Group 3 — a different axis entirely (scope level: enterprise/bc/client standards
# vs. Project), not one more cascade stage. Captured into the `cascade` dict under
# new keys (ep/bp/eb/ec) using the same containment-extraction pattern as Group 1/2,
# but deliberately NOT rendered, tiered, or anomaly-detected in this pass — that is
# a future business-center-section design decision, not an extension of this
# bug-fix prompt.
CASCADE_GROUP3_TYPES = {
    "enterprise_to_project", "bc_to_project", "enterprise_to_bc", "enterprise_to_client",
}

# Group 4 — known comparison types intentionally excluded from cascade, one reason
# each (verified against compare_cross_segment.py's actual discovery functions, not
# guessed):
CASCADE_GROUP4_EXCLUDED_TYPES = {
    "sibling_templates": (
        "Same-role peer-to-peer comparison (Template vs Template), not a cross-role "
        "directed cascade measurement. build_cascade only extracts a peer-similarity "
        "signal from one role today (sibling_projects -> xc, restricted to a specific "
        "client-pair filter) and does not generalise that pattern to other roles. "
        "Whether/how Template-vs-Template consistency should be surfaced is a design "
        "decision, not resolved by this pass."
    ),
    "sibling_containers": (
        "Same defect/reason class as sibling_templates — same-role peer comparison, "
        "no directed-cascade analog implemented for this role either."
    ),
    "sibling_generic": (
        "Same-role peer comparison among Generic/Generic-Host segments. "
        "_comparison_role_semantics() in compare_cross_segment.py already documents "
        "that used-view is not meaningful for these pairs (all-view is primary); no "
        "cascade-shaped (cross-role containment) signal applies to peer-vs-peer "
        "Generic comparisons either."
    ),
    "sibling_segments": (
        "Fallback bucket (discover_sibling_segments()'s ctype default) for peer "
        "segments whose governance_role doesn't match template/container/project/"
        "generic. By construction these aren't role-typed the way the cascade's "
        "role buckets are, so there's no directed-cascade slot to route them into."
    ),
    "governance_chain": (
        "Reserved vocabulary token in compare_cross_segment.py's DIRECTED_TYPES — "
        "verified (grep) that no discovery function ever actually emits the literal "
        "string \"governance_chain\" as a comparison_type; discover_governance_chain() "
        "itself emits concrete generic_to_*/template_to_*/container_to_project types, "
        "not this string. Nothing to feed into cascade under this name; kept here "
        "only so the coverage check below doesn't flag it as unrecognized."
    ),
}

# Group 1/2 signal keys -- a domain with data in at least one of these has something
# to tier/render. A domain whose ONLY data is Group 3 (ep/bp/eb/ec) has no cascade-
# stage signal at all; it must stay in the `cascade` dict (captured, per Group 3's
# contract) but must NOT reach render_domain_tiers()/the domain summary CSV, which
# only know how to tier/render Group 1/2 fields and would otherwise show it as a
# spurious "Insufficient Evidence" row with every visible column blank.
_CASCADE_RENDERABLE_SIGNAL_KEYS = ("tc", "cp", "tp", "xc", "wp_all", "tw", "gt", "gc", "gp")


def _has_renderable_cascade_signal(d: dict) -> bool:
    if any(d.get(k) is not None for k in _CASCADE_RENDERABLE_SIGNAL_KEYS):
        return True
    # tc_by_scope/cp_by_scope/tp_by_scope are always present as (possibly
    # empty) dicts, never None, so they can't reuse the "is not None" check
    # above -- that would trivially return True for every domain, including
    # Group-3-only ones this function exists to exclude. A domain whose ONLY
    # Group 1 signal is scoped (e.g. bc::bc) evidence -- no enterprise
    # tc/cp/tp and no other Group 1/2 signal -- must still be renderable, or
    # its TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE classification (see
    # assign_tier()/_has_group1_bc_pooled_evidence()) would be computed but
    # never shown in render_domain_tiers()/the domain summary CSV.
    return any(d.get(k) for k in ("tc_by_scope", "cp_by_scope", "tp_by_scope"))


def build_cascade(summary_rows: list[dict], sector_map: Optional[dict] = None) -> dict:
    """
    Returns domain-keyed dict with cascade scores from generic segments:
      tc: template->container containment_a_in_b_mean
      cp: container->project containment_a_in_b_mean
      tp: template->project containment_a_in_b_mean
      xc: cross-client Jaccard mean (clients whose sector is "healthcare" per
        sector_map — see load_client_sectors())
      wp_all: overall within-project Jaccard mean
      wp_disc: {disc: mean_jaccard}
      tw: within-template Jaccard
      wp_p10: within-project Jaccard p10 (generic segment, all roles)
      wp_p90: within-project Jaccard p90 (generic segment, all roles)
      gt/gc/gp: generic->template/container/project containment_a_in_b_mean, the
        "enterprise" (target-unscoped) slice only (Group 2 — one level up the
        cascade from tc/cp/tp; see CASCADE_GROUP2_TYPES)
      gt_by_scope/gc_by_scope/gp_by_scope: {scope_label: mean_containment} for
        EVERY target scope level compare_cross_segment.py emits (client/bc/
        discipline and combinations thereof, plus "enterprise" itself) -- see
        _target_scope_label(). Mirrors wp_disc's per-discipline breakdown pattern.
      tc_by_scope/cp_by_scope/tp_by_scope: {scope_pair: mean_containment} for
        EVERY (a-side scope, b-side scope) pair compare_cross_segment.py emits for
        Group 1 (template_to_container/container_to_project/template_to_project),
        keyed as f"{scope_a}::{scope_b}" (e.g. "enterprise::enterprise", "bc::bc") --
        see _target_scope_label(). Unlike Group 2 (where only the target/b side is
        classified, since the reference/a side is always gated to enterprise-only),
        BOTH sides matter here, since neither side of a Group 1 pair is gated to a
        fixed role population. tc/cp/tp themselves are UNCHANGED: still populated
        only from the "enterprise::enterprise" pair (both sides pass
        _is_unscoped_segment), matching today's behavior exactly. This mirrors
        gt_by_scope/gc_by_scope/gp_by_scope's Option C precedent for the Group 1
        gap documented in docs/governance_narrative_group1_scope_gap_investigation.md.
      tc_by_scope_spread/cp_by_scope_spread/tp_by_scope_spread: {scope_pair:
        (min, max)} for any scope_pair backed by >=2 rows -- lets detect_anomalies
        flag a scope_pair whose pooled mean hides sharp disagreement between the
        individual rows pooled into it, instead of only ever reporting the mean.
        The varying dimension depends on which scope_pair fired: "bc::bc" pools
        distinct business centers, but e.g. "client_bc::client_discipline" pools
        rows that share the same client/bc and vary only by discipline -- see
        detect_anomalies()'s note text, which is deliberately scope-neutral
        rather than always saying "business-center."
      ep/bp/eb/ec: enterprise->project / bc->project / enterprise->bc / enterprise->client
        containment_a_in_b_mean (Group 3 — scope-level fan-out, captured but not yet
        rendered/tiered/anomaly-detected; see CASCADE_GROUP3_TYPES)
    """
    tc = defaultdict(list)
    cp = defaultdict(list)
    tp = defaultdict(list)
    xc = defaultdict(list)
    wp_all = defaultdict(list)
    wp_disc = defaultdict(lambda: defaultdict(list))
    tw = defaultdict(list)
    # p10/p90 from generic (all-role) within_project rows — most representative spread signal
    wp_p10 = {}  # domain -> float
    wp_p90 = {}
    # used-view cascade scores (dual-view schema only; fall back to all-view when absent)
    tc_used = defaultdict(list)
    cp_used = defaultdict(list)
    tp_used = defaultdict(list)
    wp_used = defaultdict(list)   # within-project used-view Jaccard
    wp_used_p10 = {}
    wp_used_p90 = {}

    # Group 1 bc-pooled fallback -- per-(a-scope, b-scope)-pair breakdown mirroring
    # Group 2's Option C (gt_by_scope/etc.), see docs/governance_narrative_group1_scope_gap_investigation.md.
    # tc/cp/tp themselves stay gated to the "enterprise::enterprise" pair only --
    # unchanged from today.
    tc_by_scope = defaultdict(lambda: defaultdict(list))
    cp_by_scope = defaultdict(lambda: defaultdict(list))
    tp_by_scope = defaultdict(lambda: defaultdict(list))
    tc_used_by_scope = defaultdict(lambda: defaultdict(list))
    cp_used_by_scope = defaultdict(lambda: defaultdict(list))
    tp_used_by_scope = defaultdict(lambda: defaultdict(list))

    # Group 2 — generic->template/container/project containment (all-view + used-view).
    # gt/gc/gp remain the "enterprise" scope-level slice only (Option A -- one clean
    # number, matching tc/cp/tp; see the Scope decision comment on CASCADE_GROUP2_TYPES
    # above), populated as a subset of the *_by_scope breakdowns below rather than a
    # separately-gated accumulation, so the two can never drift apart.
    gt = defaultdict(list)
    gc = defaultdict(list)
    gp = defaultdict(list)
    gt_used = defaultdict(list)
    gc_used = defaultdict(list)
    gp_used = defaultdict(list)
    # Option C — per-target-scope-level breakdown (mirrors wp_disc's per-discipline
    # pattern): {dom: {scope_label: [values]}}. Captures the client-/bc-/discipline-
    # scoped generic_to_* rows compare_cross_segment.py intentionally emits (see the
    # Scope decision comment on CASCADE_GROUP2_TYPES above) instead of discarding
    # them -- without blending them into the enterprise number itself.
    gt_by_scope = defaultdict(lambda: defaultdict(list))
    gc_by_scope = defaultdict(lambda: defaultdict(list))
    gp_by_scope = defaultdict(lambda: defaultdict(list))
    gt_used_by_scope = defaultdict(lambda: defaultdict(list))
    gc_used_by_scope = defaultdict(lambda: defaultdict(list))
    gp_used_by_scope = defaultdict(lambda: defaultdict(list))

    # Group 3 — scope-level fan-out containment (all-view + used-view). Captured only;
    # not rendered/tiered/anomaly-detected in this pass (see CASCADE_GROUP3_TYPES above).
    ep = defaultdict(list)
    bp = defaultdict(list)
    eb = defaultdict(list)
    ec = defaultdict(list)
    ep_used = defaultdict(list)
    bp_used = defaultdict(list)
    eb_used = defaultdict(list)
    ec_used = defaultdict(list)

    seen_comparison_types: set = set()
    sector_map = sector_map or {}

    for r in summary_rows:
        ct = r["comparison_type"]
        a, b = r["segment_id_a"], r["segment_id_b"]
        dom = r["domain"]
        seen_comparison_types.add(ct)
        if dom in EXCLUDED_FROM_SCORING:
            continue

        if ct == "template_to_container":
            # Group 1 bc-pooled fallback: classify BOTH sides (unlike Group 2,
            # neither side of a Group 1 pair is gated to a fixed role population)
            # and bucket into every (scope_a, scope_b) pair observed, verifying
            # VALUE equality (not just shape equality) via _group1_scope_pair()
            # so a mismatched-value pair (e.g. BC_1 vs BC_2) never lands in the
            # same bucket as genuine same-value evidence. tc itself is promoted
            # only from "enterprise::enterprise" -- exactly the same condition
            # as today's _is_unscoped_segment(r,"a") and (r,"b") gate, since
            # _target_scope_label() returns "enterprise" iff
            # _is_unscoped_segment() is True for that side -- so tc is
            # byte-for-byte unchanged.
            scope_a, scope_b, scope_pair = _group1_scope_pair(r)
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                tc_by_scope[dom][scope_pair].append(v)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    tc[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                tc_used_by_scope[dom][scope_pair].append(vu)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    tc_used[dom].append(vu)

        elif ct == "container_to_project":
            scope_a, scope_b, scope_pair = _group1_scope_pair(r)
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                cp_by_scope[dom][scope_pair].append(v)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    cp[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                cp_used_by_scope[dom][scope_pair].append(vu)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    cp_used[dom].append(vu)

        elif ct in ("template_to_project", "parent_sibling_roles"):
            scope_a, scope_b, scope_pair = _group1_scope_pair(r)
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                tp_by_scope[dom][scope_pair].append(v)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    tp[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                tp_used_by_scope[dom][scope_pair].append(vu)
                if scope_a == "enterprise" and scope_b == "enterprise":
                    tp_used[dom].append(vu)

        elif ct == "sibling_projects":
            ca = _pick(r, "client_label_a")
            cb = _pick(r, "client_label_b")
            # discover_sibling_segments() groups purely by (parent_segment_id,
            # governance_role, unit_system), so two DIFFERENTLY-scoped Project
            # segments under the SAME client (e.g. Kaiser's discipline- or
            # collection-scoped siblings sharing a client-level parent) can pair
            # as sibling_projects with ca == cb -- a within-client comparison, not
            # cross-client convergence. The old segment_id-length==3 guard
            # incidentally excluded these (they render with >3 parts); requiring
            # distinct clients here is the direct, column-based replacement.
            if ca != cb and sector_map.get(ca) == "healthcare" and sector_map.get(cb) == "healthcare":
                v = pf(_col(r, "jaccard_mean"))
                if v is not None:
                    xc[dom].append(v)

        elif ct == "within_project":
            v = pf(_col(r, "jaccard_mean"))
            disc = _pick(r, "discipline_label_a") or "all"
            if v is not None:
                wp_disc[dom][disc].append(v)
                wp_all[dom].append(v)
            vu = pf(_col(r, "used_jaccard_mean"))
            if vu is not None:
                wp_used[dom].append(vu)
            if a == b and r["governance_role_a"] == "Template" and v is not None:
                tw[dom].append(v)
            # Capture p10/p90 for all-view and used-view from most inclusive generic segment
            if a == b and _is_unscoped_segment(r, "a"):
                n = int(r["n_files_a"]) if r.get("n_files_a") else 0
                if _col(r, "jaccard_p10") and _col(r, "jaccard_p90"):
                    existing_n = wp_p10.get(dom + "_n", -1)
                    if n > existing_n:
                        wp_p10[dom] = pf(_col(r, "jaccard_p10"))
                        wp_p90[dom] = pf(_col(r, "jaccard_p90"))
                        wp_p10[dom + "_n"] = n
                if _col(r, "used_jaccard_p10") and _col(r, "used_jaccard_p90"):
                    existing_n = wp_used_p10.get(dom + "_n", -1)
                    if n > existing_n:
                        wp_used_p10[dom] = pf(_col(r, "used_jaccard_p10"))
                        wp_used_p90[dom] = pf(_col(r, "used_jaccard_p90"))
                        wp_used_p10[dom + "_n"] = n

        # Group 2 — one level up the cascade from tc/cp/tp. The GENERIC (reference)
        # side must still be the one canonical enterprise-wide Generic population --
        # _is_unscoped_segment(r, "a") -- but the TARGET (Template/Container/Project)
        # side is bucketed by its own scope level (Option C) rather than gated to
        # broadest-only, since compare_cross_segment.py intentionally emits
        # generic_to_* rows for client-/bc-/discipline-scoped targets too (real
        # baseline-propagation evidence -- see the Scope decision comment on
        # CASCADE_GROUP2_TYPES above). gt/gc/gp (the enterprise slice) are populated
        # only when the target's own scope label is "enterprise", keeping today's
        # single clean number unchanged; every other scope level lands in
        # gt_by_scope/gc_by_scope/gp_by_scope instead of being discarded.
        elif ct == "generic_to_template" and _is_unscoped_segment(r, "a"):
            scope = _target_scope_label(r, "b")
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                gt_by_scope[dom][scope].append(v)
                if scope == "enterprise":
                    gt[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                gt_used_by_scope[dom][scope].append(vu)
                if scope == "enterprise":
                    gt_used[dom].append(vu)

        elif ct == "generic_to_container" and _is_unscoped_segment(r, "a"):
            scope = _target_scope_label(r, "b")
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                gc_by_scope[dom][scope].append(v)
                if scope == "enterprise":
                    gc[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                gc_used_by_scope[dom][scope].append(vu)
                if scope == "enterprise":
                    gc_used[dom].append(vu)

        elif ct == "generic_to_project" and _is_unscoped_segment(r, "a"):
            scope = _target_scope_label(r, "b")
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                gp_by_scope[dom][scope].append(v)
                if scope == "enterprise":
                    gp[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                gp_used_by_scope[dom][scope].append(vu)
                if scope == "enterprise":
                    gp_used[dom].append(vu)

        # Group 3 — scope-level fan-out (enterprise/bc/client vs. Project, and
        # enterprise vs. bc/client). A different axis than the cascade stages above;
        # captured under new keys only. NOT rendered, tiered, or anomaly-detected in
        # this pass — pending a future business-center-section design decision.
        elif ct == "enterprise_to_project":
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                ep[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                ep_used[dom].append(vu)

        elif ct == "bc_to_project":
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                bp[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                bp_used[dom].append(vu)

        elif ct == "enterprise_to_bc":
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                eb[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                eb_used[dom].append(vu)

        elif ct == "enterprise_to_client":
            v = pf(_col(r, "containment_a_in_b_mean"))
            if v is not None:
                ec[dom].append(v)
            vu = pf(_col(r, "used_containment_a_in_b_mean"))
            if vu is not None:
                ec_used[dom].append(vu)

        # Group 4 — known, deliberately excluded from cascade (see
        # CASCADE_GROUP4_EXCLUDED_TYPES above for the reason behind each).
        elif ct in CASCADE_GROUP4_EXCLUDED_TYPES:
            pass

    # Coverage check: every comparison_type actually present in summary_rows must be
    # accounted for by name in one of the four groups above. This is the actual fix
    # for "future producer additions are invisible by default" (docs/
    # governance_narrative_scope_gap_audit.md A1) — an unrecognized type is a real
    # signal that either this dispatch or compare_cross_segment.py's vocabulary has
    # drifted, and must not be swallowed silently the way the old bare if/elif did.
    _known_comparison_types = (
        CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES
        | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
    )
    _warn_unrecognized_comparison_types(seen_comparison_types, _known_comparison_types, "build_cascade")

    # ── Bundle signal collection ──────────────────────────────────────────────
    # Dual-view schema (future):  all_n_shared_bundle_both / used_n_shared_bundle_both
    # Single-view schema (current): n_shared_bundle_both
    # We accumulate bundle_share = bundled_shared / total_shared for each view.
    # This measures what fraction of shared patterns are formally bundled (actively used).
    # For template_to_project rows only — that's where the governance signal lives.
    bundle_schema = detect_bundle_schema(summary_rows)

    # {domain: [bundle_share_all, ...]}  — fraction of shared patterns in all-view bundles
    bshare_all = defaultdict(list)
    # {domain: [bundle_share_used, ...]} — same for used-view bundles (dual schema only)
    bshare_used = defaultdict(list)
    # {domain: [passive_indicator, ...]} — drop from all to used, 0-1 (dual schema only)
    passive_indicator = defaultdict(list)

    for r in summary_rows:
        if r["comparison_type"] not in ("template_to_project", "parent_sibling_roles"):
            continue
        if not (_is_unscoped_segment(r, "a") and _is_unscoped_segment(r, "b")):
            continue
        dom = r["domain"]
        if dom in EXCLUDED_FROM_SCORING:
            continue
        ns = pf(_col(r, "n_shared_join_hash"))
        if not ns or ns == 0:
            continue

        if bundle_schema == "dual":
            nb_all = pf(_col(r, "all_n_shared_bundle_both"))
            nb_used = pf(_col(r, "used_n_shared_bundle_both"))
            if nb_all is not None:
                share_all = nb_all / ns
                bshare_all[dom].append(share_all)
            if nb_used is not None:
                share_used = nb_used / ns
                bshare_used[dom].append(share_used)
            if nb_all is not None and nb_used is not None and nb_all > 0:
                passive_indicator[dom].append((nb_all - nb_used) / nb_all)
        elif bundle_schema == "single":
            nb = pf(_col(r, "all_n_shared_bundle_both"))
            if nb is not None:
                bshare_all[dom].append(nb / ns)

    def mean_or_none(lst):
        return statistics.mean(lst) if lst else None

    result = {}
    all_domains = (
        set(tc) | set(cp) | set(tp) | set(xc) | set(wp_all) | set(tw)
        | set(gt) | set(gc) | set(gp)
        | set(gt_by_scope) | set(gc_by_scope) | set(gp_by_scope)
        | set(tc_by_scope) | set(cp_by_scope) | set(tp_by_scope)
        | set(ep) | set(bp) | set(eb) | set(ec)
    )
    for dom in all_domains:
        bs_all = mean_or_none(bshare_all[dom])
        bs_used = mean_or_none(bshare_used[dom])

        # Passive inheritance indicator: prefer containment-based delta (more direct signal)
        # over bundle-density delta. All-view containment - used-view containment = passive floor.
        # Normalise by all-view score so 0 = no passive, 1 = all shared patterns are purgeable.
        tp_all_m = mean_or_none(tp[dom])
        tp_used_m = mean_or_none(tp_used[dom])
        if tp_all_m and tp_used_m is not None and tp_all_m > 0:
            pi_containment = (tp_all_m - tp_used_m) / tp_all_m
        else:
            pi_containment = None

        # Fall back to bundle-density delta if containment delta unavailable
        pi_bundle = mean_or_none(passive_indicator[dom])
        pi_mean = pi_containment if pi_containment is not None else pi_bundle

        result[dom] = {
            "tc": mean_or_none(tc[dom]),
            "cp": mean_or_none(cp[dom]),
            "tp": tp_all_m,
            "tp_used": tp_used_m,
            "tc_used": mean_or_none(tc_used[dom]),
            "cp_used": mean_or_none(cp_used[dom]),
            "xc": mean_or_none(xc[dom]),
            "wp_all": mean_or_none(wp_all[dom]),
            "wp_used": mean_or_none(wp_used[dom]),
            "wp_disc": {d: statistics.mean(v) for d, v in wp_disc[dom].items() if v},
            "tw": mean_or_none(tw[dom]),
            "wp_p10": wp_p10.get(dom),
            "wp_p90": wp_p90.get(dom),
            "wp_used_p10": wp_used_p10.get(dom),
            "wp_used_p90": wp_used_p90.get(dom),
            # Bundle/passive-inheritance signals
            "bundle_schema": bundle_schema,
            "bundle_share_all": bs_all,
            "bundle_share_used": bs_used,
            "passive_indicator": pi_mean,       # primary: containment delta, fallback: bundle delta
            "passive_indicator_method": "containment" if pi_containment is not None else ("bundle" if pi_bundle is not None else "none"),
            # Group 2 — generic->template/container/project containment (one level up
            # the cascade from tc/cp/tp)
            "gt": mean_or_none(gt[dom]),
            "gc": mean_or_none(gc[dom]),
            "gp": mean_or_none(gp[dom]),
            "gt_used": mean_or_none(gt_used[dom]),
            "gc_used": mean_or_none(gc_used[dom]),
            "gp_used": mean_or_none(gp_used[dom]),
            # Option C — per-target-scope-level breakdown, e.g. {"enterprise": 0.9,
            # "client": 0.4, "client_discipline": 0.3}. "enterprise" always equals
            # the "gt"/"gc"/"gp" value above (same source data); every other key is
            # scoped evidence that used to be silently discarded.
            "gt_by_scope": {s: statistics.mean(v) for s, v in gt_by_scope[dom].items() if v},
            "gc_by_scope": {s: statistics.mean(v) for s, v in gc_by_scope[dom].items() if v},
            "gp_by_scope": {s: statistics.mean(v) for s, v in gp_by_scope[dom].items() if v},
            "gt_used_by_scope": {s: statistics.mean(v) for s, v in gt_used_by_scope[dom].items() if v},
            "gc_used_by_scope": {s: statistics.mean(v) for s, v in gc_used_by_scope[dom].items() if v},
            "gp_used_by_scope": {s: statistics.mean(v) for s, v in gp_used_by_scope[dom].items() if v},
            # Group 1 bc-pooled fallback -- per-(scope_a, scope_b)-pair breakdown
            # mirroring Group 2's Option C above. "enterprise::enterprise" always
            # equals the tc/cp/tp value above (same source data); every other key
            # (typically "bc::bc") is scoped evidence that used to be silently
            # discarded. See docs/governance_narrative_group1_scope_gap_investigation.md.
            "tc_by_scope": {s: statistics.mean(v) for s, v in tc_by_scope[dom].items() if v},
            "cp_by_scope": {s: statistics.mean(v) for s, v in cp_by_scope[dom].items() if v},
            "tp_by_scope": {s: statistics.mean(v) for s, v in tp_by_scope[dom].items() if v},
            "tc_used_by_scope": {s: statistics.mean(v) for s, v in tc_used_by_scope[dom].items() if v},
            "cp_used_by_scope": {s: statistics.mean(v) for s, v in cp_used_by_scope[dom].items() if v},
            "tp_used_by_scope": {s: statistics.mean(v) for s, v in tp_used_by_scope[dom].items() if v},
            # Intra-bucket spread (min, max) for any scope_pair backed by >=2 rows --
            # lets detect_anomalies flag a pooled mean (typically "bc::bc") that hides
            # sharp per-business-center disagreement rather than genuine convergence.
            "tc_by_scope_spread": {s: (min(v), max(v)) for s, v in tc_by_scope[dom].items() if len(v) > 1},
            "cp_by_scope_spread": {s: (min(v), max(v)) for s, v in cp_by_scope[dom].items() if len(v) > 1},
            "tp_by_scope_spread": {s: (min(v), max(v)) for s, v in tp_by_scope[dom].items() if len(v) > 1},
            # Group 3 — scope-level fan-out containment. Captured only; NOT rendered,
            # tiered, or anomaly-detected in this pass — pending a future
            # business-center-section design decision (see
            # docs/governance_narrative_scope_gap_audit.md A1).
            "ep": mean_or_none(ep[dom]),
            "bp": mean_or_none(bp[dom]),
            "eb": mean_or_none(eb[dom]),
            "ec": mean_or_none(ec[dom]),
            "ep_used": mean_or_none(ep_used[dom]),
            "bp_used": mean_or_none(bp_used[dom]),
            "eb_used": mean_or_none(eb_used[dom]),
            "ec_used": mean_or_none(ec_used[dom]),
        }
    return result


# Score reliability classifications
# Based on within-project p10/p90 spread and mean
RELIABILITY_TIGHT        = "Tight"          # p10 >= 0.85 — every pair agrees; mean is a firm number
RELIABILITY_CONVERGENT   = "Convergent"     # p10 >= 0.50, spread < 0.40 — strong core, modest tail
RELIABILITY_PRESENCE     = "Presence-based" # p10 near 0, p90 near 1, mean 0.4–0.8 — domain is
                                             # optional; files either fully carry it or don't
RELIABILITY_SPARSE       = "Sparse"         # p10 near 0, mean < 0.40 — minority of files carry
                                             # the domain at all; mean understates fragmentation
RELIABILITY_UNKNOWN      = "Unknown"        # no p10/p90 data


def score_reliability(d: dict) -> str:
    """
    Classify mean score reliability from within-project p10/p90 spread.

    Tight:          p10 >= 0.85  — floor is high; mean trustworthy
    Convergent:     p10 >= 0.50, spread < 0.40  — solid core, some tail variation
    Presence-based: p10 < 0.20 AND p90 >= 0.85  — binary optional domain;
                    mean reflects how many files carry it, not how well they agree
    Sparse:         p10 < 0.20 AND mean < 0.40  — domain rarely present at all
    """
    p10 = d.get("wp_p10")
    p90 = d.get("wp_p90")
    mean = d.get("wp_all")

    if p10 is None or p90 is None:
        return RELIABILITY_UNKNOWN

    spread = p90 - p10

    if p10 >= 0.85:
        return RELIABILITY_TIGHT
    if p10 >= 0.50 and spread < 0.40:
        return RELIABILITY_CONVERGENT
    if p10 < 0.20 and p90 >= 0.85:
        return RELIABILITY_PRESENCE
    if p10 < 0.20 and (mean is None or mean < 0.40):
        return RELIABILITY_SPARSE
    # Moderate spread with moderate floor — convergent with meaningful tail
    return RELIABILITY_CONVERGENT


RELIABILITY_DESCRIPTIONS = {
    RELIABILITY_TIGHT: (
        "Score is highly reliable — nearly all file pairs agree. "
        "The reported mean reflects a genuine uniform standard."
    ),
    RELIABILITY_CONVERGENT: (
        "Score is reliable — a strong core of files agree, "
        "with some variation in the tail. Mean is a good governance signal."
    ),
    RELIABILITY_PRESENCE: (
        "Score reliability is limited — this domain follows a binary presence pattern. "
        "Files either fully carry the configuration or have none of it. "
        "The mean reflects adoption rate across files, not agreement between files that have it. "
        "Interpret as: roughly {mean_pct} of file pairs both carry this domain."
    ),
    RELIABILITY_SPARSE: (
        "Score reliability is low — this domain is present in only a minority of files. "
        "The mean understates fragmentation. "
        "Governance should focus on whether the domain should be mandatory before assessing convergence."
    ),
    RELIABILITY_UNKNOWN: (
        "Spread data not available for this domain."
    ),
}



TIER_STRONG_BASELINE = "Strong Baseline Candidate"
TIER_BASELINE_LOCAL_REVIEW = "Baseline Candidate — Local/Use Review"
TIER_BASELINE_CONTAINER_GAP = "Baseline Candidate — Container Gap"
TIER_INVESTIGATE = "Investigate Before Baseline"
TIER_ACTIVE_LOCAL = "Active Local Practice Review"
TIER_MODERATE_VARIATION = "Moderate Variation"
TIER_HIGH_FRAGMENTATION = "High Fragmentation"
TIER_SPARSE_LIMITED = "Sparse / Presence-Limited"
TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE = "Insufficient Evidence — Enterprise; BC-Level Evidence Available"
TIER_INSUFFICIENT = "Insufficient Evidence"

# Deterministic materiality thresholds used to keep baseline language conservative.
# These are narrative thresholds, not governance policy. They decide when the
# renderer must add review language rather than presenting a cleaner baseline read.
LOCAL_ACTIVE_MATERIAL_THRESHOLD = 0.15
PASSIVE_MATERIAL_THRESHOLD = 0.20
MISSING_MATERIAL_THRESHOLD = 0.20
ACTIVE_USE_MIN_FOR_STRONG_BASELINE = 0.75


def _state_value(state: Optional[dict], key: str) -> Optional[float]:
    if not state:
        return None
    return state.get(key)


def _has_material_state_exception(state: Optional[dict]) -> bool:
    """Return True when explicit state signals limit a baseline conclusion."""
    if not state:
        return False
    checks = (
        ("local_active_share", LOCAL_ACTIVE_MATERIAL_THRESHOLD),
        ("provided_passive_share", PASSIVE_MATERIAL_THRESHOLD),
        ("provided_missing_share", MISSING_MATERIAL_THRESHOLD),
    )
    for key, threshold in checks:
        val = state.get(key)
        if val is not None and val >= threshold:
            return True
    return False


def _has_group1_bc_pooled_evidence(d: dict) -> bool:
    """True when a same-bc-both-sides ("bc::bc") pooled containment value exists
    in tp_by_scope or cp_by_scope, even though the enterprise-only tp/cp is None.

    This is deliberately a presence check only, not a score-magnitude check --
    assign_tier() must not blend this pooled value into `primary` (see
    docs/governance_narrative_group1_scope_gap_investigation.md, Q2/Q3): a
    domain with bc-pooled evidence gets a distinct tier
    (TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE), not a score-banded promotion
    into TIER_STRONG_BASELINE/TIER_INVESTIGATE/etc., which would imply
    enterprise-level evidence that doesn't exist.
    """
    return (
        (d.get("tp_by_scope") or {}).get("bc::bc") is not None
        or (d.get("cp_by_scope") or {}).get("bc::bc") is not None
    )


def assign_tier(d: dict, state: Optional[dict] = None) -> str:
    """Assign a DoD-safe governance classification.

    The tier is an evidence/readiness classification, not an approval decision.
    High containment can create a baseline candidate, but explicit local-active,
    passive, missing, sparse, or presence-limited signals prevent the renderer
    from declaring a domain ready as a formal standard.
    """
    tc, cp, tp = d["tc"], d["cp"], d["tp"]
    primary = tp if tp is not None else cp
    reliability = score_reliability(d)

    if primary is None:
        if _has_group1_bc_pooled_evidence(d):
            return TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE
        return TIER_INSUFFICIENT

    local_active = _state_value(state, "local_active_share")
    passive = _state_value(state, "provided_passive_share")
    missing = _state_value(state, "provided_missing_share")
    provided_used = _state_value(state, "provided_to_used_containment")

    # Sparse or binary-presence domains are not safe to present as converged
    # standards unless they also have strong explicit active-use evidence.
    if reliability == RELIABILITY_SPARSE and primary < 0.75:
        return TIER_SPARSE_LIMITED

    if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD and primary < 0.90:
        return TIER_ACTIVE_LOCAL

    if primary >= 0.90:
        if _has_material_state_exception(state):
            return TIER_BASELINE_LOCAL_REVIEW
        if provided_used is not None and provided_used < ACTIVE_USE_MIN_FOR_STRONG_BASELINE:
            return TIER_BASELINE_LOCAL_REVIEW
        if tc is not None and tc < 0.60:
            return TIER_BASELINE_CONTAINER_GAP
        return TIER_STRONG_BASELINE

    if primary >= 0.75:
        if _has_material_state_exception(state):
            return TIER_BASELINE_LOCAL_REVIEW
        if reliability in (RELIABILITY_PRESENCE, RELIABILITY_SPARSE):
            return TIER_INVESTIGATE
        return TIER_INVESTIGATE

    if primary >= 0.55:
        if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
            return TIER_ACTIVE_LOCAL
        if reliability == RELIABILITY_SPARSE:
            return TIER_SPARSE_LIMITED
        return TIER_MODERATE_VARIATION

    return TIER_HIGH_FRAGMENTATION


TIER_ORDER = {
    TIER_STRONG_BASELINE: 0,
    TIER_BASELINE_LOCAL_REVIEW: 1,
    TIER_BASELINE_CONTAINER_GAP: 2,
    TIER_INVESTIGATE: 3,
    TIER_ACTIVE_LOCAL: 4,
    TIER_MODERATE_VARIATION: 5,
    TIER_SPARSE_LIMITED: 6,
    TIER_HIGH_FRAGMENTATION: 7,
    TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE: 8,
    TIER_INSUFFICIENT: 9,
}


def detect_anomalies(dom: str, d: dict, state: Optional[dict] = None) -> list[str]:
    notes = []
    tc, cp, tp = d["tc"], d["cp"], d["tp"]
    xc = d["xc"]
    reliability = score_reliability(d)

    # Score reliability note — only emit when it limits interpretation.
    if reliability == RELIABILITY_PRESENCE:
        mean_pct = pct(d.get("wp_all"))
        desc = RELIABILITY_DESCRIPTIONS[RELIABILITY_PRESENCE].replace("{mean_pct}", mean_pct)
        notes.append(desc)
    elif reliability == RELIABILITY_SPARSE:
        notes.append(RELIABILITY_DESCRIPTIONS[RELIABILITY_SPARSE])

    if state:
        provided_used = state.get("provided_to_used_containment")
        provided_configured = state.get("provided_to_configured_containment")
        passive = state.get("provided_passive_share")
        missing = state.get("provided_missing_share")
        local_active = state.get("local_active_share")

        if provided_configured is not None and provided_used is not None:
            if provided_configured >= 0.75 and provided_used < 0.75:
                notes.append(
                    f"Provided vocabulary is substantially carried downstream ({pct(provided_configured)}) "
                    f"but active-use containment is lower ({pct(provided_used)}). Treat this as a "
                    "baseline candidate needing active-use review, not an approval-ready standard."
                )
        if passive is not None and passive >= PASSIVE_MATERIAL_THRESHOLD:
            notes.append(
                f"Inherited-but-passive signal is material ({pct(passive)}). The domain should be "
                "reviewed for starter-content, approved-list, or exception-governance treatment before ratification."
            )
        if missing is not None and missing >= MISSING_MATERIAL_THRESHOLD:
            notes.append(
                f"Provided-but-missing signal is material ({pct(missing)}). Confirm whether missing downstream "
                "content is intentional pruning, role-specific specialization, or unmanaged propagation failure."
            )
        if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
            notes.append(
                f"Active local practice is material ({pct(local_active)}). Review whether local patterns are "
                "roll-up candidates, client/discipline playbook content, permitted variants, or project-specific exceptions."
            )

    # Bundle / passive inheritance signal fallback when explicit state outputs are absent.
    bundle_share = d.get("bundle_share_all")
    bundle_schema = d.get("bundle_schema", "none")
    passive_ind = d.get("passive_indicator")

    if not state:
        if bundle_schema == "dual" and passive_ind is not None:
            if passive_ind >= 0.40:
                notes.append(
                    f"High passive inheritance signal ({passive_ind*100:.0f}% of bundled shared "
                    "patterns drop out under the used view) — a significant fraction of the "
                    "template vocabulary is present in projects but not actively exercised. "
                    "Ratification should consider an active-use threshold, not just pattern presence."
                )
            elif passive_ind >= 0.20:
                notes.append(
                    f"Moderate passive inheritance ({passive_ind*100:.0f}% drop from all to used view). "
                    "Some template patterns are inherited but not in active use."
                )
        elif bundle_schema == "single" and bundle_share is not None:
            if dom in PASSIVE_INHERITANCE_RISK_DOMAINS and bundle_share < 0.25:
                notes.append(
                    f"Low bundle density among shared patterns ({bundle_share*100:.0f}% bundled). "
                    "This domain is in the passive inheritance risk group — shared patterns may be "
                    "inherited rather than actively configured. Used-view analysis recommended "
                    "before ratification."
                )
            elif bundle_share is not None and bundle_share < 0.15:
                notes.append(
                    f"Very low bundle density among shared patterns ({bundle_share*100:.0f}% bundled). "
                    "Shared vocabulary is largely unstructured — consider used-view analysis "
                    "to confirm patterns are actively exercised."
                )

    # Group 2 (generic->template) signal — surfaces a distinct governance question
    # from tc/cp/tp alone: the enterprise/generic baseline successfully reached
    # templates, but templates aren't cascading down to projects, so the break is
    # specifically between templates and projects rather than with the baseline
    # content itself.
    gt = d.get("gt")
    if gt is not None and gt >= 0.75 and tp is not None and tp < 0.55:
        notes.append(
            f"Generic/enterprise baseline containment into templates is strong (G→T = {pct(gt)}) "
            f"but template-to-project propagation is weak (T→P = {pct(tp)}). The enterprise "
            "baseline is reaching templates; the break is between templates and projects, "
            "not with the baseline content itself."
        )

    # Group 2 scope-breakdown divergence (Option C) — a distinct governance question
    # from the enterprise-only gt/gc/gp values alone: does the generic/enterprise
    # baseline propagate as well into SCOPED (client-/bc-/discipline-specific)
    # templates/containers/projects as it does into the single broadest one? A
    # material gap in either direction is informative (the baseline holding at the
    # enterprise level while eroding for specific clients/disciplines, or vice
    # versa) and would otherwise stay invisible now that gt_by_scope/gc_by_scope/
    # gp_by_scope capture it instead of discarding it.
    for cascade_label, enterprise_val, by_scope in (
        ("Generic→Template", d.get("gt"), d.get("gt_by_scope") or {}),
        ("Generic→Container", d.get("gc"), d.get("gc_by_scope") or {}),
        ("Generic→Project", d.get("gp"), d.get("gp_by_scope") or {}),
    ):
        scoped_vals = {k: v for k, v in by_scope.items() if k != "enterprise"}
        if enterprise_val is None or not scoped_vals:
            continue
        scoped_mean = statistics.mean(scoped_vals.values())
        if abs(enterprise_val - scoped_mean) >= 0.25:
            direction = "weaker" if scoped_mean < enterprise_val else "stronger"
            detail = ", ".join(f"{k}={pct(v)}" for k, v in sorted(scoped_vals.items()))
            notes.append(
                f"{cascade_label} propagation is {direction} into scoped targets than the "
                f"enterprise-wide reading ({pct(enterprise_val)} enterprise vs. {pct(scoped_mean)} "
                f"scoped mean — {detail}). Review whether client-/business-center-/discipline-"
                "specific practice is diverging from or exceeding the enterprise baseline."
            )

    # Group 1 by-scope intra-bucket divergence — a distinct governance question
    # from Group 2's enterprise-vs-scoped check above: Group 1 (tc/cp/tp) usually
    # has NO enterprise-level reading to compare against at all (that's the gap
    # tc_by_scope/cp_by_scope/tp_by_scope exists to fill — see
    # docs/governance_narrative_group1_scope_gap_investigation.md), so the risk
    # here isn't "enterprise differs from scoped" but "the pooled MEAN itself
    # hides sharp disagreement between the individual rows pooled into it."
    # Deliberately scope-neutral wording: a scope_pair like "bc::bc" pools
    # multiple DISTINCT business centers when more than one exists, but a
    # scope_pair like "client_bc::client_discipline" pools rows that share the
    # same client/bc and vary only by discipline (confirmed against real
    # cross_segment_summary.csv data -- see docs/governance_narrative_group1_scope_gap_investigation.md
    # follow-up) -- the note must not claim "business-center" divergence when
    # the actual varying dimension for that particular scope_pair could be
    # client or discipline instead. Uses the same >=0.25 absolute-gap
    # materiality threshold as the Group 2 check above, applied to each
    # scope_pair's own (min, max) spread instead of an enterprise-vs-mean
    # comparison.
    for cascade_label, by_scope_spread, by_scope_mean in (
        ("Template→Container", d.get("tc_by_scope_spread") or {}, d.get("tc_by_scope") or {}),
        ("Container→Project", d.get("cp_by_scope_spread") or {}, d.get("cp_by_scope") or {}),
        ("Template→Project", d.get("tp_by_scope_spread") or {}, d.get("tp_by_scope") or {}),
    ):
        for scope_pair, (lo, hi) in sorted(by_scope_spread.items()):
            if scope_pair == "enterprise::enterprise":
                continue
            if hi - lo >= 0.25:
                notes.append(
                    f"{cascade_label} pooled evidence for scope '{scope_pair}' spans "
                    f"{pct(lo)}–{pct(hi)} across the individual rows pooled into this "
                    f"bucket (pooled mean {pct(by_scope_mean.get(scope_pair))}). This "
                    "scope level is not a single converged reading — review the "
                    "underlying per-row variation before treating the pooled mean as "
                    "one number."
                )

    if tc is not None and tp is not None and tp > tc + 0.25:
        notes.append(
            "Template patterns arrive in projects via direct Revit inheritance, "
            "bypassing coordination files — coordination files are not the governance "
            "vehicle for this domain."
        )
    if tc is not None and tc < 0.20:
        notes.append(
            f"Templates propagate weakly into coordination files "
            f"(T→C = {pct(tc)}). Coordination files govern this domain independently."
        )
    if cp is not None and cp < 0.50:
        notes.append(
            f"Coordination-file-to-project cascade is weak (C→P = {pct(cp)}). "
            "Project teams are diverging from coordination file vocabulary."
        )
    if xc is not None and xc >= 0.70:
        notes.append(
            f"Strong cross-client convergence ({pct(xc)}) — natural baseline candidate "
            "for governance review regardless of formal template propagation."
        )
    if xc is not None and xc < 0.15 and tp is not None and tp > 0.70:
        notes.append(
            "Template floor propagates well but cross-client convergence is low — "
            "clients are inheriting the template floor while adding client-specific vocabulary."
        )
    if "view_template" in dom:
        disc_wp = d["wp_disc"]
        zero_discs = [_disc_label(k) for k, v in disc_wp.items() if v < 0.05 and k != "all"]
        if zero_discs:
            notes.append(
                f"Architecturally specific — near-zero within-project coherence for: "
                f"{', '.join(zero_discs)}. These disciplines require separate view template governance."
            )
    if dom == "phases":
        if tp is not None and tp < 0.85 and d["tw"] is not None and d["tw"] > 0.80:
            notes.append(
                "Templates are internally consistent on phases but projects carry "
                "phases not defined in templates — project teams are adding project-specific phases."
            )
    if dom == "loaded_family_types":
        notes.append(
            "Family loading is inherently project-specific. "
            "Template governance establishes a floor, not a ceiling. "
            "Consider approved-list governance rather than full vocabulary convergence."
        )
    return notes

def build_client_summary(
    summary_rows: list[dict],
    pooled_rows: list[dict],
    sector_map: Optional[dict] = None,
) -> list[dict]:
    """Per-client alignment summary."""
    sector_map = sector_map or {}
    # Client existence must not depend on which pool grain happens to have >=2
    # siblings for that client. _emit_for_groups() in compare_cross_segment.py
    # requires len(members) >= 2 INDEPENDENTLY per grain (parent_sibling/bc/client),
    # so a client whose Project segments never share a common immediate parent with
    # another sibling gets ZERO parent_sibling rows, even though it may have real
    # bc- or client-grain pooled rows, or real summary_rows (within_project/
    # sibling_projects) data. An earlier pool_scope filter here (meant to stop
    # pool-relative metrics from blending three distinct pools together -- see
    # docs/governance_narrative_scope_gap_audit.md A2) accidentally dropped such
    # clients from the client section entirely. client_label and n_files_focal both
    # describe the FOCAL segment itself, not the pool, so they're identical across
    # a segment's parent_sibling/bc/client pooled rows -- there is no blending risk
    # in reading them from every pool_scope grain. (If a genuinely pool-relative
    # metric -- e.g. all_containment_focal_in_pool, used_containment_pool_in_focal,
    # n_shared_join_hash -- is ever read from pooled_rows in this function, THAT
    # read must filter by pool_scope at its own point of use; client discovery and
    # n_files below must not.)
    all_clients = set()
    for r in pooled_rows:
        c = _pick(r, "client_label")
        if c and r["governance_role"] == "Project":
            all_clients.add(c)
    for r in summary_rows:
        # discover_within_project() can emit within_project rows for ANY
        # non-skip/non-registration segment, not just Project-role ones (e.g. a
        # client-scoped Template/Container/Generic segment). This section is
        # specifically about the client's PROJECT portfolio (project file counts,
        # project-vs-project coherence), so gate on governance_role_a == "Project"
        # before treating a row as evidence this client has project data.
        # sibling_projects rows are already structurally Project-only by
        # construction (discover_sibling_segments() only labels a pair
        # "sibling_projects" when both sides share governance_role == "project"),
        # but the check is kept here too for defense-in-depth.
        if r["comparison_type"] == "within_project" and r["governance_role_a"] == "Project":
            c = _pick(r, "client_label_a")
            if c:
                all_clients.add(c)
        elif r["comparison_type"] == "sibling_projects" and r["governance_role_a"] == "Project" and r["governance_role_b"] == "Project":
            for suffix in ("a", "b"):
                c = _pick(r, f"client_label_{suffix}")
                if c:
                    all_clients.add(c)

    # Cross-client Jaccard
    xc_by_client = defaultdict(list)
    for r in summary_rows:
        if r["comparison_type"] != "sibling_projects":
            continue
        pa, pb = r["segment_id_a"].split("|"), r["segment_id_b"].split("|")
        if len(pa) == 3 and pa[2] in all_clients and len(pb) == 3 and pb[2] in all_clients:
            v = pf(_col(r, "jaccard_mean"))
            if v is not None:
                xc_by_client[pa[2]].append(v)
                xc_by_client[pb[2]].append(v)

    # Within-project coherence. Gated on governance_role_a == "Project" for the
    # same reason as the all_clients fallback above -- within_project rows exist
    # for any role, and this section reports PROJECT coherence specifically.
    wp_by_client = defaultdict(list)
    for r in summary_rows:
        if r["comparison_type"] != "within_project" or r["governance_role_a"] != "Project":
            continue
        c = _pick(r, "client_label_a")
        v = pf(_col(r, "jaccard_mean"))
        if v is not None and c:
            wp_by_client[c].append(v)

    # n_files from pooled — every pool_scope grain, same rationale as all_clients
    # above: n_files_focal describes the focal segment, not the pool, so it's
    # identical across a segment's parent_sibling/bc/client rows. Falls back to
    # summary_rows' own n_files_a/n_files_b for clients discovered only from
    # summary_rows above (e.g. a single-project client with no >=2-member pool
    # grain at all) -- without this, such a client reports n_files=0 / "Low
    # corpus confidence" despite the summary row itself carrying a real count.
    client_files = {}
    for r in pooled_rows:
        c = _pick(r, "client_label")
        if c and r["governance_role"] == "Project":
            nf = int(r["n_files_focal"]) if r.get("n_files_focal") else 0
            if c not in client_files or nf > client_files[c]:
                client_files[c] = nf
    for r in summary_rows:
        # Same governance_role_a == "Project" gating as all_clients above --
        # within_project rows exist for any role, and n_files here specifically
        # means project file counts for the Client Analysis section.
        if r["comparison_type"] == "within_project" and r["governance_role_a"] == "Project":
            c = _pick(r, "client_label_a")
            nf = int(r["n_files_a"]) if r.get("n_files_a") else 0
            if c and (c not in client_files or nf > client_files[c]):
                client_files[c] = nf
        elif r["comparison_type"] == "sibling_projects" and r["governance_role_a"] == "Project" and r["governance_role_b"] == "Project":
            for suffix in ("a", "b"):
                c = _pick(r, f"client_label_{suffix}")
                nf = int(r[f"n_files_{suffix}"]) if r.get(f"n_files_{suffix}") else 0
                if c and (c not in client_files or nf > client_files[c]):
                    client_files[c] = nf

    # Domain-level xc means
    xc_dom_by_client = defaultdict(lambda: defaultdict(list))
    for r in summary_rows:
        if r["comparison_type"] != "sibling_projects":
            continue
        pa, pb = r["segment_id_a"].split("|"), r["segment_id_b"].split("|")
        if len(pa) == 3 and pa[2] in all_clients and len(pb) == 3 and pb[2] in all_clients:
            v = pf(_col(r, "jaccard_mean"))
            if v is not None and r["domain"] not in EXCLUDED_FROM_SCORING:
                xc_dom_by_client[pa[2]][r["domain"]].append(v)
                xc_dom_by_client[pb[2]][r["domain"]].append(v)

    rows_out = []
    for client in sorted(all_clients):
        xc_vals = xc_by_client.get(client, [])
        xc_mean = statistics.mean(xc_vals) if xc_vals else None
        wp_vals = wp_by_client.get(client, [])
        wp_mean = statistics.mean(wp_vals) if wp_vals else None
        n_files = client_files.get(client, 0)

        dom_means = {
            d: statistics.mean(v)
            for d, v in xc_dom_by_client[client].items()
            if v
        }
        strongest = sorted(dom_means.items(), key=lambda x: -x[1])[:3]
        weakest = sorted(dom_means.items(), key=lambda x: x[1])[:3]

        # Tier from cross-client Jaccard. sector_map is external, editable data
        # (see load_client_sectors()/--client-sector) -- a client tagged with a
        # known, non-healthcare sector is treated as non-comparable; an
        # unclassified client (absent from the file, or no file supplied at all)
        # falls through to the normal alignment tiers below rather than being
        # guessed at. See docs/governance_narrative_scope_gap_audit.md C7.
        sector = sector_map.get(client, "unknown")
        if sector not in ("unknown", "healthcare"):
            tier = "Non-comparable (different sector)"
        elif xc_mean is None:
            tier = "Insufficient Data"
        elif xc_mean >= 0.45:
            tier = "High Cross-Client Alignment"
        elif xc_mean >= 0.33:
            tier = "Moderate Cross-Client Alignment"
        else:
            tier = "Low Cross-Client Alignment"

        # Confidence note based on file count
        if n_files < 10:
            conf = f"Low corpus confidence — only {n_files} project files"
        elif n_files < 25:
            conf = f"Moderate corpus ({n_files} files)"
        else:
            conf = f"Good corpus ({n_files} files)"

        rows_out.append({
            "client": client,
            "n_files": n_files,
            "tier": tier,
            "xc_mean": xc_mean,
            "wp_mean": wp_mean,
            "confidence_note": conf,
            "strongest": strongest,
            "weakest": weakest,
            "sector": sector,
            "is_healthcare": sector == "healthcare",
        })

    rows_out.sort(key=lambda r: -(r["xc_mean"] or 0))
    return rows_out


def _pick(row: dict, *names: str) -> str:
    """Return the first non-empty value from row for the provided column names."""
    for name in names:
        value = row.get(name, "")
        if value not in (None, ""):
            return str(value)
    return ""


def _truthy(value) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _add_float(values: list[float], row: dict, *names: str) -> None:
    v = pf(_pick(row, *names))
    if v is not None:
        values.append(v)


def _state_bucket() -> dict:
    return {
        "provided_and_used_count": 0,
        "provided_but_passive_count": 0,
        "provided_but_missing_count": 0,
        "local_active_count": 0,
        "local_passive_count": 0,
        "local_unbundled_count": 0,
        "reference_all_count": 0,
        "target_all_count": 0,
        "target_used_count": 0,
        "generic_to_template_vals": [],
        "generic_to_container_vals": [],
        "generic_to_project_vals": [],
        "provided_to_configured_vals": [],
        "provided_to_used_vals": [],
        "provided_passive_vals": [],
        "provided_missing_vals": [],
        "local_active_vals": [],
    }


_STATE_COUNT_FIELDS = {
    "provided_and_used": "provided_and_used_count",
    "provided_but_passive": "provided_but_passive_count",
    "provided_but_missing": "provided_but_missing_count",
    "local_active": "local_active_count",
    "local_passive": "local_passive_count",
    "local_unbundled": "local_unbundled_count",
}



# Synced from compare_cross_segment.py's GOVERNANCE_STATE_DIRECTED_TYPES via direct
# import (this local copy had drifted -- missing all 4 new scope types, and carrying
# two entries the producer's write-gate (compare_cross_segment.py:3697,
# `if ctype in GOVERNANCE_STATE_DIRECTED_TYPES:`) confirms never reach a governance-
# state output file today:
#   - "parent_sibling_roles" -- governance-state rows are only ever written for the
#     imported 10-type set; parent_sibling_roles pairs are cascade-only (Prompt 2).
#   - "generic_to_downstream" -- appears nowhere in compare_cross_segment.py,
#     CHANGELOG.md, DECISIONS.md, or the canonical directed-type list in
#     docs/cross_segment_comparison.md:280. This repo's git history is a shallow
#     clone whose earliest visible commit already contains it in this file, so its
#     origin/intent can't be traced further from available history.
# Per this prompt's instruction not to guess and silently drop, both are kept as a
# defensive superset rather than removed -- flagged for Greg to confirm neither is
# needed before deleting.
_DIRECTED_GOVERNANCE_TYPES = GOVERNANCE_STATE_DIRECTED_TYPES | {
    "generic_to_downstream",
    "parent_sibling_roles",
}

# The subset of _DIRECTED_GOVERNANCE_TYPES that render_governance_state_section()
# actually renders today -- everything EXCEPT the four new scope-level types
# (CASCADE_GROUP3_TYPES), consistent with Prompt 2's deferred-rendering treatment of
# the same four types in build_cascade. Used to build the domain-level merged view
# without blending in enterprise_to_project/bc_to_project/enterprise_to_bc/
# enterprise_to_client -- see build_governance_state_summary().
_GOVERNANCE_STATE_RENDERED_TYPES = _DIRECTED_GOVERNANCE_TYPES - CASCADE_GROUP3_TYPES


def _mean(values: list[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def _merge_state_buckets(buckets: list) -> dict:
    """Sum a list of _state_bucket()-shaped dicts into one (counts add, vals concat)."""
    merged = _state_bucket()
    for bucket in buckets:
        for k, v in bucket.items():
            if k.endswith("_vals"):
                merged[k].extend(v)
            else:
                merged[k] += v
    return merged


def _finalize_state_bucket(bucket: dict) -> dict:
    """Compute shares/labels from one _state_bucket()-shaped dict. Same math regardless
    of whether `bucket` represents one comparison_type or a merge of several."""
    ref_n = bucket["reference_all_count"]
    tgt_used_n = bucket["target_used_count"]
    provided_used = bucket["provided_and_used_count"]
    provided_passive = bucket["provided_but_passive_count"]
    provided_missing = bucket["provided_but_missing_count"]
    local_active = bucket["local_active_count"]

    # Prefer explicit summary metrics; otherwise derive from state counts.
    provided_to_configured = _mean(bucket["provided_to_configured_vals"])
    if provided_to_configured is None and ref_n:
        provided_to_configured = (provided_used + provided_passive) / ref_n

    provided_to_used = _mean(bucket["provided_to_used_vals"])
    if provided_to_used is None and ref_n:
        provided_to_used = provided_used / ref_n

    provided_passive_share = _mean(bucket["provided_passive_vals"])
    if provided_passive_share is None and ref_n:
        provided_passive_share = provided_passive / ref_n

    provided_missing_share = _mean(bucket["provided_missing_vals"])
    if provided_missing_share is None and ref_n:
        provided_missing_share = provided_missing / ref_n

    local_active_share = _mean(bucket["local_active_vals"])
    if local_active_share is None and tgt_used_n:
        local_active_share = local_active / tgt_used_n

    if provided_to_used is not None and provided_to_used >= 0.85:
        primary_read = "Provided standard is actively used"
    elif provided_passive_share is not None and provided_passive_share >= PASSIVE_MATERIAL_THRESHOLD:
        primary_read = "Provided standard is carried but partly passive"
    elif local_active_share is not None and local_active_share >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
        primary_read = "Active local practice may need roll-up review"
    elif provided_missing_share is not None and provided_missing_share >= MISSING_MATERIAL_THRESHOLD:
        primary_read = "Provided content is missing downstream"
    else:
        primary_read = "State signal available; no dominant exception pattern"

    return {
        **{k: v for k, v in bucket.items() if not k.endswith("_vals")},
        "generic_to_template": _mean(bucket["generic_to_template_vals"]),
        "generic_to_container": _mean(bucket["generic_to_container_vals"]),
        "generic_to_project": _mean(bucket["generic_to_project_vals"]),
        "provided_to_configured_containment": provided_to_configured,
        "provided_to_used_containment": provided_to_used,
        "provided_passive_share": provided_passive_share,
        "provided_missing_share": provided_missing_share,
        "local_active_share": local_active_share,
        "primary_governance_read": primary_read,
    }


def build_governance_state_summary(
    state_rows: list[dict],
    summary_rows: list[dict],
) -> dict:
    """Aggregate optional governance-state outputs by domain.

    The renderer accepts either a compact pre-aggregated state summary or the
    detailed per-pattern governance-state file. Column names are intentionally
    read leniently so the narrative can remain compatible with early pipeline
    revisions while the comparison output stabilises.

    Aggregation is keyed by (domain, comparison_type) throughout, never by domain
    alone -- rows for enterprise_to_project/bc_to_project/enterprise_to_bc/
    enterprise_to_client must never be blended into the same number as
    template_to_project/container_to_project/generic_to_*, since they measure a
    different axis (scope level, not cascade stage). See
    docs/governance_narrative_scope_gap_audit.md A3.
    """
    by_type = defaultdict(_state_bucket)  # keyed by (domain, comparison_type)
    seen_comparison_types: set = set()

    # Compact summary rows, when provided, are authoritative for counts/shares.
    for row in summary_rows:
        dom = row.get("domain", "").strip()
        if not dom or dom in EXCLUDED_FROM_SCORING:
            continue
        ctype = row.get("comparison_type", "").strip()
        if ctype:
            seen_comparison_types.add(ctype)
        bucket = by_type[(dom, ctype)]

        for state, field in _STATE_COUNT_FIELDS.items():
            raw = _pick(row, field, f"{state}_n", f"n_{state}")
            if raw:
                try:
                    bucket[field] += int(float(raw))
                except ValueError:
                    pass

        for field in ("reference_all_count", "target_all_count", "target_used_count"):
            raw = _pick(row, field, f"n_{field}")
            if raw:
                try:
                    bucket[field] += int(float(raw))
                except ValueError:
                    pass

        _add_float(bucket["provided_to_configured_vals"], row, "provided_to_configured_containment")
        _add_float(bucket["provided_to_used_vals"], row, "provided_to_used_containment")
        _add_float(bucket["provided_passive_vals"], row, "provided_passive_share")
        _add_float(bucket["provided_missing_vals"], row, "provided_missing_share")
        _add_float(bucket["local_active_vals"], row, "local_active_share")

        if ctype == "generic_to_template":
            _add_float(bucket["generic_to_template_vals"], row, "provided_to_configured_containment", "all_containment_a_in_b_mean")
        elif ctype == "generic_to_container":
            _add_float(bucket["generic_to_container_vals"], row, "provided_to_configured_containment", "all_containment_a_in_b_mean")
        elif ctype == "generic_to_project":
            _add_float(bucket["generic_to_project_vals"], row, "provided_to_configured_containment", "all_containment_a_in_b_mean")

    # Detailed per-pattern rows fill gaps and support early-state files. Kept under
    # the same (dom, ctype) key as the compact loop so the two data sources merge
    # coherently below rather than one being type-separated and the other not.
    for row in state_rows:
        dom = row.get("domain", "").strip()
        if not dom or dom in EXCLUDED_FROM_SCORING:
            continue
        ctype = row.get("comparison_type", "").strip()
        if ctype:
            seen_comparison_types.add(ctype)
        if ctype and ctype not in _DIRECTED_GOVERNANCE_TYPES:
            continue
        bucket = by_type[(dom, ctype)]
        state = row.get("state", "").strip()
        field = _STATE_COUNT_FIELDS.get(state)
        if field:
            bucket[field] += 1
        if _truthy(row.get("in_reference_all")):
            bucket["reference_all_count"] += 1
        if _truthy(row.get("in_target_all")):
            bucket["target_all_count"] += 1
        if _truthy(row.get("in_target_used")):
            bucket["target_used_count"] += 1

    _warn_unrecognized_comparison_types(
        seen_comparison_types, _DIRECTED_GOVERNANCE_TYPES, "build_governance_state_summary"
    )

    # Finalise shares and labels: one fully-separated view per (domain, comparison_type)
    # for inspection/future use, and one merged per-domain view -- ONLY over the types
    # render_governance_state_section() actually renders today -- for the renderer.
    domains = {dom for dom, _ctype in by_type}
    result = {}
    for dom in domains:
        by_ctype = {}
        rendered_buckets = []
        for (d2, ctype), bucket in by_type.items():
            if d2 != dom:
                continue
            by_ctype[ctype or "(unspecified)"] = _finalize_state_bucket(bucket)
            if not ctype or ctype in _GOVERNANCE_STATE_RENDERED_TYPES:
                rendered_buckets.append(bucket)

        if not rendered_buckets:
            # This domain's ENTIRE governance-state signal is Group 3 (scope-level
            # fan-out) rows -- deferred, not rendered (see CASCADE_GROUP3_TYPES).
            # Omit it from the returned map entirely rather than storing an
            # all-None-but-truthy dict: render_domain_tiers()'s has_state check
            # (`any(state for _, _, state in group)`) treats ANY non-None dict as
            # "this tier group has state data" regardless of its values, which
            # would switch the WHOLE tier group's table to state columns -- hiding
            # bundle/passive columns for every domain in that group while showing
            # blank state values for this one. state_summary.get(dom) returning
            # None here is what every downstream consumer (assign_tier,
            # detect_anomalies, render_domain_tiers, the CSV writer) already
            # expects for "no governance-state input."
            continue
        merged = _finalize_state_bucket(_merge_state_buckets(rendered_buckets))
        merged["by_comparison_type"] = by_ctype
        result[dom] = merged
    return result


def load_delta_summary(delta_rows: list[dict]) -> dict:
    """
    Summarise legacy delta patterns by attribution category per comparison type.

    Supports both older delta schema (segment_id_a / segment_id_b) and newer
    directed schema (segment_id_reference / segment_id_target). This remains a
    fallback section; governance-state files are preferred when available because
    target-only deltas cannot surface inherited-but-unused patterns.
    """
    summary = defaultdict(lambda: defaultdict(lambda: {
        "ungoverned": 0, "container_governed": 0, "alt_template": 0
    }))
    for r in delta_rows:
        ref = _pick(r, "segment_id_reference", "segment_id_a")
        tgt = _pick(r, "segment_id_target", "segment_id_b")
        key = (ref, tgt)
        dom = r.get("domain", "")
        in_c = _truthy(r.get("in_any_container"))
        in_t = _truthy(r.get("in_any_template"))
        if in_t:
            summary[key][dom]["alt_template"] += 1
        elif in_c:
            summary[key][dom]["container_governed"] += 1
        else:
            summary[key][dom]["ungoverned"] += 1
    return summary


# ── section renderers ──────────────────────────────────────────────────────────
# ── section renderers ──────────────────────────────────────────────────────────


def render_header(analysis_date: str, corpus: dict, has_state_outputs: bool, legacy_used_fallback: bool) -> str:
    n_disc = len(corpus.get("disciplines", set()))
    disc_list = ", ".join(
        _disc_label(d)
        for d in sorted(corpus.get("disciplines", set()))
    ) or "Unknown"
    client_list = ", ".join(sorted(corpus.get("clients", set()))) or "Unknown"
    state_note = (
        "Explicit governance-state outputs are present, so provided/used/passive/missing/local signals are used in the interpretation."
        if has_state_outputs else
        "Explicit governance-state outputs are not present; inherited-but-unused and local-active findings are inferred only indirectly."
    )
    used_note = (
        "Used-view columns were not found in the summary schema, so used-view measures fall back to legacy all-view columns where necessary. Claims depending on active use are therefore limited."
        if legacy_used_fallback else
        "Used-view columns are present in the summary schema and are kept separate from all-view configured vocabulary."
    )
    return f"""# Revit Configuration Governance Analysis
## Stantec Consulting — BIM Fingerprint System
### Analysis Date: {analysis_date}

---

## Executive Summary

This document is a deterministic discovery and interpretation report. It identifies where
Revit configuration vocabulary appears to provide a stable common base, where upstream
content is carried into downstream files, where project files actively exercise that
content, and where local or project-created vocabulary may require governance review.

The report does **not** approve standards, assign ownership, measure compliance, or label
teams as compliant/non-compliant. Baseline language should be read as a candidate for
leadership review, not as a decision.

{state_note} {used_note}

---

## What This Analysis Is

This document summarises findings from the Revit Fingerprint System — a pipeline that
extracts configuration fingerprints from Revit project files and measures how consistently
those configurations flow from enterprise baseline content through templates, coordination
files, and live project models.

The goal is to identify which parts of Stantec's Revit configuration landscape have
evidence of convergence, propagation, active use, passive inheritance, local creation, or
missing downstream content. These findings frame governance questions; they do not decide
standards.

---

## The Corpus

This analysis covers **{corpus['total']} Revit files**:

| Role | Count | Description |
|---|---:|---|
| Templates (.rte) | {corpus['Template']} | Standard Revit template files — provided-vocabulary carriers |
| Coordination Files | {corpus['Container']} | Project coordination, container, and linked model files |
| Project Models | {corpus['Project']} | Live project Revit files where used-view interpretation is most meaningful |

Disciplines represented ({n_disc}): {disc_list}.
Clients represented: {client_list}.

Unavailable or future segment dimensions, such as project type, business center, and
region, should not be inferred from this output unless supplied by upstream CSVs.

---

## How to Read the Analysis

The analysis separates **provided vocabulary**, **configured downstream vocabulary**, and
**active project use**. This prevents a template pattern that is merely carried into a
project from being mistaken for a pattern actively used in delivery.

**All view:** complete configured vocabulary present in a file, including inherited stock
content.

**Used view:** project vocabulary excluding conclusively purgeable or unused records.
Used-view interpretation is meaningful primarily for **Project** targets. Template,
Generic, and most Container roles are provided-vocabulary references, not production-use
environments.

**Containment:** evidence that one vocabulary is present inside another. It is evidence of
reuse or propagation, not proof of governance approval or active use.

**Cross-client similarity:** evidence of shared practice across client portfolios. Low
cross-client similarity is not automatically bad; it matters when it creates onboarding,
portability, governance, or maintenance burden.

**Governance-state outputs:** when provided, these separate `provided_and_used`,
`provided_but_passive`, `provided_but_missing`, `local_active`, `local_passive`, and
`local_unbundled` signals.

Scores range from 0 to 1. In this report, high scores indicate stronger evidence for a
candidate baseline or common base; they do not automatically ratify a standard.

---

## Governance Cascade

```
Generic / Enterprise Baseline
    ↓  [generic → template/container/project containment]
Templates (.rte)
    ↓  [template → coordination file / project containment]
Coordination Files
    ↓  [coordination file → project containment]
Project Models — all configured vocabulary
    ↓  [project all → project used]
Active Project Use
```

"""


def render_evidence_authority_header(
    package_schema_version: str,
    generator_identity: str,
    emit_evidence_package: bool = True,
) -> str:
    """States this document's own epistemic role and authority ordering within
    the governance evidence package. Added alongside governance_package_manifest.json/
    _health.json/_evidence_map.json/_findings.json (see docs/governance_evidence_package.md)
    -- this document remains a controlled_interpretation artifact, not authoritative
    evidence, and no LLM is involved in producing it or any other artifact in this package.

    The health/findings/evidence-map pointer lines are gated on emit_evidence_package --
    when a caller passes --no-emit-evidence-package, those three files are never
    written, so this document must not point readers at files that don't exist.
    """
    package_pointers = (
        f"""
> **Package health:** `governance_package_health.json` (schema {package_schema_version})
> **Structured findings:** `governance_findings.json`
> **Evidence navigation:** `governance_evidence_map.json`
"""
        if emit_evidence_package else
        "\n> This run was generated with `--no-emit-evidence-package`, so no "
        "package health, structured findings, or evidence-map file exists "
        "alongside this document.\n"
    )
    return f"""> **Artifact role:** Convenience summary and controlled interpretation
> (`authority_level: {AUTHORITY_CONTROLLED_INTERPRETATION}`). This document is
> template-rendered from the deterministic CSVs below by `{generator_identity}` --
> no LLM is involved in producing this narrative or any other artifact in this package.
>
> **Authority ordering:** package health and the source comparison CSVs
> (`cross_segment_summary.csv`, `cross_segment_pooled.csv`) outrank the
> deterministic rollups below them (`governance_domain_summary.csv`,
> `governance_client_summary.csv`), which in turn outrank this narrative's prose.
> If this document disagrees with a rollup CSV or a source CSV, the CSV wins.
{package_pointers}"""


def render_governance_state_model(has_state_outputs: bool) -> str:
    limitation = "" if has_state_outputs else (
        "\n> Governance-state CSVs were not provided. Inherited-but-unused and "
        "local-active roll-up findings are therefore inferred only indirectly from "
        "summary, bundle, and legacy delta outputs.\n"
    )
    return """## Governance State Model

For directed governance comparisons, the analysis separates three questions:

1. Was the pattern provided by an upstream governance source?
2. Was it carried into the downstream file vocabulary?
3. Was it actively used in project delivery?

| State | Meaning |
|---|---|
| `provided_and_used` | Upstream content reached projects and is active in delivery. |
| `provided_but_passive` | Upstream content is carried downstream but not actively used by project files. |
| `provided_but_missing` | Upstream content did not reach the downstream target. |
| `local_active` | Downstream/project-created or modified content is actively used; possible roll-up candidate. |
| `local_passive` | Local/downstream content exists but is not actively used. |
| `local_unbundled` | Local content exists but has weak or no bundle evidence. |

""" + limitation



def render_domain_tiers(cascade: dict, state_summary: Optional[dict] = None) -> str:
    # Sort domains by DoD-safe governance classification then score.
    state_summary = state_summary or {}
    scored = []
    for dom, d in cascade.items():
        if not _has_renderable_cascade_signal(d):
            # Scope-only domain (Group 3 fan-out data only) -- captured in
            # `cascade` but not yet tiered/rendered. See CASCADE_GROUP3_TYPES.
            continue
        state = state_summary.get(dom)
        tier = assign_tier(d, state)
        primary = d["tp"] if d["tp"] is not None else d["cp"]
        scored.append((dom, tier, d, state, primary or 0.0))

    scored.sort(key=lambda x: (TIER_ORDER.get(x[1], 99), -x[4], DOMAIN_LABELS.get(x[0], x[0])))

    sections = ["## Domain Governance Classification\n"]
    tier_groups = defaultdict(list)
    for dom, tier, d, state, _ in scored:
        tier_groups[tier].append((dom, d, state))

    tier_intros = {
        TIER_STRONG_BASELINE: (
            "These domains have strong propagation evidence and no material state exception in the available data. "
            "They are candidates for baseline ratification review, not already-approved standards."
        ),
        TIER_BASELINE_LOCAL_REVIEW: (
            "These domains have meaningful baseline evidence, but local-active, passive, missing, or active-use signals "
            "must be resolved before leadership treats them as baseline candidates for approval review."
        ),
        TIER_BASELINE_CONTAINER_GAP: (
            "These domains have strong end-to-end propagation while coordination files are not the main governance vehicle. "
            "Review whether direct template inheritance is the intended path."
        ),
        TIER_INVESTIGATE: (
            "These domains have useful common-base evidence but need review before baseline language is safe."
        ),
        TIER_ACTIVE_LOCAL: (
            "These domains show material active local/project-created vocabulary. Review for roll-up, playbook, approved-list, "
            "permitted-variant, or exception-governance treatment."
        ),
        TIER_MODERATE_VARIATION: (
            "These domains show meaningful variation. Governance may require discipline-specific treatment or explicit acceptance "
            "of client/project variation."
        ),
        TIER_SPARSE_LIMITED: (
            "These domains are sparse or presence-limited. The first governance question is whether the domain should be expected "
            "across the population before convergence is assessed."
        ),
        TIER_HIGH_FRAGMENTATION: (
            "These domains show high variation. A single baseline is not supported by the current data."
        ),
        TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE: (
            "These domains have no enterprise-wide (fully unscoped) evidence, but DO have pooled "
            "same-business-center-pair evidence (see the Group 1 Propagation by Scope section below). "
            "This is business-center-level evidence only — not an enterprise reading — and should not "
            "be treated as equivalent to the tiers above it."
        ),
        TIER_INSUFFICIENT: (
            "These domains have too little usable evidence in the current corpus for a reliable governance read."
        ),
    }

    sections.append(
        "> **Classification key:** These labels describe evidence posture only. "
        "They do not approve standards, assign ownership, or measure compliance. "
        "Materiality thresholds used by this renderer: local-active ≥15%, passive ≥20%, "
        "missing ≥20%, and strong-baseline active-use containment ≥75%. These thresholds "
        "are deterministic narrative guardrails, not governance policy. "
        "**Tight** reliability means file-pair agreement is consistently high. "
        "**Presence-based** means the score may reflect whether files carry a domain at all, not agreement quality. "
        "**Sparse** means the domain appears in too few files for a simple mean to support broad governance claims.\n"
    )

    ordered_tiers = [
        TIER_STRONG_BASELINE,
        TIER_BASELINE_LOCAL_REVIEW,
        TIER_BASELINE_CONTAINER_GAP,
        TIER_INVESTIGATE,
        TIER_ACTIVE_LOCAL,
        TIER_MODERATE_VARIATION,
        TIER_SPARSE_LIMITED,
        TIER_HIGH_FRAGMENTATION,
        TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE,
        TIER_INSUFFICIENT,
    ]

    for tier in ordered_tiers:
        group = tier_groups.get(tier, [])
        if not group:
            continue
        n = len(group)
        sections.append(f"### {tier} ({n} domain{'s' if n != 1 else ''})\n")
        sections.append(tier_intros.get(tier, "") + "\n")

        has_state = any(state for _, _, state in group)
        if has_state:
            sections.append(
                "| Domain | G→Template | G→Container | G→Project | T→Container | T→Project | C→Project | Cross-Client | Reliability | Provided→Used | Local Active | Passive | Missing |"
            )
            sections.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|")
        else:
            sections.append(
                "| Domain | G→Template | G→Container | G→Project | T→Container | T→Project | C→Project | Cross-Client | Reliability | Bundle Density | Passive Inherit. |"
            )
            sections.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|")

        for dom, d, state in group:
            label = DOMAIN_LABELS.get(dom, dom)
            reliability = score_reliability(d)
            pi_flag = " ⚠️" if dom in PASSIVE_INHERITANCE_RISK_DOMAINS else ""
            if has_state:
                state = state or {}
                row = (
                    f"| {label}{pi_flag} "
                    f"| {fmt(d.get('gt'))} "
                    f"| {fmt(d.get('gc'))} "
                    f"| {fmt(d.get('gp'))} "
                    f"| {fmt(d['tc'])} "
                    f"| {fmt(d['tp'])} "
                    f"| {fmt(d['cp'])} "
                    f"| {fmt(d['xc'])} "
                    f"| {reliability} "
                    f"| {pct(state.get('provided_to_used_containment'))} "
                    f"| {pct(state.get('local_active_share'))} "
                    f"| {pct(state.get('provided_passive_share'))} "
                    f"| {pct(state.get('provided_missing_share'))} |"
                )
            else:
                row = (
                    f"| {label}{pi_flag} "
                    f"| {fmt(d.get('gt'))} "
                    f"| {fmt(d.get('gc'))} "
                    f"| {fmt(d.get('gp'))} "
                    f"| {fmt(d['tc'])} "
                    f"| {fmt(d['tp'])} "
                    f"| {fmt(d['cp'])} "
                    f"| {fmt(d['xc'])} "
                    f"| {reliability} "
                    f"| {pct(d.get('bundle_share_all'), 0)} "
                    f"| {pct(d.get('passive_indicator'), 0)} |"
                )
            sections.append(row)

        sections.append("")

        for dom, d, state in group:
            notes = detect_anomalies(dom, d, state)
            if notes:
                label = DOMAIN_LABELS.get(dom, dom)
                sections.append(f"**{label}:** " + " ".join(notes) + "\n")

    return "\n".join(sections)


def render_generic_baseline_scope_section(cascade: dict) -> str:
    """Render the Option C per-target-scope-level breakdown for gt/gc/gp.

    The scope buckets (enterprise/client/bc/discipline and combinations) are
    dynamic, not a small fixed set like disciplines -- a per-domain fixed-column
    table would either explode in width or silently drop combined-dimension
    buckets (e.g. "client_discipline"). One row per (domain, scope) instead, so
    every bucket that actually occurred is shown without inventing new columns.
    """
    rows = []
    for dom, d in cascade.items():
        scopes = set(d.get("gt_by_scope") or {}) | set(d.get("gc_by_scope") or {}) | set(d.get("gp_by_scope") or {})
        for scope in scopes:
            rows.append((
                dom, scope,
                (d.get("gt_by_scope") or {}).get(scope),
                (d.get("gc_by_scope") or {}).get(scope),
                (d.get("gp_by_scope") or {}).get(scope),
            ))
    if not rows:
        return ""

    lines = [
        "## Generic Baseline Propagation by Scope\n",
        "Breaks the Generic/Enterprise Baseline → Template/Container/Project cascade "
        "(the top rung of the Governance Cascade diagram above) down by the TARGET's "
        "own scope level, instead of only the single broadest (enterprise-wide) "
        "reading. **enterprise** is the same value already shown as G→Template/"
        "G→Container/G→Project in the Domain Governance Classification table above; "
        "the other rows are client-/business-center-/discipline-specific evidence "
        "that a prior pass deliberately excluded from that headline number to avoid "
        "blending distinct scope grains together, but which is real "
        "baseline-propagation evidence in its own right.\n",
        "| Domain | Scope | G→Template | G→Container | G→Project |",
        "|---|---|---:|---:|---:|",
    ]
    for dom, scope, gt_v, gc_v, gp_v in sorted(
        rows, key=lambda r: (DOMAIN_LABELS.get(r[0], r[0]), r[1] != "enterprise", r[1])
    ):
        lines.append(
            f"| {DOMAIN_LABELS.get(dom, dom)} | {scope} "
            f"| {fmt(gt_v)} | {fmt(gc_v)} | {fmt(gp_v)} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_group1_scope_section(cascade: dict) -> str:
    """Render the bc-pooled fallback per-scope-pair breakdown for tc/cp/tp.

    Mirrors render_generic_baseline_scope_section() (Group 2's Option C
    section) exactly, adapted for Group 1's two-sided scope key: each row is
    keyed by a (scope_a, scope_b) PAIR (e.g. "enterprise::enterprise", "bc::bc"),
    not a single target scope label, since neither side of a Group 1
    comparison is gated to a fixed role population the way Group 2's Generic
    reference side is. "enterprise::enterprise" is the same value already shown
    as T→Container/T→Project/C→Project in the Domain Governance Classification
    table above; every other row (typically "bc::bc") is the pooled evidence
    that used to be silently discarded -- see
    docs/governance_narrative_group1_scope_gap_investigation.md. Rendered only
    for domains that actually have by-scope data; omitted entirely otherwise.
    """
    rows = []
    for dom, d in cascade.items():
        scope_pairs = (
            set(d.get("tc_by_scope") or {})
            | set(d.get("cp_by_scope") or {})
            | set(d.get("tp_by_scope") or {})
        )
        for scope_pair in scope_pairs:
            rows.append((
                dom, scope_pair,
                (d.get("tc_by_scope") or {}).get(scope_pair),
                (d.get("tp_by_scope") or {}).get(scope_pair),
                (d.get("cp_by_scope") or {}).get(scope_pair),
            ))
    if not rows:
        return ""

    lines = [
        "## Group 1 Propagation by Scope\n",
        "Breaks the Template → Coordination File → Project cascade (T→Container, "
        "T→Project, C→Project in the Domain Governance Classification table above) "
        "down by the (a-side scope, b-side scope) pair of each comparison, instead "
        "of only the single broadest (enterprise-wide) pair. "
        "**enterprise::enterprise** is the same value already shown as T→Container/"
        "T→Project/C→Project above; every other row is scoped evidence that a prior "
        "pass discarded whenever no fully enterprise-wide pair existed, which is why "
        "most domains previously showed as Insufficient Evidence despite this "
        "evidence being present. The scope on each side is not always "
        "business-center: it can be client-, discipline-, business-center-scoped, "
        "or a combination — read the scope label itself (e.g. `client::bc`, "
        "`bc_discipline::bc`) to see which. Only a domain with a genuine "
        "**bc::bc** row (both sides scoped to the SAME business center) reaches "
        "the **Insufficient Evidence — Enterprise; BC-Level Evidence Available** "
        "tier above; other scope pairs are real evidence in their own right but do "
        "not by themselves place a domain into that tier.\n",
        "| Domain | Scope | T→Container | T→Project | C→Project |",
        "|---|---|---:|---:|---:|",
    ]
    for dom, scope_pair, tc_v, tp_v, cp_v in sorted(
        rows, key=lambda r: (DOMAIN_LABELS.get(r[0], r[0]), r[1] != "enterprise::enterprise", r[1])
    ):
        lines.append(
            f"| {DOMAIN_LABELS.get(dom, dom)} | {scope_pair} "
            f"| {fmt(tc_v)} | {fmt(tp_v)} | {fmt(cp_v)} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_discipline_section(cascade: dict, summary_rows: list[dict]) -> str:
    """Render per-discipline within-project coherence and cascade summary."""
    lines = ["## Discipline Analysis\n"]

    # Gather within-project by discipline
    disc_domain_wp = defaultdict(lambda: defaultdict(list))
    disc_file_counts = {}
    for r in summary_rows:
        if r["comparison_type"] != "within_project":
            continue
        disc = _pick(r, "discipline_label_a")
        v = pf(_col(r, "jaccard_mean"))
        if disc and v is not None:
            disc_domain_wp[disc][r["domain"]].append(v)
            if disc not in disc_file_counts:
                disc_file_counts[disc] = int(r["n_files_a"]) if r["n_files_a"] else 0

    # Has-template flag
    template_discs = set()
    for r in summary_rows:
        disc = _pick(r, "discipline_label_a")
        if r["governance_role_a"] == "Template" and disc:
            template_discs.add(disc)

    for disc in sorted(disc_domain_wp.keys()):
        label = _disc_label(disc)
        n_files = disc_file_counts.get(disc, "?")
        has_template = disc in template_discs

        domain_means = {
            d: statistics.mean(v)
            for d, v in disc_domain_wp[disc].items()
            if v and d not in EXCLUDED_FROM_SCORING
        }
        if not domain_means:
            continue

        overall = statistics.mean(domain_means.values())
        strongest = sorted(domain_means.items(), key=lambda x: -x[1])[:3]
        weakest = sorted(domain_means.items(), key=lambda x: x[1])[:3]

        lines.append(f"### {label}\n")
        lines.append(
            f"Files in corpus: **{n_files}**. "
            f"{'Discipline-specific templates exist. ' if has_template else 'No discipline-specific templates — coordination files are the primary governance source. '}"
            f"Mean within-population coherence: **{pct(overall)}**.\n"
        )

        lines.append("**Most consistent domains:**")
        for d, v in strongest:
            lines.append(f"- {DOMAIN_LABELS.get(d, d)}: {pct(v)}")

        lines.append("")
        lines.append("**Least consistent domains:**")
        for d, v in weakest:
            lines.append(f"- {DOMAIN_LABELS.get(d, d)}: {pct(v)}")

        lines.append("")

    return "\n".join(lines)



def _format_domain_items(items: list[tuple[str, float]], limit: int = 3) -> str:
    if not items:
        return "—"
    return ", ".join(
        f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})"
        for d, v in items[:limit]
    )


def _client_onboarding_profile(r: dict) -> dict:
    """Return deterministic onboarding implications from client-level metrics."""
    n = r.get("n_files", 0) or 0
    xc = r.get("xc_mean")
    wp = r.get("wp_mean")

    if wp is None:
        internal_read = "Internal coherence is unavailable; onboarding should not rely on this run alone."
    elif wp >= 0.75:
        internal_read = "Stable internal portfolio; a new team member can likely rely on a repeatable client/project vocabulary."
    elif wp >= 0.55:
        internal_read = "Mixed internal portfolio; a new team member needs a client orientation plus project-specific checks."
    else:
        internal_read = "High internal variation; learning this client likely means learning several local variants."

    if xc is None:
        portability_read = "Cross-client portability is unavailable from this run."
    elif xc >= 0.45:
        portability_read = "High portability from the wider corpus is plausible, subject to domain-level review."
    elif xc >= 0.33:
        portability_read = "Some common base is portable, but client-specific departures should be documented."
    else:
        portability_read = "Client-specific orientation is required; wider-corpus assumptions may not transfer cleanly."

    if n < 10:
        confidence_read = "Low sample size; treat as a prompt for review, not a settled client profile."
    elif n < 25:
        confidence_read = "Moderate sample size; useful for orientation but still sensitive to project mix."
    else:
        confidence_read = "Good sample size for an initial onboarding read."

    common_base = _format_domain_items(r.get("strongest", []))
    variant_burden = _format_domain_items(r.get("weakest", []))

    # Only a client with a KNOWN non-healthcare sector gets the different-sector
    # implication -- an unclassified client (sector == "unknown") must not be
    # treated as confirmed non-healthcare, since is_healthcare=False alone can't
    # distinguish "known different sector" from "we don't know."
    sector = r.get("sector", "unknown")
    if sector not in ("unknown", "healthcare"):
        operating_implication = (
            "Do not use healthcare baseline assumptions as the default. Treat this as a separate sector profile."
        )
    elif wp is not None and wp < 0.55:
        operating_implication = (
            "Create project-start reference material and review local variants before assigning staff across projects."
        )
    elif xc is not None and xc < 0.33:
        operating_implication = (
            "Document client-specific departures from the wider corpus before using firmwide playbooks unchanged."
        )
    elif wp is not None and wp >= 0.75:
        operating_implication = (
            "A compact client playbook is likely useful: capture the common base and the recurring exceptions."
        )
    else:
        operating_implication = (
            "Use a short client orientation plus domain-specific checks for the weakest-alignment areas."
        )

    return {
        "internal_read": internal_read,
        "portability_read": portability_read,
        "confidence_read": confidence_read,
        "common_base": common_base,
        "variant_burden": variant_burden,
        "operating_implication": operating_implication,
    }


def render_onboarding_section(client_rows: list[dict]) -> str:
    """Render client-specific onboarding and operating implications."""
    if not client_rows:
        return ""

    lines = [
        "## Onboarding / Operating Implications\n",
        "This section translates client-level consistency into practical onboarding reads. "
        "It does not judge whether divergence is good or bad. It identifies where a new team "
        "member can probably rely on a common base, where client-specific orientation is needed, "
        "and where project-to-project variants should be made explicit.\n",
        "| Client | New-team-member read | Common base to teach first | Variant / coaching burden | Operating implication |",
        "|---|---|---|---|---|",
    ]

    for r in client_rows:
        profile = _client_onboarding_profile(r)
        read = f"{profile['internal_read']} {profile['portability_read']} {profile['confidence_read']}"
        lines.append(
            f"| {r['client']} "
            f"| {read} "
            f"| {profile['common_base']} "
            f"| {profile['variant_burden']} "
            f"| {profile['operating_implication']} |"
        )

    lines += [
        "",
        "### How leadership can use this\n",
        "- Use **common base** domains as starting points for onboarding guides or client playbooks.",
        "- Use **variant / coaching burden** domains as prompts for reference examples, project-start checks, or discipline/client-specific coaching.",
        "- Treat low sample counts as review triggers, not conclusions.",
        "- Do not treat lower cross-client similarity as failure unless it affects staff portability, governance clarity, or standards maintenance.\n",
    ]
    return "\n".join(lines)

def render_client_section(client_rows: list[dict]) -> str:
    lines = ["## Client Analysis\n"]
    lines.append(
        "Cross-client similarity measures how consistent project configurations are "
        "across different client engagements, independent of the formal standard. "
        "High scores indicate practice convergence; low scores indicate client-specific divergence.\n"
    )

    lines.append("| Client | Files | Alignment | Cross-Client Similarity | Internal Coherence | Confidence |")
    lines.append("|---|---|---|---|---|---|")
    for r in client_rows:
        lines.append(
            f"| {r['client']} "
            f"| {r['n_files']} "
            f"| {r['tier']} "
            f"| {fmt(r['xc_mean'])} "
            f"| {fmt(r['wp_mean'])} "
            f"| {r['confidence_note']} |"
        )

    lines.append("")
    lines.append(
        "> **Note on scores:** Cross-client similarity in the 0.30–0.36 range is not a "
        "failure. It reflects that project configuration is partly client-specific. "
        "The scores show where common ground exists, not that divergence is wrong.\n"
    )

    # Per-client narrative
    for r in client_rows:
        lines.append(f"### {r['client']}\n")
        lines.append(
            f"**{r['n_files']} project files.** "
            f"Alignment tier: {r['tier']}. "
            f"Cross-client similarity: {fmt(r['xc_mean'])}. "
            f"Internal coherence: {fmt(r['wp_mean'])}.\n"
        )
        if r["strongest"]:
            strong_str = ", ".join(
                f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
            )
            lines.append(f"Strongest alignment domains: {strong_str}.\n")
        if r["weakest"]:
            weak_str = ", ".join(
                f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
            )
            lines.append(f"Weakest alignment domains: {weak_str}.\n")
        # Only note a different sector when it's actually KNOWN (not "unknown") --
        # an unclassified client must not be presented as confirmed non-healthcare.
        if r.get("sector", "unknown") not in ("unknown", "healthcare"):
            lines.append(
                "_Non-healthcare sector — configuration baseline differs from healthcare "
                "client comparisons. Excluded from healthcare cross-client convergence reads._\n"
            )
        lines.append("")

    return "\n".join(lines)



def render_governance_state_section(state_summary: dict) -> str:
    """Render explicit all/used governance-state findings when available."""
    if not state_summary:
        return ""

    lines = [
        "## Governance State / Roll-Up Analysis\n",
        "This section uses explicit governance-state outputs when available. It separates "
        "standards that were provided and used, standards that were provided but passive, "
        "provided content that is missing downstream, and active local/project-created "
        "patterns that may deserve roll-up review.\n",
        "> Count fields in this section are summed governance-state rows unless the upstream "
        "state summary has already deduplicated them. Use shares and rankings for leadership "
        "claims unless a unique-pattern-count guarantee is supplied by the pipeline.\n",
    ]

    def top_by(field: str, limit: int = 10):
        rows = [
            (dom, d.get(field))
            for dom, d in state_summary.items()
            if d.get(field) is not None
        ]
        rows.sort(key=lambda x: -x[1])
        return rows[:limit]

    generic_rows = []
    for dom, d in state_summary.items():
        if any(d.get(k) is not None for k in ("generic_to_template", "generic_to_container", "generic_to_project")):
            generic_rows.append((dom, d))
    if generic_rows:
        generic_rows.sort(key=lambda x: DOMAIN_LABELS.get(x[0], x[0]))
        lines.append("### Generic / Enterprise Baseline Propagation\n")
        lines.append("| Domain | Generic→Template | Generic→Container | Generic→Project |")
        lines.append("|---|---:|---:|---:|")
        for dom, d in generic_rows[:20]:
            lines.append(
                f"| {DOMAIN_LABELS.get(dom, dom)} "
                f"| {fmt(d.get('generic_to_template'))} "
                f"| {fmt(d.get('generic_to_container'))} "
                f"| {fmt(d.get('generic_to_project'))} |"
            )
        lines.append("")

    passive = top_by("provided_passive_share", 10)
    local_active = top_by("local_active_share", 10)
    missing = top_by("provided_missing_share", 10)

    if passive:
        lines.append("### Highest Inherited-but-Passive Signal\n")
        lines.append("These are candidates for starter-content, pruning, approved-list, or exception-governance review; passive inheritance is not automatically bloat.\n")
        lines.append("| Domain | Passive Share | Provided→Used | Relative Signal |")
        lines.append("|---|---:|---:|---|")
        for dom, val in passive:
            d = state_summary[dom]
            lines.append(
                f"| {DOMAIN_LABELS.get(dom, dom)} "
                f"| {pct(val)} "
                f"| {pct(d.get('provided_to_used_containment'))} "
                f"| {d.get('primary_governance_read', '')} |"
            )
        lines.append("")

    if local_active:
        lines.append("### Highest Active Local / Roll-Up Candidate Signal\n")
        lines.append("These domains should be reviewed to decide whether active local practice represents roll-up content, client/discipline playbook material, permitted variants, or legitimate project-specific exceptions.\n")
        lines.append("| Domain | Local Active Share | Primary Read |")
        lines.append("|---|---:|---|")
        for dom, val in local_active:
            d = state_summary[dom]
            lines.append(
                f"| {DOMAIN_LABELS.get(dom, dom)} "
                f"| {pct(val)} "
                f"| {d.get('primary_governance_read', '')} |"
            )
        lines.append("")

    if missing:
        lines.append("### Highest Provided-but-Missing Signal\n")
        lines.append("These domains need propagation review before the provided vocabulary can be treated as a dependable downstream baseline.\n")
        lines.append("| Domain | Missing Share | Provided→Configured |")
        lines.append("|---|---:|---:|")
        for dom, val in missing:
            d = state_summary[dom]
            lines.append(
                f"| {DOMAIN_LABELS.get(dom, dom)} "
                f"| {pct(val)} "
                f"| {pct(d.get('provided_to_configured_containment'))} |"
            )
        lines.append("")

    return "\n".join(lines)

def render_delta_section(delta_summary: dict) -> str:
    """Render ungoverned drift section from delta data."""
    if not delta_summary:
        return ""

    lines = [
        "## Configuration Drift Analysis\n",
        "Delta patterns are configurations present in project or coordination files "
        "but absent from the reference template set. They are classified by source:\n",
        "- **Ungoverned drift:** Not in any template or coordination file — "
        "project-originated configuration accumulating outside any reference file.\n",
        "- **Container-governed:** Present in coordination files but not templates — "
        "governed at the coordination file layer, not the template layer.\n",
        "- **Alternate template:** Present in a different template — may indicate "
        "wrong template in use or cross-client convergence patterns.\n",
        "",
    ]

    # Aggregate across all comparison pairs to domain-level totals
    dom_totals = defaultdict(lambda: {"ungoverned": 0, "container_governed": 0, "alt_template": 0})
    for pair_data in delta_summary.values():
        for dom, counts in pair_data.items():
            for k, v in counts.items():
                dom_totals[dom][k] += v

    # Sort by ungoverned count descending
    sorted_domains = sorted(
        dom_totals.items(), key=lambda x: -x[1]["ungoverned"]
    )[:15]

    if sorted_domains:
        lines.append("### Ungoverned Drift by Domain (top 15)\n")
        lines.append("| Domain | Ungoverned | Container-Governed | Alt-Template |")
        lines.append("|---|---|---|---|")
        for dom, counts in sorted_domains:
            label = DOMAIN_LABELS.get(dom, dom)
            lines.append(
                f"| {label} "
                f"| {counts['ungoverned']} "
                f"| {counts['container_governed']} "
                f"| {counts['alt_template']} |"
            )
        lines.append("")

    return "\n".join(lines)


def render_union_reuse_summary(
    union_inventory_rows: list,
    reuse_distribution_rows: list,
    matrix_manifest_rows: list,
) -> Optional[str]:
    if not union_inventory_rows and not reuse_distribution_rows and not matrix_manifest_rows:
        return None

    lines = ["## Union Inventory Reuse Summary\n"]

    if reuse_distribution_rows:
        bucket_order = [
            "corpus_wide",
            "client_wide",
            "multi_project",
            "single_project",
            "emerging",
            "single_file",
            "unclassified",
        ]
        bucket_priority = {bucket: i for i, bucket in enumerate(bucket_order)}
        pattern_buckets = {}
        pattern_domains = {}
        domain_counts = defaultdict(lambda: {bucket: 0 for bucket in bucket_order})
        for row in reuse_distribution_rows:
            if row.get("classification_status") != "ok":
                continue
            if row.get("inventory_status") != "ok":
                continue
            domain = row.get("domain", "")
            join_hash = row.get("join_hash", "")
            if not join_hash:
                continue
            bucket = row.get("reuse_bucket", "unclassified") or "unclassified"
            if bucket not in bucket_order:
                bucket = "unclassified"
            pattern_key = (
                row.get("view_scope", ""),
                row.get("governance_role", ""),
                row.get("discipline_label", ""),
                row.get("unit_system", ""),
                domain,
                join_hash,
            )
            previous_bucket = pattern_buckets.get(pattern_key)
            if (
                previous_bucket is None
                or bucket_priority[bucket] < bucket_priority[previous_bucket]
            ):
                pattern_buckets[pattern_key] = bucket
                pattern_domains[pattern_key] = domain

        for pattern_key, bucket in pattern_buckets.items():
            domain_counts[pattern_domains[pattern_key]][bucket] += 1

        sorted_domains = sorted(
            domain_counts.items(),
            key=lambda item: (-item[1]["corpus_wide"], item[0]),
        )

        lines.append("**Reuse breadth summary**\n")
        lines.append("| domain | corpus_wide | client_wide | multi_project | single_project | emerging | single_file | unclassified |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for domain, counts in sorted_domains[:20]:
            lines.append(
                f"| {domain} "
                f"| {counts['corpus_wide']} "
                f"| {counts['client_wide']} "
                f"| {counts['multi_project']} "
                f"| {counts['single_project']} "
                f"| {counts['emerging']} "
                f"| {counts['single_file']} "
                f"| {counts['unclassified']} |"
            )
        if len(sorted_domains) > 20:
            lines.append(f"\nTable limited to 20 domains; {len(sorted_domains) - 20} domains not shown.")
        lines.append("")

    if matrix_manifest_rows:
        lines.append("**Matrix manifest metadata**\n")
        lines.append("Matrix availability is determined by each matrix CSV `value_status`; manifest rows are descriptive metadata.\n")
        for row in matrix_manifest_rows:
            interpretation = row.get("interpretation", "")
            if len(interpretation) > 120:
                interpretation = interpretation[:120].rstrip()
            lines.append(
                f"- {row.get('matrix_name', '')}: {row.get('metric', '')} ({interpretation})"
            )
        blocking_statuses = {
            "no_patterns",
            "missing_domain_patterns",
            "missing_membership_matrix",
            "used_view_unavailable",
        }
        blocked_domains = {
            row.get("domain", "")
            for row in union_inventory_rows
            if row.get("governance_role") == "Project"
            and row.get("inventory_status") in blocking_statuses
        }
        if blocked_domains:
            lines.append(f"- Project union inventory domains with blocking status: {len(blocked_domains)}")
    elif union_inventory_rows or reuse_distribution_rows:
        lines.append("Matrix manifest not provided; matrix availability unknown.")

    return "\n".join(lines)


# Cross-client-convergence "strong" and client-coherence "low" thresholds are
# intentionally the same literal values (0.70 / 0.45) already used in
# detect_anomalies() (lines ~1438-1442) and the client-tier assignment inside
# build_client_summary() -- duplicated here as in those places rather than
# centralized, matching this generator's current state (policy/threshold
# externalization is deferred to a later PR; see docs/governance_evidence_package.md
# and CLAUDE.md's Sig-Hash/Shape-Gating precedent for the externalization pattern
# this will eventually follow).

_RULE_STRONG_BASELINE = "GOV-TIER-STRONG-BASELINE"
_RULE_BASELINE_CANDIDATE = "GOV-TIER-BASELINE-CANDIDATE"
_RULE_LOCAL_REVIEW_REQUIRED = "GOV-TIER-LOCAL-REVIEW-REQUIRED"
_RULE_ACTIVE_LOCAL_PRACTICE = "GOV-TIER-ACTIVE-LOCAL-PRACTICE"
_RULE_HIGH_FRAGMENTATION = "GOV-TIER-HIGH-FRAGMENTATION"
_RULE_INSUFFICIENT_EVIDENCE = "GOV-TIER-INSUFFICIENT-EVIDENCE"
_RULE_XC_STRONG_CONVERGENCE = "GOV-XC-STRONG-CONVERGENCE"
_RULE_PASSIVE_INHERITANCE_RISK = "GOV-PASSIVE-INHERITANCE-RISK"
_RULE_CLIENT_LOW_COHERENCE = "GOV-CLIENT-LOW-COHERENCE"
_RULE_LEADERSHIP_QUESTION = "GOV-LEADERSHIP-QUESTION"

_FINDING_LIMITS_STANDARD = [
    "Evidence posture only -- does not approve standards, assign ownership, "
    "measure compliance, or label teams as compliant/non-compliant "
    "(governance_narrative_context.md's own stated scope boundary).",
    "Does not establish organizational intent.",
]


def _classify_domains_for_findings(cascade: dict, state_summary: Optional[dict] = None) -> dict:
    """Single source of truth for domain-tier-derived classification buckets,
    keyed by raw domain id (not DOMAIN_LABELS display text). Shared by
    build_structured_findings() and render_findings_and_recommendations() so
    the two never drift into independent implementations of the same rule --
    see docs/governance_evidence_package.md.
    """
    state_summary = state_summary or {}
    tiers = {dom: assign_tier(d, state_summary.get(dom)) for dom, d in cascade.items()}
    return {
        "strong_baseline_candidate": sorted(
            dom for dom, t in tiers.items() if t == TIER_STRONG_BASELINE
        ),
        "baseline_candidate": sorted(
            dom for dom, t in tiers.items()
            if t in (TIER_STRONG_BASELINE, TIER_BASELINE_LOCAL_REVIEW, TIER_BASELINE_CONTAINER_GAP)
        ),
        "local_review_required": sorted(
            dom for dom, t in tiers.items()
            if t in (TIER_BASELINE_LOCAL_REVIEW, TIER_INVESTIGATE, TIER_ACTIVE_LOCAL)
        ),
        "active_local_practice": sorted(dom for dom, t in tiers.items() if t == TIER_ACTIVE_LOCAL),
        "high_fragmentation": sorted(dom for dom, t in tiers.items() if t == TIER_HIGH_FRAGMENTATION),
        "missing_or_degraded_evidence": sorted(
            dom for dom, t in tiers.items()
            if t in (TIER_INSUFFICIENT, TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE, TIER_SPARSE_LIMITED)
        ),
        "cross_client_convergence": sorted(
            dom for dom, d in cascade.items() if d["xc"] is not None and d["xc"] >= 0.70
        ),
    }


def _passive_inheritance_risk_domains(cascade: dict) -> list:
    """Domains in PASSIVE_INHERITANCE_RISK_DOMAINS showing a material passive
    signal, using the same thresholds and dual/single-schema branching as
    detect_anomalies()'s bundle/passive-inheritance fallback block (lines
    ~1311-1343) -- mirrored rather than shared because detect_anomalies()
    returns rendered prose strings, not a reusable boolean/value pair.
    """
    flagged = []
    for dom, d in cascade.items():
        if dom not in PASSIVE_INHERITANCE_RISK_DOMAINS:
            continue
        bundle_schema = d.get("bundle_schema", "none")
        if bundle_schema == "dual":
            passive_ind = d.get("passive_indicator")
            if passive_ind is not None and passive_ind >= 0.20:
                flagged.append(dom)
        elif bundle_schema == "single":
            bundle_share = d.get("bundle_share_all")
            if bundle_share is not None and bundle_share < 0.25:
                flagged.append(dom)
    return sorted(flagged)


def _low_coherence_clients(client_rows: list[dict]) -> list:
    return sorted(
        r["client"] for r in client_rows
        if r["wp_mean"] is not None and r["wp_mean"] < 0.45
    )


_LEADERSHIP_QUESTIONS = [
    ("Which baseline candidates should enter ratification review?",
     "Confirm intent, portability, active-use evidence, and whether local-active "
     "variants need separate handling before approval."),
    ("Where should governance use an approved-list or starter-content model "
     "instead of full convergence?",
     "This is especially relevant for families, materials, and domains with "
     "project-specific vocabulary."),
    ("Which active local practices deserve roll-up or documentation?",
     "Decide whether they are firmwide candidates, client/discipline playbook "
     "content, permitted variants, or project exceptions."),
    ("Which missing or passive inherited content is intentional?",
     "Distinguish deliberate pruning, unused starter stock, role-specific "
     "specialization, and propagation failure."),
    ("What additional segmentation is needed before stronger claims are made?",
     "Project type, business center, region, and larger segment samples remain "
     "future enhancements unless supplied upstream."),
]


def build_structured_findings(
    cascade: dict,
    client_rows: list[dict],
    state_summary: Optional[dict] = None,
) -> list[dict]:
    """Build the structured findings list backing governance_findings.json,
    reusing the exact same classification buckets render_findings_and_recommendations()
    renders as prose (see _classify_domains_for_findings()) so the two never
    diverge. finding_id assignment order is fixed (category, then sorted
    domain/client id) for run-to-run determinism.

    Only emits a finding when the underlying tier/metric already gates on
    sufficient evidence -- e.g. baseline_candidate/strong_baseline_candidate
    can never fire for a domain whose primary metric is None, because
    assign_tier() itself routes that domain to TIER_INSUFFICIENT instead.
    """
    domain_buckets = _classify_domains_for_findings(cascade, state_summary)
    passive_risk_domains = _passive_inheritance_risk_domains(cascade)
    low_coherence_clients = _low_coherence_clients(client_rows)

    findings: list[dict] = []
    counter = [0]

    def next_id() -> str:
        counter[0] += 1
        return f"GF-{counter[0]:03d}"

    def domain_support(dom: str, fields: list) -> list:
        return [{
            "artifact_id": "governance_domain_summary",
            "selector": {"domain": dom},
            "fields": fields,
        }]

    def add_domain_finding(dom: str, finding_type: str, summary: str, rule_id: str, fields: list) -> None:
        findings.append({
            "finding_id": next_id(),
            "subject": {"type": "domain", "id": dom},
            "finding_type": finding_type,
            "status": FINDING_STATUS_SUPPORTED,
            "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
            "fidelity": FINDING_FIDELITY_EXACT,
            "authority_level": AUTHORITY_CONTROLLED_INTERPRETATION,
            "summary": summary,
            "support": domain_support(dom, fields),
            "rule_ids": [rule_id],
            "limits": list(_FINDING_LIMITS_STANDARD),
        })

    label = lambda dom: DOMAIN_LABELS.get(dom, dom)

    for dom in domain_buckets["strong_baseline_candidate"]:
        add_domain_finding(
            dom, "strong_baseline_candidate",
            f"{label(dom)} meets the strong-baseline-candidate rule (governance_tier: "
            f"{TIER_STRONG_BASELINE}).",
            _RULE_STRONG_BASELINE,
            ["governance_tier", "template_to_project", "container_to_project"],
        )
    for dom in domain_buckets["baseline_candidate"]:
        tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
        add_domain_finding(
            dom, "baseline_candidate",
            f"{label(dom)} meets the baseline-candidate rule (governance_tier: {tier}).",
            _RULE_BASELINE_CANDIDATE,
            ["governance_tier", "template_to_project", "container_to_project"],
        )
    for dom in domain_buckets["local_review_required"]:
        tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
        add_domain_finding(
            dom, "local_review_required",
            f"{label(dom)} requires local/use review before baseline language is "
            f"safe (governance_tier: {tier}).",
            _RULE_LOCAL_REVIEW_REQUIRED,
            ["governance_tier", "local_active_share", "provided_to_used_containment"],
        )
    for dom in domain_buckets["active_local_practice"]:
        add_domain_finding(
            dom, "active_local_practice",
            f"{label(dom)} shows material active local practice (governance_tier: "
            f"{TIER_ACTIVE_LOCAL}).",
            _RULE_ACTIVE_LOCAL_PRACTICE,
            ["governance_tier", "local_active_share"],
        )
    for dom in domain_buckets["high_fragmentation"]:
        add_domain_finding(
            dom, "high_fragmentation",
            f"{label(dom)} is classified {TIER_HIGH_FRAGMENTATION} and is not a "
            "single-standard candidate in this run.",
            _RULE_HIGH_FRAGMENTATION,
            ["governance_tier", "template_to_project", "container_to_project"],
        )
    for dom in domain_buckets["missing_or_degraded_evidence"]:
        tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
        add_domain_finding(
            dom, "missing_or_degraded_evidence",
            f"{label(dom)} has insufficient or degraded evidence for governance "
            f"classification (governance_tier: {tier}).",
            _RULE_INSUFFICIENT_EVIDENCE,
            ["governance_tier", "score_reliability"],
        )
    for dom in domain_buckets["cross_client_convergence"]:
        add_domain_finding(
            dom, "cross_client_convergence",
            f"{label(dom)} shows strong cross-client convergence "
            f"({pct(cascade[dom]['xc'])}) -- a natural common-base candidate.",
            _RULE_XC_STRONG_CONVERGENCE,
            ["cross_client_convergence"],
        )
    for dom in passive_risk_domains:
        d = cascade[dom]
        detail = (
            f"passive_inheritance_indicator={fmt(d.get('passive_indicator'))}"
            if d.get("bundle_schema") == "dual"
            else f"bundle_share_all={fmt(d.get('bundle_share_all'))}"
        )
        add_domain_finding(
            dom, "passive_inheritance_risk",
            f"{label(dom)} is in the passive-inheritance risk group and shows a "
            f"material passive signal ({detail}).",
            _RULE_PASSIVE_INHERITANCE_RISK,
            ["passive_inheritance_indicator", "bundle_share_all", "passive_inheritance_risk"],
        )

    for client in low_coherence_clients:
        row = next(r for r in client_rows if r["client"] == client)
        findings.append({
            "finding_id": next_id(),
            "subject": {"type": "client", "id": client},
            "finding_type": "low_client_coherence",
            "status": FINDING_STATUS_SUPPORTED,
            "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
            "fidelity": FINDING_FIDELITY_EXACT,
            "authority_level": AUTHORITY_CONTROLLED_INTERPRETATION,
            "summary": (
                f"{client} shows high within-client variation "
                f"(within_project_coherence: {fmt(row['wp_mean'])})."
            ),
            "support": [{
                "artifact_id": "governance_client_summary",
                "selector": {"client": client},
                "fields": ["within_project_coherence", "n_project_files"],
            }],
            "rule_ids": [_RULE_CLIENT_LOW_COHERENCE],
            "limits": list(_FINDING_LIMITS_STANDARD) + [
                "Where file counts are small, treat as a signal for further "
                "sampling rather than a definitive client judgement.",
            ],
        })

    for question, framing in _LEADERSHIP_QUESTIONS:
        findings.append({
            "finding_id": next_id(),
            "subject": {"type": "package", "id": "governance_evidence_package"},
            "finding_type": "leadership_question",
            "status": FINDING_STATUS_QUESTION_NOT_CLAIM,
            "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
            "fidelity": FINDING_FIDELITY_EXACT,
            "authority_level": AUTHORITY_CONVENIENCE_SUMMARY,
            "summary": question,
            "support": [],
            "rule_ids": [_RULE_LEADERSHIP_QUESTION],
            "limits": [
                "This is a suggested question for human leadership review, not "
                "an observed result or a claim.",
                framing,
            ],
        })

    return findings


def render_findings_and_recommendations(
    cascade: dict,
    client_rows: list[dict],
    state_summary: Optional[dict] = None,
    findings: Optional[list] = None,
) -> str:
    state_summary = state_summary or {}
    findings = findings if findings is not None else build_structured_findings(cascade, client_rows, state_summary)

    def _domain_ids(finding_type: str) -> list:
        return [f["subject"]["id"] for f in findings if f["finding_type"] == finding_type]

    baseline_candidates = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("baseline_candidate")]
    clean_baseline = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("strong_baseline_candidate")]
    needs_review = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("local_review_required")]
    high_frag = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("high_fragmentation")]
    universal = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("cross_client_convergence")]
    low_coherence = [f["subject"]["id"] for f in findings if f["finding_type"] == "low_client_coherence"]

    lines = [
        "## Key Findings and Governance Questions\n",
        "### What appears to be working\n",
        f"**A governance floor is visible.** {len(baseline_candidates)} domains have enough propagation evidence "
        "to be treated as baseline candidates for leadership review. "
        f"{len(clean_baseline)} of those currently {'has' if len(clean_baseline) == 1 else 'have'} no material state exception in the available outputs. "
        "This is evidence of a common base, not a standards approval.\n",
    ]

    if baseline_candidates:
        lines.append(
            "Baseline candidate domains: " + ", ".join(baseline_candidates) + ".\n"
        )

    if universal:
        lines.append(
            f"**Some natural common-base candidates are visible.** "
            f"{', '.join(universal)} show strong cross-client convergence (>70%). "
            "This supports governance review, but still requires a decision about whether the convergence is intentional, portable, and worth formalising.\n"
        )

    lines += [
        "\n### What needs attention\n",
        "**View-template governance remains discipline-sensitive.** View-template domains with weak containment or low discipline coherence should be handled as discipline-specific governance questions, not forced into a single firmwide baseline.\n",
    ]

    if "phases" in cascade and cascade["phases"]["tp"] is not None:
        phases_tp = cascade["phases"]["tp"]
        phases_tw = cascade["phases"]["tw"]
        if phases_tp < 0.85 and phases_tw is not None and phases_tw > 0.80:
            lines.append(
                "**Phases show project-level extension.** Templates are internally consistent on phase definitions, but projects carry phases not defined in templates. The governance question is whether those additions are intentional project practice, client-specific vocabulary, or unmanaged accumulation.\n"
            )

    lines.append(
        "**Loaded family types and materials should not be governed like object styles.** "
        "These domains are often project-specific. Review them for approved lists, starter content, exception rules, or documentation rather than full vocabulary convergence.\n"
    )

    if needs_review:
        lines.append(
            f"**{len(needs_review)} domains need review before baseline language is safe.** "
            f"Examples include: {', '.join(needs_review[:12])}.\n"
        )

    if high_frag:
        lines.append(
            f"**High-fragmentation domains are not single-standard candidates in this run.** "
            f"{', '.join(high_frag)} should be treated as governance-design questions first.\n"
        )

    if low_coherence:
        lines.append(
            f"**Some clients show high within-client variation ({', '.join(low_coherence)}).** "
            "Where file counts are small, treat this as a signal for further sampling rather than a definitive client judgement.\n"
        )

    lines += [
        "\n### Recommended Leadership Questions\n",
        "1. **Which baseline candidates should enter ratification review?** Confirm intent, portability, active-use evidence, and whether local-active variants need separate handling before approval.\n",
        "2. **Where should governance use an approved-list or starter-content model instead of full convergence?** This is especially relevant for families, materials, and domains with project-specific vocabulary.\n",
        "3. **Which active local practices deserve roll-up or documentation?** Decide whether they are firmwide candidates, client/discipline playbook content, permitted variants, or project exceptions.\n",
        "4. **Which missing or passive inherited content is intentional?** Distinguish deliberate pruning, unused starter stock, role-specific specialization, and propagation failure.\n",
        "5. **What additional segmentation is needed before stronger claims are made?** Project type, business center, region, and larger segment samples remain future enhancements unless supplied upstream.\n",
    ]

    return "\n".join(lines)


def render_limitations(corpus: dict, legacy_used_fallback: bool = False, has_state_outputs: bool = False) -> str:
    used_fallback_note = (
        "\n- **Used-view fallback:** Used-view columns were not found in the summary schema. Where legacy columns are reused as fallback, active-use conclusions are limited and should be confirmed with dual-view outputs."
        if legacy_used_fallback else ""
    )
    state_note = (
        "\n- **Governance-state counts:** If upstream governance-state rows are not deduplicated to unique patterns, count fields should be treated as comparison-state rows. Prefer shares/rankings for leadership claims."
        if has_state_outputs else
        "\n- **Governance-state limitation:** Governance-state outputs were not provided. Inherited-but-unused and local-active findings are inferred indirectly."
    )
    return f"""---

## Analytical Notes and Limitations

- **Corpus size:** {corpus['Project']} project files is a {"moderate" if corpus['Project'] >= 80 else "small"} corpus. Client-level findings carry higher uncertainty than corpus-level findings.
- **Scope boundary:** This report is discovery and classification only. It does not approve standards, assign owners, define compliance rules, or judge project teams.
- **Segment boundary:** Project type, business center, and region should be treated as future segment dimensions unless explicit upstream segment CSVs are supplied.
- **Imperial/metric split:** All project files are imperial. Metric templates and coordination files exist but metric projects are not yet represented. Metric findings are limited to template-to-container comparisons only.
- **Scores are means across file pairs.** Individual files may score substantially higher or lower than reported means.
- **Patterns are normalised configuration fingerprints** (join_hash values) capturing the behavioural identity of a configuration record, independent of Revit element IDs. Two files sharing a pattern have identical or functionally equivalent configuration for that element.
- **Excluded domain:** `view_templates_renderings_drafting` is excluded from aggregate governance scoring because it is structurally anomalous in the current corpus.{used_fallback_note}{state_note}

---

*Generated by `{GENERATOR_IDENTITY}` from cross_segment_summary.csv, cross_segment_pooled.csv, and optional governance-state outputs.*
*Supporting tables: governance_domain_summary.csv, governance_client_summary.csv.*
"""

def main():
    parser = argparse.ArgumentParser(description="Generate governance narrative from pipeline CSVs.")
    parser.add_argument("--summary", required=True, help="cross_segment_summary.csv")
    parser.add_argument("--pooled", required=True, help="cross_segment_pooled.csv")
    parser.add_argument("--governance-states", help="cross_segment_governance_states.csv (optional)")
    parser.add_argument("--governance-state-summary", help="cross_segment_governance_state_summary.csv (optional)")
    parser.add_argument("--delta", help="cross_segment_delta.csv (optional legacy fallback)")
    parser.add_argument("--file-meta", help="file_metadata.csv (optional)")
    parser.add_argument("--client-sector", default=str(_DEFAULT_CLIENT_SECTOR_PATH),
                        help="client_sector.csv (client_label,sector columns — classifies "
                             "cross-client convergence and non-comparable-sector tiering). "
                             f"Defaults to {_DEFAULT_CLIENT_SECTOR_PATH} if present, so existing "
                             "invocations keep today's healthcare cross-client convergence "
                             "signal without needing to pass this flag. Pass an explicit path "
                             "to override, or a nonexistent path to run with every client "
                             "unclassified.")
    parser.add_argument("--union-inventory",
                        help="cross_segment_union_inventory.csv (optional)")
    parser.add_argument("--reuse-distribution",
                        help="pattern_reuse_distribution.csv (optional)")
    parser.add_argument("--matrix-manifest",
                        help="matrix_output_manifest.csv (optional). This is currently metadata-only; "
                             "see docs/governance_generator_cross_compare_coverage.md for where "
                             "the project_* matrices and fragmentation diagnostic should enter "
                             "the narrative.")
    parser.add_argument("--policy-dir", default=None,
                        help="Directory of externalized governance policy files (optional). "
                             "Not yet consumed by this generator -- reserved for a future PR "
                             "that externalizes DOMAIN_LABELS/PASSIVE_INHERITANCE_RISK_DOMAINS/ "
                             "tier thresholds (see docs/governance_evidence_package.md). Recorded "
                             "in governance_package_manifest.json for forward-compatibility "
                             "auditing only.")
    parser.add_argument("--package-schema-version", default=PACKAGE_SCHEMA_VERSION,
                        help=f"Override the emitted package_schema_version "
                             f"(default {PACKAGE_SCHEMA_VERSION}).")
    parser.add_argument("--emit-evidence-package", dest="emit_evidence_package",
                        action="store_true",
                        help="Write governance_package_manifest.json / "
                             "governance_package_health.json / governance_evidence_map.json "
                             "alongside the existing CSV/MD outputs (default: on).")
    parser.add_argument("--no-emit-evidence-package", dest="emit_evidence_package",
                        action="store_false",
                        help="Suppress the evidence-package JSON outputs; existing CSV/MD "
                             "outputs are unaffected.")
    parser.set_defaults(emit_evidence_package=True)
    parser.add_argument("--out", default="governance_narrative_context.md")
    parser.add_argument("--date", default=str(date.today()),
                        help="Analysis date string (default: today)")
    args = parser.parse_args()

    print(f"Loading {args.summary}...")
    summary_rows = read_csv(Path(args.summary))
    print(f"Loading {args.pooled}...")
    pooled_rows = read_csv(Path(args.pooled))

    governance_state_rows = []
    if args.governance_states:
        print(f"Loading {args.governance_states}...")
        governance_state_rows = read_csv(Path(args.governance_states))

    governance_state_summary_rows = []
    if args.governance_state_summary:
        print(f"Loading {args.governance_state_summary}...")
        governance_state_summary_rows = read_csv(Path(args.governance_state_summary))

    delta_rows = []
    if args.delta:
        print(f"Loading {args.delta}...")
        delta_rows = read_csv(Path(args.delta))

    file_meta_rows = None
    if args.file_meta:
        print(f"Loading {args.file_meta}...")
        file_meta_rows = read_csv(Path(args.file_meta))

    client_sector_rows = []
    if args.client_sector and Path(args.client_sector).exists():
        print(f"Loading {args.client_sector}...")
        client_sector_rows = read_csv(Path(args.client_sector))
    elif args.client_sector:
        print(f"[warn] {args.client_sector} not found — every client will be treated as "
              f"unclassified (no sector). Pass --client-sector explicitly to silence this "
              f"if that's intended.", file=sys.stderr)

    union_inventory_rows = []
    if args.union_inventory:
        print(f"Loading {args.union_inventory}...")
        union_inventory_rows = read_csv(Path(args.union_inventory))

    reuse_distribution_rows = []
    if args.reuse_distribution:
        print(f"Loading {args.reuse_distribution}...")
        reuse_distribution_rows = read_csv(Path(args.reuse_distribution))

    matrix_manifest_rows = []
    if args.matrix_manifest:
        print(f"Loading {args.matrix_manifest}...")
        matrix_manifest_rows = read_csv(Path(args.matrix_manifest))

    sector_map = load_client_sectors(client_sector_rows)

    normalise_summary_schema(summary_rows)
    print("Computing cascade scores...")
    cascade = build_cascade(summary_rows, sector_map)

    print("Computing corpus counts...")
    corpus = load_corpus_counts(summary_rows, file_meta_rows)

    print("Building client summary...")
    client_rows = build_client_summary(summary_rows, pooled_rows, sector_map)

    print("Building governance state summary...")
    governance_state_summary = build_governance_state_summary(
        governance_state_rows, governance_state_summary_rows
    )

    delta_summary = {}
    if delta_rows:
        print("Summarising legacy delta patterns...")
        delta_summary = load_delta_summary(delta_rows)

    # ── Evidence-package signal computation ────────────────────────────────────
    # Pure, read-only re-derivations from data already loaded above -- none of
    # this touches summary_rows/pooled_rows/cascade/client_rows or any existing
    # CSV/MD output. used_view_falls_back_to_legacy() is hoisted into a single
    # local var and reused below in place of the two independent calls the
    # narrative render functions used to make.
    legacy_fallback = used_view_falls_back_to_legacy()
    schema_detection = detect_bundle_schema(summary_rows)

    _cascade_known_types = (
        CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES
        | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
    )
    cascade_coverage = _comparison_type_coverage(
        {r.get("comparison_type", "") for r in summary_rows},
        _cascade_known_types,
        intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()),
    )
    gov_state_coverage = _comparison_type_coverage(
        {r.get("comparison_type", "").strip() for r in governance_state_summary_rows}
        | {r.get("comparison_type", "").strip() for r in governance_state_rows},
        _DIRECTED_GOVERNANCE_TYPES,
    )

    _client_sector_path_str = args.client_sector or ""
    _client_sector_is_default = _client_sector_path_str == str(_DEFAULT_CLIENT_SECTOR_PATH)
    _client_sector_exists = bool(_client_sector_path_str) and Path(_client_sector_path_str).exists()
    if _client_sector_exists:
        client_sector_status = "default_path_resolved" if _client_sector_is_default else "explicit_path"
    else:
        client_sector_status = "default_path_missing" if _client_sector_is_default else "explicit_path_missing"

    unit_systems_seen = sorted({r.get("unit_system", "") for r in summary_rows if r.get("unit_system")})
    matrix_names_seen = sorted({
        r.get("matrix_name", "") for r in matrix_manifest_rows if r.get("matrix_name")
    })

    print("Building structured findings...")
    findings = build_structured_findings(cascade, client_rows, governance_state_summary)

    # ── Resolve output paths ───────────────────────────────────────────────────
    # If --out is a directory (or has no .md suffix), treat it as the output
    # directory and write governance_narrative_context.md inside it.
    out_path = Path(args.out)
    if out_path.is_dir() or out_path.suffix.lower() != ".md":
        out_dir = out_path if out_path.suffix == "" else out_path.parent
        out_path = out_dir / "governance_narrative_context.md"
    else:
        out_dir = out_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Emit governance_domain_summary.csv ────────────────────────────────────
    print("Writing domain summary CSV...")
    domain_csv_path = out_dir / "governance_domain_summary.csv"

    domain_csv_rows = []
    for dom, d in sorted(cascade.items()):
        if not _has_renderable_cascade_signal(d):
            # Scope-only domain (Group 3 fan-out data only) -- captured in
            # `cascade` but not yet tiered/rendered. See CASCADE_GROUP3_TYPES.
            continue
        tier = assign_tier(d, governance_state_summary.get(dom))
        reliability = score_reliability(d)
        anomalies = detect_anomalies(dom, d, governance_state_summary.get(dom))
        domain_csv_rows.append({
            "domain": dom,
            "domain_label": DOMAIN_LABELS.get(dom, dom),
            "governance_tier": tier,
            "score_reliability": reliability,
            # Cascade-computed generic->template/container/project (Group 2), sourced
            # from the always-required cross_segment_summary.csv -- distinct from the
            # optional-governance-state-summary-sourced "generic_to_template"/etc.
            # columns below, which are blank when --governance-state-summary isn't
            # supplied. These cascade columns are populated regardless.
            "cascade_generic_to_template": fmt(d.get("gt")),
            "cascade_generic_to_container": fmt(d.get("gc")),
            "cascade_generic_to_project": fmt(d.get("gp")),
            "template_to_container": fmt(d["tc"]),
            "container_to_project": fmt(d["cp"]),
            "template_to_project": fmt(d["tp"]),
            "cross_client_convergence": fmt(d["xc"]),
            "within_project_all": fmt(d["wp_all"]),
            "within_project_p10": fmt(d["wp_p10"]),
            "within_project_p90": fmt(d["wp_p90"]),
            "within_project_spread": fmt(
                (d["wp_p90"] - d["wp_p10"])
                if d["wp_p10"] is not None and d["wp_p90"] is not None
                else None
            ),
            "within_project_architectural": fmt(d["wp_disc"].get("architectural")),
            "within_project_mechanical_plumbing": fmt(d["wp_disc"].get("mechanical_plumbing")),
            "within_project_electrical": fmt(d["wp_disc"].get("electrical")),
            "within_project_structural": fmt(d["wp_disc"].get("structural")),
            "bundle_schema": d.get("bundle_schema", "none"),
            "template_to_project_used": fmt(d.get("tp_used")),
            "bundle_share_all": fmt(d.get("bundle_share_all")),
            "bundle_share_used": fmt(d.get("bundle_share_used")),
            "passive_inheritance_indicator": fmt(d.get("passive_indicator")),
            "passive_indicator_method": d.get("passive_indicator_method", "none"),
            "passive_inheritance_risk": "yes" if dom in PASSIVE_INHERITANCE_RISK_DOMAINS else "no",
            **{
                "generic_to_template": fmt(governance_state_summary.get(dom, {}).get("generic_to_template")),
                "generic_to_container": fmt(governance_state_summary.get(dom, {}).get("generic_to_container")),
                "generic_to_project": fmt(governance_state_summary.get(dom, {}).get("generic_to_project")),
                "provided_to_configured_containment": fmt(governance_state_summary.get(dom, {}).get("provided_to_configured_containment")),
                "provided_to_used_containment": fmt(governance_state_summary.get(dom, {}).get("provided_to_used_containment")),
                "provided_passive_share": fmt(governance_state_summary.get(dom, {}).get("provided_passive_share")),
                "provided_missing_share": fmt(governance_state_summary.get(dom, {}).get("provided_missing_share")),
                "local_active_share": fmt(governance_state_summary.get(dom, {}).get("local_active_share")),
                "provided_and_used_count": governance_state_summary.get(dom, {}).get("provided_and_used_count", ""),
                "provided_but_passive_count": governance_state_summary.get(dom, {}).get("provided_but_passive_count", ""),
                "provided_but_missing_count": governance_state_summary.get(dom, {}).get("provided_but_missing_count", ""),
                "local_active_count": governance_state_summary.get(dom, {}).get("local_active_count", ""),
                "local_passive_count": governance_state_summary.get(dom, {}).get("local_passive_count", ""),
                "local_unbundled_count": governance_state_summary.get(dom, {}).get("local_unbundled_count", ""),
                "primary_governance_read": governance_state_summary.get(dom, {}).get("primary_governance_read", ""),
            },
            "notable_anomalies": " | ".join(anomalies) if anomalies else "",
        })

    tier_order_key = lambda r: (TIER_ORDER.get(r["governance_tier"], 10), r["template_to_project"])
    domain_csv_rows.sort(key=tier_order_key)

    with open(domain_csv_path, "w", newline="", encoding="utf-8") as f:
        if domain_csv_rows:
            w = csv.DictWriter(f, fieldnames=list(domain_csv_rows[0].keys()))
            w.writeheader()
            w.writerows(domain_csv_rows)
    print(f"  → {domain_csv_path} ({len(domain_csv_rows)} rows)")

    # ── Emit governance_client_summary.csv ────────────────────────────────────
    print("Writing client summary CSV...")
    client_csv_path = out_dir / "governance_client_summary.csv"
    client_csv_rows = []
    for r in client_rows:
        strongest_str = "; ".join(
            f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
        )
        weakest_str = "; ".join(
            f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
        )
        onboarding = _client_onboarding_profile(r)
        client_csv_rows.append({
            "client": r["client"],
            "n_project_files": r["n_files"],
            "alignment_tier": r["tier"],
            "cross_client_similarity_mean": fmt(r["xc_mean"]),
            "within_project_coherence": fmt(r["wp_mean"]),
            "confidence_note": r["confidence_note"],
            "most_aligned_domains": strongest_str,
            "least_aligned_domains": weakest_str,
            "onboarding_internal_read": onboarding["internal_read"],
            "onboarding_portability_read": onboarding["portability_read"],
            "onboarding_common_base": onboarding["common_base"],
            "onboarding_variant_burden": onboarding["variant_burden"],
            "onboarding_operating_implication": onboarding["operating_implication"],
        })
    with open(client_csv_path, "w", newline="", encoding="utf-8") as f:
        if client_csv_rows:
            w = csv.DictWriter(f, fieldnames=list(client_csv_rows[0].keys()))
            w.writeheader()
            w.writerows(client_csv_rows)
    print(f"  → {client_csv_path} ({len(client_csv_rows)} rows)")

    # ── Render and write narrative MD ─────────────────────────────────────────
    print("Rendering narrative...")
    sections = [
        render_header(args.date, corpus, bool(governance_state_summary), legacy_fallback),
        render_evidence_authority_header(args.package_schema_version, GENERATOR_IDENTITY, args.emit_evidence_package),
        render_governance_state_model(bool(governance_state_summary)),
        render_domain_tiers(cascade, governance_state_summary),
    ]
    generic_scope_section = render_generic_baseline_scope_section(cascade)
    if generic_scope_section:
        sections.append(generic_scope_section)
    group1_scope_section = render_group1_scope_section(cascade)
    if group1_scope_section:
        sections.append(group1_scope_section)
    sections += [
        render_discipline_section(cascade, summary_rows),
        render_client_section(client_rows),
        render_onboarding_section(client_rows),
    ]
    if governance_state_summary:
        sections.append(render_governance_state_section(governance_state_summary))
    elif delta_summary:
        sections.append(render_delta_section(delta_summary))
    union_reuse_section = render_union_reuse_summary(
        union_inventory_rows, reuse_distribution_rows, matrix_manifest_rows
    )
    if union_reuse_section:
        sections.append(union_reuse_section)
    sections += [
        render_findings_and_recommendations(cascade, client_rows, governance_state_summary, findings),
        render_limitations(corpus, legacy_fallback, bool(governance_state_summary)),
    ]

    output = "\n\n".join(sections)
    out_path.write_text(output, encoding="utf-8")
    print(f"\nWrote {out_path} ({len(output):,} chars, {len(output.splitlines())} lines)")

    # ── Evidence-package JSON outputs ───────────────────────────────────────────
    # Written last, after all three existing outputs are already safely on disk,
    # so a failure here never blocks or corrupts the existing deliverables. See
    # docs/governance_evidence_package.md.
    if args.emit_evidence_package:
        print("Writing evidence package (manifest/health/evidence_map)...")

        input_paths = {
            "cross_segment_summary": Path(args.summary),
            "cross_segment_pooled": Path(args.pooled),
            "cross_segment_governance_states": Path(args.governance_states) if args.governance_states else None,
            "cross_segment_governance_state_summary": Path(args.governance_state_summary) if args.governance_state_summary else None,
            "cross_segment_delta": Path(args.delta) if args.delta else None,
            "file_metadata": Path(args.file_meta) if args.file_meta else None,
            "client_sector": Path(_client_sector_path_str) if _client_sector_path_str else None,
            "cross_segment_union_inventory": Path(args.union_inventory) if args.union_inventory else None,
            "pattern_reuse_distribution": Path(args.reuse_distribution) if args.reuse_distribution else None,
            "matrix_output_manifest": Path(args.matrix_manifest) if args.matrix_manifest else None,
        }
        input_required = {"cross_segment_summary": True, "cross_segment_pooled": True}
        input_roles = {
            "cross_segment_summary": "authoritative_deterministic_evidence",
            "cross_segment_pooled": "authoritative_deterministic_evidence",
            "cross_segment_governance_states": "authoritative_deterministic_evidence",
            "cross_segment_governance_state_summary": "authoritative_deterministic_evidence",
            "cross_segment_delta": "authoritative_deterministic_evidence",
            "file_metadata": "authoritative_deterministic_evidence",
            "client_sector": "user_provided_note",
            "cross_segment_union_inventory": "authoritative_deterministic_evidence",
            "pattern_reuse_distribution": "authoritative_deterministic_evidence",
            "matrix_output_manifest": "convenience_summary",
        }
        input_present = {k: bool(v) and v.exists() for k, v in input_paths.items()}

        output_paths = {
            "governance_domain_summary": domain_csv_path,
            "governance_client_summary": client_csv_path,
            "governance_narrative_context": out_path,
            "governance_package_manifest": out_dir / "governance_package_manifest.json",
            "governance_package_health": out_dir / "governance_package_health.json",
            "governance_evidence_map": out_dir / "governance_evidence_map.json",
            "governance_findings": out_dir / "governance_findings.json",
        }
        output_types = {
            "governance_domain_summary": "csv", "governance_client_summary": "csv", "governance_narrative_context": "markdown",
            "governance_package_manifest": "json", "governance_package_health": "json", "governance_evidence_map": "json",
            "governance_findings": "json",
        }
        output_authority = {
            "governance_domain_summary": "authoritative_deterministic_evidence",
            "governance_client_summary": "authoritative_deterministic_evidence",
            "governance_narrative_context": "controlled_interpretation",
            "governance_package_manifest": "authoritative_deterministic_evidence",
            "governance_package_health": "controlled_interpretation",
            "governance_evidence_map": "authoritative_deterministic_evidence",
            "governance_findings": "controlled_interpretation",
        }
        output_context_role = {
            "governance_domain_summary": "primary tier/score rollup",
            "governance_client_summary": "primary client alignment/onboarding rollup",
            "governance_narrative_context": "human-readable synthesis",
            "governance_package_manifest": "provenance record",
            "governance_package_health": "coverage/health signal",
            "governance_evidence_map": "artifact navigation index",
            "governance_findings": "structured, rule-derived findings",
        }

        comparison_run_ids = sorted(
            (
                {r.get("comparison_run_id", "") for r in summary_rows}
                | {r.get("comparison_run_id", "") for r in pooled_rows}
            )
            - {""}
        )
        source_executed_utc = sorted(
            (
                {r.get("executed_utc", "") for r in summary_rows}
                | {r.get("executed_utc", "") for r in pooled_rows}
            )
            - {""}
        )

        # health.json and evidence_map.json are built and written *before* the
        # manifest, and the manifest is built from an output_paths view that
        # excludes its own file. build_package_manifest() stats each entry in
        # output_paths via Path.exists()/Path.stat() at call time -- if the
        # manifest were built (and therefore stat its sibling JSON outputs)
        # before those files existed on disk, it would permanently record them
        # as present: false/size_bytes: null once written. A manifest also
        # cannot accurately stat itself before it has been written -- that
        # self-description job already belongs to governance_evidence_map.json
        # (see its self-entry, related_artifacts). Writing health/evidence_map
        # first, then the manifest last with a self-excluded output_paths view,
        # avoids both problems without a two-pass write.
        health = build_package_health(
            schema_version=args.package_schema_version,
            schema_detection=schema_detection,
            used_view_fallback=legacy_fallback,
            comparison_type_coverage_by_fn={
                "build_cascade": cascade_coverage,
                "build_governance_state_summary": gov_state_coverage,
            },
            required_inputs={k: input_present[k] for k in input_required},
            optional_inputs={k: input_present[k] for k in input_paths if k not in input_required},
            client_sector_status=client_sector_status,
            domain_csv_row_count=len(domain_csv_rows),
            domain_rows_excluded_no_signal=sum(
                1 for d in cascade.values() if not _has_renderable_cascade_signal(d)
            ),
            client_csv_row_count=len(client_csv_rows),
            corpus_project_file_count=corpus.get("Project", 0),
            excluded_from_scoring=sorted(EXCLUDED_FROM_SCORING),
            unit_systems_seen=unit_systems_seen,
            matrix_manifest_row_count=len(matrix_manifest_rows),
            matrix_names_seen=matrix_names_seen,
        )
        write_json(out_dir / "governance_package_health.json", health)

        findings_document = build_findings_document(findings, schema_version=FINDINGS_SCHEMA_VERSION)
        write_json(out_dir / "governance_findings.json", findings_document)

        sibling_paths = {
            "file_pairs": Path(args.summary).parent / "cross_segment_file_pairs.csv",
            "comparison_registry": Path(args.summary).parent / "comparison_registry.csv",
        }
        sibling_present = {k: v.exists() for k, v in sibling_paths.items()}

        evidence_map = build_evidence_map(
            schema_version=EVIDENCE_MAP_SCHEMA_VERSION,
            input_paths=input_paths,
            input_present=input_present,
            output_paths=output_paths,
            sibling_paths=sibling_paths,
            sibling_present=sibling_present,
            package_schema_version=args.package_schema_version,
        )
        write_json(out_dir / "governance_evidence_map.json", evidence_map)

        # Built and written last, now that governance_package_health.json and
        # governance_evidence_map.json are actually on disk and stat correctly.
        # Excludes "governance_package_manifest" from the paths it stats about
        # itself -- see the comment above for why.
        manifest_output_paths = {k: v for k, v in output_paths.items() if k != "governance_package_manifest"}
        manifest_output_types = {k: v for k, v in output_types.items() if k != "governance_package_manifest"}
        manifest_output_authority = {k: v for k, v in output_authority.items() if k != "governance_package_manifest"}
        manifest_output_context_role = {k: v for k, v in output_context_role.items() if k != "governance_package_manifest"}
        manifest = build_package_manifest(
            generator_identity=GENERATOR_IDENTITY,
            generator_role=GENERATOR_ROLE,
            package_schema_version=args.package_schema_version,
            analysis_date=args.date,
            input_paths=input_paths,
            input_required=input_required,
            input_roles=input_roles,
            output_paths=manifest_output_paths,
            output_types=manifest_output_types,
            output_authority=manifest_output_authority,
            output_context_role=manifest_output_context_role,
            policy_dir=Path(args.policy_dir) if args.policy_dir else None,
            comparison_run_ids=comparison_run_ids,
            source_executed_utc=source_executed_utc,
        )
        write_json(out_dir / "governance_package_manifest.json", manifest)

        print(f"  → wrote governance_package_health.json, governance_findings.json, "
              f"governance_evidence_map.json, governance_package_manifest.json to {out_dir}")
    else:
        # A previous run over this same --out directory may have written
        # package JSONs with --emit-evidence-package (the default). The
        # narrative just rendered above states plainly that no package
        # health/findings/evidence-map file exists for this run (see
        # render_evidence_authority_header's emit_evidence_package gating) --
        # leaving stale files from an earlier run in place would contradict
        # that claim and let a downstream reader pick up out-of-date
        # provenance/health/findings data alongside the freshly-written CSV/MD.
        stale_names = (
            "governance_package_manifest.json",
            "governance_package_health.json",
            "governance_evidence_map.json",
            "governance_findings.json",
        )
        removed = [name for name in stale_names if (out_dir / name).exists()]
        for name in removed:
            (out_dir / name).unlink()
        if removed:
            print(f"  → removed stale evidence-package file(s) from a prior run: {', '.join(removed)}")


if __name__ == "__main__":
    main()
