"""Tests for the within_project score_reliability p10/p90 capture's
segment_manifest-resolved fallback in tools/generate_governance_narrative.py's
build_cascade().

Background: score_reliability() is fed exclusively by wp_p10/wp_p90, which
were only ever captured from a within_project row where
`a == b and _is_unscoped_segment(r, "a")`. Since business_center_label became
a real segmentation cut, the genuinely enterprise-wide root segment for
Project-role within_project rows is routinely demoted to
run_type="registration" by build_segment_manifest.py's redundant_single_child
pass (all Project-role files sitting in one business center) -- and
compare_cross_segment.py's discover_within_project() never resolves that
demotion via _resolve_runnable_segment(), so no within_project row for the
root is ever emitted at all. score_reliability was therefore "Unknown" for
every domain in real corpora. build_cascade() now accepts an optional
segment_manifest dict and, when a row's own segment isn't directly unscoped,
accepts it as the enterprise-wide evidence source when it IS the segment
_resolve_runnable_segment() resolves the true root to -- guaranteed
population_hash-identical by construction, so this is not a scope widening.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS  # noqa: E402
from generate_governance_narrative import (  # noqa: E402
    RELIABILITY_UNKNOWN,
    build_cascade,
    normalise_summary_schema,
    score_reliability,
)


def _row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _wp_row(dom, segment_id, unit_system="imperial", role="Project",
            p10="0.60", p90="0.90", n_files="128"):
    return _row(
        segment_id_a=segment_id, segment_id_b=segment_id,
        governance_role_a=role, governance_role_b=role,
        unit_system=unit_system, comparison_type="within_project", domain=dom,
        all_pairwise_jaccard_mean="0.75",
        all_jaccard_p10=p10, all_jaccard_p90=p90,
        n_files_a=n_files, n_files_b=n_files,
    )


def test_no_manifest_keeps_today_behavior_unknown():
    """Without segment_manifest, a bc-scoped-only row (not _is_unscoped_segment)
    must NOT be picked up -- byte-for-byte the pre-fix behavior."""
    rows = [_wp_row("arrowheads", "imperial|Project|BC_1")]
    for r in rows:
        r["business_center_label_a"] = r["business_center_label_b"] = "BC_1"
    normalise_summary_schema(rows)
    d = build_cascade(rows)["arrowheads"]
    assert d["wp_p10"] is None
    assert d["wp_p90"] is None
    assert score_reliability(d) == RELIABILITY_UNKNOWN


def test_redundant_single_child_root_resolves_to_bc_scoped_row():
    """The real-corpus scenario: the true root ("imperial|Project") is
    registration/redundant_single_child, pointing at the bc-scoped segment
    that DOES have a within_project row. Supplying segment_manifest must let
    build_cascade() pick this row up as the enterprise-wide evidence source."""
    rows = [_wp_row("arrowheads", "imperial|Project|BC_1")]
    for r in rows:
        r["business_center_label_a"] = r["business_center_label_b"] = "BC_1"
    normalise_summary_schema(rows)

    manifest = {
        "imperial|Project": {
            "segment_id": "imperial|Project",
            "run_type": "registration",
            "notes": "redundant_single_child:imperial|Project|BC_1",
        },
        "imperial|Project|BC_1": {
            "segment_id": "imperial|Project|BC_1",
            "run_type": "bundle",
            "notes": "",
        },
    }

    d = build_cascade(rows, segment_manifest=manifest)["arrowheads"]
    assert d["wp_p10"] == 0.60
    assert d["wp_p90"] == 0.90
    assert d["wp_p10_source"] == "enterprise_resolved:imperial|Project|BC_1"
    assert score_reliability(d) != RELIABILITY_UNKNOWN


def test_dead_end_redundant_chain_stays_unknown():
    """A registration segment with no redundant_single_child pointer (a
    genuine dead end, e.g. below min-files) must not be treated as resolvable
    -- score stays Unknown rather than guessing."""
    rows = [_wp_row("arrowheads", "imperial|Project|BC_1")]
    for r in rows:
        r["business_center_label_a"] = r["business_center_label_b"] = "BC_1"
    normalise_summary_schema(rows)

    manifest = {
        "imperial|Project": {
            "segment_id": "imperial|Project",
            "run_type": "registration",
            "notes": "below_min_files",
        },
        "imperial|Project|BC_1": {
            "segment_id": "imperial|Project|BC_1",
            "run_type": "bundle",
            "notes": "",
        },
    }

    d = build_cascade(rows, segment_manifest=manifest)["arrowheads"]
    assert d["wp_p10"] is None
    assert d["wp_p90"] is None
    assert score_reliability(d) == RELIABILITY_UNKNOWN


def test_directly_unscoped_row_still_tagged_enterprise_not_resolved():
    """When the row's own segment already passes _is_unscoped_segment (the
    pre-existing direct path), wp_p10_source must say "enterprise", not
    "enterprise_resolved:..." -- even when segment_manifest is supplied."""
    rows = [_wp_row("arrowheads", "imperial|Project")]
    normalise_summary_schema(rows)

    manifest = {
        "imperial|Project": {
            "segment_id": "imperial|Project",
            "run_type": "bundle",
            "notes": "",
        },
    }

    d = build_cascade(rows, segment_manifest=manifest)["arrowheads"]
    assert d["wp_p10"] == 0.60
    assert d["wp_p10_source"] == "enterprise"


def test_manifest_provided_but_no_matching_row_leaves_unknown():
    """segment_manifest resolves the root to a segment that simply has no
    within_project row in this summary at all -- must not fabricate data."""
    rows = [_wp_row("arrowheads", "imperial|Project|BC_1")]
    for r in rows:
        r["business_center_label_a"] = r["business_center_label_b"] = "BC_1"
    normalise_summary_schema(rows)

    manifest = {
        "imperial|Project": {
            "segment_id": "imperial|Project",
            "run_type": "registration",
            "notes": "redundant_single_child:imperial|Project|BC_2",
        },
        "imperial|Project|BC_2": {
            "segment_id": "imperial|Project|BC_2",
            "run_type": "bundle",
            "notes": "",
        },
    }

    d = build_cascade(rows, segment_manifest=manifest)["arrowheads"]
    assert d["wp_p10"] is None
    assert d["wp_p90"] is None
    assert score_reliability(d) == RELIABILITY_UNKNOWN
