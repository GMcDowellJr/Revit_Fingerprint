"""Tests for structured governance findings (PR2): build_structured_findings(),
build_findings_document(), and render_findings_and_recommendations()'s
consumption of the same classification buckets.

See docs/governance_evidence_package.md and the task's DoD for PR2's
"New artifact tests" (Findings) requirements: every finding has origin,
authority, status, limits, and support; rule-derived findings reference rule
IDs; findings do not reference nonexistent artifact IDs; leadership
questions are marked as questions rather than claims; no baseline finding is
emitted when required supporting metrics are unavailable.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from governance_evidence_package import (  # noqa: E402
    AUTHORITY_CONTROLLED_INTERPRETATION,
    AUTHORITY_CONVENIENCE_SUMMARY,
    FINDING_FIDELITY_EXACT,
    FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
    FINDING_STATUS_QUESTION_NOT_CLAIM,
    FINDING_STATUS_SUPPORTED,
    FINDING_TYPES,
    build_findings_document,
)
from generate_governance_narrative import (  # noqa: E402
    PASSIVE_INHERITANCE_RISK_DOMAINS,
    TIER_ACTIVE_LOCAL,
    TIER_BASELINE_CONTAINER_GAP,
    TIER_BASELINE_LOCAL_REVIEW,
    TIER_HIGH_FRAGMENTATION,
    TIER_STRONG_BASELINE,
    build_structured_findings,
    render_findings_and_recommendations,
)

_RISK_DOMAIN = next(iter(PASSIVE_INHERITANCE_RISK_DOMAINS))
_NON_RISK_DOMAIN = "line_styles"
assert _NON_RISK_DOMAIN not in PASSIVE_INHERITANCE_RISK_DOMAINS


def _min_domain_dict(**overrides):
    d = {
        "tc": None, "cp": None, "tp": None, "xc": None,
        "wp_p10": None, "wp_p90": None, "wp_all": None,
        "tp_by_scope": {}, "cp_by_scope": {},
        "bundle_schema": "none", "passive_indicator": None, "bundle_share_all": None,
    }
    d.update(overrides)
    return d


def _client_row(client="acme", wp_mean=None, xc_mean=None):
    return {
        "client": client, "n_files": 10, "tier": "n/a",
        "xc_mean": xc_mean, "wp_mean": wp_mean, "confidence_note": "",
        "strongest": [], "weakest": [],
    }


# ---------------------------------------------------------------------------
# Per-category findings
# ---------------------------------------------------------------------------

def test_strong_baseline_candidate_finding():
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "strong_baseline_candidate") in types
    assert ("line_styles", "baseline_candidate") in types


def test_baseline_candidate_without_strong_for_container_gap():
    """Regression test for a PR review finding: TIER_BASELINE_CONTAINER_GAP
    (primary >= 0.90 but template_to_container < 0.60) is the fourth possible
    reason a domain in baseline_candidate isn't also strong_baseline_candidate
    -- template_to_container must be listed in support fields, not just the
    other three primary/state drivers."""
    cascade = {"line_styles": _min_domain_dict(tc=0.30, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "baseline_candidate") in types
    assert ("line_styles", "strong_baseline_candidate") not in types

    finding = next(f for f in findings
                   if f["subject"]["id"] == "line_styles" and f["finding_type"] == "baseline_candidate")
    assert "template_to_container" in finding["support"][0]["fields"]


def test_active_local_practice_and_local_review_required():
    cascade = {"line_styles": _min_domain_dict(tc=0.80, cp=0.80, tp=0.80, wp_p10=0.80, wp_p90=0.85)}
    state = {"line_styles": {"local_active_share": 0.20}}
    findings = build_structured_findings(cascade, [], state)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "active_local_practice") in types
    assert ("line_styles", "local_review_required") in types


def test_active_local_practice_finding_at_high_primary_containment():
    """Regression test for a PR review finding: when primary >= 0.90 and
    local_active_share is material, assign_tier() returns
    TIER_BASELINE_LOCAL_REVIEW (via _has_material_state_exception()), not
    TIER_ACTIVE_LOCAL -- but the active_local_practice finding must still
    fire, since the underlying active-local signal is real regardless of
    which tier it lands in. The finding's summary must also report the
    domain's actual tier, not a hard-coded TIER_ACTIVE_LOCAL."""
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    state = {"line_styles": {"local_active_share": 0.20}}
    findings = build_structured_findings(cascade, [], state)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "active_local_practice") in types
    assert ("line_styles", "baseline_candidate") in types
    assert ("line_styles", "local_review_required") in types
    assert ("line_styles", "strong_baseline_candidate") not in types

    active_local_finding = next(
        f for f in findings if f["subject"]["id"] == "line_styles" and f["finding_type"] == "active_local_practice"
    )
    assert TIER_ACTIVE_LOCAL not in active_local_finding["summary"]
    assert TIER_BASELINE_LOCAL_REVIEW in active_local_finding["summary"]


def test_local_review_required_via_passive_or_missing_share_lists_all_triggering_fields():
    """Regression test for a PR review finding: a domain can land in
    Baseline Candidate -- Local/Use Review via _has_material_state_exception()
    tripping on provided_passive_share or provided_missing_share, not just
    local_active_share -- the finding's support fields must list all three
    state fields (plus provided_to_used_containment) regardless of which one
    actually triggered this instance, so drill-through is never incomplete."""
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    # local_active_share is absent/low; provided_passive_share alone crosses
    # the PASSIVE_MATERIAL_THRESHOLD (0.20) that _has_material_state_exception()
    # checks, downgrading an otherwise->=0.90 domain out of strong baseline.
    state = {"line_styles": {"provided_passive_share": 0.25}}
    findings = build_structured_findings(cascade, [], state)
    finding = next(f for f in findings
                   if f["subject"]["id"] == "line_styles" and f["finding_type"] == "local_review_required")
    fields = finding["support"][0]["fields"]
    assert "local_active_share" in fields
    assert "provided_passive_share" in fields
    assert "provided_missing_share" in fields
    assert "provided_to_used_containment" in fields


def test_local_review_required_via_investigate_tier_lists_primary_containment_fields():
    """Regression test for a PR review finding: TIER_INVESTIGATE (primary
    containment in [0.75, 0.90) with no material state exception at all) also
    lands in local_review_required, but none of the state fields explain that
    classification -- the real drivers are template_to_project/
    container_to_project and score_reliability. Support fields must include
    those too, not just the state-exception fields relevant to the other two
    tiers sharing this bucket."""
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.80, tp=0.80, wp_p10=0.80, wp_p90=0.85)}
    findings = build_structured_findings(cascade, [], None)
    finding = next(f for f in findings
                   if f["subject"]["id"] == "line_styles" and f["finding_type"] == "local_review_required")
    fields = finding["support"][0]["fields"]
    assert "template_to_project" in fields
    assert "container_to_project" in fields
    assert "score_reliability" in fields


def test_high_fragmentation_finding():
    cascade = {"line_styles": _min_domain_dict(tc=0.10, cp=0.20, tp=0.30, wp_p10=0.30, wp_p90=0.35)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "high_fragmentation") in types
    assert ("line_styles", "baseline_candidate") not in types


def test_missing_or_degraded_evidence_when_primary_metric_absent():
    """DoD requirement: no baseline finding is emitted when required
    supporting metrics are unavailable. wp_all is set (a plausible real case:
    within-project data exists but no upstream template/container/enterprise
    comparison is available) so the domain still has a renderable signal and
    a real governance_domain_summary.csv row for this finding to reference."""
    cascade = {"line_styles": _min_domain_dict(tc=None, cp=None, tp=None, wp_all=0.30)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "missing_or_degraded_evidence") in types
    assert ("line_styles", "baseline_candidate") not in types
    assert ("line_styles", "strong_baseline_candidate") not in types


def test_missing_or_degraded_evidence_not_emitted_for_non_renderable_domain():
    """Regression test for a PR review finding: a domain whose only signal is
    Group-3 scope-level data is retained in `cascade` but never gets a
    governance_domain_summary.csv row (_has_renderable_cascade_signal() is
    False). No finding should reference that nonexistent row."""
    cascade = {"line_styles": _min_domain_dict()}  # every key None/empty -- zero signal
    findings = build_structured_findings(cascade, [], None)
    assert findings == build_structured_findings({}, [], None)  # only leadership questions
    assert not any(f["subject"].get("id") == "line_styles" for f in findings)


def test_cross_client_convergence_finding_independent_of_tier():
    cascade = {"line_styles": _min_domain_dict(tc=None, cp=None, tp=None, xc=0.80)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert ("line_styles", "cross_client_convergence") in types
    # Same domain also correctly gets missing_or_degraded_evidence -- the two
    # categories are independent axes (tier readiness vs. cross-client signal).
    assert ("line_styles", "missing_or_degraded_evidence") in types


def test_passive_inheritance_risk_finding_for_risk_domain_dual_schema():
    cascade = {_RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="dual", passive_indicator=0.25)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert (_RISK_DOMAIN, "passive_inheritance_risk") in types


def test_passive_inheritance_risk_not_flagged_for_non_risk_domain():
    cascade = {_NON_RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="dual", passive_indicator=0.25)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert (_NON_RISK_DOMAIN, "passive_inheritance_risk") not in types
    # Confirm the domain was actually classified (renderable), not just absent.
    assert any(f["subject"].get("id") == _NON_RISK_DOMAIN for f in findings)


def test_passive_inheritance_risk_not_flagged_below_threshold():
    cascade = {_RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="dual", passive_indicator=0.05)}
    findings = build_structured_findings(cascade, [], None)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert (_RISK_DOMAIN, "passive_inheritance_risk") not in types
    assert any(f["subject"].get("id") == _RISK_DOMAIN for f in findings)


def test_passive_inheritance_risk_finding_from_state_signal_without_bundle_data():
    """Regression test for a PR review finding: a risk-group domain with
    material provided_passive_share from --governance-state-summary must be
    flagged even when it has no matching bundle/passive-indicator data in
    cascade (bundle_schema == 'none') -- detect_anomalies() already treats
    the state signal as material on its own, independent of bundle data."""
    cascade = {_RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="none")}
    state = {_RISK_DOMAIN: {"provided_passive_share": 0.30}}
    findings = build_structured_findings(cascade, [], state)
    types = {(f["subject"]["id"], f["finding_type"]) for f in findings}
    assert (_RISK_DOMAIN, "passive_inheritance_risk") in types

    finding = next(f for f in findings
                   if f["subject"]["id"] == _RISK_DOMAIN and f["finding_type"] == "passive_inheritance_risk")
    assert "provided_passive_share" in finding["summary"]
    assert "provided_passive_share" in finding["support"][0]["fields"]


def test_low_client_coherence_finding():
    cascade = {}
    client_rows = [_client_row(client="acme", wp_mean=0.30), _client_row(client="beta", wp_mean=0.90)]
    findings = build_structured_findings(cascade, client_rows, None)
    client_findings = [f for f in findings if f["finding_type"] == "low_client_coherence"]
    assert len(client_findings) == 1
    assert client_findings[0]["subject"] == {"type": "client", "id": "acme"}


def test_leadership_questions_are_questions_not_claims():
    findings = build_structured_findings({}, [], None)
    lq = [f for f in findings if f["finding_type"] == "leadership_question"]
    assert len(lq) == 5
    for f in lq:
        assert f["status"] == FINDING_STATUS_QUESTION_NOT_CLAIM
        assert f["authority_level"] == AUTHORITY_CONVENIENCE_SUMMARY
        assert f["support"] == []


# ---------------------------------------------------------------------------
# Cross-cutting DoD requirements
# ---------------------------------------------------------------------------

def test_every_finding_has_provenance_and_limits():
    cascade = {
        "line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95, xc=0.80),
        _RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="dual", passive_indicator=0.25),
    }
    client_rows = [_client_row(client="acme", wp_mean=0.30)]
    findings = build_structured_findings(cascade, client_rows, None)
    assert findings
    for f in findings:
        assert f["finding_id"]
        assert f["subject"]["type"] in ("domain", "client", "package")
        assert f["finding_type"] in FINDING_TYPES
        assert f["status"] in (FINDING_STATUS_SUPPORTED, FINDING_STATUS_QUESTION_NOT_CLAIM)
        assert f["origin"] == FINDING_ORIGIN_DETERMINISTIC_COMPUTATION
        assert f["fidelity"] == FINDING_FIDELITY_EXACT
        assert f["authority_level"] in (AUTHORITY_CONTROLLED_INTERPRETATION, AUTHORITY_CONVENIENCE_SUMMARY)
        assert f["limits"]
        assert "support" in f
        assert "rule_ids" in f and f["rule_ids"]


def test_findings_do_not_reference_nonexistent_artifact_ids():
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    client_rows = [_client_row(client="acme", wp_mean=0.30)]
    findings = build_structured_findings(cascade, client_rows, None)
    valid_artifact_ids = {"governance_domain_summary", "governance_client_summary"}
    for f in findings:
        for s in f["support"]:
            assert s["artifact_id"] in valid_artifact_ids, s["artifact_id"]


def test_finding_ids_are_unique_and_stable_order():
    cascade = {
        "line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95),
        _RISK_DOMAIN: _min_domain_dict(tp=0.85, bundle_schema="dual", passive_indicator=0.25),
    }
    findings_a = build_structured_findings(cascade, [], None)
    findings_b = build_structured_findings(cascade, [], None)
    ids_a = [f["finding_id"] for f in findings_a]
    assert len(ids_a) == len(set(ids_a))
    assert ids_a == [f["finding_id"] for f in findings_b]
    assert [f["finding_type"] for f in findings_a] == [f["finding_type"] for f in findings_b]


# ---------------------------------------------------------------------------
# build_findings_document
# ---------------------------------------------------------------------------

def test_build_findings_document_wraps_with_schema_version():
    findings = build_structured_findings({}, [], None)
    doc = build_findings_document(findings)
    assert doc["schema_version"]
    assert doc["findings"] == findings


def test_build_findings_document_rejects_unknown_finding_type():
    import pytest
    with pytest.raises(ValueError):
        build_findings_document([{"finding_type": "not_a_real_type"}])


# ---------------------------------------------------------------------------
# render_findings_and_recommendations consumes the same structured findings
# ---------------------------------------------------------------------------

def test_render_findings_uses_passed_in_findings_without_recomputing():
    """If render_findings_and_recommendations is given a findings list that
    disagrees with what build_structured_findings would compute from cascade/
    client_rows, the passed-in list must win -- proving the renderer consumes
    (not recomputes) structured findings, per the task's 'avoid maintaining
    two independent implementations' requirement."""
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    fabricated_findings = [{
        "finding_id": "GF-999", "subject": {"type": "domain", "id": "totally_fake_domain"},
        "finding_type": "high_fragmentation", "status": FINDING_STATUS_SUPPORTED,
        "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION, "fidelity": FINDING_FIDELITY_EXACT,
        "authority_level": AUTHORITY_CONTROLLED_INTERPRETATION, "summary": "fabricated",
        "support": [], "rule_ids": ["GOV-TIER-HIGH-FRAGMENTATION"], "limits": ["test"],
    }]
    md = render_findings_and_recommendations(cascade, [], None, findings=fabricated_findings)
    assert "totally_fake_domain" in md
    # The real strong-baseline domain from `cascade` is absent because the
    # fabricated findings list (not a fresh computation) drove the render.
    assert "Line Styles" not in md


def test_render_findings_defaults_to_computing_findings_when_none_passed():
    cascade = {"line_styles": _min_domain_dict(tc=0.90, cp=0.95, tp=0.95, wp_p10=0.90, wp_p90=0.95)}
    md = render_findings_and_recommendations(cascade, [], None)
    assert "Line Styles" in md
