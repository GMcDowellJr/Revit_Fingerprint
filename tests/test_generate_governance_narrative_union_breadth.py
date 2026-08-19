"""Tests for D-033's union-inventory-derived domain confidence enrichment:
build_union_breadth_by_domain()'s tier classification and detect_anomalies()'s
new broad-reuse/weak-cascade and narrow-reuse/strong-cascade exception
category. See DECISIONS.md D-033 and docs/governance_generator_cross_compare_coverage.md.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import UNION_INVENTORY_FIELDS  # noqa: E402
from governance_policy import load_governance_policy  # noqa: E402
import generate_governance_narrative as g  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_governance_policy():
    yield
    g.apply_governance_policy(load_governance_policy(None, g._POLICY_DEFAULTS))


def _union_row(**overrides):
    r = {f: "" for f in UNION_INVENTORY_FIELDS}
    r.update(
        governance_role="Project", view_scope="all", unit_system="imperial",
        client_label="acme",
    )
    r.update(overrides)
    return r


def _minimal_cascade_dict(**overrides):
    d = {
        "tc": None, "cp": None, "tp": None, "xc": None,
        "wp_p10": None, "wp_p90": None, "wp_all": None,
        "tp_by_scope": {}, "cp_by_scope": {},
        "bundle_schema": "none", "passive_indicator": None, "bundle_share_all": None,
        "wp_disc": {}, "tw": None,
    }
    d.update(overrides)
    return d


# ---------------------------------------------------------------------------
# build_union_breadth_by_domain()
# ---------------------------------------------------------------------------

def test_union_breadth_classifies_corpus_wide_pattern():
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.95",
                        n_clients_denominator="10")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["corpus_wide"] == 1
    assert breadth["line_styles"]["total"] == 1


def test_union_breadth_classifies_client_wide_pattern():
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.60",
                        n_clients_denominator="10")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["client_wide"] == 1
    assert breadth["line_styles"]["corpus_wide"] == 0


def test_union_breadth_single_client_grain_never_classifies_corpus_or_client_wide():
    """PR review finding: with only one client in the grain, pct_clients_present
    is trivially 1.0 for every pattern that client carries -- must not be
    labeled corpus-wide/client-wide reuse with no actual cross-client
    evidence, mirroring compare_cross_segment.py's own _reuse_bucket_for()
    n_clients_den > 1 guard."""
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="1.000000",
                        n_clients_denominator="1", n_projects_present="1", n_files_present="1")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["corpus_wide"] == 0
    assert breadth["line_styles"]["client_wide"] == 0


def test_union_breadth_classifies_project_wide_pattern():
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.10", n_projects_present="3")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["project_wide"] == 1


def test_union_breadth_classifies_file_level_pattern():
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.05",
                        n_projects_present="1", n_files_present="1")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["file_level"] == 1


def test_union_breadth_does_not_merge_across_discipline_grains():
    """PR review finding: pct_clients_present/n_clients_denominator are
    computed by compare_cross_segment.py's build_union_inventory_rows()
    per (view_scope, governance_role, discipline_label, unit_system,
    domain) group, NOT corpus-wide across the whole domain. A pattern
    present in every client of one small discipline must not be reported
    as corpus-wide reuse for the entire domain just because it shares a
    join_hash with a narrower-reach row in a different discipline."""
    rows = [
        # "Structural" discipline: 2/2 clients -> would look corpus-wide
        # in isolation, but this grain's denominator is scoped to Structural.
        _union_row(domain="line_styles", join_hash="h1", discipline_label="Structural",
                   pct_clients_present="1.0", n_clients_denominator="2"),
        # "Architectural" discipline, SAME join_hash: only 1/10 clients in
        # its own (much larger) grain.
        _union_row(domain="line_styles", join_hash="h1", discipline_label="Architectural",
                   pct_clients_present="0.10", n_clients_denominator="10"),
    ]
    breadth = g.build_union_breadth_by_domain(rows)
    # Each discipline grain is classified independently -- Structural's row
    # legitimately qualifies as corpus_wide within its own scope, but this
    # must not collapse into a single domain-wide "corpus_wide reuse"
    # count of 1 that silently drops the Architectural grain's evidence.
    assert breadth["line_styles"]["total"] == 2
    assert breadth["line_styles"]["corpus_wide"] == 1


def test_union_breadth_degraded_source_status_classifies_unclassified():
    """PR review finding: build_pattern_reuse_distribution_rows() sends a row
    with source_status != 'ok' (e.g. missing source-cluster IDs) straight to
    unclassified before any breadth check -- this independent classifier
    must honor the same gate rather than presenting degraded inventory as
    confident corpus-wide reuse evidence."""
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.95",
                        n_clients_denominator="10", source_status="missing_source_cluster_id")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["corpus_wide"] == 0
    assert breadth["line_styles"]["unclassified"] == 1


def test_union_breadth_degraded_inventory_status_classifies_unclassified():
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.95",
                        n_clients_denominator="10", inventory_status="not_interpretable")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["corpus_wide"] == 0
    assert breadth["line_styles"]["unclassified"] == 1


def test_union_breadth_blank_status_fields_are_not_treated_as_degraded():
    """A blank source_status/inventory_status (e.g. an older export missing
    the column) defaults to 'ok', matching this generator's general
    fail-soft posture of not silently downgrading a real signal just
    because status metadata wasn't recorded."""
    rows = [_union_row(domain="line_styles", join_hash="h1", pct_clients_present="0.95",
                        n_clients_denominator="10")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["corpus_wide"] == 1
    assert breadth["line_styles"]["unclassified"] == 0


def test_union_breadth_ignores_non_project_role():
    rows = [_union_row(domain="line_styles", join_hash="h1", governance_role="Template", pct_clients_present="0.95")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth == {}


def test_union_breadth_ignores_non_all_view_scope():
    rows = [_union_row(domain="line_styles", join_hash="h1", view_scope="used", pct_clients_present="0.95")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth == {}


def test_union_breadth_empty_input_returns_empty_dict():
    assert g.build_union_breadth_by_domain([]) == {}


def test_union_breadth_preserves_highest_tier_across_repeated_client_rows():
    """PR review finding: cross_segment_union_inventory.csv emits one row
    per (client_label, ..., join_hash) grain for the same pattern -- pct_clients_present
    is corpus-wide (identical across rows), but n_projects_present/
    n_files_present are per-client. A later, narrower client row for the
    SAME (domain, join_hash) must not downgrade an already-qualified
    project_wide classification to file_level."""
    rows = [
        # Client A: broad reach within its own files/projects -> project_wide.
        _union_row(domain="line_styles", join_hash="h1", client_label="acme",
                   pct_clients_present="0.10", n_projects_present="5", n_files_present="20"),
        # Client B, same pattern, narrower reach -> would classify file_level alone.
        _union_row(domain="line_styles", join_hash="h1", client_label="beta",
                   pct_clients_present="0.10", n_projects_present="1", n_files_present="1"),
    ]
    breadth = g.build_union_breadth_by_domain(rows)
    assert breadth["line_styles"]["project_wide"] == 1
    assert breadth["line_styles"]["file_level"] == 0
    assert breadth["line_styles"]["total"] == 1


def test_union_breadth_preserves_highest_tier_regardless_of_row_order():
    rows_reversed = [
        _union_row(domain="line_styles", join_hash="h1", client_label="beta",
                   pct_clients_present="0.10", n_projects_present="1", n_files_present="1"),
        _union_row(domain="line_styles", join_hash="h1", client_label="acme",
                   pct_clients_present="0.10", n_projects_present="5", n_files_present="20"),
    ]
    breadth = g.build_union_breadth_by_domain(rows_reversed)
    assert breadth["line_styles"]["project_wide"] == 1
    assert breadth["line_styles"]["file_level"] == 0


def test_union_breadth_never_returns_raw_pattern_content():
    """Only aggregate integer counts per domain -- never join_hash/pattern_label
    values, matching D-033's 'only derived counts' scope boundary."""
    import json
    rows = [_union_row(domain="line_styles", join_hash="SECRET_JOIN_HASH",
                        pattern_label="SECRET_PATTERN_LABEL", pct_clients_present="0.95")]
    breadth = g.build_union_breadth_by_domain(rows)
    assert "SECRET_JOIN_HASH" not in json.dumps(breadth)
    assert "SECRET_PATTERN_LABEL" not in json.dumps(breadth)


# ---------------------------------------------------------------------------
# detect_anomalies()'s union-breadth exception category
# ---------------------------------------------------------------------------

def test_broad_reuse_weak_cascade_fires():
    d = _minimal_cascade_dict(tc=0.20, cp=0.20, tp=0.10)
    union_breadth = {"total": 5, "corpus_wide": 2, "client_wide": 0, "project_wide": 1, "file_level": 2}
    notes = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert any("Broad natural reuse" in n for n in notes)
    assert not any("Narrow natural reuse" in n for n in notes)


def test_narrow_reuse_strong_cascade_fires():
    d = _minimal_cascade_dict(tc=0.90, cp=0.90, tp=0.90)
    union_breadth = {"total": 4, "corpus_wide": 0, "client_wide": 0, "project_wide": 0, "file_level": 3}
    notes = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert any("Narrow natural reuse" in n for n in notes)
    assert not any("Broad natural reuse" in n for n in notes)


def test_unremarkable_breadth_and_cascade_does_not_fire():
    """A domain whose primary containment sits between the weak and strong
    cascade thresholds, or whose breadth mix doesn't clear either bar,
    must not trigger either exception note -- only the strongest exceptions
    render, per docs/governance_generator_cross_compare_coverage.md's own
    guardrail."""
    d = _minimal_cascade_dict(tc=0.55, cp=0.55, tp=0.55)
    union_breadth = {"total": 4, "corpus_wide": 0, "client_wide": 1, "project_wide": 2, "file_level": 1}
    notes = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert not any("Broad natural reuse" in n for n in notes)
    assert not any("Narrow natural reuse" in n for n in notes)


def test_no_union_breadth_supplied_never_fires():
    d = _minimal_cascade_dict(tc=0.20, cp=0.20, tp=0.10)
    notes = g.detect_anomalies("line_styles", d, None, None)
    assert not any("natural reuse" in n for n in notes)


def test_broad_reuse_with_no_primary_containment_does_not_fire():
    """primary (tp else cp) is None -- no basis to judge 'weak cascade'."""
    d = _minimal_cascade_dict(tc=None, cp=None, tp=None)
    union_breadth = {"total": 5, "corpus_wide": 2, "client_wide": 0, "project_wide": 1, "file_level": 2}
    notes = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert not any("natural reuse" in n for n in notes)


def test_overriding_union_breadth_threshold_changes_detect_anomalies_text(tmp_path):
    """Mirrors D-021/D-029's threshold-override test pattern: a --policy-dir
    override to anomaly_thresholds.json's union_breadth_* keys must be
    reflected in detect_anomalies()'s notable_anomalies text."""
    import json
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["anomaly_thresholds"]))
    custom["thresholds"]["union_breadth_weak_cascade_max"] = 0.80
    (tmp_path / "anomaly_thresholds.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    d = _minimal_cascade_dict(tc=0.70, cp=0.70, tp=0.70)
    union_breadth = {"total": 3, "corpus_wide": 1, "client_wide": 0, "project_wide": 1, "file_level": 1}
    notes = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert any("Broad natural reuse" in n for n in notes)

    g.apply_governance_policy(load_governance_policy(None, g._POLICY_DEFAULTS))
    notes_default = g.detect_anomalies("line_styles", d, None, union_breadth)
    assert not any("Broad natural reuse" in n for n in notes_default)
