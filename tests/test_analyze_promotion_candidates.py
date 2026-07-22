"""Synthetic-fixture tests for tools/analyze_promotion_candidates.py.

No real corpus export data exists in this repo (results/segments/ output
directories are runtime artifacts, never checked into git -- see the
module docstring in tools/analyze_promotion_candidates.py). These fixtures
are built directly from the real, code-confirmed schemas of
cross_segment_governance_states.csv (tools/compare_cross_segment.py's
build_governance_state_outputs()) and pattern_reuse_distribution.csv
(build_pattern_reuse_distribution_rows()), not guessed column names.
"""

import sys
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("numpy")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import analyze_promotion_candidates as apc


DOMAIN = "text_types"


def _gov_row(join_hash, **overrides):
    row = {
        "domain": DOMAIN,
        "join_hash": join_hash,
        "unit_system": "imperial",
        "pattern_label": overrides.pop("pattern_label", join_hash),
        "state": "local_active",
        "target_usage_interpretable": "true",
        "n_files_in_target_used": 3,
        "pct_files_in_target_used": 0.3,
        "in_any_template": "false",
        "in_any_container": "false",
        "in_any_generic": "false",
        "comparison_type": "template_to_project",
        "governance_role_reference": "Template",
        "in_reference_all": "false",
        "segment_id_target": overrides.pop("segment_id_target", f"seg_project_{join_hash}"),
    }
    row.update(overrides)
    return row


def _reuse_row(join_hash, **overrides):
    row = {
        "domain": DOMAIN,
        "join_hash": join_hash,
        "pattern_label": overrides.pop("pattern_label", join_hash),
        "view_scope": "all",
        "governance_role": "Project",
        "client_label": "Acme",
        "discipline_label": "",
        "unit_system": "imperial",
        "reuse_bucket": "client_wide",
        # Kept well below --baseline-threshold's default (0.90) so tests that
        # aren't specifically exercising the penetration+seeded baseline
        # gate don't trip it by accident.
        "n_projects_present": 1,
        "n_projects_denominator": 5,
        "n_clients_present": 1,
        "n_clients_denominator": 5,
        "n_files_present": 10,
        "n_files_denominator": 10,
        "pct_projects_present": 0.2,
        "pct_clients_present": 0.2,
        "pct_files_present": 1.0,
    }
    row.update(overrides)
    return row


@pytest.fixture
def corpus_root(tmp_path):
    gov_rows = [
        # jh_candidate_enterprise: seeded at bc, reused enterprise-wide -> candidate
        _gov_row("jh_candidate_enterprise"),
        {**_gov_row("jh_candidate_enterprise"), "state": "provided_and_used",
         "comparison_type": "bc_to_project", "governance_role_reference": "Template",
         "in_reference_all": "true"},

        # jh_baseline_equal: seeded enterprise, reused enterprise -> baseline (equal)
        _gov_row("jh_baseline_equal"),
        {**_gov_row("jh_baseline_equal"), "state": "provided_and_used",
         "comparison_type": "enterprise_to_project", "governance_role_reference": "Template",
         "in_reference_all": "true"},

        # jh_underused: seeded enterprise, reused only client-wide -> governed_but_underused
        _gov_row("jh_underused"),
        {**_gov_row("jh_underused"), "state": "provided_and_used",
         "comparison_type": "enterprise_to_project", "governance_role_reference": "Template",
         "in_reference_all": "true"},

        # jh_below_floor: never seeded, single_file reuse -> below_reuse_floor
        _gov_row("jh_below_floor"),

        # jh_unclassified: never seeded, unclassified reuse -> unclassified_reuse
        _gov_row("jh_unclassified"),

        # jh_downgrade: never seeded, corpus_wide reuse but too few clients -> downgraded to client, candidate
        _gov_row("jh_downgrade"),

        # jh_generic_only: only a generic_to_project reference row (must NOT seed) -> candidate
        _gov_row("jh_generic_only"),
        {**_gov_row("jh_generic_only"), "state": "provided_and_used",
         "comparison_type": "generic_to_project", "governance_role_reference": "Generic",
         "in_reference_all": "true"},

        # jh_baseline_via_penetration: seeded bc, high project penetration -> baseline via gate,
        # even though scope-gap alone (client < bc) would say governed_but_underused
        _gov_row("jh_baseline_via_penetration"),
        {**_gov_row("jh_baseline_via_penetration"), "state": "provided_and_used",
         "comparison_type": "bc_to_project", "governance_role_reference": "Template",
         "in_reference_all": "true"},

        # jh_semantic_noise: never seeded, client-wide reuse, noisy label
        _gov_row("jh_semantic_noise", pattern_label="Foo|self"),

        # jh_dedup_targets: same target segment appears in two local_active rows
        # (compared against two different references), same n_files_in_target_used
        # each time -- must not be double-counted when summing files_used.
        _gov_row("jh_dedup_targets", segment_id_target="seg_shared",
                 n_files_in_target_used=7, pct_files_in_target_used=0.7),
        {**_gov_row("jh_dedup_targets", segment_id_target="seg_shared",
                     n_files_in_target_used=7, pct_files_in_target_used=0.7),
         "comparison_type": "bc_to_project"},

        # jh_tied_clients: never seeded, corpus_wide reuse tied across two
        # distinct clients (pct_clients_present is a shared, not
        # client-specific, quantity, so both client rows independently hit
        # the same reuse_scope_rank) -- their project/file evidence must be
        # aggregated, not have one client's row arbitrarily win.
        _gov_row("jh_tied_clients"),

        # jh_multi_label: same join_hash, two different targets carrying
        # different pattern_label spellings -- must not split into two rows
        # / undercount files_used per label.
        _gov_row("jh_multi_label", segment_id_target="seg_a",
                 pattern_label="Foo Type", n_files_in_target_used=4),
        _gov_row("jh_multi_label", segment_id_target="seg_b",
                 pattern_label="Foo Type (copy)", n_files_in_target_used=6),

        # jh_unit_test: same join_hash string reused across two unit_system
        # pools with deliberately different seeding/reuse evidence -- must
        # route independently per unit_system, not merge across the split.
        _gov_row("jh_unit_test", unit_system="imperial"),
        _gov_row("jh_unit_test", unit_system="metric"),
        {**_gov_row("jh_unit_test", unit_system="metric", segment_id_target="seg_metric_seed"),
         "state": "provided_and_used", "comparison_type": "enterprise_to_project",
         "governance_role_reference": "Template", "in_reference_all": "true"},

        # jh_used_view_pref: never seeded. all-view reuse looks corpus_wide
        # (broadly configured), but the used-view row for the same
        # (client, discipline) population shows only single_file active
        # delivery -- used-view must win, not the broader all-view figure.
        _gov_row("jh_used_view_pref"),
    ]

    reuse_rows = [
        _reuse_row("jh_candidate_enterprise", reuse_bucket="corpus_wide",
                   n_clients_present=5, n_clients_denominator=6),
        _reuse_row("jh_baseline_equal", reuse_bucket="corpus_wide",
                   n_clients_present=5, n_clients_denominator=6),
        _reuse_row("jh_underused", reuse_bucket="client_wide",
                   n_clients_present=1, n_clients_denominator=5),
        _reuse_row("jh_below_floor", reuse_bucket="single_file",
                   n_files_present=1, n_files_denominator=10,
                   n_projects_present=1, n_projects_denominator=10,
                   n_clients_present=1, n_clients_denominator=5),
        _reuse_row("jh_unclassified", reuse_bucket="unclassified",
                   n_files_present=0, n_files_denominator=0,
                   n_projects_present=0, n_projects_denominator=0,
                   n_clients_present=0, n_clients_denominator=0),
        _reuse_row("jh_downgrade", reuse_bucket="corpus_wide",
                   n_clients_present=2, n_clients_denominator=3),
        _reuse_row("jh_generic_only", reuse_bucket="client_wide",
                   client_label="Beta", n_clients_present=1, n_clients_denominator=5),
        _reuse_row("jh_baseline_via_penetration", reuse_bucket="multi_project",
                   n_projects_present=9, n_projects_denominator=10,
                   n_clients_present=1, n_clients_denominator=5),
        _reuse_row("jh_semantic_noise", pattern_label="Foo|self",
                   reuse_bucket="client_wide"),
        _reuse_row("jh_dedup_targets", reuse_bucket="client_wide"),
        _reuse_row("jh_tied_clients", client_label="Acme", reuse_bucket="corpus_wide",
                   n_clients_present=5, n_clients_denominator=6,
                   n_projects_present=2, n_projects_denominator=5,
                   n_files_present=4, n_files_denominator=10),
        _reuse_row("jh_tied_clients", client_label="Beta", reuse_bucket="corpus_wide",
                   n_clients_present=5, n_clients_denominator=6,
                   n_projects_present=3, n_projects_denominator=5,
                   n_files_present=6, n_files_denominator=10),
        _reuse_row("jh_multi_label", reuse_bucket="client_wide"),
        _reuse_row("jh_unit_test", unit_system="imperial", reuse_bucket="client_wide"),
        _reuse_row("jh_unit_test", unit_system="metric", reuse_bucket="client_wide"),
        _reuse_row("jh_used_view_pref", view_scope="all", reuse_bucket="corpus_wide",
                   n_clients_present=5, n_clients_denominator=6,
                   n_projects_present=8, n_projects_denominator=10,
                   n_files_present=20, n_files_denominator=22),
        _reuse_row("jh_used_view_pref", view_scope="used", reuse_bucket="single_file",
                   n_files_present=1, n_files_denominator=10,
                   n_projects_present=1, n_projects_denominator=10,
                   n_clients_present=1, n_clients_denominator=5),
    ]

    pd.DataFrame(gov_rows).to_csv(tmp_path / "cross_segment_governance_states.csv", index=False)
    pd.DataFrame(reuse_rows).to_csv(tmp_path / "pattern_reuse_distribution.csv", index=False)
    return tmp_path


def _read(root, name):
    return pd.read_csv(root / "promotion_candidate_analysis" / name)


def test_scope_gap_candidate_routing(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    row = candidates[candidates["join_hash"] == "jh_candidate_enterprise"].iloc[0]
    assert row["seeded_scope"] == "bc"
    assert row["reuse_scope"] == "enterprise"
    assert row["scope_gap"] == "reuse=enterprise > seeded=bc"
    assert row["candidate_class"] == "consistency_footprint_matches_enterprise_scope"


def test_baseline_equal_scope_excluded(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    baseline = _read(corpus_root, "baseline_adequately_governed.csv")
    assert "jh_baseline_equal" in set(baseline["join_hash"])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    assert "jh_baseline_equal" not in set(candidates["join_hash"])


def test_underused_routed_separately(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    underused = _read(corpus_root, "governed_but_underused.csv")
    assert set(underused["join_hash"]) >= {"jh_underused"}
    candidates = _read(corpus_root, "promotion_candidates.csv")
    baseline = _read(corpus_root, "baseline_adequately_governed.csv")
    assert "jh_underused" not in set(candidates["join_hash"])
    assert "jh_underused" not in set(baseline["join_hash"])


def test_below_reuse_floor_not_classified(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    below = _read(corpus_root, "below_reuse_floor.csv")
    assert "jh_below_floor" in set(below["join_hash"])
    for fname in ("promotion_candidates.csv", "governed_but_underused.csv",
                  "baseline_adequately_governed.csv"):
        assert "jh_below_floor" not in set(_read(corpus_root, fname)["join_hash"])


def test_files_used_not_inflated_by_repeated_target_across_references(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    row = candidates[candidates["join_hash"] == "jh_dedup_targets"].iloc[0]
    # seg_shared appears in two local_active rows (compared against two
    # different references) both reporting n_files_in_target_used=7 for the
    # same target -- files_used must reflect that one target's 7 files, not
    # 14 from summing both comparison rows.
    assert row["files_used"] == 7


def test_tied_client_rows_are_aggregated_not_dropped(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    row = candidates[candidates["join_hash"] == "jh_tied_clients"].iloc[0]
    # Acme (2/5 projects, 4/10 files) and Beta (3/5 projects, 6/10 files)
    # both hit corpus_wide and tie on reuse_scope_rank -- neither should be
    # dropped in favor of the other.
    assert row["n_projects_present"] == 5
    assert row["n_projects_denominator"] == 10
    assert row["n_files_present"] == 10
    assert row["n_files_denominator"] == 20
    assert set(row["client_label"].split(";")) == {"Acme", "Beta"}
    assert row["reuse_scope"] == "enterprise"


def test_pattern_label_variation_does_not_split_identity(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    matches = candidates[candidates["join_hash"] == "jh_multi_label"]
    # One row for the join_hash, not two -- files_used combines both targets
    # (4 + 6), and both label spellings are preserved rather than one being
    # silently dropped.
    assert len(matches) == 1
    row = matches.iloc[0]
    assert row["files_used"] == 10
    assert set(row["pattern_label"].split(";")) == {"Foo Type", "Foo Type (copy)"}


def test_unit_system_partitions_scope_evidence(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    audit = _read(corpus_root, "promotion_candidate_full_audit.csv")
    rows = audit[audit["join_hash"] == "jh_unit_test"]
    assert len(rows) == 2
    imperial = rows[rows["unit_system"] == "imperial"].iloc[0]
    metric = rows[rows["unit_system"] == "metric"].iloc[0]
    # Same join_hash string, deliberately different seeding evidence per
    # unit_system -- imperial (ungoverned, client-wide reuse) is a
    # candidate; metric (seeded enterprise-wide, only client-wide reuse) is
    # governed_but_underused. If the two unit_system rows were merged, both
    # would land in the same bucket.
    assert imperial["seeded_scope"] == "ungoverned"
    assert imperial["routing_bucket"] == "promotion_candidates"
    assert metric["seeded_scope"] == "enterprise"
    assert metric["routing_bucket"] == "governed_but_underused"


def test_used_view_preferred_over_all_view(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    audit = _read(corpus_root, "promotion_candidate_full_audit.csv")
    row = audit[audit["join_hash"] == "jh_used_view_pref"].iloc[0]
    # The all-view row alone would resolve to corpus_wide/enterprise: the
    # used-view row (single_file) for the same client/discipline population
    # must win instead, correctly reflecting narrow active-delivery reuse.
    assert row["reuse_scope"] == "ungoverned"
    assert row["reuse_view_source"] == "used"
    assert row["routing_bucket"] == "below_reuse_floor"


def test_domain_rollup_total_matches_bucket_sum(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    rollup = _read(corpus_root, "domain_rollup.csv")
    row = rollup[rollup["domain"] == DOMAIN].iloc[0]
    bucket_sum = (
        row["candidates"] + row["governed_but_underused"]
        + row["baseline_adequately_governed"] + row["below_reuse_floor"]
        + row["unclassified_reuse"] + row["semantic_noise_excluded"]
    )
    # total_patterns must count the (join_hash, unit_system) grain actually
    # routed, not distinct join_hash values -- otherwise a join_hash split
    # across unit_system pools (jh_unit_test) makes the total undercount
    # the sum of its own category columns.
    assert row["total_patterns"] == bucket_sum


def test_unclassified_reuse_routed_separately(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    unclassified = _read(corpus_root, "unclassified_reuse.csv")
    assert "jh_unclassified" in set(unclassified["join_hash"])
    for fname in ("promotion_candidates.csv", "governed_but_underused.csv",
                  "baseline_adequately_governed.csv", "below_reuse_floor.csv"):
        assert "jh_unclassified" not in set(_read(corpus_root, fname)["join_hash"])


def test_min_enterprise_clients_downgrade(corpus_root):
    apc.main([
        "--root", str(corpus_root), "--domains", DOMAIN,
        "--min-enterprise-clients", "3",
    ])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    row = candidates[candidates["join_hash"] == "jh_downgrade"].iloc[0]
    assert row["reuse_scope"] == "client"
    assert row["enterprise_evidence_downgraded"] == True  # noqa: E712


def test_generic_reference_does_not_seed(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    row = candidates[candidates["join_hash"] == "jh_generic_only"].iloc[0]
    assert row["seeded_scope"] == "ungoverned"


def test_baseline_threshold_gate_overrides_scope_gap(corpus_root):
    apc.main([
        "--root", str(corpus_root), "--domains", DOMAIN,
        "--baseline-threshold", "0.90",
    ])
    baseline = _read(corpus_root, "baseline_adequately_governed.csv")
    assert "jh_baseline_via_penetration" in set(baseline["join_hash"])
    underused = _read(corpus_root, "governed_but_underused.csv")
    assert "jh_baseline_via_penetration" not in set(underused["join_hash"])


def test_semantic_noise_filter_routes_separately(corpus_root):
    apc.main([
        "--root", str(corpus_root), "--domains", DOMAIN,
        "--enable-semantic-noise-filter",
    ])
    noise = _read(corpus_root, "semantic_noise_excluded.csv")
    assert "jh_semantic_noise" in set(noise["join_hash"])
    for fname in ("promotion_candidates.csv", "baseline_adequately_governed.csv"):
        assert "jh_semantic_noise" not in set(_read(corpus_root, fname)["join_hash"])


def test_semantic_noise_filter_disabled_by_default(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    assert "jh_semantic_noise" in set(candidates["join_hash"])
    assert not (corpus_root / "promotion_candidate_analysis" / "semantic_noise_excluded.csv").exists()


def test_rank_is_ordinal_per_domain(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    candidates = _read(corpus_root, "promotion_candidates.csv")
    ranks = sorted(candidates[candidates["domain"] == DOMAIN]["rank"].tolist())
    assert ranks == list(range(1, len(ranks) + 1))


def test_no_bare_numeric_score_in_any_output(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN, "--verbose"])
    out_dir = corpus_root / "promotion_candidate_analysis"
    for csv_path in out_dir.glob("*.csv"):
        cols = pd.read_csv(csv_path, nrows=0).columns
        assert "promotion_score" not in cols, csv_path
        assert "score" not in {c.lower() for c in cols}, csv_path
    summary_text = (out_dir / "promotion_candidate_summary.md").read_text()
    assert "promotion_score" not in summary_text
    assert "Read this first" in summary_text


def test_routing_buckets_are_mutually_exclusive(corpus_root):
    apc.main(["--root", str(corpus_root), "--domains", DOMAIN])
    out_dir = corpus_root / "promotion_candidate_analysis"
    files = [
        "promotion_candidates.csv", "governed_but_underused.csv",
        "baseline_adequately_governed.csv", "below_reuse_floor.csv",
        "unclassified_reuse.csv",
    ]
    # Identity is (join_hash, unit_system), not join_hash alone -- the same
    # join_hash string can legitimately carry different scope evidence per
    # unit_system pool (see test_unit_system_partitions_scope_evidence) and
    # land in two different buckets for that reason alone.
    seen = {}
    for fname in files:
        df = _read(corpus_root, fname)
        for jh, unit in zip(df["join_hash"], df["unit_system"]):
            key = (jh, unit)
            assert key not in seen, f"{key} appears in both {seen.get(key)} and {fname}"
            seen[key] = fname
