from __future__ import annotations
import csv, subprocess, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.suggest_discovery_params import (
    compute_domain_stats,
    suggest_sample_size,
    _cumulative_subset_count,
    solve_candidate_fields_and_k,
    suggest_params_for_domain,
)


def _write_csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def test_compute_domain_stats_counts_n_g_f_and_candidates():
    records = [
        {"domain": "units", "record_pk": "1", "sig_hash": "s1", "file_id": "f1"},
        {"domain": "units", "record_pk": "2", "sig_hash": "s1", "file_id": "f1"},
        {"domain": "units", "record_pk": "3", "sig_hash": "s2", "file_id": "f2"},
        {"domain": "other", "record_pk": "4", "sig_hash": "s3", "file_id": "f3"},
    ]
    items = [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec"},
        {"domain": "units", "record_pk": "1", "item_key": "units.accuracy"},
        {"domain": "units", "record_pk": "2", "item_key": "units.spec"},
        {"domain": "other", "record_pk": "4", "item_key": "other.k"},
    ]
    stats = compute_domain_stats(records, items, "units")
    assert stats == {
        "records_total_domain": 3,
        "distinct_sig_hash_groups": 2,
        "distinct_file_count": 2,
        "candidate_field_count": 2,
    }


def test_suggest_sample_size_scales_with_diversity_not_just_population():
    # Same population size (1000), different diversity: more groups -> larger sample.
    assert suggest_sample_size(1000, g=10, k_per_group=15, floor=500) == 500  # floored (10*15=150 < floor)
    assert suggest_sample_size(1000, g=100, k_per_group=15, floor=500) == 1000  # 100*15=1500, capped at N
    assert suggest_sample_size(1000, g=50, k_per_group=15, floor=500) == 750  # 50*15=750, between floor and N


def test_suggest_sample_size_never_exceeds_population():
    assert suggest_sample_size(50, g=40, k_per_group=15, floor=500) == 50


def test_suggest_sample_size_zero_population():
    assert suggest_sample_size(0, g=0) == 0


def test_cumulative_subset_count_matches_manual_sum():
    # C(5,1)+C(5,2)+C(5,3) = 5+10+10 = 25
    assert _cumulative_subset_count(5, 3) == 25


def test_solve_candidate_fields_and_k_keeps_all_fields_when_budget_allows():
    max_fields, max_k = solve_candidate_fields_and_k(10, budget=100000, min_k=2)
    assert max_fields == 10
    assert max_k >= 2


def test_solve_candidate_fields_and_k_trims_fields_when_budget_too_small_for_min_k():
    # 60 candidates, tiny budget: even min_k=2 across 60 fields (C(60,1)+C(60,2)=60+1770=1830)
    # exceeds a budget of 100, so the field pool must shrink instead of k dropping below min_k.
    max_fields, max_k = solve_candidate_fields_and_k(60, budget=100, min_k=2)
    assert max_k == 2
    assert max_fields < 60
    assert _cumulative_subset_count(max_fields, 2) <= 100


def test_suggest_params_for_domain_flags_required_count_exceeding_discover_k():
    stats = {"records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5, "candidate_field_count": 40}
    result = suggest_params_for_domain(stats, required_count=8, subset_budget=2000, min_k=2)
    assert result["suggested_max_k_harsh_validate"] >= 8
    assert "required_items count" in result["notes"]


def test_suggest_params_for_domain_recommends_stratify_by_file_id_on_imbalance():
    # 6000 records, 40 groups -> sample_size well under population, and 201 files
    # where one contributes the overwhelming majority (mirrors the CLI smoke test).
    stats = {"records_total_domain": 6000, "distinct_sig_hash_groups": 40, "distinct_file_count": 201, "candidate_field_count": 1}
    result = suggest_params_for_domain(stats)
    assert result["stratify_by_recommended"] == "file_id"


def test_suggest_params_for_domain_no_stratify_recommendation_when_no_sampling_needed():
    # Population already small enough that suggested_sample_size == N -- no cap, so
    # imbalance across files doesn't matter (nothing gets excluded from the "sample").
    stats = {"records_total_domain": 50, "distinct_sig_hash_groups": 30, "distinct_file_count": 5, "candidate_field_count": 3}
    result = suggest_params_for_domain(stats)
    assert result["suggested_sample_size"] == 50
    assert result["stratify_by_recommended"] == ""


def test_cli_writes_suggestions_csv_and_reads_required_counts_from_policy(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "dimension_types_linear", "record_pk": "1", "sig_hash": "s1"},
        {"file_id": "f2", "domain": "dimension_types_linear", "record_pk": "2", "sig_hash": "s2"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "dimension_types_linear", "record_pk": "1", "item_key": "dim_type.shape", "item_value_type": "str", "item_value": "Linear"},
        {"domain": "dimension_types_linear", "record_pk": "2", "item_key": "dim_type.shape", "item_value_type": "str", "item_value": "Linear"},
    ])
    policy = tmp_path / "policy.json"
    policy.write_text('{"domains":{"dimension_types_linear":{"required_items":["dim_type.shape"]}}}', encoding="utf-8")

    subprocess.run([
        sys.executable, "tools/suggest_discovery_params.py",
        "--phase0-dir", str(phase0), "--policy-json", str(policy),
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    out = phase0.parent / "diagnostics" / "discovery_param_suggestions.csv"
    assert out.exists()
    with out.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["domain"] == "dimension_types_linear"
    assert rows[0]["required_items_count"] == "1"


def test_cli_emit_commands_prints_ready_to_run_invocations(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    r = subprocess.run([
        sys.executable, "tools/suggest_discovery_params.py",
        "--phase0-dir", str(phase0), "--emit-commands",
    ], cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True)
    assert "tools/discover_join_policy.py" in r.stdout
    assert "--domains units" in r.stdout
