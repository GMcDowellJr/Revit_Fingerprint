from __future__ import annotations
import csv, subprocess, sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.suggest_discovery_params import (
    compute_domain_stats,
    suggest_sample_size,
    _cumulative_subset_count,
    solve_candidate_fields_and_k,
    suggest_params_for_domain,
    _emit_command,
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
    assert stats["records_total_domain"] == 3
    assert stats["distinct_sig_hash_groups"] == 2
    assert stats["distinct_file_count"] == 2
    assert stats["candidate_field_count"] == 2
    assert set(stats["candidate_field_names_ranked"]) == {"units.spec", "units.accuracy"}
    # f1 carries 2/3 records, f2 carries 1/3: HHI = (2/3)^2 + (1/3)^2 = 5/9
    assert stats["file_hhi"] == pytest.approx(5.0 / 9.0)
    assert stats["file_effective_cluster_count"] == pytest.approx(9.0 / 5.0)


def test_compute_domain_stats_file_hhi_treats_blank_file_id_as_unknown_bucket():
    # docs/METRICS.md convention: closed universe, explicit unknown bucket for
    # blank/missing file_id rather than silently excluding those records.
    records = [
        {"domain": "units", "record_pk": "1", "sig_hash": "s1", "file_id": "f1"},
        {"domain": "units", "record_pk": "2", "sig_hash": "s1", "file_id": ""},
    ]
    stats = compute_domain_stats(records, [], "units")
    assert stats["distinct_file_count"] == 1
    # f1: 1/2, unknown bucket: 1/2 -> HHI = 0.25+0.25 = 0.5
    assert stats["file_hhi"] == pytest.approx(0.5)


def test_compute_domain_stats_file_hhi_perfectly_even_distribution():
    records = [{"domain": "units", "record_pk": str(i), "sig_hash": "s1", "file_id": f"f{i}"} for i in range(10)]
    stats = compute_domain_stats(records, [], "units")
    assert stats["distinct_file_count"] == 10
    # Perfectly even: HHI = 10 * (1/10)^2 = 0.1 -> effective_cluster_count = 10 (no concentration)
    assert stats["file_hhi"] == pytest.approx(0.1)
    assert stats["file_effective_cluster_count"] == pytest.approx(10.0)


def test_compute_domain_stats_file_hhi_fully_concentrated_in_one_file():
    records = [{"domain": "units", "record_pk": str(i), "sig_hash": "s1", "file_id": "big"} for i in range(10)]
    stats = compute_domain_stats(records, [], "units")
    assert stats["file_hhi"] == pytest.approx(1.0)
    assert stats["file_effective_cluster_count"] == pytest.approx(1.0)


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


def test_suggest_params_for_domain_flags_infeasible_required_baseline_stays_budget_safe():
    # discover_join_policy.py's actual pareto_search() has no required-fields-aware
    # enumeration (plain itertools.combinations over every candidate up to max_k),
    # so once required_count exceeds the budget-derived discover_max_k, bumping
    # max_k to represent it is provably infeasible at the SAME pool/budget --
    # solve_candidate_fields_and_k already returns the largest affordable k, so
    # anything beyond it can never fit the identical budget (monotonicity of
    # Sum C(pool, i)). Must stay at the safe discover_max_k, not silently
    # recommend a command that would enumerate billions of subsets.
    stats = {"records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5, "candidate_field_count": 40}
    result = suggest_params_for_domain(stats, required_count=8, subset_budget=2000, min_k=2)
    assert result["suggested_max_k_harsh_validate"] == result["suggested_max_k_discover"]
    assert result["harsh_pareto_feasible"] is False
    assert "too large for harsh/validate Pareto search" in result["notes"]
    assert "--search-modes greedy" in result["notes"]


def test_suggest_params_for_domain_harsh_feasible_when_required_count_within_discover_k():
    # required_count already <= discover_max_k -- no infeasibility, no bump needed.
    stats = {"records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5, "candidate_field_count": 10}
    result = suggest_params_for_domain(stats, required_count=2, subset_budget=100000, min_k=2)
    assert result["harsh_pareto_feasible"] is True
    assert result["suggested_max_k_harsh_validate"] == result["suggested_max_k_discover"]
    assert "too large for harsh/validate" not in result["notes"]


def test_suggest_params_for_domain_optional_items_inflate_pool_but_do_not_require_a_single_subset():
    # Codex finding (corrects an earlier over-conservative version of this logic):
    # optional_items enlarge the harsh/validate candidate POOL discover_join_policy.py
    # searches over (unconditionally, like required_items), but -- unlike required
    # fields -- nothing forces them to all co-occur together in one selected subset:
    # validate mode's post-hoc filter only demands required fields be a subset of
    # `selected` (harsh imposes no such constraint on selected at all). So a domain
    # with NO required fields and a large optional_items list (Codex's own example:
    # 10 data candidates, 0 required, 20 optional) must still be FEASIBLE -- Pareto
    # can validly explore small subsets from the larger pool -- even though the
    # larger pool means harsh/validate's affordable max_k ends up smaller than
    # discover mode's (2 vs 10 here), not infeasible.
    stats = {"records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5, "candidate_field_count": 10}
    result = suggest_params_for_domain(stats, required_count=0, optional_count=20, subset_budget=2000, min_k=2)
    assert result["harsh_pareto_feasible"] is True
    assert result["suggested_max_k_harsh_validate"] < result["suggested_max_k_discover"]
    assert result["suggested_max_k_harsh_validate"] > 0
    assert "enlarges the harsh/validate candidate pool" in result["notes"]


def test_suggest_params_for_domain_required_items_alone_bump_harsh_max_k_independent_of_optional():
    # required_count (8) exceeds discover_max_k (5, capped by the tiny data
    # candidate pool here) and must be bumped, independent of optional_count
    # (0) -- the minimum k that can represent selecting the full required
    # baseline as one subset is required_count alone, never required_count +
    # optional_count (see the finding test above).
    stats = {"records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5, "candidate_field_count": 5}
    result = suggest_params_for_domain(stats, required_count=8, optional_count=0, subset_budget=10**9, min_k=1)
    assert result["harsh_pareto_feasible"] is True
    assert result["suggested_max_k_harsh_validate"] == 8
    assert result["suggested_max_k_harsh_validate"] > result["suggested_max_k_discover"]
    assert "exceeds the discover-mode budget max_k" in result["notes"]


def test_suggest_params_for_domain_dedupes_pool_size_when_required_fields_overlap_candidates():
    # Codex finding: harsh_pool_size was computed as a pessimistic arithmetic sum
    # (max_candidate_fields + required_count + optional_count), but
    # discover_join_policy.py's actual work_candidates is a DEDUPLICATED union --
    # required fields normally overlap the observed candidate pool (they were
    # presumably discovered as legitimate identity items). Codex's own example:
    # 10 observed candidates, 7 required fields drawn FROM those same 10 -- the
    # real pool is 10 (967 subsets through k=7), not the arithmetic 17 (41,225
    # subsets), which would falsely exceed a --subset-budget of 2000. Supplying
    # the actual field names must recover feasibility that count-only sizing
    # missed.
    candidate_names = [f"field{i}" for i in range(10)]
    required_names = candidate_names[:7]
    stats = {
        "records_total_domain": 500, "distinct_sig_hash_groups": 20, "distinct_file_count": 5,
        "candidate_field_count": 10, "candidate_field_names_ranked": candidate_names,
    }
    with_names = suggest_params_for_domain(
        stats, required_count=7, optional_count=0, required_field_names=required_names,
        subset_budget=2000, min_k=2,
    )
    assert with_names["harsh_pareto_feasible"] is True
    assert with_names["suggested_max_k_harsh_validate"] == with_names["suggested_max_k_discover"]

    # Without names (count-only, the fallback every existing caller/test uses),
    # the pessimistic arithmetic sum still marks this infeasible -- confirms the
    # fix is specifically about using names when available, not a budget change.
    stats_no_names = {k: v for k, v in stats.items() if k != "candidate_field_names_ranked"}
    without_names = suggest_params_for_domain(stats_no_names, required_count=7, optional_count=0, subset_budget=2000, min_k=2)
    assert without_names["harsh_pareto_feasible"] is False


def test_suggest_params_for_domain_recommends_stratify_by_file_id_on_real_concentration():
    # One file with 5000 of 6000 records, 200 files with 5 each (mirrors the CLI
    # smoke test) -- built via compute_domain_stats so file_hhi reflects the
    # actual concentration, not a hand-picked stats dict.
    records = [{"domain": "d", "record_pk": f"tpl_{i}", "sig_hash": f"g{i % 40}", "file_id": "f_template"} for i in range(5000)]
    for fi in range(200):
        for j in range(5):
            records.append({"domain": "d", "record_pk": f"proj_{fi}_{j}", "sig_hash": f"g{(fi + j) % 8}", "file_id": f"f{fi}"})
    stats = compute_domain_stats(records, [], "d")
    result = suggest_params_for_domain(stats)
    assert result["stratify_by_recommended"] == "file_id"


def test_suggest_params_for_domain_no_stratify_recommendation_when_records_evenly_spread():
    # Same N/F average as the concentrated case above (30/1 avg either way is not
    # what matters) but genuinely even distribution across files -- must NOT
    # recommend stratification just because N is large relative to sample size.
    records = [{"domain": "d", "record_pk": f"r{i}", "sig_hash": f"g{i % 40}", "file_id": f"f{i}"} for i in range(6000)]
    stats = compute_domain_stats(records, [], "d")
    result = suggest_params_for_domain(stats)
    assert result["stratify_by_recommended"] == ""


def test_suggest_params_for_domain_no_stratify_recommendation_when_no_sampling_needed():
    # Population already small enough that suggested_sample_size == N -- no cap, so
    # imbalance across files doesn't matter (nothing gets excluded from the "sample"),
    # even though this population IS concentrated in one file.
    records = [{"domain": "d", "record_pk": f"r{i}", "sig_hash": f"g{i % 30}", "file_id": "only_file"} for i in range(50)]
    stats = compute_domain_stats(records, [], "d")
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


def test_cli_emit_commands_uses_resolved_phase0_dir_not_unresolved_argument(tmp_path: Path):
    # Codex finding: --phase0-dir may be given as one of the root forms
    # _resolve_phase0_dir accepts (here: a root containing results/records/) --
    # the suggestion run itself resolves and reads correctly, but the emitted
    # ready-to-run command must use the SAME resolved directory, since
    # discover_join_policy.py performs no such resolution itself and reads
    # straight from "<argument>/records.csv". An emitted command built from the
    # unresolved root argument would fail when actually run.
    root = tmp_path / "corpus_root"
    phase0 = root / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    r = subprocess.run([
        sys.executable, "tools/suggest_discovery_params.py",
        "--phase0-dir", str(root), "--emit-commands",
    ], cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True)
    assert f"--phase0-dir {phase0}" in r.stdout
    assert f"--phase0-dir {root} " not in r.stdout
    assert not r.stdout.rstrip().endswith(f"--phase0-dir {root}")


def test_emit_command_single_command_when_discover_and_harsh_k_match():
    suggestion = {
        "suggested_sample_size": 500, "suggested_max_candidate_fields": 20,
        "suggested_max_k_discover": 3, "suggested_max_k_harsh_validate": 3,
        "stratify_by_recommended": "",
    }
    cmds = _emit_command("units", suggestion, "results/records", None)
    assert len(cmds) == 1
    assert "--max-k 3" in cmds[0]
    assert "--policy-modes" not in cmds[0]  # single command: leave default discover,validate,harsh


def test_emit_command_splits_into_discover_and_harsh_commands_when_k_differs():
    # Mirrors a domain whose existing required_items baseline pushes harsh/validate's
    # needed max_k above what the discover-mode budget solve produced.
    suggestion = {
        "suggested_sample_size": 500, "suggested_max_candidate_fields": 20,
        "suggested_max_k_discover": 3, "suggested_max_k_harsh_validate": 10,
        "stratify_by_recommended": "",
    }
    cmds = _emit_command("dimension_types_linear", suggestion, "results/records", None)
    assert len(cmds) == 2
    discover_cmd, harsh_cmd = cmds
    assert "--max-k 3" in discover_cmd and "--policy-modes discover" in discover_cmd
    assert "--max-k 10" in harsh_cmd and "--policy-modes validate,harsh" in harsh_cmd


def test_emit_command_forces_greedy_on_harsh_command_when_pareto_infeasible():
    # suggest_params_for_domain keeps suggested_max_k_harsh_validate ==
    # suggested_max_k_discover whenever Pareto can't represent the baseline within
    # budget (harsh_pareto_feasible=False) -- so equal k values alone can't be used
    # to decide whether a single combined command is safe; harsh_pareto_feasible
    # must still force a split so only the harsh/validate command (not discover,
    # which stays cheap regardless) gets --search-modes greedy.
    suggestion = {
        "suggested_sample_size": 500, "suggested_max_candidate_fields": 20,
        "suggested_max_k_discover": 4, "suggested_max_k_harsh_validate": 4,
        "harsh_pareto_feasible": False,
        "stratify_by_recommended": "",
    }
    cmds = _emit_command("dimension_types_linear", suggestion, "results/records", None)
    assert len(cmds) == 2
    discover_cmd, harsh_cmd = cmds
    assert "--search-modes greedy" not in discover_cmd
    assert "--policy-modes discover" in discover_cmd
    assert "--search-modes greedy" in harsh_cmd
    assert "--policy-modes validate,harsh" in harsh_cmd


def test_emit_command_single_command_when_harsh_feasible_and_k_matches():
    # The realistic case from suggest_params_for_domain now that harsh_max_k is
    # budget-checked: harsh_k == discover_k AND feasible -> single command, no
    # unnecessary --policy-modes/--search-modes overrides.
    suggestion = {
        "suggested_sample_size": 500, "suggested_max_candidate_fields": 20,
        "suggested_max_k_discover": 4, "suggested_max_k_harsh_validate": 4,
        "harsh_pareto_feasible": True,
        "stratify_by_recommended": "",
    }
    cmds = _emit_command("units", suggestion, "results/records", None)
    assert len(cmds) == 1
    assert "--policy-modes" not in cmds[0]
    assert "--search-modes" not in cmds[0]


def test_emit_command_quotes_paths_containing_spaces():
    # Codex finding: dynamic values (phase0_dir, policy_json, domain,
    # stratify_by_recommended) were interpolated into the printed command
    # without shell quoting -- a valid directory like "/tmp/Revit Results/records"
    # would split into two shell arguments when the emitted command is actually
    # run, and discover_join_policy.py would fail to find records.csv.
    suggestion = {
        "suggested_sample_size": 500, "suggested_max_candidate_fields": 20,
        "suggested_max_k_discover": 4, "suggested_max_k_harsh_validate": 4,
        "harsh_pareto_feasible": True,
        "stratify_by_recommended": "",
    }
    cmds = _emit_command("units", suggestion, "/tmp/Revit Results/records", "/tmp/My Policies/policy.json")
    assert len(cmds) == 1
    assert "'/tmp/Revit Results/records'" in cmds[0]
    assert "'/tmp/My Policies/policy.json'" in cmds[0]
    # Plain paths/names with no shell metacharacters must NOT gain quotes they
    # didn't need -- shlex.quote() only quotes when actually required.
    plain_cmds = _emit_command("units", suggestion, "results/records", None)
    assert "results/records" in plain_cmds[0]
    assert "'results/records'" not in plain_cmds[0]
