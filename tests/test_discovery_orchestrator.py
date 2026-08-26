import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools.discovery_orchestrator import (
    DISCOVERY_ENGINE_VERSION, SUMMARY_FIELDS, Orchestrator, Config, acceptance_reasons,
    accepted, cache_key, canonical_json, input_fingerprint, stage_cache_eligible,
    all_shape_gates_accepted, sweep_evidence_rows, timestamp_now,
)


def clean(**overrides):
    row={"status":"ok","coverage_full":"1.0","collision_rate_full":"0.0",
         "fragmentation_rate_full":"0.0","sample_vs_full_diverges":"false"}
    row.update(overrides);return row


@pytest.mark.parametrize("change,reason", [
    ({"status":"blocked"},"status_not_ok"),({"coverage_full":".9"},"coverage_incomplete"),
    ({"collision_rate_full":".1"},"collision_present"),
    ({"fragmentation_rate_full":".1"},"fragmentation_present"),
    ({"sample_vs_full_diverges":"true"},"sample_full_divergence"),
])
def test_acceptance_gate_escalation_reasons(change, reason):
    assert reason in acceptance_reasons(clean(**change))
    assert not accepted(clean(**change))


def test_clean_greedy_accepts_without_escalation():
    assert accepted(clean()) and acceptance_reasons(clean()) == []


def test_cache_key_distinguishes_shape_gate_and_modes():
    keys={cache_key("loaded_family_types","sig",p,s,g) for p in ("discover","validate")
          for s in ("greedy","pareto") for g in ("Doors","Windows")}
    assert len(keys)==8


def _write(path: Path, fields, rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)


def fixture_inputs(tmp_path: Path):
    exports=tmp_path/"exports";repo=tmp_path/"repo";records=exports/"records"
    _write(records/"records.csv",["domain","record_pk","sig_hash"],[{"domain":"walls","record_pk":"2","sig_hash":"b"},{"domain":"walls","record_pk":"1","sig_hash":"a"}])
    _write(records/"identity_items.csv",["domain","record_pk","item_key","item_value"],[{"domain":"walls","record_pk":"1","item_key":"x","item_value":"1"}])
    for name in ("domain_join_key_policies.json","domain_sig_hash_policies.json"):
        p=repo/"policies"/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps({"domains":{"walls":{"required_fields":["x"]}}}))
    (repo/"policies"/"discovery_candidate_eligibility.json").write_text(json.dumps({"schema_version":"1.0","global":{},"domains":{"walls":{}}}))
    return exports,repo,records


def test_fingerprint_is_deterministic_and_csv_order_independent(tmp_path):
    exports,repo,records=fixture_inputs(tmp_path);policy=repo/"policies/domain_join_key_policies.json"
    params={"sample_size":2,"sample_seed":17,"stratify_by":"","max_candidate_fields":4,"effective_max_k":2}
    a=input_fingerprint(records,policy,"walls","join","discover","greedy","__all__",params)
    rows=list(reversed(list(csv.DictReader((records/"records.csv").open()))))
    _write(records/"records.csv",["domain","record_pk","sig_hash"],rows)
    assert input_fingerprint(records,policy,"walls","join","discover","greedy","__all__",params)==a


@pytest.mark.parametrize("mutation",["data","policy","eligibility","parameter","version"])
def test_result_affecting_changes_invalidate_fingerprint(tmp_path, mutation):
    exports,repo,records=fixture_inputs(tmp_path);policy=repo/"policies/domain_join_key_policies.json"
    params={"sample_size":2,"sample_seed":17,"stratify_by":"","max_candidate_fields":4,"effective_max_k":2}
    a=input_fingerprint(records,policy,"walls","join","validate","greedy","__all__",params)
    version=DISCOVERY_ENGINE_VERSION
    if mutation=="data":
        with (records/"records.csv").open("a") as f:f.write("walls,3,c\n")
    elif mutation=="policy": policy.write_text(json.dumps({"domains":{"walls":{"required_fields":["y"]}}}))
    elif mutation=="eligibility": (repo/"policies"/"discovery_candidate_eligibility.json").write_text(json.dumps({"schema_version":"1.0","global":{"excluded_candidates":[{"item":"x"}]},"domains":{"walls":{}}}))
    elif mutation=="parameter": params={**params,"sample_size":1}
    else: version="next"
    assert input_fingerprint(records,policy,"walls","join","validate","greedy","__all__",params,version)!=a


def test_timestamp_has_subsecond_collision_resistance():
    assert timestamp_now()!=timestamp_now()


def test_summary_schema_contains_provenance_and_review_fields():
    required={"summary_timestamp","domain_result_timestamp","source_run_id","shape_gate",
              "decision_status","governance_status","input_fingerprint","refresh_status"}
    assert required <= set(SUMMARY_FIELDS)


def test_partition_diagnostics_are_not_sweep_evidence():
    global_row = {"result_scope": "", "selected_fields": "global"}
    partition_row = {"result_scope": "partition_diagnostic", "selected_fields": "shape_only"}
    assert sweep_evidence_rows([global_row, partition_row]) == [global_row]


def test_canonical_json_ignores_mapping_order():
    assert canonical_json({"b":2,"a":1})==canonical_json({"a":1,"b":2})


def test_partial_without_baseline_is_allowed(tmp_path):
    exports,repo,_=fixture_inputs(tmp_path)
    suggestions=exports/"diagnostics/discovery_param_suggestions.csv"
    _write(suggestions,["domain","suggested_sample_size","suggested_max_candidate_fields","suggested_max_k_discover","suggested_max_k_harsh_validate","stratify_by_recommended"],[{"domain":"walls","suggested_sample_size":"2","suggested_max_candidate_fields":"2","suggested_max_k_discover":"1","suggested_max_k_harsh_validate":"1","stratify_by_recommended":""}])
    cfg=Config(exports,repo,suggestions,["walls"],what_if=True)
    plan = Orchestrator(cfg).run_sweep()
    assert plan["requested_domains"] == ["walls"]


def test_force_is_preserved_in_shared_config(tmp_path):
    exports,repo,_=fixture_inputs(tmp_path)
    cfg=Config(exports,repo,exports/"x.csv",force=True)
    assert Orchestrator(cfg).cfg.force is True


def test_powershell_is_thin_shared_entry_point():
    text=Path("run_discovery_sweep.ps1").read_text(encoding="utf-8")
    assert "run_discovery_sweep.py" in text and "discover_join_policy.py" not in text
    for flag in ("ExportsRoot","RepoRoot","SuggestionsCsv","Domains","SkipJoin","SkipSig","Force","WhatIf","Run"):
        assert f"${flag}" in text


def test_blocked_run_cache_entries_are_discarded_without_touching_history():
    cache={"entries":{
        "failed-stage":{"result_run_id":"failed","result_path":"x.tmp/a.csv"},
        "prior-stage":{"result_run_id":"prior","result_path":"old/a.csv"},
    }}
    Orchestrator._discard_run_cache_entries(cache,"failed")
    assert set(cache["entries"])=={"prior-stage"}


def test_run_summary_aggregates_all_shape_gates_conservatively():
    rows=[
        {"shape_gate":"Doors","decision_status":"supported","pareto_required":"false",
         "harsh_required":"false","result_provenance_status":"fresh","refresh_status":"completed"},
        {"shape_gate":"Windows","decision_status":"blocked","decision_reason":"missing evidence",
         "pareto_required":"true","harsh_required":"true","result_provenance_status":"cached",
         "refresh_status":"blocked","refresh_error":"validation process failed"},
    ]
    result=Orchestrator._aggregate_domain(rows)
    assert result["decision_status"]=="blocked"
    assert result["pareto_invoked"] and result["harsh_invoked"]
    assert result["provenance"]=={"fresh","cached"}
    assert result["blocked_reasons"]==["missing evidence"]
    assert result["warnings"]==["validation process failed"]


@pytest.mark.parametrize("change",[
    {"coverage_full":".9"},{"collision_rate_full":".1"},
    {"fragmentation_rate_full":".1"},{"sample_vs_full_diverges":"true"},
])
def test_ok_status_with_failed_gate_is_not_cache_eligible(change):
    assert clean(**change)["status"]=="ok"
    assert not stage_cache_eligible([clean(**change)])


def test_every_shape_gate_must_pass_before_aggregate_stage_is_cache_eligible():
    assert stage_cache_eligible([clean(),clean()])
    assert not stage_cache_eligible([clean(),clean(collision_rate_full=".1")])


def test_harsh_is_required_when_any_validation_shape_gate_fails():
    rows=[
        {**clean(),"policy_mode":"validate","shape_gate":"Doors","search_mode":"greedy"},
        {**clean(collision_rate_full=".1"),"policy_mode":"validate","shape_gate":"Windows","search_mode":"greedy"},
        {**clean(),"policy_mode":"validate","shape_gate":"Windows","search_mode":"pareto"},
    ]
    assert all_shape_gates_accepted(rows,"validate")
    rows[-1]={**rows[-1],"collision_rate_full":".1"}
    assert not all_shape_gates_accepted(rows,"validate")


def test_missing_validation_gate_requires_harsh():
    rows=[
        {**clean(),"policy_mode":"discover","shape_gate":"Doors"},
        {**clean(),"policy_mode":"discover","shape_gate":"Windows"},
        {**clean(),"policy_mode":"validate","shape_gate":"Doors"},
    ]
    assert not all_shape_gates_accepted(rows,"validate")


def test_sig_archives_greedy_and_pareto_under_distinct_names(tmp_path):
    exports,repo,_=fixture_inputs(tmp_path)
    orchestrator=Orchestrator(Config(exports,repo,exports/"suggestions.csv"))
    diagnostics=exports/"diagnostics";run_dir=tmp_path/"run"
    generated=diagnostics/"hash_sig_discovery_exploration__walls__discover.csv"
    generated.parent.mkdir(parents=True,exist_ok=True);generated.write_text("greedy")
    greedy=orchestrator._archive_artifacts("sig","walls","discover","greedy",run_dir)
    generated.write_text("pareto")
    pareto=orchestrator._archive_artifacts("sig","walls","discover","pareto",run_dir)
    assert greedy[0] != pareto[0]
    assert greedy[0].read_text()=="greedy" and pareto[0].read_text()=="pareto"


def test_invoke_streams_output_and_reports_quiet_heartbeat(tmp_path,monkeypatch,capsys):
    exports,repo,_=fixture_inputs(tmp_path)
    orchestrator=Orchestrator(Config(exports,repo,exports/"suggestions.csv"))
    monkeypatch.setattr("tools.discovery_orchestrator.PROGRESS_HEARTBEAT_SECONDS",0.01)
    log=tmp_path/"logs/invoke.log"
    orchestrator._invoke([sys.executable,"-c","import time; time.sleep(.03); print('finished')"],log)
    console=capsys.readouterr().out
    assert "START" in console and "still running" in console and "DONE" in console
    assert "finished" in console and log.read_text()=="finished\n"


def test_invoke_keeps_fake_runner_support_for_fast_orchestration_tests(tmp_path,capsys):
    exports,repo,_=fixture_inputs(tmp_path)
    calls=[]
    def fake(cmd,**kwargs):
        calls.append((cmd,kwargs));return subprocess.CompletedProcess(cmd,0,"fake output\n","")
    orchestrator=Orchestrator(Config(exports,repo,exports/"suggestions.csv"),runner=fake)
    log=tmp_path/"fake.log";orchestrator._invoke(["fake","command"],log)
    assert calls and log.read_text()=="fake output\n"
    assert "fake output" in capsys.readouterr().out


def test_orchestrator_scans_each_large_fingerprint_csv_only_once(tmp_path,monkeypatch):
    exports,repo,_=fixture_inputs(tmp_path)
    import tools.discovery_orchestrator as module
    original=module.iter_csv;calls=[]
    def counting(path):
        calls.append(Path(path).name);yield from original(path)
    monkeypatch.setattr(module,"iter_csv",counting)
    orchestrator=Orchestrator(Config(exports,repo,exports/"suggestions.csv"))
    params={"sample_size":2,"sample_seed":17,"stratify_by":"","max_candidate_fields":4,"effective_max_k":2}
    orchestrator._input_fingerprint("walls","join","discover","greedy","__all__",params)
    orchestrator._input_fingerprint("walls","join","validate","greedy","__all__",params)
    orchestrator._input_fingerprint("walls","sig","discover","greedy","__all__",params)
    assert calls.count("records.csv")==1
    assert calls.count("identity_items.csv")==1
