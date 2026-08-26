#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json
from pathlib import Path
from typing import Dict,List
try:
    from tools.discover_join_policy import _read_csv,_write_csv,_sample_domain_records,_stratified_sample,_pick_candidate_fields,_without_excluded,_pareto_search_adapter,_diagnostics_domain_suffix,_full_population_verify
    from tools.discovery_candidate_eligibility import diagnostic_fields as _candidate_diagnostics, filter_and_cap_candidates
    from tools.join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate, summarize_shape_gate_usage
    from tools.join_key_discovery.greedy import discover_greedy
except ModuleNotFoundError:
    from discover_join_policy import _read_csv,_write_csv,_sample_domain_records,_stratified_sample,_pick_candidate_fields,_without_excluded,_pareto_search_adapter,_diagnostics_domain_suffix,_full_population_verify
    from discovery_candidate_eligibility import diagnostic_fields as _candidate_diagnostics, filter_and_cap_candidates
    from join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate, summarize_shape_gate_usage
    from join_key_discovery.greedy import discover_greedy

TARGET_FILES={"sig":["signature_items.csv","identity_items.csv","phase0_identity_items.csv"],"join":["join_items.csv","identity_items.csv","phase0_identity_items.csv"]}
CATEGORY_GATE_KEY="lft.shape_gate.category"


def _resolve_phase0_dir(path: Path) -> Path:
    """
    Accept either:
      - direct phase0 folder (contains records.csv), or
      - Results_v21 root (contains phase0_v21/records.csv).
      - pipeline default records root (results/records/records.csv).
    """
    if (path / "records.csv").exists():
        return path
    records_dir = path / "records"
    if (records_dir / "records.csv").exists():
        return records_dir
    results_records = path / "results" / "records"
    if (results_records / "records.csv").exists():
        return results_records
    nested = path / "phase0_v21"
    if (nested / "records.csv").exists():
        return nested
    return path

def _load_items(phase0:Path,target:str)->List[Dict[str,str]]:
    for name in TARGET_FILES[target]:
        p=phase0/name
        if p.exists(): return _read_csv(p)
    return []

def _domain_rows(records,items,domain,target):
    rec=[r for r in records if r.get("domain")==domain]
    if domain!="loaded_family_types":
        return [("__all__",rec,[it for it in items if it.get("domain")==domain])]
    # category-gated discovery
    by_pk={r.get('record_pk','').strip():r for r in rec if r.get('record_pk','').strip()}
    cat_by_pk={}
    for it in items:
        if it.get("domain")!=domain or it.get("item_key")!=CATEGORY_GATE_KEY: continue
        pk=it.get('record_pk','').strip(); val=it.get('item_value','').strip()
        if pk and val: cat_by_pk[pk]=val
    gates=sorted(set(cat_by_pk.values()))
    out=[]
    for gate in gates:
        pks={pk for pk,val in cat_by_pk.items() if val==gate}
        out.append((gate,[r for r in rec if r.get('record_pk','').strip() in pks],[it for it in items if it.get('domain')==domain and it.get('record_pk','').strip() in pks]))
    # Surface records lacking shape gate so diagnostics don't silently skip them.
    rec_pks = {r.get('record_pk', '').strip() for r in rec if r.get('record_pk', '').strip()}
    missing_gate_pks = rec_pks - set(cat_by_pk.keys())
    if missing_gate_pks:
        out.append((
            "__missing_shape_gate__",
            [r for r in rec if r.get('record_pk', '').strip() in missing_gate_pks],
            [it for it in items if it.get('domain') == domain and it.get('record_pk', '').strip() in missing_gate_pks],
        ))
    return out

def _run_target(target,args,records,domains,base_domains,phase0_dir: Path):
    rows=[]; candidates={}
    full_verify=not bool(getattr(args,'no_full_verify',False))
    divergence_delta=float(getattr(args,'divergence_collision_delta',0.01))
    coverage_drop_threshold=float(getattr(args,'coverage_drop_threshold',0.05))
    shard_dir=phase0_dir/"identity_items_by_domain"
    # Require the .complete sentinel before using shards. A partial shard directory
    # (interrupted flatten) must not be treated as authoritative — missing shards
    # would silently produce empty item sets for affected domains.
    _use_shards=(shard_dir/".complete").is_file()
    stratify_key=getattr(args,'stratify_by','') or ''
    for domain in domains:
        # Load only this domain's items — prefer per-domain shard, fall back to filtered monolithic.
        if _use_shards:
            _shard=shard_dir/f"{domain}.csv"
            items=_read_csv(_shard) if _shard.exists() else []
        else:
            items=[]
            for _name in TARGET_FILES[target]:
                _p=phase0_dir/_name
                if _p.exists():
                    items=[r for r in _read_csv(_p) if r.get("domain")==domain]
                    break
        normalized=normalize_policy_block(base_domains.get(domain,{}))
        req=normalized['required_fields']; opt=normalized['optional_items']; excluded=set(normalized['explicitly_excluded_items']); gates=normalized['gates']
        gate_cfg={"required_fields":req,**gates}
        for gate,dom_records_all,dom_items_all in _domain_rows(records,items,domain,target):
            if not dom_records_all:
                continue
            if stratify_key:
                dom_records=_stratified_sample(dom_records_all,dom_items_all,stratify_key,args.sample_size,args.sample_seed)
            else:
                dom_records=_sample_domain_records(dom_records_all,args.sample_size,args.sample_seed)
            if not dom_records:
                continue
            sampled={r.get('record_pk','').strip() for r in dom_records}
            dom_items=[it for it in dom_items_all if not sampled or it.get('record_pk','').strip() in sampled]
            raw_unfiltered=_pick_candidate_fields(dom_items,0)
            candidate_filter=filter_and_cap_candidates(domain,raw_unfiltered,args.max_candidate_fields)
            raw=candidate_filter["eligible"]
            scoped=_without_excluded(raw,excluded)
            if domain=="loaded_family_types" and CATEGORY_GATE_KEY in raw:
                scoped=[CATEGORY_GATE_KEY]+[f for f in scoped if f!=CATEGORY_GATE_KEY]
            idx=build_identity_index(dom_items)
            shape_gate_summary_sample=summarize_shape_gate_usage(dom_records,idx,req,gate_cfg)
            full_idx=build_identity_index(dom_items_all)
            shape_gate_summary_full=summarize_shape_gate_usage(dom_records_all,full_idx,req,gate_cfg)
            # See tools/discover_join_policy.py's identical comment: discover_greedy()
            # (and Pareto's own validate-mode fallback) now echo cfg.gates.required_fields
            # back as selected_fields regardless of whether those fields are populated
            # anywhere in the data, so name-matching selected against req alone can no
            # longer detect "required field doesn't exist in the data" -- check directly.
            # Checked against dom_items_all (the FULL, unsampled/uncapped per-gate item
            # set), not raw (_pick_candidate_fields' sampled-and-capped output) -- a
            # required field populated only on an unsampled record, or simply ranked
            # below --max-candidate-fields, would otherwise be wrongly reported as
            # absent from the data entirely.
            all_item_keys_domain={it.get('item_key','').strip() for it in dom_items_all if it.get('item_key','').strip()}
            req_missing_from_data=set(req)-all_item_keys_domain
            for pm in args.policy_modes:
                work=scoped if pm=="discover" else _without_excluded(req+opt if pm=="validate" else req+opt+scoped,excluded)
                max_k = args.max_k
                if pm == "validate" and req:
                    max_k = max(max_k, len(req))
                score_cfg={
                    "max_k":max_k,
                    "work_budget":int(args.work_budget),
                    "frontier_limit":int(args.pareto_frontier_limit),
                    "progress":not bool(args.no_pareto_progress),
                    "domain":domain,
                    "gates":dict(gates),
                    "evaluation_mode":"candidate" if pm=="discover" else "runtime",
                    "runtime_required_fields":[] if pm=="discover" else list(req),
                }
                for sm in args.search_modes:
                    selected=[];metrics={};status="ok";reason="";frontier=0;fallback=False
                    search_diagnostics={};pareto_full_verification=None
                    if not work: status="no_candidates"
                    elif sm=="pareto":
                        if full_verify:
                            def _verify_finalist(finalist):
                                finalist_metrics=finalist.get("metrics",{})
                                finalist_fields=[x for x in str(finalist.get("keys","")).split("|") if x]
                                full_metrics,finalist_diverges=_full_population_verify(
                                    dom_records_all,full_idx,finalist_fields,score_cfg,finalist_metrics,
                                    divergence_delta,coverage_drop_threshold=coverage_drop_threshold,
                                )
                                accepted_full=(
                                    float(full_metrics.get("coverage",0.0))==1.0
                                    and float(full_metrics.get("collision_rate",1.0))==0.0
                                    and float(full_metrics.get("fragmentation_rate",1.0))==0.0
                                    and not finalist_diverges
                                )
                                return {"metrics":full_metrics,"diverges":finalist_diverges,"accepted_full":accepted_full}
                            score_cfg["finalist_verifier"]=_verify_finalist
                        p=_pareto_search_adapter(dom_records,idx,work,score_cfg)
                        search_diagnostics=p.get("diagnostics",{}) if isinstance(p.get("diagnostics"),dict) else {}
                        pareto_full_verification=search_diagnostics.get("full_verification")
                        fr=p.get('frontier') if isinstance(p.get('frontier'),list) else [];frontier=len(fr)
                        if pm == "validate" and req:
                            fr = [row for row in fr if set(req).issubset(set(str(row.get("keys", "")).split("|")))]
                            frontier = len(fr)
                        if fr:
                            ch=p.get("chosen") if isinstance(p.get("chosen"),dict) else sorted(fr,key=lambda x:(x.get('k_count',99),x.get('coverage_gap',1.0),x.get('collision_rate',1.0),x.get('fragmentation_rate',1.0),x.get('keys','')))[0]
                            selected=[x for x in str(ch.get('keys','')).split('|') if x];metrics=ch.get('metrics',{}) if isinstance(ch.get('metrics'),dict) else {}
                        elif pm == "validate" and req:
                            selected = list(req)
                            metrics = score_candidate(dom_records,idx,selected,score_cfg)
                            fallback = True
                            reason = "required_set_fallback"
                        else: status="blocked";reason="no_frontier"
                    else:
                        g=discover_greedy(dom_records,idx,work,score_cfg)
                        selected=[str(x) for x in g.get('selected_fields',[]) if str(x).strip()];metrics=g.get('metrics',{}) if isinstance(g.get('metrics'),dict) else {}
                    if pm=="validate" and req and (not set(req).issubset(set(selected)) or req_missing_from_data):
                        status="blocked_missing_required"
                        if not reason:
                            reason="required_fields_absent_from_data:"+",".join(sorted(req_missing_from_data)) if req_missing_from_data else "selected_missing_required"
                    # Strip required_fields the same way discover_greedy()/pareto_search()
                    # already do for their own internal scoring, and the same way
                    # discover_join_policy.py's own full-population verify (verify_gates/
                    # verify_cfg) does: build_candidate_join_key_with_details's
                    # base_required = gates.get("required_fields") or selected_fields
                    # otherwise silently substitutes the policy's required_fields (and any
                    # shape_gating additional_required riding along with it) for the actual
                    # `selected` candidate whenever this fallback/verify path runs -- scoring
                    # a different, larger key than the one actually selected.
                    verify_cfg=dict(score_cfg)
                    if not metrics and work: metrics=score_candidate(dom_records,idx,selected,verify_cfg)

                    full_verify_status="skipped_no_full_verify_flag"
                    metrics_full={}
                    diverges=False
                    if selected and full_verify and isinstance(pareto_full_verification,dict):
                        metrics_full=pareto_full_verification.get("metrics",{})
                        diverges=bool(pareto_full_verification.get("diverges",False))
                        full_verify_status="ok"
                    elif selected and full_verify:
                        metrics_full,diverges=_full_population_verify(
                            dom_records_all,full_idx,selected,verify_cfg,metrics,divergence_delta,
                            coverage_drop_threshold=coverage_drop_threshold,
                        )
                        full_verify_status="ok"
                        if diverges:
                            print(
                                f"[discover] WARNING domain={domain} discovery_target={target} policy_mode={pm} search_mode={sm} shape_gate={gate} "
                                f"sample-based metrics diverge from full population: "
                                f"collision_rate sample={float(metrics.get('collision_rate',1.0)):.6f} full={float(metrics_full.get('collision_rate',1.0)):.6f}, "
                                f"fragmentation_rate sample={float(metrics.get('fragmentation_rate',1.0)):.6f} full={float(metrics_full.get('fragmentation_rate',1.0)):.6f}, "
                                f"coverage sample={float(metrics.get('coverage',0.0)):.6f} full={float(metrics_full.get('coverage',0.0)):.6f} "
                                f"-- do not pin this candidate without review.",
                                flush=True,
                            )
                    elif not selected:
                        full_verify_status="skipped_no_selection"

                    rows.append({"domain":domain,"discovery_target":target,"policy_mode":pm,"mode":pm,"search_mode":sm,"status":status,"reason":reason,"selected_fields":"|".join(selected),"effective_fields_actually_scored":"|".join(str(x) for x in metrics.get('effective_fields_actually_scored',[])),**_candidate_diagnostics(candidate_filter),"candidate_fields_available":"|".join(scoped),"candidate_fields_evaluated":"|".join(work),"policy_required_fields":"|".join(req),"policy_optional_fields":"|".join(opt),"policy_excluded_fields":"|".join(sorted(excluded)),"discriminator_key":str(gates.get('discriminator_key','')),"discriminator_source":"existing_policy" if gates.get('discriminator_key') else "","discriminator_value":gate,"coverage":f"{float(metrics.get('coverage',0.0)):.6f}","collision_rate":f"{float(metrics.get('collision_rate',1.0)):.6f}","fragmentation_rate":f"{float(metrics.get('fragmentation_rate',1.0)):.6f}","records_total":str(int(metrics.get('records_total',0) or 0)),"records_covered":str(int(metrics.get('records_covered',0) or 0)),"collision_records":str(int(metrics.get('collision_records',0) or 0)),"signature_group_count":str(int(metrics.get('join_group_count',0) or 0)) if target=="sig" else "","join_group_count":str(int(metrics.get('join_group_count',0) or 0)) if target=="join" else "","frontier_size":str(frontier),"fallback_used":"true" if fallback else "false","shape_gate":gate,"stratify_by":stratify_key,
                        "records_sampled_domain":str(len(dom_records)),
                        "pareto_subsets_evaluated":str(search_diagnostics.get("subsets_evaluated","")),
                        "pareto_k_levels_attempted":"|".join(str(x) for x in search_diagnostics.get("k_levels_attempted",[])),
                        "pareto_max_k_attempted":str(search_diagnostics.get("max_k_attempted","")),
                        "pareto_stop_reason":str(search_diagnostics.get("stop_reason","")),
                        "pareto_frontier_retained":str(search_diagnostics.get("frontier_retained","")),
                        "pareto_estimated_search_work":str(search_diagnostics.get("estimated_search_work","")),
                        "pareto_work_budget":str(search_diagnostics.get("work_budget",args.work_budget if sm=="pareto" else "")),
                        "pareto_work_budget_exhausted":str(search_diagnostics.get("work_budget_exhausted","")).lower(),
                        "pareto_elapsed_seconds":f"{float(search_diagnostics.get('elapsed_seconds',0.0)):.6f}" if search_diagnostics else "",
                        "coverage_full":f"{float(metrics_full.get('coverage',0.0)):.6f}" if metrics_full else "",
                        "collision_rate_full":f"{float(metrics_full.get('collision_rate',1.0)):.6f}" if metrics_full else "",
                        "fragmentation_rate_full":f"{float(metrics_full.get('fragmentation_rate',1.0)):.6f}" if metrics_full else "",
                        "records_total_full":str(int(metrics_full.get('records_total',0) or 0)) if metrics_full else "",
                        "records_covered_full":str(int(metrics_full.get('records_covered',0) or 0)) if metrics_full else "",
                        "collision_records_full":str(int(metrics_full.get('collision_records',0) or 0)) if metrics_full else "",
                        "signature_group_count_full":(str(int(metrics_full.get('join_group_count',0) or 0)) if metrics_full else "") if target=="sig" else "",
                        "join_group_count_full":(str(int(metrics_full.get('join_group_count',0) or 0)) if metrics_full else "") if target=="join" else "",
                        "full_verify_status":full_verify_status,
                        "sample_vs_full_diverges":"true" if diverges else "false",
                        # "policy_shape_gate_*" (the shape_gating/discriminator_key policy feature) is
                        # deliberately NOT prefixed "shape_gate_*" -- that name is already taken by the
                        # existing "shape_gate" column above (the loaded_family_types category-gate loop
                        # variable, e.g. "Doors"/"__all__"), an unrelated mechanism. Two different concepts
                        # sharing one name would be exactly the double-duty naming this project avoids
                        # elsewhere (e.g. --identity-basis -> --comparison-target).
                        "policy_shape_gate_enabled":"true" if shape_gate_summary_sample.get("enabled") else "false",
                        "policy_shape_gate_discriminator_key":str(shape_gate_summary_sample.get("discriminator_key","")),
                        "policy_shape_gate_summary_json":json.dumps(shape_gate_summary_sample,sort_keys=True,separators=(",",":")),
                        "policy_shape_gate_summary_full_json":json.dumps(shape_gate_summary_full,sort_keys=True,separators=(",",":")),
                        "policy_shape_gate_missing_required_sample":str(shape_gate_summary_sample.get("records_missing_required",0)),
                        "policy_shape_gate_missing_required_full":str(shape_gate_summary_full.get("records_missing_required",0))})
            candidates.setdefault(domain,{})[gate]=scoped
    return rows,candidates

def _flag_sig_join_convergence(all_rows):
    """Non-blocking flag (advisory only, same posture as the sample-vs-full
    divergence WARNING elsewhere in this tool): print, but do not fail, when a
    domain's join-target selected_fields end up IDENTICAL to its sig-target
    selected_fields for the same policy_mode/search_mode/shape_gate. Join and
    sig keys are typically expected to differ -- convergence usually means
    either sig_hash is collapsing onto join's coarser basis, or there simply
    aren't equivalent-but-varying elements in this domain's data to force a
    real distinction. Worth a second look either way; not treated as an error.
    """
    by_key={}
    for r in all_rows:
        k=(r.get("domain"),r.get("policy_mode"),r.get("search_mode"),r.get("shape_gate"))
        by_key.setdefault(k,{})[r.get("discovery_target")]=r.get("selected_fields","")
    for (domain,pm,sm,gate),by_target in sorted(by_key.items()):
        sig_sel=by_target.get("sig");join_sel=by_target.get("join")
        if sig_sel is None or join_sel is None:
            continue
        if not sig_sel and not join_sel:
            continue
        if sig_sel==join_sel:
            print(
                f"[discover] WARNING domain={domain} policy_mode={pm} search_mode={sm} shape_gate={gate} "
                f"sig and join selected_fields are IDENTICAL ({sig_sel}) -- join keys are typically "
                f"expected to differ from sig keys (coarser, or scoped differently); take a closer look "
                f"before assuming this is correct rather than an artifact of the search's candidate pool "
                f"or a lack of equivalent-but-varying elements to join against.",
                flush=True,
            )

def main():
    ap=argparse.ArgumentParser(
        description=(
            "Discovery-stage hash candidate analysis over flattened CSVs from phase0 output "
            "(records/items), not over original export JSON."
        )
    )
    ap.add_argument('--phase0-dir',default='results/records', help='Phase0 directory containing records.csv (also auto-resolves <root>/records, Results_v21/phase0_v21, and <root>/results/records).')
    ap.add_argument(
        '--policy-json',
        default=None,
        help=(
            "Optional governed policy JSON used as discovery constraints/baseline "
            "(required/optional/excluded/gates). Most relevant for validate/harsh modes."
        ),
    )
    ap.add_argument(
        '--base-policy',
        default=None,
        help='Fallback policy path if --policy-json is not provided; same schema/intent as --policy-json.',
    )
    ap.add_argument(
        '--out-policy',
        default=None,
        help=(
            "Optional output path for candidate-only policy JSON. "
            "This is advisory discovery output and not a governed contract."
        ),
    )
    ap.add_argument('--domains',default=None, help='Optional comma-separated domain allow-list.')
    ap.add_argument('--discovery-target',default='both',choices=['join','sig','both'], help='Which candidate family to explore: join, sig, or both.')
    ap.add_argument('--search-modes',default='greedy,pareto', help='Comma-separated search engines to run.')
    ap.add_argument(
        '--policy-modes',
        default='discover,validate,harsh',
        help=(
            "Comma-separated policy strictness modes: "
            "discover=free candidate pool, "
            "validate=required+optional only, "
            "harsh=required+optional plus discovered candidates."
        ),
    )
    ap.add_argument('--sample-size',type=int,default=5000, help='Per-domain sample cap (0 means no cap).')
    ap.add_argument('--sample-seed',type=int,default=17, help='Deterministic sampling seed.')
    ap.add_argument('--stratify-by',default='', help='Item key to stratify sampling by so each unique value gets equal representation regardless of group size (e.g. lft.family_name for loaded_family_types join discovery). Falls back to flat sampling when the key has no coverage.')
    ap.add_argument('--max-candidate-fields',type=int,default=64, help='Max discovered candidate fields per domain/gate.')
    ap.add_argument('--max-k',type=int,default=4, help='Max field subset size for greedy/Pareto evaluation.')
    ap.add_argument('--work-budget',type=int,default=0,help='Deterministic Pareto search-work ceiling (sampled records x candidate evaluations); 0 is unlimited.')
    ap.add_argument('--pareto-frontier-limit',type=int,default=10,help='Maximum diagnostic Pareto alternatives retained (default 10).')
    ap.add_argument('--no-pareto-progress',action='store_true',help='Suppress per-depth Pareto progress diagnostics.')
    ap.add_argument(
        "--no-full-verify",
        action="store_true",
        help=(
            "Skip re-scoring each selected candidate against the FULL (unsampled) domain "
            "population. By default, every selected candidate is re-scored against the full "
            "population (coverage_full/collision_rate_full/fragmentation_rate_full columns) "
            "so a sample-only 'collision=0'/'fragmentation=0' finding can't be pinned as policy "
            "without ever being checked against the real corpus -- this re-score is a single "
            "O(records) pass per row, not a combinatorial search, so it's cheap even when the "
            "search itself was sampled for tractability. Mirrors discover_join_policy.py's "
            "identical flag/behavior."
        ),
    )
    ap.add_argument(
        "--divergence-collision-delta",
        type=float,
        default=0.01,
        help="Absolute collision_rate_full - collision_rate threshold above which a [discover] WARNING is printed for that row (default 0.01).",
    )
    ap.add_argument(
        "--coverage-drop-threshold",
        type=float,
        default=0.05,
        help=(
            "Absolute coverage - coverage_full drop threshold above which a [discover] WARNING is "
            "printed (default 0.05). Catches a candidate that happened to cover every sampled record "
            "but is largely absent from the rest of the population -- collision_rate/fragmentation_rate "
            "alone won't catch this since both are only computed over covered records."
        ),
    )
    args=ap.parse_args();args.search_modes=[m.strip() for m in args.search_modes.split(',') if m.strip()];args.policy_modes=[m.strip() for m in args.policy_modes.split(',') if m.strip()]
    phase0=_resolve_phase0_dir(Path(args.phase0_dir))
    records_path = phase0 / "records.csv"
    if not records_path.exists():
        legacy_records_path = phase0 / "phase0_records.csv"
        if legacy_records_path.exists():
            records_path = legacy_records_path
        else:
            raise SystemExit(f"records.csv not found under phase0 dir: {phase0}")
    records=_read_csv(records_path)
    domains=sorted({r.get('domain','').strip() for r in records if r.get('domain','').strip()},key=str.lower)
    allow=set()
    if args.domains: allow={d.strip() for d in str(args.domains).split(',') if d.strip()};domains=[d for d in domains if d in allow]
    src=Path(args.policy_json) if args.policy_json else (Path(args.base_policy) if args.base_policy else None)
    base_domains={}
    if src and src.exists():
        loaded=json.loads(src.read_text(encoding='utf-8'));cand=loaded.get('domains') if isinstance(loaded,dict) else {}
        if isinstance(cand,dict): base_domains={str(k):v for k,v in cand.items() if isinstance(v,dict)}
    targets=['join','sig'] if args.discovery_target=='both' else [args.discovery_target]
    diagnostics=phase0.parent/'diagnostics'; diagnostics.mkdir(parents=True,exist_ok=True)
    domain_suffix=_diagnostics_domain_suffix(allow,args.policy_modes)
    all_rows=[];cand_out={}
    for t in targets:
        rows,cands=_run_target(t,args,records,domains,base_domains,phase0);all_rows.extend(rows);cand_out[t]=cands
        _write_csv(diagnostics/f'hash_{t}_discovery_exploration{domain_suffix}.csv',list(rows[0].keys()) if rows else ["domain","discovery_target"],rows)
    if 'sig' in targets and 'join' in targets:
        _flag_sig_join_convergence(all_rows)
    if args.out_policy:
        payload={"policy_version":"candidate","governance_status":"discovered_candidate_not_governed","domains":{}}
        for d in domains:
            payload['domains'][d]={"sig_hash_candidates":cand_out.get('sig',{}).get(d,{}),"join_hash_candidates":cand_out.get('join',{}).get(d,{}),"shape_gating":{"gate_key":CATEGORY_GATE_KEY if d=='loaded_family_types' else ""},"notes":["candidate discovery only; not governed contract"]}
        out_policy = Path(args.out_policy)
        out_policy.parent.mkdir(parents=True, exist_ok=True)
        out_policy.write_text(json.dumps(payload,indent=2,sort_keys=True),encoding='utf-8')

if __name__=='__main__': main()
