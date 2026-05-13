#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json
from pathlib import Path
from typing import Dict,List
try:
    from tools.discover_join_policy import _read_csv,_write_csv,_sample_domain_records,_pick_candidate_fields,_without_excluded,_pareto_search_adapter
    from tools.join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate
    from tools.join_key_discovery.greedy import discover_greedy
except ModuleNotFoundError:
    from discover_join_policy import _read_csv,_write_csv,_sample_domain_records,_pick_candidate_fields,_without_excluded,_pareto_search_adapter
    from join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate
    from join_key_discovery.greedy import discover_greedy

TARGET_FILES={"sig":["signature_items.csv","identity_items.csv","phase0_identity_items.csv"],"join":["join_items.csv","identity_items.csv","phase0_identity_items.csv"]}
CATEGORY_GATE_KEY="shape_gate.category"


def _resolve_phase0_dir(path: Path) -> Path:
    """
    Accept either:
      - direct phase0 folder (contains records.csv), or
      - Results_v21 root (contains phase0_v21/records.csv).
      - pipeline default records root (results/records/records.csv).
    """
    if (path / "records.csv").exists():
        return path
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
    return out

def _run_target(target,args,records,domains,base_domains):
    rows=[]; candidates={}
    items=_load_items(Path(args.phase0_dir),target)
    for domain in domains:
        normalized=normalize_policy_block(base_domains.get(domain,{}))
        req=normalized['required_fields']; opt=normalized['optional_items']; excluded=set(normalized['explicitly_excluded_items']); gates=normalized['gates']
        for gate,dom_records_all,dom_items_all in _domain_rows(records,items,domain,target):
            dom_records=_sample_domain_records(dom_records_all,args.sample_size,args.sample_seed)
            sampled={r.get('record_pk','').strip() for r in dom_records}
            dom_items=[it for it in dom_items_all if not sampled or it.get('record_pk','').strip() in sampled]
            raw=_pick_candidate_fields(dom_items,args.max_candidate_fields)
            scoped=_without_excluded(raw,excluded)
            if domain=="loaded_family_types" and CATEGORY_GATE_KEY in raw:
                scoped=[CATEGORY_GATE_KEY]+[f for f in scoped if f!=CATEGORY_GATE_KEY]
            idx=build_identity_index(dom_items)
            for pm in args.policy_modes:
                work=scoped if pm=="discover" else _without_excluded(req+opt if pm=="validate" else req+opt+scoped,excluded)
                max_k = args.max_k
                if pm == "validate" and req:
                    max_k = max(max_k, len(req))
                for sm in args.search_modes:
                    selected=[];metrics={};status="ok";reason="";frontier=0;fallback=False
                    if not work: status="no_candidates"
                    elif sm=="pareto":
                        p=_pareto_search_adapter(dom_records,idx,work,{"max_k":max_k,"gates":{"required_fields":req,**gates}})
                        fr=p.get('frontier') if isinstance(p.get('frontier'),list) else [];frontier=len(fr)
                        if pm == "validate" and req:
                            fr = [row for row in fr if set(req).issubset(set(str(row.get("keys", "")).split("|")))]
                            frontier = len(fr)
                        if fr:
                            ch=sorted(fr,key=lambda x:(x.get('collision_rate',1.0),x.get('coverage_gap',1.0),x.get('k_count',99),x.get('keys','')))[0]
                            selected=[x for x in str(ch.get('keys','')).split('|') if x];metrics=ch.get('metrics',{}) if isinstance(ch.get('metrics'),dict) else {}
                        elif pm == "validate" and req:
                            selected = list(req)
                            metrics = score_candidate(dom_records,idx,selected,{"gates":{"required_fields":req,**gates}})
                            fallback = True
                            reason = "required_set_fallback"
                        else: status="blocked";reason="no_frontier"
                    else:
                        g=discover_greedy(dom_records,idx,work,{"max_k":max_k,"gates":{"required_fields":req,**gates}})
                        selected=[str(x) for x in g.get('selected_fields',[]) if str(x).strip()];metrics=g.get('metrics',{}) if isinstance(g.get('metrics'),dict) else {}
                    if pm=="validate" and req and not set(req).issubset(set(selected)):
                        status="blocked_missing_required"
                        if not reason:
                            reason="selected_missing_required"
                    if not metrics and work: metrics=score_candidate(dom_records,idx,selected,{"gates":{"required_fields":req,**gates}})
                    rows.append({"domain":domain,"discovery_target":target,"policy_mode":pm,"search_mode":sm,"status":status,"reason":reason,"selected_fields":"|".join(selected),"coverage":f"{float(metrics.get('coverage',0.0)):.6f}","collision_rate":f"{float(metrics.get('collision_rate',1.0)):.6f}","fragmentation_rate":f"{float(metrics.get('fragmentation_rate',1.0)):.6f}","records_total":str(int(metrics.get('records_total',0) or 0)),"records_covered":str(int(metrics.get('records_covered',0) or 0)),"collision_records":str(int(metrics.get('collision_records',0) or 0)),"signature_group_count":str(int(metrics.get('join_group_count',0) or 0)) if target=="sig" else "","join_group_count":str(int(metrics.get('join_group_count',0) or 0)) if target=="join" else "","frontier_size":str(frontier),"fallback_used":"true" if fallback else "false","shape_gate":gate})
            candidates.setdefault(domain,{})[gate]=scoped
    return rows,candidates

def main():
    ap=argparse.ArgumentParser(
        description=(
            "Discovery-stage hash candidate analysis over flattened CSVs from phase0 output "
            "(records/items), not over original export JSON."
        )
    )
    ap.add_argument('--phase0-dir',default='results/records', help='Phase0 directory containing records.csv (also auto-resolves Results_v21/phase0_v21 and results/records).')
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
    ap.add_argument('--max-candidate-fields',type=int,default=64, help='Max discovered candidate fields per domain/gate.')
    ap.add_argument('--max-k',type=int,default=4, help='Max field subset size for greedy/Pareto evaluation.')
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
    if args.domains: allow={d.strip() for d in str(args.domains).split(',') if d.strip()};domains=[d for d in domains if d in allow]
    src=Path(args.policy_json) if args.policy_json else (Path(args.base_policy) if args.base_policy else None)
    base_domains={}
    if src and src.exists():
        loaded=json.loads(src.read_text(encoding='utf-8'));cand=loaded.get('domains') if isinstance(loaded,dict) else {}
        if isinstance(cand,dict): base_domains={str(k):v for k,v in cand.items() if isinstance(v,dict)}
    targets=['join','sig'] if args.discovery_target=='both' else [args.discovery_target]
    diagnostics=phase0.parent/'diagnostics'; diagnostics.mkdir(parents=True,exist_ok=True)
    all_rows=[];cand_out={}
    for t in targets:
        rows,cands=_run_target(t,args,records,domains,base_domains);all_rows.extend(rows);cand_out[t]=cands
        _write_csv(diagnostics/f'hash_{t}_discovery_exploration.csv',list(rows[0].keys()) if rows else ["domain","discovery_target"],rows)
    if args.out_policy:
        payload={"policy_version":"candidate","governance_status":"discovered_candidate_not_governed","domains":{}}
        for d in domains:
            payload['domains'][d]={"sig_hash_candidates":cand_out.get('sig',{}).get(d,{}),"join_hash_candidates":cand_out.get('join',{}).get(d,{}),"shape_gating":{"gate_key":CATEGORY_GATE_KEY if d=='loaded_family_types' else ""},"notes":["candidate discovery only; not governed contract"]}
        Path(args.out_policy).write_text(json.dumps(payload,indent=2,sort_keys=True),encoding='utf-8')

if __name__=='__main__': main()
