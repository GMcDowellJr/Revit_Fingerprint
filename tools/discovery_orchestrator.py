"""Cache-aware discovery sweep orchestration (discovery algorithms live elsewhere).

``DISCOVERY_ENGINE_VERSION`` is a cache semantic contract.  Bump it whenever a
change to discovery inputs, evaluation semantics, or orchestration acceptance
can change the evidence selected for a summary.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

DISCOVERY_ENGINE_VERSION = "discovery-sweep-v1"
CACHE_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_VERSION = 1
POLICY_MODES = ("discover", "validate", "harsh")
TARGETS = ("join", "sig")
SUMMARY_FIELDS = [
    "summary_timestamp", "domain", "target", "shape_gate", "decision_status",
    "decision_reason", "governance_status", "discover_selected_fields",
    "discover_search_mode_used", "discover_status", "validate_selected_fields",
    "validate_search_mode_used", "validate_status", "harsh_required", "harsh_reason",
    "harsh_selected_fields", "harsh_search_mode_used", "harsh_status", "coverage_full",
    "collision_rate_full", "fragmentation_rate_full", "sample_vs_full_diverges",
    "pareto_required", "pareto_reason", "result_provenance_status",
    "domain_result_timestamp", "run_id", "source_run_id", "source_evidence_path",
    "refresh_attempted", "refresh_status", "input_fingerprint",
    "discovery_engine_version",
]
RUN_FIELDS = ["summary_timestamp", "domain", "join_result_status", "sig_result_status",
              "fresh_or_cached_or_carried", "refresh_status", "pareto_invoked",
              "harsh_invoked", "warnings", "blocked_reason"]


def canonical_json(value: object) -> bytes:
    """Stable serialization; lists are semantic, mapping order is not."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_value(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def timestamp_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def acceptance_reasons(row: Mapping[str, object]) -> list[str]:
    reasons = []
    if str(row.get("status", "")) != "ok": reasons.append("status_not_ok")
    try:
        if float(row.get("coverage_full", 0)) != 1.0: reasons.append("coverage_incomplete")
    except (TypeError, ValueError): reasons.append("coverage_incomplete")
    try:
        if float(row.get("collision_rate_full", 1)) != 0.0: reasons.append("collision_present")
    except (TypeError, ValueError): reasons.append("collision_present")
    try:
        if float(row.get("fragmentation_rate_full", 1)) != 0.0: reasons.append("fragmentation_present")
    except (TypeError, ValueError): reasons.append("fragmentation_present")
    if str(row.get("sample_vs_full_diverges", "")).lower() != "false": reasons.append("sample_full_divergence")
    return reasons


def accepted(row: Mapping[str, object]) -> bool:
    return not acceptance_reasons(row)


def cache_key(domain: str, target: str, policy_mode: str, search_mode: str, shape_gate: str) -> str:
    return "|".join((domain, target, policy_mode, search_mode, shape_gate or "__all__"))


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, indent=2, sort_keys=True); f.write("\n"); f.flush(); os.fsync(f.fileno())
        os.replace(name, path)
    except BaseException:
        try: os.unlink(name)
        except FileNotFoundError: pass
        raise


def atomic_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader()
            for row in rows: w.writerow({k: row.get(k, "") for k in fields})
            f.flush(); os.fsync(f.fileno())
        os.replace(name, path)
    except BaseException:
        try: os.unlink(name)
        except FileNotFoundError: pass
        raise


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f: return list(csv.DictReader(f))


def _policy_block(path: Path, domain: str) -> tuple[object, str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("domains", {}).get(domain, {}), hashlib.sha256(path.read_bytes()).hexdigest()


def _domain_data(records_dir: Path, domain: str, target: str) -> dict:
    """Hash only relevant domain rows, sorting rows because CSV order is not semantic."""
    names = ["records.csv"]
    shard = records_dir / "identity_items_by_domain" / f"{domain}.csv"
    if shard.exists() and (shard.parent / ".complete").exists(): names.append(str(shard))
    else:
        # The sweep's JoinKey entry point is discover_join_policy.py, whose
        # candidate source is identity_items; SigHash prefers signature_items.
        for n in (("signature_items.csv", "identity_items.csv", "phase0_identity_items.csv") if target == "sig"
                  else ("identity_items.csv", "phase0_identity_items.csv")):
            if (records_dir / n).exists(): names.append(n); break
    payload = {}
    for name in names:
        p = Path(name) if Path(name).is_absolute() else records_dir / name
        rows = [r for r in read_csv(p) if r.get("domain") == domain]
        payload[p.name] = sorted(rows, key=lambda r: canonical_json(r))
    return payload


def input_fingerprint(records_dir: Path, policy_path: Path, domain: str, target: str,
                      policy_mode: str, search_mode: str, shape_gate: str, params: Mapping[str, object],
                      engine_version: str = DISCOVERY_ENGINE_VERSION) -> str:
    policy, _ = _policy_block(policy_path, domain)
    return sha256_value({"domain": domain, "target": target, "policy_mode": policy_mode,
        "search_mode": search_mode, "shape_gate": shape_gate or "__all__", "domain_data": _domain_data(records_dir, domain, target),
        "policy": policy, "parameters": dict(params), "full_verification": True,
        "discovery_engine_version": engine_version})


@dataclass
class Config:
    exports_root: Path
    repo_root: Path
    suggestions_csv: Path
    domains: list[str] | None = None
    skip_join: bool = False
    skip_sig: bool = False
    force: bool = False
    what_if: bool = False
    run: bool = False
    engine_version: str = DISCOVERY_ENGINE_VERSION


class Orchestrator:
    def __init__(self, cfg: Config, runner: Callable[..., subprocess.CompletedProcess] = subprocess.run):
        self.cfg, self.runner = cfg, runner
        self.records = cfg.exports_root / "records"
        self.root = cfg.exports_root / "diagnostics" / "discovery_results"
        self.summaries = self.root / "summaries"
        self.cache_path = self.root / "cache_manifest.json"
        self.policy_paths = {"join": cfg.repo_root / "policies/domain_join_key_policies.json",
                             "sig": cfg.repo_root / "policies/domain_sig_hash_policies.json"}

    def _load_cache(self) -> dict:
        if not self.cache_path.exists(): return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        value = json.loads(self.cache_path.read_text(encoding="utf-8"))
        if value.get("schema_version") != CACHE_SCHEMA_VERSION: return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        return value

    def _latest(self, target: str) -> tuple[Path | None, list[dict]]:
        prefix = "join_key" if target == "join" else "sig_hash"
        paths = sorted(self.summaries.glob(f"{prefix}_discovery_summary_*.csv"))
        return (paths[-1], read_csv(paths[-1])) if paths else (None, [])

    @staticmethod
    def _params(row: Mapping[str, str], mode: str) -> dict:
        max_k = row.get("suggested_max_k_discover", "4") if mode == "discover" else row.get("suggested_max_k_harsh_validate", "4")
        return {"sample_size": int(row.get("suggested_sample_size") or 5000), "sample_seed": 17,
                "stratify_by": row.get("stratify_by_recommended", ""),
                "max_candidate_fields": int(row.get("suggested_max_candidate_fields") or 64),
                "effective_max_k": int(max_k or 4)}

    def _command(self, target: str, domain: str, mode: str, search: str, params: Mapping[str, object]) -> list[str]:
        tool = "tools/discover_join_policy.py" if target == "join" else "tools/discover_hash_policy.py"
        cmd = [sys.executable, tool, "--phase0-dir", str(self.records), "--domains", domain,
               "--policy-json", str(self.policy_paths[target]), "--policy-modes", mode,
               "--search-modes", search, "--sample-size", str(params["sample_size"]),
               "--sample-seed", str(params["sample_seed"]), "--max-candidate-fields", str(params["max_candidate_fields"]),
               "--max-k", str(params["effective_max_k"])]
        if target == "sig": cmd += ["--discovery-target", "sig"]
        else: cmd += ["--warn-only"]
        if params["stratify_by"]: cmd += ["--stratify-by", str(params["stratify_by"])]
        return cmd

    def _artifact(self, target: str, domain: str, mode: str, search: str) -> Path:
        modes = mode
        suffix = f"__{domain}__{modes}"
        if target == "join": return self.cfg.exports_root / "diagnostics" / f"join_key_{mode}_{search}{suffix}.csv"
        return self.cfg.exports_root / "diagnostics" / f"hash_sig_discovery_exploration{suffix}.csv"

    def _invoke(self, cmd: list[str], log: Path) -> None:
        cp = self.runner(cmd, cwd=self.cfg.repo_root, text=True, capture_output=True)
        log.write_text((cp.stdout or "") + (cp.stderr or ""), encoding="utf-8")
        if cp.returncode: raise RuntimeError(f"exit {cp.returncode}: {' '.join(cmd)}")

    def _archive_artifacts(self, target: str, domain: str, mode: str, run_dir: Path) -> list[Path]:
        diagnostics = self.cfg.exports_root / "diagnostics"
        pattern = f"*__{domain}__{mode}.csv"
        archived=[]
        for src in sorted(diagnostics.glob(pattern)):
            # A target invocation can only own files with its established prefix.
            if target == "join" and not src.name.startswith("join_key_"): continue
            if target == "sig" and not src.name.startswith("hash_sig_"): continue
            dest=run_dir/target/src.name;dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.move(src,dest);archived.append(dest)
        return archived

    def _stage(self, target: str, domain: str, mode: str, params: dict, run_dir: Path,
               cache: dict, manifest: dict) -> tuple[list[dict], bool, str]:
        greedy_fp = input_fingerprint(self.records, self.policy_paths[target], domain, target, mode, "greedy", "__all__", params, self.cfg.engine_version)
        ck = cache_key(domain, target, mode, "greedy", "__all__")
        manifest["input_fingerprints"][ck] = greedy_fp
        hit = cache["entries"].get(ck)
        if hit and hit.get("input_fingerprint") == greedy_fp and hit.get("result_status") == "ok" and not self.cfg.force:
            rows = read_csv(Path(hit["result_path"]))
            manifest["stages_skipped"].append({"stage": f"{mode}/greedy", "reason": "cache_hit", "source_run_id": hit["result_run_id"]})
            return rows, True, greedy_fp
        cmd = self._command(target, domain, mode, "greedy", params)
        log = run_dir / "logs" / f"{target}_{mode}_greedy.log"; log.parent.mkdir(parents=True, exist_ok=True)
        self._invoke(cmd, log)
        src = self._artifact(target, domain, mode, "greedy")
        rows = [r for r in read_csv(src) if r.get("policy_mode") == mode and r.get("search_mode") == "greedy"]
        if not rows: raise RuntimeError(f"no {target}/{mode}/greedy rows produced")
        dest = run_dir / target / src.name
        archived=self._archive_artifacts(target,domain,mode,run_dir)
        manifest["commands_executed"].append(cmd); manifest["result_files"] += [str(p) for p in archived]; manifest["logs"].append(str(log))
        for r in rows:
            gate = r.get("shape_gate", "__all__")
            fp = input_fingerprint(self.records, self.policy_paths[target], domain, target, mode, "greedy", gate, params, self.cfg.engine_version)
            cache["entries"][cache_key(domain,target,mode,"greedy",gate)] = {"domain":domain,"target":target,"policy_mode":mode,"search_mode":"greedy","shape_gate":gate,"input_fingerprint":fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(dest),"result_status":r.get("status")}
        if all(r.get("status") == "ok" for r in rows):
            cache["entries"][ck] = {"domain":domain,"target":target,"policy_mode":mode,"search_mode":"greedy","shape_gate":"__all__","input_fingerprint":greedy_fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(dest),"result_status":"ok"}
        reasons = sorted({x for r in rows for x in acceptance_reasons(r)})
        if reasons:
            pareto_params = params
            cmd = self._command(target, domain, mode, "pareto", pareto_params)
            log = run_dir / "logs" / f"{target}_{mode}_pareto.log"; self._invoke(cmd, log)
            src = self._artifact(target, domain, mode, "pareto")
            prows = [r for r in read_csv(src) if r.get("policy_mode") == mode and r.get("search_mode") == "pareto"]
            archived=self._archive_artifacts(target,domain,mode,run_dir)
            manifest["commands_executed"].append(cmd); manifest["result_files"] += [str(p) for p in archived]; manifest["logs"].append(str(log))
            rows += prows
        else: manifest["stages_skipped"].append({"stage": f"{mode}/pareto", "reason": "greedy_acceptance_gate_satisfied"})
        return rows, False, greedy_fp

    @staticmethod
    def _choose(rows: Iterable[dict], mode: str) -> dict:
        candidates = [r for r in rows if r.get("policy_mode") == mode]
        good = [r for r in candidates if accepted(r)]
        return (good or candidates or [{}])[-1]

    def _summarize(self, target: str, domain: str, rows: list[dict], ts: str, run_id: str,
                   evidence: Path, fingerprint: str, provenance: str,
                   source_run_id: str | None = None, result_timestamp: str | None = None,
                   source_evidence: str | None = None) -> list[dict]:
        gates = sorted({r.get("shape_gate", "__all__") or "__all__" for r in rows}) or ["__all__"]
        out=[]
        for gate in gates:
            scoped=[r for r in rows if (r.get("shape_gate", "__all__") or "__all__")==gate]
            d,v,h=(self._choose(scoped,m) for m in POLICY_MODES)
            da,va,ha=accepted(d),accepted(v),accepted(h)
            if da and va: decision="supported" if d.get("selected_fields")==v.get("selected_fields") else "supported_alternative"
            elif da and not va and ha: decision="candidate_change"
            elif any(r.get("search_mode")=="pareto" for r in scoped): decision="ambiguous"
            elif scoped: decision="degraded"
            else: decision="blocked"
            final = v if va else h if ha else d
            preasons=sorted({x for r in scoped if r.get("search_mode")=="greedy" for x in acceptance_reasons(r)})
            out.append({"summary_timestamp":ts,"domain":domain,"target":target,"shape_gate":gate,
                "decision_status":decision,"decision_reason":";".join(acceptance_reasons(final)),"governance_status":"unratified",
                "discover_selected_fields":d.get("selected_fields",""),"discover_search_mode_used":d.get("search_mode",""),"discover_status":d.get("status","blocked"),
                "validate_selected_fields":v.get("selected_fields",""),"validate_search_mode_used":v.get("search_mode",""),"validate_status":v.get("status","blocked"),
                "harsh_required":"false" if va else "true","harsh_reason":"" if va else ";".join(acceptance_reasons(v)),
                "harsh_selected_fields":h.get("selected_fields",""),"harsh_search_mode_used":h.get("search_mode",""),"harsh_status":h.get("status", "skipped" if va else "blocked"),
                "coverage_full":final.get("coverage_full",""),"collision_rate_full":final.get("collision_rate_full",""),"fragmentation_rate_full":final.get("fragmentation_rate_full",""),"sample_vs_full_diverges":final.get("sample_vs_full_diverges",""),
                "pareto_required":"true" if preasons else "false","pareto_reason":";".join(preasons),"result_provenance_status":provenance,
                "domain_result_timestamp":result_timestamp or ts,"run_id":run_id,"source_run_id":source_run_id or run_id,"source_evidence_path":source_evidence or str(evidence),
                "refresh_attempted":"true","refresh_status":"completed","input_fingerprint":fingerprint,"discovery_engine_version":self.cfg.engine_version})
        return out

    def run_sweep(self) -> dict:
        suggestions = read_csv(self.cfg.suggestions_csv); by_domain={r["domain"]:r for r in suggestions}
        all_domains=sorted(by_domain); requested=sorted(self.cfg.domains or all_domains)
        missing=set(requested)-set(all_domains)
        if missing: raise ValueError("unknown domains: " + ",".join(sorted(missing)))
        targets=[t for t in TARGETS if not ((t=="join" and self.cfg.skip_join) or (t=="sig" and self.cfg.skip_sig))]
        prior={t:self._latest(t) for t in targets}
        if self.cfg.domains and any(not prior[t][1] for t in targets): raise RuntimeError("partial sweep requires an initial full summary")
        cache=self._load_cache()
        likely_hits=[]
        for target in targets:
            for domain in requested:
                for mode in ("discover","validate"):
                    params=self._params(by_domain[domain],mode)
                    fp=input_fingerprint(self.records,self.policy_paths[target],domain,target,mode,"greedy","__all__",params,self.cfg.engine_version)
                    entry=cache["entries"].get(cache_key(domain,target,mode,"greedy","__all__"),{})
                    if not self.cfg.force and entry.get("input_fingerprint")==fp and entry.get("result_status")=="ok":
                        likely_hits.append(f"{target}:{domain}:{mode}/greedy")
        ts=timestamp_now(); planned={"requested_domains":requested,"targets":targets,"summary_timestamp":ts,
            "initial_stages":[f"{t}:{d}:discover/greedy,validate/greedy" for t in targets for d in requested],
            "likely_cache_hits":likely_hits,
            "intended_results_root":str(self.root),
            "escalation":"Pareto and harsh cannot be known until Greedy/validate evidence is evaluated"}
        if self.cfg.what_if:
            print(json.dumps(planned,indent=2)); return planned
        if not self.cfg.run: raise RuntimeError("specify --run or --what-if")
        new_by_target={t:[] for t in targets}; run_rows=[]
        for target in targets:
            prior_rows=prior[target][1]; requested_set=set(requested)
            for old in prior_rows:
                if old["domain"] not in requested_set:
                    old=dict(old); old.update(summary_timestamp=ts,result_provenance_status="carried_forward",refresh_attempted="false",refresh_status="not_requested")
                    new_by_target[target].append(old)
            for domain in requested:
                rid=f"{ts}-{domain}-{uuid.uuid4().hex[:8]}"; final_dir=self.root/"domains"/domain/rid
                temp_dir=final_dir.with_name(final_dir.name+".tmp"); temp_dir.mkdir(parents=True)
                policy_block, policy_hash=_policy_block(self.policy_paths[target],domain)
                manifest={"schema_version":1,"run_id":rid,"timestamp":ts,"domain":domain,"requested_targets":[target],"commands_executed":[],"stages_skipped":[],"input_fingerprints":{},"policy_file_identifiers":{str(self.policy_paths[target]):policy_hash},"parameters":{},"discovery_engine_version":self.cfg.engine_version,"source_suggestions_row":by_domain[domain],"result_files":[],"logs":[],"warnings":[],"final_status":"running"}
                try:
                    rows=[]; cache_flags=[]; fps=[]
                    for mode in ("discover","validate"):
                        params=self._params(by_domain[domain],mode); manifest["parameters"][mode]=params
                        rs,cached,fp=self._stage(target,domain,mode,params,temp_dir,cache,manifest); rows+=rs;cache_flags.append(cached);fps.append(fp)
                    validate=self._choose(rows,"validate")
                    if not accepted(validate):
                        params=self._params(by_domain[domain],"harsh"); manifest["parameters"]["harsh"]=params
                        rs,cached,fp=self._stage(target,domain,"harsh",params,temp_dir,cache,manifest);rows+=rs;cache_flags.append(cached);fps.append(fp)
                    else: manifest["stages_skipped"].append({"stage":"harsh/*","reason":"validate_acceptance_gate_satisfied"})
                    provenance="cached" if cache_flags and all(cache_flags) else "fresh"
                    source_run_id=result_ts=source_path=None
                    if provenance == "cached":
                        skips=[s for s in manifest["stages_skipped"] if s.get("reason")=="cache_hit"]
                        source_run_id=skips[0].get("source_run_id") if skips else None
                        source_entry=next((e for e in cache["entries"].values() if e.get("result_run_id")==source_run_id),{})
                        result_ts=source_entry.get("result_timestamp");source_path=source_entry.get("result_path")
                    # Paths were recorded while evidence was in its temporary sibling.
                    manifest=json.loads(json.dumps(manifest).replace(str(temp_dir),str(final_dir)))
                    for entry in cache["entries"].values():
                        if entry.get("result_run_id")==rid:
                            entry["result_path"]=str(entry.get("result_path","")).replace(str(temp_dir),str(final_dir))
                    manifest["final_status"]="ok"; atomic_json(temp_dir/"run_manifest.json",manifest); os.replace(temp_dir,final_dir)
                    new_by_target[target]+=self._summarize(target,domain,rows,ts,rid,final_dir,sha256_value(sorted(fps)),provenance,source_run_id,result_ts,source_path)
                except Exception as exc:
                    manifest["final_status"]="blocked";manifest["warnings"].append(str(exc));atomic_json(temp_dir/"run_manifest.json",manifest);os.replace(temp_dir,final_dir)
                    old=[dict(r) for r in prior_rows if r["domain"]==domain]
                    if old:
                        for r in old:r.update(summary_timestamp=ts,result_provenance_status="blocked_refresh_previous_retained",refresh_attempted="true",refresh_status="blocked")
                        new_by_target[target]+=old
                    else:
                        new_by_target[target].append({"summary_timestamp":ts,"domain":domain,"target":target,"shape_gate":"__all__","decision_status":"blocked","decision_reason":str(exc),"governance_status":"unratified","result_provenance_status":"fresh","domain_result_timestamp":ts,"run_id":rid,"source_run_id":rid,"source_evidence_path":str(final_dir),"refresh_attempted":"true","refresh_status":"blocked","discovery_engine_version":self.cfg.engine_version})
        # Validate everything before publishing anything.
        for target, rows in new_by_target.items():
            keys=[(r.get("domain"),r.get("target"),r.get("shape_gate") or "__all__") for r in rows]
            if len(keys)!=len(set(keys)): raise RuntimeError(f"duplicate summary keys for {target}")
        self.summaries.mkdir(parents=True,exist_ok=True)
        for target,rows in new_by_target.items():
            prefix="join_key" if target=="join" else "sig_hash"; atomic_csv(self.summaries/f"{prefix}_discovery_summary_{ts}.csv",sorted(rows,key=lambda r:(r.get('domain',''),r.get('shape_gate',''))),SUMMARY_FIELDS)
        domains=sorted({r["domain"] for rows in new_by_target.values() for r in rows})
        for d in domains:
            jr=next((r for r in new_by_target.get("join",[]) if r["domain"]==d),{});sr=next((r for r in new_by_target.get("sig",[]) if r["domain"]==d),{})
            vals=[r for r in (jr,sr) if r]; run_rows.append({"summary_timestamp":ts,"domain":d,"join_result_status":jr.get("decision_status","skipped"),"sig_result_status":sr.get("decision_status","skipped"),"fresh_or_cached_or_carried":";".join(sorted({r.get("result_provenance_status","") for r in vals})),"refresh_status":";".join(sorted({r.get("refresh_status","") for r in vals})),"pareto_invoked":"true" if any(r.get("pareto_required")=="true" for r in vals) else "false","harsh_invoked":"true" if any(r.get("harsh_required")=="true" for r in vals) else "false","warnings":"","blocked_reason":";".join(r.get("decision_reason","") for r in vals if r.get("decision_status")=="blocked")})
        atomic_csv(self.summaries/f"discovery_run_summary_{ts}.csv",run_rows,RUN_FIELDS);atomic_json(self.cache_path,cache)
        counts=Counter(r.get("decision_status") for rows in new_by_target.values() for r in rows);counts.update(r.get("result_provenance_status") for rows in new_by_target.values() for r in rows)
        print(json.dumps({"summary_timestamp":ts,"counts":dict(sorted(counts.items()))},indent=2));return {"timestamp":ts,"rows":new_by_target}


def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description="Run staged, cache-aware JoinKey/SigHash discovery")
    p.add_argument("--exports-root",default="Fingerprint_Data");p.add_argument("--repo-root",default=str(Path(__file__).resolve().parents[1]));p.add_argument("--suggestions-csv")
    p.add_argument("--domains");p.add_argument("--skip-join",action="store_true");p.add_argument("--skip-sig",action="store_true");p.add_argument("--force",action="store_true");p.add_argument("--what-if",action="store_true");p.add_argument("--run",action="store_true")
    return p


def config_from_args(ns: argparse.Namespace) -> Config:
    exports=Path(ns.exports_root).resolve(); return Config(exports,Path(ns.repo_root).resolve(),Path(ns.suggestions_csv).resolve() if ns.suggestions_csv else exports/"diagnostics/discovery_param_suggestions.csv",[x.strip() for x in ns.domains.split(",") if x.strip()] if ns.domains else None,ns.skip_join,ns.skip_sig,ns.force,ns.what_if,ns.run)


def main(argv: list[str] | None=None) -> int:
    try: Orchestrator(config_from_args(build_parser().parse_args(argv))).run_sweep();return 0
    except Exception as exc: print(f"ERROR: {exc}",file=sys.stderr);return 1
