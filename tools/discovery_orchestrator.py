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
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty, Queue
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

# Keep a new version whenever candidate or orchestration semantics can change
# selected evidence, so older cache entries are never silently reused.
DISCOVERY_ENGINE_VERSION = "discovery-sweep-v4"
PROGRESS_HEARTBEAT_SECONDS = 30.0
CACHE_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_VERSION = 1
POLICY_MODES = ("discover", "validate", "harsh")
TARGETS = ("join", "sig")
DEFAULT_WORKERS = 4
# ThreadPoolExecutor raises ValueError when max_workers > 61 on Windows
# (WaitForMultipleObjects handle-count limit) -- mirrors
# compare_cross_segment.py::resolve_worker_count()'s _WIN32_MAX_WORKERS cap;
# not imported from there since that module is protected and this is a
# small, stable, single-purpose utility not worth a shared-module extraction.
_WIN32_MAX_WORKERS = 61


def resolve_worker_count(value: str, headroom: int = 2) -> int:
    """Resolve --workers, accepting either an int or the literal string 'auto'.

    'auto' derives a single-layer worker count from available logical cores
    minus headroom -- the sweep's ThreadPoolExecutor is not nested inside
    another worker pool, so there is no second layer to coordinate against.
    """
    if str(value).strip().lower() == "auto":
        cpu_count = os.cpu_count()
        workers = max(1, cpu_count - headroom) if cpu_count else DEFAULT_WORKERS
        if sys.platform == "win32":
            workers = min(workers, _WIN32_MAX_WORKERS)
        return workers
    return int(value)
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
    "refresh_error", "stage_provenance_json", "source_run_ids",
    "source_evidence_paths", "discovery_engine_version",
]
RUN_FIELDS = ["summary_timestamp", "domain", "join_result_status", "sig_result_status",
              "fresh_or_cached_or_carried", "refresh_status", "pareto_invoked",
              "harsh_invoked", "warnings", "blocked_reason"]
DECISION_SEVERITY = {
    "supported": 0, "supported_alternative": 1, "candidate_change": 2,
    "ambiguous": 3, "degraded": 4, "blocked": 5,
}


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


def stage_cache_eligible(rows: Iterable[Mapping[str, object]]) -> bool:
    materialized = list(rows)
    return bool(materialized) and all(accepted(row) for row in materialized)


def all_shape_gates_accepted(rows: Iterable[Mapping[str, object]], policy_mode: str) -> bool:
    """True only when every represented gate has an accepted result for a mode."""
    materialized = list(rows)
    expected_gates = {str(row.get("shape_gate") or "__all__") for row in materialized}
    by_gate: dict[str, list[Mapping[str, object]]] = {}
    for row in materialized:
        if row.get("policy_mode") == policy_mode:
            by_gate.setdefault(str(row.get("shape_gate") or "__all__"), []).append(row)
    return bool(expected_gates) and expected_gates == set(by_gate) and all(
        any(accepted(row) for row in gate_rows) for gate_rows in by_gate.values()
    )


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


def iter_csv(path: Path) -> Iterable[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def read_csv(path: Path) -> list[dict[str, str]]:
    return list(iter_csv(path))


def sweep_evidence_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    """Exclude auxiliary diagnostics from cache, escalation, and summaries.

    Empty scope is retained for artifacts written before result_scope existed.
    """
    return [row for row in rows if row.get("result_scope", "") != "partition_diagnostic"]


def _policy_block(path: Path, domain: str) -> tuple[object, str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("domains", {}).get(domain, {}), hashlib.sha256(path.read_bytes()).hexdigest()


def _eligibility_rules(path: Path, domain: str) -> object:
    """Return the global and requested-domain rules that affect a sweep."""
    if not path.exists():
        return {"registry_status": "missing"}
    data = json.loads(path.read_text(encoding="utf-8"))
    domains = data.get("domains", {}) if isinstance(data.get("domains"), dict) else {}
    return {"schema_version": data.get("schema_version"), "global": data.get("global", {}),
            "domain": domains.get(domain, {})}


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
                      engine_version: str = DISCOVERY_ENGINE_VERSION,
                      domain_data: object | None = None) -> str:
    policy, _ = _policy_block(policy_path, domain)
    eligibility = _eligibility_rules(policy_path.parent / "discovery_candidate_eligibility.json", domain)
    return sha256_value({"domain": domain, "target": target, "policy_mode": policy_mode,
        "search_mode": search_mode, "shape_gate": shape_gate or "__all__",
        "domain_data": _domain_data(records_dir, domain, target) if domain_data is None else domain_data,
        "policy": policy, "candidate_eligibility": eligibility,
        "parameters": dict(params), "full_verification": True,
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
    workers: int = DEFAULT_WORKERS


class Orchestrator:
    def __init__(self, cfg: Config, runner: Callable[..., subprocess.CompletedProcess] = subprocess.run):
        self.cfg, self.runner = cfg, runner
        self.records = cfg.exports_root / "records"
        self.root = cfg.exports_root / "diagnostics" / "discovery_results"
        self.summaries = self.root / "summaries"
        self.cache_path = self.root / "cache_manifest.json"
        self.policy_paths = {"join": cfg.repo_root / "policies/domain_join_key_policies.json",
                             "sig": cfg.repo_root / "policies/domain_sig_hash_policies.json"}
        self.eligibility_path = cfg.repo_root / "policies/discovery_candidate_eligibility.json"
        self._file_domain_hashes: dict[Path, dict[str, str]] = {}
        # Concurrent (target, domain) units can share a source CSV (records.csv
        # is common to every domain) or each own a distinct one (sharded
        # identity_items_by_domain/<domain>.csv). A single global lock would
        # needlessly serialize the common case that parallelization is meant to
        # speed up, so lock per resolved path instead; this tiny guard lock only
        # protects creation of those per-path locks, never the scan itself.
        self._file_domain_hashes_locks: dict[Path, threading.Lock] = {}
        self._file_domain_hashes_locks_guard = threading.Lock()
        # SIGINT only interrupts the main thread; a worker blocked in _invoke's
        # subprocess wait never sees it directly. This event is how the main
        # thread (run_sweep's as_completed loop) tells still-running workers to
        # terminate their child and unwind, instead of running to completion.
        self._interrupt_event = threading.Event()

    def _hashes_by_domain(self, path: Path) -> dict[str, str]:
        """Scan a large CSV once and retain compact, order-independent domain hashes."""
        path = path.resolve()
        with self._file_domain_hashes_locks_guard:
            lock = self._file_domain_hashes_locks.setdefault(path, threading.Lock())
        with lock:
            if path not in self._file_domain_hashes:
                started = time.monotonic()
                print(f"[sweep] fingerprint scan START {path}", flush=True)
                row_hashes: dict[str, list[str]] = {}
                for row in iter_csv(path):
                    domain = row.get("domain", "")
                    row_hashes.setdefault(domain, []).append(hashlib.sha256(canonical_json(row)).hexdigest())
                self._file_domain_hashes[path] = {
                    domain: sha256_value(sorted(hashes)) for domain, hashes in row_hashes.items()
                }
                print(f"[sweep] fingerprint scan DONE ({int(time.monotonic() - started)}s) {path}", flush=True)
            return self._file_domain_hashes[path]

    def _fingerprint_domain_data(self, domain: str, target: str) -> dict[str, str]:
        paths = [self.records / "records.csv"]
        shard = self.records / "identity_items_by_domain" / f"{domain}.csv"
        if shard.exists() and (shard.parent / ".complete").exists():
            paths.append(shard)
        else:
            candidates = (("signature_items.csv", "identity_items.csv", "phase0_identity_items.csv")
                          if target == "sig" else ("identity_items.csv", "phase0_identity_items.csv"))
            paths.extend(self.records / name for name in candidates if (self.records / name).exists())
            paths = paths[:2]
        return {path.name: self._hashes_by_domain(path).get(domain, sha256_value([])) for path in paths}

    def _input_fingerprint(self, domain: str, target: str, policy_mode: str,
                           search_mode: str, shape_gate: str, params: Mapping[str, object]) -> str:
        return input_fingerprint(
            self.records, self.policy_paths[target], domain, target, policy_mode,
            search_mode, shape_gate, params, self.cfg.engine_version,
            domain_data=self._fingerprint_domain_data(domain, target),
        )

    def _load_cache(self) -> dict:
        if not self.cache_path.exists(): return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        value = json.loads(self.cache_path.read_text(encoding="utf-8"))
        if value.get("schema_version") != CACHE_SCHEMA_VERSION: return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        return value

    @staticmethod
    def _discard_run_cache_entries(cache: dict, run_id: str) -> None:
        cache["entries"] = {
            key: entry for key, entry in cache["entries"].items()
            if entry.get("result_run_id") != run_id
        }

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
        log.parent.mkdir(parents=True, exist_ok=True)
        print(f"[sweep] START {' '.join(cmd)}", flush=True)
        if self.runner is not subprocess.run:
            cp = self.runner(cmd, cwd=self.cfg.repo_root, text=True, capture_output=True)
            output = (cp.stdout or "") + (cp.stderr or "")
            log.write_text(output, encoding="utf-8")
            if output:
                print(output, end="" if output.endswith("\n") else "\n", flush=True)
            if cp.returncode: raise RuntimeError(f"exit {cp.returncode}: {' '.join(cmd)}")
            print(f"[sweep] DONE {' '.join(cmd)}", flush=True)
            return

        started = time.monotonic()
        process = subprocess.Popen(
            cmd, cwd=self.cfg.repo_root, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, bufsize=1,
        )
        output: Queue[str | None] = Queue()

        def read_output() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                output.put(line)
            output.put(None)

        reader = threading.Thread(target=read_output, name="discovery-output", daemon=True)
        reader.start()
        try:
            with log.open("w", encoding="utf-8", buffering=1) as handle:
                stream_closed = False
                while not stream_closed:
                    try:
                        line = output.get(timeout=PROGRESS_HEARTBEAT_SECONDS)
                    except Empty:
                        if self._interrupt_event.is_set():
                            raise KeyboardInterrupt("discovery sweep interrupted")
                        elapsed = int(time.monotonic() - started)
                        print(f"[sweep] still running ({elapsed}s); live log: {log}", flush=True)
                        continue
                    if line is None:
                        stream_closed = True
                    else:
                        handle.write(line)
                        print(line, end="", flush=True)
            returncode = process.wait()
        except BaseException:
            process.terminate()
            process.wait()
            raise
        finally:
            reader.join(timeout=1)
        elapsed = int(time.monotonic() - started)
        if returncode:
            raise RuntimeError(f"exit {returncode}: {' '.join(cmd)}")
        print(f"[sweep] DONE ({elapsed}s) {' '.join(cmd)}", flush=True)

    def _archive_artifacts(self, target: str, domain: str, mode: str, search: str, run_dir: Path) -> list[Path]:
        diagnostics = self.cfg.exports_root / "diagnostics"
        pattern = f"*__{domain}__{mode}.csv"
        archived=[]
        for src in sorted(diagnostics.glob(pattern)):
            # A target invocation can only own files with its established prefix.
            if target == "join" and not src.name.startswith("join_key_"): continue
            if target == "sig" and not src.name.startswith("hash_sig_"): continue
            # Sig Greedy and Pareto share the same generated basename. Preserve
            # both causes/effects of escalation under distinct immutable names.
            name = f"{src.stem}__{search}{src.suffix}" if target == "sig" else src.name
            dest=run_dir/target/name;dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.move(src,dest);archived.append(dest)
        return archived

    def _stage(self, target: str, domain: str, mode: str, params: dict, run_dir: Path,
               cache: dict, manifest: dict) -> tuple[list[dict], list[dict], str]:
        greedy_fp = self._input_fingerprint(domain, target, mode, "greedy", "__all__", params)
        ck = cache_key(domain, target, mode, "greedy", "__all__")
        manifest["input_fingerprints"][ck] = greedy_fp
        hit = cache["entries"].get(ck)
        if hit and hit.get("input_fingerprint") == greedy_fp and hit.get("result_status") == "ok" and not self.cfg.force:
            rows = [r for r in sweep_evidence_rows(read_csv(Path(hit["result_path"]))) if r.get("policy_mode") == mode and r.get("search_mode") == "greedy"]
            # Defend against manifests written by older orchestration versions:
            # status=ok alone never makes incomplete/colliding/divergent evidence reusable.
            if stage_cache_eligible(rows):
                manifest["stages_skipped"].append({"stage": f"{mode}/greedy", "reason": "cache_hit", "source_run_id": hit["result_run_id"]})
                return rows, [{"policy_mode":mode,"search_mode":"greedy","provenance":"cached",
                    "source_run_id":hit["result_run_id"],"source_evidence_path":hit["result_path"],
                    "result_timestamp":hit.get("result_timestamp","")}], greedy_fp
        cmd = self._command(target, domain, mode, "greedy", params)
        log = run_dir / "logs" / f"{target}_{mode}_greedy.log"; log.parent.mkdir(parents=True, exist_ok=True)
        self._invoke(cmd, log)
        src = self._artifact(target, domain, mode, "greedy")
        rows = [r for r in sweep_evidence_rows(read_csv(src)) if r.get("policy_mode") == mode and r.get("search_mode") == "greedy"]
        if not rows: raise RuntimeError(f"no {target}/{mode}/greedy rows produced")
        archived=self._archive_artifacts(target,domain,mode,"greedy",run_dir)
        dest = next((p for p in archived if p.name.startswith(src.stem)), run_dir / target / src.name)
        manifest["commands_executed"].append(cmd); manifest["result_files"] += [str(p) for p in archived]; manifest["logs"].append(str(log))
        sources=[{"policy_mode":mode,"search_mode":"greedy","provenance":"fresh",
                  "source_run_id":manifest["run_id"],"source_evidence_path":str(dest)}]
        for r in rows:
            gate = r.get("shape_gate", "__all__")
            fp = self._input_fingerprint(domain, target, mode, "greedy", gate, params)
            if accepted(r):
                cache["entries"][cache_key(domain,target,mode,"greedy",gate)] = {"domain":domain,"target":target,"policy_mode":mode,"search_mode":"greedy","shape_gate":gate,"input_fingerprint":fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(dest),"result_status":"ok"}
        if stage_cache_eligible(rows):
            cache["entries"][ck] = {"domain":domain,"target":target,"policy_mode":mode,"search_mode":"greedy","shape_gate":"__all__","input_fingerprint":greedy_fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(dest),"result_status":"ok"}
        reasons = sorted({x for r in rows for x in acceptance_reasons(r)})
        if reasons:
            pareto_params = params
            pareto_fp=self._input_fingerprint(domain,target,mode,"pareto","__all__",pareto_params)
            pck=cache_key(domain,target,mode,"pareto","__all__")
            manifest["input_fingerprints"][pck]=pareto_fp
            phit=cache["entries"].get(pck)
            prows=[]
            if phit and phit.get("input_fingerprint")==pareto_fp and phit.get("result_status")=="ok" and not self.cfg.force:
                prows=[r for r in sweep_evidence_rows(read_csv(Path(phit["result_path"]))) if r.get("policy_mode")==mode and r.get("search_mode")=="pareto"]
                if stage_cache_eligible(prows):
                    manifest["stages_skipped"].append({"stage":f"{mode}/pareto","reason":"cache_hit","source_run_id":phit["result_run_id"]})
                    sources.append({"policy_mode":mode,"search_mode":"pareto","provenance":"cached",
                        "source_run_id":phit["result_run_id"],"source_evidence_path":phit["result_path"],
                        "result_timestamp":phit.get("result_timestamp","")})
                else: prows=[]
            if not prows:
                cmd = self._command(target, domain, mode, "pareto", pareto_params)
                log = run_dir / "logs" / f"{target}_{mode}_pareto.log"; self._invoke(cmd, log)
                src = self._artifact(target, domain, mode, "pareto")
                prows = [r for r in sweep_evidence_rows(read_csv(src)) if r.get("policy_mode") == mode and r.get("search_mode") == "pareto"]
                archived=self._archive_artifacts(target,domain,mode,"pareto",run_dir)
                pdest=next((p for p in archived if p.name.startswith(src.stem)),run_dir/target/src.name)
                manifest["commands_executed"].append(cmd); manifest["result_files"] += [str(p) for p in archived]; manifest["logs"].append(str(log))
                sources.append({"policy_mode":mode,"search_mode":"pareto","provenance":"fresh",
                    "source_run_id":manifest["run_id"],"source_evidence_path":str(pdest)})
                for r in prows:
                    gate=r.get("shape_gate","__all__")
                    fp=self._input_fingerprint(domain,target,mode,"pareto",gate,pareto_params)
                    if accepted(r):
                        cache["entries"][cache_key(domain,target,mode,"pareto",gate)]={"domain":domain,"target":target,"policy_mode":mode,"search_mode":"pareto","shape_gate":gate,"input_fingerprint":fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(pdest),"result_status":"ok"}
                if stage_cache_eligible(prows):
                    cache["entries"][pck]={"domain":domain,"target":target,"policy_mode":mode,"search_mode":"pareto","shape_gate":"__all__","input_fingerprint":pareto_fp,"result_run_id":manifest["run_id"],"result_timestamp":manifest["timestamp"],"result_path":str(pdest),"result_status":"ok"}
            rows += prows
        else: manifest["stages_skipped"].append({"stage": f"{mode}/pareto", "reason": "greedy_acceptance_gate_satisfied"})
        return rows, sources, greedy_fp

    @staticmethod
    def _choose(rows: Iterable[dict], mode: str) -> dict:
        candidates = [r for r in rows if r.get("policy_mode") == mode]
        good = [r for r in candidates if accepted(r)]
        return (good or candidates or [{}])[-1]

    def _summarize(self, target: str, domain: str, rows: list[dict], ts: str, run_id: str,
                   evidence: Path, fingerprint: str, stage_provenance: list[dict]) -> list[dict]:
        provenance_values={s["provenance"] for s in stage_provenance}
        provenance="mixed" if len(provenance_values)>1 else next(iter(provenance_values),"fresh")
        source_run_ids=sorted({s["source_run_id"] for s in stage_provenance})
        source_paths=sorted({s["source_evidence_path"] for s in stage_provenance})
        cached_timestamps=[s.get("result_timestamp") for s in stage_provenance if s.get("provenance")=="cached" and s.get("result_timestamp")]
        result_timestamp=min(cached_timestamps) if provenance=="cached" and cached_timestamps else ts
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
                "domain_result_timestamp":result_timestamp,"run_id":run_id,"source_run_id":";".join(source_run_ids),"source_evidence_path":";".join(source_paths),
                "refresh_attempted":"true","refresh_status":"completed","input_fingerprint":fingerprint,
                "stage_provenance_json":json.dumps(stage_provenance,sort_keys=True,separators=(",",":")),
                "source_run_ids":";".join(source_run_ids),"source_evidence_paths":";".join(source_paths),
                "discovery_engine_version":self.cfg.engine_version})
        return out

    @staticmethod
    def _aggregate_domain(rows: list[dict]) -> dict:
        """Collapse shape gates conservatively for the run-level review row."""
        if not rows:
            return {"decision_status": "skipped", "provenance": set(),
                    "refresh": set(), "pareto_invoked": False,
                    "harsh_invoked": False, "blocked_reasons": [], "warnings": []}
        worst = max(rows, key=lambda r: DECISION_SEVERITY.get(r.get("decision_status", "blocked"), 5))
        return {
            "decision_status": worst.get("decision_status", "blocked"),
            "provenance": {r.get("result_provenance_status", "") for r in rows},
            "refresh": {r.get("refresh_status", "") for r in rows},
            "pareto_invoked": any(r.get("pareto_required") == "true" for r in rows),
            "harsh_invoked": any(r.get("harsh_required") == "true" for r in rows),
            "blocked_reasons": [r.get("decision_reason", "") for r in rows if r.get("decision_status") == "blocked" and r.get("decision_reason")],
            "warnings": [r.get("refresh_error", "") for r in rows if r.get("refresh_error")],
        }

    def _process_unit(self, target: str, domain: str, ts: str, base_entries: dict,
                      prior_rows: list[dict], suggestions_row: Mapping[str, str]) -> dict:
        """Run one (target, domain)'s discover->validate->[harsh] sequence.

        Runs on a private cache view seeded from ``base_entries`` -- a frozen
        snapshot taken once before any unit is dispatched -- so this never reads
        or writes the shared cache dict directly; concurrent units (disjoint
        cache keys per domain+target, see ``cache_key``) cannot interfere with
        each other. The caller (run_sweep's as_completed loop, single-threaded)
        commits this unit's ``new_cache_entries`` into the shared cache and
        extends ``new_by_target`` with its ``rows`` after this returns -- the
        only two pieces of state this whole sweep shares across (target, domain)
        units. On failure, ``new_cache_entries`` is empty, so a failed run's
        stage-level cache writes never reach the shared cache (equivalent to the
        prior discard-by-run_id behavior, but by simply never propagating them).
        """
        rid = f"{ts}-{domain}-{uuid.uuid4().hex[:8]}"; final_dir = self.root / "domains" / domain / rid
        temp_dir = final_dir.with_name(final_dir.name + ".tmp"); temp_dir.mkdir(parents=True)
        local_cache = {"entries": dict(base_entries)}
        policy_block, policy_hash = _policy_block(self.policy_paths[target], domain)
        eligibility_hash = hashlib.sha256(self.eligibility_path.read_bytes()).hexdigest() if self.eligibility_path.exists() else "missing"
        manifest = {"schema_version": 1, "run_id": rid, "timestamp": ts, "domain": domain, "requested_targets": [target],
                    "commands_executed": [], "stages_skipped": [], "input_fingerprints": {},
                    "policy_file_identifiers": {str(self.policy_paths[target]): policy_hash, str(self.eligibility_path): eligibility_hash},
                    "parameters": {}, "discovery_engine_version": self.cfg.engine_version, "source_suggestions_row": suggestions_row,
                    "result_files": [], "logs": [], "warnings": [], "final_status": "running"}
        try:
            rows=[]; stage_provenance=[]; fps=[]
            for mode in ("discover","validate"):
                params=self._params(suggestions_row,mode); manifest["parameters"][mode]=params
                rs,sources,fp=self._stage(target,domain,mode,params,temp_dir,local_cache,manifest); rows+=rs;stage_provenance+=sources;fps.append(fp)
            if not all_shape_gates_accepted(rows,"validate"):
                params=self._params(suggestions_row,"harsh"); manifest["parameters"]["harsh"]=params
                rs,sources,fp=self._stage(target,domain,"harsh",params,temp_dir,local_cache,manifest);rows+=rs;stage_provenance+=sources;fps.append(fp)
            else: manifest["stages_skipped"].append({"stage":"harsh/*","reason":"validate_acceptance_gate_satisfied"})
            # Paths were recorded while evidence was in its temporary sibling.
            manifest=json.loads(json.dumps(manifest).replace(str(temp_dir),str(final_dir)))
            stage_provenance=json.loads(json.dumps(stage_provenance).replace(str(temp_dir),str(final_dir)))
            for entry in local_cache["entries"].values():
                if entry.get("result_run_id")==rid:
                    entry["result_path"]=str(entry.get("result_path","")).replace(str(temp_dir),str(final_dir))
            manifest["final_status"]="ok"; atomic_json(temp_dir/"run_manifest.json",manifest); os.replace(temp_dir,final_dir)
            new_cache_entries = {k: v for k, v in local_cache["entries"].items() if v.get("result_run_id") == rid}
            summary_rows = self._summarize(target,domain,rows,ts,rid,final_dir,sha256_value(sorted(fps)),stage_provenance)
            return {"target": target, "domain": domain, "rows": summary_rows, "new_cache_entries": new_cache_entries}
        except Exception as exc:
            manifest=json.loads(json.dumps(manifest).replace(str(temp_dir),str(final_dir)))
            manifest["final_status"]="blocked";manifest["warnings"].append(str(exc));atomic_json(temp_dir/"run_manifest.json",manifest);os.replace(temp_dir,final_dir)
            old=[dict(r) for r in prior_rows if r["domain"]==domain]
            if old:
                for r in old:r.update(summary_timestamp=ts,result_provenance_status="blocked_refresh_previous_retained",refresh_attempted="true",refresh_status="blocked",refresh_error=str(exc))
                rows_out=old
            else:
                rows_out=[{"summary_timestamp":ts,"domain":domain,"target":target,"shape_gate":"__all__","decision_status":"blocked","decision_reason":str(exc),"governance_status":"unratified","result_provenance_status":"fresh","domain_result_timestamp":ts,"run_id":rid,"source_run_id":rid,"source_evidence_path":str(final_dir),"refresh_attempted":"true","refresh_status":"blocked","refresh_error":str(exc),"discovery_engine_version":self.cfg.engine_version}]
            return {"target": target, "domain": domain, "rows": rows_out, "new_cache_entries": {}}

    def run_sweep(self) -> dict:
        if self.cfg.skip_join and self.cfg.skip_sig:
            raise ValueError("--skip-join and --skip-sig cannot be used together")
        suggestions = read_csv(self.cfg.suggestions_csv); by_domain={r["domain"]:r for r in suggestions}
        all_domains=sorted(by_domain); requested=sorted(self.cfg.domains or all_domains)
        missing=set(requested)-set(all_domains)
        if missing: raise ValueError("unknown domains: " + ",".join(sorted(missing)))
        targets=[t for t in TARGETS if not ((t=="join" and self.cfg.skip_join) or (t=="sig" and self.cfg.skip_sig))]
        prior={t:self._latest(t) for t in targets}
        # A scoped first run is valid: it publishes a summary containing only
        # requested domains. Later scoped runs carry forward whatever prior
        # domains exist and add/refresh the newly requested subset.
        cache=self._load_cache()
        print(f"[sweep] preparing fingerprints for {len(requested)} domain(s); each source CSV is scanned at most once", flush=True)
        likely_hits=[]
        for target in targets:
            for domain in requested:
                for mode in ("discover","validate"):
                    params=self._params(by_domain[domain],mode)
                    fp=self._input_fingerprint(domain,target,mode,"greedy","__all__",params)
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
        # Each (target, domain) pair owns its own rid/temp_dir/manifest and a
        # disjoint slice of cache keys (see cache_key), so the whole pair is the
        # unit of parallelism; only its own internal discover->validate->harsh
        # escalation stays sequential (inside _process_unit). Workers read from
        # a frozen pre-dispatch snapshot of the cache and return their results
        # for the (single-threaded) as_completed loop below to commit -- no
        # worker ever touches the shared `cache` or `new_by_target` directly.
        units=[(target,domain) for target in targets for domain in requested]
        base_entries=dict(cache["entries"])
        self._interrupt_event.clear()
        print(f"[sweep] dispatching {len(units)} (target, domain) unit(s) across {self.cfg.workers} worker(s)", flush=True)
        with ThreadPoolExecutor(max_workers=self.cfg.workers) as executor:
            futures={
                executor.submit(self._process_unit,target,domain,ts,base_entries,prior[target][1],by_domain[domain]):(target,domain)
                for target,domain in units
            }
            try:
                for future in as_completed(futures):
                    result=future.result()
                    cache["entries"].update(result["new_cache_entries"])
                    new_by_target[result["target"]]+=result["rows"]
            except KeyboardInterrupt:
                # SIGINT only reaches this thread; tell every still-running
                # worker (polling this event in _invoke) to terminate its
                # subprocess, and drop everything not yet started so the
                # executor's shutdown-on-exit doesn't wait for it to run.
                self._interrupt_event.set()
                for pending in futures:
                    pending.cancel()
                raise
        # Validate everything before publishing anything.
        for target, rows in new_by_target.items():
            keys=[(r.get("domain"),r.get("target"),r.get("shape_gate") or "__all__") for r in rows]
            if len(keys)!=len(set(keys)): raise RuntimeError(f"duplicate summary keys for {target}")
        self.summaries.mkdir(parents=True,exist_ok=True)
        for target,rows in new_by_target.items():
            prefix="join_key" if target=="join" else "sig_hash"; atomic_csv(self.summaries/f"{prefix}_discovery_summary_{ts}.csv",sorted(rows,key=lambda r:(r.get('domain',''),r.get('shape_gate',''))),SUMMARY_FIELDS)
        domains=sorted({r["domain"] for rows in new_by_target.values() for r in rows})
        for d in domains:
            join=self._aggregate_domain([r for r in new_by_target.get("join",[]) if r["domain"]==d])
            sig=self._aggregate_domain([r for r in new_by_target.get("sig",[]) if r["domain"]==d])
            run_rows.append({"summary_timestamp":ts,"domain":d,
                "join_result_status":join["decision_status"],"sig_result_status":sig["decision_status"],
                "fresh_or_cached_or_carried":";".join(sorted(join["provenance"]|sig["provenance"])),
                "refresh_status":";".join(sorted(join["refresh"]|sig["refresh"])),
                "pareto_invoked":"true" if join["pareto_invoked"] or sig["pareto_invoked"] else "false",
                "harsh_invoked":"true" if join["harsh_invoked"] or sig["harsh_invoked"] else "false",
                "warnings":";".join(join["warnings"]+sig["warnings"]),
                "blocked_reason":";".join(join["blocked_reasons"]+sig["blocked_reasons"])})
        atomic_csv(self.summaries/f"discovery_run_summary_{ts}.csv",run_rows,RUN_FIELDS);atomic_json(self.cache_path,cache)
        counts=Counter(r.get("decision_status") for rows in new_by_target.values() for r in rows);counts.update(r.get("result_provenance_status") for rows in new_by_target.values() for r in rows)
        print(json.dumps({"summary_timestamp":ts,"counts":dict(sorted(counts.items()))},indent=2));return {"timestamp":ts,"rows":new_by_target}


def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description="Run staged, cache-aware JoinKey/SigHash discovery")
    p.add_argument("--exports-root",default="Fingerprint_Data");p.add_argument("--repo-root",default=str(Path(__file__).resolve().parents[1]));p.add_argument("--suggestions-csv")
    p.add_argument("--domains");p.add_argument("--skip-join",action="store_true");p.add_argument("--skip-sig",action="store_true");p.add_argument("--force",action="store_true");p.add_argument("--what-if",action="store_true");p.add_argument("--run",action="store_true")
    p.add_argument("--workers",default=str(DEFAULT_WORKERS),help="Parallel (target, domain) workers, or 'auto' (cpu_count - 2, capped at 61 on Windows)")
    return p


def config_from_args(ns: argparse.Namespace) -> Config:
    exports=Path(ns.exports_root).resolve(); return Config(exports,Path(ns.repo_root).resolve(),Path(ns.suggestions_csv).resolve() if ns.suggestions_csv else exports/"diagnostics/discovery_param_suggestions.csv",[x.strip() for x in ns.domains.split(",") if x.strip()] if ns.domains else None,ns.skip_join,ns.skip_sig,ns.force,ns.what_if,ns.run,workers=resolve_worker_count(ns.workers))


def main(argv: list[str] | None=None) -> int:
    try: Orchestrator(config_from_args(build_parser().parse_args(argv))).run_sweep();return 0
    except Exception as exc: print(f"ERROR: {exc}",file=sys.stderr);return 1
