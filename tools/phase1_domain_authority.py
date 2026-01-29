#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import dataclass
from collections import Counter
import statistics
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -------------------------
# Config + enums
# -------------------------

AUTHORITY_OUTCOMES = {
    "holds_by_default",
    "changed_but_convergent",
    "changed_and_divergent",
    "ignored_almost_everywhere",
    "not_observable",
}
SCOPE_RECS = {"retain", "narrow", "downgrade_to_advisory", "defer_not_observable"}
CONF_LEVELS = {"high", "medium", "low"}

SEMANTIC_UID_DOMAINS = {
    "dimension_types",
}


@dataclass(frozen=True)
class RunConfig:
    analysis_run_id: str
    seed_baseline_id: str
    domains_in_scope: List[str]
    status_comparable_set: List[str]
    status_blocked_set: List[str]
    status_unsupported_set: List[str]
    status_failed_set: List[str]
    cluster_method: str  # "semantic_multiset_exact" (phase 1 default) or "domain_hash_exact"
    observability_min_comparable_rate: float
    convergence_high: float
    convergence_medium: float
    ignored_seed_match_max: float
    ignored_hhi_max: float

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "RunConfig":
        return RunConfig(
            analysis_run_id=str(d["analysis_run_id"]),
            seed_baseline_id=str(d.get("seed_baseline_id", "none")),
            domains_in_scope=list(d.get("domains_in_scope", [])),
            status_comparable_set=list(d.get("status_comparable_set", ["ok"])),
            status_blocked_set=list(d.get("status_blocked_set", ["blocked"])),
            status_unsupported_set=list(d.get("status_unsupported_set", ["unsupported"])),
            status_failed_set=list(d.get("status_failed_set", ["failed"])),
            cluster_method=str(d.get("cluster_method", "semantic_multiset_exact")),
            observability_min_comparable_rate=float(d.get("observability_min_comparable_rate", 0.6)),
            convergence_high=float(d.get("convergence_thresholds", {}).get("high", 0.8)),
            convergence_medium=float(d.get("convergence_thresholds", {}).get("medium", 0.6)),
            ignored_seed_match_max=float(d.get("ignored_thresholds", {}).get("seed_match_max", 0.1)),
            ignored_hhi_max=float(d.get("ignored_thresholds", {}).get("hhi_max", 0.2)),
        )


# -------------------------
# JSON reading helpers
# -------------------------

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def project_id_from_fp(fp: Dict[str, Any], fallback: str) -> str:
    # Prefer _features.identity.project_title when present (seen in both sample files)
    ident = fp.get("_features", {}).get("identity", {}) or fp.get("_contract", {}).get("identity", {})
    title = ident.get("project_title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    return fallback

# -------------------------
# Phase-1 semantic hashing (derived, order-independent)
# -------------------------

def _extract_record_sig_hashes_v2(fp: Optional[Dict[str, Any]], domain_name: str) -> Optional[List[str]]:
    """
    Contract source of truth (record.v2):
      - domain_payload["records"] containing dicts with:
          schema_version == "record.v2"
          status in {ok,degraded,blocked}
          sig_hash: 32-hex for ok/degraded; null for blocked
    Return:
      - None if domain payload missing / not record.v2
      - [] if record.v2 list exists but no sigs (still "known empty")
      - [..] list (may include duplicates)
    """
    if not isinstance(fp, dict):
        return None

    payload = fp.get(domain_name)
    if not isinstance(payload, dict):
        return None

    recs = payload.get("records")
    if not isinstance(recs, list):
        return None

    out: List[str] = []
    saw_v2 = False

    for r in recs:
        if not isinstance(r, dict):
            continue
        if r.get("schema_version") != "record.v2":
            continue

        saw_v2 = True
        st = str(r.get("status") or "").strip().lower()
        if st == "blocked":
            continue

        sig = r.get("sig_hash")
        if isinstance(sig, str) and sig:
            if domain_name in SEMANTIC_UID_DOMAINS:
                out.append(_strip_revit_uid_tail(sig))
            else:
                out.append(sig)

    if not saw_v2:
        return None

    return out


def _jaccard_set(a: Optional[List[str]], b: Optional[List[str]]) -> Optional[float]:
    if a is None or b is None:
        return None
    sa = set(a)
    sb = set(b)
    if not sa and not sb:
        return 1.0
    if not sa and sb:
        return 0.0
    if sa and not sb:
        return 0.0
    inter = len(sa & sb)
    uni = len(sa | sb)
    return inter / uni if uni else 1.0


def _jaccard_multiset(a: Optional[List[str]], b: Optional[List[str]]) -> Optional[float]:
    if a is None or b is None:
        return None
    ca = Counter(a)
    cb = Counter(b)
    if not ca and not cb:
        return 1.0
    if not ca and cb:
        return 0.0
    if ca and not cb:
        return 0.0
    keys = set(ca.keys()) | set(cb.keys())
    inter = 0
    uni = 0
    for k in keys:
        inter += min(ca.get(k, 0), cb.get(k, 0))
        uni += max(ca.get(k, 0), cb.get(k, 0))
    return inter / uni if uni else 1.0


import hashlib


SEMANTIC_UID_KEYS_BY_DOMAIN = {
    # Revit UniqueId is typically GUID-ELEMENTID. ElementId is file-local noise.
    "dimension_types": {"dim_type.uid", "dim_type.tick_mark_uid"},
}


def _md5_utf8(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _strip_revit_uid_tail(v: Any) -> Any:
    """
    Normalize Revit UniqueId-like strings: '{GUID}-{8hex elementId}' -> '{GUID}'.
    Best-effort: split on last hyphen only.
    """
    if not isinstance(v, str):
        return v
    v = v.strip()
    if "-" not in v:
        return v
    return v.rsplit("-", 1)[0]


def _canonical_item_str(k: Any, q: Any, v: Any) -> str:
    if v is None:
        v_str = "null"
    elif isinstance(v, bool):
        v_str = "true" if v else "false"
    else:
        v_str = str(v)
    return f"{k}={q}:{v_str}"


def _semantic_record_sig_hash(domain: str, rec: Dict[str, Any]) -> Optional[str]:
    """
    Derived semantic signature hash for a single record:
      - Prefer identity_basis.items when present (record.v2)
      - Normalize any domain-specific UID keys by stripping ElementId tails
      - Hash canonicalized items (order preserved as exported)
    """
    ib = rec.get("identity_basis")
    if not isinstance(ib, dict):
        return None

    items = ib.get("items")
    if not isinstance(items, list) or not items:
        return None

    uid_keys = SEMANTIC_UID_KEYS_BY_DOMAIN.get(domain, set())

    parts: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        q = it.get("q")
        v = it.get("v")

        if isinstance(k, str) and k in uid_keys:
            v = _strip_revit_uid_tail(v)

        parts.append(_canonical_item_str(k, q, v))

    if not parts:
        return None

    return _md5_utf8("|".join(parts))


def _domain_payload_from_fp(fp: Dict[str, Any], dom: str) -> Optional[Dict[str, Any]]:
    """
    Best-effort extraction for record.v2 domain payloads, matching observed shapes:
      - fp[dom] = {"records":[...]}
      - fp["records"][dom] = [...]
      - fp["domains"][dom]["records"] = [...]
    """
    v = fp.get(dom)
    if isinstance(v, dict):
        return v

    recs = fp.get("records")
    if isinstance(recs, dict) and isinstance(recs.get(dom), list):
        return {"records": recs.get(dom)}

    doms = fp.get("domains")
    if isinstance(doms, dict):
        dv = doms.get(dom)
        if isinstance(dv, dict) and isinstance(dv.get("records"), list):
            return dv

    return None


def _semantic_domain_multiset_hash(fp: Dict[str, Any], dom: str) -> Optional[str]:
    """
    Derived domain signature:
      - Compute semantic record sig hashes for all records
      - Sort (multiset, order-independent)
      - Hash the joined list

    NOTE: This intentionally treats delete+recreate as equivalent when identity_basis items match.
    """
    payload = _domain_payload_from_fp(fp, dom)
    if not isinstance(payload, dict):
        return None

    records = payload.get("records")
    if not isinstance(records, list) or not records:
        return None

    sigs: List[str] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        h = _semantic_record_sig_hash(dom, rec)
        if isinstance(h, str) and h:
            sigs.append(h)

    if not sigs:
        return None

    sigs.sort()
    return _md5_utf8("|".join(sigs))

def extract_domains_summary(fp: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Return a normalized dict:
      domain -> {"status": str|None, "hash": str|None, "domain_version": str|None, "block_reasons": list[str]}
    Preference order (most 'run-realistic' first):
      1) _manifest.domains (status/hash)
      2) _contract.domains (status/hash/domain_version)
      3) _features.domains (status/hash/count)
    """
    out: Dict[str, Dict[str, Any]] = {}

    def ingest(src: Dict[str, Any], has_domain_version: bool) -> None:
        for dom, rec in src.items():
            if not isinstance(rec, dict):
                continue
            status = rec.get("status")
            h = rec.get("hash")
            dv = rec.get("domain_version") if has_domain_version else None
            br = rec.get("block_reasons") or []
            if dom not in out:
                out[dom] = {
                    "status": status if isinstance(status, str) else None,
                    "hash": h if isinstance(h, str) else None,
                    "domain_version": str(dv) if dv is not None else None,
                    "block_reasons": list(br) if isinstance(br, list) else [],
                }

    manifest_domains = fp.get("_manifest", {}).get("domains", {})
    if isinstance(manifest_domains, dict):
        ingest(manifest_domains, has_domain_version=False)

    contract_domains = fp.get("_contract", {}).get("domains", {})
    if isinstance(contract_domains, dict):
        ingest(contract_domains, has_domain_version=True)

    features_domains = fp.get("_features", {}).get("domains", {})
    if isinstance(features_domains, dict):
        ingest(features_domains, has_domain_version=False)

    # Some files also have "_domains" top-level (seen in sample)
    extra_domains = fp.get("_domains", {})
    if isinstance(extra_domains, dict):
        ingest(extra_domains, has_domain_version=True)

    return out


# -------------------------
# Metrics
# -------------------------

def hhi(shares: Iterable[float]) -> float:
    return float(sum(s * s for s in shares))


def classify_convergence(dominant_share: float, cfg: RunConfig) -> str:
    if dominant_share >= cfg.convergence_high:
        return "high_convergence"
    if dominant_share >= cfg.convergence_medium:
        return "medium_convergence"
    return "low_convergence"


def authority_confidence(comparable_rate: float, dominant_share: float, conv: str) -> str:
    # Deterministic, conservative.
    if comparable_rate >= 0.8 and dominant_share >= 0.8 and conv == "high_convergence":
        return "high"
    if comparable_rate >= 0.6 and dominant_share >= 0.6:
        return "medium"
    return "low"


def authority_scope_recommendation(outcome: str) -> str:
    if outcome == "holds_by_default":
        return "retain"
    if outcome == "changed_but_convergent":
        return "narrow"
    if outcome in ("changed_and_divergent", "ignored_almost_everywhere"):
        return "downgrade_to_advisory"
    return "defer_not_observable"


def classify_authority_outcome(
    *,
    observable: bool,
    seed_exists: bool,
    seed_match_rate: Optional[float],
    dominant_share: float,
    conc_hhi: float,
    cfg: RunConfig,
) -> str:
    if not observable:
        return "not_observable"

    conv = classify_convergence(dominant_share, cfg)

    # If no seed, we can still classify "holds_by_default" vs "divergent-ish" via concentration.
    if not seed_exists or seed_match_rate is None:
        if conv == "high_convergence":
            return "holds_by_default"
        if conv == "medium_convergence":
            return "changed_and_divergent"
        return "ignored_almost_everywhere"

    # With seed:
    if seed_match_rate >= cfg.convergence_medium and conv == "high_convergence":
        return "holds_by_default"

    if seed_match_rate < cfg.convergence_medium and conv == "high_convergence":
        return "changed_but_convergent"

    # Divergent vs ignored:
    if seed_match_rate <= cfg.ignored_seed_match_max and conc_hhi <= cfg.ignored_hhi_max:
        return "ignored_almost_everywhere"

    return "changed_and_divergent"


# -------------------------
# Output tables
# -------------------------

def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


# -------------------------
# Main analysis
# -------------------------

def analyze(input_dir: Path, cfg: RunConfig, seed_path: Optional[Path], out_dir: Path) -> None:
    details = sorted([p for p in input_dir.glob("*.details.json") if p.is_file()])
    index = sorted([p for p in input_dir.glob("*.index.json") if p.is_file()])
    legacy = sorted([p for p in input_dir.glob("*.legacy.json") if p.is_file()])

    if details:
        json_paths = details
        if legacy:
            sys.stderr.write(
                "[WARN phase1_domain_authority] legacy bundle(s) present but ignored by default (details present).\n"
            )
    elif index:
        json_paths = index
        if legacy:
            sys.stderr.write(
                "[WARN phase1_domain_authority] legacy bundle(s) present but ignored by default (index present).\n"
            )
        sys.stderr.write(
            "[WARN phase1_domain_authority] No *.details.json found; using *.index.json "
            "(semantic multiset clustering may be degraded).\n"
        )
    else:
        json_paths = sorted(
            [p for p in input_dir.glob("*.json") if p.is_file() and not str(p).lower().endswith(".legacy.json")]
        )
        if legacy and not json_paths:
            json_paths = legacy
            sys.stderr.write(
                "[WARN phase1_domain_authority] Only legacy bundle(s) found; results may be incomplete.\n"
            )
        else:
            sys.stderr.write(
                "[WARN phase1_domain_authority] No split exports found; falling back to *.json excluding legacy.\n"
            )

    if not json_paths:
        raise SystemExit(f"No compatible export JSON files found in: {input_dir}")

    seed_fp: Optional[Dict[str, Any]] = load_json(seed_path) if seed_path else None
    seed_domains = extract_domains_summary(seed_fp) if seed_fp else {}

    # Load all projects
    projects: List[Dict[str, Any]] = []
    for p in json_paths:
        fp = load_json(p)
        pid = project_id_from_fp(fp, fallback=p.stem)
        domains = extract_domains_summary(fp)
        projects.append({"project_id": pid, "path": str(p), "fp": fp, "domains": domains})

    # Determine domains in scope
    if cfg.domains_in_scope:
        domains_in_scope = cfg.domains_in_scope
    else:
        # Union of all domains present anywhere
        s = set()
        for pr in projects:
            s.update(pr["domains"].keys())
        domains_in_scope = sorted(s)

    # Baseline coverage rows
    baseline_rows: List[Dict[str, Any]] = []

    # Cluster summary rows
    cluster_rows: List[Dict[str, Any]] = []

    # Authority summary rows
    authority_rows: List[Dict[str, Any]] = []

    for dom in domains_in_scope:
        # Collect per-project domain info
        per: List[Dict[str, Any]] = []
        for pr in projects:
            d = pr["domains"].get(dom, {})
            status = d.get("status")
            h = d.get("hash")
            dv = d.get("domain_version")
            sem_h = _semantic_domain_multiset_hash(pr["fp"], dom)

            sigs = _extract_record_sig_hashes_v2(pr["fp"], dom)

            per.append(
                {
                    "project_id": pr["project_id"],
                    "domain": dom,
                    "domain_status": status,
                    "domain_hash": h,                 # exporter-provided (raw)
                    "domain_hash_semantic": sem_h,    # derived (record-level semantic multiset hash)
                    "domain_sig_multiset": sigs,      # derived (record-level sig_hash multiset)
                    "domain_version": dv,
                }
            )

        # Counts
        projects_total = len(projects)
        counts = {
            "comparable": 0,      # comparable status + NON-EMPTY hash (hashable comparable)
            "blocked": 0,
            "unsupported": 0,
            "failed": 0,
            "other": 0,
        }
        comparable_items: List[Tuple[str, str]] = []  # (project_id, cluster_hash)

        for r in per:
            st = r["domain_status"]
            dh_raw = r["domain_hash"]
            dh_sem = r.get("domain_hash_semantic")

            dh = dh_raw
            if cfg.cluster_method == "semantic_multiset_exact":
                dh = dh_sem

            if not isinstance(st, str):
                counts["other"] += 1
                continue

            if st in cfg.status_comparable_set:
                # Comparable requires a real clustering hash; otherwise treat as "other" for authority inference.
                if isinstance(dh, str) and bool(dh):
                    counts["comparable"] += 1
                    comparable_items.append((r["project_id"], dh))
                else:
                    counts["other"] += 1

            elif st in cfg.status_blocked_set:
                counts["blocked"] += 1
            elif st in cfg.status_unsupported_set:
                counts["unsupported"] += 1
            elif st in cfg.status_failed_set:
                counts["failed"] += 1
            else:
                counts["other"] += 1

        projects_comparable = counts["comparable"]
        comparable_rate = projects_comparable / projects_total if projects_total else 0.0

        # Observability gate (based on hashable comparable)
        observable = (projects_comparable > 0) and (comparable_rate >= cfg.observability_min_comparable_rate)

        # Build clusters from comparable_items (exact hash clusters)
        clusters: Dict[str, List[str]] = {}
        for pid, dh in comparable_items:
            cid = f"hash:{dh}"
            clusters.setdefault(cid, []).append(pid)

        cluster_count = len(clusters)
        dominant_cluster_id = ""
        dominant_cluster_share = 0.0
        if clusters:
            dominant_cluster_id, members = max(clusters.items(), key=lambda kv: len(kv[1]))
            dominant_cluster_share = len(members) / projects_comparable if projects_comparable else 0.0

        # Concentration (HHI of cluster shares)
        conc_hhi = 0.0
        if projects_comparable:
            for _, members in clusters.items():
                share = len(members) / projects_comparable
                conc_hhi += share * share

        # Seed info (must be defined before any seed-dependent logic)
        seed_rec = seed_domains.get(dom, {})
        seed_hash_raw = seed_rec.get("hash") if isinstance(seed_rec, dict) else None
        seed_fp_hash_sem = _semantic_domain_multiset_hash(seed_fp, dom) if seed_fp else None

        if cfg.cluster_method == "semantic_multiset_exact":
            seed_hash = seed_fp_hash_sem
        else:
            seed_hash = seed_hash_raw

        seed_exists = isinstance(seed_hash, str) and bool(seed_hash)

        # Seed record-level sig hashes (for similarity)
        seed_sigs = _extract_record_sig_hashes_v2(seed_fp, dom) if seed_fp else None

        # Seed match stats (exact-hash match)
        seed_match_count = None
        seed_match_rate = None
        if seed_exists and projects_comparable:
            seed_match_count = 0
            for _, dh in comparable_items:
                if dh == seed_hash:
                    seed_match_count += 1
            seed_match_rate = seed_match_count / projects_comparable

        # Similarity-to-seed (record-level multiset Jaccard), and pairwise similarity (if not baseline, what)
        seed_sigs = _extract_record_sig_hashes_v2(seed_fp, dom) if seed_fp else None

        # Only compute similarity where we have per-project sig multisets.
        comp_sigs: List[Tuple[str, List[str]]] = []
        for r in per:
            st = r["domain_status"]
            if not (isinstance(st, str) and st in cfg.status_comparable_set):
                continue
            sm = r.get("domain_sig_multiset")
            if isinstance(sm, list):
                comp_sigs.append((r["project_id"], sm))

        seed_ms_sims: List[float] = []
        if seed_sigs is not None:
            for _, sm in comp_sigs:
                v = _jaccard_multiset(sm, seed_sigs)
                if isinstance(v, float):
                    seed_ms_sims.append(v)

        pairwise_ms_sims: List[float] = []
        medoid_project_id = None
        medoid_avg_similarity = None
        if len(comp_sigs) >= 2:
            # Pairwise distribution + medoid (max avg similarity to others)
            avg_by_pid: Dict[str, List[float]] = {pid: [] for pid, _ in comp_sigs}
            n = len(comp_sigs)
            for i in range(n):
                pid_a, a = comp_sigs[i]
                for j in range(i + 1, n):
                    pid_b, b = comp_sigs[j]
                    v = _jaccard_multiset(a, b)
                    if isinstance(v, float):
                        pairwise_ms_sims.append(v)
                        avg_by_pid[pid_a].append(v)
                        avg_by_pid[pid_b].append(v)

            best_pid = None
            best_avg = -1.0
            for pid, vals in avg_by_pid.items():
                if not vals:
                    continue
                av = sum(vals) / len(vals)
                if av > best_avg:
                    best_avg = av
                    best_pid = pid

            medoid_project_id = best_pid
            medoid_avg_similarity = best_avg if best_avg >= 0.0 else None

        seed_ms_median = statistics.median(seed_ms_sims) if seed_ms_sims else None
        seed_ms_p25 = statistics.quantiles(seed_ms_sims, n=4)[0] if len(seed_ms_sims) >= 4 else None

        pair_ms_median = statistics.median(pairwise_ms_sims) if pairwise_ms_sims else None
        pair_ms_p90 = statistics.quantiles(pairwise_ms_sims, n=10)[8] if len(pairwise_ms_sims) >= 10 else None

        # Convergence class (still driven by dominant cluster share for now)
        if not observable:
            conv = "not_observable"
        elif dominant_cluster_share >= cfg.convergence_high:
            conv = "high_convergence"
        elif dominant_cluster_share >= cfg.convergence_medium:
            conv = "medium_convergence"
        else:
            conv = "low_convergence"

        # Authority outcome (unchanged logic, but we now have richer evidence columns)
        if not observable:
            outcome = "not_observable"
            conf = "low"
            scope_rec = "defer_not_observable"
        else:
            # Default “normative” reading is: strong convergence (cluster share) and/or strong similarity to seed
            if dominant_cluster_share >= cfg.convergence_high:
                outcome = "holds_by_default"
                conf = "high"
                scope_rec = "retain"
            elif dominant_cluster_share >= cfg.convergence_medium:
                outcome = "changed_but_convergent"
                conf = "medium"
                scope_rec = "narrow"
            else:
                # Low convergence: decide between "ignored" vs "divergent"
                sm = seed_match_rate if seed_match_rate is not None else 0.0
                
                # "ignored" should mean: no baseline gravity AND no peer gravity.
                has_baseline_gravity = isinstance(seed_ms_median, float) and (seed_ms_median >= 0.40)
                has_peer_gravity = isinstance(pair_ms_p90, float) and (pair_ms_p90 >= 0.60)

                if (sm <= cfg.ignored_seed_match_max) and (conc_hhi <= cfg.ignored_hhi_max) and (not has_baseline_gravity) and (not has_peer_gravity):
                    outcome = "ignored_almost_everywhere"
                    conf = "low"
                    scope_rec = "downgrade_to_advisory"
                else:
                    outcome = "changed_and_divergent"
                    conf = "low"
                    scope_rec = "downgrade_to_advisory"

        # Rationale text: strictly metric-based
        if outcome == "not_observable":
            rationale = f"Comparable rate {comparable_rate:.2f} below threshold {cfg.observability_min_comparable_rate:.2f} or no comparable projects."
        else:
            sm = "n/a" if seed_match_rate is None else f"{seed_match_rate:.2f}"
            ssim = "n/a" if seed_ms_median is None else f"{seed_ms_median:.2f}"
            psim = "n/a" if pair_ms_median is None else f"{pair_ms_median:.2f}"
            rationale = f"Comparable rate {comparable_rate:.2f}; dominant cluster share {dominant_cluster_share:.2f} ({conv}); seed match {sm}; seed MS-Jaccard median {ssim}; pairwise MS-Jaccard median {psim}; clusters={cluster_count}; HHI={conc_hhi:.2f}."

        authority_rows.append(
            {
                "analysis_run_id": cfg.analysis_run_id,
                "domain": dom,
                "domain_version": seed_rec.get("domain_version") if isinstance(seed_rec, dict) else None,
                "steward": "",  # fill from your steward registry later; not inferred here
                "projects_total": projects_total,
                "projects_comparable": projects_comparable,
                "projects_blocked": counts["blocked"],
                "projects_unsupported": counts["unsupported"],
                "projects_failed": counts["failed"],
                "comparable_rate": round(comparable_rate, 6),
                "seed_baseline_id": cfg.seed_baseline_id,
                "seed_match_count": seed_match_count if seed_exists else None,
                "seed_match_rate": round(seed_match_rate, 6) if seed_match_rate is not None else None,
                "seed_ms_jaccard_median": round(seed_ms_median, 6) if isinstance(seed_ms_median, float) else None,
                "seed_ms_jaccard_p25": round(seed_ms_p25, 6) if isinstance(seed_ms_p25, float) else None,
                "pairwise_ms_jaccard_median": round(pair_ms_median, 6) if isinstance(pair_ms_median, float) else None,
                "pairwise_ms_jaccard_p90": round(pair_ms_p90, 6) if isinstance(pair_ms_p90, float) else None,
                "medoid_project_id": medoid_project_id,
                "medoid_avg_similarity": round(medoid_avg_similarity, 6) if isinstance(medoid_avg_similarity, float) else None,
                "dominant_cluster_id": dominant_cluster_id,
                "dominant_cluster_share": round(dominant_cluster_share, 6),
                "cluster_count": cluster_count,
                "cluster_concentration_hhi": round(conc_hhi, 6),
                "convergence_class": conv,
                "has_baseline_gravity": bool(has_baseline_gravity) if observable else False,
                "has_peer_gravity": bool(has_peer_gravity) if observable else False,
                "divergence_mode": (
                    "not_observable"
                    if not observable
                    else ("lineage_normative" if (has_baseline_gravity or has_peer_gravity) else "chaotic")
                ),
                "authority_outcome": outcome,
                "authority_confidence": conf,
                "authority_scope_recommendation": scope_rec,
                "rationale_short": (
                    rationale
                    if outcome == "not_observable"
                    else f"{rationale} gravity(baseline={int(bool(has_baseline_gravity))}, peer={int(bool(has_peer_gravity))}); mode={('lineage_normative' if (has_baseline_gravity or has_peer_gravity) else 'chaotic')}."
                ),
                "notes": "",
            }
        )


        # Baseline coverage table
        for r in per:
            status = r["domain_status"]
            domain_hash_raw = r["domain_hash"]
            domain_hash_sem = r.get("domain_hash_semantic")

            cluster_hash = domain_hash_raw
            if cfg.cluster_method == "semantic_multiset_exact":
                cluster_hash = domain_hash_sem

            # Comparable requires BOTH: comparable status + a real clustering hash.
            comparable = (
                isinstance(status, str)
                and status in cfg.status_comparable_set
                and isinstance(cluster_hash, str)
                and bool(cluster_hash)
            )

            match = None
            cluster_id = None

            if comparable and seed_exists:
                match = (cluster_hash == seed_hash)

            if comparable:
                cluster_id = f"hash:{cluster_hash}"

            # Similarity to seed (record-level sig hashes); None if not available.
            proj_sigs = r.get("domain_sig_multiset")
            set_j = _jaccard_set(proj_sigs, seed_sigs) if seed_sigs is not None else None
            ms_j = _jaccard_multiset(proj_sigs, seed_sigs) if seed_sigs is not None else None

            baseline_rows.append(
                {
                    "analysis_run_id": cfg.analysis_run_id,
                    "project_id": r["project_id"],
                    "domain": dom,
                    "domain_status": status,
                    "domain_hash": cluster_hash,
                    "seed_domain_hash": seed_hash if seed_exists else None,
                    "seed_hash_match": match,
                    "cluster_id": cluster_id,
                    "domain_version": r["domain_version"],
                    "seed_set_jaccard": round(set_j, 6) if isinstance(set_j, float) else None,
                    "seed_multiset_jaccard": round(ms_j, 6) if isinstance(ms_j, float) else None,
                }
            )

        # Counts
        projects_total = len(projects)
        counts = {
            "comparable": 0,      # comparable status + NON-EMPTY hash (hashable comparable)
            "blocked": 0,
            "unsupported": 0,
            "failed": 0,
            "other": 0,
        }
        comparable_items: List[Tuple[str, str]] = []  # (project_id, cluster_hash)

        for r in per:
            st = r["domain_status"]
            dh_raw = r["domain_hash"]
            dh_sem = r.get("domain_hash_semantic")

            dh = dh_raw
            if cfg.cluster_method == "semantic_multiset_exact":
                dh = dh_sem

            if not isinstance(st, str):
                counts["other"] += 1
                continue

            if st in cfg.status_comparable_set:
                # Comparable requires a real clustering hash; otherwise treat as "other" for authority inference.
                if isinstance(dh, str) and bool(dh):
                    counts["comparable"] += 1
                    comparable_items.append((r["project_id"], dh))
                else:
                    counts["other"] += 1

            elif st in cfg.status_blocked_set:
                counts["blocked"] += 1
            elif st in cfg.status_unsupported_set:
                counts["unsupported"] += 1
            elif st in cfg.status_failed_set:
                counts["failed"] += 1
            else:
                counts["other"] += 1

        projects_comparable = counts["comparable"]
        comparable_rate = projects_comparable / projects_total if projects_total else 0.0

        # Observability gate (based on hashable comparable)
        observable = (projects_comparable > 0) and (comparable_rate >= cfg.observability_min_comparable_rate)

        # Cluster rows
        for cid, members in sorted(clusters.items(), key=lambda kv: len(kv[1]), reverse=True):
            rep = sorted(members)[0] if members else ""
            rep_hash = cid.split("hash:", 1)[1] if cid.startswith("hash:") else None
            cluster_rows.append(
                {
                    "analysis_run_id": cfg.analysis_run_id,
                    "domain": dom,
                    "cluster_id": cid,
                    "cluster_method": cfg.cluster_method,
                    "cluster_key": rep_hash,
                    "projects_in_cluster": len(members),
                    "cluster_share": round(len(members) / projects_comparable, 6) if projects_comparable else None,
                    "representative_project_id": rep,
                    "representative_domain_hash": rep_hash,
                    "within_cluster_dispersion": 0.0,
                    "top_diff_vs_seed": "non_seed_hash" if seed_exists and rep_hash != seed_hash else None,
                    "notes": "",
                }
            )

    # Write outputs
    write_csv(
        out_dir / "baseline_coverage_by_project.csv",
        baseline_rows,
        [
            "analysis_run_id",
            "project_id",
            "domain",
            "domain_status",
            "domain_hash",
            "seed_domain_hash",
            "seed_hash_match",
            "cluster_id",
            "domain_version",
            "seed_set_jaccard",
            "seed_multiset_jaccard",
        ],
    )
    write_csv(
        out_dir / "domain_cluster_summary.csv",
        cluster_rows,
        [
            "analysis_run_id",
            "domain",
            "cluster_id",
            "cluster_method",
            "cluster_key",
            "projects_in_cluster",
            "cluster_share",
            "representative_project_id",
            "representative_domain_hash",
            "within_cluster_dispersion",
            "top_diff_vs_seed",
            "notes",
        ],
    )
    write_csv(
        out_dir / "domain_authority_summary.csv",
        authority_rows,
        [
            "analysis_run_id",
            "domain",
            "domain_version",
            "steward",
            "projects_total",
            "projects_comparable",
            "projects_blocked",
            "projects_unsupported",
            "projects_failed",
            "comparable_rate",
            "seed_baseline_id",
            "seed_match_count",
            "seed_match_rate",
            "seed_ms_jaccard_median",
            "seed_ms_jaccard_p25",
            "pairwise_ms_jaccard_median",
            "pairwise_ms_jaccard_p90",
            "medoid_project_id",
            "medoid_avg_similarity",
            "dominant_cluster_id",
            "dominant_cluster_share",
            "cluster_count",
            "cluster_concentration_hhi",
            "convergence_class",
            "has_baseline_gravity",
            "has_peer_gravity",
            "divergence_mode",
            "authority_outcome",
            "authority_confidence",
            "authority_scope_recommendation",
            "rationale_short",
            "notes",
        ],

    )

    # Also persist the run config for reproducibility
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "run_config.resolved.json").open("w", encoding="utf-8") as f:
        json.dump(cfg.__dict__, f, indent=2, sort_keys=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", required=True, help="Folder of fingerprint JSON files")
    ap.add_argument("--config", required=True, help="RunConfig JSON path")
    ap.add_argument("--seed-baseline", default=None, help="Optional seed baseline fingerprint JSON path")
    ap.add_argument("--out-dir", required=True, help="Output folder for CSVs")
    args = ap.parse_args()

    cfg = RunConfig.from_json(load_json(Path(args.config)))
    seed_path = Path(args.seed_baseline) if args.seed_baseline else None

    analyze(Path(args.input_dir), cfg, seed_path, Path(args.out_dir))


if __name__ == "__main__":
    main()
