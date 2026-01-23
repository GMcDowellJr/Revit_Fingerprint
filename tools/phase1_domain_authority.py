#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass
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


@dataclass(frozen=True)
class RunConfig:
    analysis_run_id: str
    seed_baseline_id: str
    domains_in_scope: List[str]
    status_comparable_set: List[str]
    status_blocked_set: List[str]
    status_unsupported_set: List[str]
    status_failed_set: List[str]
    cluster_method: str  # "domain_hash_exact" (phase 1)
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
            cluster_method=str(d.get("cluster_method", "domain_hash_exact")),
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
    json_paths = sorted([p for p in input_dir.glob("*.json") if p.is_file()])
    if not json_paths:
        raise SystemExit(f"No .json files found in: {input_dir}")

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
            per.append(
                {
                    "project_id": pr["project_id"],
                    "domain": dom,
                    "domain_status": status,
                    "domain_hash": h,
                    "domain_version": dv,
                }
            )

        # Seed info
        seed_rec = seed_domains.get(dom, {})
        seed_hash = seed_rec.get("hash") if isinstance(seed_rec, dict) else None
        seed_exists = isinstance(seed_hash, str) and bool(seed_hash)

        # Baseline coverage table
        for r in per:
            status = r["domain_status"]
            domain_hash = r["domain_hash"]

            # Comparable requires BOTH: comparable status + a real domain hash.
            comparable = (
                isinstance(status, str)
                and status in cfg.status_comparable_set
                and isinstance(domain_hash, str)
                and bool(domain_hash)
            )

            match = None
            cluster_id = None

            if comparable and seed_exists:
                match = (domain_hash == seed_hash)

            if comparable:
                cluster_id = f"hash:{domain_hash}"

            baseline_rows.append(
                {
                    "analysis_run_id": cfg.analysis_run_id,
                    "project_id": r["project_id"],
                    "domain": dom,
                    "domain_status": status,
                    "domain_hash": domain_hash,
                    "seed_domain_hash": seed_hash if seed_exists else None,
                    "seed_hash_match": match,
                    "cluster_id": cluster_id,
                    "domain_version": r["domain_version"],
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
        comparable_items: List[Tuple[str, str]] = []  # (project_id, domain_hash)

        for r in per:
            st = r["domain_status"]
            dh = r["domain_hash"]

            if not isinstance(st, str):
                counts["other"] += 1
                continue

            if st in cfg.status_comparable_set:
                # Comparable requires a real hash; otherwise treat as "other" for authority inference.
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

        # Clustering (Phase 1: exact hash)
        clusters: Dict[str, List[str]] = {}  # cluster_id -> [project_id]
        if cfg.cluster_method != "domain_hash_exact":
            raise SystemExit(f"Unsupported cluster_method for Phase 1: {cfg.cluster_method}")

        for pid, dh in comparable_items:
            cid = f"hash:{dh}"
            clusters.setdefault(cid, []).append(pid)

        cluster_count = len(clusters)
        dominant_cluster_id = ""
        dominant_cluster_share = 0.0

        shares: List[float] = []
        if projects_comparable > 0:
            for cid, members in clusters.items():
                share = len(members) / projects_comparable
                shares.append(share)
            dominant_cluster_id = max(clusters.items(), key=lambda kv: len(kv[1]))[0] if clusters else ""
            dominant_cluster_share = max(shares) if shares else 0.0

        conc_hhi = hhi(shares) if shares else 0.0
        conv = classify_convergence(dominant_cluster_share, cfg)
        conf = authority_confidence(comparable_rate, dominant_cluster_share, conv)

        # Seed match rate (denominator = hashable comparable)
        seed_match_count = 0
        seed_match_rate: Optional[float] = None
        if seed_exists and projects_comparable > 0:
            for _, dh in comparable_items:
                if dh == seed_hash:
                    seed_match_count += 1
            seed_match_rate = seed_match_count / projects_comparable

        outcome = classify_authority_outcome(
            observable=observable,
            seed_exists=seed_exists,
            seed_match_rate=seed_match_rate,
            dominant_share=dominant_cluster_share,
            conc_hhi=conc_hhi,
            cfg=cfg,
        )
        scope_rec = authority_scope_recommendation(outcome)

        # Rationale text: strictly metric-based
        if outcome == "not_observable":
            rationale = f"Comparable rate {comparable_rate:.2f} below threshold {cfg.observability_min_comparable_rate:.2f} or no comparable projects."
        else:
            sm = "n/a" if seed_match_rate is None else f"{seed_match_rate:.2f}"
            rationale = f"Comparable rate {comparable_rate:.2f}; dominant cluster share {dominant_cluster_share:.2f} ({conv}); seed match {sm}; clusters={cluster_count}; HHI={conc_hhi:.2f}."

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
                "dominant_cluster_id": dominant_cluster_id,
                "dominant_cluster_share": round(dominant_cluster_share, 6),
                "cluster_count": cluster_count,
                "cluster_concentration_hhi": round(conc_hhi, 6),
                "convergence_class": conv,
                "authority_outcome": outcome,
                "authority_confidence": conf,
                "authority_scope_recommendation": scope_rec,
                "rationale_short": rationale,
                "notes": "",
            }
        )

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
            "dominant_cluster_id",
            "dominant_cluster_share",
            "cluster_count",
            "cluster_concentration_hhi",
            "convergence_class",
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
