#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Any


# -------------------------
# Helpers
# -------------------------

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def hhi(shares: List[float]) -> float:
    return sum(s * s for s in shares)


def effective_cluster_count(shares: List[float]) -> float:
    h = hhi(shares)
    return (1.0 / h) if h > 0 else 0.0


# -------------------------
# Population baseline logic
# -------------------------

def pick_population_baselines(
    clusters: List[Tuple[str, float]],
    coverage_target: float,
    max_baselines: int,
) -> Tuple[List[str], float]:
    """
    clusters: [(cluster_id, share)] sorted desc
    """
    picked: List[str] = []
    cum = 0.0
    for cid, share in clusters:
        if share <= 0:
            continue
        picked.append(cid)
        cum += share
        if cum >= coverage_target or len(picked) >= max_baselines:
            break
    return picked, cum


def classify_population_shape(
    top1: float,
    top2: float,
    eff_n: float,
    high: float,
    medium: float,
) -> str:
    if top1 >= high:
        return "single_baseline"
    if top2 >= high or eff_n <= 2.5:
        return "multi_baseline"
    if top1 < medium and eff_n >= 3.0:
        return "no_stable_baseline"
    return "multi_baseline"


# -------------------------
# Main
# -------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain-clusters", required=True)
    ap.add_argument("--domain-authority", required=True)
    ap.add_argument("--run-config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--coverage-target", type=float, default=0.80)
    ap.add_argument("--max-baselines", type=int, default=3)
    args = ap.parse_args()

    clusters_csv = load_csv(Path(args.domain_clusters))
    authority_csv = load_csv(Path(args.domain_authority))
    run_cfg = load_json(Path(args.run_config))

    high = float(run_cfg.get("convergence_high", 0.8))
    medium = float(run_cfg.get("convergence_medium", 0.6))

    # Index authority rows by domain
    auth_by_domain: Dict[str, Dict[str, str]] = {
        r["domain"]: r for r in authority_csv
    }

    # Group clusters by domain
    clusters_by_domain: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for r in clusters_csv:
        dom = r["domain"]
        try:
            share = float(r["cluster_share"])
        except (TypeError, ValueError):
            continue
        if share <= 0:
            continue
        clusters_by_domain[dom].append((r["cluster_id"], share))

    results: List[Dict[str, Any]] = []

    for dom, clusters in clusters_by_domain.items():
        clusters.sort(key=lambda x: x[1], reverse=True)

        shares = [s for _, s in clusters]
        top1 = shares[0] if shares else 0.0
        top2 = (shares[0] + shares[1]) if len(shares) >= 2 else top1
        eff_n = effective_cluster_count(shares)

        baseline_ids, baseline_cov = pick_population_baselines(
            clusters,
            coverage_target=args.coverage_target,
            max_baselines=args.max_baselines,
        )

        auth = auth_by_domain.get(dom, {})
        authority_outcome = auth.get("authority_outcome", "unknown")

        if authority_outcome == "not_observable":
            pop_shape = "not_observable"
        else:
            pop_shape = classify_population_shape(
                top1=top1,
                top2=top2,
                eff_n=eff_n,
                high=high,
                medium=medium,
            )

        results.append(
            {
                "analysis_run_id": auth.get("analysis_run_id"),
                "domain": dom,
                "projects_total": auth.get("projects_total"),
                "projects_comparable": auth.get("projects_comparable"),
                "comparable_rate": auth.get("comparable_rate"),
                "cluster_count": len(clusters),
                "top1_share": round(top1, 6),
                "top2_share": round(top2, 6),
                "cluster_concentration_hhi": round(hhi(shares), 6),
                "effective_cluster_count": round(eff_n, 6),
                "population_baseline_count": len(baseline_ids),
                "population_baseline_ids": "|".join(baseline_ids),
                "population_baseline_coverage": round(baseline_cov, 6),
                "population_shape": pop_shape,
                "seed_match_rate": auth.get("seed_match_rate"),
                "authority_outcome_phase1": authority_outcome,
            }
        )

    # Sort for readability
    results.sort(key=lambda r: (r["population_shape"], r["domain"]))

    out_path = Path(args.out)
    write_csv(
        out_path,
        results,
        fieldnames=list(results[0].keys()) if results else [],
    )

    print(f"Wrote: {out_path}")
    print(f"Domains: {len(results)}")


if __name__ == "__main__":
    main()
