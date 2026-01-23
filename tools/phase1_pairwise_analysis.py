#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Tuple, Any


# -------------------------
# IO helpers
# -------------------------

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


# -------------------------
# Pairwise logic
# -------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-coverage", required=True,
                    help="baseline_coverage_by_project.csv")
    ap.add_argument("--out-dir", required=True,
                    help="output folder (same phase1 folder recommended)")
    args = ap.parse_args()

    rows = load_csv(Path(args.baseline_coverage))
    out_dir = Path(args.out_dir)

    # Group by domain
    by_domain: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        by_domain.setdefault(r["domain"], []).append(r)

    pairwise_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []

    for domain, records in sorted(by_domain.items()):
        # Keep only comparable (hash present)
        comparable = [
            r for r in records
            if r.get("cluster_id")
        ]

        total_projects = len(records)
        comparable_projects = len(comparable)

        if comparable_projects < 2:
            summary_rows.append(
                {
                    "domain": domain,
                    "projects_total": total_projects,
                    "projects_comparable": comparable_projects,
                    "pair_count": 0,
                    "identical_pairs": 0,
                    "divergent_pairs": 0,
                    "identical_pair_rate": None,
                    "authority_pairwise_signal": "not_observable",
                }
            )
            continue

        identical = 0
        divergent = 0
        pair_count = 0

        for a, b in combinations(comparable, 2):
            same = (a["cluster_id"] == b["cluster_id"])
            pair_count += 1
            if same:
                identical += 1
                distance = 0
            else:
                divergent += 1
                distance = 1

            pairwise_rows.append(
                {
                    "domain": domain,
                    "project_a": a["project_id"],
                    "project_b": b["project_id"],
                    "cluster_a": a["cluster_id"],
                    "cluster_b": b["cluster_id"],
                    "pair_distance": distance,
                }
            )

        identical_rate = identical / pair_count if pair_count else None

        # Pairwise authority signal (purely descriptive)
        if identical_rate is None:
            signal = "not_observable"
        elif identical_rate >= 0.80:
            signal = "strong_single_authority"
        elif identical_rate >= 0.50:
            signal = "partial_or_partitioned_authority"
        else:
            signal = "no_pairwise_authority"

        summary_rows.append(
            {
                "domain": domain,
                "projects_total": total_projects,
                "projects_comparable": comparable_projects,
                "pair_count": pair_count,
                "identical_pairs": identical,
                "divergent_pairs": divergent,
                "identical_pair_rate": round(identical_rate, 6)
                if identical_rate is not None else None,
                "authority_pairwise_signal": signal,
            }
        )

    # Write outputs
    write_csv(
        out_dir / "domain_pairwise_matrix.csv",
        pairwise_rows,
        [
            "domain",
            "project_a",
            "project_b",
            "cluster_a",
            "cluster_b",
            "pair_distance",
        ],
    )

    write_csv(
        out_dir / "domain_pairwise_summary.csv",
        summary_rows,
        [
            "domain",
            "projects_total",
            "projects_comparable",
            "pair_count",
            "identical_pairs",
            "divergent_pairs",
            "identical_pair_rate",
            "authority_pairwise_signal",
        ],
    )

    print(f"Wrote pairwise matrix: {out_dir / 'domain_pairwise_matrix.csv'}")
    print(f"Wrote summary:        {out_dir / 'domain_pairwise_summary.csv'}")


if __name__ == "__main__":
    main()
