#!/usr/bin/env python3
"""
Compute governance thresholds for the Fingerprint BI model.

Reads split-analysis cluster summaries, computes Jenks natural breaks on
alignment rates (leading cluster share per domain), and emits thresholds.csv.

Usage:
    python -m tools.compute_governance_thresholds \
        --split-root <path/to/split_analysis> \
        --out <path/to/thresholds.csv>
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List


SCHEMA_VERSION = "2.1.0"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def compute_alignment_rates(split_root: Path) -> Dict[str, float]:
    """
    Compute alignment rate per domain as leading cluster share.
    Reads cluster_summary.csv for each domain found under split_root.
    Returns {domain: leading_cluster_share_pct_as_fraction}.
    """
    rates: Dict[str, float] = {}
    for domain_dir in sorted(split_root.iterdir(), key=lambda p: p.name.lower()):
        if not domain_dir.is_dir():
            continue
        summary_csv = domain_dir / "file_level" / f"{domain_dir.name}.cluster_summary.csv"
        if not summary_csv.exists():
            continue
        rows = _read_csv(summary_csv)
        if not rows:
            continue
        try:
            top = max(rows, key=lambda r: float(r.get("percentage", 0)))
            rates[domain_dir.name] = float(top["percentage"]) / 100.0
        except (ValueError, KeyError):
            continue
    return rates


def jenks_natural_breaks(values: List[float], n_classes: int) -> List[float]:
    """
    Compute Jenks natural breaks (Fisher-Jenks algorithm).
    Returns list of n_classes - 1 break values.

    Uses jenkspy if available, otherwise falls back to a pure-Python
    implementation suitable for small N.
    """
    if n_classes < 2:
        raise ValueError("n_classes must be >= 2")
    if len(values) < n_classes:
        raise ValueError(f"Need at least {n_classes} values. Got {len(values)}.")

    try:
        import jenkspy

        breaks = jenkspy.jenks_breaks(values, n_classes=n_classes)
        return sorted(float(v) for v in breaks[1:-1])
    except ImportError:
        pass

    vals = sorted(float(v) for v in values)
    n = len(vals)

    if n <= n_classes:
        return [vals[i] for i in range(1, n)]

    def ssd(start: int, end: int) -> float:
        sub = vals[start:end]
        mean = sum(sub) / len(sub)
        return sum((x - mean) ** 2 for x in sub)

    mat = [[float("inf")] * (n_classes + 1) for _ in range(n + 1)]
    split = [[0] * (n_classes + 1) for _ in range(n + 1)]

    for i in range(1, n + 1):
        mat[i][1] = ssd(0, i)
        split[i][1] = 0

    for k in range(2, n_classes + 1):
        for i in range(k, n + 1):
            best = float("inf")
            best_j = k - 1
            for j in range(k - 1, i):
                cost = mat[j][k - 1] + ssd(j, i)
                if cost < best:
                    best = cost
                    best_j = j
            mat[i][k] = best
            split[i][k] = best_j

    breaks_idx: List[int] = []
    k = n_classes
    i = n
    while k > 1:
        j = split[i][k]
        breaks_idx.append(j)
        i = j
        k -= 1

    return [vals[idx] for idx in sorted(breaks_idx)]


def compute_thresholds(alignment_rates: Dict[str, float], n_classes: int = 3) -> Dict[str, float | int | str]:
    """
    Compute Jenks breaks on alignment rates.
    Returns threshold dict with stable_min and emerging_min.
    """
    if len(alignment_rates) < n_classes:
        raise ValueError(
            f"Need at least {n_classes} domains to compute {n_classes}-class Jenks breaks. "
            f"Got {len(alignment_rates)}."
        )

    values = list(alignment_rates.values())
    breaks = jenks_natural_breaks(values, n_classes)
    breaks_sorted = sorted(breaks)

    return {
        "emerging_min": round(float(breaks_sorted[0]), 4),
        "stable_min": round(float(breaks_sorted[1]), 4),
        "n_domains": len(values),
        "n_classes": n_classes,
        "algorithm": "jenks_natural_breaks",
        "value_min": round(min(values), 4),
        "value_max": round(max(values), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute governance thresholds via Jenks natural breaks")
    parser.add_argument("--split-root", required=True, help="Root directory of split_analysis outputs")
    parser.add_argument("--out", required=True, help="Output path for thresholds.csv")
    parser.add_argument("--n-classes", type=int, default=3, help="Number of classes (default: 3)")
    args = parser.parse_args()

    split_root = Path(args.split_root)
    out_path = Path(args.out)

    print(f"[INFO] Reading alignment rates from {split_root}")
    rates = compute_alignment_rates(split_root)
    print(f"[INFO] Found {len(rates)} domains")
    for domain, rate in sorted(rates.items(), key=lambda kv: kv[1]):
        print(f"  {domain}: {rate:.4f}")

    thresholds = compute_thresholds(rates, n_classes=args.n_classes)
    print("\n[INFO] Jenks breaks:")
    print(f"  Fragmented -> Emerging at: {thresholds['emerging_min']}")
    print(f"  Emerging -> Stable at:     {thresholds['stable_min']}")

    rows = [{
        "schema_version": SCHEMA_VERSION,
        "algorithm": str(thresholds["algorithm"]),
        "n_domains": str(thresholds["n_domains"]),
        "n_classes": str(thresholds["n_classes"]),
        "stable_min": str(thresholds["stable_min"]),
        "emerging_min": str(thresholds["emerging_min"]),
        "value_min": str(thresholds["value_min"]),
        "value_max": str(thresholds["value_max"]),
    }]

    fieldnames = [
        "schema_version",
        "algorithm",
        "n_domains",
        "n_classes",
        "stable_min",
        "emerging_min",
        "value_min",
        "value_max",
    ]
    _write_csv(out_path, fieldnames, rows)
    print(f"\n[INFO] Written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
