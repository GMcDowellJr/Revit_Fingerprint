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

    def _parse_size(value: str | None) -> int | None:
        if value is None:
            return None
        parsed = value.strip()
        if parsed == "":
            return None
        try:
            return int(parsed)
        except ValueError:
            return None

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
            if "raw_share" in rows[0] and rows[0]["raw_share"]:
                top = max(rows, key=lambda r: float(r.get("raw_share", 0)))
                rates[domain_dir.name] = float(top["raw_share"])
            elif "size" in rows[0]:
                parsed_sizes = [_parse_size(r.get("size")) for r in rows]
                sizes_available = all(size is not None for size in parsed_sizes)
                if sizes_available:
                    typed_sizes = [int(size) for size in parsed_sizes]
                    total = sum(typed_sizes)
                    if total > 0:
                        rates[domain_dir.name] = max(typed_sizes) / total
                    elif "percentage" in rows[0]:
                        top = max(rows, key=lambda r: float(r.get("percentage", 0)))
                        rates[domain_dir.name] = float(top["percentage"]) / 100.0
                elif "percentage" in rows[0]:
                    top = max(rows, key=lambda r: float(r.get("percentage", 0)))
                    rates[domain_dir.name] = float(top["percentage"]) / 100.0
            elif "percentage" in rows[0]:
                top = max(rows, key=lambda r: float(r.get("percentage", 0)))
                rates[domain_dir.name] = float(top["percentage"]) / 100.0
        except (ValueError, KeyError, ZeroDivisionError):
            continue
    return rates


def jenks_natural_breaks(values: List[float], n_classes: int) -> List[float]:
    """
    Compute Jenks natural breaks (Fisher-Jenks algorithm).
    Returns list of n_classes - 1 break values.

    Pure-Python Fisher-Jenks implementation suitable for small N.
    """
    if n_classes < 2:
        raise ValueError("n_classes must be >= 2")
    if len(values) < n_classes:
        raise ValueError(f"Need at least {n_classes} values. Got {len(values)}.")

    vals = sorted(float(v) for v in values)
    n = len(vals)

    if n == n_classes:
        return [vals[i] for i in range(1, n)]

    lower_class_limits = [[0] * (n_classes + 1) for _ in range(n + 1)]
    variance_combinations = [[float("inf")] * (n_classes + 1) for _ in range(n + 1)]

    for i in range(1, n_classes + 1):
        lower_class_limits[1][i] = 1
        variance_combinations[1][i] = 0.0
        for j in range(2, n + 1):
            variance_combinations[j][i] = float("inf")

    for l in range(2, n + 1):
        sum_ = 0.0
        sum_squares = 0.0
        w = 0

        for m in range(1, l + 1):
            lower_class_limit = l - m + 1
            val = vals[lower_class_limit - 1]

            w += 1
            sum_ += val
            sum_squares += val * val
            variance = sum_squares - (sum_ * sum_) / w

            if lower_class_limit != 1:
                for j in range(2, n_classes + 1):
                    test_variance = variance + variance_combinations[lower_class_limit - 1][j - 1]
                    if variance_combinations[l][j] >= test_variance:
                        lower_class_limits[l][j] = lower_class_limit
                        variance_combinations[l][j] = test_variance

        lower_class_limits[l][1] = 1
        variance_combinations[l][1] = variance

    breaks = [0.0] * (n_classes + 1)
    breaks[n_classes] = vals[-1]
    breaks[0] = vals[0]

    k = n
    for j in range(n_classes, 1, -1):
        idx = lower_class_limits[k][j] - 1
        breaks[j - 1] = vals[idx]
        k = lower_class_limits[k][j] - 1

    return sorted(breaks[1:-1])


def compute_thresholds(alignment_rates: Dict[str, float], n_classes: int = 3) -> Dict[str, float | int | str]:
    """
    Compute Jenks breaks on alignment rates.
    Returns threshold dict with stable_min and emerging_min.
    """
    if n_classes != 3:
        raise ValueError(
            f"compute_thresholds expects exactly 3 classes "
            f"(Fragmented/Emerging/Stable). Got n_classes={n_classes}."
        )

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
    parser.add_argument(
        "--n-classes",
        type=int,
        default=3,
        choices=[3],
        help="Number of classes. Must be 3 (Fragmented/Emerging/Stable).",
    )
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
