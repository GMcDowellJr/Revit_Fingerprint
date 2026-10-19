#!/usr/bin/env python3
"""check_reference_target_regime.py

Determines, from a real reference_comparison_summary.csv, which side
(reference or target) is typically the bigger pattern set -- the thing
that decides whether a reference-anchored novelty measure (M1) or a
child-relative one (M2) saturates and loses discriminating power for
this pipeline. See the conversation this came out of: M1 = target_only/
union saturates near 1 when reference >> target; M2 = target_only/target
saturates near 1 when target >> reference. Neither is universally right;
this script tells you which regime this corpus's compare_reference.py
output actually lives in, per domain, so you can pick (or label both)
correctly instead of assuming.

Usage
-----
    python check_reference_target_regime.py \\
        --summary "C:\\...\\reference_comparison_summary.csv" \\
        --domain-column domain \\
        --ratio-threshold 2.0 \\
        --out-csv "C:\\...\\regime_report.csv"

--ratio-threshold controls what counts as "much bigger": a row is
classified reference-dominant if reference_pattern_count >= threshold *
target_pattern_count, target-dominant if the inverse holds, and
comparable otherwise. Default 2.0 (one side at least double the other) --
adjust if that's too loose/strict for what you're seeing.

Requires no third-party packages (stdlib csv only).
"""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional


def _to_float(value: str) -> Optional[float]:
    value = (value or "").strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def classify_regime(r: float, t: float, threshold: float) -> str:
    if r <= 0 and t <= 0:
        return "both_empty"
    if t == 0:
        return "reference_dominant"
    if r == 0:
        return "target_dominant"
    if r >= threshold * t:
        return "reference_dominant"
    if t >= threshold * r:
        return "target_dominant"
    return "comparable"


def compute_row_measures(row: Dict[str, str]) -> Optional[Dict[str, float]]:
    r = _to_float(row.get("reference_pattern_count", ""))
    t = _to_float(row.get("target_pattern_count", ""))
    s = _to_float(row.get("shared_count", ""))
    tgt_only = _to_float(row.get("target_only_count", ""))
    ref_only = _to_float(row.get("reference_only_count", ""))
    union = _to_float(row.get("union_count", ""))

    if r is None or t is None:
        return None
    # Derive whatever's missing from the others rather than requiring every
    # column -- compare_reference.py's _SUMMARY_FIELDNAMES has all of these,
    # but be defensive in case an older export or a blocked-domain
    # placeholder row is missing some.
    if s is None and tgt_only is not None:
        s = t - tgt_only
    if tgt_only is None and s is not None:
        tgt_only = t - s
    if ref_only is None and s is not None:
        ref_only = r - s
    if union is None and r is not None and tgt_only is not None:
        union = r + tgt_only

    if s is None or tgt_only is None or union is None or union <= 0 or t <= 0:
        return None

    m1_reference_anchored = _to_float(row.get("jaccard", ""))
    coverage = _to_float(row.get("reference_coverage_pct", ""))
    if m1_reference_anchored is not None and coverage not in (None, 0):
        # Reproduces DIVIDE(jaccard, reference_coverage_pct) = R/union directly
        # from the columns actually present, rather than recomputing R/union
        # by hand -- if this and the r/union hand computation disagree, that's
        # worth knowing too (printed as a warning below).
        m1_from_dax = m1_reference_anchored / coverage
    else:
        m1_from_dax = None
    m1_hand = r / union if union else None
    m2 = tgt_only / t if t else None

    return {
        "reference_pattern_count": r,
        "target_pattern_count": t,
        "shared_count": s,
        "target_only_count": tgt_only,
        "reference_only_count": ref_only,
        "union_count": union,
        "m1_reference_anchored": m1_hand,
        "m1_from_dax_columns": m1_from_dax,
        "m2_child_relative": m2,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", required=True, help="Path to reference_comparison_summary.csv")
    parser.add_argument("--domain-column", default="domain", help="Column identifying the domain/view (default: domain)")
    parser.add_argument("--ratio-threshold", type=float, default=2.0, help="Dominance threshold (default: 2.0x)")
    parser.add_argument("--out-csv", default=None, help="Optional path to write the full per-row regime+measures table")
    parser.add_argument(
        "--divergence-threshold",
        type=float,
        default=0.3,
        help="Flag rows where |M1 - M2| exceeds this (default: 0.3) as a 'measures disagree materially' count.",
    )
    args = parser.parse_args()

    summary_path = Path(args.summary)
    with summary_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if args.domain_column not in fieldnames:
        print(f"WARNING: '{args.domain_column}' not found in columns: {fieldnames}", file=sys.stderr)

    regime_counts: Dict[str, int] = defaultdict(int)
    per_domain_regime: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    ratios: List[float] = []
    m1_values: List[float] = []
    m2_values: List[float] = []
    disagreement_count = 0
    skipped = 0
    out_rows: List[Dict[str, object]] = []

    for row in rows:
        measures = compute_row_measures(row)
        if measures is None:
            skipped += 1
            continue

        r = measures["reference_pattern_count"]
        t = measures["target_pattern_count"]
        regime = classify_regime(r, t, args.ratio_threshold)
        regime_counts[regime] += 1

        domain = (row.get(args.domain_column, "") or "").strip() or "(unknown)"
        per_domain_regime[domain][regime] += 1

        if t > 0:
            ratios.append(r / t)

        m1 = measures["m1_reference_anchored"]
        m2 = measures["m2_child_relative"]
        if m1 is not None and m2 is not None:
            m1_values.append(m1)
            m2_values.append(m2)
            if abs(m1 - m2) >= args.divergence_threshold:
                disagreement_count += 1

        # Cross-check: does the hand-derived M1 (R/union from raw counts)
        # agree with M1 reconstructed from the actual jaccard/coverage
        # columns as the real DAX measure would compute it? If not, the
        # summary CSV's own columns are internally inconsistent for this
        # row -- worth knowing before trusting either.
        m1_dax = measures["m1_from_dax_columns"]
        if m1 is not None and m1_dax is not None and abs(m1 - m1_dax) > 0.01:
            print(
                f"  [inconsistency] domain={domain} row: hand R/union={m1:.4f} vs "
                f"jaccard/coverage={m1_dax:.4f} -- differ by {abs(m1 - m1_dax):.4f}",
                file=sys.stderr,
            )

        out_rows.append(
            {
                "domain": domain,
                "regime": regime,
                "reference_pattern_count": r,
                "target_pattern_count": t,
                "m1_reference_anchored": m1,
                "m2_child_relative": m2,
                "abs_difference": (abs(m1 - m2) if (m1 is not None and m2 is not None) else ""),
            }
        )

    total = sum(regime_counts.values())
    print(f"Rows parsed: {len(rows)}  |  usable rows: {total}  |  skipped (missing/blocked): {skipped}\n")

    print("Overall regime distribution:")
    for regime in ("reference_dominant", "target_dominant", "comparable", "both_empty"):
        n = regime_counts.get(regime, 0)
        pct = (100.0 * n / total) if total else 0.0
        print(f"  {regime:20s} {n:6d}  ({pct:5.1f}%)")
    print()

    if ratios:
        print("reference_pattern_count / target_pattern_count ratio:")
        print(f"  median: {statistics.median(ratios):.3f}")
        print(f"  mean:   {statistics.mean(ratios):.3f}")
        if len(ratios) > 1:
            print(f"  stdev:  {statistics.stdev(ratios):.3f}")
        print(f"  min:    {min(ratios):.3f}   max: {max(ratios):.3f}")
        print("  (ratio >> 1 means reference-dominant regime overall -- M1 likely saturates;")
        print("   ratio << 1 means target-dominant overall -- M2 likely saturates instead)\n")

    print("Per-domain regime breakdown (only domains with >1 dominant regime shown in full;")
    print("this is where a single corpus-wide choice of M1 vs M2 could be wrong for some domains):")
    for domain in sorted(per_domain_regime):
        counts = per_domain_regime[domain]
        dom_total = sum(counts.values())
        dominant = max(counts, key=counts.get)
        dominant_pct = 100.0 * counts[dominant] / dom_total if dom_total else 0.0
        flag = "" if dominant_pct >= 90 else "  <-- MIXED REGIME, check before choosing one measure"
        print(f"  {domain:40s} n={dom_total:4d}  dominant={dominant:20s} ({dominant_pct:5.1f}%){flag}")
    print()

    if m1_values and m2_values:
        print(f"M1 (reference-anchored) distribution: median={statistics.median(m1_values):.3f}, "
              f"mean={statistics.mean(m1_values):.3f}")
        print(f"M2 (child-relative)     distribution: median={statistics.median(m2_values):.3f}, "
              f"mean={statistics.mean(m2_values):.3f}")
        pct_disagree = 100.0 * disagreement_count / len(m1_values)
        print(f"\nRows where |M1 - M2| >= {args.divergence_threshold}: "
              f"{disagreement_count} / {len(m1_values)} ({pct_disagree:.1f}%)")
        print("(a high percentage here means the choice of novelty formula materially changes")
        print(" the reported value for most of the corpus, not just edge cases -- exactly what")
        print(" the cross_segment cascade check found)\n")

    if args.out_csv:
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames_out = ["domain", "regime", "reference_pattern_count", "target_pattern_count",
                           "m1_reference_anchored", "m2_child_relative", "abs_difference"]
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames_out)
            writer.writeheader()
            writer.writerows(out_rows)
        print(f"Full per-row table written to: {out_path}")


if __name__ == "__main__":
    main()
