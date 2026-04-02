from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    _TOOLS_DIR = _THIS_DIR.parent
    if str(_TOOLS_DIR) not in sys.path:
        sys.path.insert(0, str(_TOOLS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, compute_effective_support, make_bundle_id, read_csv_rows
    from compute_governance_thresholds import jenks_natural_breaks
    from utils import find_closed_itemsets
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, compute_effective_support, make_bundle_id, read_csv_rows
    from ..compute_governance_thresholds import jenks_natural_breaks
    from .utils import find_closed_itemsets


# Scale note: at larger corpus sizes replace pairwise intersection candidate generation
# with FP-Growth closed-itemset mining while preserving I/O interfaces.
EXPECTED_MULTIPLIER = 2.0


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * percentile
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return float(sorted_values[lower])
    lower_value = float(sorted_values[lower])
    upper_value = float(sorted_values[upper])
    weight = rank - lower
    return lower_value + (upper_value - lower_value) * weight


def compute_auto_threshold(
    file_sets: Dict[str, frozenset],
    files_total: int,
) -> Dict[str, Any]:
    if files_total <= 0:
        raise ValueError(f"files_total must be > 0. Got {files_total}.")

    pattern_presence_counts: Dict[str, int] = defaultdict(int)
    for pattern_ids in file_sets.values():
        for pattern_id in pattern_ids:
            pattern_presence_counts[pattern_id] += 1

    pattern_ids_sorted = sorted(pattern_presence_counts.keys())
    expected_values: List[float] = []
    # SCALE NOTE: O(P^2) pairwise loop. At thousands of patterns per domain,
    # consider switching to sparse matrix co-occurrence or sampling-based
    # approximation. Acceptable at v21 scale (< 1000 patterns per domain).
    for idx, pattern_i in enumerate(pattern_ids_sorted):
        count_i = pattern_presence_counts[pattern_i]
        for pattern_j in pattern_ids_sorted[idx + 1 :]:
            count_j = pattern_presence_counts[pattern_j]
            expected_values.append((count_i * count_j) / files_total)

    cooccurrence_p90 = _percentile(expected_values, 0.90)
    expected_floor = max(2, int(math.ceil(cooccurrence_p90 * EXPECTED_MULTIPLIER)))
    expected_method_detail = (
        f"expected_floor=ceil(p90_expected_cooccurrence*{EXPECTED_MULTIPLIER:.1f}); "
        f"p90={cooccurrence_p90:.6f}; expected_pairs={len(expected_values)}"
    )

    eligible_pattern_ids = sorted([pid for pid, count in pattern_presence_counts.items() if count >= 2])
    cooccurrence_values: List[int] = []
    cooccurrence_distribution: Dict[int, int] = defaultdict(int)
    # SCALE NOTE: O(P^2) pairwise loop. At thousands of patterns per domain,
    # consider switching to sparse matrix co-occurrence or sampling-based
    # approximation. Acceptable at v21 scale (< 1000 patterns per domain).
    for idx, pattern_i in enumerate(eligible_pattern_ids):
        for pattern_j in eligible_pattern_ids[idx + 1 :]:
            cooccurrence = 0
            for pattern_set in file_sets.values():
                if pattern_i in pattern_set and pattern_j in pattern_set:
                    cooccurrence += 1
            if cooccurrence >= 2:
                cooccurrence_values.append(cooccurrence)
                cooccurrence_distribution[cooccurrence] += 1

    distinct_cooccurrence_values = sorted(set(cooccurrence_values))
    if len(distinct_cooccurrence_values) < 4:
        natural_breaks_floor = expected_floor
        natural_breaks_method_detail = (
            "fallback_to_expected_floor_due_to_insufficient_distinct_values;"
            f" distinct_values={len(distinct_cooccurrence_values)}"
        )
    else:
        breaks = sorted(jenks_natural_breaks([float(v) for v in cooccurrence_values], 3))
        natural_breaks_floor = max(2, int(math.ceil(breaks[1])))
        natural_breaks_method_detail = (
            f"natural_breaks_floor=ceil(jenks_break_1); breaks={','.join(f'{b:.6f}' for b in breaks)}"
        )

    return {
        "cooccurrence_p90": cooccurrence_p90,
        "expected_floor": expected_floor,
        "natural_breaks_floor": natural_breaks_floor,
        "chosen": natural_breaks_floor,
        "method": "natural_breaks_primary_expected_secondary",
        "expected_method_detail": expected_method_detail,
        "natural_breaks_method_detail": natural_breaks_method_detail,
        "cooccurrence_count": len(cooccurrence_values),
        "cooccurrence_distribution": dict(sorted(cooccurrence_distribution.items())),
    }


def find_bundles_for_domain(out_dir: Path, domain: str, min_support_count: int = 3, min_support_pct: float = 0.0) -> Dict[str, int]:
    domain_out_dir = out_dir / domain
    membership_rows = read_csv_rows(domain_out_dir / "membership_matrix.csv")
    scope_rows = read_csv_rows(domain_out_dir / "scope_registry.csv")
    if not membership_rows and not scope_rows:
        return {"bundles": 0, "edges": 0}

    analysis_run_id = (membership_rows[0] if membership_rows else scope_rows[0]).get("analysis_run_id", "")
    file_sets_by_scope: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in membership_rows:
        file_sets_by_scope[row.get("scope_key", "")][row.get("export_run_id", "")].add(row.get("pattern_id", ""))

    files_total_by_scope = {r.get("scope_key", ""): int((r.get("files_in_scope", "0") or "0")) for r in scope_rows}

    bundles_rows: List[Dict[str, str]] = []
    bundle_membership_rows: List[Dict[str, str]] = []
    bundle_file_rows: List[Dict[str, str]] = []
    threshold_rows: List[Dict[str, str]] = []

    for scope_key in sorted(files_total_by_scope.keys() | file_sets_by_scope.keys()):
        file_sets = {k: frozenset(v) for k, v in file_sets_by_scope.get(scope_key, {}).items() if len(v) >= 2}
        files_total = files_total_by_scope.get(scope_key, len(file_sets))
        patterns_in_scope = len({pid for pset in file_sets.values() for pid in pset})

        threshold_result: Dict[str, Any]
        try:
            threshold_result = compute_auto_threshold(file_sets, files_total)
            required_keys = {
                "cooccurrence_p90",
                "expected_floor",
                "natural_breaks_floor",
                "chosen",
                "method",
                "expected_method_detail",
                "natural_breaks_method_detail",
                "cooccurrence_count",
                "cooccurrence_distribution",
            }
            missing_keys = sorted(required_keys - set(threshold_result.keys()))
            if missing_keys:
                raise ValueError(f"missing threshold result keys: {missing_keys}")
            print(
                f"[step2_threshold] domain={domain} scope={scope_key!r} "
                f"expected_floor={threshold_result['expected_floor']} "
                f"natural_breaks_floor={threshold_result['natural_breaks_floor']} "
                f"chosen={threshold_result['chosen']} cli_floor={min_support_count} "
                f"effective={max(min_support_count, int(threshold_result['chosen']))}"
            )
        except Exception as exc:
            reason = str(exc)
            print(
                f"[step2_threshold_fallback] domain={domain} scope={scope_key!r} "
                f"reason={reason} using_cli_floor={min_support_count}"
            )
            threshold_result = {
                "cooccurrence_p90": 0.0,
                "expected_floor": max(2, min_support_count),
                "natural_breaks_floor": max(2, min_support_count),
                "chosen": max(2, min_support_count),
                "method": "fallback_exception",
                "expected_method_detail": "fallback_to_cli_floor_due_to_threshold_exception",
                "natural_breaks_method_detail": reason[:200],
                "cooccurrence_count": 0,
                "cooccurrence_distribution": {},
            }

        effective_min_support = max(2, min_support_count, int(threshold_result["chosen"]))
        threshold_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": domain,
                "scope_key": scope_key,
                "files_in_scope": str(files_total),
                "patterns_in_scope": str(patterns_in_scope),
                "cooccurrence_pairs": str(threshold_result["cooccurrence_count"]),
                "cooccurrence_p90": f"{float(threshold_result['cooccurrence_p90']):.6f}",
                "expected_floor": str(threshold_result["expected_floor"]),
                "natural_breaks_floor": str(threshold_result["natural_breaks_floor"]),
                "chosen_auto_threshold": str(threshold_result["chosen"]),
                "cli_floor": str(min_support_count),
                "effective_threshold": str(effective_min_support),
                "derivation_method": str(threshold_result["method"]),
                "expected_method_detail": str(threshold_result["expected_method_detail"]),
                "natural_breaks_method_detail": str(threshold_result["natural_breaks_method_detail"]),
                "cooccurrence_histogram": json.dumps(
                    {str(k): v for k, v in dict(threshold_result["cooccurrence_distribution"]).items()},
                    sort_keys=True,
                ),
            }
        )

        if len(file_sets) < 2:
            print(
                f"[step2] domain={domain} scope={scope_key!r} bundles_found=0 patterns_covered=0 files_covered=0 "
                f"effective_threshold={effective_min_support}"
            )
            continue

        effective_support = compute_effective_support(files_total, effective_min_support, min_support_pct)
        closed_itemsets = find_closed_itemsets(file_sets, min_support=effective_support, min_bundle_size=2)

        closed_itemsets.sort(
            key=lambda item: (
                -int(item["files_present"]),
                make_bundle_id(domain, scope_key, sorted(item["pattern_ids"])),
            )
        )
        for rank, item in enumerate(closed_itemsets, start=1):
            itemset = item["pattern_ids"]
            pattern_ids_sorted = sorted(itemset)
            bundle_id = make_bundle_id(domain, scope_key, pattern_ids_sorted)
            files_present = int(item["files_present"])
            if len(itemset) < 2:
                raise ValueError("Invalid bundle with <2 patterns encountered")
            if files_present < effective_support:
                raise ValueError("Invalid bundle below support threshold encountered")
            bundles_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": analysis_run_id,
                    "domain": domain,
                    "scope_key": scope_key,
                    "bundle_id": bundle_id,
                    "pattern_ids": "|".join(pattern_ids_sorted),
                    "pattern_count": str(len(pattern_ids_sorted)),
                    "files_present": str(files_present),
                    "files_total": str(files_total),
                    "support_pct": f"{(100.0 * files_present / files_total) if files_total else 0.0:.6f}",
                    "bundle_rank": str(rank),
                }
            )
            for pid in pattern_ids_sorted:
                bundle_membership_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": analysis_run_id,
                        "domain": domain,
                        "scope_key": scope_key,
                        "bundle_id": bundle_id,
                        "pattern_id": pid,
                    }
                )
            for export_run_id in sorted(item["file_ids"]):
                bundle_file_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": analysis_run_id,
                        "domain": domain,
                        "scope_key": scope_key,
                        "bundle_id": bundle_id,
                        "export_run_id": export_run_id,
                    }
                )

        patterns_covered = len({p for item in closed_itemsets for p in item["pattern_ids"]})
        files_covered = len({f for item in closed_itemsets for f in item["file_ids"]})
        print(
            f"[step2] domain={domain} scope={scope_key!r} bundles_found={len(closed_itemsets)} patterns_covered={patterns_covered} "
            f"files_covered={files_covered} effective_threshold={effective_min_support}"
        )

    key_set = set()
    for row in bundles_rows:
        uniq = (row["domain"], row["scope_key"], row["bundle_id"])
        if uniq in key_set:
            raise ValueError(f"Duplicate bundle_id in scope: {uniq}")
        key_set.add(uniq)

    bundles_rows.sort(key=lambda r: (r["domain"], r["scope_key"], -int(r["files_present"]), r["bundle_id"]))
    bundle_membership_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["bundle_id"], r["pattern_id"]))
    bundle_file_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["bundle_id"], r["export_run_id"]))

    atomic_write_csv(
        domain_out_dir / "bundles.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "bundle_id",
            "pattern_ids",
            "pattern_count",
            "files_present",
            "files_total",
            "support_pct",
            "bundle_rank",
        ],
        bundles_rows,
    )
    atomic_write_csv(
        domain_out_dir / "bundle_membership.csv",
        ["schema_version", "analysis_run_id", "domain", "scope_key", "bundle_id", "pattern_id"],
        bundle_membership_rows,
    )
    atomic_write_csv(
        domain_out_dir / "bundle_file_membership.csv",
        ["schema_version", "analysis_run_id", "domain", "scope_key", "bundle_id", "export_run_id"],
        bundle_file_rows,
    )
    threshold_rows.sort(key=lambda r: (r["analysis_run_id"], r["domain"], r["scope_key"]))
    atomic_write_csv(
        domain_out_dir / "bundle_analysis_thresholds.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "files_in_scope",
            "patterns_in_scope",
            "cooccurrence_pairs",
            "cooccurrence_p90",
            "expected_floor",
            "natural_breaks_floor",
            "chosen_auto_threshold",
            "cli_floor",
            "effective_threshold",
            "derivation_method",
            "expected_method_detail",
            "natural_breaks_method_detail",
            "cooccurrence_histogram",
        ],
        threshold_rows,
    )

    return {"bundles": len(bundles_rows), "files_with_bundles": len({r['export_run_id'] for r in bundle_file_rows})}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Find closed frequent itemset bundles")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    p.add_argument("--min-support-count", type=int, default=3)
    p.add_argument("--min-support-pct", type=float, default=0.0)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    find_bundles_for_domain(args.out_dir, args.domain, args.min_support_count, args.min_support_pct)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
