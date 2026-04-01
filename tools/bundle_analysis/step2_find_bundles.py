from __future__ import annotations

import argparse
import itertools
from collections import defaultdict
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Set

from .common import SCHEMA_VERSION, atomic_write_csv, compute_effective_support, make_bundle_id, read_csv_rows


# Scale note: at larger corpus sizes replace pairwise intersection candidate generation
# with FP-Growth closed-itemset mining while preserving I/O interfaces.


def _supporting_files_by_superset(
    file_sets: Dict[str, FrozenSet[str]],
    itemset: FrozenSet[str],
) -> List[str]:
    """Return files whose pattern set is a superset of ``itemset``.

    Important: support is not the number of file pairs that generated ``itemset``.
    """
    return sorted([fid for fid, pset in file_sets.items() if pset.issuperset(itemset)])


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

    for scope_key in sorted(files_total_by_scope.keys() | file_sets_by_scope.keys()):
        file_sets = {k: frozenset(v) for k, v in file_sets_by_scope.get(scope_key, {}).items() if len(v) >= 2}
        files_total = files_total_by_scope.get(scope_key, len(file_sets))
        if len(file_sets) < 2:
            continue

        effective_support = compute_effective_support(files_total, min_support_count, min_support_pct)
        candidates: Set[FrozenSet[str]] = set()
        file_ids = sorted(file_sets.keys())
        for a, b in itertools.combinations(file_ids, 2):
            intersection = file_sets[a] & file_sets[b]
            if len(intersection) >= 2:
                candidates.add(frozenset(intersection))

        support_map: Dict[FrozenSet[str], int] = {}
        files_for_candidate: Dict[FrozenSet[str], List[str]] = {}
        for cand in sorted(candidates, key=lambda s: (len(s), tuple(sorted(s)))):
            matched_files = _supporting_files_by_superset(file_sets, cand)
            support = len(matched_files)
            if support >= effective_support:
                support_map[cand] = support
                files_for_candidate[cand] = sorted(matched_files)

        closed_sets: List[FrozenSet[str]] = []
        support_items = list(support_map.items())
        for s, s_support in support_items:
            is_closed = True
            for other, other_support in support_items:
                if len(other) <= len(s):
                    continue
                if s.issubset(other) and s_support == other_support:
                    is_closed = False
                    break
            if is_closed:
                closed_sets.append(s)

        closed_sets.sort(key=lambda s: (-support_map[s], make_bundle_id(domain, scope_key, sorted(s))))
        for rank, itemset in enumerate(closed_sets, start=1):
            pattern_ids_sorted = sorted(itemset)
            bundle_id = make_bundle_id(domain, scope_key, pattern_ids_sorted)
            files_present = support_map[itemset]
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
            for export_run_id in files_for_candidate[itemset]:
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

        patterns_covered = len({p for s in closed_sets for p in s})
        files_covered = len({f for s in closed_sets for f in files_for_candidate.get(s, [])})
        print(
            f"[step2] domain={domain} scope={scope_key!r} bundles_found={len(closed_sets)} patterns_covered={patterns_covered} files_covered={files_covered}"
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
