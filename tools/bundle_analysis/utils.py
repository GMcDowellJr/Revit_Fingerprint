from __future__ import annotations

from itertools import combinations
from typing import Dict, FrozenSet, List, Set, Tuple

try:
    import pandas as pd
except ImportError:  # pragma: no cover - fallback path for minimal environments
    pd = None

try:
    from mlxtend.frequent_patterns import fpgrowth
except ImportError:  # pragma: no cover - fallback path for minimal environments
    fpgrowth = None


def _supporting_files_by_superset(
    file_sets: Dict[str, FrozenSet[str]],
    itemset: FrozenSet[str],
) -> List[str]:
    return sorted([fid for fid, pset in file_sets.items() if pset.issuperset(itemset)])


def find_closed_itemsets(
    file_sets: Dict[str, FrozenSet[str]],
    min_support: int,
    min_bundle_size: int = 2,
) -> List[Dict[str, object]]:
    """Find closed frequent itemsets via FP-Growth with graceful fallback."""
    if min_support < 1:
        raise ValueError("min_support must be >= 1")
    if min_bundle_size < 1:
        raise ValueError("min_bundle_size must be >= 1")

    if not file_sets:
        return []

    if fpgrowth is None or pd is None:
        print("[warn] mlxtend/pandas unavailable; using pairwise closed-itemset fallback")
        return _find_closed_itemsets_pairwise(file_sets, min_support, min_bundle_size)

    file_ids = sorted(file_sets.keys())
    transactions = [sorted(file_sets[file_id]) for file_id in file_ids]
    if not transactions:
        return []

    all_items = sorted({item for txn in transactions for item in txn})
    if not all_items:
        return []

    one_hot_df = pd.DataFrame(False, index=range(len(transactions)), columns=all_items)
    for row_idx, txn in enumerate(transactions):
        if txn:
            one_hot_df.loc[row_idx, txn] = True

    min_support_fraction = float(min_support) / float(len(transactions))
    frequent_itemsets = fpgrowth(one_hot_df, min_support=min_support_fraction, use_colnames=True)
    if frequent_itemsets.empty:
        return []

    support_map: Dict[FrozenSet[str], int] = {}
    for _, row in frequent_itemsets.iterrows():
        itemset = frozenset(str(item) for item in row["itemsets"])
        if len(itemset) < min_bundle_size:
            continue
        support_count = int(round(float(row["support"]) * len(transactions)))
        if support_count >= min_support:
            support_map[itemset] = support_count

    if not support_map:
        return []

    closed_sets: List[FrozenSet[str]] = []
    support_items = list(support_map.items())
    for itemset, itemset_support in support_items:
        is_closed = True
        for other, other_support in support_items:
            if len(other) <= len(itemset):
                continue
            if itemset.issubset(other) and itemset_support == other_support:
                is_closed = False
                break
        if is_closed:
            closed_sets.append(itemset)

    closed_sets.sort(key=lambda s: (len(s), tuple(sorted(s))))

    return [
        {
            "pattern_ids": itemset,
            "files_present": support_map[itemset],
            "file_ids": frozenset(_supporting_files_by_superset(file_sets, itemset)),
        }
        for itemset in closed_sets
    ]


def _find_closed_itemsets_pairwise(
    file_sets: Dict[str, FrozenSet[str]],
    min_support: int,
    min_bundle_size: int,
) -> List[Dict[str, object]]:
    """Legacy pairwise-intersection closed itemset finder."""

    candidates: Set[FrozenSet[str]] = set()
    file_ids = sorted(file_sets.keys())
    for left, right in combinations(file_ids, 2):
        intersection = file_sets[left] & file_sets[right]
        if len(intersection) >= min_bundle_size:
            candidates.add(frozenset(intersection))

    support_map: Dict[FrozenSet[str], int] = {}
    files_for_candidate: Dict[FrozenSet[str], FrozenSet[str]] = {}
    for cand in sorted(candidates, key=lambda s: (len(s), tuple(sorted(s)))):
        matched_files = frozenset(_supporting_files_by_superset(file_sets, cand))
        support = len(matched_files)
        if support >= min_support:
            support_map[cand] = support
            files_for_candidate[cand] = matched_files

    closed_sets: List[FrozenSet[str]] = []
    support_items: List[Tuple[FrozenSet[str], int]] = list(support_map.items())
    for itemset, itemset_support in support_items:
        is_closed = True
        for other, other_support in support_items:
            if len(other) <= len(itemset):
                continue
            if itemset.issubset(other) and itemset_support == other_support:
                is_closed = False
                break
        if is_closed:
            closed_sets.append(itemset)

    return [
        {
            "pattern_ids": itemset,
            "files_present": support_map[itemset],
            "file_ids": files_for_candidate[itemset],
        }
        for itemset in closed_sets
    ]


def find_root_bundles(
    file_sets: Dict[str, FrozenSet[str]],
    min_support: int,
    min_bundle_size: int = 2,
) -> List[Dict[str, object]]:
    """
    Lightweight closed frequent itemset finder returning only root bundles
    (itemsets not contained in any other itemset with equal or higher support).

    Returns list of dicts with keys:
      pattern_ids: frozenset
      files_present: int
      file_ids: frozenset
    """
    closed = find_closed_itemsets(file_sets, min_support=min_support, min_bundle_size=min_bundle_size)
    roots: List[Dict[str, object]] = []
    for candidate in closed:
        candidate_patterns = candidate["pattern_ids"]
        candidate_support = int(candidate["files_present"])
        is_root = True
        for other in closed:
            if other is candidate:
                continue
            other_patterns = other["pattern_ids"]
            if len(other_patterns) <= len(candidate_patterns):
                continue
            if candidate_patterns.issubset(other_patterns) and int(other["files_present"]) >= candidate_support:
                is_root = False
                break
        if is_root:
            roots.append(candidate)

    roots.sort(
        key=lambda r: (
            -int(r["files_present"]),
            -len(r["pattern_ids"]),
            tuple(sorted(r["pattern_ids"])),
        )
    )
    return roots
