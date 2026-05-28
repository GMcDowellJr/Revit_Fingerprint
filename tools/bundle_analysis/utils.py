from __future__ import annotations

import time
from itertools import combinations
from typing import Dict, FrozenSet, List, Set, Tuple


def _supporting_files_by_superset(
    file_sets: Dict[str, FrozenSet[str]],
    itemset: FrozenSet[str],
) -> List[str]:
    return sorted([fid for fid, pset in file_sets.items() if pset.issuperset(itemset)])


def find_closed_itemsets(
    file_sets: Dict[str, FrozenSet[str]],
    min_support: int,
    min_bundle_size: int = 2,
) -> Tuple[List[Dict[str, object]], Dict[str, float]]:
    """Find closed frequent itemsets via pairwise-intersection candidate generation.

    Returns a tuple of (itemsets, timing_diag) where timing_diag contains internal
    timing diagnostics with keys candidate_gen_seconds, support_count_seconds,
    closure_prune_seconds, n_raw_candidates, n_support_candidates.
    """
    if min_support < 1:
        raise ValueError("min_support must be >= 1")
    if min_bundle_size < 1:
        raise ValueError("min_bundle_size must be >= 1")

    t0 = time.perf_counter()
    candidates: Set[FrozenSet[str]] = set()
    file_ids = sorted(file_sets.keys())
    for left, right in combinations(file_ids, 2):
        intersection = file_sets[left] & file_sets[right]
        if len(intersection) >= min_bundle_size:
            candidates.add(frozenset(intersection))
    t1 = time.perf_counter()

    n_raw_candidates = len(candidates)
    support_map: Dict[FrozenSet[str], int] = {}
    files_for_candidate: Dict[FrozenSet[str], FrozenSet[str]] = {}
    for cand in sorted(candidates, key=lambda s: (len(s), tuple(sorted(s)))):
        matched_files = frozenset(_supporting_files_by_superset(file_sets, cand))
        support = len(matched_files)
        if support >= min_support:
            support_map[cand] = support
            files_for_candidate[cand] = matched_files
    t2 = time.perf_counter()

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
    t3 = time.perf_counter()

    timing: Dict[str, float] = {
        "candidate_gen_seconds": t1 - t0,
        "support_count_seconds": t2 - t1,
        "closure_prune_seconds": t3 - t2,
        "n_raw_candidates": n_raw_candidates,
        "n_support_candidates": len(support_map),
    }

    return [
        {
            "pattern_ids": itemset,
            "files_present": support_map[itemset],
            "file_ids": files_for_candidate[itemset],
        }
        for itemset in closed_sets
    ], timing


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
    closed, _ = find_closed_itemsets(file_sets, min_support=min_support, min_bundle_size=min_bundle_size)
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
