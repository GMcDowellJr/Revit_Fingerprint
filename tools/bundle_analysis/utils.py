from __future__ import annotations

from collections import defaultdict
from typing import Dict, FrozenSet, List, Optional, Tuple


class _FPTreeNode:
    def __init__(self, item: Optional[str], parent: Optional["_FPTreeNode"]) -> None:
        self.item = item
        self.parent = parent
        self.count = 0
        self.children: Dict[str, _FPTreeNode] = {}
        self.link: Optional[_FPTreeNode] = None


def _build_fp_tree(
    transactions: List[Tuple[List[str], int]],
    min_support: int,
) -> Tuple[_FPTreeNode, Dict[str, int], Dict[str, _FPTreeNode]]:
    item_supports: Dict[str, int] = defaultdict(int)
    for items, weight in transactions:
        for item in items:
            item_supports[item] += weight

    frequent_items = {item: support for item, support in item_supports.items() if support >= min_support}
    root = _FPTreeNode(item=None, parent=None)
    header_heads: Dict[str, _FPTreeNode] = {}
    header_tails: Dict[str, _FPTreeNode] = {}

    if not frequent_items:
        return root, {}, {}

    for items, weight in transactions:
        filtered_items = [item for item in items if item in frequent_items]
        if not filtered_items:
            continue
        filtered_items.sort(key=lambda item: (-frequent_items[item], item))
        current = root
        for item in filtered_items:
            child = current.children.get(item)
            if child is None:
                child = _FPTreeNode(item=item, parent=current)
                current.children[item] = child
                if item not in header_heads:
                    header_heads[item] = child
                    header_tails[item] = child
                else:
                    header_tails[item].link = child
                    header_tails[item] = child
            child.count += weight
            current = child

    return root, frequent_items, header_heads


def _conditional_pattern_base(node: _FPTreeNode) -> List[Tuple[List[str], int]]:
    pattern_base: List[Tuple[List[str], int]] = []
    current = node
    while current is not None:
        path: List[str] = []
        parent = current.parent
        while parent is not None and parent.item is not None:
            path.append(parent.item)
            parent = parent.parent
        if path:
            path.reverse()
            pattern_base.append((path, current.count))
        current = current.link
    return pattern_base


def _mine_fp_tree(
    frequent_items: Dict[str, int],
    header_heads: Dict[str, _FPTreeNode],
    min_support: int,
    prefix: FrozenSet[str],
    support_map: Dict[FrozenSet[str], int],
) -> None:
    for item in sorted(frequent_items.keys(), key=lambda value: (frequent_items[value], value)):
        pattern = frozenset(set(prefix) | {item})
        support_map[pattern] = frequent_items[item]
        conditional_transactions = _conditional_pattern_base(header_heads[item])
        _, conditional_supports, conditional_headers = _build_fp_tree(conditional_transactions, min_support)
        if conditional_supports:
            _mine_fp_tree(conditional_supports, conditional_headers, min_support, pattern, support_map)


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
    """Find closed frequent itemsets via a pure-Python FP-Growth implementation."""
    if min_support < 1:
        raise ValueError("min_support must be >= 1")
    if min_bundle_size < 1:
        raise ValueError("min_bundle_size must be >= 1")

    if not file_sets:
        return []

    transaction_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    for patterns in file_sets.values():
        if patterns:
            transaction_counts[tuple(sorted(patterns))] += 1

    transactions = [(list(items), weight) for items, weight in transaction_counts.items()]
    if not transactions:
        return []

    support_map: Dict[FrozenSet[str], int] = {}
    _, frequent_items, header_heads = _build_fp_tree(transactions, min_support)
    if not frequent_items:
        return []
    _mine_fp_tree(frequent_items, header_heads, min_support, frozenset(), support_map)

    frequent_itemsets = {itemset: support for itemset, support in support_map.items() if len(itemset) >= min_bundle_size}
    if not frequent_itemsets:
        return []

    closed_sets: List[FrozenSet[str]] = []
    support_items = list(frequent_itemsets.items())
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
            "files_present": frequent_itemsets[itemset],
            "file_ids": frozenset(_supporting_files_by_superset(file_sets, itemset)),
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
