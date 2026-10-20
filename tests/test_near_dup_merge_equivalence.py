"""
Equivalence test for tools/label_synthesis/label_resolver.find_near_duplicate_merges.

The bucketed implementation must return a merge map identical (including
insertion order) to the original all-pairs scan, which is embedded verbatim
below as _reference.
"""
import random
import time
from typing import Any, Dict, List

from tools.label_synthesis.label_resolver import (
    NEAR_DUP_MIN_SIZE_RATIO,
    _are_near_duplicates,
    _extract_kv_typed,
    find_near_duplicate_merges,
)


# Verbatim copy of the pre-bucketing implementation (baseline f7a38f5).
def _reference(
    cluster_rows: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Detect near-duplicate clusters and return {smaller_jh -> canonical_jh}.

    Must be called with cluster_rows sorted by files_present DESC so that
    the canonical cluster is always the larger one (index i < j).

    Each cluster_row must contain:
        join_hash:       str
        files_present:   int
        identity_items:  List[{k, v, q}]  (from phase0_identity_items lookup)

    Two clusters are near-duplicates when:
        - Identical set of identity_item keys
        - Non-numeric values match exactly
        - Numeric values within NEAR_DUP_REL_TOL relative tolerance
        - Smaller cluster is >= NEAR_DUP_MIN_SIZE_RATIO * larger cluster size
          (prevents absorbing real outlier variants)
    """
    merge_map: Dict[str, str] = {}
    n = len(cluster_rows)

    for i in range(n):
        ci = cluster_rows[i]
        jh_i = ci.get("join_hash", "")
        if jh_i in merge_map:
            continue

        items_i = ci.get("identity_items") or []
        files_i = int(ci.get("files_present", 0))
        kv_i = _extract_kv_typed(items_i)
        if not kv_i:
            continue

        for j in range(i + 1, n):
            cj = cluster_rows[j]
            jh_j = cj.get("join_hash", "")
            if jh_j in merge_map:
                continue

            items_j = cj.get("identity_items") or []
            files_j = int(cj.get("files_present", 0))
            kv_j = _extract_kv_typed(items_j)
            if not kv_j:
                continue

            # Size ratio guard
            larger = max(files_i, files_j)
            smaller = min(files_i, files_j)
            if larger > 0 and (smaller / larger) < NEAR_DUP_MIN_SIZE_RATIO:
                continue

            if _are_near_duplicates(kv_i, kv_j):
                merge_map[jh_j] = jh_i   # j merges into i (i is larger/earlier)

    return merge_map


_STR_KEYS = ["s.alpha", "s.beta"]
_NUM_KEYS = ["n.width", "n.weight"]
_STR_VALUES = ["A", "B"]
_NUM_BASES = [1.0, 10.0, 250.0]


def _make_items(rng: random.Random) -> List[Dict[str, Any]]:
    keys = rng.sample(_STR_KEYS + _NUM_KEYS, rng.randint(0, 3))
    items = []
    for k in keys:
        if k in _STR_KEYS:
            v: Any = rng.choice(_STR_VALUES)
        else:
            base = rng.choice(_NUM_BASES)
            # Perturbations straddle the 2% tolerance so merges both happen and fail.
            v = round(base * (1.0 + rng.choice([0.0, 0.005, -0.01, 0.015, 0.03])), 6)
            if rng.random() < 0.3:
                v = str(v)
        q = "ok" if rng.random() < 0.85 else rng.choice(["missing", "unreadable"])
        items.append({"k": k, "v": v, "q": q})
    return items


def _make_clusters(rng: random.Random) -> List[Dict[str, Any]]:
    n = rng.randint(0, 120)
    hash_pool = [f"jh{h:03d}" for h in range(max(1, int(n * 0.8)))]
    clusters = []
    for _ in range(n):
        # Mix of near-equal and far-apart sizes so the size-ratio guard both passes and fails.
        files = rng.choice([1, 2, 5, 10, 20, 40, 100])
        clusters.append({
            "join_hash": rng.choice(hash_pool),  # repeats across clusters
            "files_present": files,
            "identity_items": _make_items(rng),
        })
    clusters.sort(key=lambda c: -c["files_present"])
    return clusters


def test_bucketed_merge_map_matches_reference():
    nonempty = 0
    guard_blocked = 0
    for seed in range(400):
        rng = random.Random(seed)
        clusters = _make_clusters(rng)
        expected = _reference(clusters)
        actual = find_near_duplicate_merges(clusters)
        assert actual == expected, f"seed={seed}"
        assert list(actual.items()) == list(expected.items()), f"seed={seed} order"
        if expected:
            nonempty += 1
        # Count pairs that would merge on content but are rejected by the size guard.
        kvs = [_extract_kv_typed(c["identity_items"]) for c in clusters]
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if kvs[i] and kvs[j] and _are_near_duplicates(kvs[i], kvs[j]):
                    a, b = clusters[i]["files_present"], clusters[j]["files_present"]
                    if min(a, b) / max(a, b) < NEAR_DUP_MIN_SIZE_RATIO:
                        guard_blocked += 1
    assert nonempty > 0
    assert guard_blocked > 0


def test_bucketed_scales_with_distinct_signatures():
    clusters = []
    for idx in range(1500):
        items = [{"k": f"n.{m}", "v": float(idx * 1000 + m), "q": "ok"} for m in range(149)]
        items.append({"k": "s.id", "v": f"cluster-{idx}", "q": "ok"})
        clusters.append({"join_hash": f"jh{idx}", "files_present": 1, "identity_items": items})
    t0 = time.perf_counter()
    result = find_near_duplicate_merges(clusters)
    assert time.perf_counter() - t0 < 5.0
    assert result == {}
