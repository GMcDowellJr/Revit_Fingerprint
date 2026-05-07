from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple

from .index import DomainIndex


@dataclass
class ChangeCounts:
    domain: str
    baseline_file_id: str
    other_file_id: str

    added: int
    removed: int
    same: int
    modified: int

    ambiguous_duplicates: int
    ambiguous_bad_items: int

    baseline_unjoinable: int
    other_unjoinable: int


def _phase2_items_map(record: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, Optional[str]]], int]:
    """Return k -> (q,v) across phase2 buckets, and duplicate_k_count.

    Used for equality comparison of joined records.
    Order-insensitive: treats the phase2 items as a map by k.

    If a key k occurs more than once across the three buckets, that is treated
    as an ambiguity and duplicate_k_count > 0 is returned.
    """
    p2 = record.get("phase2")
    if not isinstance(p2, dict):
        return {}, 0

    out: Dict[str, Tuple[str, Optional[str]]] = {}
    dup = 0

    for bucket in ("semantic_items", "cosmetic_items", "coordination_items", "unknown_items"):
        items = p2.get(bucket)
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            k = it.get("k")
            q = it.get("q")
            v = it.get("v")
            if k is None:
                continue
            try:
                ks = str(k)
            except Exception:
                continue

            if ks in out:
                dup += 1
                continue

            try:
                qs = str(q) if q is not None else ""
            except Exception:
                qs = ""

            if v is None:
                vs = None
            else:
                try:
                    vs = str(v)
                except Exception:
                    vs = None

            out[ks] = (qs, vs)

    return out, dup


def classify_pair(
    *,
    baseline: DomainIndex,
    other: DomainIndex,
) -> ChangeCounts:
    """Classify changes for one domain between baseline and other.

    Definitions:
    - Joinable records are those with a non-null join_hash, unique within the file.
    - Join is performed by join_hash only.
    - "same" vs "modified" is based on phase2 item map equality.
    - If either record has duplicate phase2 keys (k collisions), that join_hash is
      classified as ambiguous_bad_items (excluded from same/modified).
    - If a join_hash is duplicated within either file, it is excluded from joinable
      and classified as ambiguous_duplicates if it appears in both files.
    """
    if baseline.domain != other.domain:
        raise ValueError("Domain mismatch")

    domain = baseline.domain

    # Unique joinable sets
    b_keys: Set[str] = set(baseline.joinable.keys())
    o_keys: Set[str] = set(other.joinable.keys())

    added = len(o_keys - b_keys)
    removed = len(b_keys - o_keys)

    # Keys present in both unique maps
    both = b_keys & o_keys

    same = 0
    modified = 0
    amb_bad = 0

    for jh in sorted(both):
        b_rec = baseline.joinable.get(jh)
        o_rec = other.joinable.get(jh)
        if not isinstance(b_rec, dict) or not isinstance(o_rec, dict):
            amb_bad += 1
            continue

        b_map, b_dup = _phase2_items_map(b_rec)
        o_map, o_dup = _phase2_items_map(o_rec)

        if b_dup or o_dup:
            amb_bad += 1
            continue

        if b_map == o_map:
            same += 1
        else:
            modified += 1

    # Duplicate join_hash ambiguity across files (descriptive)
    dup_overlap = (baseline.duplicates | set(baseline.duplicate_counts.keys())) & (
        other.duplicates | set(other.duplicate_counts.keys())
    )
    amb_dups = len(dup_overlap)

    return ChangeCounts(
        domain=domain,
        baseline_file_id=baseline.file_id,
        other_file_id=other.file_id,
        added=int(added),
        removed=int(removed),
        same=int(same),
        modified=int(modified),
        ambiguous_duplicates=int(amb_dups),
        ambiguous_bad_items=int(amb_bad),
        baseline_unjoinable=int(baseline.unjoinable_count),
        other_unjoinable=int(other.unjoinable_count),
    )
