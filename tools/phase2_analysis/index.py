from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class DomainIndex:
    """Per-file index for one domain keyed by join_key.join_hash.

    Ambiguity handling:
    - Records with missing/empty join_hash are unjoinable.
    - Duplicate join_hash within a file is treated as ambiguous.
      The join_hash key is retained in duplicates and excluded from joinable.
    """

    domain: str
    file_id: str

    # joinable records (unique join_hash only)
    joinable: Dict[str, Dict[str, Any]]

    # join_hash values that occur >1 within the same file
    duplicates: Set[str]
    duplicate_counts: Dict[str, int]

    # records that have no join_hash (or null)
    unjoinable_count: int

    # records where phase2 items have duplicate k within the record
    bad_phase2_item_keys: int


def _get_join_hash(record: Dict[str, Any]) -> Optional[str]:
    jk = record.get("join_key")
    if not isinstance(jk, dict):
        return None
    h = jk.get("join_hash")
    if h is None:
        return None
    try:
        hs = str(h).strip()
    except Exception:
        return None
    return hs or None


def _phase2_items_by_k(record: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, Optional[str]]], int]:
    """Return a map k -> (q, v) across concatenated phase2 item buckets.

    Also returns duplicate_k_count (number of k collisions observed).

    Notes:
    - This is used only for ambiguity detection (duplicate k within a record).
    - No normalization is performed.
    """
    p2 = record.get("phase2")
    if not isinstance(p2, dict):
        return {}, 0

    buckets = []
    for key in ("semantic_items", "cosmetic_items", "unknown_items"):
        items = p2.get(key)
        if isinstance(items, list):
            buckets.append(items)

    out: Dict[str, Tuple[str, Optional[str]]] = {}
    dup = 0

    for items in buckets:
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
            vs: Optional[str]
            if v is None:
                vs = None
            else:
                try:
                    vs = str(v)
                except Exception:
                    vs = None
            out[ks] = (qs, vs)

    return out, dup


def build_domain_index(*, domain: str, file_id: str, records: List[Dict[str, Any]]) -> DomainIndex:
    """Build a per-file join_hash index for one domain."""
    domain = str(domain)
    file_id = str(file_id)

    joinable: Dict[str, Dict[str, Any]] = {}
    duplicates: Set[str] = set()
    duplicate_counts: Dict[str, int] = {}
    unjoinable = 0
    bad_phase2_item_keys = 0

    for r in records:
        if not isinstance(r, dict):
            continue

        jh = _get_join_hash(r)
        if jh is None:
            unjoinable += 1
            continue

        # detect duplicate join_hash
        if jh in joinable:
            duplicates.add(jh)
            duplicate_counts[jh] = int(duplicate_counts.get(jh, 1)) + 1
            # remove from joinable (ambiguous)
            joinable.pop(jh, None)
            continue

        if jh in duplicates:
            duplicate_counts[jh] = int(duplicate_counts.get(jh, 2)) + 1
            continue

        # Track per-record phase2 item key ambiguity
        _, dup_k = _phase2_items_by_k(r)
        if dup_k:
            bad_phase2_item_keys += 1

        joinable[jh] = r

    return DomainIndex(
        domain=domain,
        file_id=file_id,
        joinable=joinable,
        duplicates=duplicates,
        duplicate_counts=duplicate_counts,
        unjoinable_count=int(unjoinable),
        bad_phase2_item_keys=int(bad_phase2_item_keys),
    )
