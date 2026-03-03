from __future__ import annotations

"""Population-level presence stability across authority samples.

Phase-2 analysis: descriptive only.

Implements:
- Record stability: which join_hash keys appear in >= X% of files
- Distribution across multiple thresholds

No heuristics, no ambiguity collapse.
"""

from dataclasses import dataclass
from typing import Dict, List, Sequence, Set

from .index import DomainIndex


@dataclass
class PresenceStability:
    domain: str
    threshold_pct: float
    files_total: int
    join_hash_stable_count: int


def presence_counts(indexes: Sequence[DomainIndex]) -> Dict[str, int]:
    """Return join_hash -> number of files in which it appears (unique joinable only)."""
    counts: Dict[str, int] = {}
    for idx in indexes:
        for jh in idx.joinable.keys():
            counts[jh] = int(counts.get(jh, 0)) + 1
    return counts


def stable_join_hashes(
    *,
    indexes: Sequence[DomainIndex],
    threshold_pct: float,
) -> Set[str]:
    """Return join_hash keys present in >= threshold_pct of files."""
    if not indexes:
        return set()

    n = len(indexes)
    if threshold_pct < 0:
        threshold_pct = 0.0
    if threshold_pct > 100:
        threshold_pct = 100.0

    need = (threshold_pct / 100.0) * float(n)

    counts = presence_counts(indexes)
    return {jh for jh, c in counts.items() if float(c) >= need}


def stability_distribution(
    *,
    indexes: Sequence[DomainIndex],
    thresholds_pct: Sequence[float],
) -> List[PresenceStability]:
    """Compute stable counts for each threshold in thresholds_pct."""
    out: List[PresenceStability] = []
    if not indexes:
        return out

    domain = indexes[0].domain
    n = len(indexes)

    counts = presence_counts(indexes)
    for t in thresholds_pct:
        if t < 0:
            t2 = 0.0
        elif t > 100:
            t2 = 100.0
        else:
            t2 = float(t)
        need = (t2 / 100.0) * float(n)
        stable = sum(1 for _jh, c in counts.items() if float(c) >= need)
        out.append(
            PresenceStability(
                domain=domain,
                threshold_pct=t2,
                files_total=n,
                join_hash_stable_count=int(stable),
            )
        )

    return out
