from __future__ import annotations

"""Per-attribute stability and "authority stress" reporting.

Phase-2 analysis: descriptive only.

This module will compute:
- attribute stability per k (% identical (q,v) among joined records)
- divergence breakdown by q transitions (e.g., ok->unreadable)
- stress ranking: attributes that most frequently differ within joined records

Definitions:
- Joining is by join_key.join_hash only.
- Comparisons are only among joinable records (unique join_hash within each file).
- Duplicate join_hash within a file is treated as ambiguous and excluded.

Implementation will be added after minimal change_type slice is validated.
"""

from dataclasses import dataclass
from typing import Dict, List, Sequence

from .index import DomainIndex


@dataclass
class AttrStabilityRow:
    domain: str
    bucket: str  # semantic|cosmetic|coordination|unknown
    k: str
    comparisons: int
    identical: int
    identical_pct: float

    # q-transition counts (descriptive)
    q_transitions: Dict[str, int]


@dataclass
class StressRow:
    domain: str
    bucket: str
    k: str
    comparisons: int
    diffs: int
    diff_pct: float

    # q-change counts (descriptive; no scoring)
    q_change_any: int
    q_change_to_missing: int
    q_change_to_unreadable: int
    q_change_to_unsupported: int


def compute_attr_stability(
    *,
    baseline: DomainIndex,
    other: DomainIndex,
    bucket: str,
) -> List[AttrStabilityRow]:
    raise NotImplementedError("Phase-2 attributes.py is a planned extension; not implemented in minimal slice.")


def compute_stress_rank(
    *,
    indexes: Sequence[DomainIndex],
    bucket: str,
) -> List[StressRow]:
    raise NotImplementedError("Phase-2 attributes.py is a planned extension; not implemented in minimal slice.")
