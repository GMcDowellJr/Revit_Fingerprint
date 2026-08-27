# tools/bundle_analysis/comparison_status.py
#
# Explicit reliability semantics for reference-vs-target comparison (PR2).
#
# `comparison_status` reuses core.contracts's existing DOMAIN_STATUS_OK /
# DOMAIN_STATUS_DEGRADED / DOMAIN_STATUS_BLOCKED vocabulary verbatim -- this
# module does not invent a second status vocabulary. It only adds
# comparison-specific *reason codes* (which have no existing analog) and a
# small "blocked beats degraded beats ok" aggregator matching the ordering
# already used by core.contracts.compute_run_status for domain rollups.
#
# `coverage_status` (in step_compare.py) is a distinct, pre-existing PR1
# concept -- how much of the reference set the target covers -- and is never
# set from this module. `comparison_status` instead answers a different
# question: was there enough trustworthy evidence to make that coverage
# computation meaningful at all.

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, List

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.contracts import (  # noqa: E402
    DOMAIN_STATUS_OK as COMPARISON_STATUS_OK,
    DOMAIN_STATUS_DEGRADED as COMPARISON_STATUS_DEGRADED,
    DOMAIN_STATUS_BLOCKED as COMPARISON_STATUS_BLOCKED,
)

VALID_COMPARISON_STATUSES = {
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_DEGRADED,
    COMPARISON_STATUS_BLOCKED,
}

# Reason-code vocabulary. Stable, machine-readable, deterministic.
REASON_REFERENCE_DOMAIN_UNDEFINED = "REFERENCE_DOMAIN_UNDEFINED"
REASON_REFERENCE_INVALID = "REFERENCE_INVALID"
REASON_SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"
REASON_TARGET_DOMAIN_UNAVAILABLE = "TARGET_DOMAIN_UNAVAILABLE"
REASON_TARGET_DOMAIN_DEGRADED = "TARGET_DOMAIN_DEGRADED"
REASON_TARGET_IDENTITY_INVALID = "TARGET_IDENTITY_INVALID"
REASON_COMPARISON_INPUT_INVALID = "COMPARISON_INPUT_INVALID"


def join_reason_codes(codes: Iterable[str]) -> str:
    """Deterministically serialize a set of reason codes for a CSV cell."""
    return "|".join(sorted({str(c).strip() for c in codes if str(c).strip()}))


def split_reason_codes(value: str) -> List[str]:
    return [c for c in str(value or "").split("|") if c]


def aggregate_comparison_status(statuses: Iterable[str]) -> str:
    """Deterministic rollup: blocked beats degraded beats ok.

    Mirrors the ordering of core.contracts.compute_run_status's domain
    rollup (any failed/blocked -> degraded-or-worse at the run level), but
    expressed directly in terms of comparison's own three-state vocabulary
    since comparison never needs a run-level "failed" tier distinct from
    "blocked" (empty input aggregates to ok -- a run/domain with zero rows
    to roll up did not attempt an unreliable comparison).
    """
    seen = set(statuses)
    if COMPARISON_STATUS_BLOCKED in seen:
        return COMPARISON_STATUS_BLOCKED
    if COMPARISON_STATUS_DEGRADED in seen:
        return COMPARISON_STATUS_DEGRADED
    return COMPARISON_STATUS_OK
