"""core/deps.py

Centralized dependency enforcement for domain execution.

Non-negotiables:
- No silent partials: downstream domains must not run when required upstream results
  are missing or non-acceptable.
- Block reasons must be explicit and machine-readable.

This module is pure-Python (no Revit API usage).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Set

from core.contracts import (
    DOMAIN_STATUS_BLOCKED,
    DOMAIN_STATUS_DEGRADED,
    DOMAIN_STATUS_FAILED,
    DOMAIN_STATUS_OK,
    DOMAIN_STATUS_UNSUPPORTED,
    VALID_DOMAIN_STATUSES,
)


@dataclass(frozen=True)
class Blocked(Exception):
    """Typed exception used to signal a hard dependency block.

    Attributes:
        code: Stable machine-readable code describing the block class.
        reasons: Stable machine-readable tokens explaining the block.
        upstream: Optional upstream domain name involved in the block.
    """

    code: str
    reasons: tuple[str, ...]
    upstream: Optional[str] = None

    def __str__(self) -> str:  # pragma: no cover (presentation only)
        up = f" upstream={self.upstream!r}" if self.upstream else ""
        return f"Blocked(code={self.code!r}, reasons={list(self.reasons)!r}{up})"


_DEFAULT_ACCEPT: Set[str] = {DOMAIN_STATUS_OK, DOMAIN_STATUS_DEGRADED}
_NONACCEPTABLE: Set[str] = {
    DOMAIN_STATUS_BLOCKED,
    DOMAIN_STATUS_FAILED,
    DOMAIN_STATUS_UNSUPPORTED,
}


def require_domain(
    result_map: Dict[str, Any],
    name: str,
    *,
    accept_statuses: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Require an upstream domain envelope to exist and be acceptable.

    Args:
        result_map: Dict of domain_name -> domain_envelope.
        name: Upstream domain name to require.
        accept_statuses: Iterable of acceptable statuses. Defaults to {ok, degraded}.

    Returns:
        The upstream domain envelope.

    Raises:
        Blocked: if upstream is missing or in a non-acceptable status.
    """
    if not isinstance(result_map, dict):
        raise TypeError("result_map must be a dict")

    upstream = str(name)
    if upstream not in result_map:
        raise Blocked(
            code="missing_upstream",
            reasons=(f"missing:{upstream}",),
            upstream=upstream,
        )

    env = result_map.get(upstream)
    if not isinstance(env, dict):
        raise Blocked(
            code="invalid_upstream_envelope",
            reasons=(f"invalid:{upstream}",),
            upstream=upstream,
        )

    status = env.get("status")
    if status not in VALID_DOMAIN_STATUSES:
        raise Blocked(
            code="invalid_upstream_status",
            reasons=(f"invalid_status:{upstream}",),
            upstream=upstream,
        )

    accept = set(accept_statuses) if accept_statuses is not None else set(_DEFAULT_ACCEPT)

    # Defensive: if caller passes nonsense, fail closed.
    if not accept or any(s not in VALID_DOMAIN_STATUSES for s in accept):
        raise ValueError("accept_statuses must be a non-empty subset of VALID_DOMAIN_STATUSES")

    if status in _NONACCEPTABLE or status not in accept:
        raise Blocked(
            code="upstream_not_acceptable",
            reasons=(f"{upstream}:{status}",),
            upstream=upstream,
        )

    return env
