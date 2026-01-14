# core/contracts.py
# Contract + status utilities for Revit Fingerprint output.
#
# PR1 scope:
# - Provide a versioned run envelope + per-domain envelope.
# - Provide deterministic run_status rollup.
# - Provide bounded diagnostics (no unbounded error accumulation).
#
# Non-goals (for PR1):
# - Deep schema validation of domain payloads
# - Revit API interaction (must remain pure-Python)

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# =========================
# Contract versioning
# =========================

SCHEMA_VERSION = "2.0"

# =========================
# Status constants
# =========================

RUN_STATUS_OK = "ok"
RUN_STATUS_DEGRADED = "degraded"
RUN_STATUS_FAILED = "failed"

DOMAIN_STATUS_OK = "ok"
DOMAIN_STATUS_DEGRADED = "degraded"
DOMAIN_STATUS_BLOCKED = "blocked"
DOMAIN_STATUS_FAILED = "failed"
DOMAIN_STATUS_UNSUPPORTED = "unsupported"

VALID_RUN_STATUSES = {RUN_STATUS_OK, RUN_STATUS_DEGRADED, RUN_STATUS_FAILED}
VALID_DOMAIN_STATUSES = {
    DOMAIN_STATUS_OK,
    DOMAIN_STATUS_DEGRADED,
    DOMAIN_STATUS_BLOCKED,
    DOMAIN_STATUS_FAILED,
    DOMAIN_STATUS_UNSUPPORTED,
}

# =========================
# Diagnostics (bounded errors)
# =========================

DEFAULT_ERROR_CAP = 50


@dataclass(frozen=True)
class DiagError:
    domain: str
    status: str
    code: str
    message: str


def _ensure_list(value: Optional[List[Any]]) -> List[Any]:
    return value if isinstance(value, list) else []


def new_run_diag() -> Dict[str, Any]:
    """
    Create an empty, stable run_diag structure.
    Keep this minimal and stable; domains should record their own diag separately.
    """
    return {
        "errors": [],  # list[DiagError-like dict]
        "counters": {
            "domain_total": 0,
            "domain_ok": 0,
            "domain_degraded": 0,
            "domain_blocked": 0,
            "domain_failed": 0,
            "domain_unsupported": 0,
            "errors_dropped": 0,
        },
    }


def add_bounded_error(
    run_diag: Dict[str, Any],
    *,
    domain: str,
    status: str,
    code: str,
    message: str,
    cap: int = DEFAULT_ERROR_CAP,
) -> None:
    """
    Append a structured error to run_diag['errors'] up to a hard cap.
    If cap is exceeded, increment errors_dropped and do not append further items.
    """
    if not isinstance(run_diag, dict):
        raise TypeError("run_diag must be a dict")

    errors = run_diag.get("errors")
    if not isinstance(errors, list):
        errors = []
        run_diag["errors"] = errors

    counters = run_diag.get("counters")
    if not isinstance(counters, dict):
        counters = {}
        run_diag["counters"] = counters

    # Defensive: ensure a sane cap
    if cap is None or cap <= 0:
        cap = 1

    if len(errors) >= cap:
        counters["errors_dropped"] = int(counters.get("errors_dropped", 0)) + 1
        return

    # Keep error entry stable and small
    errors.append(
        {
            "domain": str(domain),
            "status": str(status),
            "code": str(code),
            "message": str(message),
        }
    )


# =========================
# Envelope constructors
# =========================

def new_domain_envelope(
    *,
    domain: str,
    domain_version: str,
    status: str,
    block_reasons: Optional[List[str]] = None,
    diag: Optional[Dict[str, Any]] = None,
    records: Optional[Any] = None,
    hash_value: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Standard per-domain envelope.

    Notes:
    - 'records' is intentionally type-agnostic for PR1: domains may emit list/dict/None.
    - 'hash' may be None during degradation/failure.
    """
    if status not in VALID_DOMAIN_STATUSES:
        raise ValueError(f"Invalid domain status: {status!r}")

    env: Dict[str, Any] = {
        "domain": str(domain),
        "domain_version": str(domain_version),
        "status": status,
        "block_reasons": [str(x) for x in _ensure_list(block_reasons)],
        "diag": diag if isinstance(diag, dict) else {},
        "hash": (str(hash_value) if hash_value is not None else None),
    }

    # Include records only if explicitly provided (keeps envelope smaller by default)
    if records is not None:
        env["records"] = records

    return env


def new_run_envelope(
    *,
    schema_version: str,
    run_status: str,
    run_diag: Dict[str, Any],
    domains: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Top-level contract envelope. This is intended to live under output["_contract"] (recommended).
    """
    if schema_version != SCHEMA_VERSION:
        # PR1: hard fail if caller tries to emit a mismatched contract version.
        raise ValueError(f"schema_version must be {SCHEMA_VERSION!r}, got {schema_version!r}")

    if run_status not in VALID_RUN_STATUSES:
        raise ValueError(f"Invalid run status: {run_status!r}")

    if not isinstance(run_diag, dict):
        raise TypeError("run_diag must be a dict")

    if not isinstance(domains, dict):
        raise TypeError("domains must be a dict")

    return {
        "schema_version": schema_version,
        "run_status": run_status,
        "run_diag": run_diag,
        "domains": domains,
    }


# =========================
# Status rollups
# =========================

def compute_run_status(
    domains: Dict[str, Dict[str, Any]],
    *,
    base_run_diag: Optional[Dict[str, Any]] = None,
    treat_unsupported_as_degraded: bool = True,
) -> Tuple[str, Dict[str, Any]]:
    """
    Deterministically compute run_status from domain statuses and return (run_status, run_diag).

    Rollup rule (PR1 lock):
    - any domain failed -> run failed
    - else any domain degraded|blocked -> run degraded
    - else any domain unsupported -> run degraded only if treat_unsupported_as_degraded is True
    - else run ok

    Also populates counters in run_diag.
    """
    if not isinstance(domains, dict):
        raise TypeError("domains must be a dict")

    run_diag = base_run_diag if isinstance(base_run_diag, dict) else new_run_diag()
    counters = run_diag.get("counters")
    if not isinstance(counters, dict):
        counters = {}
        run_diag["counters"] = counters

    # Reset/initialize counters (idempotent)
    counters.update(
        {
            "domain_total": 0,
            "domain_ok": 0,
            "domain_degraded": 0,
            "domain_blocked": 0,
            "domain_failed": 0,
            "domain_unsupported": 0,
        }
    )

    any_failed = False
    any_degradedish = False

    for name, env in sorted(domains.items(), key=lambda kv: str(kv[0])):
        counters["domain_total"] += 1

        try:
            status = str(env.get("status", ""))
        except Exception:
            status = ""

        if status not in VALID_DOMAIN_STATUSES:
            # Explicitly fail run: contract is inconsistent/untrusted.
            any_failed = True
            counters["domain_failed"] += 1
            add_bounded_error(
                run_diag,
                domain=str(name),
                status=DOMAIN_STATUS_FAILED,
                code="invalid_domain_status",
                message=f"Invalid domain status: {status!r}",
            )
            continue

        if status == DOMAIN_STATUS_FAILED:
            any_failed = True
            counters["domain_failed"] += 1
        elif status == DOMAIN_STATUS_OK:
            counters["domain_ok"] += 1
        elif status == DOMAIN_STATUS_DEGRADED:
            any_degradedish = True
            counters["domain_degraded"] += 1
        elif status == DOMAIN_STATUS_BLOCKED:
            any_degradedish = True
            counters["domain_blocked"] += 1
        elif status == DOMAIN_STATUS_UNSUPPORTED:
            if treat_unsupported_as_degraded:
                any_degradedish = True
            counters["domain_unsupported"] += 1
        else:
            # Should be unreachable due to validation above; keep explicit.
            any_failed = True
            counters["domain_failed"] += 1
            add_bounded_error(
                run_diag,
                domain=str(name),
                status=DOMAIN_STATUS_FAILED,
                code="unreachable_domain_status",
                message=f"Unreachable status encountered: {status!r}",
            )

    if any_failed:
        return RUN_STATUS_FAILED, run_diag
    if any_degradedish:
        return RUN_STATUS_DEGRADED, run_diag
    return RUN_STATUS_OK, run_diag
