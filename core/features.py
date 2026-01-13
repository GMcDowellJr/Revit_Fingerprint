# -*- coding: utf-8 -*-
"""
Cohort-analysis feature surface.

Goals:
- Deterministic, aggregation-friendly features per project run.
- Avoid volatile telemetry (timings, paths, tracebacks, exception strings).
- Express unreadable/missing as structured status + block reasons.

This is intentionally minimal and can be extended domain-by-domain,
but it must remain stable and safe to diff/aggregate at scale.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _as_int(x: Any) -> Optional[int]:
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            # Disallow non-integer floats to prevent accidental volatility
            if int(x) == x:
                return int(x)
            return None
        if isinstance(x, str) and x.strip().isdigit():
            return int(x.strip())
    except Exception:
        return None
    return None


def _extract_counts_from_legacy(legacy: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract stable count signals from legacy domain payloads when present.

    Convention (existing domains often emit):
      - count
      - raw_count
    """
    d = _as_dict(legacy)
    c = _as_int(d.get("count", None))
    rc = _as_int(d.get("raw_count", None))
    return c, rc


def build_features(payload: Any) -> Dict[str, Any]:
    """
    Build deterministic features from payload.

    Features include:
      - schema_version, hash_mode, run_status
      - per-domain: status, hash, block_reasons, count/raw_count (when available)

    Returns:
      Dict suitable for Parquet row-shaping and cohort aggregation.
    """
    p = _as_dict(payload)
    contract = _as_dict(p.get("_contract", None))
    if not contract and "run_status" in p and "domains" in p:
        contract = p

    schema_version = contract.get("schema_version", None)
    run_status = contract.get("run_status", None)
    domains = _as_dict(contract.get("domains", None))

    out_domains: Dict[str, Any] = {}
    for name in sorted(domains.keys()):
        env = _as_dict(domains.get(name, None))

        status = env.get("status", None)
        h = env.get("hash", None)

        br = env.get("block_reasons", [])
        if not isinstance(br, list):
            br = []
        br_sorted = sorted({str(x) for x in br})

        # Pull minimal stable counts if legacy payload exists (top-level domain key).
        legacy = p.get(name, None)
        count, raw_count = _extract_counts_from_legacy(legacy)

        out_domains[str(name)] = {
            "status": status,
            "hash": (str(h) if h is not None else None),
            "block_reasons": br_sorted,
            "count": count,
            "raw_count": raw_count,
        }

    # Optional stable identity subset (no paths)
    ident = _as_dict(p.get("identity", None))
    identity = {}
    if ident:
        # Only include fields that tend to be stable and useful in cohort slices.
        for k in ("project_title", "is_workshared", "revit_version_number"):
            if k in ident:
                identity[k] = ident.get(k, None)

    out = {
        "schema_version": schema_version,
        "hash_mode": p.get("_hash_mode", None),
        "run_status": run_status,
        "domains": out_domains,
    }
    if identity:
        out["identity"] = identity
    return out
