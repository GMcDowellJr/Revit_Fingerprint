# -*- coding: utf-8 -*-
"""
Stable manifest surface for project-to-project comparison.

Intent:
- Provide a small, deterministic "comparison surface" derived from the authoritative
  contract envelope (payload["_contract"]).
- Exclude volatile telemetry (timings, machine paths, tracebacks, etc.).
- Preserve explicit domain statuses and block reasons.

This is *not* a replacement for full payloads; it is the default object to diff.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _safe_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def build_manifest(payload: Any, *, include_identity: bool = False) -> Dict[str, Any]:
    """
    Build a deterministic manifest derived from payload["_contract"].

    Args:
        payload: full runner output dict (or already-a-manifest dict)
        include_identity: if True, includes a minimal stable identity subset when available

    Returns:
        Stable manifest dict.
    """
    p = _safe_dict(payload)

    # Allow passing a contract envelope directly.
    contract = _safe_dict(p.get("_contract", None))
    if not contract and "run_status" in p and "domains" in p:
        contract = p

    schema_version = contract.get("schema_version", None)
    run_status = contract.get("run_status", None)
    run_diag = _safe_dict(contract.get("run_diag", None))
    domains = _safe_dict(contract.get("domains", None))

    out_domains: Dict[str, Any] = {}
    for name in sorted(domains.keys()):
        env = _safe_dict(domains.get(name, None))

        status = env.get("status", None)
        h = env.get("hash", None)

        br = env.get("block_reasons", [])
        if not isinstance(br, list):
            br = []
        # Stable ordering: treat reasons as a set but emit sorted strings.
        br_sorted = sorted({str(x) for x in br})

        out_domains[str(name)] = {
            "status": status,
            "hash": (str(h) if h is not None else None),
            "block_reasons": br_sorted,
        }

    manifest: Dict[str, Any] = {
        "schema_version": schema_version,
        "hash_mode": p.get("_hash_mode", None),
        "run_status": run_status,
        "domains": out_domains,
    }

    # Optional: include only stable identity fields (NO paths).
    if include_identity:
        ident = _safe_dict(p.get("identity", None))
        if ident:
            manifest["identity"] = {
                "project_title": ident.get("project_title", None),
                "is_workshared": ident.get("is_workshared", None),
                "revit_version_number": ident.get("revit_version_number", None),
            }

    # Include a small bounded signal surface for diagnostics ONLY if it already exists,
    # but keep it minimal and stable-ish.
    # (Counters can still vary across runs if collector strategy changes, so keep optional.)
    counters = run_diag.get("counters", None)
    if isinstance(counters, dict) and counters:
        # Canonicalize keys to strings; values to ints when possible
        c2: Dict[str, Any] = {}
        for k, v in counters.items():
            try:
                kk = str(k)
            except Exception:
                continue
            try:
                c2[kk] = int(v)
            except Exception:
                c2[kk] = v
        manifest["counters"] = c2

    return manifest
