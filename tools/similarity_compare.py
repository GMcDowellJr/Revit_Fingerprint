# similarity_compare.py
# Compare Revit fingerprint JSONs:
#   1) baseline file vs every JSON in a directory
#   2) pairwise comparisons among all JSONs in a directory
#
# Implements 3 similarity options:
#   A) OK-domain hash token Jaccard (domain:hash, ok only)
#   B) Domain status token Jaccard (domain:status, excluding blocked)
#   C) Record sig_hash overlap (multiset Jaccard) aggregated across comparable domains
#
# Invariants honored:
#   - blocked domains => undefined (excluded from similarity; tracked)
#   - unreadable != missing (explicit)
#   - no silent failure: parse/shape problems recorded per file and per domain

from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------
# Data model (minimal, tolerant)
# -----------------------------

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_BLOCKED = "blocked"

# Some fingerprint files may use other labels; map conservatively.
STATUS_ALIASES = {
    "OK": STATUS_OK,
    "Ok": STATUS_OK,
    "DEGRADED": STATUS_DEGRADED,
    "Degraded": STATUS_DEGRADED,
    "BLOCKED": STATUS_BLOCKED,
    "Blocked": STATUS_BLOCKED,
}

RECOGNIZED_STATUSES = {STATUS_OK, STATUS_DEGRADED, STATUS_BLOCKED}


@dataclass(frozen=True)
class DomainData:
    name: str
    status: str
    domain_hash: Optional[str]
    sig_hashes: Optional[List[str]]  # None means unavailable/unknown; [] means known empty
    unreadable: bool                 # explicitly indicates file/domain unreadable, not missing
    missing: bool                    # explicitly indicates missing domain payload
    reason: Optional[str]            # why missing/unreadable/unknown

    # Optional mapping from sig_hash -> human label (e.g., element/type name), when available.
    # None means unavailable/unknown; {} should not be emitted.
    sig_labels: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class FingerprintData:
    path: str
    ok: bool
    error: Optional[str]
    domains: Dict[str, DomainData]


@dataclass(frozen=True)
class SimilarityScalar:
    value: Optional[float]          # None means undefined
    reason: Optional[str]           # why undefined


@dataclass(frozen=True)
class SimilarityResult:
    file_a: str
    file_b: str

    # Semantic metric names (formerly opt_a / opt_b / opt_c)
    domain_hash_identity_jaccard: SimilarityScalar
    domain_status_layout_jaccard: SimilarityScalar
    signature_multiset_similarity: SimilarityScalar

    # Coverage / gating signals
    domains_total: int
    domains_compared_signatures: int
    domains_undefined_blocked: int
    domains_undefined_unreadable: int
    domains_missing: int

    note: Optional[str]


# -----------------------------
# Parsing helpers (tolerant, explicit)
# -----------------------------

def _as_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    return str(x)


def _norm_status(s: Any) -> str:
    if s is None:
        return STATUS_BLOCKED  # conservative default when absent
    if isinstance(s, str):
        s2 = STATUS_ALIASES.get(s, s.lower().strip())
        if s2 in RECOGNIZED_STATUSES:
            return s2
    # Unknown status -> blocked (don’t guess comparable)
    return STATUS_BLOCKED


def _extract_domains_obj(fp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return the domain-metadata object.

    Supported layouts:
      1) New contract: top-level "_domains" (dict)
      2) Legacy: top-level "domains" (dict)
      3) Wrapped legacy: {"fingerprint"|"result"|"results"|"data": {"domains": {...}}}
    """
    v = fp.get("_domains")
    if isinstance(v, dict):
        return v

    v = fp.get("domains")
    if isinstance(v, dict):
        return v

    for wrap in ("fingerprint", "result", "results", "data"):
        w = fp.get(wrap)
        if isinstance(w, dict):
            v2 = w.get("domains")
            if isinstance(v2, dict):
                return v2

    return None


def _extract_sig_hashes(domain_payload: Dict[str, Any]) -> Optional[List[str]]:
    """
    Return:
      - None: signature hashes unavailable/unknown
      - []: explicitly known empty
      - [..]: list of signature-hash strings (may include duplicates)

    Supported payload layouts:
      A) New contract: "signature_hashes_v2" (preferred) or "signature_hashes"
      B) Legacy: records/rows/items/elements list with per-row "sig_hash"
      C) Nested legacy: payload["data"][...]
    """
    # New contract: explicit signature lists on the domain payload
    for key in ("signature_hashes_v2", "signature_hashes"):
        v = domain_payload.get(key)
        if isinstance(v, list):
            out: List[str] = []
            for x in v:
                if isinstance(x, str) and x:
                    out.append(x)
            # Important: list exists => known (even if empty)
            return out

    # Legacy: look for per-record sig_hash
    for key in ("records", "rows", "items", "elements"):
        recs = domain_payload.get(key)
        if isinstance(recs, list):
            out = []
            saw_sig_field = False
            for r in recs:
                if isinstance(r, dict) and "sig_hash" in r:
                    saw_sig_field = True
                    sv = r.get("sig_hash")
                    if isinstance(sv, str) and sv:
                        out.append(sv)
            if saw_sig_field:
                return out
            return None

    # Nested legacy
    data = domain_payload.get("data")
    if isinstance(data, dict):
        for key in ("records", "rows", "items", "elements"):
            recs = data.get(key)
            if isinstance(recs, list):
                out = []
                saw_sig_field = False
                for r in recs:
                    if isinstance(r, dict) and "sig_hash" in r:
                        saw_sig_field = True
                        sv = r.get("sig_hash")
                        if isinstance(sv, str) and sv:
                            out.append(sv)
                if saw_sig_field:
                    return out
                return None

    return None


def _extract_domain_status(domain_payload: Dict[str, Any]) -> str:
    for key in ("status", "run_status", "domain_status"):
        if key in domain_payload:
            return _norm_status(domain_payload.get(key))
    # Sometimes status stored under "meta"
    meta = domain_payload.get("meta")
    if isinstance(meta, dict) and "status" in meta:
        return _norm_status(meta.get("status"))
    return STATUS_BLOCKED  # conservative


def _extract_domain_hash(domain_payload: Dict[str, Any]) -> Optional[str]:
    """
    Extract the domain-level hash from either:
      - new contract meta objects (from _domains), or
      - legacy domain payload objects.

    Returns:
      - hash string, or None if absent.
    """
    for key in ("hash", "domain_hash", "value_hash", "fingerprint_hash", "def_hash"):
        v = domain_payload.get(key)
        if isinstance(v, str) and v:
            return v

    # Sometimes nested under meta
    meta = domain_payload.get("meta")
    if isinstance(meta, dict):
        for key in ("hash", "domain_hash", "value_hash", "fingerprint_hash", "def_hash"):
            v = meta.get(key)
            if isinstance(v, str) and v:
                return v

    return None


def _extract_sig_hashes(domain_payload: Dict[str, Any]) -> Optional[List[str]]:
    """
    Return:
      - None: signature hashes unavailable/unknown
      - []: explicitly known empty
      - [..]: list of signature-hash strings (may include duplicates)

    Supported payload layouts:
      A) "signature_hashes_v2" (preferred) or "signature_hashes"
      B) Legacy: records/rows/items/elements list with per-row "sig_hash"
      C) Nested legacy: payload["data"][...]
    """
    # A) New contract: explicit signature lists
    for key in ("signature_hashes_v2", "signature_hashes"):
        v = domain_payload.get(key)
        if isinstance(v, list):
            out: List[str] = []
            for x in v:
                if isinstance(x, str) and x:
                    out.append(x)
            return out  # list exists => known (even if empty)

    # B) Legacy: per-record sig_hash
    for key in ("records", "rows", "items", "elements"):
        recs = domain_payload.get(key)
        if isinstance(recs, list):
            out: List[str] = []
            saw_sig_field = False
            for r in recs:
                if isinstance(r, dict) and "sig_hash" in r:
                    saw_sig_field = True
                    sv = r.get("sig_hash")
                    if isinstance(sv, str) and sv:
                        out.append(sv)
            if saw_sig_field:
                return out
            return None

    # C) Nested legacy
    data = domain_payload.get("data")
    if isinstance(data, dict):
        for key in ("records", "rows", "items", "elements"):
            recs = data.get(key)
            if isinstance(recs, list):
                out: List[str] = []
                saw_sig_field = False
                for r in recs:
                    if isinstance(r, dict) and "sig_hash" in r:
                        saw_sig_field = True
                        sv = r.get("sig_hash")
                        if isinstance(sv, str) and sv:
                            out.append(sv)
                if saw_sig_field:
                    return out
                return None

    return None


def load_fingerprint(path: str) -> FingerprintData:
    p = Path(path)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return FingerprintData(path=str(p), ok=False, error=f"unreadable_json: {type(e).__name__}: {e}", domains={})

    domains_obj = _extract_domains_obj(raw)
    if domains_obj is None:
        return FingerprintData(path=str(p), ok=False, error="missing_domains_object", domains={})

    # Detect whether this is the new contract (domains_obj == raw["_domains"])
    is_new_contract = isinstance(raw.get("_domains"), dict) and domains_obj is raw.get("_domains")

    domains: Dict[str, DomainData] = {}

    if is_new_contract:
        # domains_obj provides meta (status/hash/reasons); per-domain payload is at top-level key with same name.
        for dname, meta in domains_obj.items():
            if not isinstance(dname, str):
                dname = str(dname)

            if not isinstance(meta, dict):
                domains[dname] = DomainData(
                    name=dname,
                    status=STATUS_BLOCKED,
                    domain_hash=None,
                    sig_hashes=None,
                    unreadable=True,
                    missing=False,
                    reason=f"domain_meta_not_object:{type(meta).__name__}",
                )
                continue

            status = _extract_domain_status(meta)
            dhash = _extract_domain_hash(meta)

            payload = raw.get(dname, None)
            if payload is None:
                # Domain exists in _domains but has no payload key (distinct from blocked/unreadable)
                reason = None
                for rk in ("reason", "block_reason", "degrade_reason", "error"):
                    rv = meta.get(rk)
                    if isinstance(rv, str) and rv.strip():
                        reason = rv.strip()
                        break
                if reason is None:
                    br = meta.get("block_reasons")
                    if isinstance(br, list) and br:
                        reason = f"block_reasons:{br[0]}"
                domains[dname] = DomainData(
                    name=dname,
                    status=status,
                    domain_hash=dhash,
                    sig_hashes=None,
                    unreadable=False,
                    missing=True,
                    reason=reason or "missing_domain_payload_key",
                )
                continue

            if not isinstance(payload, dict):
                domains[dname] = DomainData(
                    name=dname,
                    status=status,
                    domain_hash=dhash,
                    sig_hashes=None,
                    unreadable=True,
                    missing=False,
                    reason=f"domain_payload_not_object:{type(payload).__name__}",
                )
                continue

            sigs = _extract_sig_hashes(payload)
            sig_labels = _extract_sig_labels(payload)

            # Capture a reason string if present (meta first, then payload)
            reason = None
            for src in (meta, payload):
                for rk in ("reason", "block_reason", "degrade_reason", "error"):
                    rv = src.get(rk)
                    if isinstance(rv, str) and rv.strip():
                        reason = rv.strip()
                        break
                if reason:
                    break
            if reason is None:
                br = meta.get("block_reasons")
                if isinstance(br, list) and br:
                    reason = f"block_reasons:{br[0]}"

            domains[dname] = DomainData(
                name=dname,
                status=status,
                domain_hash=dhash,
                sig_hashes=sigs,
                unreadable=False,
                missing=False,
                reason=reason,
                sig_labels=sig_labels,
            )


        return FingerprintData(path=str(p), ok=True, error=None, domains=domains)

    # Legacy path: domains_obj is already per-domain payload
    for dname, dpayload in domains_obj.items():
        if not isinstance(dname, str):
            dname = str(dname)

        if dpayload is None:
            domains[dname] = DomainData(
                name=dname,
                status=STATUS_BLOCKED,
                domain_hash=None,
                sig_hashes=None,
                unreadable=False,
                missing=True,
                reason="domain_payload_null",
            )
            continue

        if not isinstance(dpayload, dict):
            domains[dname] = DomainData(
                name=dname,
                status=STATUS_BLOCKED,
                domain_hash=None,
                sig_hashes=None,
                unreadable=True,
                missing=False,
                reason=f"domain_payload_not_object:{type(dpayload).__name__}",
            )
            continue

        status = _extract_domain_status(dpayload)
        dhash = _extract_domain_hash(dpayload)
        sigs = _extract_sig_hashes(dpayload)
        sig_labels = _extract_sig_labels(dpayload)

        missing = False
        unreadable = False
        reason = None
        for rk in ("reason", "block_reason", "degrade_reason", "error"):
            rv = dpayload.get(rk)
            if isinstance(rv, str) and rv.strip():
                reason = rv.strip()
                break

        domains[dname] = DomainData(
            name=dname,
            status=status,
            domain_hash=dhash,
            sig_hashes=sigs,
            unreadable=unreadable,
            missing=missing,
            reason=reason,
            sig_labels=sig_labels,
        )


    return FingerprintData(path=str(p), ok=True, error=None, domains=domains)


# -----------------------------
# Similarity metrics
# -----------------------------

def jaccard_set(a: Iterable[str], b: Iterable[str]) -> SimilarityScalar:
    sa = set(a)
    sb = set(b)
    if not sa and not sb:
        return SimilarityScalar(value=1.0, reason=None)
    if not sa and sb:
        return SimilarityScalar(value=0.0, reason=None)
    if sa and not sb:
        return SimilarityScalar(value=0.0, reason=None)
    inter = len(sa & sb)
    union = len(sa | sb)
    return SimilarityScalar(value=(inter / union) if union else None, reason=None)


def jaccard_multiset(ca: Counter, cb: Counter) -> SimilarityScalar:
    # Weighted Jaccard on multisets: sum min / sum max
    keys = set(ca.keys()) | set(cb.keys())
    if not keys:
        return SimilarityScalar(value=1.0, reason=None)
    inter = 0
    union = 0
    for k in keys:
        a = ca.get(k, 0)
        b = cb.get(k, 0)
        inter += min(a, b)
        union += max(a, b)
    if union == 0:
        return SimilarityScalar(value=None, reason="undefined_union_zero")
    return SimilarityScalar(value=inter / union, reason=None)


def _domain_union_mass(sig_a: Optional[List[str]], sig_b: Optional[List[str]]) -> Optional[int]:
    if sig_a is None or sig_b is None:
        return None
    ca = Counter(sig_a)
    cb = Counter(sig_b)
    keys = set(ca.keys()) | set(cb.keys())
    mass = 0
    for k in keys:
        mass += max(ca.get(k, 0), cb.get(k, 0))
    return mass


def compare_two(fp_a: FingerprintData, fp_b: FingerprintData) -> SimilarityResult:
    # If a file is unreadable, all metrics are undefined (explicit)
    if not fp_a.ok or not fp_b.ok:
        reason = "file_unreadable"
        note = f"a_error={fp_a.error!r} b_error={fp_b.error!r}"
        return SimilarityResult(
            file_a=fp_a.path,
            file_b=fp_b.path,

            domain_hash_identity_jaccard=SimilarityScalar(None, reason),
            domain_status_layout_jaccard=SimilarityScalar(None, reason),
            signature_multiset_similarity=SimilarityScalar(None, reason),

            domains_total=0,
            domains_compared_signatures=0,
            domains_undefined_blocked=0,
            domains_undefined_unreadable=0,
            domains_missing=0,
            note=note,
        )

    domain_names = sorted(set(fp_a.domains.keys()) | set(fp_b.domains.keys()))
    domains_total = len(domain_names)

    undefined_blocked = 0
    undefined_unreadable = 0
    missing = 0

    # ----------------
    # Metric 1: domain_hash_identity_jaccard
    # Jaccard on {domain:hash} tokens, OK-only, blocked excluded
    # ----------------
    tokens_a: List[str] = []
    tokens_b: List[str] = []

    for d in domain_names:
        da = fp_a.domains.get(d)
        db = fp_b.domains.get(d)

        if da is None or db is None:
            missing += 1
            continue

        if da.missing or db.missing:
            missing += 1
            continue

        if da.unreadable or db.unreadable:
            undefined_unreadable += 1
            continue

        if da.status == STATUS_BLOCKED or db.status == STATUS_BLOCKED:
            undefined_blocked += 1
            continue

        if da.status == STATUS_OK and isinstance(da.domain_hash, str) and da.domain_hash:
            tokens_a.append(f"{d}:{da.domain_hash}")
        if db.status == STATUS_OK and isinstance(db.domain_hash, str) and db.domain_hash:
            tokens_b.append(f"{d}:{db.domain_hash}")

    metric_domain_hash_identity_jaccard = jaccard_set(tokens_a, tokens_b)

    # ----------------
    # Metric 2: domain_status_layout_jaccard
    # Jaccard on {domain:status} tokens, blocked excluded
    # ----------------
    status_tokens_a: List[str] = []
    status_tokens_b: List[str] = []

    for d in domain_names:
        da = fp_a.domains.get(d)
        db = fp_b.domains.get(d)

        if da is None or db is None or da.missing or db.missing:
            continue
        if da.unreadable or db.unreadable:
            continue
        if da.status == STATUS_BLOCKED or db.status == STATUS_BLOCKED:
            continue

        status_tokens_a.append(f"{d}:{da.status}")
        status_tokens_b.append(f"{d}:{db.status}")

    metric_domain_status_layout_jaccard = jaccard_set(status_tokens_a, status_tokens_b)

    # ----------------
    # Metric 3: signature_multiset_similarity
    # Per-domain multiset Jaccard over sig hashes (OK-only),
    # weighted by union mass across comparable domains.
    # Fallback: if signatures unavailable for a domain, use exact domain-hash equality (OK-only).
    # ----------------
    per_domain_scores: List[Tuple[float, int]] = []  # (score, weight)
    domains_compared_signatures = 0

    for d in domain_names:
        da = fp_a.domains.get(d)
        db = fp_b.domains.get(d)
        if da is None or db is None:
            continue
        if da.missing or db.missing:
            continue
        if da.unreadable or db.unreadable:
            continue
        if da.status == STATUS_BLOCKED or db.status == STATUS_BLOCKED:
            continue
        if da.status != STATUS_OK or db.status != STATUS_OK:
            continue

        if da.sig_hashes is not None and db.sig_hashes is not None:
            score = jaccard_multiset(Counter(da.sig_hashes), Counter(db.sig_hashes))
            if score.value is None:
                continue
            weight = _domain_union_mass(da.sig_hashes, db.sig_hashes)
            if weight is None:
                continue
            per_domain_scores.append((score.value, max(weight, 1)))
            domains_compared_signatures += 1
            continue

        if isinstance(da.domain_hash, str) and da.domain_hash and isinstance(db.domain_hash, str) and db.domain_hash:
            score_val = 1.0 if da.domain_hash == db.domain_hash else 0.0
            per_domain_scores.append((score_val, 1))
            domains_compared_signatures += 1
            continue

        continue

    if not per_domain_scores:
        metric_signature_multiset_similarity = SimilarityScalar(None, "no_comparable_domains_for_signature_multiset_similarity")
    else:
        num = sum(s * w for s, w in per_domain_scores)
        den = sum(w for _, w in per_domain_scores)
        metric_signature_multiset_similarity = SimilarityScalar(
            (num / den) if den else None,
            None if den else "undefined_den_zero",
        )

    note = None
    return SimilarityResult(
        file_a=fp_a.path,
        file_b=fp_b.path,

        domain_hash_identity_jaccard=metric_domain_hash_identity_jaccard,
        domain_status_layout_jaccard=metric_domain_status_layout_jaccard,
        signature_multiset_similarity=metric_signature_multiset_similarity,

        domains_total=domains_total,
        domains_compared_signatures=domains_compared_signatures,
        domains_undefined_blocked=undefined_blocked,
        domains_undefined_unreadable=undefined_unreadable,
        domains_missing=missing,
        note=note,
    )

@dataclass(frozen=True)
class DomainSigTopK:
    # Top-K sig_hash deltas for a domain (bounded)
    items: List[Tuple[str, int]]  # (sig_hash, count)


@dataclass(frozen=True)
class DomainDetail:
    domain: str
    status_a: str
    status_b: str

    comparable: bool
    reason: Optional[str]

    # Similarities (only defined when comparable)
    set_jaccard: SimilarityScalar
    multiset_jaccard: SimilarityScalar

    # Mass / overlap counters (only meaningful when comparable and sigs available)
    a_total: Optional[int]
    b_total: Optional[int]
    matched: Optional[int]
    added_in_b: Optional[int]
    removed_from_a: Optional[int]
    union_mass: Optional[int]

    # Bounded top-K lists (sig_hash only)
    top_matched: Optional[DomainSigTopK]
    top_added: Optional[DomainSigTopK]
    top_removed: Optional[DomainSigTopK]

    # Optional sig_hash -> label map (when available)
    sig_labels: Optional[Dict[str, str]] = None



@dataclass(frozen=True)
class SimilarityDetail:
    file_a: str
    file_b: str
    summary: SimilarityResult
    domains: List[DomainDetail]


def _topk_counter_items(c: Counter, top_k: int) -> List[Tuple[str, int]]:
    # Deterministic ordering: count desc, then sig_hash asc
    items = [(k, int(v)) for k, v in c.items() if isinstance(k, str) and k]
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return items[: max(0, int(top_k))]

def _extract_sig_labels(domain_payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Return mapping sig_hash -> label/name if available.

    Supported layouts (preferred first):
      - domain_payload["record_rows"]: list of dicts containing {"sig_hash": "...", "name": "..."} (or label/type_name)
      - domain_payload["data"]["record_rows"]: same nested under "data"

    Safe: returns None if schema does not support labels.
    """
    def _from_record_rows(rr: Any) -> Optional[Dict[str, str]]:
        if not isinstance(rr, list):
            return None
        out: Dict[str, str] = {}
        for item in rr:
            if not isinstance(item, dict):
                continue
            sig = item.get("sig_hash")
            name = item.get("name") or item.get("label") or item.get("type_name")
            if isinstance(sig, str) and sig and isinstance(name, str) and name:
                # If duplicates occur, preserve first deterministically (input order is stable in JSON)
                if sig not in out:
                    out[sig] = name
        return out if out else None

    labels = _from_record_rows(domain_payload.get("record_rows"))
    if labels:
        return labels

    data = domain_payload.get("data")
    if isinstance(data, dict):
        labels = _from_record_rows(data.get("record_rows"))
        if labels:
            return labels

    return None


def _compare_domain_signatures(domain: str, da: DomainData, db: DomainData, top_k: int) -> DomainDetail:
    # Non-negotiable invariants: blocked > degraded > ok; unreadable != missing; no silent failure.

    if da.missing or db.missing:
        return DomainDetail(
            domain=domain,
            status_a=da.status,
            status_b=db.status,
            comparable=False,
            reason="domain_missing_payload",
            set_jaccard=SimilarityScalar(None, "domain_missing_payload"),
            multiset_jaccard=SimilarityScalar(None, "domain_missing_payload"),
            a_total=None,
            b_total=None,
            matched=None,
            added_in_b=None,
            removed_from_a=None,
            union_mass=None,
            top_matched=None,
            top_added=None,
            top_removed=None,
        )

    if da.unreadable or db.unreadable:
        return DomainDetail(
            domain=domain,
            status_a=da.status,
            status_b=db.status,
            comparable=False,
            reason="domain_unreadable_payload",
            set_jaccard=SimilarityScalar(None, "domain_unreadable_payload"),
            multiset_jaccard=SimilarityScalar(None, "domain_unreadable_payload"),
            a_total=None,
            b_total=None,
            matched=None,
            added_in_b=None,
            removed_from_a=None,
            union_mass=None,
            top_matched=None,
            top_added=None,
            top_removed=None,
        )

    if da.status == STATUS_BLOCKED or db.status == STATUS_BLOCKED:
        return DomainDetail(
            domain=domain,
            status_a=da.status,
            status_b=db.status,
            comparable=False,
            reason="domain_blocked",
            set_jaccard=SimilarityScalar(None, "domain_blocked"),
            multiset_jaccard=SimilarityScalar(None, "domain_blocked"),
            a_total=None,
            b_total=None,
            matched=None,
            added_in_b=None,
            removed_from_a=None,
            union_mass=None,
            top_matched=None,
            top_added=None,
            top_removed=None,
        )

    # Conservative: only compare records when both OK
    if da.status != STATUS_OK or db.status != STATUS_OK:
        return DomainDetail(
            domain=domain,
            status_a=da.status,
            status_b=db.status,
            comparable=False,
            reason="domain_not_ok",
            set_jaccard=SimilarityScalar(None, "domain_not_ok"),
            multiset_jaccard=SimilarityScalar(None, "domain_not_ok"),
            a_total=None,
            b_total=None,
            matched=None,
            added_in_b=None,
            removed_from_a=None,
            union_mass=None,
            top_matched=None,
            top_added=None,
            top_removed=None,
        )

    if da.sig_hashes is None or db.sig_hashes is None:
        # Explicitly undefined for record-level questions
        return DomainDetail(
            domain=domain,
            status_a=da.status,
            status_b=db.status,
            comparable=False,
            reason="missing_signature_hashes",
            set_jaccard=SimilarityScalar(None, "missing_signature_hashes"),
            multiset_jaccard=SimilarityScalar(None, "missing_signature_hashes"),
            a_total=None,
            b_total=None,
            matched=None,
            added_in_b=None,
            removed_from_a=None,
            union_mass=None,
            top_matched=None,
            top_added=None,
            top_removed=None,
        )

    ca = Counter(da.sig_hashes)
    cb = Counter(db.sig_hashes)

    set_sim = jaccard_set(ca.keys(), cb.keys())
    ms_sim = jaccard_multiset(ca, cb)

    # Overlap counters
    keys = set(ca.keys()) | set(cb.keys())
    matched = 0
    union_mass = 0
    added = Counter()
    removed = Counter()
    common = Counter()

    for k in keys:
        a = int(ca.get(k, 0))
        b = int(cb.get(k, 0))
        matched_k = min(a, b)
        matched += matched_k
        union_mass += max(a, b)
        if matched_k > 0:
            common[k] = matched_k
        if b > a:
            added[k] = b - a
        if a > b:
            removed[k] = a - b

    labels = da.sig_labels or db.sig_labels

    return DomainDetail(
        domain=domain,
        status_a=da.status,
        status_b=db.status,
        comparable=True,
        reason=None,
        set_jaccard=set_sim,
        multiset_jaccard=ms_sim,
        a_total=sum(int(v) for v in ca.values()),
        b_total=sum(int(v) for v in cb.values()),
        matched=matched,
        added_in_b=sum(int(v) for v in added.values()),
        removed_from_a=sum(int(v) for v in removed.values()),
        union_mass=union_mass,
        top_matched=DomainSigTopK(_topk_counter_items(common, top_k)),
        top_added=DomainSigTopK(_topk_counter_items(added, top_k)),
        top_removed=DomainSigTopK(_topk_counter_items(removed, top_k)),
        sig_labels=labels,
    )



def compare_two_detailed(fp_a: FingerprintData, fp_b: FingerprintData, top_k: int, domain_filter: Optional[set]) -> SimilarityDetail:
    summary = compare_two(fp_a, fp_b)

    # If unreadable, details are empty but explicit
    if not fp_a.ok or not fp_b.ok:
        return SimilarityDetail(file_a=fp_a.path, file_b=fp_b.path, summary=summary, domains=[])

    domain_names = sorted(set(fp_a.domains.keys()) | set(fp_b.domains.keys()))
    if domain_filter:
        domain_names = [d for d in domain_names if d in domain_filter]

    domains: List[DomainDetail] = []
    for d in domain_names:
        da = fp_a.domains.get(d)
        db = fp_b.domains.get(d)
        if da is None or db is None:
            # Explicit: present in one file only
            status_a = da.status if da else STATUS_BLOCKED
            status_b = db.status if db else STATUS_BLOCKED
            domains.append(DomainDetail(
                domain=d,
                status_a=status_a,
                status_b=status_b,
                comparable=False,
                reason="domain_missing_in_one_file",
                set_jaccard=SimilarityScalar(None, "domain_missing_in_one_file"),
                multiset_jaccard=SimilarityScalar(None, "domain_missing_in_one_file"),
                a_total=None,
                b_total=None,
                matched=None,
                added_in_b=None,
                removed_from_a=None,
                union_mass=None,
                top_matched=None,
                top_added=None,
                top_removed=None,
            ))
            continue

        domains.append(_compare_domain_signatures(d, da, db, top_k))

    # Helpful deterministic ordering for “which domains matter”
    # (most divergent first, then heavier domains first, then name)
    def _sort_key(dd: DomainDetail):
        sim = dd.multiset_jaccard.value
        sim_key = 1.0 if sim is None else float(sim)
        mass = 0 if dd.union_mass is None else int(dd.union_mass)
        return (sim_key, -mass, dd.domain)

    domains.sort(key=_sort_key)
    return SimilarityDetail(file_a=fp_a.path, file_b=fp_b.path, summary=summary, domains=domains)


def write_details_json(details: List[SimilarityDetail], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    payload = []
    for d in details:
        doms = []
        for dd in d.domains:
            doms.append({
                "domain": dd.domain,
                "status_a": dd.status_a,
                "status_b": dd.status_b,
                "comparable": dd.comparable,
                "reason": dd.reason,
                "set_jaccard": {"value": dd.set_jaccard.value, "reason": dd.set_jaccard.reason},
                "multiset_jaccard": {"value": dd.multiset_jaccard.value, "reason": dd.multiset_jaccard.reason},
                "a_total": dd.a_total,
                "b_total": dd.b_total,
                "matched": dd.matched,
                "added_in_b": dd.added_in_b,
                "removed_from_a": dd.removed_from_a,
                "union_mass": dd.union_mass,
                "top_matched": (dd.top_matched.items if dd.top_matched else None),
                "top_added": (dd.top_added.items if dd.top_added else None),
                "top_removed": (dd.top_removed.items if dd.top_removed else None),
                "sig_labels": dd.sig_labels,
            })


        payload.append({
            "file_a": d.file_a,
            "file_b": d.file_b,
            "summary": {
                "domain_hash_identity_jaccard": {
                    "value": d.summary.domain_hash_identity_jaccard.value,
                    "reason": d.summary.domain_hash_identity_jaccard.reason,
                },
                "domain_status_layout_jaccard": {
                    "value": d.summary.domain_status_layout_jaccard.value,
                    "reason": d.summary.domain_status_layout_jaccard.reason,
                },
                "signature_multiset_similarity": {
                    "value": d.summary.signature_multiset_similarity.value,
                    "reason": d.summary.signature_multiset_similarity.reason,
                },
                "domains_total": d.summary.domains_total,
                "domains_compared_signatures": d.summary.domains_compared_signatures,
                "domains_undefined_blocked": d.summary.domains_undefined_blocked,
                "domains_undefined_unreadable": d.summary.domains_undefined_unreadable,
                "domains_missing": d.summary.domains_missing,
                "note": d.summary.note,
            },
            "domains": doms,
        })

    Path(out_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

# -----------------------------
# Batch drivers
# -----------------------------

def find_json_files(dir_path: str) -> List[str]:
    p = Path(dir_path)
    if not p.exists() or not p.is_dir():
        raise ValueError(f"not_a_directory: {dir_path}")

    # Non-recursive by design: only the parent folder contents.
    # This prevents picking up drift/pairwise outputs stored in subfolders.
    files = [str(x) for x in p.glob("*.json") if x.is_file()]
    files.sort()
    return files


def write_csv(results: List[SimilarityResult], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "file_a",
            "file_b",

            "domain_hash_identity_jaccard_value",
            "domain_hash_identity_jaccard_reason",

            "domain_status_layout_jaccard_value",
            "domain_status_layout_jaccard_reason",

            "signature_multiset_similarity_value",
            "signature_multiset_similarity_reason",

            "domains_total",
            "domains_compared_signatures",
            "domains_undefined_blocked",
            "domains_undefined_unreadable",
            "domains_missing",
            "note",
        ])
        for r in results:
            w.writerow([
                r.file_a,
                r.file_b,

                "" if r.domain_hash_identity_jaccard.value is None else f"{r.domain_hash_identity_jaccard.value:.6f}",
                r.domain_hash_identity_jaccard.reason or "",

                "" if r.domain_status_layout_jaccard.value is None else f"{r.domain_status_layout_jaccard.value:.6f}",
                r.domain_status_layout_jaccard.reason or "",

                "" if r.signature_multiset_similarity.value is None else f"{r.signature_multiset_similarity.value:.6f}",
                r.signature_multiset_similarity.reason or "",

                r.domains_total,
                r.domains_compared_signatures,
                r.domains_undefined_blocked,
                r.domains_undefined_unreadable,
                r.domains_missing,
                r.note or "",
            ])


def write_json(results: List[SimilarityResult], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    payload = []
    for r in results:
        payload.append({
            "file_a": r.file_a,
            "file_b": r.file_b,

            "domain_hash_identity_jaccard": {
                "value": r.domain_hash_identity_jaccard.value,
                "reason": r.domain_hash_identity_jaccard.reason,
            },
            "domain_status_layout_jaccard": {
                "value": r.domain_status_layout_jaccard.value,
                "reason": r.domain_status_layout_jaccard.reason,
            },
            "signature_multiset_similarity": {
                "value": r.signature_multiset_similarity.value,
                "reason": r.signature_multiset_similarity.reason,
            },

            "domains_total": r.domains_total,
            "domains_compared_signatures": r.domains_compared_signatures,
            "domains_undefined_blocked": r.domains_undefined_blocked,
            "domains_undefined_unreadable": r.domains_undefined_unreadable,
            "domains_missing": r.domains_missing,
            "note": r.note,
        })
    Path(out_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=False, help="Baseline fingerprint JSON path")
    ap.add_argument("--dir", required=True, help="Parent directory containing fingerprint JSON files (non-recursive)")
    ap.add_argument("--mode", choices=["baseline", "pairwise", "both"], default="both")

    # Output base is always --dir/similarity unless user provides absolute paths.
    ap.add_argument("--out_baseline", default="baseline_vs_dir.csv", help="Baseline-vs-dir CSV (relative resolves under --dir/similarity)")
    ap.add_argument("--out_baseline_json", default=None, help="Baseline-vs-dir JSON (relative resolves under --dir/similarity)")
    ap.add_argument("--out_pairwise", default="pairwise.csv", help="Pairwise CSV (relative resolves under --dir/similarity)")
    ap.add_argument("--out_pairwise_json", default=None, help="Pairwise JSON (relative resolves under --dir/similarity)")

    # Details output (optional)
    ap.add_argument("--details_json", default=None, help="Optional details JSON (relative resolves under --dir/similarity)")
    ap.add_argument("--details_top_k", type=int, default=50, help="Top-K sig_hash deltas per domain (details only)")
    ap.add_argument("--details_domain_filter", default=None, help="Comma-separated domain list to include in details (optional)")

    ap.add_argument("--include_baseline_in_pairwise", action="store_true")

    args = ap.parse_args()

    # Discover only direct children JSONs (non-recursive)
    json_files = find_json_files(args.dir)

    fps: Dict[str, FingerprintData] = {}
    for fpath in json_files:
        fps[fpath] = load_fingerprint(fpath)

    out_base = Path(args.dir) / "similarity"
    out_base.mkdir(parents=True, exist_ok=True)

    def _resolve_out(p: Optional[str]) -> Optional[str]:
        if not p:
            return None
        pp = Path(p)
        if not pp.is_absolute():
            pp = out_base / pp
        return str(pp)

    out_baseline_csv = _resolve_out(args.out_baseline)
    out_baseline_json = _resolve_out(args.out_baseline_json)
    out_pairwise_csv = _resolve_out(args.out_pairwise)
    out_pairwise_json = _resolve_out(args.out_pairwise_json)

    details_json = _resolve_out(args.details_json)

    domain_filter = None
    if args.details_domain_filter:
        domain_filter = set([x.strip() for x in args.details_domain_filter.split(",") if x.strip()])

    baseline_results: List[SimilarityResult] = []
    pairwise_results: List[SimilarityResult] = []
    details: List[SimilarityDetail] = []

    base_fp = None
    if args.mode in ("baseline", "both"):
        if not args.baseline:
            raise SystemExit("--baseline is required for mode=baseline or both")
        base_fp = load_fingerprint(args.baseline)

        for fpath in json_files:
            if os.path.abspath(fpath) == os.path.abspath(args.baseline):
                continue
            r = compare_two(base_fp, fps[fpath])
            baseline_results.append(r)
            if details_json:
                details.append(compare_two_detailed(base_fp, fps[fpath], top_k=args.details_top_k, domain_filter=domain_filter))

    if args.mode in ("pairwise", "both"):
        files_for_pairwise = list(json_files)

        if args.include_baseline_in_pairwise and args.baseline:
            if args.baseline not in files_for_pairwise:
                files_for_pairwise.append(args.baseline)
                fps[args.baseline] = load_fingerprint(args.baseline)
            files_for_pairwise.sort()

        for a, b in itertools.combinations(files_for_pairwise, 2):
            r = compare_two(fps[a], fps[b])
            pairwise_results.append(r)
            if details_json:
                details.append(compare_two_detailed(fps[a], fps[b], top_k=args.details_top_k, domain_filter=domain_filter))

    # Write outputs named by what they contain
    if args.mode in ("baseline", "both"):
        write_csv(baseline_results, str(out_baseline_csv))
        if out_baseline_json:
            write_json(baseline_results, out_baseline_json)
        print(f"Wrote {len(baseline_results)} baseline comparisons to: {out_baseline_csv}")
        if out_baseline_json:
            print(f"Wrote baseline JSON to: {out_baseline_json}")

    if args.mode in ("pairwise", "both"):
        write_csv(pairwise_results, str(out_pairwise_csv))
        if out_pairwise_json:
            write_json(pairwise_results, out_pairwise_json)
        print(f"Wrote {len(pairwise_results)} pairwise comparisons to: {out_pairwise_csv}")
        if out_pairwise_json:
            print(f"Wrote pairwise JSON to: {out_pairwise_json}")

    if details_json:
        write_details_json(details, details_json)
        print(f"Wrote details JSON to: {details_json}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
