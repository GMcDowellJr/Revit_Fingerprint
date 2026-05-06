# join_key_derivation.py
# Phase 0.5: derive join_hash AFTER export (from record.v2 identity_basis.items) using join-key policy.
#
# Usage (example):
#   python join_key_derivation.py \
#     --details "C:\path\to\details\**\details_*.json" \
#     --policy  "C:\path\to\policies\domain_join_key_policies.json" \
#     --run-id  "2026-02-05T1200Z" \
#     --out-dir "C:\path\to\out" \
#     --mode    validate
#
# Outputs:
#   - record_join_keys.csv
#   - record_join_key_items.csv
#
# Notes:
# - Hash contract is md5_utf8_join_pipe with preimage line format:
#     "k=<k>|q=<q>|v=<v_or_empty>"
#   sorted by k, joined by "\n", MD5(UTF8).
# - Evidence source is strictly rec["identity_basis"]["items"].

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ----------------------------
# Hash contract: md5_utf8_join_pipe
# ----------------------------

def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def stable_serialize_value(v: Any) -> str:
    # Identity items SHOULD already be canonical strings or None.
    # Still, be defensive for test runs.
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

def serialize_identity_items(items: Sequence[Dict[str, Any]]) -> List[str]:
    # Matches record_v2.serialize_identity_items semantics:
    # - sorts by k only
    # - includes q in preimage
    # - v None => empty string
    if not isinstance(items, (list, tuple)):
        raise TypeError("items must be a list/tuple of dicts")

    def _k(it: Dict[str, Any]) -> str:
        try:
            return str(it.get("k", ""))
        except Exception:
            return ""

    out: List[str] = []
    for it in sorted(items, key=_k):
        k = safe_str(it.get("k"))
        q = safe_str(it.get("q"))
        v = it.get("v", None)
        v_or_empty = "" if v is None else safe_str(v)
        out.append(f"k={k}|q={q}|v={v_or_empty}")
    return out

def md5_utf8_join_pipe(lines: Sequence[str]) -> str:
    preimage = "|".join(lines)
    return hashlib.md5(preimage.encode("utf-8")).hexdigest()


# ----------------------------
# Policy loading (tolerant)
# ----------------------------

@dataclass(frozen=True)
class JoinKeyPolicy:
    domain: str
    version: str
    required_keys: Tuple[str, ...]
    optional_keys: Tuple[str, ...]
    include_optional_when_present: bool
    usable_q_required: Tuple[str, ...]
    usable_q_optional: Tuple[str, ...]
    shape_gating: Dict[str, Any]

def _as_str_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return [str(i) for i in x if i is not None]
    # accept single string
    if isinstance(x, str):
        return [x]
    return [str(x)]

def load_join_key_policies(policy_path: str) -> Dict[str, JoinKeyPolicy]:
    with open(policy_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Accept either:
    #   { "domainA": { ...policy... }, "domainB": { ... } }
    # or:
    #   { "policies": { "domainA": {...}, ... } }
    if isinstance(raw, dict):
        if "domains" in raw and isinstance(raw["domains"], dict):
            raw_pols = raw["domains"]
        elif "policies" in raw and isinstance(raw["policies"], dict):
            raw_pols = raw["policies"]
        else:
            raw_pols = raw
    else:
        raw_pols = {}

    out: Dict[str, JoinKeyPolicy] = {}
    for domain, p in (raw_pols or {}).items():
        if not isinstance(p, dict):
            continue

        version = str(
            p.get("join_key_policy_version")
            or p.get("policy_version")
            or p.get("version")
            or "unspecified"
        )

        required_keys = tuple(
            _as_str_list(
                p.get("required_items")
                or p.get("required_keys")
                or p.get("required")
                or []
            )
        )

        optional_keys = tuple(
            _as_str_list(
                p.get("optional_items")
                or p.get("optional_keys")
                or p.get("optional")
                or []
            )
        )

        include_optional_when_present = bool(
            p.get("include_optional_when_present")
            if p.get("include_optional_when_present") is not None
            else p.get("include_optional", False)
        )

        # q-allow lists are optional; default to {"ok"}.
        usable_q_required = tuple(_as_str_list(p.get("usable_q_required") or p.get("required_usable_q") or ["ok"]))
        usable_q_optional = tuple(_as_str_list(p.get("usable_q_optional") or p.get("optional_usable_q") or ["ok"]))

        shape_gating = (
            p.get("shape_gating")
            if isinstance(p.get("shape_gating"), dict)
            else {}
        )

        out[str(domain)] = JoinKeyPolicy(
            domain=str(domain),
            version=version,
            required_keys=required_keys,
            optional_keys=optional_keys,
            include_optional_when_present=include_optional_when_present,
            usable_q_required=usable_q_required,
            usable_q_optional=usable_q_optional,
            shape_gating=shape_gating,
        )

    return out


# ----------------------------
# Selection logic
# ----------------------------

def index_items_by_k(items: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for it in items or []:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not k:
            continue
        k2 = str(k)
        out.setdefault(k2, []).append({"k": k2, "q": it.get("q"), "v": it.get("v")})
    return out

def choose_candidate_deterministically(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Deterministic tie-breaker when extractor emitted duplicates for same k.
    return sorted(
        candidates,
        key=lambda it: (safe_str(it.get("q")), stable_serialize_value(it.get("v")))
    )[0]

def is_usable_q(q: Any, allowed: Iterable[str]) -> bool:
    return safe_str(q) in set(str(x) for x in allowed)

def choose_record_handle(rec: Dict[str, Any]) -> str:
    uid = rec.get("record_uid")
    if uid:
        return str(uid)
    rid = rec.get("record_id")
    return str(rid) if rid is not None else ""

def select_items_for_policy(
    items_by_k: Dict[str, List[Dict[str, Any]]],
    policy: JoinKeyPolicy,
    policy_mode: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    reasons: Dict[str, Any] = {
        "blocked": False,
        "policy_mode": policy_mode,
        "missing_required_keys": [],
        "unusable_required_keys": [],   # [{k,q}]
        "duplicate_key_items": [],      # [{k,count}]
        "shape_gating": {
            "discriminator_key": None,
            "shape_value": None,
            "shape_matched": False,
            "additional_required_keys": [],
        },
    }

    # Start with base policy keys
    required_keys = list(policy.required_keys)
    optional_keys = list(policy.optional_keys)

    # ---- shape gating (policy.shape_gating) ----
    sg = policy.shape_gating or {}
    disc_k = sg.get("discriminator_key")
    shape_reqs = sg.get("shape_requirements") if isinstance(sg.get("shape_requirements"), dict) else {}
    default_behavior = sg.get("default_shape_behavior", "common_only")

    if disc_k:
        reasons["shape_gating"]["discriminator_key"] = disc_k
        disc_candidates = items_by_k.get(str(disc_k), [])
        if disc_candidates:
            disc_item = choose_candidate_deterministically(disc_candidates)
            shape_val = safe_str(disc_item.get("v"))
            reasons["shape_gating"]["shape_value"] = shape_val

            if shape_val in shape_reqs and isinstance(shape_reqs[shape_val], dict):
                add_req = _as_str_list(shape_reqs[shape_val].get("additional_required"))
                add_opt = _as_str_list(shape_reqs[shape_val].get("additional_optional"))
                reasons["shape_gating"]["shape_matched"] = True
                reasons["shape_gating"]["additional_required_keys"] = add_req

                # Merge gated keys
                required_keys.extend(add_req)
                optional_keys.extend(add_opt)
            else:
                # default_shape_behavior currently only supports "common_only" (no additions)
                if default_behavior != "common_only":
                    # Keep conservative behavior: treat unknown behavior as common_only
                    pass

    selected: List[Dict[str, Any]] = []

    # REQUIRED KEYS:
    # - If key absent entirely => blocking and cannot hash it.
    # - If present but unusable q => still include in selected for hashing (to match exporter),
    #   but record is blocked.
    for k in required_keys:
        k = str(k)
        cands = items_by_k.get(k, [])
        if not cands:
            reasons["missing_required_keys"].append(k)
            continue

        if len(cands) > 1:
            reasons["duplicate_key_items"].append({"k": k, "count": len(cands)})

        chosen = choose_candidate_deterministically(cands)

        if not is_usable_q(chosen.get("q"), policy.usable_q_required):
            reasons["unusable_required_keys"].append({"k": k, "q": chosen.get("q")})

        # Include regardless of usability so the hash preimage matches exporter-time join_key behavior
        selected.append(chosen)

    # OPTIONAL KEYS (include only if present+usable)
    if policy.include_optional_when_present:
        for k in optional_keys:
            k = str(k)
            cands = items_by_k.get(k, [])
            if not cands:
                continue

            if len(cands) > 1:
                reasons["duplicate_key_items"].append({"k": k, "count": len(cands)})

            chosen = choose_candidate_deterministically(cands)
            if is_usable_q(chosen.get("q"), policy.usable_q_optional):
                selected.append(chosen)

    # Stable ordering by k
    selected.sort(key=lambda it: safe_str(it.get("k")))

    # Block logic:
    # - Missing required => blocked and join_hash should be empty (cannot include key without sentinels)
    # - Unusable required => blocked (but hash can still be computed from selected)
    if reasons["missing_required_keys"] or reasons["unusable_required_keys"]:
        reasons["blocked"] = True
    if policy_mode == "harsh" and reasons["duplicate_key_items"]:
        reasons["blocked"] = True

    return selected, reasons


    selected: List[Dict[str, Any]] = []

    # Required keys
    for k in policy.required_keys:
        cands = items_by_k.get(k, [])
        if not cands:
            reasons["missing_required_keys"].append(k)
            continue

        if len(cands) > 1:
            reasons["duplicate_key_items"].append({"k": k, "count": len(cands)})

        chosen = choose_candidate_deterministically(cands)
        if not is_usable_q(chosen.get("q"), policy.usable_q_required):
            reasons["unusable_required_keys"].append({"k": k, "q": chosen.get("q")})
            continue

        selected.append(chosen)

    # Optional keys (never blocking)
    if policy.include_optional_when_present:
        for k in policy.optional_keys:
            cands = items_by_k.get(k, [])
            if not cands:
                continue

            if len(cands) > 1:
                reasons["duplicate_key_items"].append({"k": k, "count": len(cands)})

            chosen = choose_candidate_deterministically(cands)
            if is_usable_q(chosen.get("q"), policy.usable_q_optional):
                selected.append(chosen)

    # Deterministic ordering by k (matches serialize_identity_items)
    selected.sort(key=lambda it: safe_str(it.get("k")))

    # Block logic
    if reasons["missing_required_keys"] or reasons["unusable_required_keys"]:
        reasons["blocked"] = True
    if policy_mode == "harsh" and reasons["duplicate_key_items"]:
        reasons["blocked"] = True

    return selected, reasons


# ----------------------------
# Details JSON reading (tolerant)
# ----------------------------

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def extract_file_id(payload: Any, fallback_from_filename: str) -> str:
    # Try common meta locations; fall back to basename.
    if isinstance(payload, dict):
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        for k in ("file_id", "file.guid", "file_guid", "guid"):
            if k in meta and meta[k]:
                return str(meta[k])
        if "file_id" in payload and payload["file_id"]:
            return str(payload["file_id"])
        if "file" in payload and isinstance(payload["file"], dict):
            for k in ("id", "file_id", "guid"):
                if payload["file"].get(k):
                    return str(payload["file"][k])
    return os.path.splitext(os.path.basename(fallback_from_filename))[0]

def extract_records(payload):
    """
    Return all record.v2 dicts found anywhere in a fingerprint.details.json payload.
    (The details export nests record.v2 objects under per-domain blocks.)
    """
    records = []

    def walk(x):
        if isinstance(x, dict):
            # record.v2 object
            if x.get("schema_version") == "record.v2":
                records.append(x)
                return
            # keep walking
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(payload)
    return records


# ----------------------------
# Main derivation
# ----------------------------

def derive_join_keys(details_json_paths: List[str], policy_path: str, analysis_run_id: str, output_dir: str, policy_mode: str) -> None:
    policies = load_join_key_policies(policy_path)

    rows_keys: List[Dict[str, Any]] = []
    rows_items: List[Dict[str, Any]] = []

    for path in details_json_paths:
        payload = read_json(path)
        file_id = extract_file_id(payload, fallback_from_filename=path)

        for rec in extract_records(payload):
            domain = safe_str(rec.get("domain"))
            record_handle = choose_record_handle(rec)

            policy = policies.get(domain)
            if policy is None:
                rows_keys.append({
                    "analysis_run_id": analysis_run_id,
                    "file_id": file_id,
                    "domain": domain,
                    "record_handle": record_handle,
                    "join_key_policy_version": "",
                    "join_hash": "",
                    "join_key_status": "blocked",
                    "join_key_reasons_json": json.dumps({"blocked": True, "missing_policy": True, "policy_mode": policy_mode}, separators=(",", ":"), sort_keys=True),
                })
                continue

            identity_items = rec.get("identity_basis", {}).get("items", [])
            items_by_k = index_items_by_k(identity_items)

            selected_items, reasons = select_items_for_policy(items_by_k, policy, policy_mode)

            # Hashing rule:
            # - If ANY required key is absent entirely, we cannot hash without inventing sentinels -> join_hash empty.
            # - Otherwise compute join_hash from selected_items even if blocked due to unusable q,
            #   to match exporter-time join_key behavior.
            if reasons.get("missing_required_keys"):
                join_hash = ""
            else:
                preimage_lines = serialize_identity_items(selected_items)
                join_hash = md5_utf8_join_pipe(preimage_lines)

            status = "blocked" if reasons["blocked"] else "ok"
            reasons_json = "" if status == "ok" else json.dumps(reasons, separators=(",", ":"), sort_keys=True)


            rows_keys.append({
                "analysis_run_id": analysis_run_id,
                "file_id": file_id,
                "domain": domain,
                "record_handle": record_handle,
                "join_key_policy_version": policy.version,
                "join_hash": join_hash,
                "join_key_status": status,
                "join_key_reasons_json": reasons_json,
            })

            for it in selected_items:
                rows_items.append({
                    "analysis_run_id": analysis_run_id,
                    "file_id": file_id,
                    "domain": domain,
                    "record_handle": record_handle,
                    "join_key_policy_version": policy.version,
                    "join_hash": join_hash,
                    "k": safe_str(it.get("k")),
                    "q": safe_str(it.get("q")),
                    "v": stable_serialize_value(it.get("v")),
                })

    os.makedirs(output_dir, exist_ok=True)
    write_csv(os.path.join(output_dir, "record_join_keys.csv"), rows_keys, [
        "analysis_run_id", "file_id", "domain", "record_handle",
        "join_key_policy_version", "join_hash", "join_key_status", "join_key_reasons_json"
    ])
    write_csv(os.path.join(output_dir, "record_join_key_items.csv"), rows_items, [
        "analysis_run_id", "file_id", "domain", "record_handle",
        "join_key_policy_version", "join_hash", "k", "q", "v"
    ])

def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames})


# ----------------------------
# CLI
# ----------------------------

def expand_globs(patterns: List[str]) -> List[str]:
    out: List[str] = []
    for p in patterns:
        hits = glob.glob(p, recursive=True)
        if hits:
            out.extend(hits)
        else:
            # allow literal paths too
            if os.path.isfile(p):
                out.append(p)
    # stable ordering
    out = sorted(set(out))
    return out

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--details", nargs="+", required=True, help="One or more JSON paths or glob patterns (use quotes).")
    ap.add_argument("--policy", required=True, help="Join-key policy JSON path.")
    ap.add_argument("--run-id", default="", help="Analysis run id (defaults to timestamp).")
    ap.add_argument("--out-dir", required=True, help="Output directory for CSVs.")
    ap.add_argument("--mode", default="validate", choices=["validate", "harsh"], help="validate blocks on missing/unusable required; harsh also blocks on duplicates.")
    args = ap.parse_args()

    details_paths = expand_globs(args.details)
    if not details_paths:
        raise SystemExit("No details JSON files matched the provided --details patterns.")

    from datetime import datetime, timezone

    run_id = args.run_id.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")

    derive_join_keys(
        details_json_paths=details_paths,
        policy_path=args.policy,
        analysis_run_id=run_id,
        output_dir=args.out_dir,
        policy_mode=args.mode,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
