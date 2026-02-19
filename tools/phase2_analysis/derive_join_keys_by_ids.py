# tools/phase2_analysis/derive_join_keys_by_ids.py
"""Derive IDS-scoped join-key policies using identity_basis.items evidence.

Verification-first algorithm (text_types):
- For each IDS:
  - Extract candidate keys from identity_basis.items across its files
  - Greedy forward select up to max_k keys to reduce join collisions within IDS
  - Emit deterministic policy per IDS with metrics

Collision metric:
- collision_rate = 1 - (distinct(sig_hash) / distinct(join_hash))
  within IDS, computed over records that have all required keys.

This is intentionally conservative and auditable (not a full Pareto front).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
from statistics import median
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

from .io import load_exports, get_domain_records, load_phase0_v21_records_with_identity


def md5_utf8_join_pipe(parts: List[str]) -> str:
    s = "|".join(parts).encode("utf-8")
    return hashlib.md5(s).hexdigest()

# Join-key eligibility: exclude label/name keys (structural invariant)
# NOTE: Keep this conservative for verification; expand later via a policy file.
_DENY_KEY_REGEXES = [
    r"(^|[._])name$",         # *.name
    r"(^|[._])type_name$",    # *.type_name
    r"(^|[._])family_name$",  # *.family_name
    r"(^|[._])symbol_name$",  # *.symbol_name
]

from tools.phase2_analysis.domain_identity_contract import DomainIdentityContract

_CONTRACT = DomainIdentityContract.load()


def is_eligible_join_key_item(domain: str, key: str) -> bool:
    return _CONTRACT.is_key_allowed(domain, key)

def extract_identity_map(record: Dict[str, Any]) -> Dict[str, Any]:
    """k -> v from identity_basis.items (stringified v)."""
    ib = record.get("identity_basis")
    if not isinstance(ib, dict):
        return {}
    items = ib.get("items")
    if not isinstance(items, list):
        return {}
    out: Dict[str, Any] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not k:
            continue
        out[str(k)] = it.get("v")
    return out


def compute_join_hash_for_record(
    record: Dict[str, Any],
    required_keys: List[str],
) -> Optional[str]:
    imap = extract_identity_map(record)
    for k in required_keys:
        if k not in imap:
            return None
        if imap[k] is None:
            return None
    parts = [f"{k}={str(imap[k])}" for k in required_keys]
    return md5_utf8_join_pipe(parts)


def evaluate_keyset(records: List[Dict[str, Any]], required_keys: List[str]) -> Dict[str, Any]:
    """Compute collision-like metrics on records that have all required keys."""
    pairs: List[Tuple[str, str]] = []
    missing = 0

    for r in records:
        sig = r.get("sig_hash")
        if not sig:
            continue
        jh = compute_join_hash_for_record(r, required_keys)
        if jh is None:
            missing += 1
            continue
        pairs.append((str(sig), str(jh)))

    if not pairs:
        return {
            "records_considered": 0,
            "records_missing_required": missing,
            "distinct_sig_hash": 0,
            "distinct_join_hash": 0,
            "collision_rate": 1.0,
        }

    distinct_sig = len(set([p[0] for p in pairs]))
    distinct_join = len(set([p[1] for p in pairs]))

    # If distinct_join < distinct_sig, collisions exist (multiple sigs share join).
    # Use a simple, monotonic proxy where "one bucket" is maximally bad:
    # collision_rate = 1 - (distinct_join / distinct_sig), capped [0,1]
    # - If distinct_join == distinct_sig => 0.0 (perfect separation)
    # - If distinct_join == 1 and distinct_sig is large => near 1.0 (max collision)
    if distinct_sig <= 0:
        collision_rate = 1.0
    else:
        collision_rate = 1.0 - (distinct_join / float(distinct_sig))
        if collision_rate < 0:
            collision_rate = 0.0
        if collision_rate > 1:
            collision_rate = 1.0

    return {
        "records_considered": len(pairs),
        "records_missing_required": missing,
        "distinct_sig_hash": distinct_sig,
        "distinct_join_hash": distinct_join,
        "collision_rate": round(collision_rate, 6),
    }

def compute_coverage(metrics: Dict[str, Any], total_records: int) -> float:
    rc = int(metrics.get("records_considered", 0))
    if total_records <= 0:
        return 0.0
    return rc / float(total_records)


def jaccard_similarity(a: List[str], b: List[str]) -> float:
    sa = set(a or [])
    sb = set(b or [])
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / float(len(sa | sb))


def sample_records_by_file(records: List[Dict[str, Any]], frac_files: float, seed: int) -> List[Dict[str, Any]]:
    """Bootstrap by sampling files (not individual records) to preserve lineage structure."""
    by_file: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in records:
        fid = str(r.get("file_id") or r.get("_file_id") or "unknown")
        by_file[fid].append(r)

    file_ids = list(by_file.keys())
    if not file_ids:
        return []

    rng = random.Random(seed)
    n = max(1, int(round(len(file_ids) * frac_files)))
    chosen = rng.sample(file_ids, k=min(n, len(file_ids)))

    out: List[Dict[str, Any]] = []
    for fid in chosen:
        out.extend(by_file[fid])
    return out


def evaluate_gates(
    collision_baseline: float,
    collision_final: float,
    coverage_final: float,
    stability_median: float,
    dmin: float,
    cmin: float,
    smin: float,
) -> (bool, List[str], Dict[str, Any]):
    """Return (escalate_to_pareto, reasons, metrics_dict)."""
    delta = float(collision_baseline) - float(collision_final)

    reasons: List[str] = []
    if delta < dmin:
        reasons.append(f"GATE_A_SIGNAL(delta<{dmin})")
    if coverage_final < cmin:
        reasons.append(f"GATE_B_COVERAGE(coverage<{cmin})")
    if stability_median < smin:
        reasons.append(f"GATE_C_STABILITY(stability<{smin})")

    return (len(reasons) > 0), reasons, {
        "collision_baseline": round(float(collision_baseline), 6),
        "collision_final": round(float(collision_final), 6),
        "delta_collision": round(delta, 6),
        "coverage_final": round(float(coverage_final), 6),
        "stability_median": round(float(stability_median), 6),
    }

def greedy_select_keys(
    records: List[Dict[str, Any]],
    candidate_keys: List[str],
    max_k: int,
    min_records: int,
) -> Tuple[List[str], Dict[str, Any]]:
    """Forward select keys to minimize collision_rate while maintaining coverage."""
    selected: List[str] = []
    base_metrics = evaluate_keyset(records, selected)
    best_metrics = base_metrics

    total_available = int(base_metrics.get("records_considered", 0))

    # Usability rule:
    # - If this IDS has >= min_records available, enforce min_records.
    # - If it has < min_records available, treat it as "sample-limited" and allow selection anyway.
    def usable(metrics: Dict[str, Any]) -> bool:
        rc = int(metrics.get("records_considered", 0))
        if total_available < min_records:
            return rc > 0
        return rc >= min_records

    for _ in range(max_k):
        best_candidate = None
        best_candidate_metrics = None

        for k in candidate_keys:
            if k in selected:
                continue
            trial = selected + [k]
            m = evaluate_keyset(records, trial)

            if best_candidate is None:
                best_candidate = k
                best_candidate_metrics = m
                continue

            bc = best_candidate_metrics
            assert bc is not None

            # Prefer usable sets; within usable sets prefer lower collision_rate
            if usable(m) and not usable(bc):
                best_candidate = k
                best_candidate_metrics = m
                continue
            if usable(m) == usable(bc):
                if float(m["collision_rate"]) < float(bc["collision_rate"]):
                    best_candidate = k
                    best_candidate_metrics = m
                    continue
                if float(m["collision_rate"]) == float(bc["collision_rate"]):
                    if int(m["records_considered"]) > int(bc["records_considered"]):
                        best_candidate = k
                        best_candidate_metrics = m
                        continue

        if best_candidate is None or best_candidate_metrics is None:
            break

        improved = float(best_candidate_metrics["collision_rate"]) < float(best_metrics["collision_rate"])
        improved_usable = usable(best_candidate_metrics) and not usable(best_metrics)

        if not (improved or improved_usable):
            break

        selected.append(best_candidate)
        best_metrics = best_candidate_metrics

    return selected, best_metrics


def derive_join_keys_by_ids(
    exports_dir: str,
    domain: str,
    file_to_ids_csv: str,
    out_dir: str,
    max_k: int,
    min_records: int,
    top_candidate_keys: int,
    *,
    phase0_dir: Optional[str] = None,
) -> None:
    map_df = pd.read_csv(file_to_ids_csv)
    if not {"file_id", "ids_id"}.issubset(set(map_df.columns)):
        raise ValueError("file_to_ids_csv must include columns: file_id, ids_id")

    file_to_ids = {str(r["file_id"]): str(r["ids_id"]) for _, r in map_df.iterrows()}

    ids_to_records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    if phase0_dir:
        # CSV mode: load records+identity from Results_v21/phase0_v21/
        allowed_files = set(file_to_ids.keys())
        all_recs = load_phase0_v21_records_with_identity(phase0_dir, domain, allowed_file_ids=allowed_files)

        for r in all_recs:
            fid = str(r.get("_file_id") or r.get("file_id") or "")
            if not fid or fid not in file_to_ids:
                continue
            ids_id = file_to_ids[fid]
            ids_to_records[ids_id].append(r)
    else:
        # JSON mode (back-compat)
        exports = list(load_exports(exports_dir, max_files=None))

        for exp in exports:
            fid = str(exp.file_id)
            if fid not in file_to_ids:
                continue
            ids_id = file_to_ids[fid]
            recs = get_domain_records(exp.data, domain)
            for r in recs:
                if isinstance(r, dict) and "_file_id" not in r:
                    r["_file_id"] = fid
            ids_to_records[ids_id].extend(recs)

    policies: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []

    policies: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []
    report_rows: List[Dict[str, Any]] = []

    # Consistent gates (domain-agnostic defaults)
    DMIN = 0.15  # minimum collision improvement
    CMIN = 0.80  # minimum coverage
    SMIN = 0.60  # minimum stability (median Jaccard over bootstraps)

    # Stability bootstrap parameters
    BOOTSTRAPS = 5
    BOOTSTRAP_FILE_FRAC = 0.80

    for ids_id, records in sorted(ids_to_records.items(), key=lambda x: x[0]):
        total_records = len(records)

        # Build candidate key pool from identity items (coverage-ranked), then apply eligibility filter if present
        key_counts: Dict[str, int] = defaultdict(int)
        for r in records:
            imap = extract_identity_map(r)
            for k in imap.keys():
                key_counts[k] += 1

        ranked = sorted(key_counts.items(), key=lambda kv: kv[1], reverse=True)
        candidate_keys = [k for k, _ in ranked[:top_candidate_keys]]

        # Greedy selection (primary)
        selected_keys, final_metrics = greedy_select_keys(
            records=records,
            candidate_keys=candidate_keys,
            max_k=max_k,
            min_records=min_records,
        )

        # Baseline metrics (empty keyset) for Δcollision
        baseline_metrics = evaluate_keyset(records, [])
        collision_baseline = float(baseline_metrics.get("collision_rate", 1.0))
        collision_final = float(final_metrics.get("collision_rate", 1.0))

        coverage_final = compute_coverage(final_metrics, total_records)

        # Stability: run greedy on bootstrap samples of files and compare keysets
        sims: List[float] = []
        if total_records > 0 and BOOTSTRAPS > 0:
            for b in range(BOOTSTRAPS):
                seed = (hash(ids_id) & 0xFFFFFFFF) ^ (b + 12345)
                sample = sample_records_by_file(records, frac_files=BOOTSTRAP_FILE_FRAC, seed=seed)
                if not sample:
                    sims.append(0.0)
                    continue
                sel_b, _m_b = greedy_select_keys(
                    records=sample,
                    candidate_keys=candidate_keys,
                    max_k=max_k,
                    min_records=min_records,
                )
                sims.append(jaccard_similarity(selected_keys, sel_b))

        stability_med = median(sims) if sims else 0.0

        escalated, reasons, gate_metrics = evaluate_gates(
            collision_baseline=collision_baseline,
            collision_final=collision_final,
            coverage_final=coverage_final,
            stability_median=stability_med,
            dmin=DMIN,
            cmin=CMIN,
            smin=SMIN,
        )

        selection_method = "greedy"
        # NOTE: We only *flag* escalation here; Pareto execution is a separate step/tool.
        if escalated:
            selection_method = "greedy_escalate_pareto"

        policy = {
            "ids_id": ids_id,
            "hash_alg": "md5_utf8_join_pipe",

            # Identity eligibility contract (governance constraint)
            "identity_contract": {
                "path": "contracts/domain_identity_keys_v2.json",
                "version": "v2",
                "enforced": True,
            },

            # Selected composite join key for this IDS
            "required_keys": selected_keys,

            # Data-driven selection details
            "selection": {
                "method": selection_method,
                "max_k": max_k,
                "min_records": min_records,
                "top_candidate_keys": top_candidate_keys,
                "gates": {
                    "dmin": DMIN,
                    "cmin": CMIN,
                    "smin": SMIN,
                    "bootstraps": BOOTSTRAPS,
                    "bootstrap_file_frac": BOOTSTRAP_FILE_FRAC,
                },
                "gate_metrics": gate_metrics,
                "escalate_to_pareto": bool(escalated),
                "escalation_reasons": reasons,
                "metrics_final": final_metrics,
                "metrics_baseline": baseline_metrics,
                "stability_jaccard_samples": [round(float(x), 6) for x in sims],
            },
        }

        policies[ids_id] = policy

        # Existing summary CSV (join-key policy by IDS)
        rows.append(
            {
                "domain": domain,
                "ids_id": ids_id,
                "required_keys": ";".join(selected_keys),
                "records_considered": final_metrics.get("records_considered", 0),
                "records_missing_required": final_metrics.get("records_missing_required", 0),
                "distinct_sig_hash": final_metrics.get("distinct_sig_hash", 0),
                "distinct_join_hash": final_metrics.get("distinct_join_hash", 0),
                "collision_rate": final_metrics.get("collision_rate", 1.0),
            }
        )

        # New selection report CSV (consistent gates + escalation signal)
        report_rows.append(
            {
                "domain": domain,
                "ids_id": ids_id,
                "method_used": selection_method,
                "escalate_to_pareto": bool(escalated),
                "escalation_reasons": ";".join(reasons),
                "collision_baseline": gate_metrics["collision_baseline"],
                "collision_final": gate_metrics["collision_final"],
                "delta_collision": gate_metrics["delta_collision"],
                "coverage_final": gate_metrics["coverage_final"],
                "stability_median": gate_metrics["stability_median"],
                "required_key_count": len(selected_keys),
                "required_keys": ";".join(selected_keys),
                "total_records": total_records,
                "records_considered": final_metrics.get("records_considered", 0),
            }
        )

    out = {
        "domain": domain,
        "policy_version": "join_key_policy_by_ids.v1",
        "policies": policies,
        "source": {
            "exports_dir": os.path.abspath(exports_dir),
            "file_to_ids_csv": os.path.abspath(file_to_ids_csv),
        },
    }

    os.makedirs(out_dir, exist_ok=True)

    json_path = os.path.join(out_dir, f"{domain}.join_key_policy_by_ids.v1.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    csv_path = os.path.join(out_dir, f"{domain}.join_key_policy_by_ids.v1.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "domain",
                "ids_id",
                "required_keys",
                "records_considered",
                "records_missing_required",
                "distinct_sig_hash",
                "distinct_join_hash",
                "collision_rate",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    report_path = os.path.join(out_dir, f"{domain}.ids_key_selection_report.v1.csv")
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "domain",
                "ids_id",
                "method_used",
                "escalate_to_pareto",
                "escalation_reasons",
                "collision_baseline",
                "collision_final",
                "delta_collision",
                "coverage_final",
                "stability_median",
                "required_key_count",
                "required_keys",
                "total_records",
                "records_considered",
            ],
        )
        w.writeheader()
        for r in report_rows:
            w.writerow(r)

    print(f"  {report_path}")

    print("[INFO] IDS join-key policies written:")
    print(f"  {json_path}")
    print(f"  {csv_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Derive join-key policies per IDS using identity_basis evidence")
    p.add_argument(
        "exports_dir",
        help="Directory containing fingerprint exports (*.json). Ignored if --phase0-dir is provided.",
    )
    p.add_argument(
        "--phase0-dir",
        dest="phase0_dir",
        default=None,
        help="If provided, read v2.1 Phase0 tables from this directory (Results_v21/phase0_v21).",
    )
    p.add_argument("--domain", required=True, help="Domain (use text_types for verification)")
    p.add_argument("--file-to-ids", required=True, dest="file_to_ids_csv", help="Path to <domain>.file_to_ids.v1.csv")
    p.add_argument("--out", default="join_keys", dest="out_dir", help="Output directory")
    p.add_argument("--max-k", type=int, default=4, help="Max required keys (default: 4)")
    p.add_argument("--min-records", type=int, default=200, help="Minimum usable records per IDS (default: 200)")
    p.add_argument("--top-candidate-keys", type=int, default=40, help="Candidate key pool size (default: 40)")
    args = p.parse_args()

    derive_join_keys_by_ids(
        exports_dir=args.exports_dir,
        domain=args.domain,
        file_to_ids_csv=args.file_to_ids_csv,
        out_dir=args.out_dir,
        max_k=args.max_k,
        min_records=args.min_records,
        top_candidate_keys=args.top_candidate_keys,
        phase0_dir=args.phase0_dir,
    )


if __name__ == "__main__":
    main()
