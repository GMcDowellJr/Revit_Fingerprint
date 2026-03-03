# tools/phase2_analysis/pareto_join_keys_by_ids.py
"""
Pareto join-key derivation per IDS (intradomain standard), using identity_basis.items.

Design goals:
- IDS-scoped (no circularity: IDS comes from file_to_ids)
- Bounded search (beam) to avoid combinatorics
- Deterministic selection (lexicographic selector)
- Auditable outputs (Pareto front detail + chosen keyset)

Metrics (within IDS, on records that have all required keys):
- collision: 1 - distinct_join / distinct_sig   (lower better)
- coverage: records_considered / total_records  (higher better)
- fragmentation: distinct_join / distinct_sig - 1 (lower better)
- key_count: len(keys)                          (lower better)

Selector (deterministic, on Pareto front survivors that satisfy coverage >= Cmin):
1) min collision
2) min fragmentation
3) min key_count
4) max coverage
Tie-break: sorted key string.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import csv
import hashlib
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .io import load_exports, get_domain_records


# ---------------------------
# Eligibility (exclude labels)
# ---------------------------

_DENY_KEY_REGEXES = [
    r"(^|[._])name$",
    r"(^|[._])type_name$",
    r"(^|[._])family_name$",
    r"(^|[._])symbol_name$",
]

from tools.analysis.authority.domain_identity_contract import DomainIdentityContract

_CONTRACT = DomainIdentityContract.load()


def is_eligible_join_key_item(domain: str, key: str) -> bool:
    return _CONTRACT.is_key_allowed(domain, key)


# ---------------------------
# Hash + identity extraction
# ---------------------------

def md5_utf8_join_pipe(parts: List[str]) -> str:
    s = "|".join(parts).encode("utf-8")
    return hashlib.md5(s).hexdigest()


def extract_identity_map(record: Dict[str, Any]) -> Dict[str, Any]:
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


def compute_join_hash_for_record(record: Dict[str, Any], keys: List[str]) -> Optional[str]:
    imap = extract_identity_map(record)
    for k in keys:
        if k not in imap or imap[k] is None:
            return None
    parts = [f"{k}={str(imap[k])}" for k in keys]
    return md5_utf8_join_pipe(parts)


# ---------------------------
# Metrics + Pareto
# ---------------------------

@dataclass(frozen=True)
class Candidate:
    keys: Tuple[str, ...]
    collision: float
    coverage: float
    fragmentation: float
    key_count: int
    records_considered: int
    total_records: int
    distinct_sig: int
    distinct_join: int

    def key_str(self) -> str:
        return ";".join(self.keys)


def evaluate_keyset(records: List[Dict[str, Any]], keys: List[str]) -> Candidate:
    total = len(records)
    pairs: List[Tuple[str, str]] = []
    missing = 0

    for r in records:
        sig = r.get("sig_hash")
        if not sig:
            continue
        jh = compute_join_hash_for_record(r, keys)
        if jh is None:
            missing += 1
            continue
        pairs.append((str(sig), str(jh)))

    if not pairs:
        return Candidate(
            keys=tuple(keys),
            collision=1.0,
            coverage=0.0,
            fragmentation=0.0,
            key_count=len(keys),
            records_considered=0,
            total_records=total,
            distinct_sig=0,
            distinct_join=0,
        )

    distinct_sig = len(set(p[0] for p in pairs))
    distinct_join = len(set(p[1] for p in pairs))

    # collision: 1 - distinct_join / distinct_sig
    if distinct_sig <= 0:
        collision = 1.0
        fragmentation = 0.0
    else:
        collision = 1.0 - (distinct_join / float(distinct_sig))
        if collision < 0:
            collision = 0.0
        if collision > 1:
            collision = 1.0

        # fragmentation: distinct_join / distinct_sig - 1
        fragmentation = (distinct_join / float(distinct_sig)) - 1.0
        if fragmentation < 0:
            fragmentation = 0.0

    coverage = len(pairs) / float(total) if total > 0 else 0.0

    return Candidate(
        keys=tuple(keys),
        collision=round(float(collision), 6),
        coverage=round(float(coverage), 6),
        fragmentation=round(float(fragmentation), 6),
        key_count=len(keys),
        records_considered=len(pairs),
        total_records=total,
        distinct_sig=distinct_sig,
        distinct_join=distinct_join,
    )


def dominates(a: Candidate, b: Candidate) -> bool:
    """Minimize collision, minimize fragmentation, minimize key_count, minimize (1-coverage)."""
    a_vec = (a.collision, a.fragmentation, a.key_count, 1.0 - a.coverage)
    b_vec = (b.collision, b.fragmentation, b.key_count, 1.0 - b.coverage)
    return all(x <= y for x, y in zip(a_vec, b_vec)) and any(x < y for x, y in zip(a_vec, b_vec))


def pareto_front(cands: List[Candidate]) -> List[Candidate]:
    front: List[Candidate] = []
    for c in cands:
        dominated = False
        # if any existing dominates c, drop it
        for f in front:
            if dominates(f, c):
                dominated = True
                break
        if dominated:
            continue

        # remove any front members dominated by c
        front = [f for f in front if not dominates(c, f)]
        front.append(c)

    # deterministic ordering (not “best”, just stable)
    front.sort(key=lambda x: (x.collision, x.fragmentation, x.key_count, -(x.coverage), x.key_str()))
    return front


def choose_from_front(front: List[Candidate], coverage_min: float) -> Optional[Candidate]:
    viable = [c for c in front if c.coverage >= coverage_min]
    if not viable:
        return None
    viable.sort(key=lambda x: (x.collision, x.fragmentation, x.key_count, -(x.coverage), x.key_str()))
    return viable[0]


# ---------------------------
# Bounded search (beam)
# ---------------------------

def build_candidate_pool(records: List[Dict[str, Any]], domain: str, top_n: int) -> Tuple[List[str], List[str]]:
    key_counts: Dict[str, int] = defaultdict(int)
    for r in records:
        imap = extract_identity_map(r)
        for k in imap.keys():
            key_counts[k] += 1

    ranked = sorted(key_counts.items(), key=lambda kv: kv[1], reverse=True)

    eligible: List[str] = []
    excluded: List[str] = []
    for k, _ in ranked:
        if is_eligible_join_key_item(domain, k):
            eligible.append(k)
        else:
            excluded.append(k)

    return eligible[:top_n], excluded


def beam_search_candidates(
    records: List[Dict[str, Any]],
    keys_pool: List[str],
    max_k: int,
    beam_width: int,
    coverage_min: float,
) -> List[Candidate]:
    """
    Deterministic beam search:
    - Start with empty set
    - At each depth, expand each partial set by adding one new key (in pool order)
    - Score cheaply by (collision, -coverage, fragmentation, key_count)
    - Keep top beam_width
    Evaluate full metric at each node (still cheap enough; we’re bounded)
    """
    evaluated: Dict[Tuple[str, ...], Candidate] = {}

    def get_eval(kset: Tuple[str, ...]) -> Candidate:
        if kset in evaluated:
            return evaluated[kset]
        c = evaluate_keyset(records, list(kset))
        evaluated[kset] = c
        return c

    level: List[Tuple[str, ...]] = [tuple()]
    get_eval(tuple())  # baseline

    for depth in range(1, max_k + 1):
        next_level: List[Tuple[str, ...]] = []
        for base in level:
            used = set(base)
            for k in keys_pool:
                if k in used:
                    continue
                cand = tuple(sorted(list(base) + [k]))
                next_level.append(cand)

        # dedupe deterministically
        next_level = sorted(set(next_level))

        # evaluate and score for beam pruning
        scored: List[Tuple[Tuple[float, float, float, int, str], Tuple[str, ...]]] = []
        for kset in next_level:
            c = get_eval(kset)
            # soft enforce coverage (don’t drop entirely yet; keep a few low-coverage sets)
            penalty = 0.0 if c.coverage >= coverage_min else 0.25
            score = (c.collision + penalty, -(c.coverage), c.fragmentation, c.key_count, c.key_str())
            scored.append((score, kset))

        scored.sort(key=lambda x: x[0])
        level = [kset for _score, kset in scored[:beam_width]]

    # return all evaluated candidates
    return list(evaluated.values())


# ---------------------------
# Main driver
# ---------------------------

def run_pareto_by_ids(
    exports_dir: str,
    domain: str,
    file_to_ids_csv: str,
    out_dir: str,
    max_k: int,
    top_candidate_keys: int,
    beam_width: int,
    coverage_min: float,
    only_escalated: bool,
    escalation_report_csv: Optional[str],
) -> None:
    map_df = pd.read_csv(file_to_ids_csv)
    if not {"file_id", "ids_id"}.issubset(set(map_df.columns)):
        raise ValueError("file_to_ids_csv must include columns: file_id, ids_id")

    file_to_ids = {str(r["file_id"]): str(r["ids_id"]) for _, r in map_df.iterrows()}

    escalated_ids: Optional[set] = None
    if only_escalated:
        if not escalation_report_csv:
            raise ValueError("--only-escalated requires --escalation-report")
        rep = pd.read_csv(escalation_report_csv)
        if not {"ids_id", "escalate_to_pareto"}.issubset(set(rep.columns)):
            raise ValueError("escalation_report_csv must include columns: ids_id, escalate_to_pareto")
        escalated_ids = set(str(r["ids_id"]) for _, r in rep.iterrows() if bool(r["escalate_to_pareto"]))

    exports = list(load_exports(exports_dir, max_files=None))
    ids_to_records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for exp in exports:
        fid = str(exp.file_id)
        if fid not in file_to_ids:
            continue
        ids_id = file_to_ids[fid]
        if escalated_ids is not None and ids_id not in escalated_ids:
            continue
        recs = get_domain_records(exp.data, domain)

        # attach file id for audit/debug (optional downstream)
        for r in recs:
            if isinstance(r, dict) and "_file_id" not in r:
                r["_file_id"] = fid

        ids_to_records[ids_id].extend(recs)

    os.makedirs(out_dir, exist_ok=True)

    policy_rows: List[Dict[str, Any]] = []
    front_rows: List[Dict[str, Any]] = []

    policies: Dict[str, Any] = {}

    for ids_id, records in sorted(ids_to_records.items(), key=lambda x: x[0]):
        pool, excluded = build_candidate_pool(records, domain, top_candidate_keys)

        cands = beam_search_candidates(
            records=records,
            keys_pool=pool,
            max_k=max_k,
            beam_width=beam_width,
            coverage_min=coverage_min,
        )

        front = pareto_front(cands)
        chosen = choose_from_front(front, coverage_min=coverage_min)

        # record front rows
        for c in front:
            front_rows.append(
                {
                    "domain": domain,
                    "ids_id": ids_id,
                    "keys": c.key_str(),
                    "collision": c.collision,
                    "coverage": c.coverage,
                    "fragmentation": c.fragmentation,
                    "key_count": c.key_count,
                    "records_considered": c.records_considered,
                    "total_records": c.total_records,
                    "distinct_sig": c.distinct_sig,
                    "distinct_join": c.distinct_join,
                    "is_chosen": bool(chosen is not None and c.keys == chosen.keys),
                }
            )

        if chosen is None:
            # No viable candidate met coverage_min; choose best available deterministically
            best = front[0] if front else evaluate_keyset(records, [])
            chosen = best

        policies[ids_id] = {
            "ids_id": ids_id,
            "hash_alg": "md5_utf8_join_pipe",

            # Identity eligibility contract (governance constraint)
            "identity_contract": {
                "path": "contracts/domain_identity_keys_v2.json",
                "version": "v2",
                "enforced": True,
            },

            "required_keys": list(chosen.keys),
            "selection": {
                "method": "pareto_beam",
                "coverage_min": coverage_min,
                "max_k": max_k,
                "beam_width": beam_width,
                "top_candidate_keys": top_candidate_keys,
                "candidate_pool_used": pool,
                "excluded_key_count": len(excluded),
                "excluded_keys_top20": excluded[:20],
                "chosen_metrics": {
                    "collision": chosen.collision,
                    "coverage": chosen.coverage,
                    "fragmentation": chosen.fragmentation,
                    "key_count": chosen.key_count,
                    "records_considered": chosen.records_considered,
                    "total_records": chosen.total_records,
                    "distinct_sig": chosen.distinct_sig,
                    "distinct_join": chosen.distinct_join,
                },
                "front_size": len(front),
            },
        }

        policy_rows.append(
            {
                "domain": domain,
                "ids_id": ids_id,
                "required_keys": ";".join(chosen.keys),
                "collision": chosen.collision,
                "coverage": chosen.coverage,
                "fragmentation": chosen.fragmentation,
                "key_count": chosen.key_count,
                "records_considered": chosen.records_considered,
                "total_records": chosen.total_records,
                "front_size": len(front),
            }
        )

    out = {
        "domain": domain,
        "policy_version": "pareto_join_key_policy_by_ids.v1",
        "policies": policies,
        "source": {
            "exports_dir": os.path.abspath(exports_dir),
            "file_to_ids_csv": os.path.abspath(file_to_ids_csv),
            "escalation_report_csv": os.path.abspath(escalation_report_csv) if escalation_report_csv else None,
        },
        "params": {
            "coverage_min": coverage_min,
            "max_k": max_k,
            "beam_width": beam_width,
            "top_candidate_keys": top_candidate_keys,
            "only_escalated": bool(only_escalated),
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.pareto_join_key_policy_by_ids.v1.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    csv_path = os.path.join(out_dir, f"{domain}.pareto_join_key_policy_by_ids.v1.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "domain",
                "ids_id",
                "required_keys",
                "collision",
                "coverage",
                "fragmentation",
                "key_count",
                "records_considered",
                "total_records",
                "front_size",
            ],
        )
        w.writeheader()
        for r in policy_rows:
            w.writerow(r)

    front_path = os.path.join(out_dir, f"{domain}.pareto_front_by_ids.v1.csv")
    with open(front_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "domain",
                "ids_id",
                "keys",
                "collision",
                "coverage",
                "fragmentation",
                "key_count",
                "records_considered",
                "total_records",
                "distinct_sig",
                "distinct_join",
                "is_chosen",
            ],
        )
        w.writeheader()
        for r in front_rows:
            w.writerow(r)

    print("[INFO] Pareto IDS join-key outputs written:")
    print(f"  {json_path}")
    print(f"  {csv_path}")
    print(f"  {front_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Derive Pareto join-key policies per IDS (bounded beam search).")
    p.add_argument("exports_dir", help="Directory containing fingerprint exports (*.details.json)")
    p.add_argument("--domain", required=True, help="Domain to analyze")
    p.add_argument("--file-to-ids", required=True, dest="file_to_ids_csv", help="Path to <domain>.file_to_ids.v1.csv")
    p.add_argument("--out", default="join_keys", dest="out_dir", help="Output directory")

    p.add_argument("--max-k", type=int, default=5, help="Max keyset size (default: 5)")
    p.add_argument("--top-candidate-keys", type=int, default=35, help="Candidate pool size (default: 35)")
    p.add_argument("--beam-width", type=int, default=120, help="Beam width (default: 120)")
    p.add_argument("--coverage-min", type=float, default=0.75, help="Coverage minimum for viability (default: 0.75)")

    p.add_argument("--only-escalated", action="store_true", help="Only run Pareto for IDSs flagged in escalation report")
    p.add_argument("--escalation-report", dest="escalation_report_csv", help="Path to <domain>.ids_key_selection_report.v1.csv")

    args = p.parse_args()

    run_pareto_by_ids(
        exports_dir=args.exports_dir,
        domain=args.domain,
        file_to_ids_csv=args.file_to_ids_csv,
        out_dir=args.out_dir,
        max_k=args.max_k,
        top_candidate_keys=args.top_candidate_keys,
        beam_width=args.beam_width,
        coverage_min=args.coverage_min,
        only_escalated=args.only_escalated,
        escalation_report_csv=args.escalation_report_csv,
    )


if __name__ == "__main__":
    main()
