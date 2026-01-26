from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from typing import Dict, List, Tuple

from .io import load_exports, get_domain_records
from .index import build_domain_index
from .stability import stable_join_hashes
from .report import write_json_report


def extract_phase2_items(record) -> Dict[str, Tuple[str, str]]:
    """Return k -> (q, v) map across all phase2 buckets.
    Assumes caller has already excluded records with duplicate k.
    """
    out = {}
    p2 = record.get("phase2", {})
    for bucket in ("semantic_items", "cosmetic_items", "unknown_items"):
        for it in p2.get(bucket, []) or []:
            k = it.get("k")
            if k is None or k in out:
                return {}  # ambiguous, caller should exclude
            q = str(it.get("q") or "")
            v = str(it.get("v") or "")
            out[str(k)] = (q, v)
    return out


def run_attribute_stress(
    *,
    exports_dir: str,
    domain: str,
    threshold_pct: float,
    out_dir: str,
) -> None:
    exports = load_exports(exports_dir)
    indexes = []
    for e in exports:
        recs = get_domain_records(e.data, domain)
        idx = build_domain_index(domain=domain, file_id=e.file_id, records=recs)
        indexes.append(idx)

    stable_ids = stable_join_hashes(indexes=indexes, threshold_pct=threshold_pct)

    # k -> counters
    comparisons = defaultdict(int)
    diffs = defaultdict(int)
    q_transitions = defaultdict(lambda: defaultdict(int))

    # pairwise across files
    for i in range(len(indexes)):
        for j in range(i + 1, len(indexes)):
            a = indexes[i]
            b = indexes[j]

            for jh in stable_ids:
                if jh not in a.joinable or jh not in b.joinable:
                    continue

                ra = a.joinable[jh]
                rb = b.joinable[jh]

                ma = extract_phase2_items(ra)
                mb = extract_phase2_items(rb)
                if not ma or not mb:
                    continue

                for k in ma.keys() & mb.keys():
                    comparisons[k] += 1
                    qa, va = ma[k]
                    qb, vb = mb[k]
                    if (qa, va) != (qb, vb):
                        diffs[k] += 1
                        q_transitions[k][f"{qa}->{qb}"] += 1

    os.makedirs(out_dir, exist_ok=True)

    stress_csv = os.path.join(out_dir, f"{domain}.attribute_stress.csv")
    with open(stress_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["k", "comparisons", "diffs", "diff_rate"])
        for k in sorted(comparisons, key=lambda x: diffs[x], reverse=True):
            c = comparisons[k]
            d = diffs[k]
            w.writerow([k, c, d, round(d / c, 4) if c else 0.0])

    q_csv = os.path.join(out_dir, f"{domain}.attribute_stress_q_breakdown.csv")
    with open(q_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["k", "transition", "count"])
        for k, trans in q_transitions.items():
            for t, c in trans.items():
                w.writerow([k, t, c])

    report = {
        "phase": "phase2_analysis",
        "analysis": "attribute_stress",
        "domain": domain,
        "threshold_pct": threshold_pct,
        "stable_join_hash_count": len(stable_ids),
        "outputs": {
            "stress_csv": os.path.abspath(stress_csv),
            "q_breakdown_csv": os.path.abspath(q_csv),
        },
        "assumptions": {
            "scope": "stable join_hash set only",
            "comparison": "(k,q,v) exact match",
            "duplicates": "excluded",
        },
    }

    write_json_report(
        out_path=os.path.join(out_dir, f"{domain}.attribute_stress.report.json"),
        report=report,
    )

    print(f"Attribute stress written:")
    print(f"  {stress_csv}")
    print(f"  {q_csv}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--threshold", type=float, default=70.0)
    p.add_argument("--out", default="phase2_out")
    ns = p.parse_args()

    run_attribute_stress(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        threshold_pct=ns.threshold,
        out_dir=ns.out,
    )


if __name__ == "__main__":
    main()
