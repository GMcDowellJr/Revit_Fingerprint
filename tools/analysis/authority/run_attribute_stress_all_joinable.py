from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import csv
import os
from collections import defaultdict
from typing import Dict, List, Tuple

from tools.analysis.authority.io import load_exports, get_domain_records
from tools.analysis.authority.index import build_domain_index
from tools.analysis.authority.stability import presence_counts
from tools.analysis.authority.report import write_json_report


def _phase2_items_map_no_dups(record) -> Tuple[Dict[str, Tuple[str, str]], bool]:
    """Return k -> (q, v) across all phase2 buckets.

    If duplicate k is detected within a single record, returns ({} , True).
    This is treated as ambiguous and must be excluded by caller.
    """
    out: Dict[str, Tuple[str, str]] = {}
    p2 = record.get("phase2")
    if not isinstance(p2, dict):
        return out, False

    dup = False
    for bucket in ("semantic_items", "cosmetic_items", "coordination_items", "unknown_items"):
        items = p2.get(bucket)
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            k = it.get("k")
            if k is None:
                continue
            ks = str(k)
            if ks in out:
                dup = True
                continue
            q = it.get("q")
            v = it.get("v")
            qs = "" if q is None else str(q)
            vs = "" if v is None else str(v)
            out[ks] = (qs, vs)

    if dup:
        return {}, True
    return out, False


def run_attribute_stress_all_joinable(
    *,
    exports_dir: str,
    domain: str,
    min_presence_files: int,
    out_dir: str,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    indexes = []
    records_total_by_file = {}
    for e in exports:
        recs = get_domain_records(e.data, domain)
        records_total_by_file[e.file_id] = len(recs)
        idx = build_domain_index(domain=domain, file_id=e.file_id, records=recs)
        indexes.append(idx)

    # Only include join_hash values that appear (joinable) in >= N files.
    counts = presence_counts(indexes)
    join_ids = {jh for jh, c in counts.items() if int(c) >= int(min_presence_files)}

    # k -> counters
    comparisons = defaultdict(int)        # k -> number of (file-pair, join_hash) comparisons where k present on both sides
    diffs = defaultdict(int)              # k -> number of non-identical (q,v)
    q_transitions = defaultdict(lambda: defaultdict(int))  # k -> "qa->qb" -> count

    # Ambiguity counters (explicit, descriptive)
    amb_bad_items = 0

    # pairwise across files
    for i in range(len(indexes)):
        for j in range(i + 1, len(indexes)):
            a = indexes[i]
            b = indexes[j]

            for jh in join_ids:
                ra = a.joinable.get(jh)
                rb = b.joinable.get(jh)
                if ra is None or rb is None:
                    continue

                ma, a_dup = _phase2_items_map_no_dups(ra)
                mb, b_dup = _phase2_items_map_no_dups(rb)
                if a_dup or b_dup:
                    amb_bad_items += 1
                    continue

                shared_keys = ma.keys() & mb.keys()
                for k in shared_keys:
                    comparisons[k] += 1
                    qa, va = ma[k]
                    qb, vb = mb[k]
                    if (qa, va) != (qb, vb):
                        diffs[k] += 1
                        q_transitions[k][f"{qa}->{qb}"] += 1

    os.makedirs(out_dir, exist_ok=True)

    stress_csv = os.path.join(out_dir, f"{domain}.attribute_stress_all_joinable.csv")
    with open(stress_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "k", "comparisons", "diffs", "diff_rate"])
        for k in sorted(comparisons.keys(), key=lambda x: (-(diffs[x]), x)):
            c = int(comparisons[k])
            d = int(diffs[k])
            w.writerow([domain, k, c, d, round(d / c, 6) if c else ""])

    q_csv = os.path.join(out_dir, f"{domain}.attribute_stress_all_joinable_q_breakdown.csv")
    with open(q_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "k", "transition", "count"])
        for k in sorted(q_transitions.keys()):
            trans = q_transitions[k]
            for t in sorted(trans.keys(), key=lambda x: (-trans[x], x)):
                w.writerow([domain, k, t, int(trans[t])])

    report = {
        "phase": "phase2_analysis",
        "analysis": "attribute_stress_all_joinable",
        "domain": domain,
        "files_total": len(indexes),
        "join_hash_total_joinable": len(counts),
        "min_presence_files": int(min_presence_files),
        "join_hash_in_scope": len(join_ids),
        "ambiguity": {
            "excluded_duplicate_join_hash_within_file": "handled by DomainIndex.joinable",
            "excluded_bad_phase2_item_key_maps_count": int(amb_bad_items),
        },
        "outputs": {
            "stress_csv": os.path.abspath(stress_csv),
            "q_breakdown_csv": os.path.abspath(q_csv),
        },
        "assumptions": {
            "scope": "all joinable join_hash with presence >= min_presence_files",
            "join_key": "record.join_key.join_hash",
            "comparison": "exact (k,q,v) map equality per k; order-insensitive",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.attribute_stress_all_joinable.report.json")
    write_json_report(out_path=json_path, report=report)

    print("All-joinable attribute stress written:")
    print(f"  {stress_csv}")
    print(f"  {q_csv}")
    print(f"  {json_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: attribute stress across all joinable identities")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--min-presence-files", type=int, default=2, dest="min_presence_files")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    ns = p.parse_args()

    run_attribute_stress_all_joinable(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        min_presence_files=ns.min_presence_files,
        out_dir=ns.out_dir,
    )


if __name__ == "__main__":
    main()
