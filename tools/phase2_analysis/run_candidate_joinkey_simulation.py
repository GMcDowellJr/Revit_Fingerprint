from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from .io import load_exports, get_domain_records
from .report import write_json_report


def _get(record: Dict[str, Any], path: List[str]) -> Any:
    cur = record
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def _qv(v: Any) -> Tuple[str, str]:
    if isinstance(v, dict):
        return str(v.get("q", "")), str(v.get("v", ""))
    if v is None:
        return "missing", ""
    return "ok", str(v)


def _extract_features(record: Dict[str, Any]) -> Dict[str, Tuple[str, str]]:
    return {
        "shape": _qv(_get(record, ["phase2", "semantic_items_map", "dim_attr.shape"])),
        "name": _qv(record.get("label")),
        "tick": _qv(_get(record, ["phase2", "cosmetic_items_map", "dim_attr.tick_mark_uid"])),
        "accuracy": _qv(_get(record, ["phase2", "semantic_items_map", "dim_attr.accuracy"])),
    }


def run_candidate_joinkey_simulation(exports_dir: str, domain: str, out_dir: str) -> None:
    exports = load_exports(exports_dir)
    os.makedirs(out_dir, exist_ok=True)

    collision_stats = defaultdict(lambda: defaultdict(int))
    fragmentation_stats = defaultdict(lambda: defaultdict(set))

    for e in exports:
        records = get_domain_records(e.data, domain)

        # group by current join_hash
        by_join = defaultdict(list)
        for r in records:
            jh = _get(r, ["join_key", "join_hash"])
            if jh:
                by_join[jh].append(r)

        for jh, group in by_join.items():
            if len(group) < 2:
                continue

            # baseline
            collision_stats[jh]["baseline"] += 1

            buckets = {
                "shape": defaultdict(list),
                "shape+name": defaultdict(list),
                "shape+name+tick": defaultdict(list),
                "shape+name+tick+accuracy": defaultdict(list),
            }

            for r in group:
                f = _extract_features(r)

                buckets["shape"][(f["shape"],)].append(r)
                buckets["shape+name"][(f["shape"], f["name"])].append(r)
                buckets["shape+name+tick"][(f["shape"], f["name"], f["tick"])].append(r)
                buckets["shape+name+tick+accuracy"][(f["shape"], f["name"], f["tick"], f["accuracy"])].append(r)

            for k, b in buckets.items():
                collision_stats[jh][k] += sum(1 for g in b.values() if len(g) > 1)

                for sig in b.keys():
                    fragmentation_stats[k][sig].add(e.file_id)

    csv_path = os.path.join(out_dir, f"{domain}.candidate_joinkey_simulation.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "join_hash",
            "baseline_collision_groups",
            "shape",
            "shape+name",
            "shape+name+tick",
            "shape+name+tick+accuracy",
        ])
        for jh in sorted(collision_stats):
            row = collision_stats[jh]
            w.writerow([
                jh,
                row["baseline"],
                row["shape"],
                row["shape+name"],
                row["shape+name+tick"],
                row["shape+name+tick+accuracy"],
            ])

    report = {
        "analysis": "candidate_joinkey_simulation",
        "domain": domain,
        "features_tested": [
            "shape",
            "shape+name",
            "shape+name+tick",
            "shape+name+tick+accuracy",
        ],
        "outputs": {
            "csv": os.path.abspath(csv_path),
        },
        "notes": [
            "Simulation only; exporter unchanged",
            "Measures collision reduction vs fragmentation pressure",
        ],
    }

    write_json_report(
        out_path=os.path.join(out_dir, f"{domain}.candidate_joinkey_simulation.report.json"),
        report=report,
    )

    print(f"Wrote simulation CSV: {csv_path}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    ns = p.parse_args()

    run_candidate_joinkey_simulation(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        out_dir=ns.out_dir,
    )


if __name__ == "__main__":
    main()
