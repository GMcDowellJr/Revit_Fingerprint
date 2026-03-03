from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import os
import csv
from typing import List

from tools.analysis.authority.io import load_exports, get_domain_records
from tools.analysis.authority.index import build_domain_index
from tools.analysis.authority.stability import presence_counts, stability_distribution
from tools.analysis.authority.report import write_json_report


def run_population_stability(
    *,
    exports_dir: str,
    domain: str,
    thresholds_pct: List[float],
    out_dir: str,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    indexes = []
    for e in exports:
        records = get_domain_records(e.data, domain)
        idx = build_domain_index(domain=domain, file_id=e.file_id, records=records)
        indexes.append(idx)

    os.makedirs(out_dir, exist_ok=True)

    # ---- presence counts per join_hash
    counts = presence_counts(indexes)

    counts_csv = os.path.join(out_dir, f"{domain}.population_presence.csv")
    with open(counts_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "join_hash", "files_present", "files_total", "presence_pct"])
        total = len(indexes)
        for jh, c in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            pct = (c / total) * 100.0 if total else 0.0
            w.writerow([domain, jh, c, total, round(pct, 2)])

    # ---- threshold stability distribution
    dist = stability_distribution(indexes=indexes, thresholds_pct=thresholds_pct)

    dist_csv = os.path.join(out_dir, f"{domain}.population_stability.csv")
    with open(dist_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "threshold_pct", "files_total", "stable_join_hash_count"])
        for r in dist:
            w.writerow([
                r.domain,
                r.threshold_pct,
                r.files_total,
                r.join_hash_stable_count,
            ])

    # ---- JSON report (descriptive)
    report = {
        "phase": "phase2_analysis",
        "analysis": "population_stability",
        "domain": domain,
        "files_total": len(indexes),
        "join_hash_total": len(counts),
        "thresholds_pct": thresholds_pct,
        "outputs": {
            "presence_csv": os.path.abspath(counts_csv),
            "stability_csv": os.path.abspath(dist_csv),
        },
        "assumptions": {
            "authority_sample_unit": "one_json_file",
            "join_key": "record.join_key.join_hash",
            "duplicates": "join_hash duplicated within file excluded from joinable",
            "meaning": "presence only; no attribute comparison",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.population_stability.report.json")
    write_json_report(out_path=json_path, report=report)

    print(f"Population stability written:")
    print(f"  {counts_csv}")
    print(f"  {dist_csv}")
    print(f"  {json_path}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-2 population stability analysis")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument(
        "--thresholds",
        default="50,70,90,95",
        help="Comma-separated percentage thresholds",
    )
    p.add_argument("--out", default="phase2_out")
    return p.parse_args()


def main() -> None:
    ns = _parse_args()
    thresholds = [float(x.strip()) for x in ns.thresholds.split(",") if x.strip()]
    run_population_stability(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        thresholds_pct=thresholds,
        out_dir=ns.out,
    )


if __name__ == "__main__":
    main()
