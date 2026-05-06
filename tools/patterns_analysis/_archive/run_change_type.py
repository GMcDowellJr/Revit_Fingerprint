from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List

from .io import ExportFile, load_exports, get_domain_records, get_run_provenance
from .index import build_domain_index
from .compare import classify_pair, ChangeCounts
from .report import write_change_type_csv, write_json_report, format_console_summary


def run_change_type(
    *,
    exports_dir: str,
    domain: str,
    baseline_file_id: str,
    out_dir: str,
) -> Dict[str, Any]:
    """Minimal working slice:
    load -> index -> baseline vs each file change classification -> CSV + JSON report.

    Returns the JSON report dict.
    """
    exports: List[ExportFile] = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    by_id = {e.file_id: e for e in exports}
    if baseline_file_id not in by_id:
        raise SystemExit(
            f"Baseline file_id not found. Available: {', '.join(sorted(by_id.keys()))}"
        )

    baseline = by_id[baseline_file_id]
    b_records = get_domain_records(baseline.data, domain)
    b_index = build_domain_index(domain=domain, file_id=baseline.file_id, records=b_records)

    comparisons: List[ChangeCounts] = []

    for e in exports:
        if e.file_id == baseline_file_id:
            continue
        recs = get_domain_records(e.data, domain)
        idx = build_domain_index(domain=domain, file_id=e.file_id, records=recs)
        comparisons.append(classify_pair(baseline=b_index, other=idx))

    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, f"{domain}.change_type.csv")
    write_change_type_csv(out_path=csv_path, rows=comparisons)

    report: Dict[str, Any] = {
        "phase": "patterns_analysis",
        "domain": domain,
        "exports_dir": os.path.abspath(exports_dir),
        "baseline_file_id": baseline_file_id,
        "counts": {
            "files_total": len(exports),
            "comparisons": len(comparisons),
            "baseline_records": len(b_records),
            "baseline_joinable": len(b_index.joinable),
            "baseline_unjoinable": b_index.unjoinable_count,
            "baseline_duplicate_join_hash": len(b_index.duplicates),
            "baseline_bad_phase2_item_keys": b_index.bad_phase2_item_keys,
        },
        "baseline_provenance": get_run_provenance(baseline.data),
        "assumptions": {
            "authority_sample_unit": "one_json_file",
            "join_key": "record.join_key.join_hash",
            "unjoinable": "record.join_key.join_hash missing or null",
            "duplicates": "join_hash duplicated within file => ambiguous",
            "same_vs_modified": "compare concatenated phase2 items (k,q,v) as map by k",
        },
        "outputs": {
            "change_type_csv": os.path.abspath(csv_path),
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.report.json")
    write_json_report(out_path=json_path, report=report)
    report["outputs"]["json_report"] = os.path.abspath(json_path)

    print(format_console_summary(domain=domain, baseline_file_id=baseline_file_id, counts=comparisons))
    print("")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")

    return report


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase-2 analysis (post-export): baseline vs file change classification"
    )
    p.add_argument("exports_dir", help="Directory containing exported fingerprint JSON files")
    p.add_argument("--domain", default="dimension_types", help="Domain name (default: dimension_types)")
    p.add_argument(
        "--baseline",
        required=True,
        dest="baseline_file_id",
        help="Baseline file id (defaults to filename; must match directory listing)",
    )
    p.add_argument("--out", default="phase2_out", dest="out_dir", help="Output directory")
    return p.parse_args()


def main() -> None:
    ns = _parse_args()
    run_change_type(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        baseline_file_id=ns.baseline_file_id,
        out_dir=ns.out_dir,
    )


if __name__ == "__main__":
    main()
