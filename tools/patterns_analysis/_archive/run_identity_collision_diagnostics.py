from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from typing import Dict, List, Tuple

from .io import load_exports, get_domain_records
from .index import build_domain_index
from .report import write_json_report


def _multiplicity_map(idx) -> Dict[str, int]:
    """join_hash -> multiplicity within a file, including duplicates.

    DomainIndex.joinable holds only unique join_hash (multiplicity 1).
    DomainIndex.duplicate_counts holds multiplicity >=2.
    """
    m = {jh: 1 for jh in idx.joinable.keys()}
    for jh, c in idx.duplicate_counts.items():
        try:
            m[str(jh)] = int(c)
        except Exception:
            m[str(jh)] = 2
    # Ensure duplicates set also represented (if any without counts)
    for jh in idx.duplicates:
        m.setdefault(str(jh), 2)
    return m


def run_identity_collision_diagnostics(
    *,
    exports_dir: str,
    domain: str,
    out_dir: str,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    indexes = []
    raw_counts_by_file = {}
    for e in exports:
        recs = get_domain_records(e.data, domain)
        raw_counts_by_file[e.file_id] = len(recs)
        idx = build_domain_index(domain=domain, file_id=e.file_id, records=recs)
        indexes.append(idx)

    os.makedirs(out_dir, exist_ok=True)

    # ---- Per-file collision summary
    per_file_csv = os.path.join(out_dir, f"{domain}.identity_collision_per_file.csv")
    with open(per_file_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "file_id",
            "records_total",
            "unjoinable_count",
            "unique_join_hash_total",
            "unique_join_hash_joinable",
            "duplicated_join_hash_count",
            "duplicate_records_excess",
            "max_multiplicity",
            "bad_phase2_item_keys_count",
        ])

        for idx in indexes:
            records_total = int(raw_counts_by_file.get(idx.file_id, 0))
            m = _multiplicity_map(idx)
            duplicated_join_hash_count = len([jh for jh, c in m.items() if int(c) >= 2])
            duplicate_records_excess = sum(max(int(c) - 1, 0) for c in m.values())
            max_mult = max(m.values()) if m else 0
            unique_total = len(m)

            w.writerow([
                domain,
                idx.file_id,
                records_total,
                int(idx.unjoinable_count),
                int(unique_total),
                int(len(idx.joinable)),
                int(duplicated_join_hash_count),
                int(duplicate_records_excess),
                int(max_mult),
                int(idx.bad_phase2_item_keys),
            ])

    # ---- Per-join_hash cross-file collision summary
    # join_hash -> stats across files
    files_present = defaultdict(int)        # appears at least once (join_hash exists)
    files_ambiguous = defaultdict(int)      # multiplicity >=2 in a file
    total_records = defaultdict(int)        # sum multiplicity across files
    max_multiplicity = defaultdict(int)     # max multiplicity in any file

    for idx in indexes:
        m = _multiplicity_map(idx)
        for jh, c in m.items():
            files_present[jh] += 1
            total_records[jh] += int(c)
            if int(c) >= 2:
                files_ambiguous[jh] += 1
            if int(c) > int(max_multiplicity[jh]):
                max_multiplicity[jh] = int(c)

    per_join_csv = os.path.join(out_dir, f"{domain}.identity_collision_by_join_hash.csv")
    with open(per_join_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "join_hash",
            "files_present",
            "files_unique_present",
            "files_ambiguous_present",
            "total_records_across_files",
            "max_multiplicity_observed",
        ])

        for jh in sorted(files_present.keys(), key=lambda x: (-files_present[x], -files_ambiguous[x], x)):
            fp = int(files_present[jh])
            fa = int(files_ambiguous.get(jh, 0))
            fu = fp - fa
            w.writerow([
                domain,
                jh,
                fp,
                fu,
                fa,
                int(total_records[jh]),
                int(max_multiplicity[jh]),
            ])

    report = {
        "phase": "patterns_analysis",
        "analysis": "identity_collision_diagnostics",
        "domain": domain,
        "files_total": len(indexes),
        "outputs": {
            "per_file_csv": os.path.abspath(per_file_csv),
            "per_join_hash_csv": os.path.abspath(per_join_csv),
        },
        "assumptions": {
            "authority_sample_unit": "one_json_file",
            "join_key": "record.join_key.join_hash",
            "duplicates": "join_hash duplicated within file => ambiguous (not joinable)",
            "unjoinable": "join_hash missing/null",
        },
        "notes": {
            "scope": "purely descriptive collision statistics; no joining decisions changed",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.identity_collision.report.json")
    write_json_report(out_path=json_path, report=report)

    print("Identity collision diagnostics written:")
    print(f"  {per_file_csv}")
    print(f"  {per_join_csv}")
    print(f"  {json_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: identity collision diagnostics")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    ns = p.parse_args()

    run_identity_collision_diagnostics(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        out_dir=ns.out_dir,
    )


if __name__ == "__main__":
    main()
