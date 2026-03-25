#!/usr/bin/env python3
"""
tools/label_synthesis/build_label_population.py

Derives per-domain label population CSVs from phase0_records.csv.
These feed the modal label layer (Layer 3) of the label resolver.

Reads:
    Results_v21/phase0_v21/phase0_records.csv

Writes (one per domain):
    Results_v21/label_synthesis/{domain}.joinhash_label_population.csv

Columns in output:
    domain, join_hash, label_v, label_q, files_count

Run this BEFORE analyze1/analyze2 to get modal labels in domain_patterns.csv.

Usage:
    python tools/label_synthesis/build_label_population.py --out-root <results_allpairs>

Example:
    python tools/label_synthesis/build_label_population.py \\
        --out-root "C:\\Users\\gmcdowell\\Documents\\Fingerprint_Out\\projects\\results_allpairs"
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set, Tuple


def build_label_population(out_root: Path) -> None:
    phase0_dir = out_root / "Results_v21" / "phase0_v21"
    records_csv = phase0_dir / "phase0_records.csv"

    if not records_csv.is_file():
        sys.exit(
            f"[ERROR] phase0_records.csv not found at: {records_csv}\n"
            f"        Run flatten stage first: --stages flatten (or flatten,apply)"
        )

    label_synth_dir = out_root / "Results_v21" / "label_synthesis"
    label_synth_dir.mkdir(parents=True, exist_ok=True)

    # Group by (domain, join_hash, label_v) -> set of file (export_run_id) that have it
    # Structure: {domain: {(join_hash, label_v, label_q): set(export_run_id)}}
    grouped: Dict[str, Dict[Tuple[str, str, str], Set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    total_rows = 0
    skipped_no_join_hash = 0
    skipped_no_label = 0

    print(f"[build_label_population] Reading: {records_csv}", flush=True)

    with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            domain = row.get("domain", "").strip()
            join_hash = row.get("join_hash", "").strip()
            export_run_id = row.get("export_run_id", "").strip()
            label_v = row.get("label_display", "").strip()
            label_q = "ok" if label_v else "missing"

            if not join_hash:
                skipped_no_join_hash += 1
                continue

            if not domain or not export_run_id:
                continue

            # Include rows with empty labels too — they surface as label_q=missing
            # and the resolver treats no-label domains as candidates for LLM synthesis
            key = (join_hash, label_v, label_q)
            grouped[domain][key].add(export_run_id)

    print(
        f"[build_label_population] Rows read: {total_rows} | "
        f"skipped (no join_hash): {skipped_no_join_hash}",
        flush=True,
    )

    domains_written = 0
    for domain, hash_label_map in sorted(grouped.items()):
        out_csv = label_synth_dir / f"{domain}.joinhash_label_population.csv"

        # Build rows sorted by join_hash then files_count desc
        rows = []
        for (join_hash, label_v, label_q), file_set in hash_label_map.items():
            rows.append({
                "domain": domain,
                "join_hash": join_hash,
                "label_v": label_v,
                "label_q": label_q,
                "files_count": len(file_set),
            })

        rows.sort(key=lambda r: (r["join_hash"], -r["files_count"], r["label_v"]))

        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["domain", "join_hash", "label_v", "label_q", "files_count"],
            )
            writer.writeheader()
            writer.writerows(rows)

        unique_hashes = len({r["join_hash"] for r in rows})
        labeled = sum(1 for r in rows if r["label_q"] == "ok" and r["label_v"])
        print(
            f"  [{domain}] {unique_hashes} join_hashes | "
            f"{labeled} labeled rows | "
            f"wrote: {out_csv.name}",
            flush=True,
        )
        domains_written += 1

    print(
        f"\n[build_label_population] Done. "
        f"{domains_written} domain CSVs written to: {label_synth_dir}",
        flush=True,
    )
    print(
        "[build_label_population] Label population artifacts are ready for emit_analysis_v21.",
        flush=True,
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Build per-domain joinhash label population CSVs from phase0_records.csv. "
            "Output feeds Layer 3 (modal label) of the label resolver."
        )
    )
    ap.add_argument(
        "--out-root",
        required=True,
        help="Same --out-root passed to run_extract_all.py (e.g. results_allpairs)",
    )
    args = ap.parse_args()
    build_label_population(Path(args.out_root).resolve())


if __name__ == "__main__":
    main()