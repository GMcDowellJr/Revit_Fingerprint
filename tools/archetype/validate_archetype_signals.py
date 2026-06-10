#!/usr/bin/env python3
"""Validate archetype signal coherence at sig_hash grain.

Inputs:
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - config/archetype/archetype_definitions.json (only promoted == true entries)
  - results/records/records.csv

Outputs:
  - Fingerprint_Out/archetype_analysis/archetype_validation.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv

Processing:
  - For each promoted archetype, build signal_id -> edge_id from its signals.
  - For each archetype_classifications.csv row, split signals_fired
    (semicolon-separated signal_ids). For each fired signal, look up the
    cross_domain_items.csv rows for (export_run_id, edge_id) to get the
    (source_domain, source_join_hash) pairs that fired the signal in that
    file.
  - Join records.csv on (export_run_id, domain=source_domain,
    join_hash=source_join_hash) to resolve sig_hash.
  - archetype_validation_detail.csv: one row per
    (export_run_id, archetype_id, signal_id, source_join_hash) with the
    resolved sig_hash and the count of distinct join_hashes the signal fired
    with in that file (n_join_hashes_in_file).
  - archetype_validation.csv: aggregated per (archetype_id, signal_id):
      n_files_classified      -- distinct export_run_id with this signal fired
      n_distinct_sig_hashes   -- distinct resolved sig_hash values (non-empty)
      coherence_score         -- n_distinct_sig_hashes / n_files_classified
      coherence_tier          -- Convergent (<0.3) | Variable (0.3-0.8) |
                                  Fragmented (>=0.8)
      n_multi_instance_files  -- files where the signal fired with >1
                                  distinct join_hash

Usage:
    python tools/archetype/validate_archetype_signals.py \\
        --repo-root . \\
        --archetype-classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --archetype-definitions config/archetype/archetype_definitions.json \\
        --records-csv results/records/records.csv \\
        --out-dir Fingerprint_Out/archetype_analysis \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from _common import (
    log,
    atomic_write_csv,
    read_csv_rows,
    read_json,
)

STAGE = "validate_archetype_signals"

VALIDATION_FIELDS = [
    "archetype_id",
    "signal_id",
    "edge_id",
    "n_files_classified",
    "n_distinct_sig_hashes",
    "coherence_score",
    "coherence_tier",
    "n_multi_instance_files",
]

DETAIL_FIELDS = [
    "export_run_id",
    "archetype_id",
    "signal_id",
    "edge_id",
    "source_domain",
    "source_join_hash",
    "sig_hash",
    "n_join_hashes_in_file",
]


def _coherence_tier(score: float) -> str:
    if score < 0.3:
        return "Convergent"
    if score < 0.8:
        return "Variable"
    return "Fragmented"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--archetype-classifications", default=None, help="Path to archetype_classifications.csv")
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv")
    ap.add_argument("--archetype-definitions", default=None, help="Path to archetype_definitions.json")
    ap.add_argument("--records-csv", default=None, help="Path to records.csv")
    ap.add_argument("--out-dir", default=None, help="Output directory")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    classifications_path = Path(args.archetype_classifications) if args.archetype_classifications else repo_root / "Fingerprint_Out" / "archetype_analysis" / "archetype_classifications.csv"
    items_path = Path(args.cross_domain_items) if args.cross_domain_items else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_items.csv"
    definitions_path = Path(args.archetype_definitions) if args.archetype_definitions else repo_root / "config" / "archetype" / "archetype_definitions.json"
    records_csv_path = Path(args.records_csv) if args.records_csv else repo_root / "results" / "records" / "records.csv"
    out_dir = Path(args.out_dir) if args.out_dir else repo_root / "Fingerprint_Out" / "archetype_analysis"

    classification_rows = read_csv_rows(classifications_path)
    log(STAGE, f"loaded {len(classification_rows)} rows from {classifications_path}")

    items_rows = read_csv_rows(items_path)
    log(STAGE, f"loaded {len(items_rows)} rows from {items_path}")

    definitions_doc = read_json(definitions_path, default={})
    all_definitions = definitions_doc.get("archetypes", definitions_doc.get("candidates", [])) if isinstance(definitions_doc, dict) else []
    if isinstance(definitions_doc, list):
        all_definitions = definitions_doc
    archetypes = [a for a in all_definitions if a.get("promoted") is True]
    log(STAGE, f"loaded {len(all_definitions)} archetype definitions from {definitions_path}; {len(archetypes)} promoted")

    # archetype_id -> signal_id -> edge_id
    signal_edge_by_archetype: Dict[str, Dict[str, str]] = {}
    for archetype in archetypes:
        archetype_id = archetype.get("archetype_id", "")
        signal_edge_by_archetype[archetype_id] = {
            s.get("signal_id", s.get("edge_id", "")): s.get("edge_id", "")
            for s in (archetype.get("signals", []) or [])
        }

    records_rows = read_csv_rows(records_csv_path)
    log(STAGE, f"loaded {len(records_rows)} rows from {records_csv_path}")

    # (export_run_id, domain, join_hash) -> sig_hash
    sig_hash_idx: Dict[Tuple[str, str, str], str] = {}
    for r in records_rows:
        export_run_id = r.get("export_run_id", "")
        domain = r.get("domain", "")
        join_hash = r.get("join_hash", "")
        if export_run_id and domain and join_hash:
            sig_hash_idx[(export_run_id, domain, join_hash)] = r.get("sig_hash", "")

    # (export_run_id, edge_id) -> [(source_domain, source_join_hash), ...]
    items_idx: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for row in items_rows:
        export_run_id = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if not export_run_id or not edge_id:
            continue
        items_idx[(export_run_id, edge_id)].append((row.get("source_domain", ""), row.get("source_join_hash", "")))

    detail_rows: List[Dict[str, Any]] = []
    # (archetype_id, signal_id) -> set of export_run_id
    files_by_signal: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    # (archetype_id, signal_id) -> set of non-empty sig_hash
    sig_hashes_by_signal: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    # (archetype_id, signal_id) -> set of export_run_id with >1 distinct join_hash
    multi_instance_by_signal: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    # (archetype_id, signal_id) -> edge_id (for output)
    edge_id_by_signal: Dict[Tuple[str, str], str] = {}

    for row in classification_rows:
        export_run_id = row.get("export_run_id", "")
        archetype_id = row.get("archetype_id", "")
        signals_fired = [s for s in (row.get("signals_fired", "") or "").split(";") if s]
        signal_edge_map = signal_edge_by_archetype.get(archetype_id, {})

        for signal_id in signals_fired:
            edge_id = signal_edge_map.get(signal_id, signal_id)
            edge_id_by_signal[(archetype_id, signal_id)] = edge_id

            pairs = items_idx.get((export_run_id, edge_id), [])
            distinct_join_hashes = {jh for _, jh in pairs if jh}
            n_join_hashes_in_file = len(distinct_join_hashes)

            files_by_signal[(archetype_id, signal_id)].add(export_run_id)
            if n_join_hashes_in_file > 1:
                multi_instance_by_signal[(archetype_id, signal_id)].add(export_run_id)

            seen_join_hashes: Set[str] = set()
            for source_domain, source_join_hash in pairs:
                if not source_join_hash or source_join_hash in seen_join_hashes:
                    continue
                seen_join_hashes.add(source_join_hash)
                sig_hash = sig_hash_idx.get((export_run_id, source_domain, source_join_hash), "")
                if sig_hash:
                    sig_hashes_by_signal[(archetype_id, signal_id)].add(sig_hash)

                detail_rows.append({
                    "export_run_id": export_run_id,
                    "archetype_id": archetype_id,
                    "signal_id": signal_id,
                    "edge_id": edge_id,
                    "source_domain": source_domain,
                    "source_join_hash": source_join_hash,
                    "sig_hash": sig_hash,
                    "n_join_hashes_in_file": n_join_hashes_in_file,
                })

    log(STAGE, f"emitted {len(detail_rows)} detail rows")

    validation_rows: List[Dict[str, Any]] = []
    for (archetype_id, signal_id), files in sorted(files_by_signal.items()):
        n_files_classified = len(files)
        n_distinct_sig_hashes = len(sig_hashes_by_signal.get((archetype_id, signal_id), set()))
        coherence_score = (n_distinct_sig_hashes / n_files_classified) if n_files_classified else 0.0
        validation_rows.append({
            "archetype_id": archetype_id,
            "signal_id": signal_id,
            "edge_id": edge_id_by_signal.get((archetype_id, signal_id), ""),
            "n_files_classified": n_files_classified,
            "n_distinct_sig_hashes": n_distinct_sig_hashes,
            "coherence_score": f"{coherence_score:.4f}",
            "coherence_tier": _coherence_tier(coherence_score),
            "n_multi_instance_files": len(multi_instance_by_signal.get((archetype_id, signal_id), set())),
        })

    log(STAGE, f"emitted {len(validation_rows)} validation rows for {len(files_by_signal)} (archetype, signal) pairs")

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(validation_rows)} validation rows and {len(detail_rows)} detail rows to {out_dir}")
        return 0

    atomic_write_csv(out_dir / "archetype_validation.csv", VALIDATION_FIELDS, validation_rows)
    log(STAGE, f"wrote {len(validation_rows)} rows to {out_dir / 'archetype_validation.csv'}")

    atomic_write_csv(out_dir / "archetype_validation_detail.csv", DETAIL_FIELDS, detail_rows)
    log(STAGE, f"wrote {len(detail_rows)} rows to {out_dir / 'archetype_validation_detail.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
