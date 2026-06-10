#!/usr/bin/env python3
"""Compute cross-domain edge co-occurrence and join_hash-pair patterns.

Inputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - Fingerprint_Out/archetype_analysis/reference_graph.json

Outputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_edge_pairs.csv (edge pair level)
  - Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv (join_hash pair level)

Processing:
  - Build per-file (export_run_id) activation sets per edge_id from
    cross_domain_items.csv.
  - For every pair of edges in reference_graph.json, compute co-occurrence
    metrics: n_both, n_a_only, n_b_only, n_neither, n_a_unavailable,
    n_b_unavailable, support_pct, jaccard, containment_a_in_b,
    containment_b_in_a. An edge with available == false in reference_graph
    contributes n_*_unavailable == n_files_total and an empty activation set
    (graceful degradation -- null signal, not an error).
  - For pairs with n_both >= --support-min-files, compute the join_hash-pair
    cross-product per file (distinct source_join_hash values per edge per
    file) and aggregate file_count per
    (edge_id_a, edge_id_b, source_join_hash_a, source_join_hash_b).
    Only patterns whose own file_count >= --support-min-files are emitted to
    cross_domain_patterns.csv (keeps one-off join_hash pairs out of
    candidate generation even when the parent edge pair is common).
    pattern_id = md5 of "{edge_id}|{edge_id}|{join_hash}|{join_hash}" with the
    two (edge_id, join_hash) pairs sorted for order independence.

Usage:
    python tools/archetype/compute_cross_domain_cooccurrence.py \\
        --repo-root . \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --reference-graph Fingerprint_Out/archetype_analysis/reference_graph.json \\
        --out-dir Fingerprint_Out/archetype_analysis \\
        [--support-min-files 5] [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from _common import (
    log,
    atomic_write_csv,
    read_csv_rows,
    read_json,
)

STAGE = "compute_cross_domain_cooccurrence"

PAIRS_FIELDS = [
    "edge_id_a", "edge_id_b",
    "source_domain_a", "target_domain_a",
    "source_domain_b", "target_domain_b",
    "n_files_total",
    "n_both", "n_a_only", "n_b_only", "n_neither",
    "n_a_unavailable", "n_b_unavailable",
    "support_pct", "jaccard", "containment_a_in_b", "containment_b_in_a",
]

PATTERNS_FIELDS = [
    "pattern_id",
    "edge_id_a", "edge_id_b",
    "join_hash_a", "join_hash_b",
    "source_domain_a", "target_domain_a",
    "source_domain_b", "target_domain_b",
    "file_count",
]


def _pattern_id(edge_id_a: str, join_hash_a: str, edge_id_b: str, join_hash_b: str) -> str:
    pair = sorted([(edge_id_a, join_hash_a), (edge_id_b, join_hash_b)])
    raw = f"{pair[0][0]}|{pair[1][0]}|{pair[0][1]}|{pair[1][1]}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--out-dir", default=None, help="Output directory for cross_domain_edge_pairs.csv / cross_domain_patterns.csv")
    ap.add_argument("--support-min-files", type=int, default=5, help="Minimum n_both file count to emit join_hash-pair patterns (default: 5)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    items_path = Path(args.cross_domain_items) if args.cross_domain_items else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_items.csv"
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    out_dir = Path(args.out_dir) if args.out_dir else repo_root / "Fingerprint_Out" / "archetype_analysis"

    items_rows = read_csv_rows(items_path)
    log(STAGE, f"loaded {len(items_rows)} rows from {items_path}")

    reference_graph = read_json(reference_graph_path, default={})
    edges = reference_graph.get("edges", []) if isinstance(reference_graph, dict) else []
    edges_by_id: Dict[str, Dict[str, Any]] = {e["edge_id"]: e for e in edges if "edge_id" in e}
    log(STAGE, f"loaded {len(edges_by_id)} edges from {reference_graph_path}")

    file_universe: Set[str] = set()
    fired: Dict[str, Set[str]] = defaultdict(set)
    # (edge_id, export_run_id) -> set of distinct source_join_hash values
    join_hashes_by_edge_file: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for row in items_rows:
        export_run_id = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if not export_run_id or not edge_id:
            continue
        file_universe.add(export_run_id)
        fired[edge_id].add(export_run_id)
        jh = row.get("source_join_hash", "")
        if jh:
            join_hashes_by_edge_file[(edge_id, export_run_id)].add(jh)

    n_files_total = len(file_universe)
    log(STAGE, f"file_universe size={n_files_total}")

    edge_ids = sorted(edges_by_id.keys())
    pairs_rows: List[Dict[str, Any]] = []
    pattern_counts: Dict[Tuple[str, str, str, str], int] = defaultdict(int)
    pattern_meta: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}

    for edge_id_a, edge_id_b in combinations(edge_ids, 2):
        edge_a = edges_by_id[edge_id_a]
        edge_b = edges_by_id[edge_id_b]
        available_a = bool(edge_a.get("available"))
        available_b = bool(edge_b.get("available"))

        fired_a = fired.get(edge_id_a, set()) if available_a else set()
        fired_b = fired.get(edge_id_b, set()) if available_b else set()

        n_both = len(fired_a & fired_b)
        n_a_only = len(fired_a - fired_b)
        n_b_only = len(fired_b - fired_a)
        n_neither = n_files_total - n_both - n_a_only - n_b_only
        n_a_unavailable = n_files_total if not available_a else 0
        n_b_unavailable = n_files_total if not available_b else 0

        support_pct = (n_both / n_files_total * 100.0) if n_files_total else 0.0
        denom_jac = n_both + n_a_only + n_b_only
        jaccard = (n_both / denom_jac) if denom_jac else 0.0
        denom_a = n_both + n_a_only
        containment_a_in_b = (n_both / denom_a) if denom_a else 0.0
        denom_b = n_both + n_b_only
        containment_b_in_a = (n_both / denom_b) if denom_b else 0.0

        pairs_rows.append({
            "edge_id_a": edge_id_a,
            "edge_id_b": edge_id_b,
            "source_domain_a": edge_a.get("source_domain", ""),
            "target_domain_a": edge_a.get("target_domain", ""),
            "source_domain_b": edge_b.get("source_domain", ""),
            "target_domain_b": edge_b.get("target_domain", ""),
            "n_files_total": n_files_total,
            "n_both": n_both,
            "n_a_only": n_a_only,
            "n_b_only": n_b_only,
            "n_neither": n_neither,
            "n_a_unavailable": n_a_unavailable,
            "n_b_unavailable": n_b_unavailable,
            "support_pct": f"{support_pct:.4f}",
            "jaccard": f"{jaccard:.4f}",
            "containment_a_in_b": f"{containment_a_in_b:.4f}",
            "containment_b_in_a": f"{containment_b_in_a:.4f}",
        })

        if n_both < args.support_min_files:
            continue

        for export_run_id in (fired_a & fired_b):
            jh_a_set = join_hashes_by_edge_file.get((edge_id_a, export_run_id), set())
            jh_b_set = join_hashes_by_edge_file.get((edge_id_b, export_run_id), set())
            for jh_a in jh_a_set:
                for jh_b in jh_b_set:
                    key = (edge_id_a, edge_id_b, jh_a, jh_b)
                    pattern_counts[key] += 1
                    pattern_meta[key] = {
                        "source_domain_a": edge_a.get("source_domain", ""),
                        "target_domain_a": edge_a.get("target_domain", ""),
                        "source_domain_b": edge_b.get("source_domain", ""),
                        "target_domain_b": edge_b.get("target_domain", ""),
                    }

    log(STAGE, f"computed {len(pairs_rows)} edge pairs; {len(pattern_counts)} candidate join_hash-pair patterns")

    patterns_rows: List[Dict[str, Any]] = []
    for (edge_id_a, edge_id_b, jh_a, jh_b), file_count in pattern_counts.items():
        if file_count < args.support_min_files:
            continue
        meta = pattern_meta[(edge_id_a, edge_id_b, jh_a, jh_b)]
        patterns_rows.append({
            "pattern_id": _pattern_id(edge_id_a, jh_a, edge_id_b, jh_b),
            "edge_id_a": edge_id_a,
            "edge_id_b": edge_id_b,
            "join_hash_a": jh_a,
            "join_hash_b": jh_b,
            "source_domain_a": meta["source_domain_a"],
            "target_domain_a": meta["target_domain_a"],
            "source_domain_b": meta["source_domain_b"],
            "target_domain_b": meta["target_domain_b"],
            "file_count": file_count,
        })

    log(STAGE, f"{len(patterns_rows)} join_hash-pair patterns at or above support threshold={args.support_min_files}")

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(pairs_rows)} edge pair rows and {len(patterns_rows)} pattern rows to {out_dir}")
        return 0

    atomic_write_csv(out_dir / "cross_domain_edge_pairs.csv", PAIRS_FIELDS, pairs_rows)
    log(STAGE, f"wrote {len(pairs_rows)} rows to {out_dir / 'cross_domain_edge_pairs.csv'}")

    atomic_write_csv(out_dir / "cross_domain_patterns.csv", PATTERNS_FIELDS, patterns_rows)
    log(STAGE, f"wrote {len(patterns_rows)} rows to {out_dir / 'cross_domain_patterns.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
