#!/usr/bin/env python3
"""Generate candidate archetype definitions from cross-domain co-occurrence patterns.

Inputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv
  - Fingerprint_Out/archetype_analysis/reference_graph.json
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv

Output:
  - Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json

Processing:
  - For each (edge_id_a, edge_id_b) edge pair appearing in
    cross_domain_patterns.csv, derive a governance_question_hint from the
    target domains touched by the pair:
      both target domains == "arrowheads"          -> arrowhead_consistency
      target domain containing "wall_types"        -> wall_graphics
      target domain starting with "fill_patterns"  -> fill_pattern_usage
      target domain == "line_patterns"             -> line_pattern_usage
      target domain == "view_filter_definitions"   -> view_filter_strategy
      otherwise                                     -> unknown
    (checked against both target domains in the pair, in that priority order)
  - Patterns sharing the same (governance_question_hint, edge_id_a, edge_id_b)
    are clustered into one candidate archetype definition.
  - Each candidate gets archetype_id containing a "CANDIDATE" marker,
    promoted=false, auto_generated=true, and one signal stub per edge. Each
    signal stub's join_hash is seeded from the top-ranked (by file_count)
    cross_domain_patterns.csv row for that edge pair: signal for edge_id_a
    gets that row's join_hash_a, signal for edge_id_b gets join_hash_b.
    join_hash_populated is true iff that value is non-empty. This is a
    starting point for human review, not a hard filter.
  - Corpus coverage / required gating: for each signal's (canonical) edge_id,
    coverage = (distinct export_run_ids in cross_domain_items.csv where this
    edge_id fired with a non-empty target_join_hash, folded through the
    edge alias map) / (distinct export_run_ids in cross_domain_items.csv).
    Dynamic VFD edges (edge_type == "dynamic" in reference_graph.json)
    intentionally carry an empty target_join_hash, so any firing of a
    dynamic edge counts toward coverage regardless of target_join_hash.
    Every signal stub gets "_coverage_pct" (0.0-100.0, two decimals) and
    "collapsed_from" (the list of original edge_ids -- from
    _common.build_edge_aliases -- that were folded onto this signal's
    edge_id when computing coverage; empty if the edge_id collapsed nothing).
    If coverage < --low-coverage-threshold (default 0.10), the signal stub
    gets "required": false and "_low_coverage_flag": true; otherwise
    "required": true and "_low_coverage_flag": false. The "_"-prefixed
    fields are human-review annotations only and do not affect downstream
    scoring. Each candidate also gets a top-level "min_signal_coverage_pct"
    equal to the minimum "_coverage_pct" across its signals.
  - Missing --cross-domain-items: if the path is not provided, or the file
    does not exist, an ERROR is written to stderr and coverage gating is
    skipped entirely -- every signal stub gets "_coverage_pct": null,
    "_low_coverage_flag": false, "required": true (safe default), and
    every candidate gets "min_signal_coverage_pct": null. The candidates
    file is still written.

Usage:
    python tools/archetype/generate_archetype_candidates.py \\
        --repo-root . \\
        --cross-domain-patterns Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv \\
        --reference-graph Fingerprint_Out/archetype_analysis/reference_graph.json \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --out Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json \\
        [--top-n-join-hash-pairs 5] [--low-coverage-threshold 0.10] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from _common import (
    log,
    atomic_write_json,
    build_edge_aliases,
    read_csv_rows,
    read_json,
    slugify,
    SCHEMA_VERSION,
)

STAGE = "generate_archetype_candidates"

_HINT_PRIORITY: List[Tuple[str, Any]] = [
    ("wall_graphics", lambda d: "wall_types" in d),
    ("fill_pattern_usage", lambda d: d.startswith("fill_patterns")),
    ("line_pattern_usage", lambda d: d == "line_patterns"),
    ("view_filter_strategy", lambda d: d == "view_filter_definitions"),
]

_DEFAULT_LOW_COVERAGE_THRESHOLD = 0.10


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _governance_question_hint(target_domain_a: str, target_domain_b: str) -> str:
    if target_domain_a == "arrowheads" and target_domain_b == "arrowheads":
        return "arrowhead_consistency"
    for hint, predicate in _HINT_PRIORITY:
        if predicate(target_domain_a) or predicate(target_domain_b):
            return hint
    return "unknown"


def _signal_coverage_pct(
    edge_id: str,
    alias_of: Dict[str, str],
    covered_files_by_edge: Dict[str, Set[str]],
    n_files_total: int,
) -> float:
    if n_files_total == 0:
        return 0.0
    canonical_edge_id = alias_of.get(edge_id, edge_id)
    covered = len(covered_files_by_edge.get(canonical_edge_id, set()))
    return round(covered / n_files_total * 100.0, 2)


def _collapsed_from_for_edge(
    edge_id: str,
    alias_of: Dict[str, str],
    collapsed: Dict[str, List[str]],
) -> List[str]:
    canonical_edge_id = alias_of.get(edge_id, edge_id)
    return collapsed.get(canonical_edge_id, [])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--cross-domain-patterns", default=None, help="Path to cross_domain_patterns.csv")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv (used for signal corpus coverage gating)")
    ap.add_argument("--out", default=None, help="Output path for archetype_definitions_candidates.json")
    ap.add_argument("--top-n-join-hash-pairs", type=int, default=5, help="Number of top join_hash pairs to retain per candidate for human review (default: 5)")
    ap.add_argument("--low-coverage-threshold", type=float, default=_DEFAULT_LOW_COVERAGE_THRESHOLD, help="Coverage fraction below which a signal stub is marked required=false (default: 0.10)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    patterns_path = Path(args.cross_domain_patterns) if args.cross_domain_patterns else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_patterns.csv"
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    items_path = Path(args.cross_domain_items) if args.cross_domain_items else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_items.csv"
    out_path = Path(args.out) if args.out else repo_root / "Fingerprint_Out" / "archetype_analysis" / "archetype_definitions_candidates.json"

    patterns_rows = read_csv_rows(patterns_path)
    log(STAGE, f"loaded {len(patterns_rows)} rows from {patterns_path}")

    reference_graph = read_json(reference_graph_path, default={})
    edges = reference_graph.get("edges", []) if isinstance(reference_graph, dict) else []
    edges_by_id: Dict[str, Dict[str, Any]] = {e["edge_id"]: e for e in edges if "edge_id" in e}
    log(STAGE, f"loaded {len(edges_by_id)} edges from {reference_graph_path}")

    alias_of, collapsed = build_edge_aliases(edges_by_id)
    if alias_of:
        log(STAGE, f"collapsed {len(alias_of)} edges onto {len(collapsed)} canonical edges")

    coverage_available = items_path.is_file()
    if not coverage_available:
        sys.stderr.write(
            "ERROR [candidates] --cross-domain-items is required for coverage\n"
            "computation. All signals will have _coverage_pct: null and\n"
            "required: true (safe default). Re-run with --cross-domain-items\n"
            "to enable low-coverage detection.\n"
        )

    items_rows = read_csv_rows(items_path)
    log(STAGE, f"loaded {len(items_rows)} rows from {items_path}")

    # corpus coverage: canonical edge_id -> set of export_run_id where the edge
    # fired with a non-empty target_join_hash. Dynamic VFD edges intentionally
    # carry an empty target_join_hash (build_cross_domain_items.py), so a
    # firing on a dynamic edge counts toward coverage regardless.
    file_universe: Set[str] = set()
    covered_files_by_edge: Dict[str, Set[str]] = defaultdict(set)
    for row in items_rows:
        export_run_id = row.get("export_run_id", "")
        if not export_run_id:
            continue
        file_universe.add(export_run_id)
        edge_id = row.get("edge_id", "")
        if not edge_id:
            continue
        canonical_edge_id = alias_of.get(edge_id, edge_id)
        is_dynamic = edges_by_id.get(edge_id, {}).get("edge_type") == "dynamic"
        if row.get("target_join_hash", "") or is_dynamic:
            covered_files_by_edge[canonical_edge_id].add(export_run_id)

    n_files_total = len(file_universe)
    log(STAGE, f"file_universe size={n_files_total} (for signal coverage gating)")

    # group rows by (edge_id_a, edge_id_b)
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in patterns_rows:
        groups[(row.get("edge_id_a", ""), row.get("edge_id_b", ""))].append(row)

    log(STAGE, f"grouped patterns into {len(groups)} edge-pair clusters")

    candidates: List[Dict[str, Any]] = []
    skipped_unknown_edges = 0
    n_low_coverage_signals = 0
    for (edge_id_a, edge_id_b), rows in sorted(groups.items()):
        edge_a = edges_by_id.get(edge_id_a)
        edge_b = edges_by_id.get(edge_id_b)
        if edge_a is None or edge_b is None:
            skipped_unknown_edges += 1
            continue

        target_domain_a = edge_a.get("target_domain", "")
        target_domain_b = edge_b.get("target_domain", "")
        hint = _governance_question_hint(target_domain_a, target_domain_b)

        archetype_id = f"CANDIDATE__{hint}__{slugify(edge_id_a)}__{slugify(edge_id_b)}"

        sorted_rows = sorted(rows, key=lambda r: int(float(r.get("file_count") or 0)), reverse=True)
        total_file_count = sum(int(float(r.get("file_count") or 0)) for r in rows)
        top_join_hash_pairs = [
            {
                "join_hash_a": r.get("join_hash_a", ""),
                "join_hash_b": r.get("join_hash_b", ""),
                "file_count": int(float(r.get("file_count") or 0)),
            }
            for r in sorted_rows[: args.top_n_join_hash_pairs]
        ]

        top_row = sorted_rows[0] if sorted_rows else {}
        top_join_hash_a = top_row.get("join_hash_a", "")
        top_join_hash_b = top_row.get("join_hash_b", "")

        if coverage_available:
            coverage_pct_a = _signal_coverage_pct(edge_id_a, alias_of, covered_files_by_edge, n_files_total)
            coverage_pct_b = _signal_coverage_pct(edge_id_b, alias_of, covered_files_by_edge, n_files_total)
            low_coverage_a = (coverage_pct_a / 100.0) < args.low_coverage_threshold
            low_coverage_b = (coverage_pct_b / 100.0) < args.low_coverage_threshold
            if low_coverage_a:
                n_low_coverage_signals += 1
            if low_coverage_b:
                n_low_coverage_signals += 1
            min_signal_coverage_pct = min(coverage_pct_a, coverage_pct_b)
        else:
            coverage_pct_a = None
            coverage_pct_b = None
            low_coverage_a = False
            low_coverage_b = False
            min_signal_coverage_pct = None

        candidates.append({
            "archetype_id": archetype_id,
            "governance_question": hint,
            "approach_label": "",
            "promoted": False,
            "auto_generated": True,
            "distinct_pattern_count": len(rows),
            "total_file_count": total_file_count,
            "min_signal_coverage_pct": min_signal_coverage_pct,
            "top_join_hash_pairs": top_join_hash_pairs,
            "signals": [
                {
                    "signal_id": edge_id_a,
                    "edge_id": edge_id_a,
                    "source_domain": edge_a.get("source_domain", ""),
                    "target_domain": target_domain_a,
                    "required": not low_coverage_a,
                    "join_hash": top_join_hash_a or None,
                    "join_hash_populated": bool(top_join_hash_a),
                    "_coverage_pct": coverage_pct_a,
                    "_low_coverage_flag": low_coverage_a,
                    "collapsed_from": _collapsed_from_for_edge(edge_id_a, alias_of, collapsed),
                },
                {
                    "signal_id": edge_id_b,
                    "edge_id": edge_id_b,
                    "source_domain": edge_b.get("source_domain", ""),
                    "target_domain": target_domain_b,
                    "required": not low_coverage_b,
                    "join_hash": top_join_hash_b or None,
                    "join_hash_populated": bool(top_join_hash_b),
                    "_coverage_pct": coverage_pct_b,
                    "_low_coverage_flag": low_coverage_b,
                    "collapsed_from": _collapsed_from_for_edge(edge_id_b, alias_of, collapsed),
                },
            ],
        })

    if n_low_coverage_signals:
        log(STAGE, f"flagged {n_low_coverage_signals} signal stubs as required=false (coverage below {args.low_coverage_threshold:.2%})")

    if skipped_unknown_edges:
        log(STAGE, f"skipped {skipped_unknown_edges} pattern groups referencing edges not present in reference_graph")

    log(STAGE, f"generated {len(candidates)} candidate archetype definitions")

    output = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _utc_now_iso(),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(candidates)} candidates to {out_path}")
        return 0

    atomic_write_json(out_path, output)
    log(STAGE, f"wrote {len(candidates)} candidates to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
