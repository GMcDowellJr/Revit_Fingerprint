#!/usr/bin/env python3
"""Generate candidate archetype definitions from cross-domain co-occurrence patterns.

Inputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv
  - Fingerprint_Out/archetype_analysis/reference_graph.json

Output:
  - Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json

Processing:
  - For each (edge_id_a, edge_id_b) edge pair appearing in
    cross_domain_patterns.csv, derive a governance_question_hint from the
    target domains touched by the pair:
      target domain containing "wall_types"      -> wall_graphics
      target domain starting with "fill_patterns" -> fill_pattern_usage
      target domain == "line_patterns"            -> line_pattern_usage
      target domain == "view_filter_definitions"  -> view_filter_strategy
      otherwise                                    -> unknown
    (checked against both target domains in the pair, in that priority order)
  - Patterns sharing the same (governance_question_hint, edge_id_a, edge_id_b)
    are clustered into one candidate archetype definition.
  - Each candidate gets archetype_id containing a "CANDIDATE" marker,
    promoted=false, auto_generated=true, and one signal stub per edge.

Usage:
    python tools/archetype/generate_archetype_candidates.py \\
        --repo-root . \\
        --cross-domain-patterns Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv \\
        --reference-graph Fingerprint_Out/archetype_analysis/reference_graph.json \\
        --out Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json \\
        [--top-n-join-hash-pairs 5] [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from _common import (
    log,
    atomic_write_json,
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _governance_question_hint(target_domain_a: str, target_domain_b: str) -> str:
    for hint, predicate in _HINT_PRIORITY:
        if predicate(target_domain_a) or predicate(target_domain_b):
            return hint
    return "unknown"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--cross-domain-patterns", default=None, help="Path to cross_domain_patterns.csv")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--out", default=None, help="Output path for archetype_definitions_candidates.json")
    ap.add_argument("--top-n-join-hash-pairs", type=int, default=5, help="Number of top join_hash pairs to retain per candidate for human review (default: 5)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    patterns_path = Path(args.cross_domain_patterns) if args.cross_domain_patterns else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_patterns.csv"
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    out_path = Path(args.out) if args.out else repo_root / "Fingerprint_Out" / "archetype_analysis" / "archetype_definitions_candidates.json"

    patterns_rows = read_csv_rows(patterns_path)
    log(STAGE, f"loaded {len(patterns_rows)} rows from {patterns_path}")

    reference_graph = read_json(reference_graph_path, default={})
    edges = reference_graph.get("edges", []) if isinstance(reference_graph, dict) else []
    edges_by_id: Dict[str, Dict[str, Any]] = {e["edge_id"]: e for e in edges if "edge_id" in e}
    log(STAGE, f"loaded {len(edges_by_id)} edges from {reference_graph_path}")

    # group rows by (edge_id_a, edge_id_b)
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in patterns_rows:
        groups[(row.get("edge_id_a", ""), row.get("edge_id_b", ""))].append(row)

    log(STAGE, f"grouped patterns into {len(groups)} edge-pair clusters")

    candidates: List[Dict[str, Any]] = []
    skipped_unknown_edges = 0
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

        candidates.append({
            "archetype_id": archetype_id,
            "governance_question": hint,
            "approach_label": "",
            "promoted": False,
            "auto_generated": True,
            "distinct_pattern_count": len(rows),
            "total_file_count": total_file_count,
            "top_join_hash_pairs": top_join_hash_pairs,
            "signals": [
                {
                    "signal_id": edge_id_a,
                    "edge_id": edge_id_a,
                    "source_domain": edge_a.get("source_domain", ""),
                    "target_domain": target_domain_a,
                    "required": True,
                    "join_hash": None,
                },
                {
                    "signal_id": edge_id_b,
                    "edge_id": edge_id_b,
                    "source_domain": edge_b.get("source_domain", ""),
                    "target_domain": target_domain_b,
                    "required": True,
                    "join_hash": None,
                },
            ],
        })

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
