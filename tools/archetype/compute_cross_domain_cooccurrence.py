#!/usr/bin/env python3
"""Compute cross-domain edge co-occurrence and join_hash-pair patterns.

Inputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - Fingerprint_Out/archetype_analysis/reference_graph.json

Outputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_edge_pairs.csv (edge pair level)
  - Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv (join_hash pair level)

Processing:
  - Edge aliasing: before enumerating pairs, edges that represent the same
    governance signal but target different domain-family partitions are
    collapsed onto a single canonical edge_id (their activation/join_hash
    data is merged):
      * fill_patterns_drafting / fill_patterns_model -- edges sharing
        (source_domain, source_field) and a target_domain differing only by
        a "_drafting"/"_model" suffix collapse onto the "_drafting" edge.
      * dimension_types_{linear,angular,radial,diameter} -- edges sharing
        (source_field, target_domain) where source_domain is one of the
        four dimension_types_* tick_mark partitions collapse onto the
        dimension_types_linear edge.
    cross_domain_patterns.csv emits the canonical edge_id plus a
    "collapsed_edge_ids_a"/"collapsed_edge_ids_b" column (pipe-separated)
    listing any non-canonical edge_ids merged into it.
  - Build per-file (export_run_id) activation sets per (canonical) edge_id
    from cross_domain_items.csv.
  - Edge pairs are restricted to those with a governance relationship.
    A pair (edge_a, edge_b) is eligible if (checked in this order):
      1. shared_target -- edge_a.target_domain == edge_b.target_domain
      2. chain         -- edge_a.target_domain == edge_b.source_domain, or
                           edge_b.target_domain == edge_a.source_domain
      3. whitelist     -- the pair appears in reference_graph.json's
                           "whitelisted_pairs" list (defaults to [] if absent)
    Ineligible pairs are skipped entirely (no rows in either output).
    cross_domain_edge_pairs.csv records which rule matched in
    "pair_eligibility_reason".
  - For every eligible pair of (canonical) edges, compute co-occurrence
    metrics: n_both, n_a_only, n_b_only, n_neither, n_a_unavailable,
    n_b_unavailable, support_pct, jaccard, containment_a_in_b,
    containment_b_in_a. An edge with available == false in reference_graph
    contributes n_*_unavailable == n_files_total and an empty activation set
    (graceful degradation -- null signal, not an error). A canonical edge is
    treated as available if any edge collapsed into it is available.
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
from typing import Any, Dict, List, Optional, Set, Tuple

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
    "pair_eligibility_reason",
]

PATTERNS_FIELDS = [
    "pattern_id",
    "edge_id_a", "edge_id_b",
    "join_hash_a", "join_hash_b",
    "source_domain_a", "target_domain_a",
    "source_domain_b", "target_domain_b",
    "collapsed_edge_ids_a", "collapsed_edge_ids_b",
    "file_count",
]

_DIM_TYPE_VARIANTS = ("linear", "angular", "radial", "diameter")


def _pattern_id(edge_id_a: str, join_hash_a: str, edge_id_b: str, join_hash_b: str) -> str:
    pair = sorted([(edge_id_a, join_hash_a), (edge_id_b, join_hash_b)])
    raw = f"{pair[0][0]}|{pair[1][0]}|{pair[0][1]}|{pair[1][1]}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _strip_partition_suffix(target_domain: str) -> Optional[str]:
    """Strip a trailing "_drafting"/"_model" suffix; None if neither present."""
    for suffix in ("_drafting", "_model"):
        if target_domain.endswith(suffix):
            return target_domain[: -len(suffix)]
    return None


def _build_edge_aliases(edges_by_id: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Build edge_id -> canonical_edge_id and canonical -> [collapsed edge_ids].

    Two grouping passes (see module docstring):
      1. fill_patterns_drafting/_model partition collapse (drafting canonical)
      2. dimension_types_{linear,angular,radial,diameter} variant collapse
         (linear canonical)
    """
    alias_of: Dict[str, str] = {}
    collapsed: Dict[str, List[str]] = defaultdict(list)

    # Pass 1: fill_patterns_drafting / fill_patterns_model collapse.
    fill_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for edge_id, edge in edges_by_id.items():
        prefix = _strip_partition_suffix(edge.get("target_domain", ""))
        if prefix is None:
            continue
        fill_groups[(edge.get("source_domain", ""), edge.get("source_field", ""), prefix)].append(edge_id)

    for edge_ids in fill_groups.values():
        if len(edge_ids) < 2:
            continue
        canonical = next(
            (e for e in edge_ids if edges_by_id[e].get("target_domain", "").endswith("_drafting")),
            sorted(edge_ids)[0],
        )
        for e in edge_ids:
            if e != canonical:
                alias_of[e] = canonical
                collapsed[canonical].append(e)

    # Pass 2: dimension_types_{variant} tick_mark collapse (skip edges already aliased).
    dim_groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for edge_id, edge in edges_by_id.items():
        if edge_id in alias_of:
            continue
        source_domain = edge.get("source_domain", "")
        if any(source_domain == f"dimension_types_{variant}" for variant in _DIM_TYPE_VARIANTS):
            dim_groups[(edge.get("source_field", ""), edge.get("target_domain", ""))].append(edge_id)

    for edge_ids in dim_groups.values():
        if len(edge_ids) < 2:
            continue
        canonical = next(
            (e for e in edge_ids if edges_by_id[e].get("source_domain") == "dimension_types_linear"),
            sorted(edge_ids)[0],
        )
        for e in edge_ids:
            if e != canonical:
                alias_of[e] = canonical
                collapsed[canonical].append(e)

    for canonical in collapsed:
        collapsed[canonical].sort()

    return alias_of, dict(collapsed)


def _eligibility_reason(
    edge_a: Dict[str, Any],
    edge_b: Dict[str, Any],
    edge_id_a: str,
    edge_id_b: str,
    whitelist_pairs: Set[Tuple[str, str]],
) -> Optional[str]:
    target_a = edge_a.get("target_domain", "")
    target_b = edge_b.get("target_domain", "")
    source_a = edge_a.get("source_domain", "")
    source_b = edge_b.get("source_domain", "")

    if target_a and target_a == target_b:
        return "shared_target"
    if (target_a and target_a == source_b) or (target_b and target_b == source_a):
        return "chain"
    if (edge_id_a, edge_id_b) in whitelist_pairs or (edge_id_b, edge_id_a) in whitelist_pairs:
        return "whitelist"
    return None


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

    alias_of, collapsed = _build_edge_aliases(edges_by_id)
    if alias_of:
        log(STAGE, f"collapsed {len(alias_of)} edges onto {len(collapsed)} canonical edges: {alias_of}")

    whitelisted_pairs_raw = reference_graph.get("whitelisted_pairs", []) if isinstance(reference_graph, dict) else []
    whitelist_pairs: Set[Tuple[str, str]] = set()
    for pair in whitelisted_pairs_raw or []:
        if isinstance(pair, (list, tuple)) and len(pair) == 2:
            a = alias_of.get(pair[0], pair[0])
            b = alias_of.get(pair[1], pair[1])
            whitelist_pairs.add((a, b))
            whitelist_pairs.add((b, a))

    file_universe: Set[str] = set()
    fired: Dict[str, Set[str]] = defaultdict(set)
    # (edge_id, export_run_id) -> set of distinct source_join_hash values
    join_hashes_by_edge_file: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for row in items_rows:
        export_run_id = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if not export_run_id or not edge_id:
            continue
        canonical_edge_id = alias_of.get(edge_id, edge_id)
        file_universe.add(export_run_id)
        fired[canonical_edge_id].add(export_run_id)
        jh = row.get("source_join_hash", "")
        if jh:
            join_hashes_by_edge_file[(canonical_edge_id, export_run_id)].add(jh)

    n_files_total = len(file_universe)
    log(STAGE, f"file_universe size={n_files_total}")

    # Canonical edge availability: available if any edge collapsed into it (or itself) is available.
    canonical_edge_ids = sorted({alias_of.get(e, e) for e in edges_by_id})
    available_by_canonical: Dict[str, bool] = {}
    for canonical in canonical_edge_ids:
        members = [canonical] + collapsed.get(canonical, [])
        available_by_canonical[canonical] = any(bool(edges_by_id[m].get("available")) for m in members)

    log(STAGE, f"{len(canonical_edge_ids)} canonical edges after aliasing (from {len(edges_by_id)} reference_graph edges)")

    pairs_rows: List[Dict[str, Any]] = []
    pattern_counts: Dict[Tuple[str, str, str, str], int] = defaultdict(int)
    pattern_meta: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    n_skipped_ineligible = 0

    for edge_id_a, edge_id_b in combinations(canonical_edge_ids, 2):
        edge_a = edges_by_id[edge_id_a]
        edge_b = edges_by_id[edge_id_b]

        reason = _eligibility_reason(edge_a, edge_b, edge_id_a, edge_id_b, whitelist_pairs)
        if reason is None:
            n_skipped_ineligible += 1
            continue

        available_a = available_by_canonical[edge_id_a]
        available_b = available_by_canonical[edge_id_b]

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
            "pair_eligibility_reason": reason,
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

    log(STAGE, f"computed {len(pairs_rows)} eligible edge pairs ({n_skipped_ineligible} skipped as ineligible); {len(pattern_counts)} candidate join_hash-pair patterns")

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
            "collapsed_edge_ids_a": "|".join(collapsed.get(edge_id_a, [])),
            "collapsed_edge_ids_b": "|".join(collapsed.get(edge_id_b, [])),
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
