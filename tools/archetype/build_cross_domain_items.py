#!/usr/bin/env python3
"""Build cross_domain_items.csv for the cross-domain archetype discovery pipeline.

Inputs:
  - Fingerprint_Out/archetype_analysis/reference_graph.json
  - records.csv
  - results/records/identity_items_by_domain/*.csv

Output:
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
    columns: export_run_id, edge_id, source_domain, target_domain,
             source_record_pk, source_join_hash, target_ref_value, target_join_hash

Processing:
  - Unavailable edges (available == false in reference_graph.json) are
    skipped entirely -- no rows are emitted for them.
  - Structural edges: filter identity_items where item_key matches
    source_field (exact or indexed prefix/suffix) and item_value is usable
    (not a sentinel, item_value_type == "ok"). Join records.csv on
    (export_run_id, domain=source_domain, record_pk=record_pk) for
    source_join_hash. Join records.csv again on
    (export_run_id, domain=target_domain, sig_hash=item_value) for
    target_join_hash -- the target record must exist in the SAME
    export_run_id; a sig_hash that only matches a record in a different
    export_run_id leaves target_join_hash empty (a structural edge is not
    "resolved" for a file based on cross-file evidence).
  - Dynamic VFD edges: filter identity_items on
    vf.rule[*].param_ref.id matching scope_conditions.param_ids, requiring
    the same record's vf.categories item to contain at least one
    scope_conditions.category_id (vf.categories may be a comma-separated
    string of category ids). target_join_hash is null for dynamic edges.

Usage:
    python tools/archetype/build_cross_domain_items.py \\
        --repo-root . \\
        --reference-graph Fingerprint_Out/archetype_analysis/reference_graph.json \\
        --records-csv results/records/records.csv \\
        --identity-items-dir results/records/identity_items_by_domain \\
        --out Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from _common import (
    field_matches,
    is_valid_item,
    log,
    atomic_write_csv,
    read_csv_rows,
    read_json,
)

STAGE = "build_cross_domain_items"

OUT_FIELDS = [
    "export_run_id",
    "edge_id",
    "source_domain",
    "target_domain",
    "source_record_pk",
    "source_join_hash",
    "target_ref_value",
    "target_join_hash",
]


def _load_identity_items(identity_items_dir: Path, domain: str, cache: Dict[str, List[Dict[str, str]]]) -> List[Dict[str, str]]:
    if domain not in cache:
        rows = read_csv_rows(identity_items_dir / f"{domain}.csv")
        cache[domain] = rows
        log(STAGE, f"loaded {len(rows)} identity_items rows for domain={domain}")
    return cache[domain]


def _build_structural_rows(
    edge: Dict[str, Any],
    identity_items_dir: Path,
    item_cache: Dict[str, List[Dict[str, str]]],
    source_join_hash_idx: Dict[Tuple[str, str, str], str],
    target_join_hash_idx: Dict[Tuple[str, str, str], str],
) -> List[Dict[str, str]]:
    source_domain = edge["source_domain"]
    target_domain = edge["target_domain"]
    source_field = edge["source_field"]
    field_match = edge.get("field_match", "exact")
    edge_id = edge["edge_id"]

    out_rows: List[Dict[str, str]] = []
    for row in _load_identity_items(identity_items_dir, source_domain, item_cache):
        item_key = row.get("item_key", "")
        if not field_matches(item_key, source_field, field_match):
            continue
        item_value = row.get("item_value", "")
        if not is_valid_item(item_value, row.get("item_value_type", "")):
            continue

        export_run_id = row.get("export_run_id", "")
        record_pk = row.get("record_pk", "")
        source_join_hash = source_join_hash_idx.get((export_run_id, source_domain, record_pk), "")
        target_join_hash = target_join_hash_idx.get((export_run_id, target_domain, item_value), "")

        out_rows.append({
            "export_run_id": export_run_id,
            "edge_id": edge_id,
            "source_domain": source_domain,
            "target_domain": target_domain,
            "source_record_pk": record_pk,
            "source_join_hash": source_join_hash,
            "target_ref_value": item_value,
            "target_join_hash": target_join_hash,
        })
    return out_rows


def _build_dynamic_rows(
    edge: Dict[str, Any],
    identity_items_dir: Path,
    item_cache: Dict[str, List[Dict[str, str]]],
    source_join_hash_idx: Dict[Tuple[str, str, str], str],
) -> List[Dict[str, str]]:
    source_domain = edge["source_domain"]
    target_domain = edge["target_domain"]
    source_field = edge["source_field"]
    field_match = edge.get("field_match", "indexed")
    edge_id = edge["edge_id"]
    scope = edge.get("scope_conditions", {}) or {}
    param_ids: Set[str] = set(scope.get("param_ids", []) or [])
    category_ids: Set[str] = set(scope.get("category_ids", []) or [])

    # Group rows by (export_run_id, record_pk) so we can evaluate
    # vf.rule[*].param_ref.id and vf.categories together per record.
    by_record: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in _load_identity_items(identity_items_dir, source_domain, item_cache):
        key = (row.get("export_run_id", ""), row.get("record_pk", ""))
        by_record[key].append(row)

    out_rows: List[Dict[str, str]] = []
    for (export_run_id, record_pk), rows in by_record.items():
        param_match = False
        for row in rows:
            item_key = row.get("item_key", "")
            if not field_matches(item_key, source_field, field_match):
                continue
            if not is_valid_item(row.get("item_value", ""), row.get("item_value_type", "")):
                continue
            if row.get("item_value", "") in param_ids:
                param_match = True
                break
        if not param_match:
            continue

        category_match = False
        for row in rows:
            if row.get("item_key", "") != "vf.categories":
                continue
            if not is_valid_item(row.get("item_value", ""), row.get("item_value_type", "")):
                continue
            record_categories = {c.strip() for c in row.get("item_value", "").split(",") if c.strip()}
            if record_categories & category_ids:
                category_match = True
                break
        if not category_match:
            continue

        source_join_hash = source_join_hash_idx.get((export_run_id, source_domain, record_pk), "")
        out_rows.append({
            "export_run_id": export_run_id,
            "edge_id": edge_id,
            "source_domain": source_domain,
            "target_domain": target_domain,
            "source_record_pk": record_pk,
            "source_join_hash": source_join_hash,
            "target_ref_value": "",
            "target_join_hash": "",
        })
    return out_rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--records-csv", default=None, help="Path to records.csv")
    ap.add_argument("--identity-items-dir", default=None, help="Path to identity_items_by_domain/ (default: <repo-root>/results/records/identity_items_by_domain)")
    ap.add_argument("--out", default=None, help="Output path for cross_domain_items.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    records_csv_path = Path(args.records_csv) if args.records_csv else repo_root / "results" / "records" / "records.csv"
    identity_items_dir = Path(args.identity_items_dir) if args.identity_items_dir else repo_root / "results" / "records" / "identity_items_by_domain"
    out_path = Path(args.out) if args.out else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_items.csv"

    reference_graph = read_json(reference_graph_path, default={})
    edges = reference_graph.get("edges", []) if isinstance(reference_graph, dict) else []
    log(STAGE, f"loaded {len(edges)} edges from {reference_graph_path}")

    available_edges = [e for e in edges if e.get("available")]
    skipped_unavailable = len(edges) - len(available_edges)
    log(STAGE, f"available_edges={len(available_edges)} skipped_unavailable={skipped_unavailable}")

    records_rows = read_csv_rows(records_csv_path)
    log(STAGE, f"loaded {len(records_rows)} rows from {records_csv_path}")

    # (export_run_id, domain, record_pk) -> join_hash
    source_join_hash_idx: Dict[Tuple[str, str, str], str] = {}
    # (export_run_id, domain, sig_hash) -> join_hash  (first occurrence wins within
    # an export; sig_hash is content-addressed but a target reference only counts
    # as resolved if the target record exists in the SAME export_run_id -- a
    # cross-file match would make a structural edge look resolved for a file that
    # never actually carried that target).
    target_join_hash_idx: Dict[Tuple[str, str, str], str] = {}
    for r in records_rows:
        export_run_id = r.get("export_run_id", "")
        domain = r.get("domain", "")
        record_pk = r.get("record_pk", "")
        join_hash = r.get("join_hash", "")
        sig_hash = r.get("sig_hash", "")
        if export_run_id and domain and record_pk:
            source_join_hash_idx[(export_run_id, domain, record_pk)] = join_hash
        if export_run_id and domain and sig_hash and (export_run_id, domain, sig_hash) not in target_join_hash_idx:
            target_join_hash_idx[(export_run_id, domain, sig_hash)] = join_hash

    item_cache: Dict[str, List[Dict[str, str]]] = {}
    out_rows: List[Dict[str, str]] = []
    for edge in available_edges:
        if edge.get("edge_type") == "dynamic":
            rows = _build_dynamic_rows(edge, identity_items_dir, item_cache, source_join_hash_idx)
        else:
            rows = _build_structural_rows(edge, identity_items_dir, item_cache, source_join_hash_idx, target_join_hash_idx)
        log(STAGE, f"edge={edge['edge_id']} ({edge.get('edge_type', 'structural')}) -> {len(rows)} rows")
        out_rows.extend(rows)

    log(STAGE, f"total rows={len(out_rows)}")

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(out_rows)} rows to {out_path}")
        return 0

    atomic_write_csv(out_path, OUT_FIELDS, out_rows)
    log(STAGE, f"wrote {len(out_rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
