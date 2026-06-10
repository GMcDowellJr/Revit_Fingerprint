#!/usr/bin/env python3
"""Generate reference_graph.json for the cross-domain archetype discovery pipeline.

Inputs:
  - config/archetype/static_edges_seed.json   (hand-maintained structural edges)
  - identity_items_by_domain/*.csv            (per-domain identity item shards)
  - vfd_dynamic_edges.csv                     (optional; VFD param/category inventory)
  - vfd_param_inventory.csv                   (optional; VFD param occurrence inventory)
  - bip_lookup.json                           (optional; builtin parameter id -> name)
  - shared_param_names.json                   (optional; shared parameter guid -> name)

Output:
  - Fingerprint_Out/archetype_analysis/reference_graph.json

Processing:
  1. For each static edge, validate the "available" flag by checking that
     source_field appears as item_key in the source domain's identity_items
     shard with at least one row carrying a usable value
     (item_value not in {<NONE>, '', ...} and item_value_type == "ok").
     Indexed fields (source_field containing "[*]") use prefix/suffix matching.
  2. Build dynamic edges from vfd_dynamic_edges.csv: exclude rows with
     kind == "project", apply support_threshold (file_count >= threshold),
     group by (param_id, target_domain) to produce scope_conditions with
     param_ids and category_ids lists. Param names are resolved via
     bip_lookup.json / shared_param_names.json. edge_id slug =
     "vfd__{target_domain}.{param_name_normalized}".
  3. Merge static (with computed available flags) + dynamic edges and write
     reference_graph.json.

Usage:
    python tools/archetype/generate_reference_graph.py \\
        --repo-root . \\
        --identity-items-dir Fingerprint_Out/identity_items_by_domain \\
        --static-edges config/archetype/static_edges_seed.json \\
        --out Fingerprint_Out/archetype_analysis/reference_graph.json \\
        [--vfd-dynamic-edges <path>] [--vfd-param-inventory <path>] \\
        [--bip-lookup <path>] [--shared-param-names <path>] \\
        [--support-threshold 10] [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from _common import (
    field_matches,
    is_valid_item,
    log,
    atomic_write_json,
    read_csv_rows,
    read_json,
    SCHEMA_VERSION,
)

STAGE = "generate_reference_graph"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _check_static_edge_availability(
    edge: Dict[str, Any],
    identity_items_dir: Path,
    shard_cache: Dict[str, List[Dict[str, str]]],
) -> Dict[str, Any]:
    source_domain = edge["source_domain"]
    source_field = edge["source_field"]
    field_match = edge.get("field_match", "exact")

    if source_domain not in shard_cache:
        shard_path = identity_items_dir / f"{source_domain}.csv"
        rows = read_csv_rows(shard_path)
        shard_cache[source_domain] = rows
        log(STAGE, f"loaded {len(rows)} identity_items rows for domain={source_domain}")

    rows = shard_cache[source_domain]
    if not rows:
        return {
            "available": False,
            "evidence_rows_total": 0,
            "evidence_rows_valid": 0,
            "evidence_note": "source_domain_identity_items_missing_or_empty",
        }

    total = 0
    valid = 0
    for row in rows:
        item_key = row.get("item_key", "")
        if not field_matches(item_key, source_field, field_match):
            continue
        total += 1
        if is_valid_item(row.get("item_value", ""), row.get("item_value_type", "")):
            valid += 1

    return {
        "available": valid > 0,
        "evidence_rows_total": total,
        "evidence_rows_valid": valid,
        "evidence_note": "ok" if valid > 0 else "no_rows_with_usable_value",
    }


def _normalize_param_name(name: str) -> str:
    out = []
    for ch in name:
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")


def _resolve_param_name(param_id: str, bip_lookup: Dict[str, Any], shared_param_names: Dict[str, Any]) -> str:
    if param_id.startswith("bip:"):
        name = bip_lookup.get(param_id) or bip_lookup.get(param_id[len("bip:"):])
        return name or param_id
    name = shared_param_names.get(param_id)
    return name or param_id


def _build_dynamic_edges(
    vfd_dynamic_edges_path: Path,
    bip_lookup: Dict[str, Any],
    shared_param_names: Dict[str, Any],
    support_threshold: int,
) -> List[Dict[str, Any]]:
    rows = read_csv_rows(vfd_dynamic_edges_path)
    if not rows:
        log(STAGE, f"vfd_dynamic_edges not found or empty at {vfd_dynamic_edges_path}; skipping dynamic edges")
        return []

    log(STAGE, f"loaded {len(rows)} rows from {vfd_dynamic_edges_path}")

    # group rows by (param_id, target_domain)
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    skipped_project_kind = 0
    skipped_below_threshold = 0
    for row in rows:
        kind = (row.get("kind") or "").strip().lower()
        if kind == "project":
            skipped_project_kind += 1
            continue
        param_id = (row.get("param_id") or "").strip()
        target_domain = (row.get("target_domain") or "").strip()
        if not param_id or not target_domain:
            continue
        try:
            file_count = int(float(row.get("file_count") or 0))
        except (TypeError, ValueError):
            file_count = 0
        if file_count < support_threshold:
            skipped_below_threshold += 1
            continue
        groups[(param_id, target_domain)].append(row)

    log(
        STAGE,
        f"dynamic edge grouping: {len(groups)} (param_id, target_domain) groups; "
        f"skipped_project_kind={skipped_project_kind} skipped_below_threshold={skipped_below_threshold}",
    )

    edges: List[Dict[str, Any]] = []
    for (param_id, target_domain), group_rows in sorted(groups.items()):
        category_ids: Set[str] = set()
        max_file_count = 0
        for row in group_rows:
            cat_id = (row.get("category_id") or "").strip()
            if cat_id:
                category_ids.add(cat_id)
            try:
                fc = int(float(row.get("file_count") or 0))
            except (TypeError, ValueError):
                fc = 0
            max_file_count = max(max_file_count, fc)

        param_name = _resolve_param_name(param_id, bip_lookup, shared_param_names)
        param_name_normalized = _normalize_param_name(param_name)
        edge_id = f"vfd__{target_domain}.{param_name_normalized}"

        edges.append({
            "edge_id": edge_id,
            "source_domain": "view_filter_definitions",
            "source_field": "vf.rule[*].param_ref.id",
            "target_domain": target_domain,
            "edge_type": "dynamic",
            "direction": "forward",
            "field_match": "indexed",
            "available": True,
            "evidence_note": "derived_from_vfd_dynamic_edges",
            "support_file_count": max_file_count,
            "scope_conditions": {
                "param_ids": [param_id],
                "category_ids": sorted(category_ids),
            },
            "param_name": param_name,
        })

    return edges


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default config/output paths)")
    ap.add_argument("--identity-items-dir", default=None, help="Path to identity_items_by_domain/ (default: <repo-root>/Fingerprint_Out/identity_items_by_domain)")
    ap.add_argument("--static-edges", default=None, help="Path to static_edges_seed.json (default: <repo-root>/config/archetype/static_edges_seed.json)")
    ap.add_argument("--out", default=None, help="Output path for reference_graph.json (default: <repo-root>/Fingerprint_Out/archetype_analysis/reference_graph.json)")
    ap.add_argument("--vfd-dynamic-edges", default=None, help="Path to vfd_dynamic_edges.csv")
    ap.add_argument("--vfd-param-inventory", default=None, help="Path to vfd_param_inventory.csv (currently informational only)")
    ap.add_argument("--bip-lookup", default=None, help="Path to bip_lookup.json")
    ap.add_argument("--shared-param-names", default=None, help="Path to shared_param_names.json")
    ap.add_argument("--support-threshold", type=int, default=10, help="Minimum file_count for dynamic edges (default: 10)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output, just log what would happen")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    identity_items_dir = Path(args.identity_items_dir) if args.identity_items_dir else repo_root / "Fingerprint_Out" / "identity_items_by_domain"
    static_edges_path = Path(args.static_edges) if args.static_edges else repo_root / "config" / "archetype" / "static_edges_seed.json"
    out_path = Path(args.out) if args.out else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    vfd_dynamic_edges_path = Path(args.vfd_dynamic_edges) if args.vfd_dynamic_edges else repo_root / "tests" / "output" / "vfd_dynamic_edges.csv"
    vfd_param_inventory_path = Path(args.vfd_param_inventory) if args.vfd_param_inventory else repo_root / "tests" / "output" / "vfd_param_inventory.csv"
    bip_lookup_path = Path(args.bip_lookup) if args.bip_lookup else repo_root / "tests" / "output" / "bip_lookup.json"
    shared_param_names_path = Path(args.shared_param_names) if args.shared_param_names else repo_root / "tests" / "output" / "shared_param_names.json"

    static_seed = read_json(static_edges_path, default={})
    static_edges = static_seed.get("edges", []) if isinstance(static_seed, dict) else []
    log(STAGE, f"loaded {len(static_edges)} static edges from {static_edges_path}")

    shard_cache: Dict[str, List[Dict[str, str]]] = {}
    resolved_static_edges: List[Dict[str, Any]] = []
    n_available = 0
    n_unavailable = 0
    for edge in static_edges:
        availability = _check_static_edge_availability(edge, identity_items_dir, shard_cache)
        out_edge = dict(edge)
        out_edge.update(availability)
        resolved_static_edges.append(out_edge)
        if out_edge["available"]:
            n_available += 1
        else:
            n_unavailable += 1

    log(STAGE, f"static edges resolved: available={n_available} unavailable={n_unavailable}")

    bip_lookup = read_json(bip_lookup_path, default={}) or {}
    shared_param_names = read_json(shared_param_names_path, default={}) or {}
    log(STAGE, f"loaded bip_lookup ({len(bip_lookup)} entries) from {bip_lookup_path}")
    log(STAGE, f"loaded shared_param_names ({len(shared_param_names)} entries) from {shared_param_names_path}")

    # vfd_param_inventory.csv is read for visibility/logging only at this stage;
    # name resolution is driven by bip_lookup.json / shared_param_names.json per spec.
    param_inventory_rows = read_csv_rows(vfd_param_inventory_path)
    log(STAGE, f"loaded {len(param_inventory_rows)} rows from {vfd_param_inventory_path} (informational)")

    dynamic_edges = _build_dynamic_edges(
        vfd_dynamic_edges_path, bip_lookup, shared_param_names, args.support_threshold,
    )
    log(STAGE, f"generated {len(dynamic_edges)} dynamic edges")

    all_edges = resolved_static_edges + dynamic_edges

    reference_graph = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _utc_now_iso(),
        "support_threshold": args.support_threshold,
        "edge_count": len(all_edges),
        "edges": all_edges,
    }

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(all_edges)} edges to {out_path}")
        return 0

    atomic_write_json(out_path, reference_graph)
    log(STAGE, f"wrote {len(all_edges)} edges to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
