#!/usr/bin/env python3
"""Build human-reviewable drill-down tables for archetype signal clusters.

Inputs:
  - Fingerprint_Out/archetype_analysis/signal_clusters.json
  - Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv
  - results/records/records.csv
  - results/records/file_metadata.csv
  - results/records/identity_items_by_domain/view_filter_definitions.csv (optional)
  - tools/archetype/bip_lookup.json (optional)
  - tools/archetype/shared_param_names.json (optional)
  - tools/archetype/vfd_category_domain_map.json

Output:
  - <out>/review_<cluster_id>.csv -- one file per cluster processed, all in
    a single directory. <out> defaults to
    Fingerprint_Out/archetype_analysis/archetype_review.

Processing:
  For each target cluster, assemble one row per (export_run_id, signal_id)
  carrying the file path, the human-readable Revit element name, and (for
  view_filter_definitions-sourced signals) resolved parameter and category
  names -- everything a reviewer needs to open a specific file and navigate
  directly to the named filter.

  If --cluster-id is omitted, every cluster in signal_clusters.json is
  processed and written to its own <out>/review_<cluster_id>.csv file
  (all in the same directory); a condensed one-line-per-cluster summary is
  printed. If --cluster-id is given, only that cluster is processed and a
  verbose per-file summary is printed.

  Stage 1: Resolve target cluster(s) from signal_clusters.json (clusters are
    keyed by governance_question; cluster_id is unique across the document).
  Stage 2: Find qualifying files from archetype_cluster_classifications.csv
    (rows where cluster_id == target).
  Stage 3: Get (export_run_id, signal_id) -> sig_hash/source_domain from
    archetype_validation_detail.csv, restricted to qualifying files, the
    cluster's signal_ids (matched by edge_id; see Stage 3 below), and the
    archetype_ids that belong to the cluster's governance_question (resolved
    via archetype_classifications.csv / archetype_id naming, the same way
    cluster_archetype_signals.py does). This prevents an edge_id that is
    promoted under more than one governance question/archetype -- each with
    its own join_hash filter -- from leaking unrelated detail rows into this
    cluster's review.
  Stage 4: Stream records.csv (once, across all clusters being processed) to
    resolve sig_hash -> label_display (element_name).
  Stage 5: For view_filter_definitions-sourced signals, resolve parameter
    names (via vf.rule[*].param_ref.id + bip_lookup.json /
    shared_param_names.json) and category names (via vf.categories +
    vfd_category_domain_map.json) from the identity_items shard.
  Stage 6: Resolve export_run_id -> file_path from file_metadata.csv.
  Stage 7: Join everything, sort (templates first, most-signals-fired first,
    all-signals-fired first, export_run_id as tiebreak), and apply --top-n by
    unique export_run_id.
  Stage 8: Write <out>/review_<cluster_id>.csv and print a console summary.

Usage:
    python tools/archetype/prepare_archetype_review.py \\
        --repo-root . \\
        [--cluster-id wall_graphics__cluster_003] \\
        [--signal-clusters Fingerprint_Out/archetype_analysis/signal_clusters.json] \\
        [--cluster-classifications Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv] \\
        [--archetype-classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv] \\
        [--validation-detail Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv] \\
        [--records results/records/records.csv] \\
        [--file-metadata results/records/file_metadata.csv] \\
        [--identity-items-dir results/records/identity_items_by_domain] \\
        [--bip-lookup tools/archetype/bip_lookup.json] \\
        [--shared-param-names tools/archetype/shared_param_names.json] \\
        [--vfd-category-map tools/archetype/vfd_category_domain_map.json] \\
        [--out Fingerprint_Out/archetype_analysis/archetype_review] \\
        [--top-n 20] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    log,
    atomic_write_csv,
    field_matches,
    is_valid_item,
    read_csv_rows,
    read_json,
)

STAGE = "prepare_archetype_review"

GOVERNANCE_ROLE_ORDER = {"Template": 0, "Container": 1, "Project": 2}

OUT_FIELDS = [
    "file_path",
    "export_run_id",
    "governance_role",
    "discipline_label",
    "unit_system",
    "client_label",
    "n_signals_fired",
    "all_signals_fired",
    "signal_id",
    "source_domain",
    "source_join_hash",
    "element_name",
    "sig_hash",
    "param_names",
    "category_names",
]

_PATH_COLUMN_CANDIDATES = ("central_path", "central_path_norm")

_VFD_PARAM_REF_SOURCE_FIELD = "vf.rule[*].param_ref.id"
_VFD_CATEGORIES_KEY = "vf.categories"

_MAX_CONSOLE_EXAMPLES = 5


def _find_cluster(signal_clusters: Dict[str, Any], cluster_id: str) -> Optional[Dict[str, Any]]:
    for c in _all_clusters(signal_clusters):
        if c.get("cluster_id") == cluster_id:
            return c
    return None


def _all_clusters(signal_clusters: Dict[str, Any]) -> List[Dict[str, Any]]:
    clusters_by_gq = signal_clusters.get("clusters", {}) if isinstance(signal_clusters, dict) else {}
    out: List[Dict[str, Any]] = []
    for cluster_defs in clusters_by_gq.values():
        for c in cluster_defs:
            if c.get("cluster_id"):
                out.append(c)
    out.sort(key=lambda c: c.get("cluster_id", ""))
    return out


def _all_cluster_ids(signal_clusters: Dict[str, Any]) -> List[str]:
    return [c["cluster_id"] for c in _all_clusters(signal_clusters)]


def _resolve_param_name(param_id: str, bip_lookup: Dict[str, Any], shared_param_names: Dict[str, Any]) -> str:
    if param_id.startswith("bip:"):
        name = bip_lookup.get(param_id) or bip_lookup.get(param_id[len("bip:"):])
        return name or param_id
    name = shared_param_names.get(param_id)
    return name or param_id


def _parse_category_ids(raw_value: str) -> List[str]:
    """Parse a vf.categories value into an ordered list of category-id strings.

    Accepts both the historical comma-separated shape and a JSON-array shape
    (see build_cross_domain_items.py._parse_vf_categories).
    """
    value = (raw_value or "").strip()
    if not value:
        return []

    comma_parts = [part.strip() for part in value.split(",")]
    if comma_parts and all(part and part.lstrip("+-").isdigit() for part in comma_parts):
        return comma_parts

    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []

    out: List[str] = []
    for item in data:
        if isinstance(item, int):
            out.append(str(item))
        elif isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _resolve_category_name(category_id: str, vfd_category_map: Dict[str, Any]) -> str:
    entry = vfd_category_map.get(category_id)
    if isinstance(entry, dict):
        name = entry.get("name")
        if name:
            return str(name)
    return f"{category_id}[?]"


def _governance_question_from_archetype_id(archetype_id: str) -> str:
    """archetype_id encodes governance_question as the second "__"-delimited
    token, e.g. CANDIDATE__wall_graphics__... -> wall_graphics.

    Mirrors cluster_archetype_signals.py's
    _governance_question_from_archetype_id().
    """
    parts = archetype_id.split("__")
    return parts[1] if len(parts) > 1 else ""


def _build_curated_gq_map(classification_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """archetype_id -> governance_question, from archetype_classifications.csv.

    Human curation can re-assign a promoted archetype to a different
    governance_question without changing its (CANDIDATE-derived) archetype_id,
    so this column is the source of truth wherever it is populated. Mirrors
    cluster_archetype_signals.py's _build_curated_gq_map().
    """
    curated: Dict[str, str] = {}
    for row in classification_rows:
        archetype_id = row.get("archetype_id", "")
        gq = row.get("governance_question", "")
        if archetype_id and gq:
            curated[archetype_id] = gq
    return curated


def _resolve_governance_question(archetype_id: str, curated_gq_map: Dict[str, str]) -> str:
    return curated_gq_map.get(archetype_id) or _governance_question_from_archetype_id(archetype_id)


class ClusterContext:
    """Per-cluster Stage 2/3 results."""

    def __init__(self, cluster: Dict[str, Any]):
        self.cluster_id: str = cluster.get("cluster_id", "")
        self.governance_question: str = cluster.get("governance_question", "")
        self.signal_ids: List[str] = list(cluster.get("signal_ids", []) or [])
        self.cluster_label_stub: str = cluster.get("cluster_label_stub", "")
        self.classification_by_file: Dict[str, Dict[str, str]] = {}
        self.detail_by_file_signal: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        self.qualifying_files: Set[str] = set()
        self.source_domains: Set[str] = set()


def _build_cluster_context(
    cluster: Dict[str, Any],
    rows_by_cluster_id: Dict[str, List[Dict[str, str]]],
    detail_rows_by_export: Dict[str, List[Dict[str, str]]],
    archetype_ids_by_gq: Dict[str, Set[str]],
) -> ClusterContext:
    ctx = ClusterContext(cluster)

    # Stage 2: find qualifying files.
    for row in rows_by_cluster_id.get(ctx.cluster_id, []):
        export_run_id = row.get("export_run_id", "")
        if export_run_id:
            ctx.classification_by_file[export_run_id] = row
    ctx.qualifying_files = set(ctx.classification_by_file.keys())

    # Stage 3: get sig_hashes per file per signal.
    # signal_ids in signal_clusters.json are edge_id nodes (see
    # cluster_archetype_signals.py Stage 1), while archetype_validation_detail.csv's
    # signal_id column may be a curated, human-friendly id distinct from its
    # edge_id. Membership in the cluster is therefore tested against edge_id;
    # the curated signal_id is preserved as the row's display id.
    #
    # The same edge_id can be promoted under more than one governance
    # question/archetype, each with its own join_hash filter, producing
    # separate archetype_validation_detail.csv rows at the (export_run_id,
    # archetype_id, signal_id) grain. Restrict to archetype_ids that belong
    # to this cluster's governance_question so a shared edge_id doesn't pull
    # in elements/signals from an unrelated archetype.
    #
    # A file can fire the same signal on multiple source records (one
    # archetype_validation_detail.csv row per source_join_hash; see
    # n_join_hashes_in_file). source_join_hash is included in the dedup key
    # so every matching element/instance is preserved for review, not just
    # the first one.
    signal_id_set = set(ctx.signal_ids)
    valid_archetype_ids = archetype_ids_by_gq.get(ctx.governance_question, set())
    files_with_detail: Set[str] = set()
    for export_run_id in ctx.qualifying_files:
        for row in detail_rows_by_export.get(export_run_id, []):
            edge_id = row.get("edge_id", "")
            if edge_id not in signal_id_set:
                continue
            if row.get("archetype_id", "") not in valid_archetype_ids:
                continue
            signal_id = row.get("signal_id", "") or edge_id
            source_join_hash = row.get("source_join_hash", "")
            key = (export_run_id, signal_id, source_join_hash)
            if key not in ctx.detail_by_file_signal:
                ctx.detail_by_file_signal[key] = row
            files_with_detail.add(export_run_id)

    for export_run_id in sorted(ctx.qualifying_files - files_with_detail):
        log(STAGE, f"WARNING: cluster_id={ctx.cluster_id}: qualifying file export_run_id={export_run_id} has no matching rows in archetype_validation_detail.csv")

    ctx.source_domains = {row.get("source_domain", "") for row in ctx.detail_by_file_signal.values() if row.get("source_domain")}
    return ctx


def _load_label_lookup(
    records_path: Path,
    qualifying_files: Set[str],
    source_domains: Set[str],
) -> Tuple[Dict[Tuple[str, str], Tuple[str, str]], Dict[Tuple[str, str], str]]:
    """Stage 4: stream records.csv to resolve sig_hash -> (label_display, label_quality)."""
    label_lookup: Dict[Tuple[str, str], Tuple[str, str]] = {}
    vfd_sig_to_record_pk: Dict[Tuple[str, str], str] = {}
    if not records_path.is_file():
        log(STAGE, f"WARNING: records file not found at {records_path}; element_name will be empty")
        return label_lookup, vfd_sig_to_record_pk

    with records_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            export_run_id = row.get("export_run_id", "")
            domain = row.get("domain", "")
            if export_run_id not in qualifying_files or domain not in source_domains:
                continue
            sig_hash = row.get("sig_hash", "")
            if not sig_hash:
                continue
            label_lookup[(export_run_id, sig_hash)] = (
                row.get("label_display", ""),
                row.get("label_quality", ""),
            )
            if domain == "view_filter_definitions":
                vfd_sig_to_record_pk[(export_run_id, sig_hash)] = row.get("record_pk", "")

    log(STAGE, f"resolved {len(label_lookup)} (export_run_id, sig_hash) label rows from {records_path}")
    return label_lookup, vfd_sig_to_record_pk


def _load_vfd_resolution(
    identity_items_dir: Path,
    qualifying_files: Set[str],
    source_domains: Set[str],
    bip_lookup: Dict[str, Any],
    shared_param_names: Dict[str, Any],
    vfd_category_map: Dict[str, Any],
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """Stage 5: resolve parameter names and category names (VFD only)."""
    vfd_resolution: Dict[Tuple[str, str], Tuple[str, str]] = {}
    if "view_filter_definitions" not in source_domains:
        return vfd_resolution

    vfd_identity_items_path = identity_items_dir / "view_filter_definitions.csv"
    vfd_identity_rows = read_csv_rows(vfd_identity_items_path)
    if not vfd_identity_rows:
        log(STAGE, f"WARNING: {vfd_identity_items_path} not found or empty; param_names/category_names will be empty for VFD rows")
        return vfd_resolution

    grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in vfd_identity_rows:
        export_run_id = row.get("export_run_id", "")
        if export_run_id not in qualifying_files:
            continue
        grouped[(export_run_id, row.get("record_pk", ""))].append(row)

    for key, rows in grouped.items():
        param_tokens: List[Tuple[str, str]] = []  # (item_key, item_value)
        categories_raw: Optional[str] = None
        for row in rows:
            item_key = row.get("item_key", "")
            item_value = row.get("item_value", "")
            item_value_type = row.get("item_value_type", "")
            if not is_valid_item(item_value, item_value_type):
                continue
            if field_matches(item_key, _VFD_PARAM_REF_SOURCE_FIELD, "indexed"):
                param_tokens.append((item_key, item_value))
            elif item_key == _VFD_CATEGORIES_KEY:
                categories_raw = item_value

        param_names = " | ".join(
            _resolve_param_name(value, bip_lookup, shared_param_names)
            for _, value in sorted(param_tokens)
        )
        category_names = " | ".join(
            _resolve_category_name(cid, vfd_category_map)
            for cid in _parse_category_ids(categories_raw or "")
        )
        vfd_resolution[key] = (param_names, category_names)

    log(STAGE, f"resolved param/category names for {len(vfd_resolution)} VFD records")
    return vfd_resolution


def _load_file_path_lookup(file_metadata_path: Path) -> Dict[str, str]:
    """Stage 6: resolve export_run_id -> file_path."""
    file_metadata_rows = read_csv_rows(file_metadata_path)
    log(STAGE, f"loaded {len(file_metadata_rows)} rows from {file_metadata_path}")

    path_column = None
    if file_metadata_rows:
        header_keys = file_metadata_rows[0].keys()
        for candidate in _PATH_COLUMN_CANDIDATES:
            if candidate in header_keys:
                path_column = candidate
                break
        if path_column is None:
            log(STAGE, f"WARNING: no path column ({', '.join(_PATH_COLUMN_CANDIDATES)}) found in {file_metadata_path}; falling back to export_run_id")

    file_path_lookup: Dict[str, str] = {}
    for row in file_metadata_rows:
        export_run_id = row.get("export_run_id", "")
        if not export_run_id:
            continue
        file_path_lookup[export_run_id] = row.get(path_column, "") if path_column else ""
    return file_path_lookup


def _sort_key(row: Dict[str, str]) -> Tuple[int, int, int, str]:
    governance_role_rank = GOVERNANCE_ROLE_ORDER.get(row["governance_role"], 3)
    try:
        n_signals_fired = int(row["n_signals_fired"])
    except (TypeError, ValueError):
        n_signals_fired = 0
    all_signals_fired = 1 if row["all_signals_fired"] == "true" else 0
    return (governance_role_rank, -n_signals_fired, -all_signals_fired, row["export_run_id"])


def _process_cluster(
    ctx: ClusterContext,
    label_lookup: Dict[Tuple[str, str], Tuple[str, str]],
    vfd_sig_to_record_pk: Dict[Tuple[str, str], str],
    vfd_resolution: Dict[Tuple[str, str], Tuple[str, str]],
    file_path_lookup: Dict[str, str],
    out_dir: Path,
    top_n: int,
    dry_run: bool,
    verbose: bool,
) -> Dict[str, Any]:
    # Stage 7: assemble and sort the review table.
    review_rows: List[Dict[str, str]] = []
    for (export_run_id, signal_id, source_join_hash), detail in ctx.detail_by_file_signal.items():
        cls = ctx.classification_by_file.get(export_run_id, {})
        source_domain = detail.get("source_domain", "")
        sig_hash = detail.get("sig_hash", "")

        label_display, label_quality = label_lookup.get((export_run_id, sig_hash), ("", ""))
        if label_display:
            element_name = label_display
        else:
            element_name = f"{label_quality}_{sig_hash[:8]}"

        param_names = ""
        category_names = ""
        if source_domain == "view_filter_definitions":
            record_pk = vfd_sig_to_record_pk.get((export_run_id, sig_hash), "")
            param_names, category_names = vfd_resolution.get((export_run_id, record_pk), ("", ""))

        file_path = file_path_lookup.get(export_run_id) or export_run_id

        review_rows.append({
            "file_path": file_path,
            "export_run_id": export_run_id,
            "governance_role": cls.get("governance_role", ""),
            "discipline_label": cls.get("discipline_label", ""),
            "unit_system": cls.get("unit_system", ""),
            "client_label": cls.get("client_label", ""),
            "n_signals_fired": cls.get("n_signals_fired", ""),
            "all_signals_fired": cls.get("all_signals_fired", ""),
            "signal_id": signal_id,
            "source_domain": source_domain,
            "source_join_hash": source_join_hash,
            "element_name": element_name,
            "sig_hash": sig_hash,
            "param_names": param_names,
            "category_names": category_names,
        })

    review_rows.sort(key=_sort_key)

    # Apply --top-n: keep only the first N unique export_run_id values.
    ordered_export_run_ids: List[str] = []
    seen_export_run_ids: Set[str] = set()
    for row in review_rows:
        eid = row["export_run_id"]
        if eid not in seen_export_run_ids:
            seen_export_run_ids.add(eid)
            ordered_export_run_ids.append(eid)

    if top_n > 0:
        selected_export_run_ids = ordered_export_run_ids[:top_n]
    else:
        selected_export_run_ids = ordered_export_run_ids
    selected_set = set(selected_export_run_ids)

    output_rows = [row for row in review_rows if row["export_run_id"] in selected_set]

    # Stage 8: write output.
    out_path = out_dir / f"review_{ctx.cluster_id}.csv"
    if dry_run:
        log(STAGE, f"dry-run: would write {len(output_rows)} rows to {out_path}")
    else:
        atomic_write_csv(out_path, OUT_FIELDS, output_rows)
        log(STAGE, f"wrote {len(output_rows)} rows to {out_path}")

    total_files = len(ctx.qualifying_files)
    n_all_signals_fired = sum(1 for row in ctx.classification_by_file.values() if row.get("all_signals_fired") == "true")
    pct_all = (n_all_signals_fired / total_files * 100.0) if total_files else 0.0

    if verbose:
        print(f"Cluster: {ctx.cluster_id}")
        print(f"Signals: {' | '.join(ctx.signal_ids)}")
        print(f"Total files: {total_files}  |  All signals fired: {n_all_signals_fired} ({pct_all:.1f}%)  |  Top-N shown: {len(selected_export_run_ids)}")
        print("Top example files (templates first):")

        rows_by_file: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for row in output_rows:
            rows_by_file[row["export_run_id"]].append(row)

        for eid in selected_export_run_ids[:_MAX_CONSOLE_EXAMPLES]:
            rows_for_file = rows_by_file.get(eid, [])
            governance_role = rows_for_file[0]["governance_role"] if rows_for_file else ""
            file_label = f"[{governance_role}] " if governance_role else ""
            print(f"  {file_label}{Path(rows_for_file[0]['file_path']).name if rows_for_file else eid}")
            for row in rows_for_file:
                kind = row["signal_id"].split("__")[0]
                if row["source_domain"] == "view_filter_definitions":
                    suffix = f" filter → categories: {row['category_names']}" if row["category_names"] else " filter"
                else:
                    suffix = f" ({row['source_domain']})"
                print(f"    {kind}: \"{row['element_name']}\"{suffix}")

        if len(selected_export_run_ids) > _MAX_CONSOLE_EXAMPLES:
            print(f"  ... ({len(selected_export_run_ids) - _MAX_CONSOLE_EXAMPLES} more in CSV)")

    return {
        "cluster_id": ctx.cluster_id,
        "cluster_label_stub": ctx.cluster_label_stub,
        "total_files": total_files,
        "n_all_signals_fired": n_all_signals_fired,
        "pct_all": pct_all,
        "top_n_shown": len(selected_export_run_ids),
        "n_rows": len(output_rows),
        "out_path": out_path,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--cluster-id", default=None, help="Target cluster_id from signal_clusters.json; if omitted, process all clusters")
    ap.add_argument("--signal-clusters", default=None, help="Path to signal_clusters.json")
    ap.add_argument("--cluster-classifications", default=None, help="Path to archetype_cluster_classifications.csv")
    ap.add_argument("--archetype-classifications", default=None, help="Path to archetype_classifications.csv")
    ap.add_argument("--validation-detail", default=None, help="Path to archetype_validation_detail.csv")
    ap.add_argument("--records", default=None, help="Path to records.csv")
    ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv")
    ap.add_argument("--identity-items-dir", default=None, help="Path to identity_items_by_domain/")
    ap.add_argument("--bip-lookup", default=None, help="Path to bip_lookup.json")
    ap.add_argument("--shared-param-names", default=None, help="Path to shared_param_names.json")
    ap.add_argument("--vfd-category-map", default=None, help="Path to vfd_category_domain_map.json")
    ap.add_argument("--out", default=None, help="Output directory; each cluster is written to <out>/review_<cluster_id>.csv")
    ap.add_argument("--top-n", type=int, default=20, help="Limit to top N files per cluster; 0 = all")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    analysis_dir = repo_root / "Fingerprint_Out" / "archetype_analysis"

    signal_clusters_path = Path(args.signal_clusters) if args.signal_clusters else analysis_dir / "signal_clusters.json"
    cluster_classifications_path = Path(args.cluster_classifications) if args.cluster_classifications else analysis_dir / "archetype_cluster_classifications.csv"
    archetype_classifications_path = Path(args.archetype_classifications) if args.archetype_classifications else analysis_dir / "archetype_classifications.csv"
    validation_detail_path = Path(args.validation_detail) if args.validation_detail else analysis_dir / "archetype_validation_detail.csv"
    records_path = Path(args.records) if args.records else repo_root / "results" / "records" / "records.csv"
    file_metadata_path = Path(args.file_metadata) if args.file_metadata else repo_root / "results" / "records" / "file_metadata.csv"
    identity_items_dir = Path(args.identity_items_dir) if args.identity_items_dir else repo_root / "results" / "records" / "identity_items_by_domain"
    bip_lookup_path = Path(args.bip_lookup) if args.bip_lookup else repo_root / "tools" / "archetype" / "bip_lookup.json"
    shared_param_names_path = Path(args.shared_param_names) if args.shared_param_names else repo_root / "tools" / "archetype" / "shared_param_names.json"
    vfd_category_map_path = Path(args.vfd_category_map) if args.vfd_category_map else repo_root / "tools" / "archetype" / "vfd_category_domain_map.json"

    out_dir = Path(args.out) if args.out else analysis_dir / "archetype_review"
    if not out_dir.is_absolute():
        out_dir = repo_root / out_dir

    # Stage 1: resolve target cluster(s).
    if not signal_clusters_path.is_file():
        log(STAGE, f"ERROR: signal_clusters.json not found at {signal_clusters_path}")
        log(STAGE, "Run tools/archetype/cluster_archetype_signals.py first to generate it.")
        return 1

    signal_clusters = read_json(signal_clusters_path, default={}) or {}

    if args.cluster_id:
        cluster = _find_cluster(signal_clusters, args.cluster_id)
        if cluster is None:
            available = _all_cluster_ids(signal_clusters)
            log(STAGE, f"ERROR: cluster_id={args.cluster_id!r} not found in {signal_clusters_path}")
            log(STAGE, f"available cluster_ids ({len(available)}): {', '.join(available)}")
            return 1
        clusters = [cluster]
        verbose = True
    else:
        clusters = _all_clusters(signal_clusters)
        if not clusters:
            log(STAGE, f"no clusters found in {signal_clusters_path}")
            return 0
        log(STAGE, f"--cluster-id not given; processing all {len(clusters)} clusters from {signal_clusters_path}")
        verbose = False

    # Stage 2/3 inputs (shared across clusters).
    cluster_classification_rows = read_csv_rows(cluster_classifications_path)
    log(STAGE, f"loaded {len(cluster_classification_rows)} rows from {cluster_classifications_path}")
    rows_by_cluster_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in cluster_classification_rows:
        cluster_id = row.get("cluster_id", "")
        if cluster_id:
            rows_by_cluster_id[cluster_id].append(row)

    validation_detail_rows = read_csv_rows(validation_detail_path)
    log(STAGE, f"loaded {len(validation_detail_rows)} rows from {validation_detail_path}")
    detail_rows_by_export: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in validation_detail_rows:
        export_run_id = row.get("export_run_id", "")
        if export_run_id:
            detail_rows_by_export[export_run_id].append(row)

    # archetype_id -> governance_question, for restricting Stage 3 detail
    # rows to the cluster's own governance_question (see _build_cluster_context).
    archetype_classification_rows = read_csv_rows(archetype_classifications_path)
    log(STAGE, f"loaded {len(archetype_classification_rows)} rows from {archetype_classifications_path}")
    curated_gq_map = _build_curated_gq_map(archetype_classification_rows)
    archetype_ids_by_gq: Dict[str, Set[str]] = defaultdict(set)
    for row in validation_detail_rows:
        archetype_id = row.get("archetype_id", "")
        if archetype_id:
            archetype_ids_by_gq[_resolve_governance_question(archetype_id, curated_gq_map)].add(archetype_id)

    contexts: List[ClusterContext] = []
    union_qualifying_files: Set[str] = set()
    union_source_domains: Set[str] = set()
    for cluster in clusters:
        ctx = _build_cluster_context(cluster, rows_by_cluster_id, detail_rows_by_export, archetype_ids_by_gq)
        log(STAGE, f"cluster_id={ctx.cluster_id} cluster_label_stub={ctx.cluster_label_stub} n_signals={len(ctx.signal_ids)} qualifying_files={len(ctx.qualifying_files)} detail_rows={len(ctx.detail_by_file_signal)}")
        contexts.append(ctx)
        union_qualifying_files |= ctx.qualifying_files
        union_source_domains |= ctx.source_domains

    # Stage 4 (shared, single pass over records.csv).
    label_lookup, vfd_sig_to_record_pk = _load_label_lookup(records_path, union_qualifying_files, union_source_domains)

    # Stage 5 (shared).
    bip_lookup = read_json(bip_lookup_path, default={}) or {}
    shared_param_names = read_json(shared_param_names_path, default={}) or {}
    vfd_category_map = read_json(vfd_category_map_path, default={}) or {}
    log(STAGE, f"loaded bip_lookup ({len(bip_lookup)} entries) from {bip_lookup_path}")
    log(STAGE, f"loaded shared_param_names ({len(shared_param_names)} entries) from {shared_param_names_path}")
    vfd_resolution = _load_vfd_resolution(
        identity_items_dir, union_qualifying_files, union_source_domains,
        bip_lookup, shared_param_names, vfd_category_map,
    )

    # Stage 6 (shared).
    file_path_lookup = _load_file_path_lookup(file_metadata_path)

    # Stages 7/8 (per cluster).
    results: List[Dict[str, Any]] = []
    for ctx in contexts:
        result = _process_cluster(
            ctx, label_lookup, vfd_sig_to_record_pk, vfd_resolution, file_path_lookup,
            out_dir, args.top_n, args.dry_run, verbose=verbose,
        )
        results.append(result)

    if not verbose:
        print(f"Processed {len(results)} clusters -> {out_dir}")
        for r in results:
            print(
                f"  {r['cluster_id']:<50} total_files={r['total_files']:<5} "
                f"all_fired={r['n_all_signals_fired']} ({r['pct_all']:.1f}%)  "
                f"rows_written={r['n_rows']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
