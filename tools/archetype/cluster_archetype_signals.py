#!/usr/bin/env python3
"""Cluster co-varying archetype signals into composite groups.

Inputs:
  - Fingerprint_Out/archetype_analysis/archetype_validation_pairs.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation.csv
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv

Outputs:
  - Fingerprint_Out/archetype_analysis/signal_clusters.json
  - Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv
  - Fingerprint_Out/archetype_analysis/cluster_coverage_summary.json

Processing:
  This script sits between join_hash-based classification (which detects
  presence) and human approach_label promotion (which assigns meaning). It
  clusters signals that structurally travel together into composite groups,
  then rolls up file classifications to the cluster grain.

  Stage 1: For each governance_question (the second "__"-delimited token of
    archetype_id), build a weighted undirected signal graph: nodes are
    unique signal_id values from archetype_validation.csv; edges are unique
    (edge_id_a, edge_id_b) pairs from archetype_validation_pairs.csv, weighted
    by the maximum top_pair_containment observed across archetype_ids.

  Stage 2: Derive a global coupling threshold via Jenks natural breaks
    (n_classes=2, threshold = breaks[0]) over all unique pairwise containment
    values. Falls back to 0.8 (with a warning) if fewer than 4 distinct
    values exist. Retain only edges with max_containment >= threshold.

  Stage 3: Find connected components on the thresholded graph per
    governance_question using union-find. Each component is a signal
    cluster; unconnected signals form singleton clusters.

  Stage 4: Generate a stable cluster_label_stub per cluster from the bare
    parameter names of its member signals.

  Stage 5: Write signal_clusters.json (the recovery artifact -- written
    before any other output).

  Stage 6: Roll up archetype_classifications.csv to file x governance_question
    x cluster grain in archetype_cluster_classifications.csv.

  Stage 7: Write cluster_coverage_summary.json (per governance_question x
    cluster adoption counts) to drive approach_label promotion decisions.

Usage:
    python tools/archetype/cluster_archetype_signals.py \\
        --pairs Fingerprint_Out/archetype_analysis/archetype_validation_pairs.csv \\
        --validation Fingerprint_Out/archetype_analysis/archetype_validation.csv \\
        --classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv \\
        --out-clusters Fingerprint_Out/archetype_analysis/signal_clusters.json \\
        --out-classifications Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv \\
        --out-summary Fingerprint_Out/archetype_analysis/cluster_coverage_summary.json \\
        [--coupling-threshold 0.8] [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (  # noqa: E402
    log,
    atomic_write_csv,
    atomic_write_json,
    read_csv_rows,
    SCHEMA_VERSION,
)
from jenks_utils import jenks_breaks  # noqa: E402

STAGE = "cluster_archetype_signals"

_MIN_DISTINCT_FOR_JENKS = 4
_FALLBACK_THRESHOLD = 0.8
_LABEL_MAX_LEN = 60
_HASH_SUFFIX_RE = re.compile(r"__[0-9a-fA-F]{4,}$")

CLUSTER_CLASSIFICATIONS_FIELDS = [
    "export_run_id",
    "governance_question",
    "cluster_id",
    "cluster_label_stub",
    "n_signals_in_cluster",
    "n_signals_fired",
    "all_signals_fired",
    "any_signal_fired",
    "max_confidence_tier",
    "is_mixed_cluster",
    "client_label",
    "governance_role",
    "discipline_label",
    "unit_system",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _governance_question_from_archetype_id(archetype_id: str) -> str:
    """archetype_id encodes governance_question as the second "__"-delimited
    token, e.g. CANDIDATE__wall_graphics__... -> wall_graphics."""
    parts = archetype_id.split("__")
    return parts[1] if len(parts) > 1 else ""


def _bare_signal_name(signal_id: str) -> str:
    """Strip a "<domain prefix>." prefix and a trailing "__<hash>" suffix."""
    name = signal_id.split(".", 1)[1] if "." in signal_id else signal_id
    return _HASH_SUFFIX_RE.sub("", name)


def _cluster_label_stub(governance_question: str, signal_ids: List[str]) -> str:
    bare_names = sorted({_bare_signal_name(s) for s in signal_ids})
    if len(bare_names) == 1:
        combo = bare_names[0]
    else:
        combo = "_x_".join(bare_names[:2])
    combo = combo[:_LABEL_MAX_LEN]
    return f"{governance_question}__{combo}"


class UnionFind:
    """Disjoint-set forest for finding connected components."""

    def __init__(self, items: Set[str]) -> None:
        self.parent: Dict[str, str] = {x: x for x in items}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if ra < rb:
            self.parent[rb] = ra
        else:
            self.parent[ra] = rb


def _build_signal_graph(
    validation_rows: List[Dict[str, str]],
    pairs_rows: List[Dict[str, str]],
) -> Tuple[Dict[str, Set[str]], Dict[str, Dict[Tuple[str, str], float]]]:
    nodes_by_gq: Dict[str, Set[str]] = defaultdict(set)
    for row in validation_rows:
        gq = _governance_question_from_archetype_id(row.get("archetype_id", ""))
        signal_id = row.get("signal_id", "")
        if signal_id:
            nodes_by_gq[gq].add(signal_id)

    edges_by_gq: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)
    for row in pairs_rows:
        gq = _governance_question_from_archetype_id(row.get("archetype_id", ""))
        edge_a = row.get("edge_id_a", "")
        edge_b = row.get("edge_id_b", "")
        if not edge_a or not edge_b:
            continue
        try:
            containment = float(row.get("top_pair_containment") or 0.0)
        except ValueError:
            containment = 0.0

        key = tuple(sorted((edge_a, edge_b)))
        existing = edges_by_gq[gq].get(key)
        if existing is None or containment > existing:
            edges_by_gq[gq][key] = containment

        nodes_by_gq[gq].add(edge_a)
        nodes_by_gq[gq].add(edge_b)

    return dict(nodes_by_gq), dict(edges_by_gq)


def _derive_coupling_threshold(
    edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
    override: Optional[float],
) -> float:
    if override is not None:
        log(STAGE, f"coupling_threshold={override:.4f} (CLI override)")
        return override

    all_containments: List[float] = []
    for edges in edges_by_gq.values():
        all_containments.extend(edges.values())

    distinct = sorted(set(all_containments))
    if len(distinct) < _MIN_DISTINCT_FOR_JENKS:
        log(
            STAGE,
            f"WARNING: only {len(distinct)} distinct pairwise containment value(s) "
            f"(<{_MIN_DISTINCT_FOR_JENKS}); falling back to coupling_threshold={_FALLBACK_THRESHOLD}",
        )
        return _FALLBACK_THRESHOLD

    breaks = jenks_breaks(all_containments, n_classes=2)
    threshold = breaks[0]
    log(
        STAGE,
        f"coupling_threshold={threshold:.4f} (Jenks natural breaks over "
        f"{len(all_containments)} pairs, {len(distinct)} distinct values)",
    )
    return threshold


def _apply_threshold(
    edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
    threshold: float,
) -> Dict[str, Dict[Tuple[str, str], float]]:
    kept_by_gq: Dict[str, Dict[Tuple[str, str], float]] = {}
    for gq, edges in edges_by_gq.items():
        before = len(edges)
        kept = {k: v for k, v in edges.items() if v >= threshold}
        kept_by_gq[gq] = kept
        log(STAGE, f"governance_question={gq}: edges before threshold={before}, after={len(kept)}")
    return kept_by_gq


def _build_clusters(
    nodes_by_gq: Dict[str, Set[str]],
    edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
) -> Dict[str, List[Dict[str, Any]]]:
    clusters_by_gq: Dict[str, List[Dict[str, Any]]] = {}

    for gq in sorted(nodes_by_gq):
        nodes = nodes_by_gq[gq]
        uf = UnionFind(nodes)
        kept_edges = edges_by_gq.get(gq, {})
        for edge_a, edge_b in kept_edges:
            uf.union(edge_a, edge_b)

        members_by_root: Dict[str, List[str]] = defaultdict(list)
        for node in nodes:
            members_by_root[uf.find(node)].append(node)

        min_containment_by_root: Dict[str, float] = {}
        for (edge_a, edge_b), containment in kept_edges.items():
            root = uf.find(edge_a)
            if root not in min_containment_by_root or containment < min_containment_by_root[root]:
                min_containment_by_root[root] = containment

        cluster_defs: List[Dict[str, Any]] = []
        for root, members in members_by_root.items():
            signal_ids = sorted(members)
            cluster_defs.append({
                "cluster_label_stub": _cluster_label_stub(gq, signal_ids),
                "governance_question": gq,
                "n_signals": len(signal_ids),
                "signal_ids": signal_ids,
                "min_containment": min_containment_by_root.get(root),
                "is_singleton": len(signal_ids) == 1,
            })

        cluster_defs.sort(key=lambda c: (-c["n_signals"], c["cluster_label_stub"]))
        for i, c in enumerate(cluster_defs, start=1):
            c["cluster_id"] = f"{gq}__cluster_{i:03d}"

        clusters_by_gq[gq] = cluster_defs
        log(STAGE, f"governance_question={gq}: {len(nodes)} signals -> {len(cluster_defs)} clusters")

    return clusters_by_gq


def _build_signal_cluster_map(
    clusters_by_gq: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Tuple[str, str, int]]]:
    signal_cluster_map: Dict[str, Dict[str, Tuple[str, str, int]]] = {}
    for gq, cluster_defs in clusters_by_gq.items():
        m: Dict[str, Tuple[str, str, int]] = {}
        for c in cluster_defs:
            for signal_id in c["signal_ids"]:
                m[signal_id] = (c["cluster_id"], c["cluster_label_stub"], c["n_signals"])
        signal_cluster_map[gq] = m
    return signal_cluster_map


def _rollup_classifications(
    classification_rows: List[Dict[str, str]],
    signal_cluster_map: Dict[str, Dict[str, Tuple[str, str, int]]],
) -> List[Dict[str, Any]]:
    # (export_run_id, governance_question, cluster_id) -> accumulator
    agg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in classification_rows:
        archetype_id = row.get("archetype_id", "")
        gq = _governance_question_from_archetype_id(archetype_id)
        export_run_id = row.get("export_run_id", "")
        confidence_tier = row.get("confidence_tier", "")
        signals_fired = [s for s in (row.get("signals_fired", "") or "").split(";") if s]
        cluster_map = signal_cluster_map.get(gq, {})

        for signal_id in signals_fired:
            info = cluster_map.get(signal_id)
            if info is None:
                log(
                    STAGE,
                    f"WARNING: signal_id={signal_id} (archetype_id={archetype_id}, "
                    f"governance_question={gq}) not found in signal cluster map; skipping",
                )
                continue
            cluster_id, cluster_label_stub, n_signals_in_cluster = info
            key = (export_run_id, gq, cluster_id)
            entry = agg.setdefault(key, {
                "cluster_label_stub": cluster_label_stub,
                "n_signals_in_cluster": n_signals_in_cluster,
                "fired_signals": set(),
                "has_full": False,
                "client_label": row.get("client_label", ""),
                "governance_role": row.get("governance_role", ""),
                "discipline_label": row.get("discipline_label", ""),
                "unit_system": row.get("unit_system", ""),
            })
            entry["fired_signals"].add(signal_id)
            if confidence_tier == "Full":
                entry["has_full"] = True

    # is_mixed_cluster: file fired on >1 cluster for the same governance_question
    clusters_by_file_gq: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for export_run_id, gq, cluster_id in agg:
        clusters_by_file_gq[(export_run_id, gq)].add(cluster_id)

    cluster_rows: List[Dict[str, Any]] = []
    for (export_run_id, gq, cluster_id), entry in sorted(agg.items()):
        n_fired = len(entry["fired_signals"])
        n_in_cluster = entry["n_signals_in_cluster"]
        is_mixed = len(clusters_by_file_gq[(export_run_id, gq)]) > 1
        cluster_rows.append({
            "export_run_id": export_run_id,
            "governance_question": gq,
            "cluster_id": cluster_id,
            "cluster_label_stub": entry["cluster_label_stub"],
            "n_signals_in_cluster": n_in_cluster,
            "n_signals_fired": n_fired,
            "all_signals_fired": "true" if n_fired == n_in_cluster else "false",
            "any_signal_fired": "true",
            "max_confidence_tier": "Full" if entry["has_full"] else "Partial",
            "is_mixed_cluster": "true" if is_mixed else "false",
            "client_label": entry["client_label"],
            "governance_role": entry["governance_role"],
            "discipline_label": entry["discipline_label"],
            "unit_system": entry["unit_system"],
        })

    return cluster_rows


def _build_coverage_summary(
    clusters_by_gq: Dict[str, List[Dict[str, Any]]],
    cluster_rows: List[Dict[str, Any]],
    total_files: int,
) -> Dict[str, Dict[str, Any]]:
    by_cluster: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in cluster_rows:
        by_cluster[(row["governance_question"], row["cluster_id"])].append(row)

    summary: Dict[str, Dict[str, Any]] = {}
    for gq, cluster_defs in clusters_by_gq.items():
        summary[gq] = {}
        for c in cluster_defs:
            rows_for_cluster = by_cluster.get((gq, c["cluster_id"]), [])
            n_any = len(rows_for_cluster)
            n_all = sum(1 for r in rows_for_cluster if r["all_signals_fired"] == "true")
            n_mixed = sum(1 for r in rows_for_cluster if r["is_mixed_cluster"] == "true")
            pct_all = round(n_all / total_files * 100.0, 2) if total_files else 0.0
            summary[gq][c["cluster_id"]] = {
                "n_files_any_signal": n_any,
                "n_files_all_signals": n_all,
                "n_files_mixed": n_mixed,
                "pct_files_all_signals": pct_all,
            }

    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--pairs", default=None, help="Path to archetype_validation_pairs.csv")
    ap.add_argument("--validation", default=None, help="Path to archetype_validation.csv")
    ap.add_argument("--classifications", default=None, help="Path to archetype_classifications.csv")
    ap.add_argument("--out-clusters", default=None, help="Output path for signal_clusters.json")
    ap.add_argument("--out-classifications", default=None, help="Output path for archetype_cluster_classifications.csv")
    ap.add_argument("--out-summary", default=None, help="Output path for cluster_coverage_summary.json")
    ap.add_argument("--coupling-threshold", type=float, default=None, help="Override the Jenks-derived coupling threshold")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    analysis_dir = repo_root / "Fingerprint_Out" / "archetype_analysis"
    pairs_path = Path(args.pairs) if args.pairs else analysis_dir / "archetype_validation_pairs.csv"
    validation_path = Path(args.validation) if args.validation else analysis_dir / "archetype_validation.csv"
    classifications_path = Path(args.classifications) if args.classifications else analysis_dir / "archetype_classifications.csv"
    out_clusters_path = Path(args.out_clusters) if args.out_clusters else analysis_dir / "signal_clusters.json"
    out_classifications_path = Path(args.out_classifications) if args.out_classifications else analysis_dir / "archetype_cluster_classifications.csv"
    out_summary_path = Path(args.out_summary) if args.out_summary else analysis_dir / "cluster_coverage_summary.json"

    validation_rows = read_csv_rows(validation_path)
    log(STAGE, f"loaded {len(validation_rows)} rows from {validation_path}")

    pairs_rows = read_csv_rows(pairs_path)
    log(STAGE, f"loaded {len(pairs_rows)} rows from {pairs_path}")

    classification_rows = read_csv_rows(classifications_path)
    log(STAGE, f"loaded {len(classification_rows)} rows from {classifications_path}")

    # Stage 1: signal graph per governance_question
    nodes_by_gq, edges_by_gq = _build_signal_graph(validation_rows, pairs_rows)

    # Stage 2: coupling threshold + apply
    coupling_threshold = _derive_coupling_threshold(edges_by_gq, args.coupling_threshold)
    thresholded_edges_by_gq = _apply_threshold(edges_by_gq, coupling_threshold)

    # Stage 3 + 4: connected components -> clusters + labels
    clusters_by_gq = _build_clusters(nodes_by_gq, thresholded_edges_by_gq)

    signal_clusters_doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _utc_now_iso(),
        "coupling_threshold": round(coupling_threshold, 4),
        "clusters": clusters_by_gq,
    }

    # Stage 6: rollup
    signal_cluster_map = _build_signal_cluster_map(clusters_by_gq)
    cluster_rows = _rollup_classifications(classification_rows, signal_cluster_map)
    log(STAGE, f"emitted {len(cluster_rows)} archetype_cluster_classifications rows")

    # Stage 7: coverage summary
    total_files = len({r.get("export_run_id", "") for r in classification_rows if r.get("export_run_id")})
    coverage_summary_doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _utc_now_iso(),
        "total_files": total_files,
        "summary": _build_coverage_summary(clusters_by_gq, cluster_rows, total_files),
    }

    if args.dry_run:
        n_clusters = sum(len(v) for v in clusters_by_gq.values())
        log(
            STAGE,
            f"dry-run: would write {n_clusters} clusters to {out_clusters_path}, "
            f"{len(cluster_rows)} rows to {out_classifications_path}, and "
            f"a coverage summary to {out_summary_path}",
        )
        return 0

    # Stage 5: signal_clusters.json is the recovery artifact -- write first.
    atomic_write_json(out_clusters_path, signal_clusters_doc)
    log(STAGE, f"wrote signal clusters to {out_clusters_path}")

    atomic_write_csv(out_classifications_path, CLUSTER_CLASSIFICATIONS_FIELDS, cluster_rows)
    log(STAGE, f"wrote {len(cluster_rows)} rows to {out_classifications_path}")

    atomic_write_json(out_summary_path, coverage_summary_doc)
    log(STAGE, f"wrote cluster coverage summary to {out_summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
