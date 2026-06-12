#!/usr/bin/env python3
"""Cluster co-varying archetype signals into composite groups.

Inputs:
  - Fingerprint_Out/archetype_analysis/archetype_validation_pairs.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation.csv
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - results/records/file_metadata.csv

Outputs:
  - Fingerprint_Out/archetype_analysis/signal_clusters.json
  - Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv
  - Fingerprint_Out/archetype_analysis/cluster_coverage_summary.json

Processing:
  This script sits between join_hash-based classification (which detects
  presence) and human approach_label promotion (which assigns meaning). It
  clusters signals that structurally travel together into composite groups,
  then rolls up file classifications to the cluster grain.

  Stage 1: For each governance_question, build a weighted undirected signal
    graph: nodes are unique edge_id values from archetype_validation.csv
    (signal_id is mapped to its edge_id via that row's signal_id -> edge_id
    pairing, since curated definitions may give a signal a human-friendly
    signal_id distinct from its canonical edge_id); edges are unique
    (edge_id_a, edge_id_b) pairs from archetype_validation_pairs.csv, weighted
    by Jaccard similarity (n_both / (count_a + count_b - n_both), maximum
    across archetype_ids), where count_a / count_b are looked up by
    (archetype_id, edge_id) from archetype_validation.csv's
    n_files_classified -- n_files_classified is emitted at (archetype_id,
    signal_id) grain after that signal's join_hash filter is applied, so the
    same edge_id can carry different counts under different archetype_ids;
    keying by edge_id alone would let one archetype's count silently
    overwrite another's. n_both is top_pair_file_count clamped to
    min(top_pair_file_count, count_a, count_b): top_pair_file_count is the
    max co-occurrence count across *any* join_hash pair on the two edges
    (unfiltered), which can exceed either signal's own filtered file count
    when a promoted archetype's join_hash filters were edited away from that
    top pattern; without the clamp this can produce Jaccard > 1 and let an
    unrelated join_hash configuration for the same edge pair drive the
    threshold and complete-linkage merges. Jaccard (vs. raw containment)
    avoids the asymmetric problem where a rare signal that is a strict subset
    of a common signal scores a perfect top_pair_containment despite being a
    poor coupling indicator. Pairs whose (archetype_id, edge_id_a) or
    (archetype_id, edge_id_b) has no n_files_classified entry are logged and
    skipped. governance_question is normally the second "__"-delimited token
    of archetype_id (e.g. CANDIDATE__wall_graphics__... -> wall_graphics), but
    archetype_classifications.csv's governance_question column -- which may
    have been edited during human curation -- takes precedence wherever an
    archetype_id appears there.

  Stage 2: Derive a global coupling threshold via Jenks natural breaks
    (n_classes=2, threshold = breaks[0]) over all unique pairwise Jaccard
    similarity values. Falls back to 0.8 (with a warning) if fewer than 4
    distinct values exist. Retain only edges with max_jaccard >= threshold.
    The legacy top_pair_containment-based threshold is also computed and
    logged (for visibility into the effect of the Jaccard switch) but is not
    used for thresholding.

  Stage 3: Build complete-linkage clusters per governance_question: starting
    from singleton clusters, repeatedly consider the pair (a, b) with the
    highest Jaccard among pairs whose clusters differ (ties broken by
    edge_id_a asc, then edge_id_b asc); merge cluster(a) and cluster(b) only
    if every pairwise Jaccard within the merged cluster (including pairs not
    present in archetype_validation_pairs.csv, treated as Jaccard 0.0) is
    >= the coupling threshold. This prevents chain bridging: a signal only
    joins a cluster if it is directly similar (above threshold) to every
    existing member, not merely transitively connected to one of them via a
    chain of pairwise edges. Unmerged signals form singleton clusters.

  Stage 4: Generate a stable cluster_label_stub per cluster from the bare
    parameter names of its member signals.

  Stage 5: Write signal_clusters.json (the recovery artifact -- written
    before any other output).

  Stage 6: Roll up archetype_classifications.csv to file x governance_question
    x cluster grain in archetype_cluster_classifications.csv. Fired signal_ids
    are mapped to their edge_id (Stage 1's signal_id -> edge_id pairing)
    before cluster lookup, and rows are grouped under the curated
    governance_question (see Stage 1).

  Stage 7: Write cluster_coverage_summary.json (per governance_question x
    cluster adoption counts) to drive approach_label promotion decisions.
    pct_files_all_signals is computed against total_files, the size of the
    same file universe assign_archetype_classifications.py uses (every file
    with at least one cross_domain_items.csv edge observation, unioned with
    every file in file_metadata.csv) -- not the count of distinct
    export_run_ids in archetype_classifications.csv, which omits files with
    zero promoted-archetype evidence and would understate the denominator.

Usage:
    python tools/archetype/cluster_archetype_signals.py \\
        --pairs Fingerprint_Out/archetype_analysis/archetype_validation_pairs.csv \\
        --validation Fingerprint_Out/archetype_analysis/archetype_validation.csv \\
        --classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --file-metadata results/records/file_metadata.csv \\
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


def _build_curated_gq_map(classification_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """archetype_id -> governance_question, from archetype_classifications.csv.

    Human curation can re-assign a promoted archetype to a different
    governance_question without changing its (CANDIDATE-derived) archetype_id,
    so this column is the source of truth wherever it is populated.
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


def _complete_linkage_clusters(
    nodes: Set[str],
    pair_jaccard: Dict[Tuple[str, str], float],
    threshold: float,
) -> List[Set[str]]:
    """Group nodes into clusters such that every pairwise Jaccard within a
    cluster meets ``threshold`` (complete linkage).

    Pairs absent from ``pair_jaccard`` are treated as Jaccard 0.0 (no
    observed co-occurrence), never as high similarity. Candidate pairs are
    processed in (Jaccard desc, edge_id_a asc, edge_id_b asc) order; a merge
    is applied only if it does not push any pairwise Jaccard within the
    resulting cluster below ``threshold``. This is deterministic and avoids
    single-linkage chain bridging.
    """

    def pair_value(a: str, b: str) -> float:
        if a == b:
            return 1.0
        return pair_jaccard.get(tuple(sorted((a, b))), 0.0)

    clusters: Dict[str, Set[str]] = {n: {n} for n in nodes}

    ordered_pairs = sorted(
        ((k, v) for k, v in pair_jaccard.items() if k[0] in nodes and k[1] in nodes),
        key=lambda kv: (-kv[1], kv[0][0], kv[0][1]),
    )

    for (edge_a, edge_b), _jaccard in ordered_pairs:
        cluster_a = clusters[edge_a]
        cluster_b = clusters[edge_b]
        if cluster_a is cluster_b:
            continue
        candidate = cluster_a | cluster_b
        if all(
            pair_value(x, y) >= threshold
            for x in candidate
            for y in candidate
            if x != y
        ):
            for node in candidate:
                clusters[node] = candidate

    seen_ids: Set[int] = set()
    result: List[Set[str]] = []
    for node in nodes:
        cluster = clusters[node]
        if id(cluster) not in seen_ids:
            seen_ids.add(id(cluster))
            result.append(cluster)
    return result


def _build_n_files_classified_lookup(validation_rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], float]:
    """(archetype_id, edge_id) -> n_files_classified, from archetype_validation.csv.

    n_files_classified is emitted at (archetype_id, signal_id) grain *after*
    applying that signal's join_hash filter (see
    validate_archetype_signals.py), so the same edge_id can carry different
    counts under different archetype_ids whose signals filter it
    differently. Keying by edge_id alone would let one archetype's count
    silently overwrite another's. archetype_validation_pairs.csv rows carry
    archetype_id, so callers look up by (archetype_id, edge_id).
    """
    lookup: Dict[Tuple[str, str], float] = {}
    for row in validation_rows:
        archetype_id = row.get("archetype_id", "")
        edge_id = row.get("edge_id", "")
        if not edge_id:
            continue
        try:
            value = float(row.get("n_files_classified") or 0.0)
        except ValueError:
            continue
        key = (archetype_id, edge_id)
        existing = lookup.get(key)
        if existing is not None and existing != value:
            log(
                STAGE,
                f"WARNING: archetype_id={archetype_id} edge_id={edge_id} has multiple "
                f"n_files_classified values ({existing}, {value}) across its signals; "
                f"using {value}",
            )
        lookup[key] = value
    return lookup


def _build_signal_graph(
    validation_rows: List[Dict[str, str]],
    pairs_rows: List[Dict[str, str]],
    curated_gq_map: Dict[str, str],
    n_files_classified: Dict[str, float],
) -> Tuple[
    Dict[str, Set[str]],
    Dict[str, Dict[Tuple[str, str], float]],
    Dict[str, Dict[Tuple[str, str], float]],
    Dict[str, Dict[str, str]],
]:
    nodes_by_gq: Dict[str, Set[str]] = defaultdict(set)
    signal_to_edge_by_gq: Dict[str, Dict[str, str]] = defaultdict(dict)
    for row in validation_rows:
        gq = _resolve_governance_question(row.get("archetype_id", ""), curated_gq_map)
        signal_id = row.get("signal_id", "")
        edge_id = row.get("edge_id", "") or signal_id
        if edge_id:
            nodes_by_gq[gq].add(edge_id)
        if signal_id:
            signal_to_edge_by_gq[gq][signal_id] = edge_id

    jaccard_edges_by_gq: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)
    containment_edges_by_gq: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)
    for row in pairs_rows:
        archetype_id = row.get("archetype_id", "")
        gq = _resolve_governance_question(archetype_id, curated_gq_map)
        edge_a = row.get("edge_id_a", "")
        edge_b = row.get("edge_id_b", "")
        if not edge_a or not edge_b:
            continue
        try:
            containment = float(row.get("top_pair_containment") or 0.0)
        except ValueError:
            containment = 0.0
        try:
            top_pair_file_count = float(row.get("top_pair_file_count") or 0.0)
        except ValueError:
            top_pair_file_count = 0.0

        count_a = n_files_classified.get((archetype_id, edge_a))
        count_b = n_files_classified.get((archetype_id, edge_b))
        if count_a is None or count_b is None:
            missing = edge_a if count_a is None else edge_b
            log(
                STAGE,
                f"WARNING: (archetype_id={archetype_id}, edge_id={missing}) not found in "
                f"archetype_validation.csv n_files_classified lookup for pair "
                f"({edge_a}, {edge_b}), governance_question={gq}; skipping pair",
            )
            continue

        # top_pair_file_count is the max co-occurrence count across *any*
        # join_hash pair on these edges (unfiltered), not necessarily the
        # count for this archetype's specific (filtered) signal pair. The
        # true filtered intersection can be no larger than either signal's
        # own filtered file count, so clamp to that bound -- this keeps
        # Jaccard in [0, 1] and avoids an unrelated join_hash configuration
        # for the same edge pair inflating the similarity.
        n_both = min(top_pair_file_count, count_a, count_b)
        if n_both < top_pair_file_count:
            log(
                STAGE,
                f"archetype_id={archetype_id}: clamped top_pair_file_count="
                f"{top_pair_file_count:g} to n_both={n_both:g} (count_a={count_a:g}, "
                f"count_b={count_b:g}) for pair ({edge_a}, {edge_b})",
            )

        denom = count_a + count_b - n_both
        jaccard = (n_both / denom) if denom > 0 else 0.0

        key = tuple(sorted((edge_a, edge_b)))

        existing_jaccard = jaccard_edges_by_gq[gq].get(key)
        if existing_jaccard is None or jaccard > existing_jaccard:
            jaccard_edges_by_gq[gq][key] = jaccard

        existing_containment = containment_edges_by_gq[gq].get(key)
        if existing_containment is None or containment > existing_containment:
            containment_edges_by_gq[gq][key] = containment

        nodes_by_gq[gq].add(edge_a)
        nodes_by_gq[gq].add(edge_b)

    return (
        dict(nodes_by_gq),
        dict(jaccard_edges_by_gq),
        dict(containment_edges_by_gq),
        {gq: dict(m) for gq, m in signal_to_edge_by_gq.items()},
    )


def _jenks_threshold_for_values(values: List[float], label: str) -> float:
    distinct = sorted(set(values))
    if len(distinct) < _MIN_DISTINCT_FOR_JENKS:
        log(
            STAGE,
            f"WARNING: only {len(distinct)} distinct pairwise {label} value(s) "
            f"(<{_MIN_DISTINCT_FOR_JENKS}); falling back to {label}-based threshold={_FALLBACK_THRESHOLD}",
        )
        return _FALLBACK_THRESHOLD

    breaks = jenks_breaks(values, n_classes=2)
    threshold = breaks[0]
    log(
        STAGE,
        f"{label}-based threshold={threshold:.4f} (Jenks natural breaks over "
        f"{len(values)} pairs, {len(distinct)} distinct values)",
    )
    return threshold


def _derive_coupling_threshold(
    jaccard_edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
    containment_edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
    override: Optional[float],
) -> float:
    # Always compute and log the legacy top_pair_containment-based threshold
    # for visibility into the effect of switching to Jaccard, even though it
    # is no longer used for thresholding.
    all_containments: List[float] = []
    for edges in containment_edges_by_gq.values():
        all_containments.extend(edges.values())
    _jenks_threshold_for_values(all_containments, "top_pair_containment")

    all_jaccards: List[float] = []
    for edges in jaccard_edges_by_gq.values():
        all_jaccards.extend(edges.values())

    if override is not None:
        log(STAGE, f"coupling_threshold={override:.4f} (CLI override, applied to Jaccard values)")
        return override

    threshold = _jenks_threshold_for_values(all_jaccards, "jaccard")
    log(STAGE, f"coupling_threshold={threshold:.4f} (jaccard-based)")
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
    jaccard_edges_by_gq: Dict[str, Dict[Tuple[str, str], float]],
    threshold: float,
) -> Dict[str, List[Dict[str, Any]]]:
    clusters_by_gq: Dict[str, List[Dict[str, Any]]] = {}

    for gq in sorted(nodes_by_gq):
        nodes = nodes_by_gq[gq]
        pair_jaccard = jaccard_edges_by_gq.get(gq, {})

        member_groups = _complete_linkage_clusters(nodes, pair_jaccard, threshold)

        cluster_defs: List[Dict[str, Any]] = []
        for members in member_groups:
            signal_ids = sorted(members)
            min_jaccard = None
            if len(signal_ids) > 1:
                min_jaccard = min(
                    pair_jaccard.get(tuple(sorted((x, y))), 0.0)
                    for i, x in enumerate(signal_ids)
                    for y in signal_ids[i + 1:]
                )
            cluster_defs.append({
                "cluster_label_stub": _cluster_label_stub(gq, signal_ids),
                "governance_question": gq,
                "n_signals": len(signal_ids),
                "signal_ids": signal_ids,
                "min_containment": min_jaccard,
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
    signal_to_edge_by_gq: Dict[str, Dict[str, str]],
    curated_gq_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    # (export_run_id, governance_question, cluster_id) -> accumulator
    agg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in classification_rows:
        archetype_id = row.get("archetype_id", "")
        gq = _resolve_governance_question(archetype_id, curated_gq_map)
        export_run_id = row.get("export_run_id", "")
        confidence_tier = row.get("confidence_tier", "")
        signals_fired = [s for s in (row.get("signals_fired", "") or "").split(";") if s]
        cluster_map = signal_cluster_map.get(gq, {})
        signal_to_edge_map = signal_to_edge_by_gq.get(gq, {})

        for signal_id in signals_fired:
            edge_id = signal_to_edge_map.get(signal_id, signal_id)
            info = cluster_map.get(edge_id)
            if info is None:
                log(
                    STAGE,
                    f"WARNING: signal_id={signal_id} (edge_id={edge_id}, archetype_id={archetype_id}, "
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
                "client_label": "",
                "governance_role": "",
                "discipline_label": "",
                "unit_system": "",
            })
            entry["fired_signals"].add(edge_id)
            if confidence_tier == "Full":
                entry["has_full"] = True
            # client_label/governance_role/discipline_label/unit_system are
            # pass-through metadata, not dedup keys: take the first
            # non-empty value seen across all rows in this group.
            for field in ("client_label", "governance_role", "discipline_label", "unit_system"):
                if not entry[field]:
                    value = row.get(field, "")
                    if value:
                        entry[field] = value

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


def _compute_file_universe(
    cross_domain_items_rows: List[Dict[str, str]],
    file_metadata_rows: List[Dict[str, str]],
) -> Set[str]:
    """The same file universe assign_archetype_classifications.py uses: every
    file with at least one cross_domain_items.csv edge observation, unioned
    with every file in file_metadata.csv. archetype_classifications.csv only
    contains files with at least one fired required signal, so it is not a
    safe source for the corpus-wide denominator."""
    universe: Set[str] = set()
    for row in cross_domain_items_rows:
        eid = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if eid and edge_id:
            universe.add(eid)
    for row in file_metadata_rows:
        eid = row.get("export_run_id", "")
        if eid:
            universe.add(eid)
    return universe


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
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv (file universe for coverage denominators)")
    ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv (file universe for coverage denominators)")
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
    cross_domain_items_path = Path(args.cross_domain_items) if args.cross_domain_items else analysis_dir / "cross_domain_items.csv"
    file_metadata_path = Path(args.file_metadata) if args.file_metadata else repo_root / "results" / "records" / "file_metadata.csv"
    out_clusters_path = Path(args.out_clusters) if args.out_clusters else analysis_dir / "signal_clusters.json"
    out_classifications_path = Path(args.out_classifications) if args.out_classifications else analysis_dir / "archetype_cluster_classifications.csv"
    out_summary_path = Path(args.out_summary) if args.out_summary else analysis_dir / "cluster_coverage_summary.json"

    validation_rows = read_csv_rows(validation_path)
    log(STAGE, f"loaded {len(validation_rows)} rows from {validation_path}")

    pairs_rows = read_csv_rows(pairs_path)
    log(STAGE, f"loaded {len(pairs_rows)} rows from {pairs_path}")

    classification_rows = read_csv_rows(classifications_path)
    log(STAGE, f"loaded {len(classification_rows)} rows from {classifications_path}")

    cross_domain_items_rows = read_csv_rows(cross_domain_items_path)
    log(STAGE, f"loaded {len(cross_domain_items_rows)} rows from {cross_domain_items_path}")

    file_metadata_rows = read_csv_rows(file_metadata_path)
    log(STAGE, f"loaded {len(file_metadata_rows)} rows from {file_metadata_path}")

    curated_gq_map = _build_curated_gq_map(classification_rows)
    n_files_classified = _build_n_files_classified_lookup(validation_rows)

    # Stage 1: signal graph per governance_question
    nodes_by_gq, jaccard_edges_by_gq, containment_edges_by_gq, signal_to_edge_by_gq = _build_signal_graph(
        validation_rows, pairs_rows, curated_gq_map, n_files_classified,
    )

    # Stage 2: coupling threshold (Jaccard-based; legacy containment-based threshold logged for comparison)
    coupling_threshold = _derive_coupling_threshold(jaccard_edges_by_gq, containment_edges_by_gq, args.coupling_threshold)
    _apply_threshold(jaccard_edges_by_gq, coupling_threshold)  # diagnostic: pairwise edges meeting threshold, before complete-linkage

    # Stage 3 + 4: complete-linkage clusters + labels
    clusters_by_gq = _build_clusters(nodes_by_gq, jaccard_edges_by_gq, coupling_threshold)

    signal_clusters_doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _utc_now_iso(),
        "coupling_threshold": round(coupling_threshold, 4),
        "clusters": clusters_by_gq,
    }

    # Stage 6: rollup
    signal_cluster_map = _build_signal_cluster_map(clusters_by_gq)
    cluster_rows = _rollup_classifications(classification_rows, signal_cluster_map, signal_to_edge_by_gq, curated_gq_map)
    log(STAGE, f"emitted {len(cluster_rows)} archetype_cluster_classifications rows")

    # Stage 7: coverage summary
    total_files = len(_compute_file_universe(cross_domain_items_rows, file_metadata_rows))
    log(STAGE, f"file_universe size={total_files}")
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
