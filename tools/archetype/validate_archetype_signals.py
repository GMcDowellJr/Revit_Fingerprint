#!/usr/bin/env python3
"""Validate archetype signal coherence at sig_hash grain.

Inputs:
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - config/archetype/archetype_definitions.json (only promoted == true entries)
  - Fingerprint_Out/archetype_analysis/reference_graph.json
  - results/records/records.csv
  - Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv (optional;
    required for archetype_validation_pairs.csv co-variation tiers)

Outputs:
  - Fingerprint_Out/archetype_analysis/archetype_validation.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv
  - Fingerprint_Out/archetype_analysis/archetype_validation_pairs.csv

Processing:
  - Build an edge alias map from reference_graph.json (see
    compute_cross_domain_cooccurrence.py / _common.build_edge_aliases).
    Promoted archetype signals reference canonical edge_ids; cross_domain_items.csv
    rows for edges collapsed onto a canonical edge_id are folded into it when
    indexing by (export_run_id, edge_id).
  - For each promoted archetype, build signal_id -> (edge_id, join_hash)
    from its signals.
  - For each archetype_classifications.csv row, split signals_fired
    (semicolon-separated signal_ids). For each fired signal, look up the
    cross_domain_items.csv rows for (export_run_id, canonical edge_id) to get
    the (source_domain, source_join_hash, target_join_hash) tuples that fired
    the signal in that file. If the signal has a non-wildcard join_hash
    filter (i.e. join_hash is neither null nor empty -- same wildcard
    semantics as Stage 3's _evaluate_signal), rows are restricted to those
    whose source_join_hash or target_join_hash matches it -- otherwise
    unrelated instances of the same edge in a file (e.g. multiple dimension
    types or materials) would inflate
    n_distinct_sig_hashes / n_multi_instance_files.
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
                                  Fragmented (>=0.8) | set_domain (see below)
      n_multi_instance_files  -- files where the signal fired with >1
                                  distinct join_hash
      domain_type             -- "singleton" if
                                  n_multi_instance_files / n_files_classified
                                  <= 0.5, else "set". Set-domain signals fire
                                  with many records per file, so per-file
                                  hash diversity is not a meaningful coherence
                                  signal; coherence_tier is forced to
                                  "set_domain" for these (coherence_score is
                                  still computed/emitted for reference).
                                  Singleton signals keep the existing
                                  coherence_tier logic unchanged.
      top5_hash_coverage_pct  -- set-domain signals only: percentage of this
                                  edge's records (across all files, by
                                  source_join_hash) covered by its 5 most
                                  frequent join_hash values. Empty string for
                                  singleton signals, or if
                                  --cross-domain-items was not provided.
  - archetype_validation_pairs.csv: one row per promoted archetype (assumes
    exactly two signals per archetype):
      archetype_id, edge_id_a, edge_id_b
      n_files_both_present    -- files where both (canonical) edges have at
                                  least one record in cross_domain_items.csv
      top_pair_file_count     -- max file_count from cross_domain_patterns.csv
                                  for any (join_hash_a, join_hash_b) pair on
                                  this edge pair
      top_pair_containment    -- top_pair_file_count / n_files_both_present
      covariation_tier        -- "Strong" (containment >= 0.40) |
                                  "Moderate" (>= 0.20) | "Weak" (>= 0.05) |
                                  "None" (< 0.05 or n_files_both_present < 10) |
                                  "Deferred" (cross_domain_patterns.csv not
                                  provided, or has no rows for this edge pair)
      data_notes              -- pipe-separated flags: "wildcard_signals" if
                                  either signal's join_hash is null,
                                  "insufficient_data" if
                                  n_files_both_present < 10

Usage:
    python tools/archetype/validate_archetype_signals.py \\
        --repo-root . \\
        --archetype-classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --archetype-definitions config/archetype/archetype_definitions.json \\
        --records-csv results/records/records.csv \\
        --cross-domain-patterns Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv \\
        --out-dir Fingerprint_Out/archetype_analysis \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from _common import (
    log,
    atomic_write_csv,
    build_edge_aliases,
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
    "domain_type",
    "top5_hash_coverage_pct",
]

DETAIL_FIELDS = [
    "export_run_id",
    "archetype_id",
    "signal_id",
    "edge_id",
    "source_domain",
    "source_record_pk",
    "source_join_hash",
    "sig_hash",
    "n_join_hashes_in_file",
]

PAIRS_FIELDS = [
    "archetype_id",
    "edge_id_a",
    "edge_id_b",
    "n_files_both_present",
    "top_pair_file_count",
    "top_pair_containment",
    "covariation_tier",
    "data_notes",
]

_TOP_N_HASHES = 5


def _coherence_tier(score: float) -> str:
    if score < 0.3:
        return "Convergent"
    if score < 0.8:
        return "Variable"
    return "Fragmented"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (code/config root; durable config defaults live here)")
    ap.add_argument("--assigned-root", default=None, help="Assigned/export root containing archetype_analysis/ and results/; omitted preserves legacy repo-local defaults")
    ap.add_argument("--archetype-classifications", default=None, help="Path to archetype_classifications.csv")
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv")
    ap.add_argument("--archetype-definitions", default=None, help="Path to archetype_definitions.json")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--records-csv", default=None, help="Path to records.csv")
    ap.add_argument("--cross-domain-patterns", default=None, help="Path to cross_domain_patterns.csv (required for archetype_validation_pairs.csv co-variation tiers)")
    ap.add_argument("--out-dir", default=None, help="Output directory")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    assigned_root = Path(args.assigned_root).resolve() if args.assigned_root else repo_root / "Fingerprint_Out"
    analysis_dir = assigned_root / "archetype_analysis"
    records_root = assigned_root / "results" if args.assigned_root else repo_root / "results"
    log(STAGE, f"repo_root={repo_root}")
    log(STAGE, f"assigned_root={assigned_root}")

    classifications_path = Path(args.archetype_classifications) if args.archetype_classifications else analysis_dir / "archetype_classifications.csv"
    items_path = Path(args.cross_domain_items) if args.cross_domain_items else analysis_dir / "cross_domain_items.csv"
    definitions_path = Path(args.archetype_definitions) if args.archetype_definitions else repo_root / "config" / "archetype" / "archetype_definitions.json"
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else analysis_dir / "reference_graph.json"
    records_csv_path = Path(args.records_csv) if args.records_csv else records_root / "records" / "records.csv"
    patterns_path = Path(args.cross_domain_patterns) if args.cross_domain_patterns else analysis_dir / "cross_domain_patterns.csv"
    out_dir = Path(args.out_dir) if args.out_dir else analysis_dir

    classification_rows = read_csv_rows(classifications_path)
    log(STAGE, f"loaded {len(classification_rows)} rows from {classifications_path}")

    items_rows = read_csv_rows(items_path)
    log(STAGE, f"loaded {len(items_rows)} rows from {items_path}")
    if not items_rows:
        log(STAGE, "WARNING: cross_domain_items.csv is empty or missing; top5_hash_coverage_pct will be empty for all set-domain signals")

    patterns_rows = read_csv_rows(patterns_path)
    log(STAGE, f"loaded {len(patterns_rows)} rows from {patterns_path}")
    if not patterns_rows:
        log(STAGE, "WARNING: cross_domain_patterns.csv not provided or empty; archetype_validation_pairs.csv covariation_tier will be 'Deferred' for all archetypes")

    definitions_doc = read_json(definitions_path, default={})
    all_definitions = definitions_doc.get("archetypes", definitions_doc.get("candidates", [])) if isinstance(definitions_doc, dict) else []
    if isinstance(definitions_doc, list):
        all_definitions = definitions_doc
    archetypes = [a for a in all_definitions if a.get("promoted") is True]
    log(STAGE, f"loaded {len(all_definitions)} archetype definitions from {definitions_path}; {len(archetypes)} promoted")

    reference_graph = read_json(reference_graph_path, default={})
    edges = reference_graph.get("edges", []) if isinstance(reference_graph, dict) else []
    edges_by_id: Dict[str, Dict[str, Any]] = {e["edge_id"]: e for e in edges if "edge_id" in e}
    alias_of, collapsed = build_edge_aliases(edges_by_id)
    if alias_of:
        log(STAGE, f"collapsed {len(alias_of)} edges onto {len(collapsed)} canonical edges")

    # archetype_id -> signal_id -> (edge_id, join_hash filter or None)
    signal_meta_by_archetype: Dict[str, Dict[str, Tuple[str, Any]]] = {}
    for archetype in archetypes:
        archetype_id = archetype.get("archetype_id", "")
        signal_meta_by_archetype[archetype_id] = {
            s.get("signal_id", s.get("edge_id", "")): (s.get("edge_id", ""), s.get("join_hash"))
            for s in (archetype.get("signals", []) or [])
        }

    records_rows = read_csv_rows(records_csv_path)
    log(STAGE, f"loaded {len(records_rows)} rows from {records_csv_path}")

    # (export_run_id, domain, record_pk) -> sig_hash; record_pk is the most
    # precise identity carried by cross_domain_items.csv and avoids ambiguous
    # join_hash-only lookups for set-like domains.
    sig_hash_by_record_pk: Dict[Tuple[str, str, str], str] = {}
    # (export_run_id, domain, join_hash) -> sig_hash fallback for older
    # cross_domain_items.csv files or rows without source_record_pk.
    sig_hash_by_join_hash: Dict[Tuple[str, str, str], str] = {}
    for r in records_rows:
        export_run_id = r.get("export_run_id", "")
        domain = r.get("domain", "")
        record_pk = r.get("record_pk", "")
        join_hash = r.get("join_hash", "")
        sig_hash = r.get("sig_hash", "")
        if export_run_id and domain and record_pk:
            sig_hash_by_record_pk[(export_run_id, domain, record_pk)] = sig_hash
        if export_run_id and domain and join_hash:
            sig_hash_by_join_hash[(export_run_id, domain, join_hash)] = sig_hash

    # (export_run_id, canonical edge_id) -> [(source_domain, source_record_pk, source_join_hash, target_join_hash), ...]
    items_idx: Dict[Tuple[str, str], List[Tuple[str, str, str, str]]] = defaultdict(list)
    # canonical edge_id -> Counter of source_join_hash -> record count (across all files)
    edge_join_hash_counts: Dict[str, "Counter[str]"] = defaultdict(Counter)
    # canonical edge_id -> set of export_run_id with at least one record
    files_by_canonical_edge: Dict[str, Set[str]] = defaultdict(set)
    for row in items_rows:
        export_run_id = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if not export_run_id or not edge_id:
            continue
        canonical_edge_id = alias_of.get(edge_id, edge_id)
        edge_meta = edges_by_id.get(edge_id) or edges_by_id.get(canonical_edge_id) or {}
        source_domain = row.get("source_domain", "") or edge_meta.get("source_domain", "")
        target_domain = row.get("target_domain", "") or edge_meta.get("target_domain", "")
        # target_domain is resolved here as a diagnostic/compatibility fallback
        # for sparse cross_domain_items rows; current detail output is source-grain.
        _ = target_domain
        items_idx[(export_run_id, canonical_edge_id)].append(
            (source_domain, row.get("source_record_pk", ""), row.get("source_join_hash", ""), row.get("target_join_hash", ""))
        )
        files_by_canonical_edge[canonical_edge_id].add(export_run_id)
        source_join_hash = row.get("source_join_hash", "")
        if source_join_hash:
            edge_join_hash_counts[canonical_edge_id][source_join_hash] += 1

    # (canonical edge_id_a, canonical edge_id_b) [sorted] -> cross_domain_patterns.csv rows
    patterns_by_pair: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in patterns_rows:
        edge_id_a = row.get("edge_id_a", "")
        edge_id_b = row.get("edge_id_b", "")
        if not edge_id_a or not edge_id_b:
            continue
        key = tuple(sorted((edge_id_a, edge_id_b)))
        patterns_by_pair[key].append(row)

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
        signal_meta_map = signal_meta_by_archetype.get(archetype_id, {})

        for signal_id in signals_fired:
            edge_id, join_hash_filter = signal_meta_map.get(signal_id, (signal_id, None))
            canonical_edge_id = alias_of.get(edge_id, edge_id)
            edge_id_by_signal[(archetype_id, signal_id)] = canonical_edge_id

            pairs = items_idx.get((export_run_id, canonical_edge_id), [])
            if join_hash_filter:
                pairs = [
                    p for p in pairs
                    if join_hash_filter == p[2] or join_hash_filter == p[3]
                ]
            distinct_join_hashes = {jh for _, _, jh, _ in pairs if jh}
            n_join_hashes_in_file = len(distinct_join_hashes)

            files_by_signal[(archetype_id, signal_id)].add(export_run_id)
            if n_join_hashes_in_file > 1:
                multi_instance_by_signal[(archetype_id, signal_id)].add(export_run_id)

            seen_join_hashes: Set[str] = set()
            for source_domain, source_record_pk, source_join_hash, _target_join_hash in pairs:
                dedupe_key = source_record_pk or source_join_hash
                if not dedupe_key or dedupe_key in seen_join_hashes:
                    continue
                seen_join_hashes.add(dedupe_key)
                sig_hash = ""
                record_pk_key = (export_run_id, source_domain, source_record_pk)
                if source_record_pk and record_pk_key in sig_hash_by_record_pk:
                    # Empty sig_hash is meaningful for an exact record_pk hit
                    # (e.g. deferred/blocked hash policy). Do not fall back to
                    # join_hash and risk borrowing a sibling record's sig_hash.
                    sig_hash = sig_hash_by_record_pk[record_pk_key]
                elif source_join_hash:
                    sig_hash = sig_hash_by_join_hash.get((export_run_id, source_domain, source_join_hash), "")
                if sig_hash:
                    sig_hashes_by_signal[(archetype_id, signal_id)].add(sig_hash)

                detail_rows.append({
                    "export_run_id": export_run_id,
                    "archetype_id": archetype_id,
                    "signal_id": signal_id,
                    "edge_id": canonical_edge_id,
                    "source_domain": source_domain,
                    "source_record_pk": source_record_pk,
                    "source_join_hash": source_join_hash,
                    "sig_hash": sig_hash,
                    "n_join_hashes_in_file": n_join_hashes_in_file,
                })

    log(STAGE, f"emitted {len(detail_rows)} detail rows")
    log(STAGE, f"detail rows with blank source_domain={sum(1 for r in detail_rows if not r.get('source_domain'))}")
    log(STAGE, f"detail rows with blank source_record_pk={sum(1 for r in detail_rows if not r.get('source_record_pk'))}")
    log(STAGE, f"detail rows with blank sig_hash={sum(1 for r in detail_rows if not r.get('sig_hash'))}")

    validation_rows: List[Dict[str, Any]] = []
    for (archetype_id, signal_id), files in sorted(files_by_signal.items()):
        n_files_classified = len(files)
        n_distinct_sig_hashes = len(sig_hashes_by_signal.get((archetype_id, signal_id), set()))
        coherence_score = (n_distinct_sig_hashes / n_files_classified) if n_files_classified else 0.0
        n_multi_instance_files = len(multi_instance_by_signal.get((archetype_id, signal_id), set()))

        domain_type = "set" if (n_files_classified and (n_multi_instance_files / n_files_classified) > 0.5) else "singleton"

        top5_hash_coverage_pct: Any = ""
        coherence_tier = _coherence_tier(coherence_score)
        if domain_type == "set":
            coherence_tier = "set_domain"
            if items_rows:
                edge_id = edge_id_by_signal.get((archetype_id, signal_id), "")
                hash_counts = edge_join_hash_counts.get(edge_id, Counter())
                total = sum(hash_counts.values())
                if total:
                    top5_total = sum(c for _, c in hash_counts.most_common(_TOP_N_HASHES))
                    top5_hash_coverage_pct = f"{(top5_total / total * 100.0):.2f}"
                else:
                    top5_hash_coverage_pct = "0.00"

        validation_rows.append({
            "archetype_id": archetype_id,
            "signal_id": signal_id,
            "edge_id": edge_id_by_signal.get((archetype_id, signal_id), ""),
            "n_files_classified": n_files_classified,
            "n_distinct_sig_hashes": n_distinct_sig_hashes,
            "coherence_score": f"{coherence_score:.4f}",
            "coherence_tier": coherence_tier,
            "n_multi_instance_files": n_multi_instance_files,
            "domain_type": domain_type,
            "top5_hash_coverage_pct": top5_hash_coverage_pct,
        })

    log(STAGE, f"emitted {len(validation_rows)} validation rows for {len(files_by_signal)} (archetype, signal) pairs")

    pairs_rows: List[Dict[str, Any]] = []
    for archetype in archetypes:
        archetype_id = archetype.get("archetype_id", "")
        signals = archetype.get("signals", []) or []
        if len(signals) < 2:
            log(STAGE, f"archetype={archetype_id}: fewer than 2 signals; skipping archetype_validation_pairs row")
            continue

        sig_a, sig_b = signals[0], signals[1]
        edge_id_a = alias_of.get(sig_a.get("edge_id", ""), sig_a.get("edge_id", ""))
        edge_id_b = alias_of.get(sig_b.get("edge_id", ""), sig_b.get("edge_id", ""))

        files_a = files_by_canonical_edge.get(edge_id_a, set())
        files_b = files_by_canonical_edge.get(edge_id_b, set())
        n_files_both_present = len(files_a & files_b)

        pattern_rows_for_pair = patterns_by_pair.get(tuple(sorted((edge_id_a, edge_id_b))), [])
        top_pair_file_count = max((int(float(r.get("file_count") or 0)) for r in pattern_rows_for_pair), default=0)
        top_pair_containment = (top_pair_file_count / n_files_both_present) if n_files_both_present else 0.0

        notes: List[str] = []
        if not sig_a.get("join_hash") or not sig_b.get("join_hash"):
            notes.append("wildcard_signals")
        if n_files_both_present < 10:
            notes.append("insufficient_data")

        if not pattern_rows_for_pair:
            covariation_tier = "Deferred"
        elif n_files_both_present < 10:
            covariation_tier = "None"
        elif top_pair_containment >= 0.40:
            covariation_tier = "Strong"
        elif top_pair_containment >= 0.20:
            covariation_tier = "Moderate"
        elif top_pair_containment >= 0.05:
            covariation_tier = "Weak"
        else:
            covariation_tier = "None"

        pairs_rows.append({
            "archetype_id": archetype_id,
            "edge_id_a": edge_id_a,
            "edge_id_b": edge_id_b,
            "n_files_both_present": n_files_both_present,
            "top_pair_file_count": top_pair_file_count,
            "top_pair_containment": f"{top_pair_containment:.4f}",
            "covariation_tier": covariation_tier,
            "data_notes": "|".join(notes),
        })

    log(STAGE, f"emitted {len(pairs_rows)} archetype_validation_pairs rows")

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(validation_rows)} validation rows, {len(detail_rows)} detail rows, and {len(pairs_rows)} pairs rows to {out_dir}")
        return 0

    atomic_write_csv(out_dir / "archetype_validation.csv", VALIDATION_FIELDS, validation_rows)
    log(STAGE, f"wrote {len(validation_rows)} rows to {out_dir / 'archetype_validation.csv'}")

    atomic_write_csv(out_dir / "archetype_validation_detail.csv", DETAIL_FIELDS, detail_rows)
    log(STAGE, f"wrote {len(detail_rows)} rows to {out_dir / 'archetype_validation_detail.csv'}")

    atomic_write_csv(out_dir / "archetype_validation_pairs.csv", PAIRS_FIELDS, pairs_rows)
    log(STAGE, f"wrote {len(pairs_rows)} rows to {out_dir / 'archetype_validation_pairs.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
