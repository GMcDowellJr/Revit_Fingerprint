#!/usr/bin/env python3
"""Assign per-file archetype classifications based on cross-domain signals.

Inputs:
  - Fingerprint_Out/archetype_analysis/cross_domain_items.csv
  - config/archetype/archetype_definitions.json (only promoted == true entries)
  - Fingerprint_Out/archetype_analysis/reference_graph.json
  - file_metadata.csv

Outputs:
  - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
  - Fingerprint_Out/archetype_analysis/archetype_coverage_summary.json

Processing:
  - Build an edge alias map from reference_graph.json (see
    compute_cross_domain_cooccurrence.py / _common.build_edge_aliases):
    edges collapsed during co-occurrence analysis (fill_patterns
    drafting/model partitions, dimension_types_{angular,radial,diameter}
    tick_mark variants) are remapped onto their canonical edge_id. Promoted
    archetype signals reference canonical edge_ids, so cross_domain_items.csv
    rows for collapsed edge_ids are folded into the canonical edge_id when
    building active_edges/fired_files_by_edge.
  - Build a per-file active_edges set (canonical edge_id -> set of
    (source_join_hash, target_join_hash) pairs) from cross_domain_items.csv.
  - Build a corpus-level unavailable_edges set of canonical edge_ids: a
    canonical edge is unavailable iff it and every edge collapsed into it
    have available == false in reference_graph.json.
  - For each (file, promoted archetype), evaluate each signal:
      unavailable -- signal.edge_id in unavailable_edges
      fired       -- edge_id active for this file, and (signal has no
                      join_hash filter -- null or empty string -- or the
                      filter matches the row's source_join_hash or
                      target_join_hash)
      absent      -- otherwise
  - Emit a classification row only if at least one required signal fired.
  - confidence_tier = "Full" if all required signals fired and no signal
    (required or not) is unavailable; otherwise "Partial".
  - All-optional archetypes: if every signal on a promoted archetype has
    required == false (e.g. all signals fell below the candidate generator's
    low-coverage threshold), candidate_files and the "at least one required
    signal fired" gate fall back to the union of all signals instead of
    required signals. confidence_tier is always "Partial" for these rows
    (an archetype with no required signals can never reach "Full").
  - is_mixed = true when a file has more than one archetype row for the
    same governance_question.
  - Null join_hash guard: for each promoted archetype, any signal whose
    join_hash is null/empty is in "wildcard mode" -- it matches any record
    on its edge regardless of join_hash. A WARNING is written to stderr per
    wildcard signal, and every classification row for that archetype gets
    "n_signals_wildcard" (count of wildcard signals) and "scoring_mode":
    "strict" (no wildcard signals), "partial" (some wildcard), or "wildcard"
    (all signals wildcard).

Usage:
    python tools/archetype/assign_archetype_classifications.py \\
        --repo-root . \\
        --cross-domain-items Fingerprint_Out/archetype_analysis/cross_domain_items.csv \\
        --archetype-definitions config/archetype/archetype_definitions.json \\
        --reference-graph Fingerprint_Out/archetype_analysis/reference_graph.json \\
        --file-metadata results/records/file_metadata.csv \\
        --out-dir Fingerprint_Out/archetype_analysis \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from _common import (
    log,
    atomic_write_csv,
    atomic_write_json,
    build_edge_aliases,
    read_csv_rows,
    read_json,
)

STAGE = "assign_archetype_classifications"

CLASSIFICATIONS_FIELDS = [
    "export_run_id",
    "archetype_id",
    "governance_question",
    "approach_label",
    "confidence_tier",
    "is_mixed",
    "signals_fired",
    "signals_absent",
    "signals_null",
    "n_signals_fired",
    "n_signals_null",
    "n_signals_wildcard",
    "scoring_mode",
    "client_label",
    "governance_role",
    "discipline_label",
    "unit_system",
    "signals_fired_join_hashes",
    "signals_fired_labels",
]

SIGNAL_LIST_SEPARATOR = ";"
FiredEdgeRow = Tuple[str, str, str, str, int]


class DomainPatternLabelCache:
    """Lazy `(domain, join_hash) -> human_label` lookup for domain patterns."""

    def __init__(self, domain_patterns_dir: Path) -> None:
        self.domain_patterns_dir = domain_patterns_dir
        self._label_cache: Dict[Tuple[str, str], str] = {}
        self._loaded_domains: Set[str] = set()

    def get(self, domain: str, join_hash: str) -> str:
        if not domain or not join_hash:
            return ""
        if domain not in self._loaded_domains:
            self._load_domain(domain)
        return self._label_cache.get((domain, join_hash), "")

    def _load_domain(self, domain: str) -> None:
        self._loaded_domains.add(domain)
        candidates = [
            self.domain_patterns_dir / f"{domain}_patterns.csv",
            self.domain_patterns_dir / "domain_patterns.csv",
        ]
        for path in candidates:
            if not path.is_file():
                continue
            for row in read_csv_rows(path):
                row_domain = row.get("domain", "").strip()
                if row_domain and row_domain != domain:
                    continue
                join_hash = row.get("join_hash", "").strip()
                if not join_hash:
                    source_cluster_id = row.get("source_cluster_id", "").strip()
                    if source_cluster_id:
                        join_hash = source_cluster_id.split("|")[-1]
                if not join_hash:
                    continue
                label = (
                    row.get("pattern_label_human", "")
                    or row.get("human_label", "")
                    or row.get("pattern_label", "")
                )
                self._label_cache[(domain, join_hash)] = label
            return


def _evaluate_signal(
    signal: Dict[str, Any],
    file_edges: Dict[str, List[FiredEdgeRow]],
    unavailable_edges: Set[str],
) -> str:
    edge_id = signal.get("edge_id", "")
    if edge_id in unavailable_edges:
        return "unavailable"

    rows = file_edges.get(edge_id)
    if not rows:
        return "absent"

    join_hash_filter = signal.get("join_hash")
    if not join_hash_filter:
        return "fired"

    for _source_domain, _target_domain, source_jh, target_jh, _support_count in rows:
        if join_hash_filter == source_jh or join_hash_filter == target_jh:
            return "fired"
    return "absent"


def _signal_fired_source(
    signal: Dict[str, Any],
    file_edges: Dict[str, List[FiredEdgeRow]],
) -> Tuple[str, str]:
    """Return the best `(source_join_hash, source_domain)` for a fired signal.

    The signal must already have evaluated to "fired". If several rows fired,
    choose the source_join_hash with the highest corpus support count computed
    from cross_domain_items.csv, with stable input order breaking ties.
    """
    edge_id = signal.get("edge_id", "")
    rows = file_edges.get(edge_id) or []
    join_hash_filter = signal.get("join_hash")
    best_source_join_hash = ""
    best_source_domain = ""
    best_support_count = -1
    for source_domain, _target_domain, source_jh, target_jh, support_count in rows:
        if join_hash_filter and join_hash_filter not in {source_jh, target_jh}:
            continue
        if support_count > best_support_count:
            best_source_join_hash = source_jh
            best_source_domain = source_domain
            best_support_count = support_count
    return best_source_join_hash, best_source_domain


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root (used for default paths)")
    ap.add_argument("--cross-domain-items", default=None, help="Path to cross_domain_items.csv")
    ap.add_argument("--archetype-definitions", default=None, help="Path to archetype_definitions.json")
    ap.add_argument("--reference-graph", default=None, help="Path to reference_graph.json")
    ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv")
    ap.add_argument("--domain-patterns-dir", default=None, help="Directory containing domain_patterns.csv or {domain}_patterns.csv")
    ap.add_argument("--out-dir", default=None, help="Output directory")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    items_path = Path(args.cross_domain_items) if args.cross_domain_items else repo_root / "Fingerprint_Out" / "archetype_analysis" / "cross_domain_items.csv"
    definitions_path = Path(args.archetype_definitions) if args.archetype_definitions else repo_root / "config" / "archetype" / "archetype_definitions.json"
    reference_graph_path = Path(args.reference_graph) if args.reference_graph else repo_root / "Fingerprint_Out" / "archetype_analysis" / "reference_graph.json"
    file_metadata_path = Path(args.file_metadata) if args.file_metadata else repo_root / "results" / "records" / "file_metadata.csv"
    domain_patterns_dir = Path(args.domain_patterns_dir) if args.domain_patterns_dir else repo_root / "results" / "analysis"
    out_dir = Path(args.out_dir) if args.out_dir else repo_root / "Fingerprint_Out" / "archetype_analysis"
    label_cache = DomainPatternLabelCache(domain_patterns_dir)

    items_rows = read_csv_rows(items_path)
    log(STAGE, f"loaded {len(items_rows)} rows from {items_path}")

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

    # A canonical edge is unavailable iff it and every edge collapsed into it are unavailable.
    canonical_edge_ids = sorted({alias_of.get(e, e) for e in edges_by_id})
    unavailable_edges: Set[str] = set()
    for canonical in canonical_edge_ids:
        members = [canonical] + collapsed.get(canonical, [])
        if not any(bool(edges_by_id[m].get("available")) for m in members):
            unavailable_edges.add(canonical)
    log(STAGE, f"unavailable_edges={len(unavailable_edges)}")

    file_metadata_rows = read_csv_rows(file_metadata_path)
    log(STAGE, f"loaded {len(file_metadata_rows)} rows from {file_metadata_path}")
    file_meta_idx: Dict[str, Dict[str, str]] = {}
    for r in file_metadata_rows:
        eid = r.get("export_run_id", "")
        if eid:
            file_meta_idx[eid] = {
                "client_label": r.get("client_label", ""),
                "governance_role": r.get("governance_role", ""),
                "discipline_label": r.get("discipline_label", ""),
                "unit_system": r.get("unit_system", ""),
            }

    # (canonical_edge_id, source_domain, source_join_hash) -> export_run_ids.
    # cross_domain_items.csv does not carry a file_count column, so compute the
    # support count directly from the rows this script consumes.
    source_support_files: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
    for row in items_rows:
        eid = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        source_join_hash = row.get("source_join_hash", "")
        if not eid or not edge_id or not source_join_hash:
            continue
        canonical_edge_id = alias_of.get(edge_id, edge_id)
        edge = edges_by_id.get(edge_id, {})
        source_domain = row.get("source_domain", "") or edge.get("source_domain", "")
        source_support_files[(canonical_edge_id, source_domain, source_join_hash)].add(eid)

    # export_run_id -> edge_id -> [
    #   (source_domain, target_domain, source_join_hash, target_join_hash, source_support_count), ...
    # ]
    file_edges: Dict[str, Dict[str, List[FiredEdgeRow]]] = defaultdict(lambda: defaultdict(list))
    # edge_id -> set of export_run_id where it fired
    fired_files_by_edge: Dict[str, Set[str]] = defaultdict(set)
    file_universe: Set[str] = set()
    for row in items_rows:
        eid = row.get("export_run_id", "")
        edge_id = row.get("edge_id", "")
        if not eid or not edge_id:
            continue
        canonical_edge_id = alias_of.get(edge_id, edge_id)
        edge = edges_by_id.get(edge_id, {})
        source_domain = row.get("source_domain", "") or edge.get("source_domain", "")
        target_domain = row.get("target_domain", "") or edge.get("target_domain", "")
        source_join_hash = row.get("source_join_hash", "")
        file_universe.add(eid)
        file_edges[eid][canonical_edge_id].append((
            source_domain,
            target_domain,
            source_join_hash,
            row.get("target_join_hash", ""),
            len(source_support_files.get((canonical_edge_id, source_domain, source_join_hash), set())),
        ))
        fired_files_by_edge[canonical_edge_id].add(eid)
    file_universe |= set(file_meta_idx.keys())
    n_files_total = len(file_universe)
    log(STAGE, f"file_universe size={n_files_total}")

    classification_rows: List[Dict[str, Any]] = []
    coverage: Dict[str, Dict[str, Any]] = {}

    for archetype in archetypes:
        archetype_id = archetype.get("archetype_id", "")
        governance_question = archetype.get("governance_question", "")
        approach_label = archetype.get("approach_label", "")
        signals = archetype.get("signals", []) or []
        required_signals = [s for s in signals if s.get("required", True)]

        signal_unavailable = {s.get("signal_id", s.get("edge_id", "")): (s.get("edge_id", "") in unavailable_edges) for s in signals}
        unavailable_signal_ids = sorted(sid for sid, unavail in signal_unavailable.items() if unavail)

        # FIX5: surface signals with no join_hash filter ("wildcard mode") --
        # they still match, but should be visible to human reviewers.
        wildcard_signal_ids = [s.get("signal_id", s.get("edge_id", "")) for s in signals if not s.get("join_hash")]
        for signal_id in wildcard_signal_ids:
            sys.stderr.write(
                f"WARNING [score] archetype={archetype_id} signal={signal_id}\n"
                "  join_hash is null — signal will match any record on this edge\n"
                "  (wildcard mode). Populate join_hash to enable approach discrimination.\n"
            )
        n_signals_wildcard = len(wildcard_signal_ids)
        if n_signals_wildcard == 0:
            scoring_mode = "strict"
        elif n_signals_wildcard == len(signals):
            scoring_mode = "wildcard"
        else:
            scoring_mode = "partial"

        if required_signals and all(s.get("edge_id", "") in unavailable_edges for s in required_signals):
            coverage[archetype_id] = {
                "n_files_full": 0,
                "n_files_partial": 0,
                "n_files_no_evidence": n_files_total,
                "unavailable_signal_ids": unavailable_signal_ids,
                "note": "all_required_signals_unavailable",
            }
            log(STAGE, f"archetype={archetype_id}: all required signals unavailable; emitting summary note only")
            continue

        # Candidate files: union of files where any required signal's edge fired.
        # If no signals are required (e.g. all signals fell below the
        # low-coverage threshold), fall back to the union over all signals so
        # all-optional promoted archetypes still get classification rows.
        gating_signals = required_signals if required_signals else signals
        candidate_files: Set[str] = set()
        for s in gating_signals:
            candidate_files |= fired_files_by_edge.get(s.get("edge_id", ""), set())

        n_full = 0
        n_partial = 0
        for export_run_id in sorted(candidate_files):
            edges_for_file = file_edges.get(export_run_id, {})
            statuses: Dict[str, str] = {}
            signals_by_id: Dict[str, Dict[str, Any]] = {}
            for s in signals:
                signal_id = s.get("signal_id", s.get("edge_id", ""))
                signals_by_id[signal_id] = s
                statuses[signal_id] = _evaluate_signal(s, edges_for_file, unavailable_edges)

            required_ids = [s.get("signal_id", s.get("edge_id", "")) for s in required_signals]
            if required_ids:
                any_required_fired = any(statuses[sid] == "fired" for sid in required_ids)
                all_required_fired = all(statuses[sid] == "fired" for sid in required_ids)
            else:
                # All-optional archetype: a row qualifies if any signal fired,
                # but with no required signals it can never reach "Full".
                all_signal_ids = [s.get("signal_id", s.get("edge_id", "")) for s in signals]
                any_required_fired = any(statuses[sid] == "fired" for sid in all_signal_ids)
                all_required_fired = False
            if not any_required_fired:
                continue

            any_unavailable = any(v == "unavailable" for v in statuses.values())
            confidence_tier = "Full" if (all_required_fired and not any_unavailable) else "Partial"
            if confidence_tier == "Full":
                n_full += 1
            else:
                n_partial += 1

            signals_fired = sorted(sid for sid, st in statuses.items() if st == "fired")
            signals_absent = sorted(sid for sid, st in statuses.items() if st == "absent")
            signals_null = sorted(sid for sid, st in statuses.items() if st == "unavailable")
            signals_fired_sources: Dict[str, Tuple[str, str]] = {
                sid: _signal_fired_source(signals_by_id[sid], edges_for_file)
                for sid in signals_fired
            }
            signals_fired_labels: Dict[str, str] = {}
            for sid in signals_fired:
                source_join_hash, source_domain = signals_fired_sources[sid]
                signals_fired_labels[sid] = label_cache.get(source_domain, source_join_hash)

            meta = file_meta_idx.get(export_run_id, {})
            classification_rows.append({
                "export_run_id": export_run_id,
                "archetype_id": archetype_id,
                "governance_question": governance_question,
                "approach_label": approach_label,
                "confidence_tier": confidence_tier,
                "is_mixed": "false",
                "signals_fired": SIGNAL_LIST_SEPARATOR.join(signals_fired),
                "signals_absent": SIGNAL_LIST_SEPARATOR.join(signals_absent),
                "signals_null": SIGNAL_LIST_SEPARATOR.join(signals_null),
                "n_signals_fired": len(signals_fired),
                "n_signals_null": len(signals_null),
                "n_signals_wildcard": n_signals_wildcard,
                "scoring_mode": scoring_mode,
                "client_label": meta.get("client_label", ""),
                "governance_role": meta.get("governance_role", ""),
                "discipline_label": meta.get("discipline_label", ""),
                "unit_system": meta.get("unit_system", ""),
                "signals_fired_join_hashes": SIGNAL_LIST_SEPARATOR.join(signals_fired_sources[sid][0] for sid in signals_fired),
                "signals_fired_labels": SIGNAL_LIST_SEPARATOR.join(signals_fired_labels[sid] for sid in signals_fired),
            })

        coverage[archetype_id] = {
            "n_files_full": n_full,
            "n_files_partial": n_partial,
            "n_files_no_evidence": n_files_total - n_full - n_partial,
            "unavailable_signal_ids": unavailable_signal_ids,
        }
        log(STAGE, f"archetype={archetype_id}: full={n_full} partial={n_partial} no_evidence={n_files_total - n_full - n_partial}")

    # is_mixed: file has >1 archetype row for the same governance_question
    by_file_question: Dict[Tuple[str, str], int] = defaultdict(int)
    for row in classification_rows:
        by_file_question[(row["export_run_id"], row["governance_question"])] += 1
    for row in classification_rows:
        if by_file_question[(row["export_run_id"], row["governance_question"])] > 1:
            row["is_mixed"] = "true"

    log(STAGE, f"emitted {len(classification_rows)} classification rows for {len(archetypes)} promoted archetypes")

    if args.dry_run:
        log(STAGE, f"dry-run: would write {len(classification_rows)} rows and coverage for {len(coverage)} archetypes to {out_dir}")
        return 0

    atomic_write_csv(out_dir / "archetype_classifications.csv", CLASSIFICATIONS_FIELDS, classification_rows)
    log(STAGE, f"wrote {len(classification_rows)} rows to {out_dir / 'archetype_classifications.csv'}")

    atomic_write_json(out_dir / "archetype_coverage_summary.json", {"archetypes": coverage})
    log(STAGE, f"wrote coverage summary for {len(coverage)} archetypes to {out_dir / 'archetype_coverage_summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
