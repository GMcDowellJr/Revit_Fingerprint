#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tools/diagnose_name_config_collisions.py -- read-only, standalone magnitude check for
Same-Name-Different-Config Collision Detection.

Answers, for one or more already-materialized segments: "within this corpus, how often does
the same name (by the Canonical Name Identity Projection, join_key_name_identity) resolve to
more than one distinct behavioral config (join_hash)?" -- the hazard where a naive person
searching or reading by name alone would wrongly assume two things are the same governance
object.

This is a SINGLE-SIDE (within-segment) scan, not a reference-vs-target comparison: it loads
one segment's own results/records/records.csv + results/analysis/domain_patterns.csv +
results/name_key/name_key_results.csv exactly as
tools/name_key_rollup.py::build_domain_name_hash_facets() already does (reused directly, not
reimplemented -- see tools/name_config_collision.py's module docstring), then inverts that
forward index in-memory (tools/name_config_collision.py::invert_domain_name_hash_facets())
to ask the inverse question.

Read-only: never writes to disk, never touches any existing output file. No wiring into
tools/compare_reference.py or tools/run_segment_orchestrator.py -- this is purely a tool for
a human (Greg) to point at real corpus data and judge, by eye, whether this is a rare
curiosity or a common governance hazard, before any follow-on PR decides on an output shape.

Each positional argument may be either a segment root itself (a directory containing
results/records/records.csv directly) OR a container directory holding many segment roots
(e.g. Fingerprint_Data/segments, one subfolder per segment_id) -- a container is
auto-detected (no results/records/records.csv of its own) and recursively searched for every
results/records/records.csv beneath it, one segment per match. This mirrors how
tools/run_segment_orchestrator.py/build_segment_manifest.py lay out segments on disk, so
Greg can point this at the whole segments/ folder in one call instead of enumerating each
segment_id by hand.

Usage:
    python tools/diagnose_name_config_collisions.py <segment_root_or_container> [...]
    python tools/diagnose_name_config_collisions.py <segments_dir> --detail
    python tools/diagnose_name_config_collisions.py <segments_dir> --detail --detail-limit 100
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from core.name_key_coverage import ELIGIBLE_DOMAINS  # noqa: E402
from name_config_collision import (  # noqa: E402
    NAME_KEY_STATUS_OK,
    find_within_side_name_ambiguities,
    load_side_collision_facets,
    name_key_side_status,
)

_DEFAULT_DETAIL_LIMIT = 50


def _is_segment_root(path: Path) -> bool:
    return (path / "results" / "records" / "records.csv").is_file()


def _resolve_segment_roots(path: Path) -> List[Path]:
    """Resolve one CLI argument to a list of segment roots.

    If `path` itself looks like a segment root (has results/records/records.csv directly),
    it's used as-is -- no directory walk. Otherwise `path` is treated as a container (e.g.
    the parent `segments/` folder holding one subdirectory per segment_id) and searched
    recursively for every results/records/records.csv beneath it; each match's segment root
    is `.../records.csv`'s grandparent (records.csv -> results/records -> results ->
    segment_root). Recursive rather than one-level-only so nested segment layouts (segment
    hierarchies with parent/child segments, per build_segment_manifest.py) are found too.
    """
    if _is_segment_root(path):
        return [path]
    found = sorted({
        records_csv.parent.parent.parent
        for records_csv in path.rglob("records.csv")
        if records_csv.parent.name == "records" and records_csv.parent.parent.name == "results"
    })
    return found


def _summarize_segment(segment_root: Path) -> List[Dict[str, object]]:
    """Per-domain summary rows for one segment: distinct name count, count of names mapping
    to >1 config, percentage, and the largest configs-per-name value observed."""
    status, _records_csv, _domain_patterns_csv, _name_key_csv = name_key_side_status(segment_root)
    if status != NAME_KEY_STATUS_OK:
        print(f"[diagnose_name_config_collisions] {segment_root}: name-key status={status} -- skipping (no evidence to scan)")
        return []

    _status, facets = load_side_collision_facets(segment_root, ELIGIBLE_DOMAINS)
    ambiguity_rows = find_within_side_name_ambiguities(facets, ELIGIBLE_DOMAINS)

    by_domain: Dict[str, List[Dict[str, object]]] = {}
    for row in ambiguity_rows:
        by_domain.setdefault(row["domain"], []).append(row)

    summary_rows: List[Dict[str, object]] = []
    for domain in sorted(by_domain):
        rows = by_domain[domain]
        total_names = len(rows)
        collided = [r for r in rows if r["is_ambiguous"]]
        max_configs = max((r["distinct_config_count"] for r in rows), default=0)
        pct = (100.0 * len(collided) / total_names) if total_names else 0.0
        summary_rows.append({
            "segment": str(segment_root),
            "domain": domain,
            "distinct_name_count": total_names,
            "names_with_gt1_config": len(collided),
            "collision_pct": pct,
            "max_configs_per_name": max_configs,
            "_collided_rows": collided,
        })
    return summary_rows


def _print_summary_table(all_summary_rows: List[Dict[str, object]]) -> None:
    header = f"{'segment':<40} {'domain':<45} {'names':>8} {'collided':>10} {'pct':>7} {'max_cfg':>8}"
    print(header)
    print("-" * len(header))
    for row in all_summary_rows:
        seg_display = row["segment"]
        if len(seg_display) > 40:
            seg_display = "..." + seg_display[-37:]
        print(
            f"{seg_display:<40} {row['domain']:<45} {row['distinct_name_count']:>8} "
            f"{row['names_with_gt1_config']:>10} {row['collision_pct']:>6.2f}% {row['max_configs_per_name']:>8}"
        )


def _print_detail(all_summary_rows: List[Dict[str, object]], detail_limit: int) -> None:
    printed = 0
    for seg_row in all_summary_rows:
        for row in seg_row["_collided_rows"]:
            if printed >= detail_limit:
                print(f"... detail output capped at {detail_limit} rows (--detail-limit to change); "
                      f"more collisions exist than shown here.")
                return
            print(
                f"  [{seg_row['segment']}] domain={row['domain']} name_hash={row['name_hash']} "
                f"label={row['representative_label']!r} distinct_configs={row['distinct_config_count']} "
                f"configs={row['config_hashes']} records={row['record_count']}"
            )
            printed += 1
    if printed == 0:
        print("  (no colliding names found in any scanned segment)")


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("segment_root", nargs="+", type=Path, help="One or more segment root directories (each containing results/records, results/analysis, results/name_key).")
    ap.add_argument("--detail", action="store_true", help="Also print the actual colliding label/config pairs for manual spot-check.")
    ap.add_argument("--detail-limit", type=int, default=_DEFAULT_DETAIL_LIMIT, help=f"Max detail rows to print (default {_DEFAULT_DETAIL_LIMIT}).")
    return ap


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)

    segment_roots: List[Path] = []
    for arg_path in args.segment_root:
        if not arg_path.is_dir():
            print(f"[diagnose_name_config_collisions][error] not a directory: {arg_path}", file=sys.stderr)
            continue
        resolved = _resolve_segment_roots(arg_path)
        if not resolved:
            print(f"[diagnose_name_config_collisions][warn] {arg_path}: no results/records/records.csv found "
                  f"here or in any subdirectory -- not a segment root and not a container of any", file=sys.stderr)
            continue
        if resolved != [arg_path]:
            print(f"[diagnose_name_config_collisions] {arg_path}: container directory -- found {len(resolved)} "
                  f"segment(s) beneath it")
        segment_roots.extend(resolved)

    all_summary_rows: List[Dict[str, object]] = []
    for segment_root in segment_roots:
        all_summary_rows.extend(_summarize_segment(segment_root))

    if not all_summary_rows:
        print("[diagnose_name_config_collisions] no eligible-domain name evidence found in any scanned segment.")
        return 0

    _print_summary_table(all_summary_rows)

    total_names = sum(r["distinct_name_count"] for r in all_summary_rows)
    total_collided = sum(r["names_with_gt1_config"] for r in all_summary_rows)
    overall_pct = (100.0 * total_collided / total_names) if total_names else 0.0
    print()
    print(f"[diagnose_name_config_collisions] TOTAL across {len(segment_roots)} segment(s): "
          f"{total_names} distinct names scanned, {total_collided} ({overall_pct:.2f}%) map to >1 config.")

    if args.detail:
        print()
        print("Detail (colliding name -> config pairs):")
        _print_detail(all_summary_rows, args.detail_limit)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
