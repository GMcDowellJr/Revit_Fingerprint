#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tools/generate_pattern_name_fragmentation.py -- Step 1 Part A.

For one already-materialized segment, computes, per config `pattern_id` in every eligible
domain (core/name_key_coverage.py::ELIGIBLE_DOMAINS), the set of distinct name-key
`join_hash` values observed among that pattern's member records, each with a
human-readable representative label. A config-identical pattern hiding under many distinct
display names (real, observed on real corpus data: up to 78 distinct names for one
text_types pattern, 8 for one arrowheads pattern) is itself a fragmentation signal, entirely
independent of any cross-segment comparison -- see
docs/namekey_crosssegment_step0_findings.md and tools/compare_reference.py's
--include-name-overlap flag (Part B, the cross-segment-capable counterpart).

Design decisions (see the Step 1 PR description for the full rationale):
  - A1: one row per (pattern_id, name_hash) pair in pattern_name_fragmentation.csv -- never
    one row per pattern with a concatenated/pipe-joined label list, which would marginalize
    away exactly the co-occurrence data this metric exists to surface (the same complaint
    already on file against tools/export_bundle_pattern_detail.py's pattern_settings.csv).
  - A2: a domain outside ELIGIBLE_DOMAINS still gets one row per its own pattern_id, with
    status=excluded_no_name_evidence -- never silently absent. Likewise, an eligible
    domain's pattern with zero resolved name evidence gets one row with
    status=no_name_evidence_resolved, rather than being omitted.
  - A4: pattern_name_fragmentation_summary.csv adds one row per domain (both eligible and
    excluded) with total_patterns/patterns_with_multiplicity/multiplicity_pct/
    max_distinct_names_observed, so a reader can compare domains without recomputing from
    the detail file.

Reads only this segment's own already-materialized CSVs (results/records/records.csv,
results/analysis/domain_patterns.csv, results/name_key/name_key_results.csv -- the last
already filtered to this segment by tools/run_segment_orchestrator.py's Step 2b /
_filter_name_key_csv_to_segment()). Never re-reads export JSON.

Usage:
    python tools/generate_pattern_name_fragmentation.py \\
        --records-csv <segment>/results/records/records.csv \\
        --domain-patterns-csv <segment>/results/analysis/domain_patterns.csv \\
        --name-key-csv <segment>/results/name_key/name_key_results.csv \\
        --out-dir <segment>/results/analysis
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from bundle_analysis.common import atomic_write_csv, read_csv_rows  # noqa: E402
from core.name_key_coverage import ELIGIBLE_DOMAINS, coverage_class, exclusion_reason  # noqa: E402
from name_key_rollup import build_domain_name_hash_facets, parse_source_cluster_id, representative_label  # noqa: E402

DETAIL_FILENAME = "pattern_name_fragmentation.csv"
SUMMARY_FILENAME = "pattern_name_fragmentation_summary.csv"

_DETAIL_FIELDNAMES = [
    "domain",
    "coverage_class",
    "pattern_id",
    "status",
    "distinct_name_count",
    "name_hash",
    "representative_label",
    "record_count_for_this_name",
]

_SUMMARY_FIELDNAMES = [
    "domain",
    "coverage_class",
    "status",
    "total_patterns",
    "patterns_with_multiplicity",
    "multiplicity_pct",
    "patterns_with_no_name_evidence",
    "max_distinct_names_observed",
]

STATUS_OK = "ok"
STATUS_EXCLUDED = "excluded_no_name_evidence"
STATUS_NO_EVIDENCE = "no_name_evidence_resolved"


def build_join_hash_to_pattern_id(domain_patterns_rows, domain: str) -> Dict[str, str]:
    """{config join_hash: pattern_id} for one domain, parsed from this segment's own
    domain_patterns.csv `source_cluster_id` column (domain|join_key_schema|join_hash --
    tools/extractor.py:723). Mirrors tools/compare_reference.py's
    load_domain_pattern_join_hash_map(), reimplemented here (not imported) to avoid this
    same-segment script depending on compare_reference.py's cross-segment-oriented module.
    """
    mapping: Dict[str, str] = {}
    for row in domain_patterns_rows:
        if (row.get("domain", "") or "").strip() != domain:
            continue
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not pattern_id:
            continue
        parsed = parse_source_cluster_id(row.get("source_cluster_id", ""))
        if parsed is None:
            continue
        _domain, _schema, join_hash = parsed
        mapping[join_hash] = pattern_id
    return mapping


def build_fragmentation_rows(records_rows, domain_patterns_rows, name_key_rows):
    """Return (detail_rows, summary_rows) for one segment, covering every domain present
    in this segment's own domain_patterns.csv -- eligible or excluded alike."""
    facets = build_domain_name_hash_facets(records_rows, domain_patterns_rows, name_key_rows)

    domains_present: Set[str] = set(facets.domains_observed)
    detail_rows: List[Dict[str, object]] = []
    summary_rows: List[Dict[str, object]] = []

    for domain in sorted(domains_present):
        is_eligible = domain in ELIGIBLE_DOMAINS
        cclass = coverage_class(domain)
        pattern_join_hashes = sorted(facets.all_pattern_join_hashes.get(domain, set()))

        if not is_eligible:
            reason = exclusion_reason(domain)
            jh_to_pid = build_join_hash_to_pattern_id(domain_patterns_rows, domain)
            for join_hash in pattern_join_hashes:
                pattern_id = jh_to_pid.get(join_hash, "")
                detail_rows.append({
                    "domain": domain,
                    "coverage_class": cclass,
                    "pattern_id": pattern_id,
                    "status": STATUS_EXCLUDED,
                    "distinct_name_count": 0,
                    "name_hash": "",
                    "representative_label": "",
                    "record_count_for_this_name": 0,
                })
            summary_rows.append({
                "domain": domain,
                "coverage_class": cclass,
                "status": STATUS_EXCLUDED,
                "total_patterns": len(pattern_join_hashes),
                "patterns_with_multiplicity": 0,
                "multiplicity_pct": "0.00",
                "patterns_with_no_name_evidence": len(pattern_join_hashes),
                "max_distinct_names_observed": 0,
            })
            continue

        jh_to_pid = build_join_hash_to_pattern_id(domain_patterns_rows, domain)
        total_patterns = len(pattern_join_hashes)
        patterns_with_multiplicity = 0
        patterns_with_no_evidence = 0
        max_distinct = 0

        for join_hash in pattern_join_hashes:
            pattern_id = jh_to_pid.get(join_hash, "")
            name_hashes = facets.name_hashes_for(domain, join_hash)
            distinct_count = len(name_hashes)
            max_distinct = max(max_distinct, distinct_count)
            if distinct_count > 1:
                patterns_with_multiplicity += 1

            if distinct_count == 0:
                patterns_with_no_evidence += 1
                detail_rows.append({
                    "domain": domain,
                    "coverage_class": cclass,
                    "pattern_id": pattern_id,
                    "status": STATUS_NO_EVIDENCE,
                    "distinct_name_count": 0,
                    "name_hash": "",
                    "representative_label": "",
                    "record_count_for_this_name": 0,
                })
                continue

            for name_hash, entry in sorted(name_hashes.items()):
                detail_rows.append({
                    "domain": domain,
                    "coverage_class": cclass,
                    "pattern_id": pattern_id,
                    "status": STATUS_OK,
                    "distinct_name_count": distinct_count,
                    "name_hash": name_hash,
                    "representative_label": representative_label(entry["label_counts"]),
                    "record_count_for_this_name": entry["record_count"],
                })

        multiplicity_pct = (100.0 * patterns_with_multiplicity / total_patterns) if total_patterns else 0.0
        summary_rows.append({
            "domain": domain,
            "coverage_class": cclass,
            "status": STATUS_OK,
            "total_patterns": total_patterns,
            "patterns_with_multiplicity": patterns_with_multiplicity,
            "multiplicity_pct": f"{multiplicity_pct:.2f}",
            "patterns_with_no_name_evidence": patterns_with_no_evidence,
            "max_distinct_names_observed": max_distinct,
        })

    detail_rows.sort(key=lambda r: (r["domain"], r["pattern_id"], r["name_hash"]))
    summary_rows.sort(key=lambda r: r["domain"])
    return detail_rows, summary_rows


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--records-csv", required=True, type=Path, help="Segment's results/records/records.csv.")
    ap.add_argument("--domain-patterns-csv", required=True, type=Path, help="Segment's results/analysis/domain_patterns.csv (config patterns).")
    ap.add_argument("--name-key-csv", required=True, type=Path, help="Segment's own (already-filtered) results/name_key/name_key_results.csv.")
    ap.add_argument("--out-dir", required=True, type=Path, help="Directory to write pattern_name_fragmentation.csv/_summary.csv into (typically results/analysis).")
    return ap


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)

    for label, path in (
        ("--records-csv", args.records_csv),
        ("--domain-patterns-csv", args.domain_patterns_csv),
        ("--name-key-csv", args.name_key_csv),
    ):
        if not path.is_file():
            sys.stderr.write(f"[generate_pattern_name_fragmentation][error] {label} not found: {path}\n")
            return 2

    records_rows = read_csv_rows(args.records_csv)
    domain_patterns_rows = read_csv_rows(args.domain_patterns_csv)
    name_key_rows = read_csv_rows(args.name_key_csv)

    detail_rows, summary_rows = build_fragmentation_rows(records_rows, domain_patterns_rows, name_key_rows)

    out_dir = args.out_dir
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, detail_rows)
    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, summary_rows)

    print(f"[generate_pattern_name_fragmentation] domains={len(summary_rows)} detail_rows={len(detail_rows)}")
    print(f"[generate_pattern_name_fragmentation] wrote {out_dir / DETAIL_FILENAME}")
    print(f"[generate_pattern_name_fragmentation] wrote {out_dir / SUMMARY_FILENAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
