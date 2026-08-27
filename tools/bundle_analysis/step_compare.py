from __future__ import annotations

import threading
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    from common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id
else:
    from .common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id

# `_GAP_FIELDNAMES` keeps the original one-way reference-coverage fields
# (patterns_required/present/missing, gap_pattern_ids, coverage_pct,
# coverage_status) for backward compatibility, and adds the symmetric
# set-comparison fields (reference/target/shared/*_only, union, jaccard).
# See tools/bundle_analysis/README.md "Reference Bundle Comparison" for the
# field-by-field contract, including zero-denominator behavior.
_GAP_FIELDNAMES = [
    "reference_bundle_id",
    "effective_date",
    "analysis_run_id",
    "domain",
    "population_id",
    "export_run_id",
    "patterns_required",
    "patterns_present",
    "patterns_missing",
    "gap_pattern_ids",
    "coverage_pct",
    "coverage_status",
    "reference_pattern_count",
    "target_pattern_count",
    "shared_count",
    "reference_only_count",
    "target_only_count",
    "union_count",
    "shared_pattern_ids",
    "reference_only_pattern_ids",
    "target_only_pattern_ids",
    "reference_coverage_pct",
    "jaccard",
]
_GAP_REPORT_LOCK = threading.Lock()

# One row per reference x target x domain x pattern_id, classified into
# exactly one of "shared" / "reference_only" / "target_only". Rows are only
# emitted when a reference is actually defined for the domain (see
# NO_REFERENCE_DEFINED handling below) -- there is no comparison class to
# assign a pattern_id to when there is no reference set to compare against.
_DETAIL_FIELDNAMES = [
    "reference_bundle_id",
    "analysis_run_id",
    "domain",
    "population_id",
    "export_run_id",
    "pattern_id",
    "comparison_class",
]


def _compute_comparison_rows(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
    population_id: str = "",
    eligible_export_run_ids: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, "")
    membership_rows = read_csv_rows(out_dir / domain / "membership_matrix.csv")
    seed_export_run_id = str(reference.get("seed_export_run_id", "")).strip()
    if eligible_export_run_ids is None:
        all_export_ids = sorted(
            {
                str(row.get("export_run_id", "")).strip()
                for row in presence_rows
                if row.get("analysis_run_id", "") == run_id
                and row.get("domain", "") == domain
                and str(row.get("export_run_id", "")).strip()
                and str(row.get("export_run_id", "")).strip() != seed_export_run_id
            }
        )
    else:
        all_export_ids = sorted(
            {
                export_run_id.strip()
                for export_run_id in eligible_export_run_ids
                if export_run_id.strip() and export_run_id.strip() != seed_export_run_id
            }
        )
    required_patterns = {
        str(pid).strip()
        for pid in (reference.get("domains", {}) or {}).get(domain, [])
        if str(pid).strip()
    }

    present_by_export: Dict[str, Set[str]] = {}
    for row in membership_rows:
        if row.get("analysis_run_id", "") != run_id:
            continue
        export_run_id = str(row.get("export_run_id", "")).strip()
        pattern_id = str(row.get("pattern_id", "")).strip()
        if not export_run_id or not pattern_id:
            continue
        if seed_export_run_id and export_run_id == seed_export_run_id:
            continue
        present_by_export.setdefault(export_run_id, set()).add(pattern_id)
    for export_run_id in all_export_ids:
        present_by_export.setdefault(export_run_id, set())

    if not required_patterns:
        # NO_REFERENCE_DEFINED: no reference set exists for this domain at
        # all. This is deliberately distinct from a defined reference that
        # happens to contain zero patterns (the sidecar schema forbids an
        # empty domain pattern list -- see reference_bundle.load_and_validate).
        # Without a reference set there is no meaningful shared/reference_only/
        # target_only partition, so those fields stay at their "not computed"
        # values (count fields "0", pattern-id/ratio fields "") and no detail
        # rows are emitted.
        gap_rows = [
            {
                "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                "effective_date": str(reference.get("effective_date", "")),
                "analysis_run_id": run_id,
                "domain": domain,
                "population_id": population_id,
                "export_run_id": export_run_id,
                "patterns_required": "0",
                "patterns_present": "0",
                "patterns_missing": "0",
                "gap_pattern_ids": "",
                "coverage_pct": "",
                "coverage_status": "NO_REFERENCE_DEFINED",
                "reference_pattern_count": "0",
                "target_pattern_count": "0",
                "shared_count": "0",
                "reference_only_count": "0",
                "target_only_count": "0",
                "union_count": "0",
                "shared_pattern_ids": "",
                "reference_only_pattern_ids": "",
                "target_only_pattern_ids": "",
                "reference_coverage_pct": "",
                "jaccard": "",
            }
            for export_run_id in all_export_ids
        ]
        return gap_rows, []

    gap_rows: List[Dict[str, str]] = []
    detail_rows: List[Dict[str, str]] = []
    required_count = len(required_patterns)
    for export_run_id in all_export_ids:
        target_patterns = present_by_export[export_run_id]
        shared = required_patterns & target_patterns
        reference_only = required_patterns - target_patterns
        target_only = target_patterns - required_patterns
        union_patterns = required_patterns | target_patterns

        present_count = len(shared)
        missing = sorted(reference_only)
        missing_count = len(missing)
        coverage = (present_count / required_count) if required_count else 0.0
        if present_count == required_count:
            status = "full"
        elif present_count == 0:
            status = "none"
        else:
            status = "partial"

        union_count = len(union_patterns)
        jaccard = (len(shared) / union_count) if union_count else 0.0

        gap_rows.append(
            {
                "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                "effective_date": str(reference.get("effective_date", "")),
                "analysis_run_id": run_id,
                "domain": domain,
                "population_id": population_id,
                "export_run_id": export_run_id,
                "patterns_required": str(required_count),
                "patterns_present": str(present_count),
                "patterns_missing": str(missing_count),
                "gap_pattern_ids": "|".join(missing),
                "coverage_pct": f"{coverage:.6f}",
                "coverage_status": status,
                "reference_pattern_count": str(required_count),
                "target_pattern_count": str(len(target_patterns)),
                "shared_count": str(len(shared)),
                "reference_only_count": str(len(reference_only)),
                "target_only_count": str(len(target_only)),
                "union_count": str(union_count),
                "shared_pattern_ids": "|".join(sorted(shared)),
                "reference_only_pattern_ids": "|".join(sorted(reference_only)),
                "target_only_pattern_ids": "|".join(sorted(target_only)),
                "reference_coverage_pct": f"{coverage:.6f}",
                "jaccard": f"{jaccard:.6f}",
            }
        )

        for pattern_id in sorted(shared):
            comparison_class = "shared"
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": comparison_class,
                }
            )
        for pattern_id in sorted(reference_only):
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": "reference_only",
                }
            )
        for pattern_id in sorted(target_only):
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": "target_only",
                }
            )
    return gap_rows, detail_rows


def run_compare_for_domain(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
    compare_out_dir: Optional[Path] = None,
    population_id: str = "",
    eligible_export_run_ids: Optional[Set[str]] = None,
    reset_domain_rows: bool = False,
) -> Dict[str, str]:
    compare_dir = compare_out_dir if compare_out_dir is not None else out_dir.parent / "compare"
    compare_dir.mkdir(parents=True, exist_ok=True)
    gap_path = compare_dir / "file_gap_report.csv"
    detail_path = compare_dir / "file_gap_detail.csv"
    domain_rows, domain_detail_rows = _compute_comparison_rows(
        analysis_dir, out_dir, reference, domain, population_id, eligible_export_run_ids
    )
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    current_run_id = resolve_analysis_run_id(presence_rows, "")

    with _GAP_REPORT_LOCK:
        existing = read_csv_rows(gap_path) if gap_path.is_file() else []
        existing_detail = read_csv_rows(detail_path) if detail_path.is_file() else []
        if reset_domain_rows:
            merged = [
                row
                for row in existing
                if not (row.get("analysis_run_id", "") == current_run_id and row.get("domain", "") == domain)
            ] + domain_rows
            merged_detail = [
                row
                for row in existing_detail
                if not (row.get("analysis_run_id", "") == current_run_id and row.get("domain", "") == domain)
            ] + domain_detail_rows
        else:
            merged = [
                row
                for row in existing
                if not (
                    row.get("analysis_run_id", "") == current_run_id
                    and row.get("domain", "") == domain
                    and row.get("population_id", "") == population_id
                )
            ] + domain_rows
            merged_detail = [
                row
                for row in existing_detail
                if not (
                    row.get("analysis_run_id", "") == current_run_id
                    and row.get("domain", "") == domain
                    and row.get("population_id", "") == population_id
                )
            ] + domain_detail_rows
        merged.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("export_run_id", "")))
        merged_detail.sort(
            key=lambda r: (
                r.get("analysis_run_id", ""),
                r.get("domain", ""),
                r.get("population_id", ""),
                r.get("export_run_id", ""),
                r.get("pattern_id", ""),
            )
        )
        atomic_write_csv(gap_path, _GAP_FIELDNAMES, merged)
        atomic_write_csv(detail_path, _DETAIL_FIELDNAMES, merged_detail)

    counts = Counter(row.get("coverage_status", "") for row in domain_rows)
    return {
        "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
        "effective_date": str(reference.get("effective_date", "")),
        "analysis_run_id": (domain_rows[0].get("analysis_run_id", "") if domain_rows else ""),
        "domain": domain,
        "population_id": population_id,
        "files_scored": str(len(domain_rows)),
        "full_count": str(counts.get("full", 0)),
        "partial_count": str(counts.get("partial", 0)),
        "none_count": str(counts.get("none", 0)),
        "no_reference_count": str(counts.get("NO_REFERENCE_DEFINED", 0)),
    }
