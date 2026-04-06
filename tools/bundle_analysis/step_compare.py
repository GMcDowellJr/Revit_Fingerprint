from __future__ import annotations

import threading
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set

if __package__ in (None, ""):
    from common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id
else:
    from .common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id

_GAP_FIELDNAMES = [
    "reference_bundle_id",
    "effective_date",
    "analysis_run_id",
    "domain",
    "export_run_id",
    "patterns_required",
    "patterns_present",
    "patterns_missing",
    "gap_pattern_ids",
    "coverage_pct",
    "coverage_status",
]
_GAP_REPORT_LOCK = threading.Lock()


def _compute_gap_rows(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
) -> List[Dict[str, str]]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, "")
    membership_rows = read_csv_rows(out_dir / domain / "membership_matrix.csv")
    seed_export_run_id = str(reference.get("seed_export_run_id", "")).strip()
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
        return [
            {
                "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                "effective_date": str(reference.get("effective_date", "")),
                "analysis_run_id": run_id,
                "domain": domain,
                "export_run_id": export_run_id,
                "patterns_required": "0",
                "patterns_present": "0",
                "patterns_missing": "0",
                "gap_pattern_ids": "",
                "coverage_pct": "",
                "coverage_status": "NO_REFERENCE_DEFINED",
            }
            for export_run_id in all_export_ids
        ]

    rows: List[Dict[str, str]] = []
    required_count = len(required_patterns)
    for export_run_id in all_export_ids:
        present = present_by_export[export_run_id]
        present_count = len(present & required_patterns)
        missing = sorted(required_patterns - present)
        missing_count = len(missing)
        coverage = (present_count / required_count) if required_count else 0.0
        if present_count == required_count:
            status = "full"
        elif present_count == 0:
            status = "none"
        else:
            status = "partial"
        rows.append(
            {
                "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                "effective_date": str(reference.get("effective_date", "")),
                "analysis_run_id": run_id,
                "domain": domain,
                "export_run_id": export_run_id,
                "patterns_required": str(required_count),
                "patterns_present": str(present_count),
                "patterns_missing": str(missing_count),
                "gap_pattern_ids": "|".join(missing),
                "coverage_pct": f"{coverage:.6f}",
                "coverage_status": status,
            }
        )
    return rows


def run_compare_for_domain(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
    compare_out_dir: Optional[Path] = None,
) -> Dict[str, str]:
    compare_dir = compare_out_dir if compare_out_dir is not None else out_dir.parent / "compare"
    compare_dir.mkdir(parents=True, exist_ok=True)
    gap_path = compare_dir / "file_gap_report.csv"
    domain_rows = _compute_gap_rows(analysis_dir, out_dir, reference, domain)

    with _GAP_REPORT_LOCK:
        existing = read_csv_rows(gap_path) if gap_path.is_file() else []
        merged = [row for row in existing if row.get("domain", "") != domain] + domain_rows
        merged.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("export_run_id", "")))
        atomic_write_csv(gap_path, _GAP_FIELDNAMES, merged)

    counts = Counter(row.get("coverage_status", "") for row in domain_rows)
    return {
        "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
        "effective_date": str(reference.get("effective_date", "")),
        "analysis_run_id": (domain_rows[0].get("analysis_run_id", "") if domain_rows else ""),
        "domain": domain,
        "files_scored": str(len(domain_rows)),
        "full_count": str(counts.get("full", 0)),
        "partial_count": str(counts.get("partial", 0)),
        "none_count": str(counts.get("none", 0)),
        "no_reference_count": str(counts.get("NO_REFERENCE_DEFINED", 0)),
    }
