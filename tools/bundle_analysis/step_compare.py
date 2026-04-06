from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Set

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id
else:
    from .common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id


FILE_GAP_FIELDNAMES = [
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


def _format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return ""
    return f"{(100.0 * numerator / denominator):.6f}"


def run_compare(
    analysis_dir: Path,
    out_dir: Path,
    reference: dict,
    domain: str,
) -> Dict[str, int]:
    matrix_path = analysis_dir / domain / "membership_matrix.csv"
    if not matrix_path.is_file():
        raise FileNotFoundError(f"membership_matrix.csv not found for domain {domain!r}: {matrix_path}")

    rows = read_csv_rows(matrix_path)
    analysis_run_id = resolve_analysis_run_id(rows, str(reference.get("_analysis_run_id_filter", "") or ""))

    filtered_rows = [
        row
        for row in rows
        if row.get("analysis_run_id", "") == analysis_run_id and row.get("domain", "") == domain
    ]

    seed_export_run_id = str(reference.get("seed_export_run_id", "") or "").strip()
    if seed_export_run_id:
        filtered_rows = [
            row for row in filtered_rows if (row.get("export_run_id", "") or "").strip() != seed_export_run_id
        ]

    file_patterns: Dict[str, Set[str]] = {}
    for row in filtered_rows:
        export_run_id = (row.get("export_run_id", "") or "").strip()
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not export_run_id or not pattern_id:
            continue
        file_patterns.setdefault(export_run_id, set()).add(pattern_id)

    domain_reference = reference.get("domains", {}).get(domain)
    out_rows: List[Dict[str, str]] = []

    summary = {
        "analysis_run_id": analysis_run_id,
        "domain": domain,
        "files_scored": 0,
        "full": 0,
        "partial": 0,
        "none": 0,
        "no_reference": 0,
    }

    if domain_reference is None:
        out_rows.append(
            {
                "reference_bundle_id": str(reference["reference_bundle_id"]),
                "effective_date": str(reference["effective_date"]),
                "analysis_run_id": analysis_run_id,
                "domain": domain,
                "export_run_id": "",
                "patterns_required": "",
                "patterns_present": "",
                "patterns_missing": "",
                "gap_pattern_ids": "",
                "coverage_pct": "",
                "coverage_status": "NO_REFERENCE_DEFINED",
            }
        )
        summary["no_reference"] = 1
    else:
        required_patterns = set(domain_reference)
        patterns_required = len(required_patterns)

        for export_run_id in sorted(file_patterns.keys()):
            present = file_patterns.get(export_run_id, set())
            present_count = len(required_patterns & present)
            missing_patterns = sorted(required_patterns - present)
            missing_count = len(missing_patterns)
            coverage_status = "full" if missing_count == 0 else ("none" if present_count == 0 else "partial")

            out_rows.append(
                {
                    "reference_bundle_id": str(reference["reference_bundle_id"]),
                    "effective_date": str(reference["effective_date"]),
                    "analysis_run_id": analysis_run_id,
                    "domain": domain,
                    "export_run_id": export_run_id,
                    "patterns_required": str(patterns_required),
                    "patterns_present": str(present_count),
                    "patterns_missing": str(missing_count),
                    "gap_pattern_ids": "|".join(missing_patterns),
                    "coverage_pct": _format_pct(present_count, patterns_required),
                    "coverage_status": coverage_status,
                }
            )
            summary["files_scored"] += 1
            summary[coverage_status] += 1

    report_path = out_dir / "file_gap_report.csv"
    existing_rows = read_csv_rows(report_path) if report_path.is_file() else []
    retained = [
        row
        for row in existing_rows
        if not (
            row.get("analysis_run_id", "") == analysis_run_id
            and row.get("domain", "") == domain
            and row.get("reference_bundle_id", "") == str(reference["reference_bundle_id"])
        )
    ]
    merged_rows = retained + out_rows
    merged_rows.sort(key=lambda r: (r["analysis_run_id"], r["domain"], r["export_run_id"], r["coverage_status"]))
    atomic_write_csv(report_path, FILE_GAP_FIELDNAMES, merged_rows)

    return summary
