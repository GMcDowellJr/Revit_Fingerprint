from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows


def emit_stub(out_dir: Path, domain: str) -> Dict[str, int]:
    fieldnames = [
        "schema_version",
        "analysis_run_id",
        "domain",
        "scope_key",
        "child_bundle_id",
        "parent_bundle_id",
        "difference_pattern_ids",
        "difference_pattern_count",
        "difference_presence_pct_min",
        "difference_presence_pct_max",
        "difference_presence_pct_mean",
    ]

    domain_out_dir = out_dir / domain
    edges_path = domain_out_dir / "bundle_dag_edges.csv"
    if not edges_path.is_file():
        atomic_write_csv(domain_out_dir / "bundle_dag_differences.csv", fieldnames, [])
        return {"rows": 0, "analysis_run_id": ""}

    edge_rows = read_csv_rows(edges_path)
    if not edge_rows:
        atomic_write_csv(domain_out_dir / "bundle_dag_differences.csv", fieldnames, [])
        return {"rows": 0, "analysis_run_id": ""}

    bundles_path = domain_out_dir / "bundles.csv"
    membership_path = domain_out_dir / "bundle_membership.csv"
    matrix_path = domain_out_dir / "membership_matrix.csv"
    scope_registry_path = domain_out_dir / "scope_registry.csv"

    bundles_rows = read_csv_rows(bundles_path) if bundles_path.is_file() else []
    membership_rows = read_csv_rows(membership_path) if membership_path.is_file() else []
    matrix_rows = read_csv_rows(matrix_path) if matrix_path.is_file() else []
    scope_rows = read_csv_rows(scope_registry_path) if scope_registry_path.is_file() else []

    run_id = ""
    if bundles_rows:
        run_id = bundles_rows[0].get("analysis_run_id", "")
    if not run_id and membership_rows:
        run_id = membership_rows[0].get("analysis_run_id", "")

    bundle_patterns: Dict[tuple[str, str], set[str]] = {}
    for row in membership_rows:
        scope_key = row.get("scope_key", "")
        bundle_id = row.get("bundle_id", "")
        pattern_id = row.get("pattern_id", "")
        if not scope_key or not bundle_id or not pattern_id:
            continue
        bundle_patterns.setdefault((scope_key, bundle_id), set()).add(pattern_id)

    scope_files_total: Dict[str, int] = {}
    for row in scope_rows:
        scope_key = row.get("scope_key", "")
        files_raw = row.get("files_in_scope", "")
        if not scope_key:
            continue
        try:
            scope_files_total[scope_key] = int(files_raw)
        except (TypeError, ValueError):
            scope_files_total[scope_key] = 0

    pattern_export_runs: Dict[tuple[str, str], set[str]] = {}
    for row in matrix_rows:
        scope_key = row.get("scope_key", "")
        pattern_id = row.get("pattern_id", "")
        export_run_id = row.get("export_run_id", "")
        if not scope_key or not pattern_id or not export_run_id:
            continue
        pattern_export_runs.setdefault((scope_key, pattern_id), set()).add(export_run_id)

    out_rows = []
    for edge in edge_rows:
        if edge.get("is_direct_parent", "").strip().lower() != "true":
            continue
        scope_key = edge.get("scope_key", "")
        child_bundle_id = edge.get("child_bundle_id", "")
        parent_bundle_id = edge.get("parent_bundle_id", "")
        if not scope_key or not child_bundle_id or not parent_bundle_id:
            continue

        child_patterns = bundle_patterns.get((scope_key, child_bundle_id), set())
        parent_patterns = bundle_patterns.get((scope_key, parent_bundle_id), set())
        diff_patterns = sorted(child_patterns - parent_patterns)
        if not diff_patterns:
            continue

        files_total = scope_files_total.get(scope_key, 0)
        presence_values = []
        for pattern_id in diff_patterns:
            if files_total <= 0:
                presence_values.append(0.0)
                continue
            present_count = len(pattern_export_runs.get((scope_key, pattern_id), set()))
            presence_values.append(present_count / files_total)

        min_presence = min(presence_values) if presence_values else 0.0
        max_presence = max(presence_values) if presence_values else 0.0
        mean_presence = (sum(presence_values) / len(presence_values)) if presence_values else 0.0

        out_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": run_id,
                "domain": domain,
                "scope_key": scope_key,
                "child_bundle_id": child_bundle_id,
                "parent_bundle_id": parent_bundle_id,
                "difference_pattern_ids": "|".join(diff_patterns),
                "difference_pattern_count": str(len(diff_patterns)),
                "difference_presence_pct_min": f"{min_presence:.6f}",
                "difference_presence_pct_max": f"{max_presence:.6f}",
                "difference_presence_pct_mean": f"{mean_presence:.6f}",
            }
        )

    out_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["child_bundle_id"], r["parent_bundle_id"]))
    atomic_write_csv(
        domain_out_dir / "bundle_dag_differences.csv",
        fieldnames,
        out_rows,
    )
    return {"rows": len(out_rows), "analysis_run_id": run_id}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 4")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
