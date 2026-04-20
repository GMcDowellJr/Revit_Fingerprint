from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id


def _is_true(value: str) -> bool:
    return (value or "").strip().lower() == "true"


def _fmt_float(value: float) -> str:
    return f"{float(value):.6f}"


def build_bundle_share_profile(
    analysis_dir: Path,
    domain_out_dir: Path,
    domain: str,
    analysis_run_id: str = "",
    scope_key: Optional[str] = None,
) -> Dict[str, int]:
    pattern_presence_path = analysis_dir / "pattern_presence_file.csv"
    if not pattern_presence_path.is_file():
        print(f"[step2b][warn] pattern_presence_file.csv not found in {analysis_dir} — skipping share profile")
        return {"bundles_profiled": 0, "member_rows": 0, "rollup_rows": 0}

    bundle_file_path = domain_out_dir / "bundle_file_membership.csv"
    bundle_membership_path = domain_out_dir / "bundle_membership.csv"
    if not bundle_file_path.is_file():
        raise FileNotFoundError(f"Required input missing for step2b: {bundle_file_path}")
    bundle_file_rows = read_csv_rows(bundle_file_path)
    bundle_membership_rows = read_csv_rows(bundle_membership_path) if bundle_membership_path.is_file() else []
    if not bundle_file_rows and not bundle_membership_rows:
        atomic_write_csv(
            domain_out_dir / "bundle_share_profile.csv",
            [
                "schema_version",
                "analysis_run_id",
                "domain",
                "scope_key",
                "bundle_id",
                "pattern_id",
                "files_in_bundle",
                "files_where_pattern_present",
                "median_share_pct",
                "mean_share_pct",
                "files_where_dominant",
                "pct_bundle_files_dominant",
                "pct_bundle_files_any_dominant",
            ],
            [],
        )
        print(f"[step2b] domain={domain} bundles_profiled=0 member_rows=0 rollup_rows=0")
        return {"bundles_profiled": 0, "member_rows": 0, "rollup_rows": 0}

    run_id = resolve_analysis_run_id(bundle_file_rows or bundle_membership_rows, analysis_run_id)

    files_by_bundle: Dict[str, Set[str]] = defaultdict(set)
    bundle_scope: Dict[str, str] = {}
    for row in bundle_file_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        bundle_id = (row.get("bundle_id", "") or "").strip()
        export_run_id = (row.get("export_run_id", "") or "").strip()
        if not bundle_id or not export_run_id:
            continue
        row_scope = (row.get("scope_key", "") or "").strip()
        if scope_key is not None and row_scope != scope_key:
            continue
        files_by_bundle[bundle_id].add(export_run_id)
        bundle_scope[bundle_id] = row_scope

    patterns_by_bundle: Dict[str, Set[str]] = defaultdict(set)
    for row in bundle_membership_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        bundle_id = (row.get("bundle_id", "") or "").strip()
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not bundle_id or not pattern_id:
            continue
        row_scope = (row.get("scope_key", "") or "").strip()
        if scope_key is not None and row_scope != scope_key:
            continue
        patterns_by_bundle[bundle_id].add(pattern_id)
        bundle_scope[bundle_id] = row_scope

    bundles = sorted(set(files_by_bundle.keys()) | set(patterns_by_bundle.keys()))

    presence_rows = read_csv_rows(pattern_presence_path)
    presence_index: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in presence_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        export_run_id = (row.get("export_run_id", "") or "").strip()
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not export_run_id or not pattern_id:
            continue
        key = (export_run_id, pattern_id)
        if key in presence_index:
            raise ValueError(f"Duplicate pattern presence row for {key}")
        presence_index[key] = row

    out_rows: List[Dict[str, str]] = []
    member_rows_count = 0
    rollup_rows_count = 0

    for bundle_id in bundles:
        bundle_files = sorted(files_by_bundle.get(bundle_id, set()))
        member_patterns = sorted(patterns_by_bundle.get(bundle_id, set()))
        files_in_bundle = len(bundle_files)
        bundle_scope_key = bundle_scope.get(bundle_id, "")

        per_pattern_medians: List[float] = []
        per_pattern_means: List[float] = []
        dominant_by_file: Dict[str, Dict[str, bool]] = defaultdict(dict)

        member_rows_for_bundle: List[Dict[str, str]] = []
        for pattern_id in member_patterns:
            shares: List[float] = []
            files_where_pattern_present = 0
            files_where_dominant = 0
            for export_run_id in bundle_files:
                key = (export_run_id, pattern_id)
                entry = presence_index.get(key)
                if entry is None:
                    continue
                files_where_pattern_present += 1
                share_pct = float((entry.get("pattern_share_pct", "0") or "0"))
                shares.append(share_pct)
                is_dominant = _is_true(entry.get("is_dominant_pattern", ""))
                dominant_by_file[export_run_id][pattern_id] = is_dominant
                if is_dominant:
                    files_where_dominant += 1

            median_share_pct = statistics.median(shares) if shares else 0.0
            mean_share_pct = statistics.mean(shares) if shares else 0.0
            pct_bundle_files_dominant = (files_where_dominant / files_in_bundle) if files_in_bundle else 0.0

            per_pattern_medians.append(float(median_share_pct))
            per_pattern_means.append(float(mean_share_pct))

            member_rows_for_bundle.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "scope_key": bundle_scope_key,
                    "bundle_id": bundle_id,
                    "pattern_id": pattern_id,
                    "files_in_bundle": str(files_in_bundle),
                    "files_where_pattern_present": str(files_where_pattern_present),
                    "median_share_pct": _fmt_float(median_share_pct),
                    "mean_share_pct": _fmt_float(mean_share_pct),
                    "files_where_dominant": str(files_where_dominant),
                    "pct_bundle_files_dominant": _fmt_float(pct_bundle_files_dominant),
                    "pct_bundle_files_any_dominant": "",
                }
            )

        member_rows_for_bundle.sort(key=lambda r: (r.get("bundle_id", ""), r.get("pattern_id", "")))
        out_rows.extend(member_rows_for_bundle)
        member_rows_count += len(member_rows_for_bundle)

        files_where_all_dominant = 0
        files_where_any_dominant = 0
        for export_run_id in bundle_files:
            pattern_flags = dominant_by_file.get(export_run_id, {})
            if member_patterns and all(pattern_flags.get(pid, False) for pid in member_patterns):
                files_where_all_dominant += 1
            if any(pattern_flags.get(pid, False) for pid in member_patterns):
                files_where_any_dominant += 1

        rollup_row = {
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": run_id,
            "domain": domain,
            "scope_key": bundle_scope_key,
            "bundle_id": bundle_id,
            "pattern_id": "__bundle__",
            "files_in_bundle": str(files_in_bundle),
            "files_where_pattern_present": "",
            "median_share_pct": _fmt_float(statistics.median(per_pattern_medians) if per_pattern_medians else 0.0),
            "mean_share_pct": _fmt_float(statistics.mean(per_pattern_means) if per_pattern_means else 0.0),
            "files_where_dominant": str(files_where_all_dominant),
            "pct_bundle_files_dominant": _fmt_float((files_where_all_dominant / files_in_bundle) if files_in_bundle else 0.0),
            "pct_bundle_files_any_dominant": _fmt_float((files_where_any_dominant / files_in_bundle) if files_in_bundle else 0.0),
        }
        out_rows.append(rollup_row)
        rollup_rows_count += 1

    atomic_write_csv(
        domain_out_dir / "bundle_share_profile.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "bundle_id",
            "pattern_id",
            "files_in_bundle",
            "files_where_pattern_present",
            "median_share_pct",
            "mean_share_pct",
            "files_where_dominant",
            "pct_bundle_files_dominant",
            "pct_bundle_files_any_dominant",
        ],
        out_rows,
    )
    print(
        f"[step2b] domain={domain} bundles_profiled={len(bundles)} "
        f"member_rows={member_rows_count} rollup_rows={rollup_rows_count}"
    )
    return {
        "bundles_profiled": len(bundles),
        "member_rows": member_rows_count,
        "rollup_rows": rollup_rows_count,
    }


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build per-bundle share profile")
    p.add_argument("--analysis-dir", required=True, type=Path)
    p.add_argument("--domain-out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    p.add_argument("--analysis-run-id", default="")
    p.add_argument("--scope-key", default=None)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    build_bundle_share_profile(
        analysis_dir=args.analysis_dir,
        domain_out_dir=args.domain_out_dir,
        domain=args.domain,
        analysis_run_id=args.analysis_run_id,
        scope_key=args.scope_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
