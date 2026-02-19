#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+].+)?$")


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{k: ("" if v is None else str(v)) for k, v in row.items()} for row in csv.DictReader(f)]


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate v2.1 split-analysis and analysis contract outputs")
    ap.add_argument("--phase0-dir", required=True)
    ap.add_argument("--analysis-dir", required=True)
    ap.add_argument("--split-dir", required=True)
    args = ap.parse_args()

    phase0_dir = Path(args.phase0_dir)
    analysis_dir = Path(args.analysis_dir)
    split_dir = Path(args.split_dir)

    file_metadata = read_csv(phase0_dir / "file_metadata.csv")
    file_exports = {r.get("export_run_id", "") for r in file_metadata if r.get("export_run_id", "")}

    errors: List[str] = []

    for r in file_metadata:
        if not SEMVER_RE.match(r.get("schema_version", "")):
            errors.append("file_metadata.csv has non-semver schema_version")
            break

    manifest = read_csv(analysis_dir / "analysis_manifest.csv")
    if manifest:
        row = manifest[0]
        for k in ("join_key_policy_version", "pattern_promotion_policy_version", "authority_metric_version"):
            if not SEMVER_RE.match(row.get(k, "")):
                errors.append(f"analysis_manifest.csv invalid or missing semver field: {k}")
        if row.get("is_incremental_update", "") not in {"0", "1"}:
            errors.append("analysis_manifest.csv is_incremental_update must be 0/1")

    required = {"schema_version", "analysis_run_id", "domain", "export_run_id"}
    for csv_path in sorted(split_dir.rglob("*.csv")):
        rows = read_csv(csv_path)
        if not rows:
            continue
        cols = set(rows[0].keys())
        missing = sorted(required - cols)
        if missing:
            errors.append(f"{csv_path}: missing required columns {missing}")
            continue
        for r in rows:
            if not r.get("domain", ""):
                errors.append(f"{csv_path}: empty domain value")
                break
            if r.get("export_run_id", "") and r.get("export_run_id", "") not in file_exports:
                # allow bridge files and synthetic rows to pass if they do not represent model rows
                if csv_path.name not in {"split_cluster_to_pattern_map.csv", "file_id_to_export_run_id.csv"}:
                    errors.append(f"{csv_path}: export_run_id not joinable to file_metadata")
                    break
            if not SEMVER_RE.match(r.get("schema_version", "")):
                errors.append(f"{csv_path}: schema_version not semver")
                break

    if errors:
        print("VALIDATION_FAILED")
        for e in errors:
            print(f"- {e}")
        raise SystemExit(1)

    print("VALIDATION_OK")


if __name__ == "__main__":
    main()
