#!/usr/bin/env python3
"""
Build a BI-friendly results_registry.csv from segment_manifest.csv and run_registry.csv.

The output is one row per segment and gives downstream reporting a single stable
file to query instead of hand-wiring individual segment output folders.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Sequence

RESULTS_REGISTRY_FIELDNAMES = [
    "segment_id",
    "parent_segment_id",
    "segment_level",
    "governance_role",
    "output_folder",
    "run_type",
    "status",
    "last_run_utc",
]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    """Read a CSV file into string-normalized dictionaries."""
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [
            {str(k): "" if v is None else str(v) for k, v in row.items()}
            for row in reader
        ]


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    """Write CSV rows atomically using a temp file in the destination directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        delete=False,
        dir=str(path.parent),
        suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


def build_results_registry_rows(
    manifest_rows: Iterable[Dict[str, str]],
    registry_rows: Iterable[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Return one results-registry row for every segment in the manifest."""
    registry_by_segment = {
        (row.get("segment_id") or "").strip(): row
        for row in registry_rows
        if (row.get("segment_id") or "").strip()
    }

    rows: List[Dict[str, str]] = []
    for manifest_row in manifest_rows:
        segment_id = (manifest_row.get("segment_id") or "").strip()
        if not segment_id:
            continue
        registry_row = registry_by_segment.get(segment_id, {})
        rows.append(
            {
                "segment_id": segment_id,
                "parent_segment_id": (manifest_row.get("parent_segment_id") or registry_row.get("parent_segment_id") or "").strip(),
                "segment_level": (manifest_row.get("segment_level") or "").strip(),
                "governance_role": (manifest_row.get("governance_role") or "").strip(),
                "output_folder": (registry_row.get("output_folder") or "").strip(),
                "run_type": (registry_row.get("run_type") or manifest_row.get("run_type") or "").strip(),
                "status": (registry_row.get("status") or "").strip(),
                "last_run_utc": (registry_row.get("last_run_utc") or "").strip(),
            }
        )

    return sorted(rows, key=lambda row: (_safe_int(row.get("segment_level")), row.get("segment_id", "")))


def _safe_int(value: object) -> int:
    try:
        return int(str(value or "0"))
    except ValueError:
        return 0


def write_results_registry(manifest_file: Path, registry_file: Path, output_file: Path) -> int:
    """Build and atomically write results_registry.csv. Returns rows written."""
    manifest_rows = read_csv_rows(manifest_file)
    registry_rows = read_csv_rows(registry_file)
    rows = build_results_registry_rows(manifest_rows, registry_rows)
    atomic_write_csv(output_file, RESULTS_REGISTRY_FIELDNAMES, rows)
    return len(rows)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build results_registry.csv from segment_manifest.csv and run_registry.csv.",
    )
    parser.add_argument("--manifest-file", required=True, help="Path to segment_manifest.csv")
    parser.add_argument("--registry-file", required=True, help="Path to run_registry.csv")
    parser.add_argument(
        "--output-file",
        required=True,
        help="Path to write results_registry.csv",
    )
    args = parser.parse_args(argv)

    manifest_file = Path(args.manifest_file)
    registry_file = Path(args.registry_file)
    output_file = Path(args.output_file)

    for label, path in (("--manifest-file", manifest_file), ("--registry-file", registry_file)):
        if not path.is_file():
            sys.stderr.write(f"[ERROR] {label} not found: {path}\n")
            return 1

    rows_written = write_results_registry(manifest_file, registry_file, output_file)
    print(f"[results_registry] wrote {rows_written} row(s) to {output_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
