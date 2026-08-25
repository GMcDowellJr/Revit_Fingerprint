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
    """Read a CSV file into string-normalized dictionaries.

    --- trace ---
    reads: `path` -- a Path to a CSV file; write_results_registry() calls this with
        manifest_file (--manifest-file, segment_manifest.csv from
        tools/build_segment_manifest.py) and registry_file (--registry-file,
        run_registry.csv, also from build_segment_manifest.py and subsequently updated
        in place by tools/run_segment_orchestrator.py's write_registry_atomic() after
        each segment run).
    calls: none (stdlib csv.DictReader only).
    thresholds: none.
    returns: list[dict[str,str]], every value normalized to str with None replaced by "";
        consumed by build_results_registry_rows() as manifest_rows/registry_rows.
    """
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [
            {str(k): "" if v is None else str(v) for k, v in row.items()}
            for row in reader
        ]


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    """Write CSV rows atomically using a temp file in the destination directory.

    --- trace ---
    reads: `path`, `fieldnames`, `rows` -- caller-supplied; write_results_registry()
        calls this with output_file (--output-file), the module-level
        RESULTS_REGISTRY_FIELDNAMES constant, and build_results_registry_rows()'s
        return value.
    calls: none (stdlib csv.DictWriter, tempfile.NamedTemporaryFile).
    thresholds: none.
    returns: None; writes `path` atomically (temp file in the same directory, then
        Path.replace()) so a reader never observes a partially-written
        results_registry.csv.
    """
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
    """Return one results-registry row for every segment in the manifest.

    --- trace ---
    reads: `manifest_rows` -- rows read from segment_manifest.csv
        (tools/build_segment_manifest.py's MANIFEST_FIELDNAMES; this function reads
        segment_id/parent_segment_id/segment_level/governance_role/run_type from it);
        `registry_rows` -- rows read from run_registry.csv
        (tools/build_segment_manifest.py's REGISTRY_FIELDNAMES, as subsequently updated
        in place by tools/run_segment_orchestrator.py's write_registry_atomic() after
        each segment run; this function reads parent_segment_id/output_folder/run_type/
        status/last_run_utc from it).
    calls: _safe_int() (via the sort key).
    thresholds: none named -- a manifest_row's own parent_segment_id/run_type values are
        only used as a fallback when the matching registry_row is missing or blank
        (registry_row.get(...) or manifest_row.get(...) or "").
    returns: list[dict] with keys segment_id/parent_segment_id/segment_level/
        governance_role/output_folder/run_type/status/last_run_utc
        (RESULTS_REGISTRY_FIELDNAMES), one row per segment_manifest.csv segment_id,
        sorted by (segment_level, segment_id); consumed by write_results_registry(),
        which writes it to results_registry.csv -- the single stable query surface this
        stage exists to produce.
    """
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
    """Best-effort int conversion for the sort key, defaulting to 0.

    --- trace ---
    reads: `value` -- caller-supplied (build_results_registry_rows()'s sort-key call
        passes row.get("segment_level")).
    calls: none (int(), str()).
    thresholds: none.
    returns: int(value), or 0 if value is missing/non-numeric (ValueError swallowed);
        consumed by build_results_registry_rows()'s sort key so a blank or malformed
        segment_level sorts first rather than raising.
    """
    try:
        return int(str(value or "0"))
    except ValueError:
        return 0


def write_results_registry(manifest_file: Path, registry_file: Path, output_file: Path) -> int:
    """Build and atomically write results_registry.csv. Returns rows written.

    --- trace ---
    reads: `manifest_file`, `registry_file`, `output_file` -- Paths, from main()'s CLI
        --manifest-file/--registry-file/--output-file, or (when this module is imported
        directly) from tools/run_segment_orchestrator.py's own call with manifest_file/
        registry_file/results_registry_file.
    calls: read_csv_rows() (x2); build_results_registry_rows(); atomic_write_csv().
    thresholds: none.
    returns: int row count written; writes results_registry.csv to `output_file`.
        Consumed by main() for the printed summary, and directly by
        tools/run_segment_orchestrator.py's run_orchestrator()/_run_one_segment(), which
        import write_results_registry from this module and call it after every segment
        registry update so results_registry.csv stays current mid-run, not only at
        Run C2.5.
    """
    manifest_rows = read_csv_rows(manifest_file)
    registry_rows = read_csv_rows(registry_file)
    rows = build_results_registry_rows(manifest_rows, registry_rows)
    atomic_write_csv(output_file, RESULTS_REGISTRY_FIELDNAMES, rows)
    return len(rows)


def main(argv: List[str] | None = None) -> int:
    """CLI entry point: validate input paths, build, and write results_registry.csv.

    --- trace ---
    reads: CLI args --manifest-file, --registry-file, --output-file (all required).
    calls: write_results_registry().
    thresholds: none.
    returns: int exit code (1 if --manifest-file/--registry-file is missing, else 0);
        prints a row-count summary to stdout. Invoked as **Run C2.5** (l.266 of
        tools/corpus_update_runbook.ps1) after tools/run_segment_orchestrator.py's
        Run C2 completes.
    """
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
