"""Build segment_manifest.csv and run_registry.csv from file_metadata.csv.

Derives a two-level segment hierarchy (unit_system → unit_system+client_label)
and produces the declarative manifest the orchestrator will execute, plus a
BI-ready run registry stub.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Sequence

SEED_ROLES = {"Template", "Container"}

MANIFEST_FIELDNAMES = [
    "segment_id",
    "parent_segment_id",
    "segment_level",
    "unit_system",
    "client_label",
    "run_type",
    "file_count",
    "export_run_ids",
    "has_seed_file",
    "seed_export_run_ids",
    "population_hash",
    "notes",
]

REGISTRY_FIELDNAMES = [
    "segment_id",
    "parent_segment_id",
    "run_type",
    "population_hash",
    "output_folder",
    "status",
    "last_run_utc",
    "notes",
]


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in csv.DictReader(f)]


def _atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


def _population_hash(export_run_ids: List[str]) -> str:
    token = "|".join(sorted(export_run_ids))
    return hashlib.sha1(token.encode("utf-8")).hexdigest()


def _sanitize_folder(segment_id: str) -> str:
    return segment_id.replace("|", "_").lower()


def _build_segments(
    rows: List[Dict[str, str]],
    min_files: int,
) -> List[Dict[str, str]]:
    # Collect per-unit_system and per-(unit_system, client_label) members.
    level1: Dict[str, List[str]] = defaultdict(list)          # unit_system -> [export_run_id]
    level1_seeds: Dict[str, List[str]] = defaultdict(list)    # unit_system -> [seed export_run_id]
    level2: Dict[tuple, List[str]] = defaultdict(list)        # (us, cl) -> [export_run_id]
    level2_seeds: Dict[tuple, List[str]] = defaultdict(list)  # (us, cl) -> [seed export_run_id]

    for row in rows:
        us = (row.get("unit_system") or "").strip()
        cl = (row.get("client_label") or "").strip()
        role = (row.get("governance_role") or "").strip()
        eid = (row.get("export_run_id") or "").strip()

        if not us or not eid:
            continue  # skip rows without unit_system or export_run_id

        level1[us].append(eid)
        level2[(us, cl)].append(eid)

        if role in SEED_ROLES:
            level1_seeds[us].append(eid)
            level2_seeds[(us, cl)].append(eid)

    manifest_rows: List[Dict[str, str]] = []

    # Level 1 segments
    for us in sorted(level1):
        eids = sorted(set(level1[us]))
        seed_eids = sorted(set(level1_seeds[us]))
        all_roles = set()
        for row in rows:
            if (row.get("unit_system") or "").strip() == us:
                r = (row.get("governance_role") or "").strip()
                if r:
                    all_roles.add(r)
        no_project = not any(
            (row.get("governance_role") or "").strip() == "Project"
            for row in rows
            if (row.get("unit_system") or "").strip() == us
        )
        notes = "seed_only" if no_project and seed_eids else ""
        manifest_rows.append({
            "segment_id": us,
            "parent_segment_id": "",
            "segment_level": "1",
            "unit_system": us,
            "client_label": "",
            "run_type": "bundle",
            "file_count": str(len(eids)),
            "export_run_ids": "|".join(eids),
            "has_seed_file": "true" if seed_eids else "false",
            "seed_export_run_ids": "|".join(seed_eids),
            "population_hash": _population_hash(eids),
            "notes": notes,
        })

    # Level 2 segments
    for (us, cl) in sorted(level2):
        eids = sorted(set(level2[(us, cl)]))
        seed_eids = sorted(set(level2_seeds[(us, cl)]))
        seg_id = f"{us}|{cl}"  # always pipe-delimited; keeps level-2 distinct from level-1 when cl is blank
        file_count = len(eids)
        no_project = not any(
            (row.get("governance_role") or "").strip() == "Project"
            for row in rows
            if (row.get("unit_system") or "").strip() == us
            and (row.get("client_label") or "").strip() == cl
        )
        notes_parts = []
        if file_count < min_files:
            notes_parts.append("below_min_files")
        if no_project and seed_eids:
            notes_parts.append("seed_only")
        run_type = "skip" if file_count < min_files else "bundle"
        manifest_rows.append({
            "segment_id": seg_id,
            "parent_segment_id": us,
            "segment_level": "2",
            "unit_system": us,
            "client_label": cl,
            "run_type": run_type,
            "file_count": str(file_count),
            "export_run_ids": "|".join(eids),
            "has_seed_file": "true" if seed_eids else "false",
            "seed_export_run_ids": "|".join(seed_eids),
            "population_hash": _population_hash(eids),
            "notes": "|".join(notes_parts),
        })

    # Sort: level 1 before level 2, then by segment_id within each level.
    manifest_rows.sort(key=lambda r: (int(r["segment_level"]), r["segment_id"]))
    return manifest_rows


def _build_registry(manifest_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    registry = []
    seen_folders: Dict[str, int] = {}
    for row in manifest_rows:
        if row["run_type"] == "skip":
            continue
        base = _sanitize_folder(row["segment_id"])
        if base in seen_folders:
            seen_folders[base] += 1
            folder = f"{base}_{seen_folders[base]}"
        else:
            seen_folders[base] = 1
            folder = base
        registry.append({
            "segment_id": row["segment_id"],
            "parent_segment_id": row["parent_segment_id"],
            "run_type": row["run_type"],
            "population_hash": row["population_hash"],
            "output_folder": folder,
            "status": "pending",
            "last_run_utc": "",
            "notes": row.get("notes", ""),
        })
    return registry


def _print_summary(
    manifest_path: Path,
    registry_path: Path,
    manifest_rows: List[Dict[str, str]],
    min_files: int,
) -> None:
    level1_rows = [r for r in manifest_rows if r["segment_level"] == "1"]
    level2_bundle = [r for r in manifest_rows if r["segment_level"] == "2" and r["run_type"] == "bundle"]
    level2_skip = [r for r in manifest_rows if r["segment_level"] == "2" and r["run_type"] == "skip"]

    n_segments = len([r for r in manifest_rows if r["run_type"] != "skip"])
    print(f"Segment manifest written: {manifest_path}")
    print(f"Run registry written: {registry_path}")
    print()
    print(f"Run plan ({n_segments} segments):")

    print("  Level 1:")
    for r in level1_rows:
        tags = []
        if r["notes"] == "seed_only":
            tags.append("[template-only]")
        if r["has_seed_file"] == "true":
            tags.append("[has seed]")
        tag_str = "  " + "  ".join(tags) if tags else ""
        print(f"    {r['segment_id']}  ({r['file_count']} files){tag_str}")

    if level2_bundle:
        print("  Level 2 (bundle):")
        for r in level2_bundle:
            tags = []
            if "seed_only" in (r.get("notes") or ""):
                tags.append("[template-only]")
            if r["has_seed_file"] == "true":
                tags.append("[has seed]")
            tag_str = "  " + "  ".join(tags) if tags else ""
            print(f"    {r['segment_id']}  ({r['file_count']} files){tag_str}")

    if level2_skip:
        print(f"  Skipped (below min_files={min_files}):")
        for r in level2_skip:
            print(f"    {r['segment_id']}  ({r['file_count']} file{'s' if int(r['file_count']) != 1 else ''})")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build segment_manifest.csv and run_registry.csv from file_metadata.csv.",
    )
    parser.add_argument("--metadata-file", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--out-dir", required=True, help="Directory to write output files")
    parser.add_argument("--min-files", type=int, default=3, help="Minimum file count for a segment (default: 3)")
    args = parser.parse_args(argv)

    metadata_path = Path(args.metadata_file)
    if not metadata_path.is_file():
        sys.stderr.write(f"[ERROR] --metadata-file not found: {metadata_path}\n")
        return 1

    out_dir = Path(args.out_dir)
    min_files: int = args.min_files

    rows = _read_csv(metadata_path)
    if not rows:
        sys.stderr.write(f"[WARN] file_metadata.csv is empty: {metadata_path}\n")

    skipped_blank = sum(1 for r in rows if not (r.get("unit_system") or "").strip())
    if skipped_blank:
        sys.stderr.write(f"[WARN] Excluded {skipped_blank} row(s) with blank unit_system\n")

    manifest_rows = _build_segments(rows, min_files)
    registry_rows = _build_registry(manifest_rows)

    manifest_path = out_dir / "segment_manifest.csv"
    registry_path = out_dir / "run_registry.csv"

    _atomic_write_csv(manifest_path, MANIFEST_FIELDNAMES, manifest_rows)
    _atomic_write_csv(registry_path, REGISTRY_FIELDNAMES, registry_rows)

    _print_summary(manifest_path, registry_path, manifest_rows, min_files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
