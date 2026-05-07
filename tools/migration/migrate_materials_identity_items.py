# -*- coding: utf-8 -*-
"""
migrate_materials_identity_items.py

Migrates existing fingerprint JSON exports to inject material.graphics_sig_hash_v2
as an identity item on every materials record.

This avoids a full Dynamo re-extraction. The value already exists on each record
as rec["graphics_sig_hash_v2"] — this script surfaces it into
rec["identity_basis"]["items"] so the apply/discover pipeline can use it as a
join key.

Changes made per materials record:
  - Adds {"k": "material.graphics_sig_hash_v2", "v": <value>, "q": "ok"} to
    identity_basis.items if not already present.
  - Does NOT modify any other field (sig_hash, record_id, etc.).
  - Does NOT remove material.uid from identity_basis.items.

Output:
  - By default writes migrated files to --out-dir (original files unchanged).
  - With --in-place, overwrites originals (keeps .bak backup unless --no-backup).

Usage:
  # Write to a new directory (safe default)
  python tools/migrate_materials_identity_items.py --exports-dir C:/Exports --out-dir C:/Exports_migrated

  # In-place with backup
  python tools/migrate_materials_identity_items.py --exports-dir C:/Exports --in-place

  # Dry run (report what would change, write nothing)
  python tools/migrate_materials_identity_items.py --exports-dir C:/Exports --out-dir C:/Exports_migrated --dry-run
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ---------------------------------------------------------------------------
# Record walking
# ---------------------------------------------------------------------------

def _iter_materials_records(payload: Dict[str, Any]):
    """
    Yield (domain_key, record_list, record_index, record_dict) for all
    materials records in the export JSON.

    Handles both:
      - Flat structure: payload["materials"]["records"] = [...]
      - Nested structure: payload["domains"]["materials"]["records"] = [...]
    """
    # Flat structure (current)
    mat = payload.get("materials")
    if isinstance(mat, dict):
        records = mat.get("records")
        if isinstance(records, list):
            for i, rec in enumerate(records):
                if isinstance(rec, dict):
                    yield ("materials", records, i, rec)
        return

    # Nested under "domains" key (older format)
    domains = payload.get("domains")
    if isinstance(domains, dict):
        mat = domains.get("materials")
        if isinstance(mat, dict):
            records = mat.get("records")
            if isinstance(records, list):
                for i, rec in enumerate(records):
                    if isinstance(rec, dict):
                        yield ("materials", records, i, rec)


def _get_identity_items(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the identity_basis.items list, creating the structure if absent."""
    ib = rec.get("identity_basis")
    if not isinstance(ib, dict):
        rec["identity_basis"] = {"items": []}
        return rec["identity_basis"]["items"]
    items = ib.get("items")
    if not isinstance(items, list):
        ib["items"] = []
        return ib["items"]
    return items


_INJECT_KEY = "material.graphics_sig_hash_v2"


def _migrate_record(rec: Dict[str, Any]) -> bool:
    """
    Inject material.graphics_sig_hash_v2 into identity_basis.items.
    Returns True if a change was made.
    """
    # Read the sig hash value from the record
    sig_value = rec.get("graphics_sig_hash_v2") or rec.get("sig_hash")
    if not sig_value:
        return False  # Nothing to inject

    items = _get_identity_items(rec)

    # Check if already present
    for item in items:
        if isinstance(item, dict) and item.get("k") == _INJECT_KEY:
            return False  # Already migrated

    # Inject — append and then re-sort alphabetically by k (pipeline convention)
    items.append({
        "k": _INJECT_KEY,
        "v": str(sig_value),
        "q": "ok",
    })
    items.sort(key=lambda it: str(it.get("k", "")))
    return True


# ---------------------------------------------------------------------------
# Per-file migration
# ---------------------------------------------------------------------------

def _migrate_file(
    src: Path,
    dst: Optional[Path],
    *,
    dry_run: bool,
    in_place: bool,
    no_backup: bool,
) -> Tuple[int, int, int]:
    """
    Migrate one JSON file.
    Returns (records_found, records_modified, records_already_migrated).
    """
    try:
        payload = _load_json(src)
    except Exception as e:
        print(f"  [SKIP] {src.name}: failed to load JSON — {e}")
        return 0, 0, 0

    found = 0
    modified = 0
    already = 0

    for _domain_key, _records, idx, rec in _iter_materials_records(payload):
        found += 1
        changed = _migrate_record(rec)
        if changed:
            modified += 1
        else:
            already += 1

    if found == 0:
        # No materials domain in this file — skip write
        return 0, 0, 0

    if dry_run:
        print(f"  [DRY RUN] {src.name}: {found} records, {modified} would be updated, {already} already ok")
        return found, modified, already

    if modified == 0:
        print(f"  [SKIP] {src.name}: all {found} records already migrated")
        return found, 0, already

    if in_place:
        if not no_backup:
            bak = src.with_suffix(src.suffix + ".bak")
            shutil.copy2(src, bak)
        _write_json(src, payload)
        print(f"  [DONE] {src.name}: updated {modified}/{found} records (in-place)")
    else:
        assert dst is not None
        _write_json(dst, payload)
        print(f"  [DONE] {src.name}: updated {modified}/{found} records -> {dst.name}")

    return found, modified, already


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _find_json_files(exports_dir: Path) -> List[Path]:
    """Find all fingerprint JSON files, preferring __fingerprint.json."""
    fps = sorted(exports_dir.glob("*__fingerprint.json"))
    if fps:
        return fps
    # Fall back to all .json (excluding .legacy.json and .bak)
    return sorted(
        p for p in exports_dir.glob("*.json")
        if not p.name.lower().endswith(".legacy.json")
        and not p.name.lower().endswith(".bak")
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Migrate fingerprint JSON exports to inject material.graphics_sig_hash_v2 "
            "as an identity item. No Dynamo re-extraction required."
        )
    )
    ap.add_argument(
        "--exports-dir",
        required=True,
        help="Directory containing fingerprint JSON export files.",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for migrated files (default: required unless --in-place).",
    )
    ap.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite originals in-place (creates .bak backups unless --no-backup).",
    )
    ap.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip .bak backup when using --in-place.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing any files.",
    )
    args = ap.parse_args()

    exports_dir = Path(args.exports_dir).resolve()
    if not exports_dir.is_dir():
        print(f"ERROR: exports-dir not found: {exports_dir}", file=sys.stderr)
        sys.exit(1)

    if not args.in_place and not args.out_dir and not args.dry_run:
        print("ERROR: provide --out-dir or --in-place (or --dry-run to preview).", file=sys.stderr)
        sys.exit(1)

    out_dir: Optional[Path] = None
    if args.out_dir:
        out_dir = Path(args.out_dir).resolve()
        if not args.dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)

    json_files = _find_json_files(exports_dir)
    if not json_files:
        print(f"No JSON files found in: {exports_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"[migrate] Found {len(json_files)} JSON files in: {exports_dir}")
    if args.dry_run:
        print("[migrate] DRY RUN — no files will be written.")
    elif args.in_place:
        backup_note = " (no .bak)" if args.no_backup else " (with .bak backup)"
        print(f"[migrate] IN-PLACE mode{backup_note}")
    else:
        print(f"[migrate] Output directory: {out_dir}")
    print()

    total_files = 0
    total_records = 0
    total_modified = 0
    total_already = 0
    skipped_no_materials = 0

    for src in json_files:
        dst = (out_dir / src.name) if out_dir else None
        found, modified, already = _migrate_file(
            src, dst,
            dry_run=args.dry_run,
            in_place=args.in_place,
            no_backup=args.no_backup,
        )
        if found == 0:
            skipped_no_materials += 1
            continue
        total_files += 1
        total_records += found
        total_modified += modified
        total_already += already

    print()
    print("=" * 60)
    print(f"  Files with materials domain: {total_files}")
    print(f"  Files without materials:     {skipped_no_materials}")
    print(f"  Records found:               {total_records:,}")
    print(f"  Records updated:             {total_modified:,}")
    print(f"  Records already migrated:    {total_already:,}")
    if args.dry_run:
        print("  (DRY RUN — nothing written)")
    print("=" * 60)
    print()
    print("NEXT STEPS after migration:")
    print("  1. Update domain_join_key_policies.json — set materials required_items")
    print('     to ["material.graphics_sig_hash_v2"], move material.uid to optional_items')
    print("  2. Re-run the apply stage:")
    print("     python tools/run_extract_all.py <exports_dir> --out-root <out_root>")
    print("         --stages apply --domain-policy-json <policy.json>")
    print("  3. Re-run analyze1/analyze2 to rebuild domain_patterns.csv with new join keys")
    print()


if __name__ == "__main__":
    main()
