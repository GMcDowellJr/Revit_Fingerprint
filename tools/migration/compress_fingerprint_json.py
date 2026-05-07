# -*- coding: utf-8 -*-
"""
compress_fingerprint_json.py

Converts human-readable (indented, dev-mode) fingerprint JSON exports to
compact production format — matching runner/run_dynamo.py production output:
  json.dump(payload, f, separators=(',', ':'), sort_keys=True)

Use this when you have dev-mode exports (indent=2) and need to bring them
into the pipeline's expected compact format before running flatten/apply.

Also useful for reducing file size before archiving or sharing exports.

Typical size reduction: 40-60% depending on record density.

Usage:
  # Compress a directory of JSONs to a new directory
  python tools/compress_fingerprint_json.py --exports-dir C:/Exports_dev --out-dir C:/Exports

  # In-place compression with backup
  python tools/compress_fingerprint_json.py --exports-dir C:/Exports_dev --in-place

  # Dry run — report file sizes without writing
  python tools/compress_fingerprint_json.py --exports-dir C:/Exports_dev --out-dir C:/Exports --dry-run

  # Single file
  python tools/compress_fingerprint_json.py --file C:/Exports_dev/myproject__fingerprint.json --out-dir C:/Exports
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

def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_compact(path: Path, data: Any) -> int:
    """
    Write compact production JSON.
    Returns bytes written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, separators=(",", ":"), sort_keys=True)
        f.write("\n")
    return path.stat().st_size


def _is_already_compact(path: Path) -> bool:
    """
    Heuristic: read first 512 bytes and check for newlines beyond the first.
    Compact JSON has no interior newlines.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            sample = f.read(512)
        interior = sample.lstrip()
        # If the first non-whitespace char is { or [ and there's no newline after it,
        # it's already compact.
        if interior and interior[0] in ("{", "["):
            rest = interior[1:]
            return "\n" not in rest
    except Exception:
        pass
    return False


def _fmt_kb(n: int) -> str:
    return f"{n / 1024:.1f} KB"


# ---------------------------------------------------------------------------
# Per-file compression
# ---------------------------------------------------------------------------

def _compress_file(
    src: Path,
    dst: Optional[Path],
    *,
    dry_run: bool,
    in_place: bool,
    no_backup: bool,
    force: bool,
) -> Tuple[bool, int, int]:
    """
    Compress one JSON file.
    Returns (was_processed, src_bytes, dst_bytes).
    dst_bytes is 0 for dry_run or already-compact skips.
    """
    src_bytes = src.stat().st_size

    if not force and _is_already_compact(src):
        print(f"  [SKIP] {src.name}: already compact ({_fmt_kb(src_bytes)})")
        return False, src_bytes, src_bytes

    try:
        payload = _load_json(src)
    except Exception as e:
        print(f"  [SKIP] {src.name}: failed to load JSON — {e}")
        return False, src_bytes, src_bytes

    if dry_run:
        # Estimate compressed size without writing
        compact = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        est_bytes = len(compact.encode("utf-8")) + 1  # +1 for trailing newline
        pct = (1 - est_bytes / src_bytes) * 100 if src_bytes else 0
        print(f"  [DRY RUN] {src.name}: {_fmt_kb(src_bytes)} -> ~{_fmt_kb(est_bytes)} ({pct:.1f}% reduction)")
        return True, src_bytes, est_bytes

    if in_place:
        if not no_backup:
            bak = src.with_suffix(src.suffix + ".bak")
            shutil.copy2(src, bak)
        dst_bytes = _write_compact(src, payload)
        pct = (1 - dst_bytes / src_bytes) * 100 if src_bytes else 0
        print(f"  [DONE] {src.name}: {_fmt_kb(src_bytes)} -> {_fmt_kb(dst_bytes)} ({pct:.1f}% reduction, in-place)")
    else:
        assert dst is not None
        dst_bytes = _write_compact(dst, payload)
        pct = (1 - dst_bytes / src_bytes) * 100 if src_bytes else 0
        print(f"  [DONE] {src.name}: {_fmt_kb(src_bytes)} -> {_fmt_kb(dst_bytes)} ({pct:.1f}% reduction)")

    return True, src_bytes, dst_bytes


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _find_json_files(exports_dir: Path) -> List[Path]:
    """Find fingerprint JSON files, preferring *__fingerprint.json."""
    fps = sorted(exports_dir.glob("*__fingerprint.json"))
    if fps:
        return fps
    return sorted(
        p for p in exports_dir.glob("*.json")
        if not p.name.lower().endswith(".legacy.json")
        and not p.name.lower().endswith(".bak")
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Convert human-readable (indented) fingerprint JSON exports to compact "
            "production format (separators=(',',':'), sort_keys=True). "
            "Typically reduces file size by 40-60%%."
        )
    )

    src_group = ap.add_mutually_exclusive_group(required=True)
    src_group.add_argument("--exports-dir",
                           help="Directory containing fingerprint JSON export files.")
    src_group.add_argument("--file",
                           help="Single fingerprint JSON file to compress.")

    ap.add_argument("--out-dir", default=None,
                    help="Output directory for compressed files (required unless --in-place).")
    ap.add_argument("--in-place", action="store_true",
                    help="Overwrite originals in-place (creates .bak backups unless --no-backup).")
    ap.add_argument("--no-backup", action="store_true",
                    help="Skip .bak backup when using --in-place.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report sizes without writing any files.")
    ap.add_argument("--force", action="store_true",
                    help="Compress even if file appears already compact (re-sorts keys).")
    args = ap.parse_args()

    if not args.in_place and not args.out_dir and not args.dry_run:
        print("ERROR: provide --out-dir or --in-place (or --dry-run to preview).", file=sys.stderr)
        sys.exit(1)

    # Collect files
    if args.file:
        src_file = Path(args.file).resolve()
        if not src_file.is_file():
            print(f"ERROR: file not found: {src_file}", file=sys.stderr)
            sys.exit(1)
        json_files = [src_file]
    else:
        exports_dir = Path(args.exports_dir).resolve()
        if not exports_dir.is_dir():
            print(f"ERROR: exports-dir not found: {exports_dir}", file=sys.stderr)
            sys.exit(1)
        json_files = _find_json_files(exports_dir)
        if not json_files:
            print(f"No JSON files found in: {exports_dir}", file=sys.stderr)
            sys.exit(1)

    out_dir: Optional[Path] = None
    if args.out_dir:
        out_dir = Path(args.out_dir).resolve()
        if not args.dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[compress] {len(json_files)} JSON file(s) to process")
    if args.dry_run:
        print("[compress] DRY RUN — no files will be written.")
    elif args.in_place:
        backup_note = " (no .bak)" if args.no_backup else " (with .bak backup)"
        print(f"[compress] IN-PLACE mode{backup_note}")
    else:
        print(f"[compress] Output directory: {out_dir}")
    print()

    total_processed = 0
    total_skipped = 0
    total_src_bytes = 0
    total_dst_bytes = 0

    for src in json_files:
        dst = (out_dir / src.name) if out_dir else None
        processed, src_bytes, dst_bytes = _compress_file(
            src, dst,
            dry_run=args.dry_run,
            in_place=args.in_place,
            no_backup=args.no_backup,
            force=args.force,
        )
        if processed:
            total_processed += 1
            total_src_bytes += src_bytes
            total_dst_bytes += dst_bytes
        else:
            total_skipped += 1
            total_src_bytes += src_bytes
            total_dst_bytes += src_bytes  # unchanged

    total_pct = (1 - total_dst_bytes / total_src_bytes) * 100 if total_src_bytes else 0

    print()
    print("=" * 60)
    print(f"  Files processed:  {total_processed}")
    print(f"  Files skipped:    {total_skipped}")
    print(f"  Total input:      {_fmt_kb(total_src_bytes)}")
    print(f"  Total output:     {_fmt_kb(total_dst_bytes)}")
    print(f"  Size reduction:   {total_pct:.1f}%")
    if args.dry_run:
        print("  (DRY RUN — nothing written)")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
