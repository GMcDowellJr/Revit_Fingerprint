"""
Reset wall_type records that are blocked solely because wt.function=unsupported.not_applicable
so that the apply stage can re-evaluate them.

Usage:
    python tools/reset_wall_types_for_reapply.py \
        --records path/to/records.csv \
        --items-dir path/to/identity_items_by_domain \
        [--dry-run]

After running this script, re-apply sig_hash for wall_types only:
    python tools/run_extract_all.py ... --stages sig_hash --domains wall_types
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

DOMAIN = "wall_types"
BAD_KEY = "wt.function"
BAD_Q = "unsupported.not_applicable"
REQUIRED_ITEMS = {"wt.layer_count", "wt.total_thickness_in", "wt.stack_hash_loose"}


def _read_wall_items(items_dir: Path) -> dict[str, dict[str, str]]:
    """Return {record_pk: {key: q}} for all wall_types items."""
    shard = items_dir / f"{DOMAIN}.csv"
    if not shard.exists():
        sys.exit(f"ERROR: shard not found: {shard}")

    by_pk: dict[str, dict[str, str]] = {}
    with shard.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Support both column-name conventions
        col_key = "item_key" if "item_key" in (reader.fieldnames or []) else "k"
        col_q = "item_value_type" if "item_value_type" in (reader.fieldnames or []) else "q"
        for row in reader:
            pk = row.get("record_pk", "").strip()
            k = row.get(col_key, "").strip()
            q = row.get(col_q, "").strip()
            if not pk or not k:
                continue
            by_pk.setdefault(pk, {})[k] = q
    return by_pk


def _is_function_only_block(items: dict[str, str]) -> bool:
    """True if wt.function is the only non-ok required item and compound structure items are ok."""
    fn_q = items.get(BAD_KEY, "")
    if fn_q != BAD_Q:
        return False
    return all(items.get(k, "") == "ok" for k in REQUIRED_ITEMS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset wall_type records blocked only by wt.function.")
    parser.add_argument("--records", required=True, help="Path to records.csv")
    parser.add_argument("--items-dir", required=True, help="Path to identity_items_by_domain/ directory")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change without writing")
    args = parser.parse_args()

    records_path = Path(args.records)
    items_dir = Path(args.items_dir)

    if not records_path.exists():
        sys.exit(f"ERROR: records file not found: {records_path}")

    print(f"Loading {DOMAIN} items from {items_dir / (DOMAIN + '.csv')} …")
    wall_items = _read_wall_items(items_dir)
    print(f"  loaded {len(wall_items)} unique record_pks")

    eligible_pks: set[str] = {
        pk for pk, items in wall_items.items() if _is_function_only_block(items)
    }
    print(f"  {len(eligible_pks)} records eligible for reset (wt.function=unsupported.not_applicable, compound structure ok)")

    if not eligible_pks:
        print("Nothing to reset.")
        return 0

    # Read all records, patch eligible wall_types rows
    with records_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    patched = 0
    for row in rows:
        if row.get("domain", "").strip() != DOMAIN:
            continue
        pk = row.get("record_pk", "").strip()
        if pk not in eligible_pks:
            continue
        if row.get("status", "").strip() != "blocked":
            continue
        if args.dry_run:
            patched += 1
            continue
        row["status"] = ""
        row["status_reasons"] = ""
        row["sig_hash"] = ""
        patched += 1

    if args.dry_run:
        print(f"[dry-run] Would reset {patched} records in {records_path}")
        print("Re-run without --dry-run to apply.")
        return 0

    tmp_path = records_path.with_suffix(".csv.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, records_path)

    print(f"Reset {patched} records in {records_path}")
    print()
    print("Next step — re-run sig_hash apply for wall_types only:")
    print("  python tools/run_extract_all.py <your-args> --stages sig_hash --domains wall_types")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
