"""
tools/label_synthesis/build_identity_items_lookup.py

Pre-processing step for synthesize_fragmented_labels.py when export JSONs
do not contain inline identity_items (i.e. the "flattened" export format).

Reads:
    results/records/records.csv          -- join_hash per (export_run_id, domain, record_pk)
    results/records/identity_items.csv   -- k/v/q per (export_run_id, domain, record_pk)

Writes:
    results/label_synthesis/identity_items_by_joinhash.csv

Output schema:
    domain, join_hash, k, v, q

Strategy:
  - For each (domain, join_hash), pick ONE representative record_pk
    (the first one encountered, stable via sort order)
  - Emit all identity_items for that record_pk
  - Downstream: synthesize_fragmented_labels loads this once and uses it
    instead of scanning JSON files

Usage:
    python tools/label_synthesis/build_identity_items_lookup.py \
        --records-dir "C:\\path\\to\\results\\records" \
        --out-dir    "C:\\path\\to\\results\\label_synthesis"

Notes:
  - Handles both column name variants:
      identity_items.csv:  item_key / item_value  (phase0 schema)
      identity_items.csv:  k / v / q              (flat schema)
  - Falls back to phase0_records.csv / phase0_identity_items.csv if the
    canonical names are not found.
  - join_hash column sourced from records.csv; skips rows where it is blank.
  - Prefers streaming identity_items_by_domain/*.csv shards (gated on the
    .complete sentinel, same convention as apply_join_policy.py) over the
    monolithic identity_items.csv. Shards are read in sorted glob order,
    the same order extractor.py concatenates them into the monolithic file,
    so total_item_rows/kept_item_rows and the (already-sorted) output are
    identical either way. Falls back to the monolithic file when the
    sentinel is absent or the shard directory doesn't exist.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _find_file(directory: Path, *candidates: str) -> Optional[Path]:
    for name in candidates:
        p = directory / name
        if p.is_file():
            return p
    return None


def _sniff_item_columns(header: List[str]) -> Tuple[str, str, Optional[str]]:
    """
    Return (key_col, value_col, quality_col) from a header row.
    Supports both schemas:
      - phase0: item_key, item_value  (no q column)
      - flat:   k, v, q
    """
    h = [c.strip().lower() for c in header]
    if "item_key" in h and "item_value" in h:
        key_col = header[h.index("item_key")]
        val_col = header[h.index("item_value")]
        q_col = None
        # q may still be present in some versions
        if "item_quality" in h:
            q_col = header[h.index("item_quality")]
        elif "q" in h:
            q_col = header[h.index("q")]
        return key_col, val_col, q_col
    if "k" in h and "v" in h:
        key_col = header[h.index("k")]
        val_col = header[h.index("v")]
        q_col = header[h.index("q")] if "q" in h else None
        return key_col, val_col, q_col
    raise ValueError(
        f"Cannot identify key/value columns in identity_items CSV. "
        f"Header: {header}"
    )


def build_lookup(records_dir: Path, out_dir: Path) -> Path:
    # ------------------------------------------------------------------ #
    # 1. Find input files                                                  #
    # ------------------------------------------------------------------ #
    records_csv = _find_file(records_dir, "records.csv", "phase0_records.csv")
    if records_csv is None:
        sys.exit(
            f"[ERROR] Could not find records.csv or phase0_records.csv in: {records_dir}"
        )

    # Prefer per-domain shards when a complete set is present (same gating
    # convention as apply_join_policy.py / compute_latent_purgeable.py). A
    # partial/interrupted flatten run leaves shard CSVs without the sentinel;
    # treating those as authoritative could silently drop domain items, so
    # fall back to the monolithic file in that case.
    shard_dir = records_dir / "identity_items_by_domain"
    _use_shards = (shard_dir / ".complete").is_file()
    if not _use_shards and shard_dir.is_dir() and any(shard_dir.glob("*.csv")):
        print(
            "[WARN build_identity_items_lookup] identity_items_by_domain/ contains CSV "
            "files but .complete sentinel is absent -- possible interrupted flatten run. "
            "Falling back to monolithic identity_items.csv to avoid silently missing "
            "domain items.",
            file=sys.stderr,
        )

    items_csv: Optional[Path] = None
    if not _use_shards:
        items_csv = _find_file(
            records_dir,
            "identity_items.csv",
            "phase0_identity_items.csv",
        )
        if items_csv is None:
            sys.exit(
                f"[ERROR] No complete shard set ({shard_dir / '.complete'} absent) and no "
                f"identity_items.csv or phase0_identity_items.csv in: {records_dir}"
            )

    print(f"[build_identity_items_lookup]")
    print(f"  records:        {records_csv}")
    if _use_shards:
        print(f"  identity_items: {shard_dir} (shard-based)")
    else:
        print(f"  identity_items: {items_csv}")

    # ------------------------------------------------------------------ #
    # 2. Build (export_run_id, domain, record_pk) → join_hash             #
    # ------------------------------------------------------------------ #
    # Also pick the FIRST (export_run_id, record_pk) per (domain, join_hash)
    # to use as the representative record.

    # pk_to_jh: (export_run_id, domain, record_pk) -> join_hash
    pk_to_jh: Dict[Tuple[str, str, str], str] = {}

    # representative: (domain, join_hash) -> (export_run_id, record_pk)
    representative: Dict[Tuple[str, str], Tuple[str, str]] = {}

    skipped_no_jh = 0
    total_records = 0

    print(f"  Reading records.csv ...", flush=True)
    with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_records += 1
            domain = (row.get("domain") or "").strip()
            export_run_id = (row.get("export_run_id") or "").strip()
            record_pk = (row.get("record_pk") or "").strip()
            join_hash = (row.get("join_hash") or "").strip()

            if not join_hash or not domain or not export_run_id or not record_pk:
                skipped_no_jh += 1
                continue

            pk_key = (export_run_id, domain, record_pk)
            pk_to_jh[pk_key] = join_hash

            rep_key = (domain, join_hash)
            if rep_key not in representative:
                representative[rep_key] = (export_run_id, record_pk)

    print(
        f"  Records read: {total_records:,} | "
        f"skipped (no join_hash): {skipped_no_jh:,} | "
        f"unique (domain, join_hash) pairs: {len(representative):,}",
        flush=True,
    )

    # Build reverse: (export_run_id, domain, record_pk) -> True for representative rows
    rep_pk_set: set = set()
    for (domain, join_hash), (export_run_id, record_pk) in representative.items():
        rep_pk_set.add((export_run_id, domain, record_pk))

    # ------------------------------------------------------------------ #
    # 3. Stream identity items, keep only representative rows             #
    # ------------------------------------------------------------------ #
    # Output: list of (domain, join_hash, k, v, q)
    out_rows: List[Dict] = []

    total_item_rows = 0
    kept_item_rows = 0
    _sniffed: Optional[Tuple[str, str, Optional[str]]] = None

    def _process_item_rows(rows_iter, fieldnames: List[str], source_name: str) -> None:
        nonlocal total_item_rows, kept_item_rows, _sniffed
        cols = _sniff_item_columns(fieldnames)
        if _sniffed is None:
            _sniffed = cols
        elif cols != _sniffed:
            # Schema is expected to be uniform across all shards written by a
            # single flatten run (extractor.py uses one fixed field list for
            # every shard). Flag it loudly rather than silently mis-reading
            # a shard if that ever stops being true.
            raise ValueError(
                f"Inconsistent identity_items schema: {source_name} sniffed as {cols}, "
                f"expected {_sniffed} (from an earlier shard). Refusing to guess."
            )
        key_col, val_col, q_col = cols

        for row in rows_iter:
            total_item_rows += 1
            export_run_id = (row.get("export_run_id") or "").strip()
            domain = (row.get("domain") or "").strip()
            record_pk = (row.get("record_pk") or "").strip()

            pk_key = (export_run_id, domain, record_pk)
            if pk_key not in rep_pk_set:
                continue

            join_hash = pk_to_jh.get(pk_key, "")
            if not join_hash:
                continue

            k = (row.get(key_col) or "").strip()
            v = row.get(val_col)
            if v is not None:
                v = v.strip()
            q = (row.get(q_col) or "ok").strip() if q_col else "ok"

            if not k:
                continue

            out_rows.append({
                "domain": domain,
                "join_hash": join_hash,
                "k": k,
                "v": v if v else "",
                "q": q,
            })
            kept_item_rows += 1

    if _use_shards:
        shard_files = sorted(shard_dir.glob("*.csv"))
        print(
            f"  Reading identity_items_by_domain/*.csv ({len(shard_files)} shards) ...",
            flush=True,
        )
        for shard_path in shard_files:
            with shard_path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                _process_item_rows(reader, reader.fieldnames or [], shard_path.name)
    else:
        print(f"  Reading identity_items.csv ...", flush=True)
        with items_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            _process_item_rows(reader, reader.fieldnames or [], items_csv.name)

    print(
        f"  Item rows read: {total_item_rows:,} | "
        f"kept (representative only): {kept_item_rows:,}",
        flush=True,
    )

    # ------------------------------------------------------------------ #
    # 4. Write output                                                      #
    # ------------------------------------------------------------------ #
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "identity_items_by_joinhash.csv"

    out_rows.sort(key=lambda r: (r["domain"], r["join_hash"], r["k"]))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["domain", "join_hash", "k", "v", "q"],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    # Summary by domain
    by_domain: Dict[str, set] = defaultdict(set)
    for r in out_rows:
        by_domain[r["domain"]].add(r["join_hash"])

    print(f"\n  Output: {out_path}")
    print(f"  {'Domain':<45} {'Patterns':>8}  {'Items':>8}")
    print(f"  {'-'*45} {'-'*8}  {'-'*8}")
    domain_counts: Dict[str, int] = defaultdict(int)
    for r in out_rows:
        domain_counts[r["domain"]] += 1
    for domain in sorted(by_domain):
        print(
            f"  {domain:<45} {len(by_domain[domain]):>8,}  "
            f"{domain_counts[domain]:>8,}"
        )
    print(f"\n  Total output rows: {len(out_rows):,}")
    print(f"  Done.", flush=True)

    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Build identity_items_by_joinhash.csv from flat CSVs. "
            "Required pre-step for synthesize_fragmented_labels when export "
            "JSONs do not contain inline identity_items."
        )
    )
    ap.add_argument(
        "--records-dir",
        required=True,
        help="Directory containing records.csv and identity_items.csv "
             "(e.g. results/records/)",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Directory to write identity_items_by_joinhash.csv "
             "(e.g. results/label_synthesis/)",
    )
    args = ap.parse_args()

    build_lookup(
        records_dir=Path(args.records_dir).resolve(),
        out_dir=Path(args.out_dir).resolve(),
    )


if __name__ == "__main__":
    main()
