#!/usr/bin/env python3
"""
Create per-shape input CSVs for Pareto join-key search (Option B).

Reads:
  - records.csv (one row per record)
  - identity_items.csv (multiple rows per record; includes k and v)

Writes (per shape_norm value, derived from k == "dim_type.shape"):
  - records__shape=<S>.csv
  - identity_items__shape=<S>.csv

Notes:
  - record_key is computed as file_id|domain|record_id (same as your Power BI model).
  - shape_norm here is derived from the raw identity_items value for dim_type.shape
    (trimmed string). This is sufficient for per-shape filtering.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Set

import pandas as pd


SHAPE_K = "dim_type.shape"


def add_record_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["file_id"].astype(str)
        + "|"
        + df["domain"].astype(str)
        + "|"
        + df["record_id"].astype(str)
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", required=True, type=Path)
    ap.add_argument("--items", required=True, type=Path)
    ap.add_argument("--out_dir", required=True, type=Path)
    ap.add_argument("--domain", default=None, help="optional: filter to a single domain")
    ap.add_argument("--shape_k", default=SHAPE_K, help="k to use for shape (default dim_type.shape)")
    args = ap.parse_args()

    records = pd.read_csv(args.records)
    items = pd.read_csv(args.items)

    # Optional domain filter (recommended if your CSVs include multiple domains)
    if args.domain:
        records = records[records["domain"].astype(str) == str(args.domain)]
        items = items[items["domain"].astype(str) == str(args.domain)]

    records = records.copy()
    items = items.copy()

    records["record_key"] = add_record_key(records)
    items["record_key"] = add_record_key(items)

    # Build shape map: record_key -> shape_norm
    shape_rows = items[items["k"].astype(str) == str(args.shape_k)].copy()

    # Prefer v_norm if present; else fallback to v
    if "v_norm" in shape_rows.columns:
        shape_val = shape_rows["v_norm"]
    else:
        shape_val = shape_rows["v"]

    shape_rows["shape_norm"] = shape_val.astype(str).str.strip()
    shape_rows = shape_rows[shape_rows["shape_norm"] != ""]

    # Deterministic single value per record_key (if duplicates exist, take MIN string)
    shape_map = shape_rows.groupby("record_key")["shape_norm"].min()

    records["shape_norm"] = records["record_key"].map(shape_map)

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    shapes = sorted(records["shape_norm"].dropna().unique().tolist())

    if not shapes:
        raise SystemExit(
            f"No shapes found using k={args.shape_k}. "
            f"Check that identity_items contains that k for the filtered domain."
        )

    # Write per-shape files
    for s in shapes:
        rec_s = records[records["shape_norm"] == s].copy()
        rk_set: Set[str] = set(rec_s["record_key"].tolist())
        it_s = items[items["record_key"].isin(rk_set)].copy()

        # Drop helper columns for clean downstream use
        rec_out = rec_s.drop(columns=["record_key", "shape_norm"], errors="ignore")
        it_out = it_s.drop(columns=["record_key"], errors="ignore")

        safe_s = str(s).replace("/", "_").replace("\\", "_").replace(":", "_")
        rec_path = out_dir / f"records__shape={safe_s}.csv"
        it_path = out_dir / f"identity_items__shape={safe_s}.csv"

        rec_out.to_csv(rec_path, index=False)
        it_out.to_csv(it_path, index=False)

    print(f"Wrote {len(shapes)} shapes to: {out_dir}")


if __name__ == "__main__":
    main()
