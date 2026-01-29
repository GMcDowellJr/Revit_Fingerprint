#!/usr/bin/env python3
"""
Pareto join-key search for Revit Fingerprint exports.

Inputs:
  - records.csv: one row per record, includes sig_hash_no_uid
  - identity_items.csv: many rows per record, includes k and v

We compute:
  record_key = file_id|domain|record_id
  v_norm: UID-like normalization (strip suffix after last '-' for keys containing 'uid')

For any subset of candidate keys K:
  composite_key(record) = "|".join(sorted(f"{k}={v_norm}" for k in K if present/nonblank))
Then compute multi-objective metrics vs sig_hash_no_uid.

Outputs:
  - all_results.csv (optional, can be large)
  - pareto_front.csv (non-dominated solutions)
"""

from __future__ import annotations

import argparse
import itertools
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd


# -------------------------
# Normalization + utilities
# -------------------------

def make_record_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["file_id"].astype(str)
        + "|"
        + df["domain"].astype(str)
        + "|"
        + df["record_id"].astype(str)
    )


def compute_v_norm(identity_items: pd.DataFrame) -> pd.Series:
    """
    Match your Power Query logic in spirit:
      - UID-like keys: k contains "uid" (case-insensitive)
      - If UID-like and value contains '-', strip suffix after the last '-'
        (keeps prefix before last dash; excludes dash and suffix)
      - Otherwise, pass through as string
      - Preserve nulls / blanks
    """
    k_lower = identity_items["k"].astype(str).str.lower()
    is_uid_like = k_lower.str.contains("uid", na=False)

    v = identity_items["v"]
    out: List[str | None] = []

    for val, is_uid in zip(v.tolist(), is_uid_like.tolist()):
        if pd.isna(val):
            out.append(None)
            continue

        s = str(val)
        if is_uid:
            idx = s.rfind("-")
            if idx >= 0:
                s = s[:idx]
        s = s.strip()
        if s == "":
            out.append(None)
        else:
            out.append(s)

    return pd.Series(out, index=identity_items.index)


def pareto_front(rows: List[dict], objective_cols: Sequence[str]) -> List[dict]:
    """
    Non-dominated filter.
    A dominates B if A is <= B on all objectives and < on at least one.
    """
    front: List[dict] = []
    for r in rows:
        dominated = False
        to_remove = []
        for f in front:
            if dominates(f, r, objective_cols):
                dominated = True
                break
            if dominates(r, f, objective_cols):
                to_remove.append(f)
        if dominated:
            continue
        for f in to_remove:
            front.remove(f)
        front.append(r)
    return front


def dominates(a: dict, b: dict, objective_cols: Sequence[str]) -> bool:
    le_all = True
    lt_any = False
    for c in objective_cols:
        av = a[c]
        bv = b[c]
        if av > bv:
            le_all = False
            break
        if av < bv:
            lt_any = True
    return le_all and lt_any


# -------------------------
# Core evaluation
# -------------------------

@dataclass(frozen=True)
class EvalConfig:
    max_subset_size: int
    include_uid_like: bool
    max_candidates: int | None  # optional cap after sorting


def build_wide_kv_table(records: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a dataframe indexed by record_key with:
      - sig_hash_no_uid
      - one column per k: the string "k=v_norm" (or NaN)
    If there are multiple rows per (record_key, k), we deterministically choose MIN(v_norm).
    """
    rec = records.copy()
    rec["record_key"] = make_record_key(rec)
    rec = rec[["record_key", "sig_hash_no_uid"]].drop_duplicates()

    it = items.copy()
    it["record_key"] = make_record_key(it)
    it["v_norm"] = compute_v_norm(it)

    it = it.dropna(subset=["v_norm"])
    it["v_norm"] = it["v_norm"].astype(str).str.strip()
    it = it[it["v_norm"] != ""]

    # choose one per (record_key, k) deterministically
    g = it.groupby(["record_key", "k"], as_index=False)["v_norm"].min()
    g["kv"] = g["k"].astype(str) + "=" + g["v_norm"].astype(str)

    wide = g.pivot(index="record_key", columns="k", values="kv")
    df = rec.set_index("record_key").join(wide, how="left")

    return df

def sample_records(records: pd.DataFrame, items: pd.DataFrame, n: int, seed: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if n <= 0:
        return records, items

    r = records.copy()
    r["record_key"] = make_record_key(r)
    unique_keys = r["record_key"].dropna().drop_duplicates()

    if unique_keys.empty:
        return records, items

    n_eff = min(int(n), int(unique_keys.shape[0]))
    sampled_keys = unique_keys.sample(n=n_eff, random_state=int(seed))

    r = r[r["record_key"].isin(sampled_keys)].drop(columns=["record_key"], errors="ignore")

    it = items.copy()
    it["record_key"] = make_record_key(it)
    it = it[it["record_key"].isin(sampled_keys)].drop(columns=["record_key"], errors="ignore")

    return r, it


def eval_subset(df: pd.DataFrame, keys: Tuple[str, ...]) -> dict:
    """
    Compute composite key, then multi-objective metrics.
    """
    # composite key: join non-null kv strings for selected keys, sorted by k
    sub = df[list(keys)].copy() if len(keys) > 0 else pd.DataFrame(index=df.index)

    if len(keys) == 0:
        ck = pd.Series([""] * len(df), index=df.index)
    else:
        # Ensure consistent column order (keys already sorted in caller)
        def row_join(row: pd.Series) -> str:
            vals = [v for v in row.tolist() if isinstance(v, str) and v.strip() != ""]
            # already "k=v", so sorting by k is equivalent to sorting by string
            vals.sort()
            return "|".join(vals)

        ck = sub.apply(row_join, axis=1)

    tmp = pd.DataFrame(
        {
            "sig": df["sig_hash_no_uid"].astype(str),
            "ck": ck.astype(str),
        },
        index=df.index,
    )

    # Collision: per ck, how many distinct sigs?
    sigcnt_by_ck = tmp.groupby("ck")["sig"].nunique()
    max_sigcnt = int(sigcnt_by_ck.max()) if len(sigcnt_by_ck) else 0

    colliding_cks = sigcnt_by_ck[sigcnt_by_ck > 1]
    collision_groups = int(colliding_cks.shape[0])

    # collision records: records whose ck is colliding
    collision_records = int(tmp["ck"].isin(colliding_cks.index).sum())

    # Fragmentation: per sig, how many distinct cks?
    ckcnt_by_sig = tmp.groupby("sig")["ck"].nunique()
    fragmented_sigs = ckcnt_by_sig[ckcnt_by_sig > 1]
    fragmentation_groups = int(fragmented_sigs.shape[0])
    fragmentation_records = int(tmp["sig"].isin(fragmented_sigs.index).sum())

    return {
        "keys": "|".join(keys),
        "k_count": len(keys),
        "max_sigcnt": max_sigcnt,
        "collision_groups": collision_groups,
        "collision_records": collision_records,
        "fragmentation_groups": fragmentation_groups,
        "fragmentation_records": fragmentation_records,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", required=True, type=Path)
    ap.add_argument("--items", required=True, type=Path)
    ap.add_argument("--max_k", default=4, type=int, help="max subset size to search")
    ap.add_argument("--candidates", nargs="*", default=None, help="explicit candidate k list; default = all ks found")
    ap.add_argument("--exclude_uid_like", action="store_true", help="exclude keys containing 'uid'")
    ap.add_argument("--limit_candidates", default=None, type=int, help="optional cap on candidate count (after filtering)")
    ap.add_argument("--out_dir", default=Path("."), type=Path)
    ap.add_argument("--write_all", action="store_true", help="write all_results.csv (can be large)")
    ap.add_argument(
        "--pareto_name",
        default="pareto_front.csv",
        help="output filename for Pareto front CSV (default: pareto_front.csv)",
    )

    ap.add_argument("--seed", default=1337, type=int, help="random seed for sampling")
    ap.add_argument(
        "--sample_records",
        default=None,
        type=int,
        help="optional: randomly sample N unique records (record_key) before analysis",
    )
    ap.add_argument(
        "--sample_candidates",
        default=None,
        type=int,
        help="optional: randomly sample N candidate keys after filtering (useful for large domains)",
    )

    args = ap.parse_args()

    records = pd.read_csv(args.records)
    items = pd.read_csv(args.items)

    # optional record sampling (must happen before pivot/wide build)
    if args.sample_records is not None and args.sample_records > 0:
        records, items = sample_records(records, items, int(args.sample_records), int(args.seed))

    # auto-candidates from identity_items.k (not from records)
    items_ks = (
        items["k"].dropna().astype(str).str.strip()
        if "k" in items.columns
        else pd.Series([], dtype=str)
    )
    auto_candidates = sorted(set([k for k in items_ks.tolist() if k != ""]))

    # candidate keys: explicit list wins, otherwise auto
    if args.candidates and len(args.candidates) > 0:
        candidates = [str(k).strip() for k in args.candidates if str(k).strip() != ""]
    else:
        candidates = auto_candidates[:]

    if args.exclude_uid_like:
        candidates = [k for k in candidates if "uid" not in str(k).lower()]

    candidates = sorted(set(candidates))

    # optional random downselect of candidates (after filtering)
    if args.sample_candidates is not None and args.sample_candidates > 0 and len(candidates) > args.sample_candidates:
        candidates = (
            pd.Series(candidates)
            .sample(n=int(args.sample_candidates), random_state=int(args.seed))
            .tolist()
        )
        candidates = sorted(set(candidates))

    # optional deterministic cap (after filtering / sampling)
    if args.limit_candidates is not None and args.limit_candidates > 0:
        candidates = candidates[: args.limit_candidates]

    if len(candidates) == 0:
        raise SystemExit("No candidates after filtering. Provide --candidates or remove filters.")

    # build wide table AFTER candidate selection and sampling
    df = build_wide_kv_table(records, items)

    # ensure candidates exist in wide columns (some ks may be absent post-sample)
    all_ks = [c for c in df.columns if c != "sig_hash_no_uid"]
    candidates = [k for k in candidates if k in all_ks]

    if len(candidates) == 0:
        raise SystemExit("No candidates present in sampled data. Increase --sample_records or adjust candidate filters.")

    if args.exclude_uid_like:
        candidates = [k for k in candidates if "uid" not in str(k).lower()]

    candidates = sorted(set(candidates))

    if args.limit_candidates is not None and args.limit_candidates > 0:
        candidates = candidates[: args.limit_candidates]

    if len(candidates) == 0:
        raise SystemExit("No candidates after filtering. Provide --candidates or remove filters.")

    # search (stream subsets; do not materialize into a list)
    max_k = max(0, int(args.max_k))

    def iter_subsets(cands: List[str], mk: int) -> Iterable[Tuple[str, ...]]:
        for ksize in range(1, mk + 1):
            yield from itertools.combinations(cands, ksize)

    results: List[dict] = []
    for keys in iter_subsets(candidates, max_k):
        results.append(eval_subset(df, keys))

    # Pareto objectives (minimize)
    objective_cols = ["max_sigcnt", "collision_records", "collision_groups", "k_count"]
    front = pareto_front(results, objective_cols)

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    pareto_df = pd.DataFrame(front).sort_values(objective_cols).reset_index(drop=True)
    pareto_path = out_dir / args.pareto_name
    pareto_df.to_csv(pareto_path, index=False)

    print(f"Wrote Pareto front: {pareto_path} ({len(pareto_df)} rows)")

    if args.write_all:
        all_df = pd.DataFrame(results).sort_values(objective_cols).reset_index(drop=True)
        all_path = out_dir / "all_results.csv"
        all_df.to_csv(all_path, index=False)
        print(f"Wrote all results: {all_path} ({len(all_df)} rows)")

    # quick summary to stdout
    best = pareto_df.head(10)
    print("\nTop Pareto candidates (first 10):")
    print(best.to_string(index=False))


if __name__ == "__main__":
    main()
