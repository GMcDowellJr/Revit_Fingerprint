#!/usr/bin/env python3

"""
Pareto join-key search for Revit Fingerprint exports.

Inputs:
  - records.csv: one row per record, includes sig_hash
  - identity_items.csv: many rows per record, includes k and v

We compute:
  record_key = file_id|domain|record_id

For any subset of candidate keys K:
  composite_key(record) = "|".join(sorted(f"{k}={v}" for k in K if present/nonblank))

Then compute multi-objective metrics vs sig_hash.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import itertools
import math
import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd

try:
    from tools.join_key_discovery.eval import score_candidate
except ModuleNotFoundError:
    from join_key_discovery.eval import score_candidate


def pareto_search(
    domain_records: Sequence[Dict[str, str]],
    domain_identity_items: Dict[str, Dict[str, Tuple[str, str]]],
    candidate_fields: Sequence[str],
    cfg: dict | None = None,
) -> Dict[str, object]:
    """Callable API for v2.1 discovery orchestration."""
    cfg = cfg or {}
    max_k = int(cfg.get("max_k", 4))
    fields = sorted({str(f).strip() for f in candidate_fields if str(f).strip()}, key=lambda s: s.lower())
    rows: List[dict] = []
    for k in range(1, min(max_k, len(fields)) + 1):
        for subset in itertools.combinations(fields, k):
            metrics = score_candidate(domain_records, domain_identity_items, list(subset), cfg)
            rows.append({
                "keys": "|".join(subset),
                "k_count": k,
                "collision_rate": float(metrics.get("collision_rate", 1.0)),
                "coverage_gap": 1.0 - float(metrics.get("coverage", 0.0)),
                "fragmentation_rate": float(metrics.get("fragmentation_rate", 1.0)),
                "metrics": metrics,
            })
    if not rows:
        return {"frontier": [], "chosen": None}
    front = pareto_front(rows, ["coverage_gap", "collision_rate", "fragmentation_rate", "k_count"])
    front = sorted(front, key=lambda r: (r["collision_rate"], r["coverage_gap"], r["k_count"], r["keys"]))
    return {"frontier": front, "chosen": front[0]}


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_join_key_policy(policy_json: Path, domain: str) -> tuple[list[str], list[str], list[str], dict | None]:
    """
    Return (required_items, optional_items, explicitly_excluded_items) for a domain.
    Expects domain_join_key_policies.json shape:
      { "domains": { "<domain>": { "required_items": [...], "optional_items": [...], "explicitly_excluded_items": [...] } } }
    """
    with policy_json.open("r", encoding="utf-8") as f:
        d = json.load(f)
    doms = d.get("domains") if isinstance(d, dict) else None
    dom = doms.get(domain) if isinstance(doms, dict) else None
    if not isinstance(dom, dict):
        raise SystemExit(f"Domain not found in policy_json: {domain}")

    req = dom.get("required_items") if isinstance(dom.get("required_items"), list) else []
    opt = dom.get("optional_items") if isinstance(dom.get("optional_items"), list) else []
    exc = dom.get("explicitly_excluded_items") if isinstance(dom.get("explicitly_excluded_items"), list) else []

    req = [str(x).strip() for x in req if str(x).strip()]
    opt = [str(x).strip() for x in opt if str(x).strip()]
    exc = [str(x).strip() for x in exc if str(x).strip()]

    req = _dedupe_preserve_order(req)
    opt = _dedupe_preserve_order(opt)
    exc = _dedupe_preserve_order(exc)

    shape_gating = dom.get("shape_gating") if isinstance(dom.get("shape_gating"), dict) else None

    return req, opt, exc, shape_gating


def _rank_challengers_from_wide(df: pd.DataFrame, keys: list[str]) -> list[str]:
    """
    Deterministically rank keys by a simple usefulness score:
      score = coverage * log(1 + distinct_count)
    where coverage is fraction of records with a non-null kv string for that key.
    """
    if df.empty or not keys:
        return []

    n = float(len(df.index))
    out: list[tuple[float, str]] = []
    for k in keys:
        if k not in df.columns:
            continue
        col = df[k]
        present = col.notna()
        cov = float(present.sum()) / n if n > 0 else 0.0
        if cov <= 0.0:
            continue
        distinct = int(col[present].nunique(dropna=True))
        score = cov * math.log(1.0 + float(distinct))
        out.append((score, k))

    out.sort(key=lambda t: (-t[0], t[1].lower()))
    return [k for _, k in out]

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
    Value normalization for join-key candidates.

    Contract: signature hashing is handled upstream in the exporter.
    This normalization is ONLY for candidate key values:
      - stringify
      - strip
      - treat blanks as null
    """
    v = identity_items["v"]
    out: List[str | None] = []

    for val in v.tolist():
        if pd.isna(val):
            out.append(None)
            continue
        s = str(val).strip()
        out.append(None if s == "" else s)

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
    rec = rec[["record_key", "sig_hash"]].drop_duplicates()

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
    # composite key: join non-null kv strings for selected keys, in policy order
    sub = df[list(keys)].copy() if len(keys) > 0 else pd.DataFrame(index=df.index)

    if len(keys) == 0:
        ck = pd.Series([""] * len(df), index=df.index)
    else:
        # Ensure consistent column order (keys already sorted in caller)
        def row_join(row: pd.Series) -> str:
            vals = [v for v in row.tolist() if isinstance(v, str) and v.strip() != ""]
            return "|".join(vals)

        ck = sub.apply(row_join, axis=1)

    tmp = pd.DataFrame(
        {
            "sig": df["sig_hash"].astype(str),
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
    ap.add_argument("--domain", default=None, help="optional: filter to a single domain (recommended for combined flat tables)")
    ap.add_argument(
        "--policy_json",
        default=None,
        type=Path,
        help="optional: domain_join_key_policies.json path (enables validate/harsh modes)",
    )
    ap.add_argument(
        "--mode",
        default="discover",
        choices=["discover", "validate", "harsh"],
        help="discover=unconstrained search; validate=policy-respecting; harsh=policy-seeded but allows omitting required",
    )
    ap.add_argument(
        "--challenger_top_n",
        default=0,
        type=int,
        help="optional: add top-N deterministic challenger keys outside policy (ranked by coverage*distinctness)",
    )
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
    ap.add_argument(
        "--shape_mode",
        default="off",
        choices=["off", "per_shape"],
        help="shape-gating mode: off (default) or per_shape evaluation",
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
    # Optional domain filter (recommended if your CSVs include multiple domains)
    if args.domain:
        records = records[records["domain"].astype(str) == str(args.domain)]
        items = items[items["domain"].astype(str) == str(args.domain)]

    # optional record sampling (must happen before pivot/wide build)
    if args.sample_records is not None and args.sample_records > 0:
        records, items = sample_records(records, items, int(args.sample_records), int(args.seed))

    # build wide table early so we can rank challengers deterministically (and validate keys exist)
    df = build_wide_kv_table(records, items)

    # Candidate selection
    policy_required: list[str] = []
    policy_optional: list[str] = []
    policy_excluded: list[str] = []
    policy_shape_gating: dict | None = None

    if args.policy_json is not None:
        if not args.domain:
            raise SystemExit("--policy_json requires --domain (policy is domain-scoped).")
        (
            policy_required,
            policy_optional,
            policy_excluded,
            policy_shape_gating,
        ) = _load_join_key_policy(Path(args.policy_json), str(args.domain))

    # Auto-candidates from identity_items.k (not from records)
    items_ks = (
        items["k"].dropna().astype(str).str.strip()
        if "k" in items.columns
        else pd.Series([], dtype=str)
    )
    auto_candidates = _dedupe_preserve_order([k for k in items_ks.tolist() if k != ""])

    # In validate/harsh mode, candidates are policy-seeded (plus optional challengers).
    if str(args.mode) in ("validate", "harsh"):
        candidates = _dedupe_preserve_order(policy_required + policy_optional)
    else:
        # discover mode: explicit list wins, otherwise auto
        if args.candidates and len(args.candidates) > 0:
            candidates = [str(k).strip() for k in args.candidates if str(k).strip() != ""]
        else:
            candidates = auto_candidates[:]

    # Exclude UID-like keys if requested (all modes)
    if args.exclude_uid_like:
        candidates = [k for k in candidates if "uid" not in str(k).lower()]

    # Exclude policy-explicit exclusions (validate/harsh)
    if str(args.mode) in ("validate", "harsh") and policy_excluded:
        excl = {k.lower() for k in policy_excluded}
        candidates = [k for k in candidates if str(k).lower() not in excl]

    candidates = _dedupe_preserve_order(candidates)

    # Optional challengers (validate/harsh): deterministic, ranked from wide df columns
    if str(args.mode) in ("validate", "harsh") and int(args.challenger_top_n or 0) > 0:
        wide_keys = [c for c in df.columns if c != "sig_hash"]
        ranked = _rank_challengers_from_wide(df, wide_keys)

        # filter challengers: not already in candidates, not excluded, optionally UID-like already handled
        cand_set = {k.lower() for k in candidates}
        excl_set = {k.lower() for k in policy_excluded} if policy_excluded else set()

        challengers: list[str] = []
        for k in ranked:
            kl = str(k).lower()
            if kl in cand_set:
                continue
            if kl in excl_set:
                continue
            challengers.append(k)
            if len(challengers) >= int(args.challenger_top_n):
                break

        candidates = _dedupe_preserve_order(candidates + challengers)

    # Optional random downselect of candidates (discover mode only)
    if str(args.mode) == "discover":
        if args.sample_candidates is not None and args.sample_candidates > 0 and len(candidates) > args.sample_candidates:
            candidates = (
                pd.Series(candidates)
                .sample(n=int(args.sample_candidates), random_state=int(args.seed))
                .tolist()
            )
            candidates = _dedupe_preserve_order(candidates)

    # Optional deterministic cap (after filtering / sampling)
    if args.limit_candidates is not None and args.limit_candidates > 0:
        candidates = candidates[: args.limit_candidates]

    if len(candidates) == 0:
        raise SystemExit("No candidates after filtering. Provide --candidates or remove filters / adjust policy.")

    # ensure candidates exist in wide columns (some ks may be absent post-sample)
    all_ks = [c for c in df.columns if c != "sig_hash_no_uid"]
    candidates = [k for k in candidates if k in all_ks]

    if len(candidates) == 0:
        raise SystemExit("No candidates present in data. Check domain filter / exports / policy keys.")

    candidates = _dedupe_preserve_order(candidates)

    if args.limit_candidates is not None and args.limit_candidates > 0:
        candidates = candidates[: args.limit_candidates]

    if len(candidates) == 0:
        raise SystemExit("No candidates after filtering. Provide --candidates or remove filters.")

    # ensure candidates exist in wide columns (some ks may be absent post-sample)
    all_ks = [c for c in df.columns if c != "sig_hash_no_uid"]
    candidates = [k for k in candidates if k in all_ks]

    if len(candidates) == 0:
        raise SystemExit("No candidates present in sampled data. Increase --sample_records or adjust candidate filters.")

    if args.exclude_uid_like:
        candidates = [k for k in candidates if "uid" not in str(k).lower()]

    candidates = _dedupe_preserve_order(candidates)

    if args.limit_candidates is not None and args.limit_candidates > 0:
        candidates = candidates[: args.limit_candidates]

    if len(candidates) == 0:
        raise SystemExit("No candidates after filtering. Provide --candidates or remove filters.")

    # search (stream subsets; do not materialize into a list)
    max_k = max(0, int(args.max_k))

    # Validate-mode requires that all policy required keys exist as columns in the wide table.
    # If they don't, we cannot evaluate any policy-respecting subsets from Phase-0 identity_items.
    if str(args.mode) == "validate" and policy_required:
        missing_cols = [k for k in policy_required if k not in df.columns]
        if missing_cols:
            out_dir = args.out_dir
            out_dir.mkdir(parents=True, exist_ok=True)

            cols = [
                "keys",
                "k_count",
                "max_sigcnt",
                "collision_groups",
                "collision_records",
                "fragmentation_groups",
                "fragmentation_records",
            ]
            pareto_path = out_dir / args.pareto_name
            pd.DataFrame([], columns=cols).to_csv(pareto_path, index=False)

            # Also write a small validation summary to make the reason unmissable

            summary_name = f"validation_summary__{args.domain}__{args.mode}.csv"
            summary_path = out_dir / summary_name

            pd.DataFrame(
                [
                    {"kind": "validate_blocked_missing_required_columns", "value": "|".join(missing_cols)},
                    {"kind": "note", "value": "Required policy keys not present in Phase-0 identity_items; cannot validate."},
                ]
            ).to_csv(summary_path, index=False)

            print(
                "[WARN pareto] validate mode blocked: required policy key(s) not present in data columns: "
                + ", ".join(missing_cols)
            )
            print(f"Wrote empty Pareto front: {pareto_path}")
            print(f"Wrote validation summary: {summary_path}")
            return

    # Auto-bump max_k in validate mode so required policy keys can be satisfied.
    # (Without this, validate can evaluate zero subsets when len(required) > max_k.)
    if str(args.mode) == "validate" and policy_required:
        req_n = len(policy_required)
        if max_k < req_n:
            print(f"[WARN pareto] validate mode: max_k={max_k} < required_items={req_n}; auto-bumping max_k to {req_n}.")
            max_k = req_n

    required_set = set(policy_required) if str(args.mode) == "validate" else set()

    shape_disc = None
    if isinstance(policy_shape_gating, dict):
        disc = policy_shape_gating.get("discriminator_key")
        if isinstance(disc, str) and disc:
            shape_disc = disc

    def iter_subsets(cands: List[str], mk: int) -> Iterable[Tuple[str, ...]]:
        for ksize in range(1, mk + 1):
            yield from itertools.combinations(cands, ksize)

    def iter_subsets_policy_respecting(cands: List[str], mk: int, required: set[str]) -> Iterable[Tuple[str, ...]]:
        """
        Only yield subsets that include all required keys.
        """
        if not required:
            yield from iter_subsets(cands, mk)
            return

        # If required alone exceeds max_k, nothing is possible.
        if len(required) > mk:
            return

        req_sorted = tuple(_dedupe_preserve_order(list(required)))
        remaining = [k for k in cands if k not in required]
        # Choose extra keys (0..mk-len(required))
        for extra_size in range(0, (mk - len(required)) + 1):
            for extra in itertools.combinations(remaining, extra_size):
                keys = tuple(req_sorted + tuple(extra))
                yield keys

    # Always evaluate the policy key itself (if available) in validate/harsh mode
    policy_key_tuple: Tuple[str, ...] = tuple()
    policy_eval: dict | None = None
    if args.policy_json is not None and (policy_required or policy_optional):
        policy_key_tuple = tuple(_dedupe_preserve_order(policy_required + policy_optional))
        # If policy key exists in data columns, evaluate even if it exceeds max_k (still useful)
        policy_in_cols = all(k in df.columns for k in policy_key_tuple)
        if policy_in_cols and len(policy_key_tuple) > 0:
            policy_eval = eval_subset(df, policy_key_tuple)

    def _run_search(
        *,
        df_slice: pd.DataFrame,
        candidates_slice: List[str],
        max_k_slice: int,
        required_slice: set[str],
        policy_key: Tuple[str, ...],
        disc_key: str | None,
    ) -> tuple[List[dict], int]:
        # Main search
        if str(args.mode) == "validate":
            key_iter = iter_subsets_policy_respecting(candidates_slice, max_k_slice, required_slice)
        else:
            key_iter = iter_subsets(candidates_slice, max_k_slice)

        local_results: List[dict] = []
        skipped_missing = 0
        for keys in key_iter:
            if policy_key and keys == policy_key:
                continue
            if disc_key and disc_key not in keys:
                skipped_missing += 1
                continue
            local_results.append(eval_subset(df_slice, keys))
        return local_results, skipped_missing

    results = []
    skipped_shape_missing = 0
    per_shape_results: dict[str, List[dict]] = {}

    if str(args.shape_mode) == "per_shape" and shape_disc and shape_disc in df.columns:
        shape_series = df[shape_disc]
        shape_values = _dedupe_preserve_order(
            [_shape_label(v) for v in shape_series.dropna().tolist()] + ["unknown"]
        )
        for shape_label in shape_values:
            if shape_label == "unknown":
                mask = shape_series.isna() | (shape_series.astype(str).str.strip() == "")
            else:
                mask = shape_series.astype(str).apply(_shape_label) == shape_label
            df_slice = df[mask]
            if df_slice.empty:
                per_shape_results[shape_label] = []
                continue
            local_results, skipped = _run_search(
                df_slice=df_slice,
                candidates_slice=candidates,
                max_k_slice=max_k,
                required_slice=required_set,
                policy_key=policy_key_tuple,
                disc_key=shape_disc,
            )
            per_shape_results[shape_label] = local_results
            skipped_shape_missing += skipped
            results.extend(local_results)
    else:
        results, skipped_shape_missing = _run_search(
            df_slice=df,
            candidates_slice=candidates,
            max_k_slice=max_k,
            required_slice=required_set,
            policy_key=policy_key_tuple,
            disc_key=shape_disc,
        )

    if policy_eval is not None:
        results.append(policy_eval)

    # Pareto objectives (minimize)
    objective_cols = ["max_sigcnt", "collision_records", "collision_groups", "k_count"]
    front = pareto_front(results, objective_cols)

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not front:
        # No evaluated subsets (or nothing survived to the Pareto filter). This can happen
        # after domain filtering + sampling if no usable candidate columns exist in the data slice.
        out_dir = args.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        cols = [
            "keys",
            "k_count",
            "max_sigcnt",
            "collision_groups",
            "collision_records",
            "fragmentation_groups",
            "fragmentation_records",
        ]
        pareto_path = out_dir / args.pareto_name
        pd.DataFrame([], columns=cols).to_csv(pareto_path, index=False)

        print(
            "No Pareto candidates evaluated (front is empty). "
            "Try increasing --sample_records, removing --sample_records, "
            "or reducing policy strictness / challenger_top_n for this domain slice."
        )
        if skipped_shape_missing:
            print(
                f"[INFO pareto] skipped {skipped_shape_missing} candidate subsets missing discriminator '{shape_disc}'."
            )
        print(f"Wrote empty Pareto front: {pareto_path}")
        return

    pareto_df = pd.DataFrame(front).sort_values(objective_cols).reset_index(drop=True)
    pareto_path = out_dir / args.pareto_name
    pareto_df.to_csv(pareto_path, index=False)

    if str(args.shape_mode) == "per_shape" and shape_disc and per_shape_results:
        rollup_rows: List[Dict[str, object]] = []
        for shape_label, shape_rows in per_shape_results.items():
            shape_front = pareto_front(shape_rows, objective_cols)
            shape_df = pd.DataFrame(shape_front).sort_values(objective_cols).reset_index(drop=True)
            shape_path = out_dir / f"pareto__{args.domain}__shape__{shape_label}.csv"
            shape_df.to_csv(shape_path, index=False)

            top_n = min(5, len(shape_df.index))
            best_keys = ""
            if top_n > 0:
                best_keys = " || ".join(shape_df.head(top_n)["keys"].tolist())

            note = ""
            if shape_label == "unknown":
                note = "unrecognized_or_blank_shape"

            rollup_rows.append(
                {
                    "shape": shape_label,
                    "population": int(len(shape_rows)),
                    "best_candidates": best_keys,
                    "note": note,
                }
            )

        rollup_path = out_dir / f"pareto__{args.domain}__shape_rollup.csv"
        pd.DataFrame(rollup_rows).to_csv(rollup_path, index=False)

    # Optional validation summary
    if str(args.mode) in ("validate", "harsh") and args.policy_json is not None:
        summary_rows: List[Dict[str, object]] = []

        if policy_key_tuple:
            # Find the evaluated row for the policy key (may be absent if keys missing from data)
            pol_key_str = "|".join(policy_key_tuple)
            pol_rows = [r for r in results if r.get("keys") == pol_key_str]
            if pol_rows:
                pr = pol_rows[0]
                summary_rows.append({
                    "kind": "policy_key",
                    "keys": pr.get("keys", ""),
                    "k_count": pr.get("k_count", 0),
                    "max_sigcnt": pr.get("max_sigcnt", 0),
                    "collision_records": pr.get("collision_records", 0),
                    "collision_groups": pr.get("collision_groups", 0),
                    "fragmentation_records": pr.get("fragmentation_records", 0),
                    "fragmentation_groups": pr.get("fragmentation_groups", 0),
                })
            else:
                summary_rows.append({
                    "kind": "policy_key",
                    "keys": pol_key_str,
                    "note": "policy_key_not_evaluated_missing_columns_or_empty",
                })

        # Record whether policy key is on Pareto front (exact match)
        if policy_key_tuple:
            pol_key_str = "|".join(policy_key_tuple)
            on_front = int(any(str(r.get("keys", "")) == pol_key_str for r in front))
            summary_rows.append({"kind": "policy_key_on_pareto_front", "value": on_front})

        if skipped_shape_missing:
            summary_rows.append(
                {
                    "kind": "skipped_missing_discriminator",
                    "value": skipped_shape_missing,
                }
            )

        if summary_rows:
            summary_df = pd.DataFrame(summary_rows)

            summary_name = f"validation_summary__{args.domain}__{args.mode}.csv"
            summary_path = out_dir / summary_name

            summary_df.to_csv(summary_path, index=False)
            print(f"Wrote validation summary: {summary_path}")

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

    if skipped_shape_missing:
        print(
            f"[INFO pareto] skipped {skipped_shape_missing} candidate subsets missing discriminator '{shape_disc}'."
        )


def _shape_label(shape_val: str) -> str:
    if shape_val is None:
        return "unknown"
    s = str(shape_val).strip()
    if not s:
        return "unknown"
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


if __name__ == "__main__":
    main()
