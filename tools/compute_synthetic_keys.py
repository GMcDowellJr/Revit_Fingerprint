#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compute synthetic identity items from flat-table exports.

## Usage
# python tools/compute_synthetic_keys.py \\
#   --items <exports_dir>/identity_items__line_patterns.csv \\
#   --domain line_patterns \\
#   --out <out_dir>/identity_items_lp_augmented.csv
#
# python -m tools.pareto_joinkey_search \\
#   --records <exports_dir>/records__line_patterns.csv \\
#   --items <out_dir>/identity_items_lp_augmented.csv \\
#   --domain line_patterns \\
#   --mode discover \\
#   --candidates line_pattern.segments_norm_hash line_pattern.segments_def_hash line_pattern.segment_count \\
#   --max_k 2 \\
#   --out_dir <out_dir>/pareto_lp_discover \\
#   --pareto_name pareto_lp_discover.csv \\
#   --write_all
#
# python -m tools.pareto_joinkey_search \\
#   --records <exports_dir>/records__line_patterns.csv \\
#   --items <out_dir>/identity_items_lp_augmented.csv \\
#   --domain line_patterns \\
#   --policy_json policies/domain_join_key_policies.json \\
#   --mode validate \\
#   --max_k 2 \\
#   --out_dir <out_dir>/pareto_lp_validate \\
#   --pareto_name pareto_lp_validate.csv
"""

import argparse
import hashlib
import re
from typing import Callable, Dict, Tuple

import pandas as pd

EXPECTED_COLUMNS = [
    "file_id",
    "domain",
    "record_id",
    "record_ordinal",
    "record_pk",
    "item_index",
    "k",
    "q",
    "v",
]

SEGMENT_KEY_RE = re.compile(r"^line_pattern\.(?:seg|segment)\[(\d{3})\]\.(kind|length)$")
SEGMENT_COUNT_KEY = "line_pattern.segment_count"


def _synthetic_line_patterns(items_df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    domain_df = items_df[items_df["domain"] == "line_patterns"].copy()

    synthetic_rows = []
    ok_count = 0
    missing_count = 0

    for _, group in domain_df.groupby("record_pk", sort=False):
        base = group.iloc[0]
        segment_rows = group[group["k"].str.match(SEGMENT_KEY_RE, na=False)]

        out_q = "ok"
        out_v = ""

        if segment_rows.empty:
            seg_count_rows = group[group["k"] == SEGMENT_COUNT_KEY]
            seg_count_v = str(seg_count_rows.iloc[0]["v"]).strip() if not seg_count_rows.empty else ""
            seg_count_q = str(seg_count_rows.iloc[0]["q"]).strip() if not seg_count_rows.empty else ""
            try:
                seg_count_is_zero = int(seg_count_v) == 0
            except Exception:
                seg_count_is_zero = False

            if seg_count_q == "ok" and seg_count_is_zero:
                out_v = hashlib.md5("segment_count=0".encode("utf-8")).hexdigest()
                ok_count += 1
            else:
                out_q = "missing"
                missing_count += 1
        elif (segment_rows["q"] != "ok").any():
            out_q = "missing"
            missing_count += 1
        else:
            segments: Dict[int, Dict[str, object]] = {}
            parse_error = False

            for _, row in segment_rows.iterrows():
                m = SEGMENT_KEY_RE.match(str(row["k"]))
                if not m:
                    continue

                idx = int(m.group(1))
                field = m.group(2)
                segments.setdefault(idx, {})

                try:
                    if field == "kind":
                        segments[idx]["kind"] = int(row["v"])
                    else:
                        segments[idx]["length"] = float(row["v"])
                except Exception:
                    parse_error = True
                    break

            if parse_error or any("kind" not in d or "length" not in d for d in segments.values()):
                out_q = "missing"
                missing_count += 1
            else:
                ordered = [(idx, int(v["kind"]), float(v["length"])) for idx, v in sorted(segments.items())]
                non_dot_total = sum(length for _, kind, length in ordered if kind != 2)
                has_non_dot = any(kind != 2 for _, kind, _ in ordered)
                dot_count = sum(1 for _, kind, _ in ordered if kind == 2)
                eff_total = non_dot_total if has_non_dot else float(dot_count)

                tokens = []
                for idx, kind, length in ordered:
                    if kind == 2:
                        eff_length = 0.0 if has_non_dot else 1.0
                    else:
                        eff_length = length
                    norm = (eff_length / eff_total) if eff_total > 0 else 0.0
                    tokens.append(f"seg[{idx:03d}].kind={kind}")
                    tokens.append(f"seg[{idx:03d}].norm_length={norm:.9f}")

                out_v = hashlib.md5("|".join(tokens).encode("utf-8")).hexdigest()
                ok_count += 1

        synthetic_rows.append(
            {
                "file_id": base["file_id"],
                "domain": base["domain"],
                "record_id": base["record_id"],
                "record_ordinal": base["record_ordinal"],
                "record_pk": base["record_pk"],
                "item_index": "synthetic",
                "k": "line_pattern.segments_norm_hash",
                "q": out_q,
                "v": out_v,
            }
        )

    return pd.DataFrame(synthetic_rows, columns=EXPECTED_COLUMNS), ok_count, missing_count


def compute_synthetic_keys(items_df: pd.DataFrame, domain: str) -> Tuple[pd.DataFrame, int, int]:
    registry: Dict[str, Callable[[pd.DataFrame], Tuple[pd.DataFrame, int, int]]] = {
        "line_patterns": _synthetic_line_patterns,
    }

    if domain not in registry:
        raise ValueError(f"Unsupported domain '{domain}'. Supported domains: {', '.join(sorted(registry))}")

    return registry[domain](items_df)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute synthetic identity items from flat-table CSV exports")
    parser.add_argument("--items", required=True, help="Path to identity_items CSV")
    parser.add_argument("--domain", required=True, choices=["line_patterns"], help="Domain to compute synthetic keys for")
    parser.add_argument("--out", required=True, help="Output path for augmented CSV")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    items_df = pd.read_csv(args.items, dtype=str, keep_default_na=False)

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in items_df.columns]
    if missing_cols:
        raise ValueError(f"Input CSV missing required columns: {missing_cols}")

    synthetic_df, ok_count, missing_count = compute_synthetic_keys(items_df, args.domain)
    augmented_df = pd.concat([items_df[EXPECTED_COLUMNS], synthetic_df], ignore_index=True)
    augmented_df.to_csv(args.out, index=False)

    total = len(synthetic_df)
    print(f"Computed line_pattern.segments_norm_hash for {total} records ({ok_count} ok, {missing_count} missing)")
    print(f"Wrote augmented items to: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
