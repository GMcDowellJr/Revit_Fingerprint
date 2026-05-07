# tools/patterns_analysis/intradomain_summary.py
"""Intradomain summary: profile representative files per detected standard.

Goal:
- After file-level clustering detects >=2 standards, summarize what differs between
  the cluster representatives using identity_basis.items evidence.

Outputs:
- <domain>.intradomain_representative_profiles.json
- <domain>.intradomain_discriminators.csv
- <domain>.intradomain_summary.json
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .io import load_exports, get_domain_records


def _safe_str(v: Any) -> str:
    if v is None:
        return "null"
    return str(v)


def _extract_identity_items(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    ib = record.get("identity_basis")
    if not isinstance(ib, dict):
        return []
    items = ib.get("items")
    if not isinstance(items, list):
        return []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not k:
            continue
        out.append(
            {
                "k": _safe_str(k),
                "q": _safe_str(it.get("q")),
                "v": _safe_str(it.get("v")),
            }
        )
    return out


def _profile_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate identity evidence across records in a representative file."""
    key_to_vals = defaultdict(list)
    key_to_qs = defaultdict(list)

    for r in records:
        for it in _extract_identity_items(r):
            key_to_vals[it["k"]].append(it["v"])
            key_to_qs[it["k"]].append(it["q"])

    keys_profile = {}
    for k, vals in key_to_vals.items():
        vc = Counter(vals)
        qc = Counter(key_to_qs.get(k, []))
        keys_profile[k] = {
            "distinct_values": len(vc),
            "top_value": vc.most_common(1)[0][0] if vc else None,
            "top_value_share": (vc.most_common(1)[0][1] / sum(vc.values())) if vc else 0.0,
            "value_counts_top10": vc.most_common(10),
            "q_counts": qc.most_common(),
        }

    return {
        "record_count": len(records),
        "keys_profile": keys_profile,
    }


def _pick_representative(clusters_df: pd.DataFrame, standard_name: str) -> str:
    rows = clusters_df[clusters_df["standard_name"] == standard_name]
    rep = rows[rows["is_representative"] == True]
    if len(rep) > 0:
        return str(rep.iloc[0]["file_id"])
    return str(rows.iloc[0]["file_id"])


def _load_export_by_file_id(exports_dir: str, file_id: str):
    for exp in load_exports(exports_dir, max_files=None):
        if exp.file_id == file_id:
            return exp
    return None


def build_intradomain_summary(
    clusters_csv: str,
    exports_dir: str,
    domain: str,
    out_dir: str,
    min_top_value_share_gap: float = 0.25,
) -> None:
    clusters_df = pd.read_csv(clusters_csv)

    if "standard_name" not in clusters_df.columns:
        raise ValueError("clusters_csv missing required column: standard_name")

    standards = sorted([_safe_str(s) for s in clusters_df["standard_name"].unique().tolist()])

    profiles: Dict[str, Any] = {}
    rep_map: Dict[str, str] = {}

    # Build representative profiles
    for std in standards:
        rep_file_id = _pick_representative(clusters_df, std)
        rep_map[std] = rep_file_id

        export = _load_export_by_file_id(exports_dir, rep_file_id)
        if export is None:
            profiles[std] = {
                "representative_file_id": rep_file_id,
                "error": f"export_not_found: {rep_file_id}",
            }
            continue

        records = get_domain_records(export.data, domain)
        prof = _profile_records(records)
        profiles[std] = {
            "representative_file_id": rep_file_id,
            "record_count": prof["record_count"],
            "keys_profile": prof["keys_profile"],
        }

    # Compute discriminators: keys whose representative top_value differs across standards
    # and are reasonably "stable" within at least one standard (high top_value_share).
    all_keys = set()
    for std, p in profiles.items():
        kp = p.get("keys_profile", {})
        if isinstance(kp, dict):
            all_keys.update(kp.keys())

    rows = []
    for k in sorted(all_keys):
        std_vals = []
        std_shares = []
        for std in standards:
            kp = profiles.get(std, {}).get("keys_profile", {})
            if not isinstance(kp, dict) or k not in kp:
                std_vals.append(None)
                std_shares.append(0.0)
                continue
            std_vals.append(kp[k].get("top_value"))
            std_shares.append(float(kp[k].get("top_value_share", 0.0)))

        distinct_top_vals = len(set([v for v in std_vals if v is not None]))
        share_gap = (max(std_shares) - min(std_shares)) if std_shares else 0.0

        # Flag as discriminator if representative top values differ AND there's meaningful stability contrast
        is_discriminator = (distinct_top_vals >= 2) and (share_gap >= min_top_value_share_gap)

        rows.append(
            {
                "key": k,
                "distinct_top_values": distinct_top_vals,
                "top_value_share_min": round(min(std_shares) if std_shares else 0.0, 3),
                "top_value_share_max": round(max(std_shares) if std_shares else 0.0, 3),
                "top_value_share_gap": round(share_gap, 3),
                "is_discriminator": bool(is_discriminator),
                **{f"top_value__{std}": std_vals[i] for i, std in enumerate(standards)},
                **{f"top_share__{std}": round(std_shares[i], 3) for i, std in enumerate(standards)},
            }
        )

    disc_df = pd.DataFrame(rows)
    disc_df = disc_df.sort_values(
        by=["is_discriminator", "top_value_share_gap", "distinct_top_values"],
        ascending=[False, False, False],
    )

    os.makedirs(out_dir, exist_ok=True)

    profiles_path = os.path.join(out_dir, f"{domain}.intradomain_representative_profiles.json")
    with open(profiles_path, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)

    disc_path = os.path.join(out_dir, f"{domain}.intradomain_discriminators.csv")
    disc_df.to_csv(disc_path, index=False)

    summary = {
        "analysis": "intradomain_summary",
        "domain": domain,
        "standards_detected": len(standards),
        "standards": standards,
        "representatives": rep_map,
        "outputs": {
            "representative_profiles_json": os.path.abspath(profiles_path),
            "discriminators_csv": os.path.abspath(disc_path),
        },
        "parameters": {
            "min_top_value_share_gap": min_top_value_share_gap,
        },
    }

    summary_path = os.path.join(out_dir, f"{domain}.intradomain_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("[INFO] Intradomain summary written:")
    print(f"  {profiles_path}")
    print(f"  {disc_path}")
    print(f"  {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize intradomain standards via cluster representative profiling"
    )
    parser.add_argument("clusters_csv", help="Path to <domain>.file_clusters.csv")
    parser.add_argument("exports_dir", help="Directory containing fingerprint exports")
    parser.add_argument("--domain", required=True, help="Domain to analyze")
    parser.add_argument(
        "--min-top-share-gap",
        type=float,
        default=0.25,
        help="Min top_value_share gap to flag discriminator (default: 0.25)",
    )
    parser.add_argument(
        "--out",
        default="intradomain_out",
        dest="out_dir",
        help="Output directory (default: intradomain_out)",
    )

    args = parser.parse_args()

    build_intradomain_summary(
        clusters_csv=args.clusters_csv,
        exports_dir=args.exports_dir,
        domain=args.domain,
        out_dir=args.out_dir,
        min_top_value_share_gap=args.min_top_share_gap,
    )


if __name__ == "__main__":
    main()
