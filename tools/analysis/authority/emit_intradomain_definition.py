# tools/phase2_analysis/emit_intradomain_definition.py
"""Emit stable intradomain artifacts from file-level clustering output.

Phase 2A: materialize intradomain standards (IDS) as a versioned artifact.
Phase 2B: materialize deterministic file -> IDS assignments.

This is a pure transform of <domain>.file_clusters.csv to avoid rerunning clustering.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd


@dataclass(frozen=True)
class IDS:
    ids_id: str
    standard_name: str


def _make_ids_ids(standard_names: List[str], prefix: str = "IDS") -> Dict[str, str]:
    """Stable mapping from standard_name -> IDS_### (sorted by name)."""
    out: Dict[str, str] = {}
    for i, name in enumerate(sorted(standard_names), start=1):
        out[name] = f"{prefix}_{i:03d}"
    return out


def emit_ids_artifacts(
    clusters_csv: str,
    domain: str,
    out_dir: str,
    ids_version: str = "intradomain.v1",
) -> None:
    df = pd.read_csv(clusters_csv)

    required_cols = {"file_id", "standard_name", "cluster_id", "cluster_size", "internal_similarity", "is_representative"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"clusters_csv missing required columns: {sorted(missing)}")

    standard_names = [str(s) for s in df["standard_name"].unique().tolist()]
    std_to_ids = _make_ids_ids(standard_names)

    # Phase 2A artifact: IDS definition
    ids_def = {
        "domain": domain,
        "ids_version": ids_version,
        "ids": [],
        "source": {
            "clusters_csv": os.path.abspath(clusters_csv),
        },
    }

    for std_name in sorted(standard_names):
        ids_id = std_to_ids[std_name]
        rows = df[df["standard_name"] == std_name]

        rep_rows = rows[rows["is_representative"] == True]
        rep_file_id = str(rep_rows.iloc[0]["file_id"]) if len(rep_rows) > 0 else str(rows.iloc[0]["file_id"])

        ids_def["ids"].append(
            {
                "ids_id": ids_id,
                "standard_name": std_name,
                "cluster_id": int(rows.iloc[0]["cluster_id"]),
                "cluster_size": int(rows.iloc[0]["cluster_size"]),
                "representative_file_id": rep_file_id,
                "member_file_ids": [str(x) for x in rows["file_id"].tolist()],
                "avg_internal_similarity": float(rows["internal_similarity"].mean()),
            }
        )

    # Phase 2B artifact: file -> ids mapping
    mapping_rows = []
    for _, r in df.iterrows():
        std_name = str(r["standard_name"])
        mapping_rows.append(
            {
                "file_id": str(r["file_id"]),
                "ids_id": std_to_ids[std_name],
                "standard_name": std_name,
                "cluster_id": int(r["cluster_id"]),
                "internal_similarity": float(r["internal_similarity"]),
                "is_representative": bool(r["is_representative"]),
                "mixed_flag": bool(float(r["internal_similarity"]) < 0.85),
            }
        )

    os.makedirs(out_dir, exist_ok=True)

    ids_json_path = os.path.join(out_dir, f"{domain}.{ids_version}.json")
    with open(ids_json_path, "w", encoding="utf-8") as f:
        json.dump(ids_def, f, indent=2)

    file_to_ids_path = os.path.join(out_dir, f"{domain}.file_to_ids.v1.csv")
    with open(file_to_ids_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "file_id",
                "ids_id",
                "standard_name",
                "cluster_id",
                "internal_similarity",
                "is_representative",
                "mixed_flag",
            ],
        )
        w.writeheader()
        for row in mapping_rows:
            w.writerow(row)

    print("[INFO] IDS artifacts written:")
    print(f"  {ids_json_path}")
    print(f"  {file_to_ids_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Emit intradomain IDS artifacts from file clustering output")
    p.add_argument("clusters_csv", help="Path to <domain>.file_clusters.csv")
    p.add_argument("--domain", required=True, help="Domain name (e.g. text_types)")
    p.add_argument("--out", default="intradomain", dest="out_dir", help="Output directory")
    args = p.parse_args()

    emit_ids_artifacts(
        clusters_csv=args.clusters_csv,
        domain=args.domain,
        out_dir=args.out_dir,
    )


if __name__ == "__main__":
    main()
