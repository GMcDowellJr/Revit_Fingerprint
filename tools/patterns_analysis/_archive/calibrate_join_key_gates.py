# tools/patterns_analysis/calibrate_join_key_gates.py
from __future__ import annotations
import argparse, json, os
import pandas as pd
import numpy as np

def calibrate(csv_path: str, domain: str, out_dir: str, coverage_margin: float = 0.05):
    df = pd.read_csv(csv_path)

    # Guard: only consider IDSs with any records
    df = df[df["total_records"] > 0]

    calib = {
        "domain": domain,
        "source_csv": os.path.abspath(csv_path),
        "suggested_gates": {
            "delta_collision_min": float(np.percentile(df["delta_collision"], 25)),
            "coverage_min": max(
                0.0,
                float(np.percentile(df["coverage_final"], 50) - coverage_margin),
            ),
            "stability_min": float(np.percentile(df["stability_median"], 25)),
        },
        "distributions": {
            "delta_collision": {
                "p25": float(np.percentile(df["delta_collision"], 25)),
                "p50": float(np.percentile(df["delta_collision"], 50)),
                "p75": float(np.percentile(df["delta_collision"], 75)),
            },
            "coverage_final": {
                "p25": float(np.percentile(df["coverage_final"], 25)),
                "p50": float(np.percentile(df["coverage_final"], 50)),
                "p75": float(np.percentile(df["coverage_final"], 75)),
            },
            "stability_median": {
                "p25": float(np.percentile(df["stability_median"], 25)),
                "p50": float(np.percentile(df["stability_median"], 50)),
                "p75": float(np.percentile(df["stability_median"], 75)),
            },
        },
        "notes": "Calibration only. Gates are not enforced until pinned in policy.",
    }

    os.makedirs(out_dir, exist_ok=True)

    json_path = os.path.join(out_dir, f"{domain}.join_key_gate_calibration.v1.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(calib, f, indent=2)

    csv_path_out = os.path.join(out_dir, f"{domain}.join_key_gate_calibration.v1.csv")
    df[[
        "ids_id",
        "delta_collision",
        "coverage_final",
        "stability_median",
        "escalate_to_pareto",
    ]].to_csv(csv_path_out, index=False)

    print("[INFO] Calibration written:")
    print(f"  {json_path}")
    print(f"  {csv_path_out}")

def main():
    p = argparse.ArgumentParser("Calibrate join-key gates from IDS report")
    p.add_argument("ids_report_csv")
    p.add_argument("--domain", required=True)
    p.add_argument("--out", default="join_key_calibration")
    p.add_argument("--coverage-margin", type=float, default=0.05)
    args = p.parse_args()

    calibrate(
        csv_path=args.ids_report_csv,
        domain=args.domain,
        out_dir=args.out,
        coverage_margin=args.coverage_margin,
    )

if __name__ == "__main__":
    main()
