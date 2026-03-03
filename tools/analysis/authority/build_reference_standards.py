# tools/phase2_analysis/build_reference_standards.py
"""Build reference standards from file clusters."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from tools.analysis.authority.io import load_exports, get_domain_records, _read_csv_rows


def build_reference_standards_from_clusters(
    clusters_csv: str,
    exports_dir: str,
    domain: str,
    out_dir: str,
    *,
    phase0_dir: str | None = None,
) -> None:
    """Build reference standards from cluster representatives."""
    
    # Load cluster assignments
    clusters_df = pd.read_csv(clusters_csv)
    
    # Get unique standards
    standards = clusters_df['standard_name'].unique()
    
    print(f"[INFO] Building reference standards for {len(standards)} detected standards...")
    
    reference_standards = {}
    
    for std_name in standards:
        # Get representative file for this standard
        cluster_rows = clusters_df[clusters_df['standard_name'] == std_name]
        rep_row = cluster_rows[cluster_rows['is_representative'] == True]
        
        if len(rep_row) == 0:
            # Fallback: use first file
            rep_row = cluster_rows.iloc[[0]]
        
        rep_file_id = rep_row['file_id'].values[0]
        cluster_size = int(cluster_rows['cluster_size'].values[0])
        
        print(f"  {std_name}: representative = {rep_file_id}")
        
        if phase0_dir:
            # CSV mode: read sig_hashes from out/current/flatten/phase0_records.csv
            rec_csv = os.path.join(os.path.abspath(phase0_dir), "phase0_records.csv")
            if not os.path.isfile(rec_csv):
                sys.stderr.write(f"[WARN] phase0_records.csv not found: {rec_csv}\n")
                continue

            sig_hashes = set()
            for r in _read_csv_rows(rec_csv):
                if r.get("domain", "") != domain:
                    continue
                # v2.1 uses export_run_id; interim equals file_id. Match either.
                if r.get("export_run_id", "") != rep_file_id and r.get("file_id", "") != rep_file_id:
                    continue
                sh = (r.get("sig_hash", "") or "").strip()
                if sh:
                    sig_hashes.add(sh)
        else:
            # JSON mode (back-compat)
            rep_export = None
            exports = load_exports(exports_dir, max_files=None)

            for export in exports:
                if export.file_id == rep_file_id:
                    rep_export = export
                    break

            if not rep_export:
                sys.stderr.write(f"[WARN] Could not find export for {rep_file_id}\n")
                continue

            records = get_domain_records(rep_export.data, domain)
            sig_hashes = {r.get('sig_hash') for r in records if r.get('sig_hash')}
        
        reference_standards[std_name] = {
            'representative_file': rep_file_id,
            'element_count': len(sig_hashes),
            'cluster_size': cluster_size,
            'sig_hashes': list(sig_hashes)  # Convert set to list for JSON
        }
        
        print(f"    Elements: {len(sig_hashes)}")
    
    # Save reference standards
    os.makedirs(out_dir, exist_ok=True)
    
    output_path = os.path.join(out_dir, f"{domain}.reference_standards.json")
    
    with open(output_path, 'w') as f:
        json.dump(reference_standards, f, indent=2)
    
    # Also save a summary (without full sig_hash lists)
    summary = {
        std_name: {
            'representative_file': std_data['representative_file'],
            'element_count': std_data['element_count'],
            'cluster_size': std_data['cluster_size']
        }
        for std_name, std_data in reference_standards.items()
    }
    
    summary_path = os.path.join(out_dir, f"{domain}.reference_standards_summary.json")
    
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n[INFO] Reference standards built:")
    print(f"  Full definitions: {output_path}")
    print(f"  Summary: {summary_path}")
    
    return reference_standards


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build reference standards from file clusters"
    )
    parser.add_argument(
        'clusters_csv',
        help="Path to cluster assignments CSV (from split_detection_file_level.py)"
    )
    parser.add_argument(
        'exports_dir',
        help="Directory containing fingerprint exports. Ignored if --phase0-dir is provided."
    )
    parser.add_argument(
        '--phase0-dir',
        dest='phase0_dir',
        default=None,
        help="If provided, read v2.1 Phase0 tables from this directory (out/current/flatten).",
    )
    parser.add_argument(
        '--domain',
        required=True,
        help="Domain to analyze"
    )
    parser.add_argument(
        '--out',
        default='reference_standards',
        dest='out_dir',
        help="Output directory (default: reference_standards)"
    )
    
    args = parser.parse_args()
    
    build_reference_standards_from_clusters(
        clusters_csv=args.clusters_csv,
        exports_dir=args.exports_dir,
        domain=args.domain,
        out_dir=args.out_dir,
        phase0_dir=args.phase0_dir,
    )


if __name__ == '__main__':
    main()
