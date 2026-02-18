# tools/run_split_detection_all.py
"""Complete split detection workflow orchestrator."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list, description: str) -> None:
    """Run command with error handling."""
    
    print(f"\n{'='*80}")
    print(f"RUNNING: {description}")
    print(f"{'='*80}\n")
    print(f"Command: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, check=False)
    
    if result.returncode != 0:
        sys.stderr.write(f"\n[ERROR] {description} failed with code {result.returncode}\n")
        sys.exit(1)


def run_split_detection_workflow(
    exports_dir: str,
    domain: str,
    out_root: str,
    threshold: float = 0.70,
    verify_ids_joinkey: bool = False,
    run_calibration: bool = False,
    run_pareto: bool = False,
    phase0_dir: str | None = None,
) -> None:
    """Run complete split detection workflow."""
    
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    
    file_level_out = out_root / 'file_level'
    standards_out = out_root / 'reference_standards'
    element_out = out_root / 'element_level'
    intradomain_out = out_root / 'intradomain'
    join_keys_out = out_root / 'join_keys'
    
    file_level_out.mkdir(exist_ok=True)
    standards_out.mkdir(exist_ok=True)
    element_out.mkdir(exist_ok=True)
    intradomain_out.mkdir(exist_ok=True)
    join_keys_out.mkdir(exist_ok=True)
    
    # Phase 1: File-level clustering
    clusters_csv = file_level_out / f"{domain}.file_clusters.csv"
    
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.split_detection_file_level',
            exports_dir,
            '--domain', domain,
            '--threshold', str(threshold),
            '--out', str(file_level_out),
            *(['--phase0-dir', str(phase0_dir)] if phase0_dir else []),
        ],
        description=f"Phase 1: File-level clustering ({domain})"
    )
    
    # Check if split was detected
    report_json = file_level_out / f"{domain}.file_clustering_report.json"
    
    if not report_json.exists():
        sys.stderr.write("[ERROR] File clustering report not found\n")
        sys.exit(1)
    
    with open(report_json, 'r') as f:
        report = json.load(f)
    
    num_clusters = report['clusters_found']
    
    print(f"\n[INFO] Detected {num_clusters} cluster(s)")
    
    if num_clusters < 2:
        print("[INFO] No split detected - single population")
        print("[INFO] Skipping reference standard building and element-level analysis")
        return
    
    # Phase 2: Build reference standards
    standards_json = standards_out / f"{domain}.reference_standards.json"
    
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.build_reference_standards',
            str(clusters_csv),
            exports_dir,
            '--domain', domain,
            '--out', str(standards_out)
        ],
        description=f"Phase 2: Build reference standards ({domain})"
    )

    # Phase 2.5: Intradomain summary (existing, optional if present)
    # NOTE: only run if module exists in your repo.
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.intradomain_summary',
            str(clusters_csv),
            exports_dir,
            '--domain', domain,
            '--out', str(intradomain_out)
        ],
        description=f"Phase 2.5: Intradomain summary ({domain})"
    )

    # Phase 2A/2B: emit IDS definition + file->IDS mapping
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.emit_intradomain_definition',
            str(clusters_csv),
            '--domain', domain,
            '--out', str(intradomain_out)
        ],
        description="Phase 2A/2B: Emit IDS artifacts"
    )

    file_to_ids_csv = intradomain_out / f"{domain}.file_to_ids.v1.csv"

    # Phase 2C: derive join-key policies per IDS
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.derive_join_keys_by_ids',
            exports_dir,
            '--domain', domain,
            '--file-to-ids', str(file_to_ids_csv),
            '--out', str(join_keys_out),
            '--max-k', '4'
        ],
        description="Phase 2C: Derive join-key policies per IDS"
    )

    policy_json = join_keys_out / f"{domain}.join_key_policy_by_ids.v1.json"
    out_csv = join_keys_out / f"{domain}.join_hash_ids.v1.csv"

    # Phase 2D: apply join-keys per IDS and write join_hash_ids
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.apply_join_keys_by_ids',
            exports_dir,
            '--domain', domain,
            '--file-to-ids', str(file_to_ids_csv),
            '--policy', str(policy_json),
            '--out', str(out_csv)
        ],
        description="Phase 2D: Apply IDS join-keys (write join_hash_ids CSV)"
    )

    ids_report_csv = join_keys_out / f"{domain}.ids_key_selection_report.v1.csv"
    if run_calibration:
        # Phase 2E: calibrate join-key gates from IDS report (optional)
        run_command(
            [
                sys.executable,
                '-m', 'tools.phase2_analysis.calibrate_join_key_gates',
                str(ids_report_csv),
                '--domain', domain,
                '--out', str(join_keys_out)
            ],
            description="Phase 2E: Calibrate IDS join-key gates"
        )

    if run_pareto:
        # Phase 2F: Pareto on escalated IDS policies only (optional)
        run_command(
            [
                sys.executable,
                '-m', 'tools.phase2_analysis.pareto_join_keys_by_ids',
                exports_dir,
                '--domain', domain,
                '--file-to-ids', str(file_to_ids_csv),
                '--out', str(join_keys_out),
                '--only-escalated',
                '--escalation-report', str(ids_report_csv),
                '--max-k', '5',
                '--coverage-min', '0.75'
            ],
            description="Phase 2F: Pareto IDS join-key refinement"
        )

    
    # Phase 3: Element-level classification
    run_command(
        [
            sys.executable,
            '-m', 'tools.phase2_analysis.split_detection_element_level',
            str(clusters_csv),
            str(standards_json),
            exports_dir,
            '--domain', domain,
            '--contamination-threshold', '85.0',
            '--out', str(element_out)
        ],
        description=f"Phase 3: Element-level classification ({domain})"
    )
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SPLIT DETECTION COMPLETE: {domain}")
    print(f"{'='*80}\n")
    
    print(f"Number of standards detected: {num_clusters}")
    print(f"\nOutputs:")
    print(f"  File-level clustering: {file_level_out}")
    print(f"  Reference standards: {standards_out}")
    print(f"  Intradomain: {intradomain_out}")
    print(f"  Join-keys: {join_keys_out}")
    print(f"  Element-level analysis: {element_out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Complete split detection workflow"
    )
    parser.add_argument(
        'exports_dir',
        help="Directory containing fingerprint exports"
    )
    parser.add_argument(
        '--domain',
        required=True,
        help="Domain to analyze (e.g., dimension_types)"
    )
    parser.add_argument(
        '--out-root',
        default='split_detection_output',
        help="Root output directory (default: split_detection_output)"
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.70,
        help="Clustering threshold (default: 0.70)"
    )
    parser.add_argument(
        '--verify-ids-joinkey',
        action='store_true',
        help="Verification pipeline for IDS-aware join-keys (restricted to text_types)"
    )
    parser.add_argument(
        '--run-calibration',
        action='store_true',
        help="Run optional Phase 2E calibration step"
    )
    parser.add_argument(
        '--run-pareto',
        action='store_true',
        help="Run optional Phase 2F pareto refinement step"
    )
    
    args = parser.parse_args()
    
    run_split_detection_workflow(
        exports_dir=args.exports_dir,
        domain=args.domain,
        out_root=args.out_root,
        threshold=args.threshold,
        verify_ids_joinkey=args.verify_ids_joinkey,
        run_calibration=args.run_calibration,
        run_pareto=args.run_pareto,
    )


if __name__ == '__main__':
    main()
