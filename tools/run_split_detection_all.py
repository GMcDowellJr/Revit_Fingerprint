# tools/run_split_detection_all.py
"""Complete split detection workflow orchestrator."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


SCHEMA_VERSION = "2.1.0"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in reader]


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def _load_export_mapping(phase0_dir: str | None) -> Dict[str, str]:
    if not phase0_dir:
        return {}
    metadata_csv = Path(phase0_dir) / "file_metadata.csv"
    if not metadata_csv.exists():
        return {}
    out: Dict[str, str] = {}
    rows = _read_csv(metadata_csv)
    for row in rows:
        file_id = row.get("file_id", "").strip()
        export_run_id = row.get("export_run_id", "").strip()
        if file_id and export_run_id:
            out[file_id] = export_run_id
    return out


def _load_analysis_run_id(analysis_dir: str | None) -> str:
    if not analysis_dir:
        return ""
    manifest_csv = Path(analysis_dir) / "analysis_manifest.csv"
    if not manifest_csv.exists():
        return ""
    rows = _read_csv(manifest_csv)
    return rows[0].get("analysis_run_id", "").strip() if rows else ""


def _derive_analysis_run_id(out_root: Path, file_to_export: Dict[str, str]) -> str:
    candidates: List[str] = []
    for csv_path in sorted(out_root.rglob("*.csv"), key=lambda p: str(p).lower()):
        rows = _read_csv(csv_path)
        for row in rows:
            export_run_id = row.get("export_run_id", "").strip()
            file_id = row.get("file_id", "").strip()
            resolved = export_run_id or file_to_export.get(file_id, "") or file_id
            if resolved:
                candidates.append(resolved)
    if not candidates:
        return ""
    src = "|".join(sorted(set(candidates), key=lambda v: v.lower()))
    return f"ana_{hashlib.sha1(src.encode('utf-8')).hexdigest()[:12]}"


def _finalize_split_outputs(out_root: Path, *, domain: str, phase0_dir: str | None, analysis_dir: str | None) -> None:
    file_to_export = _load_export_mapping(phase0_dir)
    analysis_run_id = _load_analysis_run_id(analysis_dir) or _derive_analysis_run_id(out_root, file_to_export)
    _inject_split_contract_headers(
        out_root,
        domain=domain,
        analysis_run_id=analysis_run_id,
        file_to_export=file_to_export,
    )
    _emit_file_to_export_bridge(out_root, file_to_export)
    _emit_cluster_to_pattern_map(out_root, analysis_dir, domain, analysis_run_id)


def _inject_split_contract_headers(
    out_root: Path,
    *,
    domain: str,
    analysis_run_id: str,
    file_to_export: Dict[str, str],
) -> None:
    for csv_path in sorted(out_root.rglob("*.csv"), key=lambda p: str(p).lower()):
        rows = _read_csv(csv_path)
        if not rows:
            continue

        old_fields = list(rows[0].keys())
        new_fields = ["schema_version", "analysis_run_id", "domain", "export_run_id"] + [
            f for f in old_fields if f not in {"schema_version", "analysis_run_id", "domain", "export_run_id"}
        ]

        for row in rows:
            row["schema_version"] = SCHEMA_VERSION
            row["analysis_run_id"] = analysis_run_id
            row["domain"] = row.get("domain", "").strip() or domain
            export_run_id = row.get("export_run_id", "").strip()
            file_id = row.get("file_id", "").strip()
            if not export_run_id:
                export_run_id = file_to_export.get(file_id, "") or file_id
            row["export_run_id"] = export_run_id

        sort_keys = [k for k in ("analysis_run_id", "domain", "export_run_id", "cluster_id", "record_pk", "record_id", "group_type", "group_id") if k in new_fields]
        rows.sort(key=lambda r: tuple(r.get(k, "") for k in sort_keys))
        _write_csv(csv_path, new_fields, rows)


def _emit_file_to_export_bridge(out_root: Path, file_to_export: Dict[str, str]) -> None:
    if not file_to_export:
        return
    rows = [{"schema_version": SCHEMA_VERSION, "analysis_run_id": "", "domain": "*", "file_id": k, "export_run_id": v} for k, v in sorted(file_to_export.items(), key=lambda kv: kv[0].lower())]
    _write_csv(out_root / "file_id_to_export_run_id.csv", ["schema_version", "analysis_run_id", "domain", "file_id", "export_run_id"], rows)


def _emit_cluster_to_pattern_map(out_root: Path, analysis_dir: str | None, domain: str, analysis_run_id: str) -> None:
    if not analysis_dir:
        return
    clusters_csv = out_root / "file_level" / f"{domain}.file_clusters.csv"
    membership_csv = Path(analysis_dir) / "record_pattern_membership.csv"
    if not clusters_csv.exists() or not membership_csv.exists():
        return

    cluster_rows = _read_csv(clusters_csv)
    membership_rows = [r for r in _read_csv(membership_csv) if r.get("domain", "") == domain]
    if not cluster_rows or not membership_rows:
        return

    file_to_cluster: Dict[str, str] = {}
    for r in cluster_rows:
        export_run_id = r.get("export_run_id", "").strip() or r.get("file_id", "").strip()
        if export_run_id:
            file_to_cluster[export_run_id] = r.get("cluster_id", "")

    counts: Dict[str, Dict[str, int]] = {}
    for r in membership_rows:
        export_run_id = r.get("export_run_id", "").strip()
        pattern_id = r.get("pattern_id", "").strip()
        cluster_id = file_to_cluster.get(export_run_id, "")
        if not cluster_id or not pattern_id:
            continue
        counts.setdefault(cluster_id, {})
        counts[cluster_id][pattern_id] = counts[cluster_id].get(pattern_id, 0) + 1

    out_rows: List[Dict[str, str]] = []
    for cluster_id in sorted({r.get("cluster_id", "") for r in cluster_rows if r.get("cluster_id", "")}, key=str):
        pattern_counts = counts.get(cluster_id, {})
        if not pattern_counts:
            out_rows.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": domain,
                "export_run_id": "",
                "cluster_id": cluster_id,
                "pattern_id": "",
                "mapping_quality": "NONE",
                "mapping_reason": "no overlapping record membership",
            })
            continue
        ranked = sorted(pattern_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        top_pattern, top_count = ranked[0]
        total = sum(pattern_counts.values())
        exact = len(pattern_counts) == 1
        tied = len(ranked) > 1 and ranked[1][1] == top_count
        dominant_share = (top_count / total) if total else 0.0
        if exact:
            quality = "EXACT"
            reason = "single pattern observed"
        elif tied:
            quality = "AMBIGUOUS"
            reason = "top pattern tie"
        elif dominant_share >= 0.5:
            quality = "DOMINANT"
            reason = f"top pattern share={dominant_share:.6f}"
        else:
            quality = "AMBIGUOUS"
            reason = f"weak top pattern share={dominant_share:.6f}"
        out_rows.append({
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": analysis_run_id,
            "domain": domain,
            "export_run_id": "",
            "cluster_id": cluster_id,
            "pattern_id": top_pattern,
            "mapping_quality": quality,
            "mapping_reason": reason,
        })

    _write_csv(
        out_root / "split_cluster_to_pattern_map.csv",
        ["schema_version", "analysis_run_id", "domain", "export_run_id", "cluster_id", "pattern_id", "mapping_quality", "mapping_reason"],
        out_rows,
    )


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
    mode: str = 'allpairs',
    verify_ids_joinkey: bool = False,
    run_calibration: bool = False,
    run_pareto: bool = False,
    phase0_dir: str | None = None,
    analysis_dir: str | None = None,
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
            '--mode', mode,
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
        _finalize_split_outputs(out_root, domain=domain, phase0_dir=phase0_dir, analysis_dir=analysis_dir)
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
            '--out', str(standards_out),
            *(['--phase0-dir', str(phase0_dir)] if phase0_dir else []),
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
            '--out', str(element_out),
            *(['--phase0-dir', str(phase0_dir)] if phase0_dir else []),
        ],
        description=f"Phase 3: Element-level classification ({domain})"
    )

    _finalize_split_outputs(out_root, domain=domain, phase0_dir=phase0_dir, analysis_dir=analysis_dir)
    
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
        '--phase0-dir',
        dest='phase0_dir',
        default=None,
        help="If provided, use v2.1 Phase0 tables from this directory (Results_v21/phase0_v21) for all split-analysis steps that support CSV mode.",
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
        '--analysis-dir',
        dest='analysis_dir',
        default=None,
        help="Optional path to analysis_v21 output directory (for analysis_run_id and pattern mapping joins).",
    )
    parser.add_argument(
        '--mode',
        choices=('allpairs', 'candidates'),
        default='allpairs',
        help='File-level split detection mode (default: allpairs)'
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
        phase0_dir=args.phase0_dir,
        domain=args.domain,
        out_root=args.out_root,
        threshold=args.threshold,
        mode=args.mode,
        verify_ids_joinkey=args.verify_ids_joinkey,
        run_calibration=args.run_calibration,
        run_pareto=args.run_pareto,
        analysis_dir=args.analysis_dir,
    )


if __name__ == '__main__':
    main()
