# tools/phase2_analysis/split_detection_element_level.py
"""Element-level classification against reference standards."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List
import csv
from typing import Iterator

import pandas as pd

from .io import load_exports, get_domain_records, load_export_file
from .report import write_json_report


def _read_csv_rows(path: str) -> Iterator[Dict[str, str]]:
    """Stream rows from a CSV file as dicts (UTF-8 with BOM support)."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if isinstance(row, dict):
                yield {str(k): ("" if v is None else str(v)) for k, v in row.items()}


def extract_label_display(record: Dict) -> str:
    """Extract human-readable label from record.
    
    Priority order:
    1. label_display (if present)
    2. Construct from label components
    3. record_id (fallback)
    """
    
    # Option 1: Direct label_display field
    if 'label_display' in record and record['label_display']:
        return record['label_display']
    
    # Option 2: Construct from label components
    label = record.get('label')
    if label:
        if isinstance(label, dict):
            # label is structured
            components = []
            
            # Common patterns for different domains
            if 'type_name' in label:
                components.append(label['type_name'])
            elif 'name' in label:
                components.append(label['name'])
            elif 'family_name' in label and 'type_name' in label:
                components.append(f"{label['family_name']}: {label['type_name']}")
            else:
                # Fallback: join all non-empty values
                components = [str(v) for v in label.values() if v]
            
            if components:
                return ' - '.join(components)
        
        elif isinstance(label, str):
            # label is already a string
            return label
    
    # Option 3: Check if there's a 'name' field directly
    if 'name' in record and record['name']:
        return record['name']
    
    # Option 4: Fallback to record_id
    return record.get('record_id', 'Unknown')


def classify_file_elements(
    file_export,
    domain: str,
    reference_standards: Dict,
    *,
    phase0_dir: str | None = None,
    file_id_override: str | None = None,
) -> Dict:
    """Classify each element in file against reference standards.

    Modes:
    - JSON mode (default): uses file_export.data via get_domain_records
    - CSV mode: if phase0_dir is provided, reads v2.1 phase0_records.csv for (file_id, domain)
    """
    element_classifications = []

    if phase0_dir:
        file_id = str(file_id_override or getattr(file_export, "file_id", "") or "")
        if not file_id:
            raise ValueError("CSV mode requires file_id (file_id_override or file_export.file_id).")

        rec_csv = os.path.join(os.path.abspath(phase0_dir), "phase0_records.csv")
        if not os.path.isfile(rec_csv):
            raise FileNotFoundError(f"phase0_records.csv not found: {rec_csv}")

        for record in _read_csv_rows(rec_csv):
            if record.get("domain", "") != domain:
                continue
            if record.get("export_run_id", "") != file_id and record.get("file_id", "") != file_id:
                continue

            sig_hash = (record.get("sig_hash", "") or "").strip()
            record_id = (record.get("record_id", "") or "").strip()
            record_pk = (record.get("record_pk", "") or "").strip()
            join_hash = (record.get("join_hash", "") or "").strip() or None
            label_display = (record.get("label_display", "") or "").strip() or (record_id or "Unknown")

            matches = []
            for std_name, std_data in reference_standards.items():
                if sig_hash and sig_hash in std_data['sig_hashes']:
                    matches.append(std_name)

            if len(matches) == 0:
                classification = 'Custom/Unknown'
            elif len(matches) == 1:
                classification = matches[0]
            else:
                classification = f"Ambiguous: {', '.join(matches)}"

            element_classifications.append({
                'record_id': record_id,
                'record_pk': record_pk,
                'label_display': label_display,
                'sig_hash': sig_hash,
                'join_hash': join_hash,
                'classification': classification,
                'matched_standards': matches
            })

        stats = compute_element_statistics(element_classifications)

        return {
            'file_id': file_id,
            'domain': domain,
            'elements': element_classifications,
            'statistics': stats
        }

    # JSON mode (back-compat)
    records = get_domain_records(file_export.data, domain)

    for record in records:
        sig_hash = record.get('sig_hash')
        record_id = record.get('record_id')
        join_hash = record.get('join_key', {}).get('join_hash') if isinstance(record.get('join_key'), dict) else None

        label_display = extract_label_display(record)

        matches = []
        for std_name, std_data in reference_standards.items():
            if sig_hash in std_data['sig_hashes']:
                matches.append(std_name)

        if len(matches) == 0:
            classification = 'Custom/Unknown'
        elif len(matches) == 1:
            classification = matches[0]
        else:
            classification = f"Ambiguous: {', '.join(matches)}"

        element_classifications.append({
            'record_id': record_id,
            'record_pk': "",
            'label_display': label_display,
            'sig_hash': sig_hash,
            'join_hash': join_hash,
            'classification': classification,
            'matched_standards': matches
        })
    
    # Compute statistics
    stats = compute_element_statistics(element_classifications)
    
    return {
        'file_id': file_export.file_id,
        'domain': domain,
        'elements': element_classifications,
        'statistics': stats
    }


def compute_element_statistics(element_classifications: List[Dict]) -> Dict:
    """Aggregate element-level classifications."""
    
    from collections import Counter
    
    total = len(element_classifications)
    
    if total == 0:
        return {
            'total_elements': 0,
            'by_classification': {},
            'dominant_standard': None,
            'dominant_percentage': 0,
            'is_pure': False,
            'contamination_rate': 0
        }
    
    class_counts = Counter([e['classification'] for e in element_classifications])
    
    stats = {
        'total_elements': total,
        'by_classification': {}
    }
    
    for classification, count in class_counts.items():
        stats['by_classification'][classification] = {
            'count': count,
            'percentage': round(100 * count / total, 1)
        }
    
    # Dominant standard
    dominant = class_counts.most_common(1)[0]
    stats['dominant_standard'] = dominant[0]
    stats['dominant_percentage'] = round(100 * dominant[1] / total, 1)
    stats['is_pure'] = (len(class_counts) == 1)
    stats['contamination_rate'] = round(100 * (1 - dominant[1] / total), 1)
    
    return stats


def generate_remediation_plan(classification_result: Dict) -> Dict:
    """Create actionable remediation plan for contaminated file."""
    
    stats = classification_result['statistics']
    
    if stats['is_pure']:
        return {
            'needs_remediation': False,
            'file_id': classification_result['file_id'],
            'message': f"File is pure {stats['dominant_standard']}"
        }
    
    dominant = stats['dominant_standard']
    
    # Identify elements that don't match dominant
    non_dominant = [
        e for e in classification_result['elements']
        if e['classification'] != dominant
    ]
    
    # Group by source
    by_source = {}
    for elem in non_dominant:
        source = elem['classification']
        if source not in by_source:
            by_source[source] = []
        by_source[source].append(elem)
    
    return {
        'needs_remediation': True,
        'file_id': classification_result['file_id'],
        'target_standard': dominant,
        'contamination_rate': stats['contamination_rate'],
        'elements_to_remediate': len(non_dominant),
        'by_source': {
            source: {
                'count': len(elements),
                'elements': [
                    {
                        'record_id': e['record_id'],
                        'label_display': e['label_display'],
                        'current_classification': e['classification']
                    }
                    for e in elements
                ],
                'action': f"Replace with {dominant} equivalent"
            }
            for source, elements in by_source.items()
        },
        'summary': f"Align to {dominant} by remediating {len(non_dominant)} elements"
    }


def run_element_level_classification(
    clusters_csv: str,
    reference_standards_json: str,
    exports_dir: str,
    domain: str,
    contamination_threshold: float,
    out_dir: str,
    *,
    phase0_dir: str | None = None,
) -> None:
    """Run element-level classification for contaminated files."""
    
    # Load clusters to identify outliers/contaminated
    clusters_df = pd.read_csv(clusters_csv)
    
    # Load reference standards
    with open(reference_standards_json, 'r') as f:
        reference_standards_raw = json.load(f)
    
    # Convert sig_hashes from list back to set for fast lookup
    reference_standards = {
        std_name: {
            **std_data,
            'sig_hashes': set(std_data['sig_hashes'])
        }
        for std_name, std_data in reference_standards_raw.items()
    }
    
    print(f"[INFO] Loaded {len(reference_standards)} reference standards")
    
    # Identify files to analyze
    # Files with low internal_similarity are likely contaminated
    threshold = contamination_threshold / 100.0
    
    contaminated_candidates = clusters_df[
        clusters_df['internal_similarity'] < threshold
    ]['file_id'].unique()
    
    print(f"[INFO] Found {len(contaminated_candidates)} files with similarity < {contamination_threshold}%")
    
    if len(contaminated_candidates) == 0:
        print("[INFO] No contaminated files detected. Exiting.")
        return
    
    exports_dict = {}
    if not phase0_dir:
        exports = load_exports(exports_dir, max_files=None)
        for export in exports:
            exports_dict[export.file_id] = export
    
    # Classify each contaminated file
    contamination_reports = []
    remediation_plans = []
    
    for file_id in contaminated_candidates:
        export = None
        if not phase0_dir:
            if file_id not in exports_dict:
                sys.stderr.write(f"[WARN] Export not found for {file_id}\n")
                continue
            export = exports_dict[file_id]

        print(f"[INFO] Classifying elements in {file_id}...")

        result = classify_file_elements(
            file_export=export,
            domain=domain,
            reference_standards=reference_standards,
            phase0_dir=phase0_dir,
            file_id_override=file_id,
        )
        
        contamination_reports.append(result)
        
        # Generate remediation plan if needed
        if result['statistics']['contamination_rate'] > 5.0:  # Only if >5% contamination
            plan = generate_remediation_plan(result)
            remediation_plans.append(plan)
            
            print(f"  Contamination: {result['statistics']['contamination_rate']:.1f}%")
            print(f"  Target: {plan.get('target_standard', 'N/A')}")
    
    # Save results
    os.makedirs(out_dir, exist_ok=True)
    
    # Detailed element classifications
    elements_data = []
    for report in contamination_reports:
        for elem in report['elements']:
            elements_data.append({
                'file_id': report['file_id'],
                'record_id': elem['record_id'],
                'record_pk': elem.get('record_pk', ''),
                'label_display': elem['label_display'],
                'sig_hash': elem['sig_hash'],
                'join_hash': elem['join_hash'],
                'classification': elem['classification']
            })
    
    elements_df = pd.DataFrame(elements_data)
    
    # Reorder columns to put label_display first
    column_order = ['file_id', 'label_display', 'classification', 'record_pk', 'record_id', 'sig_hash', 'join_hash']
    elements_df = elements_df[column_order]
    
    elements_csv = os.path.join(out_dir, f"{domain}.element_classifications.csv")
    elements_df.to_csv(elements_csv, index=False)
    
    # Summary of contaminated files
    summary_data = []
    for report in contamination_reports:
        summary_data.append({
            'file_id': report['file_id'],
            'total_elements': report['statistics']['total_elements'],
            'dominant_standard': report['statistics']['dominant_standard'],
            'contamination_rate': report['statistics']['contamination_rate'],
            'is_pure': report['statistics']['is_pure']
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_csv = os.path.join(out_dir, f"{domain}.contamination_summary.csv")
    summary_df.to_csv(summary_csv, index=False)
    
    # Remediation plans JSON
    plans_json = os.path.join(out_dir, f"{domain}.remediation_plans.json")
    with open(plans_json, 'w') as f:
        json.dump(remediation_plans, f, indent=2)
    
    # Report
    report = {
        'analysis': 'element_level_classification',
        'domain': domain,
        'files_analyzed': len(contaminated_candidates),
        'files_with_contamination': len([r for r in contamination_reports if r['statistics']['contamination_rate'] > 5]),
        'outputs': {
            'element_classifications_csv': os.path.abspath(elements_csv),
            'contamination_summary_csv': os.path.abspath(summary_csv),
            'remediation_plans_json': os.path.abspath(plans_json)
        }
    }
    
    report_path = os.path.join(out_dir, f"{domain}.element_classification_report.json")
    write_json_report(out_path=report_path, report=report)
    
    print(f"\n[INFO] Element-level classification complete:")
    print(f"  Files analyzed: {len(contaminated_candidates)}")
    print(f"  Files with >5% contamination: {len(remediation_plans)}")
    print(f"  Outputs:")
    print(f"    {elements_csv}")
    print(f"    {summary_csv}")
    print(f"    {plans_json}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify elements in contaminated files against reference standards"
    )
    parser.add_argument(
        'clusters_csv',
        help="Path to cluster assignments CSV"
    )
    parser.add_argument(
        'reference_standards_json',
        help="Path to reference standards JSON"
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
        '--contamination-threshold',
        type=float,
        default=85.0,
        help="Files with internal similarity < this are considered contaminated (default: 85.0)"
    )
    parser.add_argument(
        '--out',
        default='contamination_reports',
        dest='out_dir',
        help="Output directory (default: contamination_reports)"
    )
    
    args = parser.parse_args()
    
    run_element_level_classification(
        clusters_csv=args.clusters_csv,
        reference_standards_json=args.reference_standards_json,
        exports_dir=args.exports_dir,
        domain=args.domain,
        contamination_threshold=args.contamination_threshold,
        out_dir=args.out_dir,
        phase0_dir=args.phase0_dir,
    )


if __name__ == '__main__':
    main()
