 # tools/patterns_analysis/pareto_with_splits.py
"""Enhanced Pareto analysis with automatic split detection."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Import from existing pareto tool (modify path as needed)
sys.path.insert(0, str(Path(__file__).parent.parent))
from pareto_joinkey_search import run_pareto_analysis


def detect_pareto_cliffs(pareto_front_df: pd.DataFrame, min_cliff_drop: float = 0.15) -> List[Dict]:
    """Detect cliffs in Pareto front that indicate splits."""
    
    cliffs = []
    
    for i in range(len(pareto_front_df) - 1):
        current = pareto_front_df.iloc[i]
        next_point = pareto_front_df.iloc[i + 1]
        
        collision_drop = current['collision_rate'] - next_point['collision_rate']
        frag_increase = next_point['num_variants'] - current['num_variants']
        
        if collision_drop > min_cliff_drop and frag_increase < 20:
            # Try to extract added property
            current_keys = set(str(current.get('join_key', '')).split('+'))
            next_keys = set(str(next_point.get('join_key', '')).split('+'))
            
            added = next_keys - current_keys
            added_property = list(added)[0] if added else 'unknown'
            
            cliff_ratio = collision_drop / (frag_increase + 1)
            
            cliffs.append({
                'sequence_order': len(cliffs) + 1,
                'from_join_key': str(current.get('join_key', '')),
                'to_join_key': str(next_point.get('join_key', '')),
                'from_collision': float(current['collision_rate']),
                'to_collision': float(next_point['collision_rate']),
                'collision_drop': float(collision_drop),
                'fragmentation_increase': int(frag_increase),
                'cliff_ratio': float(cliff_ratio),
                'discriminator_property': added_property
            })
    
    return sorted(cliffs, key=lambda x: x['cliff_ratio'], reverse=True)


def assess_split_likelihood(cliffs: List[Dict]) -> Dict:
    """Assess likelihood of organizational split based on Pareto cliffs."""
    
    evidence_score = 0.0
    evidence_details = []
    
    if not cliffs:
        return {
            'likely': False,
            'confidence': 0.0,
            'evidence_score': 0.0,
            'evidence_details': ['No Pareto cliffs detected'],
            'recommendation': 'No split signal - treat as single population'
        }
    
    # Check top cliff
    top_cliff = cliffs[0]
    
    if top_cliff['cliff_ratio'] > 10:
        evidence_score += 0.5
        evidence_details.append(f"Strong Pareto cliff (ratio: {top_cliff['cliff_ratio']:.1f})")
    elif top_cliff['cliff_ratio'] > 5:
        evidence_score += 0.3
        evidence_details.append(f"Moderate Pareto cliff (ratio: {top_cliff['cliff_ratio']:.1f})")
    else:
        evidence_score += 0.1
        evidence_details.append(f"Weak Pareto cliff (ratio: {top_cliff['cliff_ratio']:.1f})")
    
    # Multiple cliffs suggest multiple splits
    if len(cliffs) >= 3:
        evidence_score += 0.2
        evidence_details.append(f"{len(cliffs)} cliffs detected (possible {len(cliffs)+1}-way split)")
    elif len(cliffs) == 2:
        evidence_score += 0.15
        evidence_details.append("2 cliffs detected (possible 3-way split)")
    
    # Assess confidence
    if evidence_score >= 0.6:
        likely = True
        confidence = min(evidence_score, 0.95)
        recommendation = "Run file-level clustering to identify groups"
    elif evidence_score >= 0.3:
        likely = True
        confidence = evidence_score
        recommendation = "Possible split - investigate with file-level clustering"
    else:
        likely = False
        confidence = 1 - evidence_score
        recommendation = "No strong split signal - treat as single population"
    
    return {
        'likely': likely,
        'confidence': confidence,
        'evidence_score': evidence_score,
        'evidence_details': evidence_details,
        'recommendation': recommendation,
        'estimated_num_standards': len(cliffs) + 1 if likely else 1
    }


def run_pareto_with_split_detection(
    records_csv: str,
    items_csv: str,
    domain: str,
    out_dir: str,
    **pareto_kwargs
) -> None:
    """Run Pareto analysis with automatic split detection."""
    
    print(f"[INFO] Running Pareto analysis for {domain}...")
    
    # Run standard Pareto analysis
    # Note: You'll need to adapt this to call your existing pareto_joinkey_search
    pareto_results = run_pareto_analysis(
        records_csv=records_csv,
        items_csv=items_csv,
        domain=domain,
        out_dir=out_dir,
        **pareto_kwargs
    )
    
    # Load Pareto front results
    pareto_csv = os.path.join(out_dir, f"pareto_front_{domain}.csv")
    
    if not os.path.exists(pareto_csv):
        sys.stderr.write(f"[WARN] Pareto front CSV not found: {pareto_csv}\n")
        return
    
    pareto_df = pd.read_csv(pareto_csv)
    
    # Detect cliffs
    print(f"[INFO] Detecting split signatures in Pareto front...")
    cliffs = detect_pareto_cliffs(pareto_df)
    
    # Assess split likelihood
    assessment = assess_split_likelihood(cliffs)
    
    # Output split detection report
    split_report = {
        'domain': domain,
        'split_likely': assessment['likely'],
        'confidence': assessment['confidence'],
        'estimated_num_standards': assessment['estimated_num_standards'],
        'cliffs': cliffs,
        'assessment': assessment
    }
    
    os.makedirs(out_dir, exist_ok=True)
    
    split_json = os.path.join(out_dir, f"{domain}.split_detection.json")
    with open(split_json, 'w') as f:
        json.dump(split_report, f, indent=2)
    
    # Print results
    print(f"\n{'='*80}")
    print(f"SPLIT DETECTION RESULTS: {domain}")
    print(f"{'='*80}\n")
    
    print(f"Split likely: {'YES' if assessment['likely'] else 'NO'}")
    print(f"Confidence: {assessment['confidence']:.0%}")
    print(f"Estimated standards: {assessment['estimated_num_standards']}")
    print(f"\nEvidence:")
    for detail in assessment['evidence_details']:
        print(f"  - {detail}")
    
    if cliffs:
        print(f"\nDetected cliffs:")
        for cliff in cliffs[:3]:  # Show top 3
            print(f"  Cliff {cliff['sequence_order']}: {cliff['discriminator_property']}")
            print(f"    Collision drop: {cliff['from_collision']:.1%} → {cliff['to_collision']:.1%}")
            print(f"    Fragmentation: +{cliff['fragmentation_increase']} variants")
    
    print(f"\nRecommendation: {assessment['recommendation']}")
    print(f"\nSplit detection report: {split_json}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pareto analysis with automatic split detection"
    )
    parser.add_argument('records_csv', help="Path to records.csv")
    parser.add_argument('items_csv', help="Path to identity_items.csv")
    parser.add_argument('--domain', required=True, help="Domain to analyze")
    parser.add_argument('--out', default='pareto_split_out', dest='out_dir')
    parser.add_argument('--max-k', type=int, default=4, help="Max properties in join key")
    
    args = parser.parse_args()
    
    run_pareto_with_split_detection(
        records_csv=args.records_csv,
        items_csv=args.items_csv,
        domain=args.domain,
        out_dir=args.out_dir,
        max_k=args.max_k
    )


if __name__ == '__main__':
    main()