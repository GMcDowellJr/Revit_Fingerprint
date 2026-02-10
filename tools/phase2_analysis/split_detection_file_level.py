# tools/phase2_analysis/split_detection_file_level.py
"""File-level split detection via domain profile clustering."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

from .io import load_exports, get_domain_records
from .report import write_json_report
from .split_detection import (
    Cluster,
    extract_metadata_patterns,
    compute_silhouette_score,
    interpret_silhouette_score,
    cluster_assignments_to_labels,
    build_distance_matrix_from_similarity
)


def build_element_profiles(exports_dir: str, domain: str) -> Tuple[Dict, Dict]:
    """Build element signature profiles for each file.
    
    Returns:
        file_profiles: Dict[file_id -> {sig_hashes, path, count}]
        file_paths: Dict[file_id -> path]
    """
    
    exports = load_exports(exports_dir)
    
    file_profiles = {}
    file_paths = {}
    
    for export in exports:
        records = get_domain_records(export.data, domain)
        
        # Extract sig_hash values
        sig_hashes = {
            r.get('sig_hash') 
            for r in records 
            if r.get('sig_hash')
        }
        
        file_profiles[export.file_id] = {
            'sig_hashes': sig_hashes,
            'path': export.path,
            'element_count': len(records)
        }
        
        file_paths[export.file_id] = export.path
    
    return file_profiles, file_paths


def compute_pairwise_similarity(file_profiles: Dict) -> Dict[Tuple[str, str], float]:
    """Compute Jaccard similarity between all file pairs."""
    
    file_ids = sorted(file_profiles.keys())
    similarity_matrix = {}
    
    for i, fid_a in enumerate(file_ids):
        for j, fid_b in enumerate(file_ids):
            if i >= j:
                continue
            
            set_a = file_profiles[fid_a]['sig_hashes']
            set_b = file_profiles[fid_b]['sig_hashes']
            
            if not set_a or not set_b:
                jaccard = 0.0
            else:
                intersection = len(set_a & set_b)
                union = len(set_a | set_b)
                jaccard = intersection / union if union > 0 else 0.0
            
            similarity_matrix[(fid_a, fid_b)] = jaccard
    
    return similarity_matrix


def hierarchical_cluster_files(
    file_profiles: Dict,
    similarity_matrix: Dict[Tuple[str, str], float],
    threshold: float = 0.70
) -> List[Cluster]:
    """Perform hierarchical clustering on files."""
    
    file_ids = sorted(file_profiles.keys())
    n = len(file_ids)
    
    if n < 2:
        # Only one file - single cluster
        return [Cluster(
            cluster_id=0,
            members=file_ids,
            size=len(file_ids),
            avg_internal_similarity=1.0,
            metadata_patterns={}
        )]
    
    # Build distance matrix
    distance_matrix = np.zeros((n, n))
    
    for i, fid_a in enumerate(file_ids):
        for j, fid_b in enumerate(file_ids):
            if i == j:
                distance_matrix[i, j] = 0
            else:
                pair = tuple(sorted([fid_a, fid_b]))
                sim = similarity_matrix.get(pair, 0)
                distance_matrix[i, j] = 1 - sim
    
    # Hierarchical clustering
    try:
        condensed = squareform(distance_matrix)
        linkage_matrix = linkage(condensed, method='average')
        
        # Cut tree
        distance_threshold = 1 - threshold
        labels = fcluster(linkage_matrix, distance_threshold, criterion='distance')
    except Exception as e:
        sys.stderr.write(f"[WARN] Clustering failed: {e}. Treating as single cluster.\n")
        labels = np.ones(n, dtype=int)
    
    # Group files by cluster label
    clusters_dict = {}
    for fid, label in zip(file_ids, labels):
        if label not in clusters_dict:
            clusters_dict[label] = []
        clusters_dict[label].append(fid)
    
    # Build Cluster objects
    clusters = []
    for label, members in sorted(clusters_dict.items()):
        avg_sim = compute_avg_internal_similarity(members, similarity_matrix)
        
        clusters.append(Cluster(
            cluster_id=len(clusters),
            members=members,
            size=len(members),
            avg_internal_similarity=avg_sim,
            metadata_patterns={}  # Will be filled later
        ))
    
    return clusters


def compute_avg_internal_similarity(
    members: List[str],
    similarity_matrix: Dict[Tuple[str, str], float]
) -> float:
    """Compute average pairwise similarity within cluster."""
    
    if len(members) < 2:
        return 1.0
    
    similarities = []
    for i, fid_a in enumerate(members):
        for fid_b in members[i+1:]:
            pair = tuple(sorted([fid_a, fid_b]))
            sim = similarity_matrix.get(pair, 0)
            similarities.append(sim)
    
    return np.mean(similarities) if similarities else 0.0


def compute_avg_between_cluster_similarity(
    clusters: List[Cluster],
    similarity_matrix: Dict[Tuple[str, str], float]
) -> float:
    """Compute average similarity between different clusters."""
    
    if len(clusters) < 2:
        return 0.0
    
    between_sims = []
    
    for i, cluster_a in enumerate(clusters):
        for cluster_b in clusters[i+1:]:
            for fid_a in cluster_a.members:
                for fid_b in cluster_b.members:
                    pair = tuple(sorted([fid_a, fid_b]))
                    sim = similarity_matrix.get(pair, 0)
                    between_sims.append(sim)
    
    return np.mean(between_sims) if between_sims else 0.0


def select_cluster_representative(
    cluster: Cluster,
    similarity_matrix: Dict[Tuple[str, str], float]
) -> str:
    """Select most representative file (highest avg similarity to cluster)."""
    
    if len(cluster.members) == 1:
        return cluster.members[0]
    
    avg_sims = {}
    
    for member in cluster.members:
        sims = []
        for other in cluster.members:
            if member == other:
                continue
            pair = tuple(sorted([member, other]))
            sim = similarity_matrix.get(pair, 0)
            sims.append(sim)
        
        avg_sims[member] = np.mean(sims) if sims else 0
    
    return max(avg_sims, key=avg_sims.get)


def infer_standard_name(cluster: Cluster, metadata_patterns: Dict) -> str:
    """Infer human-readable name for standard."""
    
    # Try region
    region = metadata_patterns.get('likely_region', 'Unknown')
    if region != 'Unknown':
        return f"{region} Standard"
    
    # Try office
    office = metadata_patterns.get('likely_office', 'Unknown')
    if office != 'Unknown':
        return f"{office} Standard"
    
    # Fallback to cluster ID
    return f"Standard_{cluster.cluster_id + 1}"


def run_file_level_clustering(
    exports_dir: str,
    domain: str,
    threshold: float,
    out_dir: str
) -> None:
    """Run complete file-level clustering analysis."""
    
    print(f"[INFO] Building element profiles for {domain}...")
    file_profiles, file_paths = build_element_profiles(exports_dir, domain)
    
    if len(file_profiles) < 2:
        sys.stderr.write(f"[WARN] Only {len(file_profiles)} file(s) found. Cannot cluster.\n")
        return
    
    print(f"[INFO] Computing pairwise similarity ({len(file_profiles)} files)...")
    similarity_matrix = compute_pairwise_similarity(file_profiles)
    
    print(f"[INFO] Performing hierarchical clustering (threshold: {threshold})...")
    clusters = hierarchical_cluster_files(file_profiles, similarity_matrix, threshold)
    
    print(f"[INFO] Found {len(clusters)} cluster(s)")
    
    # Annotate clusters with metadata
    for cluster in clusters:
        cluster.metadata_patterns = extract_metadata_patterns(
            cluster.members,
            file_paths
        )
        cluster.representative_file = select_cluster_representative(
            cluster,
            similarity_matrix
        )
    
    # Assess cluster quality
    file_ids = sorted(file_profiles.keys())
    distance_matrix = build_distance_matrix_from_similarity(file_ids, similarity_matrix)
    labels = cluster_assignments_to_labels(clusters, file_ids)
    
    silhouette = compute_silhouette_score(distance_matrix, labels)
    interpretation = interpret_silhouette_score(silhouette)
    
    # Between-cluster similarity
    between_sim = compute_avg_between_cluster_similarity(clusters, similarity_matrix)
    
    # Output cluster assignments
    os.makedirs(out_dir, exist_ok=True)
    
    assignments = []
    for cluster in clusters:
        std_name = infer_standard_name(cluster, cluster.metadata_patterns)
        for fid in cluster.members:
            assignments.append({
                'file_id': fid,
                'cluster_id': cluster.cluster_id,
                'cluster_size': cluster.size,
                'standard_name': std_name,
                'internal_similarity': round(cluster.avg_internal_similarity, 3),
                'is_representative': (fid == cluster.representative_file)
            })
    
    assignments_df = pd.DataFrame(assignments)
    assignments_csv = os.path.join(out_dir, f"{domain}.file_clusters.csv")
    assignments_df.to_csv(assignments_csv, index=False)
    
    # Output cluster summary
    summary = []
    for cluster in clusters:
        std_name = infer_standard_name(cluster, cluster.metadata_patterns)
        summary.append({
            'cluster_id': cluster.cluster_id,
            'standard_name': std_name,
            'size': cluster.size,
            'percentage': round(100 * cluster.size / len(file_profiles), 1),
            'avg_internal_similarity': round(cluster.avg_internal_similarity, 3),
            'representative_file': cluster.representative_file,
            'likely_region': cluster.metadata_patterns.get('likely_region'),
            'likely_office': cluster.metadata_patterns.get('likely_office'),
            'date_range': cluster.metadata_patterns.get('date_range')
        })
    
    summary_df = pd.DataFrame(summary)
    summary_csv = os.path.join(out_dir, f"{domain}.cluster_summary.csv")
    summary_df.to_csv(summary_csv, index=False)
    
    # JSON report
    report = {
        'analysis': 'file_level_clustering',
        'domain': domain,
        'files_total': len(file_profiles),
        'clusters_found': len(clusters),
        'cluster_quality': {
            'silhouette_score': round(silhouette, 3),
            'interpretation': interpretation,
            'avg_between_cluster_similarity': round(between_sim, 3)
        },
        'clustering_parameters': {
            'threshold': threshold,
            'method': 'hierarchical_average_linkage'
        },
        'outputs': {
            'assignments_csv': os.path.abspath(assignments_csv),
            'summary_csv': os.path.abspath(summary_csv)
        }
    }
    
    report_path = os.path.join(out_dir, f"{domain}.file_clustering_report.json")
    write_json_report(out_path=report_path, report=report)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"FILE-LEVEL CLUSTERING RESULTS: {domain}")
    print(f"{'='*80}\n")
    
    print(f"Total files analyzed: {len(file_profiles)}")
    print(f"Clusters detected: {len(clusters)}")
    print(f"Cluster quality (silhouette): {silhouette:.3f} - {interpretation}")
    print(f"Avg between-cluster similarity: {between_sim:.3f}\n")
    
    for i, row in summary_df.iterrows():
        print(f"Cluster {row['cluster_id']}: \"{row['standard_name']}\"")
        print(f"  Size: {row['size']} files ({row['percentage']}%)")
        print(f"  Internal similarity: {row['avg_internal_similarity']:.3f}")
        print(f"  Region: {row['likely_region']}")
        print(f"  Representative: {row['representative_file']}\n")
    
    print(f"Outputs written:")
    print(f"  {assignments_csv}")
    print(f"  {summary_csv}")
    print(f"  {report_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect organizational splits via file-level clustering"
    )
    parser.add_argument(
        'exports_dir',
        help="Directory containing fingerprint exports (*.details.json)"
    )
    parser.add_argument(
        '--domain',
        required=True,
        help="Domain to analyze (e.g., dimension_types)"
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.70,
        help="Similarity threshold for clustering (default: 0.70)"
    )
    parser.add_argument(
        '--out',
        default='split_detection_out',
        dest='out_dir',
        help="Output directory (default: split_detection_out)"
    )
    
    args = parser.parse_args()
    
    run_file_level_clustering(
        exports_dir=args.exports_dir,
        domain=args.domain,
        threshold=args.threshold,
        out_dir=args.out_dir
    )


if __name__ == '__main__':
    main()