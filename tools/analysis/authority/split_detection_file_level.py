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

from .io import (
    load_exports,
    get_domain_records,
    load_phase0_v21_sig_profiles,
)
from .report import write_json_report
from .split_detection import (
    Cluster,
    extract_metadata_patterns,
    compute_silhouette_score,
    interpret_silhouette_score,
    cluster_assignments_to_labels,
    build_distance_matrix_from_similarity
)


def build_element_profiles(exports_dir: str, domain: str, *, phase0_dir: str | None = None) -> Tuple[Dict, Dict]:
    """Build element signature profiles for each file.

    Modes:
    - JSON mode (default): read fingerprint exports in exports_dir
    - CSV mode: if phase0_dir is provided, read v2.1 Phase0 tables from phase0_dir

    Returns:
        file_profiles: Dict[file_id/export_run_id -> {sig_hashes, path, count}]
        file_paths: Dict[file_id/export_run_id -> path]
    """
    if phase0_dir:
        return load_phase0_v21_sig_profiles(phase0_dir, domain)

    exports = load_exports(exports_dir)

    file_profiles = {}
    file_paths = {}

    for export in exports:
        records = get_domain_records(export.data, domain)

        sig_hashes = {r.get("sig_hash") for r in records if r.get("sig_hash")}

        file_profiles[export.file_id] = {
            "sig_hashes": sig_hashes,
            "path": export.path,
            "element_count": len(records),
        }

        file_paths[export.file_id] = export.path

    return file_profiles, file_paths


def compute_pairwise_similarity_candidates(file_profiles: Dict) -> Tuple[Dict[Tuple[str, str], float], Dict[str, int]]:
    """
    Compute exact Jaccard similarity only for file pairs that share >=1 sig_hash.

    Returns:
        similarity_matrix: Dict[(fid_a,fid_b) -> jaccard] for candidate pairs only (fid_a < fid_b)
        stats: candidate diagnostics (pairs_possible, pairs_evaluated, isolates, avg_candidates_per_file)
    """
    file_ids = sorted(file_profiles.keys())
    n = len(file_ids)

    # Inverted index: sig_hash -> list(file_id)
    inv: Dict[str, List[str]] = {}
    sig_counts_by_file: Dict[str, int] = {}

    for fid in file_ids:
        s = file_profiles[fid].get("sig_hashes") or set()
        sig_counts_by_file[fid] = len(s)
        for sig in s:
            if not sig:
                continue
            inv.setdefault(sig, []).append(fid)

    # Count shared tokens per pair from inverted index
    shared_counts: Dict[Tuple[str, str], int] = {}
    for sig, fids in inv.items():
        if len(fids) < 2:
            continue
        fids_sorted = sorted(fids)
        for i in range(len(fids_sorted) - 1):
            a = fids_sorted[i]
            for j in range(i + 1, len(fids_sorted)):
                b = fids_sorted[j]
                key = (a, b)  # already sorted
                shared_counts[key] = shared_counts.get(key, 0) + 1

    # Compute exact Jaccard for candidate pairs (shared >= 1)
    sim: Dict[Tuple[str, str], float] = {}
    for (a, b), _shared in shared_counts.items():
        set_a = file_profiles[a].get("sig_hashes") or set()
        set_b = file_profiles[b].get("sig_hashes") or set()

        if not set_a or not set_b:
            jaccard = 0.0
        else:
            inter = len(set_a & set_b)
            uni = len(set_a | set_b)
            jaccard = inter / uni if uni > 0 else 0.0

        if jaccard > 0.0:
            sim[(a, b)] = float(jaccard)

    pairs_possible = n * (n - 1) // 2
    pairs_evaluated = len(shared_counts)
    files_with_any_candidate = _files_with_any_candidate(shared_counts)
    isolates = sum(1 for fid in file_ids if sig_counts_by_file.get(fid, 0) > 0 and fid not in files_with_any_candidate)
    avg_candidates = (2 * pairs_evaluated / n) if n else 0.0

    stats = {
        "pairs_possible": int(pairs_possible),
        "pairs_evaluated": int(pairs_evaluated),
        "pairs_skipped": int(pairs_possible - pairs_evaluated),
        "isolates": int(isolates),
        "avg_candidates_per_file": float(avg_candidates),
    }
    return sim, stats


def _files_with_any_candidate(shared_counts: Dict[Tuple[str, str], int]) -> Set[str]:
    out: Set[str] = set()
    for (a, b) in shared_counts.keys():
        out.add(a)
        out.add(b)
    return out


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


def threshold_graph_cluster_files(
    file_profiles: Dict,
    similarity_matrix: Dict[Tuple[str, str], float],
    threshold: float = 0.70,
) -> List[Cluster]:
    """
    Cluster files as connected components of a similarity-threshold graph.

    Edge (a,b) exists iff Jaccard(a,b) >= threshold.

    Notes:
    - Deterministic: stable file_id sorting and component ordering.
    - Pairs missing in similarity_matrix are treated as similarity 0.0.
    """
    file_ids = sorted(file_profiles.keys())
    n = len(file_ids)

    if n < 2:
        return [Cluster(
            cluster_id=0,
            members=file_ids,
            size=len(file_ids),
            avg_internal_similarity=1.0,
            metadata_patterns={},
        )]

    # Union-Find
    parent: Dict[str, str] = {fid: fid for fid in file_ids}
    rank: Dict[str, int] = {fid: 0 for fid in file_ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra = find(a)
        rb = find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    # Add edges for pairs meeting threshold
    # similarity_matrix keys are stored as (min,max)
    for (a, b), sim in similarity_matrix.items():
        if sim >= threshold:
            union(a, b)

    # Group by root
    groups: Dict[str, List[str]] = {}
    for fid in file_ids:
        r = find(fid)
        groups.setdefault(r, []).append(fid)

    # Deterministic cluster ordering:
    # order by (size desc, first_member asc)
    comps = []
    for root, members in groups.items():
        members_sorted = sorted(members)
        comps.append((members_sorted, root))
    comps.sort(key=lambda t: (-len(t[0]), t[0][0].lower()))

    clusters: List[Cluster] = []
    for members_sorted, _root in comps:
        avg_sim = compute_avg_internal_similarity(members_sorted, similarity_matrix)
        clusters.append(Cluster(
            cluster_id=len(clusters),
            members=members_sorted,
            size=len(members_sorted),
            avg_internal_similarity=avg_sim,
            metadata_patterns={},
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
    out_dir: str,
    *,
    phase0_dir: str | None = None,
    mode: str = "allpairs",
) -> None:
    """Run complete file-level clustering analysis."""
    
    print(f"[INFO] Building element profiles for {domain}...")
    file_profiles, file_paths = build_element_profiles(exports_dir, domain, phase0_dir=phase0_dir)
    
    if len(file_profiles) < 2:
        sys.stderr.write(f"[WARN] Only {len(file_profiles)} file(s) found. Cannot cluster.\n")
        return
    
    candidate_stats = None

    if mode == "candidates":
        print(f"[INFO] Computing candidate-pair similarity ({len(file_profiles)} files).")
        similarity_matrix, candidate_stats = compute_pairwise_similarity_candidates(file_profiles)

        print(f"[INFO] Clustering by threshold-graph components (threshold: {threshold}).")
        clusters = threshold_graph_cluster_files(file_profiles, similarity_matrix, threshold)
    else:
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
    labels = cluster_assignments_to_labels(clusters, file_ids)

    if mode == "candidates":
        # Avoid O(F^2) distance matrix construction in candidates mode.
        silhouette = 0.0
        interpretation = "not_computed_candidates_mode"
    else:
        distance_matrix = build_distance_matrix_from_similarity(file_ids, similarity_matrix)
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
            'mode': mode,
            'method': ('threshold_graph_components' if mode == 'candidates' else 'hierarchical_average_linkage'),
            **({'candidate_stats': candidate_stats} if candidate_stats else {}),
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
        help="Directory containing fingerprint exports (*.json). Ignored if --phase0-dir is provided."
    )
    parser.add_argument(
        '--phase0-dir',
        dest='phase0_dir',
        default=None,
        help="If provided, read v2.1 Phase0 tables from this directory (out/current/flatten)."
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
        '--mode',
        choices=['allpairs', 'candidates'],
        default='allpairs',
        help=(
            "Similarity evaluation mode. "
            "'allpairs' computes all file pairs (O(F^2)). "
            "'candidates' computes exact Jaccard only for pairs sharing >=1 sig_hash, "
            "then clusters by threshold graph components (much faster for large F)."
        ),
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
        out_dir=args.out_dir,
        phase0_dir=args.phase0_dir,
        mode=args.mode,
    )


if __name__ == '__main__':
    main()
