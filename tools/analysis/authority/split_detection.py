# tools/phase2_analysis/split_detection.py
"""Core split detection functionality.

This module provides utilities for detecting organizational splits
(multiple standards) in fingerprint data.
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict


@dataclass
class Cluster:
    """Represents a cluster of files following the same standard."""
    
    cluster_id: int
    members: List[str]  # file_ids
    size: int
    avg_internal_similarity: float
    metadata_patterns: Dict[str, Any]
    representative_file: Optional[str] = None
    domain_hash: Optional[str] = None


@dataclass
class SplitSignal:
    """Evidence of a potential split."""
    
    signal_type: str  # 'pareto_cliff', 'collision_pair', 'file_clustering'
    confidence: float  # 0-1
    details: Dict[str, Any]
    interpretation: str


def extract_metadata_patterns(file_ids: List[str], file_paths: Dict[str, str]) -> Dict[str, Any]:
    """Extract common patterns from file paths.
    
    Args:
        file_ids: List of file IDs in cluster
        file_paths: Mapping of file_id -> full path
        
    Returns:
        Dictionary of detected patterns
    """
    
    paths = [file_paths.get(fid, '') for fid in file_ids if fid in file_paths]
    
    if not paths:
        return {
            'common_path_parts': [],
            'likely_region': 'Unknown',
            'likely_office': 'Unknown',
            'date_range': None
        }
    
    # Extract common substrings
    common_parts = find_common_path_components(paths)
    
    # Infer region
    region = infer_region_from_paths(paths)
    
    # Infer office
    office = infer_office_from_paths(paths)
    
    # Date range (if extractable from paths)
    dates = extract_dates_from_paths(paths)
    date_range = f"{min(dates)} to {max(dates)}" if dates else None
    
    return {
        'common_path_parts': common_parts,
        'likely_region': region,
        'likely_office': office,
        'date_range': date_range,
        'sample_paths': paths[:3]
    }


def find_common_path_components(paths: List[str]) -> List[str]:
    """Find path components that appear in >50% of paths."""
    
    # Split paths into components
    all_components = []
    for path in paths:
        # Split by both / and \
        components = path.replace('\\', '/').split('/')
        all_components.extend([c.lower() for c in components if c])
    
    # Count occurrences
    counter = Counter(all_components)
    threshold = len(paths) * 0.5
    
    # Return components appearing in >50% of paths
    common = [comp for comp, count in counter.items() if count >= threshold]
    
    return sorted(common, key=lambda x: counter[x], reverse=True)


def infer_region_from_paths(paths: List[str]) -> str:
    """Heuristically detect region from file paths."""
    
    region_indicators = {
        'West Coast': ['west', 'ca', 'california', 'seattle', 'portland', 'pacific', 'sf', 'san francisco'],
        'East Coast': ['east', 'ny', 'newyork', 'boston', 'philadelphia', 'atlantic', 'dc'],
        'Central': ['central', 'chicago', 'midwest', 'tx', 'texas', 'dallas', 'houston'],
        'South': ['south', 'atlanta', 'miami', 'fl', 'florida', 'ga', 'georgia'],
        'International': ['uk', 'canada', 'europe', 'asia', 'international', 'global']
    }
    
    region_scores = {region: 0 for region in region_indicators}
    
    for path in paths:
        path_lower = path.lower()
        for region, keywords in region_indicators.items():
            if any(kw in path_lower for kw in keywords):
                region_scores[region] += 1
    
    # Return region with highest score if >50% of paths
    max_score = max(region_scores.values()) if region_scores else 0
    if max_score > len(paths) * 0.5:
        return max(region_scores, key=region_scores.get)
    
    return "Unknown"


def infer_office_from_paths(paths: List[str]) -> str:
    """Extract office/location from paths."""
    
    # Look for common office patterns
    office_patterns = [
        'office', 'studio', 'branch', 'location', 'site'
    ]
    
    office_candidates = []
    
    for path in paths:
        parts = path.replace('\\', '/').split('/')
        for i, part in enumerate(parts):
            if any(pattern in part.lower() for pattern in office_patterns):
                # The office name might be this part or adjacent parts
                office_candidates.append(part)
                if i > 0:
                    office_candidates.append(parts[i-1])
                if i < len(parts) - 1:
                    office_candidates.append(parts[i+1])
    
    if office_candidates:
        # Return most common
        counter = Counter(office_candidates)
        return counter.most_common(1)[0][0]
    
    return "Unknown"


def extract_dates_from_paths(paths: List[str]) -> List[str]:
    """Extract dates from file paths."""
    
    import re
    
    dates = []
    
    # Pattern: YYYY-MM-DD or YYYY_MM_DD or YYYYMMDD
    date_pattern = r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})'
    
    for path in paths:
        matches = re.findall(date_pattern, path)
        for match in matches:
            year, month, day = match
            dates.append(f"{year}-{month}-{day}")
    
    return sorted(set(dates))


def compute_silhouette_score(distance_matrix: np.ndarray, labels: np.ndarray) -> float:
    """Compute silhouette score for cluster quality assessment.
    
    Args:
        distance_matrix: Pairwise distance matrix
        labels: Cluster assignment for each item
        
    Returns:
        Silhouette score (range: -1 to 1, higher is better)
    """
    
    from sklearn.metrics import silhouette_score
    
    try:
        score = silhouette_score(distance_matrix, labels, metric='precomputed')
        return float(score)
    except Exception:
        return 0.0


def interpret_silhouette_score(score: float) -> str:
    """Human-readable interpretation of silhouette score."""
    
    if score > 0.7:
        return "Strong, well-separated clusters"
    elif score > 0.5:
        return "Clear clusters with good separation"
    elif score > 0.25:
        return "Moderate clusters with some overlap"
    else:
        return "Weak clusters - may be noise or single population"


def cluster_assignments_to_labels(clusters: List[Cluster], all_file_ids: List[str]) -> np.ndarray:
    """Convert cluster assignments to label array for silhouette calculation."""
    
    file_to_label = {}
    for cluster in clusters:
        for fid in cluster.members:
            file_to_label[fid] = cluster.cluster_id
    
    # Files not in any cluster get label -1
    labels = [file_to_label.get(fid, -1) for fid in all_file_ids]
    
    return np.array(labels)


def build_distance_matrix_from_similarity(
    file_ids: List[str],
    similarity_matrix: Dict[Tuple[str, str], float]
) -> np.ndarray:
    """Convert similarity dictionary to distance matrix."""
    
    n = len(file_ids)
    distance_matrix = np.zeros((n, n))
    
    file_to_idx = {fid: i for i, fid in enumerate(file_ids)}
    
    for i, fid_a in enumerate(file_ids):
        for j, fid_b in enumerate(file_ids):
            if i == j:
                distance_matrix[i, j] = 0
            else:
                pair = tuple(sorted([fid_a, fid_b]))
                sim = similarity_matrix.get(pair, 0)
                distance_matrix[i, j] = 1 - sim
    
    return distance_matrix