"""Cross-segment comparison tool.

Compares pattern vocabularies across segments using join_hash as the
cross-segment identity unit.

Single measurement path
-----------------------
All comparisons load per-file join_hash inventories from membership_matrix.csv
and resolve join_hash via domain_patterns.csv (source_cluster_id.split('|')[-1]).
There is no bundle-mode / file-mode branch. All set operations (Jaccard,
containment) operate on the full join_hash inventories from membership_matrix.csv.

Bundle membership as post-hoc annotation
-----------------------------------------
After computing scores, bundle membership is looked up from
bundle_analysis/<domain>/bundle_membership.csv for each segment and annotated
onto n_shared using three buckets:

  n_shared_bundle_both   — join_hashes in shared that are bundle members in BOTH
                           segments (core overlap, strongest signal)
  n_shared_bundle_a_only — bundle member in A, not B (A has institutionalised the
                           pattern; the "emerging standard" signal)
  n_shared_bundle_b_only — bundle member in B, not A (same signal, other direction)

The remainder of n_shared are shared but unstructured (weakest signal). Segments
without bundle_membership.csv (reference or small segments) contribute an empty
set — annotation counts are always emitted, with zeros when data is absent.

N-1 pooled comparison (cross_segment_pooled.csv)
-------------------------------------------------
Each segment is compared against the union of all sibling segments sharing the
same (parent_segment_id, governance_role, unit_system). This is the primary
signal for small segments where pairwise Jaccard is dominated by size asymmetry.
Containment in both directions is reported; no Jaccard is computed on this file.

data_sufficient flag
--------------------
Scores are always computed and emitted. data_sufficient = "true" only when both
sides have n_files >= 5. The flag signals interpretability, not validity.

Reference segment participation
--------------------------------
Reference segments participate in template_to_project, template_to_container, and
container_to_project comparisons using their file inventories from
membership_matrix.csv. They will have has_bundles = "false" and
data_sufficient = "false" for most domains — this is expected and correct.

Usage:
    python tools/compare_cross_segment.py \\
        --segments-root segments/ \\
        --records-dir   results/records/ \\
        --out-dir       results/cross_segment/ \\
        [--within-segment] [--sibling-segments] [--parent-siblings] \\
        [--within-project] [--governance-chain] \\
        [--domain DOMAIN] [--segment-a ID] [--segment-b ID] \\
        [--min-patterns INT] [--dry-run] [--no-delta]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [
            {str(k): ("" if v is None else str(v)) for k, v in row.items()}
            for row in csv.DictReader(f)
        ]


def atomic_write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False,
        dir=str(path.parent), suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

SUMMARY_FIELDS: List[str] = [
    "comparison_run_id",
    "segment_id_a", "segment_id_b",
    "segment_label_a", "segment_label_b",
    "governance_role_a", "governance_role_b",
    "client_label_a", "client_label_b",
    "discipline_label_a", "discipline_label_b",
    "unit_system",
    "comparison_type",
    "domain",
    "n_patterns_a", "n_patterns_b", "n_shared_join_hash",
    "n_unique_patterns_a", "n_unique_patterns_b",
    "containment_a_in_b_mean", "containment_a_in_b_min",
    "containment_b_in_a_mean", "containment_b_in_a_min",
    "jaccard_mean", "jaccard_p10", "jaccard_p90",
    "has_bundles_a", "has_bundles_b",
    "n_shared_bundle_both", "n_shared_bundle_a_only", "n_shared_bundle_b_only",
    "n_files_a", "n_files_b", "n_pairs",
    "data_sufficient",
    "executed_utc",
]

PAIRS_FIELDS: List[str] = [
    "comparison_run_id",
    "segment_id_a", "segment_id_b",
    "domain",
    "export_run_id_a", "export_run_id_b",
    "project_label_a", "project_label_b",
    "n_patterns_a", "n_patterns_b", "n_shared",
    "jaccard", "containment_a_in_b", "containment_b_in_a",
    "n_shared_bundle_both", "n_shared_bundle_a_only", "n_shared_bundle_b_only",
]

DELTA_FIELDS: List[str] = [
    "comparison_run_id",
    "segment_id_reference", "segment_id_target",
    "segment_label_reference", "segment_label_target",
    "comparison_type", "domain",
    "join_hash",
    "pattern_label",
    "n_files_in_target",
    "pct_files_in_target",
    "in_any_container",
    "in_any_template",
    "executed_utc",
]

POOLED_FIELDS: List[str] = [
    "comparison_run_id",
    "segment_id", "segment_label",
    "governance_role", "client_label",
    "unit_system",
    "domain",
    "n_files_focal", "n_files_pool",
    "n_unique_patterns_focal", "n_unique_patterns_pool", "n_shared_join_hash",
    "containment_focal_in_pool", "containment_pool_in_focal",
    "has_bundles_focal", "has_bundles_pool",
    "n_shared_bundle_both", "n_shared_bundle_focal_only", "n_shared_bundle_pool_only",
    "data_sufficient",
    "executed_utc",
]

# Comparison types for which delta rows are emitted (directed, reference side defined).
DELTA_DIRECTED_TYPES = {
    "template_to_project",
    "template_to_container",
    "container_to_project",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_manifest(records_dir: Path) -> Dict[str, Dict[str, str]]:
    path = records_dir / "segment_manifest.csv"
    if not path.exists():
        sys.exit(f"[error] segment_manifest.csv not found at {path}")
    return {row["segment_id"]: row for row in read_csv_rows(path)}


def load_registry(records_dir: Path) -> Dict[str, Dict[str, str]]:
    path = records_dir / "run_registry.csv"
    if not path.exists():
        sys.exit(f"[error] run_registry.csv not found at {path}")
    return {row["segment_id"]: row for row in read_csv_rows(path)}


def load_file_metadata(records_dir: Path) -> Dict[str, Dict[str, str]]:
    path = records_dir / "file_metadata.csv"
    if not path.exists():
        print(f"[warn] file_metadata.csv not found at {path}", file=sys.stderr)
        return {}
    return {row["export_run_id"]: row for row in read_csv_rows(path)}


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def segment_output_dir(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
) -> Optional[Path]:
    rec = registry.get(segment_id)
    if rec is None:
        return None
    folder = rec.get("output_folder", "").strip()
    if not folder:
        return None
    return segments_root / folder


def bundle_analysis_dir(seg_out: Path, domain: str) -> Path:
    return seg_out / "results" / "bundle_analysis" / domain


def domain_patterns_path(seg_out: Path) -> Path:
    return seg_out / "results" / "analysis" / "domain_patterns.csv"


# ---------------------------------------------------------------------------
# Domain discovery
# ---------------------------------------------------------------------------

def discover_domains_for_segment(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
) -> Set[str]:
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return set()
    ba_root = seg_out / "results" / "bundle_analysis"
    if not ba_root.exists():
        return set()
    return {p.name for p in ba_root.iterdir() if p.is_dir()}


# ---------------------------------------------------------------------------
# join_hash resolution cache
# ---------------------------------------------------------------------------

# Cache: (segment_id, domain) -> {pattern_id: join_hash}
_jh_cache: Dict[Tuple[str, str], Dict[str, str]] = {}

# Cache: (segment_id, domain) -> {join_hash: human_label}
_pattern_label_cache: Dict[Tuple[str, str], Dict[str, str]] = {}

# Cache: (governance_role, domain, unit_system) -> Set[join_hash]
_role_jh_cache: Dict[Tuple[str, str, str, str], Set[str]] = {}

# Cache: (segment_id, domain) -> Set[join_hash]  (bundle members only)
_bundle_jh_cache: Dict[Tuple[str, str], Set[str]] = {}


def resolve_join_hashes(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Dict[str, str]:
    key = (segment_id, domain)
    if key in _jh_cache:
        return _jh_cache[key]

    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        _jh_cache[key] = {}
        return {}

    dp_path = domain_patterns_path(seg_out)
    if not dp_path.exists():
        _jh_cache[key] = {}
        return {}

    result: Dict[str, str] = {}
    for row in read_csv_rows(dp_path):
        if row.get("domain", "") != domain:
            continue
        pid = row.get("pattern_id", "").strip()
        scid = row.get("source_cluster_id", "").strip()
        if not pid:
            continue
        if not scid:
            print(
                f"[warn] segment={segment_id} domain={domain} pattern_id={pid} "
                "has blank source_cluster_id — skipped",
                file=sys.stderr,
            )
            continue
        result[pid] = scid.split("|")[-1]

    _jh_cache[key] = result
    return result


def load_pattern_labels(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Dict[str, str]:
    """Return {join_hash: label} from the segment's domain_patterns.csv.

    Prefers pattern_label_human; falls back to pattern_label; else empty string.
    """
    key = (segment_id, domain)
    if key in _pattern_label_cache:
        return _pattern_label_cache[key]

    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        _pattern_label_cache[key] = {}
        return {}

    dp_path = domain_patterns_path(seg_out)
    if not dp_path.exists():
        _pattern_label_cache[key] = {}
        return {}

    result: Dict[str, str] = {}
    for row in read_csv_rows(dp_path):
        if row.get("domain", "") != domain:
            continue
        scid = row.get("source_cluster_id", "").strip()
        if not scid:
            continue
        jh = scid.split("|")[-1]
        label = (
            row.get("pattern_label_human", "").strip()
            or row.get("pattern_label", "").strip()
        )
        result[jh] = label

    _pattern_label_cache[key] = result
    return result


def get_role_jh_set(
    role: str,
    domain: str,
    unit_system: str,
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    segments_root: Path,
    exclude_segment_id: str = "",
) -> Set[str]:
    """Return the union of all join_hashes present in segments with the given role.

    Built once per (role, domain, unit_system, exclude_segment_id) and cached
    for the run lifetime. Segments with run_type skip/registration are silently
    excluded. Pass exclude_segment_id to omit a specific segment from the union
    (used when the target segment is itself the role being looked up).
    """
    cache_key = (role, domain, unit_system, exclude_segment_id)
    if cache_key in _role_jh_cache:
        return _role_jh_cache[cache_key]

    result: Set[str] = set()
    for sid, mrow in manifest.items():
        if sid == exclude_segment_id:
            continue
        if mrow.get("governance_role", "").strip().lower() != role:
            continue
        if mrow.get("unit_system", "").strip() != unit_system:
            continue
        rt = registry.get(sid, {}).get("run_type", "").strip().lower()
        if rt in ("skip", "registration"):
            continue
        seg_out = segment_output_dir(segments_root, registry, sid)
        if seg_out is None:
            continue
        mm_path = bundle_analysis_dir(seg_out, domain) / "membership_matrix.csv"
        if not mm_path.exists():
            continue
        jh_map = resolve_join_hashes(segments_root, registry, sid, domain)
        for row in read_csv_rows(mm_path):
            pid = row.get("pattern_id", "").strip()
            if pid:
                jh = jh_map.get(pid)
                if jh:
                    result.add(jh)

    _role_jh_cache[cache_key] = result
    return result


# ---------------------------------------------------------------------------
# Membership loading
# ---------------------------------------------------------------------------

def load_file_join_hashes(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Dict[str, Set[str]]:
    """Return {export_run_id: set_of_join_hashes} from membership_matrix.csv."""
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return {}

    mm_path = bundle_analysis_dir(seg_out, domain) / "membership_matrix.csv"
    if not mm_path.exists():
        return {}

    jh_map = resolve_join_hashes(segments_root, registry, segment_id, domain)
    result: Dict[str, Set[str]] = defaultdict(set)
    for row in read_csv_rows(mm_path):
        eid = row.get("export_run_id", "").strip()
        pid = row.get("pattern_id", "").strip()
        if not eid or not pid:
            continue
        jh = jh_map.get(pid)
        if jh:
            result[eid].add(jh)
    return dict(result)


def load_bundle_join_hash_set(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Set[str]:
    """Return join_hashes that are bundle members for segment/domain.

    Empty set if bundle_membership.csv absent (reference or small segments).
    Path: {segment_output_folder}/results/bundle_analysis/{domain}/bundle_membership.csv
    Resolves pattern_id → join_hash via domain_patterns.csv.
    """
    key = (segment_id, domain)
    if key in _bundle_jh_cache:
        return _bundle_jh_cache[key]

    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        _bundle_jh_cache[key] = set()
        return set()

    bm_path = bundle_analysis_dir(seg_out, domain) / "bundle_membership.csv"
    if not bm_path.exists():
        _bundle_jh_cache[key] = set()
        return set()

    jh_map = resolve_join_hashes(segments_root, registry, segment_id, domain)
    result: Set[str] = set()
    for row in read_csv_rows(bm_path):
        pid = row.get("pattern_id", "").strip()
        if not pid:
            continue
        jh = jh_map.get(pid)
        if jh:
            result.add(jh)

    _bundle_jh_cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Bundle annotation
# ---------------------------------------------------------------------------

def annotate_bundle_overlap(
    shared_jhs: Set[str],
    bundle_jhs_a: Set[str],
    bundle_jhs_b: Set[str],
) -> Tuple[int, int, int]:
    """Return (n_both, n_a_only, n_b_only) for shared join_hashes."""
    n_both = len(shared_jhs & bundle_jhs_a & bundle_jhs_b)
    n_a_only = len(shared_jhs & bundle_jhs_a - bundle_jhs_b)
    n_b_only = len(shared_jhs & bundle_jhs_b - bundle_jhs_a)
    return n_both, n_a_only, n_b_only


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------

def _pct(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    xs_sorted = sorted(xs)
    idx = (len(xs_sorted) - 1) * p / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(xs_sorted) - 1)
    frac = idx - lo
    return xs_sorted[lo] * (1 - frac) + xs_sorted[hi] * frac


def _fmt(v: float) -> str:
    return f"{v:.6f}"


def _mean(xs: List[float]) -> str:
    return _fmt(sum(xs) / len(xs)) if xs else ""


def _min(xs: List[float]) -> str:
    return _fmt(min(xs)) if xs else ""


# ---------------------------------------------------------------------------
# Comparison engine — directed (containment)
# ---------------------------------------------------------------------------

def compare_directed_file(
    ref_files: Dict[str, Set[str]],
    tgt_files: Dict[str, Set[str]],
) -> Dict[str, str]:
    ref_union: Set[str] = set()
    for jhs in ref_files.values():
        ref_union |= jhs

    if not ref_union:
        return {}

    b_in_a: List[float] = []
    a_in_b: List[float] = []

    for jhs in tgt_files.values():
        shared = len(jhs & ref_union)
        b_in_a.append(shared / len(ref_union))
        a_in_b.append(shared / len(jhs) if jhs else 0.0)

    all_b: Set[str] = set()
    for jhs in tgt_files.values():
        all_b |= jhs

    return {
        "n_shared_join_hash": str(len(ref_union & all_b)),
        "containment_a_in_b_mean": _mean(a_in_b),
        "containment_a_in_b_min": _min(a_in_b),
        "containment_b_in_a_mean": _mean(b_in_a),
        "containment_b_in_a_min": _min(b_in_a),
        "n_files_a": str(len(ref_files)),
        "n_files_b": str(len(tgt_files)),
        "n_pairs": str(len(tgt_files)),
    }


# ---------------------------------------------------------------------------
# Comparison engine — symmetric (Jaccard + containment)
# ---------------------------------------------------------------------------

def compare_symmetric_file(
    files_a: Dict[str, Set[str]],
    files_b: Dict[str, Set[str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """Return (summary_metrics, pairwise_rows).

    Containment is computed per file pair in both directions and aggregated to
    mean/min for the summary — these columns are always populated regardless of
    comparison type.
    """
    jaccards: List[float] = []
    c_ab_list: List[float] = []
    c_ba_list: List[float] = []
    pair_rows: List[Dict[str, str]] = []

    for eid_a, jhs_a in files_a.items():
        for eid_b, jhs_b in files_b.items():
            union = jhs_a | jhs_b
            j = len(jhs_a & jhs_b) / len(union) if union else 0.0
            c_ab = len(jhs_a & jhs_b) / len(jhs_a) if jhs_a else 0.0
            c_ba = len(jhs_a & jhs_b) / len(jhs_b) if jhs_b else 0.0
            jaccards.append(j)
            c_ab_list.append(c_ab)
            c_ba_list.append(c_ba)
            pair_rows.append({
                "export_run_id_a": eid_a,
                "export_run_id_b": eid_b,
                "n_patterns_a": str(len(jhs_a)),
                "n_patterns_b": str(len(jhs_b)),
                "n_shared": str(len(jhs_a & jhs_b)),
                "jaccard": _fmt(j),
                "containment_a_in_b": _fmt(c_ab),
                "containment_b_in_a": _fmt(c_ba),
            })

    all_a: Set[str] = set()
    for jhs in files_a.values():
        all_a |= jhs
    all_b: Set[str] = set()
    for jhs in files_b.values():
        all_b |= jhs

    summary = {
        "n_shared_join_hash": str(len(all_a & all_b)),
        "containment_a_in_b_mean": _mean(c_ab_list),
        "containment_a_in_b_min": _min(c_ab_list),
        "containment_b_in_a_mean": _mean(c_ba_list),
        "containment_b_in_a_min": _min(c_ba_list),
        "jaccard_mean": _mean(jaccards),
        "jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
        "jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
        "n_files_a": str(len(files_a)),
        "n_files_b": str(len(files_b)),
        "n_pairs": str(len(jaccards)),
    }
    return summary, pair_rows


# ---------------------------------------------------------------------------
# Pair descriptor
# ---------------------------------------------------------------------------

DIRECTED_TYPES = {
    "template_to_project",
    "template_to_container",
    "container_to_project",
    "parent_sibling_roles",
    "governance_chain",
}

ComparisonPair = Tuple[str, str, str]  # (seg_a, seg_b, comparison_type)


# ---------------------------------------------------------------------------
# Pair discovery
# ---------------------------------------------------------------------------

def _same_unit(
    manifest: Dict[str, Dict[str, str]],
    sid_a: str,
    sid_b: str,
) -> bool:
    return (
        manifest.get(sid_a, {}).get("unit_system", "")
        == manifest.get(sid_b, {}).get("unit_system", "")
        and manifest.get(sid_a, {}).get("unit_system", "") != ""
    )


def discover_within_segment(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    by_parent: Dict[str, List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        parent = row.get("parent_segment_id", "").strip()
        if parent:
            by_parent[parent].append(sid)

    pairs: List[ComparisonPair] = []
    for _parent, children in by_parent.items():
        role_map: Dict[str, List[str]] = defaultdict(list)
        for c in children:
            role = manifest[c].get("governance_role", "").strip().lower()
            role_map[role].append(c)

        templates = role_map.get("template", [])
        projects = role_map.get("project", [])
        containers = role_map.get("container", [])

        for t in templates:
            for p in projects:
                if _same_unit(manifest, t, p):
                    pairs.append((t, p, "template_to_project"))
            for c in containers:
                if _same_unit(manifest, t, c):
                    pairs.append((t, c, "template_to_container"))

        for c in containers:
            for p in projects:
                if _same_unit(manifest, c, p):
                    pairs.append((c, p, "container_to_project"))

    return pairs


def discover_sibling_segments(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    # Group by (parent_segment_id, governance_role, unit_system)
    groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        parent = row.get("parent_segment_id", "").strip()
        role = row.get("governance_role", "").strip().lower()
        us = row.get("unit_system", "").strip()
        if parent and role and us:
            groups[(parent, role, us)].append(sid)

    pairs: List[ComparisonPair] = []
    for (_, role, _), members in groups.items():
        if len(members) < 2:
            continue
        ctype = {
            "template": "sibling_templates",
            "project": "sibling_projects",
            "container": "sibling_containers",
        }.get(role, "sibling_segments")
        for a, b in combinations(sorted(members), 2):
            pairs.append((a, b, ctype))
    return pairs


def discover_parent_siblings(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    # Level-2 segments sharing same level-1 parent, different governance_role
    # Specifically: Template-role vs Project-role
    level2: List[str] = [
        sid for sid, row in manifest.items()
        if row.get("segment_level", "").strip() == "2"
    ]

    by_parent: Dict[str, List[str]] = defaultdict(list)
    for sid in level2:
        parent = manifest[sid].get("parent_segment_id", "").strip()
        if parent:
            by_parent[parent].append(sid)

    pairs: List[ComparisonPair] = []
    for _parent, siblings in by_parent.items():
        templates = [
            s for s in siblings
            if manifest[s].get("governance_role", "").strip().lower() == "template"
        ]
        projects = [
            s for s in siblings
            if manifest[s].get("governance_role", "").strip().lower() == "project"
        ]
        for t in templates:
            for p in projects:
                if _same_unit(manifest, t, p):
                    pairs.append((t, p, "parent_sibling_roles"))
    return pairs


def discover_governance_chain(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    # Directed pairs: Template→Project, Template→Container, Container→Project
    # Scoped by client_label (and discipline_label when populated).
    # Reference segments are included — they participate using their file inventories.
    def _key(row: Dict[str, str]) -> Tuple[str, str]:
        return (
            row.get("client_label", "").strip(),
            row.get("unit_system", "").strip(),
        )

    def _disc(row: Dict[str, str]) -> str:
        return row.get("discipline_label", "").strip()

    def _disc_match(ra: Dict[str, str], rb: Dict[str, str]) -> bool:
        da, db = _disc(ra), _disc(rb)
        if not da or not db:
            return True
        return da == db

    by_key: Dict[Tuple[str, str], Dict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for sid, row in manifest.items():
        role = row.get("governance_role", "").strip().lower()
        if role in ("template", "project", "container"):
            by_key[_key(row)][role].append(sid)

    pairs: List[ComparisonPair] = []
    for (_client, _us), role_map in by_key.items():
        templates = role_map.get("template", [])
        projects = role_map.get("project", [])
        containers = role_map.get("container", [])

        for t in templates:
            for p in projects:
                if _disc_match(manifest[t], manifest[p]):
                    pairs.append((t, p, "template_to_project"))
            for c in containers:
                if _disc_match(manifest[t], manifest[c]):
                    pairs.append((t, c, "template_to_container"))
        for c in containers:
            for p in projects:
                if _disc_match(manifest[c], manifest[p]):
                    pairs.append((c, p, "container_to_project"))
    return pairs


def discover_within_project(
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    file_metadata: Dict[str, Dict[str, str]],
    segments_root: Path,
) -> List[ComparisonPair]:
    # Within a single segment, group files by project_label, pair files within group
    # Represented as (segment_id, segment_id, "within_project") with same seg on both sides
    pairs: List[ComparisonPair] = []
    for sid in manifest:
        reg = registry.get(sid, {})
        rt = reg.get("run_type", "").strip().lower()
        if rt in ("skip", "registration"):
            continue
        seg_out = segment_output_dir(segments_root, registry, sid)
        if seg_out is None:
            continue
        ba_root = seg_out / "results" / "bundle_analysis"
        if not ba_root.exists():
            continue
        # Collect eids from ALL domains so eligibility doesn't depend on which
        # membership_matrix.csv glob happens to return first.
        eids: Set[str] = set()
        for mm_path in ba_root.glob("*/membership_matrix.csv"):
            for row in read_csv_rows(mm_path):
                eid = row.get("export_run_id", "").strip()
                if eid:
                    eids.add(eid)
        if not eids:
            continue
        by_proj: Dict[str, List[str]] = defaultdict(list)
        for eid in eids:
            meta = file_metadata.get(eid, {})
            proj = meta.get("project_label", "").strip() or eid
            by_proj[proj].append(eid)
        if any(len(v) >= 2 for v in by_proj.values()):
            pairs.append((sid, sid, "within_project"))
    return pairs


# ---------------------------------------------------------------------------
# Pair deduplication
# ---------------------------------------------------------------------------

def deduplicate_pairs(pairs: List[ComparisonPair]) -> List[ComparisonPair]:
    # Dedup on the full (seg_a, seg_b, comparison_type) triple. Different comparison
    # types for the same segment pair represent distinct analytical questions and must
    # all be preserved — only exact triple duplicates are dropped.
    seen: Set[ComparisonPair] = set()
    result: List[ComparisonPair] = []
    for triple in pairs:
        if triple not in seen:
            seen.add(triple)
            result.append(triple)
    return result


# ---------------------------------------------------------------------------
# comparison_run_id
# ---------------------------------------------------------------------------

def make_comparison_run_id(seg_a: str, seg_b: str, executed_utc: str) -> str:
    token = f"{seg_a}|{seg_b}|{executed_utc}"
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()
    return f"cmp_{digest[:12]}"


# ---------------------------------------------------------------------------
# Core comparison dispatcher
# ---------------------------------------------------------------------------

def run_pair(
    seg_a: str,
    seg_b: str,
    comparison_type: str,
    domain: str,
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    file_metadata: Dict[str, Dict[str, str]],
    segments_root: Path,
    min_patterns: int,
    executed_utc: str,
) -> Tuple[Optional[Dict[str, str]], List[Dict[str, str]]]:
    """Return (summary_row_or_None, pair_detail_rows).

    All comparisons use file-level join_hash inventories from membership_matrix.csv.
    Bundle membership is added as post-hoc annotation after scores are computed.
    """
    is_directed = comparison_type in DIRECTED_TYPES
    is_within_project = comparison_type == "within_project"

    # For within_project: group by project_label within the single segment, then
    # aggregate all intra-project pairs into ONE summary row for (segment, domain).
    if is_within_project:
        all_files = load_file_join_hashes(segments_root, registry, seg_a, domain)
        by_proj: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
        for eid, jhs in all_files.items():
            meta = file_metadata.get(eid, {})
            proj = meta.get("project_label", "").strip() or eid
            by_proj[proj][eid] = jhs

        PairRecord = Tuple[str, str, str, int, int, int, float, float, float]
        raw_pairs: List[PairRecord] = []  # (eid_a, eid_b, proj, na, nb, ns, j, c_ab, c_ba)
        participating_eids: Set[str] = set()

        for proj, proj_files in by_proj.items():
            if len(proj_files) < 2:
                continue
            eids_sorted = sorted(proj_files.keys())
            for i in range(len(eids_sorted)):
                for jj in range(i + 1, len(eids_sorted)):
                    eid_a2, eid_b2 = eids_sorted[i], eids_sorted[jj]
                    jhs_a2 = proj_files[eid_a2]
                    jhs_b2 = proj_files[eid_b2]
                    union = jhs_a2 | jhs_b2
                    j_val = len(jhs_a2 & jhs_b2) / len(union) if union else 0.0
                    c_ab = len(jhs_a2 & jhs_b2) / len(jhs_a2) if jhs_a2 else 0.0
                    c_ba = len(jhs_a2 & jhs_b2) / len(jhs_b2) if jhs_b2 else 0.0
                    raw_pairs.append((
                        eid_a2, eid_b2, proj,
                        len(jhs_a2), len(jhs_b2), len(jhs_a2 & jhs_b2),
                        j_val, c_ab, c_ba,
                    ))
                    participating_eids.add(eid_a2)
                    participating_eids.add(eid_b2)

        if not raw_pairs:
            return None, []

        jaccards = [p[6] for p in raw_pairs]
        total_jhs: Set[str] = set()
        for eid in participating_eids:
            total_jhs |= all_files.get(eid, set())

        if len(total_jhs) < min_patterns:
            return None, []

        from collections import Counter as _Counter
        jhs_file_count: Dict[str, int] = _Counter(
            jh for eid in participating_eids for jh in all_files.get(eid, set())
        )
        n_shared_jh = sum(1 for v in jhs_file_count.values() if v > 1)
        n_files = len(participating_eids)

        # Bundle annotation on the shared set
        # For within_project, shared is defined as appearing in >1 participating file
        shared_jhs_wp: Set[str] = {jh for jh, cnt in jhs_file_count.items() if cnt > 1}
        bnd_a_wp = load_bundle_join_hash_set(segments_root, registry, seg_a, domain)
        n_both_wp, n_aonly_wp, n_bonly_wp = annotate_bundle_overlap(
            shared_jhs_wp, bnd_a_wp, bnd_a_wp
        )

        metrics: Dict[str, str] = {
            "n_shared_join_hash": str(n_shared_jh),
            "jaccard_mean": _mean(jaccards),
            "jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
            "jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
            "n_files_a": str(n_files),
            "n_files_b": str(n_files),
            "n_pairs": str(len(raw_pairs)),
        }

        crid = make_comparison_run_id(seg_a, seg_b, executed_utc)
        has_bundles = "true" if bnd_a_wp else "false"
        n_unique_wp = len(total_jhs)
        n_files_a_int = n_files
        n_files_b_int = n_files
        data_suff = "true" if (n_files_a_int >= 5 and n_files_b_int >= 5) else "false"

        summary_row = _build_summary_row(
            crid, seg_a, seg_b, comparison_type, domain,
            manifest, metrics,
            n_patterns_a=n_unique_wp,
            n_patterns_b=n_unique_wp,
            n_unique_patterns_a=n_unique_wp,
            n_unique_patterns_b=n_unique_wp,
            has_bundles_a=has_bundles,
            has_bundles_b=has_bundles,
            n_shared_bundle_both=n_both_wp,
            n_shared_bundle_a_only=n_aonly_wp,
            n_shared_bundle_b_only=n_bonly_wp,
            data_sufficient=data_suff,
            executed_utc=executed_utc,
        )

        # Emit ALL pair rows (no suppression threshold)
        c_ab_list_wp = [p[7] for p in raw_pairs]
        c_ba_list_wp = [p[8] for p in raw_pairs]
        detail_rows: List[Dict[str, str]] = []
        for eid_a2, eid_b2, proj, na, nb, ns, j_val, c_ab, c_ba in raw_pairs:
            shared_pair: Set[str] = all_files.get(eid_a2, set()) & all_files.get(eid_b2, set())
            pb, pao, pbo = annotate_bundle_overlap(shared_pair, bnd_a_wp, bnd_a_wp)
            detail_rows.append({
                "comparison_run_id": crid,
                "segment_id_a": seg_a,
                "segment_id_b": seg_b,
                "domain": domain,
                "export_run_id_a": eid_a2,
                "export_run_id_b": eid_b2,
                "project_label_a": proj,
                "project_label_b": proj,
                "n_patterns_a": str(na),
                "n_patterns_b": str(nb),
                "n_shared": str(ns),
                "jaccard": _fmt(j_val),
                "containment_a_in_b": _fmt(c_ab),
                "containment_b_in_a": _fmt(c_ba),
                "n_shared_bundle_both": str(pb),
                "n_shared_bundle_a_only": str(pao),
                "n_shared_bundle_b_only": str(pbo),
            })

        # Patch containment into summary metrics (mean/min over all pairs)
        summary_row["containment_a_in_b_mean"] = _mean(c_ab_list_wp)
        summary_row["containment_a_in_b_min"] = _min(c_ab_list_wp)
        summary_row["containment_b_in_a_mean"] = _mean(c_ba_list_wp)
        summary_row["containment_b_in_a_min"] = _min(c_ba_list_wp)

        return summary_row, detail_rows

    # Normal path — always file-based
    files_a = load_file_join_hashes(segments_root, registry, seg_a, domain)
    files_b = load_file_join_hashes(segments_root, registry, seg_b, domain)

    all_jhs_a: Set[str] = set()
    for jhs in files_a.values():
        all_jhs_a |= jhs
    all_jhs_b: Set[str] = set()
    for jhs in files_b.values():
        all_jhs_b |= jhs

    n_a = len(all_jhs_a)
    n_b = len(all_jhs_b)

    if n_a < min_patterns or n_b < min_patterns:
        return None, []

    # Compute metrics
    pair_rows: List[Dict[str, str]] = []

    if is_directed:
        metrics = compare_directed_file(files_a, files_b)
    else:
        metrics, pair_rows_raw = compare_symmetric_file(files_a, files_b)
        # Emit ALL pair rows — no suppression threshold
        crid_pre = make_comparison_run_id(seg_a, seg_b, executed_utc)
        bnd_a_pre = load_bundle_join_hash_set(segments_root, registry, seg_a, domain)
        bnd_b_pre = load_bundle_join_hash_set(segments_root, registry, seg_b, domain)
        for r in pair_rows_raw:
            eid_a2 = r.get("export_run_id_a", "")
            eid_b2 = r.get("export_run_id_b", "")
            shared_pair = files_a.get(eid_a2, set()) & files_b.get(eid_b2, set())
            pb, pao, pbo = annotate_bundle_overlap(shared_pair, bnd_a_pre, bnd_b_pre)
            r.update({
                "comparison_run_id": crid_pre,
                "segment_id_a": seg_a,
                "segment_id_b": seg_b,
                "domain": domain,
                "project_label_a": file_metadata.get(eid_a2, {}).get("project_label", ""),
                "project_label_b": file_metadata.get(eid_b2, {}).get("project_label", ""),
                "n_shared_bundle_both": str(pb),
                "n_shared_bundle_a_only": str(pao),
                "n_shared_bundle_b_only": str(pbo),
            })
        pair_rows = pair_rows_raw

    if not metrics:
        return None, []

    # Post-hoc bundle annotation on the population-grain shared set
    shared_jhs_norm = all_jhs_a & all_jhs_b
    bnd_a = load_bundle_join_hash_set(segments_root, registry, seg_a, domain)
    bnd_b = load_bundle_join_hash_set(segments_root, registry, seg_b, domain)
    n_both, n_aonly, n_bonly = annotate_bundle_overlap(shared_jhs_norm, bnd_a, bnd_b)

    has_bundles_a = "true" if bnd_a else "false"
    has_bundles_b = "true" if bnd_b else "false"

    n_files_a_int = len(files_a)
    n_files_b_int = len(files_b)
    data_suff = "true" if (n_files_a_int >= 5 and n_files_b_int >= 5) else "false"

    crid = make_comparison_run_id(seg_a, seg_b, executed_utc)
    summary = _build_summary_row(
        crid, seg_a, seg_b, comparison_type, domain,
        manifest, metrics,
        n_patterns_a=n_a,
        n_patterns_b=n_b,
        n_unique_patterns_a=n_a,
        n_unique_patterns_b=n_b,
        has_bundles_a=has_bundles_a,
        has_bundles_b=has_bundles_b,
        n_shared_bundle_both=n_both,
        n_shared_bundle_a_only=n_aonly,
        n_shared_bundle_b_only=n_bonly,
        data_sufficient=data_suff,
        executed_utc=executed_utc,
    )
    for r in pair_rows:
        r["comparison_run_id"] = crid
    return summary, pair_rows


def _build_summary_row(
    crid: str,
    seg_a: str,
    seg_b: str,
    comparison_type: str,
    domain: str,
    manifest: Dict[str, Dict[str, str]],
    metrics: Dict[str, str],
    n_patterns_a: int,
    n_patterns_b: int,
    n_unique_patterns_a: int,
    n_unique_patterns_b: int,
    has_bundles_a: str,
    has_bundles_b: str,
    n_shared_bundle_both: int,
    n_shared_bundle_a_only: int,
    n_shared_bundle_b_only: int,
    data_sufficient: str,
    executed_utc: str,
) -> Dict[str, str]:
    ma = manifest.get(seg_a, {})
    mb = manifest.get(seg_b, {})
    return {
        "comparison_run_id": crid,
        "segment_id_a": seg_a,
        "segment_id_b": seg_b,
        "segment_label_a": ma.get("segment_label", ""),
        "segment_label_b": mb.get("segment_label", ""),
        "governance_role_a": ma.get("governance_role", ""),
        "governance_role_b": mb.get("governance_role", ""),
        "client_label_a": ma.get("client_label", ""),
        "client_label_b": mb.get("client_label", ""),
        "discipline_label_a": ma.get("discipline_label", ""),
        "discipline_label_b": mb.get("discipline_label", ""),
        "unit_system": ma.get("unit_system", ""),
        "comparison_type": comparison_type,
        "domain": domain,
        "n_patterns_a": str(n_patterns_a),
        "n_patterns_b": str(n_patterns_b),
        "n_shared_join_hash": metrics.get("n_shared_join_hash", ""),
        "n_unique_patterns_a": str(n_unique_patterns_a),
        "n_unique_patterns_b": str(n_unique_patterns_b),
        "containment_a_in_b_mean": metrics.get("containment_a_in_b_mean", ""),
        "containment_a_in_b_min": metrics.get("containment_a_in_b_min", ""),
        "containment_b_in_a_mean": metrics.get("containment_b_in_a_mean", ""),
        "containment_b_in_a_min": metrics.get("containment_b_in_a_min", ""),
        "jaccard_mean": metrics.get("jaccard_mean", ""),
        "jaccard_p10": metrics.get("jaccard_p10", ""),
        "jaccard_p90": metrics.get("jaccard_p90", ""),
        "has_bundles_a": has_bundles_a,
        "has_bundles_b": has_bundles_b,
        "n_shared_bundle_both": str(n_shared_bundle_both),
        "n_shared_bundle_a_only": str(n_shared_bundle_a_only),
        "n_shared_bundle_b_only": str(n_shared_bundle_b_only),
        "n_files_a": metrics.get("n_files_a", ""),
        "n_files_b": metrics.get("n_files_b", ""),
        "n_pairs": metrics.get("n_pairs", ""),
        "data_sufficient": data_sufficient,
        "executed_utc": executed_utc,
    }


# ---------------------------------------------------------------------------
# Pooled comparison
# ---------------------------------------------------------------------------

def run_pooled_comparison(
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    segments_root: Path,
    min_patterns: int,
    executed_utc: str,
    domain_filter: Optional[str] = None,
    focal_segment_ids: Optional[Set[str]] = None,
) -> List[Dict[str, str]]:
    """N-1 pooled comparison: each segment vs its sibling pool.

    Pool = all files from sibling segments sharing the same
    (parent_segment_id, governance_role, unit_system), excluding the focal segment.
    Emits one row per (segment_id, domain).
    """
    # Group segments by (parent, role, unit_system)
    groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        parent = row.get("parent_segment_id", "").strip()
        role = row.get("governance_role", "").strip().lower()
        us = row.get("unit_system", "").strip()
        rt = registry.get(sid, {}).get("run_type", "").strip().lower()
        if rt in ("skip", "registration"):
            continue
        if parent and role and us:
            groups[(parent, role, us)].append(sid)

    # Only groups with >=2 members have siblings
    sibling_groups = {k: v for k, v in groups.items() if len(v) >= 2}

    rows: List[Dict[str, str]] = []

    for (parent, role, us), members in sibling_groups.items():
        for focal_sid in members:
            if focal_segment_ids is not None and focal_sid not in focal_segment_ids:
                continue
            pool_sids = [s for s in members if s != focal_sid]

            # Discover domains from the focal segment
            focal_domains = discover_domains_for_segment(segments_root, registry, focal_sid)
            if domain_filter:
                focal_domains = focal_domains & {domain_filter}

            for domain in sorted(focal_domains):
                focal_files = load_file_join_hashes(
                    segments_root, registry, focal_sid, domain
                )
                focal_union: Set[str] = set()
                for jhs in focal_files.values():
                    focal_union |= jhs

                if len(focal_union) < min_patterns:
                    continue

                # Aggregate pool files
                pool_files_by_eid: Dict[str, Set[str]] = {}
                for pool_sid in pool_sids:
                    pf = load_file_join_hashes(segments_root, registry, pool_sid, domain)
                    pool_files_by_eid.update(pf)

                pool_union: Set[str] = set()
                for jhs in pool_files_by_eid.values():
                    pool_union |= jhs

                if len(pool_union) < min_patterns:
                    continue

                shared = focal_union & pool_union
                n_shared = len(shared)
                n_focal_unique = len(focal_union)
                n_pool_unique = len(pool_union)

                c_focal_in_pool = n_shared / n_focal_unique if n_focal_unique else 0.0
                c_pool_in_focal = n_shared / n_pool_unique if n_pool_unique else 0.0

                n_files_focal = len(focal_files)
                n_files_pool = len(pool_files_by_eid)
                data_suff = "true" if (n_files_focal >= 5 and n_files_pool >= 5) else "false"

                # Bundle annotation
                focal_bundle = load_bundle_join_hash_set(
                    segments_root, registry, focal_sid, domain
                )
                pool_bundle: Set[str] = set()
                for pool_sid in pool_sids:
                    pool_bundle |= load_bundle_join_hash_set(
                        segments_root, registry, pool_sid, domain
                    )

                has_bundles_focal = "true" if focal_bundle else "false"
                has_bundles_pool = "true" if pool_bundle else "false"

                n_both, n_focal_only, n_pool_only = annotate_bundle_overlap(
                    shared, focal_bundle, pool_bundle
                )

                mf = manifest.get(focal_sid, {})
                crid = make_comparison_run_id(focal_sid, f"pool_{parent}_{role}_{us}", executed_utc)

                rows.append({
                    "comparison_run_id": crid,
                    "segment_id": focal_sid,
                    "segment_label": mf.get("segment_label", ""),
                    "governance_role": mf.get("governance_role", ""),
                    "client_label": mf.get("client_label", ""),
                    "unit_system": us,
                    "domain": domain,
                    "n_files_focal": str(n_files_focal),
                    "n_files_pool": str(n_files_pool),
                    "n_unique_patterns_focal": str(n_focal_unique),
                    "n_unique_patterns_pool": str(n_pool_unique),
                    "n_shared_join_hash": str(n_shared),
                    "containment_focal_in_pool": _fmt(c_focal_in_pool),
                    "containment_pool_in_focal": _fmt(c_pool_in_focal),
                    "has_bundles_focal": has_bundles_focal,
                    "has_bundles_pool": has_bundles_pool,
                    "n_shared_bundle_both": str(n_both),
                    "n_shared_bundle_focal_only": str(n_focal_only),
                    "n_shared_bundle_pool_only": str(n_pool_only),
                    "data_sufficient": data_suff,
                    "executed_utc": executed_utc,
                })

    return rows


# ---------------------------------------------------------------------------
# Segment validation
# ---------------------------------------------------------------------------

def segment_is_runnable(
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
) -> bool:
    rec = registry.get(segment_id)
    if rec is None:
        return False
    rt = rec.get("run_type", "").strip().lower()
    if rt in ("skip", "registration"):
        print(
            f"[warn] segment={segment_id} has run_type={rt!r} — skipping",
            file=sys.stderr,
        )
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cross-segment comparison — computes join_hash overlap metrics\n"
                    "across segment pairs discovered from the manifest hierarchy.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--segments-root", required=True, metavar="DIR",
                    help="Base directory for resolving segment output_folder paths from run_registry.csv")
    ap.add_argument("--records-dir", required=True, metavar="DIR",
                    help="Directory containing segment_manifest.csv, run_registry.csv, and file_metadata.csv")
    ap.add_argument("--out-dir", required=True, metavar="DIR",
                    help="Output directory for cross_segment_summary.csv, cross_segment_file_pairs.csv, and cross_segment_pooled.csv")

    # Mode flags
    ap.add_argument("--within-segment", action="store_true",
                    help="Mode A: pairs child Template/Project/Container within same parent")
    ap.add_argument("--sibling-segments", action="store_true",
                    help="Mode B: sibling segments sharing same parent and same governance_role")
    ap.add_argument("--parent-siblings", action="store_true",
                    help="Mode C: level-2 segments with different governance_role under same level-1 parent")
    ap.add_argument("--within-project", action="store_true",
                    help="Mode D: file pairs within same project_label within a single segment")
    ap.add_argument("--governance-chain", action="store_true",
                    help="Mode E: directed governance pairs scoped by client_label and discipline_label")

    # Filters
    ap.add_argument("--domain", metavar="DOMAIN",
                    help="Restrict comparison to a single domain")
    ap.add_argument("--segment-a", metavar="SEGMENT_ID",
                    help="Restrict left side of pairs to this segment")
    ap.add_argument("--segment-b", metavar="SEGMENT_ID",
                    help="Restrict right side of pairs to this segment")
    ap.add_argument("--min-patterns", type=int, default=3, metavar="INT",
                    help="Skip domain/segment pairs with fewer than N join_hashes (default: 3)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print discovered pairs without computing; no output files written")
    ap.add_argument("--no-delta", action="store_true",
                    help="Skip delta pattern output (cross_segment_delta.csv); useful for large corpora")

    args = ap.parse_args()

    segments_root = Path(args.segments_root).resolve()
    records_dir = Path(args.records_dir).resolve()
    out_dir = Path(args.out_dir).resolve()

    # Default: all modes if none specified
    any_mode = any([
        args.within_segment, args.sibling_segments, args.parent_siblings,
        args.within_project, args.governance_chain,
    ])
    if not any_mode:
        args.within_segment = args.sibling_segments = args.parent_siblings = True
        args.within_project = args.governance_chain = True

    manifest = load_manifest(records_dir)
    registry = load_registry(records_dir)
    file_metadata = load_file_metadata(records_dir)

    # Discover pairs
    pairs: List[ComparisonPair] = []
    if args.within_segment:
        pairs.extend(discover_within_segment(manifest))
    if args.sibling_segments:
        pairs.extend(discover_sibling_segments(manifest))
    if args.parent_siblings:
        pairs.extend(discover_parent_siblings(manifest))
    if args.governance_chain:
        pairs.extend(discover_governance_chain(manifest))
    if args.within_project:
        pairs.extend(discover_within_project(manifest, registry, file_metadata, segments_root))

    pairs = deduplicate_pairs(pairs)

    # Filter by --segment-a / --segment-b
    if args.segment_a:
        pairs = [(a, b, ct) for a, b, ct in pairs if a == args.segment_a]
    if args.segment_b:
        pairs = [(a, b, ct) for a, b, ct in pairs if b == args.segment_b]

    if not pairs:
        print("[compare] no pairs discovered — check manifest hierarchy and mode flags")
        return 0

    # --dry-run: print table and exit
    if args.dry_run:
        col_w = 36
        print(f"{'segment_a':<{col_w}}  {'segment_b':<{col_w}}  {'comparison_type':<28}")
        print("-" * (col_w * 2 + 32))
        for a, b, ctype in pairs:
            la = manifest.get(a, {}).get("segment_label", a)
            lb = manifest.get(b, {}).get("segment_label", b)
            print(f"{la:<{col_w}}  {lb:<{col_w}}  {ctype:<28}")
        print(f"\n[compare] {len(pairs)} pairs discovered")
        return 0

    # Run comparisons
    executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_rows: List[Dict[str, str]] = []
    pair_detail_rows: List[Dict[str, str]] = []
    delta_rows: List[Dict[str, str]] = []
    delta_combo_count = 0

    for seg_a, seg_b, ctype in pairs:
        if not segment_is_runnable(registry, seg_a):
            continue
        if seg_a != seg_b and not segment_is_runnable(registry, seg_b):
            continue

        # Discover domains for this pair
        if args.domain:
            domains = [args.domain]
        else:
            domains_a = discover_domains_for_segment(segments_root, registry, seg_a)
            domains_b = (
                discover_domains_for_segment(segments_root, registry, seg_b)
                if seg_a != seg_b
                else domains_a
            )
            domains = sorted(domains_a | domains_b)

        for domain in domains:
            result, pairs_out = run_pair(
                seg_a, seg_b, ctype, domain,
                manifest, registry, file_metadata,
                segments_root, args.min_patterns, executed_utc,
            )
            if result is None:
                continue

            summary_rows.append(result)
            pair_detail_rows.extend(pairs_out)
            n_p = result.get("n_pairs", "?")
            print(
                f"[compare] segment_a={seg_a} segment_b={seg_b} "
                f"domain={domain} pairs={n_p}"
            )

            # Delta pattern output — directed pairs only, opt-out via --no-delta
            if not args.no_delta and ctype in DELTA_DIRECTED_TYPES:
                tgt_files = load_file_join_hashes(segments_root, registry, seg_b, domain)
                ref_files = load_file_join_hashes(segments_root, registry, seg_a, domain)
                ref_union: Set[str] = set()
                for jhs in ref_files.values():
                    ref_union |= jhs
                tgt_union: Set[str] = set()
                for jhs in tgt_files.values():
                    tgt_union |= jhs
                delta_jhs = tgt_union - ref_union

                if delta_jhs:
                    unit_system = manifest.get(seg_a, {}).get("unit_system", "")
                    container_set = get_role_jh_set(
                        "container", domain, unit_system, manifest, registry, segments_root,
                        exclude_segment_id=seg_b,
                    )
                    template_set = get_role_jh_set(
                        "template", domain, unit_system, manifest, registry, segments_root
                    )
                    pattern_labels = load_pattern_labels(
                        segments_root, registry, seg_b, domain
                    )
                    n_tgt_files = len(tgt_files)
                    crid = result.get("comparison_run_id", "")
                    ma = manifest.get(seg_a, {})
                    mb = manifest.get(seg_b, {})

                    for jh in delta_jhs:
                        n_files_in_tgt = sum(1 for jhs in tgt_files.values() if jh in jhs)
                        pct = n_files_in_tgt / n_tgt_files if n_tgt_files else 0.0
                        delta_rows.append({
                            "comparison_run_id": crid,
                            "segment_id_reference": seg_a,
                            "segment_id_target": seg_b,
                            "segment_label_reference": ma.get("segment_label", ""),
                            "segment_label_target": mb.get("segment_label", ""),
                            "comparison_type": ctype,
                            "domain": domain,
                            "join_hash": jh,
                            "pattern_label": pattern_labels.get(jh, ""),
                            "n_files_in_target": str(n_files_in_tgt),
                            "pct_files_in_target": _fmt(pct),
                            "in_any_container": "true" if jh in container_set else "false",
                            "in_any_template": "true" if jh in template_set else "false",
                            "executed_utc": executed_utc,
                        })
                    delta_combo_count += 1

    # Pooled comparison
    focal_filter: Optional[Set[str]] = None
    if args.segment_a or args.segment_b:
        focal_filter = set()
        if args.segment_a:
            focal_filter.add(args.segment_a)
        if args.segment_b:
            focal_filter.add(args.segment_b)

    pooled_rows = run_pooled_comparison(
        manifest, registry, segments_root,
        args.min_patterns, executed_utc,
        domain_filter=args.domain,
        focal_segment_ids=focal_filter,
    )

    # Write outputs
    if summary_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(out_dir / "cross_segment_summary.csv", SUMMARY_FIELDS, summary_rows)
        print(f"[compare] wrote {len(summary_rows)} rows → {out_dir / 'cross_segment_summary.csv'}")

    if pair_detail_rows:
        atomic_write_csv(out_dir / "cross_segment_file_pairs.csv", PAIRS_FIELDS, pair_detail_rows)
        print(f"[compare] wrote {len(pair_detail_rows)} rows → {out_dir / 'cross_segment_file_pairs.csv'}")

    if delta_rows:
        delta_rows.sort(key=lambda r: (
            r["comparison_type"],
            r["segment_id_reference"],
            r["segment_id_target"],
            r["domain"],
            -float(r["pct_files_in_target"] or "0"),
            r["join_hash"],
        ))
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(out_dir / "cross_segment_delta.csv", DELTA_FIELDS, delta_rows)
        print(
            f"[compare] delta patterns written: {len(delta_rows)} rows across "
            f"{delta_combo_count} domain/pair combinations"
        )
        print(f"[compare] wrote {len(delta_rows)} rows → {out_dir / 'cross_segment_delta.csv'}")

    if pooled_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(out_dir / "cross_segment_pooled.csv", POOLED_FIELDS, pooled_rows)
        print(f"[compare] wrote {len(pooled_rows)} rows → {out_dir / 'cross_segment_pooled.csv'}")

    if not summary_rows and not pooled_rows:
        print("[compare] no comparison rows produced — check segment data and min-patterns threshold")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
