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
bundle_analysis/{all,used}/<domain>/bundle_membership.csv for each segment and
annotated onto n_shared using two views and three buckets each:

  all_n_shared_bundle_both   — join_hashes in shared that are bundle members in
                               BOTH segments under the all view
  all_n_shared_bundle_a_only — bundle member in A (all view), not B
  all_n_shared_bundle_b_only — bundle member in B (all view), not A
  used_*                     — same three columns for the used view

The used view excludes patterns that are conclusively purgeable; the delta
between all and used views quantifies passive inheritance.

All-view vs used-view scores
-----------------------------
Jaccard and containment scores are computed independently from both the all-view
and used-view membership matrices. All-view scores (all_jaccard_*, all_containment_*)
reflect the full configured pattern vocabulary. Used-view scores (used_jaccard_*,
used_containment_*) reflect only patterns present in active view/sheet assignments.
The delta between all-view and used-view scores quantifies passive inheritance —
patterns configured but never rendered. used_n_shared_join_hash is the count of
join_hashes that appear in both segments' used-view inventories.

N-1 pooled comparison (cross_segment_pooled.csv)
-------------------------------------------------
Each segment is compared against the union of all sibling segments sharing the
same (parent_segment_id, governance_role, unit_system). This is the primary
signal for small segments where pairwise Jaccard is dominated by size asymmetry.
Containment in both directions is reported for both all and used views; no
Jaccard is computed on this file.

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
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
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
    "all_containment_a_in_b_mean", "all_containment_a_in_b_min",
    "all_containment_b_in_a_mean", "all_containment_b_in_a_min",
    "all_jaccard_mean", "all_jaccard_p10", "all_jaccard_p90",
    "used_jaccard_mean", "used_jaccard_p10", "used_jaccard_p90",
    "used_containment_a_in_b_mean", "used_containment_a_in_b_min",
    "used_containment_b_in_a_mean", "used_containment_b_in_a_min",
    "used_n_shared_join_hash",
    "all_has_bundles_a", "all_has_bundles_b",
    "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
    "used_has_bundles_a", "used_has_bundles_b",
    "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
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
    "all_jaccard", "all_containment_a_in_b", "all_containment_b_in_a",
    "used_n_shared", "used_jaccard", "used_containment_a_in_b", "used_containment_b_in_a",
    "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
    "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
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
    "used_pct_files_in_target",
    "is_bundle_member_all",
    "is_bundle_member_used",
    "delta_class",
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
    "all_containment_focal_in_pool", "all_containment_pool_in_focal",
    "used_containment_focal_in_pool", "used_containment_pool_in_focal",
    "all_has_bundles_focal", "all_has_bundles_pool",
    "all_n_shared_bundle_both", "all_n_shared_bundle_focal_only", "all_n_shared_bundle_pool_only",
    "used_has_bundles_focal", "used_has_bundles_pool",
    "used_n_shared_bundle_both", "used_n_shared_bundle_focal_only", "used_n_shared_bundle_pool_only",
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
# Delta pattern classification
# ---------------------------------------------------------------------------

def _classify_delta(
    in_any_container: bool,
    in_any_template: bool,
    is_bundle_member_all: bool,
    is_bundle_member_used: bool,
) -> str:
    """Classify a delta pattern by origin and active-use status.

    Classes:
      passive_inherited   — pattern came from governance (container/template) but is
                            not actively used in the target; pure configuration bloat
      active_inherited    — came from governance AND is actively used in the target;
                            target intentionally extends the governance vocabulary
      locally_custom_active  — not from governance context, actively used; target has
                                its own patterns it is rendering
      locally_custom_passive — not from governance, in all-view bundle but not used;
                                locally defined orphan
      locally_custom_unbundled — not from governance, not in any bundle analysis;
                                  raw local definition with no bundle data
    """
    from_governance = in_any_container or in_any_template
    if from_governance:
        if is_bundle_member_used:
            return "active_inherited"
        return "passive_inherited"
    if is_bundle_member_used:
        return "locally_custom_active"
    if is_bundle_member_all:
        return "locally_custom_passive"
    return "locally_custom_unbundled"


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


def bundle_analysis_dir(seg_out: Path, domain: str, purge_view: str = "all") -> Path:
    return seg_out / "results" / "bundle_analysis" / purge_view / domain


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
    # Always discover from the all view — it is the domain authority source.
    ba_root = seg_out / "results" / "bundle_analysis" / "all"
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

# Cache: (governance_role, domain, unit_system, exclude_segment_id) -> Set[join_hash]
_role_jh_cache: Dict[Tuple[str, str, str, str], Set[str]] = {}

# Cache: (segment_id, domain, purge_view) -> Set[join_hash]  (bundle members only)
_bundle_jh_cache: Dict[Tuple[str, str, str], Set[str]] = {}


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
        # Use all view — scores are view-invariant
        mm_path = bundle_analysis_dir(seg_out, domain, "all") / "membership_matrix.csv"
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
    purge_view: str = "all",
) -> Dict[str, Set[str]]:
    """Return {export_run_id: set_of_join_hashes} from membership_matrix.csv."""
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return {}

    mm_path = bundle_analysis_dir(seg_out, domain, purge_view) / "membership_matrix.csv"
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
    purge_view: str = "all",
) -> Set[str]:
    """Return join_hashes that are bundle members for segment/domain/purge_view.

    Empty set if bundle_membership.csv absent for this view.
    Path: {segment_output_folder}/results/bundle_analysis/{purge_view}/{domain}/bundle_membership.csv
    """
    key = (segment_id, domain, purge_view)
    if key in _bundle_jh_cache:
        return _bundle_jh_cache[key]

    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        _bundle_jh_cache[key] = set()
        return set()

    bm_path = bundle_analysis_dir(seg_out, domain, purge_view) / "bundle_membership.csv"
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
        "all_containment_a_in_b_mean": _mean(a_in_b),
        "all_containment_a_in_b_min": _min(a_in_b),
        "all_containment_b_in_a_mean": _mean(b_in_a),
        "all_containment_b_in_a_min": _min(b_in_a),
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
                "all_jaccard": _fmt(j),
                "all_containment_a_in_b": _fmt(c_ab),
                "all_containment_b_in_a": _fmt(c_ba),
            })

    all_a: Set[str] = set()
    for jhs in files_a.values():
        all_a |= jhs
    all_b: Set[str] = set()
    for jhs in files_b.values():
        all_b |= jhs

    summary = {
        "n_shared_join_hash": str(len(all_a & all_b)),
        "all_containment_a_in_b_mean": _mean(c_ab_list),
        "all_containment_a_in_b_min": _min(c_ab_list),
        "all_containment_b_in_a_mean": _mean(c_ba_list),
        "all_containment_b_in_a_min": _min(c_ba_list),
        "all_jaccard_mean": _mean(jaccards),
        "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
        "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
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
        rt = row.get("run_type", "").strip().lower()
        if parent and rt in ("bundle", "reference"):
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
        rt = row.get("run_type", "").strip().lower()
        if parent and role and us and rt in ("bundle", "reference"):
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
        and row.get("run_type", "").strip().lower() in ("bundle", "reference")
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
        rt = row.get("run_type", "").strip().lower()
        if role in ("template", "project", "container") and rt in ("bundle", "reference"):
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
        # Always discover from the all view
        ba_root = seg_out / "results" / "bundle_analysis" / "all"
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
        all_files_used = load_file_join_hashes(segments_root, registry, seg_a, domain, "used")

        by_proj: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
        for eid, jhs in all_files.items():
            meta = file_metadata.get(eid, {})
            proj = meta.get("project_label", "").strip() or eid
            by_proj[proj][eid] = jhs

        # Used-view project grouping (same labels, but used-view join_hash sets)
        by_proj_used: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
        for eid, jhs in all_files_used.items():
            meta = file_metadata.get(eid, {})
            proj = meta.get("project_label", "").strip() or eid
            by_proj_used[proj][eid] = jhs

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

        # Used-view intra-project pairs (indexed by (eid_a, eid_b) for join onto all-view)
        UsedRec = Tuple[int, float, float, float]  # (n_shared, jaccard, c_ab, c_ba)
        used_pair_index_wp: Dict[Tuple[str, str], UsedRec] = {}
        used_jaccards_wp: List[float] = []
        for proj, proj_files_used in by_proj_used.items():
            if len(proj_files_used) < 2:
                continue
            eids_sorted_u = sorted(proj_files_used.keys())
            for i in range(len(eids_sorted_u)):
                for jj in range(i + 1, len(eids_sorted_u)):
                    eu_a, eu_b = eids_sorted_u[i], eids_sorted_u[jj]
                    ju_a = proj_files_used[eu_a]
                    ju_b = proj_files_used[eu_b]
                    union_u = ju_a | ju_b
                    j_u = len(ju_a & ju_b) / len(union_u) if union_u else 0.0
                    cu_ab = len(ju_a & ju_b) / len(ju_a) if ju_a else 0.0
                    cu_ba = len(ju_a & ju_b) / len(ju_b) if ju_b else 0.0
                    used_pair_index_wp[(eu_a, eu_b)] = (len(ju_a & ju_b), j_u, cu_ab, cu_ba)
                    used_jaccards_wp.append(j_u)

        # Used-view shared join_hash count (patterns seen in >1 file under used view)
        used_jhs_file_count_wp: Dict[str, int] = _Counter(
            jh for eid in participating_eids for jh in all_files_used.get(eid, set())
        )
        used_n_shared_jh_wp = sum(1 for v in used_jhs_file_count_wp.values() if v > 1)

        # Bundle annotation on the shared set (dual-view)
        shared_jhs_wp: Set[str] = {jh for jh, cnt in jhs_file_count.items() if cnt > 1}
        bnd_a_wp_all = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
        bnd_a_wp_used = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
        n_both_wp_all, n_aonly_wp_all, n_bonly_wp_all = annotate_bundle_overlap(
            shared_jhs_wp, bnd_a_wp_all, bnd_a_wp_all
        )
        n_both_wp_used, n_aonly_wp_used, n_bonly_wp_used = annotate_bundle_overlap(
            shared_jhs_wp, bnd_a_wp_used, bnd_a_wp_used
        )

        metrics: Dict[str, str] = {
            "n_shared_join_hash": str(n_shared_jh),
            "all_jaccard_mean": _mean(jaccards),
            "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
            "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
            "n_files_a": str(n_files),
            "n_files_b": str(n_files),
            "n_pairs": str(len(raw_pairs)),
        }

        crid = make_comparison_run_id(seg_a, seg_b, executed_utc)
        all_has_bundles = "true" if bnd_a_wp_all else "false"
        used_has_bundles = "true" if bnd_a_wp_used else "false"
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
            all_has_bundles_a=all_has_bundles,
            all_has_bundles_b=all_has_bundles,
            all_n_shared_bundle_both=n_both_wp_all,
            all_n_shared_bundle_a_only=n_aonly_wp_all,
            all_n_shared_bundle_b_only=n_bonly_wp_all,
            used_has_bundles_a=used_has_bundles,
            used_has_bundles_b=used_has_bundles,
            used_n_shared_bundle_both=n_both_wp_used,
            used_n_shared_bundle_a_only=n_aonly_wp_used,
            used_n_shared_bundle_b_only=n_bonly_wp_used,
            used_n_shared_join_hash=str(used_n_shared_jh_wp),
            used_jaccard_mean=_mean(used_jaccards_wp),
            used_jaccard_p10=_fmt(_pct(used_jaccards_wp, 10)) if used_jaccards_wp else "",
            used_jaccard_p90=_fmt(_pct(used_jaccards_wp, 90)) if used_jaccards_wp else "",
            data_sufficient=data_suff,
            executed_utc=executed_utc,
        )

        # Emit ALL pair rows (no suppression threshold)
        c_ab_list_wp = [p[7] for p in raw_pairs]
        c_ba_list_wp = [p[8] for p in raw_pairs]
        detail_rows: List[Dict[str, str]] = []
        used_c_ab_list_wp: List[float] = []
        used_c_ba_list_wp: List[float] = []
        for eid_a2, eid_b2, proj, na, nb, ns, j_val, c_ab, c_ba in raw_pairs:
            shared_pair: Set[str] = all_files.get(eid_a2, set()) & all_files.get(eid_b2, set())
            pb_all, pao_all, pbo_all = annotate_bundle_overlap(shared_pair, bnd_a_wp_all, bnd_a_wp_all)
            pb_used, pao_used, pbo_used = annotate_bundle_overlap(shared_pair, bnd_a_wp_used, bnd_a_wp_used)
            u_ns, u_j, u_cab, u_cba = used_pair_index_wp.get((eid_a2, eid_b2), (0, 0.0, 0.0, 0.0))
            used_c_ab_list_wp.append(u_cab)
            used_c_ba_list_wp.append(u_cba)
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
                "all_jaccard": _fmt(j_val),
                "all_containment_a_in_b": _fmt(c_ab),
                "all_containment_b_in_a": _fmt(c_ba),
                "used_n_shared": str(u_ns),
                "used_jaccard": _fmt(u_j),
                "used_containment_a_in_b": _fmt(u_cab),
                "used_containment_b_in_a": _fmt(u_cba),
                "all_n_shared_bundle_both": str(pb_all),
                "all_n_shared_bundle_a_only": str(pao_all),
                "all_n_shared_bundle_b_only": str(pbo_all),
                "used_n_shared_bundle_both": str(pb_used),
                "used_n_shared_bundle_a_only": str(pao_used),
                "used_n_shared_bundle_b_only": str(pbo_used),
            })

        # Patch containment into summary metrics (mean/min over all pairs)
        summary_row["all_containment_a_in_b_mean"] = _mean(c_ab_list_wp)
        summary_row["all_containment_a_in_b_min"] = _min(c_ab_list_wp)
        summary_row["all_containment_b_in_a_mean"] = _mean(c_ba_list_wp)
        summary_row["all_containment_b_in_a_min"] = _min(c_ba_list_wp)
        summary_row["used_containment_a_in_b_mean"] = _mean(used_c_ab_list_wp)
        summary_row["used_containment_a_in_b_min"] = _min(used_c_ab_list_wp)
        summary_row["used_containment_b_in_a_mean"] = _mean(used_c_ba_list_wp)
        summary_row["used_containment_b_in_a_min"] = _min(used_c_ba_list_wp)

        return summary_row, detail_rows

    # Normal path — file-based, both all-view and used-view
    files_a = load_file_join_hashes(segments_root, registry, seg_a, domain)
    files_b = load_file_join_hashes(segments_root, registry, seg_b, domain)
    files_a_used = load_file_join_hashes(segments_root, registry, seg_a, domain, "used")
    files_b_used = load_file_join_hashes(segments_root, registry, seg_b, domain, "used")

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

    pair_rows: List[Dict[str, str]] = []

    # Load bundle sets for both views upfront
    bnd_a_all = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
    bnd_b_all = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "all")
    bnd_a_used = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
    bnd_b_used = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "used")

    # All-view metrics
    if is_directed:
        metrics = compare_directed_file(files_a, files_b)
        metrics_used = compare_directed_file(files_a_used, files_b_used)
    else:
        metrics, pair_rows_raw = compare_symmetric_file(files_a, files_b)
        metrics_used, pair_rows_used = compare_symmetric_file(files_a_used, files_b_used)
        # Index used-view rows by (eid_a, eid_b) for join
        used_row_index: Dict[Tuple[str, str], Dict[str, str]] = {
            (r["export_run_id_a"], r["export_run_id_b"]): r
            for r in pair_rows_used
        }
        # Emit ALL pair rows — no suppression threshold
        crid_pre = make_comparison_run_id(seg_a, seg_b, executed_utc)
        for r in pair_rows_raw:
            eid_a2 = r.get("export_run_id_a", "")
            eid_b2 = r.get("export_run_id_b", "")
            shared_pair = files_a.get(eid_a2, set()) & files_b.get(eid_b2, set())
            pb_all, pao_all, pbo_all = annotate_bundle_overlap(shared_pair, bnd_a_all, bnd_b_all)
            pb_used, pao_used, pbo_used = annotate_bundle_overlap(shared_pair, bnd_a_used, bnd_b_used)
            ur = used_row_index.get((eid_a2, eid_b2), {})
            r.update({
                "comparison_run_id": crid_pre,
                "segment_id_a": seg_a,
                "segment_id_b": seg_b,
                "domain": domain,
                "project_label_a": file_metadata.get(eid_a2, {}).get("project_label", ""),
                "project_label_b": file_metadata.get(eid_b2, {}).get("project_label", ""),
                "used_n_shared": ur.get("n_shared", "0"),
                "used_jaccard": ur.get("all_jaccard", ""),
                "used_containment_a_in_b": ur.get("all_containment_a_in_b", ""),
                "used_containment_b_in_a": ur.get("all_containment_b_in_a", ""),
                "all_n_shared_bundle_both": str(pb_all),
                "all_n_shared_bundle_a_only": str(pao_all),
                "all_n_shared_bundle_b_only": str(pbo_all),
                "used_n_shared_bundle_both": str(pb_used),
                "used_n_shared_bundle_a_only": str(pao_used),
                "used_n_shared_bundle_b_only": str(pbo_used),
            })
        pair_rows = pair_rows_raw

    if not metrics:
        return None, []

    # Used-view population-grain shared count
    all_jhs_a_used: Set[str] = set()
    for jhs in files_a_used.values():
        all_jhs_a_used |= jhs
    all_jhs_b_used: Set[str] = set()
    for jhs in files_b_used.values():
        all_jhs_b_used |= jhs
    used_n_shared_jh = len(all_jhs_a_used & all_jhs_b_used)

    # Post-hoc bundle annotation on the population-grain shared set (dual-view)
    shared_jhs_norm = all_jhs_a & all_jhs_b
    n_both_all, n_aonly_all, n_bonly_all = annotate_bundle_overlap(shared_jhs_norm, bnd_a_all, bnd_b_all)
    n_both_used, n_aonly_used, n_bonly_used = annotate_bundle_overlap(shared_jhs_norm, bnd_a_used, bnd_b_used)

    all_has_bundles_a = "true" if bnd_a_all else "false"
    all_has_bundles_b = "true" if bnd_b_all else "false"
    used_has_bundles_a = "true" if bnd_a_used else "false"
    used_has_bundles_b = "true" if bnd_b_used else "false"

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
        all_has_bundles_a=all_has_bundles_a,
        all_has_bundles_b=all_has_bundles_b,
        all_n_shared_bundle_both=n_both_all,
        all_n_shared_bundle_a_only=n_aonly_all,
        all_n_shared_bundle_b_only=n_bonly_all,
        used_has_bundles_a=used_has_bundles_a,
        used_has_bundles_b=used_has_bundles_b,
        used_n_shared_bundle_both=n_both_used,
        used_n_shared_bundle_a_only=n_aonly_used,
        used_n_shared_bundle_b_only=n_bonly_used,
        used_n_shared_join_hash=str(used_n_shared_jh),
        used_jaccard_mean=metrics_used.get("all_jaccard_mean", ""),
        used_jaccard_p10=metrics_used.get("all_jaccard_p10", ""),
        used_jaccard_p90=metrics_used.get("all_jaccard_p90", ""),
        used_containment_a_in_b_mean=metrics_used.get("all_containment_a_in_b_mean", ""),
        used_containment_a_in_b_min=metrics_used.get("all_containment_a_in_b_min", ""),
        used_containment_b_in_a_mean=metrics_used.get("all_containment_b_in_a_mean", ""),
        used_containment_b_in_a_min=metrics_used.get("all_containment_b_in_a_min", ""),
        data_sufficient=data_suff,
        executed_utc=executed_utc,
    )
    for r in pair_rows:
        r["comparison_run_id"] = crid
    return summary, pair_rows


def _run_pair_domain(
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
    no_delta: bool,
) -> Tuple[Optional[Dict[str, str]], List[Dict[str, str]]]:
    """Wrapper around run_pair for a single pair×domain. Returns (summary_row, detail_rows)."""
    _ = no_delta  # Accepted for future use; run_pair does not currently consume it.
    return run_pair(
        seg_a=seg_a,
        seg_b=seg_b,
        comparison_type=comparison_type,
        domain=domain,
        manifest=manifest,
        registry=registry,
        file_metadata=file_metadata,
        segments_root=segments_root,
        min_patterns=min_patterns,
        executed_utc=executed_utc,
    )


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
    all_has_bundles_a: str,
    all_has_bundles_b: str,
    all_n_shared_bundle_both: int,
    all_n_shared_bundle_a_only: int,
    all_n_shared_bundle_b_only: int,
    used_has_bundles_a: str,
    used_has_bundles_b: str,
    used_n_shared_bundle_both: int,
    used_n_shared_bundle_a_only: int,
    used_n_shared_bundle_b_only: int,
    data_sufficient: str,
    executed_utc: str,
    used_n_shared_join_hash: str = "",
    used_jaccard_mean: str = "",
    used_jaccard_p10: str = "",
    used_jaccard_p90: str = "",
    used_containment_a_in_b_mean: str = "",
    used_containment_a_in_b_min: str = "",
    used_containment_b_in_a_mean: str = "",
    used_containment_b_in_a_min: str = "",
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
        "all_containment_a_in_b_mean": metrics.get("all_containment_a_in_b_mean", ""),
        "all_containment_a_in_b_min": metrics.get("all_containment_a_in_b_min", ""),
        "all_containment_b_in_a_mean": metrics.get("all_containment_b_in_a_mean", ""),
        "all_containment_b_in_a_min": metrics.get("all_containment_b_in_a_min", ""),
        "all_jaccard_mean": metrics.get("all_jaccard_mean", ""),
        "all_jaccard_p10": metrics.get("all_jaccard_p10", ""),
        "all_jaccard_p90": metrics.get("all_jaccard_p90", ""),
        "used_jaccard_mean": used_jaccard_mean,
        "used_jaccard_p10": used_jaccard_p10,
        "used_jaccard_p90": used_jaccard_p90,
        "used_containment_a_in_b_mean": used_containment_a_in_b_mean,
        "used_containment_a_in_b_min": used_containment_a_in_b_min,
        "used_containment_b_in_a_mean": used_containment_b_in_a_mean,
        "used_containment_b_in_a_min": used_containment_b_in_a_min,
        "used_n_shared_join_hash": used_n_shared_join_hash,
        "all_has_bundles_a": all_has_bundles_a,
        "all_has_bundles_b": all_has_bundles_b,
        "all_n_shared_bundle_both": str(all_n_shared_bundle_both),
        "all_n_shared_bundle_a_only": str(all_n_shared_bundle_a_only),
        "all_n_shared_bundle_b_only": str(all_n_shared_bundle_b_only),
        "used_has_bundles_a": used_has_bundles_a,
        "used_has_bundles_b": used_has_bundles_b,
        "used_n_shared_bundle_both": str(used_n_shared_bundle_both),
        "used_n_shared_bundle_a_only": str(used_n_shared_bundle_a_only),
        "used_n_shared_bundle_b_only": str(used_n_shared_bundle_b_only),
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

                # Aggregate pool files — key by (segment_id, export_run_id) so that
                # the same export_run_id appearing in two sibling segments is counted twice
                # rather than silently collapsed into one entry.
                pool_files_keyed: Dict[Tuple[str, str], Set[str]] = {}
                for pool_sid in pool_sids:
                    pf = load_file_join_hashes(segments_root, registry, pool_sid, domain)
                    for eid, jhs in pf.items():
                        pool_files_keyed[(pool_sid, eid)] = jhs

                pool_union: Set[str] = set()
                for jhs in pool_files_keyed.values():
                    pool_union |= jhs

                if len(pool_union) < min_patterns:
                    continue

                shared = focal_union & pool_union
                n_shared = len(shared)
                n_focal_unique = len(focal_union)
                n_pool_unique = len(pool_union)

                c_focal_in_pool = n_shared / n_focal_unique if n_focal_unique else 0.0
                c_pool_in_focal = n_shared / n_pool_unique if n_pool_unique else 0.0

                # Used-view containment
                focal_files_used = load_file_join_hashes(
                    segments_root, registry, focal_sid, domain, "used"
                )
                focal_union_used: Set[str] = set()
                for jhs in focal_files_used.values():
                    focal_union_used |= jhs
                pool_files_used_keyed: Dict[Tuple[str, str], Set[str]] = {}
                for pool_sid in pool_sids:
                    pf_u = load_file_join_hashes(
                        segments_root, registry, pool_sid, domain, "used"
                    )
                    for eid, jhs in pf_u.items():
                        pool_files_used_keyed[(pool_sid, eid)] = jhs
                pool_union_used: Set[str] = set()
                for jhs in pool_files_used_keyed.values():
                    pool_union_used |= jhs
                shared_used = focal_union_used & pool_union_used
                used_c_focal_in_pool = (
                    len(shared_used) / len(focal_union_used) if focal_union_used else 0.0
                )
                used_c_pool_in_focal = (
                    len(shared_used) / len(pool_union_used) if pool_union_used else 0.0
                )

                n_files_focal = len(focal_files)
                n_files_pool = len(pool_files_keyed)
                data_suff = "true" if (n_files_focal >= 5 and n_files_pool >= 5) else "false"

                # Bundle annotation — dual-view
                focal_bundle_all = load_bundle_join_hash_set(
                    segments_root, registry, focal_sid, domain, "all"
                )
                focal_bundle_used = load_bundle_join_hash_set(
                    segments_root, registry, focal_sid, domain, "used"
                )
                pool_bundle_all: Set[str] = set()
                pool_bundle_used: Set[str] = set()
                for pool_sid in pool_sids:
                    pool_bundle_all |= load_bundle_join_hash_set(
                        segments_root, registry, pool_sid, domain, "all"
                    )
                    pool_bundle_used |= load_bundle_join_hash_set(
                        segments_root, registry, pool_sid, domain, "used"
                    )

                all_has_bundles_focal = "true" if focal_bundle_all else "false"
                all_has_bundles_pool = "true" if pool_bundle_all else "false"
                used_has_bundles_focal = "true" if focal_bundle_used else "false"
                used_has_bundles_pool = "true" if pool_bundle_used else "false"

                n_both_all, n_focal_only_all, n_pool_only_all = annotate_bundle_overlap(
                    shared, focal_bundle_all, pool_bundle_all
                )
                n_both_used, n_focal_only_used, n_pool_only_used = annotate_bundle_overlap(
                    shared, focal_bundle_used, pool_bundle_used
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
                    "all_containment_focal_in_pool": _fmt(c_focal_in_pool),
                    "all_containment_pool_in_focal": _fmt(c_pool_in_focal),
                    "used_containment_focal_in_pool": _fmt(used_c_focal_in_pool),
                    "used_containment_pool_in_focal": _fmt(used_c_pool_in_focal),
                    "all_has_bundles_focal": all_has_bundles_focal,
                    "all_has_bundles_pool": all_has_bundles_pool,
                    "all_n_shared_bundle_both": str(n_both_all),
                    "all_n_shared_bundle_focal_only": str(n_focal_only_all),
                    "all_n_shared_bundle_pool_only": str(n_pool_only_all),
                    "used_has_bundles_focal": used_has_bundles_focal,
                    "used_has_bundles_pool": used_has_bundles_pool,
                    "used_n_shared_bundle_both": str(n_both_used),
                    "used_n_shared_bundle_focal_only": str(n_focal_only_used),
                    "used_n_shared_bundle_pool_only": str(n_pool_only_used),
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
    ap.add_argument("--workers", type=int, default=4,
                    help="Max parallel pair×domain workers (default: 4)")

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

    if args.workers < 1:
        sys.exit("[error] --workers must be >= 1")

    runnable_pairs = [
        (seg_a, seg_b, ctype)
        for seg_a, seg_b, ctype in pairs
        if segment_is_runnable(registry, seg_a)
        and (seg_a == seg_b or segment_is_runnable(registry, seg_b))
    ]

    # Discover active domains across all relevant segments
    all_segment_ids = sorted({seg for pair in runnable_pairs for seg in (pair[0], pair[1])})
    active_domains: Set[str] = set()
    for sid in all_segment_ids:
        rec = registry.get(sid, {})
        out_folder = rec.get("output_folder", "").strip()
        if not out_folder:
            continue
        presence_csv = segments_root / out_folder / "results" / "analysis" / "pattern_presence_file.csv"
        if presence_csv.is_file():
            for row in read_csv_rows(presence_csv):
                dom = row.get("domain", "").strip()
                if dom:
                    active_domains.add(dom)

    domain_filter = [args.domain] if args.domain else sorted(active_domains)

    # Build flat work list: one item per (pair × domain)
    work_items = [
        (seg_a, seg_b, ctype, dom)
        for seg_a, seg_b, ctype in runnable_pairs
        for dom in domain_filter
    ]

    print(
        f"[compare] {len(runnable_pairs)} pairs × {len(domain_filter)} domains = "
        f"{len(work_items)} work items  workers={args.workers}"
    )

    n_complete = 0
    n_skipped = 0

    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        future_to_item = {
            executor.submit(
                _run_pair_domain,
                seg_a, seg_b, ctype, dom,
                manifest, registry, file_metadata,
                segments_root, args.min_patterns,
                executed_utc, args.no_delta,
            ): (seg_a, seg_b, ctype, dom)
            for seg_a, seg_b, ctype, dom in work_items
        }
        for future in as_completed(future_to_item):
            seg_a, seg_b, ctype, domain = future_to_item[future]
            try:
                result, pairs_out = future.result()
            except Exception as exc:
                for pending in future_to_item:
                    if pending is not future:
                        pending.cancel()
                raise RuntimeError(
                    f"pair=({seg_a}, {seg_b}) type={ctype} domain={domain} failed"
                ) from exc

            if result is not None:
                summary_rows.append(result)
                pair_detail_rows.extend(pairs_out)
                n_complete += 1
                n_p = result.get("n_pairs", "?")
                print(
                    f"[compare] segment_a={seg_a} segment_b={seg_b} "
                    f"domain={domain} pairs={n_p}"
                )

                # Delta pattern output — directed pairs only, opt-out via --no-delta.
                # Delta generation remains in the parent process so worker results stay
                # limited to the existing (summary_row, detail_rows) contract.
                if not args.no_delta and ctype in DELTA_DIRECTED_TYPES:
                    tgt_files = load_file_join_hashes(segments_root, registry, seg_b, domain)
                    tgt_files_used = load_file_join_hashes(
                        segments_root, registry, seg_b, domain, "used"
                    )
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
                        bnd_tgt_all = load_bundle_join_hash_set(
                            segments_root, registry, seg_b, domain, "all"
                        )
                        bnd_tgt_used = load_bundle_join_hash_set(
                            segments_root, registry, seg_b, domain, "used"
                        )
                        n_tgt_files = len(tgt_files)
                        crid = result.get("comparison_run_id", "")
                        ma = manifest.get(seg_a, {})
                        mb = manifest.get(seg_b, {})

                        for jh in delta_jhs:
                            n_files_in_tgt = sum(1 for jhs in tgt_files.values() if jh in jhs)
                            pct = n_files_in_tgt / n_tgt_files if n_tgt_files else 0.0
                            used_n_files_in_tgt = sum(
                                1 for jhs in tgt_files_used.values() if jh in jhs
                            )
                            used_pct = used_n_files_in_tgt / n_tgt_files if n_tgt_files else 0.0
                            in_container = jh in container_set
                            in_template = jh in template_set
                            is_bnd_all = jh in bnd_tgt_all
                            is_bnd_used = jh in bnd_tgt_used
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
                                "in_any_container": "true" if in_container else "false",
                                "in_any_template": "true" if in_template else "false",
                                "used_pct_files_in_target": _fmt(used_pct),
                                "is_bundle_member_all": "true" if is_bnd_all else "false",
                                "is_bundle_member_used": "true" if is_bnd_used else "false",
                                "delta_class": _classify_delta(
                                    in_container, in_template, is_bnd_all, is_bnd_used
                                ),
                                "executed_utc": executed_utc,
                            })
                        delta_combo_count += 1
            else:
                n_skipped += 1

            done = n_complete + n_skipped
            if done % 50 == 0 or done == len(work_items):
                print(
                    f"[compare] progress: {done}/{len(work_items)} "
                    f"complete={n_complete} skipped={n_skipped}",
                    flush=True,
                )

    elapsed = time.perf_counter() - t0
    print(
        f"[compare] done  pairs={len(runnable_pairs)}  domains={len(domain_filter)}  "
        f"work_items={len(work_items)}  complete={n_complete}  skipped={n_skipped}  "
        f"elapsed={elapsed:.1f}s  ({elapsed/60:.1f} min)",
        flush=True,
    )

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
