"""Cross-segment comparison tool.

Discovers comparison pairs from the segment manifest hierarchy and computes
join_hash overlap metrics using bundle or file membership data.

Usage:
    python tools/compare_cross_segment.py \
        --segments-root segments/ \
        --records-dir   results/records/ \
        --out-dir       results/cross_segment/ \
        [--within-segment] [--sibling-segments] [--parent-siblings] \
        [--within-project] [--governance-chain] \
        [--domain DOMAIN] [--segment-a ID] [--segment-b ID] \
        [--min-patterns INT] [--dry-run]
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
    "comparison_type", "comparison_mode",
    "domain",
    "n_patterns_a", "n_patterns_b", "n_shared_join_hash",
    "containment_a_in_b_mean", "containment_a_in_b_min",
    "containment_b_in_a_mean", "containment_b_in_a_min",
    "jaccard_mean", "jaccard_p10", "jaccard_p90",
    "n_bundles_a", "n_bundles_b",
    "n_files_a", "n_files_b", "n_pairs",
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
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_manifest(segments_root: Path) -> Dict[str, Dict[str, str]]:
    path = segments_root / "segment_manifest.csv"
    if not path.exists():
        sys.exit(f"[error] segment_manifest.csv not found at {path}")
    return {row["segment_id"]: row for row in read_csv_rows(path)}


def load_registry(segments_root: Path) -> Dict[str, Dict[str, str]]:
    path = segments_root / "run_registry.csv"
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


# ---------------------------------------------------------------------------
# Membership loading
# ---------------------------------------------------------------------------

def load_file_join_hashes(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Dict[str, Set[str]]:
    """Return {export_run_id: set_of_join_hashes}."""
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


def load_bundle_join_hashes(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Dict[str, Set[str]]:
    """Return {bundle_id: set_of_join_hashes}. Empty dict if step6 not run."""
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return {}

    bm_path = bundle_analysis_dir(seg_out, domain) / "bundle_membership.csv"
    if not bm_path.exists():
        return {}

    jh_map = resolve_join_hashes(segments_root, registry, segment_id, domain)
    result: Dict[str, Set[str]] = defaultdict(set)
    for row in read_csv_rows(bm_path):
        bid = row.get("bundle_id", "").strip()
        pid = row.get("pattern_id", "").strip()
        if not bid or not pid:
            continue
        jh = jh_map.get(pid)
        if jh:
            result[bid].add(jh)
    return dict(result)


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

def compare_directed_bundle(
    ref_bundles: Dict[str, Set[str]],
    tgt_bundles: Dict[str, Set[str]],
) -> Dict[str, str]:
    """Reference side: union; target side: per-bundle containment."""
    ref_union: Set[str] = set()
    for jhs in ref_bundles.values():
        ref_union |= jhs

    if not ref_union:
        return {}

    containment_rates: List[float] = []
    top_ref_cov: List[float] = []

    for jhs in tgt_bundles.values():
        shared = len(jhs & ref_union)
        containment_rates.append(shared / len(ref_union))
        top_ref_cov.append(shared / len(jhs) if jhs else 0.0)

    shared_all: Set[str] = set()
    for jhs in tgt_bundles.values():
        shared_all |= jhs & ref_union

    all_a: Set[str] = ref_union
    all_b: Set[str] = set()
    for jhs in tgt_bundles.values():
        all_b |= jhs

    return {
        "n_shared_join_hash": str(len(all_a & all_b)),
        "containment_a_in_b_mean": _mean(top_ref_cov),
        "containment_a_in_b_min": _min(top_ref_cov),
        "containment_b_in_a_mean": _mean(containment_rates),
        "containment_b_in_a_min": _min(containment_rates),
        "n_bundles_a": str(len(ref_bundles)),
        "n_bundles_b": str(len(tgt_bundles)),
        "n_pairs": str(len(tgt_bundles)),
    }


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
# Comparison engine — symmetric (Jaccard)
# ---------------------------------------------------------------------------

def compare_symmetric_bundle(
    bundles_a: Dict[str, Set[str]],
    bundles_b: Dict[str, Set[str]],
) -> Dict[str, str]:
    jaccards: List[float] = []
    for jhs_a in bundles_a.values():
        for jhs_b in bundles_b.values():
            union = jhs_a | jhs_b
            jaccards.append(len(jhs_a & jhs_b) / len(union) if union else 0.0)

    all_a: Set[str] = set()
    for jhs in bundles_a.values():
        all_a |= jhs
    all_b: Set[str] = set()
    for jhs in bundles_b.values():
        all_b |= jhs

    return {
        "n_shared_join_hash": str(len(all_a & all_b)),
        "jaccard_mean": _mean(jaccards),
        "jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
        "jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
        "n_bundles_a": str(len(bundles_a)),
        "n_bundles_b": str(len(bundles_b)),
        "n_pairs": str(len(jaccards)),
    }


def compare_symmetric_file(
    files_a: Dict[str, Set[str]],
    files_b: Dict[str, Set[str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """Return (summary_metrics, pairwise_rows)."""
    jaccards: List[float] = []
    pair_rows: List[Dict[str, str]] = []

    for eid_a, jhs_a in files_a.items():
        for eid_b, jhs_b in files_b.items():
            union = jhs_a | jhs_b
            j = len(jhs_a & jhs_b) / len(union) if union else 0.0
            c_ab = len(jhs_a & jhs_b) / len(jhs_a) if jhs_a else 0.0
            c_ba = len(jhs_a & jhs_b) / len(jhs_b) if jhs_b else 0.0
            jaccards.append(j)
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
    # Scoped by client_label (and discipline_label when populated)
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
        # Check if there are ≥2 files mapping to different projects
        mm_any = next(ba_root.glob("*/membership_matrix.csv"), None)
        if mm_any is None:
            continue
        eids: Set[str] = set()
        for row in read_csv_rows(mm_any):
            eid = row.get("export_run_id", "").strip()
            if eid:
                eids.add(eid)
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
    seen: Dict[Tuple[str, str], str] = {}
    result: List[ComparisonPair] = []
    # Directed types take priority over symmetric when same (a,b) appears
    priority = {
        "template_to_project": 0,
        "template_to_container": 1,
        "container_to_project": 2,
        "parent_sibling_roles": 3,
        "sibling_templates": 4,
        "sibling_projects": 5,
        "sibling_containers": 6,
        "within_project": 7,
    }
    for a, b, ctype in pairs:
        k = (a, b) if ctype not in DIRECTED_TYPES else (a, b)
        existing = seen.get(k)
        if existing is None or priority.get(ctype, 99) < priority.get(existing, 99):
            seen[k] = ctype
    # rebuild in stable order
    done: Set[Tuple[str, str]] = set()
    for a, b, ctype in pairs:
        k = (a, b)
        if k not in done and seen.get(k) == ctype:
            result.append((a, b, ctype))
            done.add(k)
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
    """Return (summary_row_or_None, pair_detail_rows)."""
    is_directed = comparison_type in DIRECTED_TYPES
    is_within_project = comparison_type == "within_project"

    # Determine mode
    if not is_within_project:
        bnd_a = load_bundle_join_hashes(segments_root, registry, seg_a, domain)
        bnd_b = load_bundle_join_hashes(segments_root, registry, seg_b, domain)
        use_bundle = bool(bnd_a) and bool(bnd_b)
    else:
        bnd_a = bnd_b = {}
        use_bundle = False

    mode = "bundle" if use_bundle else "file"

    # Load file-level data
    if not use_bundle:
        files_a = load_file_join_hashes(segments_root, registry, seg_a, domain)
        files_b = load_file_join_hashes(segments_root, registry, seg_b, domain)
    else:
        files_a = files_b = {}

    # For within_project: group by project_label within the single segment
    if is_within_project:
        all_files = load_file_join_hashes(segments_root, registry, seg_a, domain)
        by_proj: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
        for eid, jhs in all_files.items():
            meta = file_metadata.get(eid, {})
            proj = meta.get("project_label", "").strip() or eid
            by_proj[proj][eid] = jhs

        all_summary_rows: List[Dict[str, str]] = []
        all_pair_rows: List[Dict[str, str]] = []

        for proj, proj_files in by_proj.items():
            if len(proj_files) < 2:
                continue
            eids_sorted = sorted(proj_files.keys())
            for i in range(len(eids_sorted)):
                for j in range(i + 1, len(eids_sorted)):
                    eid_a2 = eids_sorted[i]
                    eid_b2 = eids_sorted[j]
                    fa = {eid_a2: proj_files[eid_a2]}
                    fb = {eid_b2: proj_files[eid_b2]}
                    sm, pr = compare_symmetric_file(fa, fb)
                    if not sm:
                        continue
                    crid = make_comparison_run_id(eid_a2, eid_b2, executed_utc)
                    summary_row = _build_summary_row(
                        crid, seg_a, seg_b, comparison_type, mode, domain,
                        manifest, file_metadata,
                        sm,
                        n_patterns_a=len(proj_files[eid_a2]),
                        n_patterns_b=len(proj_files[eid_b2]),
                        executed_utc=executed_utc,
                    )
                    all_summary_rows.append(summary_row)
                    for r in pr:
                        r.update({
                            "comparison_run_id": crid,
                            "segment_id_a": seg_a,
                            "segment_id_b": seg_b,
                            "domain": domain,
                            "project_label_a": proj,
                            "project_label_b": proj,
                        })
                        all_pair_rows.append(r)

        # Return first summary row with aggregate (or None if nothing)
        if not all_summary_rows:
            return None, []
        # For within_project, callers handle multiple summaries via list;
        # here we return the first and expect caller to handle list form
        # Actually: return a sentinel summary row that merges; simpler to return list
        # We abuse the tuple: return ("MULTI", all_summary_rows, all_pair_rows)
        # Instead, handle in caller — just return the list packed into a dummy dict
        # We'll use a special return marker
        return {"__multi__": "1", "__rows__": all_summary_rows, "__pairs__": all_pair_rows}, []  # type: ignore[return-value]

    # Normal path
    all_jhs_a: Set[str] = set()
    all_jhs_b: Set[str] = set()

    if use_bundle:
        for jhs in bnd_a.values():
            all_jhs_a |= jhs
        for jhs in bnd_b.values():
            all_jhs_b |= jhs
    else:
        for jhs in files_a.values():
            all_jhs_a |= jhs
        for jhs in files_b.values():
            all_jhs_b |= jhs

    n_a = len(all_jhs_a)
    n_b = len(all_jhs_b)

    if n_a < min_patterns or n_b < min_patterns:
        return None, []

    # Compute metrics
    pair_rows: List[Dict[str, str]] = []

    if use_bundle:
        if is_directed:
            metrics = compare_directed_bundle(bnd_a, bnd_b)
        else:
            metrics = compare_symmetric_bundle(bnd_a, bnd_b)
    else:
        if is_directed:
            metrics = compare_directed_file(files_a, files_b)
        else:
            metrics, pair_rows_raw = compare_symmetric_file(files_a, files_b)
            n_pairs_val = int(metrics.get("n_pairs", "0"))
            if n_pairs_val <= 50:
                crid = make_comparison_run_id(seg_a, seg_b, executed_utc)
                for r in pair_rows_raw:
                    r.update({
                        "comparison_run_id": crid,
                        "segment_id_a": seg_a,
                        "segment_id_b": seg_b,
                        "domain": domain,
                        "project_label_a": file_metadata.get(r.get("export_run_id_a", ""), {}).get("project_label", ""),
                        "project_label_b": file_metadata.get(r.get("export_run_id_b", ""), {}).get("project_label", ""),
                    })
                pair_rows = pair_rows_raw

    if not metrics:
        return None, []

    crid = make_comparison_run_id(seg_a, seg_b, executed_utc)
    summary = _build_summary_row(
        crid, seg_a, seg_b, comparison_type, mode, domain,
        manifest, file_metadata,
        metrics,
        n_patterns_a=n_a,
        n_patterns_b=n_b,
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
    mode: str,
    domain: str,
    manifest: Dict[str, Dict[str, str]],
    file_metadata: Dict[str, Dict[str, str]],
    metrics: Dict[str, str],
    n_patterns_a: int,
    n_patterns_b: int,
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
        "comparison_mode": mode,
        "domain": domain,
        "n_patterns_a": str(n_patterns_a),
        "n_patterns_b": str(n_patterns_b),
        "n_shared_join_hash": metrics.get("n_shared_join_hash", ""),
        "containment_a_in_b_mean": metrics.get("containment_a_in_b_mean", ""),
        "containment_a_in_b_min": metrics.get("containment_a_in_b_min", ""),
        "containment_b_in_a_mean": metrics.get("containment_b_in_a_mean", ""),
        "containment_b_in_a_min": metrics.get("containment_b_in_a_min", ""),
        "jaccard_mean": metrics.get("jaccard_mean", ""),
        "jaccard_p10": metrics.get("jaccard_p10", ""),
        "jaccard_p90": metrics.get("jaccard_p90", ""),
        "n_bundles_a": metrics.get("n_bundles_a", ""),
        "n_bundles_b": metrics.get("n_bundles_b", ""),
        "n_files_a": metrics.get("n_files_a", ""),
        "n_files_b": metrics.get("n_files_b", ""),
        "n_pairs": metrics.get("n_pairs", ""),
        "executed_utc": executed_utc,
    }


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
            f"[warn] segment={segment_id} has run_type={rt!r} — treating as file-mode only",
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
                    help="Root directory containing segment_manifest.csv and run_registry.csv")
    ap.add_argument("--records-dir", required=True, metavar="DIR",
                    help="Directory containing file_metadata.csv")
    ap.add_argument("--out-dir", required=True, metavar="DIR",
                    help="Output directory for cross_segment_summary.csv and cross_segment_file_pairs.csv")

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

    manifest = load_manifest(segments_root)
    registry = load_registry(segments_root)
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
        print(f"{'segment_a':<{col_w}}  {'segment_b':<{col_w}}  {'comparison_type':<28}  mode")
        print("-" * (col_w * 2 + 60))
        for a, b, ctype in pairs:
            # Peek at mode without loading data
            seg_out_a = segment_output_dir(segments_root, registry, a)
            seg_out_b = segment_output_dir(segments_root, registry, b)
            mode_hint = "?"
            if seg_out_a and seg_out_b:
                # Check any domain for bundle_membership
                ba_a = seg_out_a / "results" / "bundle_analysis"
                ba_b = seg_out_b / "results" / "bundle_analysis"
                if ba_a.exists() and ba_b.exists():
                    domains_a = {p.name for p in ba_a.iterdir() if p.is_dir()}
                    if domains_a:
                        d_probe = next(iter(sorted(domains_a)))
                        bma = ba_a / d_probe / "bundle_membership.csv"
                        bmb = ba_b / d_probe / "bundle_membership.csv"
                        mode_hint = "bundle" if (bma.exists() and bmb.exists()) else "file"
            la = manifest.get(a, {}).get("segment_label", a)
            lb = manifest.get(b, {}).get("segment_label", b)
            print(f"{la:<{col_w}}  {lb:<{col_w}}  {ctype:<28}  {mode_hint}")
        print(f"\n[compare] {len(pairs)} pairs discovered")
        return 0

    # Run comparisons
    executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_rows: List[Dict[str, str]] = []
    pair_detail_rows: List[Dict[str, str]] = []

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

            # Handle within_project multi-row sentinel
            if isinstance(result, dict) and result.get("__multi__"):
                multi_rows = result["__rows__"]  # type: ignore[index]
                multi_pairs = result["__pairs__"]  # type: ignore[index]
                summary_rows.extend(multi_rows)
                pair_detail_rows.extend(multi_pairs)
                n_total = len(multi_rows)
                print(
                    f"[compare] segment_a={seg_a} segment_b={seg_b} "
                    f"domain={domain} mode=file within_project pairs={n_total}"
                )
            else:
                summary_rows.append(result)
                pair_detail_rows.extend(pairs_out)
                mode_val = result.get("comparison_mode", "?")
                n_p = result.get("n_pairs", "?")
                print(
                    f"[compare] segment_a={seg_a} segment_b={seg_b} "
                    f"domain={domain} mode={mode_val} pairs={n_p}"
                )

    # Write outputs
    if summary_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(out_dir / "cross_segment_summary.csv", SUMMARY_FIELDS, summary_rows)
        print(f"[compare] wrote {len(summary_rows)} rows → {out_dir / 'cross_segment_summary.csv'}")

    if pair_detail_rows:
        atomic_write_csv(out_dir / "cross_segment_file_pairs.csv", PAIRS_FIELDS, pair_detail_rows)
        print(f"[compare] wrote {len(pair_detail_rows)} rows → {out_dir / 'cross_segment_file_pairs.csv'}")

    if not summary_rows:
        print("[compare] no comparison rows produced — check segment data and min-patterns threshold")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
