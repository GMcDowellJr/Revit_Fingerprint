#!/usr/bin/env python3
"""Segment decomposition analytics (D-052).

Downstream of compare_cross_segment.py. Runs over every runnable segment of one
governance role (default Project) and answers three descriptive questions the
standard cross-segment pull cannot:

  A. Split lift (segment_split_lift.csv) -- for a parent segment P and its
     children C1..Cn that differ from P by exactly one --split-dimension value
     (default discipline_label), how internally consistent is each Ci compared
     with how consistent it is with its siblings?
       * internal consistency = segment-wide mean file-pair Jaccard over ALL
         file pairs inside Ci, recomputed from Ci's membership_matrix.csv
         (see "Recompute path" below). The within_project_only_mean column
         carries compare_cross_segment.py's within_project value separately and
         is NOT segment-wide consistency -- see "Why not the matrix diagonal".
       * sibling consistency = cross_segment_summary.csv's
         {all,used}_pairwise_jaccard_mean for each symmetric (Ci, Cj)
         comparison, keyed by segment_id.
     Both are equal-domain means over the domains that have a value
     (ALL_DOMAINS, the same aggregation compare_cross_segment.py uses for its
     matrix). The two sides are not forced onto a common domain set: internal
     covers the child's own domains, a sibling value covers the domains that
     comparison scored (internal_n_domains is reported). Each child row reports equal-sibling-weighted and
     file-weighted (manifest file_count) sibling means, the two lifts, and
     weighting_difference = lift_file_weighted - lift_equal_weighted. One parent
     row per family aggregates its children (pair weight n_i*n_j for the
     file-weighted cross-sibling mean). Siblings compare_cross_segment.py never
     compared emit row_status=unavailable, skip_reason=no_sibling_comparison --
     no Jaccard is computed here to fill that gap.

  B. Domain leave-one-out impact (segment_domain_loo_impact.csv) -- per
     segment and domain, impact = mean_all - mean_without, where mean_all is the
     equal-domain mean of available domain values and mean_without drops that
     one domain. Emitted on two bases, labeled in the `basis` column:
       * segment_wide        -- recomputed segment-wide domain means;
       * within_project_only -- compare_cross_segment.py's within_project
         summary values. These are exactly what the matrix diagonal of
         project_mean_file_pair_jaccard_matrix.csv publishes, so this basis
         carries a drift guard: the recalculated mean_all is compared with the
         published ALL_DOMAINS diagonal cell (value_status == "ok" only); any
         |difference| > drift_tolerance (policy JSON) is recorded in
         segment_decomposition_run.json as a named finding and printed. It never
         fails the run.

  C. Project effect (segment_project_effect_by_{domain,category,segment}.csv)
     -- per segment, mean Jaccard of file pairs within the same project_label
     vs between different project_labels, recomputed from membership_matrix.csv.
     Category/segment rollups give equal-domain and comparison-weighted
     variants over the domains that have BOTH a within and a between value, with
     the pair counts they were computed from.

Recompute path
--------------
cross_segment_file_pairs.csv cannot answer A's internal consistency or C:
compare_cross_segment.py's only same-segment comparison is within_project, so
the file has no same-segment between-project pairs, and pooling its rows across
nested segments double counts (a parent's within_project pair is often also a
child's within_project pair or a cross-sibling row). This tool therefore never
reads that file. For each segment, domain and view it loads per-file join_hash
sets through compare_cross_segment.py's own load_file_join_hashes() and scores
every intra-segment pair with its _jaccard_sets() -- imported, not copied -- so
every statistic is scoped to exactly one segment and a file pair is counted at
most once per segment.

Reconciliation gate: the recomputed within-project mean must equal
compare_cross_segment.py's within_project summary value for every
(segment, view, domain) within drift_tolerance. A mismatch (or a summary value
with no recomputed within-project pairs) is a hard failure: it is listed in
segment_decomposition_run.json, the CSV outputs are not written (stale copies
are removed), and the exit code is 1. A recomputed value with no summary row is
a coverage note, not a failure (compare_cross_segment.py drops a within_project
row below --min-patterns).

Missing values
--------------
* Unavailable values are blank, never 0. A used-view file pair where either
  file is absent from the used vocabulary is excluded from every used-view mean
  and counted in n_pairs_missing_view (compare_cross_segment.py's within_project
  branch writes 0.0 for such pairs into cross_segment_file_pairs.csv; this tool
  deliberately does not).
* A file whose project_label is blank or an N/A spelling (na_token) is excluded
  from both project-effect buckets; pairs involving it are counted in
  n_pairs_excluded_unlabeled and the file in n_unlabeled_files. It still counts
  toward the segment-wide mean, which is over ALL file pairs.
* Every derived row carries the counts it was computed from.

Why not the matrix diagonal
---------------------------
project_mean_file_pair_jaccard_matrix.csv's observed self cells come from
within_project comparisons, i.e. only file pairs sharing a project_label;
segments with no project of >= 2 files get a synthetic 1.0. Its row_id /
column_id are segment_labels, which are not unique across unit_system. The
matrix is read only for analysis B's drift guard, resolved to segment_id via the
manifest (ambiguous labels are skipped and listed in the run summary).

Known limitation
----------------
Within-project similarity is partly mechanical -- files of one project share
copied and linked content -- so a high within-project mean, a positive project
effect, or a positive split lift does not by itself indicate governance quality.

Presentation bands
------------------
presentation_band_descriptive columns are descriptive presentation bands, not
governance thresholds. Band edges live in policies/domain_functional_categories.json.
Nothing here feeds assign_tier() or governance_domain_summary.csv; like the
narrative's Project Portfolio section, these are segment/portfolio diagnostics
that never override a domain tier.

Usage:
    python tools/segment_decomposition.py \\
        --records-dir results/records \\
        --cross-segment-dir results/cross_segment \\
        --segments-root segments \\
        [--out-dir DIR] [--view-scope used|all] [--split-dimension discipline_label] \\
        [--governance-role Project] [--parent-segment SEGMENT_ID] [--client CLIENT_LABEL] \\
        [--policy policies/domain_functional_categories.json]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

import compare_cross_segment as ccs  # noqa: E402
from build_segment_manifest import DIMENSION_CONFIG  # noqa: E402
from na_token import is_blank_or_na  # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

SCHEMA_VERSION = "segment_decomposition.v1"
POLICY_SCHEMA = "domain_functional_categories.v1"
DEFAULT_POLICY_PATH = Path(__file__).resolve().parent.parent / "policies" / "domain_functional_categories.json"
UNMAPPED = "UNMAPPED"
ALL_DOMAINS = "ALL_DOMAINS"
RUNNABLE_RUN_TYPES = ("bundle", "reference")
DIMENSION_FIELDS: List[str] = [d["field"] for d in DIMENSION_CONFIG]
SPLITTABLE_FIELDS: List[str] = [d["field"] for d in DIMENSION_CONFIG if d["type"] == "cut"]
VIEW_COLUMN = {"all": "all_pairwise_jaccard_mean", "used": "used_pairwise_jaccard_mean"}

OUTPUT_FILES = {
    "split_lift": "segment_split_lift.csv",
    "loo": "segment_domain_loo_impact.csv",
    "effect_domain": "segment_project_effect_by_domain.csv",
    "effect_category": "segment_project_effect_by_category.csv",
    "effect_segment": "segment_project_effect_by_segment.csv",
}
RUN_SUMMARY_FILE = "segment_decomposition_run.json"

SPLIT_LIFT_FIELDS = [
    "row_grain", "view_scope", "split_dimension",
    "parent_segment_id", "parent_segment_label", "parent_file_count",
    "parent_segment_wide_mean", "parent_within_project_only_mean", "n_children",
    "segment_id", "segment_label", "split_value", "file_count",
    "internal_mean_equal_weighted", "internal_mean_file_weighted",
    "internal_n_domains", "internal_n_pairs",
    "within_project_only_mean", "within_project_only_n_domains",
    "n_siblings", "n_siblings_with_value",
    "sibling_mean_equal_weighted", "sibling_mean_file_weighted", "sibling_weight_sum",
    "lift_equal_weighted", "lift_file_weighted", "weighting_difference",
    "presentation_band_descriptive", "row_status", "skip_reason",
]

LOO_FIELDS = [
    "basis", "view_scope", "segment_id", "segment_label", "domain", "functional_category",
    "domain_value", "domain_n_pairs", "n_domains_available", "mean_all", "mean_without", "impact",
    "published_all_domains", "published_status", "recalc_difference",
    "presentation_band_descriptive", "row_status", "skip_reason",
]

EFFECT_DOMAIN_FIELDS = [
    "view_scope", "segment_id", "segment_label", "domain", "functional_category",
    "n_files", "n_files_all_view", "n_projects", "n_unlabeled_files",
    "n_segment_pairs", "segment_wide_mean",
    "n_within_pairs", "within_mean", "n_between_pairs", "between_mean", "project_effect",
    "n_pairs_excluded_unlabeled", "n_pairs_missing_view",
    "compare_within_project_mean", "reconciliation_status",
    "presentation_band_descriptive", "row_status", "skip_reason",
]

_ROLLUP_FIELDS = [
    "n_domains", "n_domains_with_both",
    "equal_domain_within_mean", "equal_domain_between_mean", "equal_domain_effect",
    "comparison_weighted_within_mean", "comparison_weighted_between_mean", "comparison_weighted_effect",
    "n_within_pairs", "n_between_pairs", "n_pairs_excluded_unlabeled",
    "presentation_band_descriptive", "row_status", "skip_reason",
]
EFFECT_CATEGORY_FIELDS = ["view_scope", "segment_id", "segment_label", "functional_category"] + _ROLLUP_FIELDS
EFFECT_SEGMENT_FIELDS = (
    ["view_scope", "segment_id", "segment_label", "n_files", "n_projects", "n_unlabeled_files", "n_unmapped_domains"]
    + _ROLLUP_FIELDS
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _f(value: Optional[float]) -> str:
    return ccs._fmt(value) if value is not None else ""


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _weighted_mean(pairs: Sequence[Tuple[float, float]]) -> Optional[float]:
    """Mean of (value, weight) pairs; None when there is no positive weight."""
    total = sum(w for _v, w in pairs)
    return sum(v * w for v, w in pairs) / total if total > 0 else None


def _diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    return a - b if a is not None and b is not None else None


def _int(value: str) -> int:
    try:
        return int((value or "").strip())
    except ValueError:
        return 0


def _pairs(n: int) -> int:
    return n * (n - 1) // 2


def band_for(value: Optional[float], bands: Sequence[Dict[str, object]]) -> str:
    if value is None:
        return ""
    for band in bands:
        lower, upper = band.get("lower"), band.get("upper")
        if (lower is None or value >= float(lower)) and (upper is None or value < float(upper)):
            return str(band.get("label", ""))
    return ""


def peak_memory_mb() -> Optional[float]:
    try:
        import resource  # POSIX only
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss / (1024.0 * 1024.0) if sys.platform == "darwin" else rss / 1024.0
    except ImportError:
        pass
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        pmc = _PMC()
        pmc.cb = ctypes.sizeof(_PMC)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        get_info = getattr(kernel32, "K32GetProcessMemoryInfo", None)
        if get_info is None:
            get_info = ctypes.WinDLL("psapi", use_last_error=True).GetProcessMemoryInfo
        get_info.argtypes = [wintypes.HANDLE, ctypes.POINTER(_PMC), wintypes.DWORD]
        get_info.restype = wintypes.BOOL
        if get_info(kernel32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
            return pmc.PeakWorkingSetSize / (1024.0 * 1024.0)
    except Exception:
        return None
    return None


def _file_fingerprint(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"path": str(path), "present": False}
    st = path.stat()
    return {"path": str(path), "present": True, "size_bytes": st.st_size,
            "mtime_utc": datetime.fromtimestamp(st.st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

def load_policy(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as f:
        raw = f.read()
    policy = json.loads(raw)
    if policy.get("schema") != POLICY_SCHEMA:
        raise ValueError(f"{path}: schema must be {POLICY_SCHEMA!r}, got {policy.get('schema')!r}")
    categories = policy.get("categories")
    if not isinstance(categories, dict) or not all(isinstance(v, str) and v for v in categories.values()):
        raise ValueError(f"{path}: 'categories' must map domain -> non-empty category string")
    if UNMAPPED in categories.values():
        raise ValueError(f"{path}: {UNMAPPED!r} is reserved for domains absent from the map")
    tolerance = policy.get("drift_tolerance")
    if not isinstance(tolerance, (int, float)) or tolerance < 0:
        raise ValueError(f"{path}: 'drift_tolerance' must be a non-negative number")
    bands = policy.get("presentation_bands") or {}
    for key in ("split_lift", "project_effect", "domain_loo_impact"):
        if not isinstance(bands.get(key), list):
            raise ValueError(f"{path}: presentation_bands.{key} must be a list")
    policy["_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return policy


# ---------------------------------------------------------------------------
# Segment selection and split families
# ---------------------------------------------------------------------------

def select_segments(
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    governance_role: str,
    client: Optional[str],
) -> Tuple[Dict[str, Dict[str, str]], Counter]:
    """Runnable segments of one governance role (optionally one client)."""
    skips: Counter = Counter()
    eligible: Dict[str, Dict[str, str]] = {}
    for sid, row in manifest.items():
        if row.get("governance_role", "").strip().lower() != governance_role.lower():
            continue
        if client is not None and row.get("client_label", "").strip().lower() != client.lower():
            continue
        if row.get("run_type", "").strip().lower() not in RUNNABLE_RUN_TYPES:
            skips["segment:not_runnable"] += 1
            continue
        if not registry.get(sid, {}).get("output_folder", "").strip():
            skips["segment:no_output_folder"] += 1
            continue
        eligible[sid] = row
    return eligible, skips


def find_split_families(
    segments: Dict[str, Dict[str, str]], split_dimension: str
) -> List[Tuple[str, List[str]]]:
    """(parent, children) where each child differs from the parent by exactly
    one added split_dimension value: same value on every other DIMENSION_CONFIG
    column, split column blank on the parent and set on the child, and one
    segment_level deeper. Pure column comparison -- segment_ids are never parsed."""
    others = [f for f in DIMENSION_FIELDS if f != split_dimension]
    groups: Dict[Tuple[str, ...], List[str]] = defaultdict(list)
    for sid, row in segments.items():
        groups[tuple(row.get(f, "").strip() for f in others)].append(sid)
    families: List[Tuple[str, List[str]]] = []
    for members in groups.values():
        parents = [s for s in members if not segments[s].get(split_dimension, "").strip()]
        kids = [s for s in members if segments[s].get(split_dimension, "").strip()]
        for p in sorted(parents):
            level = _int(segments[p].get("segment_level", ""))
            children = sorted(c for c in kids if _int(segments[c].get("segment_level", "")) == level + 1)
            if children:
                families.append((p, children))
    families.sort(key=lambda fam: fam[0])
    return families


# ---------------------------------------------------------------------------
# Recompute from membership_matrix.csv
# ---------------------------------------------------------------------------

class DomainStats:
    __slots__ = (
        "n_files", "n_files_all", "n_projects", "n_unlabeled_files",
        "seg_sum", "seg_n", "within_sum", "within_n", "between_sum", "between_n",
        "excluded_unlabeled", "missing_view",
    )

    def __init__(self) -> None:
        self.n_files = self.n_files_all = self.n_projects = self.n_unlabeled_files = 0
        self.seg_sum = self.within_sum = self.between_sum = 0.0
        self.seg_n = self.within_n = self.between_n = 0
        self.excluded_unlabeled = self.missing_view = 0

    @property
    def segment_wide_mean(self) -> Optional[float]:
        return self.seg_sum / self.seg_n if self.seg_n else None

    @property
    def within_mean(self) -> Optional[float]:
        return self.within_sum / self.within_n if self.within_n else None

    @property
    def between_mean(self) -> Optional[float]:
        return self.between_sum / self.between_n if self.between_n else None


def recompute_segment(
    segment_id: str,
    view: str,
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    file_metadata: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, DomainStats], Set[str]]:
    """Score every intra-segment file pair per domain for one view, using
    compare_cross_segment.py's own membership reader and Jaccard. Also returns
    the union of export_run_ids seen in this view across domains."""
    out: Dict[str, DomainStats] = {}
    seen: Set[str] = set()
    try:
        for domain in sorted(ccs.discover_domains_for_segment(segments_root, registry, segment_id)):
            files = ccs.load_file_join_hashes(segments_root, registry, segment_id, domain, view)
            all_files = files if view == "all" else ccs.load_file_join_hashes(
                segments_root, registry, segment_id, domain, "all")
            stats = DomainStats()
            stats.n_files = len(files)
            stats.n_files_all = len(all_files)
            # Pairs present in the all view but not scorable in this view are
            # left out of every mean -- never scored as 0.
            stats.missing_view = _pairs(len(set(all_files) | set(files))) - _pairs(len(files))

            eids = sorted(files)
            seen.update(eids)
            labels: List[Optional[str]] = []
            projects: Set[str] = set()
            for eid in eids:
                label = file_metadata.get(eid, {}).get("project_label", "").strip()
                if is_blank_or_na(label):
                    labels.append(None)
                    stats.n_unlabeled_files += 1
                else:
                    labels.append(label)
                    projects.add(label)
            stats.n_projects = len(projects)

            sets = [files[e] for e in eids]
            for i in range(len(eids)):
                a, la = sets[i], labels[i]
                for j in range(i + 1, len(eids)):
                    jac = ccs._jaccard_sets(a, sets[j])
                    if jac is None:  # empty union: load_file_join_hashes never yields empty sets
                        continue
                    stats.seg_sum += jac
                    stats.seg_n += 1
                    lb = labels[j]
                    if la is None or lb is None:
                        stats.excluded_unlabeled += 1
                    elif la == lb:
                        stats.within_sum += jac
                        stats.within_n += 1
                    else:
                        stats.between_sum += jac
                        stats.between_n += 1
            if stats.n_files_all or stats.n_files:
                out[domain] = stats
    finally:
        # compare_cross_segment.py's join_hash cache grows without bound across
        # segments; nothing here re-reads a finished segment.
        ccs._jh_cache.clear()
    return out, seen


# ---------------------------------------------------------------------------
# cross_segment_summary.csv / matrix readers
# ---------------------------------------------------------------------------

def read_summary(
    path: Path,
    view: str,
    segments: Set[str],
    sibling_pairs: Set[FrozenSet[str]],
    tolerance: float,
) -> Tuple[Dict[Tuple[str, str], float], Dict[Tuple[FrozenSet[str], str], float], List[Dict[str, str]], Counter]:
    """Stream the summary once. Returns
      within_ref[(segment_id, domain)] -> within_project value,
      sibling[(frozenset(a, b), domain)] -> symmetric pairwise mean,
      conflicts (same key, different values beyond tolerance -- excluded),
      skip counts."""
    column = VIEW_COLUMN[view]
    skips: Counter = Counter()
    within_vals: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    sibling_vals: Dict[Tuple[FrozenSet[str], str], List[Tuple[float, str]]] = defaultdict(list)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            sa, sb = row.get("segment_id_a", ""), row.get("segment_id_b", "")
            ctype = row.get("comparison_type", "")
            domain = row.get("domain", "")
            raw = (row.get(column) or "").strip()
            if ctype == "within_project" and sa == sb:
                if sa not in segments:
                    continue
                if not raw:
                    skips["summary:within_project_value_blank"] += 1
                    continue
                within_vals[(sa, domain)].append(float(raw))
            elif sa != sb and ctype not in ccs.DIRECTED_TYPES:
                key = frozenset((sa, sb))
                if key not in sibling_pairs:
                    continue
                if not raw:
                    skips["summary:sibling_value_blank"] += 1
                    continue
                sibling_vals[(key, domain)].append((float(raw), ctype))

    conflicts: List[Dict[str, str]] = []
    within_ref: Dict[Tuple[str, str], float] = {}
    for (sid, domain), values in within_vals.items():
        if max(values) - min(values) > tolerance:
            conflicts.append({"kind": "within_project", "segment_id": sid, "domain": domain,
                              "values": ",".join(_f(v) for v in values)})
            continue
        within_ref[(sid, domain)] = values[0]
    sibling: Dict[Tuple[FrozenSet[str], str], float] = {}
    for (key, domain), entries in sibling_vals.items():
        values = [v for v, _c in entries]
        if max(values) - min(values) > tolerance:
            a, b = sorted(key)
            conflicts.append({"kind": "sibling", "segment_id_a": a, "segment_id_b": b, "domain": domain,
                              "values": ",".join(f"{c}={_f(v)}" for v, c in entries)})
            continue
        sibling[(key, domain)] = values[0]
    return within_ref, sibling, conflicts, skips


def read_matrix_diagonal(
    path: Path,
    view: str,
    manifest: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Tuple[str, Optional[float]]], List[str]]:
    """ALL_DOMAINS self cells keyed by resolved segment_id -> (status, value).
    row_id is segment_label when present, else segment_id; resolved through the
    manifest, never by splitting the id. Ambiguous ids are returned separately."""
    by_label: Dict[str, List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        label = row.get("segment_label", "")
        if label:
            by_label[label].append(sid)
    diagonal: Dict[str, Tuple[str, Optional[float]]] = {}
    ambiguous: Set[str] = set()
    if not path.exists():
        return diagonal, []
    for row in ccs.read_csv_rows(path):
        if row.get("domain") != ALL_DOMAINS or row.get("view_scope") != view:
            continue
        rid = row.get("row_id", "")
        if rid != row.get("column_id", ""):
            continue
        candidates = set(by_label.get(rid, []))
        if rid in manifest:
            candidates.add(rid)
        if len(candidates) != 1:
            ambiguous.add(rid)
            continue
        raw = row.get("value", "").strip()
        diagonal[candidates.pop()] = (row.get("value_status", ""), float(raw) if raw else None)
    return diagonal, sorted(ambiguous)


# ---------------------------------------------------------------------------
# Analyses
# ---------------------------------------------------------------------------

def _equal_domain(values: Dict[str, float]) -> Optional[float]:
    return _mean([values[d] for d in sorted(values)])


def build_split_lift_rows(
    families: List[Tuple[str, List[str]]],
    segments: Dict[str, Dict[str, str]],
    split_dimension: str,
    view: str,
    seg_wide: Dict[str, Dict[str, float]],
    seg_pairs: Dict[str, int],
    within_by_seg: Dict[str, Dict[str, float]],
    sibling: Dict[Tuple[FrozenSet[str], str], float],
    bands: Sequence[Dict[str, object]],
    skips: Counter,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    # Index sibling values by pair once (avoid an O(pairs) scan per lookup).
    by_pair: Dict[FrozenSet[str], Dict[str, float]] = defaultdict(dict)
    for (key, domain), value in sibling.items():
        by_pair[key][domain] = value

    for parent, children in families:
        prow = segments[parent]
        base = {
            "view_scope": view, "split_dimension": split_dimension,
            "parent_segment_id": parent, "parent_segment_label": prow.get("segment_label", ""),
            "parent_file_count": prow.get("file_count", ""),
            "parent_segment_wide_mean": _f(_equal_domain(seg_wide.get(parent, {}))),
            "parent_within_project_only_mean": _f(_equal_domain(within_by_seg.get(parent, {}))),
            "n_children": str(len(children)),
        }
        internal: Dict[str, Optional[float]] = {c: _equal_domain(seg_wide.get(c, {})) for c in children}
        fc = {c: _int(segments[c].get("file_count", "")) for c in children}
        pair_vals: Dict[FrozenSet[str], Optional[float]] = {
            frozenset((a, b)): _equal_domain(by_pair.get(frozenset((a, b)), {}))
            for i, a in enumerate(children) for b in children[i + 1:]
        }

        for c in children:
            sibs = [s for s in children if s != c]
            sib_vals = [(pair_vals[frozenset((c, s))], fc[s]) for s in sibs]
            sib_vals = [(v, w) for v, w in sib_vals if v is not None]
            sib_eq = _mean([v for v, _w in sib_vals])
            sib_fw = _weighted_mean(sib_vals)
            own = internal[c]
            lift_eq, lift_fw = _diff(own, sib_eq), _diff(own, sib_fw)
            if not sibs:
                status, reason = "unavailable", "single_child"
            elif not sib_vals:
                status, reason = "unavailable", "no_sibling_comparison"
            elif own is None:
                status, reason = "unavailable", "internal_unavailable"
            else:
                status, reason = "ok", ""
            if reason:
                skips[f"split_lift:{reason}"] += 1
            rows.append({
                **base, "row_grain": "child",
                "segment_id": c, "segment_label": segments[c].get("segment_label", ""),
                "split_value": segments[c].get(split_dimension, ""), "file_count": str(fc[c]),
                "internal_mean_equal_weighted": _f(own), "internal_mean_file_weighted": _f(own),
                "internal_n_domains": str(len(seg_wide.get(c, {}))),
                "internal_n_pairs": str(seg_pairs.get(c, 0)),
                "within_project_only_mean": _f(_equal_domain(within_by_seg.get(c, {}))),
                "within_project_only_n_domains": str(len(within_by_seg.get(c, {}))),
                "n_siblings": str(len(sibs)), "n_siblings_with_value": str(len(sib_vals)),
                "sibling_mean_equal_weighted": _f(sib_eq), "sibling_mean_file_weighted": _f(sib_fw),
                "sibling_weight_sum": str(sum(w for _v, w in sib_vals)),
                "lift_equal_weighted": _f(lift_eq), "lift_file_weighted": _f(lift_fw),
                "weighting_difference": _f(_diff(lift_fw, lift_eq)),
                "presentation_band_descriptive": band_for(lift_eq, bands),
                "row_status": status, "skip_reason": reason,
            })

        # Parent-grain aggregate over its children.
        int_vals = [(internal[c], fc[c]) for c in children if internal[c] is not None]
        int_eq = _mean([v for v, _w in int_vals])
        int_fw = _weighted_mean(int_vals)
        cross = [(v, fc[a] * fc[b]) for key, v in pair_vals.items() if v is not None
                 for a, b in [sorted(key)]]
        cross_eq = _mean([v for v, _w in cross])
        cross_fw = _weighted_mean(cross)
        lift_eq, lift_fw = _diff(int_eq, cross_eq), _diff(int_fw, cross_fw)
        if len(children) < 2:
            status, reason = "unavailable", "single_child"
        elif not cross:
            status, reason = "unavailable", "no_sibling_comparison"
        elif int_eq is None:
            status, reason = "unavailable", "internal_unavailable"
        else:
            status, reason = "ok", ""
        rows.append({
            **base, "row_grain": "parent",
            "segment_id": parent, "segment_label": prow.get("segment_label", ""),
            "split_value": "", "file_count": prow.get("file_count", ""),
            "internal_mean_equal_weighted": _f(int_eq), "internal_mean_file_weighted": _f(int_fw),
            "internal_n_domains": str(len(set().union(*[seg_wide.get(c, {}).keys() for c in children]))),
            "internal_n_pairs": str(sum(seg_pairs.get(c, 0) for c in children)),
            "within_project_only_mean": "", "within_project_only_n_domains": "",
            "n_siblings": str(len(pair_vals)), "n_siblings_with_value": str(len(cross)),
            "sibling_mean_equal_weighted": _f(cross_eq), "sibling_mean_file_weighted": _f(cross_fw),
            "sibling_weight_sum": str(sum(w for _v, w in cross)),
            "lift_equal_weighted": _f(lift_eq), "lift_file_weighted": _f(lift_fw),
            "weighting_difference": _f(_diff(lift_fw, lift_eq)),
            "presentation_band_descriptive": band_for(lift_eq, bands),
            "row_status": status, "skip_reason": reason,
        })
    return rows


def build_loo_rows(
    basis: str,
    view: str,
    segments: Dict[str, Dict[str, str]],
    values_by_seg: Dict[str, Dict[str, float]],
    n_pairs_by_seg_domain: Dict[Tuple[str, str], int],
    categories: Dict[str, str],
    bands: Sequence[Dict[str, object]],
    tolerance: float,
    diagonal: Optional[Dict[str, Tuple[str, Optional[float]]]],
    findings: List[Dict[str, object]],
    skips: Counter,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for sid in sorted(segments):
        values = values_by_seg.get(sid, {})
        if not values:
            skips[f"loo:{basis}:no_domain_values"] += 1
            continue
        n = len(values)
        total = sum(values.values())
        mean_all = total / n
        published, pub_status, recalc = None, "", None
        if diagonal is not None:
            pub_status, published = diagonal.get(sid, ("not_in_matrix", None))
            if pub_status == "ok" and published is not None:
                recalc = mean_all - published
                if abs(recalc) > tolerance:
                    finding = {
                        "finding": "loo_published_vs_recalculated_drift",
                        "segment_id": sid, "view_scope": view,
                        "published_all_domains": published, "recalculated_mean_all": mean_all,
                        "difference": recalc, "tolerance": tolerance,
                    }
                    findings.append(finding)
                    print(f"[drift] {sid} view={view}: published ALL_DOMAINS={published:.6f} "
                          f"recalculated={mean_all:.9f} diff={recalc:.3e} > {tolerance}")
            else:
                skips[f"loo:drift_guard_not_applicable:{pub_status}"] += 1
        for domain in sorted(values):
            mean_without = (total - values[domain]) / (n - 1) if n > 1 else None
            impact = _diff(mean_all, mean_without)
            rows.append({
                "basis": basis, "view_scope": view, "segment_id": sid,
                "segment_label": segments[sid].get("segment_label", ""),
                "domain": domain, "functional_category": categories.get(domain, UNMAPPED),
                "domain_value": _f(values[domain]),
                "domain_n_pairs": str(n_pairs_by_seg_domain[(sid, domain)]) if (sid, domain) in n_pairs_by_seg_domain else "",
                "n_domains_available": str(n),
                "mean_all": _f(mean_all), "mean_without": _f(mean_without), "impact": _f(impact),
                "published_all_domains": _f(published) if pub_status == "ok" else "",
                "published_status": pub_status,
                "recalc_difference": f"{recalc:.3e}" if recalc is not None else "",
                "presentation_band_descriptive": band_for(impact, bands),
                "row_status": "ok" if mean_without is not None else "unavailable",
                "skip_reason": "" if mean_without is not None else "single_domain",
            })
    return rows


def _rollup(domain_stats: Iterable[DomainStats], bands: Sequence[Dict[str, object]]) -> Dict[str, str]:
    stats = list(domain_stats)
    both = [s for s in stats if s.within_n and s.between_n]
    eq_w = _mean([s.within_mean for s in both])
    eq_b = _mean([s.between_mean for s in both])
    wn = sum(s.within_n for s in both)
    bn = sum(s.between_n for s in both)
    cw_w = sum(s.within_sum for s in both) / wn if wn else None
    cw_b = sum(s.between_sum for s in both) / bn if bn else None
    eq_e = _diff(eq_w, eq_b)
    return {
        "n_domains": str(len(stats)), "n_domains_with_both": str(len(both)),
        "equal_domain_within_mean": _f(eq_w), "equal_domain_between_mean": _f(eq_b),
        "equal_domain_effect": _f(eq_e),
        "comparison_weighted_within_mean": _f(cw_w), "comparison_weighted_between_mean": _f(cw_b),
        "comparison_weighted_effect": _f(_diff(cw_w, cw_b)),
        "n_within_pairs": str(wn), "n_between_pairs": str(bn),
        "n_pairs_excluded_unlabeled": str(sum(s.excluded_unlabeled for s in stats)),
        "presentation_band_descriptive": band_for(eq_e, bands),
        "row_status": "ok" if both else "unavailable",
        "skip_reason": "" if both else "no_domain_with_within_and_between_pairs",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--records-dir", required=True, type=Path,
                    help="Directory with segment_manifest.csv, run_registry.csv, file_metadata.csv")
    ap.add_argument("--cross-segment-dir", required=True, type=Path,
                    help="compare_cross_segment.py --out-dir (cross_segment_summary.csv, matrices)")
    ap.add_argument("--segments-root", required=True, type=Path,
                    help="Per-segment output root (same as compare_cross_segment.py --segments-root)")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="Output directory (default: <cross-segment-dir>/segment_decomposition)")
    ap.add_argument("--view-scope", choices=sorted(VIEW_COLUMN), default="used")
    ap.add_argument("--split-dimension", choices=SPLITTABLE_FIELDS, default="discipline_label")
    ap.add_argument("--governance-role", default="Project",
                    help="governance_role of the segments analysed (default: Project, the population "
                         "compare_cross_segment.py's project matrices cover)")
    ap.add_argument("--parent-segment", default=None,
                    help="Restrict to this parent segment_id and its split children")
    ap.add_argument("--client", default=None, help="Restrict to segments with this client_label")
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    t_start = time.perf_counter()
    executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    view = args.view_scope
    out_dir: Path = args.out_dir or (args.cross_segment_dir / "segment_decomposition")
    summary_path = args.cross_segment_dir / "cross_segment_summary.csv"
    matrix_path = args.cross_segment_dir / "project_mean_file_pair_jaccard_matrix.csv"

    policy = load_policy(args.policy)
    categories: Dict[str, str] = policy["categories"]  # type: ignore[assignment]
    tolerance = float(policy["drift_tolerance"])  # type: ignore[arg-type]
    bands: Dict[str, list] = policy["presentation_bands"]  # type: ignore[assignment]

    try:
        manifest = ccs.load_manifest(args.records_dir)
        registry = ccs.load_registry(args.records_dir)
    except SystemExit as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    if not summary_path.exists():
        print(f"[error] cross_segment_summary.csv not found at {summary_path}", file=sys.stderr)
        return 2
    file_metadata = ccs.load_file_metadata(args.records_dir)

    segments, skips = select_segments(manifest, registry, args.governance_role, args.client)
    families = find_split_families(segments, args.split_dimension)
    if args.parent_segment:
        families = [fam for fam in families if fam[0] == args.parent_segment]
        if not families:
            print(f"[error] --parent-segment {args.parent_segment!r} is not a runnable "
                  f"{args.governance_role} parent with {args.split_dimension} children", file=sys.stderr)
            return 2
        keep = {args.parent_segment, *families[0][1]}
        segments = {s: r for s, r in segments.items() if s in keep}
    print(f"[decomp] view={view} segments={len(segments)} split_families={len(families)} "
          f"split_dimension={args.split_dimension}")

    # --- Recompute (analysis A internal + analysis C) ----------------------
    stats_by_seg: Dict[str, Dict[str, DomainStats]] = {}
    eids_by_seg: Dict[str, Set[str]] = {}
    no_membership: List[str] = []
    for i, sid in enumerate(sorted(segments), 1):
        t0 = time.perf_counter()
        stats_by_seg[sid], eids_by_seg[sid] = recompute_segment(
            sid, view, args.segments_root, registry, file_metadata)
        n_pairs = sum(s.seg_n for s in stats_by_seg[sid].values())
        if not stats_by_seg[sid]:
            no_membership.append(sid)
            skips["segment:no_membership_inventory"] += 1
        print(f"[decomp] {i}/{len(segments)} {sid}: domains={len(stats_by_seg[sid])} pairs={n_pairs} "
              f"({time.perf_counter() - t0:.1f}s)")

    # --- Summary (analysis A siblings, B within basis, reconciliation) ------
    sibling_pairs = {frozenset((a, b)) for _p, kids in families for i, a in enumerate(kids) for b in kids[i + 1:]}
    within_ref, sibling, conflicts, summary_skips = read_summary(
        summary_path, view, set(segments), sibling_pairs, tolerance)
    skips.update(summary_skips)

    # --- Reconciliation gate ------------------------------------------------
    mismatches: List[Dict[str, object]] = []
    recon_status: Dict[Tuple[str, str], str] = {}
    n_checked = n_matched = n_no_reference = 0
    keys = {(s, d) for s, ds in stats_by_seg.items() for d, st in ds.items() if st.within_n} | set(within_ref)
    for sid, domain in sorted(keys):
        st = stats_by_seg.get(sid, {}).get(domain)
        rec = st.within_mean if st is not None else None
        ref = within_ref.get((sid, domain))
        if ref is None:
            n_no_reference += 1
            recon_status[(sid, domain)] = "no_reference"
            continue
        n_checked += 1
        if rec is not None and abs(rec - ref) <= tolerance:
            n_matched += 1
            recon_status[(sid, domain)] = "match"
        else:
            recon_status[(sid, domain)] = "mismatch"
            mismatches.append({
                "segment_id": sid, "view_scope": view, "domain": domain,
                "compare_within_project_mean": ref, "recomputed_within_mean": rec,
                "recomputed_within_pairs": st.within_n if st is not None else 0,
                "difference": (rec - ref) if rec is not None else None,
            })

    # --- Assemble rows ------------------------------------------------------
    seg_wide = {s: {d: st.segment_wide_mean for d, st in ds.items() if st.seg_n}
                for s, ds in stats_by_seg.items()}
    seg_pairs = {s: sum(st.seg_n for st in ds.values()) for s, ds in stats_by_seg.items()}
    within_by_seg: Dict[str, Dict[str, float]] = defaultdict(dict)
    for (sid, domain), value in within_ref.items():
        within_by_seg[sid][domain] = value

    split_rows = build_split_lift_rows(
        families, segments, args.split_dimension, view, seg_wide, seg_pairs,
        within_by_seg, sibling, bands["split_lift"], skips)

    diagonal, ambiguous_labels = read_matrix_diagonal(matrix_path, view, manifest)
    findings: List[Dict[str, object]] = []
    seg_pairs_by_domain = {(s, d): st.seg_n for s, ds in stats_by_seg.items() for d, st in ds.items()}
    loo_rows = build_loo_rows("segment_wide", view, segments, seg_wide, seg_pairs_by_domain,
                              categories, bands["domain_loo_impact"], tolerance, None, findings, skips)
    loo_rows += build_loo_rows("within_project_only", view, segments, within_by_seg, {},
                               categories, bands["domain_loo_impact"], tolerance,
                               diagonal if matrix_path.exists() else None, findings, skips)

    effect_domain_rows: List[Dict[str, str]] = []
    effect_category_rows: List[Dict[str, str]] = []
    effect_segment_rows: List[Dict[str, str]] = []
    seen_domains: Set[str] = {d for ds in stats_by_seg.values() for d in ds} | {d for _s, d in within_ref}
    for sid in sorted(segments):
        label = segments[sid].get("segment_label", "")
        ds = stats_by_seg.get(sid, {})
        seg_eids = eids_by_seg.get(sid, set())
        seg_labels: Counter = Counter()
        for eid in seg_eids:
            plabel = file_metadata.get(eid, {}).get("project_label", "").strip()
            if not is_blank_or_na(plabel):
                seg_labels[plabel] += 1
        for domain in sorted(ds):
            st = ds[domain]
            effect = _diff(st.within_mean, st.between_mean)
            ok = st.within_n > 0 and st.between_n > 0
            reason = "" if ok else (
                "fewer_than_two_files" if st.n_files < 2 else
                "no_within_project_pairs" if not st.within_n else "no_between_project_pairs")
            if reason:
                skips[f"project_effect:{reason}"] += 1
            ref = within_ref.get((sid, domain))
            effect_domain_rows.append({
                "view_scope": view, "segment_id": sid, "segment_label": label, "domain": domain,
                "functional_category": categories.get(domain, UNMAPPED),
                "n_files": str(st.n_files), "n_files_all_view": str(st.n_files_all),
                "n_projects": str(st.n_projects), "n_unlabeled_files": str(st.n_unlabeled_files),
                "n_segment_pairs": str(st.seg_n), "segment_wide_mean": _f(st.segment_wide_mean),
                "n_within_pairs": str(st.within_n), "within_mean": _f(st.within_mean),
                "n_between_pairs": str(st.between_n), "between_mean": _f(st.between_mean),
                "project_effect": _f(effect),
                "n_pairs_excluded_unlabeled": str(st.excluded_unlabeled),
                "n_pairs_missing_view": str(st.missing_view),
                "compare_within_project_mean": _f(ref),
                "reconciliation_status": recon_status.get((sid, domain), "not_applicable"),
                "presentation_band_descriptive": band_for(effect, bands["project_effect"]),
                "row_status": "ok" if ok else "unavailable", "skip_reason": reason,
            })
        by_cat: Dict[str, List[DomainStats]] = defaultdict(list)
        for domain, st in ds.items():
            by_cat[categories.get(domain, UNMAPPED)].append(st)
        for cat in sorted(by_cat):
            effect_category_rows.append({"view_scope": view, "segment_id": sid, "segment_label": label,
                                         "functional_category": cat,
                                         **_rollup(by_cat[cat], bands["project_effect"])})
        effect_segment_rows.append({
            "view_scope": view, "segment_id": sid, "segment_label": label,
            "n_files": segments[sid].get("file_count", ""),
            "n_projects": str(len(seg_labels)),
            "n_unlabeled_files": str(len(seg_eids) - sum(seg_labels.values())),
            "n_unmapped_domains": str(sum(1 for d in ds if d not in categories)),
            **_rollup(ds.values(), bands["project_effect"]),
        })

    unmapped = sorted(d for d in seen_domains if d and d not in categories)
    outputs = {
        "split_lift": (SPLIT_LIFT_FIELDS, split_rows),
        "loo": (LOO_FIELDS, loo_rows),
        "effect_domain": (EFFECT_DOMAIN_FIELDS, effect_domain_rows),
        "effect_category": (EFFECT_CATEGORY_FIELDS, effect_category_rows),
        "effect_segment": (EFFECT_SEGMENT_FIELDS, effect_segment_rows),
    }
    status = "failed_reconciliation" if mismatches else "ok"
    out_dir.mkdir(parents=True, exist_ok=True)
    for key, (fields, rows) in outputs.items():
        path = out_dir / OUTPUT_FILES[key]
        if status == "ok":
            ccs.atomic_write_csv(path, fields, rows)
        elif path.exists():
            path.unlink()  # never leave a prior run's numbers beside a failed gate

    peak = peak_memory_mb()
    run_summary = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "executed_utc": executed_utc,
        "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "inputs": {
            "segment_manifest": _file_fingerprint(args.records_dir / "segment_manifest.csv"),
            "run_registry": _file_fingerprint(args.records_dir / "run_registry.csv"),
            "file_metadata": _file_fingerprint(args.records_dir / "file_metadata.csv"),
            "cross_segment_summary": _file_fingerprint(summary_path),
            "project_mean_file_pair_jaccard_matrix": _file_fingerprint(matrix_path),
            "policy": {"path": str(args.policy), "schema": policy["schema"], "sha256": policy["_sha256"]},
        },
        "segments_processed": len(segments),
        "split_families": len(families),
        "file_pairs_scored": sum(seg_pairs.values()),
        "row_counts": {OUTPUT_FILES[k]: len(rows) for k, (_f2, rows) in outputs.items()},
        "outputs_written": status == "ok",
        "skip_counts": dict(sorted(skips.items())),
        "unmapped_domains": unmapped,
        "reconciliation": {
            "tolerance": tolerance, "checked": n_checked, "matched": n_matched,
            "no_reference": n_no_reference, "mismatches": mismatches,
        },
        "findings": findings,
        "summary_conflicts": conflicts,
        "coverage_gaps": {
            "segments_without_membership_inventory": no_membership,
            "ambiguous_matrix_row_ids": ambiguous_labels,
            "matrix_present": matrix_path.exists(),
        },
        "runtime_seconds": round(time.perf_counter() - t_start, 3),
        "peak_memory_mb": round(peak, 1) if peak is not None else None,
        "known_limitation": (
            "Within-project similarity is partly mechanical (shared/copied content within a project) "
            "and does not by itself indicate governance quality."),
    }
    with (out_dir / RUN_SUMMARY_FILE).open("w", encoding="utf-8") as f:
        json.dump(run_summary, f, indent=2, sort_keys=False)
        f.write("\n")

    print(f"[decomp] status={status} pairs_scored={run_summary['file_pairs_scored']} "
          f"reconciliation checked={n_checked} matched={n_matched} mismatches={len(mismatches)} "
          f"no_reference={n_no_reference}")
    print(f"[decomp] drift findings={len(findings)} summary conflicts={len(conflicts)} "
          f"unmapped domains={unmapped or 'none'}")
    for k, v in sorted(skips.items()):
        print(f"[decomp]   skip {k}: {v}")
    print(f"[decomp] runtime={run_summary['runtime_seconds']}s peak_memory="
          f"{run_summary['peak_memory_mb'] if peak is not None else 'unavailable'}MB -> {out_dir}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
