"""Cross-segment comparison tool.

Compares pattern vocabularies across segments using join_hash as the
cross-segment identity unit.

Single measurement path
-----------------------
Comparisons prefer per-file join_hash inventories from membership_matrix.csv
and resolve join_hash via domain_patterns.csv (source_cluster_id.split('|')[-1]).
Generic/reference segments that only provide analysis outputs can fall back to
domain_patterns.csv for all-view provision inventories. There is no bundle-mode /
file-mode branch. All set operations (Jaccard, containment) operate on the full
join_hash inventories loaded for the selected view.

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

Sufficiency and ambiguity judgment
-----------------------------------
Scores are always computed and emitted, along with the raw counts
(n_files_a/b, n_files_focal/pool, n_shared_join_hash, n_unique_patterns_*)
needed to judge them. This file does not classify a comparison as
interpretable, sufficient, or ambiguous — no score_ambiguity_band label.
signal_spread is reported as a raw float (computed from the same
shared/unique counts) for downstream banding; it is not itself a judgment.
That interpretive layer belongs to generate_governance_narrative.py.

comparison_status is the one exception: it is explicit, non-suppressive
cardinality metadata (ok/degraded/blocked, computed purely from file
counts), not a judgment about the scores themselves. blocked means zero
readable file inventory on a required side; degraded means one side has
exactly one file while the other has more; everything else, including a
symmetric 1x1 comparison, is ok. No comparison is ever suppressed on this
basis — this is the replacement for the removed n_files >= 5
data_sufficient gate, which silently hid narrow-but-valid rows instead of
labeling them. cardinality_shape and file_count_ratio are purely
descriptive siblings of comparison_status and never gate output either.

Cartesian pairwise means (all_pairwise_jaccard_mean, used_pairwise_jaccard_mean,
all_pairwise_containment_a_in_b_mean, etc.; aggregation_method =
"cartesian_file_pair_mean") answer "what's the mean of all A-file x B-file
pairs" -- a different question from the population-union metrics
(all_union_jaccard, all_union_containment_a_in_b/b_in_a, and their used_
counterparts), which answer "how similar are these two populations" from
each side's union footprint, independent of n_files_a x n_files_b. The two
families diverge exactly when file counts are imbalanced; neither
supersedes the other. all_a_file_mean_similarity_to_b_mean/min and its B
counterpart expose directional population experience for symmetric
comparisons -- in a 1xN comparison, the A-side summary is one file's
average similarity to N files, while the B-side summary is the
distribution of N files against that one A file.

Directed comparisons keep the reference-union -> per-target-file-
distribution approach (reference_aggregation = "union", target_aggregation
= "per_file_distribution"); reference_union_pattern_count,
reference_intersection_pattern_count, and reference_core_share (=
intersection/union across every reference file) are heterogeneity
diagnostics that reveal whether a multi-file reference is a coherent
standard or a broad union of conflicting sources, independent of how well
any target matches it. reference_core_share degrades to 1.0 for a
single-file reference -- not an artificial failure.

Reference segment participation
--------------------------------
Reference segments participate in generic_to_template, generic_to_container,
generic_to_project, template_to_project, template_to_container, and
container_to_project comparisons using their file inventories from
membership_matrix.csv when present. Generic/reference provided-vocabulary sources
may not emit bundle_analysis membership matrices; for all-view comparisons they
fall back to domain_patterns.csv. They will have has_bundles = "false" for most
domains, often alongside small n_files counts — this is expected and correct.

Governance all/used semantics
------------------------------
The provision chain is Generic / Generic-Host → Template → Container → Project
all-view vocabulary. The usage chain is Project all → Project used. Generic,
Template, and most Container segments are standards-carrier / provided-vocabulary
references; used-view and purge signals are meaningful primarily when the target
role is Project and must not be used to label Template or Generic stock content
as unused bloat. Directed governance-state output therefore compares upstream
reference all-view vocabulary to downstream target all-view and, for Project
targets, target used-view vocabulary.

Organizational scope levels
----------------------------
Scope is derived from explicit, literal client_label/business_center_label
values (see _scope_level() / _is_client_wide_rollup()), not blank inference:
enterprise (client_label=="Stantec", business_center_label=="0000" --
"BC_0000"/any-case spelling variants canonicalize to "0000" via
_normalize_bc_label(), they are not folded to blank),
business_center (client_label=="Stantec", a real business_center_label), and
client_business_center (a real external client_label, a real
business_center_label). A row where either dimension isn't cut at all
(blank) is a roll-up pooling multiple real scopes, handled by individual
comparison-type discovery rather than by _scope_level() itself. The
governance chain fans out across these levels: enterprise_to_project,
bc_to_project, enterprise_to_bc, enterprise_to_client (discover_governance_
chain()), bc_to_bc (peer business centers, also in discover_governance_
chain()), cross_client (discover_cross_client(), now grouped by discipline_
label too), and client_cross_bc (discover_client_cross_bc(), a real client's
populations compared across every business center it appears in).

Usage:
    python tools/compare_cross_segment.py \\
        --segments-root segments/ \\
        --records-dir   results/records/ \\
        --out-dir       results/cross_segment/ \\
        [--within-segment] [--sibling-segments] [--parent-siblings] \\
        [--within-project] [--governance-chain] [--cross-client] \\
        [--domain DOMAIN] [--segment-a ID] [--segment-b ID] \\
        [--min-patterns INT] [--dry-run] [--no-delta]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from na_token import is_blank_or_na, ENTERPRISE_BC_BOOKKEEPING_TOKENS as _ENTERPRISE_BC_BOOKKEEPING_TOKENS
from jenks_utils import jenks_breaks


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
    "business_center_label_a", "business_center_label_b",
    "scope_level_a", "scope_level_b",
    "discipline_label_a", "discipline_label_b",
    "unit_system",
    "comparison_type",
    "domain",
    "n_patterns_a", "n_patterns_b", "n_shared_join_hash",
    "n_unique_patterns_a", "n_unique_patterns_b",
    "signal_spread",
    "all_pairwise_containment_a_in_b_mean", "all_containment_a_in_b_min",
    "all_pairwise_containment_b_in_a_mean", "all_containment_b_in_a_min",
    "all_pairwise_jaccard_mean", "all_jaccard_p10", "all_jaccard_p90",
    "used_pairwise_jaccard_mean", "used_jaccard_p10", "used_jaccard_p90",
    "used_pairwise_containment_a_in_b_mean", "used_containment_a_in_b_min",
    "used_pairwise_containment_b_in_a_mean", "used_containment_b_in_a_min",
    "used_n_shared_join_hash",
    "aggregation_method",
    "all_union_jaccard", "all_union_containment_a_in_b", "all_union_containment_b_in_a",
    "used_union_jaccard", "used_union_containment_a_in_b", "used_union_containment_b_in_a",
    "all_a_file_mean_similarity_to_b_mean", "all_a_file_mean_similarity_to_b_min",
    "all_b_file_mean_similarity_to_a_mean", "all_b_file_mean_similarity_to_a_min",
    "reference_aggregation", "target_aggregation", "n_reference_files",
    "reference_union_pattern_count", "reference_intersection_pattern_count", "reference_core_share",
    "all_has_bundles_a", "all_has_bundles_b",
    "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
    "used_has_bundles_a", "used_has_bundles_b",
    "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
    "n_files_a", "n_files_b", "n_pairs",
    "comparison_status", "cardinality_shape", "file_count_ratio",
    "inventory_status_a", "inventory_status_b",
    "reference_usage_interpretable",
    "target_usage_interpretable",
    "recommended_primary_view",
    "comparison_role_semantics",
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

COMPARISON_REGISTRY_FIELDS: List[str] = [
    "segment_id_a", "segment_id_b", "comparison_type", "domain",
    "population_hash_a", "population_hash_b",
    "last_run_utc_a", "last_run_utc_b",
    "conformance_reference_mode",
    "computed_utc",
]

POOLED_FIELDS: List[str] = [
    "comparison_run_id",
    "segment_id", "segment_label",
    "governance_role", "client_label",
    "business_center_label",
    "scope_level",
    "unit_system",
    "domain",
    "pool_scope",
    "n_files_focal", "n_files_pool",
    "comparison_status", "cardinality_shape", "file_count_ratio",
    "n_unique_patterns_focal", "n_unique_patterns_pool", "n_shared_join_hash",
    "signal_spread",
    "all_containment_focal_in_pool", "all_containment_pool_in_focal",
    "used_containment_focal_in_pool", "used_containment_pool_in_focal",
    "all_has_bundles_focal", "all_has_bundles_pool",
    "all_n_shared_bundle_both", "all_n_shared_bundle_focal_only", "all_n_shared_bundle_pool_only",
    "used_has_bundles_focal", "used_has_bundles_pool",
    "used_n_shared_bundle_both", "used_n_shared_bundle_focal_only", "used_n_shared_bundle_pool_only",
    "executed_utc",
]

GOVERNANCE_STATE_FIELDS: List[str] = [
    "comparison_run_id",
    "comparison_type",
    "segment_id_reference", "segment_id_target",
    "segment_label_reference", "segment_label_target",
    "governance_role_reference", "governance_role_target",
    "business_center_label_reference", "business_center_label_target",
    "unit_system",
    "domain",
    "join_hash",
    "pattern_label",
    "in_reference_all",
    "in_target_all",
    "in_target_used",
    "state",
    "n_files_in_target_all",
    "pct_files_in_target_all",
    "n_files_in_target_used",
    "pct_files_in_target_used",
    "in_any_generic",
    "in_any_template",
    "in_any_container",
    "is_bundle_member_target_all",
    "is_bundle_member_target_used",
    "reference_usage_interpretable",
    "target_usage_interpretable",
    "recommended_primary_view",
    "executed_utc",
]


UNION_INVENTORY_FIELDS: List[str] = [
    "governance_role",
    "client_label",
    "discipline_label",
    "unit_system",
    "domain",
    "view_scope",
    "join_hash",
    "pattern_label",
    "n_segments_present",
    "n_files_present",
    "n_files_denominator",
    "pct_files_present",
    "n_projects_present",
    "n_projects_denominator",
    "n_clients_present",
    "n_clients_denominator",
    "pct_clients_present",
    "pct_projects_present",
    "usage_interpretable",
    "inventory_status",
    "source_status",
    "executed_utc",
]


REUSE_DISTRIBUTION_FIELDS: List[str] = [
    "view_scope",
    "governance_role",
    "client_label",
    "discipline_label",
    "unit_system",
    "domain",
    "join_hash",
    "pattern_label",
    "n_files_present",
    "n_files_denominator",
    "pct_files_present",
    "n_projects_present",
    "n_projects_denominator",
    "pct_projects_present",
    "n_clients_present",
    "n_clients_denominator",
    "pct_clients_present",
    "reuse_bucket",
    "bucket_basis",
    "usage_interpretable",
    "inventory_status",
    "classification_status",
    "executed_utc",
]



MATRIX_OUTPUT_FIELDS: List[str] = [
    "matrix_name", "row_id", "column_id", "view_scope", "domain", "metric",
    "value", "value_status", "self_comparison", "interpretation", "executed_utc",
]

FRAGMENTATION_DIAGNOSTIC_FIELDS: List[str] = [
    "matrix_name", "row_id", "column_id", "view_scope", "domain",
    "footprint_similarity", "exact_identity_overlap", "fragmentation_diagnostic",
    "value_status", "interpretation", "executed_utc",
]

MATRIX_MANIFEST_FIELDS: List[str] = [
    "matrix_name", "governance_role", "view_scope", "source_file",
    "source_grain", "metric", "identity_unit", "aggregation_method",
    "interpretation", "known_limitations", "executed_utc",
]

REUSE_SUMMARY_FIELDS: List[str] = [
    "view_scope",
    "governance_role",
    "client_label",
    "discipline_label",
    "unit_system",
    "domain",
    "reuse_bucket",
    "bucket_basis",
    "n_patterns",
    "usage_interpretable",
    "classification_status",
    "executed_utc",
]

# Centralized neutral reporting thresholds for reuse breadth. These are
# classifications for governance reporting, not correctness judgments.
REUSE_BUCKET_THRESHOLDS = {
    "corpus_wide_min_pct_clients": 0.80,
    "client_wide_min_pct_files": 0.80,
    "multi_project_min_projects": 3,
    "emerging_min_files": 2,
}

GOVERNANCE_STATE_SUMMARY_FIELDS: List[str] = [
    "comparison_run_id",
    "comparison_type",
    "segment_id_reference", "segment_id_target",
    "segment_label_reference", "segment_label_target",
    "governance_role_reference", "governance_role_target",
    "business_center_label_reference", "business_center_label_target",
    "unit_system",
    "domain",
    "reference_all_count",
    "target_all_count",
    "target_used_count",
    "provided_to_configured_containment",
    "provided_to_used_containment",
    "provided_passive_share",
    "provided_missing_share",
    "local_active_share",
    "provided_and_used_count",
    "provided_but_passive_count",
    "provided_but_missing_count",
    "local_active_count",
    "local_passive_count",
    "local_unbundled_count",
    "provided_configured_count",
    "local_configured_count",
    "provided_and_used_pct_of_reference_all",
    "provided_but_passive_pct_of_reference_all",
    "provided_but_missing_pct_of_reference_all",
    "local_active_pct_of_target_used",
    "local_passive_pct_of_target_all",
    "local_unbundled_pct_of_target_all",
    "reference_usage_interpretable",
    "target_usage_interpretable",
    "recommended_primary_view",
    "comparison_role_semantics",
    "executed_utc",
]

# Comparison types for which delta rows are emitted (directed, reference side defined).
DELTA_DIRECTED_TYPES = {
    "template_to_project",
    "template_to_container",
    "container_to_project",
    "enterprise_to_project",
    "bc_to_project",
}

GOVERNANCE_STATE_DIRECTED_TYPES = {
    "generic_to_template",
    "generic_to_container",
    "generic_to_project",
    "template_to_project",
    "template_to_container",
    "container_to_project",
    "enterprise_to_project",
    "bc_to_project",
    "enterprise_to_bc",
    "enterprise_to_client",
}

GENERIC_ROLE_KEYS = {"generic", "generic-host", "generic_host"}


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
# Governance-state semantics
# ---------------------------------------------------------------------------

def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def _role_key(role: str) -> str:
    return role.strip().lower().replace("_", "-")


def _is_generic_role(role: str) -> bool:
    return _role_key(role) in GENERIC_ROLE_KEYS


def _role_matches(row_role: str, wanted_role: str) -> bool:
    if wanted_role == "generic":
        return _is_generic_role(row_role)
    return _role_key(row_role) == wanted_role


def _usage_interpretable_for_role(role: str) -> bool:
    # Used/non-purgeable is a delivery signal for project targets. Standards-carrier
    # roles can still have used-view files, but those values are annotations only.
    return _role_key(role) == "project"


def _recommended_primary_view(role_a: str, role_b: str, comparison_type: str) -> str:
    if comparison_type in ("sibling_projects", "cross_client") or _role_key(role_b) == "project":
        return "used"
    return "all"


def _comparison_role_semantics(role_a: str, role_b: str, comparison_type: str) -> str:
    if comparison_type in GOVERNANCE_STATE_DIRECTED_TYPES:
        if _usage_interpretable_for_role(role_b):
            return "directed_governance: reference all-view provides vocabulary; project target used-view is active delivery"
        return "directed_governance: reference and target are provided-vocabulary inventories; all-view is primary"
    if comparison_type == "sibling_projects":
        return "sibling_projects: used-view is active practice; all-view is configured/inherited context"
    if comparison_type == "cross_client":
        return "cross_client: used-view is active practice; all-view is configured/inherited context (same semantics as sibling_projects, across clients rather than within one)"
    if comparison_type == "sibling_templates":
        return "sibling_templates: all-view is primary; used-view must not be interpreted as bloat"
    if comparison_type == "sibling_containers":
        return "sibling_containers: all-view is primary unless an external subtype establishes delivery use semantics"
    if _is_generic_role(role_a) and _is_generic_role(role_b):
        return "sibling_generic: all-view is primary; used-view is not meaningful"
    return "all-view is configured vocabulary; used-view is meaningful primarily for Project targets"


def _classify_governance_state(
    in_reference_all: bool,
    in_target_all: bool,
    in_target_used: bool,
    is_bundle_member_target_all: bool,
    target_usage_interpretable: bool,
) -> str:
    if target_usage_interpretable:
        if in_reference_all and in_target_used:
            return "provided_and_used"
        if in_reference_all and in_target_all and not in_target_used:
            return "provided_but_passive"
        if in_reference_all and not in_target_all:
            return "provided_but_missing"
        if not in_reference_all and in_target_used:
            return "local_active"
        if not in_reference_all and in_target_all and is_bundle_member_target_all:
            return "local_passive"
        return "local_unbundled"

    # For Template, Generic, and most Container targets, avoid usage-judgment labels:
    # configured stock is inventory, not passive bloat. Keep target_used as annotation.
    if in_reference_all and in_target_all:
        return "provided_configured"
    if in_reference_all and not in_target_all:
        return "provided_but_missing"
    if in_target_all and is_bundle_member_target_all:
        return "local_configured"
    return "local_unbundled"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_manifest(records_dir: Path) -> Dict[str, Dict[str, str]]:
    path = records_dir / "segment_manifest.csv"
    if not path.exists():
        sys.exit(f"[error] segment_manifest.csv not found at {path}")
    manifest: Dict[str, Dict[str, str]] = {}
    for row in read_csv_rows(path):
        sid = row["segment_id"]
        prior = manifest.get(sid)
        if prior is not None and prior != row:
            sys.exit(
                f"[error] Blocked: segment_manifest.csv has conflicting rows for "
                f"segment_id={sid!r} — this file must not be trusted as an "
                "authoritative hierarchy until the duplicate is resolved"
            )
        manifest[sid] = row
    return manifest


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


def load_membership(records_dir: Path) -> Dict[str, Set[str]]:
    """Load segment_membership.csv into segment_id -> real export_run_id set.

    This is the ground-truth population source for `population_containment`
    (see _population_containment_map()) -- unlike segment_manifest.csv's
    file_count/population_hash columns, membership rows carry the actual
    member export_run_ids, which is what a subset-or-equal check needs.
    Optional: absent on older/partial output directories, in which case
    population_containment is simply unavailable (callers treat an empty
    map as "no containment data", not an error) — the structural_ancestor
    guard still functions independently.
    """
    path = records_dir / "segment_membership.csv"
    if not path.exists():
        print(f"[warn] segment_membership.csv not found at {path}", file=sys.stderr)
        return {}
    membership: Dict[str, Set[str]] = defaultdict(set)
    for row in read_csv_rows(path):
        sid = row.get("segment_id", "").strip()
        eid = row.get("export_run_id", "").strip()
        if sid and eid:
            membership[sid].add(eid)
    return dict(membership)


def validate_membership_against_manifest(
    manifest: Dict[str, Dict[str, str]],
    membership: Dict[str, Set[str]],
) -> List[str]:
    """Return one error string per segment_id where segment_membership.csv
    disagrees with segment_manifest.csv's file_count/population_hash, OR
    where a manifest segment expected to have members has none at all.

    Same check tools/run_segment_orchestrator.py's own
    validate_membership_against_manifest() performs (adapted here for
    segment_id -> Set[str] membership instead of List[str]). Guards against a
    stale or mismatched segment_membership.csv — e.g. build_segment_
    manifest.py interrupted after replacing segment_manifest.csv but before
    replacing segment_membership.csv — silently driving population_
    containment()'s subset/materiality checks off a population that no
    longer matches what segment_manifest.csv itself describes for that
    segment_id, which could either wrongly suppress a valid comparison pair
    or wrongly retain one that should have been excluded.

    Two passes: the first (over `membership`) catches count/hash mismatches
    for segments the sidecar DOES have rows for; the second (over
    `manifest`) catches a segment build_segment_manifest.py's own
    _build_membership_rows() guarantees a membership row for (file_count > 0)
    but that's entirely ABSENT from `membership` — a truncated/partially
    written sidecar, not just a stale one. Missing this second pass would
    silently exclude that segment from every population_containment check
    instead of flagging the sidecar as untrustworthy (Codex review finding
    on PR #423): _population_containment_map()/_compute_containment_
    thresholds() only iterate segment_ids present in `membership`, so an
    entirely-missing entry doesn't raise a count/hash mismatch on its own --
    it just silently drops out of consideration.
    """
    errors: List[str] = []
    for sid, eids in membership.items():
        mrow = manifest.get(sid)
        if mrow is None:
            continue
        expected_count = (mrow.get("file_count") or "").strip()
        if expected_count and str(len(eids)) != expected_count:
            errors.append(
                f"segment={sid}: segment_membership.csv has {len(eids)} export_run_id(s) "
                f"but segment_manifest.csv file_count={expected_count}"
            )
            continue
        expected_hash = (mrow.get("population_hash") or "").strip()
        if expected_hash:
            actual_hash = hashlib.sha1("|".join(sorted(eids)).encode()).hexdigest()
            if actual_hash != expected_hash:
                errors.append(
                    f"segment={sid}: segment_membership.csv population_hash={actual_hash} "
                    f"does not match segment_manifest.csv population_hash={expected_hash}"
                )

    for sid, mrow in manifest.items():
        if sid in membership:
            continue
        expected_count = (mrow.get("file_count") or "").strip()
        if expected_count and expected_count != "0":
            errors.append(
                f"segment={sid}: segment_manifest.csv file_count={expected_count} but "
                f"segment_membership.csv has no export_run_id rows for this segment at all"
            )
    return errors


# ---------------------------------------------------------------------------
# Comparison staleness registry
#
# compare_cross_segment.py has no cached results of its own — every invocation
# recomputes whatever (pair × domain) work items it is given, from whatever is
# currently on disk under segments/. comparison_registry.csv exists purely to
# let a run-plan preview (--dry-run below) tell a caller *which* pair×domain
# comparisons are worth recomputing, by recording each side's
# population_hash/last_run_utc (from run_registry.csv) at the moment that
# specific (pair, domain) was last actually computed. It is not consulted to
# skip computation — a live run always recomputes every work item it is given.
#
# Keyed on (segment_id_a, segment_id_b, comparison_type, domain) — matching
# the actual work granularity (work_items from build_pair_domain_work_items),
# not just the pair. A --domain-scoped invocation only recomputes one domain
# per pair; stamping at pair granularity would mark every other domain for
# that pair "current" without having recomputed it, hiding real staleness in
# a later --dry-run.
#
# The file is a full snapshot of this invocation only — never merged with a
# prior comparison_registry.csv — matching every other output this tool
# writes (cross_segment_summary.csv etc. are always a full atomic_write_csv
# replace, never a merge). A --domain/--segment-scoped run sharing the same
# --out-dir as an earlier full run already destroys those other domains'
# output rows; carrying their old registry stamp forward would falsely claim
# they are still current. Only (pair, domain) work items that actually
# produced a persisted output row this run are written.
# ---------------------------------------------------------------------------

ComparisonRegistryKey = Tuple[str, str, str, str]  # (seg_a, seg_b, comparison_type, domain)


def load_comparison_registry(out_dir: Path) -> Dict[ComparisonRegistryKey, Dict[str, str]]:
    path = out_dir / "comparison_registry.csv"
    if not path.exists():
        return {}
    result: Dict[ComparisonRegistryKey, Dict[str, str]] = {}
    for row in read_csv_rows(path):
        key = (
            row.get("segment_id_a", ""), row.get("segment_id_b", ""),
            row.get("comparison_type", ""), row.get("domain", ""),
        )
        result[key] = row
    return result


def _segment_status_complete(registry: Dict[str, Dict[str, str]], segment_id: str) -> bool:
    return registry.get(segment_id, {}).get("status", "").strip().lower() == "complete"


def build_comparison_registry_rows(
    completed_work_items: Sequence[Tuple[str, str, str, str]],
    registry: Dict[str, Dict[str, str]],
    computed_utc: str,
) -> List[Dict[str, str]]:
    """Return comparison_registry.csv rows: a fresh stamp for every (pair,
    domain) that actually produced output this run (`completed_work_items`)
    where both sides' run_registry.csv status is "complete".

    Deliberately no carryover of prior comparison_registry.csv rows: every
    other output this tool writes (cross_segment_summary.csv,
    cross_segment_file_pairs.csv, ...) is a full atomic_write_csv replace from
    only this invocation's rows, not a merge — a --domain/--segment-scoped run
    sharing the same --out-dir as an earlier full run already destroys those
    other domains'/pairs' output rows. Carrying their old comparison_registry
    stamp forward would claim they are still "current" when the data backing
    that claim no longer exists on disk. comparison_registry.csv must mirror
    the same full-snapshot-of-this-run semantics, so a scoped run correctly
    makes every non-recomputed (pair, domain) report as stale (no recorded
    stamp) on the next --dry-run — matching reality.

    Only work items that actually produced a persisted output row are
    included — `run_pair()`/`_run_pair_domain()` returning None (e.g. a domain
    below --min-patterns, or a within-project pair with no eligible file
    pairs) must not get a fresh "current" stamp for output that was never
    written.

    A (pair, domain) is also excluded if either side's registry status is not
    "complete". build_segment_manifest.py updates population_hash to reflect
    a segment's new file population immediately on manifest rebuild, resetting
    status to "pending" (and clearing last_run_utc) until the orchestrator
    actually re-runs that segment — but its output folder on disk still holds
    the OLD population's results until then. A compare run in that window
    reads the stale on-disk data yet would otherwise get stamped with the
    segment's already-updated (new) population_hash, so once the segment
    finally reaches "complete" with that same hash, a later --dry-run would
    wrongly report the pair as already current."""
    rows: List[Dict[str, str]] = []
    for a, b, ctype, dom in completed_work_items:
        if not (_segment_status_complete(registry, a) and _segment_status_complete(registry, b)):
            continue
        rec_a = registry.get(a, {})
        rec_b = registry.get(b, {})
        rows.append({
            "segment_id_a": a,
            "segment_id_b": b,
            "comparison_type": ctype,
            "domain": dom,
            "population_hash_a": rec_a.get("population_hash", ""),
            "population_hash_b": rec_b.get("population_hash", ""),
            "last_run_utc_a": rec_a.get("last_run_utc", ""),
            "last_run_utc_b": rec_b.get("last_run_utc", ""),
            "conformance_reference_mode": rec_a.get("conformance_reference_mode", "") or "latest",
            "computed_utc": computed_utc,
        })
    return rows


def comparison_is_stale(
    seg_a: str,
    seg_b: str,
    comparison_type: str,
    domain: str,
    registry: Dict[str, Dict[str, str]],
    comparison_registry: Dict[ComparisonRegistryKey, Dict[str, str]],
) -> bool:
    """True if this (pair, domain) has never been computed, or either side's
    population_hash/last_run_utc has moved since it was last computed —
    including a Template/Container reference re-running and producing new
    bundle output with the target's own population unchanged."""
    prior = comparison_registry.get((seg_a, seg_b, comparison_type, domain))
    if prior is None:
        return True
    rec_a = registry.get(seg_a, {})
    rec_b = registry.get(seg_b, {})
    if prior.get("population_hash_a", "") != rec_a.get("population_hash", ""):
        return True
    if prior.get("population_hash_b", "") != rec_b.get("population_hash", ""):
        return True
    if prior.get("last_run_utc_a", "") != rec_a.get("last_run_utc", ""):
        return True
    if prior.get("last_run_utc_b", "") != rec_b.get("last_run_utc", ""):
        return True
    return False


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


def pattern_presence_file_path(seg_out: Path) -> Path:
    return seg_out / "results" / "analysis" / "pattern_presence_file.csv"


def _load_export_run_ids_for_segment(seg_out: Path) -> List[str]:
    ids_path = seg_out / "export_run_ids.txt"
    if not ids_path.exists():
        return []
    return [
        line.strip()
        for line in ids_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


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
    # Always prefer bundle all-view discovery — it remains the domain authority
    # source for bundle-producing segments. Generic/reference segments, however,
    # are provided-vocabulary sources and may only have analysis CSVs. In that
    # case, discover their domains from analysis outputs so they can participate
    # in containment/provision comparisons.
    ba_root = seg_out / "results" / "bundle_analysis" / "all"
    domains: Set[str] = set()
    if ba_root.exists():
        domains = {
            p.name.strip()
            for p in ba_root.iterdir()
            if p.is_dir() and p.name.strip()
        }
    if domains:
        return domains

    dp_path = domain_patterns_path(seg_out)
    if dp_path.exists():
        domains = {
            row.get("domain", "").strip()
            for row in read_csv_rows(dp_path)
            if row.get("domain", "").strip()
        }
    if domains:
        return domains

    presence_path = pattern_presence_file_path(seg_out)
    if presence_path.exists():
        domains = {
            row.get("domain", "").strip()
            for row in read_csv_rows(presence_path)
            if row.get("domain", "").strip()
        }
    return domains


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
        if not _role_matches(mrow.get("governance_role", ""), role):
            continue
        if mrow.get("unit_system", "").strip() != unit_system:
            continue
        rt = registry.get(sid, {}).get("run_type", "").strip().lower()
        if rt in ("skip", "registration"):
            continue
        # Use all view — scores are view-invariant. load_segment_join_hash_union
        # preserves membership_matrix behavior for bundle segments and also allows
        # Generic/reference provided-vocabulary segments to contribute their
        # domain_patterns.csv fallback inventory when bundle outputs are absent.
        result |= load_segment_join_hash_union(
            segments_root, registry, sid, domain, "all"
        )

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
    """Return {export_run_id: set_of_join_hashes} for a segment/domain/view."""
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return {}

    mm_path = bundle_analysis_dir(seg_out, domain, purge_view) / "membership_matrix.csv"
    if mm_path.exists():
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

    # Generic/reference segments are provided-vocabulary sources. They may not
    # produce bundle_analysis or membership matrices, but their analysis
    # inventory is valid for all-view containment/provision comparisons. File
    # membership comes from pattern_presence_file.csv when available. Used-view
    # is intentionally not inferred because analysis rows do not distinguish
    # active project use from configured/provided vocabulary.
    if purge_view != "all":
        return {}

    dp_path = domain_patterns_path(seg_out)
    if not dp_path.exists():
        return {}

    pattern_join_hashes: Dict[str, str] = {}
    pattern_export_run_ids: Dict[str, str] = {}
    for row in read_csv_rows(dp_path):
        if row.get("domain", "").strip() != domain:
            continue
        pid = row.get("pattern_id", "").strip()
        scid = row.get("source_cluster_id", "").strip()
        if not pid or not scid:
            continue
        join_hash = scid.split("|")[-1]
        if join_hash:
            pattern_join_hashes[pid] = join_hash
            eid = row.get("export_run_id", "").strip()
            if eid:
                pattern_export_run_ids[pid] = eid

    if not pattern_join_hashes:
        return {}

    result: Dict[str, Set[str]] = defaultdict(set)

    # Standard v2.1 analysis writes file membership to pattern_presence_file.csv,
    # not domain_patterns.csv. Use it when present so multi-file Generic/reference
    # inventories preserve per-export containment inputs instead of collapsing or
    # dropping rows that have no export_run_id in domain_patterns.csv.
    presence_path = pattern_presence_file_path(seg_out)
    if presence_path.exists():
        for row in read_csv_rows(presence_path):
            if row.get("domain", "").strip() != domain:
                continue
            eid = row.get("export_run_id", "").strip()
            pid = row.get("pattern_id", "").strip()
            if not eid or not pid:
                continue
            join_hash = pattern_join_hashes.get(pid)
            if join_hash:
                result[eid].add(join_hash)
        if result:
            return dict(result)

    for pid, join_hash in pattern_join_hashes.items():
        eid = pattern_export_run_ids.get(pid, "")
        if eid:
            result[eid].add(join_hash)
    if result:
        return dict(result)

    export_run_ids = _load_export_run_ids_for_segment(seg_out)
    single_export_run_id = export_run_ids[0] if len(export_run_ids) == 1 else ""
    if single_export_run_id:
        result[single_export_run_id] = set(pattern_join_hashes.values())
    return dict(result)



def _segment_domain_source_status(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
) -> Tuple[str, int]:
    """Return (source_status, missing_source_cluster_count) for a segment/domain."""
    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return "missing_domain_patterns", 0
    dp_path = domain_patterns_path(seg_out)
    if not dp_path.exists():
        return "missing_domain_patterns", 0
    domain_rows = [
        row for row in read_csv_rows(dp_path)
        if row.get("domain", "").strip() == domain
    ]
    if not domain_rows:
        return "no_patterns", 0
    missing = sum(
        1 for row in domain_rows
        if row.get("pattern_id", "").strip()
        and not row.get("source_cluster_id", "").strip()
    )
    valid = any(
        row.get("pattern_id", "").strip()
        and row.get("source_cluster_id", "").strip()
        for row in domain_rows
    )
    if not valid:
        return "no_patterns", missing
    return "ok", missing


def _load_segment_file_join_hashes_with_status(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
    view_scope: str,
) -> Tuple[Dict[str, Set[str]], str, int]:
    """Load file join_hashes plus explicit status for union inventory output."""
    source_status, missing_scid = _segment_domain_source_status(
        segments_root, registry, segment_id, domain
    )
    if source_status == "missing_domain_patterns":
        return {}, "missing_domain_patterns", missing_scid
    if source_status == "no_patterns":
        return {}, "no_patterns", missing_scid

    seg_out = segment_output_dir(segments_root, registry, segment_id)
    if seg_out is None:
        return {}, "missing_domain_patterns", missing_scid
    mm_path = bundle_analysis_dir(seg_out, domain, view_scope) / "membership_matrix.csv"
    if view_scope == "used" and not mm_path.exists():
        return {}, "used_view_unavailable", missing_scid

    files = load_file_join_hashes(segments_root, registry, segment_id, domain, view_scope)
    if files:
        return files, "ok", missing_scid
    if view_scope == "all":
        return {}, "no_patterns", missing_scid
    return {}, "used_view_unavailable", missing_scid


def _project_label_for_file(file_metadata: Dict[str, Dict[str, str]], export_run_id: str) -> str:
    label = file_metadata.get(export_run_id, {}).get("project_label", "").strip()
    return export_run_id if is_blank_or_na(label) else label


def build_union_inventory_rows(
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    file_metadata: Dict[str, Dict[str, str]],
    segments_root: Path,
    executed_utc: str,
    domain_filter: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Build normalized union inventory rows at governance/client/discipline/unit/domain/view/join_hash grain."""
    groups: Dict[Tuple[str, str, str, str, str], List[str]] = defaultdict(list)
    for segment_id, mrow in manifest.items():
        if not segment_is_runnable(registry, segment_id):
            continue
        domains = {domain_filter} if domain_filter else discover_domains_for_segment(
            segments_root, registry, segment_id
        )
        for domain in sorted(d for d in domains if d):
            groups[(
                mrow.get("governance_role", "").strip(),
                mrow.get("client_label", "").strip(),
                mrow.get("discipline_label", "").strip(),
                mrow.get("unit_system", "").strip(),
                domain,
            )].append(segment_id)

    rows: List[Dict[str, str]] = []
    for (role, client, discipline, unit_system, domain), segment_ids in sorted(groups.items()):
        for view_scope in ("all", "used"):
            usage_ok = _usage_interpretable_for_role(role)
            by_jh: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: {
                "segments": set(), "files": set(), "projects": set()
            })
            labels: Dict[str, str] = {}
            statuses: Set[str] = set()
            missing_scid_total = 0
            denominator_files: Set[str] = set()
            denominator_projects: Set[str] = set()
            client_has_inventory = False

            for segment_id in sorted(segment_ids):
                files, status, missing_scid = _load_segment_file_join_hashes_with_status(
                    segments_root, registry, segment_id, domain, view_scope
                )
                statuses.add(status)
                missing_scid_total += missing_scid
                for jh, label in load_pattern_labels(segments_root, registry, segment_id, domain).items():
                    labels.setdefault(jh, label)
                for export_run_id, join_hashes in files.items():
                    if join_hashes:
                        client_has_inventory = True
                        denominator_files.add(export_run_id)
                        denominator_projects.add(_project_label_for_file(file_metadata, export_run_id))
                    for join_hash in join_hashes:
                        entry = by_jh[join_hash]
                        entry["segments"].add(segment_id)
                        entry["files"].add(export_run_id)
                        entry["projects"].add(_project_label_for_file(file_metadata, export_run_id))

            if view_scope == "used" and not usage_ok and by_jh:
                inventory_status = "not_interpretable"
            elif by_jh:
                inventory_status = "ok"
            elif "missing_domain_patterns" in statuses:
                inventory_status = "missing_domain_patterns"
            elif view_scope == "used" and "used_view_unavailable" in statuses:
                inventory_status = "used_view_unavailable"
            elif statuses == {"no_patterns"} or "no_patterns" in statuses:
                inventory_status = "no_patterns"
            else:
                inventory_status = "ok"

            source_status = "ok" if missing_scid_total == 0 else "missing_source_cluster_id"
            if not by_jh:
                if inventory_status in {"ok", "not_interpretable"}:
                    continue
                rows.append({
                    "governance_role": role,
                    "client_label": client,
                    "discipline_label": discipline,
                    "unit_system": unit_system,
                    "domain": domain,
                    "view_scope": view_scope,
                    "join_hash": "",
                    "pattern_label": "",
                    "n_segments_present": "0",
                    "n_files_present": "0",
                    "n_files_denominator": "0",
                    "pct_files_present": "0.000000",
                    "n_projects_present": "0",
                    "n_projects_denominator": "0",
                    "n_clients_present": "0",
                    "n_clients_denominator": "1" if client_has_inventory else "0",
                    "pct_clients_present": "0.000000",
                    "pct_projects_present": "0.000000",
                    "usage_interpretable": _bool_str(usage_ok),
                    "inventory_status": inventory_status,
                    "source_status": source_status,
                    "executed_utc": executed_utc,
                })
                continue

            file_den = len(denominator_files)
            project_den = len(denominator_projects)
            for join_hash in sorted(by_jh):
                entry = by_jh[join_hash]
                n_files = len(entry["files"])
                n_projects = len(entry["projects"])
                rows.append({
                    "governance_role": role,
                    "client_label": client,
                    "discipline_label": discipline,
                    "unit_system": unit_system,
                    "domain": domain,
                    "view_scope": view_scope,
                    "join_hash": join_hash,
                    "pattern_label": labels.get(join_hash, ""),
                    "n_segments_present": str(len(entry["segments"])),
                    "n_files_present": str(n_files),
                    "n_files_denominator": str(file_den),
                    "pct_files_present": _safe_pct(n_files, file_den) or "0.000000",
                    "n_projects_present": str(n_projects),
                    "n_projects_denominator": str(project_den),
                    "n_clients_present": "1",
                    "n_clients_denominator": "1",
                    "pct_clients_present": "1.000000",
                    "pct_projects_present": _safe_pct(n_projects, project_den) or "0.000000",
                    "usage_interpretable": _bool_str(usage_ok),
                    "inventory_status": inventory_status,
                    "source_status": source_status,
                    "executed_utc": executed_utc,
                })

    clients_by_group: Dict[Tuple[str, str, str, str, str], Set[str]] = defaultdict(set)
    clients_by_pattern: Dict[Tuple[str, str, str, str, str, str], Set[str]] = defaultdict(set)
    for row in rows:
        if not row.get("join_hash", "").strip() and row.get("inventory_status", "") == "ok":
            continue
        group_key = (
            row.get("view_scope", ""),
            row.get("governance_role", ""),
            row.get("discipline_label", ""),
            row.get("unit_system", ""),
            row.get("domain", ""),
        )
        clients_by_group[group_key].add(row.get("client_label", ""))
        clients_by_pattern[(*group_key, row.get("join_hash", ""))].add(row.get("client_label", ""))

    for row in rows:
        if not row.get("join_hash", "").strip() and row.get("inventory_status", "") == "ok":
            continue
        group_key = (
            row.get("view_scope", ""),
            row.get("governance_role", ""),
            row.get("discipline_label", ""),
            row.get("unit_system", ""),
            row.get("domain", ""),
        )
        n_clients_present = len(clients_by_pattern.get((*group_key, row.get("join_hash", "")), set()))
        n_clients_denominator = len(clients_by_group.get(group_key, set()))
        row["n_clients_present"] = str(n_clients_present)
        row["n_clients_denominator"] = str(n_clients_denominator)
        row["pct_clients_present"] = _safe_pct(n_clients_present, n_clients_denominator) or "0.000000"

    rows.sort(key=lambda r: (
        r["governance_role"], r["client_label"], r["discipline_label"],
        r["unit_system"], r["domain"], r["view_scope"], r["join_hash"],
    ))
    return rows


def _safe_pct(numerator: int, denominator: int) -> str:
    return _fmt(numerator / denominator) if denominator else ""


def _reuse_bucket_for(
    *,
    n_files: int,
    n_files_den: int,
    n_projects: int,
    n_projects_den: int,
    n_clients: int,
    n_clients_den: int,
) -> Tuple[str, str, str]:
    """Classify reuse breadth with explicit denominator basis.

    Buckets are neutral reporting classes, not approval or correctness claims.
    Returns (reuse_bucket, bucket_basis, classification_status).
    """
    if n_files_den <= 0 or n_projects_den <= 0 or n_clients_den <= 0:
        return "unclassified", "denominator_unavailable", "degraded_zero_denominator"

    pct_clients = n_clients / n_clients_den
    pct_files = n_files / n_files_den
    if pct_clients >= REUSE_BUCKET_THRESHOLDS["corpus_wide_min_pct_clients"] and n_clients_den > 1:
        return "corpus_wide", "clients_in_corpus_domain", "ok"
    if pct_files >= REUSE_BUCKET_THRESHOLDS["client_wide_min_pct_files"]:
        return "client_wide", "files_in_role_client_domain", "ok"
    if n_projects >= REUSE_BUCKET_THRESHOLDS["multi_project_min_projects"] and n_projects_den > 1:
        return "multi_project", "projects_in_client_domain", "ok"
    if n_files == 1:
        return "single_file", "files_in_role_client_domain", "ok"
    if n_projects == 1:
        return "single_project", "projects_in_client_domain", "ok"
    if n_files >= REUSE_BUCKET_THRESHOLDS["emerging_min_files"]:
        return "emerging", "files_in_role_client_domain", "ok"
    return "unclassified", "files_in_role_client_domain", "ok"


def build_pattern_reuse_distribution_rows(
    union_rows: List[Dict[str, str]],
    executed_utc: str,
) -> List[Dict[str, str]]:
    """Build reuse distribution rows from normalized union-inventory join_hash rows."""
    candidate_rows = [
        r for r in union_rows
        if r.get("join_hash", "").strip() or r.get("inventory_status", "") != "ok"
    ]
    positive = [r for r in candidate_rows if r.get("join_hash", "").strip()]
    file_den_by_group: Dict[Tuple[str, str, str, str, str, str], int] = {}
    project_den_by_group: Dict[Tuple[str, str, str, str, str, str], int] = {}
    clients_by_group: Dict[Tuple[str, str, str, str, str], Set[str]] = defaultdict(set)
    clients_by_pattern: Dict[Tuple[str, str, str, str, str, str], Set[str]] = defaultdict(set)

    for r in candidate_rows:
        key = (
            r.get("view_scope", ""), r.get("governance_role", ""),
            r.get("client_label", ""), r.get("discipline_label", ""),
            r.get("unit_system", ""), r.get("domain", ""),
        )
        file_den_by_group[key] = max(
            file_den_by_group.get(key, 0),
            int(r.get("n_files_denominator") or r.get("n_files_present") or "0"),
        )
        project_den_by_group[key] = max(
            project_den_by_group.get(key, 0),
            int(r.get("n_projects_denominator") or r.get("n_projects_present") or "0"),
        )
        clients_by_group[(key[0], key[1], key[3], key[4], key[5])].add(key[2])
        clients_by_pattern[(
            key[0], key[1], key[3], key[4], key[5], r.get("join_hash", "")
        )].add(key[2])

    rows: List[Dict[str, str]] = []
    for r in candidate_rows:
        key = (
            r.get("view_scope", ""), r.get("governance_role", ""),
            r.get("client_label", ""), r.get("discipline_label", ""),
            r.get("unit_system", ""), r.get("domain", ""),
        )
        client_group = (key[0], key[1], key[3], key[4], key[5])
        n_files = int(r.get("n_files_present") or "0")
        n_projects = int(r.get("n_projects_present") or "0")
        n_files_den = file_den_by_group.get(key, 0)
        n_projects_den = project_den_by_group.get(key, 0)
        n_clients = len(clients_by_pattern.get((
            key[0], key[1], key[3], key[4], key[5], r.get("join_hash", "")
        ), set()))
        n_clients_den = len(clients_by_group.get(client_group, set()))
        if r.get("source_status", "ok") != "ok":
            bucket, basis, status = (
                "unclassified", "source_status",
                "degraded_" + r.get("source_status", "unknown"),
            )
        elif r.get("inventory_status") != "ok":
            bucket, basis, status = (
                "unclassified", "inventory_status",
                "blocked_" + r.get("inventory_status", "unknown"),
            )
        else:
            bucket, basis, status = _reuse_bucket_for(
                n_files=n_files, n_files_den=n_files_den,
                n_projects=n_projects, n_projects_den=n_projects_den,
                n_clients=n_clients, n_clients_den=n_clients_den,
            )
        rows.append({
            "view_scope": r.get("view_scope", ""),
            "governance_role": r.get("governance_role", ""),
            "client_label": r.get("client_label", ""),
            "discipline_label": r.get("discipline_label", ""),
            "unit_system": r.get("unit_system", ""),
            "domain": r.get("domain", ""),
            "join_hash": r.get("join_hash", ""),
            "pattern_label": r.get("pattern_label", ""),
            "n_files_present": str(n_files),
            "n_files_denominator": str(n_files_den),
            "pct_files_present": _safe_pct(n_files, n_files_den),
            "n_projects_present": str(n_projects),
            "n_projects_denominator": str(n_projects_den),
            "pct_projects_present": _safe_pct(n_projects, n_projects_den),
            "n_clients_present": str(n_clients),
            "n_clients_denominator": str(n_clients_den),
            "pct_clients_present": _safe_pct(n_clients, n_clients_den),
            "reuse_bucket": bucket,
            "bucket_basis": basis,
            "usage_interpretable": r.get("usage_interpretable", ""),
            "inventory_status": r.get("inventory_status", ""),
            "classification_status": status,
            "executed_utc": executed_utc,
        })
    rows.sort(key=lambda r: (
        r["view_scope"], r["governance_role"], r["client_label"],
        r["discipline_label"], r["unit_system"], r["domain"], r["join_hash"],
    ))
    return rows


def build_pattern_reuse_summary_rows(
    distribution_rows: List[Dict[str, str]],
    *,
    by_client: bool,
) -> List[Dict[str, str]]:
    grouped: Dict[Tuple[str, ...], Dict[str, str]] = {}
    counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    for r in distribution_rows:
        key = (
            r["view_scope"], r["governance_role"],
            r["client_label"] if by_client else "",
            r["discipline_label"], r["unit_system"], r["domain"],
            r["reuse_bucket"], r["bucket_basis"], r["usage_interpretable"],
            r["classification_status"], r["executed_utc"],
        )
        counts[key] += 1
        grouped[key] = r
    rows = []
    for key in sorted(counts):
        r = grouped[key]
        rows.append({
            "view_scope": r["view_scope"],
            "governance_role": r["governance_role"],
            "client_label": r["client_label"] if by_client else "",
            "discipline_label": r["discipline_label"],
            "unit_system": r["unit_system"],
            "domain": r["domain"],
            "reuse_bucket": r["reuse_bucket"],
            "bucket_basis": r["bucket_basis"],
            "n_patterns": str(counts[key]),
            "usage_interpretable": r["usage_interpretable"],
            "classification_status": r["classification_status"],
            "executed_utc": r["executed_utc"],
        })
    return rows

def load_segment_join_hash_union(
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    segment_id: str,
    domain: str,
    purge_view: str = "all",
) -> Set[str]:
    result: Set[str] = set()
    for jhs in load_file_join_hashes(segments_root, registry, segment_id, domain, purge_view).values():
        result |= jhs
    return result


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
# Cardinality / status classification — explicit, non-suppressive.
#
# comparison_status replaces the removed n_files >= 5 data_sufficient gate.
# Scores are always computed and emitted regardless of status; status is
# purely interpretive metadata. blocked is reserved for "no data at all" —
# degraded/ok comparisons still carry full, trustworthy metrics, just with
# narrower (degraded) or normal (ok) evidence breadth. cardinality_shape and
# file_count_ratio are descriptive only and never gate anything.
# ---------------------------------------------------------------------------

def _comparison_status(n_files_a: int, n_files_b: int) -> str:
    if n_files_a == 0 or n_files_b == 0:
        return "blocked"
    if (n_files_a == 1 or n_files_b == 1) and n_files_a != n_files_b:
        return "degraded"
    return "ok"


def _cardinality_shape(n_files_a: int, n_files_b: int) -> str:
    if n_files_a == n_files_b:
        return "balanced"
    if n_files_a == 1:
        return "single_a"
    if n_files_b == 1:
        return "single_b"
    return "imbalanced"


def _file_count_ratio(n_files_a: int, n_files_b: int) -> str:
    if n_files_a == 0 or n_files_b == 0:
        return ""
    return _fmt(max(n_files_a, n_files_b) / min(n_files_a, n_files_b))


def _cardinality_fields(n_files_a: int, n_files_b: int) -> Dict[str, str]:
    return {
        "comparison_status": _comparison_status(n_files_a, n_files_b),
        "cardinality_shape": _cardinality_shape(n_files_a, n_files_b),
        "file_count_ratio": _file_count_ratio(n_files_a, n_files_b),
    }


def _union_similarity(jhs_a: Set[str], jhs_b: Set[str]) -> Tuple[str, str, str]:
    """Population-footprint metrics: union(A) vs union(B), independent of
    n_files_a x n_files_b. Returns (jaccard, containment_a_in_b, containment_b_in_a)."""
    union = jhs_a | jhs_b
    shared = jhs_a & jhs_b
    jac = _fmt(len(shared) / len(union)) if union else ""
    c_ab = _fmt(len(shared) / len(jhs_a)) if jhs_a else ""
    c_ba = _fmt(len(shared) / len(jhs_b)) if jhs_b else ""
    return jac, c_ab, c_ba


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

    # Reference heterogeneity: is a multi-file reference a coherent standard
    # (high core share) or a broad union of conflicting sources (low core
    # share)? Degrades gracefully to 1.0 for a single-file reference — a
    # lone file is trivially coherent with itself, not an artificial failure.
    ref_intersection: Optional[Set[str]] = None
    for jhs in ref_files.values():
        ref_intersection = jhs if ref_intersection is None else (ref_intersection & jhs)
    ref_intersection = ref_intersection or set()
    ref_core_share = (
        len(ref_intersection) / len(ref_union) if ref_union else 0.0
    )

    return {
        "n_shared_join_hash": str(len(ref_union & all_b)),
        "all_pairwise_containment_a_in_b_mean": _mean(a_in_b),
        "all_containment_a_in_b_min": _min(a_in_b),
        "all_pairwise_containment_b_in_a_mean": _mean(b_in_a),
        "all_containment_b_in_a_min": _min(b_in_a),
        "n_files_a": str(len(ref_files)),
        "n_files_b": str(len(tgt_files)),
        "n_pairs": str(len(tgt_files)),
        "n_reference_files": str(len(ref_files)),
        "reference_union_pattern_count": str(len(ref_union)),
        "reference_intersection_pattern_count": str(len(ref_intersection)),
        "reference_core_share": _fmt(ref_core_share),
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
    per_a_jaccards: Dict[str, List[float]] = defaultdict(list)
    per_b_jaccards: Dict[str, List[float]] = defaultdict(list)

    for eid_a, jhs_a in files_a.items():
        for eid_b, jhs_b in files_b.items():
            union = jhs_a | jhs_b
            j = len(jhs_a & jhs_b) / len(union) if union else 0.0
            c_ab = len(jhs_a & jhs_b) / len(jhs_a) if jhs_a else 0.0
            c_ba = len(jhs_a & jhs_b) / len(jhs_b) if jhs_b else 0.0
            jaccards.append(j)
            c_ab_list.append(c_ab)
            c_ba_list.append(c_ba)
            per_a_jaccards[eid_a].append(j)
            per_b_jaccards[eid_b].append(j)
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

    # Side-balanced summaries: each A-file's own mean similarity to every B
    # file, then mean/min of those per-file means (and the inverse for B).
    # Exposes directional population experience — in a 1xN comparison, the
    # A-side summary is one file's average similarity to N files; the B-side
    # summary is the distribution of N files against that one A file.
    a_file_means = [sum(v) / len(v) for v in per_a_jaccards.values()]
    b_file_means = [sum(v) / len(v) for v in per_b_jaccards.values()]

    summary = {
        "n_shared_join_hash": str(len(all_a & all_b)),
        "all_pairwise_containment_a_in_b_mean": _mean(c_ab_list),
        "all_containment_a_in_b_min": _min(c_ab_list),
        "all_pairwise_containment_b_in_a_mean": _mean(c_ba_list),
        "all_containment_b_in_a_min": _min(c_ba_list),
        "all_pairwise_jaccard_mean": _mean(jaccards),
        "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
        "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
        "n_files_a": str(len(files_a)),
        "n_files_b": str(len(files_b)),
        "n_pairs": str(len(jaccards)),
        "all_a_file_mean_similarity_to_b_mean": _mean(a_file_means),
        "all_a_file_mean_similarity_to_b_min": _min(a_file_means),
        "all_b_file_mean_similarity_to_a_mean": _mean(b_file_means),
        "all_b_file_mean_similarity_to_a_min": _min(b_file_means),
    }
    return summary, pair_rows


# ---------------------------------------------------------------------------
# Pair descriptor
# ---------------------------------------------------------------------------

DIRECTED_TYPES = {
    "generic_to_template",
    "generic_to_container",
    "generic_to_project",
    "template_to_project",
    "template_to_container",
    "container_to_project",
    "parent_sibling_roles",
    "governance_chain",
    "enterprise_to_project",
    "bc_to_project",
    "enterprise_to_bc",
    "enterprise_to_client",
}

# ---------------------------------------------------------------------------
# Scope-level classification (enterprise / business_center / client_business_center)
#
# Under the explicit-metadata contract (PR1), client_label and
# business_center_label are real, literal, non-blank values on every
# file_metadata.csv row -- "Stantec" / "0000" for Stantec-internal work, a
# real client name / business center number otherwise. A blank value on a
# segment_manifest.csv row therefore no longer means "not a client
# engagement" -- it means this segment's subset simply did not cut on that
# dimension, so the segment pools every value of it (a roll-up). Scope level
# is a classification of a segment's OWN cut values, not of what it pools;
# roll-ups are handled separately by the callers that need them (see
# discover_cross_client() / the enterprise_to_client target logic below),
# not by this function.
#
# A row is Enterprise-scoped only when BOTH client_label == "Stantec" AND
# business_center_label == "0000" -- either alone is not sufficient (a real
# external client can still carry the "0000" bookkeeping tag in principle,
# and Stantec-internal work can carry a real business center). Scope level
# is orthogonal to governance_role -- do not encode Project into it; a
# client+bc segment can be Template, Container, or Project.
# ---------------------------------------------------------------------------

_ENTERPRISE_BC_LABEL = "0000"


def _normalize_bc_label(value: str) -> str:
    v = (value or "").strip()
    if is_blank_or_na(v):
        return ""
    # "0000"/"BC_0000" (any case) are spelling variants of the same
    # enterprise-bookkeeping value elsewhere in the pipeline (e.g. the
    # extraction completeness gate documents both) -- canonicalize to the
    # literal "0000" so they group/classify identically instead of
    # fragmenting into two distinct-looking business centers. This is
    # distinct from the removed blank-fold: a real, non-blank value is
    # still returned, just spelled consistently.
    if v.lower() in _ENTERPRISE_BC_BOOKKEEPING_TOKENS:
        return _ENTERPRISE_BC_LABEL
    return v


def _bc_of(row: Dict[str, str]) -> str:
    return _normalize_bc_label(row.get("business_center_label", ""))


def _client_of(row: Dict[str, str]) -> str:
    v = row.get("client_label", "").strip()
    return "" if is_blank_or_na(v) else v


def _is_internal_client(client_label: str) -> bool:
    return client_label.strip().lower() == "stantec"


def _is_enterprise_bc(bc_label: str) -> bool:
    return bc_label.strip() == _ENTERPRISE_BC_LABEL


def _scope_level(row: Dict[str, str]) -> Optional[str]:
    """Classify a segment row's own (client_label, business_center_label)
    cut values. Returns None when either dimension is not cut on this row
    (a roll-up pooling multiple real scopes) -- callers that need roll-up
    populations (client-wide standards, cross-client comparisons) handle
    that case explicitly rather than treating it as a fourth scope level.
    """
    client = _client_of(row)
    bc = _bc_of(row)
    if not client or not bc:
        return None
    internal = _is_internal_client(client)
    enterprise_bc = _is_enterprise_bc(bc)
    if internal and enterprise_bc:
        return "enterprise"
    if internal and not enterprise_bc:
        return "business_center"
    if not internal and not enterprise_bc:
        return "client_business_center"
    # Real external client literally tagged with the "0000" bookkeeping
    # value -- does not fit a defined scope level.
    return None


def _is_client_wide_rollup(row: Dict[str, str]) -> bool:
    """A real, non-Stantec client's row with business_center_label not cut
    (pools that client's work across whichever real BCs it touches). This
    is the "client-wide roll-up" population -- distinct from
    _scope_level()'s "client_business_center" bucket, which requires bc to
    be cut to one specific real value.
    """
    client = _client_of(row)
    if not client or _is_internal_client(client):
        return False
    return not _bc_of(row)


def _is_standard_role(role_key: str) -> bool:
    # Template/Container are the two governance roles that can carry an
    # independent enterprise/bc/client scope identity for this fan-out.
    # Generic/Generic-Host already pairs unconditionally against every
    # Template/Container/Project in discover_governance_chain()'s
    # generic_ids loop below (no client/bc scoping at all today), so it has
    # no separate scope-scoped edge to add here.
    return role_key in ("template", "container")


# Non-root DIMENSION_CONFIG fields (build_segment_manifest.py), duplicated
# here rather than imported since this module has no dependency on that
# one -- used only by detect_stale_ancestor_encoding()'s heuristic below.
_NON_ROOT_DIMENSION_FIELDS = ("governance_role", "client_label", "discipline_label", "business_center_label")


def detect_stale_ancestor_encoding(manifest: Dict[str, Dict[str, str]]) -> List[str]:
    """Return one warning string per segment whose ancestor_segment_ids value
    looks like it was written before D-028 (pipe-joined instead of
    semicolon-joined) rather than genuinely having only one immediate
    structural ancestor.

    Heuristic, not a proof: a segment with N non-root dimension fields
    present has up to N one-field-drop immediate ancestors (see
    build_segment_manifest.py's _build_segments()), but can legitimately
    have fewer if not every dropped-field variant exists as its own row in a
    sparse corpus -- so "fewer ancestors than fields present" is not itself
    abnormal. What IS a strong, low-false-positive signal: N >= 2 non-root
    fields present, a non-empty ancestor_segment_ids value, ";" not present
    in it at all (splitting on ";" yields exactly one token), AND that one
    token itself contains more than one "|" -- pointing at a single
    multi-part string that looks like more than one concatenated segment_id
    with no way to tell them apart. A well-formed post-D-028 field would
    either have used ";" to separate multiple real ancestors, or have
    exactly one real ancestor whose own segment_id naturally contains at
    most a few "|" characters for a low non-root-field-count segment -- the
    combination of "many fields present, one blob, multiple internal
    pipes" is what the pre-D-028 "|".join(ancestor_ids) bug produces on any
    segment with more than one real ancestor.

    Warning-only (not blocking): a false positive here would incorrectly
    accuse a legitimately sparse, single-ancestor segment of being stale,
    and _build_ancestor_map()'s parent_segment_id fallback already keeps
    lineage exclusion from silently disappearing entirely even in the worst
    case -- this is a diagnostic aid pointed at DECISIONS.md D-028's
    documented "requires a full manifest regeneration" guidance, not a new
    trust gate like the cyclic-ancestry guard below.
    """
    warnings: List[str] = []
    for sid, row in manifest.items():
        raw = (row.get("ancestor_segment_ids") or "").strip()
        if not raw or ";" in raw:
            continue
        fields_present = sum(1 for f in _NON_ROOT_DIMENSION_FIELDS if (row.get(f) or "").strip())
        if fields_present >= 2 and raw.count("|") >= 2:
            warnings.append(
                f"segment={sid}: ancestor_segment_ids={raw!r} has no ';' but {fields_present} "
                f"non-root dimension fields are present and the value contains multiple '|' -- "
                f"this looks like a pre-D-028 pipe-joined manifest (stale/unparseable ancestor "
                f"data). Re-run build_segment_manifest.py to regenerate segment_manifest.csv."
            )
    return warnings


def _build_ancestor_map(manifest: Dict[str, Dict[str, str]]) -> Dict[str, Set[str]]:
    """Map each segment_id to the full transitive closure of its structural
    ancestors (the `structural_ancestor` relation, D-027) — every segment
    whose dimension key is a proper subset of this one's, at any lattice
    depth, not just the single primary parent_segment_id chain.

    Segments are hierarchical cuts of the same underlying file population —
    build_segment_manifest.py derives each child as its parent's population
    narrowed by one additional cut dimension, so a child's files are always
    a subset of its parent's (this is also why a collection-blank BC
    roll-up's own population can be a strict superset of its
    collection-specific children's — see discover_governance_chain()'s
    _is_collection_rollup). Treating an ancestor and its own descendant as
    independent peers — whether pooled together or paired directly — compares
    a segment against data that already contains (some or all of) its own.

    Source: `ancestor_segment_ids` (";"-delimited — see
    build_segment_manifest.py's _build_segments() comment on the encoding),
    which for each segment lists its immediate structural parents — one per
    dropped non-root dimension field, so a segment with N non-root fields
    present can have up to N distinct immediate parents. This is a
    multi-parent adjacency list, not itself the full closure; the walk below
    recursively unions each immediate parent's own ancestor set to complete
    it.

    `parent_segment_id` (the single primary parent) is folded in as an
    additional immediate parent alongside whatever `ancestor_segment_ids`
    lists, rather than replaced outright — this is what actually guarantees
    the "never removes a previously-detected ancestor" superset property:
    `ancestor_segment_ids` may be blank/absent on a manifest a caller built
    by hand (every pre-D-027 test fixture in this repo populates only
    `parent_segment_id`), and treating that as "no ancestors" would silently
    regress exactly the lineage exclusion this function exists to provide.
    On a real, freshly-built segment_manifest.csv the two sources agree
    (`parent_segment_id` is always itself one of `ancestor_segment_ids`'
    entries — see build_segment_manifest.py's _build_segments()), so this
    union changes nothing there; it only matters as a fallback.
    """
    ancestors: Dict[str, Set[str]] = {}

    def _immediate_parents(sid: str) -> Set[str]:
        row = manifest.get(sid, {})
        raw = row.get("ancestor_segment_ids", "")
        parents = {p for p in raw.split(";") if p}
        primary_parent = row.get("parent_segment_id", "").strip()
        if primary_parent:
            parents.add(primary_parent)
        return parents

    def _walk(sid: str, seen: Set[str]) -> Set[str]:
        if sid in ancestors:
            return ancestors[sid]
        result: Set[str] = set()
        for parent in _immediate_parents(sid):
            if parent == sid or parent in seen:
                sys.exit(
                    "[error] Blocked: cyclic segment ancestry detected — "
                    f"{sid!r} revisits already-seen segment {parent!r} while "
                    "walking ancestor_segment_ids; segment_manifest.csv cannot "
                    "be trusted for lineage exclusion until this is fixed"
                )
            result.add(parent)
            result |= _walk(parent, seen | {sid})
        ancestors[sid] = result
        return result

    for sid in manifest:
        _walk(sid, set())
    return ancestors


def _is_lineage_related(ancestor_map: Dict[str, Set[str]], sid_a: str, sid_b: str) -> bool:
    return sid_b in ancestor_map.get(sid_a, set()) or sid_a in ancestor_map.get(sid_b, set())


# ---------------------------------------------------------------------------
# population_containment — empirical containment relation (D-027)
#
# structural_ancestor (_build_ancestor_map/_is_lineage_related above) is
# derived from the dimension lattice and is reliable but incomplete — many
# real population-subset relationships in the corpus have no dimensional
# explanation at all (e.g. a segment whose files all happen to also belong
# to another segment with no shared cut dimension). population_containment
# is computed directly from real export_run_id membership instead, so it
# does not require or assume a dimensional relationship exists, and can
# catch a materially-significant containment coincidence whether or not
# structural_ancestor also explains it.
#
# Materiality-gated: an exact population-subset relationship between two
# very small or very lopsided segments is common by pure chance (a 1-file
# segment is trivially "contained" in nearly anything) and is not evidence
# of real inheritance. Two Jenks-natural-breaks (tools/jenks_utils.py —
# the general-purpose implementation reused here; tools/compute_governance_
# thresholds.py carries its own near-duplicate jenks_natural_breaks() for a
# narrower use, not consolidated here to avoid an unrelated behavior change)
# passes gate this:
#   1. size-noise filter: drop pairs whose smaller side is strictly below the
#      break in min(|pop_a|, |pop_b|) across all non-structural subset pairs
#      (the break value itself belongs to the upper/signal class, per
#      jenks_breaks()'s own documented "values below break_0 are class 1
#      (lowest)" contract — a pair sitting exactly at the break clears the
#      floor, it isn't treated as noise).
#   2. containment-ratio filter, among size survivors only: drop pairs whose
#      min/max size ratio is below the break (same convention: at-the-break
#      clears the floor).
# Both thresholds are fit ONLY on non-structural subset pairs (pairs
# structural_ancestor does not already explain), so the fit isn't diluted by
# structural's own well-behaved signal — but the resulting containment MAP is
# evaluated over every population pair with membership data, structural or
# not, so a guard using it catches both kinds uniformly (this is what lets
# discover_sibling_segments() rely on it alone rather than needing a second,
# separate structural check — see its docstring). Byte-identical populations
# (pa == pb) bypass both thresholds entirely and are always treated as
# contained — equality is the strongest possible form of the subset
# relationship this guard exists to catch, so there is no "how much overlap"
# materiality question left to ask.
# ---------------------------------------------------------------------------

POPULATION_CONTAINMENT_THRESHOLDS_FIELDS: List[str] = [
    "stage", "algorithm", "n_classes", "break_value",
    "source_value_min", "source_value_max",
    "pairs_before", "pairs_after",
]


def _compute_containment_thresholds(
    manifest: Dict[str, Dict[str, str]],
    membership: Dict[str, Set[str]],
    ancestor_map: Dict[str, Set[str]],
) -> Dict[str, object]:
    """Derive population_containment's two materiality thresholds
    (min_population_for_containment, min_containment_ratio) via Jenks
    natural breaks (n_classes=2) over real, non-structural population-subset
    pairs. See the module-level population_containment comment block above
    for the two-stage method and why the fit is restricted to non-structural
    pairs. Returns a dict with both thresholds plus the audit trail consumed
    by write_population_containment_thresholds().
    """
    sids = sorted(sid for sid in manifest if membership.get(sid))
    non_structural_pairs: List[Tuple[str, str]] = []
    for i in range(len(sids)):
        pa = membership[sids[i]]
        for j in range(i + 1, len(sids)):
            b = sids[j]
            pb = membership[b]
            if pa == pb:
                continue
            if not (pa <= pb or pb <= pa):
                continue
            if _is_lineage_related(ancestor_map, sids[i], b):
                continue
            non_structural_pairs.append((sids[i], b))

    sizes = [min(len(membership[a]), len(membership[b])) for a, b in non_structural_pairs]
    size_breaks = jenks_breaks(sizes, n_classes=2)
    min_population_for_containment = float(size_breaks[0]) if size_breaks else 0.0

    size_survivors = [
        (a, b) for a, b in non_structural_pairs
        if min(len(membership[a]), len(membership[b])) >= min_population_for_containment
    ]
    ratios = [
        min(len(membership[a]), len(membership[b])) / max(len(membership[a]), len(membership[b]))
        for a, b in size_survivors
    ]
    ratio_breaks = jenks_breaks(ratios, n_classes=2)
    min_containment_ratio = float(ratio_breaks[0]) if ratio_breaks else 0.0

    ratio_survivors = [
        (a, b) for a, b in size_survivors
        if (min(len(membership[a]), len(membership[b])) / max(len(membership[a]), len(membership[b])))
        >= min_containment_ratio
    ]

    return {
        "min_population_for_containment": min_population_for_containment,
        "min_containment_ratio": min_containment_ratio,
        "size_stage": {
            "source_value_min": min(sizes) if sizes else None,
            "source_value_max": max(sizes) if sizes else None,
            "pairs_before": len(non_structural_pairs),
            "pairs_after": len(size_survivors),
        },
        "ratio_stage": {
            "source_value_min": round(min(ratios), 4) if ratios else None,
            "source_value_max": round(max(ratios), 4) if ratios else None,
            "pairs_before": len(size_survivors),
            "pairs_after": len(ratio_survivors),
        },
    }


def write_population_containment_thresholds(out_dir: Path, thresholds: Dict[str, object]) -> Path:
    """Write the Jenks-derived population_containment thresholds to
    population_containment_thresholds.csv, following the same
    break-value/n_classes/algorithm/source-range/pair-count audit pattern as
    tools/compute_governance_thresholds.py's thresholds.csv — a first-pass,
    inspectable output for human sanity-check, not silently baked into code.
    """
    size_stage = thresholds["size_stage"]
    ratio_stage = thresholds["ratio_stage"]
    rows = [
        {
            "stage": "size_noise_filter",
            "algorithm": "jenks_breaks",
            "n_classes": "2",
            "break_value": str(thresholds["min_population_for_containment"]),
            "source_value_min": str(size_stage["source_value_min"]),
            "source_value_max": str(size_stage["source_value_max"]),
            "pairs_before": str(size_stage["pairs_before"]),
            "pairs_after": str(size_stage["pairs_after"]),
        },
        {
            "stage": "containment_ratio_filter",
            "algorithm": "jenks_breaks",
            "n_classes": "2",
            "break_value": str(thresholds["min_containment_ratio"]),
            "source_value_min": str(ratio_stage["source_value_min"]),
            "source_value_max": str(ratio_stage["source_value_max"]),
            "pairs_before": str(ratio_stage["pairs_before"]),
            "pairs_after": str(ratio_stage["pairs_after"]),
        },
    ]
    path = out_dir / "population_containment_thresholds.csv"
    atomic_write_csv(path, POPULATION_CONTAINMENT_THRESHOLDS_FIELDS, rows)
    return path


def _population_containment_map(
    manifest: Dict[str, Dict[str, str]],
    membership: Dict[str, Set[str]],
    thresholds: Dict[str, object],
) -> Dict[str, Set[str]]:
    """Map each segment_id to the set of other segment_ids it has a
    materially-significant, empirically-real population containment
    relationship with (either direction — the map is symmetric, same
    convention as _build_ancestor_map's per-segment sets).

    Evaluated over every segment pair with real membership data (not
    restricted to non-structural pairs, unlike the threshold fit in
    _compute_containment_thresholds() — see the module-level comment above).
    """
    min_pop = thresholds["min_population_for_containment"]
    min_ratio = thresholds["min_containment_ratio"]
    contains: Dict[str, Set[str]] = defaultdict(set)
    sids = sorted(sid for sid in manifest if membership.get(sid))
    for i in range(len(sids)):
        pa = membership[sids[i]]
        for j in range(i + 1, len(sids)):
            b = sids[j]
            pb = membership[b]
            if pa == pb:
                # Byte-identical populations are the strongest possible form
                # of the subset relationship this guard exists to catch --
                # unconditionally contained, no materiality threshold needed
                # (there's no "how much overlap" question when it's total).
                # A real, if currently unobserved, case: build_segment_
                # manifest.py only WARNS on duplicate bundle population_hash
                # values, it doesn't block the build, so two distinct
                # segment_ids with identical populations are a possible live
                # state, not just a hypothetical one.
                contains[sids[i]].add(b)
                contains[b].add(sids[i])
                continue
            if not (pa <= pb or pb <= pa):
                continue
            smin, smax = sorted((len(pa), len(pb)))
            if smin < min_pop:
                continue
            if smax and (smin / smax) < min_ratio:
                continue
            contains[sids[i]].add(b)
            contains[b].add(sids[i])
    return dict(contains)


def _is_population_contained(
    containment_map: Dict[str, Set[str]], sid_a: str, sid_b: str,
) -> bool:
    return sid_b in containment_map.get(sid_a, set())


ComparisonPair = Tuple[str, str, str]  # (seg_a, seg_b, comparison_type)


# ---------------------------------------------------------------------------
# Pair discovery
#
# Lineage/containment guard audit (D-027): of the pair-emitting functions
# below, discover_sibling_segments() carries both the structural_ancestor
# and population_containment guards (the corpus-verified 101-violation
# defect lived there). discover_governance_chain() carries structural_ancestor
# only, via its own internal _build_ancestor_map() call (see that function's
# decision note). discover_within_segment(), discover_cross_client(),
# discover_client_cross_bc(), and discover_parent_siblings() carry NEITHER
# guard as of this audit — re-running the corpus-level violation check
# (pa <= pb or pb <= pa on real export_run_id sets) against each of their
# emitted pairs found zero real violations on the current corpus, so they
# are flagged here as a known, currently-latent gap rather than fixed
# outright (same posture discover_governance_chain itself was in before this
# session — see finding 5 in the D-027 write-up). discover_within_project()
# is exempt by construction: both sides of every pair it emits are the same
# segment_id, so there is no cross-segment lineage question to guard.
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
            role_map["generic" if _is_generic_role(role) else role].append(c)

        generics = role_map.get("generic", [])
        templates = role_map.get("template", [])
        projects = role_map.get("project", [])
        containers = role_map.get("container", [])

        for g in generics:
            for t in templates:
                if _same_unit(manifest, g, t):
                    pairs.append((g, t, "generic_to_template"))
            for c in containers:
                if _same_unit(manifest, g, c):
                    pairs.append((g, c, "generic_to_container"))
            for p in projects:
                if _same_unit(manifest, g, p):
                    pairs.append((g, p, "generic_to_project"))

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


def _redundant_child_segment_id(row: Dict[str, str]) -> Optional[str]:
    """Extract the target segment_id from a "redundant_single_child:<segment_id>"
    note, if present (see build_segment_manifest.py's _build_segments() pass5).

    build_segment_manifest.py demotes a segment to run_type="registration"
    whenever a direct child's population is byte-identical to its own
    (e.g. a client whose every Project file happens to sit in a single
    business_center_label, now that business_center_label is a real cut
    dimension rather than always-blank) -- correctly avoiding running the
    same population twice under two different segment_ids. That child is not
    a narrower/rescoped population; it IS the same population_hash, just
    recorded under a more specific segment_id. Substituting it back in where
    the demoted row would otherwise have been used is therefore not blending
    distinct comparison grains -- see _is_client_only_project_segment()'s and
    discover_cross_client()'s docstrings on why that anti-pattern must be
    avoided elsewhere in this module.

    segment_id itself uses "|" as its own internal field separator, and other
    notes may already share the pipe-joined `notes` string, so a naive
    `notes.split("|")` would mangle a multi-part child segment_id. pass5 always
    runs last (see build_segment_manifest.py), so "redundant_single_child:" is
    guaranteed to be the final note appended -- take everything after the
    marker to the end of the string instead of splitting.
    """
    notes = row.get("notes", "") or ""
    marker = "redundant_single_child:"
    idx = notes.find(marker)
    if idx == -1:
        return None
    return notes[idx + len(marker):]


def _resolve_runnable_segment(
    manifest: Dict[str, Dict[str, str]], sid: str
) -> Optional[str]:
    """Resolve sid to a run_type in (bundle, reference) segment_id: sid
    itself if already eligible, or -- transitively -- whatever
    population-identical segment build_segment_manifest.py's
    redundant_single_child pass ultimately points at, if any. Transitive
    because a redundant_single_child pointer can itself be redundant to a
    further child (e.g. a Template rollup redundant to its
    single-real-client child, which -- since business_center_label's
    promotion -- is itself redundant to a single-business-center child one
    level deeper); a single-hop lookup would wrongly treat that intermediate,
    still-ineligible row as a dead end. Returns None if sid isn't eligible and
    carries no pointer that eventually resolves to an eligible segment (e.g.
    a genuinely below-min-files/skip segment, or a cycle -- guarded against
    via `visited`, though build_segment_manifest.py's population-subset
    strictly shrinks along any real chain and cannot actually cycle).
    """
    visited: Set[str] = set()
    cur = sid
    while cur not in visited:
        visited.add(cur)
        row = manifest.get(cur)
        if row is None:
            return None
        if row.get("run_type", "").strip().lower() in ("bundle", "reference"):
            return cur
        nxt = _redundant_child_segment_id(row)
        if not nxt:
            return None
        cur = nxt
    return None


def _scope_override_key(comparison_type: str) -> str:
    return f"_scope_override__{comparison_type}"


def _stash_scope_override(
    manifest: Dict[str, Dict[str, str]],
    resolved_sid: str,
    comparison_type: str,
    original_row: Dict[str, str],
) -> None:
    """Record `original_row`'s scope metadata onto the RESOLVED descendant's
    manifest entry, namespaced by comparison_type.

    segment_id_a/segment_id_b in cross_segment_summary.csv must stay the
    resolved descendant -- it's the only segment with real on-disk analysis
    data (segment_output_dir() looks it up via the registry, and the demoted
    original never gets its own analysis run). But _build_summary_row() also
    derives business_center_label_a/_b, discipline_label_a/_b, and
    scope_level_a/_b straight from that same segment's manifest row, which for
    a resolved descendant is its own narrower identity (e.g.
    business_center_label="BC_C") rather than the broader (typically blank-bc)
    population this comparison was actually matched under (Codex review
    finding on PR #380). Stashing the override here lets _build_summary_row()
    show the scope the comparison was grouped on, without needing to change
    which segment_id is used to load data.

    Namespaced by comparison_type (not just resolved_sid) because the SAME
    resolved segment can legitimately appear under its own true identity in a
    different comparison_type -- e.g. "imperial|Project|Sutter|BC_C" is
    correctly bc-scoped when discover_client_cross_bc() uses it directly, even
    while cross_client's override for the SAME sid says otherwise.
    """
    manifest[resolved_sid][_scope_override_key(comparison_type)] = {
        "business_center_label": _bc_of(original_row),
        "discipline_label": original_row.get("discipline_label", ""),
        "scope_level": _scope_level(original_row) or "",
    }


# role_key -> sibling comparison_type, shared between discover_sibling_segments()'s
# candidate-collection pass (which needs the eventual ctype to key a scope
# override -- see _stash_scope_override()) and its pair-emission pass.
_SIBLING_CTYPE_BY_ROLE = {
    "template": "sibling_templates",
    "project": "sibling_projects",
    "container": "sibling_containers",
    "generic": "sibling_generic",
    "generic-host": "sibling_generic",
    "generic_host": "sibling_generic",
}


def discover_sibling_segments(
    manifest: Dict[str, Dict[str, str]],
    ancestor_map: Optional[Dict[str, Set[str]]] = None,
    containment_map: Optional[Dict[str, Set[str]]] = None,
) -> List[ComparisonPair]:
    # Group by (parent_segment_id, governance_role, unit_system). A segment
    # demoted to run_type="registration" by build_segment_manifest.py's
    # redundant_single_child pass is resolved to its population-identical
    # runnable descendant (see _resolve_runnable_segment()) and bucketed under
    # THIS row's own (parent, role, unit) key, not the descendant's own --
    # the descendant's parent_segment_id is one or more levels deeper (e.g.
    # this client's own Project node) and would not be shared with sibling
    # clients' equivalent substitutes.
    #
    # That resolution is also the mechanism behind a real, corpus-verified
    # defect (D-027): resolving a demoted segment to its population-identical
    # runnable descendant and then bucketing that descendant under the
    # DEMOTED row's own (parent, role, unit) key can land two segments in the
    # same sibling group even though one is a genuine structural or empirical
    # ancestor/descendant of the other (e.g. a client-wide Container rollup
    # and that same client's discipline-scoped Container child, both folded
    # into "sibling_containers" once the discipline child's own parent chain
    # resolves through a redundant intermediate). Both guards below are
    # checked -- not just population_containment -- because on the real
    # corpus every verified violation of this kind turned out to be a
    # structural_ancestor relation once _build_ancestor_map() was made
    # complete (D-027); population_containment's Jenks materiality
    # thresholds, fit deliberately only on non-structural pairs, did not
    # independently flag any of them (several are small-population pairs
    # that the materiality filter is designed to treat as noise). It is kept
    # as a second, independent guard for the non-structural coincidental-
    # containment case findings showed exists elsewhere in the corpus (see
    # docs/... population_containment write-up) even though it was not what
    # fixed today's known violations.
    #
    # Both maps are optional and default to "no exclusion" when omitted, so
    # a caller (or test fixture) with no ancestor_segment_ids data and no
    # membership data gets the pre-D-027 behavior unchanged; ancestor_map
    # itself is cheap to derive from the manifest alone when not supplied.
    if ancestor_map is None:
        ancestor_map = _build_ancestor_map(manifest)

    groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        parent = row.get("parent_segment_id", "").strip()
        role = row.get("governance_role", "").strip().lower()
        role_key = "generic" if _is_generic_role(role) else role
        us = row.get("unit_system", "").strip()
        if not (parent and role_key and us):
            continue
        resolved = _resolve_runnable_segment(manifest, sid)
        if resolved is None:
            continue
        if resolved != sid:
            _stash_scope_override(
                manifest, resolved,
                _SIBLING_CTYPE_BY_ROLE.get(role_key, "sibling_segments"),
                row,
            )
        groups[(parent, role_key, us)].append(resolved)

    pairs: List[ComparisonPair] = []
    for (_, role, _), members in groups.items():
        members = sorted(set(members))
        if len(members) < 2:
            continue
        ctype = _SIBLING_CTYPE_BY_ROLE.get(role, "sibling_segments")
        for a, b in combinations(members, 2):
            if _is_lineage_related(ancestor_map, a, b):
                continue
            if containment_map is not None and _is_population_contained(containment_map, a, b):
                continue
            pairs.append((a, b, ctype))
    return pairs


def _is_client_only_project_segment(row: Dict[str, str]) -> bool:
    """True for a Project-role segment scoped by client_label (and,
    optionally, discipline_label) alone -- no business_center/collection
    narrowing -- the "client-level pooled vocabulary" population
    discover_cross_client() compares peer-to-peer. discipline_label is a
    grouping dimension for that comparison, not a disqualifier: a client's
    per-discipline roll-up (e.g. "Kaiser, Architectural") is just as valid a
    client-only population as the client's fully blank-discipline portfolio,
    as long as it isn't further narrowed by business_center or collection.

    Deliberately stricter than _scope_level(row) == "client_business_center":
    a client's own business_center- or collection-scoped Project child would
    also fail that check but is a narrower population than the client's
    per-discipline portfolio, and comparing a narrower slice for one client
    against a broader one for another would silently mix comparison grains --
    exactly the anti-pattern documented on CASCADE_GROUP2_TYPES in
    generate_governance_narrative.py.
    """
    role = row.get("governance_role", "").strip().lower()
    if role != "project":
        return False
    client = row.get("client_label", "").strip()
    if is_blank_or_na(client):
        return False
    return (
        is_blank_or_na(row.get("business_center_label", ""))
        and is_blank_or_na(row.get("collection_label", ""))
    )


def discover_cross_client(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    """Purpose-built client-vs-client comparison: each client's own broadest
    (client-only-scoped) Project population for a given discipline, paired
    against every OTHER client's population for that SAME discipline, within
    the same unit_system. A client's fully blank-discipline portfolio and its
    per-discipline roll-ups are each distinct populations, compared only
    against the matching population (same discipline value, blank included)
    on the other client's side -- never mixed across disciplines.

    Unlike discover_sibling_segments()'s "sibling_projects" -- which only pairs
    Project segments sharing an immediate parent_segment_id, an accident of the
    segment lattice's hierarchy that a corpus with client-scoped Project
    segments nested straight under one enterprise-wide "Project" parent may
    still satisfy, but is not guaranteed to -- this function groups purely by
    (client_label, unit_system, discipline_label) and pairs every distinct
    client combination sharing a discipline. No shared-parent requirement, no
    hardcoded sector restriction (sector filtering, where wanted, is a
    downstream concern of the comparison_type's consumers -- see
    policies/client_sector.csv).

    bc-scoped fallback: a client-only Project segment whose Project files all
    sit in a single business_center_label is population-identical to that
    business-center-scoped child, so build_segment_manifest.py's
    redundant_single_child pass demotes it to run_type="registration" instead
    of leaving a duplicate-population segment runnable -- see
    _resolve_runnable_segment(). That demotion is now common for single-BC
    clients (business_center_label having been promoted to a real cut
    dimension), so the row is resolved to its population-identical runnable
    descendant instead, so those clients aren't silently dropped from
    cross_client entirely. This is not the "loosen the blank-bc requirement"
    anti-pattern _is_client_only_project_segment()'s docstring warns against
    -- the substitute carries the exact same population_hash the demoted
    client-only segment would have, not a narrower slice of it.
    """
    by_client_unit_disc: Dict[Tuple[str, str, str], str] = {}
    for sid, row in manifest.items():
        if not _is_client_only_project_segment(row):
            continue
        client = row.get("client_label", "").strip()
        unit = row.get("unit_system", "").strip()
        disc = row.get("discipline_label", "").strip()
        if not unit:
            continue
        resolved = _resolve_runnable_segment(manifest, sid)
        if resolved is None:
            continue
        if resolved != sid:
            _stash_scope_override(manifest, resolved, "cross_client", row)
        # First-seen wins if the manifest somehow carries more than one
        # client-only Project segment for the same (client, unit, discipline)
        # -- shouldn't happen given build_segment_manifest.py's
        # one-row-per-subset-key contract, but a silent duplicate overwrite
        # would be worse than a deterministic pick.
        by_client_unit_disc.setdefault((client, unit, disc), resolved)

    pairs: List[ComparisonPair] = []
    items = sorted(by_client_unit_disc.items())
    for i, ((client_a, unit_a, disc_a), sid_a) in enumerate(items):
        for (client_b, unit_b, disc_b), sid_b in items[i + 1:]:
            if client_a == client_b or unit_a != unit_b or disc_a != disc_b:
                continue
            pairs.append((sid_a, sid_b, "cross_client"))
    return pairs


def discover_client_cross_bc(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    """Same-client, cross-business-center comparison: for a real (non-Stantec)
    client whose work spans more than one real business center, compare that
    client's per-business-center populations against each other, for every
    pair of business centers the client actually appears in -- not a fixed
    two-BC comparison.

    Matched by client_label, governance_role, discipline_label, unit_system.
    Client-wide roll-ups (business_center_label not cut -- see
    _is_client_wide_rollup()) are out of scope here; this compares only the
    client's client_business_center-scoped populations against each other.
    """
    by_group: Dict[Tuple[str, str, str, str], List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        if row.get("run_type", "").strip().lower() not in ("bundle", "reference"):
            continue
        if _scope_level(row) != "client_business_center":
            continue
        client = row.get("client_label", "").strip()
        unit = row.get("unit_system", "").strip()
        disc = row.get("discipline_label", "").strip()
        role = row.get("governance_role", "").strip().lower()
        by_group[(client, unit, disc, role)].append(sid)

    pairs: List[ComparisonPair] = []
    for _group_key, sids in by_group.items():
        for a_sid, b_sid in combinations(sorted(sids), 2):
            if _bc_of(manifest[a_sid]) == _bc_of(manifest[b_sid]):
                continue
            pairs.append((a_sid, b_sid, "client_cross_bc"))
    return pairs


def discover_parent_siblings(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    # Level-2 segments sharing same level-1 parent, different governance_role
    # Specifically: Template-role vs Project-role. A segment demoted to
    # run_type="registration" by build_segment_manifest.py's
    # redundant_single_child pass is resolved to its population-identical
    # runnable descendant (see _resolve_runnable_segment()), grouped under
    # THIS row's own parent since the descendant's own parent_segment_id is
    # one or more levels deeper. Role is classified from the ORIGINAL
    # (level-2) row, not the resolved descendant -- a blank-role, client-only
    # rollup (e.g. "imperial|Kaiser", pooling every role for that client) can
    # itself be redundant_single_child to a role-scoped descendant (e.g.
    # "imperial|Project|Kaiser", if that client happens to have no non-Project
    # files) whose OWN governance_role is "Project"; classifying by the
    # descendant's role would misfile that blank-role rollup as a genuine
    # Project sibling, which it was never scoped to be.
    #
    # Unlike discover_cross_client()/discover_sibling_segments(), no
    # _stash_scope_override() call here: parent_sibling_roles feeds
    # generate_governance_narrative.py's _group1_scope_pair() (via
    # _target_scope_label()/_is_unscoped_segment()), which classifies
    # "enterprise" scope by re-deriving structure from segment_id_a/_b itself
    # (splitting on "|" and requiring every part past index 2 to be blank) --
    # not by trusting business_center_label_a/_b/discipline_label_a/_b at face
    # value. Since segment_id must stay the resolved descendant (the only
    # segment with real on-disk data), no column override can make
    # _is_unscoped_segment() see it as unscoped; overriding the label columns
    # here would only make the row internally inconsistent (columns
    # disagreeing with segment_id) without changing that already-shipped,
    # untouchable classification. The row still lands in whichever
    # non-enterprise scope_pair bucket its resolved descendant's TRUE shape
    # implies (e.g. tp_by_scope["bc::enterprise"]) -- a real, if not headline,
    # Group 1 evidence source, same as any other already-supported
    # non-enterprise scope_pair.
    by_parent: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for sid, row in manifest.items():
        if row.get("segment_level", "").strip() != "2":
            continue
        parent = row.get("parent_segment_id", "").strip()
        if not parent:
            continue
        role = row.get("governance_role", "").strip().lower()
        if role not in ("template", "project"):
            continue
        resolved = _resolve_runnable_segment(manifest, sid)
        if resolved is None:
            continue
        by_parent[parent].append((resolved, role))

    pairs: List[ComparisonPair] = []
    for _parent, siblings in by_parent.items():
        siblings = sorted(set(siblings))
        templates = [s for s, role in siblings if role == "template"]
        projects = [s for s, role in siblings if role == "project"]
        for t in templates:
            for p in projects:
                if _same_unit(manifest, t, p):
                    pairs.append((t, p, "parent_sibling_roles"))
    return pairs


def discover_governance_chain(
    manifest: Dict[str, Dict[str, str]],
) -> List[ComparisonPair]:
    # Directed pairs along the provision chain:
    # Generic/Generic-Host→Template/Container/Project, Template→Project/Container,
    # and Container→Project. Project target used-view is usage; other target roles
    # remain provided-vocabulary inventories.
    # Reference segments are included — they participate using their file inventories.
    def _key(row: Dict[str, str]) -> Tuple[str, ...]:
        # client_label is blank, or an explicit "not applicable" spelling
        # (na, N/A, __NOT_APPLICABLE__, ...), for roll-up rows that don't cut
        # on client at all (e.g. a BC-wide aggregate). Pooling all of those
        # under a single "" key would group unrelated collections together;
        # fall back to business_center_label first (the real, populated cut
        # dimension for BC-scoped rows per build_segment_manifest.py), then
        # to collection_label as a last-resort fallback for whenever that
        # field does get wired in. is_blank_or_na() (shared with
        # build_segment_manifest.py) recognizes any NA spelling, not just the
        # one literal "__NOT_APPLICABLE__" token this used to hardcode.
        #
        # client_label, business_center_label, and collection_label are
        # distinct cut dimensions with independent text namespaces — a real
        # client named e.g. "BC_2270" must not collide with a business-center
        # row whose business_center_label happens to be the same text. The
        # key therefore tags which dimension supplied the value instead of
        # collapsing them all into one bare string slot.
        #
        # When client_label is populated, business_center_label is folded
        # into the same key alongside it (rather than being ignored) --
        # under the explicit-metadata contract client_label is always
        # populated for Stantec-internal rows too ("Stantec"), so without
        # this, an Enterprise-scoped Template (Stantec/0000) and a specific
        # business center's Template (Stantec/2270) would collapse into one
        # "client=Stantec" bucket and incorrectly pair with that business
        # center's Projects as if they were the same governance population.
        # A populated-client, blank-bc row (a client-wide roll-up) still
        # gets its own distinct bucket via the empty bc slot.
        #
        # collection_label is NOT folded into this key, even though it is a
        # real cut dimension in build_segment_manifest.py's DIMENSION_CONFIG
        # that can distinguish multiple named collections under the same
        # client or business_center. It is intentionally handled the same
        # way discipline_label is — via _collection_match() below, applied
        # when pairs are generated — rather than as a hard partition here.
        # Hard-partitioning by collection would sever the client_label case:
        # a real client's Container/Template rows are typically tagged with
        # that client's own collection_label (e.g. "Sutter Standards"), but
        # its Project rows are typically not tagged with any collection at
        # all. Splitting on collection here would put those two populations
        # in different buckets and silently stop producing
        # template_to_project/container_to_project pairs for that client —
        # the tool's primary comparison. A soft match (required only when
        # both sides have a populated value) blocks two different, both-
        # populated collections from pairing while still letting a
        # collection-tagged standards segment pair against its
        # collection-blank usage.
        unit = row.get("unit_system", "").strip()
        client = row.get("client_label", "").strip()
        if not is_blank_or_na(client):
            return ("client", client, _bc_of(row), unit)
        bc = _bc_of(row)
        if bc:
            return ("business_center", bc, unit)
        collection = row.get("collection_label", "").strip()
        if not is_blank_or_na(collection):
            return ("collection", collection, unit)
        # client_label, business_center_label, and collection_label are all
        # blank/NA — every spelling of "not applicable" must land on the
        # same key here, or e.g. a Template row spelled "__NOT_APPLICABLE__"
        # and a Container row spelled "n/a" (both otherwise-blank, no bc, no
        # collection) would fragment into different by_key buckets and never
        # get compared. Returning the raw `client` token instead of a
        # canonical "" would reintroduce exactly the fragmentation this
        # fallback chain exists to prevent.
        return ("client", "", unit)

    def _disc(row: Dict[str, str]) -> str:
        return row.get("discipline_label", "").strip()

    def _disc_match(ra: Dict[str, str], rb: Dict[str, str]) -> bool:
        # Discipline comparisons require the same unit_system and the same
        # discipline_label, full stop -- no cross-discipline wildcard mode.
        # Under the explicit-metadata contract discipline_label is a
        # required, always-populated field, so this is an exact match in
        # practice; it is intentionally not blank-tolerant for any
        # malformed/legacy row that reaches this function directly.
        return _disc(ra) == _disc(rb)

    def _collection(row: Dict[str, str]) -> str:
        value = row.get("collection_label", "").strip()
        return "" if is_blank_or_na(value) else value

    # A collection-blank row is a wildcard ONLY when its blankness means
    # "collection is simply not tracked here" (the Sutter-shaped case: a
    # Project row that never got a collection_label). It must NOT wildcard
    # when the blankness instead means "this segment is a roll-up pooling
    # every collection under it together" — e.g. build_segment_manifest.py
    # now keeps a runnable business-center-scoped Template/Container
    # aggregate (blank collection_label) alongside its collection-specific
    # children whenever the aggregate's population isn't identical to any
    # single child's (i.e. the business center hosts more than one named
    # collection). Wildcard-matching that aggregate against one specific
    # collection's segment on the other side would mix the pooled
    # population with a single library's population in the same
    # comparison — precisely what collection_label was added to keep
    # apart. A row counts as a roll-up when some OTHER manifest row's
    # parent_segment_id points at it and that other row has a populated
    # collection_label.
    _collection_rollup_ids = {
        row.get("parent_segment_id", "").strip()
        for row in manifest.values()
        if row.get("parent_segment_id", "").strip()
        and not is_blank_or_na(row.get("collection_label", ""))
    }

    def _is_collection_rollup(row: Dict[str, str]) -> bool:
        return row.get("segment_id", "") in _collection_rollup_ids

    def _collection_match(ra: Dict[str, str], rb: Dict[str, str]) -> bool:
        ca, cb = _collection(ra), _collection(rb)
        if ca and cb:
            return ca == cb
        if ca and not cb:
            return not _is_collection_rollup(rb)
        if cb and not ca:
            return not _is_collection_rollup(ra)
        return True

    by_key: Dict[Tuple[str, ...], Dict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for sid, row in manifest.items():
        role = row.get("governance_role", "").strip().lower()
        rt = row.get("run_type", "").strip().lower()
        if (role in ("template", "project", "container") or _is_generic_role(role)) and rt in ("bundle", "reference"):
            by_key[_key(row)]["generic" if _is_generic_role(role) else role].append(sid)

    pairs: List[ComparisonPair] = []

    # Generic / Generic-Host is an upstream stock vocabulary. Compare it across
    # matching unit_system even when its client_label differs from the downstream
    # Template/Container/Project client scope. Discipline and collection, when
    # populated on both sides, still scope the comparison.
    generic_ids = [
        sid for sid, row in manifest.items()
        if _is_generic_role(row.get("governance_role", ""))
        and row.get("run_type", "").strip().lower() in ("bundle", "reference")
    ]
    for g in generic_ids:
        for sid, row in manifest.items():
            role = row.get("governance_role", "").strip().lower()
            if role not in ("template", "container", "project"):
                continue
            if row.get("run_type", "").strip().lower() not in ("bundle", "reference"):
                continue
            if not _same_unit(manifest, g, sid) or not _disc_match(manifest[g], row) or not _collection_match(manifest[g], row):
                continue
            pairs.append((g, sid, f"generic_to_{role}"))

    for _key_tuple, role_map in by_key.items():
        generics = role_map.get("generic", [])
        templates = role_map.get("template", [])
        projects = role_map.get("project", [])
        containers = role_map.get("container", [])

        for g in generics:
            for t in templates:
                if _disc_match(manifest[g], manifest[t]) and _collection_match(manifest[g], manifest[t]):
                    pairs.append((g, t, "generic_to_template"))
            for c in containers:
                if _disc_match(manifest[g], manifest[c]) and _collection_match(manifest[g], manifest[c]):
                    pairs.append((g, c, "generic_to_container"))
            for p in projects:
                if _disc_match(manifest[g], manifest[p]) and _collection_match(manifest[g], manifest[p]):
                    pairs.append((g, p, "generic_to_project"))

        for t in templates:
            for p in projects:
                if _disc_match(manifest[t], manifest[p]) and _collection_match(manifest[t], manifest[p]):
                    pairs.append((t, p, "template_to_project"))
            for c in containers:
                if _disc_match(manifest[t], manifest[c]) and _collection_match(manifest[t], manifest[c]):
                    pairs.append((t, c, "template_to_container"))
        for c in containers:
            for p in projects:
                if _disc_match(manifest[c], manifest[p]) and _collection_match(manifest[c], manifest[p]):
                    pairs.append((c, p, "container_to_project"))

    # --- Scope-level fan-out (enterprise / business_center / client) ---
    # Independent parallel edges alongside the by_key() pairs above — no
    # fixed override precedence is assumed between enterprise/business_center/
    # client standards, since any of them may or may not have adapted from
    # any other. A business_center-scoped Template/Container is meant to
    # apply across whichever clients happen to have work in that bc. An
    # enterprise-scoped Template/Container (Stantec/"0000") has no client/bc
    # narrowing of its own, so it is compared against every runnable Project
    # regardless of scope.
    eligible_rows = [
        (sid, row) for sid, row in manifest.items()
        if row.get("run_type", "").strip().lower() in ("bundle", "reference")
    ]
    standard_rows = [
        (sid, row) for sid, row in eligible_rows
        if _is_standard_role(_role_key(row.get("governance_role", "")))
    ]
    project_rows = [
        (sid, row) for sid, row in eligible_rows
        if _role_key(row.get("governance_role", "")) == "project"
    ]
    enterprise_standards = [(sid, row) for sid, row in standard_rows if _scope_level(row) == "enterprise"]
    bc_standards = [(sid, row) for sid, row in standard_rows if _scope_level(row) == "business_center"]
    # enterprise_to_client targets: a client's standards, whether narrowed to
    # one specific business center (client_business_center scope) or pooled
    # across every business center that client touches (a client-wide
    # roll-up). Both are legitimate, distinct targets -- if a client has
    # both, they produce separate comparison rows, not a merged population.
    client_standards = [
        (sid, row) for sid, row in standard_rows
        if _scope_level(row) == "client_business_center" or _is_client_wide_rollup(row)
    ]

    # These loops group purely by scope level, ignoring parent_segment_id —
    # so an ancestor and its own descendant (e.g. an enterprise-scoped
    # Template and a bc/client-scoped Template nested under it) can
    # otherwise land on opposite sides of one of these edges even though
    # segments are hierarchical cuts of the same underlying file population
    # (a descendant's data is always a subset of its ancestor's). Pairing
    # them as independent standards would compare a segment against data
    # that already contains its own.
    #
    # structural_ancestor only (D-027 decision): this function's guard stays
    # on _build_ancestor_map()/_is_lineage_related() alone, now upgraded for
    # free to the complete transitive-closure lattice (previously a single
    # parent_segment_id chain, which could under-report ancestors whenever a
    # segment had more than one non-root dimension present). Re-running the
    # corpus-level violation check against this function found zero real
    # population-subset violations both before and after that completeness
    # fix, so layering population_containment here too — as
    # discover_sibling_segments() does, where a corpus-verified defect
    # justified it — is deferred rather than spent speculatively. Revisit if
    # a future corpus run surfaces a governance_chain violation
    # structural_ancestor alone doesn't catch.
    ancestor_map = _build_ancestor_map(manifest)

    for e_sid, e_row in enterprise_standards:
        for p_sid, p_row in project_rows:
            if _is_lineage_related(ancestor_map, e_sid, p_sid):
                continue
            if (
                _same_unit(manifest, e_sid, p_sid)
                and _disc_match(e_row, p_row)
                and _collection_match(e_row, p_row)
            ):
                pairs.append((e_sid, p_sid, "enterprise_to_project"))

    for bc_sid, bc_row in bc_standards:
        bc_value = _bc_of(bc_row)
        for p_sid, p_row in project_rows:
            if _bc_of(p_row) != bc_value:
                continue
            if _is_lineage_related(ancestor_map, bc_sid, p_sid):
                continue
            if (
                _same_unit(manifest, bc_sid, p_sid)
                and _disc_match(bc_row, p_row)
                and _collection_match(bc_row, p_row)
            ):
                pairs.append((bc_sid, p_sid, "bc_to_project"))

    for e_sid, e_row in enterprise_standards:
        e_role = _role_key(e_row.get("governance_role", ""))
        for bc_sid, bc_row in bc_standards:
            if _role_key(bc_row.get("governance_role", "")) != e_role:
                continue
            if _is_lineage_related(ancestor_map, e_sid, bc_sid):
                continue
            if (
                _same_unit(manifest, e_sid, bc_sid)
                and _disc_match(e_row, bc_row)
                and _collection_match(e_row, bc_row)
            ):
                pairs.append((e_sid, bc_sid, "enterprise_to_bc"))

    for e_sid, e_row in enterprise_standards:
        e_role = _role_key(e_row.get("governance_role", ""))
        for c_sid, c_row in client_standards:
            if _role_key(c_row.get("governance_role", "")) != e_role:
                continue
            if _is_lineage_related(ancestor_map, e_sid, c_sid):
                continue
            if (
                _same_unit(manifest, e_sid, c_sid)
                and _disc_match(e_row, c_row)
                and _collection_match(e_row, c_row)
            ):
                pairs.append((e_sid, c_sid, "enterprise_to_client"))

    # --- BC-to-BC peers ---
    # Purpose-built discovery, not an accident of shared parent_segment_id:
    # every pair of real business centers' same-role, same-discipline
    # populations. Spans whichever role (Template/Container/Project) has
    # business_center-scoped rows -- scope level is orthogonal to role.
    by_role_bc: Dict[str, List[str]] = defaultdict(list)
    for sid, row in eligible_rows:
        if _scope_level(row) == "business_center":
            by_role_bc[_role_key(row.get("governance_role", ""))].append(sid)
    for _role, sids in by_role_bc.items():
        for a_sid, b_sid in combinations(sorted(sids), 2):
            a_row, b_row = manifest[a_sid], manifest[b_sid]
            if _bc_of(a_row) == _bc_of(b_row):
                continue
            if _is_lineage_related(ancestor_map, a_sid, b_sid):
                continue
            if _same_unit(manifest, a_sid, b_sid) and _disc_match(a_row, b_row):
                pairs.append((a_sid, b_sid, "bc_to_bc"))

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
            label = meta.get("project_label", "").strip()
            proj = eid if is_blank_or_na(label) else label
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


# sibling_* comparison_type values discover_sibling_segments() can emit,
# grouped purely by shared parent_segment_id -- every one of these can
# collide with a purpose-built peer comparison for the exact same
# (seg_a, seg_b) pair (see drop_legacy_siblings_covered_by_peer_comparisons()).
_SIBLING_PEER_TYPES = {
    "sibling_projects", "sibling_templates", "sibling_containers",
    "sibling_generic", "sibling_segments",
}

# Purpose-built peer-comparison types, each discovered independently of
# parent_segment_id (cross_client: client_label; bc_to_bc/client_cross_bc:
# business_center_label/scope_level) -- any of these can duplicate a
# sibling_* row for the same pair whenever the peers they connect also
# happen to share an immediate parent.
_PURPOSE_BUILT_PEER_TYPES = {"cross_client", "bc_to_bc", "client_cross_bc"}


def drop_legacy_siblings_covered_by_peer_comparisons(
    pairs: List[ComparisonPair],
) -> List[ComparisonPair]:
    """A sibling_* row and a purpose-built peer comparison (cross_client,
    bc_to_bc, client_cross_bc) can both fire for the exact same (seg_a, seg_b)
    pair: discover_sibling_segments() groups segments purely by
    (parent_segment_id, governance_role, unit_system), so two segments a
    purpose-built peer function already pairs (by client_label, or by
    scope_level/business_center_label) can ALSO share an immediate parent
    (e.g. an enterprise-wide "unit|Project" rollup, or a client/bc segment's
    natural lattice parent) and get re-paired as a sibling_* type too. Unlike
    deduplicate_pairs()'s general case (different comparison_types for the
    same pair are usually distinct analytical questions and must all be
    preserved), a sibling_* row and its purpose-built counterpart measure the
    identical underlying file-level comparison for the identical two
    segments (both symmetric Jaccard/containment over the same file
    inventories) -- keeping both would just double the row count for zero
    additional signal, since cross_segment_file_pairs.csv carries no
    comparison_type column to distinguish them by. make_comparison_run_id()
    now includes comparison_type in its hash, so the two rows would no
    longer collide on ID -- but they would still be exact duplicates. The
    purpose-built type is the unambiguous producer for its signal; drop the
    sibling_* entry (order-independent) for any pair a purpose-built peer
    type already covers, and leave every other pair/type untouched.

    enterprise_to_bc/enterprise_to_client (discover_governance_chain()) are
    NOT in _PURPOSE_BUILT_PEER_TYPES despite having the identical
    shared-parent collision risk with sibling_templates/sibling_containers,
    because they are directed reference-union containment, not a duplicate
    of the sibling_* symmetric measurement -- dropping the sibling_* row
    there would silently discard real, distinct signal. That case is
    resolved by make_comparison_run_id() disambiguating on comparison_type
    instead: both rows survive, each with its own correct ID.
    """
    peer_covered_pairs = {
        frozenset((a, b)) for a, b, ctype in pairs if ctype in _PURPOSE_BUILT_PEER_TYPES
    }
    return [
        (a, b, ctype) for a, b, ctype in pairs
        if not (ctype in _SIBLING_PEER_TYPES and frozenset((a, b)) in peer_covered_pairs)
    ]


# ---------------------------------------------------------------------------
# comparison_run_id
# ---------------------------------------------------------------------------

def make_comparison_run_id(
    seg_a: str, seg_b: str, executed_utc: str, comparison_type: str = "",
) -> str:
    """comparison_type is included so that two distinct comparison types for
    the exact same (seg_a, seg_b) pair and timestamp never collide on the
    same ID. This does happen in practice: e.g. an enterprise (Stantec/0000)
    standard and a real-BC standard of the same role can share a
    parent_segment_id, so discover_sibling_segments() pairs them as
    sibling_templates/sibling_containers *in addition to*
    discover_governance_chain() pairing them as enterprise_to_bc -- both use
    the same (seg_a, seg_b) orientation (sibling's sorted-ID order happens to
    match enterprise-then-bc order whenever the enterprise segment's
    generated ID sorts first). Unlike the sibling_*-vs-purpose-built-peer
    overlap drop_legacy_siblings_covered_by_peer_comparisons() handles
    (genuinely duplicate symmetric measurements of the same pair), sibling_*
    and enterprise_to_bc/enterprise_to_client are not duplicates -- sibling_*
    is symmetric Jaccard, the enterprise_to_* pairing is directed reference-
    union containment -- so the fix here is to keep both rows and give them
    distinct IDs, not to drop one."""
    token = f"{seg_a}|{seg_b}|{comparison_type}|{executed_utc}"
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
            label = meta.get("project_label", "").strip()
            proj = eid if is_blank_or_na(label) else label
            by_proj[proj][eid] = jhs

        # Used-view project grouping (same labels, but used-view join_hash sets)
        by_proj_used: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
        for eid, jhs in all_files_used.items():
            meta = file_metadata.get(eid, {})
            label = meta.get("project_label", "").strip()
            proj = eid if is_blank_or_na(label) else label
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
            "all_pairwise_jaccard_mean": _mean(jaccards),
            "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
            "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
            "n_files_a": str(n_files),
            "n_files_b": str(n_files),
            "n_pairs": str(len(raw_pairs)),
        }

        crid = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
        all_has_bundles = "true" if bnd_a_wp_all else "false"
        used_has_bundles = "true" if bnd_a_wp_used else "false"
        n_unique_wp = len(total_jhs)

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
            used_pairwise_jaccard_mean=_mean(used_jaccards_wp),
            used_jaccard_p10=_fmt(_pct(used_jaccards_wp, 10)) if used_jaccards_wp else "",
            used_jaccard_p90=_fmt(_pct(used_jaccards_wp, 90)) if used_jaccards_wp else "",
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
        summary_row["all_pairwise_containment_a_in_b_mean"] = _mean(c_ab_list_wp)
        summary_row["all_containment_a_in_b_min"] = _min(c_ab_list_wp)
        summary_row["all_pairwise_containment_b_in_a_mean"] = _mean(c_ba_list_wp)
        summary_row["all_containment_b_in_a_min"] = _min(c_ba_list_wp)
        summary_row["used_pairwise_containment_a_in_b_mean"] = _mean(used_c_ab_list_wp)
        summary_row["used_containment_a_in_b_min"] = _min(used_c_ab_list_wp)
        summary_row["used_pairwise_containment_b_in_a_mean"] = _mean(used_c_ba_list_wp)
        summary_row["used_containment_b_in_a_min"] = _min(used_c_ba_list_wp)
        summary_row["aggregation_method"] = "cartesian_file_pair_mean"
        summary_row.update(_cardinality_fields(n_files, n_files))

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
    n_files_a_ct = len(files_a)
    n_files_b_ct = len(files_b)

    # Zero readable file inventory on either side is the only case that means
    # "don't trust this row at all" -- emit a real, schema-complete row marked
    # blocked instead of silently suppressing it. inventory_status_a/b
    # distinguishes a confirmed-empty domain (source read succeeded, zero
    # patterns) from a side that couldn't be read at all -- both have zero
    # files, but they are not the same fact.
    if n_files_a_ct == 0 or n_files_b_ct == 0:
        status_a, _ = _segment_domain_source_status(segments_root, registry, seg_a, domain)
        status_b, _ = _segment_domain_source_status(segments_root, registry, seg_b, domain)
        crid_blocked = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
        # has_bundles_* documents whether bundle analysis produced output for
        # each side -- availability metadata, not a similarity score -- so
        # it must be computed per side even when the comparison itself is
        # blocked. A populated side's bundles are real and available; only
        # the shared-overlap bucket counts are meaningless when one side has
        # zero files, so those stay at 0.
        bnd_a_all_blocked = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
        bnd_b_all_blocked = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "all")
        bnd_a_used_blocked = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
        bnd_b_used_blocked = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "used")
        blocked_row = _build_summary_row(
            crid_blocked, seg_a, seg_b, comparison_type, domain,
            manifest, {"n_files_a": str(n_files_a_ct), "n_files_b": str(n_files_b_ct), "n_pairs": "0"},
            # n_a/n_b are the populated side's real pattern counts (a blocked
            # side is zero by definition, but the other side may not be) --
            # reporting them as 0 here would corrupt the raw inventory counts
            # a downstream reader needs to understand what was blocked.
            n_patterns_a=n_a, n_patterns_b=n_b,
            n_unique_patterns_a=n_a, n_unique_patterns_b=n_b,
            all_has_bundles_a="true" if bnd_a_all_blocked else "false",
            all_has_bundles_b="true" if bnd_b_all_blocked else "false",
            all_n_shared_bundle_both=0, all_n_shared_bundle_a_only=0, all_n_shared_bundle_b_only=0,
            used_has_bundles_a="true" if bnd_a_used_blocked else "false",
            used_has_bundles_b="true" if bnd_b_used_blocked else "false",
            used_n_shared_bundle_both=0, used_n_shared_bundle_a_only=0, used_n_shared_bundle_b_only=0,
            executed_utc=executed_utc,
        )
        blocked_row.update(_cardinality_fields(n_files_a_ct, n_files_b_ct))
        blocked_row["inventory_status_a"] = status_a
        blocked_row["inventory_status_b"] = status_b
        for key in (
            "all_union_jaccard", "all_union_containment_a_in_b", "all_union_containment_b_in_a",
            "used_union_jaccard", "used_union_containment_a_in_b", "used_union_containment_b_in_a",
            "all_a_file_mean_similarity_to_b_mean", "all_a_file_mean_similarity_to_b_min",
            "all_b_file_mean_similarity_to_a_mean", "all_b_file_mean_similarity_to_a_min",
            "reference_union_pattern_count", "reference_intersection_pattern_count", "reference_core_share",
        ):
            blocked_row[key] = ""
        if is_directed:
            blocked_row["reference_aggregation"] = "union"
            blocked_row["target_aggregation"] = "per_file_distribution"
            blocked_row["n_reference_files"] = str(n_files_a_ct)
        else:
            blocked_row["aggregation_method"] = "cartesian_file_pair_mean"
        return blocked_row, []

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
        crid_pre = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
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

    crid = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
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
        used_pairwise_jaccard_mean=metrics_used.get("all_pairwise_jaccard_mean", ""),
        used_jaccard_p10=metrics_used.get("all_jaccard_p10", ""),
        used_jaccard_p90=metrics_used.get("all_jaccard_p90", ""),
        used_pairwise_containment_a_in_b_mean=metrics_used.get("all_pairwise_containment_a_in_b_mean", ""),
        used_containment_a_in_b_min=metrics_used.get("all_containment_a_in_b_min", ""),
        used_pairwise_containment_b_in_a_mean=metrics_used.get("all_pairwise_containment_b_in_a_mean", ""),
        used_containment_b_in_a_min=metrics_used.get("all_containment_b_in_a_min", ""),
        executed_utc=executed_utc,
    )
    summary.update(_cardinality_fields(n_files_a_ct, n_files_b_ct))
    if is_directed:
        summary["reference_aggregation"] = "union"
        summary["target_aggregation"] = "per_file_distribution"
        summary["n_reference_files"] = metrics.get("n_reference_files", "")
        summary["reference_union_pattern_count"] = metrics.get("reference_union_pattern_count", "")
        summary["reference_intersection_pattern_count"] = metrics.get("reference_intersection_pattern_count", "")
        summary["reference_core_share"] = metrics.get("reference_core_share", "")
    else:
        summary["aggregation_method"] = "cartesian_file_pair_mean"
        all_union_jaccard, all_union_c_ab, all_union_c_ba = _union_similarity(all_jhs_a, all_jhs_b)
        used_union_jaccard, used_union_c_ab, used_union_c_ba = _union_similarity(all_jhs_a_used, all_jhs_b_used)
        summary["all_union_jaccard"] = all_union_jaccard
        summary["all_union_containment_a_in_b"] = all_union_c_ab
        summary["all_union_containment_b_in_a"] = all_union_c_ba
        summary["used_union_jaccard"] = used_union_jaccard
        summary["used_union_containment_a_in_b"] = used_union_c_ab
        summary["used_union_containment_b_in_a"] = used_union_c_ba
        summary["all_a_file_mean_similarity_to_b_mean"] = metrics.get("all_a_file_mean_similarity_to_b_mean", "")
        summary["all_a_file_mean_similarity_to_b_min"] = metrics.get("all_a_file_mean_similarity_to_b_min", "")
        summary["all_b_file_mean_similarity_to_a_mean"] = metrics.get("all_b_file_mean_similarity_to_a_mean", "")
        summary["all_b_file_mean_similarity_to_a_min"] = metrics.get("all_b_file_mean_similarity_to_a_min", "")
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
    executed_utc: str,
    used_n_shared_join_hash: str = "",
    used_pairwise_jaccard_mean: str = "",
    used_jaccard_p10: str = "",
    used_jaccard_p90: str = "",
    used_pairwise_containment_a_in_b_mean: str = "",
    used_containment_a_in_b_min: str = "",
    used_pairwise_containment_b_in_a_mean: str = "",
    used_containment_b_in_a_min: str = "",
) -> Dict[str, str]:
    ma = manifest.get(seg_a, {})
    mb = manifest.get(seg_b, {})
    # A resolved redundant_single_child descendant (see _resolve_runnable_segment())
    # carries an override, stashed by _stash_scope_override(), of the ORIGINAL
    # (demoted) row's business_center_label/discipline_label/scope_level --
    # the broader population this comparison was actually matched under, as
    # opposed to the descendant's own narrower identity. segment_id_a/_b stay
    # the resolved descendant regardless (the only segment with real on-disk
    # data); only these display fields are affected. See Codex review finding
    # on PR #380.
    override_a = ma.get(_scope_override_key(comparison_type)) or {}
    override_b = mb.get(_scope_override_key(comparison_type)) or {}

    # signal_spread: raw containment-asymmetry measure (min-side minus max-side
    # containment share of the shared set); no interpretive banding applied here.
    _n_shared_ss = int(float(metrics.get("n_shared_join_hash") or 0))
    _n_a_ss = int(n_unique_patterns_a) if n_unique_patterns_a else 0
    _n_b_ss = int(n_unique_patterns_b) if n_unique_patterns_b else 0
    _min_ss = min(_n_a_ss, _n_b_ss)
    _max_ss = max(_n_a_ss, _n_b_ss)
    if _min_ss > 0:
        _signal_spread = (_n_shared_ss / _min_ss) - (_n_shared_ss / _max_ss if _max_ss > 0 else 0.0)
        _signal_spread_str = f"{_signal_spread:.4f}"
    else:
        _signal_spread_str = ""

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
        "business_center_label_a": override_a.get("business_center_label", _bc_of(ma)),
        "business_center_label_b": override_b.get("business_center_label", _bc_of(mb)),
        "scope_level_a": override_a.get("scope_level", _scope_level(ma) or ""),
        "scope_level_b": override_b.get("scope_level", _scope_level(mb) or ""),
        "discipline_label_a": override_a.get("discipline_label", ma.get("discipline_label", "")),
        "discipline_label_b": override_b.get("discipline_label", mb.get("discipline_label", "")),
        "unit_system": ma.get("unit_system", ""),
        "comparison_type": comparison_type,
        "domain": domain,
        "n_patterns_a": str(n_patterns_a),
        "n_patterns_b": str(n_patterns_b),
        "n_shared_join_hash": metrics.get("n_shared_join_hash", ""),
        "n_unique_patterns_a": str(n_unique_patterns_a),
        "n_unique_patterns_b": str(n_unique_patterns_b),
        "signal_spread": _signal_spread_str,
        "all_pairwise_containment_a_in_b_mean": metrics.get("all_pairwise_containment_a_in_b_mean", ""),
        "all_containment_a_in_b_min": metrics.get("all_containment_a_in_b_min", ""),
        "all_pairwise_containment_b_in_a_mean": metrics.get("all_pairwise_containment_b_in_a_mean", ""),
        "all_containment_b_in_a_min": metrics.get("all_containment_b_in_a_min", ""),
        "all_pairwise_jaccard_mean": metrics.get("all_pairwise_jaccard_mean", ""),
        "all_jaccard_p10": metrics.get("all_jaccard_p10", ""),
        "all_jaccard_p90": metrics.get("all_jaccard_p90", ""),
        "used_pairwise_jaccard_mean": used_pairwise_jaccard_mean,
        "used_jaccard_p10": used_jaccard_p10,
        "used_jaccard_p90": used_jaccard_p90,
        "used_pairwise_containment_a_in_b_mean": used_pairwise_containment_a_in_b_mean,
        "used_containment_a_in_b_min": used_containment_a_in_b_min,
        "used_pairwise_containment_b_in_a_mean": used_pairwise_containment_b_in_a_mean,
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
        "reference_usage_interpretable": _bool_str(_usage_interpretable_for_role(ma.get("governance_role", ""))),
        "target_usage_interpretable": _bool_str(_usage_interpretable_for_role(mb.get("governance_role", ""))),
        "recommended_primary_view": _recommended_primary_view(
            ma.get("governance_role", ""), mb.get("governance_role", ""), comparison_type
        ),
        "comparison_role_semantics": _comparison_role_semantics(
            ma.get("governance_role", ""), mb.get("governance_role", ""), comparison_type
        ),
        "executed_utc": executed_utc,
    }


def build_governance_state_outputs(
    crid: str,
    seg_ref: str,
    seg_tgt: str,
    comparison_type: str,
    domain: str,
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    segments_root: Path,
    executed_utc: str,
) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """Emit directed governance rows over reference_all ∪ target_all.

    Reference all-view represents provisioned vocabulary. Target all-view is the
    configured downstream vocabulary. Target used-view is a governance/use signal
    only for Project targets; otherwise it is preserved as annotation.
    """
    ma = manifest.get(seg_ref, {})
    mb = manifest.get(seg_tgt, {})
    role_ref = ma.get("governance_role", "")
    role_tgt = mb.get("governance_role", "")
    unit_system = ma.get("unit_system", "")
    ref_usage_interpretable = _usage_interpretable_for_role(role_ref)
    tgt_usage_interpretable = _usage_interpretable_for_role(role_tgt)
    recommended_view = _recommended_primary_view(role_ref, role_tgt, comparison_type)

    ref_all = load_segment_join_hash_union(segments_root, registry, seg_ref, domain, "all")
    tgt_files_all = load_file_join_hashes(segments_root, registry, seg_tgt, domain, "all")
    tgt_files_used = load_file_join_hashes(segments_root, registry, seg_tgt, domain, "used")
    tgt_all: Set[str] = set()
    for jhs in tgt_files_all.values():
        tgt_all |= jhs
    tgt_used: Set[str] = set()
    for jhs in tgt_files_used.values():
        tgt_used |= jhs

    bnd_tgt_all = load_bundle_join_hash_set(segments_root, registry, seg_tgt, domain, "all")
    bnd_tgt_used = load_bundle_join_hash_set(segments_root, registry, seg_tgt, domain, "used")
    pattern_labels = load_pattern_labels(segments_root, registry, seg_tgt, domain)
    ref_labels = load_pattern_labels(segments_root, registry, seg_ref, domain)

    generic_set = get_role_jh_set("generic", domain, unit_system, manifest, registry, segments_root)
    template_set = get_role_jh_set("template", domain, unit_system, manifest, registry, segments_root)
    container_set = get_role_jh_set("container", domain, unit_system, manifest, registry, segments_root)

    n_tgt_all_files = len(tgt_files_all)
    n_tgt_used_files = len(tgt_files_used)
    rows: List[Dict[str, str]] = []
    state_counts: Dict[str, int] = defaultdict(int)

    for jh in sorted(ref_all | tgt_all):
        in_ref = jh in ref_all
        in_tgt_all = jh in tgt_all
        in_tgt_used = jh in tgt_used
        is_bnd_all = jh in bnd_tgt_all
        is_bnd_used = jh in bnd_tgt_used
        state = _classify_governance_state(
            in_ref, in_tgt_all, in_tgt_used, is_bnd_all, tgt_usage_interpretable
        )
        state_counts[state] += 1
        n_files_tgt_all = sum(1 for jhs in tgt_files_all.values() if jh in jhs)
        n_files_tgt_used = sum(1 for jhs in tgt_files_used.values() if jh in jhs)
        rows.append({
            "comparison_run_id": crid,
            "comparison_type": comparison_type,
            "segment_id_reference": seg_ref,
            "segment_id_target": seg_tgt,
            "segment_label_reference": ma.get("segment_label", ""),
            "segment_label_target": mb.get("segment_label", ""),
            "governance_role_reference": role_ref,
            "governance_role_target": role_tgt,
            "business_center_label_reference": _bc_of(ma),
            "business_center_label_target": _bc_of(mb),
            "unit_system": unit_system,
            "domain": domain,
            "join_hash": jh,
            "pattern_label": pattern_labels.get(jh, "") or ref_labels.get(jh, ""),
            "in_reference_all": _bool_str(in_ref),
            "in_target_all": _bool_str(in_tgt_all),
            "in_target_used": _bool_str(in_tgt_used),
            "state": state,
            "n_files_in_target_all": str(n_files_tgt_all),
            "pct_files_in_target_all": _fmt(n_files_tgt_all / n_tgt_all_files) if n_tgt_all_files else _fmt(0.0),
            "n_files_in_target_used": str(n_files_tgt_used),
            "pct_files_in_target_used": _fmt(n_files_tgt_used / n_tgt_used_files) if n_tgt_used_files else _fmt(0.0),
            "in_any_generic": _bool_str(jh in generic_set),
            "in_any_template": _bool_str(jh in template_set),
            "in_any_container": _bool_str(jh in container_set),
            "is_bundle_member_target_all": _bool_str(is_bnd_all),
            "is_bundle_member_target_used": _bool_str(is_bnd_used),
            "reference_usage_interpretable": _bool_str(ref_usage_interpretable),
            "target_usage_interpretable": _bool_str(tgt_usage_interpretable),
            "recommended_primary_view": recommended_view,
            "executed_utc": executed_utc,
        })

    ref_den = len(ref_all)
    tgt_all_den = len(tgt_all)
    tgt_used_den = len(tgt_used)
    provided_configured = len(ref_all & tgt_all)
    # Used-view governance summary metrics are active-delivery signals only for
    # Project targets. For Template/Generic/most Container targets, target_used is
    # retained as row-level annotation but not summarized as passive/active state.
    provided_used = len(ref_all & tgt_used) if tgt_usage_interpretable else 0
    provided_passive = len((ref_all & tgt_all) - tgt_used) if tgt_usage_interpretable else 0
    provided_missing = len(ref_all - tgt_all)
    local_active = len(tgt_used - ref_all) if tgt_usage_interpretable else 0

    summary = {
        "comparison_run_id": crid,
        "comparison_type": comparison_type,
        "segment_id_reference": seg_ref,
        "segment_id_target": seg_tgt,
        "segment_label_reference": ma.get("segment_label", ""),
        "segment_label_target": mb.get("segment_label", ""),
        "governance_role_reference": role_ref,
        "governance_role_target": role_tgt,
        "business_center_label_reference": _bc_of(ma),
        "business_center_label_target": _bc_of(mb),
        "unit_system": unit_system,
        "domain": domain,
        "reference_all_count": str(ref_den),
        "target_all_count": str(tgt_all_den),
        "target_used_count": str(tgt_used_den),
        "provided_to_configured_containment": _fmt(provided_configured / ref_den) if ref_den else "",
        "provided_to_used_containment": _fmt(provided_used / ref_den) if tgt_usage_interpretable and ref_den else "",
        "provided_passive_share": _fmt(provided_passive / ref_den) if tgt_usage_interpretable and ref_den else "",
        "provided_missing_share": _fmt(provided_missing / ref_den) if ref_den else "",
        "local_active_share": _fmt(local_active / tgt_used_den) if tgt_usage_interpretable and tgt_used_den else "",
        "provided_and_used_count": str(state_counts.get("provided_and_used", 0)),
        "provided_but_passive_count": str(state_counts.get("provided_but_passive", 0)),
        "provided_but_missing_count": str(state_counts.get("provided_but_missing", 0)),
        "local_active_count": str(state_counts.get("local_active", 0)),
        "local_passive_count": str(state_counts.get("local_passive", 0)),
        "local_unbundled_count": str(state_counts.get("local_unbundled", 0)),
        "provided_configured_count": str(state_counts.get("provided_configured", 0)),
        "local_configured_count": str(state_counts.get("local_configured", 0)),
        "provided_and_used_pct_of_reference_all": _fmt(state_counts.get("provided_and_used", 0) / ref_den) if tgt_usage_interpretable and ref_den else "",
        "provided_but_passive_pct_of_reference_all": _fmt(state_counts.get("provided_but_passive", 0) / ref_den) if tgt_usage_interpretable and ref_den else "",
        "provided_but_missing_pct_of_reference_all": _fmt(state_counts.get("provided_but_missing", 0) / ref_den) if ref_den else "",
        "local_active_pct_of_target_used": _fmt(state_counts.get("local_active", 0) / tgt_used_den) if tgt_usage_interpretable and tgt_used_den else "",
        "local_passive_pct_of_target_all": _fmt(state_counts.get("local_passive", 0) / tgt_all_den) if tgt_usage_interpretable and tgt_all_den else "",
        "local_unbundled_pct_of_target_all": _fmt(state_counts.get("local_unbundled", 0) / tgt_all_den) if tgt_all_den else "",
        "reference_usage_interpretable": _bool_str(ref_usage_interpretable),
        "target_usage_interpretable": _bool_str(tgt_usage_interpretable),
        "recommended_primary_view": recommended_view,
        "comparison_role_semantics": _comparison_role_semantics(role_ref, role_tgt, comparison_type),
        "executed_utc": executed_utc,
    }
    return rows, summary


# ---------------------------------------------------------------------------
# Pooled comparison
# ---------------------------------------------------------------------------

def _build_pooled_row(
    focal_sid: str,
    pool_sids: List[str],
    domain: str,
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    segments_root: Path,
    min_patterns: int,
    executed_utc: str,
    pool_scope: str,
    pool_key_str: str,
) -> Optional[Dict[str, str]]:
    """Compute one focal-vs-pool row. Shared across every pool_scope grain
    (parent_sibling, bc, client) — only pool membership and the reported
    pool_scope differ between grains; the containment/bundle math is
    identical."""
    focal_files = load_file_join_hashes(segments_root, registry, focal_sid, domain)
    focal_union: Set[str] = set()
    for jhs in focal_files.values():
        focal_union |= jhs

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

    n_files_focal = len(focal_files)
    n_files_pool = len(pool_files_keyed)

    # Zero readable file inventory on either side -- emit a blocked row with
    # blank similarity fields (not a zero-valued one) instead of suppressing
    # it outright. See run_pair()'s equivalent short-circuit for rationale.
    if n_files_focal == 0 or n_files_pool == 0:
        mf_blocked = manifest.get(focal_sid, {})
        crid_blocked = make_comparison_run_id(focal_sid, f"pool_{pool_scope}_{pool_key_str}", executed_utc)
        # has_bundles_* is availability metadata (did bundle analysis
        # produce output for this side), not a similarity score -- compute
        # it per side even when blocked. The pool side is an aggregate of
        # every pool_sids member, same as the non-blocked path below; only
        # the shared-overlap bucket counts are meaningless when the focal
        # side has zero files, so those stay at 0.
        focal_bundle_all_blocked = load_bundle_join_hash_set(
            segments_root, registry, focal_sid, domain, "all"
        )
        focal_bundle_used_blocked = load_bundle_join_hash_set(
            segments_root, registry, focal_sid, domain, "used"
        )
        pool_bundle_all_blocked: Set[str] = set()
        pool_bundle_used_blocked: Set[str] = set()
        for pool_sid in pool_sids:
            pool_bundle_all_blocked |= load_bundle_join_hash_set(
                segments_root, registry, pool_sid, domain, "all"
            )
            pool_bundle_used_blocked |= load_bundle_join_hash_set(
                segments_root, registry, pool_sid, domain, "used"
            )
        blocked_row = {
            "comparison_run_id": crid_blocked,
            "segment_id": focal_sid,
            "segment_label": mf_blocked.get("segment_label", ""),
            "governance_role": mf_blocked.get("governance_role", ""),
            "client_label": mf_blocked.get("client_label", ""),
            "business_center_label": _bc_of(mf_blocked),
            "scope_level": _scope_level(mf_blocked) or "",
            "unit_system": mf_blocked.get("unit_system", ""),
            "domain": domain,
            "pool_scope": pool_scope,
            "n_files_focal": str(n_files_focal),
            "n_files_pool": str(n_files_pool),
            "n_unique_patterns_focal": str(len(focal_union)),
            "n_unique_patterns_pool": str(len(pool_union)),
            "n_shared_join_hash": "",
            "signal_spread": "",
            "all_containment_focal_in_pool": "",
            "all_containment_pool_in_focal": "",
            "used_containment_focal_in_pool": "",
            "used_containment_pool_in_focal": "",
            "all_has_bundles_focal": "true" if focal_bundle_all_blocked else "false",
            "all_has_bundles_pool": "true" if pool_bundle_all_blocked else "false",
            "all_n_shared_bundle_both": "0",
            "all_n_shared_bundle_focal_only": "0",
            "all_n_shared_bundle_pool_only": "0",
            "used_has_bundles_focal": "true" if focal_bundle_used_blocked else "false",
            "used_has_bundles_pool": "true" if pool_bundle_used_blocked else "false",
            "used_n_shared_bundle_both": "0",
            "used_n_shared_bundle_focal_only": "0",
            "used_n_shared_bundle_pool_only": "0",
            "executed_utc": executed_utc,
        }
        blocked_row.update(_cardinality_fields(n_files_focal, n_files_pool))
        return blocked_row

    if len(focal_union) < min_patterns or len(pool_union) < min_patterns:
        return None

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

    # signal_spread: raw containment-asymmetry measure, same formula as
    # _build_summary_row; no interpretive banding applied here.
    _min_pu = min(n_focal_unique, n_pool_unique)
    _max_pu = max(n_focal_unique, n_pool_unique)
    if _min_pu > 0:
        _pooled_signal_spread = (n_shared / _min_pu) - (n_shared / _max_pu if _max_pu > 0 else 0.0)
        _pooled_signal_spread_str = f"{_pooled_signal_spread:.4f}"
    else:
        _pooled_signal_spread_str = ""

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
    crid = make_comparison_run_id(focal_sid, f"pool_{pool_scope}_{pool_key_str}", executed_utc)

    row = {
        "comparison_run_id": crid,
        "segment_id": focal_sid,
        "segment_label": mf.get("segment_label", ""),
        "governance_role": mf.get("governance_role", ""),
        "client_label": mf.get("client_label", ""),
        "business_center_label": _bc_of(mf),
        "scope_level": _scope_level(mf) or "",
        "unit_system": mf.get("unit_system", ""),
        "domain": domain,
        "pool_scope": pool_scope,
        "n_files_focal": str(n_files_focal),
        "n_files_pool": str(n_files_pool),
        "n_unique_patterns_focal": str(n_focal_unique),
        "n_unique_patterns_pool": str(n_pool_unique),
        "n_shared_join_hash": str(n_shared),
        "signal_spread": _pooled_signal_spread_str,
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
        "executed_utc": executed_utc,
    }
    row.update(_cardinality_fields(n_files_focal, n_files_pool))
    return row


def run_pooled_comparison(
    manifest: Dict[str, Dict[str, str]],
    registry: Dict[str, Dict[str, str]],
    segments_root: Path,
    min_patterns: int,
    executed_utc: str,
    domain_filter: Optional[str] = None,
    focal_segment_ids: Optional[Set[str]] = None,
) -> List[Dict[str, str]]:
    """N-1 pooled comparison, across three independent pool grains.

    Each grain is a genuinely different pool with different membership, not
    a different view of the same pool (grid analogy: fix-row-vary-column vs.
    fix-column-vary-row):

      - parent_sibling: pool = sibling segments sharing the same
        (parent_segment_id, governance_role, unit_system) — the narrowest
        client+bc-together pool. This is the original/default pool grain.
      - bc: pool = segments sharing the same (business_center_label, role,
        unit_system), ignoring client_label — pools whichever clients happen
        to have work in that bc, to check bc-level consistency.
      - client: pool = segments sharing the same (client_label, role,
        unit_system), ignoring business_center_label — pools whichever bcs
        happen to have work for that client, to check client-level
        consistency.

    business_center_label is normalized via _bc_of() before bc-pool grouping
    (blank/NA spellings fold to blank; "0000"/"BC_0000" spelling variants
    canonicalize to the literal "0000" rather than folding to blank -- see
    _normalize_bc_label()).

    Emits one row per (segment_id, domain, pool_scope).
    """
    parent_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    bc_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    client_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        role = row.get("governance_role", "").strip().lower()
        us = row.get("unit_system", "").strip()
        rt = registry.get(sid, {}).get("run_type", "").strip().lower()
        if rt in ("skip", "registration"):
            continue
        if not role or not us:
            continue
        parent = row.get("parent_segment_id", "").strip()
        if parent:
            parent_groups[(parent, role, us)].append(sid)
        # pool_scope ("parent_sibling"|"bc"|"client") answers a different question
        # than scope_level ("enterprise"|"business_center"|"client_business_center"):
        # pool_scope is which axis this pool was GROUPED along, not where the
        # segment sits organizationally. They are not parallel/competing
        # classifications -- both are derived from the same normalized
        # business_center_label via _bc_of()/_normalize_bc_label(), so an
        # Enterprise segment (business_center_label == "0000") is never silently
        # excluded or mis-bucketed by either path. Do not attempt to collapse
        # pool_scope into scope_level; a segment's scope_level is fixed, but the
        # same segment can appear in a "bc" pool and a "client" pool depending on
        # which sibling group is being pooled against.
        bc = _bc_of(row)
        if bc:
            bc_groups[(bc, role, us)].append(sid)
        client = _client_of(row)
        if client:
            client_groups[(client, role, us)].append(sid)

    ancestor_map = _build_ancestor_map(manifest)

    rows: List[Dict[str, str]] = []

    # Memoized across every group/grain in this call -- the same segment_id
    # can appear as a member of several sibling groups (parent_sibling, bc,
    # client grains all draw from the same manifest), so without this a
    # large corpus would re-discover the same segment's domains repeatedly.
    domains_cache: Dict[str, Set[str]] = {}

    def _domains_for(sid: str) -> Set[str]:
        if sid not in domains_cache:
            domains_cache[sid] = discover_domains_for_segment(segments_root, registry, sid)
        return domains_cache[sid]

    def _emit_for_groups(
        groups: Dict[Tuple[str, str, str], List[str]], pool_scope: str
    ) -> None:
        sibling_groups = {k: v for k, v in groups.items() if len(v) >= 2}
        for key, members in sibling_groups.items():
            pool_key_str = "_".join(key)
            for focal_sid in members:
                if focal_segment_ids is not None and focal_sid not in focal_segment_ids:
                    continue
                # Exclude any member in the focal's own parent_segment_id
                # lineage (ancestor or descendant) — a bc/client pool grain
                # ignores parent_segment_id for grouping, so an ancestor
                # roll-up and its own child can otherwise land in the same
                # pool even though the roll-up's population already
                # contains (some or all of) the child's own data.
                pool_sids = [
                    s for s in members
                    if s != focal_sid and not _is_lineage_related(ancestor_map, focal_sid, s)
                ]
                if not pool_sids:
                    # Lineage filtering removed every candidate peer (e.g. a
                    # 2-member group where the other member is this focal's
                    # own ancestor/descendant) -- there is no pool to compare
                    # against, not an unreadable one. Emitting a
                    # comparison_status="blocked" row here would misrepresent
                    # "no eligible pool exists" as "the pool's inventory
                    # couldn't be read," inflating blocked counts with
                    # comparisons that were never eligible in the first
                    # place. Skip entirely, matching pre-blocked-row behavior
                    # for this case.
                    continue

                # Union with the pool's own domains, not just the focal
                # segment's -- otherwise a focal segment with zero inventory
                # for a domain the pool has (n_files_focal=0, n_files_pool>0,
                # the exact case _build_pooled_row()'s blocked-row path
                # exists to report) never gets scheduled at all, since there
                # would be no domain to iterate for it.
                focal_domains = _domains_for(focal_sid)
                for s in pool_sids:
                    focal_domains = focal_domains | _domains_for(s)
                if domain_filter:
                    focal_domains = focal_domains & {domain_filter}

                for domain in sorted(focal_domains):
                    pooled_row = _build_pooled_row(
                        focal_sid, pool_sids, domain, manifest, registry,
                        segments_root, min_patterns, executed_utc,
                        pool_scope, pool_key_str,
                    )
                    if pooled_row is not None:
                        rows.append(pooled_row)

    _emit_for_groups(parent_groups, "parent_sibling")
    _emit_for_groups(bc_groups, "bc")
    _emit_for_groups(client_groups, "client")

    return rows



# ---------------------------------------------------------------------------
# Explicit matrix/reporting outputs
# ---------------------------------------------------------------------------

def _matrix_group_id_from_values(
    role: str,
    client: str,
    discipline: str,
    unit_system: str,
) -> str:
    return "|".join([role, client, discipline, unit_system])


def _matrix_group_id(row: Dict[str, str]) -> str:
    return _matrix_group_id_from_values(
        row.get("governance_role", ""),
        row.get("client_label", ""),
        row.get("discipline_label", ""),
        row.get("unit_system", ""),
    )


def _label_by_project_group(summary_rows: List[Dict[str, str]]) -> Dict[str, str]:
    labels: Dict[str, Set[str]] = defaultdict(set)
    for row in summary_rows:
        for suffix in ("a", "b"):
            if _role_key(row.get(f"governance_role_{suffix}", "")) != "project":
                continue
            group_id = _matrix_group_id_from_values(
                row.get(f"governance_role_{suffix}", ""),
                row.get(f"client_label_{suffix}", ""),
                row.get(f"discipline_label_{suffix}", ""),
                row.get("unit_system", ""),
            )
            label = row.get(f"segment_label_{suffix}", "").strip() or row.get(f"segment_id_{suffix}", "").strip()
            if group_id and label:
                labels[group_id].add(label)
    return {group_id: next(iter(values)) for group_id, values in labels.items() if len(values) == 1}


def _jaccard_sets(a: Set[str], b: Set[str]) -> Optional[float]:
    union = a | b
    if not union:
        return None
    return len(a & b) / len(union)


def _cosine_similarity(a: Dict[str, float], b: Dict[str, float]) -> Optional[float]:
    keys = set(a) | set(b)
    if not keys:
        return None
    dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
    na = sum(a.get(k, 0.0) ** 2 for k in keys) ** 0.5
    nb = sum(b.get(k, 0.0) ** 2 for k in keys) ** 0.5
    if na == 0.0 or nb == 0.0:
        return None
    return dot / (na * nb)


def build_explicit_matrix_outputs(
    summary_rows: List[Dict[str, str]],
    pooled_rows: List[Dict[str, str]],
    union_inventory_rows: List[Dict[str, str]],
    executed_utc: str,
) -> Tuple[Dict[str, List[Dict[str, str]]], List[Dict[str, str]], List[Dict[str, str]]]:
    """Build metric-specific matrix outputs with explicit semantics.

    Returns (matrix_rows_by_filename, fragmentation_rows, manifest_rows). Missing
    union inventory blocks union/density matrices by emitting unavailable-status
    rows in their named outputs rather than falling back to file-pair signals.
    """
    outputs: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    manifests: List[Dict[str, str]] = []
    label_by_group = _label_by_project_group(summary_rows)

    def add_manifest(name: str, role: str, view: str, source: str, grain: str,
                     metric: str, identity: str, agg: str, interp: str, limits: str) -> None:
        manifests.append({
            "matrix_name": name, "governance_role": role, "view_scope": view,
            "source_file": source, "source_grain": grain, "metric": metric,
            "identity_unit": identity, "aggregation_method": agg,
            "interpretation": interp, "known_limitations": limits,
            "executed_utc": executed_utc,
        })

    def add_matrix(filename: str, row_id: str, col_id: str, view: str, domain: str,
                   metric: str, value: Optional[float], status: str, interp: str) -> None:
        outputs[filename].append({
            "matrix_name": filename, "row_id": row_id, "column_id": col_id,
            "view_scope": view, "domain": domain, "metric": metric,
            "value": _fmt(value) if isinstance(value, float) else "",
            "value_status": status, "self_comparison": _bool_str(row_id == col_id),
            "interpretation": interp, "executed_utc": executed_utc,
        })

    ok_union = [r for r in union_inventory_rows if r.get("inventory_status") == "ok" and r.get("join_hash", "").strip()]
    project_ok_union = [r for r in ok_union if _role_key(r.get("governance_role", "")) == "project"]
    if not union_inventory_rows or not ok_union or not project_ok_union:
        if not union_inventory_rows:
            status = "blocked_missing_union_inventory"
        elif not ok_union:
            status = "blocked_no_ok_union_inventory"
        else:
            status = "blocked_no_ok_project_union_inventory"
        for filename, metric in [
            ("project_union_jaccard_matrix.csv", "union_jaccard"),
            ("project_density_similarity_matrix.csv", "density_similarity"),
        ]:
            add_matrix(filename, "unavailable", "unavailable", "unavailable", "", metric, None, status,
                       "Union-derived matrix blocked because normalized project union inventory is unavailable.")
    else:
        by_group_view: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        by_group_view_domain: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
        for r in project_ok_union:
            raw_gid = _matrix_group_id(r)
            gid = label_by_group.get(raw_gid, raw_gid)
            view = r.get("view_scope", "")
            domain = r.get("domain", "")
            jh = r.get("join_hash", "")
            by_group_view[(gid, view)].add(jh)
            by_group_view_domain[(gid, view, domain)].add(jh)
        for view in sorted({v for _, v in by_group_view}):
            ids = sorted(g for g, v in by_group_view if v == view)
            for a in ids:
                for b in ids:
                    value = 1.0 if a == b else _jaccard_sets(by_group_view[(a, view)], by_group_view[(b, view)])
                    add_matrix("project_union_jaccard_matrix.csv", a, b, view, "ALL_DOMAINS", "union_jaccard", value, "ok",
                               "Jaccard between normalized project-level join_hash unions; answers whether systems contain the same canonical patterns.")
        vectors: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(dict)
        for (gid, view, domain), jhs in by_group_view_domain.items():
            vectors[(gid, view)][domain] = float(len(jhs))
        for view in sorted({v for _, v in vectors}):
            ids = sorted(g for g, v in vectors if v == view)
            for a in ids:
                for b in ids:
                    value = 1.0 if a == b else _cosine_similarity(vectors[(a, view)], vectors[(b, view)])
                    add_matrix("project_density_similarity_matrix.csv", a, b, view, "ALL_DOMAINS", "density_similarity", value, "ok",
                               "Cosine similarity of domain pattern-density vectors; absent domains are treated as zero occupancy by definition.")
    add_manifest("project_union_jaccard_matrix.csv", "Project", "all,used", "cross_segment_union_inventory.csv", "role/client/discipline/unit/domain/view/join_hash", "union_jaccard", "normalized join_hash", "Jaccard on system-level unions", "Do these project scopes contain/use the same canonical patterns?", "Requires PR 1 union inventory; not a file-to-file similarity score.")
    add_manifest("project_density_similarity_matrix.csv", "Project", "all,used", "cross_segment_union_inventory.csv", "role/client/discipline/unit/domain/view/join_hash", "density_similarity", "domain pattern count", "Cosine similarity over domain occupancy counts", "Are domains populated to similar degrees?", "Treats absent domains as zero occupancy; does not measure exact identity overlap.")

    # Existing file-pair mean Jaccard preserved under explicit name. Domain rows
    # remain available, and an explicit ALL_DOMAINS aggregate is added so
    # fragmentation diagnostics never collapse an arbitrary domain into an
    # all-domain union comparison.
    project_summary = [r for r in summary_rows if _role_key(r.get("governance_role_a", "")) == "project" and _role_key(r.get("governance_role_b", "")) == "project"]
    file_pair_values: Dict[Tuple[str, str, str], List[Tuple[str, float]]] = defaultdict(list)
    file_pair_ids_by_view: Dict[str, Set[str]] = defaultdict(set)
    file_pair_domains_by_id_view: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for r in sorted(project_summary, key=lambda x: (
        x.get("segment_label_a") or x.get("segment_id_a", ""),
        x.get("segment_label_b") or x.get("segment_id_b", ""),
        x.get("domain", ""),
    )):
        row_id = r.get("segment_label_a") or r.get("segment_id_a", "")
        col_id = r.get("segment_label_b") or r.get("segment_id_b", "")
        for view, col in [("all", "all_pairwise_jaccard_mean"), ("used", "used_pairwise_jaccard_mean")]:
            raw = r.get(col, "")
            value = float(raw) if raw else None
            status = "ok" if raw else "unavailable"
            add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, col_id, view, r.get("domain", ""),
                       "mean_file_pair_jaccard", value, status,
                       "Mean of pairwise file Jaccard comparisons; answers whether individual files are typically similar across groups.")
            if row_id != col_id:
                add_matrix("project_mean_file_pair_jaccard_matrix.csv", col_id, row_id, view, r.get("domain", ""),
                           "mean_file_pair_jaccard", value, status,
                           "Symmetric mean file-pair Jaccard cell mirrored from the observed unordered project pair.")
            if raw:
                for a_id, b_id in [(row_id, col_id), (col_id, row_id)]:
                    file_pair_values[(a_id, b_id, view)].append((r.get("domain", ""), float(raw)))
                file_pair_ids_by_view[view].update([row_id, col_id])
                if r.get("domain", ""):
                    file_pair_domains_by_id_view[(row_id, view)].add(r.get("domain", ""))
                    file_pair_domains_by_id_view[(col_id, view)].add(r.get("domain", ""))
    for (row_id, col_id, view), values in sorted(file_pair_values.items()):
        if values:
            aggregate = sum(v for _domain, v in sorted(values)) / len(values)
            add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, col_id, view, "ALL_DOMAINS",
                       "mean_file_pair_jaccard", aggregate, "ok",
                       "Mean of domain-level mean file-pair Jaccard values; aligned to all-domain union_jaccard for diagnostics.")
    existing_pair_keys = {
        (r["row_id"], r["column_id"], r["view_scope"], r["domain"])
        for r in outputs.get("project_mean_file_pair_jaccard_matrix.csv", [])
    }
    for view, ids in sorted(file_pair_ids_by_view.items()):
        for row_id in sorted(ids):
            observed_domains = file_pair_domains_by_id_view.get((row_id, view), set())
            for domain in sorted(observed_domains | ({"ALL_DOMAINS"} if observed_domains else set())):
                key = (row_id, row_id, view, domain)
                if key in existing_pair_keys:
                    continue
                add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, row_id, view, domain,
                           "mean_file_pair_jaccard", 1.0, "synthetic_self_comparison",
                           "Synthetic deterministic self-comparison cell for square matrix pivots; not an observed file-pair comparison.")
                existing_pair_keys.add(key)
    add_manifest("project_mean_file_pair_jaccard_matrix.csv", "Project", "all,used", "cross_segment_summary.csv", "segment_pair/domain plus deterministic ALL_DOMAINS aggregate", "mean_file_pair_jaccard", "file join_hash set", "Mean of file-pair Jaccard values; ALL_DOMAINS is the mean across available domain means", "Are individual files typically similar across project groups?", "Not equivalent to union_jaccard; can diverge when file inventories are partitioned differently.")

    for r in pooled_rows:
        if _role_key(r.get("governance_role", "")) != "project":
            continue
        row_id = r.get("segment_label") or r.get("segment_id", "")
        # A project can now appear once per applicable pool_scope grain
        # (parent_sibling, bc, client — see run_pooled_comparison()). Fold
        # pool_scope into col_id so different grains for the same project
        # land on distinct matrix coordinates instead of colliding on
        # identical (row_id, col_id, view, domain) with different values.
        pool_scope = r.get("pool_scope", "") or "parent_sibling"
        col_id = f"peer_pool:{pool_scope}:{row_id}"
        for view, col in [("all", "all_containment_focal_in_pool"), ("used", "used_containment_focal_in_pool")]:
            raw = r.get(col, "")
            add_matrix("project_pool_containment_similarity_matrix.csv", row_id, col_id, view, r.get("domain", ""),
                       "pool_containment_similarity", float(raw) if raw else None, "ok" if raw else "unavailable",
                       "Focal-in-peer-pool containment; answers how much each system aligns with its peer pool.")
    add_manifest("project_pool_containment_similarity_matrix.csv", "Project", "all,used", "cross_segment_pooled.csv", "focal_segment/domain/peer_pool_scope", "pool_containment_similarity", "normalized join_hash", "Focal union contained in sibling pool union", "How much does each project system align with its peer pool?", "Peer pools derive only from existing manifest sibling grain; no new authority taxonomy is inferred. column_id encodes pool_scope (parent_sibling/bc/client) so a project's separate pool grains never share a matrix cell.")

    # Diagnostic: union footprint minus exact mean identity overlap, only when both inputs are available.
    union_index = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]): r for r in outputs.get("project_union_jaccard_matrix.csv", []) if r.get("value_status") == "ok" and r.get("domain") == "ALL_DOMAINS"}
    pair_index = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]): r for r in outputs.get("project_mean_file_pair_jaccard_matrix.csv", []) if r.get("value_status") == "ok" and r.get("domain") == "ALL_DOMAINS"}
    frag_rows: List[Dict[str, str]] = []
    for key in sorted(set(union_index) & set(pair_index)):
        u = float(union_index[key]["value"])
        p = float(pair_index[key]["value"])
        frag_rows.append({
            "matrix_name": "project_fragmentation_diagnostic.csv",
            "row_id": key[0], "column_id": key[1], "view_scope": key[2], "domain": key[3],
            "footprint_similarity": _fmt(u), "exact_identity_overlap": _fmt(p),
            "fragmentation_diagnostic": _fmt(u - p), "value_status": "diagnostic",
            "interpretation": "Diagnostic difference between union footprint similarity and mean exact file identity overlap; not a mathematically authoritative index.",
            "executed_utc": executed_utc,
        })
    if not frag_rows:
        frag_rows.append({
            "matrix_name": "project_fragmentation_diagnostic.csv", "row_id": "unavailable",
            "column_id": "unavailable", "view_scope": "unavailable", "domain": "ALL_DOMAINS",
            "footprint_similarity": "", "exact_identity_overlap": "", "fragmentation_diagnostic": "",
            "value_status": "unavailable_required_inputs", "interpretation": "Requires both union_jaccard and mean_file_pair_jaccard inputs.",
            "executed_utc": executed_utc,
        })
    add_manifest("project_fragmentation_diagnostic.csv", "Project", "all,used", "project_union_jaccard_matrix.csv + project_mean_file_pair_jaccard_matrix.csv", "matrix cell", "fragmentation_diagnostic", "normalized join_hash", "union_jaccard minus mean_file_pair_jaccard when both are available", "Highlights divergence between footprint overlap and exact per-file identity overlap.", "Diagnostic only; do not treat as an authoritative governance index.")

    for rows in outputs.values():
        rows.sort(key=lambda r: (r["matrix_name"], r["row_id"], r["column_id"], r["view_scope"], r["domain"], r["metric"]))
    manifests.sort(key=lambda r: (r["matrix_name"], r["governance_role"], r["view_scope"]))
    return dict(outputs), frag_rows, manifests

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



def build_pair_domain_work_items(
    runnable_pairs: Sequence[ComparisonPair],
    segments_root: Path,
    registry: Dict[str, Dict[str, str]],
    requested_domain: Optional[str] = None,
) -> Tuple[List[Tuple[str, str, str, str]], Dict[str, Set[str]], List[str]]:
    """Return runnable (pair × domain) work scoped to each pair's domain union.

    Domains are sparse in segmented corpora. Scheduling every pair against every
    globally active domain creates mostly-empty worker tasks and filesystem churn,
    so each pair is expanded only across domains present in either participating
    segment.
    """
    segment_ids = sorted({seg for pair in runnable_pairs for seg in (pair[0], pair[1])})
    domains_by_segment = {
        sid: discover_domains_for_segment(segments_root, registry, sid)
        for sid in segment_ids
    }

    active_domains: Set[str] = set()
    work_items: List[Tuple[str, str, str, str]] = []
    for seg_a, seg_b, ctype in runnable_pairs:
        pair_domains = domains_by_segment.get(seg_a, set()) | domains_by_segment.get(seg_b, set())
        if requested_domain:
            domains = [requested_domain] if requested_domain in pair_domains else []
        else:
            domains = sorted(pair_domains)
        active_domains.update(domains)
        for dom in domains:
            work_items.append((seg_a, seg_b, ctype, dom))

    return work_items, domains_by_segment, sorted(active_domains)


def sort_summary_rows(rows: List[Dict[str, str]]) -> None:
    rows.sort(key=lambda r: (
        r.get("comparison_type", ""),
        r.get("segment_id_a", ""),
        r.get("segment_id_b", ""),
        r.get("domain", ""),
    ))


def sort_pair_detail_rows(rows: List[Dict[str, str]]) -> None:
    rows.sort(key=lambda r: (
        r.get("_comparison_type", ""),
        r.get("segment_id_a", ""),
        r.get("segment_id_b", ""),
        r.get("domain", ""),
        r.get("project_label_a", ""),
        r.get("project_label_b", ""),
        r.get("export_run_id_a", ""),
        r.get("export_run_id_b", ""),
    ))

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# ProcessPoolExecutor raises ValueError when max_workers > 61 on Windows
# (WaitForMultipleObjects handle-count limit) — auto-detected counts must
# respect this cap there, or a default `--workers auto` run on a 64+-core
# Windows host fails outright.
_WIN32_MAX_WORKERS = 61


def resolve_worker_count(value: str, headroom: int = 2) -> int:
    """Resolve --workers, accepting either an int or the literal string 'auto'.

    'auto' derives a single-layer worker count from available logical cores
    minus headroom — this script's ProcessPoolExecutor is not nested inside
    another worker pool, so (unlike run_segment_orchestrator.py's bundle-stage
    subprocess) there is no second layer to coordinate against.
    """
    if str(value).strip().lower() == "auto":
        cpu_count = os.cpu_count()
        workers = max(1, cpu_count - headroom) if cpu_count else 4
        if sys.platform == "win32":
            workers = min(workers, _WIN32_MAX_WORKERS)
        return workers
    return int(value)


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
    ap.add_argument("--cross-client", action="store_true",
                    help="Mode F: each client's client-level pooled Project vocabulary vs. every other client's, same unit_system")

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
    ap.add_argument("--workers", default="auto",
                    help="Max parallel pair×domain workers, or 'auto' to derive from "
                         "CPU count (default: auto)")

    args = ap.parse_args()
    args.workers = resolve_worker_count(args.workers)

    segments_root = Path(args.segments_root).resolve()
    records_dir = Path(args.records_dir).resolve()
    out_dir = Path(args.out_dir).resolve()

    # Default: all modes if none specified
    any_mode = any([
        args.within_segment, args.sibling_segments, args.parent_siblings,
        args.within_project, args.governance_chain, args.cross_client,
    ])
    if not any_mode:
        args.within_segment = args.sibling_segments = args.parent_siblings = True
        args.within_project = args.governance_chain = args.cross_client = True

    manifest = load_manifest(records_dir)
    registry = load_registry(records_dir)
    file_metadata = load_file_metadata(records_dir)
    membership = load_membership(records_dir)

    stale_ancestor_warnings = detect_stale_ancestor_encoding(manifest)
    if stale_ancestor_warnings:
        print(
            f"[warn] {len(stale_ancestor_warnings)} segment(s) look like they carry "
            f"pre-D-028 ancestor_segment_ids data -- structural_ancestor lineage "
            f"exclusion may be incomplete until segment_manifest.csv is regenerated:",
            file=sys.stderr,
        )
        for w in stale_ancestor_warnings[:20]:
            print(f"[warn]   {w}", file=sys.stderr)
    if membership:
        membership_errors = validate_membership_against_manifest(manifest, membership)
        if membership_errors:
            print(
                f"[warn] segment_membership.csv disagrees with segment_manifest.csv for "
                f"{len(membership_errors)} segment(s) -- population_containment disabled "
                f"for this run (structural_ancestor guard still applies). Re-run "
                f"build_segment_manifest.py to regenerate a consistent set:",
                file=sys.stderr,
            )
            for err in membership_errors:
                print(f"[warn]   {err}", file=sys.stderr)
            membership = {}

    # structural_ancestor / population_containment (D-027): computed once up
    # front and threaded into discover_sibling_segments() below. ancestor_map
    # is cheap and always available (derived purely from manifest); the
    # containment_map additionally needs real population data (membership) --
    # when segment_membership.csv is absent (or fails validation above),
    # containment_map stays None and discover_sibling_segments() falls back
    # to the structural guard alone.
    ancestor_map = _build_ancestor_map(manifest)
    containment_map: Optional[Dict[str, Set[str]]] = None
    if membership:
        containment_thresholds = _compute_containment_thresholds(manifest, membership, ancestor_map)
        containment_map = _population_containment_map(manifest, membership, containment_thresholds)
        if not args.dry_run:
            thresholds_path = write_population_containment_thresholds(out_dir, containment_thresholds)
            print(f"[compare] population_containment thresholds written to {thresholds_path}")

    # Discover pairs
    pairs: List[ComparisonPair] = []
    if args.within_segment:
        pairs.extend(discover_within_segment(manifest))
    if args.sibling_segments:
        pairs.extend(discover_sibling_segments(manifest, ancestor_map, containment_map))
    if args.parent_siblings:
        pairs.extend(discover_parent_siblings(manifest))
    if args.governance_chain:
        pairs.extend(discover_governance_chain(manifest))
    if args.within_project:
        pairs.extend(discover_within_project(manifest, registry, file_metadata, segments_root))
    if args.cross_client:
        pairs.extend(discover_cross_client(manifest))
        pairs.extend(discover_client_cross_bc(manifest))

    pairs = deduplicate_pairs(pairs)

    # Filter by --segment-a / --segment-b. Must run BEFORE
    # drop_legacy_siblings_covered_by_peer_comparisons(): that drop is
    # order-independent (frozenset((a, b))), but discover_sibling_segments()
    # orders its pairs by sorted segment ID while discover_cross_client() orders
    # by sorted client label (bc_to_bc/discover_client_cross_bc() both order by
    # sorted segment ID too, matching sibling's own convention) -- the surviving
    # cross_client row can therefore be the reverse (b, a) of the sibling_projects
    # row it replaces. Since these filters are position-sensitive
    # (a == args.segment_a, b == args.segment_b), running the drop first could
    # remove the correctly-oriented sibling row and leave only a
    # reversed-orientation peer row that then fails the filter too, making a
    # scoped run silently report zero pairs for segments that do have a
    # comparison. Filtering here first means the drop only ever sees (and only
    # ever needs to reconcile) whichever orientation actually survived the
    # requested scope.
    if args.segment_a:
        pairs = [(a, b, ct) for a, b, ct in pairs if a == args.segment_a]
    if args.segment_b:
        pairs = [(a, b, ct) for a, b, ct in pairs if b == args.segment_b]

    pairs = drop_legacy_siblings_covered_by_peer_comparisons(pairs)

    if not pairs:
        print("[compare] no pairs discovered — check manifest hierarchy and mode flags")

    runnable_pairs = [
        (seg_a, seg_b, ctype)
        for seg_a, seg_b, ctype in pairs
        if segment_is_runnable(registry, seg_a)
        and (seg_a == seg_b or segment_is_runnable(registry, seg_b))
    ]

    # Build flat work list: one item per (pair × domain), limited to domains
    # present in either side of the pair so sparse corpora do not generate a
    # global-domain cross product of mostly-empty worker tasks. Computed
    # before the --dry-run branch so the preview reflects exactly the
    # (pair, domain) granularity a live run would recompute — including a
    # --domain filter, which only ever touches one domain per pair.
    work_items, _domains_by_segment, active_domain_filter = build_pair_domain_work_items(
        runnable_pairs, segments_root, registry, args.domain
    )

    # --dry-run: print table and exit
    if args.dry_run:
        comparison_registry = load_comparison_registry(out_dir)
        col_w = 36
        print(f"{'segment_a':<{col_w}}  {'segment_b':<{col_w}}  {'comparison_type':<28}  {'domain':<24}  {'staleness':<10}")
        print("-" * (col_w * 2 + 68))
        n_stale = 0
        for a, b, ctype, dom in work_items:
            la = manifest.get(a, {}).get("segment_label", a)
            lb = manifest.get(b, {}).get("segment_label", b)
            stale = comparison_is_stale(a, b, ctype, dom, registry, comparison_registry)
            if stale:
                n_stale += 1
            staleness_label = "stale" if stale else "current"
            print(f"{la:<{col_w}}  {lb:<{col_w}}  {ctype:<28}  {dom:<24}  {staleness_label:<10}")
        print(
            f"\n[compare] {len(runnable_pairs)} pairs, {len(work_items)} pair-domain work items "
            f"({n_stale} stale, {len(work_items) - n_stale} current)"
        )
        return 0

    # Run comparisons
    executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_rows: List[Dict[str, str]] = []
    delta_rows: List[Dict[str, str]] = []
    governance_state_rows: List[Dict[str, str]] = []
    governance_state_summary_rows: List[Dict[str, str]] = []
    governance_combo_count = 0
    delta_combo_count = 0

    if args.workers < 1:
        sys.exit("[error] --workers must be >= 1")

    print(
        f"[compare] {len(runnable_pairs)} pairs × {len(active_domain_filter)} active domains = "
        f"{len(work_items)} pair-domain work items  workers={args.workers}"
    )

    n_complete = 0
    n_skipped = 0
    completed_work_items: List[Tuple[str, str, str, str]] = []

    # cross_segment_file_pairs.csv rows are streamed to a temp file as work items
    # complete rather than accumulated in memory — one row per matched file pair
    # within a domain comparison, easily millions of rows for a large corpus, which
    # was the dominant driver of multi-GB peak memory when held in a single Python
    # list for the whole run. Streamed rows land in worker-completion order rather
    # than the fully sorted order sort_pair_detail_rows() used to produce; nothing
    # downstream depends on that ordering. The temp file is only published (atomic
    # rename) later, in the "Write outputs" section below, alongside the other
    # outputs — not here — so a failure in the pooled/union/reuse/matrix steps
    # between here and there leaves the previous run's file untouched rather than
    # publishing a new pairs file paired with stale companion outputs.
    out_dir.mkdir(parents=True, exist_ok=True)
    pair_detail_tmp = NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False,
        dir=str(out_dir), suffix=".tmp",
    )
    pair_detail_writer = csv.DictWriter(pair_detail_tmp, fieldnames=PAIRS_FIELDS)
    pair_detail_writer.writeheader()
    pair_detail_row_count = 0
    pair_detail_tmp_path = Path(pair_detail_tmp.name)

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
                for pair_row in pairs_out:
                    pair_row["_comparison_type"] = ctype
                    pair_detail_writer.writerow(
                        {name: pair_row.get(name, "") for name in PAIRS_FIELDS}
                    )
                pair_detail_row_count += len(pairs_out)
                n_complete += 1
                n_p = result.get("n_pairs", "?")
                print(
                    f"[compare] segment_a={seg_a} segment_b={seg_b} "
                    f"domain={domain} pairs={n_p}"
                )

                # Delta pattern output — directed pairs only, opt-out via --no-delta.
                # Delta generation remains in the parent process so worker results stay
                # limited to the existing (summary_row, detail_rows) contract.
                #
                # comparison_status == "blocked" (zero readable files on the
                # reference side, or the target side) must be excluded here:
                # an empty ref_union would make every target join_hash look
                # like it's outside the reference, i.e. tgt_union - ref_union
                # == tgt_union -- every target pattern gets misreported as
                # locally-invented drift instead of "reference unknown, not
                # locally drifted." A blocked row still exists in
                # cross_segment_summary.csv (so the block itself is visible),
                # it just can't source a trustworthy delta.
                if (
                    not args.no_delta
                    and ctype in DELTA_DIRECTED_TYPES
                    and result.get("comparison_status") != "blocked"
                ):
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

            produced_output = result is not None

            # Governance-state output is independent of legacy run_pair() summary
            # thresholds. Sparse or empty targets still need provided_but_missing
            # rows so missing downstream stock is visible.
            if ctype in GOVERNANCE_STATE_DIRECTED_TYPES:
                crid = (
                    result.get("comparison_run_id", "")
                    if result is not None
                    else make_comparison_run_id(seg_a, seg_b, executed_utc, ctype)
                )
                state_rows, state_summary = build_governance_state_outputs(
                    crid=crid,
                    seg_ref=seg_a,
                    seg_tgt=seg_b,
                    comparison_type=ctype,
                    domain=domain,
                    manifest=manifest,
                    registry=registry,
                    segments_root=segments_root,
                    executed_utc=executed_utc,
                )
                if state_rows:
                    governance_state_rows.extend(state_rows)
                    governance_state_summary_rows.append(state_summary)
                    governance_combo_count += 1
                    produced_output = True

            # comparison_registry.csv must only stamp (pair, domain) work items
            # that actually produced a persisted output row somewhere this run
            # (cross_segment_summary.csv via `result`, or governance-state
            # output) — a domain below --min-patterns or a within-project pair
            # with no eligible file pairs must not get a fresh "current" stamp
            # for output that was never written.
            if produced_output:
                completed_work_items.append((seg_a, seg_b, ctype, domain))

            done = n_complete + n_skipped
            if done % 50 == 0 or done == len(work_items):
                print(
                    f"[compare] progress: {done}/{len(work_items)} "
                    f"complete={n_complete} skipped={n_skipped}",
                    flush=True,
                )

    elapsed = time.perf_counter() - t0
    print(
        f"[compare] done  pairs={len(runnable_pairs)}  active_domains={len(active_domain_filter)}  "
        f"work_items={len(work_items)}  complete={n_complete}  skipped={n_skipped}  "
        f"elapsed={elapsed:.1f}s  ({elapsed/60:.1f} min)",
        flush=True,
    )

    # Rows for cross_segment_file_pairs.csv are fully streamed to the temp file at
    # this point, but the rename into place is deferred to the "Write outputs"
    # section below (after pooled/union/reuse/matrix computation and the registry
    # write) rather than published here. Publishing immediately would let this one
    # file jump ahead to reflect the new run while comparison_registry.csv and the
    # other outputs still reflect the old run, if any later step below raises —
    # breaking the previous all-or-nothing guarantee across the output set.
    pair_detail_tmp.close()

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

    union_inventory_rows = build_union_inventory_rows(
        manifest, registry, file_metadata, segments_root, executed_utc,
        domain_filter=args.domain,
    )
    reuse_distribution_rows = build_pattern_reuse_distribution_rows(
        union_inventory_rows, executed_utc
    )
    reuse_summary_by_domain_rows = build_pattern_reuse_summary_rows(
        reuse_distribution_rows, by_client=False
    )
    reuse_summary_by_client_rows = build_pattern_reuse_summary_rows(
        reuse_distribution_rows, by_client=True
    )
    matrix_outputs, fragmentation_rows, matrix_manifest_rows = build_explicit_matrix_outputs(
        summary_rows, pooled_rows, union_inventory_rows, executed_utc
    )

    # Write outputs
    if summary_rows:
        sort_summary_rows(summary_rows)
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(out_dir / "cross_segment_summary.csv", SUMMARY_FIELDS, summary_rows)
        print(f"[compare] wrote {len(summary_rows)} rows → {out_dir / 'cross_segment_summary.csv'}")

    # Publish the streamed cross_segment_file_pairs.csv here, alongside the other
    # outputs, so a failure anywhere above (pooled/union/reuse/matrix computation)
    # leaves the previous run's file untouched instead of a fresh pairs file paired
    # with stale companions. Rows are in worker-completion order, not the fully
    # sorted order sort_pair_detail_rows() used to produce — confirmed with the
    # requester that nothing downstream depends on that ordering.
    if pair_detail_row_count:
        pair_detail_tmp_path.replace(out_dir / "cross_segment_file_pairs.csv")
        print(
            f"[compare] wrote {pair_detail_row_count} rows (streamed, unsorted) → "
            f"{out_dir / 'cross_segment_file_pairs.csv'}"
        )
    else:
        pair_detail_tmp_path.unlink(missing_ok=True)

    if governance_state_rows:
        governance_state_rows.sort(key=lambda r: (
            r["comparison_type"],
            r["segment_id_reference"],
            r["segment_id_target"],
            r["domain"],
            r["state"],
            r["join_hash"],
        ))
        governance_state_summary_rows.sort(key=lambda r: (
            r["comparison_type"],
            r["segment_id_reference"],
            r["segment_id_target"],
            r["domain"],
        ))
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(
            out_dir / "cross_segment_governance_states.csv",
            GOVERNANCE_STATE_FIELDS,
            governance_state_rows,
        )
        atomic_write_csv(
            out_dir / "cross_segment_governance_state_summary.csv",
            GOVERNANCE_STATE_SUMMARY_FIELDS,
            governance_state_summary_rows,
        )
        print(
            f"[compare] governance states written: {len(governance_state_rows)} rows across "
            f"{governance_combo_count} domain/pair combinations"
        )
        print(
            f"[compare] wrote {len(governance_state_rows)} rows → "
            f"{out_dir / 'cross_segment_governance_states.csv'}"
        )
        print(
            f"[compare] wrote {len(governance_state_summary_rows)} rows → "
            f"{out_dir / 'cross_segment_governance_state_summary.csv'}"
        )

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

    if union_inventory_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(
            out_dir / "cross_segment_union_inventory.csv",
            UNION_INVENTORY_FIELDS,
            union_inventory_rows,
        )
        print(
            f"[compare] wrote {len(union_inventory_rows)} rows → "
            f"{out_dir / 'cross_segment_union_inventory.csv'}"
        )

    if reuse_distribution_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(
            out_dir / "pattern_reuse_distribution.csv",
            REUSE_DISTRIBUTION_FIELDS,
            reuse_distribution_rows,
        )
        atomic_write_csv(
            out_dir / "pattern_reuse_summary_by_domain.csv",
            REUSE_SUMMARY_FIELDS,
            reuse_summary_by_domain_rows,
        )
        atomic_write_csv(
            out_dir / "pattern_reuse_summary_by_client.csv",
            REUSE_SUMMARY_FIELDS,
            reuse_summary_by_client_rows,
        )
        print(
            f"[compare] wrote {len(reuse_distribution_rows)} rows → "
            f"{out_dir / 'pattern_reuse_distribution.csv'}"
        )

    if matrix_outputs or fragmentation_rows or matrix_manifest_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        for filename, rows in sorted(matrix_outputs.items()):
            atomic_write_csv(out_dir / filename, MATRIX_OUTPUT_FIELDS, rows)
            print(f"[compare] wrote {len(rows)} rows → {out_dir / filename}")
        atomic_write_csv(
            out_dir / "project_fragmentation_diagnostic.csv",
            FRAGMENTATION_DIAGNOSTIC_FIELDS,
            fragmentation_rows,
        )
        atomic_write_csv(
            out_dir / "matrix_output_manifest.csv",
            MATRIX_MANIFEST_FIELDS,
            matrix_manifest_rows,
        )
        print(f"[compare] wrote {len(matrix_manifest_rows)} rows → {out_dir / 'matrix_output_manifest.csv'}")

    if not summary_rows and not pooled_rows and not governance_state_rows and not union_inventory_rows:
        print("[compare] no comparison rows produced — check segment data and min-patterns threshold")

    comparison_registry_rows = build_comparison_registry_rows(
        completed_work_items, registry, executed_utc
    )
    atomic_write_csv(out_dir / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS, comparison_registry_rows)
    print(f"[compare] wrote {len(comparison_registry_rows)} rows → {out_dir / 'comparison_registry.csv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
