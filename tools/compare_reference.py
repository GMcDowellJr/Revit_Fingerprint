#!/usr/bin/env python3
"""tools/compare_reference.py

Compare a reference fingerprint export against either one target export or
an entire already-materialized segment, using the segment-local outputs
tools/run_segment_orchestrator.py already produced for that segment. The
reference (--reference-segment) and the target (--target-segment) may be
resolved against the *same* segment or two *different* segments -- both
modes are supported; --target-segment defaults to --reference-segment.

This module implements NO comparison mathematics of its own. It is a thin
orchestration/output layer over the existing, authoritative comparison
implementation, tools/bundle_analysis/step_compare.py::run_compare_for_domain
(and its ok/degraded/blocked reliability semantics in
tools/bundle_analysis/comparison_status.py), called in-process -- there
remains exactly one implementation of comparison semantics. That
implementation is itself segment-agnostic (it only ever reads the
analysis_dir/bundle_dir paths and the portable `reference` dict it is
given), so cross-segment comparison requires no change to it -- only to how
this module resolves its two sides.

Unlike the tool's previous incarnation, this version does NOT read, stage,
flatten, or otherwise process fingerprint export JSON, and does NOT invoke
tools/run_extract_all.py or any other extraction/pattern/join-key
subprocess. A reference or target is selected by an export filename, but
that filename is only ever matched against its own segment's
already-produced results/records/file_metadata.csv -- the JSON files
themselves are never opened. If either requested segment's materialization
is missing, incomplete, or a filename cannot be resolved unambiguously, the
comparison blocks explicitly rather than falling back to any JSON-driven
path. When the two segments differ, several additional cross-segment gates
apply before any domain is compared: a whole-run unit_system check
(CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH) and extractor_schema_version check
(CROSS_SEGMENT_SCHEMA_MISMATCH), then a per-domain join-key-provenance gate
(CROSS_SEGMENT_JOIN_POLICY_MISMATCH, see resolve_cross_segment_compatibility())
and a per-domain pattern-identity resolution gate
(CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED, see
resolve_cross_segment_pattern_identity()). That last gate matters because
`pattern_id` values are segment-local (assigned independently by each
segment's own patterns stage) -- cross-segment mode translates both sides to
the cross-segment-stable `join_hash` identity (via each segment's own
domain_patterns.csv, the same resolution tools/compare_cross_segment.py
already uses) before any shared/reference_only/target_only classification,
rather than comparing raw pattern_id values that happen to look alike.

See docs/reference_comparison_tool.md for the full runbook, CLI reference,
and output-field documentation, including the neutral-terminology note: a
"reference" here is a comparison anchor only, not a standard, approved
content, or a compliance requirement.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from bundle_analysis.common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id  # noqa: E402
from bundle_analysis.comparison_status import (  # noqa: E402
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_DEGRADED,
    COMPARISON_STATUS_BLOCKED,
    aggregate_comparison_status,
    split_reason_codes,
)
from bundle_analysis.step_compare import run_compare_for_domain, write_blocked_gap_placeholder  # noqa: E402
from core.name_key_coverage import ELIGIBLE_DOMAINS, exclusion_reason  # noqa: E402
from name_key_rollup import build_domain_name_hash_facets  # noqa: E402
from name_config_collision import classify_name_config_collisions  # noqa: E402

MANIFEST_FILENAME = "reference_comparison_report.json"
SUMMARY_FILENAME = "reference_comparison_summary.csv"
DETAIL_FILENAME = "reference_comparison_detail.csv"
DIAGNOSTICS_FILENAME = "reference_comparison_diagnostics.json"
SEMANTIC_CHANGES_FILENAME = "reference_comparison_semantic_changes.csv"
NAME_OVERLAP_FILENAME = "reference_comparison_name_overlap.csv"
NAME_OVERLAP_NAMES_FILENAME = "reference_comparison_name_overlap_names.csv"
NAME_CONFIG_COLLISION_FILENAME = "reference_comparison_name_config_collisions.csv"
NAME_CONFIG_COLLISION_CONFIGS_FILENAME = "reference_comparison_name_config_collisions_configs.csv"

VALID_PURGE_VIEWS = ("all", "used")

# Pre-flight reason codes owned by this tool -- distinct from, and never
# duplicating, tools/bundle_analysis/comparison_status.py's REASON_* set
# (those classify a domain's own comparison outcome; these classify why a
# comparison could not even be attempted for a requested selector/segment).
REASON_REFERENCE_NOT_MATERIALIZED = "REFERENCE_NOT_MATERIALIZED"
REASON_TARGET_NOT_MATERIALIZED = "TARGET_NOT_MATERIALIZED"
REASON_REFERENCE_AMBIGUOUS = "REFERENCE_AMBIGUOUS"
REASON_TARGET_AMBIGUOUS = "TARGET_AMBIGUOUS"
REASON_SEGMENT_NOT_FOUND = "SEGMENT_NOT_FOUND"
REASON_SEGMENT_MATERIALIZATION_INCOMPLETE = "SEGMENT_MATERIALIZATION_INCOMPLETE"
REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING = "REQUIRED_ANALYSIS_ARTIFACT_MISSING"
REASON_MATERIALIZATION_VERSION_INCOMPATIBLE = "MATERIALIZATION_VERSION_INCOMPATIBLE"
REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN = "MATERIALIZATION_COMPATIBILITY_UNPROVEN"
# Distinct from MATERIALIZATION_VERSION_INCOMPATIBLE/_UNPROVEN, which check
# internal consistency *within* one segment's own records.csv: this fires
# only when --reference-segment != --target-segment, and blocks a domain
# whose (join_key_schema, join_key_policy_id, join_key_policy_version) tuple
# either isn't "ok" on one side or doesn't agree between the two segments --
# comparing pattern_id sets across incompatible join policies is meaningless.
REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH = "CROSS_SEGMENT_JOIN_POLICY_MISMATCH"
REASON_NO_COMPARISON_TARGETS = "NO_COMPARISON_TARGETS"
REASON_REFERENCE_HAS_NO_PATTERNS = "REFERENCE_HAS_NO_PATTERNS"
# Only checked/reachable in cross-segment mode (reference_segment !=
# target_segment); skipped entirely in same-segment mode, preserving byte-
# identical same-segment output. Mirrors
# tools/bundle_analysis/reference_bundle.py::load_and_validate's own
# sidecar-vs-current extractor_schema_version rejection (lines ~181-186) --
# without this, two segments materialized under different extractor schema
# versions that happen to share the same join-key tuple would pass the
# CROSS_SEGMENT_JOIN_POLICY_MISMATCH gate and report "ok" comparison metrics
# across pattern evidence that isn't actually comparable (Codex review, PR #471).
REASON_CROSS_SEGMENT_SCHEMA_MISMATCH = "CROSS_SEGMENT_SCHEMA_MISMATCH"
# Only checked/reachable in cross-segment mode. docs/cross_segment_comparison.md
# ("No cross-unit-system pairs"): join_hashes for the same logical pattern
# differ between unit systems because behavioral hashes include unit-bearing
# values, so imperial/metric segments are never compared by the existing
# authoritative cross-segment tool (tools/compare_cross_segment.py) either
# (Codex review, PR #471).
REASON_CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH = "CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH"
# Only checked/reachable in cross-segment mode, and only for a domain that
# already passed the CROSS_SEGMENT_JOIN_POLICY_MISMATCH gate above. pattern_id
# values are segment-local identifiers assigned independently by each
# segment's own patterns stage -- they are NOT stable across segments and
# must never be compared directly (docs/cross_segment_comparison.md section 4,
# "join_hash Resolution"). The authoritative cross-segment tool
# (tools/compare_cross_segment.py::resolve_join_hashes) resolves pattern_id ->
# join_hash per (segment, domain) via that segment's own domain_patterns.csv
# (join_hash = source_cluster_id.split("|")[-1]) before any set operation;
# resolve_cross_segment_pattern_identity() below does the same translation
# here. This reason code fires when that translation cannot be completed for
# every pattern_id actually in play for a domain -- domain_patterns.csv is
# missing/has zero rows for the domain, or at least one relevant pattern_id
# has a blank source_cluster_id -- rather than silently dropping the
# unresolvable pattern_id(s) and comparing an understated set (Codex review,
# PR #471).
REASON_CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED = "CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED"
REASON_STALE_MEMBERSHIP_MATRIX = "STALE_MEMBERSHIP_MATRIX"
# Pure pre-flight/out-dir-safety failures -- raised before --out-dir can be
# safely prepared at all, so no diagnostics file is ever written for these.
REASON_OUT_DIR_UNSAFE = "OUT_DIR_UNSAFE"
# export_run_id is documented (tools/build_segment_manifest.py's own
# docstring, line ~311) as a unique join key into file_metadata.csv, and
# resolve_export_run_id() above already depends on that for --reference/
# --target selector resolution. build_file_metadata_label_lookup() below
# re-verifies uniqueness itself rather than trusting the doc comment --
# two rows sharing an export_run_id with disagreeing label values would
# otherwise be silently resolved by picking one arbitrarily, which is
# exactly the kind of silent partial join this tool's fail-loudly
# convention forbids.
REASON_FILE_METADATA_LABEL_JOIN_AMBIGUOUS = "FILE_METADATA_LABEL_JOIN_AMBIGUOUS"

# Governance/organizational label columns carried straight through from
# file_metadata.csv onto every summary/detail/semantic-changes/name-overlap
# output row (once for the reference file, once for the target file), so
# Jon's Power BI model can slice comparison results by client, discipline,
# BC, etc. without a separate manual join step. file_metadata.csv is the
# canonical source for these values elsewhere in the pipeline (see
# tools/build_segment_manifest.py's REQUIRED_ROW_FIELDS/MANIFEST_FIELDNAMES);
# this only reads columns that already exist there, it defines none of them.
_FILE_METADATA_LABEL_FIELDS = [
    "governance_role",
    "client_label",
    "discipline_label",
    "business_center_label",
    "collection_label",
    "project_label",
]


def _file_metadata_label_fieldnames(prefix: str) -> List[str]:
    return [f"{prefix}_{field}" for field in _FILE_METADATA_LABEL_FIELDS]
# --- Name-set-overlap classification (Step 1 Part B, default on; --no-name-overlap opts out) ---
#
# Unlike compute_semantic_changes_rows() above (same-segment-only, string-matched on
# pattern_label_human), this classifies the relationship between the reference side's and
# target side's *name-key join_hash sets* for each pattern identity -- sound in both
# same-segment and cross-segment mode, since it never depends on a single "the" name for a
# pattern (docs/namekey_crosssegment_step0_findings.md's Step 1 premise: 44% of arrowheads
# patterns and 25% of text_types patterns in a real corpus segment have more than one
# distinct name-hash -- a pattern's name identity is a SET, never a scalar).
NAME_SETS_IDENTICAL = "name_sets_identical"
NAME_SETS_OVERLAP = "name_sets_overlap"
NAME_SETS_DISJOINT = "name_sets_disjoint"
NAME_EVIDENCE_EXCLUDED = "name_evidence_excluded"
NAME_EVIDENCE_MISSING = "name_evidence_missing"

NAME_KEY_STATUS_OK = "ok"
NAME_KEY_STATUS_NOT_MATERIALIZED = "not_materialized"
NAME_KEY_STATUS_STALE = "stale"

# KNOWN GAP (Step 1 Part B design decision B4, do not silently resolve without discussion):
# unlike the config join_hash, which has BOTH a join_key_policy_version column (records.csv)
# AND a CROSS_SEGMENT_JOIN_POLICY_MISMATCH gate (resolve_cross_segment_compatibility() above)
# to detect two segments compared under different join-key policies, the name-key hash has
# NEITHER today (docs/namekey_crosssegment_step0_findings.md D.9:
# policies/domain_name_key_policies.json carries no version field, and
# tools/apply_name_key_policy.py's output has no policy-id/version column to compare). This
# is safe in practice only because core/record_v2.py::canonicalize_str() is a pure function
# with no segment-specific state (Step 0 D.10) -- so today's cross-segment name-hash
# comparisons ARE sound -- but there is no structural protection against a future
# domain_name_key_policies.json edit silently producing incomparable hashes between segments
# extracted before/after that edit, the way CROSS_SEGMENT_JOIN_POLICY_MISMATCH already
# protects the config side. Adding an equivalent CROSS_SEGMENT_NAME_KEY_POLICY_MISMATCH gate
# is deferred to a follow-up PR, not silently assumed away here.
REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED = "NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED"

_SUMMARY_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    *_file_metadata_label_fieldnames("reference"),
    "analysis_run_id",
    "target_export_run_id",
    *_file_metadata_label_fieldnames("target"),
    "domain",
    "population_id",
    "comparison_status",
    "comparison_reason_codes",
    "reference_pattern_count",
    "target_pattern_count",
    "shared_count",
    "reference_only_count",
    "target_only_count",
    "union_count",
    "reference_coverage_pct",
    "jaccard",
]

_DETAIL_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    *_file_metadata_label_fieldnames("reference"),
    "analysis_run_id",
    "target_export_run_id",
    *_file_metadata_label_fieldnames("target"),
    "domain",
    "population_id",
    "pattern_id",
    "comparison_class",
    "reference_revit_name",
    "reference_revit_name_status",
    "reference_revit_name_count",
    "target_revit_name",
    "target_revit_name_status",
    "target_revit_name_count",
]

REVIT_NAME_STATUS_OK = "ok"
REVIT_NAME_STATUS_MISSING = "missing"
REVIT_NAME_STATUS_UNREADABLE = "unreadable"
REVIT_NAME_STATUS_AMBIGUOUS = "ambiguous"
REVIT_NAME_STATUS_BLOCKED = "blocked"
_USABLE_LABEL_QUALITIES = {"human", "system"}
_UNREADABLE_LABEL_QUALITIES = {"placeholder_unreadable", "placeholder_unsupported"}

_SEMANTIC_CHANGES_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    *_file_metadata_label_fieldnames("reference"),
    "analysis_run_id",
    "target_export_run_id",
    *_file_metadata_label_fieldnames("target"),
    "domain",
    "population_id",
    "pattern_name",
    "reference_pattern_id",
    "target_pattern_id",
    "semantic_change_class",
    "name_match_basis",
    "reference_revit_name",
    "reference_revit_name_status",
    "target_revit_name",
    "target_revit_name_status",
]

_NAME_OVERLAP_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    *_file_metadata_label_fieldnames("reference"),
    "analysis_run_id",
    "target_export_run_id",
    *_file_metadata_label_fieldnames("target"),
    "domain",
    "population_id",
    "pattern_id",
    "comparison_class",
    "name_set_classification",
    "exclusion_reason",
    "reference_name_key_status",
    "target_name_key_status",
    "reference_name_hash_count",
    "target_name_hash_count",
    "shared_name_hash_count",
]

# Sidecar to _NAME_OVERLAP_FIELDNAMES above: one row per (pattern, side, name_hash), never a
# pipe-joined list column. A pattern can carry thousands of distinct name-hashes (real corpus
# data: max 5904 for one line_patterns pattern) -- a single CSV cell holding that many
# pipe-joined values overflows Excel's per-cell limit and corrupts the surrounding rows on
# import. Mirrors pattern_name_fragmentation.csv's one-row-per-value design (A1).
_NAME_OVERLAP_NAMES_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    "analysis_run_id",
    "target_export_run_id",
    "domain",
    "population_id",
    "pattern_id",
    "side",
    "name_hash",
]

# --- Name-config-collision classification (--include-name-config-collisions) ---------------
#
# Inverse question to the name-overlap classification above: "given the same name, does the
# config agree?" (tools/name_config_collision.py::classify_name_config_collisions()). Row
# grain here is (domain, name_hash) directly -- NOT (purge_view, population_id, pattern_id) --
# since this classifier enumerates every name-key join_hash observed on either side rather
# than reclassifying all_detail_rows, and the hazard it detects (the same name resolving to
# disjoint configs) is inherently view-independent. Mirrors _NAME_OVERLAP_FIELDNAMES's
# file-metadata-label placement exactly; mirrors _NAME_OVERLAP_NAMES_FIELDNAMES's
# hash-values-only-in-the-sidecar design for the same Excel-cell-limit reason (PR #476).
_NAME_CONFIG_COLLISION_FIELDNAMES = [
    "domain",
    "reference_export_run_id",
    *_file_metadata_label_fieldnames("reference"),
    "target_export_run_id",
    *_file_metadata_label_fieldnames("target"),
    "name_hash",
    "representative_label",
    "name_config_classification",
    "exclusion_reason",
    "reference_name_key_status",
    "target_name_key_status",
    "reference_config_hash_count",
    "target_config_hash_count",
    "shared_config_hash_count",
]

_NAME_CONFIG_COLLISION_CONFIGS_FIELDNAMES = [
    "domain",
    "reference_export_run_id",
    "target_export_run_id",
    "name_hash",
    "side",
    "config_hash",
]

# Never used as a match key: fallback labels are templated placeholders
# (e.g. "Line Pattern (Variant 2 of 5)"), not observed values -- see
# tools/label_synthesis/label_resolver.py::resolve_pattern_label.
_FALLBACK_PATTERN_LABEL_SOURCE = "fallback"
_NAME_MATCH_BASIS_PATTERN_LABEL_HUMAN = "pattern_label_human"

# Suffixes checked most-specific first, matching tools/extractor.py's own
# split-export/file-format convention, so a filename selector like
# "model.details.json" resolves to the same export identity as
# "model.index.json" (the canonical export_run_id whenever both exist).
# Deliberately excludes ".legacy.json": legacy exports are never picked up
# implicitly anywhere else in this project (tools/extractor.py's own
# discovery never registers one as a real export_run_id), so a selector
# ending in ".legacy.json" must resolve through an exact match only --
# never fall back to matching a differently-suffixed file's stem (Codex
# review, PR #467).
_EXPORT_SUFFIXES = (".details.json", ".index.json", "__fingerprint.json", ".json")


class CompareReferenceError(RuntimeError):
    """A comparison could not be attempted or completed for a stated,
    explicit reason. `reason_code` is one of this module's REASON_* /
    tools/bundle_analysis/comparison_status.py's REASON_* constants --
    never a guess, never silently downgraded to a fallback path.
    """

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


# ---------------------------------------------------------------------------
# Filename -> export_run_id resolution (segment-local file_metadata.csv only)
# ---------------------------------------------------------------------------


def _export_stem(name: str) -> str:
    for suffix in _EXPORT_SUFFIXES:
        if name.lower().endswith(suffix.lower()):
            return name[: -len(suffix)]
    return Path(name).stem


def resolve_export_run_id(
    label: str,
    selector: str,
    file_metadata_rows: Sequence[Dict[str, str]],
    not_found_code: str,
    ambiguous_code: str,
) -> str:
    """Resolve a user-supplied filename selector to exactly one
    export_run_id from a segment's own file_metadata.csv. Zero matches ->
    not_found_code; more than one match (even after stem-normalization) ->
    ambiguous_code. Never picks a first/arbitrary match.
    """
    ids = [(r.get("export_run_id", "") or "").strip() for r in file_metadata_rows]
    ids = [eid for eid in ids if eid]

    exact = sorted({eid for eid in ids if eid == selector})
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise CompareReferenceError(
            ambiguous_code,
            f"{label} {selector!r} matches more than one materialized export_run_id: {exact}",
        )

    selector_stem = _export_stem(selector)
    candidates = sorted({eid for eid in ids if _export_stem(eid) == selector_stem})
    if not candidates:
        raise CompareReferenceError(
            not_found_code,
            f"{label} {selector!r} does not resolve to any materialized export in this segment's file_metadata.csv.",
        )
    if len(candidates) > 1:
        raise CompareReferenceError(
            ambiguous_code,
            f"{label} {selector!r} resolves ambiguously to more than one export_run_id: {candidates}.",
        )
    return candidates[0]


# ---------------------------------------------------------------------------
# Segment resolution / materialization-completeness gate
# ---------------------------------------------------------------------------


def resolve_segment(registry_file: Path, segments_root: Path, segment_folder: str) -> Tuple[Path, str, str]:
    """Resolve `segment_folder` to its materialized segment_root, using
    run_registry.csv (the authoritative per-segment completion signal --
    see tools/run_segment_orchestrator.py's own already_satisfied logic,
    which is exactly `status == "complete"`). Never treats file existence
    alone as proof of completeness.

    Matched against run_registry.csv's own `output_folder` column -- the
    normalized, filesystem-safe folder name (e.g.
    "imperial_template_architectural") that
    tools/build_segment_manifest.py::_sanitize_folder() derives from the
    raw, pipe-delimited `segment_id` (e.g. "imperial|Template|Architectural")
    -- rather than `segment_id` itself, so a caller never has to type or
    shell-quote a pipe character on the command line. `output_folder` is
    already the unique, on-disk identifier for a materialized segment (with
    a "_2", "_3", ... suffix appended by _build_registry() on a folder-name
    collision), so this is a strictly more usable selector, not a narrower
    one -- a segment_id with no pipe in it (e.g. "enterprise_all") sanitizes
    to itself, so existing simple selectors are unaffected.

    Returns (segment_root, status, canonical_segment_id) -- the matched
    row's own `segment_id` column, which differs from the `segment_folder`
    selector whenever the segment_id itself contains a pipe. Callers must
    use this canonical segment_id -- never the folder-name selector -- for
    any output field (reference_comparison_summary.csv/_detail.csv, the
    manifest, or diagnostics), so those outputs keep joining to
    segment_manifest.csv/run_registry.csv by segment_id (Codex review, PR
    #475): the folder selector is a lookup convenience only, not a
    replacement identifier.
    """
    if not registry_file.is_file():
        raise CompareReferenceError(REASON_SEGMENT_NOT_FOUND, f"--registry-file not found: {registry_file}")
    rows = read_csv_rows(registry_file)
    matches = [r for r in rows if (r.get("output_folder", "") or "").strip() == segment_folder]
    if not matches:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND, f"segment folder {segment_folder!r} not found in {registry_file}"
        )
    if len(matches) > 1:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND,
            f"segment folder {segment_folder!r} has {len(matches)} rows in {registry_file} (expected exactly one).",
        )
    row = matches[0]
    output_folder = (row.get("output_folder", "") or "").strip()
    status = (row.get("status", "") or "").strip()
    canonical_segment_id = (row.get("segment_id", "") or "").strip()
    if not output_folder:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND, f"segment folder {segment_folder!r} has no output_folder recorded in {registry_file}."
        )
    if not canonical_segment_id:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND, f"segment folder {segment_folder!r} has no segment_id recorded in {registry_file}."
        )
    if status != "complete":
        raise CompareReferenceError(
            REASON_SEGMENT_MATERIALIZATION_INCOMPLETE,
            f"segment {segment_folder!r} run_registry.csv status={status!r} (expected 'complete'); "
            "this segment's materialization is not known-complete.",
        )
    return segments_root / output_folder, status, canonical_segment_id


def require_segment_artifacts(
    segment_root: Path, views: Sequence[str], require_domain_patterns: bool = False
) -> Dict[str, Path]:
    """Confirm the segment-wide artifacts every comparison needs are actually
    present on disk, despite run_registry.csv reporting status=complete
    (internal-consistency check, not a substitute for that status check).
    Never silently regenerates a missing artifact.

    `require_domain_patterns=True` (cross-segment mode only) additionally
    requires `results/analysis/domain_patterns.csv` -- needed to resolve
    segment-local pattern_id values to the cross-segment-stable join_hash
    identity (see resolve_cross_segment_pattern_identity()). Same-segment
    mode never sets this, since raw pattern_id is already directly comparable
    within one segment and this file isn't otherwise required.
    """
    records_dir = segment_root / "results" / "records"
    analysis_dir = segment_root / "results" / "analysis"
    bundle_dir = segment_root / "results" / "bundle_analysis"

    required_files = {
        "records.csv": records_dir / "records.csv",
        "file_metadata.csv": records_dir / "file_metadata.csv",
        "pattern_presence_file.csv": analysis_dir / "pattern_presence_file.csv",
    }
    if require_domain_patterns:
        required_files["domain_patterns.csv"] = analysis_dir / "domain_patterns.csv"
    missing = [str(p) for p in required_files.values() if not p.is_file()]
    for view in views:
        view_dir = bundle_dir / view
        if not view_dir.is_dir():
            missing.append(str(view_dir))
    if missing:
        raise CompareReferenceError(
            REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING,
            f"segment materialization is missing required artifact(s), despite run_registry.csv "
            f"reporting status=complete: {missing}",
        )
    return {"records_dir": records_dir, "analysis_dir": analysis_dir, "bundle_dir": bundle_dir}


def resolve_run_id_and_domains(
    presence_rows: Sequence[Dict[str, str]], explicit_domains: Optional[str]
) -> Tuple[str, List[str]]:
    try:
        run_id = resolve_analysis_run_id(presence_rows)
    except ValueError as exc:
        raise CompareReferenceError(
            REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING,
            f"pattern_presence_file.csv does not have exactly one analysis_run_id: {exc}",
        ) from exc

    if explicit_domains:
        domains = sorted({d.strip() for d in explicit_domains.split(",") if d.strip()})
    else:
        domains = sorted(
            {
                (r.get("domain", "") or "").strip()
                for r in presence_rows
                if r.get("analysis_run_id", "") == run_id and (r.get("domain", "") or "").strip()
            }
        )
    if not domains:
        raise CompareReferenceError(
            REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING,
            "no domains found in this segment's pattern_presence_file.csv for its analysis_run_id.",
        )
    return run_id, domains


def resolve_reference_run_id(presence_rows: Sequence[Dict[str, str]]) -> str:
    """Like resolve_run_id_and_domains's run_id half, but for the reference
    segment alone -- used only in cross-segment mode, where the reference
    segment's own analysis_run_id is not necessarily the same value as the
    target segment's (each segment independently satisfies the one-run-id-
    per-segment invariant enforced by resolve_analysis_run_id, but the two
    segments' run_ids are otherwise unrelated identifiers).
    """
    try:
        return resolve_analysis_run_id(presence_rows)
    except ValueError as exc:
        raise CompareReferenceError(
            REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING,
            f"reference segment's pattern_presence_file.csv does not have exactly one analysis_run_id: {exc}",
        ) from exc


def read_extractor_schema_version(analysis_dir: Path) -> str:
    path = analysis_dir / "corpus_manifest.csv"
    if not path.is_file():
        return ""
    rows = read_csv_rows(path)
    versions = {(r.get("schema_version", "") or "").strip() for r in rows if (r.get("schema_version", "") or "").strip()}
    return next(iter(versions)) if len(versions) == 1 else ""


def read_segment_unit_system(file_metadata_rows: Sequence[Dict[str, str]]) -> str:
    """Mirrors read_extractor_schema_version's single-value-or-blank pattern:
    returns the segment's unit_system only if exactly one distinct non-blank
    value is present across its own file_metadata.csv rows, else "" (not
    provably uniform). Only consulted in cross-segment mode -- see
    REASON_CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH.
    """
    values = {(r.get("unit_system", "") or "").strip() for r in file_metadata_rows if (r.get("unit_system", "") or "").strip()}
    return next(iter(values)) if len(values) == 1 else ""


# ---------------------------------------------------------------------------
# Reference-domain-set construction (analogous to
# tools/bundle_analysis/reference_bundle.py::write_sidecar's payload shape,
# but sourced directly from this segment's own already-materialized
# pattern_presence_file.csv instead of a --seed run's domain_patterns.csv,
# since segments are not built with --seed).
# ---------------------------------------------------------------------------


def build_reference(
    presence_rows: Sequence[Dict[str, str]],
    run_id: str,
    reference_export_run_id: str,
    extractor_schema_version: str,
) -> Dict[str, object]:
    domains: Dict[str, Set[str]] = {}
    for row in presence_rows:
        if row.get("analysis_run_id", "") != run_id:
            continue
        if (row.get("export_run_id", "") or "").strip() != reference_export_run_id:
            continue
        dom = (row.get("domain", "") or "").strip()
        pid = (row.get("pattern_id", "") or "").strip()
        if not dom or not pid:
            continue
        domains.setdefault(dom, set()).add(pid)
    sorted_domains = {d: sorted(p) for d, p in sorted(domains.items())}
    today_iso = date.today().isoformat()
    return {
        "reference_bundle_id": f"{_export_stem(reference_export_run_id)}-{today_iso}",
        "effective_date": today_iso,
        "extractor_schema_version": extractor_schema_version,
        "seed_export_run_id": reference_export_run_id,
        "domains": sorted_domains,
    }


# ---------------------------------------------------------------------------
# Materialization-compatibility gate
# ---------------------------------------------------------------------------


def check_domain_compatibility(records_csv: Path, domains: Sequence[str]) -> Dict[str, Dict[str, object]]:
    """Single streaming pass over this segment's own records.csv, deriving
    each requested domain's (join_key_schema, join_key_policy_id,
    join_key_policy_version) tuple(s). Every record in one segment was
    produced by one patterns-stage invocation under one --join-policy, so a
    domain with more than one distinct *complete* (all three fields
    populated) tuple indicates an internally-inconsistent materialization
    (block); a domain where every field isn't populated for every record --
    zero complete tuples at all, or even one record with any blank field
    coexisting with an otherwise-consistent complete tuple -- means
    compatibility cannot be established for the *whole* domain from what is
    persisted today (block, distinctly: a partially-populated tuple such as
    `(schema, "", "")` is never treated as proof of anything, and a blank
    record is never simply discarded in the presence of a populated one).
    Structured as a per-domain tuple set so a future artifact-SHA field can
    be folded into the same tuple without changing this function's shape or
    callers.
    """
    domain_set = set(domains)
    seen: Dict[str, Set[Tuple[str, str, str]]] = {d: set() for d in domain_set}
    with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dom = (row.get("domain") or "").strip()
            if dom not in domain_set:
                continue
            key = (
                (row.get("join_key_schema") or "").strip(),
                (row.get("join_key_policy_id") or "").strip(),
                (row.get("join_key_policy_version") or "").strip(),
            )
            seen[dom].add(key)

    result: Dict[str, Dict[str, object]] = {}
    for dom in domain_set:
        tuples = seen[dom]
        complete = sorted(k for k in tuples if all(k))
        has_incomplete = any(not all(k) for k in tuples)
        if len(complete) > 1:
            result[dom] = {"status": "incompatible", "values": complete}
        elif has_incomplete or not complete:
            result[dom] = {"status": "unproven", "values": complete}
        else:
            result[dom] = {"status": "ok", "values": complete}
    return result


def resolve_cross_segment_compatibility(
    target_compatibility: Dict[str, Dict[str, object]],
    reference_compatibility: Dict[str, Dict[str, object]],
    domains: Sequence[str],
    reference_domains: Dict[str, List[str]],
) -> Dict[str, Dict[str, object]]:
    """Cross-segment join-policy compatibility gate. Only ever called when
    --reference-segment != --target-segment (the same-segment path uses
    target_compatibility directly and never calls this, preserving byte-
    identical same-segment output). For each domain, compares the target
    segment's own (join_key_schema, join_key_policy_id, join_key_policy_version)
    tuple -- as derived by check_domain_compatibility() against that segment's
    own records.csv -- to the reference segment's tuple for the same domain.
    A domain is only cross-segment "ok" when *both* sides are independently
    "ok" (see check_domain_compatibility) AND their complete-tuple value sets
    are identical; any other outcome (either side incompatible/unproven, or
    both ok but disagreeing) blocks the domain via
    REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH rather than comparing pattern_id
    sets that were produced under different join policies.

    `reference_domains` is `reference["domains"]` as built by build_reference()
    (pre-join_hash-translation) -- a domain absent from it (or mapped to an
    empty list) has NO reference pattern set at all, which is an entirely
    ordinary outcome when `domains` defaults from the *target* segment's own
    population (the reference may legitimately not define every domain the
    target has). check_domain_compatibility() would otherwise report the
    reference side "unproven" for such a domain purely because its records.csv
    has zero rows there, which is a records.csv-provenance signal, not a
    join-policy disagreement -- forcing CROSS_SEGMENT_JOIN_POLICY_MISMATCH
    here would incorrectly block a comparison the comparator's own existing
    REFERENCE_DOMAIN_UNDEFINED outcome already handles correctly and
    non-blockingly. Such domains are passed through as "ok" here, deferring
    entirely to that existing behavior (Codex review, PR #471).
    """
    result: Dict[str, Dict[str, object]] = {}
    for dom in domains:
        if not reference_domains.get(dom):
            result[dom] = {"status": "ok", "values": target_compatibility[dom]["values"]}
            continue
        target = target_compatibility[dom]
        reference = reference_compatibility[dom]
        if target["status"] == "ok" and reference["status"] == "ok" and target["values"] == reference["values"]:
            result[dom] = {"status": "ok", "values": target["values"]}
        else:
            result[dom] = {
                "status": "cross_segment_mismatch",
                "values": {"target": target, "reference": reference},
            }
    return result


# ---------------------------------------------------------------------------
# Cross-segment pattern-identity resolution (pattern_id -> join_hash).
#
# pattern_id values are segment-local: each segment's own patterns stage
# assigns them independently, so the same integer/label in two different
# segments' membership_matrix.csv files carries no shared meaning. The
# cross-segment-stable identity is join_hash, resolved from each segment's
# own results/analysis/domain_patterns.csv (join_hash =
# source_cluster_id.split("|")[-1]) -- the same resolution
# tools/compare_cross_segment.py::resolve_join_hashes already performs for
# the corpus-wide cross-segment tool. Only relevant/called in cross-segment
# mode; same-segment mode compares raw pattern_id directly (as before),
# since within one segment's own single patterns-stage run pattern_id
# already is the stable identity (Codex review, PR #471).
# ---------------------------------------------------------------------------


def load_domain_pattern_join_hash_map(segment_root: Path, domain: str) -> Tuple[Dict[str, str], bool]:
    """Return ({pattern_id: join_hash}, fully_resolved) for one segment's
    results/analysis/domain_patterns.csv, scoped to `domain`. `fully_resolved`
    is False when the domain has zero rows at all in that file, or when at
    least one row for the domain has a blank source_cluster_id (cannot be
    resolved to a join_hash) -- mirroring
    tools/compare_cross_segment.py::resolve_join_hashes's own blank-
    source_cluster_id case, but treated as a hard block here (via the caller)
    rather than a silent warn-and-skip: an undetected partial resolution
    could silently understate a reference or target pattern set without any
    visible signal.
    """
    path = segment_root / "results" / "analysis" / "domain_patterns.csv"
    if not path.is_file():
        return {}, False
    jh_map: Dict[str, str] = {}
    saw_row = False
    fully_resolved = True
    for row in read_csv_rows(path):
        if (row.get("domain", "") or "").strip() != domain:
            continue
        saw_row = True
        pid = (row.get("pattern_id", "") or "").strip()
        scid = (row.get("source_cluster_id", "") or "").strip()
        if not pid:
            continue
        if not scid:
            fully_resolved = False
            continue
        jh_map[pid] = scid.split("|")[-1]
    return jh_map, (fully_resolved and saw_row)


def resolve_cross_segment_pattern_identity(
    reference_segment_root: Path,
    target_segment_root: Path,
    domains: Sequence[str],
    reference_domains: Dict[str, List[str]],
) -> Tuple[Dict[str, Dict[str, object]], Dict[str, Dict[str, str]]]:
    """For each domain, resolve both segments' pattern_id -> join_hash maps
    and determine whether cross-segment comparison can proceed for it.
    Mutates `reference_domains` (the `reference["domains"]` dict already
    built by build_reference()) IN PLACE, replacing each resolvable domain's
    raw reference pattern_id list with the resolved, deduplicated, sorted
    join_hash list -- so the rest of the pipeline (run_compare_for_domain via
    the translated target membership below) compares join_hash to join_hash.

    Returns (identity_status, target_jh_maps): identity_status[domain] is
    {"status": "ok"}, {"status": "reference_undefined"} (this domain has no
    reference pattern set at all -- see below), or {"status": "unresolved",
    "detail": ...}; target_jh_maps[domain] is the target segment's own
    {pattern_id: join_hash} map, needed later to translate each requested
    view's real membership_matrix.csv before it reaches run_compare_for_domain
    (see _write_translated_membership_matrix()).

    A domain absent from `reference_domains` (or mapped to an empty list) has
    no reference pattern set to translate at all -- an ordinary outcome when
    `domains` defaults from the target segment's own population (mirrors the
    identical skip in resolve_cross_segment_compatibility(), Codex review,
    PR #471). run_compare_for_domain's own NO_REFERENCE_DEFINED shortcut never
    reads target membership data when the reference pattern set is empty, so
    such a domain is marked "reference_undefined" here without even touching
    domain_patterns.csv -- run_comparisons() skips translation for it and
    compares directly against the real (untranslated) target bundle_dir,
    which is exactly as harmless as it is for a domain with no reference
    definition today.
    """
    identity_status: Dict[str, Dict[str, object]] = {}
    target_jh_maps: Dict[str, Dict[str, str]] = {}
    for dom in domains:
        if not reference_domains.get(dom):
            identity_status[dom] = {"status": "reference_undefined"}
            continue
        reference_jh_map, reference_resolved = load_domain_pattern_join_hash_map(reference_segment_root, dom)
        target_jh_map, target_resolved = load_domain_pattern_join_hash_map(target_segment_root, dom)
        target_jh_maps[dom] = target_jh_map
        if not reference_resolved or not target_resolved:
            identity_status[dom] = {
                "status": "unresolved",
                "detail": (
                    f"domain={dom!r}: domain_patterns.csv join_hash resolution incomplete or missing "
                    f"(reference_resolvable={reference_resolved}, target_resolvable={target_resolved})"
                ),
            }
            continue

        raw_pattern_ids = reference_domains.get(dom, [])
        translated: Set[str] = set()
        all_resolved = True
        for pid in raw_pattern_ids:
            jh = reference_jh_map.get(pid)
            if not jh:
                all_resolved = False
                continue
            translated.add(jh)
        if not all_resolved:
            identity_status[dom] = {
                "status": "unresolved",
                "detail": (
                    f"domain={dom!r}: at least one reference pattern_id has no join_hash in the "
                    "reference segment's own domain_patterns.csv"
                ),
            }
            continue

        identity_status[dom] = {"status": "ok"}
        reference_domains[dom] = sorted(translated)
    return identity_status, target_jh_maps


# ---------------------------------------------------------------------------
# Comparison execution (per domain, per purge_view) -- reuses
# run_compare_for_domain directly against this segment's own already-built
# bundle_analysis/<view>/<domain>/membership_matrix.csv; no step1 recompute.
# ---------------------------------------------------------------------------


def _synthesized_blocked_summary(
    reference: Dict[str, object], run_id: str, domain: str, reason_code: str, detail: str
) -> Dict[str, str]:
    """A blocked domain summary dict shaped exactly like
    run_compare_for_domain's own return value (see
    tools/bundle_analysis/run_bundle_analysis.py::_blocked_compare_summary,
    which this mirrors field-for-field), for a domain this tool itself
    blocked before ever calling the comparator (compatibility gate).
    Reimplemented locally rather than imported: run_bundle_analysis.py's
    own relative-import chain (`..jenks_utils`) only resolves correctly
    under the `tools.bundle_analysis` package path, not the bare
    `bundle_analysis` path this module's sys.path setup uses.
    """
    return {
        "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
        "effective_date": str(reference.get("effective_date", "")),
        "analysis_run_id": run_id,
        "domain": domain,
        "population_id": "",
        "files_scored": "0",
        "full_count": "0",
        "partial_count": "0",
        "none_count": "0",
        "no_reference_count": "0",
        "comparison_status": COMPARISON_STATUS_BLOCKED,
        "comparison_reason_codes": reason_code,
        "comparison_ok_count": "0",
        "comparison_degraded_count": "0",
        "comparison_blocked_count": "0",
    }


def _membership_matrix_is_current(bundle_dir: Path, view: str, domain: str, run_id: str) -> bool:
    """A domain's bundle_analysis/<view>/<domain>/membership_matrix.csv can
    persist from an older segment run whose analysis_run_id no longer
    matches the current pattern_presence_file.csv (e.g. a domain that was
    active in a prior population but wasn't touched by the most recent
    patterns+bundle pass). run_compare_for_domain filters membership rows
    to the current run_id internally, so a stale file with only old-run
    rows silently degrades to "target has zero patterns" -- reported as a
    trustworthy comparison_status=ok/none result rather than blocked
    (Codex review, PR #467). A missing file is not this function's concern
    (the comparator's own existing missing-input handling covers it); a
    genuinely empty file (zero rows at all) is not stale, just empty.
    """
    path = bundle_dir / view / domain / "membership_matrix.csv"
    if not path.is_file():
        return True
    rows = read_csv_rows(path)
    if not rows:
        return True
    return any(row.get("analysis_run_id", "") == run_id for row in rows)


def _write_translated_membership_matrix(src_path: Path, dest_path: Path, jh_map: Dict[str, str]) -> bool:
    """Materialize a translated copy of one (view, domain)'s real
    membership_matrix.csv, with each non-blank `pattern_id` value replaced by
    its resolved join_hash from `jh_map` (a target segment's own {pattern_id:
    join_hash} map, from load_domain_pattern_join_hash_map()). Written into
    this tool's own --out-dir (never the real segment materialization, which
    this tool only ever reads) so run_compare_for_domain -- unmodified, and
    unaware any translation happened -- can be pointed at it in place of the
    real target bundle_dir for cross-segment mode.

    Returns True if every non-blank pattern_id row resolved. A blank
    pattern_id row (the existing "unknown identity" bucket -- see
    tools/extractor.py's own "Unknown join_hash rows still get membership
    rows with blank pattern_id" comment) passes through unchanged; it isn't a
    translation failure. A row whose pattern_id has no entry in `jh_map`
    despite the domain's overall resolution having been proven complete by
    resolve_cross_segment_pattern_identity() would be an unexpected
    inconsistency between this segment's own membership_matrix.csv and
    domain_patterns.csv -- returning False here lets the caller block that
    (domain, view) explicitly rather than silently dropping the row.

    A missing `src_path` is NOT translated into an empty `dest_path`: writing
    an empty-but-present file would make the downstream read see "zero
    target patterns" (an `ok`/`none` result) instead of the missing-file
    condition run_compare_for_domain's own try/except already turns into
    `COMPARISON_INPUT_INVALID` for the untranslated same-segment path --
    materializing an empty file here would silently suppress that signal
    (Codex review, PR #471). Leaving `dest_path` absent lets the same
    FileNotFoundError propagate identically in cross-segment mode.
    """
    if not src_path.is_file():
        return True
    rows = read_csv_rows(src_path)
    out_rows: List[Dict[str, str]] = []
    fully_resolved = True
    for row in rows:
        pid = (row.get("pattern_id", "") or "").strip()
        if not pid:
            out_rows.append(row)
            continue
        jh = jh_map.get(pid)
        if not jh:
            fully_resolved = False
            continue
        translated = dict(row)
        translated["pattern_id"] = jh
        out_rows.append(translated)
    atomic_write_csv(dest_path, ["analysis_run_id", "export_run_id", "pattern_id"], out_rows)
    return fully_resolved


def run_comparisons(
    run_id: str,
    analysis_dir: Path,
    bundle_dir: Path,
    reference: Dict[str, object],
    domains: Sequence[str],
    views: Sequence[str],
    compatibility: Dict[str, Dict[str, object]],
    target_export_run_id: Optional[str],
    all_export_run_ids: Sequence[str],
    out_dir: Path,
    pattern_identity_status: Optional[Dict[str, Dict[str, object]]] = None,
    target_jh_maps: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    """`pattern_identity_status`/`target_jh_maps` are only ever passed
    (non-None) in cross-segment mode, from resolve_cross_segment_pattern_identity().
    Same-segment invocations pass neither, so the cross-segment identity gate
    and membership-matrix translation below are entirely skipped -- byte-
    identical to this function's pre-cross-segment-support behavior.
    """
    per_view_summaries: Dict[str, List[Dict[str, str]]] = {}
    eligible = {target_export_run_id} if target_export_run_id else set(all_export_run_ids)
    cross_segment = pattern_identity_status is not None

    # Cross-segment only: the comparator's own seed_export_run_id exclusion
    # (tools/bundle_analysis/step_compare.py::_compute_comparison_rows, never
    # modified here) drops any target export_run_id string equal to
    # reference["seed_export_run_id"] -- correct in same-segment mode (the
    # reference genuinely is one of the segment's own files), but wrong
    # cross-segment: reference and target export_run_id strings come from two
    # independent segments' namespaces, so an accidental string collision
    # would silently exclude an unrelated, legitimate target file. Blanking
    # seed_export_run_id (falsy -- the comparator's own `if seed_export_run_id
    # and ...` guard then never fires) neutralizes that exclusion for the
    # actual comparison call. `reference_bundle_id`/`effective_date` (the
    # fields that actually appear in output rows) are untouched, and the real
    # reference dict (with its true seed_export_run_id, used for manifest/
    # diagnostics output) is never mutated -- only this local copy differs
    # (Codex review, PR #471).
    comparator_reference = reference
    if cross_segment:
        comparator_reference = dict(reference)
        comparator_reference["seed_export_run_id"] = ""

    for view in views:
        compare_out_dir = out_dir / f"compare_{view}"
        view_summaries: List[Dict[str, str]] = []
        for dom in domains:
            compat = compatibility[dom]
            if compat["status"] != "ok":
                if compat["status"] == "incompatible":
                    reason_code = REASON_MATERIALIZATION_VERSION_INCOMPATIBLE
                    detail = f"join_key_schema/policy compatibility for domain={dom!r}: {compat['values']}"
                elif compat["status"] == "cross_segment_mismatch":
                    reason_code = REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH
                    detail = (
                        f"join_key_schema/policy compatibility mismatch across segments for domain={dom!r}: "
                        f"{compat['values']}"
                    )
                else:
                    reason_code = REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN
                    detail = f"join_key_schema/policy compatibility for domain={dom!r}: {compat['values']}"
                write_blocked_gap_placeholder(
                    compare_out_dir, reference, run_id, dom, "", reason_code, detail, match_any_population=True
                )
                view_summaries.append(_synthesized_blocked_summary(reference, run_id, dom, reason_code, detail))
                continue
            if cross_segment and pattern_identity_status[dom]["status"] == "unresolved":
                reason_code = REASON_CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED
                detail = str(pattern_identity_status[dom]["detail"])
                write_blocked_gap_placeholder(
                    compare_out_dir, reference, run_id, dom, "", reason_code, detail, match_any_population=True
                )
                view_summaries.append(_synthesized_blocked_summary(reference, run_id, dom, reason_code, detail))
                continue
            if not _membership_matrix_is_current(bundle_dir, view, dom, run_id):
                detail = f"membership_matrix.csv for domain={dom!r} view={view!r} has no rows for analysis_run_id={run_id!r}"
                write_blocked_gap_placeholder(
                    compare_out_dir, reference, run_id, dom, "", REASON_STALE_MEMBERSHIP_MATRIX, detail, match_any_population=True
                )
                view_summaries.append(_synthesized_blocked_summary(reference, run_id, dom, REASON_STALE_MEMBERSHIP_MATRIX, detail))
                continue
            if cross_segment and pattern_identity_status[dom]["status"] == "ok":
                # This domain has a real reference pattern set that was
                # successfully translated to join_hash -- the target's real
                # membership_matrix.csv must likewise be translated before
                # the comparator ever reads it.
                translated_dir = out_dir / "_xseg_translated_membership" / view
                real_path = bundle_dir / view / dom / "membership_matrix.csv"
                translated_path = translated_dir / dom / "membership_matrix.csv"
                fully_resolved = _write_translated_membership_matrix(real_path, translated_path, target_jh_maps[dom])
                if not fully_resolved:
                    detail = (
                        f"domain={dom!r} view={view!r}: at least one target pattern_id in membership_matrix.csv "
                        "has no join_hash in the target segment's own domain_patterns.csv, despite this domain's "
                        "overall resolution having been proven complete -- unexpected inconsistency, blocking "
                        "rather than silently dropping the row"
                    )
                    write_blocked_gap_placeholder(
                        compare_out_dir,
                        reference,
                        run_id,
                        dom,
                        "",
                        REASON_CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED,
                        detail,
                        match_any_population=True,
                    )
                    view_summaries.append(
                        _synthesized_blocked_summary(
                            reference, run_id, dom, REASON_CROSS_SEGMENT_PATTERN_IDENTITY_UNRESOLVED, detail
                        )
                    )
                    continue
                summary = run_compare_for_domain(
                    analysis_dir,
                    translated_dir,
                    comparator_reference,
                    dom,
                    compare_out_dir=compare_out_dir,
                    eligible_export_run_ids=eligible,
                )
            else:
                # Same-segment mode, or a cross-segment domain with no
                # reference pattern set at all ("reference_undefined" --
                # run_compare_for_domain's own NO_REFERENCE_DEFINED shortcut
                # never needs join_hash-translated target data for those, so
                # comparing directly against the real, untranslated target
                # bundle_dir is exactly as harmless as it is today for a
                # same-segment domain the reference doesn't define).
                summary = run_compare_for_domain(
                    analysis_dir,
                    bundle_dir / view,
                    comparator_reference,
                    dom,
                    compare_out_dir=compare_out_dir,
                    eligible_export_run_ids=eligible,
                )
            view_summaries.append(summary)
        per_view_summaries[view] = view_summaries
    return per_view_summaries


# ---------------------------------------------------------------------------
# Output assembly
# ---------------------------------------------------------------------------


def _finalize_view(out_dir: Path, view: str, segment_id: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    compare_dir = out_dir / f"compare_{view}"
    gap_rows = read_csv_rows(compare_dir / "file_gap_report.csv") if (compare_dir / "file_gap_report.csv").is_file() else []
    detail_rows = (
        read_csv_rows(compare_dir / "file_gap_detail.csv") if (compare_dir / "file_gap_detail.csv").is_file() else []
    )

    summary_rows = [
        {
            "segment_id": segment_id,
            "purge_view": view,
            "reference_bundle_id": row.get("reference_bundle_id", ""),
            "analysis_run_id": row.get("analysis_run_id", ""),
            "target_export_run_id": row.get("export_run_id", ""),
            "domain": row.get("domain", ""),
            "population_id": row.get("population_id", ""),
            "comparison_status": row.get("comparison_status", ""),
            "comparison_reason_codes": row.get("comparison_reason_codes", ""),
            "reference_pattern_count": row.get("reference_pattern_count", ""),
            "target_pattern_count": row.get("target_pattern_count", ""),
            "shared_count": row.get("shared_count", ""),
            "reference_only_count": row.get("reference_only_count", ""),
            "target_only_count": row.get("target_only_count", ""),
            "union_count": row.get("union_count", ""),
            "reference_coverage_pct": row.get("reference_coverage_pct", ""),
            "jaccard": row.get("jaccard", ""),
        }
        for row in gap_rows
    ]
    detail_out_rows = [
        {
            "segment_id": segment_id,
            "purge_view": view,
            "reference_bundle_id": row.get("reference_bundle_id", ""),
            "analysis_run_id": row.get("analysis_run_id", ""),
            "target_export_run_id": row.get("export_run_id", ""),
            "domain": row.get("domain", ""),
            "population_id": row.get("population_id", ""),
            "pattern_id": row.get("pattern_id", ""),
            "comparison_class": row.get("comparison_class", ""),
        }
        for row in detail_rows
    ]
    return summary_rows, detail_out_rows


def build_revit_name_lookup(
    segment_root: Path,
    identity_maps: Optional[Dict[str, Dict[str, str]]] = None,
) -> Tuple[str, Dict[Tuple[str, str, str], Dict[str, str]]]:
    """Index observed labels at (export, domain, comparison identity) grain.

    The authoritative linkage is record_pattern_membership.record_pk to
    records.record_pk.  ``identity_maps`` translates segment-local pattern_id
    to join_hash in cross-segment mode; names remain evidence and never enter
    comparison identity or set arithmetic.
    """
    records_path = segment_root / "results" / "records" / "records.csv"
    membership_path = segment_root / "results" / "analysis" / "record_pattern_membership.csv"
    if not records_path.is_file() or not membership_path.is_file():
        return REVIT_NAME_STATUS_BLOCKED, {}
    try:
        records = read_csv_rows(records_path)
        memberships = read_csv_rows(membership_path)
    except (OSError, UnicodeError, csv.Error):
        return REVIT_NAME_STATUS_BLOCKED, {}
    if records and not {"record_pk", "label_display", "label_quality"}.issubset(records[0]):
        return REVIT_NAME_STATUS_BLOCKED, {}
    if memberships and not {"export_run_id", "domain", "record_pk", "pattern_id"}.issubset(memberships[0]):
        return REVIT_NAME_STATUS_BLOCKED, {}

    record_labels: Dict[str, List[Tuple[str, str]]] = {}
    for row in records:
        record_pk = (row.get("record_pk", "") or "").strip()
        if record_pk:
            record_labels.setdefault(record_pk, []).append(
                ((row.get("label_display", "") or "").strip(), (row.get("label_quality", "") or "").strip())
            )

    evidence: Dict[Tuple[str, str, str], List[Tuple[str, str]]] = {}
    for row in memberships:
        export_id = (row.get("export_run_id", "") or "").strip()
        domain = (row.get("domain", "") or "").strip()
        local_pid = (row.get("pattern_id", "") or "").strip()
        identity = identity_maps.get(domain, {}).get(local_pid, "") if identity_maps is not None else local_pid
        if not export_id or not domain or not identity:
            continue
        evidence.setdefault((export_id, domain, identity), []).extend(
            record_labels.get((row.get("record_pk", "") or "").strip(), [])
        )

    lookup: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for key, labels in evidence.items():
        usable = sorted({name for name, quality in labels if name and quality in _USABLE_LABEL_QUALITIES})
        has_unreadable = any(quality in _UNREADABLE_LABEL_QUALITIES for _name, quality in labels)
        if len(usable) == 1:
            lookup[key] = {"name": usable[0], "status": REVIT_NAME_STATUS_OK, "count": "1"}
        elif len(usable) > 1:
            lookup[key] = {
                "name": json.dumps(usable, ensure_ascii=False, separators=(",", ":")),
                "status": REVIT_NAME_STATUS_AMBIGUOUS,
                "count": str(len(usable)),
            }
        elif has_unreadable:
            lookup[key] = {"name": "", "status": REVIT_NAME_STATUS_UNREADABLE, "count": "0"}
        else:
            lookup[key] = {"name": "", "status": REVIT_NAME_STATUS_MISSING, "count": "0"}
    return REVIT_NAME_STATUS_OK, lookup


def add_revit_names(
    rows: Sequence[Dict[str, str]],
    reference_export_run_id: str,
    reference_source_status: str,
    reference_lookup: Dict[Tuple[str, str, str], Dict[str, str]],
    target_source_status: str,
    target_lookup: Dict[Tuple[str, str, str], Dict[str, str]],
) -> None:
    """Attach per-file descriptive name evidence without changing row identity."""
    blocked = {"name": "", "status": REVIT_NAME_STATUS_BLOCKED, "count": "0"}
    missing = {"name": "", "status": REVIT_NAME_STATUS_MISSING, "count": "0"}
    for row in rows:
        domain, identity = row.get("domain", ""), row.get("pattern_id", "")
        ref = blocked if reference_source_status != REVIT_NAME_STATUS_OK else reference_lookup.get(
            (reference_export_run_id, domain, identity), missing
        )
        tgt = blocked if target_source_status != REVIT_NAME_STATUS_OK else target_lookup.get(
            (row.get("target_export_run_id", ""), domain, identity), missing
        )
        for side, result in (("reference", ref), ("target", tgt)):
            row[f"{side}_revit_name"] = result["name"]
            row[f"{side}_revit_name_status"] = result["status"]
            row[f"{side}_revit_name_count"] = result["count"]


def _normalize_business_center_label(raw: str) -> str:
    """Left-zero-pad a purely-numeric business_center_label shorter than 4
    digits (e.g. "796" -> "0796") -- the same rule and rationale as
    tools/build_segment_manifest.py::_normalize_rows() (Excel reinterprets a
    leading-zero business_center_label as a number and drops the zeros on
    save). Deliberately reimplemented here rather than imported: that
    function is a private (`_`-prefixed), segment-manifest-build-specific
    helper that also does corpus-wide first-seen-casing folds this module has
    no equivalent population-scan for; only the stateless zero-pad rule
    applies one row at a time and is safe to duplicate. Without this, a
    segment whose file_metadata.csv still has the un-padded value would show
    a different business_center_label here than segment_manifest.csv/the
    governance narrative already show for the same file (Greg, PR #478
    follow-up).
    """
    return raw.zfill(4) if raw.isdigit() and len(raw) < 4 else raw


def build_file_metadata_label_lookup(
    file_metadata_rows: Sequence[Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """One {label_field: value} dict per export_run_id, read straight from a
    segment's own file_metadata.csv -- the same rows resolve_export_run_id()
    already reads, so every target_export_run_id / reference
    seed_export_run_id used elsewhere in this module is guaranteed present
    here under normal operation. Raises REASON_FILE_METADATA_LABEL_JOIN_AMBIGUOUS
    rather than silently keeping the first-seen row if export_run_id turns out
    not to be unique with disagreeing label values (fail-loudly convention).
    """
    lookup: Dict[str, Dict[str, str]] = {}
    for row in file_metadata_rows:
        export_id = (row.get("export_run_id", "") or "").strip()
        if not export_id:
            continue
        values = {field: (row.get(field, "") or "").strip() for field in _FILE_METADATA_LABEL_FIELDS}
        if "business_center_label" in values:
            values["business_center_label"] = _normalize_business_center_label(values["business_center_label"])
        existing = lookup.get(export_id)
        if existing is not None and existing != values:
            raise CompareReferenceError(
                REASON_FILE_METADATA_LABEL_JOIN_AMBIGUOUS,
                f"file_metadata.csv has more than one row for export_run_id={export_id!r} with "
                f"disagreeing label values ({existing!r} vs {values!r}) -- export_run_id is documented "
                "as a unique join key (tools/build_segment_manifest.py); refusing to silently pick one.",
            )
        lookup[export_id] = values
    return lookup


def add_file_metadata_labels(
    rows: Sequence[Dict[str, str]],
    target_lookup: Dict[str, Dict[str, str]],
    reference_export_run_id: str,
    reference_lookup: Dict[str, Dict[str, str]],
) -> None:
    """Attach file_metadata.csv governance/organizational labels for BI
    slicing, in place, mirroring add_revit_names()'s row-mutation pattern.

    reference_export_run_id is the same value for every row produced by one
    invocation (one reference file per run), so its label lookup is resolved
    once here rather than per row; target_export_run_id varies per row. A
    lookup miss (e.g. a synthesized blocked-domain placeholder row with no
    single target file behind it) degrades to blank labels rather than
    raising -- the same convention add_revit_names() uses for its own
    per-row "missing" fallback -- since a miss here reflects the row's own
    provenance, not a file_metadata.csv defect (that's already caught by
    build_file_metadata_label_lookup() above).
    """
    blank = {field: "" for field in _FILE_METADATA_LABEL_FIELDS}
    ref_values = reference_lookup.get(reference_export_run_id, blank)
    for row in rows:
        for field in _FILE_METADATA_LABEL_FIELDS:
            row[f"reference_{field}"] = ref_values[field]
        tgt_values = target_lookup.get(row.get("target_export_run_id", ""), blank)
        for field in _FILE_METADATA_LABEL_FIELDS:
            row[f"target_{field}"] = tgt_values[field]


def build_domain_pattern_name_maps(analysis_dir: Path, domains: Sequence[str]) -> Dict[str, Dict[str, str]]:
    """Build {domain: {pattern_id: pattern_label_human}} from this segment's
    own results/analysis/domain_patterns.csv, once (not per comparison
    group). Excludes any row whose pattern_label_source == "fallback"
    (templated placeholder, not an observed name -- see
    tools/label_synthesis/label_resolver.py::resolve_pattern_label) and any
    row with a blank pattern_id or a name that is empty after .strip().

    Same-segment mode never requires domain_patterns.csv to exist (see
    require_segment_artifacts()'s require_domain_patterns=False for that
    mode) -- a missing file here simply yields an empty map per domain,
    which flows through as "zero resolvable names", not an error.
    """
    name_maps: Dict[str, Dict[str, str]] = {dom: {} for dom in domains}
    path = analysis_dir / "domain_patterns.csv"
    if not path.is_file():
        return name_maps
    domain_set = set(domains)
    for row in read_csv_rows(path):
        dom = (row.get("domain", "") or "").strip()
        if dom not in domain_set:
            continue
        if (row.get("pattern_label_source", "") or "").strip() == _FALLBACK_PATTERN_LABEL_SOURCE:
            continue
        pid = (row.get("pattern_id", "") or "").strip()
        name = (row.get("pattern_label_human", "") or "").strip()
        if not pid or not name:
            continue
        name_maps[dom][pid] = name
    return name_maps


def compute_semantic_changes_rows(
    all_detail_rows: Sequence[Dict[str, str]], name_maps: Dict[str, Dict[str, str]]
) -> List[Dict[str, str]]:
    """Reclassify reference_only/target_only rows from `all_detail_rows`
    (already-computed pattern-identity set membership -- no new comparison
    math here) into likely renames/content-changes under a stable name,
    grouped by the same (purge_view, domain, population_id,
    target_export_run_id) key `all_detail_rows` is itself sorted by.
    `pattern_id` values here are same-segment raw pattern_id (config/
    sig_hash identity) -- never join_hash, since this is only ever called
    when same_segment is True.
    """
    groups: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    for row in all_detail_rows:
        comparison_class = row.get("comparison_class", "")
        if comparison_class not in ("reference_only", "target_only"):
            continue
        key = (row.get("purge_view", ""), row.get("domain", ""), row.get("population_id", ""), row.get("target_export_run_id", ""))
        group = groups.setdefault(
            key,
            {
                "segment_id": row.get("segment_id", ""),
                "reference_bundle_id": row.get("reference_bundle_id", ""),
                "analysis_run_id": row.get("analysis_run_id", ""),
                "reference_only": [],
                "target_only": [],
            },
        )
        group[comparison_class].append(row.get("pattern_id", ""))

    out_rows: List[Dict[str, str]] = []
    for (purge_view, domain, population_id, target_export_run_id), group in groups.items():
        name_map = name_maps.get(domain, {})

        reference_names_to_pids: Dict[str, List[str]] = {}
        for pid in group["reference_only"]:
            name = name_map.get(pid)
            if name is not None:
                reference_names_to_pids.setdefault(name, []).append(pid)
        target_names_to_pids: Dict[str, List[str]] = {}
        for pid in group["target_only"]:
            name = name_map.get(pid)
            if name is not None:
                target_names_to_pids.setdefault(name, []).append(pid)

        base_row = {
            "segment_id": group["segment_id"],
            "purge_view": purge_view,
            "reference_bundle_id": group["reference_bundle_id"],
            "analysis_run_id": group["analysis_run_id"],
            "target_export_run_id": target_export_run_id,
            "domain": domain,
            "population_id": population_id,
            "name_match_basis": _NAME_MATCH_BASIS_PATTERN_LABEL_HUMAN,
        }

        all_names = set(reference_names_to_pids) | set(target_names_to_pids)
        for name in all_names:
            ref_pids = sorted(reference_names_to_pids.get(name, []))
            tgt_pids = sorted(target_names_to_pids.get(name, []))
            if ref_pids and tgt_pids:
                semantic_change_class = "changed" if len(ref_pids) == 1 and len(tgt_pids) == 1 else "ambiguous_name_match"
            elif ref_pids:
                semantic_change_class = "removed"
            else:
                semantic_change_class = "added"
            out_rows.append(
                {
                    **base_row,
                    "pattern_name": name,
                    "reference_pattern_id": "|".join(ref_pids),
                    "target_pattern_id": "|".join(tgt_pids),
                    "semantic_change_class": semantic_change_class,
                }
            )

    out_rows.sort(key=lambda r: (r["purge_view"], r["domain"], r["population_id"], r["target_export_run_id"], r["pattern_name"]))
    return out_rows


def compute_cross_segment_semantic_changes_rows(
    all_detail_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Produce the existing name-based change report from file-observed names.

    Cross-segment detail ``pattern_id`` is already join_hash.  This function
    only groups the comparator's already-established reference_only and
    target_only identities by independently resolved, usable names; it never
    uses a name to alter those comparison classes.
    """
    groups: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    for row in all_detail_rows:
        comparison_class = row.get("comparison_class", "")
        if comparison_class not in ("reference_only", "target_only"):
            continue
        key = (
            row.get("purge_view", ""), row.get("domain", ""),
            row.get("population_id", ""), row.get("target_export_run_id", ""),
        )
        group = groups.setdefault(key, {"base": row, "reference_only": [], "target_only": []})
        side = "reference" if comparison_class == "reference_only" else "target"
        if row.get(f"{side}_revit_name_status") == REVIT_NAME_STATUS_OK:
            group[comparison_class].append((row.get(f"{side}_revit_name", ""), row.get("pattern_id", "")))

    out_rows: List[Dict[str, str]] = []
    for key, group in groups.items():
        purge_view, domain, population_id, target_export_run_id = key
        base = group["base"]
        ref_by_name: Dict[str, List[str]] = {}
        tgt_by_name: Dict[str, List[str]] = {}
        for name, identity in group["reference_only"]:
            ref_by_name.setdefault(name, []).append(identity)
        for name, identity in group["target_only"]:
            tgt_by_name.setdefault(name, []).append(identity)
        for name in sorted(set(ref_by_name) | set(tgt_by_name)):
            ref_ids = sorted(ref_by_name.get(name, []))
            tgt_ids = sorted(tgt_by_name.get(name, []))
            if ref_ids and tgt_ids:
                change_class = "changed" if len(ref_ids) == len(tgt_ids) == 1 else "ambiguous_name_match"
            elif ref_ids:
                change_class = "removed"
            else:
                change_class = "added"
            out_rows.append({
                "segment_id": base.get("segment_id", ""),
                "purge_view": purge_view,
                "reference_bundle_id": base.get("reference_bundle_id", ""),
                "analysis_run_id": base.get("analysis_run_id", ""),
                "target_export_run_id": target_export_run_id,
                "domain": domain,
                "population_id": population_id,
                "pattern_name": name,
                "reference_pattern_id": "|".join(ref_ids),
                "target_pattern_id": "|".join(tgt_ids),
                "semantic_change_class": change_class,
                "name_match_basis": "revit_observed_label_display",
                "reference_revit_name": name if ref_ids else "",
                "reference_revit_name_status": REVIT_NAME_STATUS_OK if ref_ids else REVIT_NAME_STATUS_MISSING,
                "target_revit_name": name if tgt_ids else "",
                "target_revit_name_status": REVIT_NAME_STATUS_OK if tgt_ids else REVIT_NAME_STATUS_MISSING,
            })
    out_rows.sort(key=lambda row: (
        row["purge_view"], row["domain"], row["population_id"],
        row["target_export_run_id"], row["pattern_name"],
    ))
    return out_rows


# ---------------------------------------------------------------------------
# Name-set-overlap classification (Step 1 Part B, default on; --no-name-overlap opts out).
# ---------------------------------------------------------------------------


def _name_key_side_status(segment_root: Path) -> Tuple[str, Optional[Path], Optional[Path], Optional[Path]]:
    """Resolve one segment's own records.csv / domain_patterns.csv / name_key_results.csv
    paths and an availability status for the name-key side specifically.

    Returns (status, records_csv, domain_patterns_csv, name_key_csv). `status` is
    NAME_KEY_STATUS_OK, NAME_KEY_STATUS_NOT_MATERIALIZED (name_key_results.csv absent --
    e.g. -NameKey / --comparison-target name|both was never run for this segment; see
    docs/namekey_crosssegment_step0_findings.md A.1/A.2), or NAME_KEY_STATUS_STALE
    (name_key_results.csv exists but is older than this segment's own records.csv --
    mirrors tools/corpus_update_runbook.ps1's own Run-C staleness guard, lines 291-301).
    B2 design decision: this is a fail-soft per-domain/per-pattern signal, never a hard
    failure of the whole comparison -- a segment missing name-key materialization still
    gets its ordinary config-identity comparison; only the name-overlap columns degrade.
    """
    records_csv = segment_root / "results" / "records" / "records.csv"
    domain_patterns_csv = segment_root / "results" / "analysis" / "domain_patterns.csv"
    name_key_csv = segment_root / "results" / "name_key" / "name_key_results.csv"
    if not records_csv.is_file() or not domain_patterns_csv.is_file():
        return NAME_KEY_STATUS_NOT_MATERIALIZED, None, None, None
    if not name_key_csv.is_file():
        return NAME_KEY_STATUS_NOT_MATERIALIZED, records_csv, domain_patterns_csv, None
    if name_key_csv.stat().st_mtime < records_csv.stat().st_mtime:
        return NAME_KEY_STATUS_STALE, records_csv, domain_patterns_csv, name_key_csv
    return NAME_KEY_STATUS_OK, records_csv, domain_patterns_csv, name_key_csv


def _load_side_facets(segment_root: Path):
    """Load one side's DomainNameHashFacets (empty if not materialized/stale -- see
    _name_key_side_status()) plus its own {domain: {pattern_id: join_hash}} map (needed
    only in same-segment mode, to translate reference_comparison_detail.csv's raw
    pattern_id into the join_hash identity facets are keyed by; unused/harmless to build in
    cross-segment mode too, since load_domain_pattern_join_hash_map() only ever reads that
    segment's own domain_patterns.csv)."""
    status, records_csv, domain_patterns_csv, name_key_csv = _name_key_side_status(segment_root)
    if status != NAME_KEY_STATUS_OK:
        return status, build_domain_name_hash_facets([], [], [])
    records_rows = read_csv_rows(records_csv)
    domain_patterns_rows = read_csv_rows(domain_patterns_csv)
    name_key_rows = read_csv_rows(name_key_csv)
    return status, build_domain_name_hash_facets(records_rows, domain_patterns_rows, name_key_rows)


def compute_name_overlap_rows(
    all_detail_rows: Sequence[Dict[str, str]],
    domains: Sequence[str],
    reference_segment_root: Path,
    target_segment_root: Path,
    same_segment: bool,
    reference_export_run_id: str,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Reclassify every all_detail_rows row (shared/reference_only/target_only -- the same
    already-computed config-identity set membership compute_semantic_changes_rows() reuses)
    by the SET relationship between the reference side's and target side's name-key
    join_hash sets for that same pattern identity.

    Returns (out_rows, name_rows). `out_rows` is the one-row-per-pattern summary (counts and
    classification only). `name_rows` is one row per (pattern, side, name_hash) -- the actual
    hash values live ONLY here, never pipe-joined into an `out_rows` cell: a pattern can carry
    thousands of distinct name-hashes (real corpus data: max 5904 for one `line_patterns`
    pattern), and a single CSV cell holding that many pipe-joined values overflows Excel's
    per-cell limit and corrupts the surrounding rows on import (PR #476 follow-up). Mirrors
    Part A's `pattern_name_fragmentation.csv` design (A1): one row per value, never a
    concatenated list column.

    `row["pattern_id"]` is same-segment raw pattern_id when `same_segment` is True, or
    already the cross-segment-stable join_hash when False (resolve_cross_segment_pattern_
    identity() translates and mutates reference["domains"]/the target membership matrix into
    join_hash terms before run_compare_for_domain ever runs -- see that function's own
    docstring above). Same-segment mode translates pattern_id -> join_hash itself, via the
    existing load_domain_pattern_join_hash_map() (this file, above) -- one call per domain,
    reused for both sides since reference_segment_root == target_segment_root there.

    Each side's name-hash lookup MUST be scoped to the one specific export actually being
    compared in this row -- `reference_export_run_id` (the single resolved reference file,
    constant across every row) for the reference side, `row["target_export_run_id"]` (that
    row's own target file) for the target side -- via `name_hashes_for_export()`, never the
    segment-wide aggregate `name_hashes_for()`. The aggregate mixes in names from every other
    file in the segment that happens to share the same config identity: in same-segment mode
    it made every comparison collapse to `name_sets_identical` (reference and target facets
    are literally the same aggregated object for the same key), and in cross-segment mode it
    let names from unrelated target files contaminate a row about one specific target file
    (PR #476 review).
    """
    reference_status, reference_facets = _load_side_facets(reference_segment_root)
    if same_segment:
        target_status, target_facets = reference_status, reference_facets
    else:
        target_status, target_facets = _load_side_facets(target_segment_root)

    # Same-segment only: pattern_id -> join_hash per domain, built once, reused across every
    # row for that domain (load_domain_pattern_join_hash_map() re-reads domain_patterns.csv
    # per call, so caching here avoids re-reading it once per row).
    same_segment_jh_maps: Dict[str, Dict[str, str]] = {}
    if same_segment:
        for dom in domains:
            jh_map, _resolved = load_domain_pattern_join_hash_map(target_segment_root, dom)
            same_segment_jh_maps[dom] = jh_map

    out_rows: List[Dict[str, str]] = []
    name_rows: List[Dict[str, str]] = []
    for row in all_detail_rows:
        comparison_class = row.get("comparison_class", "")
        if comparison_class not in ("shared", "reference_only", "target_only"):
            continue
        domain = row.get("domain", "")
        raw_pattern_id = row.get("pattern_id", "")

        base_row = {
            "segment_id": row.get("segment_id", ""),
            "purge_view": row.get("purge_view", ""),
            "reference_bundle_id": row.get("reference_bundle_id", ""),
            "analysis_run_id": row.get("analysis_run_id", ""),
            "target_export_run_id": row.get("target_export_run_id", ""),
            "domain": domain,
            "population_id": row.get("population_id", ""),
            "pattern_id": raw_pattern_id,
            "comparison_class": comparison_class,
            "reference_name_key_status": reference_status,
            "target_name_key_status": target_status,
        }

        if domain not in ELIGIBLE_DOMAINS:
            out_rows.append({
                **base_row,
                "name_set_classification": NAME_EVIDENCE_EXCLUDED,
                "exclusion_reason": exclusion_reason(domain),
                "reference_name_hash_count": "0",
                "target_name_hash_count": "0",
                "shared_name_hash_count": "0",
            })
            continue

        if same_segment:
            identity_join_hash = same_segment_jh_maps.get(domain, {}).get(raw_pattern_id)
        else:
            identity_join_hash = raw_pattern_id or None

        if not identity_join_hash:
            out_rows.append({
                **base_row,
                "name_set_classification": NAME_EVIDENCE_MISSING,
                "exclusion_reason": "pattern_identity_unresolved",
                "reference_name_hash_count": "0",
                "target_name_hash_count": "0",
                "shared_name_hash_count": "0",
            })
            continue

        ref_names = set(
            reference_facets.name_hashes_for_export(domain, identity_join_hash, reference_export_run_id).keys()
        )
        tgt_names = set(
            target_facets.name_hashes_for_export(
                domain, identity_join_hash, row.get("target_export_run_id", "")
            ).keys()
        )

        if not ref_names or not tgt_names:
            classification = NAME_EVIDENCE_MISSING
        elif ref_names == tgt_names:
            classification = NAME_SETS_IDENTICAL
        elif ref_names & tgt_names:
            classification = NAME_SETS_OVERLAP
        else:
            classification = NAME_SETS_DISJOINT

        out_rows.append({
            **base_row,
            "name_set_classification": classification,
            "exclusion_reason": "",
            "reference_name_hash_count": str(len(ref_names)),
            "target_name_hash_count": str(len(tgt_names)),
            "shared_name_hash_count": str(len(ref_names & tgt_names)),
        })
        name_base = {k: base_row[k] for k in (
            "segment_id", "purge_view", "reference_bundle_id", "analysis_run_id",
            "target_export_run_id", "domain", "population_id", "pattern_id",
        )}
        for name_hash in sorted(ref_names):
            name_rows.append({**name_base, "side": "reference", "name_hash": name_hash})
        for name_hash in sorted(tgt_names):
            name_rows.append({**name_base, "side": "target", "name_hash": name_hash})

    out_rows.sort(
        key=lambda r: (r["purge_view"], r["domain"], r["population_id"], r["target_export_run_id"], r["pattern_id"])
    )
    name_rows.sort(
        key=lambda r: (
            r["purge_view"], r["domain"], r["population_id"], r["target_export_run_id"],
            r["pattern_id"], r["side"], r["name_hash"],
        )
    )
    return out_rows, name_rows


def assemble_final_outputs(
    out_dir: Path,
    segment_id: str,
    segment_root: Path,
    run_id: str,
    domains: Sequence[str],
    views: Sequence[str],
    reference: Dict[str, object],
    target_export_run_id: Optional[str],
    extractor_schema_version: str,
    reference_segment_id: str,
    same_segment: bool,
    reference_file_metadata_rows: Sequence[Dict[str, str]],
    target_file_metadata_rows: Sequence[Dict[str, str]],
    compatibility: Dict[str, Dict[str, object]],
    reference_segment_root: Optional[Path] = None,
    include_name_overlap: bool = True,
    include_name_config_collisions: bool = True,
) -> Dict[str, object]:
    all_summary_rows: List[Dict[str, str]] = []
    all_detail_rows: List[Dict[str, str]] = []
    for view in views:
        summary_rows, detail_rows = _finalize_view(out_dir, view, segment_id)
        all_summary_rows.extend(summary_rows)
        all_detail_rows.extend(detail_rows)

    all_summary_rows.sort(key=lambda r: (r["purge_view"], r["domain"], r["population_id"], r["target_export_run_id"]))
    all_detail_rows.sort(
        key=lambda r: (r["purge_view"], r["domain"], r["population_id"], r["target_export_run_id"], r["pattern_id"])
    )
    reference_identity_maps = None
    target_identity_maps = None
    if not same_segment:
        reference_identity_maps = {
            dom: load_domain_pattern_join_hash_map(reference_segment_root or segment_root, dom)[0]
            for dom in domains
        }
        target_identity_maps = {
            dom: load_domain_pattern_join_hash_map(segment_root, dom)[0]
            for dom in domains
        }
    reference_name_status, reference_name_lookup = build_revit_name_lookup(
        reference_segment_root or segment_root, reference_identity_maps
    )
    if same_segment:
        target_name_status, target_name_lookup = reference_name_status, reference_name_lookup
    else:
        target_name_status, target_name_lookup = build_revit_name_lookup(segment_root, target_identity_maps)
    add_revit_names(
        all_detail_rows,
        str(reference.get("seed_export_run_id", "")),
        reference_name_status,
        reference_name_lookup,
        target_name_status,
        target_name_lookup,
    )

    # BI-slicer labels (client/discipline/BC/governance-role/etc.), joined
    # from each segment's own file_metadata.csv onto every output row --
    # see build_file_metadata_label_lookup()/add_file_metadata_labels() above.
    reference_export_run_id = str(reference.get("seed_export_run_id", ""))
    reference_label_lookup = build_file_metadata_label_lookup(reference_file_metadata_rows)
    target_label_lookup = (
        reference_label_lookup if same_segment else build_file_metadata_label_lookup(target_file_metadata_rows)
    )
    add_file_metadata_labels(all_summary_rows, target_label_lookup, reference_export_run_id, reference_label_lookup)
    add_file_metadata_labels(all_detail_rows, target_label_lookup, reference_export_run_id, reference_label_lookup)

    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, all_summary_rows)
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, all_detail_rows)

    semantic_changes_skipped_reason = ""
    output_files = [SUMMARY_FILENAME, DETAIL_FILENAME, DIAGNOSTICS_FILENAME]
    if same_segment:
        name_maps = build_domain_pattern_name_maps(segment_root / "results" / "analysis", domains)
        semantic_changes_rows = compute_semantic_changes_rows(all_detail_rows, name_maps)
    else:
        semantic_changes_rows = compute_cross_segment_semantic_changes_rows(all_detail_rows)
    add_file_metadata_labels(semantic_changes_rows, target_label_lookup, reference_export_run_id, reference_label_lookup)
    atomic_write_csv(out_dir / SEMANTIC_CHANGES_FILENAME, _SEMANTIC_CHANGES_FIELDNAMES, semantic_changes_rows)
    output_files.append(SEMANTIC_CHANGES_FILENAME)

    # Written by default (opt-out --no-name-overlap), Step 1 Part B: set-based name-hash
    # overlap classification, sound in both same-segment and cross-segment mode (unlike
    # compute_semantic_changes_rows() above) -- see compute_name_overlap_rows()'s own
    # docstring and the NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED known-gap note above.
    if include_name_overlap:
        name_overlap_rows, name_overlap_name_rows = compute_name_overlap_rows(
            all_detail_rows,
            domains,
            reference_segment_root or segment_root,
            segment_root,
            same_segment,
            str(reference.get("seed_export_run_id", "")),
        )
        add_file_metadata_labels(name_overlap_rows, target_label_lookup, reference_export_run_id, reference_label_lookup)
        atomic_write_csv(out_dir / NAME_OVERLAP_FILENAME, _NAME_OVERLAP_FIELDNAMES, name_overlap_rows)
        atomic_write_csv(out_dir / NAME_OVERLAP_NAMES_FILENAME, _NAME_OVERLAP_NAMES_FIELDNAMES, name_overlap_name_rows)
        output_files.append(NAME_OVERLAP_FILENAME)
        output_files.append(NAME_OVERLAP_NAMES_FILENAME)

    # Opt-out (--no-name-config-collisions, default on), mirroring the --include-name-overlap
    # wiring above as closely as classify_name_config_collisions()'s own signature allows.
    # Step 0 design decision (Greg, PR description): independent load of each side's
    # name-key evidence, rather than sharing compute_name_overlap_rows()'s already-built
    # facets -- compute_name_overlap_rows()'s signature is depended on directly by
    # tests/test_compare_reference_name_overlap.py's unit tests, and
    # classify_name_config_collisions()'s segment-root-based signature is depended on
    # directly by tests/test_name_config_collision.py's classification tests; sharing one
    # load would require changing both signatures and rewriting both test suites.
    #
    # classify_name_config_collisions() takes exactly ONE target_export_run_id per call
    # (unlike compute_name_overlap_rows(), which reclassifies each all_detail_rows row using
    # that row's own target_export_run_id) -- so a whole-segment target comparison (--target
    # omitted) requires one call per distinct target file actually compared. Derived from
    # all_summary_rows, NOT all_detail_rows: a target file whose domain(s) hit
    # NO_REFERENCE_DEFINED (step_compare.py's own "no reference set exists for this domain at
    # all" shortcut -- e.g. --domains selects a domain absent from the reference) still gets a
    # summary/gap row per (view, target, domain) but zero detail rows (see step_compare.py's
    # own comment: "no detail rows are emitted"), so deriving from all_detail_rows would drop
    # that target file from name-config-collision scanning entirely even though its
    # underlying name-key evidence is otherwise valid (Codex review, PR #483). all_summary_rows
    # always carries one row per (purge_view, target, domain) actually compared, regardless of
    # comparison outcome -- see _finalize_view()'s own gap_rows -> summary_rows mapping.
    #
    # This re-loads each side's name-key CSVs once per target file rather than once per run --
    # a real, deliberately-accepted cost for whole-segment mode (a single-file --target run
    # still pays exactly the one extra load Greg already approved). Not something to silently
    # special-case away: an empty/aggregate target scope would reintroduce the exact
    # cross-file name contamination PR #476 fixed for the name-overlap classifier.
    #
    # classify_name_config_collisions() reads raw config join_hash values directly off
    # records.csv -- it has no idea `compatibility` (resolve_cross_segment_compatibility(),
    # the same gate run_comparisons() already enforced per domain above) exists, so passing
    # the full requested `domains` list would happily classify a domain whose join_hash
    # values were never proven comparable between the two sides (MATERIALIZATION_VERSION_
    # INCOMPATIBLE/_UNPROVEN or CROSS_SEGMENT_JOIN_POLICY_MISMATCH -- e.g. reference and
    # target segments extracted under different join-key policy versions), producing an
    # apparently-valid classification built from hashes that aren't actually the same
    # identity space (Codex review, PR #483). Restrict to domains this run already proved
    # "ok" -- the same domain set every other classification in this file already respects.
    collision_domains = [dom for dom in domains if compatibility.get(dom, {}).get("status") == "ok"]
    if include_name_config_collisions:
        name_config_collision_rows: List[Dict[str, str]] = []
        name_config_collision_config_rows: List[Dict[str, str]] = []
        target_export_run_ids_for_collisions = sorted({
            (row.get("target_export_run_id") or "").strip()
            for row in all_summary_rows
            if (row.get("target_export_run_id") or "").strip()
        }) or ([target_export_run_id] if target_export_run_id else [])
        for tgt_export_id in target_export_run_ids_for_collisions:
            rows, config_rows = classify_name_config_collisions(
                collision_domains,
                reference_segment_root or segment_root,
                segment_root,
                same_segment,
                str(reference.get("seed_export_run_id", "")),
                tgt_export_id,
            )
            name_config_collision_rows.extend(rows)
            name_config_collision_config_rows.extend(config_rows)
        name_config_collision_rows.sort(key=lambda r: (r["domain"], r["target_export_run_id"], r["name_hash"]))
        name_config_collision_config_rows.sort(
            key=lambda r: (r["domain"], r["target_export_run_id"], r["name_hash"], r["side"], r["config_hash"])
        )
        add_file_metadata_labels(
            name_config_collision_rows, target_label_lookup, reference_export_run_id, reference_label_lookup
        )
        atomic_write_csv(
            out_dir / NAME_CONFIG_COLLISION_FILENAME, _NAME_CONFIG_COLLISION_FIELDNAMES, name_config_collision_rows
        )
        atomic_write_csv(
            out_dir / NAME_CONFIG_COLLISION_CONFIGS_FILENAME,
            _NAME_CONFIG_COLLISION_CONFIGS_FIELDNAMES,
            name_config_collision_config_rows,
        )
        output_files.append(NAME_CONFIG_COLLISION_FILENAME)
        output_files.append(NAME_CONFIG_COLLISION_CONFIGS_FILENAME)

    statuses_by_key: Dict[Tuple[str, str], List[str]] = {}
    reasons_by_key: Dict[Tuple[str, str], List[str]] = {}
    for row in all_summary_rows:
        key = (row["purge_view"], row["domain"])
        statuses_by_key.setdefault(key, []).append(row["comparison_status"] or COMPARISON_STATUS_OK)
        reasons_by_key.setdefault(key, []).extend(split_reason_codes(row["comparison_reason_codes"]))

    key_level_status = {k: aggregate_comparison_status(v) for k, v in statuses_by_key.items()}
    run_status = aggregate_comparison_status(key_level_status.values()) if key_level_status else COMPARISON_STATUS_OK
    status_counts = Counter(key_level_status.values())

    domain_summaries = [
        {
            "purge_view": view,
            "domain": dom,
            "comparison_status": key_level_status[(view, dom)],
            "comparison_reason_codes": sorted({c for c in reasons_by_key[(view, dom)] if c}),
        }
        for (view, dom) in sorted(key_level_status.keys())
    ]
    target_diagnostics = sorted(
        (
            {
                "purge_view": row["purge_view"],
                "target_export_run_id": row["target_export_run_id"],
                "domain": row["domain"],
                "population_id": row["population_id"],
                "comparison_status": row["comparison_status"],
                "comparison_reason_codes": split_reason_codes(row["comparison_reason_codes"]),
            }
            for row in all_summary_rows
            if row["comparison_status"] and row["comparison_status"] != COMPARISON_STATUS_OK
        ),
        key=lambda r: (r["purge_view"], r["domain"], r["target_export_run_id"]),
    )

    diagnostics = {
        "segment_id": segment_id,
        "reference_segment_id": reference_segment_id,
        "analysis_run_id": run_id,
        "run_comparison_status": run_status,
        "run_comparison_reason_codes": sorted({c for codes in reasons_by_key.values() for c in codes if c}),
        "domains_total": str(len(key_level_status)),
        "domains_ok": str(status_counts.get(COMPARISON_STATUS_OK, 0)),
        "domains_degraded": str(status_counts.get(COMPARISON_STATUS_DEGRADED, 0)),
        "domains_blocked": str(status_counts.get(COMPARISON_STATUS_BLOCKED, 0)),
        "domain_summaries": domain_summaries,
        "target_diagnostics": target_diagnostics,
        "revit_name_resolution": {
            "reference_status": reference_name_status,
            "target_status": target_name_status,
            "status_counts": dict(sorted(Counter(
                value
                for row in all_detail_rows
                for value in (row["reference_revit_name_status"], row["target_revit_name_status"])
            ).items())),
        },
    }
    (out_dir / DIAGNOSTICS_FILENAME).write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = {
        "tool": "tools/compare_reference.py",
        "mode": "segment-native",
        "segment_id": segment_id,
        "reference_segment_id": reference_segment_id,
        "segment_output_folder": segment_root.name,
        "resolved_reference_export_run_id": reference.get("seed_export_run_id", ""),
        "resolved_target_export_run_id": target_export_run_id or "",
        "target_scope": "single_file" if target_export_run_id else "segment",
        "analysis_run_id": run_id,
        "extractor_schema_version": extractor_schema_version,
        "purge_views": list(views),
        "domains": list(domains),
        "output_files": output_files,
        "aggregate_comparison_status": run_status,
        "semantic_changes_skipped_reason": semantic_changes_skipped_reason,
        "name_overlap_included": include_name_overlap,
        "name_overlap_known_gaps": (
            [REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED] if include_name_overlap else []
        ),
        "name_config_collisions_included": include_name_config_collisions,
        "name_config_collisions_known_gaps": (
            [REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED] if include_name_config_collisions else []
        ),
    }
    (out_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def write_top_level_blocked(
    out_dir: Path,
    reason_code: str,
    detail: str,
    reference_segment_id: str,
    target_segment_id: str,
    target_selector: str,
    same_segment: bool,
    include_name_overlap: bool = True,
    include_name_config_collisions: bool = True,
) -> Dict[str, object]:
    """Written when a comparison could not even be attempted (segment not
    found, materialization incomplete, reference/target unresolved). Still
    produces exactly the standard 4-file output contract -- summary/detail
    are header-only, never omitted -- so the failure is never console-only.

    `reference_segment_id`/`target_segment_id` are equal in the same-segment
    case (mirroring the prior single `segment_id` value byte-for-byte); they
    differ only in cross-segment mode, where the failure could originate on
    either side.

    `semantic_changes_skipped_reason` remains present and empty on this
    pre-flight path. Cross-segment semantic reporting is supported; the
    top-level comparison failure itself explains why no rows were produced.

    `include_name_overlap`/`include_name_config_collisions` populate
    `name_overlap_included`/`name_overlap_known_gaps` and
    `name_config_collisions_included`/`name_config_collisions_known_gaps` the same way
    assemble_final_outputs() does, for the same reason: those manifest keys must never
    simply be absent just because this pre-flight path fired instead of reaching
    assemble_final_outputs() (PR #476 review, second round). Both `*_included` keys are
    always False here regardless of what was requested -- nothing was actually produced on
    a blocked run, and that field's contract is "was the file written," not "was it
    requested."
    """
    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, [])
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, [])
    diagnostics = {
        "segment_id": target_segment_id,
        "reference_segment_id": reference_segment_id,
        "analysis_run_id": "",
        "run_comparison_status": COMPARISON_STATUS_BLOCKED,
        "run_comparison_reason_codes": [reason_code],
        "run_comparison_detail": detail,
        "domains_total": "0",
        "domains_ok": "0",
        "domains_degraded": "0",
        "domains_blocked": "0",
        "domain_summaries": [],
        "target_diagnostics": [],
    }
    (out_dir / DIAGNOSTICS_FILENAME).write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = {
        "tool": "tools/compare_reference.py",
        "mode": "segment-native",
        "segment_id": target_segment_id,
        "reference_segment_id": reference_segment_id,
        "resolved_reference_export_run_id": "",
        "resolved_target_export_run_id": target_selector or "",
        "output_files": [SUMMARY_FILENAME, DETAIL_FILENAME, DIAGNOSTICS_FILENAME],
        "aggregate_comparison_status": COMPARISON_STATUS_BLOCKED,
        "semantic_changes_skipped_reason": "",
        "name_overlap_included": False,
        "name_overlap_known_gaps": (
            [REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED] if include_name_overlap else []
        ),
        "name_config_collisions_included": False,
        "name_config_collisions_known_gaps": (
            [REASON_NAME_KEY_POLICY_VERSIONING_NOT_IMPLEMENTED] if include_name_config_collisions else []
        ),
    }
    (out_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


# ---------------------------------------------------------------------------
# --out-dir ownership / overwrite semantics (unchanged from prior tool
# behavior -- still a purely orchestration-level concern, not comparison
# mathematics)
# ---------------------------------------------------------------------------


def check_out_dir_safety(out_dir: Path, segments_root: Path, registry_file: Path) -> None:
    """--out-dir is unconditionally cleared on every run (see
    prepare_out_dir). If it were the same as, or an ancestor of, the
    segments root or the run registry file, that clearing would destroy
    already-materialized segment data this tool only ever reads.
    """
    resolved_out = out_dir.resolve()
    for candidate in (segments_root.resolve(), registry_file.resolve(), registry_file.resolve().parent):
        if resolved_out == candidate or resolved_out in candidate.parents:
            raise CompareReferenceError(
                REASON_OUT_DIR_UNSAFE,
                f"--out-dir ({resolved_out}) is the same as, or an ancestor of, {candidate}. This tool "
                "clears --out-dir on every run, which would destroy segment materialization it only "
                "ever reads. Choose a --out-dir outside --segments-root and --registry-file.",
            )


def prepare_out_dir(out_dir: Path, overwrite: bool) -> None:
    """Deterministic overwrite policy: each invocation cleanly REPLACES
    --out-dir. --out-dir is treated as owned exclusively by this tool -- if
    it already exists, is non-empty, and does not carry this tool's own
    manifest from a prior run, refuse to clear it unless --overwrite is
    passed explicitly.
    """
    if out_dir.exists():
        if any(out_dir.iterdir()):
            manifest_path = out_dir / MANIFEST_FILENAME
            if not manifest_path.is_file() and not overwrite:
                raise CompareReferenceError(
                    REASON_OUT_DIR_UNSAFE,
                    f"--out-dir already exists and was not produced by a prior run of this tool: {out_dir}. "
                    "This tool always cleanly replaces its output directory rather than merging across runs. "
                    "Pass --overwrite to replace it anyway, or choose an empty directory.",
                )
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="compare_reference.py",
        description=(
            "Compare a reference fingerprint export against one target export, or an entire "
            "materialized segment, using the segment-local outputs tools/run_segment_orchestrator.py "
            "already produced. Reuses the existing reference-vs-target comparison implementation "
            "(tools/bundle_analysis/step_compare.py::run_compare_for_domain) directly -- no comparison "
            "mathematics is implemented here, and no fingerprint JSON is read, staged, or re-extracted. "
            "A reference is a comparison anchor only: not a standard, approved content, or a compliance "
            "requirement. See docs/reference_comparison_tool.md."
        ),
    )
    ap.add_argument("--segments-root", required=True, type=Path, help="Root directory containing every segment's output folder.")
    ap.add_argument("--registry-file", required=True, type=Path, help="Corpus-level run_registry.csv (segment_id -> output_folder/status).")
    ap.add_argument(
        "--reference-segment",
        required=True,
        help="Normalized segment folder name (run_registry.csv's own output_folder column, e.g. "
        "'imperial_template_architectural' -- not the raw pipe-delimited segment_id) the --reference "
        "selector and the reference pattern set are resolved against (matched against --registry-file).",
    )
    ap.add_argument(
        "--target-segment",
        default=None,
        help="Normalized segment folder name (see --reference-segment) the --target selector (or, if --target "
        "is omitted, the whole-segment comparison) is resolved against. "
        "Default: the same segment as --reference-segment.",
    )
    ap.add_argument("--reference", required=True, help="Reference export filename selector, resolved against the reference segment's own file_metadata.csv.")
    ap.add_argument("--target", default=None, help="Target export filename selector. Omit to compare against the entire target segment.")
    ap.add_argument("--out-dir", required=True, type=Path, help="Output directory for this tool's own artifacts (owned exclusively by this tool -- see --overwrite).")
    ap.add_argument("--overwrite", action="store_true", help="Allow clearing --out-dir even if it wasn't produced by a prior run of this tool.")
    ap.add_argument("--domains", default=None, help="Comma-separated domain list. Default: every domain present in the target segment's pattern_presence_file.csv.")
    ap.add_argument("--purge-view", choices=["all", "used", "both"], default="both", help="Which segment-local bundle-analysis view(s) to compare against. Default: both.")
    ap.add_argument(
        "--no-name-overlap",
        dest="include_name_overlap",
        action="store_false",
        default=True,
        help="Skip reference_comparison_name_overlap.csv (written by default): for each "
        "pattern, classify the relationship between the reference side's and target side's "
        "name-key join_hash sets (name_sets_identical/overlap/disjoint, or "
        "name_evidence_excluded/missing). Fail-soft: a segment missing name-key "
        "materialization (see tools/apply_name_key_policy.py / the -NameKey runbook switch) "
        "still gets its ordinary config-identity comparison; only the name-overlap file "
        "degrades to name_evidence_missing rows for the affected side. See "
        "docs/namekey_crosssegment_step0_findings.md.",
    )
    ap.add_argument(
        "--no-name-config-collisions",
        dest="include_name_config_collisions",
        action="store_false",
        default=True,
        help="Skip reference_comparison_name_config_collisions.csv (written by default): the "
        "inverse question to --no-name-overlap -- for each name observed on either side, "
        "classify the relationship between the reference side's and target side's config "
        "join_hash sets for that name identity (config_sets_identical/overlap/disjoint, or "
        "name_evidence_excluded/missing/name_ambiguous_within_side). Surfaces the hazard "
        "where the same name legitimately resolves to two unrelated configs. Fail-soft, same "
        "as --no-name-overlap. See tools/name_config_collision.py.",
    )
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    segments_root = Path(args.segments_root).resolve()
    registry_file = Path(args.registry_file).resolve()
    out_dir = Path(args.out_dir).resolve()
    views: List[str] = list(VALID_PURGE_VIEWS) if args.purge_view == "both" else [args.purge_view]

    # Raw CLI selectors (normalized folder names -- see resolve_segment()),
    # used only for segment lookup and the same-segment check below, and as
    # the best-effort label in the pre-resolution-failure output path
    # (write_top_level_blocked, in the except block): if resolution itself
    # fails, there is no canonical segment_id to report instead.
    reference_segment_selector = args.reference_segment
    target_segment_selector = args.target_segment if args.target_segment else args.reference_segment
    # Same-segment invocations must reproduce the tool's pre-cross-segment
    # behavior byte-for-byte: every branch below reuses the reference
    # segment's already-resolved root/paths/rows instead of re-resolving or
    # re-reading, and the new cross-segment compatibility gate
    # (resolve_cross_segment_compatibility) is never invoked in this case.
    # Comparing the raw folder selectors (rather than resolved canonical
    # segment_ids) is correct here: output_folder is already the unique,
    # on-disk identifier for a materialized segment, so two equal selectors
    # always mean the same segment and two different ones never do.
    same_segment = target_segment_selector == reference_segment_selector
    # Fallback labels for the pre-flight-failure output path below
    # (write_top_level_blocked, in the except block), in case resolve_segment()
    # itself fails before either canonical segment_id is known. Each is
    # overwritten with the real canonical segment_id immediately after its
    # own successful resolve_segment() call.
    reference_segment_id = reference_segment_selector
    target_segment_id = target_segment_selector

    try:
        check_out_dir_safety(out_dir, segments_root, registry_file)
        prepare_out_dir(out_dir, overwrite=args.overwrite)
    except CompareReferenceError as exc:
        print(f"[compare_reference][error] {exc.reason_code}: {exc}", file=sys.stderr)
        return 2

    try:
        # reference_segment_id/target_segment_id (reassigned below, once
        # resolution succeeds) are each segment's own canonical segment_id
        # from run_registry.csv -- NOT the folder-name selector above -- so
        # every output field downstream (summary/detail CSVs, manifest,
        # diagnostics) keeps joining to segment_manifest.csv/run_registry.csv
        # by segment_id, exactly as before this tool accepted a folder-name
        # selector (Codex review, PR #475).
        reference_segment_root, _ref_status, reference_segment_id = resolve_segment(
            registry_file, segments_root, reference_segment_selector
        )
        reference_paths = require_segment_artifacts(
            reference_segment_root, views, require_domain_patterns=not same_segment
        )
        reference_file_metadata_rows = read_csv_rows(reference_paths["records_dir"] / "file_metadata.csv")

        if same_segment:
            target_segment_root = reference_segment_root
            target_segment_id = reference_segment_id
            target_paths = reference_paths
            target_file_metadata_rows = reference_file_metadata_rows
        else:
            target_segment_root, _tgt_status, target_segment_id = resolve_segment(
                registry_file, segments_root, target_segment_selector
            )
            target_paths = require_segment_artifacts(target_segment_root, views, require_domain_patterns=True)
            target_file_metadata_rows = read_csv_rows(target_paths["records_dir"] / "file_metadata.csv")

        # Validate file_metadata.csv's export_run_id uniqueness up front:
        # build_file_metadata_label_lookup() raises
        # FILE_METADATA_LABEL_JOIN_AMBIGUOUS on a duplicate export_run_id with
        # disagreeing label values. Doing this here, inside this try block,
        # gives that failure the same clean write_top_level_blocked + exit-2
        # handling as every other CompareReferenceError, instead of letting it
        # escape as an uncaught traceback from inside assemble_final_outputs()
        # (called after this try/except, once comparisons have already run) --
        # PR #478 review. assemble_final_outputs() re-runs the same lookup
        # build on this already-validated data, redundantly but harmlessly.
        build_file_metadata_label_lookup(reference_file_metadata_rows)
        if not same_segment:
            build_file_metadata_label_lookup(target_file_metadata_rows)

        if not same_segment:
            reference_unit_system = read_segment_unit_system(reference_file_metadata_rows)
            target_unit_system = read_segment_unit_system(target_file_metadata_rows)
            if not reference_unit_system or not target_unit_system or reference_unit_system != target_unit_system:
                raise CompareReferenceError(
                    REASON_CROSS_SEGMENT_UNIT_SYSTEM_MISMATCH,
                    f"reference segment {reference_segment_id!r} unit_system={reference_unit_system!r} does not "
                    f"match (or either is absent/non-uniform in) target segment {target_segment_id!r} "
                    f"unit_system={target_unit_system!r} -- join_hashes for the same logical pattern differ "
                    "between unit systems because behavioral hashes include unit-bearing values.",
                )

        reference_export_run_id = resolve_export_run_id(
            "--reference", args.reference, reference_file_metadata_rows, REASON_REFERENCE_NOT_MATERIALIZED, REASON_REFERENCE_AMBIGUOUS
        )
        target_export_run_id: Optional[str] = None
        if args.target:
            target_export_run_id = resolve_export_run_id(
                "--target", args.target, target_file_metadata_rows, REASON_TARGET_NOT_MATERIALIZED, REASON_TARGET_AMBIGUOUS
            )

        reference_presence_rows = read_csv_rows(reference_paths["analysis_dir"] / "pattern_presence_file.csv")
        target_presence_rows = (
            reference_presence_rows if same_segment else read_csv_rows(target_paths["analysis_dir"] / "pattern_presence_file.csv")
        )

        # domains (when --domains isn't explicit) always default from the
        # *target* segment's pattern_presence_file.csv -- that's the
        # population actually being scored, matching current single-segment
        # semantics exactly when reference_segment == target_segment.
        run_id, domains = resolve_run_id_and_domains(target_presence_rows, args.domains)
        reference_run_id = run_id if same_segment else resolve_reference_run_id(reference_presence_rows)

        extractor_schema_version = read_extractor_schema_version(reference_paths["analysis_dir"])
        if not same_segment:
            target_extractor_schema_version = read_extractor_schema_version(target_paths["analysis_dir"])
            if (
                not extractor_schema_version
                or not target_extractor_schema_version
                or extractor_schema_version != target_extractor_schema_version
            ):
                raise CompareReferenceError(
                    REASON_CROSS_SEGMENT_SCHEMA_MISMATCH,
                    f"reference segment {reference_segment_id!r} extractor_schema_version="
                    f"{extractor_schema_version!r} does not match (or either is absent from) target segment "
                    f"{target_segment_id!r} extractor_schema_version={target_extractor_schema_version!r} -- "
                    "comparing pattern evidence materialized under different extractor schema versions is not "
                    "supported.",
                )
        reference = build_reference(reference_presence_rows, reference_run_id, reference_export_run_id, extractor_schema_version)
        if not reference["domains"]:
            # Mirrors reference_bundle.py::write_sidecar's own rejection of a
            # globally empty reference (the --seed path never persists a
            # reference_bundle.json with no domains at all). Without this,
            # every requested domain would report ok/REFERENCE_DOMAIN_UNDEFINED
            # and the run would exit successfully despite comparing nothing
            # meaningful (Codex review, PR #467).
            raise CompareReferenceError(
                REASON_REFERENCE_HAS_NO_PATTERNS,
                f"reference {reference_export_run_id!r} has zero pattern_id evidence in its segment's "
                "pattern_presence_file.csv, across every domain -- there is nothing to compare against.",
            )

        target_compatibility = check_domain_compatibility(target_paths["records_dir"] / "records.csv", domains)
        pattern_identity_status: Optional[Dict[str, Dict[str, object]]] = None
        target_jh_maps: Optional[Dict[str, Dict[str, str]]] = None
        if same_segment:
            compatibility = target_compatibility
        else:
            reference_compatibility = check_domain_compatibility(reference_paths["records_dir"] / "records.csv", domains)
            compatibility = resolve_cross_segment_compatibility(
                target_compatibility, reference_compatibility, domains, reference["domains"]
            )
            # pattern_id is segment-local; translate both sides to the
            # cross-segment-stable join_hash identity before any domain is
            # compared. Mutates reference["domains"] in place for every
            # domain that resolves.
            pattern_identity_status, target_jh_maps = resolve_cross_segment_pattern_identity(
                reference_segment_root, target_segment_root, domains, reference["domains"]
            )

        all_export_run_ids = sorted(
            {
                (r.get("export_run_id", "") or "").strip()
                for r in target_file_metadata_rows
                if (r.get("export_run_id", "") or "").strip()
            }
        )

        requested_targets = {target_export_run_id} if target_export_run_id else set(all_export_run_ids)
        # The reference-itself exclusion only makes sense in same-segment
        # mode, where reference_export_run_id and the target's export_run_id
        # values share one real namespace (the reference genuinely can BE one
        # of the segment's own files). In cross-segment mode the two
        # identifiers come from two independent segments -- subtracting
        # reference_export_run_id there would incorrectly drop a legitimate
        # target file whose export_run_id string merely happens to collide
        # with the reference's (Codex review, PR #471); this tool's own
        # comparator call already neutralizes the analogous exclusion inside
        # run_compare_for_domain for the same reason (see run_comparisons's
        # comparator_reference).
        effective_targets = (requested_targets - {reference_export_run_id}) if same_segment else requested_targets
        if not effective_targets:
            # Either --target resolved to the reference itself (same-segment
            # only), or the target segment has no (other, for same-segment)
            # materialized files at all. The comparator's own
            # seed_export_run_id exclusion would silently produce zero gap
            # rows for every domain in this case -- which must never
            # roll up to an unearned "ok" (Codex review, PR #467): there is
            # nothing to compare, so the run blocks explicitly instead.
            raise CompareReferenceError(
                REASON_NO_COMPARISON_TARGETS,
                "no comparison target remains"
                + (" after excluding the reference itself" if same_segment else "")
                + f" (reference={reference_export_run_id!r}, requested targets={sorted(requested_targets)!r}).",
            )
    except CompareReferenceError as exc:
        write_top_level_blocked(
            out_dir, exc.reason_code, str(exc), reference_segment_id, target_segment_id, args.target or "", same_segment,
            include_name_overlap=args.include_name_overlap,
            include_name_config_collisions=args.include_name_config_collisions,
        )
        print(f"[compare_reference][error] {exc.reason_code}: {exc}", file=sys.stderr)
        return 2

    run_comparisons(
        run_id,
        target_paths["analysis_dir"],
        target_paths["bundle_dir"],
        reference,
        domains,
        views,
        compatibility,
        target_export_run_id,
        all_export_run_ids,
        out_dir,
        pattern_identity_status=pattern_identity_status,
        target_jh_maps=target_jh_maps,
    )

    manifest = assemble_final_outputs(
        out_dir,
        target_segment_id,
        target_segment_root,
        run_id,
        domains,
        views,
        reference,
        target_export_run_id,
        extractor_schema_version,
        reference_segment_id,
        same_segment,
        reference_file_metadata_rows,
        target_file_metadata_rows,
        compatibility,
        reference_segment_root=reference_segment_root,
        include_name_overlap=args.include_name_overlap,
        include_name_config_collisions=args.include_name_config_collisions,
    )
    print(f"[compare_reference] comparison_status={manifest['aggregate_comparison_status']}")
    print(f"[compare_reference] wrote outputs to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
