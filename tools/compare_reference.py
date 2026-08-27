#!/usr/bin/env python3
"""tools/compare_reference.py

Compare a reference fingerprint export against either one target export or
an entire already-materialized segment, using the segment-local outputs
tools/run_segment_orchestrator.py already produced for that segment.

This module implements NO comparison mathematics of its own. It is a thin
orchestration/output layer over the existing, authoritative comparison
implementation, tools/bundle_analysis/step_compare.py::run_compare_for_domain
(and its ok/degraded/blocked reliability semantics in
tools/bundle_analysis/comparison_status.py), called in-process -- there
remains exactly one implementation of comparison semantics.

Unlike the tool's previous incarnation, this version does NOT read, stage,
flatten, or otherwise process fingerprint export JSON, and does NOT invoke
tools/run_extract_all.py or any other extraction/pattern/join-key
subprocess. A reference or target is selected by an export filename, but
that filename is only ever matched against a segment's own already-produced
results/records/file_metadata.csv -- the JSON files themselves are never
opened. If the requested segment's materialization is missing, incomplete,
or a filename cannot be resolved unambiguously, the comparison blocks
explicitly rather than falling back to any JSON-driven path.

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

MANIFEST_FILENAME = "reference_comparison_report.json"
SUMMARY_FILENAME = "reference_comparison_summary.csv"
DETAIL_FILENAME = "reference_comparison_detail.csv"
DIAGNOSTICS_FILENAME = "reference_comparison_diagnostics.json"

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
# Pure pre-flight/out-dir-safety failures -- raised before --out-dir can be
# safely prepared at all, so no diagnostics file is ever written for these.
REASON_OUT_DIR_UNSAFE = "OUT_DIR_UNSAFE"

_SUMMARY_FIELDNAMES = [
    "segment_id",
    "purge_view",
    "reference_bundle_id",
    "analysis_run_id",
    "target_export_run_id",
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
    "analysis_run_id",
    "target_export_run_id",
    "domain",
    "population_id",
    "pattern_id",
    "comparison_class",
]

# Suffixes checked most-specific first, matching tools/extractor.py's own
# split-export/file-format convention, so a filename selector like
# "model.details.json" resolves to the same export identity as
# "model.index.json" (the canonical export_run_id whenever both exist).
_EXPORT_SUFFIXES = (".details.json", ".index.json", "__fingerprint.json", ".legacy.json", ".json")


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


def resolve_segment(registry_file: Path, segments_root: Path, segment_id: str) -> Tuple[Path, str]:
    """Resolve `segment_id` to its materialized segment_root, using
    run_registry.csv (the authoritative per-segment completion signal --
    see tools/run_segment_orchestrator.py's own already_satisfied logic,
    which is exactly `status == "complete"`). Never treats file existence
    alone as proof of completeness.
    """
    if not registry_file.is_file():
        raise CompareReferenceError(REASON_SEGMENT_NOT_FOUND, f"--registry-file not found: {registry_file}")
    rows = read_csv_rows(registry_file)
    matches = [r for r in rows if (r.get("segment_id", "") or "").strip() == segment_id]
    if not matches:
        raise CompareReferenceError(REASON_SEGMENT_NOT_FOUND, f"segment_id {segment_id!r} not found in {registry_file}")
    if len(matches) > 1:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND,
            f"segment_id {segment_id!r} has {len(matches)} rows in {registry_file} (expected exactly one).",
        )
    row = matches[0]
    output_folder = (row.get("output_folder", "") or "").strip()
    status = (row.get("status", "") or "").strip()
    if not output_folder:
        raise CompareReferenceError(
            REASON_SEGMENT_NOT_FOUND, f"segment_id {segment_id!r} has no output_folder recorded in {registry_file}."
        )
    if status != "complete":
        raise CompareReferenceError(
            REASON_SEGMENT_MATERIALIZATION_INCOMPLETE,
            f"segment_id {segment_id!r} run_registry.csv status={status!r} (expected 'complete'); "
            "this segment's materialization is not known-complete.",
        )
    return segments_root / output_folder, status


def require_segment_artifacts(segment_root: Path, views: Sequence[str]) -> Dict[str, Path]:
    """Confirm the segment-wide artifacts every comparison needs are actually
    present on disk, despite run_registry.csv reporting status=complete
    (internal-consistency check, not a substitute for that status check).
    Never silently regenerates a missing artifact.
    """
    records_dir = segment_root / "results" / "records"
    analysis_dir = segment_root / "results" / "analysis"
    bundle_dir = segment_root / "results" / "bundle_analysis"

    required_files = {
        "records.csv": records_dir / "records.csv",
        "file_metadata.csv": records_dir / "file_metadata.csv",
        "pattern_presence_file.csv": analysis_dir / "pattern_presence_file.csv",
    }
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


def read_extractor_schema_version(analysis_dir: Path) -> str:
    path = analysis_dir / "corpus_manifest.csv"
    if not path.is_file():
        return ""
    rows = read_csv_rows(path)
    versions = {(r.get("schema_version", "") or "").strip() for r in rows if (r.get("schema_version", "") or "").strip()}
    return next(iter(versions)) if len(versions) == 1 else ""


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
    domain with more than one distinct non-blank tuple indicates an
    internally-inconsistent materialization (block); a domain with none
    populated at all means compatibility cannot be established from what is
    persisted today (block, distinctly). A single, consistent, non-blank
    tuple is the only "ok" case. Structured as a per-domain tuple set so a
    future artifact-SHA field can be folded into the same tuple without
    changing this function's shape or callers.
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
        non_blank = sorted(k for k in seen[dom] if any(k))
        if len(non_blank) > 1:
            result[dom] = {"status": "incompatible", "values": non_blank}
        elif not non_blank:
            result[dom] = {"status": "unproven", "values": []}
        else:
            result[dom] = {"status": "ok", "values": non_blank}
    return result


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
) -> Dict[str, List[Dict[str, str]]]:
    per_view_summaries: Dict[str, List[Dict[str, str]]] = {}
    eligible = {target_export_run_id} if target_export_run_id else set(all_export_run_ids)

    for view in views:
        compare_out_dir = out_dir / f"compare_{view}"
        view_summaries: List[Dict[str, str]] = []
        for dom in domains:
            compat = compatibility[dom]
            if compat["status"] != "ok":
                reason_code = (
                    REASON_MATERIALIZATION_VERSION_INCOMPATIBLE
                    if compat["status"] == "incompatible"
                    else REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN
                )
                detail = f"join_key_schema/policy compatibility for domain={dom!r}: {compat['values']}"
                write_blocked_gap_placeholder(
                    compare_out_dir, reference, run_id, dom, "", reason_code, detail, match_any_population=True
                )
                view_summaries.append(_synthesized_blocked_summary(reference, run_id, dom, reason_code, detail))
                continue
            summary = run_compare_for_domain(
                analysis_dir,
                bundle_dir / view,
                reference,
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
    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, all_summary_rows)
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, all_detail_rows)

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
        "analysis_run_id": run_id,
        "run_comparison_status": run_status,
        "run_comparison_reason_codes": sorted({c for codes in reasons_by_key.values() for c in codes if c}),
        "domains_total": str(len(key_level_status)),
        "domains_ok": str(status_counts.get(COMPARISON_STATUS_OK, 0)),
        "domains_degraded": str(status_counts.get(COMPARISON_STATUS_DEGRADED, 0)),
        "domains_blocked": str(status_counts.get(COMPARISON_STATUS_BLOCKED, 0)),
        "domain_summaries": domain_summaries,
        "target_diagnostics": target_diagnostics,
    }
    (out_dir / DIAGNOSTICS_FILENAME).write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = {
        "tool": "tools/compare_reference.py",
        "mode": "segment-native",
        "segment_id": segment_id,
        "segment_output_folder": segment_root.name,
        "resolved_reference_export_run_id": reference.get("seed_export_run_id", ""),
        "resolved_target_export_run_id": target_export_run_id or "",
        "target_scope": "single_file" if target_export_run_id else "segment",
        "analysis_run_id": run_id,
        "extractor_schema_version": extractor_schema_version,
        "purge_views": list(views),
        "domains": list(domains),
        "output_files": [SUMMARY_FILENAME, DETAIL_FILENAME, DIAGNOSTICS_FILENAME],
        "aggregate_comparison_status": run_status,
    }
    (out_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def write_top_level_blocked(
    out_dir: Path,
    reason_code: str,
    detail: str,
    segment_id: str,
    target_selector: str,
) -> Dict[str, object]:
    """Written when a comparison could not even be attempted (segment not
    found, materialization incomplete, reference/target unresolved). Still
    produces exactly the standard 4-file output contract -- summary/detail
    are header-only, never omitted -- so the failure is never console-only.
    """
    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, [])
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, [])
    diagnostics = {
        "segment_id": segment_id,
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
        "segment_id": segment_id,
        "resolved_reference_export_run_id": "",
        "resolved_target_export_run_id": target_selector or "",
        "output_files": [SUMMARY_FILENAME, DETAIL_FILENAME, DIAGNOSTICS_FILENAME],
        "aggregate_comparison_status": COMPARISON_STATUS_BLOCKED,
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
    ap.add_argument("--segment", required=True, help="segment_id to compare within (matched against --registry-file).")
    ap.add_argument("--reference", required=True, help="Reference export filename selector, resolved against the segment's own file_metadata.csv.")
    ap.add_argument("--target", default=None, help="Target export filename selector. Omit to compare against the entire segment.")
    ap.add_argument("--out-dir", required=True, type=Path, help="Output directory for this tool's own artifacts (owned exclusively by this tool -- see --overwrite).")
    ap.add_argument("--overwrite", action="store_true", help="Allow clearing --out-dir even if it wasn't produced by a prior run of this tool.")
    ap.add_argument("--domains", default=None, help="Comma-separated domain list. Default: every domain present in the segment's pattern_presence_file.csv.")
    ap.add_argument("--purge-view", choices=["all", "used", "both"], default="both", help="Which segment-local bundle-analysis view(s) to compare against. Default: both.")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    segments_root = Path(args.segments_root).resolve()
    registry_file = Path(args.registry_file).resolve()
    out_dir = Path(args.out_dir).resolve()
    views: List[str] = list(VALID_PURGE_VIEWS) if args.purge_view == "both" else [args.purge_view]

    try:
        check_out_dir_safety(out_dir, segments_root, registry_file)
        prepare_out_dir(out_dir, overwrite=args.overwrite)
    except CompareReferenceError as exc:
        print(f"[compare_reference][error] {exc.reason_code}: {exc}", file=sys.stderr)
        return 2

    try:
        segment_root, _status = resolve_segment(registry_file, segments_root, args.segment)
        paths = require_segment_artifacts(segment_root, views)
        file_metadata_rows = read_csv_rows(paths["records_dir"] / "file_metadata.csv")

        reference_export_run_id = resolve_export_run_id(
            "--reference", args.reference, file_metadata_rows, REASON_REFERENCE_NOT_MATERIALIZED, REASON_REFERENCE_AMBIGUOUS
        )
        target_export_run_id: Optional[str] = None
        if args.target:
            target_export_run_id = resolve_export_run_id(
                "--target", args.target, file_metadata_rows, REASON_TARGET_NOT_MATERIALIZED, REASON_TARGET_AMBIGUOUS
            )

        presence_rows = read_csv_rows(paths["analysis_dir"] / "pattern_presence_file.csv")
        run_id, domains = resolve_run_id_and_domains(presence_rows, args.domains)
        extractor_schema_version = read_extractor_schema_version(paths["analysis_dir"])
        reference = build_reference(presence_rows, run_id, reference_export_run_id, extractor_schema_version)
        compatibility = check_domain_compatibility(paths["records_dir"] / "records.csv", domains)
        all_export_run_ids = sorted(
            {(r.get("export_run_id", "") or "").strip() for r in file_metadata_rows if (r.get("export_run_id", "") or "").strip()}
        )
    except CompareReferenceError as exc:
        write_top_level_blocked(out_dir, exc.reason_code, str(exc), args.segment, args.target or "")
        print(f"[compare_reference][error] {exc.reason_code}: {exc}", file=sys.stderr)
        return 2

    run_comparisons(
        run_id,
        paths["analysis_dir"],
        paths["bundle_dir"],
        reference,
        domains,
        views,
        compatibility,
        target_export_run_id,
        all_export_run_ids,
        out_dir,
    )

    manifest = assemble_final_outputs(
        out_dir, args.segment, segment_root, run_id, domains, views, reference, target_export_run_id, extractor_schema_version
    )
    print(f"[compare_reference] comparison_status={manifest['aggregate_comparison_status']}")
    print(f"[compare_reference] wrote outputs to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
