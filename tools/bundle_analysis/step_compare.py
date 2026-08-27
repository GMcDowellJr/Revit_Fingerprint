from __future__ import annotations

import threading
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    from common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id
    from comparison_status import (
        COMPARISON_STATUS_OK,
        COMPARISON_STATUS_DEGRADED,
        COMPARISON_STATUS_BLOCKED,
        REASON_REFERENCE_DOMAIN_UNDEFINED,
        REASON_TARGET_DOMAIN_UNAVAILABLE,
        REASON_TARGET_DOMAIN_DEGRADED,
        REASON_TARGET_IDENTITY_INVALID,
        REASON_COMPARISON_INPUT_INVALID,
        aggregate_comparison_status,
        join_reason_codes,
    )
else:
    from .common import atomic_write_csv, read_csv_rows, resolve_analysis_run_id
    from .comparison_status import (
        COMPARISON_STATUS_OK,
        COMPARISON_STATUS_DEGRADED,
        COMPARISON_STATUS_BLOCKED,
        REASON_REFERENCE_DOMAIN_UNDEFINED,
        REASON_TARGET_DOMAIN_UNAVAILABLE,
        REASON_TARGET_DOMAIN_DEGRADED,
        REASON_TARGET_IDENTITY_INVALID,
        REASON_COMPARISON_INPUT_INVALID,
        aggregate_comparison_status,
        join_reason_codes,
    )

# `_GAP_FIELDNAMES` keeps the original one-way reference-coverage fields
# (patterns_required/present/missing, gap_pattern_ids, coverage_pct,
# coverage_status) for backward compatibility, and adds the symmetric
# set-comparison fields (reference/target/shared/*_only, union, jaccard).
# See tools/bundle_analysis/README.md "Reference Bundle Comparison" for the
# field-by-field contract, including zero-denominator behavior.
_GAP_FIELDNAMES = [
    "reference_bundle_id",
    "effective_date",
    "analysis_run_id",
    "domain",
    "population_id",
    "export_run_id",
    "patterns_required",
    "patterns_present",
    "patterns_missing",
    "gap_pattern_ids",
    "coverage_pct",
    "coverage_status",
    "reference_pattern_count",
    "target_pattern_count",
    "shared_count",
    "reference_only_count",
    "target_only_count",
    "union_count",
    "shared_pattern_ids",
    "reference_only_pattern_ids",
    "target_only_pattern_ids",
    "reference_coverage_pct",
    "jaccard",
    # PR2: explicit comparison reliability semantics (see comparison_status.py).
    # Distinct from coverage_status: coverage_status describes how much of the
    # reference set the target covers; comparison_status describes whether
    # there was enough trustworthy evidence to make that coverage computation meaningful.
    "comparison_status",
    "comparison_reason_codes",
    "comparison_detail",
]
_GAP_REPORT_LOCK = threading.Lock()

# Fields whose values are derived from target-side evidence and must not carry
# plausible-looking placeholder values (zero counts, 0%/100% ratios) when
# comparison_status is "blocked" -- the target evidence needed to compute them
# was insufficient. `patterns_required` / `reference_pattern_count` are excluded:
# those describe the reference side only, which is unaffected by target-evidence
# reliability and remains a true fact even when the target can't be compared.
_TARGET_DERIVED_BLANK_ON_BLOCKED = [
    "patterns_present",
    "patterns_missing",
    "gap_pattern_ids",
    "coverage_pct",
    "coverage_status",
    "target_pattern_count",
    "shared_count",
    "reference_only_count",
    "target_only_count",
    "union_count",
    "shared_pattern_ids",
    "reference_only_pattern_ids",
    "target_only_pattern_ids",
    "reference_coverage_pct",
    "jaccard",
]


def _safe_pct(value: object) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _classify_target_evidence(presence_rows_for_file: List[Dict[str, str]]) -> Tuple[str, List[str], str]:
    """Classify a (export_run_id, domain) target's evidence reliability.

    Returns (comparison_status, reason_codes, detail). Uses presence rows'
    `pattern_id` column -- when populated (the current production
    pattern_presence_file.csv schema), an empty pattern_id marks the
    "UNKNOWN" bucket (records present but unassignable to any pattern, e.g.
    missing/invalid join identity; see tools/extractor.py::_process_one_domain).
    A schema without a `pattern_id` column at all (older/minimal presence
    exports) carries no per-pattern granularity to classify from, so presence
    alone is treated as sufficient evidence of a genuine (possibly empty)
    observation -- this is the pre-PR2 behavior, preserved exactly.
    """
    has_granularity = any("pattern_id" in row for row in presence_rows_for_file)
    if not has_granularity:
        return COMPARISON_STATUS_OK, [], ""

    has_known = any((row.get("pattern_id") or "").strip() for row in presence_rows_for_file)
    unknown_rows = [row for row in presence_rows_for_file if "pattern_id" in row and not (row.get("pattern_id") or "").strip()]
    has_unknown = bool(unknown_rows)

    if has_unknown and not has_known:
        return COMPARISON_STATUS_BLOCKED, [REASON_TARGET_IDENTITY_INVALID], (
            "all target records for this domain/file have unassignable (missing/invalid) join identity"
        )
    if has_unknown:
        unknown_share = sum(_safe_pct(row.get("pattern_share_pct", "0")) for row in unknown_rows)
        return COMPARISON_STATUS_DEGRADED, [REASON_TARGET_DOMAIN_DEGRADED], f"identity_unknown_share={unknown_share:.6f}"
    return COMPARISON_STATUS_OK, [], ""

# One row per reference x target x domain x pattern_id, classified into
# exactly one of "shared" / "reference_only" / "target_only". Rows are only
# emitted when a reference is actually defined for the domain (see
# NO_REFERENCE_DEFINED handling below) -- there is no comparison class to
# assign a pattern_id to when there is no reference set to compare against.
_DETAIL_FIELDNAMES = [
    "reference_bundle_id",
    "analysis_run_id",
    "domain",
    "population_id",
    "export_run_id",
    "pattern_id",
    "comparison_class",
]


def _compute_comparison_rows(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
    population_id: str = "",
    eligible_export_run_ids: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, "")
    membership_rows = read_csv_rows(out_dir / domain / "membership_matrix.csv")
    seed_export_run_id = str(reference.get("seed_export_run_id", "")).strip()
    if eligible_export_run_ids is None:
        all_export_ids = sorted(
            {
                str(row.get("export_run_id", "")).strip()
                for row in presence_rows
                if row.get("analysis_run_id", "") == run_id
                and row.get("domain", "") == domain
                and str(row.get("export_run_id", "")).strip()
                and str(row.get("export_run_id", "")).strip() != seed_export_run_id
            }
        )
    else:
        all_export_ids = sorted(
            {
                export_run_id.strip()
                for export_run_id in eligible_export_run_ids
                if export_run_id.strip() and export_run_id.strip() != seed_export_run_id
            }
        )
    required_patterns = {
        str(pid).strip()
        for pid in (reference.get("domains", {}) or {}).get(domain, [])
        if str(pid).strip()
    }

    present_by_export: Dict[str, Set[str]] = {}
    for row in membership_rows:
        if row.get("analysis_run_id", "") != run_id:
            continue
        export_run_id = str(row.get("export_run_id", "")).strip()
        pattern_id = str(row.get("pattern_id", "")).strip()
        if not export_run_id or not pattern_id:
            continue
        if seed_export_run_id and export_run_id == seed_export_run_id:
            continue
        present_by_export.setdefault(export_run_id, set()).add(pattern_id)
    for export_run_id in all_export_ids:
        present_by_export.setdefault(export_run_id, set())

    presence_by_export: Dict[str, List[Dict[str, str]]] = {}
    for row in presence_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        export_run_id = str(row.get("export_run_id", "")).strip()
        if not export_run_id:
            continue
        presence_by_export.setdefault(export_run_id, []).append(row)

    if not required_patterns:
        # NO_REFERENCE_DEFINED: no reference set exists for this domain at
        # all. This is deliberately distinct from a defined reference that
        # happens to contain zero patterns (the sidecar schema forbids an
        # empty domain pattern list -- see reference_bundle.load_and_validate).
        # It is also distinct from a target-evidence failure: nothing about the
        # target is unreliable here, there is simply no governance reference to
        # compare it against. So comparison_status stays "ok" (comparison
        # completed and truthfully reports "no reference"), carrying
        # REFERENCE_DOMAIN_UNDEFINED as an explicit, machine-readable condition
        # rather than silently treating "no reference" as "empty reference set".
        # Without a reference set there is no meaningful shared/reference_only/
        # target_only partition, so those fields stay at their pre-PR2 "not
        # computed" values (count fields "0", pattern-id/ratio fields "") and no
        # detail rows are emitted.
        gap_rows = [
            {
                "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                "effective_date": str(reference.get("effective_date", "")),
                "analysis_run_id": run_id,
                "domain": domain,
                "population_id": population_id,
                "export_run_id": export_run_id,
                "patterns_required": "0",
                "patterns_present": "0",
                "patterns_missing": "0",
                "gap_pattern_ids": "",
                "coverage_pct": "",
                "coverage_status": "NO_REFERENCE_DEFINED",
                "reference_pattern_count": "0",
                "target_pattern_count": "0",
                "shared_count": "0",
                "reference_only_count": "0",
                "target_only_count": "0",
                "union_count": "0",
                "shared_pattern_ids": "",
                "reference_only_pattern_ids": "",
                "target_only_pattern_ids": "",
                "reference_coverage_pct": "",
                "jaccard": "",
                "comparison_status": COMPARISON_STATUS_OK,
                "comparison_reason_codes": REASON_REFERENCE_DOMAIN_UNDEFINED,
                "comparison_detail": "",
            }
            for export_run_id in all_export_ids
        ]
        return gap_rows, []

    gap_rows: List[Dict[str, str]] = []
    detail_rows: List[Dict[str, str]] = []
    required_count = len(required_patterns)
    for export_run_id in all_export_ids:
        presence_rows_for_file = presence_by_export.get(export_run_id, [])
        comparison_status, reason_codes, detail = _classify_target_evidence(presence_rows_for_file)

        if not presence_rows_for_file:
            # No presence evidence at all for this (export_run_id, domain):
            # the file was never observed for this domain in this analysis
            # run. Only reachable when eligible_export_run_ids widens the
            # candidate set beyond what pattern_presence_file.csv itself
            # attests to (role filters, population membership) -- the default
            # (eligible_export_run_ids=None) path only ever proposes
            # export_run_ids that already have >=1 presence row. Absence of
            # evidence here is not evidence of a genuinely empty domain (that
            # would still be observed and would carry a presence row -- see
            # tools/extractor.py::_process_one_domain), so this blocks rather
            # than reporting a false reference_only gap.
            comparison_status = COMPARISON_STATUS_BLOCKED
            reason_codes = [REASON_TARGET_DOMAIN_UNAVAILABLE]
            detail = "no presence evidence for this domain/file in the eligible export set"

        target_patterns = present_by_export[export_run_id]
        shared = required_patterns & target_patterns
        reference_only = required_patterns - target_patterns
        target_only = target_patterns - required_patterns
        union_patterns = required_patterns | target_patterns

        present_count = len(shared)
        missing = sorted(reference_only)
        missing_count = len(missing)
        coverage = (present_count / required_count) if required_count else 0.0
        if present_count == required_count:
            status = "full"
        elif present_count == 0:
            status = "none"
        else:
            status = "partial"

        union_count = len(union_patterns)
        jaccard = (len(shared) / union_count) if union_count else 0.0

        row = {
            "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
            "effective_date": str(reference.get("effective_date", "")),
            "analysis_run_id": run_id,
            "domain": domain,
            "population_id": population_id,
            "export_run_id": export_run_id,
            "patterns_required": str(required_count),
            "patterns_present": str(present_count),
            "patterns_missing": str(missing_count),
            "gap_pattern_ids": "|".join(missing),
            "coverage_pct": f"{coverage:.6f}",
            "coverage_status": status,
            "reference_pattern_count": str(required_count),
            "target_pattern_count": str(len(target_patterns)),
            "shared_count": str(len(shared)),
            "reference_only_count": str(len(reference_only)),
            "target_only_count": str(len(target_only)),
            "union_count": str(union_count),
            "shared_pattern_ids": "|".join(sorted(shared)),
            "reference_only_pattern_ids": "|".join(sorted(reference_only)),
            "target_only_pattern_ids": "|".join(sorted(target_only)),
            "reference_coverage_pct": f"{coverage:.6f}",
            "jaccard": f"{jaccard:.6f}",
            "comparison_status": comparison_status,
            "comparison_reason_codes": join_reason_codes(reason_codes),
            "comparison_detail": detail,
        }

        if comparison_status == COMPARISON_STATUS_BLOCKED:
            # Target evidence was insufficient to trust any of the derived
            # comparison metrics -- do not emit plausible-looking zeroes/100%s
            # for them (they would read as a real, complete comparison result).
            for field_name in _TARGET_DERIVED_BLANK_ON_BLOCKED:
                row[field_name] = ""
            gap_rows.append(row)
            # No reliable partition to report at pattern granularity.
            continue

        gap_rows.append(row)

        for pattern_id in sorted(shared):
            comparison_class = "shared"
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": comparison_class,
                }
            )
        for pattern_id in sorted(reference_only):
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": "reference_only",
                }
            )
        for pattern_id in sorted(target_only):
            detail_rows.append(
                {
                    "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
                    "analysis_run_id": run_id,
                    "domain": domain,
                    "population_id": population_id,
                    "export_run_id": export_run_id,
                    "pattern_id": pattern_id,
                    "comparison_class": "target_only",
                }
            )
    return gap_rows, detail_rows


def run_compare_for_domain(
    analysis_dir: Path,
    out_dir: Path,
    reference: Dict[str, object],
    domain: str,
    compare_out_dir: Optional[Path] = None,
    population_id: str = "",
    eligible_export_run_ids: Optional[Set[str]] = None,
    reset_domain_rows: bool = False,
) -> Dict[str, str]:
    compare_dir = compare_out_dir if compare_out_dir is not None else out_dir.parent / "compare"
    compare_dir.mkdir(parents=True, exist_ok=True)
    gap_path = compare_dir / "file_gap_report.csv"
    detail_path = compare_dir / "file_gap_detail.csv"

    try:
        domain_rows, domain_detail_rows = _compute_comparison_rows(
            analysis_dir, out_dir, reference, domain, population_id, eligible_export_run_ids
        )
        presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
        current_run_id = resolve_analysis_run_id(presence_rows, "")
    except (FileNotFoundError, ValueError) as exc:
        # Comparison inputs (pattern_presence_file.csv / membership_matrix.csv)
        # were missing, unreadable, or internally inconsistent (e.g. more than
        # one analysis_run_id present). This is not "no reference defined" or
        # "target unavailable" -- it's the comparison run's own inputs failing
        # to support a comparison at all for this domain. Recorded here
        # (rather than letting the exception propagate into a bare
        # `except Exception: print(...)` at the run_bundle_analysis.py call
        # site, which would drop it into console output only) so it survives
        # as a machine-readable, blocked domain summary instead of silently
        # vanishing from compare_run_summary.csv.
        print(f"[compare][error] domain={domain} population_id={population_id} comparison inputs invalid: {exc}")
        try:
            fallback_run_id = resolve_analysis_run_id(read_csv_rows(analysis_dir / "pattern_presence_file.csv"), "")
        except (FileNotFoundError, ValueError):
            fallback_run_id = ""
        return {
            "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
            "effective_date": str(reference.get("effective_date", "")),
            "analysis_run_id": fallback_run_id,
            "domain": domain,
            "population_id": population_id,
            "files_scored": "0",
            "full_count": "0",
            "partial_count": "0",
            "none_count": "0",
            "no_reference_count": "0",
            "comparison_status": COMPARISON_STATUS_BLOCKED,
            "comparison_reason_codes": REASON_COMPARISON_INPUT_INVALID,
            "comparison_ok_count": "0",
            "comparison_degraded_count": "0",
            "comparison_blocked_count": "0",
        }

    with _GAP_REPORT_LOCK:
        existing = read_csv_rows(gap_path) if gap_path.is_file() else []
        existing_detail = read_csv_rows(detail_path) if detail_path.is_file() else []
        if reset_domain_rows:
            merged = [
                row
                for row in existing
                if not (row.get("analysis_run_id", "") == current_run_id and row.get("domain", "") == domain)
            ] + domain_rows
            merged_detail = [
                row
                for row in existing_detail
                if not (row.get("analysis_run_id", "") == current_run_id and row.get("domain", "") == domain)
            ] + domain_detail_rows
        else:
            merged = [
                row
                for row in existing
                if not (
                    row.get("analysis_run_id", "") == current_run_id
                    and row.get("domain", "") == domain
                    and row.get("population_id", "") == population_id
                )
            ] + domain_rows
            merged_detail = [
                row
                for row in existing_detail
                if not (
                    row.get("analysis_run_id", "") == current_run_id
                    and row.get("domain", "") == domain
                    and row.get("population_id", "") == population_id
                )
            ] + domain_detail_rows
        merged.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("export_run_id", "")))
        merged_detail.sort(
            key=lambda r: (
                r.get("analysis_run_id", ""),
                r.get("domain", ""),
                r.get("population_id", ""),
                r.get("export_run_id", ""),
                r.get("pattern_id", ""),
            )
        )
        atomic_write_csv(gap_path, _GAP_FIELDNAMES, merged)
        atomic_write_csv(detail_path, _DETAIL_FIELDNAMES, merged_detail)

    counts = Counter(row.get("coverage_status", "") for row in domain_rows)
    comparison_counts = Counter(row.get("comparison_status", "") for row in domain_rows)
    domain_comparison_status = aggregate_comparison_status(
        row.get("comparison_status", COMPARISON_STATUS_OK) for row in domain_rows
    )
    domain_reason_codes = join_reason_codes(
        code
        for row in domain_rows
        for code in (row.get("comparison_reason_codes", "") or "").split("|")
    )
    return {
        "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
        "effective_date": str(reference.get("effective_date", "")),
        "analysis_run_id": (domain_rows[0].get("analysis_run_id", "") if domain_rows else ""),
        "domain": domain,
        "population_id": population_id,
        "files_scored": str(len(domain_rows)),
        "full_count": str(counts.get("full", 0)),
        "partial_count": str(counts.get("partial", 0)),
        "none_count": str(counts.get("none", 0)),
        "no_reference_count": str(counts.get("NO_REFERENCE_DEFINED", 0)),
        "comparison_status": domain_comparison_status,
        "comparison_reason_codes": domain_reason_codes,
        "comparison_ok_count": str(comparison_counts.get(COMPARISON_STATUS_OK, 0)),
        "comparison_degraded_count": str(comparison_counts.get(COMPARISON_STATUS_DEGRADED, 0)),
        "comparison_blocked_count": str(comparison_counts.get(COMPARISON_STATUS_BLOCKED, 0)),
    }
