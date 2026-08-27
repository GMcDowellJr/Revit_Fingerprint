from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import csv
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id, retry_fs_op
    from step0_discover_populations import discover_populations
    from step1_membership_matrix import build_membership_matrix
    from step2_find_bundles import find_bundles_for_domain
    from step2b_bundle_share_profile import build_bundle_share_profile
    from step3_build_dag import build_dag_for_domain
    from step4_difference_sets import emit_stub as emit_step4
    from step5_classify_patterns import emit_stub as emit_step5
    from step6_classify_files import emit_stub as emit_step6
    from step7_overlap_report import emit_stub as emit_step7
    from reference_bundle import load_and_validate, ReferenceBundleError
    from step_compare import run_compare_for_domain, write_blocked_gap_placeholder
    from placeholder_exclusions import compute_placeholder_exclusions
    from jenks_utils import jenks_breaks
    from comparison_status import (
        COMPARISON_STATUS_OK,
        COMPARISON_STATUS_DEGRADED,
        COMPARISON_STATUS_BLOCKED,
        REASON_COMPARISON_INPUT_INVALID,
        aggregate_comparison_status,
        join_reason_codes,
    )
    from name_projection_adapter import (
        DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
        emit_name_target_provenance,
        stage_name_projection_analysis_dir,
    )
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id, retry_fs_op
    from .step0_discover_populations import discover_populations
    from .step1_membership_matrix import build_membership_matrix
    from .step2_find_bundles import find_bundles_for_domain
    from .step2b_bundle_share_profile import build_bundle_share_profile
    from .step3_build_dag import build_dag_for_domain
    from .step4_difference_sets import emit_stub as emit_step4
    from .step5_classify_patterns import emit_stub as emit_step5
    from .step6_classify_files import emit_stub as emit_step6
    from .step7_overlap_report import emit_stub as emit_step7
    from .reference_bundle import load_and_validate, ReferenceBundleError
    from .step_compare import run_compare_for_domain, write_blocked_gap_placeholder
    from .placeholder_exclusions import compute_placeholder_exclusions
    from ..jenks_utils import jenks_breaks
    from .comparison_status import (
        COMPARISON_STATUS_OK,
        COMPARISON_STATUS_DEGRADED,
        COMPARISON_STATUS_BLOCKED,
        REASON_COMPARISON_INPUT_INVALID,
        aggregate_comparison_status,
        join_reason_codes,
    )
    from .name_projection_adapter import (
        DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
        emit_name_target_provenance,
        stage_name_projection_analysis_dir,
    )

TIMING_FIELDNAMES = ["schema_version", "analysis_run_id", "domain", "population_id", "step", "seconds"]
TIMING_STEPS = ("step1", "step2", "step2b", "step3", "step4", "step5", "step6", "step7")
ROLE_GROUP_ALIASES = {
    "template-group": ["Generic", "Generic-Host", "Template"],
}
VALID_ROLES = {"Project", "Template", "Generic", "Generic-Host", "Container"}
VALID_COMPARISON_TARGETS = {"config", "name", "both"}
DEFAULT_NAME_KEY_PATTERNS_DIR = Path("Results_v21/name_key/patterns/name")


def _view_out_dir(out_dir: Path, purge_view: str) -> Path:
    """Return out_dir/all or out_dir/used."""
    return out_dir / purge_view


def _ensure_latent_purgeable(latent_purgeable_file: Path, records_dir: Path) -> Path:
    """Run compute_latent_purgeable.py if latent_purgeable.csv does not exist."""
    if latent_purgeable_file.exists():
        print(f"[run] latent_purgeable.csv found at {latent_purgeable_file}")
        return latent_purgeable_file

    print(f"[run] latent_purgeable.csv not found — running compute_latent_purgeable.py ...")
    cmd = [
        sys.executable,
        str(Path(__file__).parent.parent / "compute_latent_purgeable.py"),
        "--records-dir", str(records_dir),
        "--out-file", str(latent_purgeable_file),
    ]
    subprocess.run(cmd, check=True)
    if not latent_purgeable_file.exists():
        raise RuntimeError(
            f"compute_latent_purgeable.py completed but {latent_purgeable_file} was not created"
        )
    print(f"[run] latent_purgeable.csv written to {latent_purgeable_file}")
    return latent_purgeable_file


def _emit_meta_scatter_thresholds(out_dir: Path, run_id: str, domain_filter: str = "") -> None:
    if domain_filter:
        return

    rows: List[Dict[str, str]] = []
    for dom_dir in sorted([p for p in out_dir.iterdir() if p.is_dir() and not p.name.startswith("_")], key=lambda p: p.name.lower()):
        bundle_files = sorted(dom_dir.rglob("bundles.csv"))
        scope_files = sorted(dom_dir.rglob("scope_registry.csv"))
        if not bundle_files or not scope_files:
            continue
        bundle_rows = []
        for p in bundle_files:
            bundle_rows.extend(read_csv_rows(p))
        scope_rows = []
        for p in scope_files:
            scope_rows.extend(read_csv_rows(p))
        run_bundles = [r for r in bundle_rows if r.get("analysis_run_id", "") == run_id]
        run_scopes = [r for r in scope_rows if r.get("analysis_run_id", "") == run_id]
        if not run_scopes:
            continue
        bundle_count = len(run_bundles)
        population_files = sum(int(r.get("files_in_scope", "0") or "0") for r in run_scopes)
        top_alignment = 0.0
        for r in run_bundles:
            try:
                files_present = int(r.get("files_present", "0") or "0")
                files_total = int(r.get("files_total", "0") or "0")
            except ValueError:
                continue
            if files_total > 0:
                top_alignment = max(top_alignment, files_present / files_total)
        bundle_density = (bundle_count / population_files) if population_files > 0 else 0.0
        rows.append({"domain": dom_dir.name, "b_alignment_rate": f"{top_alignment:.6f}", "bundle_density": f"{bundle_density:.6f}"})

    axis_map = {
        "alignment_rate": [float(r["b_alignment_rate"]) for r in rows if float(r["b_alignment_rate"]) > 0.0 and float(r["bundle_density"]) > 0.0],
        "bundle_density": [float(r["bundle_density"]) for r in rows if float(r["b_alignment_rate"]) > 0.0 and float(r["bundle_density"]) > 0.0],
    }
    out_rows: List[Dict[str, str]] = []
    for axis, values in axis_map.items():
        breaks = jenks_breaks(values, n_classes=2) if values else []
        break_value = breaks[0] if breaks else 0.0
        out_rows.append(
            {
                "analysis_run_id": run_id,
                "axis": axis,
                "break_value": f"{break_value:.4f}",
                "n_domains": str(len(values)),
                "input_min": f"{(min(values) if values else 0.0):.4f}",
                "input_max": f"{(max(values) if values else 0.0):.4f}",
            }
        )
    atomic_write_csv(
        out_dir / "meta_scatter_thresholds.csv",
        ["analysis_run_id", "axis", "break_value", "n_domains", "input_min", "input_max"],
        out_rows,
    )


_COMPARE_RUN_SUMMARY_FIELDNAMES = [
    "reference_bundle_id",
    "effective_date",
    "analysis_run_id",
    "domain",
    "population_id",
    "files_scored",
    "full_count",
    "partial_count",
    "none_count",
    "no_reference_count",
    "comparison_status",
    "comparison_reason_codes",
    "comparison_ok_count",
    "comparison_degraded_count",
    "comparison_blocked_count",
]

_COMPARE_RUN_STATUS_FIELDNAMES = [
    "analysis_run_id",
    "comparison_status",
    "comparison_reason_codes",
    "comparison_detail",
    "domains_total",
    "domains_ok",
    "domains_degraded",
    "domains_blocked",
]


def _write_compare_run_outputs(
    compare_out_dir: Path,
    run_id: str,
    compare_rows: List[Dict[str, str]],
    expected_domains: Optional[List[str]] = None,
    missing_domain_reason_code: str = REASON_COMPARISON_INPUT_INVALID,
) -> None:
    """Write compare_run_summary.csv (per domain/population) and
    compare_run_status.csv (a single deterministic run-level rollup: blocked
    beats degraded beats ok -- see comparison_status.aggregate_comparison_status)
    so a blocked/degraded comparison run is observable without console-log
    inspection, per the "every comparison run declares ok/degraded/blocked"
    invariant.

    `expected_domains`, when given, is the full set of domains --compare was
    asked to run for this view. A domain whose own pipeline stage (step0/
    step1/discovery) raised before run_compare_for_domain ever appended a
    summary is silently absent from `compare_rows` -- caught only by the
    surrounding per-domain `except Exception: print(...)` handlers, which are
    out of scope for this PR to rework. Without this check, an entirely
    empty `compare_rows` (every requested domain failed upstream of
    comparison) would aggregate to comparison_status=ok/domains_total=0,
    falsely certifying a comparison run where nothing was actually compared.
    A synthesized blocked row (reason `missing_domain_reason_code`) is added
    per missing domain so both compare_run_summary.csv and the run-level
    rollup reflect it. This is a coarse, domain-level safety net for failures
    with no (domain, population_id)-scoped row at all; population-aware call
    sites should prefer appending a precisely-scoped blocked row at the
    actual point of failure (see run_bundle_analysis()'s per-population loop)
    -- this net only catches what that per-site handling didn't.

    `compare_rows` may carry more than one row per domain (population-aware
    mode: one row per (domain, population_id)). domains_total/ok/degraded/
    blocked in compare_run_status.csv count distinct *domains* -- each
    domain's rows are first rolled up to one status via
    aggregate_comparison_status before counting -- not summary rows, so a
    domain with three population rows is not counted as three domains.
    """
    compare_rows = list(compare_rows)
    if expected_domains:
        domains_with_summary = {r.get("domain", "") for r in compare_rows}
        missing_domains = sorted({d for d in expected_domains if d} - domains_with_summary)
        for dom in missing_domains:
            compare_rows.append(
                {
                    "reference_bundle_id": "",
                    "effective_date": "",
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "population_id": "",
                    "files_scored": "0",
                    "full_count": "0",
                    "partial_count": "0",
                    "none_count": "0",
                    "no_reference_count": "0",
                    "comparison_status": COMPARISON_STATUS_BLOCKED,
                    "comparison_reason_codes": missing_domain_reason_code,
                    "comparison_ok_count": "0",
                    "comparison_degraded_count": "0",
                    "comparison_blocked_count": "0",
                }
            )
        compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")))

    atomic_write_csv(compare_out_dir / "compare_run_summary.csv", _COMPARE_RUN_SUMMARY_FIELDNAMES, compare_rows)

    statuses_by_domain: Dict[str, List[str]] = {}
    reasons_by_domain: Dict[str, List[str]] = {}
    for r in compare_rows:
        dom = r.get("domain", "")
        statuses_by_domain.setdefault(dom, []).append(r.get("comparison_status", COMPARISON_STATUS_OK))
        reasons_by_domain.setdefault(dom, []).extend(str(r.get("comparison_reason_codes", "") or "").split("|"))

    domain_level_status = {dom: aggregate_comparison_status(sts) for dom, sts in statuses_by_domain.items()}
    run_status = aggregate_comparison_status(domain_level_status.values())
    run_reason_codes = join_reason_codes(code for codes in reasons_by_domain.values() for code in codes)
    status_counts = Counter(domain_level_status.values())
    atomic_write_csv(
        compare_out_dir / "compare_run_status.csv",
        _COMPARE_RUN_STATUS_FIELDNAMES,
        [
            {
                "analysis_run_id": run_id,
                "comparison_status": run_status,
                "comparison_reason_codes": run_reason_codes,
                "comparison_detail": "",
                "domains_total": str(len(domain_level_status)),
                "domains_ok": str(status_counts.get(COMPARISON_STATUS_OK, 0)),
                "domains_degraded": str(status_counts.get(COMPARISON_STATUS_DEGRADED, 0)),
                "domains_blocked": str(status_counts.get(COMPARISON_STATUS_BLOCKED, 0)),
            }
        ],
    )


def _blocked_compare_summary(
    reference: Optional[Dict[str, object]],
    run_id: str,
    domain: str,
    population_id: str,
    reason_code: str,
    detail: str = "",
) -> Dict[str, str]:
    """A synthesized blocked compare_summary dict, shaped like
    run_compare_for_domain()'s return value, for a (domain, population_id)
    whose own pipeline stage failed before run_compare_for_domain ever ran
    (so there is no real summary to report) -- e.g. build_membership_matrix/
    _run_step2_to_step7/discover_populations raising. Precisely scoped to the
    (domain, population_id) that actually failed, unlike
    _write_compare_run_outputs' coarser expected_domains safety net.
    """
    reference = reference or {}
    return {
        "reference_bundle_id": str(reference.get("reference_bundle_id", "")),
        "effective_date": str(reference.get("effective_date", "")),
        "analysis_run_id": run_id,
        "domain": domain,
        "population_id": population_id,
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


def _load_purgeable_only_set(
    latent_purgeable_file: Path,
) -> Set[Tuple[str, str, str]]:
    """Read latent_purgeable.csv once and return the purgeable_only set.

    purgeable_only = rows where latent_purgeable=true AND the same
    (export_run_id, domain, sig_hash) triple never appears as latent_purgeable!=true.
    Matches the logic in step1_membership_matrix.py exactly.
    """
    used_set: Set[Tuple[str, str, str]] = set()
    excluded_set: Set[Tuple[str, str, str]] = set()
    for row in read_csv_rows(latent_purgeable_file):
        eid = row.get("export_run_id", "").strip()
        dom = row.get("domain", "").strip()
        sig = row.get("sig_hash", "").strip()
        lp  = row.get("latent_purgeable", "").strip().lower()
        if not (eid and dom and sig):
            continue
        if lp != "true":
            used_set.add((eid, dom, sig))
        else:
            excluded_set.add((eid, dom, sig))
    result = excluded_set - used_set
    print(f"[run] purgeable_only_set loaded: {len(result)} entries from {latent_purgeable_file.name}")
    return result


def _run_pipeline_once(
    analysis_dir: Path,
    work_out_dir: Path,
    domain: str,
    run_id: str,
    min_support_count: int,
    min_support_pct: float,
    compute_share_profile: bool = False,
    population_id: Optional[str] = None,
    analysis_run_id: str = "",
    population_registry_dir: Optional[Path] = None,
    scope_key_filter: Optional[str] = None,
    allowed_export_run_ids: Optional[Set[str]] = None,
    purge_view: str = "all",
    latent_purgeable_file: Optional[Path] = None,
    purgeable_only_set: Optional[Set[Tuple[str, str, str]]] = None,
) -> Dict[str, object]:
    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0

    t0 = time.time()
    build_membership_matrix(
        analysis_dir,
        work_out_dir,
        domain,
        run_id,
        population_id,
        population_registry_dir,
        scope_key_filter,
        allowed_export_run_ids,
        purge_view,
        latent_purgeable_file,
        purgeable_only_set=purgeable_only_set,
    )
    t1 = time.time() - t0
    print(f"[run] domain={domain} step1_seconds={t1:.3f}")

    t0 = time.time()
    step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
    total_bundles += step2.get("bundles", 0)
    t2 = time.time() - t0
    print(f"[run] domain={domain} step2_seconds={t2:.3f}")

    t2b = 0.0
    if compute_share_profile:
        t0 = time.time()
        build_bundle_share_profile(
            analysis_dir=analysis_dir,
            domain_out_dir=work_out_dir / domain,
            domain=domain,
            analysis_run_id=run_id,
            scope_key=scope_key_filter,
        )
        t2b = time.time() - t0
        print(f"[run] domain={domain} step2b_seconds={t2b:.3f}")

    t0 = time.time()
    step3 = build_dag_for_domain(work_out_dir, domain)
    total_edges += step3.get("edges", 0)
    t3 = time.time() - t0
    print(f"[run] domain={domain} step3_seconds={t3:.3f}")

    t0 = time.time()
    emit_step4(work_out_dir, domain)
    t4 = time.time() - t0
    print(f"[run] domain={domain} step4_seconds={t4:.3f}")

    t0 = time.time()
    emit_step5(work_out_dir, domain)
    t5 = time.time() - t0
    print(f"[run] domain={domain} step5_seconds={t5:.3f}")

    t0 = time.time()
    step6 = emit_step6(work_out_dir, domain)
    total_files_no_bundle += step6.get("files_no_bundle", 0)
    t6 = time.time() - t0
    print(f"[run] domain={domain} step6_seconds={t6:.3f}")

    t0 = time.time()
    emit_step7(work_out_dir, domain)
    t7 = time.time() - t0
    print(f"[run] domain={domain} step7_seconds={t7:.3f}")

    total = t1 + t2 + t2b + t3 + t4 + t5 + t6 + t7
    print(
        f"[timing] summary domain={domain} population_id={population_id or 'none'} "
        f"step1={t1:.2f} step2={t2:.2f} step2b={t2b:.2f} step3={t3:.2f} step4={t4:.2f} "
        f"step5={t5:.2f} step6={t6:.2f} step7={t7:.2f} total={total:.2f}"
    )

    return {
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
        "step_times": {
            "step1": t1,
            "step2": t2,
            "step2b": t2b,
            "step3": t3,
            "step4": t4,
            "step5": t5,
            "step6": t6,
            "step7": t7,
        },
    }


def _run_step2_to_step7(
    analysis_dir: Path,
    work_out_dir: Path,
    domain: str,
    min_support_count: int,
    min_support_pct: float,
    run_id: str,
    compute_share_profile: bool = False,
) -> Dict[str, object]:
    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0

    t0 = time.time()
    step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
    total_bundles += step2.get("bundles", 0)
    t2 = time.time() - t0
    print(f"[run] domain={domain} step2_seconds={t2:.3f}")

    t2b = 0.0
    if compute_share_profile:
        t0 = time.time()
        build_bundle_share_profile(
            analysis_dir=analysis_dir,
            domain_out_dir=work_out_dir / domain,
            domain=domain,
            analysis_run_id=run_id,
        )
        t2b = time.time() - t0
        print(f"[run] domain={domain} step2b_seconds={t2b:.3f}")

    t0 = time.time()
    step3 = build_dag_for_domain(work_out_dir, domain)
    total_edges += step3.get("edges", 0)
    t3 = time.time() - t0
    print(f"[run] domain={domain} step3_seconds={t3:.3f}")

    t0 = time.time()
    emit_step4(work_out_dir, domain)
    t4 = time.time() - t0
    print(f"[run] domain={domain} step4_seconds={t4:.3f}")

    t0 = time.time()
    emit_step5(work_out_dir, domain)
    t5 = time.time() - t0
    print(f"[run] domain={domain} step5_seconds={t5:.3f}")

    t0 = time.time()
    step6 = emit_step6(work_out_dir, domain)
    total_files_no_bundle += step6.get("files_no_bundle", 0)
    t6 = time.time() - t0
    print(f"[run] domain={domain} step6_seconds={t6:.3f}")

    t0 = time.time()
    emit_step7(work_out_dir, domain)
    t7 = time.time() - t0
    print(f"[run] domain={domain} step7_seconds={t7:.3f}")

    return {
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
        "step_times": {"step2": t2, "step2b": t2b, "step3": t3, "step4": t4, "step5": t5, "step6": t6, "step7": t7},
    }


def run_bundle_analysis(
    analysis_dir: Path,
    out_dir: Path,
    domain: str = "",
    min_support_count: int = 3,
    min_support_pct: float = 0.0,
    analysis_run_id: str = "",
    discover_populations_flag: bool = True,
    min_population_size: int = 0,
    max_population_overlap: float = 0.20,
    min_population_jaccard: float = 0.30,
    discovery_support_pct: float = 0.10,
    compare: bool = False,
    compute_share_profile: bool = False,
    roles: Optional[List[str]] = None,
    metadata_file: Optional[Path] = None,
    purge_view: str = "both",
    latent_purgeable_file: Optional[Path] = None,
    workers: int = 4,
) -> Dict[str, int]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, analysis_run_id)

    domains = [domain] if domain else sorted({r.get("domain", "") for r in presence_rows if r.get("analysis_run_id", "") == run_id})
    resolved_roles: Optional[List[str]] = None
    allowed_export_run_ids: Optional[Set[str]] = None
    if roles:
        if metadata_file is None:
            raise ValueError("--metadata-file is required when --roles is provided")
        expanded_roles: List[str] = []
        for role in roles:
            if role in ROLE_GROUP_ALIASES:
                expanded_roles.extend(ROLE_GROUP_ALIASES[role])
            else:
                expanded_roles.append(role)
        invalid_roles = sorted({r for r in expanded_roles if r not in VALID_ROLES})
        if invalid_roles:
            raise ValueError(f"invalid --roles values: {', '.join(invalid_roles)}")
        resolved_roles = sorted(set(expanded_roles))
        role_set = set(resolved_roles)
        allowed_export_run_ids = set()
        with metadata_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                role = (row.get("governance_role", "") or "").strip()
                eid = (row.get("export_run_id", "") or "").strip()
                if role in role_set and eid:
                    allowed_export_run_ids.add(eid)
        print(f"[role_filter] roles={resolved_roles} allowed_files={len(allowed_export_run_ids)}")

    # Pre-step: ensure latent_purgeable.csv exists when needed
    if purge_view in ("used", "both"):
        records_candidates = [
            analysis_dir / "records",
            analysis_dir.parent / "records",
        ]
        records_dir_derived = next((p for p in records_candidates if p.is_dir()), analysis_dir.parent / "records")
        if latent_purgeable_file is None:
            latent_purgeable_file = records_dir_derived / "latent_purgeable.csv"
        latent_purgeable_file = _ensure_latent_purgeable(latent_purgeable_file, records_dir_derived)

    views_to_run = ["all", "used"] if purge_view == "both" else [purge_view]

    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0
    processed = len([d for d in domains if d])

    reference: Optional[Dict[str, object]] = None
    if compare:
        try:
            reference = load_and_validate(analysis_dir, SCHEMA_VERSION)
        except ReferenceBundleError as exc:
            # The reference sidecar is missing, malformed, or schema-incompatible.
            # Comparison cannot truthfully proceed at all -- there is no domain
            # loop to enter, so every requested domain blocks at run scope.
            # Recorded to the same compare_run_summary.csv/compare_run_status.csv
            # paths a successful run would write, under each requested view's own
            # compare_<view>/ directory (not a one-off top-level path a consumer
            # monitoring the normal per-view outputs would never check) -- and
            # this also overwrites any stale "ok" status left there by an earlier
            # successful run into the same reused out_dir. All logged before
            # re-raising, so "comparison was requested but blocked" survives even
            # though the process exits with a hard failure per "do not silently
            # skip comparison".
            for view in views_to_run:
                _write_compare_run_outputs(
                    out_dir / f"compare_{view}",
                    run_id,
                    [],
                    expected_domains=domains,
                    missing_domain_reason_code=exc.reason_code,
                )
                # Per-file rows (file_gap_report.csv/file_gap_detail.csv) live
                # in the same compare_<view>/ directory. A prior successful
                # run into this reused out_dir may have left "ok"-looking
                # per-file rows there for these domains; clear them (any
                # population_id -- none is known yet at this point) and
                # replace with one blocked placeholder per domain so they
                # can't be read as still-valid despite the now-blocked status.
                for dom in {d for d in domains if d}:
                    write_blocked_gap_placeholder(
                        out_dir / f"compare_{view}",
                        {},
                        run_id,
                        dom,
                        "",
                        exc.reason_code,
                        str(exc),
                        match_any_population=True,
                    )
                # A domain-free run (e.g. an explicit empty domain filter)
                # still needs the failure recorded even though there is no
                # domain to synthesize a row for.
                if not [d for d in domains if d]:
                    atomic_write_csv(
                        out_dir / f"compare_{view}" / "compare_run_status.csv",
                        _COMPARE_RUN_STATUS_FIELDNAMES,
                        [
                            {
                                "analysis_run_id": run_id,
                                "comparison_status": COMPARISON_STATUS_BLOCKED,
                                "comparison_reason_codes": exc.reason_code,
                                "comparison_detail": str(exc),
                                "domains_total": "0",
                                "domains_ok": "0",
                                "domains_degraded": "0",
                                "domains_blocked": "0",
                            }
                        ],
                    )
            print(f"[run][blocked] comparison blocked: reason={exc.reason_code} detail={exc}")
            raise

    if not discover_populations_flag:
        for view in views_to_run:
            view_out = _view_out_dir(out_dir, view)
            view_out.mkdir(parents=True, exist_ok=True)
            lp_file = latent_purgeable_file if view == "used" else None
            purgeable_only_set: Optional[Set[Tuple[str, str, str]]] = None
            if view == "used" and lp_file is not None:
                t_lp = time.time()
                purgeable_only_set = _load_purgeable_only_set(lp_file)
                print(f"[run] purgeable_only_set load elapsed={time.time()-t_lp:.2f}s")

            role_dir_name = f"role_{'_'.join(resolved_roles)}" if resolved_roles else ""
            role_stage_root = view_out / "_role_stage"
            if resolved_roles and role_stage_root.exists():
                shutil.rmtree(role_stage_root)

            view_timing_rows: List[Dict[str, str]] = []
            view_compare_summary_rows: List[Dict[str, str]] = []
            compare_reset_domains: Set[str] = set()

            compare_out: Optional[Path] = None
            if compare:
                compare_out = view_out.parent / f"compare_{view}"
                compare_out.mkdir(parents=True, exist_ok=True)

            if not compare:
                active_domains = [d for d in domains if d]
                if not active_domains:
                    print(f"[run] view={view} no active domains — skipping")
                    continue

                pool_size = min(workers, len(active_domains))

                print(f"[run] view={view} submitting {len(active_domains)} domains to {pool_size} workers")

                with ProcessPoolExecutor(max_workers=pool_size) as executor:
                    future_to_dom = {
                        executor.submit(
                            _run_pipeline_once,
                            analysis_dir=analysis_dir,
                            work_out_dir=role_stage_root if resolved_roles else view_out,
                            domain=dom,
                            run_id=run_id,
                            min_support_count=min_support_count,
                            min_support_pct=min_support_pct,
                            compute_share_profile=compute_share_profile,
                            analysis_run_id=run_id,
                            allowed_export_run_ids=allowed_export_run_ids,
                            purge_view=view,
                            latent_purgeable_file=lp_file,
                            purgeable_only_set=purgeable_only_set,
                        ): dom
                        for dom in active_domains
                    }
                    for future in as_completed(future_to_dom):
                        dom = future_to_dom[future]
                        try:
                            stats = future.result()
                        except Exception as exc:
                            print(f"[run][error] domain={dom} view={view} failed: {exc}")
                            continue
                        if resolved_roles:
                            produced = (role_stage_root if resolved_roles else view_out) / dom
                            final_out = view_out / dom / role_dir_name
                            if final_out.exists():
                                shutil.rmtree(final_out)
                            final_out.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(produced), str(final_out))
                        total_bundles += stats["total_bundles_found"]
                        total_edges += stats["total_dag_edges"]
                        total_files_no_bundle += stats["files_with_no_bundle_match"]
                        step_times = stats.get("step_times", {})
                        for step_name in TIMING_STEPS:
                            view_timing_rows.append(
                                {
                                    "schema_version": SCHEMA_VERSION,
                                    "analysis_run_id": run_id,
                                    "domain": dom,
                                    "population_id": "",
                                    "step": step_name,
                                    "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
                                }
                            )
                        print(f"[run] domain={dom} view={view} complete")
            else:
                for dom in domains:
                    if not dom:
                        continue
                    print(f"[run] domain={dom} start")
                    compare_appended = False
                    try:
                        work_out_base = role_stage_root if resolved_roles else view_out
                        t0 = time.time()
                        build_membership_matrix(
                            analysis_dir,
                            work_out_base,
                            dom,
                            run_id,
                            None,
                            None,
                            None,
                            allowed_export_run_ids,
                            view,
                            lp_file,
                            purgeable_only_set=purgeable_only_set,
                        )
                        t1 = time.time() - t0
                        print(f"[run] domain={dom} step1_seconds={t1:.3f}")

                        _thread_workers = max(2, min(4, (len(domains) or 1)))
                        with ThreadPoolExecutor(max_workers=_thread_workers) as executor:
                            discovery_future = executor.submit(
                                _run_step2_to_step7,
                                analysis_dir,
                                work_out_base,
                                dom,
                                min_support_count,
                                min_support_pct,
                                run_id,
                                compute_share_profile,
                            )
                            compare_started = time.time()
                            compare_future = executor.submit(
                                run_compare_for_domain,
                                analysis_dir,
                                work_out_base,
                                reference or {},
                                dom,
                                compare_out_dir=compare_out,
                                eligible_export_run_ids=allowed_export_run_ids,
                            )
                            tail = discovery_future.result()
                            compare_summary = compare_future.result()
                        compare_seconds = time.time() - compare_started
                        view_compare_summary_rows.append(compare_summary)
                        compare_appended = True
                        step_times = {"step1": t1, **tail.get("step_times", {})}
                        print(
                            f"[timing] domain={dom} discovery_seconds={sum(float(step_times.get(k, 0.0)) for k in ('step1','step2','step2b','step3','step4','step5','step6','step7')):.3f} "
                            f"compare_seconds={compare_seconds:.3f}"
                        )
                        stats = {
                            "total_bundles_found": tail.get("total_bundles_found", 0),
                            "total_dag_edges": tail.get("total_dag_edges", 0),
                            "files_with_no_bundle_match": tail.get("files_with_no_bundle_match", 0),
                            "step_times": step_times,
                        }

                        if resolved_roles:
                            produced = work_out_base / dom
                            final_out = view_out / dom / role_dir_name
                            if final_out.exists():
                                shutil.rmtree(final_out)
                            final_out.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(produced), str(final_out))

                        total_bundles += stats["total_bundles_found"]
                        total_edges += stats["total_dag_edges"]
                        total_files_no_bundle += stats["files_with_no_bundle_match"]
                        step_times = stats.get("step_times", {})
                        for step_name in TIMING_STEPS:
                            view_timing_rows.append(
                                {
                                    "schema_version": SCHEMA_VERSION,
                                    "analysis_run_id": run_id,
                                    "domain": dom,
                                    "population_id": "",
                                    "step": step_name,
                                    "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
                                }
                            )
                    except Exception as exc:
                        print(f"[run][error] domain={dom} failed: {exc}")
                        if not compare_appended:
                            view_compare_summary_rows.append(
                                _blocked_compare_summary(reference, run_id, dom, "", REASON_COMPARISON_INPUT_INVALID, str(exc))
                            )
                            # build_membership_matrix/step2-7 raised before
                            # run_compare_for_domain was ever reached, so its
                            # own stale-row cleanup never ran either -- do it
                            # here for this domain (population_id is always
                            # "" in this non-population-aware loop).
                            write_blocked_gap_placeholder(
                                compare_out, reference or {}, run_id, dom, "", REASON_COMPARISON_INPUT_INVALID, str(exc)
                            )

            existing_timing_rows = read_csv_rows(view_out / "bundle_analysis_timing.csv") if (view_out / "bundle_analysis_timing.csv").exists() else []
            merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + view_timing_rows
            merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
            atomic_write_csv(view_out / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)

            if compare and compare_out is not None:
                compare_rows = [r for r in view_compare_summary_rows if r.get("analysis_run_id", "") == run_id]
                compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")))
                _write_compare_run_outputs(compare_out, run_id, compare_rows, expected_domains=domains)

            _emit_meta_scatter_thresholds(view_out, run_id, domain)

        print(
            f"[run] complete domains_processed={processed} total_bundles_found={total_bundles} "
            f"total_dag_edges={total_edges} files_with_no_bundle_match={total_files_no_bundle}"
        )
        return {
            "domains_processed": processed,
            "total_bundles_found": total_bundles,
            "total_dag_edges": total_edges,
            "files_with_no_bundle_match": total_files_no_bundle,
        }

    # ── Population-aware path ──────────────────────────────────────────────────
    # TODO: pre-load purgeable_only_set for population-aware path
    records_csv_candidates = [
        analysis_dir / "records" / "records.csv",
        analysis_dir.parent / "records" / "records.csv",
        analysis_dir / "records.csv",
        analysis_dir.parent / "records.csv",
        analysis_dir / "records.csv",
        analysis_dir.parent / "records.csv",
    ]
    records_csv_path = next((p for p in records_csv_candidates if p.exists()), None)
    placeholder_exclusions_path: Optional[Path] = None
    if records_csv_path is None:
        searched = ", ".join(str(p) for p in records_csv_candidates)
        print(f"[run_bundle_analysis] WARNING: records CSV not found for placeholder exclusion; searched: {searched}")
    else:
        with records_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            has_purgeable = "is_purgeable" in (reader.fieldnames or [])
        if has_purgeable:
            placeholder_exclusions_path = out_dir / "domain_placeholder_exclusions.csv"
            compute_placeholder_exclusions(records_csv_path, placeholder_exclusions_path)
            print(f"[run_bundle_analysis] placeholder exclusions computed: {placeholder_exclusions_path}")
        else:
            print("[run_bundle_analysis] WARNING: is_purgeable column not found in records CSV — placeholder exclusion skipped")

    step0_times: Dict[str, float] = {}
    domain_primary_counts: Dict[str, int] = {}
    outliers_by_domain: Dict[str, int] = {}
    domain_elapsed_seconds: Dict[str, float] = {}
    domain_population_counts: Dict[str, int] = {}
    populations_analyzed = 0

    # Per-view accumulation structures (keyed by view name)
    view_timing_rows: Dict[str, List[Dict[str, str]]] = {v: [] for v in views_to_run}
    view_compare_summary_rows: Dict[str, List[Dict[str, str]]] = {v: [] for v in views_to_run}
    compare_reset_domains_by_view: Dict[str, Set[str]] = {v: set() for v in views_to_run}

    for dom in domains:
        if not dom:
            continue
        try:
            t0 = time.time()
            discover_populations(
                analysis_dir=analysis_dir,
                out_dir=out_dir,
                domain=dom,
                analysis_run_id=run_id,
                min_population_size=min_population_size,
                max_population_overlap=max_population_overlap,
                min_population_jaccard=min_population_jaccard,
                discovery_support_pct=discovery_support_pct,
                placeholder_exclusions_path=placeholder_exclusions_path,
                allowed_export_run_ids=allowed_export_run_ids,
            )
            step0_elapsed = time.time() - t0
            step0_times[dom] = step0_elapsed
            print(f"[timing] stage=step0 domain={dom} seconds={step0_elapsed:.2f}")
        except Exception as exc:
            print(f"[run][error] domain={dom} step0 failed: {exc}")
            continue

        summary_rows = read_csv_rows(out_dir / "corpus_population_summary.csv") if (out_dir / "corpus_population_summary.csv").exists() else []
        corpus_population_rows = read_csv_rows(out_dir / "corpus_populations.csv") if (out_dir / "corpus_populations.csv").exists() else []
        pop_ids = sorted(
            {
                (row.get("population_id", ""), row.get("scope_key", ""))
                for row in summary_rows
                if row.get("analysis_run_id", "") == run_id
                and row.get("domain", "") == dom
                and row.get("population_role", "") == "primary"
                and row.get("population_id", "")
            }
        )
        domain_primary_counts[dom] = len(pop_ids)
        outlier_count = sum(
            int(row.get("file_count", "0") or "0")
            for row in summary_rows
            if row.get("analysis_run_id", "") == run_id
            and row.get("domain", "") == dom
            and row.get("population_role", "") == "outlier"
        )
        outliers_by_domain[dom] = outlier_count
        if not pop_ids:
            print(f"[run][warn] domain={dom} has no primary populations; skipping main pass")
            continue

        for pid, _scope_key_from_summary in pop_ids:
            scope_keys_for_population = sorted(
                {
                    (row.get("scope_key", "") or "").strip()
                    for row in corpus_population_rows
                    if row.get("analysis_run_id", "") == run_id
                    and row.get("domain", "") == dom
                    and row.get("population_id", "") == pid
                }
            )
            if not scope_keys_for_population:
                print(f"[run][warn] domain={dom} population_id={pid} has no scope_key mapping; skipping")
                continue
            if len(scope_keys_for_population) > 1:
                raise ValueError(
                    f"Population invariant violation for analysis_run_id={run_id}, domain={dom!r}, "
                    f"population_id={pid!r}: expected exactly one scope_key, found {scope_keys_for_population}"
                )
            population_scope_key = scope_keys_for_population[0]
            print(f"[run] domain={dom} population_id={pid} start")
            populations_analyzed += 1
            domain_population_counts[dom] = domain_population_counts.get(dom, 0) + 1

            for view in views_to_run:
                view_out = _view_out_dir(out_dir, view)
                view_out.mkdir(parents=True, exist_ok=True)
                lp_file = latent_purgeable_file if view == "used" else None

                staging_root = view_out / "_population_runs"
                stage_out = staging_root / f"{dom}__{pid}"
                final_out_base = view_out / dom
                if resolved_roles:
                    final_out_base = final_out_base / f"role_{'_'.join(resolved_roles)}"
                final_out = final_out_base / pid

                if stage_out.exists():
                    shutil.rmtree(stage_out)
                if final_out.exists():
                    shutil.rmtree(final_out)

                compare_appended = False
                try:
                    t0 = time.time()
                    stats = _run_pipeline_once(
                        analysis_dir=analysis_dir,
                        work_out_dir=stage_out,
                        domain=dom,
                        run_id=run_id,
                        min_support_count=min_support_count,
                        min_support_pct=min_support_pct,
                        compute_share_profile=compute_share_profile,
                        population_id=pid,
                        analysis_run_id=run_id,
                        population_registry_dir=out_dir,
                        scope_key_filter=population_scope_key,
                        allowed_export_run_ids=allowed_export_run_ids,
                        purge_view=view,
                        latent_purgeable_file=lp_file,
                    )
                    domain_elapsed_seconds[dom] = domain_elapsed_seconds.get(dom, 0.0) + (time.time() - t0)
                    total_bundles += stats["total_bundles_found"]
                    total_edges += stats["total_dag_edges"]
                    total_files_no_bundle += stats["files_with_no_bundle_match"]
                    step_times = stats.get("step_times", {})
                    for step_name in TIMING_STEPS:
                        view_timing_rows[view].append(
                            {
                                "schema_version": SCHEMA_VERSION,
                                "analysis_run_id": run_id,
                                "domain": dom,
                                "population_id": pid,
                                "step": step_name,
                                "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
                            }
                        )

                    if compare and reference is not None:
                        membership_csv = stage_out / dom / "membership_matrix.csv"
                        eligible_export_run_ids = {
                            str(row.get("export_run_id", "")).strip()
                            for row in read_csv_rows(membership_csv)
                            if row.get("analysis_run_id", "") == run_id and str(row.get("export_run_id", "")).strip()
                        } if membership_csv.exists() else set()
                        compare_out_dir = view_out.parent / f"compare_{view}"
                        compare_summary = run_compare_for_domain(
                            analysis_dir=analysis_dir,
                            out_dir=stage_out,
                            reference=reference,
                            domain=dom,
                            compare_out_dir=compare_out_dir,
                            population_id=pid,
                            eligible_export_run_ids=eligible_export_run_ids,
                            reset_domain_rows=dom not in compare_reset_domains_by_view[view],
                        )
                        compare_reset_domains_by_view[view].add(dom)
                        view_compare_summary_rows[view].append(compare_summary)
                        compare_appended = True

                    produced = stage_out / dom
                    final_out.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(produced), str(final_out))
                except Exception as exc:
                    print(f"[run][error] domain={dom} population_id={pid} view={view} failed: {exc}")
                    if compare and reference is not None and not compare_appended:
                        view_compare_summary_rows[view].append(
                            _blocked_compare_summary(reference, run_id, dom, pid, REASON_COMPARISON_INPUT_INVALID, str(exc))
                        )
                        # _run_pipeline_once raised before run_compare_for_domain
                        # was ever reached, so its own stale-row cleanup never
                        # ran either -- do it here for this exact
                        # (domain, population_id).
                        write_blocked_gap_placeholder(
                            view_out.parent / f"compare_{view}",
                            reference,
                            run_id,
                            dom,
                            pid,
                            REASON_COMPARISON_INPUT_INVALID,
                            str(exc),
                        )

    total_outliers = sum(outliers_by_domain.get(dom, 0) for dom in domains)
    print("[run] complete (population-aware)")
    print(f"  domains_processed={processed}")
    print(f"  populations_analyzed={populations_analyzed}")
    print(f"  total_outlier_files={total_outliers}")
    print(f"  total_bundles_found={total_bundles}")
    print(f"  total_dag_edges={total_edges}")
    print("  populations_detail:")
    for dom in domains:
        if not dom:
            continue
        print(
            f"    {dom}: {domain_primary_counts.get(dom, 0)} populations, "
            f"{outliers_by_domain.get(dom, 0)} outliers"
        )
    for dom in domains:
        if not dom:
            continue
        print(
            f"[timing] domain_total domain={dom} populations={domain_population_counts.get(dom, 0)} "
            f"total_seconds={domain_elapsed_seconds.get(dom, 0.0):.2f}"
        )
        if dom in step0_times:
            for view in views_to_run:
                view_timing_rows[view].append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "population_id": "",
                        "step": "step0",
                        "seconds": f"{step0_times.get(dom, 0.0):.3f}",
                    }
                )

    for view in views_to_run:
        view_out = _view_out_dir(out_dir, view)
        existing_timing_rows = read_csv_rows(view_out / "bundle_analysis_timing.csv") if (view_out / "bundle_analysis_timing.csv").exists() else []
        merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + view_timing_rows[view]
        merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
        atomic_write_csv(view_out / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)

        if compare:
            compare_out_dir = view_out.parent / f"compare_{view}"
            compare_rows = [r for r in view_compare_summary_rows[view] if r.get("analysis_run_id", "") == run_id]
            compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")))
            _write_compare_run_outputs(compare_out_dir, run_id, compare_rows, expected_domains=domains)

        _emit_meta_scatter_thresholds(view_out, run_id, domain)

    return {
        "domains_processed": processed,
        "populations_analyzed": populations_analyzed,
        "total_outlier_files": total_outliers,
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
    }


def _validate_name_target_constraints(
    comparison_target: str,
    purge_view: str,
    compute_share_profile: bool,
    compare: bool,
) -> None:
    """Fail loudly (never guess, never silently fall back) when a caller asks the
    name-projection target for a feature that has no defined name-projection equivalent
    yet. See DECISIONS.md D-037."""
    if comparison_target not in ("name", "both"):
        return
    if purge_view != "all":
        raise SystemExit(
            f"--comparison-target {comparison_target} only supports --purge-view all. "
            "USED-view purgeability filtering has no defined name-projection equivalent "
            "(latent_purgeable.csv is sig_hash-keyed; name-projection patterns key off "
            "join_key_name_identity's join_hash instead) -- pass --purge-view all "
            "explicitly, or run comparison_target=config separately for USED-view output. "
            "See DECISIONS.md D-037."
        )
    if compute_share_profile:
        raise SystemExit(
            f"--comparison-target {comparison_target} does not support "
            "--compute-share-profile. pattern_share_pct/is_dominant_pattern have no "
            "name-projection equivalent (PR2's pattern_membership.csv carries neither "
            "field). See DECISIONS.md D-037."
        )
    if compare:
        raise SystemExit(
            f"--comparison-target {comparison_target} does not support --compare. No "
            "name-projection reference-bundle baseline is defined yet, and resolving that "
            "gap is explicitly out of scope for this PR. See "
            "DECISIONS.md D-037."
        )


def run_bundle_analysis_for_target(
    analysis_dir: Path,
    out_dir: Path,
    comparison_target: str = "config",
    name_key_patterns_dir: Optional[Path] = None,
    domain: str = "",
    min_support_count: int = 3,
    min_support_pct: float = 0.0,
    analysis_run_id: str = "",
    discover_populations_flag: bool = True,
    min_population_size: int = 0,
    max_population_overlap: float = 0.20,
    min_population_jaccard: float = 0.30,
    discovery_support_pct: float = 0.10,
    compare: bool = False,
    compute_share_profile: bool = False,
    roles: Optional[List[str]] = None,
    metadata_file: Optional[Path] = None,
    purge_view: Optional[str] = None,
    latent_purgeable_file: Optional[Path] = None,
    workers: int = 4,
) -> Dict[str, Dict[str, int]]:
    """Run bundle analysis for one or both join-basis projections
    (`--comparison-target {config,name,both}`, PR3), namespacing name-target output under
    its own subdirectory so it can never collide with or overwrite config-target output.

    The `config` leg (default, and the only leg run when `comparison_target="config"`) is a
    direct, argument-for-argument passthrough to `run_bundle_analysis()` writing to `out_dir`
    exactly as before this function existed -- byte-identical by construction, not by
    convention, since it is literally the same function call.

    `purge_view=None` (the default -- distinct from an explicit choice, so this can be
    target-aware) resolves to `"both"` for `comparison_target="config"` (unchanged from
    `run_bundle_analysis()`'s own default) and to `"all"` for `comparison_target` in
    `{"name", "both"}`, since ALL is the only view name-target supports (PR #389 review: the
    old flat `"both"` default made `--comparison-target name` fail out of the box even
    though the caller never asked for anything but ALL). An *explicit* `--purge-view
    used`/`both` under `comparison_target` in `{"name", "both"}` still raises via
    `_validate_name_target_constraints()` -- only the unset-default case is target-aware.

    The `name` leg stages `Results_v21/name_key/patterns/name/` (PR2's output) into the
    exact `analysis_dir` shape `run_bundle_analysis()` already expects (see
    `name_projection_adapter.py`), forces `--purge-view all` /
    `--compute-share-profile=False` / `--compare=False` (validated up front -- see
    `_validate_name_target_constraints`), and writes a `bundle_provenance.csv` +
    `domain_coverage.csv` + `README.md` declaring `comparison_target`, `coverage_class`, and
    the analysis-side-reconstruction provenance note for every bundle produced.

    The name leg's final output lands at `out_dir/name_all` (per-domain step0-step7 output,
    `bundle_provenance.csv`, `domain_coverage.csv`, `README.md` -- everything the internal
    `out_dir/name/all` staging path produced, relocated as the last step of this branch).
    `out_dir/name_all` is cleared *before* staging even starts, not only after a fresh
    tree is produced -- if staging/mining/provenance raises partway through, a prior
    successful run's `name_all/` must not survive untouched and look like current output
    to Power BI (PR review, #391). This flat, single-path-segment location matches the
    existing Power BI model's
    `pPurgeView` folder-splice convention (`<segment>\\results\\bundle_analysis\\
    <pPurgeView>\\*_combined.csv`) so a report author can point `pPurgeView` at `name_all`
    exactly the way they already point it at `all`/`used` today -- see the PR3 BI-output-
    compatibility brief. `tools/run_segment_orchestrator.py`'s BI-merge step reads/writes
    `*_combined.csv` directly under `out_dir/name_all`, then calls
    `name_projection_adapter.annotate_name_target_combined_files()` to add
    `comparison_target`/`coverage_class`/`provenance_note` columns to each one.
    """
    if comparison_target not in VALID_COMPARISON_TARGETS:
        raise ValueError(f"--comparison-target must be one of {sorted(VALID_COMPARISON_TARGETS)}, got {comparison_target!r}")
    if purge_view is None:
        purge_view = "all" if comparison_target in ("name", "both") else "both"
    _validate_name_target_constraints(comparison_target, purge_view, compute_share_profile, compare)

    targets = ["config"] if comparison_target == "config" else (["name"] if comparison_target == "name" else ["config", "name"])
    results: Dict[str, Dict[str, int]] = {}

    if "config" in targets:
        config_out_dir = out_dir if comparison_target == "config" else out_dir / "config"
        print(f"[run_multi_target] comparison_target=config out_dir={config_out_dir}")
        results["config"] = run_bundle_analysis(
            analysis_dir=analysis_dir,
            out_dir=config_out_dir,
            domain=domain,
            min_support_count=min_support_count,
            min_support_pct=min_support_pct,
            analysis_run_id=analysis_run_id,
            discover_populations_flag=discover_populations_flag,
            min_population_size=min_population_size,
            max_population_overlap=max_population_overlap,
            min_population_jaccard=min_population_jaccard,
            discovery_support_pct=discovery_support_pct,
            compare=compare,
            compute_share_profile=compute_share_profile,
            roles=roles,
            metadata_file=metadata_file,
            purge_view=purge_view,
            latent_purgeable_file=latent_purgeable_file,
            workers=workers,
        )

    if "name" in targets:
        resolved_name_patterns_dir = name_key_patterns_dir or DEFAULT_NAME_KEY_PATTERNS_DIR
        name_run_id = analysis_run_id or DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID
        name_out_dir = out_dir / "name"
        staging_dir = name_out_dir / "_staging_analysis_input"

        # Clear any previous run's BI-facing output before starting regeneration, not only
        # after a fresh name/all source has been produced. Without this, a failure during
        # staging/mining/provenance below (raised before the relocation step near the end
        # of this branch is ever reached) would leave a prior successful run's name_all/
        # completely untouched -- Power BI would silently keep reading stale combined
        # files from an old run even though this run is marked failed upstream (PR review,
        # #391). Matches the same "never leave a misleading stale artifact" rationale as
        # the orchestrator's own pre-clean of the internal bundle_analysis/name/ directory.
        name_all_dir = out_dir / "name_all"
        if name_all_dir.exists():
            retry_fs_op(shutil.rmtree, str(name_all_dir))

        # A details-only export (no sibling *.index.json) keeps its *.details.json name as
        # its canonical export_run_id -- normalize_export_run_id() can't tell that apart
        # from a split-export file's raw name by string shape alone, and blindly rewriting
        # it produces an id that matches nothing real (PR #390 review). file_metadata.csv's
        # own export_run_id column is the corpus's real id set, so when --metadata-file is
        # available it resolves this correctly; without it, staging falls back to the
        # original blind-rewrite behavior (unchanged for callers with no metadata file).
        known_export_run_ids = None
        if metadata_file is not None and Path(metadata_file).is_file():
            known_export_run_ids = {
                (row.get("export_run_id", "") or "").strip()
                for row in read_csv_rows(Path(metadata_file))
                if (row.get("export_run_id", "") or "").strip()
            }

        stage_stats = stage_name_projection_analysis_dir(
            name_patterns_dir=resolved_name_patterns_dir,
            staging_dir=staging_dir,
            analysis_run_id=name_run_id,
            known_export_run_ids=known_export_run_ids,
        )
        print(f"[run_multi_target] comparison_target=name staged={stage_stats} out_dir={name_out_dir}")

        results["name"] = run_bundle_analysis(
            analysis_dir=staging_dir,
            out_dir=name_out_dir,
            domain=domain,
            min_support_count=min_support_count,
            min_support_pct=min_support_pct,
            analysis_run_id=name_run_id,
            discover_populations_flag=discover_populations_flag,
            min_population_size=min_population_size,
            max_population_overlap=max_population_overlap,
            min_population_jaccard=min_population_jaccard,
            discovery_support_pct=discovery_support_pct,
            compare=False,
            compute_share_profile=False,
            roles=roles,
            metadata_file=metadata_file,
            purge_view="all",
            latent_purgeable_file=None,
            workers=workers,
        )

        provenance_stats = emit_name_target_provenance(
            view_out_dir=name_out_dir,
            name_patterns_dir=resolved_name_patterns_dir,
            analysis_run_id=name_run_id,
        )
        print(f"[run_multi_target] comparison_target=name provenance={provenance_stats}")

        # Relocate the completed ALL-view output to a flat out_dir/name_all directory.
        # name_out_dir/"all" (this function's own internal staging/namespacing shape) is
        # two path segments; the Power BI model's pPurgeView parameter splices in a single
        # segment (`<segment>\results\bundle_analysis\<pPurgeView>\*_combined.csv`), so the
        # BI-facing output must land at out_dir/name_all, not out_dir/name/all.
        # name_all_dir was already cleared of any stale prior run above, before staging
        # even started -- re-checked here defensively in case anything unexpected
        # recreated it in between. Guarded on name_all_source existing so a caller that
        # mocks run_bundle_analysis / emit_name_target_provenance out (as some tests do)
        # doesn't hit a missing-directory error here. Every mutating call goes through
        # retry_fs_op() -- a segments root synced by OneDrive (or similar) can transiently
        # lock a file/folder this function just finished writing (WinError 5 "Access is
        # denied"), and this is dozens of small per-domain files written and immediately
        # relocated in one pass.
        name_all_source = name_out_dir / "all"
        if name_all_source.is_dir():
            if name_all_dir.exists():
                retry_fs_op(shutil.rmtree, str(name_all_dir))
            retry_fs_op(shutil.move, str(name_all_source), str(name_all_dir))
            for extra in ("bundle_provenance.csv", "domain_coverage.csv", "README.md"):
                src = name_out_dir / extra
                if src.is_file():
                    retry_fs_op(shutil.move, str(src), str(name_all_dir / extra))

    return results


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bundle analysis pipeline")
    p.add_argument("--analysis-dir", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument(
        "--comparison-target", choices=sorted(VALID_COMPARISON_TARGETS), default="config",
        help="Which join-basis projection to run bundle analysis against: the existing "
             "configuration join_hash (config, default -- unchanged behavior/output), "
             "PR2's Canonical Name Identity Projection output (name, ALL view only), or "
             "both, namespaced separately under --out-dir/config and --out-dir/name_all.",
    )
    p.add_argument(
        "--name-key-patterns-dir", type=Path, default=None,
        help="Directory containing PR2's name-target domain_patterns.csv/"
             "pattern_membership.csv/domain_coverage.csv (default: "
             "Results_v21/name_key/patterns/name). Only used when --comparison-target is "
             "name or both.",
    )
    p.add_argument("--domain", default="")
    p.add_argument("--analysis-run-id", default="")
    p.add_argument("--min-support-count", type=int, default=3)
    p.add_argument("--min-support-pct", type=float, default=0.0)
    p.add_argument("--no-discover-populations", dest="discover_populations", action="store_false")
    p.set_defaults(discover_populations=True)
    p.add_argument("--min-population-size", type=int, default=0)
    p.add_argument("--max-population-overlap", type=float, default=0.20)
    p.add_argument("--min-population-jaccard", type=float, default=0.30)
    p.add_argument("--discovery-support-pct", type=float, default=0.10)
    p.add_argument("--compare", action="store_true")
    p.add_argument("--compute-share-profile", action="store_true")
    p.add_argument("--metadata-file", type=Path, default=None, help="Path to file_metadata.csv. Required when --roles is used.")
    p.add_argument("--roles", nargs="+", default=None, help="Governance roles: Project Template Generic Generic-Host Container, or alias template-group")
    p.add_argument(
        "--purge-view", choices=["all", "used", "both"], default=None,
        help="Default: both for --comparison-target config (unchanged); all for "
             "name/both, since ALL is the only view name-target supports. An explicit "
             "used/both under name/both still errors -- only the unset default is "
             "target-aware.",
    )
    p.add_argument("--latent-purgeable-file", type=Path, default=None, help="Path to latent_purgeable.csv")
    p.add_argument("--workers", type=int, default=4,
                   help="Max parallel domains for bundle analysis (default: 4)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    run_bundle_analysis_for_target(
        analysis_dir=args.analysis_dir,
        out_dir=args.out_dir,
        comparison_target=args.comparison_target,
        name_key_patterns_dir=args.name_key_patterns_dir,
        domain=args.domain,
        min_support_count=args.min_support_count,
        min_support_pct=args.min_support_pct,
        analysis_run_id=args.analysis_run_id,
        discover_populations_flag=args.discover_populations,
        min_population_size=args.min_population_size,
        max_population_overlap=args.max_population_overlap,
        min_population_jaccard=args.min_population_jaccard,
        discovery_support_pct=args.discovery_support_pct,
        compare=args.compare,
        compute_share_profile=args.compute_share_profile,
        roles=args.roles,
        metadata_file=args.metadata_file,
        purge_view=args.purge_view,
        latent_purgeable_file=args.latent_purgeable_file,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
