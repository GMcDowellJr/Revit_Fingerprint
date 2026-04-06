from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id
    from step0_discover_populations import discover_populations
    from step1_membership_matrix import build_membership_matrix
    from step2_find_bundles import find_bundles_for_domain
    from step3_build_dag import build_dag_for_domain
    from step4_difference_sets import emit_stub as emit_step4
    from step5_classify_patterns import emit_stub as emit_step5
    from step6_classify_files import emit_stub as emit_step6
    from step7_overlap_report import emit_stub as emit_step7
    from reference_bundle import load_and_validate
    from step_compare import run_compare_for_domain
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id
    from .step0_discover_populations import discover_populations
    from .step1_membership_matrix import build_membership_matrix
    from .step2_find_bundles import find_bundles_for_domain
    from .step3_build_dag import build_dag_for_domain
    from .step4_difference_sets import emit_stub as emit_step4
    from .step5_classify_patterns import emit_stub as emit_step5
    from .step6_classify_files import emit_stub as emit_step6
    from .step7_overlap_report import emit_stub as emit_step7
    from .reference_bundle import load_and_validate
    from .step_compare import run_compare_for_domain

TIMING_FIELDNAMES = ["schema_version", "analysis_run_id", "domain", "population_id", "step", "seconds"]


def _run_pipeline_once(
    analysis_dir: Path,
    work_out_dir: Path,
    domain: str,
    run_id: str,
    min_support_count: int,
    min_support_pct: float,
    population_id: Optional[str] = None,
    analysis_run_id: str = "",
    population_registry_dir: Optional[Path] = None,
    scope_key_filter: Optional[str] = None,
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
    )
    t1 = time.time() - t0
    print(f"[run] domain={domain} step1_seconds={t1:.3f}")

    t0 = time.time()
    step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
    total_bundles += step2.get("bundles", 0)
    t2 = time.time() - t0
    print(f"[run] domain={domain} step2_seconds={t2:.3f}")

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

    total = t1 + t2 + t3 + t4 + t5 + t6 + t7
    print(
        f"[timing] summary domain={domain} population_id={population_id or 'none'} "
        f"step1={t1:.2f} step2={t2:.2f} step3={t3:.2f} step4={t4:.2f} "
        f"step5={t5:.2f} step6={t6:.2f} step7={t7:.2f} total={total:.2f}"
    )

    return {
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
        "step_times": {
            "step1": t1,
            "step2": t2,
            "step3": t3,
            "step4": t4,
            "step5": t5,
            "step6": t6,
            "step7": t7,
        },
    }


def _run_step2_to_step7(
    work_out_dir: Path,
    domain: str,
    min_support_count: int,
    min_support_pct: float,
) -> Dict[str, object]:
    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0

    t0 = time.time()
    step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
    total_bundles += step2.get("bundles", 0)
    t2 = time.time() - t0
    print(f"[run] domain={domain} step2_seconds={t2:.3f}")

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
        "step_times": {"step2": t2, "step3": t3, "step4": t4, "step5": t5, "step6": t6, "step7": t7},
    }


def run_bundle_analysis(
    analysis_dir: Path,
    out_dir: Path,
    domain: str = "",
    min_support_count: int = 3,
    min_support_pct: float = 0.0,
    analysis_run_id: str = "",
    discover_populations_flag: bool = False,
    min_population_size: int = 0,
    max_population_overlap: float = 0.20,
    min_population_jaccard: float = 0.30,
    discovery_support_pct: float = 0.10,
    compare: bool = False,
) -> Dict[str, int]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, analysis_run_id)

    domains = [domain] if domain else sorted({r.get("domain", "") for r in presence_rows if r.get("analysis_run_id", "") == run_id})

    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0
    processed = 0
    domain_elapsed_seconds: Dict[str, float] = {}
    domain_population_counts: Dict[str, int] = {}
    timing_rows: List[Dict[str, str]] = []
    compare_summary_rows: List[Dict[str, str]] = []
    reference: Optional[Dict[str, object]] = None

    if compare:
        if discover_populations_flag:
            raise ValueError("--compare is not supported with --discover-populations.")
        reference = load_and_validate(analysis_dir, SCHEMA_VERSION)
        compare_dir = out_dir / "compare"
        compare_dir.mkdir(parents=True, exist_ok=True)

    if not discover_populations_flag:
        for dom in domains:
            if not dom:
                continue
            processed += 1
            print(f"[run] domain={dom} start")
            try:
                if not compare:
                    stats = _run_pipeline_once(
                        analysis_dir=analysis_dir,
                        work_out_dir=out_dir,
                        domain=dom,
                        run_id=run_id,
                        min_support_count=min_support_count,
                        min_support_pct=min_support_pct,
                        analysis_run_id=run_id,
                    )
                else:
                    t0 = time.time()
                    build_membership_matrix(
                        analysis_dir,
                        out_dir,
                        dom,
                        run_id,
                        None,
                        None,
                        None,
                    )
                    t1 = time.time() - t0
                    print(f"[run] domain={dom} step1_seconds={t1:.3f}")

                    workers = max(2, min(4, (len(domains) or 1)))
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        discovery_future = executor.submit(
                            _run_step2_to_step7,
                            out_dir,
                            dom,
                            min_support_count,
                            min_support_pct,
                        )
                        compare_started = time.time()
                        compare_future = executor.submit(
                            run_compare_for_domain,
                            analysis_dir,
                            out_dir,
                            reference or {},
                            dom,
                        )
                        tail = discovery_future.result()
                        compare_summary = compare_future.result()
                    compare_seconds = time.time() - compare_started
                    compare_summary_rows.append(compare_summary)
                    step_times = {"step1": t1, **tail.get("step_times", {})}
                    print(
                        f"[timing] domain={dom} discovery_seconds={sum(float(step_times.get(k, 0.0)) for k in ('step1','step2','step3','step4','step5','step6','step7')):.3f} "
                        f"compare_seconds={compare_seconds:.3f}"
                    )
                    stats = {
                        "total_bundles_found": tail.get("total_bundles_found", 0),
                        "total_dag_edges": tail.get("total_dag_edges", 0),
                        "files_with_no_bundle_match": tail.get("files_with_no_bundle_match", 0),
                        "step_times": step_times,
                    }
                total_bundles += stats["total_bundles_found"]
                total_edges += stats["total_dag_edges"]
                total_files_no_bundle += stats["files_with_no_bundle_match"]
                step_times = stats.get("step_times", {})
                for step_name in ("step1", "step2", "step3", "step4", "step5", "step6", "step7"):
                    timing_rows.append(
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

        existing_timing_rows = read_csv_rows(out_dir / "bundle_analysis_timing.csv") if (out_dir / "bundle_analysis_timing.csv").exists() else []
        merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + timing_rows
        merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
        atomic_write_csv(out_dir / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)
        if compare:
            compare_rows = [r for r in compare_summary_rows if r.get("analysis_run_id", "") == run_id]
            compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", "")))
            atomic_write_csv(
                out_dir / "compare" / "compare_run_summary.csv",
                [
                    "reference_bundle_id",
                    "effective_date",
                    "analysis_run_id",
                    "domain",
                    "files_scored",
                    "full_count",
                    "partial_count",
                    "none_count",
                    "no_reference_count",
                ],
                compare_rows,
            )

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

    step0_times: Dict[str, float] = {}
    domain_primary_counts: Dict[str, int] = {}
    outliers_by_domain: Dict[str, int] = {}

    populations_analyzed = 0
    staging_root = out_dir / "_population_runs"
    if staging_root.exists():
        shutil.rmtree(staging_root)

    for dom in domains:
        if not dom:
            continue
        processed += 1
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
            stage_out = staging_root / f"{dom}__{pid}"
            # `pid` already includes the "pop_" prefix from step0.
            final_out = out_dir / dom / pid
            if stage_out.exists():
                shutil.rmtree(stage_out)
            if final_out.exists():
                shutil.rmtree(final_out)
            try:
                t0 = time.time()
                stats = _run_pipeline_once(
                    analysis_dir=analysis_dir,
                    work_out_dir=stage_out,
                    domain=dom,
                    run_id=run_id,
                    min_support_count=min_support_count,
                    min_support_pct=min_support_pct,
                    population_id=pid,
                    analysis_run_id=run_id,
                    population_registry_dir=out_dir,
                    scope_key_filter=population_scope_key,
                )
                domain_elapsed_seconds[dom] = domain_elapsed_seconds.get(dom, 0.0) + (time.time() - t0)
                total_bundles += stats["total_bundles_found"]
                total_edges += stats["total_dag_edges"]
                total_files_no_bundle += stats["files_with_no_bundle_match"]
                step_times = stats.get("step_times", {})
                for step_name in ("step1", "step2", "step3", "step4", "step5", "step6", "step7"):
                    timing_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "analysis_run_id": run_id,
                            "domain": dom,
                            "population_id": pid,
                            "step": step_name,
                            "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
                        }
                    )

                produced = stage_out / dom
                final_out.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(produced), str(final_out))
            except Exception as exc:
                print(f"[run][error] domain={dom} population_id={pid} failed: {exc}")

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
            timing_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "population_id": "",
                    "step": "step0",
                    "seconds": f"{step0_times.get(dom, 0.0):.3f}",
                }
            )

    existing_timing_rows = read_csv_rows(out_dir / "bundle_analysis_timing.csv") if (out_dir / "bundle_analysis_timing.csv").exists() else []
    merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + timing_rows
    merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
    atomic_write_csv(out_dir / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)

    return {
        "domains_processed": processed,
        "populations_analyzed": populations_analyzed,
        "total_outlier_files": total_outliers,
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
    }


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bundle analysis pipeline")
    p.add_argument("--analysis-dir", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", default="")
    p.add_argument("--analysis-run-id", default="")
    p.add_argument("--min-support-count", type=int, default=3)
    p.add_argument("--min-support-pct", type=float, default=0.0)
    p.add_argument("--discover-populations", action="store_true")
    p.add_argument("--min-population-size", type=int, default=0)
    p.add_argument("--max-population-overlap", type=float, default=0.20)
    p.add_argument("--min-population-jaccard", type=float, default=0.30)
    p.add_argument("--discovery-support-pct", type=float, default=0.10)
    p.add_argument("--compare", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    run_bundle_analysis(
        analysis_dir=args.analysis_dir,
        out_dir=args.out_dir,
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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
