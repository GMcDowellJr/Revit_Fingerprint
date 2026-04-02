from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import read_csv_rows, resolve_analysis_run_id
    from step0_discover_populations import discover_populations
    from step1_membership_matrix import build_membership_matrix
    from step2_find_bundles import find_bundles_for_domain
    from step3_build_dag import build_dag_for_domain
    from step4_difference_sets import emit_stub as emit_step4
    from step5_classify_patterns import emit_stub as emit_step5
    from step6_classify_files import emit_stub as emit_step6
    from step7_overlap_report import emit_stub as emit_step7
else:
    from .common import read_csv_rows, resolve_analysis_run_id
    from .step0_discover_populations import discover_populations
    from .step1_membership_matrix import build_membership_matrix
    from .step2_find_bundles import find_bundles_for_domain
    from .step3_build_dag import build_dag_for_domain
    from .step4_difference_sets import emit_stub as emit_step4
    from .step5_classify_patterns import emit_stub as emit_step5
    from .step6_classify_files import emit_stub as emit_step6
    from .step7_overlap_report import emit_stub as emit_step7


def _run_pipeline_once(
    analysis_dir: Path,
    work_out_dir: Path,
    domain: str,
    run_id: str,
    min_support_count: int,
    min_support_pct: float,
    population_id: Optional[str] = None,
    population_registry_dir: Optional[Path] = None,
) -> Dict[str, int]:
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
    )
    print(f"[run] domain={domain} step1_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
    total_bundles += step2.get("bundles", 0)
    print(f"[run] domain={domain} step2_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    step3 = build_dag_for_domain(work_out_dir, domain)
    total_edges += step3.get("edges", 0)
    print(f"[run] domain={domain} step3_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    emit_step4(work_out_dir, domain)
    print(f"[run] domain={domain} step4_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    emit_step5(work_out_dir, domain)
    print(f"[run] domain={domain} step5_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    step6 = emit_step6(work_out_dir, domain)
    total_files_no_bundle += step6.get("files_no_bundle", 0)
    print(f"[run] domain={domain} step6_seconds={time.time() - t0:.3f}")

    t0 = time.time()
    emit_step7(work_out_dir, domain)
    print(f"[run] domain={domain} step7_seconds={time.time() - t0:.3f}")

    return {
        "total_bundles_found": total_bundles,
        "total_dag_edges": total_edges,
        "files_with_no_bundle_match": total_files_no_bundle,
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
) -> Dict[str, int]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, analysis_run_id)

    domains = [domain] if domain else sorted({r.get("domain", "") for r in presence_rows if r.get("analysis_run_id", "") == run_id})

    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0
    processed = 0

    if not discover_populations_flag:
        for dom in domains:
            if not dom:
                continue
            processed += 1
            print(f"[run] domain={dom} start")
            try:
                stats = _run_pipeline_once(
                    analysis_dir=analysis_dir,
                    work_out_dir=out_dir,
                    domain=dom,
                    run_id=run_id,
                    min_support_count=min_support_count,
                    min_support_pct=min_support_pct,
                )
                total_bundles += stats["total_bundles_found"]
                total_edges += stats["total_dag_edges"]
                total_files_no_bundle += stats["files_with_no_bundle_match"]
            except Exception as exc:
                print(f"[run][error] domain={dom} failed: {exc}")

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

    for dom in domains:
        if not dom:
            continue
        processed += 1
        try:
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
        except Exception as exc:
            print(f"[run][error] domain={dom} step0 failed: {exc}")

    summary_rows = read_csv_rows(out_dir / "corpus_population_summary.csv") if (out_dir / "corpus_population_summary.csv").exists() else []
    domain_populations: Dict[str, List[str]] = {}
    outliers_by_domain: Dict[str, int] = {}
    for row in summary_rows:
        if row.get("analysis_run_id", "") != run_id:
            continue
        dom = row.get("domain", "")
        if row.get("population_role", "") == "primary":
            domain_populations.setdefault(dom, []).append(row.get("population_id", ""))
        elif row.get("population_role", "") == "outlier":
            outliers_by_domain[dom] = int(row.get("file_count", "0") or "0")

    populations_analyzed = 0
    staging_root = out_dir / "_population_runs"
    if staging_root.exists():
        shutil.rmtree(staging_root)

    for dom in domains:
        if not dom:
            continue
        pop_ids = sorted(set(pid for pid in domain_populations.get(dom, []) if pid))
        if not pop_ids:
            print(f"[run][warn] domain={dom} has no primary populations; skipping main pass")
            continue
        for pid in pop_ids:
            print(f"[run] domain={dom} population_id={pid} start")
            populations_analyzed += 1
            stage_out = staging_root / f"{dom}__{pid}"
            final_out = out_dir / dom / f"pop_{pid}"
            if stage_out.exists():
                shutil.rmtree(stage_out)
            if final_out.exists():
                shutil.rmtree(final_out)
            try:
                stats = _run_pipeline_once(
                    analysis_dir=analysis_dir,
                    work_out_dir=stage_out,
                    domain=dom,
                    run_id=run_id,
                    min_support_count=min_support_count,
                    min_support_pct=min_support_pct,
                    population_id=pid,
                    population_registry_dir=out_dir,
                )
                total_bundles += stats["total_bundles_found"]
                total_edges += stats["total_dag_edges"]
                total_files_no_bundle += stats["files_with_no_bundle_match"]

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
            f"    {dom}: {len(set(domain_populations.get(dom, [])))} populations, "
            f"{outliers_by_domain.get(dom, 0)} outliers"
        )

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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
