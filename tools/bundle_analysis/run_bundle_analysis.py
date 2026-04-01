from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import read_csv_rows, resolve_analysis_run_id
    from step1_membership_matrix import build_membership_matrix
    from step2_find_bundles import find_bundles_for_domain
    from step3_build_dag import build_dag_for_domain
    from step4_difference_sets import emit_stub as emit_step4
    from step5_classify_patterns import emit_stub as emit_step5
    from step6_classify_files import emit_stub as emit_step6
    from step7_overlap_report import emit_stub as emit_step7
else:
    from .common import read_csv_rows, resolve_analysis_run_id
    from .step1_membership_matrix import build_membership_matrix
    from .step2_find_bundles import find_bundles_for_domain
    from .step3_build_dag import build_dag_for_domain
    from .step4_difference_sets import emit_stub as emit_step4
    from .step5_classify_patterns import emit_stub as emit_step5
    from .step6_classify_files import emit_stub as emit_step6
    from .step7_overlap_report import emit_stub as emit_step7


def run_bundle_analysis(
    analysis_dir: Path,
    out_dir: Path,
    domain: str = "",
    min_support_count: int = 3,
    min_support_pct: float = 0.0,
    analysis_run_id: str = "",
) -> Dict[str, int]:
    presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    run_id = resolve_analysis_run_id(presence_rows, analysis_run_id)

    domains = [domain] if domain else sorted({r.get("domain", "") for r in presence_rows if r.get("analysis_run_id", "") == run_id})

    total_bundles = 0
    total_edges = 0
    total_files_no_bundle = 0
    processed = 0

    for dom in domains:
        if not dom:
            continue
        processed += 1
        print(f"[run] domain={dom} start")
        try:
            t0 = time.time()
            build_membership_matrix(analysis_dir, out_dir, dom, run_id)
            print(f"[run] domain={dom} step1_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            step2 = find_bundles_for_domain(out_dir, dom, min_support_count, min_support_pct)
            total_bundles += step2.get("bundles", 0)
            print(f"[run] domain={dom} step2_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            step3 = build_dag_for_domain(out_dir, dom)
            total_edges += step3.get("edges", 0)
            print(f"[run] domain={dom} step3_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            emit_step4(out_dir, dom)
            print(f"[run] domain={dom} step4_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            emit_step5(out_dir, dom)
            print(f"[run] domain={dom} step5_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            step6 = emit_step6(out_dir, dom)
            total_files_no_bundle += step6.get("files_no_bundle", 0)
            print(f"[run] domain={dom} step6_seconds={time.time() - t0:.3f}")

            t0 = time.time()
            emit_step7(out_dir, dom)
            print(f"[run] domain={dom} step7_seconds={time.time() - t0:.3f}")
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


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bundle analysis pipeline")
    p.add_argument("--analysis-dir", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", default="")
    p.add_argument("--analysis-run-id", default="")
    p.add_argument("--min-support-count", type=int, default=3)
    p.add_argument("--min-support-pct", type=float, default=0.0)
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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
