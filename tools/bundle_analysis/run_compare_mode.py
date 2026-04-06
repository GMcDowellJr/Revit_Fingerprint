from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    _REPO_ROOT = _THIS_DIR.parent.parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv
    from reference_bundle import derive_from_analysis_output, load_and_validate
    from step_compare import run_compare
else:
    from .common import SCHEMA_VERSION, atomic_write_csv
    from .reference_bundle import derive_from_analysis_output, load_and_validate
    from .step_compare import run_compare

SUMMARY_FIELDNAMES = [
    "reference_bundle_id",
    "effective_date",
    "analysis_run_id",
    "domain",
    "files_scored",
    "full_count",
    "partial_count",
    "none_count",
    "no_reference_count",
]


def _extract_from_rvt(rvt_path: Path, work_dir: Path) -> Path:
    work_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(_REPO_ROOT / "tools" / "run_extract_all.py"),
        str(rvt_path),
        "--out-root",
        str(work_dir),
        "--stages",
        "flatten,discover,apply,analyze1",
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"RVT extraction failed for {rvt_path} with exit code {result.returncode}. "
            f"Command: {' '.join(cmd)}"
        )

    analysis_out_dir = work_dir / "Results_v21" / "analysis_v21"
    if not analysis_out_dir.is_dir():
        raise RuntimeError(
            f"RVT extraction completed but analysis output directory was not found: {analysis_out_dir}"
        )
    return analysis_out_dir


def _discover_membership_domains(analysis_dir: Path) -> Set[str]:
    domains: Set[str] = set()
    for child in analysis_dir.iterdir():
        if child.is_dir() and (child / "membership_matrix.csv").is_file():
            domains.add(child.name)
    return domains


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run compare mode against a reference bundle")
    parser.add_argument("--analysis-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--domain", default="")
    parser.add_argument("--analysis-run-id", default="")
    return parser.parse_args(argv)




def run_compare_mode(
    analysis_dir: Path,
    out_dir: Path,
    reference_path: Path,
    domain: str = "",
    analysis_run_id: str = "",
) -> List[Dict[str, str]]:
    out_dir.mkdir(parents=True, exist_ok=True)

    ref_path = Path(reference_path)
    if ref_path.suffix.lower() == ".rvt":
        work_dir = out_dir / f"_rvt_extract_{ref_path.stem}"
        analysis_out_dir = _extract_from_rvt(ref_path, work_dir)
        reference = derive_from_analysis_output(analysis_out_dir, ref_path, SCHEMA_VERSION)
    else:
        reference = load_and_validate(ref_path, SCHEMA_VERSION)

    reference_for_compare = dict(reference)
    if analysis_run_id:
        reference_for_compare["_analysis_run_id_filter"] = analysis_run_id

    membership_domains = _discover_membership_domains(analysis_dir)
    reference_domains = set(reference_for_compare.get("domains", {}).keys())
    domains = sorted(reference_domains | membership_domains)
    if domain:
        domains = [d for d in domains if d == domain]

    summary_rows: List[Dict[str, str]] = []

    for dom in domains:
        if dom not in membership_domains:
            continue

        stats = run_compare(
            analysis_dir=analysis_dir,
            out_dir=out_dir,
            reference=reference_for_compare,
            domain=dom,
        )
        print(
            f"[compare] domain={dom} files_scored={stats['files_scored']} "
            f"full={stats['full']} partial={stats['partial']} none={stats['none']} "
            f"no_reference={stats['no_reference']}"
        )

        summary_rows.append(
            {
                "reference_bundle_id": str(reference_for_compare["reference_bundle_id"]),
                "effective_date": str(reference_for_compare["effective_date"]),
                "analysis_run_id": str(stats["analysis_run_id"]),
                "domain": dom,
                "files_scored": str(stats["files_scored"]),
                "full_count": str(stats["full"]),
                "partial_count": str(stats["partial"]),
                "none_count": str(stats["none"]),
                "no_reference_count": str(stats["no_reference"]),
            }
        )

    summary_rows.sort(key=lambda r: (r["analysis_run_id"], r["domain"]))
    atomic_write_csv(out_dir / "compare_run_summary.csv", SUMMARY_FIELDNAMES, summary_rows)
    return summary_rows


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    run_compare_mode(
        analysis_dir=args.analysis_dir,
        out_dir=args.out_dir,
        reference_path=args.reference,
        domain=args.domain,
        analysis_run_id=args.analysis_run_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
