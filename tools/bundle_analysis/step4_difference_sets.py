from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

from .common import atomic_write_csv, read_csv_rows


def emit_stub(out_dir: Path, domain: str) -> Dict[str, int]:
    domain_out_dir = out_dir / domain
    run_id = ""
    bundles_path = domain_out_dir / "bundles.csv"
    if bundles_path.is_file():
        rows = read_csv_rows(bundles_path)
        run_id = rows[0].get("analysis_run_id", "") if rows else ""
    atomic_write_csv(
        domain_out_dir / "bundle_dag_differences.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "child_bundle_id",
            "parent_bundle_id",
            "difference_pattern_ids",
            "difference_pattern_count",
            "difference_presence_pct_min",
            "difference_presence_pct_max",
            "difference_presence_pct_mean",
        ],
        [],
    )
    return {"rows": 0, "analysis_run_id": run_id}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 4")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
