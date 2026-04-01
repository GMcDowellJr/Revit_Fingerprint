from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv
else:
    from .common import atomic_write_csv


def emit_stub(out_dir: Path, domain: str) -> Dict[str, int]:
    base = out_dir / domain
    atomic_write_csv(
        base / "cross_bundle_overlap.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "pattern_id",
            "bundle_ids_containing",
            "bundle_count_total",
            "bundle_count_unrelated",
            "is_cross_branch_shared",
        ],
        [],
    )
    atomic_write_csv(
        base / "bundle_pair_overlap.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "bundle_id_a",
            "bundle_id_b",
            "relationship",
            "shared_pattern_count",
            "shared_pattern_ids",
        ],
        [],
    )
    return {"rows": 0}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 7")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
