from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

from .common import atomic_write_csv


def emit_stub(out_dir: Path, domain: str) -> Dict[str, int]:
    atomic_write_csv(
        out_dir / domain / "pattern_bundle_classification.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "pattern_id",
            "bundle_role",
            "bundle_count",
            "is_cross_branch_shared",
            "corpus_presence_pct",
        ],
        [],
    )
    return {"rows": 0}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 5")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
