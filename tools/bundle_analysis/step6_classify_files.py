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
    atomic_write_csv(
        out_dir / domain / "file_bundle_classification.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "export_run_id",
            "scope_key",
            "primary_bundle_id",
            "primary_bundle_depth",
            "is_ambiguous",
            "bundle_count",
            "file_pattern_count",
            "noise_count_primary",
            "noise_count_any",
            "noise_pct_primary",
            "noise_pct_any",
        ],
        [],
    )
    return {"rows": 0, "files_no_bundle": 0}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 6")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
