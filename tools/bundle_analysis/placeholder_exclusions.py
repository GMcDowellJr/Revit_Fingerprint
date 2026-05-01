from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    import sys

    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv, read_csv_rows
else:
    from .common import atomic_write_csv, read_csv_rows


TARGET_DOMAINS = ("wall_types", "ceiling_types", "floor_types", "roof_types")
MIN_GAP = 0.30
MIN_FILES = 5


def _is_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _largest_gap_threshold(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if len(values) < MIN_FILES:
        return None, None
    ordered = sorted(values)
    largest_gap = -1.0
    left = right = None
    for idx in range(len(ordered) - 1):
        a = ordered[idx]
        b = ordered[idx + 1]
        gap = b - a
        if gap > largest_gap:
            largest_gap = gap
            left, right = a, b
    if largest_gap < MIN_GAP or left is None or right is None:
        return None, largest_gap
    return (left + right) / 2.0, largest_gap


def compute_placeholder_exclusions(records_csv_path: Path, out_csv_path: Path) -> None:
    rows = read_csv_rows(records_csv_path)
    counts: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"total": 0, "purgeable": 0})
    for row in rows:
        dom = (row.get("domain", "") or "").strip()
        if dom not in TARGET_DOMAINS:
            continue
        # Use canonical export_run_id when available. Keep `file_id` as fallback
        # for backward compatibility with legacy flat-record exports.
        fid = (row.get("export_run_id", "") or "").strip() or (row.get("file_id", "") or "").strip()
        if not fid:
            continue
        key = (dom, fid)
        counts[key]["total"] += 1
        if _is_truthy(row.get("is_purgeable", "")):
            counts[key]["purgeable"] += 1

    by_domain: Dict[str, List[Dict[str, object]]] = {d: [] for d in TARGET_DOMAINS}
    for (dom, fid), c in counts.items():
        total = c["total"]
        pct = (float(c["purgeable"]) / float(total)) if total > 0 else None
        by_domain[dom].append({"domain": dom, "file_id": fid, "purgeable_pct": pct})

    out_rows: List[Dict[str, str]] = []
    for dom in TARGET_DOMAINS:
        entries = sorted(by_domain[dom], key=lambda r: str(r["file_id"]))
        pcts = [float(r["purgeable_pct"]) for r in entries if r["purgeable_pct"] is not None]
        threshold: Optional[float] = None
        if len(pcts) < MIN_FILES:
            print(f"[placeholder_exclusions] domain={dom} insufficient files ({len(pcts)}) for threshold derivation — exclusion skipped")
        else:
            threshold, largest_gap = _largest_gap_threshold(pcts)
            if threshold is None:
                print(f"[placeholder_exclusions] domain={dom} no clean gap found (largest_gap={float(largest_gap or 0.0):.3f} < 0.30) — exclusion skipped")

        excluded_count = 0
        for entry in entries:
            pct = float(entry["purgeable_pct"]) if entry["purgeable_pct"] is not None else 0.0
            excluded = threshold is not None and pct >= threshold
            if excluded:
                excluded_count += 1
            out_rows.append(
                {
                    "schema_version": "2.1",
                    "domain": dom,
                    "file_id": str(entry["file_id"]),
                    "purgeable_pct": f"{pct:.4f}",
                    "threshold": "" if threshold is None else f"{threshold:.4f}",
                    "excluded": "true" if excluded else "false",
                }
            )
        if threshold is not None:
            print(
                f"[placeholder_exclusions] domain={dom} threshold={threshold:.3f} "
                f"files_total={len(entries)} files_excluded={excluded_count} files_retained={len(entries) - excluded_count}"
            )

    atomic_write_csv(
        out_csv_path,
        ["schema_version", "domain", "file_id", "purgeable_pct", "threshold", "excluded"],
        sorted(out_rows, key=lambda r: (r["domain"], r["file_id"])),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute per-domain placeholder exclusions")
    parser.add_argument("--records-csv", required=True, type=Path)
    parser.add_argument("--out-csv", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    compute_placeholder_exclusions(args.records_csv, args.out_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
