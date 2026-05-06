from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from .compare import ChangeCounts


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_change_type_csv(
    *,
    out_path: str,
    rows: Iterable[ChangeCounts],
) -> None:
    """Write baseline-vs-file change classification counts.

    CSV is purely descriptive: counts only.
    """
    ensure_dir(os.path.dirname(out_path) or ".")

    fieldnames = [
        "domain",
        "baseline_file_id",
        "other_file_id",
        "added",
        "removed",
        "same",
        "modified",
        "ambiguous_duplicates",
        "ambiguous_bad_items",
        "baseline_unjoinable",
        "other_unjoinable",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            d = asdict(r)
            w.writerow({k: d.get(k) for k in fieldnames})


def write_json_report(
    *,
    out_path: str,
    report: Dict[str, Any],
) -> None:
    """Write a small JSON report containing provenance + counts + assumptions."""
    ensure_dir(os.path.dirname(out_path) or ".")

    # Attach a timestamp if caller didn't.
    if "generated_utc" not in report:
        report = dict(report)
        report["generated_utc"] = utc_timestamp()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")


def format_console_summary(
    *,
    domain: str,
    baseline_file_id: str,
    counts: List[ChangeCounts],
) -> str:
    """Console-friendly summary text for baseline vs others."""
    lines: List[str] = []
    lines.append(f"Phase-2 analysis: change type (domain={domain})")
    lines.append(f"Baseline: {baseline_file_id}")
    lines.append("")

    if not counts:
        lines.append("No comparisons computed.")
        return "\n".join(lines)

    # Totals across all comparisons (descriptive only)
    tot = {
        "added": 0,
        "removed": 0,
        "same": 0,
        "modified": 0,
        "ambiguous_duplicates": 0,
        "ambiguous_bad_items": 0,
    }
    for c in counts:
        for k in list(tot.keys()):
            tot[k] += int(getattr(c, k))

    lines.append(
        "Totals across comparisons: "
        + ", ".join([f"{k}={v}" for k, v in tot.items()])
    )
    lines.append("")

    # Per-file one-liners
    for c in counts:
        lines.append(
            f"{c.other_file_id}: added={c.added} removed={c.removed} same={c.same} modified={c.modified} "
            f"amb_dup={c.ambiguous_duplicates} amb_bad_items={c.ambiguous_bad_items} "
            f"unjoinable(base={c.baseline_unjoinable}, other={c.other_unjoinable})"
        )

    return "\n".join(lines)
