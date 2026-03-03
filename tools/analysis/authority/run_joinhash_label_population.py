from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import csv
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from tools.analysis.authority.io import load_exports, get_domain_records
from tools.analysis.authority.report import write_json_report


def _get_join_hash(record: Dict[str, Any]) -> Optional[str]:
    jk = record.get("join_key")
    if not isinstance(jk, dict):
        return None
    h = jk.get("join_hash")
    if h is None:
        return None
    try:
        hs = str(h).strip()
    except Exception:
        return None
    return hs or None


def _extract_label_qv(record: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Best-effort extraction of a label surface.

    Preserves explicit states:
    - If missing => ("missing", None)
    - If unreadable-like shape => ("unreadable", None)
    - Otherwise => ("ok", <string value>)

    We do NOT infer meaning from provenance; we only read what's present.
    """
    if "label" not in record:
        return "missing", None

    lab = record.get("label")

    # Common patterns:
    # - label: "Some Name"
    # - label: { "q": "ok", "v": "Some Name", ... }
    # - label: { "display": "...", ... }
    if isinstance(lab, str):
        return "ok", lab

    if isinstance(lab, dict):
        q = lab.get("q")
        if isinstance(q, str) and q.strip():
            qs = q.strip()
            v = lab.get("v")
            if v is None:
                # explicit non-ok state with null v
                return qs, None
            try:
                return qs, str(v)
            except Exception:
                return qs, None

        # no explicit q/v; try display/value-like fields
        for k in ("display", "value", "name"):
            if k in lab and lab.get(k) is not None:
                try:
                    return "ok", str(lab.get(k))
                except Exception:
                    return "ok", None

        # Unknown dict shape => treat as unreadable (explicitly)
        return "unreadable", None

    # Non-string/non-dict => unreadable
    return "unreadable", None


def run_joinhash_label_population(
    *,
    exports_dir: str,
    domain: str,
    out_dir: str,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    os.makedirs(out_dir, exist_ok=True)

    # Aggregates across *records* (including collisions)
    # (join_hash, label_q, label_v) -> {records_count, files_set}
    agg_records = defaultdict(int)
    agg_files = defaultdict(set)

    # Per-file mapping (explicitly shows multiplicity and within-file variation)
    # (file_id, join_hash, label_q, label_v) -> count
    per_file = defaultdict(int)

    unjoinable = 0
    records_total = 0

    for e in exports:
        recs = get_domain_records(e.data, domain)
        records_total += len(recs)

        for r in recs:
            if not isinstance(r, dict):
                continue
            jh = _get_join_hash(r)
            if jh is None:
                unjoinable += 1
                continue

            q, v = _extract_label_qv(r)
            key = (jh, q, v)

            agg_records[key] += 1
            agg_files[key].add(e.file_id)

            per_file[(e.file_id, jh, q, v)] += 1

    pop_csv = os.path.join(out_dir, f"{domain}.joinhash_label_population.csv")
    with open(pop_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "join_hash",
            "label_q",
            "label_v",
            "records_count",
            "files_count",
        ])

        # Sort by files_count desc, then records_count desc
        keys = list(agg_records.keys())
        keys.sort(
            key=lambda k: (
                -len(agg_files[k]),
                -agg_records[k],
                k[0],
                k[1],
                "" if k[2] is None else k[2],
            )
        )
        for (jh, q, v) in keys:
            w.writerow([
                domain,
                jh,
                q,
                "" if v is None else v,
                int(agg_records[(jh, q, v)]),
                int(len(agg_files[(jh, q, v)])),
            ])

    by_file_csv = os.path.join(out_dir, f"{domain}.joinhash_label_population_by_file.csv")
    with open(by_file_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "file_id",
            "join_hash",
            "label_q",
            "label_v",
            "records_count_in_file",
        ])

        rows = list(per_file.items())
        rows.sort(
            key=lambda kv: (
                kv[0][0],          # file_id
                kv[0][1],          # join_hash
                kv[0][2],          # label_q
                "" if kv[0][3] is None else kv[0][3],  # label_v
            )
        )
        for (file_id, jh, q, v), c in rows:
            w.writerow([
                domain,
                file_id,
                jh,
                q,
                "" if v is None else v,
                int(c),
            ])

    report = {
        "phase": "phase2_analysis",
        "analysis": "joinhash_label_population",
        "domain": domain,
        "files_total": len(exports),
        "counts": {
            "records_total": int(records_total),
            "unjoinable_records_total": int(unjoinable),
            "distinct_joinhash_label_pairs": int(len(agg_records)),
        },
        "outputs": {
            "population_csv": os.path.abspath(pop_csv),
            "by_file_csv": os.path.abspath(by_file_csv),
        },
        "assumptions": {
            "join_key": "record.join_key.join_hash",
            "label_source": "record.label (q/v if present; else display/value/name best-effort)",
            "scope": "descriptive frequencies only; no normalization; collisions included",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.joinhash_label_population.report.json")
    write_json_report(out_path=json_path, report=report)

    print("join_hash × label population written:")
    print(f"  {pop_csv}")
    print(f"  {by_file_csv}")
    print(f"  {json_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: join_hash × label (Name) population frequencies")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    ns = p.parse_args()

    run_joinhash_label_population(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        out_dir=ns.out_dir,
    )


if __name__ == "__main__":
    main()
