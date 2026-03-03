from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import csv
import os
from collections import defaultdict
from typing import Any, Dict, Optional, Set, Tuple

from tools.analysis.authority.io import load_exports, get_domain_records
from tools.analysis.authority.report import write_json_report


def _norm_scalar(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    return s if s else None


def _get_top(record: Dict[str, Any], key: str) -> Optional[str]:
    return _norm_scalar(record.get(key))


def _get_p2_value(record: Dict[str, Any], bucket: str, k: str) -> Optional[str]:
    p2 = record.get("phase2")
    if not isinstance(p2, dict):
        return None
    items = p2.get(bucket)
    if not isinstance(items, list):
        return None
    for it in items:
        if not isinstance(it, dict):
            continue
        if it.get("k") == k:
            # preserve explicit missing/unreadable by returning None only if v is None
            return _norm_scalar(it.get("v"))
    return None


def _extract_features(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Extract the typography surfaces we care about, preferring top-level where present,
    falling back to semantic phase2 items when available.

    NOTE: This is descriptive: values are strings or None (explicit missing).
    """
    # Name / label surface: top.type_name is stable and present in your variability summary
    name = _get_top(record, "type_name") or _get_top(record, "label")

    # Font / size / width factor: your variability summary shows these as top.* keys
    font = _get_top(record, "font") or _get_p2_value(record, "semantic_items", "text_type.font")
    size = _get_top(record, "text_size_in") or _get_p2_value(record, "semantic_items", "text_type.size_in")
    width = _get_top(record, "width_factor") or _get_p2_value(record, "semantic_items", "text_type.width_factor")

    return name, font, size, width


def run(exports_dir: str, out_dir: str, min_files: int) -> None:
    exports = load_exports(exports_dir)
    os.makedirs(out_dir, exist_ok=True)

    # join_hash -> files present
    jh_files: Dict[str, Set[str]] = defaultdict(set)

    # join_hash -> set of observed feature values across files
    jh_name: Dict[str, Set[Optional[str]]] = defaultdict(set)
    jh_font: Dict[str, Set[Optional[str]]] = defaultdict(set)
    jh_size: Dict[str, Set[Optional[str]]] = defaultdict(set)
    jh_width: Dict[str, Set[Optional[str]]] = defaultdict(set)
    jh_tuple: Dict[str, Set[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]] = defaultdict(set)

    records_total = 0
    joinable_total = 0

    for e in exports:
        records = get_domain_records(e.data, "text_types")
        records_total += len(records)

        for r in records:
            if not isinstance(r, dict):
                continue
            jk = r.get("join_key")
            if not isinstance(jk, dict):
                continue
            jh = _norm_scalar(jk.get("join_hash"))
            if not jh:
                continue

            joinable_total += 1
            jh_files[jh].add(e.file_id)

            name, font, size, width = _extract_features(r)
            jh_name[jh].add(name)
            jh_font[jh].add(font)
            jh_size[jh].add(size)
            jh_width[jh].add(width)
            jh_tuple[jh].add((name, font, size, width))

    out_csv = os.path.join(out_dir, "text_types.joinhash_typography_variants.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "join_hash",
            "files_count",
            "distinct_name",
            "distinct_font",
            "distinct_size",
            "distinct_width_factor",
            "distinct_tuple",
        ])

        rows = []
        for jh, files in jh_files.items():
            if len(files) < min_files:
                continue
            rows.append((
                jh,
                len(files),
                len(jh_name[jh]),
                len(jh_font[jh]),
                len(jh_size[jh]),
                len(jh_width[jh]),
                len(jh_tuple[jh]),
            ))

        # sort: most fragmented first
        rows.sort(key=lambda r: (-r[6], -r[1], r[0]))
        for r in rows:
            w.writerow(list(r))

    report = {
        "analysis": "text_types_joinhash_typography_variants",
        "domain": "text_types",
        "min_files": int(min_files),
        "counts": {
            "files_total": int(len(exports)),
            "records_total": int(records_total),
            "joinable_records_total": int(joinable_total),
            "distinct_join_hash_total": int(len(jh_files)),
            "rows_emitted": int(sum(1 for jh in jh_files if len(jh_files[jh]) >= min_files)),
        },
        "outputs": {
            "csv": os.path.abspath(out_csv),
        },
        "assumptions": {
            "join_basis": "record.join_key.join_hash",
            "scope": "cross-file variation (not within-file collisions)",
            "name_source_preference": ["top.type_name", "top.label", "p2.cosmetic.text_type.name"],
            "font_source_preference": ["top.font", "p2.semantic.text_type.font"],
            "size_source_preference": ["top.text_size_in", "p2.semantic.text_type.size_in"],
            "width_source_preference": ["top.width_factor", "p2.semantic.text_type.width_factor"],
        },
    }

    out_report = os.path.join(out_dir, "text_types.joinhash_typography_variants.report.json")
    write_json_report(out_path=out_report, report=report)

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_report}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: text_types join_hash cross-file typography variants")
    p.add_argument("exports_dir")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    p.add_argument("--min-files", type=int, default=2, dest="min_files")
    ns = p.parse_args()
    run(ns.exports_dir, ns.out_dir, ns.min_files)


if __name__ == "__main__":
    main()
