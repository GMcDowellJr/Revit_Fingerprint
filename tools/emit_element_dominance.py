#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Optional, Tuple

SCHEMA_VERSION = "2.1.0"
STANDARD_PRESENCE_MIN = 0.75
ROW_KEY_DOMAINS = ("object_styles_model", "object_styles_annotation", "view_category_overrides")


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _split_label(pattern_label_human: str) -> Tuple[str, str]:
    if "|" not in pattern_label_human:
        return pattern_label_human, ""
    left, right = pattern_label_human.split("|", 1)
    return left, right


def _write_csv_atomic(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent)) as tmp:
        tmp_path = Path(tmp.name)
        w = csv.DictWriter(tmp, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
    tmp_path.replace(path)


def emit_element_dominance(analysis_dir: Path, domain: Optional[str] = None) -> Path:
    analysis_dir = Path(analysis_dir)
    domain_patterns_csv = analysis_dir / "domain_patterns.csv"
    authority_csv = analysis_dir / "phase2_authority_pattern.csv"
    out_csv = analysis_dir / "element_dominance.csv"

    if not domain_patterns_csv.is_file():
        raise FileNotFoundError(f"Missing input CSV: {domain_patterns_csv}")
    if not authority_csv.is_file():
        raise FileNotFoundError(f"Missing input CSV: {authority_csv}")

    selected_domains = [domain] if domain else list(ROW_KEY_DOMAINS)
    invalid = [d for d in selected_domains if d not in ROW_KEY_DOMAINS]
    if invalid:
        raise ValueError(f"--domain must be one of {ROW_KEY_DOMAINS}, got: {invalid}")

    patterns_rows = _read_csv_rows(domain_patterns_csv)
    authority_rows = _read_csv_rows(authority_csv)

    authority_by_domain_pid: Dict[Tuple[str, str], Dict[str, str]] = {
        (r.get("domain", ""), r.get("pattern_id", "")): r for r in authority_rows
    }
    grouped: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in patterns_rows:
        dom = row.get("domain", "")
        if dom not in selected_domains:
            continue
        pattern_label_human = row.get("pattern_label_human", "")
        element_label, sub_label = _split_label(pattern_label_human)
        grouped[(dom, element_label, sub_label)].append(row)

    output_rows: List[Dict[str, str]] = []
    by_domain_counts: Dict[str, int] = defaultdict(int)
    by_domain_candidates: Dict[str, int] = defaultdict(int)

    for (dom, element_label, sub_label), rows in sorted(grouped.items(), key=lambda kv: kv[0]):
        candidates: List[Tuple[float, str, Dict[str, str], Dict[str, str]]] = []
        for p_row in rows:
            pid = p_row.get("pattern_id", "")
            auth = authority_by_domain_pid.get((dom, pid))
            if not auth:
                continue
            try:
                presence_pct = float(auth.get("presence_pct", "0") or 0.0)
            except ValueError:
                presence_pct = 0.0
            candidates.append((presence_pct, pid, p_row, auth))
        if not candidates:
            continue
        dominant_presence, dominant_pid, dominant_pattern, dominant_authority = sorted(
            candidates,
            key=lambda item: (-item[0], item[1]),
        )[0]
        is_candidate_standard = "true" if dominant_presence >= STANDARD_PRESENCE_MIN else "false"
        out_row = {
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": dominant_pattern.get("analysis_run_id", ""),
            "domain": dom,
            "element_label": element_label,
            "sub_label": sub_label,
            "pattern_label_human": dominant_pattern.get("pattern_label_human", ""),
            "variant_count": str(len({r.get('pattern_id', '') for r in rows if r.get('pattern_id', '')})),
            "dominant_pattern_id": dominant_pid,
            "dominant_presence_pct": f"{dominant_presence:.6f}",
            "dominant_files_present": dominant_authority.get("files_present", ""),
            "files_total": dominant_authority.get("files_total", ""),
            "is_element_candidate_standard": is_candidate_standard,
            "is_cad_import": "true" if dominant_pattern.get("is_cad_import", "") == "true" else "false",
            "confidence_tier": dominant_authority.get("confidence_tier", ""),
        }
        output_rows.append(out_row)
        by_domain_counts[dom] += 1
        if is_candidate_standard == "true":
            by_domain_candidates[dom] += 1

    fieldnames = [
        "schema_version",
        "analysis_run_id",
        "domain",
        "element_label",
        "sub_label",
        "pattern_label_human",
        "variant_count",
        "dominant_pattern_id",
        "dominant_presence_pct",
        "dominant_files_present",
        "files_total",
        "is_element_candidate_standard",
        "is_cad_import",
        "confidence_tier",
    ]
    sorted_rows = sorted(output_rows, key=lambda r: (r["domain"], r["element_label"], r["sub_label"]))
    _write_csv_atomic(out_csv, fieldnames, sorted_rows)

    for dom in selected_domains:
        print(
            f"[emit_element_dominance] domain={dom} elements={by_domain_counts.get(dom, 0)} "
            f"candidate_standards={by_domain_candidates.get(dom, 0)}",
            flush=True,
        )
    print(f"[emit_element_dominance] done: {len(sorted_rows)} total rows written to {out_csv}", flush=True)
    return out_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit element_dominance.csv from v2.1 analysis CSVs.")
    parser.add_argument("--analysis-dir", required=True, help="Path to Results_v21/analysis_v21 directory.")
    parser.add_argument("--domain", help="Optional single row_key domain filter.")
    args = parser.parse_args()
    emit_element_dominance(Path(args.analysis_dir), domain=args.domain)


if __name__ == "__main__":
    main()
