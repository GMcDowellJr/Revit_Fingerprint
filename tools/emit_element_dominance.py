#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Optional, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from compute_governance_thresholds import jenks_natural_breaks

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

    authority_by_run_domain_pid: Dict[Tuple[str, str, str], Dict[str, str]] = {
        (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("pattern_id", "")): r
        for r in authority_rows
    }
    grouped: Dict[Tuple[str, str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in patterns_rows:
        run_id = row.get("analysis_run_id", "")
        dom = row.get("domain", "")
        if dom not in selected_domains:
            continue
        if row.get("is_cad_import", "").strip().lower() == "true":
            continue
        pattern_label_human = row.get("pattern_label_human", "")
        element_label, sub_label = _split_label(pattern_label_human)
        grouped[(run_id, dom, element_label, sub_label)].append(row)

    output_rows: List[Dict[str, str]] = []
    by_domain_counts: Dict[str, int] = defaultdict(int)
    by_domain_candidates: Dict[str, int] = defaultdict(int)

    for (run_id, dom, element_label, sub_label), rows in sorted(grouped.items(), key=lambda kv: kv[0]):
        candidates: List[Tuple[float, str, Dict[str, str], Dict[str, str]]] = []
        for p_row in rows:
            pid = p_row.get("pattern_id", "")
            auth = authority_by_run_domain_pid.get((run_id, dom, pid))
            if not auth:
                continue
            try:
                presence_pct = float(auth.get("presence_pct", "0") or 0.0)
            except ValueError:
                presence_pct = 0.0
            candidates.append((presence_pct, pid, p_row, auth))
        if not candidates:
            continue
        sorted_candidates = sorted(candidates, key=lambda item: (-item[0], item[1]))
        dominant_presence, dominant_pid, dominant_pattern, dominant_authority = sorted_candidates[0]
        if len(sorted_candidates) >= 2:
            runner_up_presence = sorted_candidates[1][0]
            lead_gap = dominant_presence - runner_up_presence
        else:
            lead_gap = dominant_presence
        is_candidate_standard = "true" if dominant_presence >= STANDARD_PRESENCE_MIN else "false"
        out_row = {
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": run_id,
            "domain": dom,
            "element_label": element_label,
            "sub_label": sub_label,
            "pattern_label_human": dominant_pattern.get("pattern_label_human", ""),
            "variant_count": str(len({r.get('pattern_id', '') for r in rows if r.get('pattern_id', '')})),
            "dominant_pattern_id": dominant_pid,
            "dominant_presence_pct": f"{dominant_presence:.6f}",
            "lead_gap": f"{lead_gap:.6f}",
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

    def _compute_breaks(values: List[float], n_classes: int = 3) -> Tuple[float, float, str]:
        if len(set(values)) >= n_classes:
            breaks = sorted(jenks_natural_breaks(values, n_classes))
            return breaks[0], breaks[1], "jenks_natural_breaks"
        return STANDARD_PRESENCE_MIN / 2.0, STANDARD_PRESENCE_MIN, "fallback_standard_presence_min"

    def _bucket(presence_pct: float, competitive_min: float, standard_min: float) -> str:
        if presence_pct >= standard_min:
            return "standard"
        if presence_pct >= competitive_min:
            return "competitive"
        return "fragmented"

    rows_by_run: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in output_rows:
        rows_by_run[row.get("analysis_run_id", "")].append(row)

    thresholds_csv = analysis_dir / "element_characterization_thresholds.csv"
    threshold_rows: List[Dict[str, str]] = []
    thresholds_by_run: Dict[str, Tuple[float, float]] = {}
    if domain and thresholds_csv.is_file():
        for row in _read_csv_rows(thresholds_csv):
            run_id = row.get("analysis_run_id", "")
            try:
                competitive_min = float(row.get("competitive_min", "0") or 0.0)
                standard_min = float(row.get("standard_min", "0") or 0.0)
            except ValueError:
                continue
            thresholds_by_run[run_id] = (competitive_min, standard_min)
    for run_id in sorted(rows_by_run):
        run_rows = rows_by_run[run_id]
        presence_values = [
            float(r["dominant_presence_pct"])
            for r in run_rows
            if r.get("dominant_presence_pct", "")
        ]
        algorithm = "jenks_natural_breaks"
        if domain and run_id in thresholds_by_run:
            competitive_min, standard_min = thresholds_by_run[run_id]
            algorithm = "precomputed_thresholds"
        else:
            competitive_min, standard_min, algorithm = _compute_breaks(presence_values)
        for row in run_rows:
            try:
                pct = float(row.get("dominant_presence_pct", "0") or 0.0)
            except ValueError:
                pct = 0.0
            row["element_dominance_bucket"] = _bucket(pct, competitive_min, standard_min)

        value_min = min(presence_values) if presence_values else 0.0
        value_max = max(presence_values) if presence_values else 0.0
        if not domain:
            threshold_rows.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": run_id,
                "algorithm": algorithm,
                "n_classes": "3",
                "n_elements": str(len(run_rows)),
                "competitive_min": f"{competitive_min:.6f}",
                "standard_min": f"{standard_min:.6f}",
                "value_min": f"{value_min:.6f}",
                "value_max": f"{value_max:.6f}",
            })
        print(
            f"[emit_element_dominance] thresholds: analysis_run_id={run_id} "
            f"competitive_min={competitive_min:.4f} standard_min={standard_min:.4f} "
            f"n_elements={len(run_rows)} algorithm={algorithm}",
            flush=True,
        )

    if not threshold_rows and not domain:
        threshold_rows.append({
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": "",
            "algorithm": "fallback_standard_presence_min",
            "n_classes": "3",
            "n_elements": "0",
            "competitive_min": f"{(STANDARD_PRESENCE_MIN / 2.0):.6f}",
            "standard_min": f"{STANDARD_PRESENCE_MIN:.6f}",
            "value_min": "0.000000",
            "value_max": "0.000000",
        })
        print(
            "[emit_element_dominance] thresholds: analysis_run_id= "
            f"competitive_min={(STANDARD_PRESENCE_MIN / 2.0):.4f} "
            f"standard_min={STANDARD_PRESENCE_MIN:.4f} n_elements=0",
            flush=True,
        )
    threshold_fieldnames = [
        "schema_version",
        "analysis_run_id",
        "algorithm",
        "n_classes",
        "n_elements",
        "competitive_min",
        "standard_min",
        "value_min",
        "value_max",
    ]

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
        "lead_gap",
        "dominant_files_present",
        "files_total",
        "is_element_candidate_standard",
        "element_dominance_bucket",
        "is_cad_import",
        "confidence_tier",
    ]
    sorted_rows = sorted(
        output_rows,
        key=lambda r: (r["analysis_run_id"], r["domain"], r["element_label"], r["sub_label"]),
    )
    _write_csv_atomic(out_csv, fieldnames, sorted_rows)
    if domain:
        print(
            f"[emit_element_dominance] thresholds: skipped write for --domain={domain} "
            "(using existing run-scoped thresholds when available)",
            flush=True,
        )
    else:
        _write_csv_atomic(thresholds_csv, threshold_fieldnames, threshold_rows)

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
