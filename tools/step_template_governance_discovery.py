"""
step_template_governance_discovery.py

Reads phase0_records.csv and computes per-domain alignment of a corpus to a
designated template file. Emits:
  - template_governance_readiness.csv  (one row per domain, sorted by alignment_rate desc)
  - template_comparison_profile.json   (stable-domain comparison profile)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Jaccard helpers
# ---------------------------------------------------------------------------

def jaccard_multiset(ca: Counter, cb: Counter) -> Optional[float]:
    keys = set(ca.keys()) | set(cb.keys())
    if not keys:
        return None
    matched = sum(min(ca.get(k, 0), cb.get(k, 0)) for k in keys)
    union_mass = sum(max(ca.get(k, 0), cb.get(k, 0)) for k in keys)
    return matched / union_mass if union_mass > 0 else None


# ---------------------------------------------------------------------------
# Governance classification
# ---------------------------------------------------------------------------

def classify(alignment_rate: float, files_comparable: int, threshold: float = 0.75) -> str:
    if files_comparable < 3:
        return "insufficient_evidence"
    if alignment_rate >= threshold:
        return "stable"
    if alignment_rate >= (1 - threshold):
        return "emerging"
    return "fragmented"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def run(
    *,
    phase0_dir: str,
    template_id: str,
    threshold: float,
    out_dir: str,
) -> None:
    phase0_path = Path(phase0_dir) / "phase0_records.csv"
    if not phase0_path.exists():
        print(f"[ERROR] phase0_records.csv not found at: {phase0_path}", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 1 — single-pass load
    # ------------------------------------------------------------------
    template_sigs: Dict[str, Counter] = {}   # {domain: Counter(sig_hashes)}
    corpus_sigs: Dict[str, Dict[str, Counter]] = {}  # {file_key: {domain: Counter}}
    all_file_keys: List[str] = []
    total_rows = 0
    template_found = False

    with phase0_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            total_rows += 1

            export_run_id = (row.get("export_run_id") or "").strip()
            file_id = (row.get("file_id") or "").strip()
            file_key = export_run_id or file_id
            if not file_key:
                continue

            status = (row.get("status") or "").strip()
            sig_hash = (row.get("sig_hash") or "").strip()
            domain = (row.get("domain") or "").strip()

            if status not in ("ok", "degraded") or not sig_hash or not domain:
                continue

            is_template = template_id in (export_run_id, file_id) and template_id != ""
            if is_template:
                template_found = True
                template_sigs.setdefault(domain, Counter())[sig_hash] += 1
            else:
                if file_key not in corpus_sigs:
                    corpus_sigs[file_key] = {}
                    all_file_keys.append(file_key)
                corpus_sigs[file_key].setdefault(domain, Counter())[sig_hash] += 1

    print(f"[INFO] Loaded phase0_records.csv: {total_rows:,} rows")

    if not template_found:
        # Collect sample of available keys for the error message
        sample_keys = sorted(
            {k for k in all_file_keys[:50]}
        )[:10]
        print(
            f"[ERROR] Template file_id/export_run_id '{template_id}' not found in phase0_records.csv.\n"
            f"        Available file keys (first 10): {sample_keys}",
            file=sys.stderr,
        )
        sys.exit(1)

    template_domain_count = len(template_sigs)
    template_record_count = sum(sum(c.values()) for c in template_sigs.values())
    corpus_file_count = len(corpus_sigs)

    print(f"[INFO] Template '{template_id}': {template_domain_count} domains, {template_record_count:,} total records")
    print(f"[INFO] Corpus: {corpus_file_count} files (template excluded)")
    print("[INFO] Computing per-domain alignment...")

    # ------------------------------------------------------------------
    # Step 2 — per-file, per-domain multiset Jaccard
    # ------------------------------------------------------------------
    # {domain: [(file_key, score, union_mass), ...]}
    domain_scores: Dict[str, List[Tuple[str, float, int]]] = {}

    for domain, tmpl_counter in template_sigs.items():
        if not tmpl_counter:
            # Template has no usable records for this domain — skip
            continue
        entries: List[Tuple[str, float, int]] = []
        for file_key, file_domains in corpus_sigs.items():
            corp_counter = file_domains.get(domain, Counter())
            score = jaccard_multiset(tmpl_counter, corp_counter)
            if score is None:
                continue
            keys = set(tmpl_counter.keys()) | set(corp_counter.keys())
            union_mass = sum(max(tmpl_counter.get(k, 0), corp_counter.get(k, 0)) for k in keys)
            entries.append((file_key, score, union_mass))
        domain_scores[domain] = entries

    # ------------------------------------------------------------------
    # Step 3 — per-domain aggregation
    # ------------------------------------------------------------------
    rows_out: List[dict] = []

    for domain, entries in domain_scores.items():
        files_comparable = len(entries)
        scores = [s for _, s, _ in entries]
        union_masses = [u for _, _, u in entries]

        if files_comparable == 0:
            alignment_rate = 0.0
            median_sim: Optional[float] = None
            p25: Optional[float] = None
            p75: Optional[float] = None
            mean_um: Optional[float] = None
        else:
            aligned = sum(1 for s in scores if s >= threshold)
            alignment_rate = aligned / files_comparable

            median_sim = statistics.median(scores)
            if files_comparable >= 4:
                qs = statistics.quantiles(scores, n=4)
                p25 = qs[0]
                p75 = qs[2]
            else:
                p25 = None
                p75 = None
            mean_um = sum(union_masses) / len(union_masses)

        state = classify(alignment_rate, files_comparable, threshold)
        tmpl_rec_count = sum(template_sigs[domain].values())

        rows_out.append({
            "domain": domain,
            "governance_state": state,
            "alignment_rate": alignment_rate,
            "files_comparable": files_comparable,
            "median_similarity": median_sim,
            "p25_similarity": p25,
            "p75_similarity": p75,
            "mean_union_mass": mean_um,
            "template_record_count": tmpl_rec_count,
            "threshold_used": threshold,
            "template_id": template_id,
        })

    # Sort by alignment_rate descending
    rows_out.sort(key=lambda r: r["alignment_rate"], reverse=True)

    # ------------------------------------------------------------------
    # Step 4 — count governance states
    # ------------------------------------------------------------------
    state_counts: Counter = Counter(r["governance_state"] for r in rows_out)
    n_stable = state_counts["stable"]
    n_emerging = state_counts["emerging"]
    n_fragmented = state_counts["fragmented"]
    n_insufficient = state_counts["insufficient_evidence"]

    print(f"[INFO] Domains evaluated: {len(rows_out)}")
    print(
        f"[INFO] Stable: {n_stable} | Emerging: {n_emerging} | "
        f"Fragmented: {n_fragmented} | Insufficient evidence: {n_insufficient}"
    )

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 5 — emit template_governance_readiness.csv
    # ------------------------------------------------------------------
    csv_fieldnames = [
        "domain",
        "governance_state",
        "alignment_rate",
        "files_comparable",
        "median_similarity",
        "p25_similarity",
        "p75_similarity",
        "mean_union_mass",
        "template_record_count",
        "threshold_used",
        "template_id",
    ]

    float_fields = {
        "alignment_rate",
        "median_similarity",
        "p25_similarity",
        "p75_similarity",
        "mean_union_mass",
        "threshold_used",
    }

    csv_out_path = out_path / "template_governance_readiness.csv"
    with csv_out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_fieldnames)
        writer.writeheader()
        for row in rows_out:
            csv_row: dict = {}
            for field in csv_fieldnames:
                val = row[field]
                if val is None:
                    csv_row[field] = ""
                elif field in float_fields:
                    csv_row[field] = f"{val:.6f}"
                else:
                    csv_row[field] = str(val)
            writer.writerow(csv_row)

    print(f"[INFO] Written: {csv_out_path.resolve()}")

    # ------------------------------------------------------------------
    # Step 6 — emit template_comparison_profile.json
    # ------------------------------------------------------------------
    stable_domains = sorted(r["domain"] for r in rows_out if r["governance_state"] == "stable")
    emerging_domains = sorted(r["domain"] for r in rows_out if r["governance_state"] == "emerging")

    profile = {
        "profile_id": "template_governance_discovered_v1",
        "label": "Template Governance \u2014 Discovered (stable domains only)",
        "template_id": template_id,
        "threshold_used": threshold,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "corpus_files_evaluated": corpus_file_count,
        "domains_stable": n_stable,
        "domains_emerging": n_emerging,
        "domains_fragmented": n_fragmented,
        "domains_insufficient_evidence": n_insufficient,
        "domains_in_scope": stable_domains,
        "domains_emerging_candidates": emerging_domains,
        "notes": (
            "domains_in_scope contains stable domains only. "
            "domains_emerging_candidates listed for reference but excluded from scope."
        ),
    }

    json_out_path = out_path / "template_comparison_profile.json"
    with json_out_path.open("w", encoding="utf-8") as fh:
        json.dump(profile, fh, indent=2)
        fh.write("\n")

    print(f"[INFO] Written: {json_out_path.resolve()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compute per-domain template governance readiness from phase0_records.csv."
    )
    p.add_argument(
        "--phase0-dir",
        required=True,
        dest="phase0_dir",
        help="Directory containing phase0_records.csv",
    )
    p.add_argument(
        "--template-id",
        required=True,
        dest="template_id",
        help="file_id or export_run_id of the template file in phase0_records.csv",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.75,
        dest="threshold",
        help="Similarity threshold for alignment classification (default: 0.75)",
    )
    p.add_argument(
        "--out",
        default=None,
        dest="out_dir",
        help="Output directory (default: <phase0-dir>/template_governance)",
    )
    return p.parse_args()


def main() -> None:
    ns = _parse_args()
    out_dir = ns.out_dir or os.path.join(ns.phase0_dir, "template_governance")
    run(
        phase0_dir=ns.phase0_dir,
        template_id=ns.template_id,
        threshold=ns.threshold,
        out_dir=out_dir,
    )


if __name__ == "__main__":
    main()
