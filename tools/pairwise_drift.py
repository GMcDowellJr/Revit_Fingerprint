# -*- coding: utf-8 -*-
"""
Pairwise drift scoring for a folder of fingerprint payloads (Mode B).

- Computes unordered pairs only (A vs B, once).
- Writes:
    - drift_pairs/<A>__VS__<B>.drift.json
    - drift_pairs/pairwise_drift.csv

Run folder resolution:
- CLI arg:   --runs <folder>
- Env var:   REVIT_FINGERPRINT_RUNS_DIR
- Default:   current working directory

This is intentionally simple and explicit.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from subprocess import run, PIPE

def _as_dict(x):
    return x if isinstance(x, dict) else {}

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _summarize_drift(drift_obj):
    """
    Returns:
      summary: dict with meaning columns
      domain_scores: dict {domain_name: score_float}
    """
    d = _as_dict(drift_obj)
    by_dom = _as_dict(d.get("drift_by_domain", None))

    domain_scores = {}
    hash_mismatch = 0
    status_mismatch = 0
    count_delta = 0
    domains_with_drift = 0

    # Collect per-domain contributions
    for dom in sorted(by_dom.keys()):
        rec = _as_dict(by_dom.get(dom, None))
        score = _safe_float(rec.get("score", None))
        if score is None:
            continue
        domain_scores[dom] = score

        parts = _as_dict(rec.get("parts", None))
        p_hash = _safe_float(parts.get("hash", 0.0)) or 0.0
        p_status = _safe_float(parts.get("status", 0.0)) or 0.0
        p_count = _safe_float(parts.get("count", 0.0)) or 0.0

        if score > 0:
            domains_with_drift += 1
        if p_hash > 0:
            hash_mismatch += 1
        if p_status > 0:
            status_mismatch += 1
        if p_count > 0:
            count_delta += 1

    # Top contributors (deterministic tie-break by name)
    top = sorted(domain_scores.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
    top3 = "|".join(["{0}:{1:.2f}".format(k, v) for k, v in top])

    drift_total = _safe_float(d.get("drift_total", None))

    summary = {
        "drift_total": drift_total,
        "domains_with_drift": domains_with_drift,
        "hash_mismatch_count": hash_mismatch,
        "status_mismatch_count": status_mismatch,
        "count_delta_count": count_delta,
        "top3_domains": top3,
    }
    return summary, domain_scores

def _repo_root() -> str:
    # tools/pairwise_drift.py -> repo root is one level up
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(here, os.pardir))


def _resolve_runs_dir(cli_val: str | None) -> str:
    if cli_val:
        return cli_val
    env = os.environ.get("REVIT_FINGERPRINT_RUNS_DIR", "").strip()
    if env:
        return env
    return os.getcwd()


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--runs",
        help="Folder containing fingerprint payload JSON files "
             "(overrides REVIT_FINGERPRINT_RUNS_DIR)",
    )
    ap.add_argument(
        "--pattern",
        default="*__fingerprint.json",
        help="Glob pattern for inputs (default: *__fingerprint.json). "
             "Use *.index.json or *.details.json if you intentionally want legacy split inputs.",
    )
    ap.add_argument(
        "--baseline",
        default="",
        help="If provided, run baseline-vs-all instead of pairwise. "
             "Path to baseline payload/manifest/features JSON.",
    )
    args = ap.parse_args(argv)

    runs_dir = _resolve_runs_dir(args.runs)
    runs_dir = os.path.abspath(runs_dir)

    if not os.path.isdir(runs_dir):
        sys.stderr.write(f"ERROR: runs dir not found: {runs_dir}\n")
        return 2

    files = sorted(glob.glob(os.path.join(runs_dir, args.pattern)))
    if len(files) < 2:
        sys.stderr.write("ERROR: need at least two payload JSON files\n")
        return 2

    baseline_path = os.path.abspath(args.baseline) if args.baseline else ""
    if baseline_path:
        if not os.path.isfile(baseline_path):
            sys.stderr.write(f"ERROR: baseline file not found: {baseline_path}\n")
            return 2

    # --- Baseline vs all mode ---
    if baseline_path:
        out_dir = os.path.join(runs_dir, "drift_vs_standard")
        os.makedirs(out_dir, exist_ok=True)

        csv_path = os.path.join(out_dir, "drift_vs_standard.csv")

        rows = []
        repo_root = _repo_root()

        for f in files:
            f_abs = os.path.abspath(f)

            # Skip comparing baseline to itself if it's in the same folder/pattern set
            if os.path.abspath(f_abs) == os.path.abspath(baseline_path):
                continue

            t_base = os.path.splitext(os.path.basename(f_abs))[0]
            out_json = os.path.join(out_dir, f"{t_base}.drift.json")

            proc = run(
                [
                    sys.executable,
                    os.path.join(repo_root, "tools", "score_drift.py"),
                    os.path.abspath(baseline_path),
                    f_abs,
                    "--out",
                    os.path.abspath(out_json),
                ],
                cwd=repo_root,
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )

            if proc.returncode != 0:
                rows.append(
                    {
                        "file_a": os.path.splitext(os.path.basename(baseline_path))[0],
                        "file_b": t_base,
                        "drift_total": "",
                        "domains_with_drift": "",
                        "hash_mismatch_count": "",
                        "status_mismatch_count": "",
                        "count_delta_count": "",
                        "top3_domains": "",
                        "status": "error",
                        "error": proc.stderr.strip(),
                        "_domain_scores": {},
                    }
                )
                continue

            try:
                with open(out_json, "r", encoding="utf-8") as fh:
                    data = json.load(fh)

                summary, dom_scores = _summarize_drift(data)

                rows.append(
                    {
                        "file_a": os.path.splitext(os.path.basename(baseline_path))[0],
                        "file_b": t_base,
                        "drift_total": summary.get("drift_total", ""),
                        "domains_with_drift": summary.get("domains_with_drift", ""),
                        "hash_mismatch_count": summary.get("hash_mismatch_count", ""),
                        "status_mismatch_count": summary.get("status_mismatch_count", ""),
                        "count_delta_count": summary.get("count_delta_count", ""),
                        "top3_domains": summary.get("top3_domains", ""),
                        "status": "ok",
                        "error": "",
                        "_domain_scores": dom_scores,
                    }
                )
            except Exception as e:
                rows.append(
                    {
                        "file_a": os.path.splitext(os.path.basename(baseline_path))[0],
                        "file_b": t_base,
                        "drift_total": "",
                        "domains_with_drift": "",
                        "hash_mismatch_count": "",
                        "status_mismatch_count": "",
                        "count_delta_count": "",
                        "top3_domains": "",
                        "status": "error",
                        "error": str(e),
                        "_domain_scores": {},
                    }
                )

        # Build deterministic union of domains for wide columns
        all_domains = set()
        for r in rows:
            dom_scores = r.get("_domain_scores", None)
            if isinstance(dom_scores, dict):
                for dn in dom_scores.keys():
                    all_domains.add(str(dn))

        domain_cols = ["domain_{0}_score".format(dn) for dn in sorted(all_domains)]

        base_cols = [
            "file_a",
            "file_b",
            "drift_total",
            "domains_with_drift",
            "hash_mismatch_count",
            "status_mismatch_count",
            "count_delta_count",
            "top3_domains",
            "status",
            "error",
        ]

        fieldnames = base_cols + domain_cols

        with open(csv_path, "w", newline="", encoding="utf-8") as fcsv:
            writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                out = {k: r.get(k, "") for k in base_cols}
                dom_scores = r.get("_domain_scores", None)
                if isinstance(dom_scores, dict):
                    for dn in sorted(all_domains):
                        col = "domain_{0}_score".format(dn)
                        out[col] = dom_scores.get(dn, "")
                writer.writerow(out)

        print(f"Wrote {len(rows)} baseline-vs-all drift results")
        print(f"JSON: {out_dir}")
        print(f"CSV:  {csv_path}")
        return 0

    out_dir = os.path.join(runs_dir, "drift_pairs")
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "pairwise_drift.csv")

    rows = []
    n = len(files)

    for i in range(n):
        for j in range(i + 1, n):
            a = files[i]
            b = files[j]

            a_base = os.path.splitext(os.path.basename(a))[0]
            b_base = os.path.splitext(os.path.basename(b))[0]

            out_json = os.path.join(
                out_dir, f"{a_base}__VS__{b_base}.drift.json"
            )

            # Call existing scorer
            repo_root = _repo_root()

            a_abs = os.path.abspath(a)
            b_abs = os.path.abspath(b)
            out_json_abs = os.path.abspath(out_json)

            proc = run(
                [
                    sys.executable,
                    os.path.join(repo_root, "tools", "score_drift.py"),
                    a_abs,
                    b_abs,
                    "--out",
                    out_json_abs,
                ],
                cwd=repo_root,
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )

            if proc.returncode != 0:
                rows.append(
                    {
                        "file_a": a_base,
                        "file_b": b_base,
                        "drift_total": "",
                        "domains_with_drift": "",
                        "hash_mismatch_count": "",
                        "status_mismatch_count": "",
                        "count_delta_count": "",
                        "top3_domains": "",
                        "status": "error",
                        "error": proc.stderr.strip(),
                        "_domain_scores": {},
                    }
                )
                continue

            try:
                with open(out_json, "r", encoding="utf-8") as f:
                    data = json.load(f)

                summary, dom_scores = _summarize_drift(data)

                rows.append(
                    {
                        "file_a": a_base,
                        "file_b": b_base,
                        "drift_total": summary.get("drift_total", ""),
                        "domains_with_drift": summary.get("domains_with_drift", ""),
                        "hash_mismatch_count": summary.get("hash_mismatch_count", ""),
                        "status_mismatch_count": summary.get("status_mismatch_count", ""),
                        "count_delta_count": summary.get("count_delta_count", ""),
                        "top3_domains": summary.get("top3_domains", ""),
                        "status": "ok",
                        "error": "",
                        "_domain_scores": dom_scores,  # internal; not a CSV column directly
                    }
                )

            except Exception as e:
                rows.append(
                    {
                        "file_a": a_base,
                        "file_b": b_base,
                        "drift_total": "",
                        "domains_with_drift": "",
                        "hash_mismatch_count": "",
                        "status_mismatch_count": "",
                        "count_delta_count": "",
                        "top3_domains": "",
                        "status": "error",
                        "error": proc.stderr.strip(),
                        "_domain_scores": {},
                    }
                )

    # Write CSV summary (derived artifact)
    # Build deterministic union of domains for wide columns
    all_domains = set()
    for r in rows:
        dom_scores = r.get("_domain_scores", None)
        if isinstance(dom_scores, dict):
            for dn in dom_scores.keys():
                all_domains.add(str(dn))

    domain_cols = ["domain_{0}_score".format(dn) for dn in sorted(all_domains)]

    base_cols = [
        "file_a",
        "file_b",
        "drift_total",
        "domains_with_drift",
        "hash_mismatch_count",
        "status_mismatch_count",
        "count_delta_count",
        "top3_domains",
        "status",
        "error",
    ]

    fieldnames = base_cols + domain_cols

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            out = {}

            # Base
            for k in base_cols:
                out[k] = r.get(k, "")

            # Wide domain score columns
            dom_scores = r.get("_domain_scores", None)
            if isinstance(dom_scores, dict):
                for dn in sorted(all_domains):
                    col = "domain_{0}_score".format(dn)
                    v = dom_scores.get(dn, "")
                    out[col] = v

            writer.writerow(out)


    print(f"Wrote {len(rows)} pairwise drift results")
    print(f"JSON: {out_dir}")
    print(f"CSV:  {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
