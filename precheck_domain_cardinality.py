#!/usr/bin/env python3
"""Throwaway pre-check for the emergent-analysis PR plan (E.1).

Answers one question against a real corpus, with no repo changes and no
re-extraction: for each domain, how many DISTINCT domain-configuration
states are there across sources, relative to the number of sources?

The PR 2 design's P-STATE projection is only useful where
n_distinct_states << n_sources. If most domains come back near-unique,
P-CONFORM must carry the result and the metric emphasis changes.

Reads ONLY <segment>/results/analysis/pattern_presence_file.csv.
Writes ONE small CSV. Not part of any PR; do not commit.

Usage:
  python precheck_domain_cardinality.py \
      --presence "<segment>/results/analysis/pattern_presence_file.csv" \
      --out      "./domain_cardinality_precheck.csv"
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import Counter, defaultdict
from pathlib import Path

try:
    csv.field_size_limit(2**31 - 1)
except OverflowError:
    csv.field_size_limit(2**31 - 1 if sys.maxsize > 2**31 else 2**30)

FIELDS = [
    "domain", "n_sources_observed", "n_distinct_states", "state_ratio",
    "modal_state_share", "modal_tie", "n_distinct_patterns",
    "mean_patterns_per_source", "unknown_share_mean", "projection_verdict",
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--presence", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    # (domain, source) -> set of pattern_id ; plus unknown mass per (domain, source)
    configs: dict[tuple[str, str], set[str]] = defaultdict(set)
    unknown: dict[tuple[str, str], float] = defaultdict(float)
    run_ids: set[str] = set()

    with args.presence.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            run_ids.add((row.get("analysis_run_id") or "").strip())
            dom = (row.get("domain") or "").strip()
            src = (row.get("export_run_id") or "").strip()
            pid = (row.get("pattern_id") or "").strip()
            if not dom or not src:
                continue
            key = (dom, src)
            if pid:
                configs[key].add(pid)
            else:
                try:
                    unknown[key] += float(row.get("pattern_share_pct") or 0.0)
                except ValueError:
                    pass
            configs.setdefault(key, set())

    if len(run_ids) > 1:
        sys.stderr.write(f"WARNING: {len(run_ids)} analysis_run_id values present; results mix runs: {sorted(run_ids)}\n")

    by_domain: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)
    for (dom, src), pids in configs.items():
        by_domain[dom].append((src, pids))

    rows = []
    for dom in sorted(by_domain):
        entries = by_domain[dom]
        n_sources = len(entries)
        states = Counter()
        all_pids: set[str] = set()
        pat_counts = []
        for src, pids in entries:
            token = "|".join(sorted(pids))
            states[hashlib.md5(token.encode("utf-8")).hexdigest()] += 1
            all_pids |= pids
            pat_counts.append(len(pids))
        n_states = len(states)
        ranked = states.most_common()
        top = ranked[0][1] if ranked else 0
        tie = sum(1 for _, c in ranked if c == top) > 1
        unk = [unknown.get((dom, s), 0.0) for s, _ in entries]

        ratio = n_states / n_sources if n_sources else 0.0
        if n_states <= 1:
            verdict = "uniform_no_variance"
        elif ratio >= 0.9:
            verdict = "near_unique_P_STATE_suppressed"
        elif ratio <= 0.5:
            verdict = "P_STATE_usable"
        else:
            verdict = "P_STATE_marginal"

        rows.append({
            "domain": dom,
            "n_sources_observed": n_sources,
            "n_distinct_states": n_states,
            "state_ratio": f"{ratio:.4f}",
            "modal_state_share": f"{(top / n_sources) if n_sources else 0.0:.4f}",
            "modal_tie": "true" if tie else "false",
            "n_distinct_patterns": len(all_pids),
            "mean_patterns_per_source": f"{(sum(pat_counts)/len(pat_counts)) if pat_counts else 0.0:.2f}",
            "unknown_share_mean": f"{(sum(unk)/len(unk)) if unk else 0.0:.4f}",
            "projection_verdict": verdict,
        })

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    verdicts = Counter(r["projection_verdict"] for r in rows)
    sys.stderr.write(
        f"[precheck] domains={len(rows)} sources_max={max((r['n_sources_observed'] for r in rows), default=0)}\n"
        f"[precheck] verdicts: {dict(verdicts)}\n"
        f"[precheck] wrote {args.out}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
