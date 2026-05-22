"""
tools/label_synthesis/patch_all_domain_patterns.py

Recursively patches pattern_label_human in domain_patterns.csv across:
  - the corpus results root
  - every segment subfolder under segments root

Reads the shared llm_name_cache.json once (from corpus label_synthesis dir)
and applies it to all domain_patterns.csv files found.

Path conventions (mirrors run_extract_all / run_segment_orchestrator):
  Corpus:
    {results_root}/analysis/domain_patterns.csv
    {results_root}/label_synthesis/llm_name_cache.json
    {results_root}/label_synthesis/{domain}.joinhash_label_population.csv

  Segments:
    {segments_root}/{segment_name}/results/analysis/domain_patterns.csv
    {segments_root}/{segment_name}/results/label_synthesis/{domain}.joinhash_label_population.csv
    (cache is always the corpus-level one)

Usage:
    python tools\\label_synthesis\\patch_all_domain_patterns.py ^
        --results-root  "path\\to\\exports\\results" ^
        --segments-root "path\\to\\exports\\segments" ^
        [--cache        "path\\to\\llm_name_cache.json"]  # defaults to results-root/label_synthesis/
        [--dry-run]
        [--force]

Options:
    --dry-run   Print all changes without writing any files
    --force     Update rows of any source, not just fallback/llm_unreviewed
    --cache     Override cache path (default: {results-root}/label_synthesis/llm_name_cache.json)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Thresholds (mirror label_resolver.py)
# ---------------------------------------------------------------------------

MODAL_THRESHOLD = 0.60
MAX_LABELS_FOR_MODAL = 3
MODAL_MAX_NORM_ENTROPY = 0.5

# Sources that are considered authoritative — never overwritten without --force
AUTHORITATIVE_SOURCES = {"curator", "synopsis", "near_dup", "modal"}


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Cache + label population loaders
# ---------------------------------------------------------------------------

def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.is_file():
        print(f"  WARN: cache not found at {cache_path}")
        return {}
    with cache_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_label_population(label_synth_dir: Path, domain: str) -> Dict[str, List[Dict[str, Any]]]:
    candidates = [
        label_synth_dir / f"{domain}.joinhash_label_population.csv",
        label_synth_dir.parent / "analysis" / f"{domain}.joinhash_label_population.csv",
    ]
    pop_path = next((p for p in candidates if p.is_file()), None)
    if pop_path is None:
        return {}
    result: Dict[str, List[Dict[str, Any]]] = {}
    with pop_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("domain", "").strip() != domain:
                continue
            jh = row.get("join_hash", "").strip()
            if not jh:
                continue
            result.setdefault(jh, []).append({
                "label_q": row.get("label_q", "ok"),
                "label_v": row.get("label_v", ""),
                "files_count": int(row.get("files_count", 0) or 0),
            })
    return result


# ---------------------------------------------------------------------------
# Modal check (mirrors label_resolver.py Layer 3)
# ---------------------------------------------------------------------------

def _try_modal(label_pop: List[Dict[str, Any]]) -> Optional[str]:
    ok_rows = [
        r for r in label_pop
        if r.get("label_q", "ok") == "ok" and r.get("label_v", "").strip()
    ]
    if not ok_rows:
        return None
    ok_rows = sorted(ok_rows, key=lambda r: -int(r.get("files_count", 0)))
    total = sum(int(r.get("files_count", 0)) for r in ok_rows)
    if total == 0:
        return None
    modal_share = int(ok_rows[0].get("files_count", 0)) / total
    distinct = len(ok_rows)
    shares = [int(r.get("files_count", 0)) / total for r in ok_rows]
    entropy = -sum(s * math.log2(s) for s in shares if s > 0)
    max_entropy = math.log2(distinct) if distinct > 1 else 0.0
    norm_entropy = (entropy / max_entropy) if max_entropy > 0 else 0.0
    if (modal_share >= MODAL_THRESHOLD
            and distinct <= MAX_LABELS_FOR_MODAL
            and norm_entropy <= MODAL_MAX_NORM_ENTROPY):
        return ok_rows[0]["label_v"].strip()
    return None


# ---------------------------------------------------------------------------
# Single-file patcher
# ---------------------------------------------------------------------------

def _patch_one(
    domain_patterns_csv: Path,
    cache: Dict[str, Any],
    label_synth_dir: Path,
    dry_run: bool,
    force: bool,
    label: str,
) -> Tuple[int, int, int]:
    """
    Patch one domain_patterns.csv.
    Returns (n_updated, n_skipped_source, n_skipped_no_cache).
    """
    if not domain_patterns_csv.is_file():
        print(f"  SKIP (not found): {domain_patterns_csv}")
        return 0, 0, 0

    fieldnames, rows = _read_csv(domain_patterns_csv)

    for col in ("pattern_label_human", "pattern_label_source", "pattern_label_fallback"):
        if col not in fieldnames:
            fieldnames.append(col)
            for row in rows:
                row.setdefault(col, "")

    pop_by_domain: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    n_updated = n_skipped_source = n_skipped_no_cache = 0
    changes: List[Tuple[str, str, str, str, str, str]] = []  # domain,jh,old_lbl,old_src,new_lbl,new_src

    for row in rows:
        domain = row.get("domain", "").strip()
        join_hash = row.get("join_hash", "").strip()
        if not join_hash:
            src_cluster = row.get("source_cluster_id", "").strip()
            if src_cluster:
                join_hash = src_cluster.split("|")[-1]
        if not join_hash:
            continue

        current_source = row.get("pattern_label_source", "").strip()
        current_label = row.get("pattern_label_human", "").strip()

        if not force:
            if current_source in AUTHORITATIVE_SOURCES:
                n_skipped_source += 1
                continue
            if current_source == "llm":
                # Already reviewed LLM label — don't touch
                n_skipped_source += 1
                continue
            if current_source not in ("fallback", "llm_unreviewed", ""):
                n_skipped_source += 1
                continue

        entry = cache.get(join_hash)
        if not entry:
            n_skipped_no_cache += 1
            continue

        recommended = (entry.get("recommended") or "").strip()
        if not recommended:
            n_skipped_no_cache += 1
            continue

        reviewed = entry.get("reviewed", False)
        new_source = "llm" if reviewed else "llm_unreviewed"
        new_label = recommended

        # Re-check modal (Layer 3 beats LLM Layer 4)
        if domain not in pop_by_domain:
            pop_by_domain[domain] = _load_label_population(label_synth_dir, domain)
        label_pop = pop_by_domain[domain].get(join_hash, [])
        modal = _try_modal(label_pop)
        if modal and current_source == "fallback":
            new_label = modal
            new_source = "modal"

        if new_label == current_label and new_source == current_source:
            n_skipped_source += 1
            continue

        changes.append((domain, join_hash[:16] + "...", current_label, current_source, new_label, new_source))
        if not dry_run:
            row["pattern_label_human"] = new_label
            row["pattern_label_source"] = new_source
        n_updated += 1

    if changes:
        print(f"\n  {label}  ({n_updated} updates)")
        by_domain: Dict[str, list] = {}
        for c in changes:
            by_domain.setdefault(c[0], []).append(c)
        for dom in sorted(by_domain):
            print(f"    [{dom}]")
            for c in by_domain[dom]:
                print(f"      {c[1]}")
                print(f"        {c[3]:20s} {c[2]!r}")
                print(f"      → {c[5]:20s} {c[4]!r}")
    else:
        print(f"  {label}  (nothing to update)")

    if not dry_run and n_updated > 0:
        bak = domain_patterns_csv.with_suffix(".csv.bak")
        shutil.copy2(domain_patterns_csv, bak)
        _write_csv(domain_patterns_csv, fieldnames, rows)

    return n_updated, n_skipped_source, n_skipped_no_cache


# ---------------------------------------------------------------------------
# Discovery: find all domain_patterns.csv targets
# ---------------------------------------------------------------------------

def _find_targets(
    results_root: Path,
    segments_root: Optional[Path],
) -> List[Tuple[Path, Path, str]]:
    """
    Return list of (domain_patterns_csv, label_synth_dir, label) tuples.
    label is a short human-readable identifier for logging.
    """
    targets: List[Tuple[Path, Path, str]] = []

    # Corpus
    corpus_dp = results_root / "analysis" / "domain_patterns.csv"
    corpus_ls = results_root / "label_synthesis"
    targets.append((corpus_dp, corpus_ls, "corpus"))

    # Segments
    if segments_root and segments_root.is_dir():
        for seg_dir in sorted(segments_root.iterdir()):
            if not seg_dir.is_dir():
                continue
            seg_dp = seg_dir / "results" / "analysis" / "domain_patterns.csv"
            seg_ls = seg_dir / "results" / "label_synthesis"
            if seg_dp.is_file():
                targets.append((seg_dp, seg_ls, f"segment/{seg_dir.name}"))
            else:
                # Also check without the extra "results" level
                alt_dp = seg_dir / "analysis" / "domain_patterns.csv"
                alt_ls = seg_dir / "label_synthesis"
                if alt_dp.is_file():
                    targets.append((alt_dp, alt_ls, f"segment/{seg_dir.name}"))

    return targets


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Recursively patch pattern_label_human in all domain_patterns.csv files "
            "across corpus results and segment folders."
        )
    )
    ap.add_argument(
        "--results-root",
        required=True,
        metavar="PATH",
        help="Corpus results root (contains analysis/ and label_synthesis/ subdirs)",
    )
    ap.add_argument(
        "--segments-root",
        default=None,
        metavar="PATH",
        help="Segments root directory (contains one subfolder per segment). "
             "Omit to patch corpus only.",
    )
    ap.add_argument(
        "--cache",
        default=None,
        metavar="PATH",
        help="Path to llm_name_cache.json. "
             "Defaults to {results-root}/label_synthesis/llm_name_cache.json",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print all changes without writing any files",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Update rows of any label source, not just fallback/llm_unreviewed",
    )
    args = ap.parse_args()

    results_root = Path(args.results_root).resolve()
    segments_root = Path(args.segments_root).resolve() if args.segments_root else None

    cache_path = (
        Path(args.cache).resolve()
        if args.cache
        else results_root / "label_synthesis" / "llm_name_cache.json"
    )

    print(f"[patch_all] results_root:  {results_root}")
    print(f"[patch_all] segments_root: {segments_root or '(none)'}")
    print(f"[patch_all] cache:         {cache_path}")
    print(f"[patch_all] dry_run={args.dry_run}  force={args.force}")

    cache = _load_cache(cache_path)
    if not cache:
        print("[patch_all] Cache is empty or missing — nothing to do.")
        sys.exit(0)
    print(f"[patch_all] Cache entries: {len(cache)}")

    targets = _find_targets(results_root, segments_root)
    print(f"[patch_all] Targets found: {len(targets)}\n")

    total_updated = 0
    total_skipped_source = 0
    total_skipped_no_cache = 0

    for dp_csv, ls_dir, label in targets:
        u, ss, sn = _patch_one(
            domain_patterns_csv=dp_csv,
            cache=cache,
            label_synth_dir=ls_dir,
            dry_run=args.dry_run,
            force=args.force,
            label=label,
        )
        total_updated += u
        total_skipped_source += ss
        total_skipped_no_cache += sn

    print(f"\n[patch_all] ── Summary ──────────────────────────────")
    print(f"  Targets processed:    {len(targets)}")
    print(f"  Total updated:        {total_updated}")
    print(f"  Skipped (auth src):   {total_skipped_source}")
    print(f"  Skipped (no cache):   {total_skipped_no_cache}")
    if args.dry_run:
        print(f"\n  DRY RUN — no files written.")
    else:
        print(f"\n  Done. Backups written as *.csv.bak alongside each patched file.")


if __name__ == "__main__":
    main()
