"""
tools/label_synthesis/patch_domain_patterns_labels.py

Targeted label patcher: updates pattern_label_human and pattern_label_source
in an existing domain_patterns.csv using llm_name_cache.json, without
re-running the full patterns pipeline stage.

Only updates rows where the LLM cache produces a better label than what is
currently stored (i.e. current source is 'fallback' or 'llm_unreviewed' and
cache now has a reviewed entry, or current source is 'fallback' and cache
has any entry).

Writes the updated CSV back in-place (keeps a .bak backup).

Usage:
    python tools\\label_synthesis\\patch_domain_patterns_labels.py ^
        --domain-patterns  results\\analysis\\domain_patterns.csv ^
        --cache            results\\label_synthesis\\llm_name_cache.json ^
        --label-population results\\label_synthesis ^
        [--dry-run]        # print changes without writing
        [--force]          # update all rows where cache has an entry,
                           # regardless of current source

How it resolves labels (mirrors label_resolver.py layer order):
    1. Curator override (pattern_annotations.csv) -- skipped here, handled
       by full emit only
    2. Synopsis -- already emitted; not re-derived here
    3. Near-dup -- already emitted; not re-derived here
    4. Modal label -- re-checked from label_population CSVs
    5. LLM cache  -- checked here; this is the new step
    6. Fallback   -- kept if nothing else matches

Upgrade policy (default, without --force):
    - 'fallback' source  → upgrade to llm/llm_unreviewed if cache has entry
    - 'llm_unreviewed'   → upgrade to 'llm' if cache entry is now reviewed=true
    - all other sources  → never touched (synopsis, modal, curator, near_dup
                           are all considered authoritative)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers
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


def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.is_file():
        return {}
    with cache_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_label_population(label_synth_dir: Path, domain: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load joinhash_label_population.csv for a domain, keyed by join_hash."""
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
# Minimal modal check (mirrors label_resolver Layer 3)
# ---------------------------------------------------------------------------

import math

MODAL_THRESHOLD = 0.60
MAX_LABELS_FOR_MODAL = 3
MODAL_MAX_NORM_ENTROPY = 0.5


def _try_modal(label_population: List[Dict[str, Any]]) -> Optional[str]:
    ok_rows = [
        r for r in label_population
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
# Main patcher
# ---------------------------------------------------------------------------

def patch(
    domain_patterns_csv: Path,
    cache_path: Path,
    label_synth_dir: Path,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    print(f"[patch_labels] domain_patterns: {domain_patterns_csv}")
    print(f"[patch_labels] cache:           {cache_path}")
    print(f"[patch_labels] label_synth_dir: {label_synth_dir}")
    print(f"[patch_labels] dry_run={dry_run}  force={force}")

    fieldnames, rows = _read_csv(domain_patterns_csv)
    cache = _load_cache(cache_path)
    print(f"[patch_labels] cache entries: {len(cache)}")
    print(f"[patch_labels] domain_patterns rows: {len(rows)}")

    # Ensure columns exist
    for col in ("pattern_label_human", "pattern_label_source", "pattern_label_fallback"):
        if col not in fieldnames:
            fieldnames.append(col)
            for row in rows:
                row.setdefault(col, "")

    # Pre-load label populations per domain (lazy)
    pop_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    n_updated = 0
    n_skipped_source = 0
    n_skipped_no_cache = 0
    n_no_join_hash = 0

    changes: List[Dict[str, str]] = []

    for row in rows:
        domain = row.get("domain", "").strip()
        join_hash = row.get("join_hash", "").strip()

        # Extract join_hash from source_cluster_id if join_hash column absent/blank
        if not join_hash:
            src = row.get("source_cluster_id", "").strip()
            if src:
                join_hash = src.split("|")[-1]

        if not join_hash:
            n_no_join_hash += 1
            continue

        current_source = row.get("pattern_label_source", "").strip()
        current_label = row.get("pattern_label_human", "").strip()

        # Determine whether this row is eligible for upgrade
        if not force:
            # Never touch authoritative sources
            if current_source in ("curator", "synopsis", "near_dup", "modal"):
                n_skipped_source += 1
                continue
            # Only upgrade llm_unreviewed → llm (reviewed flag improved)
            # and fallback → llm/llm_unreviewed
            if current_source not in ("fallback", "llm_unreviewed", ""):
                n_skipped_source += 1
                continue

        # Check cache
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

        # Re-check modal first — if modal now resolves, prefer it over LLM
        # (modal is Layer 3, LLM is Layer 4)
        if domain not in pop_cache:
            pop_cache[domain] = _load_label_population(label_synth_dir, domain)
        label_pop = pop_cache[domain].get(join_hash, [])
        modal = _try_modal(label_pop)
        if modal and current_source not in ("llm", "llm_unreviewed"):
            # Modal wins — don't overwrite with LLM if modal resolves.
            # Treat blank source like fallback so legacy rows are patchable.
            if current_source in ("fallback", ""):
                new_label = modal
                new_source = "modal"
            else:
                n_skipped_source += 1
                continue
        else:
            new_label = recommended
            # Keep existing reviewed llm label if it's already better
            if current_source == "llm" and not force:
                n_skipped_source += 1
                continue

        if new_label == current_label and new_source == current_source:
            n_skipped_source += 1
            continue

        changes.append({
            "domain": domain,
            "pattern_id": row.get("pattern_id", ""),
            "join_hash": join_hash[:16] + "...",
            "old_label": current_label,
            "old_source": current_source,
            "new_label": new_label,
            "new_source": new_source,
        })

        if not dry_run:
            row["pattern_label_human"] = new_label
            row["pattern_label_source"] = new_source

        n_updated += 1

    # Summary
    print(f"\n[patch_labels] Results:")
    print(f"  Updated:              {n_updated}")
    print(f"  Skipped (source ok):  {n_skipped_source}")
    print(f"  Skipped (no cache):   {n_skipped_no_cache}")
    print(f"  Skipped (no hash):    {n_no_join_hash}")

    if changes:
        print(f"\n[patch_labels] Changes ({len(changes)} rows):")
        # Group by domain
        by_domain: Dict[str, List] = {}
        for c in changes:
            by_domain.setdefault(c["domain"], []).append(c)
        for domain in sorted(by_domain):
            print(f"\n  {domain} ({len(by_domain[domain])} updates):")
            for c in by_domain[domain]:
                print(f"    {c['join_hash']}")
                print(f"      {c['old_source']:20s} {c['old_label']!r}")
                print(f"    → {c['new_source']:20s} {c['new_label']!r}")

    if dry_run:
        print(f"\n[patch_labels] DRY RUN — no files written.")
        return

    if n_updated == 0:
        print(f"\n[patch_labels] Nothing to update.")
        return

    # Backup and write
    bak = domain_patterns_csv.with_suffix(".csv.bak")
    shutil.copy2(domain_patterns_csv, bak)
    print(f"\n[patch_labels] Backup: {bak}")

    _write_csv(domain_patterns_csv, fieldnames, rows)
    print(f"[patch_labels] Written: {domain_patterns_csv}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Patch pattern_label_human in domain_patterns.csv from llm_name_cache.json "
            "without re-running the full patterns pipeline stage."
        )
    )
    ap.add_argument(
        "--domain-patterns",
        required=True,
        metavar="PATH",
        help="Path to domain_patterns.csv (updated in-place)",
    )
    ap.add_argument(
        "--cache",
        required=True,
        metavar="PATH",
        help="Path to llm_name_cache.json",
    )
    ap.add_argument(
        "--label-population",
        required=True,
        metavar="DIR",
        help="Directory containing {domain}.joinhash_label_population.csv files",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print changes without writing",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Update all rows where cache has an entry, regardless of current source",
    )
    args = ap.parse_args()

    patch(
        domain_patterns_csv=Path(args.domain_patterns).resolve(),
        cache_path=Path(args.cache).resolve(),
        label_synth_dir=Path(args.label_population).resolve(),
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
