"""
tools/label_synthesis/synthesize_fragmented_labels.py

Offline batch script to pre-populate the LLM name cache for fragmented patterns.

This script:
  1. Loads joinhash_label_population.csv for the given domain
  2. Identifies join_hashes where modal label promotion fails (fragmented)
  3. Finds representative identity_items for each fragmented join_hash
  4. Calls Claude API to synthesize canonical name candidates
  5. Writes results to llm_name_cache.json (keyed by join_hash, stable across re-runs)

The LLM cache is then read by label_resolver.py during v21_emit.py runs.
Never called at emit time — this is a background enrichment step.

Usage:
    python -m tools.label_synthesis.synthesize_fragmented_labels \
        --exports-dir Results_v21/exports \
        --analysis-dir Results_v21/analysis_v21 \
        --domain dimension_types \
        --cache label_synthesis/llm_name_cache.json \
        [--dry-run]              # print prompts/responses without writing
        [--force-refresh]        # re-synthesize even if join_hash already in cache
        [--only-unreviewed]      # only synthesize entries missing "reviewed": true
        [--review-csv out.csv]   # emit pending-review CSV for curator workflow
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .label_resolver import (
    is_fragmented,
    load_label_population,
    load_llm_cache,
    save_llm_cache,
    MODAL_THRESHOLD,
)


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def _call_claude(
    system_prompt: str,
    user_prompt: str,
    model: str = "claude-sonnet-4-6",
    max_tokens: int = 512,
    retry_count: int = 2,
    retry_delay: float = 2.0,
) -> Optional[Dict[str, Any]]:
    """
    Call Claude API and parse JSON response.

    Returns parsed dict with keys: candidates, recommended, rationale
    Returns None on failure (caller logs and skips).
    """
    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic package not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic()

    for attempt in range(retry_count + 1):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            raw_text = response.content[0].text.strip()

            # Strip markdown fences if model emits them despite instructions
            if raw_text.startswith("```"):
                raw_text = raw_text.split("\n", 1)[-1]
                if raw_text.endswith("```"):
                    raw_text = raw_text[: raw_text.rfind("```")]

            result = json.loads(raw_text)
            # Validate expected keys
            if "recommended" not in result:
                raise ValueError("Missing 'recommended' key in response")
            return result

        except json.JSONDecodeError as e:
            print(f"  WARN: JSON parse failed (attempt {attempt+1}): {e}")
        except Exception as e:
            print(f"  WARN: API call failed (attempt {attempt+1}): {e}")

        if attempt < retry_count:
            time.sleep(retry_delay)

    return None


# ---------------------------------------------------------------------------
# Representative identity_items lookup
# ---------------------------------------------------------------------------

def _load_representative_identity_items(
    exports_dir: str,
    domain: str,
    join_hash: str,
) -> List[Dict[str, Any]]:
    """
    Find the first export JSON record with this join_hash and return its identity_items.

    Searches exports_dir for *_fingerprint_export*.json files.
    Returns empty list if not found.
    """
    for fname in sorted(os.listdir(exports_dir)):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(exports_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        records = _get_domain_records(data, domain)
        for rec in records:
            jk = rec.get("join_key") or {}
            jh = jk.get("join_hash", "") if isinstance(jk, dict) else ""
            if jh == join_hash:
                items = rec.get("identity_items") or rec.get("identity_basis", {}).get("items", [])
                if isinstance(items, list):
                    return items
    return []


def _get_domain_records(data: Any, domain: str) -> List[Dict[str, Any]]:
    """Extract records for a domain from export JSON."""
    if isinstance(data, dict):
        # Try common shapes
        for key in ("records", domain, f"{domain}_records"):
            val = data.get(key)
            if isinstance(val, list):
                return val
        # Nested: data[domain][records]
        dom_data = data.get(domain)
        if isinstance(dom_data, dict):
            recs = dom_data.get("records", [])
            if isinstance(recs, list):
                return recs
    return []


# ---------------------------------------------------------------------------
# Domain prompt loader
# ---------------------------------------------------------------------------

def _load_domain_prompt_module(domain: str):
    """Import domain prompt module. Returns None if not implemented."""
    try:
        import importlib
        return importlib.import_module(
            f"tools.label_synthesis.domain_prompts.{domain}"
        )
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Review CSV emitter
# ---------------------------------------------------------------------------

def _write_review_csv(review_path: str, cache: Dict[str, Any], domain: str) -> None:
    """Write pending-review CSV for curator workflow."""
    rows = []
    for join_hash, entry in sorted(cache.items()):
        if entry.get("domain", domain) != domain:
            continue
        rows.append({
            "domain": domain,
            "join_hash": join_hash,
            "recommended_name": entry.get("recommended", ""),
            "candidates": " | ".join(entry.get("candidates", [])),
            "rationale": entry.get("rationale", ""),
            "reviewed": entry.get("reviewed", False),
            "generated_at": entry.get("generated_at", ""),
        })

    if not rows:
        print(f"  No entries for domain '{domain}' in cache.")
        return

    os.makedirs(os.path.dirname(os.path.abspath(review_path)), exist_ok=True)
    with open(review_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "domain", "join_hash", "recommended_name", "candidates",
            "rationale", "reviewed", "generated_at",
        ])
        w.writeheader()
        w.writerows(rows)
    print(f"  Review CSV written: {review_path}  ({len(rows)} entries)")


# ---------------------------------------------------------------------------
# Main synthesis loop
# ---------------------------------------------------------------------------

def synthesize(
    *,
    exports_dir: str,
    analysis_dir: str,
    domain: str,
    cache_path: str,
    dry_run: bool = False,
    force_refresh: bool = False,
    only_unreviewed: bool = False,
    review_csv: Optional[str] = None,
) -> None:
    print(f"\n=== Label Synthesis: {domain} ===")
    print(f"  Exports dir:   {exports_dir}")
    print(f"  Analysis dir:  {analysis_dir}")
    print(f"  Cache path:    {cache_path}")
    print(f"  Dry run:       {dry_run}")

    # Load domain prompt module
    prompt_mod = _load_domain_prompt_module(domain)
    if prompt_mod is None:
        print(f"  WARN: No prompt module for domain '{domain}'. "
              f"Create tools/label_synthesis/domain_prompts/{domain}.py")
        print("  Falling back to generic prompt.")
        system_prompt = _generic_system_prompt(domain)
        build_prompt_fn = _generic_build_prompt
    else:
        system_prompt = prompt_mod.SYSTEM_PROMPT
        build_prompt_fn = prompt_mod.build_prompt

    # Load label population
    pop_csv = os.path.join(analysis_dir, f"{domain}.joinhash_label_population.csv")
    if not os.path.exists(pop_csv):
        # Try alternate location
        pop_csv = os.path.join(analysis_dir, "label_population", f"{domain}.joinhash_label_population.csv")
    if not os.path.exists(pop_csv):
        print(f"  ERROR: Label population CSV not found. Run run_joinhash_label_population first.")
        print(f"  Looked at: {pop_csv}")
        return

    label_pop_by_hash = load_label_population(pop_csv, domain)
    print(f"  Loaded {len(label_pop_by_hash)} join_hash entries from population CSV")

    # Identify fragmented hashes
    fragmented_hashes = [
        jh for jh, rows in label_pop_by_hash.items()
        if is_fragmented(rows)
    ]
    print(f"  Fragmented patterns: {len(fragmented_hashes)} / {len(label_pop_by_hash)}")

    if not fragmented_hashes:
        print("  Nothing to synthesize.")
        return

    # Load existing cache
    cache = load_llm_cache(cache_path)
    print(f"  Existing cache entries: {len(cache)}")

    # Determine which hashes need synthesis
    to_process = []
    for jh in fragmented_hashes:
        if not force_refresh and jh in cache:
            if only_unreviewed and not cache[jh].get("reviewed", False):
                to_process.append(jh)
            elif not only_unreviewed:
                pass  # already cached, skip
            continue
        to_process.append(jh)

    print(f"  To process: {len(to_process)}")
    if not to_process:
        print("  Cache is current. Use --force-refresh to re-synthesize.")
        if review_csv:
            _write_review_csv(review_csv, cache, domain)
        return

    # Process each fragmented hash
    success = 0
    failed = 0
    today = date.today().isoformat()

    for i, jh in enumerate(to_process, 1):
        rows = label_pop_by_hash.get(jh, [])
        # Sort by files_count desc
        rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))

        # Get representative identity_items
        identity_items = _load_representative_identity_items(exports_dir, domain, jh)

        # Build prompt
        user_prompt = build_prompt_fn(
            join_hash=jh,
            observed_labels=rows_sorted,
            identity_items=identity_items,
        )

        print(f"\n  [{i}/{len(to_process)}] join_hash={jh[:16]}...")
        print(f"    Labels: {[r['label_v'] for r in rows_sorted[:5]]}")

        if dry_run:
            print(f"    --- SYSTEM PROMPT (first 300 chars) ---")
            print(f"    {system_prompt[:300]}...")
            print(f"    --- USER PROMPT ---")
            print(f"    {user_prompt[:500]}...")
            print(f"    [DRY RUN — skipping API call]")
            continue

        result = _call_claude(system_prompt=system_prompt, user_prompt=user_prompt)

        if result is None:
            print(f"    FAILED — skipping")
            failed += 1
            continue

        recommended = result.get("recommended", "")
        candidates = result.get("candidates", [])
        rationale = result.get("rationale", "")
        print(f"    Recommended: {recommended!r}")
        print(f"    Candidates:  {candidates}")

        cache[jh] = {
            "domain": domain,
            "recommended": recommended,
            "candidates": candidates,
            "rationale": rationale,
            "reviewed": False,
            "generated_at": today,
            "observed_labels": [r.get("label_v", "") for r in rows_sorted[:10]],
        }
        success += 1

        # Save after each entry (safe against interruption)
        save_llm_cache(cache_path, cache)

    print(f"\n  Done. Success: {success}  Failed: {failed}  Skipped: {len(to_process) - success - failed}")

    if review_csv and not dry_run:
        _write_review_csv(review_csv, cache, domain)


# ---------------------------------------------------------------------------
# Generic fallbacks for domains without a prompt module
# ---------------------------------------------------------------------------

def _generic_system_prompt(domain: str) -> str:
    return (
        f"You are a Revit standards specialist naming {domain.replace('_', ' ')} "
        f"configuration patterns for a standards analytics dashboard at an engineering firm. "
        f"Produce concise canonical names under 40 characters that standards managers will recognize."
    )


def _generic_build_prompt(
    join_hash: str,
    observed_labels: List[Dict[str, Any]],
    identity_items: List[Dict[str, Any]],
    corpus_context: Optional[Dict[str, Any]] = None,
) -> str:
    lines = ["OBSERVED NAMES:"]
    for r in observed_labels[:8]:
        lines.append(f'  "{r.get("label_v", "")}" ({r.get("files_count", 0)} files)')
    lines.append("\nPARAMETERS:")
    for item in identity_items:
        if item.get("q") == "ok":
            lines.append(f"  {item.get('k')}: {item.get('v')}")
    lines.append(
        '\nRespond with ONLY JSON: {"candidates": [...], "recommended": "...", "rationale": "..."}'
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Pre-populate LLM name cache for fragmented dimension type patterns."
    )
    ap.add_argument("--exports-dir", required=True,
                    help="Directory containing fingerprint export JSON files")
    ap.add_argument("--analysis-dir", required=True,
                    help="Directory containing joinhash_label_population.csv")
    ap.add_argument("--domain", required=True,
                    help="Domain to synthesize labels for (e.g. dimension_types)")
    ap.add_argument("--cache", required=True, dest="cache_path",
                    help="Path to llm_name_cache.json (created/updated by this script)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print prompts without calling API or writing cache")
    ap.add_argument("--force-refresh", action="store_true",
                    help="Re-synthesize even if join_hash already in cache")
    ap.add_argument("--only-unreviewed", action="store_true",
                    help="Only process cache entries where reviewed=false")
    ap.add_argument("--review-csv", default=None,
                    help="Path to write pending-review CSV for curator workflow")
    args = ap.parse_args()

    synthesize(
        exports_dir=args.exports_dir,
        analysis_dir=args.analysis_dir,
        domain=args.domain,
        cache_path=args.cache_path,
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        only_unreviewed=args.only_unreviewed,
        review_csv=args.review_csv,
    )


if __name__ == "__main__":
    main()
