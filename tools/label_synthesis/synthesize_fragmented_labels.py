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
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


from .label_resolver import (
    is_fragmented,
    load_label_population,
    load_llm_cache,
    save_llm_cache,
    MODAL_THRESHOLD,
)


def _load_governance_join_hashes(
    *,
    domain: str,
    filter_mode: str,
    analysis_dir: str,
    domain_patterns_csv: Optional[str] = None,
    bundle_dir: Optional[str] = None,
) -> Optional[set]:
    """
    Return the set of join_hashes eligible for synthesis under filter_mode.
    Returns None when filter_mode == 'all' (no filtering).

    Join surface:
      domain_patterns.csv  →  source_cluster_id column is pipe-delimited:
                               {domain}|{join_key_schema}|{join_hash}
                               join_hash = split('|')[-1]

      bundle_membership.csv  →  (domain, pattern_id) rows;
                                 pattern_id joins to domain_patterns.pattern_id
                                 to recover join_hash
    """
    if filter_mode == "all":
        return None

    analysis_path = Path(analysis_dir)
    candidate_paths = []
    if domain_patterns_csv:
        candidate_paths.append(Path(domain_patterns_csv))
    else:
        candidate_paths.extend([
            analysis_path / "domain_patterns.csv",
            analysis_path.parent / "analysis_v21" / "domain_patterns.csv",
        ])
    dp_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])
    if not dp_path.exists():
        searched_paths = ", ".join(str(p) for p in candidate_paths)
        raise FileNotFoundError(
            f"domain_patterns.csv is required for --filter-mode {filter_mode!r} "
            f"but was not found. Looked at: {searched_paths}"
        )

    candidate_jhs: set = set()
    jh_to_pid: dict = {}
    pid_to_jh: dict = {}

    with dp_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("domain") != domain:
                continue
            pid = row.get("pattern_id", "").strip()
            raw_src = row.get("source_cluster_id", "").strip()
            jh = raw_src.split("|")[-1] if raw_src else ""
            is_cand = row.get("is_candidate_standard", "").strip().lower()
            if pid and jh:
                jh_to_pid[jh] = pid
                pid_to_jh[pid] = jh
            if is_cand == "true" and jh:
                candidate_jhs.add(jh)

    bundle_jhs: set = set()
    if filter_mode in ("bundles", "governance") and bundle_dir is None:
        raise ValueError(
            f"--bundle-dir is required when --filter-mode is {filter_mode!r}"
        )
    if filter_mode in ("bundles", "governance") and bundle_dir is not None:
        bm_path = Path(bundle_dir) / "bundle_membership.csv"
        if bm_path.exists():
            with bm_path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("domain") != domain:
                        continue
                    pid = row.get("pattern_id", "").strip()
                    if pid and pid in pid_to_jh:
                        bundle_jhs.add(pid_to_jh[pid])
        else:
            print(
                f"  WARN: bundle_membership.csv not found at {bm_path}. "
                f"Bundle filter will match nothing."
            )

    if filter_mode == "candidates":
        result = candidate_jhs
    elif filter_mode == "bundles":
        result = bundle_jhs
    elif filter_mode == "governance":
        result = candidate_jhs | bundle_jhs
    else:
        raise ValueError(f"Unknown filter_mode: {filter_mode!r}")

    print(
        f"  [filter_mode={filter_mode}] "
        f"candidates={len(candidate_jhs)} "
        f"bundle_members={len(bundle_jhs)} "
        f"eligible={len(result)}"
    )
    return result


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def _strip_json_fences(raw_text: str) -> str:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _call_llm(
    system_prompt: str,
    user_prompt: str,
    *,
    provider: str = "anthropic",
    model: str | None = None,
    max_tokens: int = 512,
    retry_count: int = 2,
    retry_delay: float = 2.0,
    groups_vocab: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Call the configured LLM provider and parse JSON response."""
    resolved_prompt = user_prompt
    if groups_vocab:
        lines = ["EXISTING GROUPS IN THIS DOMAIN:"]
        for label, definition in sorted(groups_vocab.items()):
            lines.append(f"  {label}: {definition}")
        resolved_prompt = resolved_prompt.replace(
            "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)",
            "\n".join(lines),
        )

    provider = provider.lower().strip()
    if provider not in {"anthropic", "openrouter"}:
        raise ValueError(f"Unsupported provider: {provider}")

    if provider == "openrouter" and not os.getenv("OPENROUTER_API_KEY"):
        raise RuntimeError("OPENROUTER_API_KEY is required when --provider openrouter is used")

    resolved_model = model or (
        "claude-haiku-4-5" if provider == "anthropic" else "anthropic/claude-haiku-4-5"
    )

    anthropic_client = None
    if provider == "anthropic":
        try:
            import anthropic
        except ImportError:
            print("ERROR: anthropic package not installed. Run: pip install anthropic")
            sys.exit(1)
        anthropic_client = anthropic.Anthropic()

    for attempt in range(retry_count + 1):
        try:
            if provider == "anthropic":
                response = anthropic_client.messages.create(
                    model=resolved_model,
                    max_tokens=max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": resolved_prompt}],
                )
                raw_text = response.content[0].text.strip()
            else:
                import requests

                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": resolved_model,
                        "max_tokens": max_tokens,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": resolved_prompt},
                        ],
                    },
                    timeout=60,
                )
                response.raise_for_status()
                data = response.json()
                raw_text = data["choices"][0]["message"]["content"].strip()

            result = json.loads(_strip_json_fences(raw_text))
            if "recommended" not in result:
                raise ValueError("Missing 'recommended' key in response")
            return result
        except json.JSONDecodeError as e:
            print(f"  WARN: JSON parse failed (attempt {attempt + 1}): {e}")
        except Exception as e:
            print(f"  WARN: API call failed (attempt {attempt + 1}): {e}")

        if attempt < retry_count:
            time.sleep(retry_delay)

    return None


def _groups_vocab_path(cache_path: str) -> str:
    stem, _ = os.path.splitext(cache_path)
    return f"{stem}_groups.json"


def load_groups_vocab(cache_path: str) -> Dict[str, str]:
    groups_path = _groups_vocab_path(cache_path)
    if not os.path.exists(groups_path):
        return {}
    try:
        with open(groups_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def save_groups_vocab(cache_path: str, vocab: Dict[str, str]) -> None:
    groups_path = _groups_vocab_path(cache_path)
    os.makedirs(os.path.dirname(os.path.abspath(groups_path)), exist_ok=True)
    with open(groups_path, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(vocab.items())), f, indent=2, ensure_ascii=False)
        f.write("\n")


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
    """Import domain prompt module, with progressive base-name fallback.

    Tries exact match first, then progressively strips trailing underscore
    segments to find a base module. Examples:
      dimension_types_linear  → dimension_types_linear, dimension_types
      fill_patterns_drafting  → fill_patterns_drafting, fill_patterns
      object_styles_model     → object_styles_model, object_styles
    """
    import importlib

    parts = domain.split("_")
    # Try from full name down to 1-segment base.
    # This preserves underscore fallback behavior while still supporting
    # single-word domains like "arrowheads".
    for n in range(len(parts), 0, -1):
        candidate = "_".join(parts[:n])
        try:
            return importlib.import_module(
                f"tools.label_synthesis.domain_prompts.{candidate}"
            )
        except ImportError:
            continue
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
    export_prompts: Optional[str] = None,
    import_results: Optional[str] = None,
    provider: str = "anthropic",
    model: Optional[str] = None,
    workers: int = 3,
    filter_mode: str = "all",
    domain_patterns_csv: Optional[str] = None,
    bundle_dir: Optional[str] = None,
) -> None:
    print(f"\n=== Label Synthesis: {domain} ===")
    print(f"  Exports dir:   {exports_dir}")
    print(f"  Analysis dir:  {analysis_dir}")
    print(f"  Cache path:    {cache_path}")
    print(f"  Dry run:       {dry_run}")
    print(f"  Export prompts: {export_prompts or '(disabled)'}")
    print(f"  Import results: {import_results or '(disabled)'}")
    print(f"  Provider:      {provider}")
    print(f"  Model:         {model or '(provider default)'}")
    print(f"  Workers:       {workers}")

    # Load existing cache
    cache = load_llm_cache(cache_path)
    print(f"  Existing cache entries: {len(cache)}")

    if import_results:
        with open(import_results, "r", encoding="utf-8") as f:
            imported_entries = json.load(f)
        for entry in imported_entries:
            join_hash = entry["join_hash"]
            cache[join_hash] = {
                "domain": domain,
                "recommended": entry["recommended"],
                "candidates": entry.get("candidates", [entry["recommended"]]),
                "rationale": entry.get("rationale", ""),
                "reviewed": False,
                "generated_at": date.today().isoformat(),
                "source": "import",
            }
        save_llm_cache(cache_path, cache)
        print(f"  Imported {len(imported_entries)} results → cache written to {cache_path}")
        return

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

    # governance filter
    if filter_mode != "all":
        eligible_jhs = _load_governance_join_hashes(
            domain=domain,
            filter_mode=filter_mode,
            analysis_dir=analysis_dir,
            domain_patterns_csv=domain_patterns_csv,
            bundle_dir=bundle_dir,
        )
        if eligible_jhs is not None:
            before = len(to_process)
            to_process = [jh for jh in to_process if jh in eligible_jhs]
            print(f"  Filter applied: {before} → {len(to_process)} patterns")

    print(f"  To process: {len(to_process)}")
    if not to_process:
        print("  Cache is current. Use --force-refresh to re-synthesize.")
        if review_csv:
            _write_review_csv(review_csv, cache, domain)
        return

    if export_prompts:
        prompt_exports = []
        for jh in to_process:
            rows = label_pop_by_hash.get(jh, [])
            rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))
            identity_items = _load_representative_identity_items(exports_dir, domain, jh)
            user_prompt = build_prompt_fn(
                join_hash=jh,
                observed_labels=rows_sorted,
                identity_items=identity_items,
            )
            prompt_exports.append({
                "join_hash": jh,
                "domain": domain,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
            })
        os.makedirs(os.path.dirname(os.path.abspath(export_prompts)), exist_ok=True)
        with open(export_prompts, "w", encoding="utf-8") as f:
            json.dump(prompt_exports, f, indent=2, ensure_ascii=False)
            f.write("\n")
        print(f"  Exported {len(prompt_exports)} prompts → {export_prompts}")
        return

    if not dry_run and provider == "openrouter" and not os.getenv("OPENROUTER_API_KEY"):
        raise RuntimeError("OPENROUTER_API_KEY is required when --provider openrouter is used")

    # Process each fragmented hash
    success = 0
    failed = 0
    today = date.today().isoformat()
    groups_vocab = load_groups_vocab(cache_path)
    discovered_groups: Dict[str, str] = {}
    cache_lock = threading.Lock()

    def _process_join_hash(jh: str):
        rows = label_pop_by_hash.get(jh, [])
        rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))
        identity_items = _load_representative_identity_items(exports_dir, domain, jh)
        user_prompt = build_prompt_fn(
            join_hash=jh,
            observed_labels=rows_sorted,
            identity_items=identity_items,
        )
        if dry_run:
            return (
                jh,
                {
                    "dry_run": True,
                    "user_prompt": user_prompt,
                    "labels": [r.get("label_v", "") for r in rows_sorted[:5]],
                },
                rows_sorted,
            )

        result = _call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            provider=provider,
            model=model,
            groups_vocab=groups_vocab,
        )
        return (jh, result, rows_sorted)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_process_join_hash, jh): jh for jh in to_process}

        for i, fut in enumerate(as_completed(futures), 1):
            jh = futures[fut]
            try:
                jh, result, rows_sorted = fut.result()
            except Exception as e:
                print(f"\n  [{i}/{len(to_process)}] join_hash={jh[:16]}...")
                print(f"    FAILED — worker exception: {e}")
                failed += 1
                continue

            print(f"\n  [{i}/{len(to_process)}] join_hash={jh[:16]}...")
            print(f"    Labels: {[r.get('label_v', '') for r in rows_sorted[:5]]}")

            if dry_run:
                print("    --- SYSTEM PROMPT (first 300 chars) ---")
                print(f"    {system_prompt[:300]}...")
                injected_prompt = result["user_prompt"]
                if groups_vocab:
                    vocab_lines = ["EXISTING GROUPS IN THIS DOMAIN:"]
                    for label, definition in sorted(groups_vocab.items()):
                        vocab_lines.append(f"  {label}: {definition}")
                    injected_prompt = injected_prompt.replace(
                        "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)",
                        "\n".join(vocab_lines),
                    )
                print("    --- USER PROMPT ---")
                print(f"    {injected_prompt[:500]}...")
                print("    [DRY RUN — skipping API call]")
                continue

            if result is None:
                print("    FAILED — skipping")
                failed += 1
                continue

            recommended = result.get("recommended", "")
            candidates = result.get("candidates", [])
            rationale = result.get("rationale", "")
            print(f"    Recommended: {recommended!r}")
            print(f"    Candidates:  {candidates}")

            semantic_group = (result.get("semantic_group") or "").strip()
            if semantic_group and semantic_group not in groups_vocab:
                discovered_groups[semantic_group] = rationale

            with cache_lock:
                cache[jh] = {
                    "domain": domain,
                    "recommended": recommended,
                    "candidates": candidates,
                    "rationale": rationale,
                    "reviewed": False,
                    "generated_at": today,
                    "observed_labels": [r.get("label_v", "") for r in rows_sorted[:10]],
                }
                save_llm_cache(cache_path, cache)
            success += 1

    if not dry_run:
        if discovered_groups:
            groups_vocab.update(discovered_groups)
        save_groups_vocab(cache_path, groups_vocab)

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
    ap.add_argument(
        "--export-prompts", default=None, metavar="PATH",
        help="Write assembled prompts to this JSON file instead of calling the API. "
             "No API calls are made and the cache is not written.",
    )
    ap.add_argument(
        "--import-results", default=None, metavar="PATH",
        help="Import LLM results from this JSON file and merge into cache. "
             "No API calls are made.",
    )
    ap.add_argument("--provider", choices=["anthropic", "openrouter"], default="anthropic",
                    help="LLM provider backend")
    ap.add_argument("--model", default=None,
                    help="Optional model override for selected provider")
    ap.add_argument("--workers", type=int, default=3,
                    help="Concurrent worker count for API calls")
    ap.add_argument(
        "--filter-mode",
        choices=["all", "candidates", "bundles", "governance"],
        default="all",
        help=(
            "Which patterns to synthesize. "
            "'all' = every fragmented pattern (default). "
            "'candidates' = is_candidate_standard=true only. "
            "'bundles' = patterns in at least one bundle. "
            "'governance' = union of candidates and bundle members."
        ),
    )
    ap.add_argument(
        "--bundle-dir",
        default=None,
        metavar="PATH",
        help=(
            "Directory containing bundle_membership.csv "
            "(required when --filter-mode is 'bundles' or 'governance')."
        ),
    )
    ap.add_argument(
        "--domain-patterns-csv",
        default=None,
        metavar="PATH",
        help=(
            "Optional explicit path to domain_patterns.csv used by non-'all' filter modes. "
            "Defaults to <analysis-dir>/domain_patterns.csv, then "
            "<analysis-dir>/../analysis_v21/domain_patterns.csv."
        ),
    )
    args = ap.parse_args()

    if args.export_prompts and args.import_results:
        ap.error("--export-prompts and --import-results are mutually exclusive.")
    if args.dry_run and (args.export_prompts or args.import_results):
        ap.error("--dry-run cannot be combined with --export-prompts or --import-results.")
    if args.filter_mode in ("bundles", "governance") and not args.bundle_dir:
        ap.error("--bundle-dir is required when --filter-mode is 'bundles' or 'governance'.")

    synthesize(
        exports_dir=args.exports_dir,
        analysis_dir=args.analysis_dir,
        domain=args.domain,
        cache_path=args.cache_path,
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        only_unreviewed=args.only_unreviewed,
        review_csv=args.review_csv,
        export_prompts=args.export_prompts,
        import_results=args.import_results,
        provider=args.provider,
        model=args.model,
        workers=args.workers,
        filter_mode=args.filter_mode,
        domain_patterns_csv=args.domain_patterns_csv,
        bundle_dir=args.bundle_dir,
    )


if __name__ == "__main__":
    main()
