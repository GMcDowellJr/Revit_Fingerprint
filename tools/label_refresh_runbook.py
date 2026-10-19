#!/usr/bin/env python3
"""Corrected port of label_refresh_runbook.ps1.

The .ps1 derived results/segments under Fingerprint_Data\\exports\\; this port
uses the corpus_update_runbook layout (results = exports_root, segments =
exports_root/segments) per the 2026-07 move. Not a faithful port.

Path layout (--exports-root, default ./Fingerprint_Data, same semantics as
tools/corpus_update_runbook.py):
  exports  = exports_root/exports
  results  = exports_root
  segments = exports_root/segments
  records  = results/records
  cache    = results/label_synthesis/llm_name_cache.json
  lookup   = results/label_synthesis/identity_items_by_joinhash.csv
  dp_csv   = results/analysis/domain_patterns.csv

Steps L1-L4 are otherwise preserved from the .ps1: same CLIs, same flags,
same L2 domain list, stop on the first failed L1/L1b/L2/L3/L4 command.

L1b (new, not in the .ps1): rebuild the corpus-level
label_synthesis/{domain}.joinhash_label_population.csv files from
records/records.csv via build_label_population.py before L2. L2's
synthesize_fragmented_labels.py reads them from --analysis-dir and, when one
is missing, prints an error but exits 0. Nothing else writes them at corpus
level by default: run_extract_all.py builds them only in the authority/patterns
stage (corpus_update_runbook Run B with -RunAuthorityPatterns, opt-in), and Run C
writes them per segment. Without L1b, a default Run A -> Run C -> label refresh
sequence would skip synthesis for every domain and still report success (or use
stale files left by an older Run B).

L2 filter mode (resolved): the .ps1 passed --filter-mode bundles with
--segments-root/--registry-file and carried an "until union mode is
confirmed, use --filter-mode candidates" note. Union mode IS implemented in
tools/label_synthesis/synthesize_fragmented_labels.py
(_collect_union_bundle_join_hashes(), selected by _load_governance_join_hashes()
when filter_mode is bundles/governance and both --segments-root and
--registry-file are given; main() validates the pair). This port therefore
keeps --filter-mode bundles with --segments-root and --registry-file; the
candidates fallback is not needed.

--dry-run prints one "DRYRUN: <command>" line per would-be subprocess and
does not prompt for OPENROUTER_API_KEY, write os.environ, or chdir. Reads
(run_registry.csv, bundles.csv discovery for L4) still execute.

NOTES (carried over from the .ps1)

When to run:
  After corpus_update_runbook Run C completes and the corpus is stable.
  After adding new extraction domains that have bundle analysis output.
  After curator label review (--import-results workflow).
  Not needed on every corpus update -- the LLM cache handles incremental runs.

Incremental behaviour:
  L1 (lookup)    - always rebuild, fast ~2 min
  L1b (population) - always rebuild from records.csv, fast, no API calls
  L2 (synthesis) - skips join_hashes already in cache; only new patterns cost tokens
                   stops immediately on any failed domain synthesis command
  L3 (patch)     - skips rows with authoritative sources (curator/synopsis/modal)
  L4 (export)    - overwrites BI export CSVs in bundle_analysis view folders for
                   completed bundle runs; fast, no API calls

Adding domains to L2:
  Add the domain name to L2_DOMAINS below.
  Ensure a domain_prompts/{domain}.py module exists for best results;
  falls back to generic prompt if absent.

Force-refresh all cached labels (after major prompt module changes):
  Add --force-refresh to the synthesize invocation in L2.
  Then re-run L3 and L4.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import os
import sys
from pathlib import Path
from typing import Sequence

try:
    import tools.corpus_update_runbook as _runbook
except ImportError:  # executed as `python tools/label_refresh_runbook.py`
    import corpus_update_runbook as _runbook

cyan = _runbook.cyan
green = _runbook.green
yellow = _runbook.yellow
red = _runbook.red
run_external = _runbook.run_external

L2_DOMAINS = (
    "arrowheads",
    "fill_patterns_drafting",
    "fill_patterns_model",
    "line_patterns",
    "line_styles",
    "view_filter_definitions",
)


def assert_native_success(step: str, returncode: int) -> None:
    """PowerShell Assert-NativeSuccess equivalent (throw -> exit 1)."""
    if returncode != 0:
        print(red(f"{step} failed with exit code {returncode}"))
        raise SystemExit(1)


def normalize_powershell_style_argv(argv: Sequence[str]) -> list[str]:
    """Make parameter names case-insensitive like PowerShell."""
    option_map = {
        "-exportsroot": "--exports-root",
        "--exportsroot": "--exports-root",
        "--exports-root": "--exports-root",
        "-dryrun": "--dry-run",
        "--dryrun": "--dry-run",
        "--dry-run": "--dry-run",
        "-h": "--help",
        "--help": "--help",
    }
    return [option_map.get(token.lower(), token) for token in argv]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Label refresh runbook (L1-L4); corrected port of label_refresh_runbook.ps1.",
        allow_abbrev=False,
    )
    parser.add_argument("--exports-root", dest="exports_root", default="./Fingerprint_Data")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(normalize_powershell_style_argv(raw_argv))

    repo = Path(__file__).resolve().parent.parent
    exports_root = Path(args.exports_root).expanduser()
    exports = exports_root / "exports"
    results = exports_root
    segments = exports_root / "segments"
    records = results / "records"
    cache = results / "label_synthesis" / "llm_name_cache.json"
    lookup = results / "label_synthesis" / "identity_items_by_joinhash.csv"
    dp_csv = results / "analysis" / "domain_patterns.csv"

    _runbook.DRY_RUN = args.dry_run
    dry_run = args.dry_run

    def observed(path: Path) -> Path:
        # Live mode has chdir'd to repo; dry-run hasn't, so resolve explicitly.
        return repo / path if dry_run else path

    if dry_run:
        print(f"DRYRUN: would chdir {repo}")
    else:
        os.chdir(repo)
    python = sys.executable

    print(cyan("--- L1: identity_items lookup (corpus-level, always rebuild) ---"))
    assert_native_success("L1 identity_items lookup", run_external([
        python, "tools/label_synthesis/build_identity_items_lookup.py",
        "--records-dir", records,
        "--out-dir", results / "label_synthesis",
    ], checked=False))

    print(cyan("--- L1b: corpus label population (always rebuild; feeds L2) ---"))
    assert_native_success("L1b corpus label population", run_external([
        python, "tools/label_synthesis/build_label_population.py",
        "--out-root", results,
        "--records-dir", records,
        "--label-synth-dir", results / "label_synthesis",
    ], checked=False))

    if not os.environ.get("OPENROUTER_API_KEY"):
        if dry_run:
            print("DRYRUN: OPENROUTER_API_KEY unset; would prompt")
        else:
            os.environ["OPENROUTER_API_KEY"] = getpass.getpass("Enter OPENROUTER_API_KEY: ")

    print(cyan("--- L2: LLM synthesis (bundle-eligible domains, corpus-level cache) ---"))
    for dom in L2_DOMAINS:
        print(cyan(f"  synthesizing: {dom}"))
        assert_native_success(f"L2 LLM synthesis for {dom}", run_external([
            python, "-m", "tools.label_synthesis.synthesize_fragmented_labels",
            "--exports-dir", exports,
            "--analysis-dir", results / "label_synthesis",
            "--domain", dom,
            "--cache", cache,
            "--identity-items-lookup", lookup,
            "--domain-patterns-csv", dp_csv,
            "--provider", "openrouter",
            "--filter-mode", "bundles",
            "--segments-root", segments,
            "--registry-file", records / "run_registry.csv",
            "--workers", "8",
        ], checked=False))

    print(cyan("--- L3: patch all domain_patterns (corpus + all segments) ---"))
    assert_native_success("L3 patch all domain_patterns", run_external([
        python, "tools/label_synthesis/patch_all_domain_patterns.py",
        "--results-root", results,
        "--segments-root", segments,
    ], checked=False))

    print(cyan("--- L4: export bundle pattern detail (completed bundle segments x all/used) ---"))
    registry_csv = records / "run_registry.csv"
    active: list[dict] = []
    if not observed(registry_csv).exists():
        # .ps1: Import-Csv on a missing file is a terminating error.
        if dry_run:
            print(f"DRYRUN: guard would fail: {registry_csv} not found")
        else:
            print(red(f"ERROR: {registry_csv} not found"))
            return 1
    else:
        with observed(registry_csv).open(newline="", encoding="utf-8-sig") as handle:
            # PowerShell -eq is case-insensitive.
            active = [
                row for row in csv.DictReader(handle)
                if (row.get("run_type") or "").lower() == "bundle"
                and (row.get("status") or "").lower() == "complete"
            ]

    for seg in active:
        output_folder = seg.get("output_folder") or ""
        for view in ("all", "used"):
            bundle_view_dir = segments / output_folder / "results" / "bundle_analysis" / view
            bundle_files: list[Path] = []
            if observed(bundle_view_dir).exists():
                bundle_files = [p for p in observed(bundle_view_dir).rglob("bundles.csv") if p.is_file()]
            if not bundle_files:
                print(yellow(f"  skipping segment={output_folder}  view={view} (no bundle_analysis/{view}/*/bundles.csv)"))
                continue

            print(cyan(f"  segment={output_folder}  view={view}"))
            assert_native_success(f"L4 export bundle pattern detail for {output_folder} {view}", run_external([
                python, "tools/export_bundle_pattern_detail.py",
                "--output-folder", output_folder,
                "--segments-root", segments,
                "--records-dir", records,
                "--purge-view", view,
                "--out-dir", bundle_view_dir,
            ], checked=False))

    print()
    print(green("=== LABEL REFRESH COMPLETE ==="))
    print(green("Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
