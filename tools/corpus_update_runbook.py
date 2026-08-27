#!/usr/bin/env python3
"""Python port of corpus_update_runbook.ps1.

Preserves the PowerShell runbook's A/B/C workflow, validation, subprocess
failure semantics, NameKey behavior, freshness guard, ForceAll cache clearing,
and operator-facing messages.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def color(text: str, ansi: str) -> str:
    if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
        return text
    return f"\033[{ansi}m{text}\033[0m"


def green(text: str) -> str:
    return color(text, "32")


def yellow(text: str) -> str:
    return color(text, "33")


def cyan(text: str) -> str:
    return color(text, "36")


def red(text: str) -> str:
    return color(text, "31")


def invoke_checked(step_name: str, returncode: int) -> None:
    """PowerShell Invoke-Checked equivalent."""
    if returncode != 0:
        print(red(f"ERROR: {step_name} failed (exit code {returncode})"))
        raise SystemExit(1)


def run_external(command: Sequence[object], step_name: str | None = None,
                 checked: bool = True) -> int:
    """Run an external process without a shell and optionally enforce success."""
    cmd = [str(part) for part in command]
    try:
        completed = subprocess.run(cmd, check=False)
        returncode = completed.returncode
    except OSError as exc:
        print(red(f"ERROR: could not start {cmd[0]}: {exc}"))
        returncode = 1
    if checked:
        invoke_checked(step_name or "External command", returncode)
    return returncode


def remove_latent_purgeable_files(segments: Path, records: Path) -> None:
    """Equivalent to Get-ChildItem -Recurse plus Remove-Item -Force."""
    if segments.exists():
        for path in segments.rglob("latent_purgeable.csv"):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
    corpus_file = records / "latent_purgeable.csv"
    try:
        corpus_file.unlink()
    except FileNotFoundError:
        pass


def print_usage(exports_root: Path, records: Path, name_key_csv: Path) -> None:
    print()
    print("Usage:")
    print("  python tools/corpus_update_runbook.py -Run A    # flatten + apply + placeholders")
    print("  python tools/corpus_update_runbook.py -Run B    # authority + patterns + patch")
    print("  python tools/corpus_update_runbook.py -Run C    # segments + all/used bundle analysis (use compare_cross_segment.py for cross-segment comparison)")
    print("  python tools/corpus_update_runbook.py -Run C -ForceAll   # Run C, but re-run every segment regardless of registry status")
    print("  python tools/corpus_update_runbook.py -Run A -ForceAll   # Run A, but reprocess every export file regardless of the incremental cache")
    print("  python tools/corpus_update_runbook.py -Run A -ExportsRoot 'D:\\Somewhere\\Else'   # override the exports/results/segments root")
    print()
    print(f"  -ExportsRoot (default: {exports_root}):")
    print("    Root containing the raw *.json exports plus the results\\ and segments\\")
    print("    folders nested under it. Override if the data has moved without editing")
    print("    this script.")
    print()
    print("  -ForceAll: registry-/cache-driven skip is the default for both Run A and Run C.")
    print("    Run A: an export file whose content and both policy files (sig_hash, join)")
    print(f"    are unchanged since the last successful Run A is reused from")
    print(f"    {records.parent / '.run_a_cache'} instead of being re-flattened. -ForceAll bypasses this")
    print("    gate and reprocesses every export file, rebuilding the cache fresh -- use it")
    print("    if you suspect the cache is stale/corrupt, or after a change the cache can't")
    print("    detect on its own (an edit to a policy file already forces a full rebuild")
    print("    automatically; this is for edge cases beyond that).")
    print("    Run C: a segment is re-run only if its file population changed since the last")
    print("    complete run. Pass -ForceAll after a sig_hash/join_hash policy change")
    print("    (population_hash is membership-only and cannot detect those) to force a")
    print("    full-corpus rebuild.")
    print()
    print("  -NameKey (Run A/B/C, opt-in, additive): also produce the Canonical Name")
    print("    Identity Projection (join_key_name_identity) alongside the default")
    print("    join_hash output. Does NOT change any default Run A/B/C output -- see")
    print("    DECISIONS.md D-037 for what this does and does not cover.")
    print(f"      -Run A -NameKey   # parse exports once, corpus-wide -> {name_key_csv}")
    print("      -Run B -NameKey   # OPTIONAL whole-corpus (unsegmented) name patterns;")
    print("                        # not required before Run C, which re-clusters per segment")
    print("      -Run C -NameKey   # also writes results/bundle_analysis/name_all/ per segment")
    print("                        # (requires -Run A -NameKey to have been run first)")
    print()
    print("MANDATORY PAUSE between Run A and Run B:")
    print(f"  Edit {records / 'file_metadata.csv'}")
    print("  Set for each new file:")
    print("    governance_role        ->  Container | Template | Project | Generic")
    print("    client_label           ->  client name or internal identifier (e.g. 'InternalEnterprise' for enterprise work)")
    print("    business_center_label  ->  bare numeric business center code (e.g. '2014'), or '0000'/'BC_0000' for enterprise-scoped work")
    print("    collection_label       ->  standards/resource collection this file belongs to (optional; independent of governance_role - see header comment)")
    print("    unit_system            ->  imperial | metric")
    print()
    print("  Run B hard-fails if client_label or business_center_label is blank or an N/A spelling - see")
    print("  run_extract_all.py's _check_governance_field_completeness().")
    print()


def normalize_powershell_style_argv(argv: Sequence[str]) -> list[str]:
    """Make parameter names and ValidateSet values case-insensitive like PowerShell."""
    option_map = {
        "-run": "--run",
        "--run": "--run",
        "-forceall": "--force-all",
        "--forceall": "--force-all",
        "--force-all": "--force-all",
        "-namekey": "--name-key",
        "--namekey": "--name-key",
        "--name-key": "--name-key",
        "-exportsroot": "--exports-root",
        "--exportsroot": "--exports-root",
        "--exports-root": "--exports-root",
        "-h": "--help",
        "--help": "--help",
    }
    normalized: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        canonical = option_map.get(token.lower(), token)
        normalized.append(canonical)
        if canonical == "--run" and index + 1 < len(argv):
            index += 1
            normalized.append(argv[index].upper())
        index += 1
    return normalized


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--run", dest="run", choices=("A", "B", "C"), default="")
    parser.add_argument("--force-all", dest="force_all", action="store_true")
    parser.add_argument("--name-key", dest="name_key", action="store_true")
    parser.add_argument("--exports-root", dest="exports_root", default="./Fingerprint_Data")
    parser.add_argument("--help", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(normalize_powershell_style_argv(raw_argv))

    # Equivalent to Resolve-Path (Join-Path $PSScriptRoot "..").
    repo = Path(__file__).resolve().parent.parent
    exports_root = Path(args.exports_root).expanduser()
    exports = exports_root / "exports"
    results = exports_root
    segments = exports_root / "segments"
    records = results / "records"
    sig_pol = repo / "policies" / "domain_sig_hash_policies.json"
    join_pol = repo / "policies" / "domain_join_key_policies.json"
    name_key_pol = repo / "policies" / "domain_name_key_policies.json"
    name_key_csv = results / "name_key" / "name_key_results.csv"

    os.chdir(repo)
    python = sys.executable

    if args.help or args.run == "":
        print_usage(exports_root, records, name_key_csv)
        return 0

    if args.run == "A":
        print(green("=== RUN A: Flatten / Apply / Placeholders ==="))

        # --incremental: skip re-parsing an export file whose content and both
        # policy files are unchanged since the last successful Run A
        # (tools/run_a_cache.py / tools/run_a_incremental.py). placeholders
        # (T2b) always still runs over the full current population regardless
        # of cache state -- only flatten/sig_hash/apply's per-file work is
        # gated. -ForceAll bypasses the gate for this run.
        run_a_force_args: list[object] = []
        if args.force_all:
            print(cyan("--- Run A: -ForceAll -> bypassing the incremental cache, reprocessing every export file ---"))
            run_a_force_args = ["--force-full-cache"]

        run_external([
            python, "tools/run_extract_all.py", exports,
            "--out-root", results,
            "--out-root-is-results-root",
            "--stages", "sig_hash,flatten,apply,placeholders",
            "--sig-hash-policy", sig_pol,
            "--join-policy", join_pol,
            "--incremental",
            *run_a_force_args,
        ], "Run A: flatten/apply/placeholders")

        if args.name_key:
            print()
            print(cyan("--- A-NameKey: parse exports for name-identity projection (join_key_name_identity) ---"))
            run_external([
                python, "tools/apply_name_key_policy.py",
                "--export-dir", exports,
                "--name-key-policy", name_key_pol,
                "--out", name_key_csv,
            ], "Run A-NameKey: apply_name_key_policy.py")
            print(cyan(f"  wrote {name_key_csv}"))

        print()
        print(yellow("=== RUN A COMPLETE ==="))
        print(yellow("NEXT: Edit file_metadata.csv before running Run B"))
        print(yellow(f"  File: {records / 'file_metadata.csv'}"))
        print(yellow("  Set for each new file:"))
        print(yellow("    governance_role        ->  Container | Template | Project | Generic"))
        print(yellow("    client_label           ->  client name or internal identifier (e.g. 'InternalEnterprise' for enterprise work)"))
        print(yellow("    business_center_label  ->  bare numeric business center code (e.g. '2014'), or '0000'/'BC_0000' for enterprise-scoped work"))
        print(yellow("    collection_label       ->  standards/resource collection this file belongs to (optional; independent of governance_role - see header comment)"))
        print(yellow("    unit_system            ->  imperial | metric"))
        print(yellow("  Run B hard-fails if client_label or business_center_label is blank or an N/A spelling."))
        print(yellow("Then run: python tools/corpus_update_runbook.py -Run B"))

    if args.run == "B":
        print(green("=== RUN B: Authority / Patterns / Patch ==="))
        print(cyan("--- B1: authority + patterns ---"))
        run_external([
            python, "tools/run_extract_all.py", exports,
            "--out-root", results,
            "--out-root-is-results-root",
            "--stages", "authority,patterns",
        ], "Run B1: authority/patterns")

        print(cyan("--- B2: patch corpus domain_patterns ---"))
        run_external([
            python, "tools/label_synthesis/patch_all_domain_patterns.py",
            "--results-root", results,
            "--segments-root", segments,
        ], "Run B2: patch_all_domain_patterns.py")

        if args.name_key:
            print(cyan("--- B-NameKey (OPTIONAL; not required before Run C -- Run C re-clusters per segment) ---"))
            if not name_key_csv.exists():
                print(yellow(f"  SKIPPED: {name_key_csv} not found -- run 'python tools/corpus_update_runbook.py -Run A -NameKey' first."))
            else:
                run_external([
                    python, "tools/generate_name_key_patterns.py",
                    "--comparison-target", "name",
                    "--name-key-csv", name_key_csv,
                    "--out-root", results / "name_key" / "patterns",
                ], "Run B-NameKey: generate_name_key_patterns.py")

        print(green("=== RUN B COMPLETE - proceed to Run C ==="))

    if args.run == "C":
        print(green("=== RUN C: Segments + all/used bundle analysis ==="))
        print(yellow("IF FINGERPRINT_DATA IS UNDER A ONEDRIVE-SYNCED FOLDER: bundle segment runs can"))
        print(yellow("  fail near-100% at clear_stale_name_all with [WinError 5] Access is denied on"))
        print(yellow("  results\\bundle_analysis\\name_all\\<domain>. Pausing OneDrive sync did NOT fix"))
        print(yellow("  this in testing (2026-08-19) -- root cause is unconfirmed, not necessarily"))
        print(yellow("  OneDrive itself. Confirmed workaround: relocate Fingerprint_Data outside any"))
        print(yellow("  OneDrive-synced folder and pass -ExportsRoot pointing at the new location."))
        print(yellow("  This starves comparison_registry.csv -- see CLAUDE.md Warnings for detail."))
        print(cyan("Run C contract:"))
        print(cyan("  All view  = full configured vocabulary for each segment."))
        print(cyan("  Used view = project vocabulary excluding conclusively purgeable records."))
        print(cyan("  Template, Generic, and most Container roles are provided-vocabulary references;"))
        print(cyan("  purge/used interpretation is meaningful primarily for Project targets."))

        print(cyan("--- C1: segment manifest ---"))
        run_external([
            python, "tools/build_segment_manifest.py",
            "--metadata-file", records / "file_metadata.csv",
            "--out-dir", records,
            "--enable-parent-bundle-runs",
        ], "Run C1: build_segment_manifest.py")

        if args.force_all:
            print(cyan("--- C1.5: clear stale latent_purgeable.csv (-ForceAll) ---"))
            remove_latent_purgeable_files(segments, records)
        else:
            print(cyan("--- C1.5: skipped (pass -ForceAll to clear cached latent_purgeable.csv) ---"))

        print(cyan("--- C2: segment orchestrator (produces all-view and used-view bundle analysis) ---"))
        force_args = ["--force"] if args.force_all else []
        name_key_args: list[object] = []

        if args.name_key:
            if not name_key_csv.exists():
                print(red(f"ERROR: -NameKey requires {name_key_csv} to exist -- run 'python tools/corpus_update_runbook.py -Run A -NameKey' first."))
                return 1

            records_csv = records / "records.csv"
            if records_csv.exists():
                name_key_age = name_key_csv.stat().st_mtime
                records_age = records_csv.stat().st_mtime
                if name_key_age < records_age:
                    name_key_utc = __import__('datetime').datetime.fromtimestamp(name_key_age, __import__('datetime').timezone.utc)
                    records_utc = __import__('datetime').datetime.fromtimestamp(records_age, __import__('datetime').timezone.utc)
                    print(red(f"ERROR: {name_key_csv} ({name_key_utc} UTC) is older than {records_csv} ({records_utc} UTC)."))
                    print(red("  This usually means Run A ran again for new/changed exports without -NameKey, so"))
                    print(red("  name_key_results.csv is stale and would silently miss those files. Re-run:"))
                    print(red("    python tools/corpus_update_runbook.py -Run A -NameKey"))
                    return 1

            print(cyan("--- C2-NameKey: also producing results/bundle_analysis/name_all/ per segment ---"))
            name_key_args = ["--comparison-target", "both", "--name-key-results-csv", name_key_csv]

        orchestrator_code = run_external([
            python, "tools/run_segment_orchestrator.py",
            "--manifest-file", records / "segment_manifest.csv",
            "--registry-file", records / "run_registry.csv",
            "--results-registry-file", records / "results_registry.csv",
            "--records-dir", records,
            "--exports-dir", exports,
            "--segments-root", segments,
            "--repo-root", repo,
            "--join-policy", join_pol,
            *force_args,
            *name_key_args,
        ], checked=False)

        if orchestrator_code != 0:
            print(yellow(f"WARNING: run_segment_orchestrator.py reported segment failures (exit code {orchestrator_code})."))
            print(yellow("  See run_registry.csv (status=failed rows) and each failed segment's bundle.log / bundle_name.log / run.log."))

        print(cyan("--- C2.5: rebuild BI results registry ---"))
        run_external([
            python, "tools/build_results_registry.py",
            "--manifest-file", records / "segment_manifest.csv",
            "--registry-file", records / "run_registry.csv",
            "--output-file", records / "results_registry.csv",
        ], "Run C2.5: build_results_registry.py")

        print(cyan("--- C3: re-patch all segment domain_patterns ---"))
        run_external([
            python, "tools/label_synthesis/patch_all_domain_patterns.py",
            "--results-root", results,
            "--segments-root", segments,
        ], "Run C3: patch_all_domain_patterns.py")

        print(green("=== RUN C COMPLETE ==="))
        print(green("Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh"))
        print(cyan("Cross-segment comparison: run compare_cross_segment.py separately"))
        print(cyan("Reminder: used/purge signals are active-delivery signals primarily for Project targets; do not label Template or Generic stock content as unused bloat."))
        if args.name_key:
            print(cyan("Name-projection output: {segment folder}\\results\\bundle_analysis\\name_all\\... (join_key_name_identity instead of join_hash; ALL view only -- no used-view/compare/share-profile equivalent yet, see DECISIONS.md D-037)"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
