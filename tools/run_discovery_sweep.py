#!/usr/bin/env python3
"""
Run the full greedy/Pareto join_key + sig_hash discovery sweep across every
configured domain, sized per-domain from discovery_param_suggestions.csv.

Python replacement for run_discovery_sweep.ps1.

Examples
--------
Execute the full sweep:
    python tools/run_discovery_sweep.py --run

Preview commands without executing:
    python tools/run_discovery_sweep.py --what-if

Run selected domains:
    python tools/run_discovery_sweep.py \
        --domains fill_patterns_drafting,fill_patterns_model \
        --run

Run only join-key discovery:
    python tools/run_discovery_sweep.py --skip-sig --run

Run only sig-hash discovery:
    python tools/run_discovery_sweep.py --skip-join --run
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence, TextIO


WARNING_PATTERN = re.compile(
    r"\[discover\]\s+WARNING|sample_vs_full_diverges.*true",
    flags=re.IGNORECASE,
)

# discover_hash_policy.py's --discovery-target sig invocations and
# discover_join_policy.py's join invocations run as two entirely separate
# subprocesses in this sweep, so discover_hash_policy.py's own in-process
# sig/join convergence flag (which only fires for --discovery-target both,
# never used here) can never see both sides. Re-check convergence at the
# sweep level instead, once per domain, by reading back whatever diagnostics
# CSVs each side actually wrote.
_SIG_CSV_GLOB = "hash_sig_discovery_exploration*__{domain}__*.csv"
_JOIN_CSV_GLOB = "join_key_discovery_exploration*__{domain}__*.csv"

REQUIRED_COLUMNS = {
    "domain",
    "suggested_sample_size",
    "suggested_max_candidate_fields",
    "suggested_max_k_discover",
    "suggested_max_k_harsh_validate",
    "stratify_by_recommended",
}


@dataclass(frozen=True)
class Suggestion:
    domain: str
    sample_size: str
    max_candidate_fields: str
    max_k_discover: int
    max_k_harsh_validate: int
    stratify_by: str | None


@dataclass(frozen=True)
class Invocation:
    label: str
    args: tuple[str, ...]
    log_path: Path


@dataclass(frozen=True)
class InvocationResult:
    label: str
    exit_code: int
    log_path: Path
    warning_lines: tuple[str, ...]
    error: str | None = None


class Tee:
    """Write text to multiple streams."""

    def __init__(self, *streams: TextIO) -> None:
        self.streams = streams

    def write(self, text: str) -> None:
        for stream in self.streams:
            stream.write(text)
            stream.flush()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    script_path = Path(__file__).resolve()
    default_repo_root = script_path.parent.parent

    parser = argparse.ArgumentParser(
        description=(
            "Run join-key and sig-hash discovery across domains using values "
            "from discovery_param_suggestions.csv."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--exports-root",
        type=Path,
        default=Path("Fingerprint_Data"),
        help="Root containing records/ and diagnostics/ (default: Fingerprint_Data).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo_root,
        help="Repository root containing tools/ and policies/.",
    )
    parser.add_argument(
        "--suggestions-csv",
        type=Path,
        default=None,
        help=(
            "Suggestion CSV. Default: "
            "<exports-root>/diagnostics/discovery_param_suggestions.csv."
        ),
    )
    parser.add_argument(
        "--domains",
        default="",
        help="Optional comma-separated domain allow-list.",
    )
    parser.add_argument(
        "--skip-join",
        action="store_true",
        help="Skip discover_join_policy.py.",
    )
    parser.add_argument(
        "--skip-sig",
        action="store_true",
        help="Skip discover_hash_policy.py sig discovery.",
    )

    execution = parser.add_mutually_exclusive_group()
    execution.add_argument(
        "--run",
        action="store_true",
        help="Execute the generated commands.",
    )
    execution.add_argument(
        "--what-if",
        action="store_true",
        help="Print generated commands without executing them.",
    )

    return parser.parse_args(argv)


def clean_required_text(row: dict[str, str | None], column: str, domain: str) -> str:
    value = (row.get(column) or "").strip()
    if not value:
        raise ValueError(f"row for domain '{domain}' has blank '{column}'")
    return value


def parse_positive_int(value: str, column: str, domain: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(
            f"row for domain '{domain}' has non-integer '{column}': {value!r}"
        ) from exc
    if parsed < 1:
        raise ValueError(
            f"row for domain '{domain}' has '{column}' < 1: {parsed}"
        )
    return parsed


def load_suggestions(csv_path: Path) -> list[Suggestion]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - columns)
        if missing:
            raise ValueError(
                f"suggestions CSV is missing required column(s): {', '.join(missing)}"
            )

        suggestions: list[Suggestion] = []
        seen_domains: set[str] = set()

        for line_number, row in enumerate(reader, start=2):
            domain = (row.get("domain") or "").strip()
            if not domain:
                raise ValueError(f"blank domain at CSV line {line_number}")
            if domain in seen_domains:
                raise ValueError(
                    f"duplicate domain '{domain}' at CSV line {line_number}"
                )
            seen_domains.add(domain)

            sample_size = clean_required_text(
                row, "suggested_sample_size", domain
            )
            max_fields = clean_required_text(
                row, "suggested_max_candidate_fields", domain
            )
            max_k_discover_text = clean_required_text(
                row, "suggested_max_k_discover", domain
            )
            max_k_harsh_text = clean_required_text(
                row, "suggested_max_k_harsh_validate", domain
            )

            # Validate values passed through to the downstream tools.
            parse_positive_int(sample_size, "suggested_sample_size", domain)
            parse_positive_int(
                max_fields, "suggested_max_candidate_fields", domain
            )
            max_k_discover = parse_positive_int(
                max_k_discover_text, "suggested_max_k_discover", domain
            )
            max_k_harsh = parse_positive_int(
                max_k_harsh_text,
                "suggested_max_k_harsh_validate",
                domain,
            )
            stratify = (row.get("stratify_by_recommended") or "").strip() or None

            suggestions.append(
                Suggestion(
                    domain=domain,
                    sample_size=sample_size,
                    max_candidate_fields=max_fields,
                    max_k_discover=max_k_discover,
                    max_k_harsh_validate=max_k_harsh,
                    stratify_by=stratify,
                )
            )

    return suggestions


def select_domains(
    suggestions: Iterable[Suggestion], domains_text: str
) -> list[Suggestion]:
    rows = list(suggestions)
    if not domains_text.strip():
        return rows

    requested = [part.strip() for part in domains_text.split(",") if part.strip()]
    requested_set = set(requested)
    selected = [row for row in rows if row.domain in requested_set]
    found = {row.domain for row in selected}
    missing = [domain for domain in requested if domain not in found]

    if missing:
        raise ValueError(
            "requested domain(s) not found in suggestions CSV: "
            + ", ".join(missing)
        )
    return selected


def build_join_invocations(
    suggestion: Suggestion,
    records_dir: Path,
    join_policy: Path,
    log_dir: Path,
) -> list[Invocation]:
    domain = suggestion.domain
    base = [
        "tools/discover_join_policy.py",
        "--phase0-dir",
        str(records_dir),
        "--domains",
        domain,
        "--sample-size",
        suggestion.sample_size,
        "--max-candidate-fields",
        suggestion.max_candidate_fields,
        "--policy-json",
        str(join_policy),
        "--warn-only",
    ]
    if suggestion.stratify_by:
        base.extend(["--stratify-by", suggestion.stratify_by])

    discover_k = suggestion.max_k_discover
    harsh_k = suggestion.max_k_harsh_validate

    if discover_k == harsh_k:
        return [
            Invocation(
                label=f"join_key ({domain}, combined, max-k={discover_k})",
                args=tuple(base + ["--max-k", str(discover_k)]),
                log_path=log_dir / f"join__{domain}__combined.log",
            )
        ]

    return [
        Invocation(
            label=f"join_key ({domain}, discover, max-k={discover_k})",
            args=tuple(
                base
                + [
                    "--max-k",
                    str(discover_k),
                    "--policy-modes",
                    "discover",
                ]
            ),
            log_path=log_dir / f"join__{domain}__discover.log",
        ),
        Invocation(
            label=f"join_key ({domain}, validate+harsh, max-k={harsh_k})",
            args=tuple(
                base
                + [
                    "--max-k",
                    str(harsh_k),
                    "--policy-modes",
                    "validate,harsh",
                ]
            ),
            log_path=log_dir / f"join__{domain}__validate_harsh.log",
        ),
    ]


def build_sig_invocation(
    suggestion: Suggestion,
    records_dir: Path,
    sig_policy: Path,
    log_dir: Path,
) -> list[Invocation]:
    domain = suggestion.domain
    base = [
        "tools/discover_hash_policy.py",
        "--phase0-dir",
        str(records_dir),
        "--domains",
        domain,
        "--discovery-target",
        "sig",
        "--sample-size",
        suggestion.sample_size,
        "--max-candidate-fields",
        suggestion.max_candidate_fields,
        "--policy-json",
        str(sig_policy),
    ]
    if suggestion.stratify_by:
        base.extend(["--stratify-by", suggestion.stratify_by])

    discover_k = suggestion.max_k_discover
    harsh_k = suggestion.max_k_harsh_validate

    if discover_k == harsh_k:
        return [
            Invocation(
                label=f"sig_hash ({domain}, combined, max-k={discover_k})",
                args=tuple(base + ["--max-k", str(discover_k)]),
                log_path=log_dir / f"sig__{domain}__combined.log",
            )
        ]

    # Split exactly like build_join_invocations: a mismatched discover_k vs
    # harsh_k means join runs discover mode at one search depth and
    # validate/harsh at another. Sig must match that split invocation-for-
    # invocation -- previously sig ran ALL policy modes (including discover)
    # in one invocation sized off max(discover_k, harsh_k), so sig's
    # "discover" search silently ran deeper than join's "discover" search
    # whenever the two suggested values differed, invalidating the sig/join
    # convergence check for that mode (see 2026-08 sweep run against
    # identity: validate flagged, discover/harsh did not -- an artifact of
    # this mismatch, not a real signal, since discover_hash_policy.py floors
    # validate mode's effective max-k to len(required_fields) regardless of
    # --max-k, masking the discrepancy there but not in discover/harsh).
    return [
        Invocation(
            label=f"sig_hash ({domain}, discover, max-k={discover_k})",
            args=tuple(
                base
                + [
                    "--max-k",
                    str(discover_k),
                    "--policy-modes",
                    "discover",
                ]
            ),
            log_path=log_dir / f"sig__{domain}__discover.log",
        ),
        Invocation(
            label=f"sig_hash ({domain}, validate+harsh, max-k={harsh_k})",
            args=tuple(
                base
                + [
                    "--max-k",
                    str(harsh_k),
                    "--policy-modes",
                    "validate,harsh",
                ]
            ),
            log_path=log_dir / f"sig__{domain}__validate_harsh.log",
        ),
    ]


def format_command(args: Sequence[str]) -> str:
    command = [sys.executable, *args]
    if os.name == "nt":
        return subprocess.list2cmdline(command)
    return shlex.join(command)


def run_invocation(invocation: Invocation, repo_root: Path) -> InvocationResult:
    invocation.log_path.parent.mkdir(parents=True, exist_ok=True)
    warning_lines: list[str] = []
    exit_code = -1
    error: str | None = None

    try:
        with invocation.log_path.open("w", encoding="utf-8", newline="") as log:
            process = subprocess.Popen(
                [sys.executable, *invocation.args],
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            assert process.stdout is not None
            tee = Tee(sys.stdout, log)
            for line in process.stdout:
                tee.write(line)
                if WARNING_PATTERN.search(line):
                    warning_lines.append(line.strip())
            exit_code = process.wait()
    except Exception as exc:  # Keep one failed domain from ending the sweep.
        error = f"{type(exc).__name__}: {exc}"
        try:
            invocation.log_path.parent.mkdir(parents=True, exist_ok=True)
            invocation.log_path.write_text(
                "ERROR: invocation threw before/without producing output: "
                + error
                + "\n",
                encoding="utf-8",
            )
        except OSError:
            pass

    return InvocationResult(
        label=invocation.label,
        exit_code=exit_code,
        log_path=invocation.log_path,
        warning_lines=tuple(warning_lines),
        error=error,
    )


def _read_selected_fields_by_key(
    diagnostics_dir: Path, glob_pattern: str, domain: str
) -> dict[tuple[str, str, str], str]:
    """Read every matching diagnostics CSV and index selected_fields by
    (domain, policy_mode, search_mode). A domain can have more than one
    matching file (e.g. join's separate discover-only and validate+harsh
    runs), so all matches are merged; a later file's row for the same key
    overwrites an earlier one only if they actually disagree, which would
    itself be worth knowing about but isn't expected in normal operation.
    """
    out: dict[tuple[str, str, str], str] = {}
    for csv_path in sorted(diagnostics_dir.glob(glob_pattern.format(domain=domain))):
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    key = (
                        (row.get("domain") or "").strip(),
                        (row.get("policy_mode") or "").strip(),
                        (row.get("search_mode") or "").strip(),
                    )
                    if not all(key):
                        continue
                    out[key] = (row.get("selected_fields") or "").strip()
        except OSError:
            continue
    return out


def check_sig_join_convergence(diagnostics_dir: Path, domain: str) -> list[str]:
    """Non-blocking, informational only -- mirrors discover_hash_policy.py's
    own _flag_sig_join_convergence(), but sourced from the two tools'
    diagnostics CSVs on disk since sig (discover_hash_policy.py) and join
    (discover_join_policy.py) run as separate subprocesses here and can't
    compare notes in-process. Join and sig keys are typically expected to
    differ; identical results for the same policy_mode/search_mode are worth
    a second look, not treated as a failure.
    """
    sig_by_key = _read_selected_fields_by_key(diagnostics_dir, _SIG_CSV_GLOB, domain)
    join_by_key = _read_selected_fields_by_key(diagnostics_dir, _JOIN_CSV_GLOB, domain)

    lines: list[str] = []
    for key in sorted(set(sig_by_key) & set(join_by_key)):
        dom, policy_mode, search_mode = key
        sig_sel = sig_by_key[key]
        join_sel = join_by_key[key]
        if not sig_sel and not join_sel:
            continue
        if sig_sel == join_sel:
            lines.append(
                f"[discover] WARNING domain={dom} policy_mode={policy_mode} "
                f"search_mode={search_mode} sig and join selected_fields are "
                f"IDENTICAL ({sig_sel}) -- join keys are typically expected to "
                f"differ from sig keys; take a closer look before assuming this "
                f"is correct rather than an artifact of the search's candidate "
                f"pool or a lack of equivalent-but-varying elements to join "
                f"against."
            )
    return lines


def validate_paths(
    repo_root: Path,
    suggestions_csv: Path,
    records_dir: Path,
    join_policy: Path,
    sig_policy: Path,
    skip_join: bool,
    skip_sig: bool,
) -> None:
    checks: list[tuple[str, Path]] = [
        ("RepoRoot", repo_root),
        ("suggestions CSV", suggestions_csv),
        ("records directory", records_dir),
    ]
    if not skip_join:
        checks.extend(
            [
                ("join discovery tool", repo_root / "tools/discover_join_policy.py"),
                ("join policy JSON", join_policy),
            ]
        )
    if not skip_sig:
        checks.extend(
            [
                ("hash discovery tool", repo_root / "tools/discover_hash_policy.py"),
                ("sig policy JSON", sig_policy),
            ]
        )

    missing = [f"{label}: {path}" for label, path in checks if not path.exists()]
    if missing:
        raise FileNotFoundError("required path(s) not found:\n  " + "\n  ".join(missing))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.run and not args.what_if:
        print("No execution mode selected. Use --run or --what-if.\n")
        parse_args(["--help"])
        return 0

    repo_root = args.repo_root.expanduser().resolve()
    exports_root = args.exports_root.expanduser().resolve()
    suggestions_csv = (
        args.suggestions_csv.expanduser().resolve()
        if args.suggestions_csv is not None
        else exports_root / "diagnostics/discovery_param_suggestions.csv"
    )
    records_dir = exports_root / "records"
    join_policy = repo_root / "policies/domain_join_key_policies.json"
    sig_policy = repo_root / "policies/domain_sig_hash_policies.json"
    log_dir = exports_root / "diagnostics/discover_logs"
    diagnostics_dir = exports_root / "diagnostics"

    if args.skip_join and args.skip_sig:
        print("ERROR: --skip-join and --skip-sig leave nothing to run.", file=sys.stderr)
        return 2

    try:
        validate_paths(
            repo_root=repo_root,
            suggestions_csv=suggestions_csv,
            records_dir=records_dir,
            join_policy=join_policy,
            sig_policy=sig_policy,
            skip_join=args.skip_join,
            skip_sig=args.skip_sig,
        )
        suggestions = select_domains(
            load_suggestions(suggestions_csv),
            args.domains,
        )
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if not suggestions_csv.exists():
            print("Run this first:", file=sys.stderr)
            print(
                "  "
                + format_command(
                    [
                        "tools/suggest_discovery_params.py",
                        "--phase0-dir",
                        str(records_dir),
                        "--policy-json",
                        str(join_policy),
                        "--emit-commands",
                    ]
                ),
                file=sys.stderr,
            )
        return 1

    print(f"=== Discovery sweep: {len(suggestions)} domain(s) ===")
    print(f"records:   {records_dir}")
    print(f"join pol:  {join_policy}")
    print(f"sig pol:   {sig_policy}")
    print(f"logs:      {log_dir}")
    print()

    failed: list[str] = []
    warning_summary: list[str] = []
    ran_count = 0

    for suggestion in suggestions:
        print(f"[{suggestion.domain}]")
        try:
            invocations: list[Invocation] = []
            if not args.skip_join:
                invocations.extend(
                    build_join_invocations(
                        suggestion,
                        records_dir,
                        join_policy,
                        log_dir,
                    )
                )
            if not args.skip_sig:
                invocations.extend(
                    build_sig_invocation(
                        suggestion,
                        records_dir,
                        sig_policy,
                        log_dir,
                    )
                )

            for invocation in invocations:
                print(f"  -> {invocation.label}")
                if args.what_if:
                    print(f"     {format_command(invocation.args)}")
                    continue

                ran_count += 1
                result = run_invocation(invocation, repo_root)
                if result.exit_code != 0:
                    detail = f"exit {result.exit_code}"
                    if result.error:
                        detail += f"; {result.error}"
                    print(
                        f"     FAILED ({detail}) -- see {result.log_path}",
                        file=sys.stderr,
                    )
                    failed.append(f"{result.label} ({detail})")

                if not result.log_path.exists():
                    print("     no log file was produced", file=sys.stderr)
                    failed.append(f"{result.label} (no log file produced)")

                warning_summary.extend(
                    f"{result.label}: {line}" for line in result.warning_lines
                )

            if not args.what_if and not args.skip_join and not args.skip_sig:
                convergence_lines = check_sig_join_convergence(
                    diagnostics_dir, suggestion.domain
                )
                for line in convergence_lines:
                    print(f"     {line}")
                warning_summary.extend(
                    f"sig/join convergence check ({suggestion.domain}): {line}"
                    for line in convergence_lines
                )

        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            print(
                f"  DOMAIN-LEVEL FAILURE ({suggestion.domain}): {message}",
                file=sys.stderr,
            )
            failed.append(f"{suggestion.domain} (domain-level: {message})")
        print()

    if args.what_if:
        print("=== WHAT-IF: no commands were executed ===")
        return 0

    print(f"=== SWEEP COMPLETE: {ran_count} invocation(s) ===")

    if failed:
        print("\nFAILED invocations:")
        for item in failed:
            print(f"  {item}")

    if warning_summary:
        print(
            "\nWARNINGS / divergence flags found "
            "(sample-derived candidate may not hold on full population):"
        )
        for item in warning_summary:
            print(f"  {item}")
        warning_path = log_dir / "_warning_summary.txt"
        warning_path.parent.mkdir(parents=True, exist_ok=True)
        warning_path.write_text(
            "\n".join(warning_summary) + "\n",
            encoding="utf-8",
        )
        print(f"\nFull warning summary written to: {warning_path}")
    else:
        print(
            "\nNo [discover] WARNING or "
            "sample_vs_full_diverges=true lines found across any log."
        )

    print(f"\nPer-domain logs: {log_dir}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
