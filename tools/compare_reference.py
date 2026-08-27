#!/usr/bin/env python3
"""tools/compare_reference.py

PR3: standalone orchestration CLI for comparing a reference fingerprint
export against one or more target fingerprint exports (or a target corpus),
producing a stable, deterministic package of consumable outputs.

This module implements NO comparison mathematics of its own. It is a thin
orchestration/output layer over the existing, authoritative comparison
implementation:

  - tools/run_extract_all.py (--seed) builds the reference_bundle.json
    sidecar and the analysis-side pattern outputs (pattern_presence_file.csv,
    domain_patterns.csv) via tools/extractor.py's emit_analysis.
  - tools/bundle_analysis/run_bundle_analysis.py (--compare) computes the
    symmetric shared/reference_only/target_only comparison
    (tools/bundle_analysis/step_compare.py) and its ok/degraded/blocked
    reliability semantics (tools/bundle_analysis/comparison_status.py).

Both are invoked as subprocesses -- the same orchestration convention
run_extract_all.py itself already uses for its own sub-stages -- so there
remains exactly one implementation of comparison semantics.

See docs/reference_comparison_tool.md for the full runbook, CLI reference,
and output-field documentation, including the neutral-terminology note: a
"reference" here is a comparison anchor only, not a standard, approved
content, or a compliance requirement.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from bundle_analysis.common import atomic_write_csv, read_csv_rows  # noqa: E402
from collections import Counter  # noqa: E402
from bundle_analysis.comparison_status import (  # noqa: E402
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_DEGRADED,
    COMPARISON_STATUS_BLOCKED,
    REASON_COMPARISON_INPUT_INVALID,
    REASON_TARGET_DOMAIN_UNAVAILABLE,
    aggregate_comparison_status,
    join_reason_codes,
    split_reason_codes,
)

MANIFEST_FILENAME = "reference_comparison_report.json"
SUMMARY_FILENAME = "reference_comparison_summary.csv"
DETAIL_FILENAME = "reference_comparison_detail.csv"
DIAGNOSTICS_FILENAME = "reference_comparison_diagnostics.json"

# Suffixes checked most-specific first, so `.json` (the generic fallback)
# never masks `.details.json` / `.index.json` / `__fingerprint.json`. Mirrors
# the "Input format priority" rule (CLAUDE.md / docs): *.details.json before
# *.index.json before a bare *.json export; *.legacy.json is never picked up
# implicitly.
_EXPORT_SUFFIXES = (".details.json", ".index.json", "__fingerprint.json", ".legacy.json", ".json")
# Matches tools/extractor.py::_iter_export_files exactly: for a split-export
# pair, the *.index.json file (not *.details.json) is the canonical file_id
# when both exist -- *.details.json is only primary when no index file
# accompanies it. Getting this wrong makes --seed name-match nothing (Codex
# review, PR #466).
_PRIMARY_PRIORITY = ("__fingerprint.json", ".index.json", ".details.json", ".json")

_SUMMARY_FIELDNAMES = [
    "reference_bundle_id",
    "analysis_run_id",
    "target_export_run_id",
    "domain",
    "population_id",
    "comparison_status",
    "comparison_reason_codes",
    "reference_pattern_count",
    "target_pattern_count",
    "shared_count",
    "reference_only_count",
    "target_only_count",
    "union_count",
    "reference_coverage_pct",
    "jaccard",
]

_DETAIL_FIELDNAMES = [
    "reference_bundle_id",
    "analysis_run_id",
    "target_export_run_id",
    "domain",
    "population_id",
    "pattern_id",
    "comparison_class",
]


class CompareReferenceError(RuntimeError):
    """Orchestration-level failure (pre-flight validation, staging, output
    directory conflicts). Distinct from failures inside the underlying
    comparator (tools/bundle_analysis/reference_bundle.py's typed errors,
    surfaced as a nonzero subprocess exit) which are allowed to propagate
    rather than being reclassified here.
    """


# ---------------------------------------------------------------------------
# Export file discovery / staging
# ---------------------------------------------------------------------------


def _export_stem(path: Path) -> str:
    name = path.name
    for suffix in _EXPORT_SUFFIXES:
        if name.lower().endswith(suffix.lower()):
            return name[: -len(suffix)]
    return path.stem


def _sibling_export_files(path: Path) -> List[Path]:
    """Every file alongside `path` that belongs to the SAME export
    representation as `path` -- a split-export .details.json/.index.json
    pair stages together, but never an unrelated *alternate-format*
    representation of the same stem (Codex review, PR #466): a migrated
    directory can retain e.g. both `foo__fingerprint.json` and an obsolete
    `foo.details.json`/`foo.index.json` pair for the same conceptual export.
    tools/extractor.py::_iter_export_files treats those as two distinct
    exports (different file_ids); staging both would let an obsolete
    representation of a chosen file sneak into the corpus as if it were a
    separate export, or double-count a target scored under two
    representations at once. Never includes a *.legacy.json sibling that
    wasn't the path explicitly given.
    """
    stem = _export_stem(path)
    parent = path.parent
    name_lower = path.name.lower()
    if name_lower.endswith(".details.json") or name_lower.endswith(".index.json"):
        candidate_suffixes: Sequence[str] = (".details.json", ".index.json")
    elif name_lower.endswith("__fingerprint.json"):
        candidate_suffixes = ("__fingerprint.json",)
    elif name_lower.endswith(".legacy.json"):
        candidate_suffixes = (".legacy.json",)
    else:
        candidate_suffixes = (".json",)

    found: List[Path] = []
    for suffix in candidate_suffixes:
        candidate = parent / f"{stem}{suffix}"
        if candidate.is_file() and candidate not in found:
            found.append(candidate)
    if path.is_file() and path not in found:
        found.append(path)
    return found


def _pick_primary_export(paths: Sequence[Path]) -> Path:
    """Pick the file run_extract_all.py's own file-discovery would treat as
    the export's canonical `file_id` for a set of sibling files -- fingerprint
    > index (when a split pair) > details-only > plain, matching
    tools/extractor.py::_iter_export_files exactly (index is preferred over
    details whenever both are present).
    """
    for suffix in _PRIMARY_PRIORITY:
        for p in paths:
            lname = p.name.lower()
            if lname.endswith(suffix) and not lname.endswith(".legacy.json"):
                return p
    return paths[0]


def _validate_export_path(label: str, path: Path) -> Path:
    if not path.is_file():
        raise CompareReferenceError(f"{label} not found: {path}")
    if path.name.lower().endswith(".legacy.json"):
        raise CompareReferenceError(
            f"{label} is a *.legacy.json export ({path}); legacy exports are never used, even when named explicitly."
        )
    if not path.name.endswith(".json"):
        # Case-sensitive on purpose: tools/extractor.py::_iter_export_files
        # discovers inputs via the case-sensitive glob exports_dir.glob("*.json"),
        # which never matches e.g. "model.JSON" on a case-sensitive filesystem
        # (Linux). Accepting it here (as a case-insensitive check previously
        # did) would let a file pass pre-flight validation, get staged, and
        # then vanish from the pipeline with zero diagnostic trace -- it
        # never reaches file_metadata.csv at all, so even the
        # missing-target synthesis above can't catch it (Codex review, PR #466).
        raise CompareReferenceError(
            f"{label} does not look like a fingerprint export (expected a lowercase .json extension, "
            f"exactly as tools/extractor.py's discovery glob requires): {path}"
        )
    try:
        with path.open("r", encoding="utf-8") as f:
            json.load(f)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompareReferenceError(f"{label} is not valid JSON: {path} ({exc})") from exc
    return path


def _stage_export(src: Path, staging_dir: Path, seen: Dict[str, Path]) -> List[Path]:
    """Hardlink-or-copy every sibling file for `src` into staging_dir.

    `seen` maps a staged destination filename to the resolved *source* path
    that produced it, so re-staging the exact same source file under a name
    already seen (e.g. the reference happening to also live inside
    --target-dir) is a no-op -- but two genuinely different source files that
    happen to share a destination filename (e.g. --reference /a/model.json
    and an explicitly different --target /b/model.json) raise explicitly
    instead of silently aliasing the second onto the first (Codex review, PR
    #466 -- the earlier destination-name-only dedup silently discarded an
    explicitly requested, distinct target, which then read as "nothing to
    compare" with no indication why).
    """
    staged: List[Path] = []
    for sibling in _sibling_export_files(src):
        resolved_source = sibling.resolve()
        dest = staging_dir / sibling.name
        prior_source = seen.get(sibling.name)
        if prior_source is not None:
            if prior_source == resolved_source:
                staged.append(dest)
                continue
            raise CompareReferenceError(
                f"Filename collision while staging exports: {prior_source} and {resolved_source} "
                f"are different files that would both be staged as {sibling.name!r}. Rename one of "
                "them, or ensure the reference and target(s) don't share export filenames."
            )
        staging_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.link(resolved_source, dest)
        except OSError:
            shutil.copy2(resolved_source, dest)
        seen[sibling.name] = resolved_source
        staged.append(dest)
    return staged


def _discover_primary_export_files(directory: Path) -> List[Path]:
    """Enumerate one primary export file per distinct export (stem) in a
    target corpus directory. Discovery is per-stem, not per-format across the
    whole directory: a corpus containing a mix of *__fingerprint.json,
    *.details.json/*.index.json pairs, and plain *.json exports (e.g. an
    incrementally-migrated corpus) must surface every one of them, not only
    whichever single format happens to be present anywhere in the directory
    (Codex review, PR #466 -- the earlier whole-directory-priority version
    silently dropped every non-fingerprint export the moment even one
    *__fingerprint.json file existed). Within one stem, fingerprint > details
    > plain still applies for discovery's representative-file choice (mirrors
    tools/extractor.py, though the exact file chosen here doesn't need to
    match extractor.py's own file_id -- see _pick_primary_export for where
    that distinction actually matters, for --seed). A *.index.json is only
    skipped when its stem was already claimed by a *.details.json mate --
    tools/extractor.py::_iter_export_files registers a *standalone* index
    file (no details mate) as its own primary export, so this must too
    (Codex review, PR #466 second finding -- the earlier version
    unconditionally skipped every *.index.json, silently losing index-only
    targets). *.legacy.json is never picked up implicitly.
    """
    seen_stems: Dict[str, Path] = {}
    result: List[Path] = []

    def _consider(p: Path) -> None:
        stem = _export_stem(p)
        if stem not in seen_stems:
            seen_stems[stem] = p
            result.append(p)

    for p in sorted(directory.glob("*__fingerprint.json")):
        _consider(p)
    for p in sorted(directory.glob("*.details.json")):
        _consider(p)
    for p in sorted(directory.glob("*.index.json")):
        _consider(p)  # no-op if a details mate already claimed this stem
    for p in sorted(directory.glob("*.json")):
        lname = p.name.lower()
        if lname.endswith(".legacy.json") or lname.endswith(".index.json"):
            continue
        if lname.endswith("__fingerprint.json") or lname.endswith(".details.json"):
            continue
        _consider(p)
    return sorted(result, key=lambda p: p.name.lower())


def stage_comparison_inputs(
    reference: Path,
    targets: Sequence[Path],
    target_dir: Optional[Path],
    staging_dir: Path,
) -> Dict[str, object]:
    """Assemble a single combined exports directory containing the reference
    export plus every requested target export, without mutating any of the
    caller's original files. Returns provenance describing what was staged.
    """
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)

    seen: Dict[str, Path] = {}
    reference_files = _stage_export(reference, staging_dir, seen)
    if not reference_files:
        raise CompareReferenceError(f"Reference export not found: {reference}")
    # Resolved *source* paths the reference actually staged from -- used to
    # recognize "this target-dir file literally IS the reference" (e.g.
    # --target-dir points at a corpus root that also contains the reference
    # itself). Deliberately not name-based: a different file that merely
    # shares the reference's basename is not the same export and must not be
    # silently dropped here (Codex review, PR #466) -- it should either
    # stage normally or hit _stage_export's own explicit collision error.
    reference_source_paths = set(seen.values())

    target_files: List[Path] = []
    for t in targets:
        staged = _stage_export(t, staging_dir, seen)
        if not staged:
            raise CompareReferenceError(f"Target export not found: {t}")
        target_files.extend(staged)

    if target_dir is not None:
        if not target_dir.is_dir():
            raise CompareReferenceError(f"--target-dir is not a directory: {target_dir}")
        corpus_primaries = _discover_primary_export_files(target_dir)
        if not corpus_primaries:
            raise CompareReferenceError(f"--target-dir contains no fingerprint exports: {target_dir}")
        for primary in corpus_primaries:
            if primary.resolve() in reference_source_paths:
                # This literally is the reference file (same resolved source)
                # rediscovered inside the target corpus -- excluded rather
                # than compared against itself. A different file that merely
                # shares its basename is NOT excluded here; _stage_export
                # will stage it normally or raise on a genuine collision.
                continue
            target_files.extend(_stage_export(primary, staging_dir, seen))

    if not target_files:
        raise CompareReferenceError("No target export files resolved (after excluding the reference) -- nothing to compare.")

    return {
        "reference_files": sorted(str(p) for p in reference_files),
        "target_files": sorted({str(p) for p in target_files}),
        # Counted by export stem, not raw staged filename: a split-export
        # target spans two files (.details.json + .index.json) but is one
        # target export.
        "target_file_count": len({_export_stem(p) for p in target_files}),
    }


# ---------------------------------------------------------------------------
# Output directory ownership / overwrite semantics
# ---------------------------------------------------------------------------


def check_out_dir_does_not_contain_inputs(
    out_dir: Path,
    reference: Path,
    targets: Sequence[Path],
    target_dir: Optional[Path],
    auxiliary_inputs: Sequence[Path] = (),
) -> None:
    """--out-dir is unconditionally cleared on every run (see
    prepare_out_dir). If it were the same as, or an ancestor of, ANY input
    path -- the reference, a --target, --target-dir, or an auxiliary input
    file such as --join-policy/--sig-hash-policy/--metadata-file -- that
    clearing would destroy the user's data before either subprocess ever
    reads it -- reachable via --overwrite, or even without it whenever the
    directory happens to already carry this tool's own manifest from an
    unrelated prior run (Codex review, PR #466; auxiliary_inputs added in a
    follow-up finding on the same issue -- the first fix only covered the
    reference/target/target-dir paths). Must be checked before
    prepare_out_dir is ever called, once all input paths are already known
    to exist.
    """
    resolved_out = out_dir.resolve()
    candidates: List[Path] = [reference, *targets, *auxiliary_inputs]
    if target_dir is not None:
        candidates.append(target_dir)
    for candidate in candidates:
        resolved_candidate = candidate.resolve()
        if resolved_out == resolved_candidate or resolved_out in resolved_candidate.parents:
            raise CompareReferenceError(
                f"--out-dir ({resolved_out}) is the same as, or an ancestor of, an input path "
                f"({resolved_candidate}). This tool clears --out-dir on every run, which would "
                "destroy your source data. Choose a --out-dir that does not contain any input "
                "(--reference/--target/--target-dir/--join-policy/--sig-hash-policy/--metadata-file) path."
            )


def prepare_out_dir(out_dir: Path, overwrite: bool) -> None:
    """Deterministic overwrite policy: each invocation cleanly REPLACES
    --out-dir rather than merging into it, so a prior run's rows can never be
    confused with the current run's. --out-dir is treated as owned
    exclusively by this tool -- if it already exists and does not carry this
    tool's own manifest from a prior run, refuse to clear it unless
    --overwrite is passed explicitly, to avoid silently deleting unrelated
    directory contents.
    """
    if out_dir.exists():
        if any(out_dir.iterdir()):
            manifest_path = out_dir / MANIFEST_FILENAME
            if not manifest_path.is_file() and not overwrite:
                raise CompareReferenceError(
                    f"--out-dir already exists and was not produced by a prior run of this tool: {out_dir}. "
                    "This tool always cleanly replaces its output directory rather than merging across runs "
                    "(so a stale prior comparison can never be confused with the current one). "
                    "Pass --overwrite to replace it anyway, or choose an empty directory."
                )
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)


# ---------------------------------------------------------------------------
# Sub-tool invocation (the one place this module shells out)
# ---------------------------------------------------------------------------


def _execute(cmd: Sequence[str]) -> None:
    print(f"[compare_reference] RUN: {' '.join(cmd)}", flush=True)
    subprocess.run(list(cmd), check=True)


def build_run_extract_all_cmd(exports_dir: Path, out_root: Path, reference_file: Path, args: argparse.Namespace) -> List[str]:
    # Recommended usage is an already-established, corpus-discovered
    # --join-policy: discovering a meaningful join-key policy from just a
    # reference plus a handful of targets is not meaningful (see
    # docs/reference_comparison_tool.md). Skip the `discover` stage whenever
    # an existing policy is supplied.
    stages = "flatten,sig_hash,apply,patterns" if args.join_policy else "flatten,sig_hash,discover,apply,patterns"
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_extract_all.py"),
        str(exports_dir),
        "--out-root",
        str(out_root),
        "--seed",
        str(reference_file),
        "--stages",
        stages,
        "--emit-analysis-workers",
        str(args.emit_analysis_workers),
    ]
    if args.domains:
        cmd += ["--domains", args.domains]
    if args.join_policy:
        cmd += ["--join-policy", str(args.join_policy)]
    if args.sig_hash_policy:
        cmd += ["--sig-hash-policy", str(args.sig_hash_policy)]
    if args.skip_sig_hash_missing_policy:
        cmd += ["--skip-sig-hash-missing-policy"]
    if args.allow_sig_hash_join_key:
        cmd += ["--allow-sig-hash-join-key"]
    return cmd


def build_run_bundle_analysis_cmd(analysis_dir: Path, bundle_out_dir: Path, records_dir: Path, args: argparse.Namespace) -> List[str]:
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "bundle_analysis" / "run_bundle_analysis.py"),
        "--analysis-dir",
        str(analysis_dir),
        "--out-dir",
        str(bundle_out_dir),
        "--compare",
        "--purge-view",
        args.purge_view,
        "--min-support-count",
        str(args.min_support_count),
        "--min-support-pct",
        str(args.min_support_pct),
        "--workers",
        str(args.workers),
    ]
    if args.discover_populations:
        cmd += [
            "--min-population-size",
            str(args.min_population_size),
            "--max-population-overlap",
            str(args.max_population_overlap),
            "--min-population-jaccard",
            str(args.min_population_jaccard),
            "--discovery-support-pct",
            str(args.discovery_support_pct),
        ]
    else:
        cmd += ["--no-discover-populations"]
    if args.roles:
        metadata_file = args.metadata_file or (records_dir / "file_metadata.csv")
        cmd += ["--metadata-file", str(metadata_file), "--roles", *args.roles]
    return cmd


# ---------------------------------------------------------------------------
# Output assembly (pure function over already-produced compare_<view>/ output)
# ---------------------------------------------------------------------------


def _read_reference_bundle_domains(analysis_dir: Optional[Path]) -> Dict[str, object]:
    if analysis_dir is None:
        return {}
    path = analysis_dir / "reference_bundle.json"
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _synthesize_missing_target_rows(
    gap_rows: List[Dict[str, str]],
    run_summary_rows: List[Dict[str, str]],
    known_target_export_run_ids: Sequence[str],
    reference_bundle: Dict[str, object],
) -> List[Dict[str, str]]:
    """A target with zero presence rows for a domain (e.g. the domain was
    never observed for that file at all) is invisible to
    run_compare_for_domain's default derivation of "which export_run_ids are
    eligible for this domain" -- it silently produces no row at all, rather
    than a blocked one, unless the caller supplies the full eligible target
    universe up front (see tools/bundle_analysis/step_compare.py's own
    docstring on this). run_bundle_analysis.py's CLI only ever populates
    that eligible-universe parameter as a side effect of --roles filtering,
    which this tool cannot rely on for an ad hoc comparison (governance_role
    is frequently unpopulated for files outside the normal governed corpus).
    So: for every domain this compare run touched, treat any known target
    export_run_id with no row at all as the same TARGET_DOMAIN_UNAVAILABLE
    condition run_compare_for_domain already uses for a widened-eligibility
    "no presence evidence" case (Codex review, PR #466) -- not a new
    classification, the existing one applied where the CLI wiring doesn't
    reach it.
    """
    known = {str(t).strip() for t in known_target_export_run_ids if str(t).strip()}
    if not known:
        return []
    domains = {row.get("domain", "") for row in run_summary_rows if row.get("domain", "")}
    domains |= {row.get("domain", "") for row in gap_rows if row.get("domain", "")}
    reference_domains = reference_bundle.get("domains", {}) if isinstance(reference_bundle.get("domains"), dict) else {}
    reference_bundle_id_fallback = str(reference_bundle.get("reference_bundle_id", ""))

    synthesized: List[Dict[str, str]] = []
    for domain in sorted(domains):
        present = {row.get("export_run_id", "") for row in gap_rows if row.get("domain", "") == domain}
        missing = sorted(known - present)
        if not missing:
            continue
        existing_row = next((r for r in gap_rows if r.get("domain", "") == domain), None)
        if existing_row is not None:
            reference_bundle_id = existing_row.get("reference_bundle_id", "")
            effective_date = existing_row.get("effective_date", "")
            analysis_run_id = existing_row.get("analysis_run_id", "")
            reference_pattern_count = existing_row.get("reference_pattern_count", "")
        else:
            reference_bundle_id = reference_bundle_id_fallback
            effective_date = str(reference_bundle.get("effective_date", ""))
            analysis_run_id = ""
            reference_pattern_count = str(len(reference_domains.get(domain, []))) if reference_domains else ""
        for target_id in missing:
            synthesized.append(
                {
                    "reference_bundle_id": reference_bundle_id,
                    "effective_date": effective_date,
                    "analysis_run_id": analysis_run_id,
                    "domain": domain,
                    "population_id": "",
                    "export_run_id": target_id,
                    "reference_pattern_count": reference_pattern_count,
                    "target_pattern_count": "",
                    "shared_count": "",
                    "reference_only_count": "",
                    "target_only_count": "",
                    "union_count": "",
                    "reference_coverage_pct": "",
                    "jaccard": "",
                    "comparison_status": COMPARISON_STATUS_BLOCKED,
                    "comparison_reason_codes": REASON_TARGET_DOMAIN_UNAVAILABLE,
                    "comparison_detail": "no presence evidence for this domain/target in this comparison run",
                }
            )
    return synthesized


def assemble_outputs(
    bundle_out_dir: Path,
    purge_view: str,
    out_dir: Path,
    reference_file: Path,
    stage_info: Dict[str, object],
    extract_cmd: Sequence[str],
    bundle_cmd: Sequence[str],
    analysis_dir: Optional[Path] = None,
    known_target_export_run_ids: Optional[Sequence[str]] = None,
) -> Dict[str, object]:
    """Reshape run_bundle_analysis.py's compare_<view>/ outputs into the PR3
    consumable package. Every field here is a rename/passthrough/rollup of an
    existing field from file_gap_report.csv / file_gap_detail.csv /
    compare_run_summary.csv / compare_run_status.csv
    (tools/bundle_analysis/step_compare.py) -- no comparison result is
    computed or altered here, EXCEPT for one gap-closing addition: a target
    entirely missing from a domain's rows (see
    _synthesize_missing_target_rows) is surfaced as the existing
    blocked/TARGET_DOMAIN_UNAVAILABLE classification rather than silently
    omitted, when `known_target_export_run_ids` is supplied.
    """
    compare_dir = bundle_out_dir / f"compare_{purge_view}"
    gap_rows = read_csv_rows(compare_dir / "file_gap_report.csv") if (compare_dir / "file_gap_report.csv").is_file() else []
    detail_rows = read_csv_rows(compare_dir / "file_gap_detail.csv") if (compare_dir / "file_gap_detail.csv").is_file() else []
    run_summary_rows = read_csv_rows(compare_dir / "compare_run_summary.csv") if (compare_dir / "compare_run_summary.csv").is_file() else []
    run_status_rows = read_csv_rows(compare_dir / "compare_run_status.csv") if (compare_dir / "compare_run_status.csv").is_file() else []

    synthesized_rows: List[Dict[str, str]] = []
    if known_target_export_run_ids is not None:
        reference_bundle = _read_reference_bundle_domains(analysis_dir)
        synthesized_rows = _synthesize_missing_target_rows(gap_rows, run_summary_rows, known_target_export_run_ids, reference_bundle)
        gap_rows = gap_rows + synthesized_rows

    summary_rows = [
        {
            "reference_bundle_id": row.get("reference_bundle_id", ""),
            "analysis_run_id": row.get("analysis_run_id", ""),
            "target_export_run_id": row.get("export_run_id", ""),
            "domain": row.get("domain", ""),
            "population_id": row.get("population_id", ""),
            "comparison_status": row.get("comparison_status", ""),
            "comparison_reason_codes": row.get("comparison_reason_codes", ""),
            "reference_pattern_count": row.get("reference_pattern_count", ""),
            "target_pattern_count": row.get("target_pattern_count", ""),
            "shared_count": row.get("shared_count", ""),
            "reference_only_count": row.get("reference_only_count", ""),
            "target_only_count": row.get("target_only_count", ""),
            "union_count": row.get("union_count", ""),
            "reference_coverage_pct": row.get("reference_coverage_pct", ""),
            "jaccard": row.get("jaccard", ""),
        }
        for row in gap_rows
    ]
    summary_rows.sort(key=lambda r: (r["domain"], r["population_id"], r["target_export_run_id"]))
    atomic_write_csv(out_dir / SUMMARY_FILENAME, _SUMMARY_FIELDNAMES, summary_rows)

    detail_out_rows = [
        {
            "reference_bundle_id": row.get("reference_bundle_id", ""),
            "analysis_run_id": row.get("analysis_run_id", ""),
            "target_export_run_id": row.get("export_run_id", ""),
            "domain": row.get("domain", ""),
            "population_id": row.get("population_id", ""),
            "pattern_id": row.get("pattern_id", ""),
            "comparison_class": row.get("comparison_class", ""),
        }
        for row in detail_rows
    ]
    detail_out_rows.sort(key=lambda r: (r["domain"], r["population_id"], r["target_export_run_id"], r["pattern_id"]))
    atomic_write_csv(out_dir / DETAIL_FILENAME, _DETAIL_FIELDNAMES, detail_out_rows)

    # No compare_run_status.csv at all means run_bundle_analysis.py never
    # reached the point of recording a real run-level status -- e.g. it
    # failed before --compare's own reference-sidecar handling ran (a
    # missing --metadata-file for --roles, an argument error). That is a
    # failed/unknown run, never "ok": defaulting to ok here would certify a
    # comparison that never actually happened (Codex review, PR #466).
    run_status = (
        run_status_rows[0]
        if run_status_rows
        else {
            "analysis_run_id": "",
            "comparison_status": COMPARISON_STATUS_BLOCKED,
            "comparison_reason_codes": REASON_COMPARISON_INPUT_INVALID,
            "comparison_detail": "no compare_run_status.csv was produced by run_bundle_analysis.py",
            "domains_total": "0",
            "domains_ok": "0",
            "domains_degraded": "0",
            "domains_blocked": "0",
        }
    )
    run_status = dict(run_status)

    if synthesized_rows:
        # run_bundle_analysis.py's own compare_run_status.csv/
        # compare_run_summary.csv rollups can't know about a target this
        # tool discovered was silently missing (see
        # _synthesize_missing_target_rows) -- recompute BOTH the run-level
        # rollup and the per-domain summaries from the merged row set, using
        # the same "blocked beats degraded beats ok, rolled up per domain
        # first" algorithm tools/bundle_analysis/run_bundle_analysis.py's own
        # _write_compare_run_outputs uses. Without this, a synthesized
        # blocked target could be masked by a stale "ok" run status
        # (Codex review, PR #466), or the top-level rollup could say
        # blocked while domain_summaries still showed that same domain as
        # ok with stale counts (Codex review, PR #466 follow-up finding).
        # Synthesis is only ever enabled in single-pass mode (see main()),
        # so every row's population_id is "" here -- safe to rebuild
        # per-domain only, with no population_id dimension to preserve.
        statuses_by_domain: Dict[str, List[str]] = {}
        reasons_by_domain: Dict[str, List[str]] = {}
        for row in gap_rows:
            dom = row.get("domain", "")
            statuses_by_domain.setdefault(dom, []).append(row.get("comparison_status", COMPARISON_STATUS_OK))
            reasons_by_domain.setdefault(dom, []).extend(str(row.get("comparison_reason_codes", "") or "").split("|"))
        domain_level_status = {dom: aggregate_comparison_status(sts) for dom, sts in statuses_by_domain.items()}
        status_counts = Counter(domain_level_status.values())
        run_status["comparison_status"] = aggregate_comparison_status(domain_level_status.values())
        run_status["comparison_reason_codes"] = join_reason_codes(
            code for codes in reasons_by_domain.values() for code in codes
        )
        run_status["domains_total"] = str(len(domain_level_status))
        run_status["domains_ok"] = str(status_counts.get(COMPARISON_STATUS_OK, 0))
        run_status["domains_degraded"] = str(status_counts.get(COMPARISON_STATUS_DEGRADED, 0))
        run_status["domains_blocked"] = str(status_counts.get(COMPARISON_STATUS_BLOCKED, 0))

        domain_summaries = [
            {
                "domain": dom,
                "population_id": "",
                "comparison_status": domain_level_status[dom],
                "comparison_reason_codes": sorted({c for c in reasons_by_domain[dom] if c}),
                "files_scored": str(len(statuses_by_domain[dom])),
                "comparison_ok_count": str(Counter(statuses_by_domain[dom]).get(COMPARISON_STATUS_OK, 0)),
                "comparison_degraded_count": str(Counter(statuses_by_domain[dom]).get(COMPARISON_STATUS_DEGRADED, 0)),
                "comparison_blocked_count": str(Counter(statuses_by_domain[dom]).get(COMPARISON_STATUS_BLOCKED, 0)),
            }
            for dom in sorted(statuses_by_domain)
        ]
    else:
        domain_summaries = [
            {
                "domain": row.get("domain", ""),
                "population_id": row.get("population_id", ""),
                "comparison_status": row.get("comparison_status", ""),
                "comparison_reason_codes": split_reason_codes(row.get("comparison_reason_codes", "")),
                "files_scored": row.get("files_scored", ""),
                "comparison_ok_count": row.get("comparison_ok_count", ""),
                "comparison_degraded_count": row.get("comparison_degraded_count", ""),
                "comparison_blocked_count": row.get("comparison_blocked_count", ""),
            }
            for row in run_summary_rows
        ]
    domain_summaries.sort(key=lambda r: (r["domain"], r["population_id"]))

    non_ok_targets = [
        {
            "target_export_run_id": row.get("export_run_id", ""),
            "domain": row.get("domain", ""),
            "population_id": row.get("population_id", ""),
            "comparison_status": row.get("comparison_status", ""),
            "comparison_reason_codes": split_reason_codes(row.get("comparison_reason_codes", "")),
            "comparison_detail": row.get("comparison_detail", ""),
        }
        for row in gap_rows
        if row.get("comparison_status", COMPARISON_STATUS_OK) != COMPARISON_STATUS_OK
    ]
    non_ok_targets.sort(key=lambda r: (r["domain"], r["population_id"], r["target_export_run_id"]))

    reference_bundle_id = ""
    if gap_rows:
        reference_bundle_id = gap_rows[0].get("reference_bundle_id", "")
    elif run_summary_rows:
        reference_bundle_id = run_summary_rows[0].get("reference_bundle_id", "")

    diagnostics = {
        "reference_bundle_id": reference_bundle_id,
        "analysis_run_id": run_status.get("analysis_run_id", ""),
        "run_comparison_status": run_status.get("comparison_status", COMPARISON_STATUS_OK),
        "run_comparison_reason_codes": split_reason_codes(run_status.get("comparison_reason_codes", "")),
        "run_comparison_detail": run_status.get("comparison_detail", ""),
        "domains_total": run_status.get("domains_total", "0"),
        "domains_ok": run_status.get("domains_ok", "0"),
        "domains_degraded": run_status.get("domains_degraded", "0"),
        "domains_blocked": run_status.get("domains_blocked", "0"),
        "domain_summaries": domain_summaries,
        "target_diagnostics": non_ok_targets,
    }
    (out_dir / DIAGNOSTICS_FILENAME).write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = {
        "tool": "tools/compare_reference.py",
        "reference_export": str(reference_file),
        "reference_bundle_id": reference_bundle_id,
        "target_scope": stage_info,
        "analysis_run_id": diagnostics["analysis_run_id"],
        "purge_view": purge_view,
        "commands": [list(extract_cmd), list(bundle_cmd)],
        "output_files": [SUMMARY_FILENAME, DETAIL_FILENAME, DIAGNOSTICS_FILENAME],
        "aggregate_comparison_status": diagnostics["run_comparison_status"],
    }
    (out_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="compare_reference.py",
        description=(
            "Compare a reference fingerprint export against one or more target fingerprint "
            "exports (or a target corpus). Reuses the existing reference-vs-target comparison "
            "implementation (tools/run_extract_all.py --seed + "
            "tools/bundle_analysis/run_bundle_analysis.py --compare) -- no comparison "
            "mathematics is implemented here. A reference is a comparison anchor only: not a "
            "standard, approved content, or a compliance requirement. "
            "See docs/reference_comparison_tool.md."
        ),
    )
    ap.add_argument("--reference", required=True, type=Path, help="Path to the reference fingerprint export file.")
    target_group = ap.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--target", type=Path, nargs="+", default=None, help="One or more target fingerprint export files.")
    target_group.add_argument("--target-dir", type=Path, default=None, help="Directory containing a target corpus of fingerprint exports.")
    ap.add_argument("--out-dir", required=True, type=Path, help="Output directory for this tool's own artifacts (owned exclusively by this tool -- see --overwrite).")
    ap.add_argument("--overwrite", action="store_true", help="Allow clearing --out-dir even if it wasn't produced by a prior run of this tool.")
    ap.add_argument("--domains", default=None, help="Comma-separated domain list, passed through to run_extract_all.py --domains. Default: infer from the staged exports.")
    ap.add_argument("--join-policy", default=None, type=Path, help="Existing join-key policy JSON (recommended: point at your corpus's already-discovered domain_join_key_policies.v21.json). Skips the discover stage when given.")
    ap.add_argument("--sig-hash-policy", default=None, type=Path, help="sig_hash policy JSON, passed through to run_extract_all.py --sig-hash-policy.")
    ap.add_argument("--skip-sig-hash-missing-policy", action="store_true")
    ap.add_argument("--allow-sig-hash-join-key", action="store_true", help="Allow degraded identity-mode join keys for exploratory comparisons not intended for governance conclusions.")
    ap.add_argument("--metadata-file", default=None, type=Path, help="file_metadata.csv for --roles filtering. Default: the staged run's own records/file_metadata.csv.")
    ap.add_argument("--roles", nargs="+", default=None, help="Restrict target scope to these governance roles (see run_bundle_analysis.py --roles).")
    ap.add_argument("--purge-view", choices=["all", "used"], default="all")
    ap.add_argument("--discover-populations", action="store_true", help="Opt into population-aware mode (see run_bundle_analysis.py --discover-populations). Default: single-pass mode.")
    ap.add_argument("--min-population-size", type=int, default=0)
    ap.add_argument("--max-population-overlap", type=float, default=0.20)
    ap.add_argument("--min-population-jaccard", type=float, default=0.30)
    ap.add_argument("--discovery-support-pct", type=float, default=0.10)
    ap.add_argument("--min-support-count", type=int, default=3)
    ap.add_argument("--min-support-pct", type=float, default=0.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--emit-analysis-workers", type=int, default=4)
    return ap


def _load_known_target_export_run_ids(records_dir: Path, reference_file_id: str) -> Optional[List[str]]:
    """Read the just-produced file_metadata.csv to determine the full staged
    export_run_id universe, minus the reference's own export_run_id -- see
    _synthesize_missing_target_rows for why this tool needs to know this
    independently of run_bundle_analysis.py's own (--roles-filter-only)
    eligible-target derivation. Returns None (meaning "don't synthesize") if
    file_metadata.csv is missing or unreadable, rather than failing the
    whole run over a best-effort enhancement.
    """
    path = records_dir / "file_metadata.csv"
    if not path.is_file():
        return None
    try:
        rows = read_csv_rows(path)
    except (OSError, UnicodeDecodeError):
        return None
    reference_export_run_id = next(
        (row.get("export_run_id", "") for row in rows if row.get("file_id", "") == reference_file_id),
        None,
    )
    all_ids = {row.get("export_run_id", "") for row in rows if row.get("export_run_id", "")}
    if reference_export_run_id:
        all_ids.discard(reference_export_run_id)
    return sorted(all_ids)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    try:
        reference = _validate_export_path("--reference", Path(args.reference).resolve())
        targets: List[Path] = []
        if args.target:
            targets = [_validate_export_path("--target", Path(t).resolve()) for t in args.target]
        target_dir = Path(args.target_dir).resolve() if args.target_dir else None

        out_dir = Path(args.out_dir).resolve()
        auxiliary_inputs = [
            Path(p).resolve()
            for p in (args.join_policy, args.sig_hash_policy, args.metadata_file)
            if p is not None
        ]
        check_out_dir_does_not_contain_inputs(out_dir, reference, targets, target_dir, auxiliary_inputs)
        prepare_out_dir(out_dir, overwrite=args.overwrite)

        staging_dir = out_dir / "staged_exports"
        stage_info = stage_comparison_inputs(reference, targets, target_dir, staging_dir)
        reference_primary = _pick_primary_export([Path(p) for p in stage_info["reference_files"]])
    except CompareReferenceError as exc:
        print(f"[compare_reference][error] {exc}", file=sys.stderr)
        return 2

    extraction_out_root = out_dir / "extraction"
    bundle_out_dir = out_dir / "bundle_analysis"
    records_dir = extraction_out_root / "results" / "records"
    analysis_dir = extraction_out_root / "results" / "analysis"

    extract_cmd = build_run_extract_all_cmd(staging_dir, extraction_out_root, reference_primary, args)
    try:
        _execute(extract_cmd)
    except subprocess.CalledProcessError as exc:
        print(f"[compare_reference][error] run_extract_all.py failed (exit {exc.returncode})", file=sys.stderr)
        return exc.returncode or 1

    # Missing-target synthesis (see _synthesize_missing_target_rows) assumes
    # "every staged export other than the reference" is the eligible target
    # universe. That assumption only holds in the default mode: --roles
    # already makes run_bundle_analysis.py thread its own, role-filtered
    # eligible_export_run_ids through to the comparator (which correctly
    # flags a role-eligible-but-evidence-missing target as blocked on its
    # own), so re-deriving an unfiltered universe here would wrongly flag
    # targets the user explicitly excluded by role as TARGET_DOMAIN_UNAVAILABLE
    # (Codex review, PR #466). --discover-populations groups rows by
    # (domain, population_id), a dimension this synthesis doesn't model.
    # Both are left to the underlying comparator's own eligibility handling.
    known_target_export_run_ids = (
        None
        if (args.roles or args.discover_populations)
        else _load_known_target_export_run_ids(records_dir, reference_primary.name)
    )

    bundle_cmd = build_run_bundle_analysis_cmd(analysis_dir, bundle_out_dir, records_dir, args)
    try:
        _execute(bundle_cmd)
    except subprocess.CalledProcessError as exc:
        # run_bundle_analysis.py records a blocked compare_run_status.csv
        # before re-raising on a totally invalid/schema-incompatible
        # reference sidecar (see tools/bundle_analysis/README.md) -- assemble
        # outputs from whatever it managed to write before surfacing the
        # failure, so "comparison was requested but blocked" is never
        # console-only, then still propagate the nonzero exit.
        try:
            manifest = assemble_outputs(
                bundle_out_dir, args.purge_view, out_dir, reference, stage_info, extract_cmd, bundle_cmd,
                analysis_dir=analysis_dir, known_target_export_run_ids=known_target_export_run_ids,
            )
            print(f"[compare_reference] comparison_status={manifest['aggregate_comparison_status']} (from blocked run)")
        except Exception:
            pass
        print(f"[compare_reference][error] run_bundle_analysis.py failed (exit {exc.returncode})", file=sys.stderr)
        return exc.returncode or 1

    manifest = assemble_outputs(
        bundle_out_dir, args.purge_view, out_dir, reference, stage_info, extract_cmd, bundle_cmd,
        analysis_dir=analysis_dir, known_target_export_run_ids=known_target_export_run_ids,
    )
    print(f"[compare_reference] comparison_status={manifest['aggregate_comparison_status']}")
    print(f"[compare_reference] wrote outputs to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
