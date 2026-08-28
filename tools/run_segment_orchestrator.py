#!/usr/bin/env python3
"""
tools/run_segment_orchestrator.py

Reads segment_manifest.csv and run_registry.csv, executes patterns then bundle
stages for each bundle segment in level order, writes outputs to per-segment
folders under a segments/ root, and updates the registry after each run.

Usage:
    python tools/run_segment_orchestrator.py \\
        --manifest-file segment_manifest.csv \\
        --registry-file run_registry.csv \\
        --records-dir /path/to/results/records \\
        --exports-dir /path/to/exports \\
        --segments-root /path/to/segments \\
        --repo-root /path/to/repo \\
        --join-policy /path/to/domain_join_key_policies.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow import of bundle_analysis package from the same tools/ directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundle_analysis.common import atomic_write_csv, retry_fs_op
from bundle_analysis.name_projection_adapter import annotate_name_target_combined_files, normalize_export_run_id
from build_results_registry import write_results_registry

VALID_COMPARISON_TARGETS = {"config", "name", "both"}

# Maximum destination file handles open simultaneously during preshard.
# Keeps fd usage well below typical OS limits (1024) regardless of segment count.
# Each batch re-streams the source file once, so total passes = ceil(N/batch).
_PRESHARD_BATCH = 64

_CORPUS_PRESHARD_MARKER = ".preshard_complete_corpus"

BI_MERGE_FILES = [
    "membership_matrix.csv",
    "bundles.csv",
    "bundle_dag_nodes.csv",
    "bundle_dag_edges.csv",
    "bundle_dag_differences.csv",
    "pattern_bundle_classification.csv",
    "bundle_membership.csv",
    "file_bundle_classification.csv",
    "bundle_file_membership.csv",
    "scope_registry.csv",
]


# ── CSV helpers ──────────────────────────────────────────────────────────────

def load_manifest(path: Path) -> Dict[str, dict]:
    """Load segment_manifest.csv keyed by segment_id.

    --- trace ---
    reads: `path` -- Path to segment_manifest.csv, from run_orchestrator()'s
        --manifest-file (tools/build_segment_manifest.py's MANIFEST_FIELDNAMES output:
        segment_id, parent_segment_id, segment_level, unit_system, governance_role,
        client_label, discipline_label, business_center_label, run_type, file_count,
        ...).
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: Dict[segment_id, row-dict]; consumed by run_orchestrator() as `manifest`,
        passed to build_run_plan()/validate_membership_against_manifest() and read per
        segment (segment_level, governance_role, etc.) throughout _run_one_segment().
    """
    manifest: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            sid = row.get("segment_id", "").strip()
            if sid:
                manifest[sid] = row
    return manifest


def load_registry(path: Path) -> List[dict]:
    """Load run_registry.csv as a list of row dicts.

    --- trace ---
    reads: `path` -- Path to run_registry.csv, from run_orchestrator()'s
        --registry-file (tools/build_segment_manifest.py's REGISTRY_FIELDNAMES output:
        segment_id, parent_segment_id, run_type, population_hash,
        conformance_reference_mode, output_folder, status, last_run_utc, notes,
        segment_purpose, segment_label).
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: list[dict], one row per registered segment; consumed by run_orchestrator()
        as `registry`, mutated in place by _run_one_segment() (status/last_run_utc/
        notes) under registry_lock and persisted via write_registry_atomic() after
        each segment.
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_membership(path: Path) -> Dict[str, List[str]]:
    """Load segment_membership.csv grouped by segment_id -> sorted export_run_ids.

    Replaces the old segment_manifest.csv `export_run_ids` pipe-delimited column,
    which could exceed spreadsheet cell limits for large populations.

    --- trace ---
    reads: `path` -- Path to segment_membership.csv, from run_orchestrator()'s
        --membership-file (default: sibling of --manifest-file); reads its
        segment_id/export_run_id columns (tools/build_segment_manifest.py's
        MEMBERSHIP_FIELDNAMES output).
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: Dict[segment_id, sorted list[export_run_id]]; consumed by
        run_orchestrator() as `membership`, passed to
        validate_membership_against_manifest() and used throughout
        (_run_one_segment()'s export_run_ids, preshard's segment_plans allowed_ids,
        dry-run's file_count) as the authoritative per-segment file population.
    """
    membership: Dict[str, List[str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        grouped: Dict[str, List[str]] = {}
        for row in csv.DictReader(f):
            sid = (row.get("segment_id") or "").strip()
            eid = (row.get("export_run_id") or "").strip()
            if sid and eid:
                grouped.setdefault(sid, []).append(eid)
        for sid, eids in grouped.items():
            membership[sid] = sorted(eids)
    return membership


def write_registry_atomic(path: Path, rows: List[dict]) -> None:
    """Write registry rows atomically via temp-file + replace.

    --- trace ---
    reads: `path`, `rows` -- caller-supplied; _run_one_segment() calls this with
        registry_file and the shared in-memory `registry` list (mutated in place under
        registry_lock immediately before this call).
    calls: none (stdlib csv.DictWriter).
    thresholds: none.
    returns: None; no-op if `rows` is empty. Writes run_registry.csv's exact current
        in-memory field set (fieldnames = list(rows[0].keys())) atomically (temp file +
        Path.replace()). Called by _run_one_segment() after every segment's status
        update, so run_registry.csv reflects live progress rather than only a final
        batch write.
    """
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 "Z"-suffixed string.

    --- trace ---
    reads: system clock (datetime.now(timezone.utc)).
    calls: none (stdlib datetime).
    thresholds: the format string "%Y-%m-%dT%H:%M:%SZ" is a hardcoded literal.
    returns: str; consumed by _run_one_segment() (registry last_run_utc) and
        run_orchestrator() (run_start_utc/run_end_utc for the run summary).
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_worker_split(
    total_budget: Optional[int] = None,
    headroom: int = 2,
    segment_workers: Optional[int] = None,
) -> tuple[int, int]:
    """Returns (segment_workers, domain_workers) whose product approximates
    available logical cores minus headroom.

    If segment_workers is None (auto mode), both values are derived from the
    budget using a sqrt-biased split, favoring segment-level concurrency since
    segments are more I/O-independent than domain workers within one segment's
    bundle stage.

    If segment_workers is given (explicit --workers N), domain_workers is
    solved as budget // segment_workers so the bundle-stage pool stays
    coordinated to the same CPU budget instead of defaulting to
    run_bundle_analysis.py's own fixed default of 4 — which would otherwise let
    total concurrency grow unbounded as N grows (N x 4 instead of ~N).

    --- trace ---
    reads: `total_budget` -- Optional int, from main() only when --workers is not
        "auto" (the explicit-N path calls this with segment_workers=N and
        total_budget left None, so it's derived from os.cpu_count() internally);
        `headroom` -- int, default 2 (not overridden by any caller in this file);
        `segment_workers` -- Optional int, from main()'s parsed --workers value in the
        explicit-N branch.
    calls: os.cpu_count() (stdlib).
    thresholds: `headroom = 2` (default param); the hardcoded (4, 4) fallback when
        os.cpu_count() returns None; the sqrt-biased split constant `0.8`
        (`round(math.sqrt(total_budget) * 0.8)`) for the auto (segment_workers=None)
        case.
    returns: (segment_workers, domain_workers) tuple of ints; consumed by main() to set
        args.workers/args.bundle_workers, for both the "auto" (--workers auto) and
        explicit-N (coordinate bundle_workers to the same CPU budget) cases.
    """
    if total_budget is None:
        cpu_count = os.cpu_count()
        if not cpu_count:
            # os.cpu_count() returned None (restricted/containerized environment) —
            # fall back to the existing hardcoded default of 4 for both values.
            if segment_workers is None:
                return 4, 4
            total_budget = 4
        else:
            total_budget = max(1, cpu_count - headroom)

    if segment_workers is not None:
        domain_workers = max(1, total_budget // max(1, segment_workers))
        return segment_workers, domain_workers

    domain_workers = max(1, round(math.sqrt(total_budget) * 0.8))
    segment_workers = max(1, total_budget // domain_workers)
    return segment_workers, domain_workers


def _write_run_summary(
    segments_root: Path,
    run_start_utc: str,
    run_end_utc: str,
    total_elapsed_s: int,
    segment_results: List[Dict],
    workers: int,
    bundle_workers: int,
    workers_auto: bool,
) -> Path:
    """Write run_summary.txt to segments_root atomically (temp + replace).

    --- trace ---
    reads: `segments_root` -- Path, from run_orchestrator()'s --segments-root;
        `run_start_utc`/`run_end_utc` -- str, from run_orchestrator()'s utc_now_iso()
        calls bracketing the run; `total_elapsed_s` -- int, run_orchestrator()'s
        wall-clock elapsed; `segment_results` -- list[dict], accumulated by
        run_orchestrator() from every _run_one_segment() return value plus
        skip/exception entries (keys: segment_id/status/level/files/prepare_s/
        patterns_s/bundle_s/bi_merge_s/total_s/worker_id/patterns_top5/failure_note);
        `workers`/`bundle_workers`/`workers_auto` -- from run_orchestrator()'s args.
    calls: none (str.format()/print-style formatting only).
    thresholds: none named -- column widths, "top 3"/"top-5" slicing
        (`sorted_by_pat[:3]`, `[:5]`), and the 120-char failure-note truncation
        (`[:120]`) are hardcoded literals.
    returns: Path to the written run_summary.txt (segments_root/run_summary.txt,
        written atomically via a ".tmp" sibling + Path.replace()); consumed by
        run_orchestrator() only to print the path, not read back.
    """
    out_path = segments_root / "run_summary.txt"
    tmp_path = segments_root / "run_summary.txt.tmp"

    n_complete = sum(1 for r in segment_results if r.get("status") == "complete")
    n_failed = sum(1 for r in segment_results if r.get("status") == "failed")
    n_skipped = sum(1 for r in segment_results if r.get("status") == "skipped")
    segments_run = n_complete + n_failed

    total_min = total_elapsed_s / 60.0

    # Per-segment timing table
    col_w = max((len(r.get("segment_id", "")) for r in segment_results), default=30)
    col_w = max(col_w, 30)
    header_fmt = f"{{:<{col_w}}}  {{:>3}}  {{:>5}}  {{:>7}}  {{:>8}}  {{:>6}}  {{:>8}}  {{:>5}}  {{}}"
    row_fmt    = f"{{:<{col_w}}}  {{:>3}}  {{:>5}}  {{:>7}}  {{:>8}}  {{:>6}}  {{:>8}}  {{:>5}}  {{}}"

    seg_lines: List[str] = [
        header_fmt.format("segment", "lvl", "files", "prepare", "patterns", "bundle", "bi_merge", "total", "status"),
    ]
    for r in sorted(segment_results, key=lambda x: (-x.get("patterns_s", 0), x.get("segment_id", ""))):
        sid     = r.get("segment_id", "")
        lvl     = r.get("level", 0)
        files   = r.get("files", 0)
        prep    = r.get("prepare_s", 0)
        pat     = r.get("patterns_s", 0)
        bun     = r.get("bundle_s", 0)
        mrg     = r.get("bi_merge_s", 0)
        tot     = r.get("total_s", 0)
        status  = "✓" if r.get("status") == "complete" else "✗"
        seg_lines.append(
            row_fmt.format(sid, lvl, files, f"{prep}s", f"{pat}s", f"{bun}s", f"{mrg}s", f"{tot}s", status)
        )

    # Failed segments block
    failed_lines: List[str] = []
    for r in segment_results:
        if r.get("status") == "failed":
            note = r.get("failure_note", "").split("\n")[0][:120]
            failed_lines.append(
                f"  {r.get('segment_id', ''):<{col_w}}  {note}"
            )

    # Top-5 patterns timing — sub-breakdown only for the 3 slowest by patterns_s
    sorted_by_pat = sorted(
        [r for r in segment_results if r.get("patterns_s", 0) > 0],
        key=lambda x: -x.get("patterns_s", 0),
    )
    top3_sids = {r["segment_id"] for r in sorted_by_pat[:3]}
    timing_blocks: List[str] = []
    for r in sorted_by_pat[:5]:
        sid = r.get("segment_id", "")
        pat_s = r.get("patterns_s", 0)
        top5 = r.get("patterns_top5", [])
        timing_blocks.append(f"[segment: {sid}  patterns={pat_s}s]")
        for ln in top5:
            if sid in top3_sids:
                timing_blocks.append(f"  {ln}")
            else:
                # Truncate to domain name + total only (drop sub-breakdown fields)
                parts = ln.split()
                short_parts = [p for p in parts if p.startswith("domain=") or p.startswith("elapsed=")]
                timing_blocks.append(f"  {' '.join(short_parts)}")

    # Totals
    total_prep_s    = sum(r.get("prepare_s", 0) for r in segment_results)
    total_pat_s     = sum(r.get("patterns_s", 0) for r in segment_results)
    total_bun_s     = sum(r.get("bundle_s", 0) for r in segment_results)
    total_mrg_s     = sum(r.get("bi_merge_s", 0) for r in segment_results)
    total_work_s    = sum(r.get("total_s", 0) for r in segment_results)
    avg_pat         = total_pat_s // segments_run if segments_run > 0 else 0
    avg_bun         = total_bun_s // segments_run if segments_run > 0 else 0
    avg_mrg         = total_mrg_s // segments_run if segments_run > 0 else 0
    parallelism_eff = total_work_s / total_elapsed_s if total_elapsed_s > 0 else 0.0

    lines: List[str] = [
        "Revit Fingerprint — Run Summary",
        "================================",
        f"run_start_utc : {run_start_utc}",
        f"run_end_utc   : {run_end_utc}",
        f"total_elapsed : {total_elapsed_s}s ({total_min:.1f} min)",
        f"workers       : {workers}",
        f"worker_split  : segment_workers={workers} domain_workers={bundle_workers}"
        f" (mode={'auto' if workers_auto else 'explicit'})",
        f"segments_run  : {segments_run}",
        f"  complete    : {n_complete}",
        f"  failed      : {n_failed}",
        f"  skipped     : {n_skipped}",
        "",
        "── Per-segment timing ──────────────────────────────────────────────────────",
    ]
    lines.extend(seg_lines)

    if failed_lines:
        lines.append("")
        lines.append("── Failed segments ─────────────────────────────────────────────────────────")
        lines.extend(failed_lines)

    if timing_blocks:
        lines.append("")
        lines.append("── Patterns top-5 domains (slowest segments only, top 3 segments by patterns time) ──")
        lines.extend(timing_blocks)

    lines += [
        "",
        "── Totals ──────────────────────────────────────────────────────────────────",
        f"total_prepare   : {total_prep_s:>6}s",
        f"total_patterns  : {total_pat_s:>6}s  (avg {avg_pat}s/segment)",
        f"total_bundle    : {total_bun_s:>6}s  (avg {avg_bun}s/segment)",
        f"total_bi_merge  : {total_mrg_s:>6}s  (avg {avg_mrg}s/segment)",
        f"total_work      : {total_work_s:>6}s  (sum of all segment times, not wall time)",
        f"wall_time       : {total_elapsed_s:>6}s  ({total_min:.1f} min)",
        f"parallelism_eff : {parallelism_eff:.2f}×   (total_work / wall_time)",
        "",
    ]

    segments_root.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text("\n".join(lines), encoding="utf-8")
    tmp_path.replace(out_path)
    return out_path


# ── Subprocess helpers ────────────────────────────────────────────────────────

def run_step(cmd: List[str]) -> subprocess.CompletedProcess:
    """Run a subprocess step, capturing stderr, raising on non-zero exit.

    --- trace ---
    reads: `cmd` -- list[str], a subprocess argv; no caller in this file currently
        invokes run_step() directly (run_step_capture()/run_step_log() are used
        instead throughout _run_one_segment()/run_orchestrator()) -- retained as a
        simple raising variant.
    calls: subprocess.run() (stdlib, check=True).
    thresholds: none.
    returns: subprocess.CompletedProcess; raises subprocess.CalledProcessError on
        non-zero exit (check=True).
    """
    return subprocess.run(cmd, check=True, capture_output=False, text=True)


def run_step_capture(cmd: List[str], cwd: Optional[str] = None) -> tuple[int, str, str]:
    """Run a subprocess step, return (returncode, last_20_lines_stderr, full_stderr).

    --- trace ---
    reads: `cmd` -- list[str] subprocess argv; `cwd` -- Optional[str] working
        directory; no caller in this file currently invokes run_step_capture()
        directly (run_step_log() is used throughout instead) -- retained as a
        capture-without-file-logging variant.
    calls: subprocess.run() (stdlib, capture_output=True).
    thresholds: `-20` (last-20-lines tail, hardcoded literal, matching
        run_step_log()'s own convention).
    returns: (returncode, tail (last 20 stderr lines joined), full stderr); never
        raises on non-zero exit (no check=True).
    """
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    stderr_lines = (result.stderr or "").splitlines()
    tail = "\n".join(stderr_lines[-20:]) if stderr_lines else ""
    return result.returncode, tail, result.stderr or ""


def run_step_log(
    cmd: List[str],
    log_path: Path,
    cwd: Optional[str] = None,
) -> tuple[int, str, str]:
    """Run subprocess writing all output (stdout+stderr) to log_path.
    Returns (returncode, last_20_lines, full_output).

    --- trace ---
    reads: `cmd` -- list[str] subprocess argv, from _run_one_segment()'s per-step
        command lists (extract_cmd/name_patterns_cmd/bundle_cmd/name_bundle_cmd);
        `log_path` -- Path, one of
        out_root/{patterns,name_patterns,bundle,bundle_name}.log; `cwd` --
        Optional[str], always str(repo_root) at every call site in this file.
    calls: subprocess.run() (stdlib, stdout+stderr merged to `log_path`).
    thresholds: `-20` (last-20-lines tail, hardcoded literal).
    returns: (returncode, tail (last 20 lines of the combined log), full log content);
        consumed by _run_one_segment() to decide step_failed/failure_notes for each of
        the 4 subprocess steps, and to scan for "[patterns_timing]"-prefixed lines
        after the patterns step.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8", errors="replace") as log_f:
        result = subprocess.run(
            cmd, stdout=log_f, stderr=subprocess.STDOUT,
            text=True, cwd=cwd,
        )
    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    lines = content.splitlines()
    tail = "\n".join(lines[-20:])
    return result.returncode, tail, content


# ── Record helpers ────────────────────────────────────────────────────────────

def _preshard_one_shard(
    shard_file: Path,
    segment_plans: Dict[str, Dict],
    force: bool,
) -> tuple[str, int, int]:
    """Process one corpus shard file, fan out to all segment shard dirs.
    Returns (shard_name, files_written, files_skipped).

    --- trace ---
    reads: `shard_file` -- one Path under records_dir/identity_items_by_domain/, from
        _preshard_corpus_records()'s ThreadPoolExecutor.submit() loop over every *.csv
        shard; `segment_plans` -- Dict[sid, plan] built by run_orchestrator() (each
        plan: sid/segment_records_dir/allowed_ids/status), passed through unchanged;
        `force` -- bool, from run_orchestrator()'s args.force/--force-preshard.
    calls: csv.reader()/csv.writer() (stdlib) only; no other module function.
    thresholds: _PRESHARD_BATCH = 64 (module constant, l.49: max simultaneous open
        destination file handles per fan-out batch, so total open fds stay well below
        typical OS limits regardless of segment count); the shard file's own
        "export_run_id" column is looked up by name (`header.index("export_run_id")`),
        not a hardcoded column index.
    returns: (shard_name, files_written, files_skipped) tuple; consumed by
        _preshard_corpus_records() only to accumulate `total_written` for its own
        summary print -- no other function reads the individual per-shard result.
    """
    if not shard_file.is_file() or shard_file.suffix != ".csv":
        return shard_file.name, 0, 0

    # Determine which segments need this shard.
    # Skip only completed segments; pending/failed segments always get fresh
    # inputs so retries without --force don't run against stale data.
    segments_to_write = {}
    for sid, plan in segment_plans.items():
        if not force and plan.get("status") == "complete":
            continue
        seg_shard_dir = plan["segment_records_dir"] / "identity_items_by_domain"
        dst = seg_shard_dir / shard_file.name
        segments_to_write[sid] = (plan, seg_shard_dir, dst)

    if not segments_to_write:
        return shard_file.name, 0, len(segment_plans)

    # Read header once; eid_col is stable across all batches.
    with shard_file.open("r", encoding="utf-8-sig", newline="") as _hf:
        header = next(csv.reader(_hf), None)
    if not header:
        return shard_file.name, 0, len(segment_plans)
    eid_col = header.index("export_run_id") if "export_run_id" in header else None
    if eid_col is None:
        return shard_file.name, 0, len(segment_plans)

    # Ensure all destination shard dirs exist before batching.
    seen_dirs: set = set()
    for sid, (plan, seg_shard_dir, dst) in segments_to_write.items():
        if seg_shard_dir not in seen_dirs:
            seg_shard_dir.mkdir(parents=True, exist_ok=True)
            seen_dirs.add(seg_shard_dir)

    # Fan out in batches so at most _PRESHARD_BATCH destination handles are
    # open simultaneously.  Each batch re-streams the shard file once.
    seg_items = list(segments_to_write.items())
    for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
        batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])

        # Build one-to-many lookup scoped to this batch.
        id_to_targets: Dict[str, List] = {}
        for sid, (plan, seg_shard_dir, dst) in batch.items():
            for eid in plan["allowed_ids"]:
                id_to_targets.setdefault(eid, []).append(sid)

        writers: Dict[str, Any] = {}
        handles: Dict[str, Any] = {}
        try:
            for sid, (plan, seg_shard_dir, dst) in batch.items():
                fh = dst.open("w", newline="", encoding="utf-8")
                handles[sid] = fh
                w = csv.writer(fh)
                w.writerow(header)
                writers[sid] = w

            with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header row
                for row in reader:
                    if len(row) <= eid_col:
                        continue
                    eid = row[eid_col].strip()
                    for sid in id_to_targets.get(eid, ()):
                        if sid in writers:
                            writers[sid].writerow(row)
        finally:
            for fh in handles.values():
                fh.close()

    return shard_file.name, len(segments_to_write), len(segment_plans) - len(segments_to_write)


def _preshard_corpus_records(
    records_dir: Path,
    segment_plans: Dict[str, Dict],
    force: bool,
) -> None:
    """
    Stream each corpus source file once and fan out rows to per-segment
    destination files keyed by export_run_id.  Segments whose destination
    files already exist and are non-empty are skipped when force=False.

    --- trace ---
    reads: `records_dir` -- Path, corpus-level results/records/ (run_orchestrator()'s
        --records-dir); reads records.csv/file_metadata.csv directly and
        identity_items_by_domain/*.csv via _preshard_one_shard(); `segment_plans` --
        Dict built by run_orchestrator() (see _preshard_one_shard()); `force` -- bool,
        from run_orchestrator()'s args.force.
    calls: csv.reader()/csv.writer() (stdlib, for records.csv/file_metadata.csv);
        _preshard_one_shard() (via ThreadPoolExecutor, one submission per shard file);
        concurrent.futures.ThreadPoolExecutor/as_completed() (stdlib).
    thresholds: _PRESHARD_BATCH = 64 (module constant, shared with
        _preshard_one_shard()); `shard_pool_size = max(1, min(8, _PRESHARD_BATCH //
        max_seg))` (hardcoded `8` cap on concurrent shard-processing threads, derived
        from _PRESHARD_BATCH and segment count); records.csv/file_metadata.csv's
        "export_run_id" column looked up by name, matching _preshard_one_shard()'s
        convention.
    returns: None; writes each segment's records.csv/file_metadata.csv/
        identity_items_by_domain/*.csv under segment_plans[sid]["segment_records_dir"],
        plus ".preshard_complete" and identity_items_by_domain/.complete marker files --
        but only for segments in `segments_to_write` (force=True, or status !=
        "complete"), so an already-complete segment's existing sharded records are
        left untouched. Consumed by run_orchestrator() as a side-effecting step before
        the per-segment executor runs; the resulting ".preshard_complete" markers are
        read back by _write_segment_records().
    """
    # csv.field_size_limit() converts to a C long; on Windows CPython the C long
    # is 32-bit so sys.maxsize overflows.  Cap at 2^31-1 which fits everywhere.
    try:
        csv.field_size_limit(2 ** 31 - 1)
    except OverflowError:
        csv.field_size_limit(2 ** 30)

    t0 = time.monotonic()

    # Segments actually being (re)processed this pass. Skip only completed
    # segments; pending/failed segments always get fresh inputs so retries
    # without --force don't run against stale data. Computed once and reused
    # below for marker-stamping too -- a segment excluded here must NOT have
    # its .preshard_complete / identity_items_by_domain/.complete markers
    # touched, since those markers are what _write_segment_records() trusts to
    # skip re-copying. Stamping them for every planned segment regardless of
    # whether records.csv/shards were actually written let a segment whose
    # registry status was stale-"complete" end up "marked done" with an empty
    # records dir (imperial_container_2014 step=bundle incident).
    segments_to_write = {
        sid: plan for sid, plan in segment_plans.items()
        if force or plan.get("status") != "complete"
    }

    # ── records.csv and file_metadata.csv ─────────────────────────────────────
    for fname in ("records.csv", "file_metadata.csv"):
        src = records_dir / fname
        if not src.is_file():
            continue
        if not segments_to_write:
            print(f"[preshard] {fname} → 0 segments written, {len(segment_plans)} skipped")
            continue

        # Read header once; eid_col is stable across all batches.
        with src.open("r", encoding="utf-8-sig", newline="") as _hf:
            header: Optional[List[str]] = next(csv.reader(_hf), None)
        if not header:
            continue
        eid_col = header.index("export_run_id") if "export_run_id" in header else None
        if eid_col is None:
            continue

        # Fan out in batches so at most _PRESHARD_BATCH destination handles are
        # open simultaneously.  Each batch re-streams the source file once.
        seg_items = list(segments_to_write.items())
        for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
            batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])

            # Build one-to-many lookup scoped to this batch.
            id_to_plans: Dict[str, List] = {}
            for plan in batch.values():
                for eid in plan["allowed_ids"]:
                    id_to_plans.setdefault(eid, []).append(plan)

            writers: Dict[str, Any] = {}
            handles: Dict[str, Any] = {}
            try:
                for sid, plan in batch.items():
                    dst = plan["segment_records_dir"] / fname
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    fh = dst.open("w", newline="", encoding="utf-8")
                    handles[sid] = fh
                    w = csv.writer(fh)
                    w.writerow(header)
                    writers[sid] = w

                with src.open("r", encoding="utf-8-sig", newline="") as src_f:
                    reader = csv.reader(src_f)
                    next(reader, None)  # skip header row
                    for row in reader:
                        if len(row) <= eid_col:
                            continue
                        eid = row[eid_col].strip()
                        plans = id_to_plans.get(eid)
                        if not plans:
                            continue
                        for plan in plans:
                            writers[plan["sid"]].writerow(row)
            finally:
                for fh in handles.values():
                    fh.close()

        print(f"[preshard] {fname} → {len(segments_to_write)} segments written, "
              f"{len(segment_plans)-len(segments_to_write)} skipped (already exist)")

    # ── identity_items_by_domain/ shards ──────────────────────────────────────
    corpus_shard_dir = records_dir / "identity_items_by_domain"
    if corpus_shard_dir.is_dir():
        shard_files = sorted(f for f in corpus_shard_dir.iterdir() if f.is_file() and f.suffix == ".csv")
        # Cap concurrency so total open handles ≤ _PRESHARD_BATCH:
        # each worker opens one handle per segment, so workers = BATCH // segments.
        max_seg = max(1, len(segment_plans))
        shard_pool_size = max(1, min(8, _PRESHARD_BATCH // max_seg)) if shard_files else 1

        total_written = 0
        with ThreadPoolExecutor(max_workers=shard_pool_size) as executor:
            futures = {
                executor.submit(_preshard_one_shard, shard_file, segment_plans, force): shard_file
                for shard_file in shard_files
            }
            for future in as_completed(futures):
                name, written, skipped = future.result()
                total_written += written

        # Write .complete markers only for segments actually (re)processed this
        # pass -- a segment excluded from segments_to_write (registry status
        # already "complete") keeps whatever marker it already has rather than
        # having a fresh "ok" stamped over a shard dir this pass never wrote to.
        for plan_entry in segments_to_write.values():
            seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
            if seg_shard_dir.is_dir():
                (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")

        print(
            f"[preshard] identity_items shards → {len(shard_files)} shards processed, "
            f"{total_written} segment×shard files written",
            flush=True,
        )

    # Write per-segment completion markers only for segments actually
    # (re)processed this pass (see segments_to_write comment above). Done
    # after all source files and shards so a partial run (exception before
    # this point) leaves no markers, meaning the next run re-processes those
    # segments from scratch.
    for plan_entry in segments_to_write.values():
        plan_entry["segment_records_dir"].mkdir(parents=True, exist_ok=True)
        (plan_entry["segment_records_dir"] / ".preshard_complete").write_text("ok", encoding="utf-8")

    elapsed = int(time.monotonic() - t0)
    print(f"[preshard] complete elapsed={elapsed}s", flush=True)


def _write_segment_records(
    records_dir: Path,
    segment_records_dir: Path,
    allowed_ids: set,
) -> None:
    """
    Copy records.csv and file_metadata.csv from corpus records_dir into the
    segment records dir, filtered to the segment's export_run_ids.

    Also copies filtered identity_items shards from
    records_dir/identity_items_by_domain/ into
    segment_records_dir/identity_items_by_domain/ so that emit_analysis can
    load identity_items for synopsis label resolution.

    Missing source files are skipped silently — patterns stage will simply see
    an empty (or absent) input and the guard will surface the failure cleanly.

    --- trace ---
    reads: `records_dir` -- corpus records dir; `segment_records_dir` -- this
        segment's own results/records/ dir, from _run_one_segment()'s Step 1;
        `allowed_ids` -- set of export_run_ids for this segment, from
        _run_one_segment()'s `export_run_ids` (membership.get(sid, [])). Also checks
        `segment_records_dir / ".preshard_complete"` and whether
        `segment_records_dir/records.csv` actually exists.
    calls: none (stdlib csv.DictReader/DictWriter only) -- this is the per-segment
        row-by-row fallback path used only when _preshard_corpus_records() did NOT
        already write this segment's inputs.
    thresholds: none named -- the "trust the marker only if records.csv is also
        present" defense-in-depth check (`preshard_marker_valid`) is inline control
        flow, not a named constant.
    returns: None; writes segment_records_dir/{records.csv,file_metadata.csv} and
        segment_records_dir/identity_items_by_domain/*.csv (plus a .complete marker),
        each filtered to rows whose export_run_id is in `allowed_ids` -- but only for
        files/rows not already written by a valid preshard marker. Missing source
        files are skipped silently (see the docstring above): the patterns stage
        surfaces the resulting empty/absent input as its own failure via
        _build_patterns_missing_notes().
    """
    preshard_marker = segment_records_dir / ".preshard_complete"
    # Defense in depth: trust the marker only if records.csv is actually present
    # alongside it. _preshard_corpus_records() now only stamps this marker for
    # segments it actually wrote to, but a marker/reality mismatch from any other
    # cause (manual cleanup, interrupted write, older data) must not cause this
    # step to silently skip regenerating a segment's records -- that's exactly
    # what let imperial_container_2014 reach run_bundle_analysis.py with no
    # records.csv on disk despite both completion markers reading "ok".
    preshard_marker_valid = preshard_marker.is_file() and (segment_records_dir / "records.csv").is_file()
    for fname in ("records.csv", "file_metadata.csv"):
        src = records_dir / fname
        if not src.is_file():
            continue
        dst = segment_records_dir / fname
        if preshard_marker_valid:
            continue  # preshard already wrote this segment's inputs
        with src.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = [r for r in reader if r.get("export_run_id", "").strip() in allowed_ids]
        with dst.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    # Copy filtered identity_items shards so synopsis formatter has behavioral
    # parameters at segment emit time. Without this, _load_identity_items_by_record
    # returns {} for every domain and all synopsis-resolvable patterns fall through
    # to modal or fallback.
    corpus_shard_dir = records_dir / "identity_items_by_domain"
    if corpus_shard_dir.is_dir():
        seg_shard_dir = segment_records_dir / "identity_items_by_domain"
        seg_shard_dir.mkdir(parents=True, exist_ok=True)
        for shard_file in sorted(corpus_shard_dir.iterdir()):
            if not shard_file.is_file() or not shard_file.suffix == ".csv":
                continue
            dst_shard = seg_shard_dir / shard_file.name
            if preshard_marker_valid:
                continue  # preshard already wrote this segment's inputs
            with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = list(reader.fieldnames or [])
                rows = [
                    r for r in reader
                    if r.get("export_run_id", "").strip() in allowed_ids
                ]
            if not rows:
                continue
            with dst_shard.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        # Write completion marker so partial runs are detectable
        (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")


def _filter_name_key_csv_to_segment(
    name_key_results_csv: Path,
    out_csv: Path,
    allowed_ids: set,
) -> int:
    """Filter a corpus-wide name_key_results.csv (tools/apply_name_key_policy.py output,
    computed once for the whole corpus -- there is no per-segment re-parse of raw JSON,
    unlike the join_hash "patterns" step below) down to one segment's file population, so
    tools/generate_name_key_patterns.py re-clusters name-identity patterns scoped to just
    this segment -- the name-projection analog of run_extract_all.py's
    --filter-export-run-ids for the config/join_hash "patterns" step.

    name_key_results.csv's `export_file` column is the raw *.details.json/*.index.json
    basename PR1 saw on disk, not necessarily the canonical export_run_id a segment's
    export_run_ids.txt uses (tools/bundle_analysis/name_projection_adapter.py's
    normalize_export_run_id() documents why those differ for a split-export pair).
    Membership is tested against the normalized id first so a segment's export_run_ids.txt
    actually matches split-export rows; each row's own export_file value is left
    unmodified in the output -- stage_name_projection_analysis_dir() normalizes it again
    downstream when building bundle-pipeline input, so re-normalizing here would be
    redundant, not incorrect, but keeping the raw value is what the filter's one job
    (membership, not transformation) calls for.

    If the normalized id isn't in allowed_ids, the raw (un-normalized) export_file is also
    tried before excluding the row. normalize_export_run_id() can't distinguish a genuine
    split-export pair from a details-only export with no sibling *.index.json file --
    tools/extractor.py's _iter_export_files() keeps the *.details.json name itself as the
    canonical export_run_id in that case (there is no *.index.json to rewrite to), so
    blindly normalizing every *.details.json row would silently drop every row for that
    export from the segment (PR #390 review). allowed_ids is this segment's own real
    membership list, not a heuristic guess, so trying the raw id against it is safe.

    Returns the number of rows written.

    --- trace ---
    reads: `name_key_results_csv` -- Path to the corpus-wide name_key_results.csv
        (tools/apply_name_key_policy.py's output, e.g. from --name-key-results-csv),
        passed in by _run_one_segment()'s Step 2b; `out_csv` -- this segment's
        results/name_key/name_key_results.csv Path; `allowed_ids` -- this segment's
        export_run_ids set.
    calls: nested _in_segment() (per row); normalize_export_run_id()
        (tools/bundle_analysis/name_projection_adapter.py, imported at module top), via
        _in_segment().
    thresholds: none named -- membership is tested against `allowed_ids` itself (this
        segment's real population), not a hardcoded list.
    returns: int rows written; raises FileNotFoundError if `name_key_results_csv` is
        missing. Writes `out_csv` with the same fieldnames as the corpus-wide input,
        filtered to rows whose (normalized-or-raw) export_file is in `allowed_ids`.
        Consumed by _run_one_segment(), which then invokes
        tools/generate_name_key_patterns.py --comparison-target name against this
        filtered file.
    """
    if not name_key_results_csv.is_file():
        raise FileNotFoundError(f"--name-key-results-csv not found: {name_key_results_csv}")

    def _in_segment(raw_export_file: str) -> bool:
        """Test whether one name_key_results.csv row belongs to this segment, trying
        the normalized export_run_id first and the raw export_file second.

        --- trace ---
        reads: `raw_export_file` -- one row's raw `export_file` cell value; closes
            over `allowed_ids` (the enclosing _filter_name_key_csv_to_segment()'s
            parameter).
        calls: normalize_export_run_id() (tools/bundle_analysis/name_projection_adapter.py).
        thresholds: none.
        returns: bool; consumed by the enclosing function's row filter (list
            comprehension).
        """
        if not raw_export_file:
            return False
        if normalize_export_run_id(raw_export_file) in allowed_ids:
            return True
        return raw_export_file in allowed_ids

    with name_key_results_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [r for r in reader if _in_segment((r.get("export_file", "") or "").strip())]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


# ── Diagnostic helpers ────────────────────────────────────────────────────────

def _build_patterns_missing_notes(
    sid: str,
    out_root: Path,
    records_dir: Path,
    patterns_stderr: str,
) -> str:
    """Build a diagnostic failure message when patterns exits 0 but writes no output.

    --- trace ---
    reads: `sid` -- segment_id, for the message header; `out_root` -- this segment's
        output root, to locate export_run_ids.txt; `records_dir` -- this segment's
        results/records/ dir, to locate records.csv/file_metadata.csv;
        `patterns_stderr` -- the captured stderr/stdout content from the patterns
        subprocess (run_step_log()'s third return value), passed in by
        _run_one_segment().
    calls: none (file reads + csv.reader() only).
    thresholds: `[WARN extract_all]` (hardcoded literal substring used to filter
        relevant warning lines out of patterns_stderr); `-10` (last-10-warning-lines
        slice).
    returns: str diagnostic message (export_run_ids.txt id count, records.csv/
        file_metadata.csv first export_run_id, relevant [WARN] lines); consumed by
        _run_one_segment() as `failure_notes` when the patterns step exits 0 but
        pattern_presence_file.csv was never written -- distinguishing a silent
        zero-records-matched failure from a genuine subprocess error.
    """
    parts = [
        f"step=patterns returncode=0 but pattern_presence_file.csv was not written.",
        f"segment={sid}",
        "emit_analysis was skipped — most likely because no records matched the export_run_id filter.",
        "",
    ]

    ids_file = out_root / "export_run_ids.txt"
    if ids_file.is_file():
        ids = [l.strip() for l in ids_file.read_text(encoding="utf-8").splitlines() if l.strip()]
        parts.append(f"export_run_ids.txt: {len(ids)} IDs")
        if ids:
            parts.append(f"  first 3: {ids[:3]}")
    else:
        parts.append(f"export_run_ids.txt NOT FOUND at {ids_file}")

    records_csv = records_dir / "records.csv"
    if records_csv.is_file():
        with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.reader(f)
            header = next(rdr, [])
            first_row = next(rdr, [])
        row_dict = dict(zip(header, first_row)) if first_row else {}
        first_eid = row_dict.get("export_run_id", "<column missing>")
        parts.append(f"records.csv first export_run_id: {first_eid!r}")
    else:
        parts.append(f"records.csv NOT FOUND at {records_csv}")

    meta_csv = records_dir / "file_metadata.csv"
    if meta_csv.is_file():
        with meta_csv.open("r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.reader(f)
            header = next(rdr, [])
            first_row = next(rdr, [])
        row_dict = dict(zip(header, first_row)) if first_row else {}
        first_eid = row_dict.get("export_run_id", "<column missing>")
        parts.append(f"file_metadata.csv first export_run_id: {first_eid!r}")
    else:
        parts.append(f"file_metadata.csv NOT FOUND at {meta_csv}")

    # Surface WARN lines from patterns stderr (run_extract_all.py warnings)
    warn_lines = [ln for ln in patterns_stderr.splitlines() if "[WARN extract_all]" in ln]
    if warn_lines:
        parts.append("")
        parts.append("patterns stderr warnings:")
        parts.extend(f"  {ln}" for ln in warn_lines[-10:])

    return "\n".join(parts)


# ── BI merge ─────────────────────────────────────────────────────────────────

def _active_domains_from_presence_csv(analysis_dir: Path) -> Optional[frozenset]:
    """Return the set of domain names present in pattern_presence_file.csv, or None on failure.

    Mirrors the domain-discovery logic in run_bundle_analysis.py so the merge
    uses exactly the same domain set that the bundle step processed.
    Returns None (not an empty frozenset) when the file is absent or contains no
    domains, so callers fall back to unfiltered behaviour rather than writing
    empty combined files.

    --- trace ---
    reads: `analysis_dir` -- this segment's results/analysis/ dir, from
        _run_one_segment()'s BI-merge step; reads
        `analysis_dir/pattern_presence_file.csv`'s `domain` column.
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: frozenset[str] of domain names, or None if the file is absent or has no
        domain rows; consumed by _run_one_segment() as `active_domains`, passed to
        merge_bi_outputs() to restrict the config-leg BI merge to genuinely active
        domains (excluding stale per-domain folders from an earlier, larger-population
        run).
    """
    presence_csv = analysis_dir / "pattern_presence_file.csv"
    if not presence_csv.is_file():
        return None
    with presence_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        domains = frozenset(
            r.get("domain", "").strip() for r in reader if r.get("domain", "").strip()
        )
    return domains if domains else None


def _active_domains_from_name_patterns(name_patterns_dir: Path) -> Optional[frozenset]:
    """Same purpose as _active_domains_from_presence_csv(), but for the name-projection
    pattern shape (tools/generate_name_key_patterns.py's domain_patterns.csv has no
    pattern_presence_file.csv equivalent -- see DECISIONS.md D-037 for the schema diff).

    Unlike _active_domains_from_presence_csv(), an empty-but-present domain_patterns.csv
    is a legitimate, expected outcome for the name projection (a segment whose files don't
    intersect any of the 25 eligible domains -- see DECISIONS.md D-037's "what this PR
    does not attempt"), not a signal to fall back to "unfiltered." This function therefore
    returns `frozenset()` (not `None`) when the file exists but has zero domain rows, so
    merge_bi_outputs() excludes every domain subfolder instead of treating None-as-unfiltered
    and resurrecting stale per-domain output left over from a previous run of this segment
    under a different (larger) population. `None` is reserved for "the file is missing" --
    a genuinely different condition (the name-patterns step never ran or failed).

    --- trace ---
    reads: `name_patterns_dir` -- this segment's results/name_key/patterns/name/ dir,
        from _run_one_segment()'s name-leg BI-merge step; reads
        `name_patterns_dir/domain_patterns.csv`'s `domain` column
        (tools/generate_name_key_patterns.py's output schema).
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: frozenset[str] (possibly empty) if the file exists, or None if it's
        missing; consumed by _run_one_segment() as `active_domains_name`, passed to
        merge_bi_outputs() for the name-leg BI merge. The empty-vs-None distinction
        (see the docstring above) is load-bearing: empty means "exclude every domain
        folder", None means "file missing, treat as unfiltered".
    """
    patterns_csv = name_patterns_dir / "domain_patterns.csv"
    if not patterns_csv.is_file():
        return None
    with patterns_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return frozenset(
            r.get("domain", "").strip() for r in reader if r.get("domain", "").strip()
        )


def _segment_has_name_leg_output(out_root: Path, run_type: str) -> bool:
    """Whether this segment's name-projection leg (step 2b/3c, plus 3b/BI-merge-name for a
    "bundle" segment) has already completed at least once. emit_name_target_provenance()
    (tools/bundle_analysis's --comparison-target name path) always writes
    bundle_provenance.csv on a successful run, even when the segment's name-target pattern
    set comes back empty -- so its presence is a reliable "bundle name leg already ran"
    marker, independent of run_registry.csv's single whole-segment `status` column, which
    has no notion of per-leg completion. Used so a segment already marked complete under a
    config-only run (or, for pattern_name_fragmentation.csv specifically, one already marked
    complete before Step 1 Part A existed) isn't skipped once the operator later asks for
    --comparison-target name/both -- see PR #390 review, and PR #476 review (Step 1 Part A's
    fragmentation artifacts weren't originally included in this check, so an
    already-complete segment would silently never produce them without --force).

    bundle_provenance.csv lives at bundle_analysis/name_all/ (the flat, single-path-segment
    BI-facing output location -- see run_bundle_analysis_for_target()'s docstring), not
    under the internal bundle_analysis/name/ staging path. pattern_name_fragmentation.csv is
    Step 3c's own output, gated identically to Step 2b (not run_type-gated -- a "reference"
    segment gets it too, unlike Step 3b's bundle_provenance.csv, which only a "bundle"
    segment ever produces at all).

    --- trace ---
    reads: `out_root` -- this segment's output root; checks
        `out_root/results/analysis/pattern_name_fragmentation.csv` for existence always, and
        (only when `run_type == "bundle"`) also
        `out_root/results/bundle_analysis/name_all/bundle_provenance.csv`; `run_type` --
        caller's own reg_row["run_type"], since the bundle_provenance.csv requirement only
        applies to a "bundle" segment (a "reference" segment never produces Step 3b output
        regardless of comparison_target).
    calls: none (Path.is_file()).
    thresholds: none.
    returns: bool; consumed by run_orchestrator() (dry-run and live-run skip-check
        loops) to decide `already_satisfied` for a segment whose registry status is
        already "complete" but comparison_target requests the name leg.
    """
    fragmentation_ok = (out_root / "results" / "analysis" / "pattern_name_fragmentation.csv").is_file()
    if run_type != "bundle":
        return fragmentation_ok
    bundle_ok = (out_root / "results" / "bundle_analysis" / "name_all" / "bundle_provenance.csv").is_file()
    return fragmentation_ok and bundle_ok


def merge_bi_outputs(bundle_analysis_dir: Path, active_domains: Optional[frozenset] = None) -> dict:
    """Pre-merge per-domain bundle analysis CSVs into single combined files for Power BI.

    active_domains: when provided, only subfolders whose name is in this set are
    merged.  Pass the set derived from pattern_presence_file.csv so that stale
    domain folders left over from earlier runs are excluded.

    When a filename has no current candidates (active_domains excludes every existing
    folder, or none exist at all), any pre-existing `{stem}_combined.csv` from a previous
    run is deleted rather than left in place -- otherwise a rerun that legitimately finds
    nothing (e.g. a segment whose active domain set has genuinely gone from non-empty to
    empty) would leave Power BI reading stale bundle data as if it were current (PR #390
    review).

    --- trace ---
    reads: `bundle_analysis_dir` -- this segment's results/bundle_analysis/all/ (config
        leg) or results/bundle_analysis/name_all/ (name leg), from _run_one_segment();
        `active_domains` -- Optional[frozenset], from
        _active_domains_from_presence_csv()/_active_domains_from_name_patterns();
        globs `bundle_analysis_dir/*/<filename>` for each name in BI_MERGE_FILES.
    calls: none (stdlib csv.DictReader/atomic_write_csv() from
        tools/bundle_analysis/common.py, imported at module top).
    thresholds: BI_MERGE_FILES (module constant, l.53-64: the 10 per-domain filenames
        merged into `{stem}_combined.csv`); the "_population_discovery"/
        "_population_runs" substring exclusions (hardcoded literals) filtering out
        non-domain subfolders from the glob.
    returns: dict[filename, {"files_merged": int, "rows_written": int}]; consumed by
        _run_one_segment() only to log totals. Deletes a stale `{stem}_combined.csv`
        when no current candidates exist (rather than leaving it in place), and skips
        (with a WARN print) any candidate file whose header doesn't match the first
        file's header.
    """
    if not bundle_analysis_dir.is_dir():
        return {}

    result: Dict[str, dict] = {}
    for filename in BI_MERGE_FILES:
        stem = Path(filename).stem
        out_path = bundle_analysis_dir / f"{stem}_combined.csv"

        candidates = [
            p for p in bundle_analysis_dir.glob(f"*/{filename}")
            if "_population_discovery" not in str(p)
            and "_population_runs" not in str(p)
            and (active_domains is None or p.parent.name in active_domains)
        ]
        if not candidates:
            if out_path.is_file():
                out_path.unlink()
            continue

        header: Optional[List[str]] = None
        all_rows: List[Dict[str, str]] = []
        files_merged = 0
        for csv_path in sorted(candidates):
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                file_header = list(reader.fieldnames or [])
                rows = [
                    {str(k): "" if v is None else str(v) for k, v in row.items()}
                    for row in reader
                ]
            if not file_header:
                # Truly empty file — no header at all; skip without counting
                continue
            if header is None:
                header = file_header
            elif file_header != header:
                print(
                    f"[WARN orchestrator] bi_merge header mismatch in {csv_path} "
                    f"(expected {header}, got {file_header}) — skipping",
                    flush=True,
                )
                continue
            all_rows.extend(rows)
            files_merged += 1

        if header is None:
            if out_path.is_file():
                out_path.unlink()
            continue

        atomic_write_csv(out_path, header, all_rows)
        result[filename] = {"files_merged": files_merged, "rows_written": len(all_rows)}

    return result


# ── Core orchestration ────────────────────────────────────────────────────────

def build_run_plan(
    manifest: Dict[str, dict],
    registry: List[dict],
    segment_filter: Optional[str],
    force: bool,
) -> List[tuple[dict, dict]]:
    """
    Return ordered list of (registry_row, manifest_row) pairs for bundle segments,
    sorted by segment_level asc then segment_id asc.
    Segments to skip are excluded; dry-run callers handle skip annotation separately.

    --- trace ---
    reads: `manifest` -- load_manifest()'s return value; `registry` --
        load_registry()'s return value, reads each row's run_type/segment_id;
        `segment_filter` -- unused by this function's own body (the --segment CLI
        filter is applied later, by run_orchestrator() itself, not here); `force` --
        also unused by this function's own body (also applied later by
        run_orchestrator()).
    calls: nested sort_key() (via list.sort()).
    thresholds: `{"bundle", "reference"}` (hardcoded literal set: which run_type
        values are even eligible to appear in the plan).
    returns: List[(registry_row, manifest_row)] tuples, ordered by (segment_level asc,
        segment_id asc); consumed by run_orchestrator() as `plan`, then filtered again
        by --segment/--force/status before being split into `plan_to_run` vs. skipped.
    notes: `segment_filter`/`force` are accepted parameters this function's own body
        never reads -- both are applied later downstream (run_orchestrator()'s
        per-row loop), so a reader relying only on this function's signature would
        wrongly assume filtering happens here.
    """
    run_rows = [r for r in registry if r.get("run_type", "").strip() in {"bundle", "reference"}]

    def sort_key(row: dict) -> tuple:
        """Sort key: (segment_level from the manifest, segment_id), defaulting level
        to 0 on a missing/malformed value.

        --- trace ---
        reads: `row` -- one registry row; closes over `manifest` (the enclosing
            build_run_plan()'s parameter) to look up the matching manifest row's
            segment_level.
        calls: none.
        thresholds: `0` -- the fallback segment_level on ValueError/TypeError
            (hardcoded literal).
        returns: (int, str) tuple; consumed by `run_rows.sort(key=sort_key)`.
        """
        sid = row.get("segment_id", "")
        mrow = manifest.get(sid, {})
        try:
            level = int(mrow.get("segment_level", 0))
        except (ValueError, TypeError):
            level = 0
        return (level, sid)

    run_rows.sort(key=sort_key)

    plan: List[tuple[dict, dict]] = []
    for reg_row in run_rows:
        sid = reg_row.get("segment_id", "").strip()
        mrow = manifest.get(sid, {})
        plan.append((reg_row, mrow))
    return plan


def validate_membership_against_manifest(
    plan: List[tuple[dict, dict]],
    membership: Dict[str, List[str]],
) -> List[str]:
    """Return one error string per segment where segment_membership.csv disagrees
    with segment_manifest.csv's file_count/population_hash for that segment_id.

    Guards against a stale or mismatched segment_membership.csv silently driving
    a segment's export_run_ids.txt/preshard population — e.g. build_segment_manifest.py
    interrupted after replacing segment_manifest.csv/run_registry.csv but before
    replacing segment_membership.csv, or a custom --membership-file pointing at
    the wrong sidecar. A mismatch here means population_hash/file_count on the
    manifest row describe a different population than the membership rows
    actually loaded, which could mark a segment complete for the wrong file set.

    --- trace ---
    reads: `plan` -- build_run_plan()'s return value, from run_orchestrator(); reads
        each manifest_row's file_count/population_hash; `membership` --
        load_membership()'s return value, reads each segment_id's export_run_id list.
    calls: hashlib.sha1() (stdlib, to recompute population_hash for comparison).
    thresholds: none named -- the comparison is a direct equality check against the
        manifest row's own file_count/population_hash values (not a separate constant
        table).
    returns: list[str] error messages (empty if everything agrees); consumed by
        run_orchestrator(), which aborts the entire run (prints to stderr, returns 1)
        before any segment is processed if this list is non-empty -- refusing to run
        against a stale/mismatched segment_membership.csv.
    """
    errors: List[str] = []
    for reg_row, mrow in plan:
        sid = reg_row.get("segment_id", "").strip()
        if not sid:
            continue
        ids = membership.get(sid, [])
        expected_count = (mrow.get("file_count") or "").strip()
        if expected_count and str(len(ids)) != expected_count:
            errors.append(
                f"segment={sid}: segment_membership.csv has {len(ids)} export_run_id(s) "
                f"but segment_manifest.csv file_count={expected_count}"
            )
            continue
        expected_hash = (mrow.get("population_hash") or "").strip()
        if expected_hash:
            actual_hash = hashlib.sha1("|".join(sorted(ids)).encode()).hexdigest()
            if actual_hash != expected_hash:
                errors.append(
                    f"segment={sid}: segment_membership.csv population_hash={actual_hash} "
                    f"does not match segment_manifest.csv population_hash={expected_hash}"
                )
    return errors


def _clear_stale_name_all_before_run(out_root: Path, run_type: str, comparison_target: str, log) -> None:
    """Clear this segment's stale name-leg BI-facing output before any step of this run
    begins -- not just before step 3b, and not only inside
    run_bundle_analysis_for_target()'s own upfront clear. A failure in step 2b
    (name-pattern generation) or step 3 (config bundle, which gates step 3b even under
    comparison_target=both) skips step 3b entirely, so run_bundle_analysis_for_target()
    is never invoked at all and its own clear never runs -- without this, Power BI would
    keep reading name_all/ from an old successful run even though this run is recorded
    as failed (PR review, #391, second round).

    Only fires for segments this call actually intends to (re)run with name-leg work --
    already-complete segments are filtered out of plan_to_run before _run_one_segment()
    is ever invoked, so their still-current name_all/ is never touched by this function.

    --- trace ---
    reads: `out_root` -- this segment's output root; `run_type` -- from the registry
        row ("bundle"/"reference"/etc.); `comparison_target` -- from
        run_orchestrator()'s args.comparison_target ("config"/"name"/"both"); `log` --
        the per-segment log closure from _run_one_segment().
    calls: retry_fs_op() (tools/bundle_analysis/common.py, imported at module top),
        wrapping shutil.rmtree().
    thresholds: `run_type == "bundle" and comparison_target in ("name", "both")` --
        inline condition, not a named constant; the target path
        `out_root/results/bundle_analysis/name_all` is a hardcoded relative path.
    returns: None; deletes `name_all/` if present and the condition holds. Called once
        at the very start of _run_one_segment(), before Step 1, specifically so a
        failure in step 2b or step 3 (which would otherwise skip step 3b's own
        upfront clear entirely) still leaves this segment's stale name-leg BI output
        removed rather than silently stale.
    """
    if run_type == "bundle" and comparison_target in ("name", "both"):
        stale_name_all = out_root / "results" / "bundle_analysis" / "name_all"
        if stale_name_all.is_dir():
            log(f"[orchestrator]   clearing stale {stale_name_all} before name-leg regeneration")
            retry_fs_op(shutil.rmtree, str(stale_name_all))


def _run_one_segment(
    idx: int,
    total: int,
    reg_row: dict,
    mrow: dict,
    membership: Dict[str, List[str]],
    records_dir: Path,
    exports_dir: Path,
    segments_root: Path,
    repo_root: Path,
    join_policy: Path,
    skip_bi_merge: bool,
    registry: List[dict],
    reg_index: Dict[str, int],
    registry_file: Path,
    manifest_file: Path,
    results_registry_file: Path,
    registry_lock: threading.Lock,
    counters: Dict[str, object],
    counters_lock: threading.Lock,
    worker_id: int,
    bundle_workers: int,
    comparison_target: str = "config",
    name_key_results_csv: Optional[Path] = None,
) -> Dict:
    """Process one segment. Returns result dict.

    comparison_target="config" (default) is byte-identical to this function before PR4 --
    every new code path below is gated on comparison_target in {"name", "both"} and adds no
    new file writes, subprocess calls, or log lines otherwise. When enabled, it adds a
    parallel name-projection leg (tools/bundle_analysis, --comparison-target name) alongside
    the existing join_hash leg: re-cluster this segment's slice of a corpus-wide
    name_key_results.csv (tools/apply_name_key_policy.py, computed once up front -- see
    _filter_name_key_csv_to_segment()'s docstring for why no per-segment JSON re-parse is
    needed), then bundle-mine it into results/bundle_analysis/name_all/, mirroring
    results/bundle_analysis/all/ for the config leg but using join_key_name_identity as the
    join instead of join_hash.

    --- trace ---
    reads: `reg_row` -- one run_registry.csv row (segment_id/output_folder/run_type),
        from build_run_plan()'s plan; `mrow` -- the matching segment_manifest.csv row
        (segment_level); `membership` -- load_membership()'s Dict, indexed by sid for
        export_run_ids; `records_dir`/`exports_dir`/`segments_root`/`repo_root`/
        `join_policy` -- Paths, from run_orchestrator()'s args, passed through
        unchanged; `skip_bi_merge` -- from args.skip_bi_merge; `registry`/`reg_index`/
        `registry_file`/`manifest_file`/`results_registry_file` -- shared state for
        the registry-update block; `registry_lock`/`counters`/`counters_lock` --
        threading.Lock/dict shared across every worker in the ThreadPoolExecutor;
        `worker_id`/`bundle_workers` -- for logging and the bundle subprocess's
        --workers flag; `comparison_target`/`name_key_results_csv` -- from
        args.comparison_target/args.name_key_results_csv.
    calls: _clear_stale_name_all_before_run(); _write_segment_records();
        run_step_log() (x5: patterns, bundle, and -- gated on comparison_target --
        name patterns, name-fragmentation (Step 1 Part A), name bundle);
        _filter_name_key_csv_to_segment();
        _build_patterns_missing_notes(); _active_domains_from_presence_csv();
        _active_domains_from_name_patterns(); merge_bi_outputs();
        annotate_name_target_combined_files()
        (tools/bundle_analysis/name_projection_adapter.py); retry_fs_op() (via
        shutil.rmtree, for the name-bundle stale-output clear); utc_now_iso();
        write_registry_atomic(); write_results_registry()
        (tools/build_results_registry.py, imported at module top); nested log().
    thresholds: none named beyond the module-level VALID_COMPARISON_TARGETS check
        (enforced earlier, in main()) -- this function's own step gating is
        `run_type == "bundle"` / `comparison_target in ("name", "both")` inline
        conditions, not named constants.
    returns: dict (segment_id/status/files/level/prepare_s/patterns_s/bundle_s/
        bi_merge_s/total_s/worker_id/patterns_top5/failure_note); consumed by
        run_orchestrator()'s ThreadPoolExecutor future-collection loop, appended to
        `segment_results` (feeds _write_run_summary()) and used to update `counters`.
        Also has the side effect (under registry_lock) of mutating the shared
        `registry` list in place and persisting it via write_registry_atomic()/
        write_results_registry() before returning -- so run_registry.csv/
        results_registry.csv reflect this segment's outcome immediately, not only
        after the whole run completes.
    notes: (mechanical-extraction risk) `step_failed`/`failure_notes` accumulate
        through 8 sequential try/except-guarded steps (clear_stale_name_all, prepare,
        patterns, patterns_name, patterns_name_fragmentation, bundle, bundle_name,
        bi_merge/bi_merge_name), each gated on `step_failed is None` from the previous step --
        sequential-control-flow-as-policy: which steps actually execute for a given
        segment depends on run_type and comparison_target, evaluated fresh at each
        gate, not declared as a single up-front plan. `registry` (the list) and
        `counters` (the dict) are both caller-owned mutable state shared across every
        concurrent worker thread and mutated in place under their respective locks --
        a per-function reader would need to also read run_orchestrator() to know
        these mutations are thread-safe only because of that locking discipline.
    """
    sid = reg_row.get("segment_id", "").strip()
    output_folder = reg_row.get("output_folder", "").strip()
    out_root = segments_root / output_folder

    try:
        level = int(mrow.get("segment_level", 0))
    except (ValueError, TypeError):
        level = 0

    export_run_ids = sorted(membership.get(sid, []))
    file_count = len(export_run_ids)
    run_type = reg_row.get("run_type", "bundle").strip()

    print(
        f"\n[orchestrator] ── segment={sid} ({idx}/{total}) level={level} files={file_count} [worker={worker_id}] ──",
        flush=True,
    )

    log_path = out_root / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    step_failed: Optional[str] = None
    failure_notes: str = ""
    notes_parts: List[str] = []
    patterns_timing_lines: List[str] = []
    t_start = time.monotonic()
    t_prepare = 0
    t_patterns = 0
    t_bundle: Optional[int] = None
    t_merge: Optional[int] = None
    t_patterns_name: Optional[int] = None
    t_bundle_name: Optional[int] = None
    t_merge_name: Optional[int] = None
    elapsed = 0

    with log_path.open("w", encoding="utf-8", errors="replace") as log_f:
        def log(msg: str) -> None:
            """Write one line to this segment's run.log and flush immediately.

            --- trace ---
            reads: `msg` -- caller-supplied string; closes over `log_f` (the
                enclosing _run_one_segment()'s open file handle for out_root/run.log).
            calls: none (file write + flush).
            thresholds: none.
            returns: None; used throughout _run_one_segment() as the per-step logging
                call (separate from the `print()` calls that go to console).
            """
            log_f.write(msg + "\n")
            log_f.flush()

        # Caught here (rather than left to propagate) so a persistent lock -- retry_fs_op
        # exhausting every attempt, not just a transient one -- still routes through
        # step_failed and the registry-update block below. An uncaught exception this
        # early would escape _run_one_segment() entirely; the ThreadPoolExecutor caller's
        # generic "unhandled exception" handler only updates in-memory counters/
        # segment_results, never registry_file, leaving the segment's registry row (and
        # bundle_provenance.csv) at whatever they were before this run -- often
        # status=complete from a prior successful run -- so the next non-forced run would
        # skip it forever, silently reading stale Power BI output (PR review, #391, third
        # round).
        try:
            _clear_stale_name_all_before_run(out_root, run_type, comparison_target, log)
        except Exception as exc:
            step_failed = "clear_stale_name_all"
            failure_notes = f"step=clear_stale_name_all error={exc}"

        # Step 1 — Prepare: directories, export_run_ids.txt, segment-level records
        log(f"[orchestrator]   step 1/3 prepare...")
        t_step1_start = time.monotonic()
        try:
            segment_records_dir = out_root / "results" / "records"
            segment_records_dir.mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "bundle_analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "label_synthesis").mkdir(parents=True, exist_ok=True)

            ids_file = out_root / "export_run_ids.txt"
            ids_file.write_text("\n".join(export_run_ids) + "\n", encoding="utf-8")

            # _ensure_latent_purgeable() in run_bundle_analysis.py short-circuits
            # if this file already exists, so a stale one from this segment's
            # prior population would make the "used" view compute purgeability
            # against the old file set. This step only runs for segments that
            # are actually being (re)processed (skipped-complete segments never
            # reach here), so it is always safe to drop the cached file and let
            # it regenerate fresh against the current export_run_ids.
            (segment_records_dir / "latent_purgeable.csv").unlink(missing_ok=True)

            _write_segment_records(records_dir, segment_records_dir, set(export_run_ids))
        except Exception as exc:
            step_failed = "prepare"
            failure_notes = f"step=prepare error={exc}"
        t_prepare = int(time.monotonic() - t_step1_start)
        log(f"[orchestrator]   step 1/3 prepare elapsed={t_prepare}s")

        # Step 2 — Patterns stage
        # --records-dir points at corpus records so build_label_population (run internally
        # by run_extract_all) reads the full population, not just this segment's subset.
        # --label-synth-dir points at corpus label_synthesis so emit_analysis picks up the
        # LLM cache and curator annotations built in Run B without rebuilding per segment.
        if step_failed is None:
            log(f"[orchestrator]   step 2/3 patterns...")
            corpus_label_synth_dir = records_dir.parent / "label_synthesis"
            extract_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_extract_all.py"),
                str(exports_dir),
                "--out-root", str(out_root),
                "--stages", "patterns",
                "--records-dir", str(records_dir),
                "--label-synth-dir", str(corpus_label_synth_dir),
                "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
                "--join-policy", str(join_policy),
                "--allow-sig-hash-join-key",
            ]
            t_step2_start = time.monotonic()
            rc, tail, patterns_content = run_step_log(extract_cmd, out_root / "patterns.log", cwd=str(repo_root))
            t_patterns = int(time.monotonic() - t_step2_start)
            log(f"[orchestrator]   step 2/3 patterns elapsed={t_patterns}s")
            if rc != 0:
                step_failed = "patterns"
                failure_notes = f"step=patterns returncode={rc}\n{tail}"
            else:
                presence_csv = out_root / "results" / "analysis" / "pattern_presence_file.csv"
                if not presence_csv.is_file():
                    step_failed = "patterns"
                    failure_notes = _build_patterns_missing_notes(
                        sid, out_root, records_dir, patterns_content
                    )

            # Surface patterns timing from captured output — top-5 to console
            patterns_timing_lines = [
                ln for ln in patterns_content.splitlines()
                if ln.startswith("[patterns_timing]")
            ]
            if patterns_timing_lines:
                summary_lines = [ln for ln in patterns_timing_lines if "domain=" not in ln]
                domain_lines  = [ln for ln in patterns_timing_lines if "domain=" in ln]
                lines_to_show = domain_lines + summary_lines
                print(f"[orchestrator]   patterns timing:", flush=True)
                for ln in lines_to_show:
                    print(f"[orchestrator]     {ln}", flush=True)

        # Step 2b — Name-projection patterns stage (opt-in, comparison_target in {name, both})
        if step_failed is None and comparison_target in ("name", "both"):
            log(f"[orchestrator]   step 2b name-patterns...")
            t_step2b_start = time.monotonic()
            try:
                segment_name_key_csv = out_root / "results" / "name_key" / "name_key_results.csv"
                rows_written = _filter_name_key_csv_to_segment(
                    name_key_results_csv, segment_name_key_csv, set(export_run_ids)
                )
                log(f"[orchestrator]   step 2b name-patterns filtered_rows={rows_written}")
                name_patterns_cmd = [
                    sys.executable,
                    str(repo_root / "tools" / "generate_name_key_patterns.py"),
                    "--comparison-target", "name",
                    "--name-key-csv", str(segment_name_key_csv),
                    "--out-root", str(out_root / "results" / "name_key" / "patterns"),
                ]
                rc, tail, _name_patterns_content = run_step_log(
                    name_patterns_cmd, out_root / "name_patterns.log", cwd=str(repo_root)
                )
                if rc != 0:
                    step_failed = "patterns_name"
                    failure_notes = f"step=patterns_name returncode={rc}\n{tail}"
            except Exception as exc:
                step_failed = "patterns_name"
                failure_notes = f"step=patterns_name error={exc}"
            t_patterns_name = int(time.monotonic() - t_step2b_start)
            log(f"[orchestrator]   step 2b name-patterns elapsed={t_patterns_name}s")

        # Step 3c — Same-segment name-fragmentation metric (opt-in, comparison_target in
        # {name, both}; Step 1 Part A). Gated identically to Step 2b (not run_type-gated --
        # a "reference" segment gets this too, same as a "bundle" segment) and runs
        # immediately after it, reusing Step 2b's already-filtered segment_name_key_csv --
        # no new opt-in surface, no extra JSON re-parse.
        if step_failed is None and comparison_target in ("name", "both"):
            log(f"[orchestrator]   step 3c name-fragmentation...")
            t_step3c_start = time.monotonic()
            frag_cmd = [
                sys.executable,
                str(repo_root / "tools" / "generate_pattern_name_fragmentation.py"),
                "--records-csv", str(out_root / "results" / "records" / "records.csv"),
                "--domain-patterns-csv", str(out_root / "results" / "analysis" / "domain_patterns.csv"),
                "--name-key-csv", str(segment_name_key_csv),
                "--out-dir", str(out_root / "results" / "analysis"),
            ]
            rc, tail, _frag_content = run_step_log(
                frag_cmd, out_root / "name_fragmentation.log", cwd=str(repo_root)
            )
            if rc != 0:
                step_failed = "patterns_name_fragmentation"
                failure_notes = f"step=patterns_name_fragmentation returncode={rc}\n{tail}"
            t_name_fragmentation = int(time.monotonic() - t_step3c_start)
            log(f"[orchestrator]   step 3c name-fragmentation elapsed={t_name_fragmentation}s")

        # Step 3 — Bundle stage
        if step_failed is None and run_type == "bundle":
            log(f"[orchestrator]   step 3/3 bundle...")
            bundle_cmd = [
                sys.executable,
                str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
                "--analysis-dir", str(out_root / "results" / "analysis"),
                "--out-dir", str(out_root / "results" / "bundle_analysis"),
                "--metadata-file", str(records_dir / "file_metadata.csv"),
                "--no-discover-populations",
                "--purge-view", "both",
                "--latent-purgeable-file", str(out_root / "results" / "records" / "latent_purgeable.csv"),
            ]
            bundle_cmd += ["--workers", str(bundle_workers)]
            t_step3_start = time.monotonic()
            rc, tail, _content = run_step_log(bundle_cmd, out_root / "bundle.log", cwd=str(repo_root))
            t_bundle = int(time.monotonic() - t_step3_start)
            log(f"[orchestrator]   step 3/3 bundle elapsed={t_bundle}s")
            if rc != 0:
                step_failed = "bundle"
                failure_notes = f"step=bundle returncode={rc}\n{tail}"

        # Step 3b — Name-projection bundle stage (opt-in, comparison_target in {name, both})
        # --purge-view is left unset: run_bundle_analysis.py's target-aware default resolves
        # it to "all" for --comparison-target name (the only view name-target supports).
        if step_failed is None and run_type == "bundle" and comparison_target in ("name", "both"):
            log(f"[orchestrator]   step 3b name-bundle...")
            # Clear any name-leg output from a previous run of this segment before
            # regenerating. run_bundle_analysis.py only writes per-domain folders for
            # domains present in *this* run's pattern set -- it never deletes a stale
            # <domain>/ folder left over from a prior run whose population included a
            # domain this one doesn't. Left in place, emit_name_target_provenance()'s
            # rglob("bundles.csv") (inside the run_bundle_analysis.py subprocess below)
            # would pick up those stale files and report them in a fresh
            # bundle_provenance.csv even for a segment that now has zero active domains --
            # merge_bi_outputs()'s *_combined.csv cleanup doesn't cover this, since
            # provenance is built independently (PR #390 review, third round). Matches the
            # same explicit stale-file cleanup tools/extractor.py's emit_records() already
            # does for identity_items_by_domain/*.csv before a fresh regenerate.
            name_bundle_analysis_dir = out_root / "results" / "bundle_analysis" / "name"
            if name_bundle_analysis_dir.is_dir():
                try:
                    # retry_fs_op: a cloud-synced segments root (OneDrive, etc.) can hold a
                    # transient lock on a file/folder this pipeline just finished writing on
                    # the previous run, producing a Windows PermissionError ([WinError 5]
                    # Access is denied) on an otherwise-correct rmtree. Caught here (rather
                    # than left to propagate) for the same reason as
                    # _clear_stale_name_all_before_run above: retry_fs_op exhausting every
                    # attempt -- not just a transient lock -- must still route through
                    # step_failed/registry update rather than escape _run_one_segment()
                    # uncaught, which would leave this segment's registry row at a stale
                    # status=complete and cause it to be silently skipped forever.
                    retry_fs_op(shutil.rmtree, str(name_bundle_analysis_dir))
                except Exception as exc:
                    step_failed = "clear_stale_name_bundle"
                    failure_notes = f"step=clear_stale_name_bundle error={exc}"
            if step_failed is None:
                name_bundle_cmd = [
                    sys.executable,
                    str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
                    "--analysis-dir", str(out_root / "results" / "analysis"),
                    "--out-dir", str(out_root / "results" / "bundle_analysis"),
                    "--comparison-target", "name",
                    "--name-key-patterns-dir", str(out_root / "results" / "name_key" / "patterns" / "name"),
                    "--metadata-file", str(records_dir / "file_metadata.csv"),
                    "--no-discover-populations",
                ]
                name_bundle_cmd += ["--workers", str(bundle_workers)]
                t_step3b_start = time.monotonic()
                rc, tail, _content = run_step_log(name_bundle_cmd, out_root / "bundle_name.log", cwd=str(repo_root))
                t_bundle_name = int(time.monotonic() - t_step3b_start)
                log(f"[orchestrator]   step 3b name-bundle elapsed={t_bundle_name}s")
                if rc != 0:
                    step_failed = "bundle_name"
                    failure_notes = f"step=bundle_name returncode={rc}\n{tail}"

        # Post-bundle validation (warn only, runs before registry write so warnings land in notes)
        if step_failed is None and run_type == "bundle":
            dag_nodes = out_root / "results" / "bundle_analysis" / "all" / "line_patterns" / "bundle_dag_nodes.csv"
            if not dag_nodes.is_file() or dag_nodes.stat().st_size == 0:
                warn = (
                    f"[WARN orchestrator] segment={sid} line_patterns/bundle_dag_nodes.csv "
                    f"missing or empty — bundle analysis may not have run correctly"
                )
                log(warn)
                notes_parts.append(warn)

        # BI merge (non-fatal; only runs when bundle succeeded)
        if step_failed is None and run_type == "bundle" and not skip_bi_merge:
            t_merge_start = time.monotonic()
            try:
                active_domains = _active_domains_from_presence_csv(out_root / "results" / "analysis")
                bundle_analysis_dir = out_root / "results" / "bundle_analysis" / "all"
                merge_result = merge_bi_outputs(bundle_analysis_dir, active_domains=active_domains)
                total_files = sum(v["files_merged"] for v in merge_result.values())
                total_rows = sum(v["rows_written"] for v in merge_result.values())
                log(
                    f"[orchestrator] bi_merge segment={sid} files_merged={total_files} rows_written={total_rows}"
                )
            except Exception as merge_exc:
                log(f"[WARN orchestrator] bi_merge failed for segment={sid}: {merge_exc}")
            t_merge = int(time.monotonic() - t_merge_start)
            log(f"[orchestrator]   bi_merge elapsed={t_merge}s")

        # Name-projection BI merge (opt-in; mirrors the config-leg merge above but reads
        # bundle_analysis/name_all/ -- the flat, single-path-segment location
        # run_bundle_analysis_for_target() relocates the name leg's ALL-view output to, so
        # it matches the Power BI model's pPurgeView folder-splice convention -- and the
        # name-target domain set)
        if step_failed is None and run_type == "bundle" and comparison_target in ("name", "both") and not skip_bi_merge:
            t_merge_name_start = time.monotonic()
            try:
                active_domains_name = _active_domains_from_name_patterns(
                    out_root / "results" / "name_key" / "patterns" / "name"
                )
                bundle_analysis_name_dir = out_root / "results" / "bundle_analysis" / "name_all"
                merge_result_name = merge_bi_outputs(bundle_analysis_name_dir, active_domains=active_domains_name)
                total_files_name = sum(v["files_merged"] for v in merge_result_name.values())
                total_rows_name = sum(v["rows_written"] for v in merge_result_name.values())
                log(
                    f"[orchestrator] bi_merge_name segment={sid} files_merged={total_files_name} rows_written={total_rows_name}"
                )
                # PR3 BI-output-compatibility brief's "Column-shape constraint": every
                # *_combined.csv under name_all/ must additionally declare
                # comparison_target/coverage_class/provenance_note per row, strictly
                # additive to the existing typed columns the Power BI model already reads.
                annotate_stats = annotate_name_target_combined_files(bundle_analysis_name_dir)
                log(f"[orchestrator] bi_merge_name_annotate segment={sid} files_annotated={len(annotate_stats)}")
            except Exception as merge_exc:
                # Unlike the config leg's own bi_merge above (deliberately non-fatal --
                # its "complete" status has no separate output-verifying marker),
                # a failure here MUST fail the segment. _segment_has_name_leg_output()
                # only checks that bundle_provenance.csv exists, which step 3b already
                # wrote successfully before this block ever runs -- so a merge/annotate
                # failure logged as a mere warning would still record status=complete,
                # and a later non-forced run would then skip this segment forever,
                # permanently leaving Power BI with combined files that are stale or
                # missing the required comparison_target/coverage_class/provenance_note
                # columns (PR review, #391, second round).
                step_failed = "bi_merge_name"
                failure_notes = f"step=bi_merge_name error={merge_exc}"
                log(f"[WARN orchestrator] bi_merge_name failed for segment={sid}: {merge_exc}")
            t_merge_name = int(time.monotonic() - t_merge_name_start)
            log(f"[orchestrator]   bi_merge_name elapsed={t_merge_name}s")

        elapsed = int(time.monotonic() - t_start)

        timing_parts = [
            f"segment={sid}",
            f"prepare={t_prepare}s",
            f"patterns={t_patterns}s",
        ]
        if t_bundle is not None:
            timing_parts.append(f"bundle={t_bundle}s")
        if t_merge is not None:
            timing_parts.append(f"bi_merge={t_merge}s")
        if t_patterns_name is not None:
            timing_parts.append(f"patterns_name={t_patterns_name}s")
        if t_bundle_name is not None:
            timing_parts.append(f"bundle_name={t_bundle_name}s")
        if t_merge_name is not None:
            timing_parts.append(f"bi_merge_name={t_merge_name}s")
        timing_parts.append(f"total={elapsed}s")
        log(f"[orchestrator]   timing {' '.join(timing_parts)}")

        if step_failed is not None:
            log(f"[orchestrator]   failure_notes: {failure_notes}")

    # Update registry under lock
    with registry_lock:
        ri = reg_index.get(sid)
        if ri is not None:
            if step_failed is None:
                registry[ri]["status"] = "complete"
                registry[ri]["last_run_utc"] = utc_now_iso()
                if "notes" in registry[ri]:
                    registry[ri]["notes"] = "; ".join(notes_parts)
            else:
                registry[ri]["status"] = "failed"
                registry[ri]["last_run_utc"] = utc_now_iso()
                registry[ri]["notes"] = failure_notes[:500]
        write_registry_atomic(registry_file, registry)
        write_results_registry(
            manifest_file=manifest_file,
            registry_file=registry_file,
            output_file=results_registry_file,
        )

    # Update counters and read progress snapshot under lock
    with counters_lock:
        if step_failed is None:
            counters["complete"] += 1
        else:
            counters["failed"] += 1
            counters["failed_ids"].append(sid)
        done = counters["complete"] + counters["failed"]
        running = total - done - counters.get("skipped", 0)
        n_complete_now = counters["complete"]
        n_failed_now = counters["failed"]

    # Console: complete/failed status
    if step_failed is None:
        print(f"[orchestrator]   ✓ complete elapsed={elapsed}s [worker={worker_id}]", flush=True)
    else:
        print(f"[orchestrator]   ✗ failed at step={step_failed} [worker={worker_id}]", flush=True)

    # Console: progress after every completion
    print(
        f"[orchestrator]   progress: {n_complete_now}/{total} complete"
        f"  {max(0, running)} running  {n_failed_now} failed",
        flush=True,
    )

    return {
        "segment_id": sid,
        "status": "complete" if step_failed is None else "failed",
        "files": file_count,
        "level": level,
        "prepare_s": t_prepare,
        "patterns_s": t_patterns,
        "bundle_s": t_bundle if t_bundle is not None else 0,
        "bi_merge_s": t_merge if t_merge is not None else 0,
        "total_s": elapsed,
        "worker_id": worker_id,
        "patterns_top5": patterns_timing_lines[:5],
        "failure_note": failure_notes if step_failed else "",
    }


def run_orchestrator(args: argparse.Namespace) -> int:
    """Load the manifest/registry/membership, build and validate the run plan, then
    execute (or, in --dry-run, print) the patterns+bundle pipeline for every eligible
    segment in level order, writing run_registry.csv/results_registry.csv/
    run_summary.txt.

    --- trace ---
    reads: `args` -- argparse.Namespace from main() (--manifest-file, --registry-file,
        --results-registry-file, --membership-file, --records-dir, --exports-dir,
        --segments-root, --repo-root, --join-policy, --segment, --force, --dry-run,
        --skip-bi-merge, --workers/--bundle-workers/--workers-auto, --no-preshard,
        --force-preshard, --comparison-target, --name-key-results-csv).
    calls: load_manifest(); load_registry(); load_membership(); build_run_plan();
        validate_membership_against_manifest(); _segment_has_name_leg_output()
        (skip-check, dry-run and live); _preshard_corpus_records() (gated on
        marker/force state); ThreadPoolExecutor/_run_one_segment() (live run, once per
        segment in plan_to_run); write_results_registry(); _write_run_summary().
    thresholds: `_CORPUS_PRESHARD_MARKER = ".preshard_complete_corpus"` (module
        constant, l.51: gates whether preshard re-runs); the preshard skip condition
        itself (`preshard_marker.is_file() and not _has_pending`) is inline control
        flow built from that marker plus a freshly-computed `_has_pending` flag, not a
        single named constant.
    returns: int exit code (1 if segment_membership.csv disagrees with
        segment_manifest.csv -- via validate_membership_against_manifest() -- or if
        any segment failed or results_registry write failed; 0 otherwise). In
        --dry-run mode, returns 0 unconditionally after printing the full plan without
        executing anything. Writes run_registry.csv (incrementally, per segment, and
        once more at the end via write_results_registry()) and
        segments_root/run_summary.txt. Called by main() as `run_orchestrator(args)`.
    """
    manifest_file = Path(args.manifest_file).resolve()
    registry_file = Path(args.registry_file).resolve()
    results_registry_file = Path(args.results_registry_file).resolve()
    membership_file = Path(args.membership_file).resolve()
    records_dir = Path(args.records_dir).resolve()
    exports_dir = Path(args.exports_dir).resolve()
    segments_root = Path(args.segments_root).resolve()
    repo_root = Path(args.repo_root).resolve()
    join_policy = Path(args.join_policy).resolve()

    manifest = load_manifest(manifest_file)
    registry = load_registry(registry_file)
    membership = load_membership(membership_file)

    plan = build_run_plan(
        manifest, registry, args.segment, args.force
    )

    membership_errors = validate_membership_against_manifest(plan, membership)
    if membership_errors:
        sys.stderr.write(
            f"[ERROR orchestrator] segment_membership.csv ({membership_file}) disagrees "
            f"with segment_manifest.csv ({manifest_file}) for {len(membership_errors)} "
            f"segment(s) — refusing to run against a possibly stale or mismatched "
            f"membership file. Re-run build_segment_manifest.py, or check --membership-file:\n"
        )
        for err in membership_errors:
            sys.stderr.write(f"  {err}\n")
        return 1

    total = len(plan)
    n_complete = 0
    n_failed = 0
    n_skipped = 0
    failed_ids: List[str] = []
    skipped_ids: List[str] = []

    # Build a lookup from segment_id → index in registry list for in-place update
    reg_index: Dict[str, int] = {
        r.get("segment_id", ""): i for i, r in enumerate(registry)
    }

    # ── dry-run ──────────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"[dry-run] {total} bundle segment(s) in plan")
        print(
            f"[dry-run] workers: segment_workers={args.workers} domain_workers={args.bundle_workers}"
            f" (mode={'auto' if args.workers_auto else 'explicit'})\n"
        )
        for idx, (reg_row, mrow) in enumerate(plan, 1):
            sid = reg_row.get("segment_id", "")
            output_folder = reg_row.get("output_folder", "").strip()
            status = reg_row.get("status", "").strip()
            run_type = reg_row.get("run_type", "bundle").strip()
            out_root = segments_root / output_folder

            # --segment filter
            if args.segment and sid not in set(args.segment):
                continue

            # skip check -- a segment already marked complete under a prior config-only
            # run still needs (re)processing if this run additionally requests the name
            # leg and that leg hasn't produced output for this segment yet (PR #390 review).
            # Not run_type-gated: Step 3c (pattern_name_fragmentation.csv) applies to every
            # run_type, not just "bundle" -- see the matching comment in the live-run
            # skip-check loop below and _segment_has_name_leg_output()'s own docstring.
            needs_name_leg = args.comparison_target in ("name", "both")
            already_satisfied = status == "complete" and (
                not needs_name_leg or _segment_has_name_leg_output(out_root, run_type)
            )
            skip = already_satisfied and not args.force

            try:
                level = int(mrow.get("segment_level", 0))
            except (ValueError, TypeError):
                level = 0

            file_count = len(membership.get(sid, []))

            status_label = "complete (would skip)" if skip else status or "pending"
            reason_note = reg_row.get("notes", "").strip()
            reason_suffix = f"  reason={reason_note}" if (not skip and reason_note) else ""
            print(
                f"[dry-run] segment={sid}  level={level}  files={file_count}"
                f"  output={output_folder}  status={status_label}{reason_suffix}"
            )
            if skip:
                print(f"  (skipped — already complete; use --force to re-run)")
                continue

            corpus_label_synth_dir = records_dir.parent / "label_synthesis"
            extract_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_extract_all.py"),
                str(exports_dir),
                "--out-root", str(out_root),
                "--stages", "patterns",
                "--records-dir", str(records_dir),
                "--label-synth-dir", str(corpus_label_synth_dir),
                "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
                "--join-policy", str(join_policy),
                "--allow-sig-hash-join-key",
            ]
            print(f"  step 1: prepare (dirs + segment records filter)")
            print(f"  step 2: {' '.join(extract_cmd[1:])}")
            if args.comparison_target in ("name", "both"):
                segment_name_key_csv = out_root / "results" / "name_key" / "name_key_results.csv"
                name_patterns_cmd = [
                    sys.executable,
                    str(repo_root / "tools" / "generate_name_key_patterns.py"),
                    "--comparison-target", "name",
                    "--name-key-csv", str(segment_name_key_csv),
                    "--out-root", str(out_root / "results" / "name_key" / "patterns"),
                ]
                print(f"  step 2b: filter {args.name_key_results_csv} -> {segment_name_key_csv}")
                print(f"  step 2b: {' '.join(name_patterns_cmd[1:])}")
                frag_cmd = [
                    sys.executable,
                    str(repo_root / "tools" / "generate_pattern_name_fragmentation.py"),
                    "--records-csv", str(out_root / "results" / "records" / "records.csv"),
                    "--domain-patterns-csv", str(out_root / "results" / "analysis" / "domain_patterns.csv"),
                    "--name-key-csv", str(segment_name_key_csv),
                    "--out-dir", str(out_root / "results" / "analysis"),
                ]
                print(f"  step 3c: {' '.join(frag_cmd[1:])}")
            if run_type == "bundle":
                bundle_cmd = [
                    sys.executable,
                    str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
                    "--analysis-dir", str(out_root / "results" / "analysis"),
                    "--out-dir", str(out_root / "results" / "bundle_analysis"),
                    "--metadata-file", str(records_dir / "file_metadata.csv"),
                    "--no-discover-populations",
                    "--purge-view", "both",
                    "--latent-purgeable-file", str(out_root / "results" / "records" / "latent_purgeable.csv"),
                ]
                bundle_cmd += ["--workers", str(args.bundle_workers)]
                print(f"  step 3: {' '.join(bundle_cmd[1:])}")
                if args.comparison_target in ("name", "both"):
                    name_bundle_cmd = [
                        sys.executable,
                        str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
                        "--analysis-dir", str(out_root / "results" / "analysis"),
                        "--out-dir", str(out_root / "results" / "bundle_analysis"),
                        "--comparison-target", "name",
                        "--name-key-patterns-dir", str(out_root / "results" / "name_key" / "patterns" / "name"),
                        "--metadata-file", str(records_dir / "file_metadata.csv"),
                        "--no-discover-populations",
                    ]
                    name_bundle_cmd += ["--workers", str(args.bundle_workers)]
                    print(f"  step 3b: rmtree {out_root / 'results' / 'bundle_analysis' / 'name'} (if exists)")
                    print(f"  step 3b: {' '.join(name_bundle_cmd[1:])}")
            print()
        return 0

    # ── live run ─────────────────────────────────────────────────────────────
    run_start_utc = utc_now_iso()
    run_t_start = time.monotonic()

    # Build segment_plans for preshard (respects --segment filter)
    segment_plans: Dict[str, Dict] = {}
    for reg_row, mrow in plan:
        sid = reg_row.get("segment_id", "").strip()
        if args.segment and sid not in set(args.segment):
            continue
        output_folder = reg_row.get("output_folder", "").strip()
        allowed_ids = set(membership.get(sid, []))
        out_root = segments_root / output_folder
        segment_records_dir = out_root / "results" / "records"
        segment_plans[sid] = {
            "sid": sid,
            "segment_records_dir": segment_records_dir,
            "allowed_ids": allowed_ids,
            "status": reg_row.get("status", "").strip(),
        }

    if segment_plans:
        preshard_marker = records_dir / _CORPUS_PRESHARD_MARKER
        _do_preshard = False
        # The corpus marker only means "nothing needs fresh sharded records" if
        # every planned segment is already complete. A registry-driven skip run
        # (default, no --force) can carry pending segments whose population
        # just changed — those need their records.csv/identity shards refreshed
        # even though the marker predates that change, otherwise _write_segment_records()'s
        # per-segment .preshard_complete fallback marker (also stale) causes the
        # segment to run against its OLD population while export_run_ids.txt
        # reflects the NEW one.
        _has_pending = any(plan.get("status") != "complete" for plan in segment_plans.values())
        if args.no_preshard:
            print("[orchestrator] preshard skipped (--no-preshard)", flush=True)
        elif args.force_preshard or args.force:
            _do_preshard = True
            preshard_marker.unlink(missing_ok=True)
        elif preshard_marker.is_file() and not _has_pending:
            print("[orchestrator] preshard skipped (corpus marker found, no pending segments)", flush=True)
        else:
            _do_preshard = True

        if _do_preshard:
            t_preshard = time.monotonic()
            _preshard_corpus_records(records_dir, segment_plans, force=args.force)
            print(f"[orchestrator] preshard complete elapsed={int(time.monotonic()-t_preshard)}s", flush=True)
            preshard_marker.write_text("ok", encoding="utf-8")

    # Apply --segment filter and skip check; count skips before submitting to executor
    segment_results: List[Dict] = []
    segment_results_lock = threading.Lock()

    plan_to_run: List[tuple[dict, dict]] = []
    for reg_row, mrow in plan:
        sid = reg_row.get("segment_id", "").strip()
        status = reg_row.get("status", "").strip()
        run_type = reg_row.get("run_type", "bundle").strip()
        out_root = segments_root / reg_row.get("output_folder", "").strip()

        if args.segment and sid not in set(args.segment):
            continue

        # A segment already marked complete under a prior config-only run still needs
        # (re)processing if this run additionally requests the name leg and that leg
        # hasn't produced output for this segment yet (PR #390 review) -- otherwise
        # --comparison-target name/both silently produces nothing for already-complete
        # segments unless the operator also remembers --force (which would needlessly
        # redo the config leg for every segment, not just the ones missing the name leg).
        # NOT run_type-gated (PR #476 review): step 3/3b (the bundle name leg) are gated on
        # run_type == "bundle", but Step 3c (pattern_name_fragmentation.csv, Step 1 Part A)
        # is not -- a "reference" row gets Step 2b/3c the same as a "bundle" row does, so a
        # "reference" segment must be re-checked for the name leg too, not treated as
        # automatically satisfied. _segment_has_name_leg_output() itself only requires
        # bundle_provenance.csv when run_type == "bundle", so a "reference" segment is never
        # needlessly reprocessed for output it can't produce.
        needs_name_leg = args.comparison_target in ("name", "both")
        already_satisfied = status == "complete" and (
            not needs_name_leg or _segment_has_name_leg_output(out_root, run_type)
        )
        if already_satisfied and not args.force:
            print(f"[orchestrator] skip segment={sid} (status=complete; use --force to re-run)")
            n_skipped += 1
            skipped_ids.append(f"{sid} — status=complete")
            try:
                _skip_level = int(mrow.get("segment_level", 0))
            except (ValueError, TypeError):
                _skip_level = 0
            _skip_files = len(membership.get(sid, []))
            segment_results.append({
                "segment_id": sid,
                "status": "skipped",
                "files": _skip_files,
                "level": _skip_level,
                "prepare_s": 0,
                "patterns_s": 0,
                "bundle_s": 0,
                "bi_merge_s": 0,
                "total_s": 0,
                "worker_id": 0,
                "patterns_top5": [],
                "failure_note": "",
            })
            continue

        plan_to_run.append((reg_row, mrow))

    registry_lock = threading.Lock()
    counters_lock = threading.Lock()
    counters: Dict[str, object] = {
        "complete": 0,
        "failed": 0,
        "skipped": n_skipped,
        "failed_ids": [],
    }

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _run_one_segment,
                idx, total, reg_row, mrow, membership,
                records_dir, exports_dir, segments_root, repo_root,
                join_policy, args.skip_bi_merge,
                registry, reg_index, registry_file,
                manifest_file, results_registry_file,
                registry_lock, counters, counters_lock,
                worker_id=(i % args.workers) + 1,
                bundle_workers=args.bundle_workers,
                comparison_target=args.comparison_target,
                name_key_results_csv=(
                    Path(args.name_key_results_csv).resolve() if args.name_key_results_csv else None
                ),
            ): reg_row.get("segment_id", "")
            for i, (idx, (reg_row, mrow)) in enumerate(enumerate(plan_to_run, 1))
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                with segment_results_lock:
                    segment_results.append(result)
            except Exception as exc:
                sid = futures[future]
                print(f"[orchestrator] ✗ segment={sid} unhandled exception: {exc}", flush=True)
                with counters_lock:
                    counters["failed"] += 1
                    counters["failed_ids"].append(sid)
                with segment_results_lock:
                    segment_results.append({
                        "segment_id": sid,
                        "status": "failed",
                        "files": 0,
                        "level": 0,
                        "prepare_s": 0,
                        "patterns_s": 0,
                        "bundle_s": 0,
                        "bi_merge_s": 0,
                        "total_s": 0,
                        "worker_id": 0,
                        "patterns_top5": [],
                        "failure_note": str(exc),
                    })

    run_end_utc = utc_now_iso()
    n_complete = counters["complete"]
    n_failed = counters["failed"]
    failed_ids = counters["failed_ids"]

    results_registry_failed = False
    try:
        rows_written = write_results_registry(
            manifest_file=manifest_file,
            registry_file=registry_file,
            output_file=results_registry_file,
        )
        print(
            f"[orchestrator] results_registry written to {results_registry_file} "
            f"({rows_written} row(s))",
            flush=True,
        )
    except Exception as exc:
        results_registry_failed = True
        print(f"[WARN orchestrator] results_registry write failed: {exc}", flush=True)

    # ── Final summary ─────────────────────────────────────────────────────────
    # Count non-bundle rows as additional skips
    non_bundle = [r for r in registry if r.get("run_type", "").strip() not in {"bundle", "reference"}]
    non_bundle_skipped = len(non_bundle)

    print(f"\n[orchestrator] ── run complete ──")
    print(f"  complete : {n_complete}")
    if failed_ids:
        print(f"  failed   : {n_failed}  ({', '.join(failed_ids)})")
    else:
        print(f"  failed   : {n_failed}")
    skip_detail = ""
    if skipped_ids:
        skip_detail = f"  ({'; '.join(skipped_ids)})"
    if non_bundle_skipped:
        skip_detail += f"  ({non_bundle_skipped} non-bundle rows — run_type!=bundle)"
    print(f"  skipped  : {n_skipped + non_bundle_skipped}{skip_detail}")
    print(f"  total    : {total}")

    segments_run = n_complete + n_failed
    total_elapsed = int(time.monotonic() - run_t_start)
    avg_per_segment = total_elapsed // segments_run if segments_run > 0 else 0
    print(
        f"[orchestrator] timing_summary segments_run={segments_run}"
        f" total_elapsed={total_elapsed}s avg_per_segment={avg_per_segment}s"
    )

    if segment_results:
        try:
            summary_path = _write_run_summary(
                segments_root,
                run_start_utc,
                run_end_utc,
                total_elapsed,
                segment_results,
                workers=args.workers,
                bundle_workers=args.bundle_workers,
                workers_auto=args.workers_auto,
            )
            print(f"[orchestrator] run_summary written to {summary_path}", flush=True)
        except Exception as _sum_exc:
            print(f"[WARN orchestrator] run_summary write failed: {_sum_exc}", flush=True)

    return 1 if n_failed > 0 or results_registry_failed else 0


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    """CLI entry point: parse arguments, resolve --workers/auto worker split and
    --results-registry-file/--membership-file defaults, then hand off to
    run_orchestrator().

    --- trace ---
    reads: CLI args (see the module-level argparse block for the full list:
        --manifest-file, --registry-file, --results-registry-file, --membership-file,
        --records-dir, --exports-dir, --segments-root, --repo-root, --join-policy,
        --segment, --force, --dry-run, --skip-bi-merge, --workers, --no-preshard,
        --force-preshard, --comparison-target, --name-key-results-csv).
    calls: compute_worker_split() (when --workers is "auto", or to derive
        bundle_workers from an explicit --workers N); run_orchestrator().
    thresholds: VALID_COMPARISON_TARGETS = {"config", "name", "both"} (module
        constant, l.44, enforced via argparse choices); default --workers=4; the
        --name-key-results-csv-required-when-name/both check (ap.error(), inline, not
        a named constant).
    returns: None; calls sys.exit(run_orchestrator(args)) -- this is **Run C2**
        (l.245 of tools/corpus_update_runbook.ps1), invoked per segment_manifest.csv/
        run_registry.csv (Run C1's output) after the mandatory file_metadata.csv
        human-edit pause, and its own output (run_registry.csv, updated in place;
        results_registry.csv) is consumed by **Run C2.5**
        (tools/build_results_registry.py).
    """
    ap = argparse.ArgumentParser(
        description="Segment orchestrator: run patterns + bundle stages per segment in level order."
    )
    ap.add_argument("--manifest-file", required=True, help="Path to segment_manifest.csv")
    ap.add_argument(
        "--registry-file", required=True,
        help="Path to run_registry.csv (updated in-place after each segment)",
    )
    ap.add_argument(
        "--results-registry-file",
        default=None,
        help="Path to results_registry.csv (default: sibling of run_registry.csv)",
    )
    ap.add_argument(
        "--membership-file",
        default=None,
        help="Path to segment_membership.csv (default: sibling of segment_manifest.csv)",
    )
    ap.add_argument(
        "--records-dir", required=True,
        help="Path to corpus-level results/records/ directory",
    )
    ap.add_argument("--exports-dir", required=True, help="Path to fingerprint JSON exports folder")
    ap.add_argument(
        "--segments-root", required=True,
        help="Output root for segment folders — each segment written under {segments-root}/{output_folder}/",
    )
    ap.add_argument("--repo-root", required=True, help="Path to repo root (for resolving tool script paths)")
    ap.add_argument("--join-policy", required=True, help="Path to domain_join_key_policies.json")
    ap.add_argument(
        "--segment", nargs="+", default=None,
        help="Optional: run only these segment_id(s) (space-separated, targeted re-run or resume)",
    )
    ap.add_argument(
        "--force", action="store_true",
        help="Re-run segments already marked complete in the registry",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Print full run plan without executing anything",
    )
    ap.add_argument(
        "--skip-bi-merge", action="store_true",
        help="Skip the BI merge post-processing step (useful for dry runs and debugging)",
    )
    ap.add_argument(
        "--workers", default=4,
        help="Max parallel segments, or 'auto' to derive from CPU count (default: 4)",
    )
    ap.add_argument(
        "--no-preshard", action="store_true",
        help="Skip preshard unconditionally",
    )
    ap.add_argument(
        "--force-preshard", action="store_true",
        help="Force preshard even if corpus marker exists",
    )
    ap.add_argument(
        "--comparison-target", choices=sorted(VALID_COMPARISON_TARGETS), default="config",
        help="config (default, unchanged behavior/output): join_hash only, exactly as "
             "before this flag existed. name/both additionally re-cluster this segment's "
             "slice of --name-key-results-csv (PR1's join_key_name_identity) and bundle-mine "
             "it into results/bundle_analysis/name_all/, alongside the existing "
             "results/bundle_analysis/{all,used}/ config-target output.",
    )
    ap.add_argument(
        "--name-key-results-csv", default=None,
        help="Path to a corpus-wide name_key_results.csv (tools/apply_name_key_policy.py "
             "output, run once for the whole corpus beforehand). Required when "
             "--comparison-target is name or both.",
    )
    args = ap.parse_args()
    if args.comparison_target in ("name", "both") and not args.name_key_results_csv:
        ap.error("--name-key-results-csv is required when --comparison-target is name or both")
    if str(args.workers).strip().lower() == "auto":
        args.workers, args.bundle_workers = compute_worker_split()
        args.workers_auto = True
    else:
        args.workers = int(args.workers)
        # Coordinate the bundle-stage pool to the same CPU budget rather than
        # letting it default to run_bundle_analysis.py's own fixed default of 4
        # — otherwise total concurrency grows unbounded as --workers N grows
        # (N x 4 instead of staying near the actual core budget).
        _, args.bundle_workers = compute_worker_split(segment_workers=args.workers)
        args.workers_auto = False
    if args.results_registry_file is None:
        args.results_registry_file = str(Path(args.registry_file).resolve().with_name("results_registry.csv"))
    if args.membership_file is None:
        args.membership_file = str(Path(args.manifest_file).resolve().with_name("segment_membership.csv"))
    sys.exit(run_orchestrator(args))


if __name__ == "__main__":
    main()
