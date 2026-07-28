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
from bundle_analysis.common import atomic_write_csv
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
    """Load segment_manifest.csv keyed by segment_id."""
    manifest: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            sid = row.get("segment_id", "").strip()
            if sid:
                manifest[sid] = row
    return manifest


def load_registry(path: Path) -> List[dict]:
    """Load run_registry.csv as a list of row dicts."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_membership(path: Path) -> Dict[str, List[str]]:
    """Load segment_membership.csv grouped by segment_id -> sorted export_run_ids.

    Replaces the old segment_manifest.csv `export_run_ids` pipe-delimited column,
    which could exceed spreadsheet cell limits for large populations.
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
    """Write registry rows atomically via temp-file + replace."""
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
    """Write run_summary.txt to segments_root atomically (temp + replace)."""
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
    """Run a subprocess step, capturing stderr, raising on non-zero exit."""
    return subprocess.run(cmd, check=True, capture_output=False, text=True)


def run_step_capture(cmd: List[str], cwd: Optional[str] = None) -> tuple[int, str, str]:
    """Run a subprocess step, return (returncode, last_20_lines_stderr, full_stderr)."""
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
    """
    # csv.field_size_limit() converts to a C long; on Windows CPython the C long
    # is 32-bit so sys.maxsize overflows.  Cap at 2^31-1 which fits everywhere.
    try:
        csv.field_size_limit(2 ** 31 - 1)
    except OverflowError:
        csv.field_size_limit(2 ** 30)

    t0 = time.monotonic()

    # ── records.csv and file_metadata.csv ─────────────────────────────────────
    for fname in ("records.csv", "file_metadata.csv"):
        src = records_dir / fname
        if not src.is_file():
            continue

        # Determine which segments need this file.
        # Skip only completed segments; pending/failed segments always get fresh
        # inputs so retries without --force don't run against stale data.
        segments_to_write = {
            sid: plan for sid, plan in segment_plans.items()
            if force or plan.get("status") != "complete"
        }
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

        # Write .complete markers for all segment shard dirs
        for plan_entry in segment_plans.values():
            seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
            if seg_shard_dir.is_dir():
                (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")

        print(
            f"[preshard] identity_items shards → {len(shard_files)} shards processed, "
            f"{total_written} segment×shard files written",
            flush=True,
        )

    # Write per-segment completion markers.  Done after all source files and
    # shards so a partial run (exception before this point) leaves no markers,
    # meaning the next run re-processes those segments from scratch.
    for plan_entry in segment_plans.values():
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
    """
    preshard_marker = segment_records_dir / ".preshard_complete"
    for fname in ("records.csv", "file_metadata.csv"):
        src = records_dir / fname
        if not src.is_file():
            continue
        dst = segment_records_dir / fname
        if preshard_marker.is_file():
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
            if preshard_marker.is_file():
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
    """
    if not name_key_results_csv.is_file():
        raise FileNotFoundError(f"--name-key-results-csv not found: {name_key_results_csv}")

    def _in_segment(raw_export_file: str) -> bool:
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
    """Build a diagnostic failure message when patterns exits 0 but writes no output."""
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
    pattern_presence_file.csv equivalent -- see audit_results/audit_8 for the schema diff).

    Unlike _active_domains_from_presence_csv(), an empty-but-present domain_patterns.csv
    is a legitimate, expected outcome for the name projection (a segment whose files don't
    intersect any of the 25 eligible domains -- see audit_results/audit_9's "what this PR
    does not attempt"), not a signal to fall back to "unfiltered." This function therefore
    returns `frozenset()` (not `None`) when the file exists but has zero domain rows, so
    merge_bi_outputs() excludes every domain subfolder instead of treating None-as-unfiltered
    and resurrecting stale per-domain output left over from a previous run of this segment
    under a different (larger) population. `None` is reserved for "the file is missing" --
    a genuinely different condition (the name-patterns step never ran or failed).
    """
    patterns_csv = name_patterns_dir / "domain_patterns.csv"
    if not patterns_csv.is_file():
        return None
    with patterns_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return frozenset(
            r.get("domain", "").strip() for r in reader if r.get("domain", "").strip()
        )


def _segment_has_name_leg_output(out_root: Path) -> bool:
    """Whether this segment's name-projection leg (step 2b/3b/BI-merge-name) has already
    completed at least once. emit_name_target_provenance() (tools/bundle_analysis's
    --comparison-target name path) always writes bundle_provenance.csv on a successful run,
    even when the segment's name-target pattern set comes back empty -- so its presence is
    a reliable "name leg already ran" marker, independent of run_registry.csv's single
    whole-segment `status` column, which has no notion of per-leg completion. Used so a
    segment already marked complete under a config-only run isn't skipped once the operator
    later asks for --comparison-target name/both -- see PR #390 review.

    bundle_provenance.csv lives at bundle_analysis/name_all/ (the flat, single-path-segment
    BI-facing output location -- see run_bundle_analysis_for_target()'s docstring), not
    under the internal bundle_analysis/name/ staging path."""
    return (out_root / "results" / "bundle_analysis" / "name_all" / "bundle_provenance.csv").is_file()


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
    """
    run_rows = [r for r in registry if r.get("run_type", "").strip() in {"bundle", "reference"}]

    def sort_key(row: dict) -> tuple:
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
    needed), then bundle-mine it into results/bundle_analysis/name/all/, mirroring
    results/bundle_analysis/all/ for the config leg but using join_key_name_identity as the
    join instead of join_hash.
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
            log_f.write(msg + "\n")
            log_f.flush()

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
                shutil.rmtree(name_bundle_analysis_dir)
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
            # run_type == "bundle" is required too -- see the matching comment in the
            # live-run skip-check loop below for why "reference" rows must be excluded.
            needs_name_leg = args.comparison_target in ("name", "both") and run_type == "bundle"
            already_satisfied = status == "complete" and (
                not needs_name_leg or _segment_has_name_leg_output(out_root)
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
        # run_type == "bundle" is required too: step 3/3b (both legs) are gated on
        # run_type == "bundle", so a "reference" row can never produce a name-leg marker
        # regardless of comparison_target -- without this gate, reference rows would never
        # be recognized as satisfied under name/both and would be needlessly reprocessed
        # (prepare/patterns/name-patterns) on every run instead of honoring the existing
        # registry-driven skip.
        needs_name_leg = args.comparison_target in ("name", "both") and run_type == "bundle"
        already_satisfied = status == "complete" and (
            not needs_name_leg or _segment_has_name_leg_output(out_root)
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
             "it into results/bundle_analysis/name/all/, alongside the existing "
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
