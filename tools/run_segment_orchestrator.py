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
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# Allow import of bundle_analysis package from the same tools/ directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundle_analysis.common import atomic_write_csv

# Maximum destination file handles open simultaneously during preshard.
# Keeps fd usage well below typical OS limits (1024) regardless of segment count.
# Each batch re-streams the source file once, so total passes = ceil(N/batch).
_PRESHARD_BATCH = 64

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


# ── Subprocess helper ─────────────────────────────────────────────────────────

def run_step(cmd: List[str]) -> subprocess.CompletedProcess:
    """Run a subprocess step, capturing stderr, raising on non-zero exit."""
    return subprocess.run(cmd, check=True, capture_output=False, text=True)


def run_step_capture(cmd: List[str], cwd: Optional[str] = None) -> tuple[int, str, str]:
    """Run a subprocess step, return (returncode, last_20_lines_stderr, full_stderr)."""
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    stderr_lines = (result.stderr or "").splitlines()
    tail = "\n".join(stderr_lines[-20:]) if stderr_lines else ""
    return result.returncode, tail, result.stderr or ""


# ── Record helpers ────────────────────────────────────────────────────────────

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
    import csv as _csv

    # csv.field_size_limit() converts to a C long; on Windows CPython the C long
    # is 32-bit so sys.maxsize overflows.  Cap at 2^31-1 which fits everywhere.
    try:
        _csv.field_size_limit(2 ** 31 - 1)
    except OverflowError:
        _csv.field_size_limit(2 ** 30)

    t0 = time.monotonic()

    # One-to-many lookup: export_run_id → list of segment_ids
    # An export_run_id can appear in multiple segments (parent/child and scoped
    # segments built by build_segment_manifest overlap intentionally).
    id_to_sids: Dict[str, List[str]] = {}
    for sid, plan_entry in segment_plans.items():
        for eid in plan_entry["allowed_ids"]:
            id_to_sids.setdefault(eid, []).append(sid)

    # ── records.csv and file_metadata.csv ─────────────────────────────────────
    for fname in ("records.csv", "file_metadata.csv"):
        src = records_dir / fname
        if not src.is_file():
            continue

        # Determine which segments need this file.
        # Only skip when the marker exists AND the segment is already complete —
        # pending/failed segments must always get fresh inputs so retries without
        # --force don't run against stale membership from a prior interrupted run.
        segments_to_write: Dict[str, Dict] = {}
        for sid, plan_entry in segment_plans.items():
            marker = plan_entry["segment_records_dir"] / ".preshard_complete"
            if not force and marker.is_file() and plan_entry.get("status") == "complete":
                continue
            segments_to_write[sid] = plan_entry

        n_skipped = len(segment_plans) - len(segments_to_write)
        if not segments_to_write:
            print(
                f"[preshard] {fname} → 0 segments written,"
                f" {n_skipped} segments skipped (already exist)",
                flush=True,
            )
            continue

        # Ensure destination dirs exist
        for plan_entry in segments_to_write.values():
            plan_entry["segment_records_dir"].mkdir(parents=True, exist_ok=True)

        # Read fieldnames once before batching so we don't need a separate
        # header-only open inside the batch loop.
        with src.open("r", encoding="utf-8-sig", newline="") as f:
            fieldnames: List[str] = list(_csv.DictReader(f).fieldnames or [])
        if not fieldnames:
            continue

        # Fan out in batches so at most _PRESHARD_BATCH destination handles are
        # open simultaneously.  Each batch re-streams the source file once.
        seg_items = list(segments_to_write.items())
        for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
            batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])
            writers: Dict[str, _csv.DictWriter] = {}
            handles: Dict[str, object] = {}
            for sid, plan_entry in batch.items():
                dst = plan_entry["segment_records_dir"] / fname
                fh = dst.open("w", newline="", encoding="utf-8")
                handles[sid] = fh
                w = _csv.DictWriter(fh, fieldnames=fieldnames)
                w.writeheader()
                writers[sid] = w
            with src.open("r", encoding="utf-8-sig", newline="") as f:
                for row in _csv.DictReader(f):
                    eid = row.get("export_run_id", "").strip()
                    for row_sid in id_to_sids.get(eid, ()):
                        if row_sid in writers:
                            writers[row_sid].writerow(row)
            for fh in handles.values():
                fh.close()

        print(
            f"[preshard] {fname} → {len(segments_to_write)} segments written,"
            f" {n_skipped} segments skipped (already exist)",
            flush=True,
        )

    # ── identity_items_by_domain/ shards ──────────────────────────────────────
    corpus_shard_dir = records_dir / "identity_items_by_domain"
    if corpus_shard_dir.is_dir():
        shards_processed = 0
        seg_shard_files_written = 0

        for shard_file in sorted(corpus_shard_dir.iterdir()):
            if not shard_file.is_file() or shard_file.suffix != ".csv":
                continue

            # Determine which segments need this shard — same marker+status gate.
            segments_to_write: Dict[str, Dict] = {}
            for sid, plan_entry in segment_plans.items():
                marker = plan_entry["segment_records_dir"] / ".preshard_complete"
                if not force and marker.is_file() and plan_entry.get("status") == "complete":
                    continue
                segments_to_write[sid] = plan_entry

            if not segments_to_write:
                continue

            # Ensure shard dirs exist
            for plan_entry in segments_to_write.values():
                seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
                seg_shard_dir.mkdir(parents=True, exist_ok=True)

            with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
                shard_fieldnames: List[str] = list(_csv.DictReader(f).fieldnames or [])
            if not shard_fieldnames:
                continue

            seg_items = list(segments_to_write.items())
            for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
                batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])
                writers: Dict[str, _csv.DictWriter] = {}
                handles: Dict[str, object] = {}
                for sid, plan_entry in batch.items():
                    seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
                    dst_shard = seg_shard_dir / shard_file.name
                    fh = dst_shard.open("w", newline="", encoding="utf-8")
                    handles[sid] = fh
                    w = _csv.DictWriter(fh, fieldnames=shard_fieldnames)
                    w.writeheader()
                    writers[sid] = w
                with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
                    for row in _csv.DictReader(f):
                        eid = row.get("export_run_id", "").strip()
                        for row_sid in id_to_sids.get(eid, ()):
                            if row_sid in writers:
                                writers[row_sid].writerow(row)
                for fh in handles.values():
                    fh.close()

            shards_processed += 1
            seg_shard_files_written += len(segments_to_write)

        # Write .complete markers for all segment shard dirs
        for plan_entry in segment_plans.values():
            seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
            if seg_shard_dir.is_dir():
                (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")

        print(
            f"[preshard] identity_items shards → {shards_processed} shards processed,"
            f" {seg_shard_files_written} segment×shard files written",
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


def merge_bi_outputs(bundle_analysis_dir: Path, active_domains: Optional[frozenset] = None) -> dict:
    """Pre-merge per-domain bundle analysis CSVs into single combined files for Power BI.

    active_domains: when provided, only subfolders whose name is in this set are
    merged.  Pass the set derived from pattern_presence_file.csv so that stale
    domain folders left over from earlier runs are excluded.
    """
    if not bundle_analysis_dir.is_dir():
        return {}

    result: Dict[str, dict] = {}
    for filename in BI_MERGE_FILES:
        candidates = [
            p for p in bundle_analysis_dir.glob(f"*/{filename}")
            if "_population_discovery" not in str(p)
            and "_population_runs" not in str(p)
            and (active_domains is None or p.parent.name in active_domains)
        ]
        if not candidates:
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
            continue

        stem = Path(filename).stem
        out_path = bundle_analysis_dir / f"{stem}_combined.csv"
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


def run_orchestrator(args: argparse.Namespace) -> int:
    manifest_file = Path(args.manifest_file).resolve()
    registry_file = Path(args.registry_file).resolve()
    records_dir = Path(args.records_dir).resolve()
    exports_dir = Path(args.exports_dir).resolve()
    segments_root = Path(args.segments_root).resolve()
    repo_root = Path(args.repo_root).resolve()
    join_policy = Path(args.join_policy).resolve()

    manifest = load_manifest(manifest_file)
    registry = load_registry(registry_file)

    plan = build_run_plan(
        manifest, registry, args.segment, args.force
    )

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
        print(f"[dry-run] {total} bundle segment(s) in plan\n")
        for idx, (reg_row, mrow) in enumerate(plan, 1):
            sid = reg_row.get("segment_id", "")
            output_folder = reg_row.get("output_folder", "").strip()
            status = reg_row.get("status", "").strip()
            run_type = reg_row.get("run_type", "bundle").strip()
            out_root = segments_root / output_folder

            # --segment filter
            if args.segment and sid != args.segment:
                continue

            # skip check
            skip = (status == "complete" and not args.force)

            try:
                level = int(mrow.get("segment_level", 0))
            except (ValueError, TypeError):
                level = 0

            export_run_ids_raw = mrow.get("export_run_ids", "")
            file_count = len([x for x in export_run_ids_raw.split("|") if x.strip()])

            status_label = "complete (would skip)" if skip else status or "pending"
            print(
                f"[dry-run] segment={sid}  level={level}  files={file_count}"
                f"  output={output_folder}  status={status_label}"
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
                print(f"  step 3: {' '.join(bundle_cmd[1:])}")
            print()
        return 0

    # ── live run ─────────────────────────────────────────────────────────────
    run_t_start = time.monotonic()

    # Build segment_plans for preshard (respects --segment filter)
    segment_plans: Dict[str, Dict] = {}
    for reg_row, mrow in plan:
        sid = reg_row.get("segment_id", "").strip()
        if args.segment and sid != args.segment:
            continue
        output_folder = reg_row.get("output_folder", "").strip()
        export_run_ids_raw = mrow.get("export_run_ids", "")
        allowed_ids = set(x.strip() for x in export_run_ids_raw.split("|") if x.strip())
        out_root = segments_root / output_folder
        segment_records_dir = out_root / "results" / "records"
        segment_plans[sid] = {
            "segment_records_dir": segment_records_dir,
            "allowed_ids": allowed_ids,
            "status": reg_row.get("status", "").strip(),
        }

    if segment_plans:
        t_preshard = time.monotonic()
        _preshard_corpus_records(records_dir, segment_plans, force=args.force)
        print(f"[orchestrator] preshard complete elapsed={int(time.monotonic() - t_preshard)}s", flush=True)

    for idx, (reg_row, mrow) in enumerate(plan, 1):
        sid = reg_row.get("segment_id", "").strip()
        output_folder = reg_row.get("output_folder", "").strip()
        status = reg_row.get("status", "").strip()
        out_root = segments_root / output_folder

        # --segment filter
        if args.segment and sid != args.segment:
            continue

        # skip check
        if status == "complete" and not args.force:
            print(f"[orchestrator] skip segment={sid} (status=complete; use --force to re-run)")
            n_skipped += 1
            skipped_ids.append(f"{sid} — status=complete")
            continue

        try:
            level = int(mrow.get("segment_level", 0))
        except (ValueError, TypeError):
            level = 0

        export_run_ids_raw = mrow.get("export_run_ids", "")
        export_run_ids = sorted(x.strip() for x in export_run_ids_raw.split("|") if x.strip())
        file_count = len(export_run_ids)
        run_type = reg_row.get("run_type", "bundle").strip()

        print(
            f"\n[orchestrator] ── segment={sid} ({idx}/{total}) level={level} files={file_count} ──",
            flush=True,
        )

        step_failed: Optional[str] = None
        failure_notes: str = ""
        notes_parts: List[str] = []
        t_start = time.monotonic()
        t_prepare = 0
        t_patterns = 0
        t_bundle: Optional[int] = None
        t_merge: Optional[int] = None

        # Step 1 — Prepare: directories, export_run_ids.txt, segment-level records
        print(f"[orchestrator]   step 1/3 prepare...", flush=True)
        t_step1_start = time.monotonic()
        try:
            segment_records_dir = out_root / "results" / "records"
            segment_records_dir.mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "bundle_analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "label_synthesis").mkdir(parents=True, exist_ok=True)

            ids_file = out_root / "export_run_ids.txt"
            ids_file.write_text("\n".join(export_run_ids) + "\n", encoding="utf-8")

            _write_segment_records(records_dir, segment_records_dir, set(export_run_ids))
        except Exception as exc:
            step_failed = "prepare"
            failure_notes = f"step=prepare error={exc}"
        t_prepare = int(time.monotonic() - t_step1_start)
        print(f"[orchestrator]   step 1/3 prepare elapsed={t_prepare}s", flush=True)

        # Step 2 — Patterns stage
        # --records-dir points at corpus records so build_label_population (run internally
        # by run_extract_all) reads the full population, not just this segment's subset.
        # --label-synth-dir points at corpus label_synthesis so emit_analysis picks up the
        # LLM cache and curator annotations built in Run B without rebuilding per segment.
        if step_failed is None:
            print(f"[orchestrator]   step 2/3 patterns...", flush=True)
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
            rc, tail, patterns_stderr = run_step_capture(extract_cmd, cwd=str(repo_root))
            t_patterns = int(time.monotonic() - t_step2_start)
            print(f"[orchestrator]   step 2/3 patterns elapsed={t_patterns}s", flush=True)
            if rc != 0:
                step_failed = "patterns"
                failure_notes = f"step=patterns returncode={rc}\n{tail}"
            else:
                presence_csv = out_root / "results" / "analysis" / "pattern_presence_file.csv"
                if not presence_csv.is_file():
                    step_failed = "patterns"
                    failure_notes = _build_patterns_missing_notes(
                        sid, out_root, records_dir, patterns_stderr
                    )

        # Step 3 — Bundle stage
        if step_failed is None and run_type == "bundle":
            print(f"[orchestrator]   step 3/3 bundle...", flush=True)
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
            t_step3_start = time.monotonic()
            rc, tail, _stderr = run_step_capture(bundle_cmd, cwd=str(repo_root))
            t_bundle = int(time.monotonic() - t_step3_start)
            print(f"[orchestrator]   step 3/3 bundle elapsed={t_bundle}s", flush=True)
            if rc != 0:
                step_failed = "bundle"
                failure_notes = f"step=bundle returncode={rc}\n{tail}"

        # Post-bundle validation (warn only, runs before registry write so warnings land in notes)
        if step_failed is None and run_type == "bundle":
            dag_nodes = out_root / "results" / "bundle_analysis" / "all" / "line_patterns" / "bundle_dag_nodes.csv"
            if not dag_nodes.is_file() or dag_nodes.stat().st_size == 0:
                warn = (
                    f"[WARN orchestrator] segment={sid} line_patterns/bundle_dag_nodes.csv "
                    f"missing or empty — bundle analysis may not have run correctly"
                )
                print(warn, flush=True)
                notes_parts.append(warn)

        # BI merge (non-fatal; only runs when bundle succeeded)
        if step_failed is None and run_type == "bundle" and not args.skip_bi_merge:
            t_merge_start = time.monotonic()
            try:
                active_domains = _active_domains_from_presence_csv(out_root / "results" / "analysis")
                bundle_analysis_dir = out_root / "results" / "bundle_analysis" / "all"
                merge_result = merge_bi_outputs(bundle_analysis_dir, active_domains=active_domains)
                total_files = sum(v["files_merged"] for v in merge_result.values())
                total_rows = sum(v["rows_written"] for v in merge_result.values())
                print(
                    f"[orchestrator] bi_merge segment={sid} files_merged={total_files} rows_written={total_rows}",
                    flush=True,
                )
            except Exception as merge_exc:
                print(
                    f"[WARN orchestrator] bi_merge failed for segment={sid}: {merge_exc}",
                    flush=True,
                )
            t_merge = int(time.monotonic() - t_merge_start)
            print(f"[orchestrator]   bi_merge elapsed={t_merge}s", flush=True)

        elapsed = int(time.monotonic() - t_start)

        # Update registry row
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

        timing_parts = [
            f"segment={sid}",
            f"prepare={t_prepare}s",
            f"patterns={t_patterns}s",
        ]
        if t_bundle is not None:
            timing_parts.append(f"bundle={t_bundle}s")
        if t_merge is not None:
            timing_parts.append(f"bi_merge={t_merge}s")
        timing_parts.append(f"total={elapsed}s")
        print(f"[orchestrator]   timing {' '.join(timing_parts)}", flush=True)

        if step_failed is None:
            print(f"[orchestrator]   ✓ complete (elapsed: {elapsed}s)", flush=True)
            n_complete += 1
        else:
            print(
                f"[orchestrator]   ✗ failed at step={step_failed} (elapsed: {elapsed}s)",
                flush=True,
            )
            print(f"[orchestrator]   {failure_notes}", flush=True)
            n_failed += 1
            failed_ids.append(sid)

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

    return 1 if n_failed > 0 else 0


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
        "--segment", default=None,
        help="Optional: run only this segment_id (targeted re-run or resume)",
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
    args = ap.parse_args()
    sys.exit(run_orchestrator(args))


if __name__ == "__main__":
    main()
