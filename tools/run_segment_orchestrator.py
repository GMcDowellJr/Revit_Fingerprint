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


def run_step_capture(cmd: List[str]) -> tuple[int, str]:
    """Run a subprocess step, return (returncode, last_20_lines_stderr)."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    stderr_lines = (result.stderr or "").splitlines()
    tail = "\n".join(stderr_lines[-20:]) if stderr_lines else ""
    return result.returncode, tail


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
    bundle_rows = [r for r in registry if r.get("run_type", "").strip() == "bundle"]

    def sort_key(row: dict) -> tuple:
        sid = row.get("segment_id", "")
        mrow = manifest.get(sid, {})
        try:
            level = int(mrow.get("segment_level", 0))
        except (ValueError, TypeError):
            level = 0
        return (level, sid)

    bundle_rows.sort(key=sort_key)

    plan: List[tuple[dict, dict]] = []
    for reg_row in bundle_rows:
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

            lp_cmd = [
                sys.executable,
                str(repo_root / "tools" / "label_synthesis" / "build_label_population.py"),
                "--out-root", str(out_root),
                "--records-dir", str(records_dir),
            ]
            extract_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_extract_all.py"),
                str(exports_dir),
                "--out-root", str(out_root),
                "--stages", "patterns",
                "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
                "--join-policy", str(join_policy),
                "--allow-sig-hash-join-key",
            ]
            bundle_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_bundle_analysis.py"),
                "--analysis-dir", str(out_root / "results" / "analysis"),
                "--out-dir", str(out_root / "results" / "bundle_analysis"),
                "--metadata-file", str(records_dir / "file_metadata.csv"),
                "--no-discover-populations",
            ]
            print(f"  step 1: {' '.join(lp_cmd[1:])}")
            print(f"  step 2: {' '.join(extract_cmd[1:])}")
            print(f"  step 3: {' '.join(bundle_cmd[1:])}")
            print()
        return 0

    # ── live run ─────────────────────────────────────────────────────────────
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

        print(
            f"\n[orchestrator] ── segment={sid} ({idx}/{total}) level={level} files={file_count} ──",
            flush=True,
        )

        step_failed: Optional[str] = None
        failure_notes: str = ""
        t_start = time.monotonic()

        # Step 1 — Prepare directories and export_run_ids.txt
        print(f"[orchestrator]   step 1/3 label_population...", flush=True)
        try:
            (out_root / "results" / "analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "bundle_analysis").mkdir(parents=True, exist_ok=True)
            (out_root / "results" / "label_synthesis").mkdir(parents=True, exist_ok=True)

            ids_file = out_root / "export_run_ids.txt"
            ids_file.write_text("\n".join(export_run_ids) + "\n", encoding="utf-8")

            lp_cmd = [
                sys.executable,
                str(repo_root / "tools" / "label_synthesis" / "build_label_population.py"),
                "--out-root", str(out_root),
                "--records-dir", str(records_dir),
            ]
            rc, tail = run_step_capture(lp_cmd)
            if rc != 0:
                raise subprocess.CalledProcessError(rc, lp_cmd, stderr=tail)
        except Exception as exc:
            step_failed = "label_population"
            failure_notes = f"step=label_population error={exc}"

        # Step 2 — Patterns stage
        if step_failed is None:
            print(f"[orchestrator]   step 2/3 patterns...", flush=True)
            extract_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_extract_all.py"),
                str(exports_dir),
                "--out-root", str(out_root),
                "--stages", "patterns",
                "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
                "--join-policy", str(join_policy),
                "--allow-sig-hash-join-key",
            ]
            rc, tail = run_step_capture(extract_cmd)
            if rc != 0:
                step_failed = "patterns"
                failure_notes = f"step=patterns returncode={rc}\n{tail}"

        # Step 3 — Bundle stage
        if step_failed is None:
            print(f"[orchestrator]   step 3/3 bundle...", flush=True)
            bundle_cmd = [
                sys.executable,
                str(repo_root / "tools" / "run_bundle_analysis.py"),
                "--analysis-dir", str(out_root / "results" / "analysis"),
                "--out-dir", str(out_root / "results" / "bundle_analysis"),
                "--metadata-file", str(records_dir / "file_metadata.csv"),
                "--no-discover-populations",
            ]
            rc, tail = run_step_capture(bundle_cmd)
            if rc != 0:
                step_failed = "bundle"
                failure_notes = f"step=bundle returncode={rc}\n{tail}"

        elapsed = int(time.monotonic() - t_start)

        # Update registry row
        ri = reg_index.get(sid)
        if ri is not None:
            if step_failed is None:
                registry[ri]["status"] = "complete"
                registry[ri]["last_run_utc"] = utc_now_iso()
                if "notes" in registry[ri]:
                    registry[ri]["notes"] = ""
            else:
                registry[ri]["status"] = "failed"
                registry[ri]["last_run_utc"] = utc_now_iso()
                registry[ri]["notes"] = failure_notes[:500]

        write_registry_atomic(registry_file, registry)

        if step_failed is None:
            print(f"[orchestrator]   ✓ complete (elapsed: {elapsed}s)", flush=True)
            n_complete += 1

            # Post-bundle validation (warn only)
            dag_nodes = out_root / "results" / "bundle_analysis" / "bundle_dag_nodes.csv"
            if not dag_nodes.is_file():
                print(
                    f"[WARN orchestrator] segment={sid} bundle_dag_nodes.csv missing or empty"
                    f" — some domains may have been silently skipped"
                    f" (check for stale _population_runs staging directories)",
                    flush=True,
                )
            else:
                with dag_nodes.open("r", encoding="utf-8-sig", newline="") as f:
                    rdr = csv.reader(f)
                    next(rdr, None)  # skip header
                    has_data = next(rdr, None) is not None
                if not has_data:
                    print(
                        f"[WARN orchestrator] segment={sid} bundle_dag_nodes.csv missing or empty"
                        f" — some domains may have been silently skipped"
                        f" (check for stale _population_runs staging directories)",
                        flush=True,
                    )
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
    non_bundle = [r for r in registry if r.get("run_type", "").strip() != "bundle"]
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
    args = ap.parse_args()
    sys.exit(run_orchestrator(args))


if __name__ == "__main__":
    main()
