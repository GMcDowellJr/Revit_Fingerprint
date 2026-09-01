#!/usr/bin/env python3
"""tools/compare_reference_multi.py

Fan-out driver over tools/compare_reference.py: crosses a small CSV of
references (reference_segment, reference) against a list of target
segments, running each (reference_segment, reference, target_segment)
combination as its own `python tools/compare_reference.py` subprocess, then
aggregating every combo's own reference_comparison_summary.csv into one
unioned multi_reference_comparison_summary.csv.

This module implements NO comparison mathematics of its own -- it is pure
fan-out orchestration and CSV/JSON aggregation over the existing, unmodified
tools/compare_reference.py (never imported, never modified: always invoked
as a subprocess, exactly the way run_segment_orchestrator.py drives other
tools/*.py scripts). See docs/reference_comparison_tool.md and
audit_results/compare_reference_multi_step0_findings.md for the read-only
audit this driver's design is built against.

Child return codes and what they mean here (see the Step 0 findings doc,
"Exit codes are not strictly {0, 2}"): compare_reference.py's own main()
returns 0 (comparison ran; see the child's own manifest for ok/degraded) or
2 (a CompareReferenceError -- segment not found, materialization incomplete,
reference/target unresolved, etc; still writes a header-only
reference_comparison_summary.csv and a manifest via write_top_level_blocked).
Any OTHER return code is not one compare_reference.py's own main() ever
returns deliberately -- it can only arise from an uncaught exception inside
it (Python's default exit code 1) or a subprocess that failed to launch at
all -- and is treated here as a distinct "crashed" outcome, never silently
folded into "blocked".
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from bundle_analysis.common import atomic_write_csv, read_csv_rows  # noqa: E402

COMPARE_REFERENCE_SCRIPT = SCRIPT_DIR / "compare_reference.py"

RUN_REPORT_FILENAME = "compare_reference_multi_run_report.json"
MULTI_SUMMARY_FILENAME = "multi_reference_comparison_summary.csv"

# Filenames compare_reference.py itself owns (tools/compare_reference.py's
# own SUMMARY_FILENAME/MANIFEST_FILENAME constants) -- duplicated here as
# plain string literals rather than imported, since this driver never
# imports compare_reference.py (subprocess-only, per the module docstring).
CHILD_SUMMARY_FILENAME = "reference_comparison_summary.csv"
CHILD_MANIFEST_FILENAME = "reference_comparison_report.json"

REQUIRED_REFERENCES_HEADER = ["reference_segment", "reference"]

COMBO_STATUS_OK = "ok"
COMBO_STATUS_BLOCKED = "blocked"
COMBO_STATUS_CRASHED = "crashed"

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]+")

# ProcessPoolExecutor raises ValueError when max_workers > 61 on Windows
# (WaitForMultipleObjects handle-count limit). Mirrors
# tools/compare_cross_segment.py's own _WIN32_MAX_WORKERS/resolve_worker_count
# convention exactly (same default, same headroom, same cap) -- duplicated
# rather than imported, since compare_cross_segment.py is out of scope for
# this driver to depend on (see docs/reference_comparison_tool.md).
_WIN32_MAX_WORKERS = 61


def resolve_worker_count(value: str, headroom: int = 2) -> int:
    """Resolve --workers, accepting either an int or the literal string 'auto'."""
    if str(value).strip().lower() == "auto":
        cpu_count = os.cpu_count()
        workers = max(1, cpu_count - headroom) if cpu_count else 4
        if sys.platform == "win32":
            workers = min(workers, _WIN32_MAX_WORKERS)
        return workers
    return int(value)


# Bounds each sanitized path component well under common filesystem
# per-component limits (e.g. 255 bytes on ext4/NTFS): three components at
# _MAX_COMPONENT_LENGTH + 1 ("_") + 8 (hash) chars each, joined by "__",
# stays far short of that even before accounting for --out-root's own length.
_MAX_COMPONENT_LENGTH = 60


def _sanitize_path_component(value: str) -> str:
    """Make an arbitrary string (a --reference/segment selector) safe to use
    as one filesystem path component: collapse any run of characters outside
    [A-Za-z0-9._-] to a single underscore, strip leading/trailing dots or
    underscores, and bound the result to _MAX_COMPONENT_LENGTH characters
    (appending an 8-hex-char digest of the original value when truncated, so
    two different long values that happen to share a long common prefix
    still sanitize to different components rather than colliding silently).
    --reference selectors are resolved against a segment's own
    file_metadata.csv export_run_id (compare_reference.py::resolve_export_run_id),
    an unconstrained, often filename-derived string that can contain spaces,
    parentheses, or other characters unsafe as a bare directory-name
    component, and can in principle be long enough to overflow a filesystem's
    per-component length limit once combined with the other two selectors.
    """
    sanitized = _SANITIZE_RE.sub("_", value).strip("._")
    sanitized = sanitized or "_"
    if len(sanitized) > _MAX_COMPONENT_LENGTH:
        digest = hashlib.md5(value.encode("utf-8", errors="surrogateescape")).hexdigest()[:8]
        sanitized = f"{sanitized[:_MAX_COMPONENT_LENGTH]}_{digest}"
    return sanitized


@dataclass(frozen=True)
class ReferenceRow:
    reference_segment: str
    reference: str


def read_references_csv(path: Path) -> List[ReferenceRow]:
    """Read and validate --references. Fails fast (SystemExit) on a missing
    file, a header that isn't exactly ['reference_segment', 'reference']
    (no extra/reordered columns silently accepted), a malformed data row, or
    zero data rows after the header. An exact duplicate (reference_segment,
    reference) row is silently deduplicated (first occurrence kept, stable
    order preserved) -- see build_combos()'s own docstring for why this
    matters: without it, two identical Combo objects would race on the same
    --out-dir under parallel workers.
    """
    if not path.is_file():
        raise SystemExit(f"[compare_reference_multi][error] --references file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise SystemExit(
                f"[compare_reference_multi][error] --references CSV is empty: {path}. Expected "
                f"header exactly {','.join(REQUIRED_REFERENCES_HEADER)!r}."
            )
        if header != REQUIRED_REFERENCES_HEADER:
            raise SystemExit(
                f"[compare_reference_multi][error] --references CSV {path} has header "
                f"{','.join(header)!r}; expected exactly {','.join(REQUIRED_REFERENCES_HEADER)!r} "
                "(no extra or reordered columns)."
            )
        rows: List[ReferenceRow] = []
        seen_rows = set()
        for line_no, raw in enumerate(reader, start=2):
            if not raw or all(not (cell or "").strip() for cell in raw):
                continue
            if len(raw) != 2:
                raise SystemExit(
                    f"[compare_reference_multi][error] --references CSV {path} line {line_no} has "
                    f"{len(raw)} field(s); expected exactly 2 (reference_segment,reference): {raw!r}"
                )
            reference_segment, reference = (raw[0] or "").strip(), (raw[1] or "").strip()
            if not reference_segment or not reference:
                raise SystemExit(
                    f"[compare_reference_multi][error] --references CSV {path} line {line_no} has a "
                    f"blank reference_segment or reference value: {raw!r}"
                )
            dedupe_key = (reference_segment, reference)
            if dedupe_key in seen_rows:
                continue
            seen_rows.add(dedupe_key)
            rows.append(ReferenceRow(reference_segment=reference_segment, reference=reference))
    if not rows:
        raise SystemExit(
            f"[compare_reference_multi][error] --references CSV {path} has zero data rows after "
            "the header -- nothing to run."
        )
    return rows


def read_target_segments(spec: str) -> List[str]:
    """--target-segments accepts either an existing newline-delimited file
    (blank lines and '#'-prefixed comment lines skipped) or a comma-separated
    inline list. Fails fast on zero resulting segments either way. An exact
    duplicate segment name is silently deduplicated (first occurrence kept,
    stable order preserved) -- same rationale as read_references_csv()'s own
    dedup: avoids handing build_combos() two identical Combo objects that
    would race on the same --out-dir.
    """
    path = Path(spec)
    if path.is_file():
        lines = path.read_text(encoding="utf-8-sig").splitlines()
        raw_segments = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    else:
        raw_segments = [s.strip() for s in spec.split(",") if s.strip()]
    segments: List[str] = []
    seen = set()
    for s in raw_segments:
        if s in seen:
            continue
        seen.add(s)
        segments.append(s)
    if not segments:
        raise SystemExit(
            f"[compare_reference_multi][error] --target-segments {spec!r} resolved to zero target "
            "segments (not an existing file, and no comma-separated entries)."
        )
    return segments


@dataclass(frozen=True)
class Combo:
    reference_segment: str
    reference: str
    target_segment: str
    out_dir: Path

    @property
    def combo_key(self) -> str:
        return f"{self.reference_segment}::{self.reference}::{self.target_segment}"


def build_combos(references: Sequence[ReferenceRow], target_segments: Sequence[str], out_root: Path) -> List[Combo]:
    """Cross-product every reference row with every target segment. Each
    combo's --out-dir is a deterministic path under out_root, derived from
    the sanitized (reference_segment, reference, target_segment) triple.

    Fails fast (SystemExit) rather than silently proceeding in two distinct
    unsafe cases -- both would otherwise let two Combo objects share one
    --out-dir, which under ProcessPoolExecutor's parallel workers means both
    children can concurrently clear/write the same directory, and the
    aggregator would double-count or silently drop one side's result
    (worker_results is keyed by combo_key, so only the last write survives):

    - Two IDENTICAL combos (same reference_segment, reference, and
      target_segment) -- read_references_csv()/read_target_segments()
      already dedupe their own inputs, so this only fires if build_combos()
      is ever called directly with un-deduplicated data.
    - Two DISTINCT combos whose sanitized selectors happen to collide on the
      same directory name (e.g. two references differing only in characters
      _sanitize_path_component() collapses, or differing only in case --
      collision detection compares case-folded paths specifically so this is
      still caught on a case-insensitive filesystem, i.e. Windows or a
      default macOS volume, where "Ref.rvt" and "ref.rvt" would otherwise
      resolve to the same on-disk directory despite sanitizing to different
      strings).
    """
    combos: List[Combo] = []
    for ref_row in references:
        for target_segment in target_segments:
            dir_name = "__".join(
                _sanitize_path_component(part)
                for part in (ref_row.reference_segment, ref_row.reference, target_segment)
            )
            combos.append(
                Combo(
                    reference_segment=ref_row.reference_segment,
                    reference=ref_row.reference,
                    target_segment=target_segment,
                    out_dir=out_root / dir_name,
                )
            )
    seen_combo_keys: Dict[str, str] = {}
    seen_out_dirs: Dict[str, str] = {}
    for combo in combos:
        combo_key = combo.combo_key
        if combo_key in seen_combo_keys:
            raise SystemExit(
                f"[compare_reference_multi][error] duplicate combo "
                f"(reference_segment, reference, target_segment) = {combo_key!r} appears more than "
                "once after deduplication -- this should not happen; if calling build_combos() "
                "directly, deduplicate references/target_segments first."
            )
        seen_combo_keys[combo_key] = combo_key

        # Case-folded, not raw str(): two out_dir paths differing only in
        # case are the same directory on Windows/case-insensitive macOS, and
        # must be caught here even though they're distinct Python strings.
        out_dir_key = str(combo.out_dir).casefold()
        if out_dir_key in seen_out_dirs and seen_out_dirs[out_dir_key] != combo_key:
            raise SystemExit(
                f"[compare_reference_multi][error] two distinct combos sanitize to the same "
                f"--out-dir (case-insensitively) {out_dir_key!r}: {seen_out_dirs[out_dir_key]!r} and "
                f"{combo_key!r}. Rename one of the conflicting reference_segment/reference/"
                "target-segment values to disambiguate."
            )
        seen_out_dirs[out_dir_key] = combo_key
    return combos


def classify_combo_status(returncode: int) -> str:
    if returncode == 0:
        return COMBO_STATUS_OK
    if returncode == 2:
        return COMBO_STATUS_BLOCKED
    return COMBO_STATUS_CRASHED


def _run_combo_worker(
    compare_reference_script: str,
    segments_root: str,
    registry_file: str,
    reference_segment: str,
    reference: str,
    target_segment: str,
    out_dir: str,
    overwrite: bool,
    domains: Optional[str],
    purge_view: str,
    include_name_overlap: bool,
    repo_root: str,
) -> Dict[str, object]:
    """Run one (reference_segment, reference, target_segment) combo as its
    own `python tools/compare_reference.py` subprocess. Module-level (not a
    closure) and taking only plain str/bool/None arguments so it is
    picklable for ProcessPoolExecutor -- mirrors
    tools/compare_cross_segment.py's own worker-submission shape.
    """
    cmd = [
        sys.executable,
        compare_reference_script,
        "--segments-root", segments_root,
        "--registry-file", registry_file,
        "--reference-segment", reference_segment,
        "--target-segment", target_segment,
        "--reference", reference,
        "--out-dir", out_dir,
        "--purge-view", purge_view,
    ]
    if overwrite:
        cmd.append("--overwrite")
    if domains:
        cmd.extend(["--domains", domains])
    if include_name_overlap:
        cmd.append("--include-name-overlap")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=repo_root)
    stderr_lines = (result.stderr or "").splitlines()
    stderr_tail = "\n".join(stderr_lines[-20:])
    return {"returncode": result.returncode, "stderr_tail": stderr_tail}


def read_child_manifest(out_dir: Path) -> Optional[Dict[str, object]]:
    manifest_path = out_dir / CHILD_MANIFEST_FILENAME
    if not manifest_path.is_file():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def clear_stale_combo_output(out_dir: Path) -> None:
    """Remove any pre-existing content at a combo's --out-dir before its
    child subprocess is even submitted, so anything found there AFTER the
    run (summary CSV, manifest, detail CSV, ...) is guaranteed to have been
    produced by THIS invocation -- never a leftover from an earlier one into
    the same --out-root.

    This replaces an earlier wall-clock-mtime-based staleness check that was
    itself unsound both directions: a genuinely fresh file's mtime can be
    truncated by filesystem timestamp-resolution rounding to appear to
    predate the run, and a genuinely stale file from a run that finished
    less than the check's own tolerance-buffer before this run started would
    still pass it. Not trusting timestamps for this at all -- clearing
    first, so presence after the run is itself the freshness signal -- has
    neither failure mode. compare_reference.py's own prepare_out_dir() would
    perform an equivalent clear on a successful run anyway (see
    audit_results/compare_reference_multi_step0_findings.md finding #3); the
    gap this closes is specifically the child crashing before reaching that
    point (a launch failure, or the process being killed mid-run), which
    would otherwise leave an untouched, stale directory behind.
    """
    if out_dir.exists():
        shutil.rmtree(out_dir)


def aggregate_summaries(
    combos: Sequence[Combo], report_entries: Sequence[Dict[str, object]], out_root: Path
) -> Tuple[Optional[Path], int, int, List[str]]:
    """Union every combo's own reference_comparison_summary.csv into one
    multi_reference_comparison_summary.csv, tagging each row with the
    driving `reference` selector and the child manifest's
    `reference_segment_id` -- neither is a column in the child's own summary
    CSV (reference_segment_id lives only in that child's manifest/
    diagnostics JSON; `reference` isn't recorded in any child output at all;
    see audit_results/compare_reference_multi_step0_findings.md findings #4/#5).
    `segment_id` (the target segment) is already a column on every child
    summary row, so it is carried through unchanged.

    A combo whose reference_comparison_summary.csv is genuinely missing on
    disk (a "crashed" combo that never reached compare_reference.py's own
    output-writing code, or the rarer pre-flight out-dir-safety failure) is
    skipped and noted, not treated as an aggregation failure -- a combo that
    blocked cleanly (returncode 2) still writes a header-only summary CSV
    and is not "missing" in that sense; it simply contributes zero rows.
    Every combo's out_dir is cleared before its child is submitted (see
    clear_stale_combo_output()), so a summary file found here is always
    current-run data, never a stale leftover.

    Returns out_path=None (and combos_included=0, rows=0) when literally no
    combo produced a summary file at all. This is NOT itself an error: every
    combo could be legitimately, cleanly blocked (returncode 2) yet still
    write no summary, in the rare case where compare_reference.py's own
    pre-flight out-dir-safety check refuses before reaching
    write_top_level_blocked (see the Step 0 findings doc, finding #2) --
    that is a normal "blocked", not a "crashed", outcome, and must not
    prevent the driver's own exit code from correctly reflecting zero
    crashed combos. main()'s own combos_crashed count (not whether this
    function found any data) is what determines the driver's exit code.
    """
    entry_by_combo_key = {entry["combo_key"]: entry for entry in report_entries}
    base_fieldnames: Optional[List[str]] = None
    all_rows: List[Dict[str, str]] = []
    combos_included = 0
    combos_skipped: List[str] = []

    for combo in combos:
        summary_path = combo.out_dir / CHILD_SUMMARY_FILENAME
        if not summary_path.is_file():
            combos_skipped.append(combo.combo_key)
            continue
        combos_included += 1
        rows = read_csv_rows(summary_path)
        if base_fieldnames is None:
            with summary_path.open("r", encoding="utf-8-sig", newline="") as f:
                header = next(csv.reader(f), None)
            if header:
                base_fieldnames = header
        entry = entry_by_combo_key.get(combo.combo_key, {})
        for row in rows:
            tagged = dict(row)
            tagged["reference"] = combo.reference
            tagged["reference_segment_id"] = entry.get("reference_segment_id", "")
            all_rows.append(tagged)

    if base_fieldnames is None:
        return None, 0, 0, combos_skipped

    fieldnames = ["reference", "reference_segment_id"] + base_fieldnames
    out_path = out_root / MULTI_SUMMARY_FILENAME
    atomic_write_csv(out_path, fieldnames, all_rows)
    return out_path, len(all_rows), combos_included, combos_skipped


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="compare_reference_multi.py",
        description=(
            "Fan out tools/compare_reference.py across an M x N grid: a CSV of references "
            "crossed against a list of target segments, running each combination as its own "
            "compare_reference.py subprocess, then aggregating every combo's "
            "reference_comparison_summary.csv into one unioned "
            "multi_reference_comparison_summary.csv. Implements no comparison mathematics of "
            "its own. See docs/reference_comparison_tool.md."
        ),
    )
    ap.add_argument("--segments-root", required=True, type=Path, help="Passed through to every child compare_reference.py call.")
    ap.add_argument("--registry-file", required=True, type=Path, help="Passed through to every child compare_reference.py call.")
    ap.add_argument(
        "--references",
        required=True,
        type=Path,
        help="CSV with header exactly 'reference_segment,reference' -- one row per reference to run.",
    )
    ap.add_argument(
        "--target-segments",
        required=True,
        help="Newline-delimited file, OR comma-separated inline list, of target segment folder names.",
    )
    ap.add_argument(
        "--out-root",
        required=True,
        type=Path,
        help="Parent output directory; one subdirectory per (reference_segment, reference, "
        "target_segment) combo is created under this root and passed as that combo's --out-dir.",
    )
    ap.add_argument(
        "--workers",
        default="auto",
        help="Max parallel compare_reference.py subprocess workers, or 'auto' (default) to derive "
        "from CPU count, mirroring tools/compare_cross_segment.py's --workers convention.",
    )
    ap.add_argument("--overwrite", action="store_true", help="Passed through to every child compare_reference.py call.")
    ap.add_argument(
        "--domains",
        default=None,
        help="Passed through to every child compare_reference.py call, identically across the whole grid.",
    )
    ap.add_argument(
        "--purge-view", choices=["all", "used", "both"], default="both", help="Passed through to every child compare_reference.py call."
    )
    ap.add_argument("--include-name-overlap", action="store_true", help="Passed through to every child compare_reference.py call.")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    segments_root = Path(args.segments_root).resolve()
    registry_file = Path(args.registry_file).resolve()
    out_root = Path(args.out_root).resolve()

    workers = resolve_worker_count(args.workers)
    if workers < 1:
        raise SystemExit("[compare_reference_multi][error] --workers must be >= 1")

    references = read_references_csv(Path(args.references).resolve())
    target_segments = read_target_segments(args.target_segments)
    combos = build_combos(references, target_segments, out_root)

    out_root.mkdir(parents=True, exist_ok=True)

    # Clear each combo's out_dir before its child is ever submitted -- see
    # clear_stale_combo_output()'s own docstring for why this is done
    # unconditionally here rather than relying on wall-clock timestamps.
    for combo in combos:
        clear_stale_combo_output(combo.out_dir)

    print(
        f"[compare_reference_multi] running {len(combos)} combo(s) "
        f"({len(references)} reference(s) x {len(target_segments)} target segment(s)) with workers={workers}"
    )

    worker_results: Dict[str, Dict[str, object]] = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_combo = {
            executor.submit(
                _run_combo_worker,
                str(COMPARE_REFERENCE_SCRIPT),
                str(segments_root),
                str(registry_file),
                combo.reference_segment,
                combo.reference,
                combo.target_segment,
                str(combo.out_dir),
                bool(args.overwrite),
                args.domains,
                args.purge_view,
                bool(args.include_name_overlap),
                str(REPO_ROOT),
            ): combo
            for combo in combos
        }
        for future in as_completed(future_to_combo):
            combo = future_to_combo[future]
            try:
                worker_result = future.result()
            except Exception as exc:  # subprocess launch failure inside the worker process itself
                worker_result = {
                    "returncode": -1,
                    "stderr_tail": f"driver-side exception launching subprocess: {exc}",
                }
            worker_results[combo.combo_key] = worker_result
            status = classify_combo_status(worker_result["returncode"])
            print(
                f"[compare_reference_multi]   combo={combo.combo_key} "
                f"returncode={worker_result['returncode']} status={status}"
            )

    report_entries: List[Dict[str, object]] = []
    for combo in combos:
        worker_result = worker_results[combo.combo_key]
        returncode = worker_result["returncode"]
        combo_status = classify_combo_status(returncode)
        manifest = read_child_manifest(combo.out_dir)
        report_entries.append(
            {
                "combo_key": combo.combo_key,
                "reference_segment": combo.reference_segment,
                "reference": combo.reference,
                "target_segment": combo.target_segment,
                "out_dir": str(combo.out_dir),
                "returncode": returncode,
                "combo_status": combo_status,
                "aggregate_comparison_status": (manifest or {}).get("aggregate_comparison_status", ""),
                "reference_segment_id": (manifest or {}).get("reference_segment_id", ""),
                "resolved_reference_export_run_id": (manifest or {}).get("resolved_reference_export_run_id", ""),
                "segment_id": (manifest or {}).get("segment_id", ""),
                "stderr_tail": worker_result.get("stderr_tail", ""),
            }
        )

    status_counts = Counter(entry["combo_status"] for entry in report_entries)
    run_report = {
        "tool": "tools/compare_reference_multi.py",
        "combos_total": len(combos),
        "combos_ok": status_counts.get(COMBO_STATUS_OK, 0),
        "combos_blocked": status_counts.get(COMBO_STATUS_BLOCKED, 0),
        "combos_crashed": status_counts.get(COMBO_STATUS_CRASHED, 0),
        "references_count": len(references),
        "target_segments_count": len(target_segments),
        "workers": workers,
        "combos": report_entries,
    }
    (out_root / RUN_REPORT_FILENAME).write_text(json.dumps(run_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary_path, rows_written, combos_included, combos_skipped = aggregate_summaries(combos, report_entries, out_root)

    print(
        f"[compare_reference_multi] wrote {RUN_REPORT_FILENAME} "
        f"(ok={run_report['combos_ok']} blocked={run_report['combos_blocked']} crashed={run_report['combos_crashed']})"
    )
    if summary_path is not None:
        print(
            f"[compare_reference_multi] wrote {summary_path} "
            f"({rows_written} rows from {combos_included}/{len(combos)} combo(s))"
        )
    else:
        print(
            f"[compare_reference_multi] no combo produced a {CHILD_SUMMARY_FILENAME} to aggregate -- "
            f"{MULTI_SUMMARY_FILENAME} was not written. Check {RUN_REPORT_FILENAME}."
        )
    if combos_skipped:
        print(
            f"[compare_reference_multi] {len(combos_skipped)} combo(s) had no "
            f"{CHILD_SUMMARY_FILENAME} to aggregate (crashed before writing output, or cleanly "
            f"blocked at a pre-flight stage that itself writes no output): {combos_skipped}"
        )

    # Exit code reflects combos_crashed only -- an all-cleanly-blocked grid
    # (every child returned 2, none crashed) is still a clean run, even if
    # aggregate_summaries() had nothing to write (see its own docstring).
    return 0 if status_counts.get(COMBO_STATUS_CRASHED, 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
