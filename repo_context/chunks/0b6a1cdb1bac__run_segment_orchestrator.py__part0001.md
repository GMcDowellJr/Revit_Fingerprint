# Chunk of tools/run_segment_orchestrator.py

- Source relative path: `tools/run_segment_orchestrator.py`
- Chunk: 1 of 4
- Original line range: 1-410
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: load_manifest, load_registry, load_membership, write_registry_atomic, utc_now_iso, compute_worker_split, _write_run_summary, run_step, run_step_capture, run_step_log, _preshard_one_shard
- Source SHA-256: c1d79ae240bf0af45e5deb47ebd929be191e1d6bb8a42be87fe41cbe5dfc7646
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| """
     3| tools/run_segment_orchestrator.py
     4| 
     5| Reads segment_manifest.csv and run_registry.csv, executes patterns then bundle
     6| stages for each bundle segment in level order, writes outputs to per-segment
     7| folders under a segments/ root, and updates the registry after each run.
     8| 
     9| Usage:
    10|     python tools/run_segment_orchestrator.py \\
    11|         --manifest-file segment_manifest.csv \\
    12|         --registry-file run_registry.csv \\
    13|         --records-dir /path/to/results/records \\
    14|         --exports-dir /path/to/exports \\
    15|         --segments-root /path/to/segments \\
    16|         --repo-root /path/to/repo \\
    17|         --join-policy /path/to/domain_join_key_policies.json
    18| """
    19| 
    20| from __future__ import annotations
    21| 
    22| import argparse
    23| import csv
    24| import hashlib
    25| import math
    26| import os
    27| import shutil
    28| import subprocess
    29| import sys
    30| import tempfile
    31| import threading
    32| import time
    33| from concurrent.futures import ThreadPoolExecutor, as_completed
    34| from datetime import datetime, timezone
    35| from pathlib import Path
    36| from typing import Any, Dict, List, Optional
    37| 
    38| # Allow import of bundle_analysis package from the same tools/ directory
    39| sys.path.insert(0, str(Path(__file__).resolve().parent))
    40| from bundle_analysis.common import atomic_write_csv, retry_fs_op
    41| from bundle_analysis.name_projection_adapter import annotate_name_target_combined_files, normalize_export_run_id
    42| from build_results_registry import write_results_registry
    43| 
    44| VALID_COMPARISON_TARGETS = {"config", "name", "both"}
    45| 
    46| # Maximum destination file handles open simultaneously during preshard.
    47| # Keeps fd usage well below typical OS limits (1024) regardless of segment count.
    48| # Each batch re-streams the source file once, so total passes = ceil(N/batch).
    49| _PRESHARD_BATCH = 64
    50| 
    51| _CORPUS_PRESHARD_MARKER = ".preshard_complete_corpus"
    52| 
    53| BI_MERGE_FILES = [
    54|     "membership_matrix.csv",
    55|     "bundles.csv",
    56|     "bundle_dag_nodes.csv",
    57|     "bundle_dag_edges.csv",
    58|     "bundle_dag_differences.csv",
    59|     "pattern_bundle_classification.csv",
    60|     "bundle_membership.csv",
    61|     "file_bundle_classification.csv",
    62|     "bundle_file_membership.csv",
    63|     "scope_registry.csv",
    64| ]
    65| 
    66| 
    67| # ── CSV helpers ──────────────────────────────────────────────────────────────
    68| 
    69| def load_manifest(path: Path) -> Dict[str, dict]:
    70|     """Load segment_manifest.csv keyed by segment_id."""
    71|     manifest: Dict[str, dict] = {}
    72|     with path.open("r", encoding="utf-8-sig", newline="") as f:
    73|         for row in csv.DictReader(f):
    74|             sid = row.get("segment_id", "").strip()
    75|             if sid:
    76|                 manifest[sid] = row
    77|     return manifest
    78| 
    79| 
    80| def load_registry(path: Path) -> List[dict]:
    81|     """Load run_registry.csv as a list of row dicts."""
    82|     with path.open("r", encoding="utf-8-sig", newline="") as f:
    83|         return list(csv.DictReader(f))
    84| 
    85| 
    86| def load_membership(path: Path) -> Dict[str, List[str]]:
    87|     """Load segment_membership.csv grouped by segment_id -> sorted export_run_ids.
    88| 
    89|     Replaces the old segment_manifest.csv `export_run_ids` pipe-delimited column,
    90|     which could exceed spreadsheet cell limits for large populations.
    91|     """
    92|     membership: Dict[str, List[str]] = {}
    93|     with path.open("r", encoding="utf-8-sig", newline="") as f:
    94|         grouped: Dict[str, List[str]] = {}
    95|         for row in csv.DictReader(f):
    96|             sid = (row.get("segment_id") or "").strip()
    97|             eid = (row.get("export_run_id") or "").strip()
    98|             if sid and eid:
    99|                 grouped.setdefault(sid, []).append(eid)
   100|         for sid, eids in grouped.items():
   101|             membership[sid] = sorted(eids)
   102|     return membership
   103| 
   104| 
   105| def write_registry_atomic(path: Path, rows: List[dict]) -> None:
   106|     """Write registry rows atomically via temp-file + replace."""
   107|     if not rows:
   108|         return
   109|     fieldnames = list(rows[0].keys())
   110|     tmp = path.with_suffix(".tmp")
   111|     with tmp.open("w", newline="", encoding="utf-8") as f:
   112|         writer = csv.DictWriter(f, fieldnames=fieldnames)
   113|         writer.writeheader()
   114|         writer.writerows(rows)
   115|     tmp.replace(path)
   116| 
   117| 
   118| def utc_now_iso() -> str:
   119|     return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
   120| 
   121| 
   122| def compute_worker_split(
   123|     total_budget: Optional[int] = None,
   124|     headroom: int = 2,
   125|     segment_workers: Optional[int] = None,
   126| ) -> tuple[int, int]:
   127|     """Returns (segment_workers, domain_workers) whose product approximates
   128|     available logical cores minus headroom.
   129| 
   130|     If segment_workers is None (auto mode), both values are derived from the
   131|     budget using a sqrt-biased split, favoring segment-level concurrency since
   132|     segments are more I/O-independent than domain workers within one segment's
   133|     bundle stage.
   134| 
   135|     If segment_workers is given (explicit --workers N), domain_workers is
   136|     solved as budget // segment_workers so the bundle-stage pool stays
   137|     coordinated to the same CPU budget instead of defaulting to
   138|     run_bundle_analysis.py's own fixed default of 4 — which would otherwise let
   139|     total concurrency grow unbounded as N grows (N x 4 instead of ~N).
   140|     """
   141|     if total_budget is None:
   142|         cpu_count = os.cpu_count()
   143|         if not cpu_count:
   144|             # os.cpu_count() returned None (restricted/containerized environment) —
   145|             # fall back to the existing hardcoded default of 4 for both values.
   146|             if segment_workers is None:
   147|                 return 4, 4
   148|             total_budget = 4
   149|         else:
   150|             total_budget = max(1, cpu_count - headroom)
   151| 
   152|     if segment_workers is not None:
   153|         domain_workers = max(1, total_budget // max(1, segment_workers))
   154|         return segment_workers, domain_workers
   155| 
   156|     domain_workers = max(1, round(math.sqrt(total_budget) * 0.8))
   157|     segment_workers = max(1, total_budget // domain_workers)
   158|     return segment_workers, domain_workers
   159| 
   160| 
   161| def _write_run_summary(
   162|     segments_root: Path,
   163|     run_start_utc: str,
   164|     run_end_utc: str,
   165|     total_elapsed_s: int,
   166|     segment_results: List[Dict],
   167|     workers: int,
   168|     bundle_workers: int,
   169|     workers_auto: bool,
   170| ) -> Path:
   171|     """Write run_summary.txt to segments_root atomically (temp + replace)."""
   172|     out_path = segments_root / "run_summary.txt"
   173|     tmp_path = segments_root / "run_summary.txt.tmp"
   174| 
   175|     n_complete = sum(1 for r in segment_results if r.get("status") == "complete")
   176|     n_failed = sum(1 for r in segment_results if r.get("status") == "failed")
   177|     n_skipped = sum(1 for r in segment_results if r.get("status") == "skipped")
   178|     segments_run = n_complete + n_failed
   179| 
   180|     total_min = total_elapsed_s / 60.0
   181| 
   182|     # Per-segment timing table
   183|     col_w = max((len(r.get("segment_id", "")) for r in segment_results), default=30)
   184|     col_w = max(col_w, 30)
   185|     header_fmt = f"{{:<{col_w}}}  {{:>3}}  {{:>5}}  {{:>7}}  {{:>8}}  {{:>6}}  {{:>8}}  {{:>5}}  {{}}"
   186|     row_fmt    = f"{{:<{col_w}}}  {{:>3}}  {{:>5}}  {{:>7}}  {{:>8}}  {{:>6}}  {{:>8}}  {{:>5}}  {{}}"
   187| 
   188|     seg_lines: List[str] = [
   189|         header_fmt.format("segment", "lvl", "files", "prepare", "patterns", "bundle", "bi_merge", "total", "status"),
   190|     ]
   191|     for r in sorted(segment_results, key=lambda x: (-x.get("patterns_s", 0), x.get("segment_id", ""))):
   192|         sid     = r.get("segment_id", "")
   193|         lvl     = r.get("level", 0)
   194|         files   = r.get("files", 0)
   195|         prep    = r.get("prepare_s", 0)
   196|         pat     = r.get("patterns_s", 0)
   197|         bun     = r.get("bundle_s", 0)
   198|         mrg     = r.get("bi_merge_s", 0)
   199|         tot     = r.get("total_s", 0)
   200|         status  = "✓" if r.get("status") == "complete" else "✗"
   201|         seg_lines.append(
   202|             row_fmt.format(sid, lvl, files, f"{prep}s", f"{pat}s", f"{bun}s", f"{mrg}s", f"{tot}s", status)
   203|         )
   204| 
   205|     # Failed segments block
   206|     failed_lines: List[str] = []
   207|     for r in segment_results:
   208|         if r.get("status") == "failed":
   209|             note = r.get("failure_note", "").split("\n")[0][:120]
   210|             failed_lines.append(
   211|                 f"  {r.get('segment_id', ''):<{col_w}}  {note}"
   212|             )
   213| 
   214|     # Top-5 patterns timing — sub-breakdown only for the 3 slowest by patterns_s
   215|     sorted_by_pat = sorted(
   216|         [r for r in segment_results if r.get("patterns_s", 0) > 0],
   217|         key=lambda x: -x.get("patterns_s", 0),
   218|     )
   219|     top3_sids = {r["segment_id"] for r in sorted_by_pat[:3]}
   220|     timing_blocks: List[str] = []
   221|     for r in sorted_by_pat[:5]:
   222|         sid = r.get("segment_id", "")
   223|         pat_s = r.get("patterns_s", 0)
   224|         top5 = r.get("patterns_top5", [])
   225|         timing_blocks.append(f"[segment: {sid}  patterns={pat_s}s]")
   226|         for ln in top5:
   227|             if sid in top3_sids:
   228|                 timing_blocks.append(f"  {ln}")
   229|             else:
   230|                 # Truncate to domain name + total only (drop sub-breakdown fields)
   231|                 parts = ln.split()
   232|                 short_parts = [p for p in parts if p.startswith("domain=") or p.startswith("elapsed=")]
   233|                 timing_blocks.append(f"  {' '.join(short_parts)}")
   234| 
   235|     # Totals
   236|     total_prep_s    = sum(r.get("prepare_s", 0) for r in segment_results)
   237|     total_pat_s     = sum(r.get("patterns_s", 0) for r in segment_results)
   238|     total_bun_s     = sum(r.get("bundle_s", 0) for r in segment_results)
   239|     total_mrg_s     = sum(r.get("bi_merge_s", 0) for r in segment_results)
   240|     total_work_s    = sum(r.get("total_s", 0) for r in segment_results)
   241|     avg_pat         = total_pat_s // segments_run if segments_run > 0 else 0
   242|     avg_bun         = total_bun_s // segments_run if segments_run > 0 else 0
   243|     avg_mrg         = total_mrg_s // segments_run if segments_run > 0 else 0
   244|     parallelism_eff = total_work_s / total_elapsed_s if total_elapsed_s > 0 else 0.0
   245| 
   246|     lines: List[str] = [
   247|         "Revit Fingerprint — Run Summary",
   248|         "================================",
   249|         f"run_start_utc : {run_start_utc}",
   250|         f"run_end_utc   : {run_end_utc}",
   251|         f"total_elapsed : {total_elapsed_s}s ({total_min:.1f} min)",
   252|         f"workers       : {workers}",
   253|         f"worker_split  : segment_workers={workers} domain_workers={bundle_workers}"
   254|         f" (mode={'auto' if workers_auto else 'explicit'})",
   255|         f"segments_run  : {segments_run}",
   256|         f"  complete    : {n_complete}",
   257|         f"  failed      : {n_failed}",
   258|         f"  skipped     : {n_skipped}",
   259|         "",
   260|         "── Per-segment timing ──────────────────────────────────────────────────────",
   261|     ]
   262|     lines.extend(seg_lines)
   263| 
   264|     if failed_lines:
   265|         lines.append("")
   266|         lines.append("── Failed segments ─────────────────────────────────────────────────────────")
   267|         lines.extend(failed_lines)
   268| 
   269|     if timing_blocks:
   270|         lines.append("")
   271|         lines.append("── Patterns top-5 domains (slowest segments only, top 3 segments by patterns time) ──")
   272|         lines.extend(timing_blocks)
   273| 
   274|     lines += [
   275|         "",
   276|         "── Totals ──────────────────────────────────────────────────────────────────",
   277|         f"total_prepare   : {total_prep_s:>6}s",
   278|         f"total_patterns  : {total_pat_s:>6}s  (avg {avg_pat}s/segment)",
   279|         f"total_bundle    : {total_bun_s:>6}s  (avg {avg_bun}s/segment)",
   280|         f"total_bi_merge  : {total_mrg_s:>6}s  (avg {avg_mrg}s/segment)",
   281|         f"total_work      : {total_work_s:>6}s  (sum of all segment times, not wall time)",
   282|         f"wall_time       : {total_elapsed_s:>6}s  ({total_min:.1f} min)",
   283|         f"parallelism_eff : {parallelism_eff:.2f}×   (total_work / wall_time)",
   284|         "",
   285|     ]
   286| 
   287|     segments_root.mkdir(parents=True, exist_ok=True)
   288|     tmp_path.write_text("\n".join(lines), encoding="utf-8")
   289|     tmp_path.replace(out_path)
   290|     return out_path
   291| 
   292| 
   293| # ── Subprocess helpers ────────────────────────────────────────────────────────
   294| 
   295| def run_step(cmd: List[str]) -> subprocess.CompletedProcess:
   296|     """Run a subprocess step, capturing stderr, raising on non-zero exit."""
   297|     return subprocess.run(cmd, check=True, capture_output=False, text=True)
   298| 
   299| 
   300| def run_step_capture(cmd: List[str], cwd: Optional[str] = None) -> tuple[int, str, str]:
   301|     """Run a subprocess step, return (returncode, last_20_lines_stderr, full_stderr)."""
   302|     result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
   303|     stderr_lines = (result.stderr or "").splitlines()
   304|     tail = "\n".join(stderr_lines[-20:]) if stderr_lines else ""
   305|     return result.returncode, tail, result.stderr or ""
   306| 
   307| 
   308| def run_step_log(
   309|     cmd: List[str],
   310|     log_path: Path,
   311|     cwd: Optional[str] = None,
   312| ) -> tuple[int, str, str]:
   313|     """Run subprocess writing all output (stdout+stderr) to log_path.
   314|     Returns (returncode, last_20_lines, full_output).
   315|     """
   316|     log_path.parent.mkdir(parents=True, exist_ok=True)
   317|     with log_path.open("w", encoding="utf-8", errors="replace") as log_f:
   318|         result = subprocess.run(
   319|             cmd, stdout=log_f, stderr=subprocess.STDOUT,
   320|             text=True, cwd=cwd,
   321|         )
   322|     with log_path.open("r", encoding="utf-8", errors="replace") as f:
   323|         content = f.read()
   324|     lines = content.splitlines()
   325|     tail = "\n".join(lines[-20:])
   326|     return result.returncode, tail, content
   327| 
   328| 
   329| # ── Record helpers ────────────────────────────────────────────────────────────
   330| 
   331| def _preshard_one_shard(
   332|     shard_file: Path,
   333|     segment_plans: Dict[str, Dict],
   334|     force: bool,
   335| ) -> tuple[str, int, int]:
   336|     """Process one corpus shard file, fan out to all segment shard dirs.
   337|     Returns (shard_name, files_written, files_skipped).
   338|     """
   339|     if not shard_file.is_file() or shard_file.suffix != ".csv":
   340|         return shard_file.name, 0, 0
   341| 
   342|     # Determine which segments need this shard.
   343|     # Skip only completed segments; pending/failed segments always get fresh
   344|     # inputs so retries without --force don't run against stale data.
   345|     segments_to_write = {}
   346|     for sid, plan in segment_plans.items():
   347|         if not force and plan.get("status") == "complete":
   348|             continue
   349|         seg_shard_dir = plan["segment_records_dir"] / "identity_items_by_domain"
   350|         dst = seg_shard_dir / shard_file.name
   351|         segments_to_write[sid] = (plan, seg_shard_dir, dst)
   352| 
   353|     if not segments_to_write:
   354|         return shard_file.name, 0, len(segment_plans)
   355| 
   356|     # Read header once; eid_col is stable across all batches.
   357|     with shard_file.open("r", encoding="utf-8-sig", newline="") as _hf:
   358|         header = next(csv.reader(_hf), None)
   359|     if not header:
   360|         return shard_file.name, 0, len(segment_plans)
   361|     eid_col = header.index("export_run_id") if "export_run_id" in header else None
   362|     if eid_col is None:
   363|         return shard_file.name, 0, len(segment_plans)
   364| 
   365|     # Ensure all destination shard dirs exist before batching.
   366|     seen_dirs: set = set()
   367|     for sid, (plan, seg_shard_dir, dst) in segments_to_write.items():
   368|         if seg_shard_dir not in seen_dirs:
   369|             seg_shard_dir.mkdir(parents=True, exist_ok=True)
   370|             seen_dirs.add(seg_shard_dir)
   371| 
   372|     # Fan out in batches so at most _PRESHARD_BATCH destination handles are
   373|     # open simultaneously.  Each batch re-streams the shard file once.
   374|     seg_items = list(segments_to_write.items())
   375|     for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
   376|         batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])
   377| 
   378|         # Build one-to-many lookup scoped to this batch.
   379|         id_to_targets: Dict[str, List] = {}
   380|         for sid, (plan, seg_shard_dir, dst) in batch.items():
   381|             for eid in plan["allowed_ids"]:
   382|                 id_to_targets.setdefault(eid, []).append(sid)
   383| 
   384|         writers: Dict[str, Any] = {}
   385|         handles: Dict[str, Any] = {}
   386|         try:
   387|             for sid, (plan, seg_shard_dir, dst) in batch.items():
   388|                 fh = dst.open("w", newline="", encoding="utf-8")
   389|                 handles[sid] = fh
   390|                 w = csv.writer(fh)
   391|                 w.writerow(header)
   392|                 writers[sid] = w
   393| 
   394|             with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
   395|                 reader = csv.reader(f)
   396|                 next(reader, None)  # skip header row
   397|                 for row in reader:
   398|                     if len(row) <= eid_col:
   399|                         continue
   400|                     eid = row[eid_col].strip()
   401|                     for sid in id_to_targets.get(eid, ()):
   402|                         if sid in writers:
   403|                             writers[sid].writerow(row)
   404|         finally:
   405|             for fh in handles.values():
   406|                 fh.close()
   407| 
   408|     return shard_file.name, len(segments_to_write), len(segment_plans) - len(segments_to_write)
   409| 
   410| 
```
