#!/usr/bin/env python3
"""
namekey_step0_diagnostics.py

Answers the four empirical questions left BLOCKED in
docs/namekey_crosssegment_step0_findings.md (A.2, C.6, C.7, C.8) in a
single pass against real materialized data. Run this locally, where
Fingerprint_Data actually exists -- it cannot run in a sandboxed session.

Read-only. Does not modify any pipeline file, CSV, or the corpus itself.
Does not touch build_segment_manifest.py or compare_cross_segment.py
(protected files) -- it only imports normalize_export_run_id from
tools/bundle_analysis/name_projection_adapter.py, per finding B.4's
explicit recommendation to reuse the existing fix rather than reinvent it.

Authoritative path convention used below (segment_root / "results" / ...)
is taken directly from tools/compare_reference.py::require_segment_artifacts(),
NOT from the illustrative (unverified) "analysis_v21" path in the findings
doc's own C.6 snippet -- confirm your on-disk layout matches if this errors.

Usage:
    python namekey_step0_diagnostics.py \\
        --repo-root C:\\path\\to\\Revit_Fingerprint \\
        --corpus-root C:\\Users\\gmcdowell\\Documents\\Fingerprint_Data \\
        --segment-root C:\\Users\\gmcdowell\\Documents\\Fingerprint_Data\\segments\\imperial_project_architectural \\
        --name-key-csv C:\\Users\\gmcdowell\\Documents\\Fingerprint_Data\\Results_v21\\name_key\\name_key_results.csv \\
        --domain arrowheads \\
        --out namekey_step0_completion.md
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


def _read_csv(path: Path):
    if not path.is_file():
        raise SystemExit(f"FAIL LOUD: required file not found: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit(f"FAIL LOUD: {path} has no header row.")
        rows = list(reader)
    return reader.fieldnames, rows


def _require_columns(fieldnames, required, path):
    missing = [c for c in required if c not in fieldnames]
    if missing:
        raise SystemExit(
            f"FAIL LOUD: {path} is missing expected column(s) {missing}. "
            f"Actual columns: {fieldnames}. Schema has likely drifted since "
            f"this script was written -- do not guess a substitute column."
        )


def check_a2(corpus_root: Path, records_csv: Path, report: list):
    report.append("## A.2 -- Does name_key_results.csv exist, and is it stale?\n")
    candidates = sorted(corpus_root.rglob("name_key_results.csv"))
    if not candidates:
        report.append(
            "**Result: NOT FOUND anywhere under corpus root.** "
            "No `-NameKey` run has ever completed for this corpus, OR it was "
            "produced somewhere outside corpus_root. Nothing downstream "
            "(C.7/C.8, or Step 1 itself) can proceed until "
            "`apply_name_key_policy.py --export-dir <exports>` is run at least once "
            "(see findings A.3 for exact invocation).\n"
        )
        return []
    report.append(
        f"Found {len(candidates)} candidate file(s) corpus-wide "
        "(one per segment, from _filter_name_key_csv_to_segment(), plus possibly "
        "one corpus-wide unfiltered copy). Listed here for visibility only -- "
        "the segment-local file is what actually gets used (see below), NOT "
        "the corpus-wide newest-by-mtime file, since a name_key_results.csv "
        "belongs to exactly one segment's export_run_id population and joining "
        "against the wrong segment's copy silently produces a 100% join miss "
        "with no error (this bit an earlier version of this script -- see the "
        "explicit selection logic below instead of trusting mtime alone).\n"
    )
    records_mtime = records_csv.stat().st_mtime if records_csv.is_file() else None
    for c in candidates:
        mtime = c.stat().st_mtime
        stale_note = ""
        if records_mtime is not None:
            stale_note = (
                " -- older than THIS segment's records.csv (only meaningful if this IS this segment's own file)"
                if mtime < records_mtime
                else " -- newer than THIS segment's records.csv (only meaningful if this IS this segment's own file)"
            )
        report.append(f"- `{c}` (mtime={mtime:.0f}){stale_note}")
    report.append("")
    return candidates


def resolve_name_key_csv(segment_root: Path, corpus_root: Path, override: Path, report: list) -> Path:
    """Resolution order, most-trustworthy first. Never falls back to "newest file
    anywhere in the corpus" -- that silently joins against a different segment's
    already-filtered population and produces a 100% join miss with no error.
    """
    if override is not None:
        report.append(f"Using explicit --name-key-csv override: `{override}`\n")
        return override

    segment_local = segment_root / "results" / "name_key" / "name_key_results.csv"
    if segment_local.is_file():
        report.append(
            f"Using this segment's own filtered copy (from "
            f"_filter_name_key_csv_to_segment()): `{segment_local}`\n"
        )
        return segment_local

    corpus_wide_candidates = [
        corpus_root / "name_key" / "name_key_results.csv",
        corpus_root / "Results_v21" / "name_key" / "name_key_results.csv",
    ]
    for c in corpus_wide_candidates:
        if c.is_file():
            report.append(
                f"No segment-local file at `{segment_local}`. Falling back to the "
                f"corpus-wide unfiltered file: `{c}`. This file is NOT pre-filtered to "
                f"this segment's export_run_ids -- the join below still relies entirely "
                f"on normalize_export_run_id() + this segment's own known_ids set to "
                f"select only relevant rows, per finding B.4.\n"
            )
            return c

    raise SystemExit(
        f"FAIL LOUD: no usable name_key_results.csv found. Checked segment-local "
        f"({segment_local}) and corpus-wide locations ({corpus_wide_candidates}). "
        f"Pass --name-key-csv explicitly, or run apply_name_key_policy.py first."
    )


def _join_hash_from_row(row: dict, path: Path) -> str:
    """Production domain_patterns.csv (tools/extractor.py's writer) carries no literal
    join_hash column -- only source_cluster_id, formatted "domain|schema|join_hash"
    (tools/extractor.py: cluster_id = f"{dom}|{schema}|{join_hash}"). This mirrors
    compare_reference.py::load_domain_pattern_join_hash_map's own derivation exactly,
    rather than reinventing it. Only generate_name_key_patterns.py's synthetic "name"
    target ever writes a bare join_hash column -- that is not what production segments
    materialize, and this script should read production data.
    """
    sc = row.get("source_cluster_id", "")
    if not sc:
        return ""
    parts = sc.split("|")
    if len(parts) < 2:
        raise SystemExit(
            f"FAIL LOUD: {path} has a source_cluster_id value that doesn't match the "
            f"expected 'domain|schema|join_hash' shape: {sc!r}. Do not guess a partial parse."
        )
    return parts[-1]


def check_c6(domain_patterns_csv: Path, domain: str, report: list):
    report.append("## C.6 -- join_hash <-> pattern_id uniqueness (empirical spot-check)\n")
    fieldnames, rows = _read_csv(domain_patterns_csv)
    _require_columns(fieldnames, ["domain", "pattern_id", "source_cluster_id"], domain_patterns_csv)
    rows = [r for r in rows if r.get("domain") == domain]
    if not rows:
        report.append(f"**No rows found for domain={domain!r} in {domain_patterns_csv}.**\n")
        return {}

    pid_to_jh = defaultdict(set)
    jh_to_pid = defaultdict(set)
    for r in rows:
        pid = r["pattern_id"]
        jh = _join_hash_from_row(r, domain_patterns_csv)
        if pid and jh:
            pid_to_jh[pid].add(jh)
            jh_to_pid[jh].add(pid)

    bad_pid = {k: v for k, v in pid_to_jh.items() if len(v) > 1}
    bad_jh = {k: v for k, v in jh_to_pid.items() if len(v) > 1}

    if not bad_pid and not bad_jh:
        report.append(
            f"**Result: CLEAN.** {len(pid_to_jh)} distinct pattern_id values, "
            f"{len(jh_to_pid)} distinct join_hash values, strict 1:1 for domain={domain!r}. "
            "The by-construction guarantee holds on this segment's actual data.\n"
        )
    else:
        report.append("**Result: VIOLATION FOUND -- report before proceeding to Step 1.**\n")
        for pid, jhs in list(bad_pid.items())[:5]:
            report.append(f"- pattern_id `{pid}` maps to {len(jhs)} join_hash values: {sorted(jhs)}")
        for jh, pids in list(bad_jh.items())[:5]:
            report.append(f"- join_hash `{jh}` maps to {len(pids)} pattern_id values: {sorted(pids)}")
        report.append("")

    # join_hash -> pattern_id map, for use in check_c7
    jh_to_single_pid = {jh: next(iter(pids)) for jh, pids in jh_to_pid.items() if len(pids) == 1}
    return jh_to_single_pid


def check_c7_c8(
    records_csv: Path,
    name_key_csv: Path,
    domain: str,
    jh_to_pid: dict,
    normalize_export_run_id,
    report: list,
):
    report.append("## C.7/C.8 -- name-hash status coverage and per-pattern multiplicity\n")

    rec_fields, rec_rows = _read_csv(records_csv)
    _require_columns(rec_fields, ["domain", "export_run_id", "record_id", "join_hash"], records_csv)
    rec_rows = [r for r in rec_rows if r.get("domain") == domain]
    if not rec_rows:
        report.append(f"**No records.csv rows found for domain={domain!r}.**\n")
        return
    known_ids = {r["export_run_id"] for r in rec_rows}

    nk_fields, nk_rows = _read_csv(name_key_csv)
    _require_columns(
        nk_fields, ["export_file", "domain", "record_id", "join_hash", "status"], name_key_csv
    )
    nk_rows_domain = [r for r in nk_rows if r.get("domain") == domain]
    if not nk_rows_domain:
        report.append(f"**No name_key_results.csv rows found for domain={domain!r}.**\n")
        return

    # C.8: status distribution for this domain, corpus-wide (before segment/join filtering)
    status_counts = defaultdict(int)
    for r in nk_rows_domain:
        status_counts[r.get("status", "")] += 1
    total = sum(status_counts.values())
    report.append(f"**C.8 -- status distribution for domain={domain!r}** ({total} rows):\n")
    for status, count in sorted(status_counts.items(), key=lambda kv: -kv[1]):
        pct = 100.0 * count / total if total else 0.0
        report.append(f"- `{status or '(blank)'}`: {count} ({pct:.1f}%)")
    report.append("")

    ok_rows = [r for r in nk_rows_domain if r.get("status") == "ok"]
    name_index = {}
    for r in ok_rows:
        norm_id = normalize_export_run_id(r["export_file"], known_ids)
        name_index[(norm_id, r["record_id"])] = r["join_hash"]

    by_pattern = defaultdict(set)
    unmatched_records = 0
    unmatched_pattern = 0
    for r in rec_rows:
        pid = jh_to_pid.get(r["join_hash"])
        if pid is None:
            unmatched_pattern += 1
            continue
        name_jh = name_index.get((r["export_run_id"], r["record_id"]))
        if name_jh is None:
            unmatched_records += 1
            continue
        by_pattern[pid].add(name_jh)

    single = {k: v for k, v in by_pattern.items() if len(v) == 1}
    multi = {k: v for k, v in by_pattern.items() if len(v) > 1}

    report.append(f"**C.7 -- per-pattern name-hash multiplicity** ({len(rec_rows)} records.csv rows examined):\n")
    report.append(f"- records with no resolvable config pattern_id (via join_hash): {unmatched_pattern}")
    report.append(
        f"- records with a resolvable pattern_id but no matching (status=ok) name-key row: {unmatched_records}"
    )
    report.append(f"- patterns with exactly ONE name-hash: {len(single)}")
    report.append(f"- patterns with MORE THAN ONE name-hash: {len(multi)}")
    if multi:
        report.append(
            "\n**Multiplicity confirmed non-zero -- the 'one name per pattern' assumption "
            "needs the documented tie-break rule before Step 1's classifier schema is locked "
            "(see findings recommendation #4). Example patterns:**\n"
        )
        for pid, jhs in list(multi.items())[:5]:
            report.append(f"- pattern_id `{pid}`: {len(jhs)} distinct name-hashes: {sorted(jhs)}")
    else:
        report.append(
            "\n**Multiplicity is zero on this segment/domain -- 'one name per pattern' holds "
            "in practice here. Still only one segment/domain; do not generalize to all 25 "
            "eligible domains without spot-checking at least one more.**"
        )
    report.append("")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", required=True, type=Path, help="Path to Revit_Fingerprint repo checkout")
    ap.add_argument("--corpus-root", required=True, type=Path, help="Path to Fingerprint_Data root")
    ap.add_argument("--segment-root", required=True, type=Path, help="Path to one materialized segment root")
    ap.add_argument("--name-key-csv", type=Path, default=None, help="Override name_key_results.csv path")
    ap.add_argument("--domain", default="arrowheads")
    ap.add_argument("--out", type=Path, default=Path("namekey_step0_completion.md"))
    args = ap.parse_args()

    sys.path.insert(0, str(args.repo_root))
    try:
        from tools.bundle_analysis.name_projection_adapter import normalize_export_run_id
    except ImportError as e:
        raise SystemExit(
            f"FAIL LOUD: could not import normalize_export_run_id from --repo-root={args.repo_root}. "
            f"Confirm this is the actual repo checkout root (should contain tools/bundle_analysis/). "
            f"Original error: {e}"
        )

    records_csv = args.segment_root / "results" / "records" / "records.csv"
    domain_patterns_csv = args.segment_root / "results" / "analysis" / "domain_patterns.csv"

    report = [
        f"# Name-Key Step 0 Completion Report -- domain={args.domain}\n",
        f"segment_root: `{args.segment_root}`\n",
        f"corpus_root: `{args.corpus_root}`\n",
    ]

    newest_nk_csv = check_a2(args.corpus_root, records_csv, report)
    if not newest_nk_csv and args.name_key_csv is None:
        report.append(
            "\n**C.7/C.8 SKIPPED -- no name_key_results.csv available anywhere (see A.2 above). "
            "Run apply_name_key_policy.py first.**\n"
        )
        (args.out).write_text("\n".join(report), encoding="utf-8")
        print("\n".join(report))
        return

    name_key_csv = resolve_name_key_csv(args.segment_root, args.corpus_root, args.name_key_csv, report)

    selected_mtime = name_key_csv.stat().st_mtime
    records_mtime = records_csv.stat().st_mtime if records_csv.is_file() else None
    if records_mtime is not None:
        if selected_mtime < records_mtime:
            report.append(
                f"**STALE**: selected name_key_results.csv (mtime={selected_mtime:.0f}) is older than "
                f"this segment's records.csv (mtime={records_mtime:.0f}) -- recent exports may not be "
                f"reflected in the name-key data used below.\n"
            )
        else:
            report.append(
                f"Selected file (mtime={selected_mtime:.0f}) is not stale relative to "
                f"this segment's records.csv (mtime={records_mtime:.0f}).\n"
            )

    jh_to_pid = check_c6(domain_patterns_csv, args.domain, report)
    if jh_to_pid:
        check_c7_c8(records_csv, name_key_csv, args.domain, jh_to_pid, normalize_export_run_id, report)
    else:
        report.append("\n**C.7/C.8 SKIPPED -- C.6 produced no usable join_hash -> pattern_id map.**\n")

    args.out.write_text("\n".join(report), encoding="utf-8")
    print("\n".join(report))
    print(f"\n--- Written to {args.out} ---")


if __name__ == "__main__":
    main()
