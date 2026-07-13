#!/usr/bin/env python3
"""Select the minimum set of files needed to review all archetype clusters.

Uses a greedy set-cover approach over archetype_cluster_classifications.csv
to find the fewest files that together cover all promoted clusters, with
governance_role priority (Project > Template > Container).

Produces one row per (selected file × signal) so each archetype's signal
detail is directly readable without cross-referencing separate review CSVs.

Inputs (resolved relative to --repo-root unless overridden):
  Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv
  Fingerprint_Out/archetype_analysis/signal_clusters.json
  Fingerprint_Out/archetype_analysis/archetype_review/review_<cluster_id>.csv  (per cluster)
  config/archetype/archetype_definitions.json
  results/records/file_metadata.csv

Outputs:
  Fingerprint_Out/archetype_analysis/archetype_review/archetype_review_schedule.csv
  Fingerprint_Out/archetype_analysis/archetype_review/archetype_review_gaps.csv

archetype_review_schedule.csv  — one row per (file × signal):
  priority            sequential open order (1-based, same value for all rows in same file)
  governance_role
  file_path
  export_run_id
  client_label
  discipline_label
  cluster_id
  governance_question
  approach_label
  n_signals_in_cluster
  signal_id
  source_domain
  source_join_hash    join_hash filter that seeded this signal
  element_name        resolved Revit element name (or "(unresolved)" for VFD/missing)
  sig_hash
  param_names         VFD signals: pipe-separated parameter names
  category_names      VFD signals: pipe-separated category names
  n_signals_fired     how many cluster signals fired in this file
  all_signals_fired
  coverage_type       "full" or "partial"

archetype_review_gaps.csv — clusters with no project-level full coverage:
  cluster_id
  governance_question
  reason

Usage:
    python tools/archetype/select_archetype_review_files.py --repo-root .
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

STAGE = "select_archetype_review_files"

ROLE_PRIORITY: Dict[str, int] = {
    "Project": 0,
    "Template": 1,
    "Container": 2,
    "Generic": 3,
}

SCHEDULE_FIELDS = [
    "priority",
    "governance_role",
    "file_path",
    "export_run_id",
    "client_label",
    "discipline_label",
    "cluster_id",
    "governance_question",
    "approach_label",
    "n_signals_in_cluster",
    "signal_id",
    "source_domain",
    "source_join_hash",
    "element_name",
    "sig_hash",
    "param_names",
    "category_names",
    "n_signals_fired",
    "all_signals_fired",
    "coverage_type",
]

GAPS_FIELDS = [
    "cluster_id",
    "governance_question",
    "reason",
]

_PATH_COLUMN_CANDIDATES = ("central_path", "central_path_norm", "file_path")


# ── helpers ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{STAGE}] {msg}", file=sys.stderr)


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def atomic_write_csv(path: Path, fields: List[str], rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


# ── lookup builders ───────────────────────────────────────────────────────────

def _load_file_paths(file_metadata_path: Path) -> Dict[str, str]:
    rows = read_csv_rows(file_metadata_path)
    path_col: Optional[str] = None
    if rows:
        keys = rows[0].keys()
        for c in _PATH_COLUMN_CANDIDATES:
            if c in keys:
                path_col = c
                break
    result: Dict[str, str] = {}
    for row in rows:
        eid = row.get("export_run_id", "").strip()
        if eid:
            result[eid] = row.get(path_col, "") if path_col else ""
    return result


def _all_cluster_pairs(signal_clusters: dict) -> List[Tuple[str, str]]:
    """All (cluster_id, governance_question) pairs."""
    result: List[Tuple[str, str]] = []
    for gq, clusters in (signal_clusters.get("clusters") or {}).items():
        for c in clusters:
            cid = c.get("cluster_id", "")
            if cid:
                result.append((cid, gq))
    return result


def _cluster_signal_ids(signal_clusters: dict) -> Dict[str, List[str]]:
    """cluster_id -> list of signal_ids."""
    result: Dict[str, List[str]] = {}
    for clusters in (signal_clusters.get("clusters") or {}).values():
        for c in clusters:
            cid = c.get("cluster_id", "")
            if cid:
                result[cid] = list(c.get("signal_ids") or [])
    return result


def _build_approach_label_map(
    archetype_defs: dict,
    signal_clusters: dict,
) -> Dict[str, str]:
    """cluster_id -> approach_label.

    Derived by mapping each archetype's signals through signal_clusters to
    find which cluster they land in, then associating the archetype's
    approach_label with that cluster.  When multiple archetypes contribute
    to the same cluster they should share an approach_label; if they differ,
    labels are joined with ' / '.
    """
    # signal_id -> cluster_id
    sig_to_cluster: Dict[str, str] = {}
    for clusters in (signal_clusters.get("clusters") or {}).values():
        for c in clusters:
            cid = c.get("cluster_id", "")
            for sid in (c.get("signal_ids") or []):
                sig_to_cluster[sid] = cid

    cluster_labels: Dict[str, Set[str]] = defaultdict(set)
    for arch in (archetype_defs.get("archetypes") or []):
        label = arch.get("approach_label", "")
        if not label:
            continue
        for sig in (arch.get("signals") or []):
            sid = sig.get("signal_id", "")
            cid = sig_to_cluster.get(sid, "")
            if cid:
                cluster_labels[cid].add(label)

    return {cid: " / ".join(sorted(labels)) for cid, labels in cluster_labels.items()}


def _build_file_cluster_index(
    cc_rows: List[Dict[str, str]],
) -> Tuple[
    Dict[str, Dict[str, List[Dict]]],   # cluster_id -> role -> [row]
    Dict[str, Dict[str, str]],           # export_run_id -> cluster_id -> "full"|"partial"
    Dict[str, str],                      # export_run_id -> governance_role
    Dict[str, str],                      # export_run_id -> client_label
    Dict[str, str],                      # export_run_id -> discipline_label
    Dict[str, str],                      # cluster_id -> governance_question
    Dict[str, str],                      # export_run_id -> n_signals_fired (per cluster row)
    Dict[str, str],                      # export_run_id -> all_signals_fired (per cluster row)
]:
    by_cluster_role: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    file_clusters: Dict[str, Dict[str, str]] = defaultdict(dict)
    file_role: Dict[str, str] = {}
    file_client: Dict[str, str] = {}
    file_discipline: Dict[str, str] = {}
    cluster_gq: Dict[str, str] = {}
    # (export_run_id, cluster_id) -> n_signals_fired / all_signals_fired
    file_cluster_signals_fired: Dict[Tuple[str, str], str] = {}
    file_cluster_all_fired: Dict[Tuple[str, str], str] = {}

    for row in cc_rows:
        eid = row.get("export_run_id", "").strip()
        cid = row.get("cluster_id", "").strip()
        role = row.get("governance_role", "").strip()
        all_fired = row.get("all_signals_fired", "").strip().lower() in ("true", "1", "yes")
        any_fired = row.get("any_signal_fired", "").strip().lower() in ("true", "1", "yes")
        gq = row.get("governance_question", "").strip()
        if not eid or not cid:
            continue

        coverage_type = "full" if all_fired else ("partial" if any_fired else None)
        if coverage_type is None:
            continue

        by_cluster_role[cid][role].append(row)
        existing = file_clusters[eid].get(cid)
        if existing != "full":
            file_clusters[eid][cid] = coverage_type

        file_role[eid] = role
        file_client[eid] = row.get("client_label", "").strip()
        file_discipline[eid] = row.get("discipline_label", "").strip()
        if gq:
            cluster_gq[cid] = gq
        file_cluster_signals_fired[(eid, cid)] = row.get("n_signals_fired", "")
        file_cluster_all_fired[(eid, cid)] = row.get("all_signals_fired", "")

    return (
        dict(by_cluster_role),
        dict(file_clusters),
        file_role,
        file_client,
        file_discipline,
        cluster_gq,
        file_cluster_signals_fired,
        file_cluster_all_fired,
    )


def _load_review_csvs(review_dir: Path) -> Dict[str, Dict[Tuple[str, str], List[Dict]]]:
    """cluster_id -> (export_run_id, signal_id) -> list of row dicts.

    Reads all review_<cluster_id>.csv files from review_dir.
    """
    result: Dict[str, Dict[Tuple[str, str], List[Dict]]] = {}
    if not review_dir.is_dir():
        return result
    for csv_file in review_dir.glob("review_*.csv"):
        # extract cluster_id from filename: review_<cluster_id>.csv
        stem = csv_file.stem  # "review_arrowhead_consistency__cluster_001"
        cluster_id = stem[len("review_"):]  # strip leading "review_"
        rows = read_csv_rows(csv_file)
        by_file_signal: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        for row in rows:
            eid = row.get("export_run_id", "")
            sid = row.get("signal_id", "")
            if eid and sid:
                by_file_signal[(eid, sid)].append(row)
        result[cluster_id] = dict(by_file_signal)
    return result


# ── greedy set cover ──────────────────────────────────────────────────────────

def _greedy_cover(
    target_cluster_ids: List[str],
    file_clusters: Dict[str, Dict[str, str]],
    file_role: Dict[str, str],
    file_client: Dict[str, str],
    file_discipline: Dict[str, str],
    cluster_gq: Dict[str, str],
    file_paths: Dict[str, str],
    coverage_mode: str,
) -> List[Tuple[str, str, Set[str], str]]:
    """Returns list of (export_run_id, file_path, new_clusters_covered, coverage_type)."""
    uncovered: Set[str] = set(target_cluster_ids)
    selected: List[Tuple[str, str, Set[str], str]] = []
    role_tiers = ["Project", "Template", "Container", "Generic"]

    for role in role_tiers:
        if not uncovered:
            break
        candidates = {
            eid: clusters
            for eid, clusters in file_clusters.items()
            if file_role.get(eid) == role
            and any(
                (coverage_mode == "full" and ct == "full") or (coverage_mode == "any")
                for ct in clusters.values()
            )
        }
        while uncovered and candidates:
            best_eid: Optional[str] = None
            best_new: Set[str] = set()
            best_gqs: Set[str] = set()

            for eid, clusters in candidates.items():
                if coverage_mode == "full":
                    fc = {c for c, ct in clusters.items() if ct == "full"}
                else:
                    fc = set(clusters.keys())
                new = fc & uncovered
                if not new:
                    continue
                gqs = {cluster_gq.get(c, "") for c in new}
                if (
                    best_eid is None
                    or len(gqs) > len(best_gqs)
                    or (len(gqs) == len(best_gqs) and len(new) > len(best_new))
                ):
                    best_eid, best_new, best_gqs = eid, new, gqs

            if best_eid is None:
                break

            fp = file_paths.get(best_eid, best_eid)
            selected.append((best_eid, fp, best_new, coverage_mode))
            uncovered -= best_new
            del candidates[best_eid]

    return selected


# ── row assembly ──────────────────────────────────────────────────────────────

def _build_output_rows(
    schedule_entries: List[Tuple[str, str, Set[str], str]],
    file_clusters: Dict[str, Dict[str, str]],
    file_role: Dict[str, str],
    file_client: Dict[str, str],
    file_discipline: Dict[str, str],
    cluster_gq: Dict[str, str],
    cluster_signal_ids: Dict[str, List[str]],
    approach_label_map: Dict[str, str],
    review_data: Dict[str, Dict[Tuple[str, str], List[Dict]]],
    file_cluster_signals_fired: Dict[Tuple[str, str], str],
    file_cluster_all_fired: Dict[Tuple[str, str], str],
    signal_join_hash: Dict[str, str],
) -> List[Dict]:
    """One row per (file × signal) for each (file, cluster) in the schedule."""
    rows: List[Dict] = []

    for priority, (eid, fp, new_clusters, coverage_type) in enumerate(schedule_entries, start=1):
        role = file_role.get(eid, "")
        client = file_client.get(eid, "")
        discipline = file_discipline.get(eid, "")

        for cluster_id in sorted(new_clusters):
            gq = cluster_gq.get(cluster_id, "")
            approach = approach_label_map.get(cluster_id, "")
            signal_ids = cluster_signal_ids.get(cluster_id, [])
            n_sigs_in_cluster = len(signal_ids)
            n_signals_fired = file_cluster_signals_fired.get((eid, cluster_id), "")
            all_signals_fired = file_cluster_all_fired.get((eid, cluster_id), "")

            cluster_review = review_data.get(cluster_id, {})

            for signal_id in signal_ids:
                # Look up detail row from review CSV
                detail_rows = cluster_review.get((eid, signal_id), [])

                if detail_rows:
                    # Use first detail row (one per file×signal in practice)
                    d = detail_rows[0]
                    rows.append({
                        "priority": priority,
                        "governance_role": role,
                        "file_path": d.get("file_path") or fp,
                        "export_run_id": eid,
                        "client_label": d.get("client_label") or client,
                        "discipline_label": d.get("discipline_label") or discipline,
                        "cluster_id": cluster_id,
                        "governance_question": gq,
                        "approach_label": approach,
                        "n_signals_in_cluster": n_sigs_in_cluster,
                        "signal_id": signal_id,
                        "source_domain": d.get("source_domain", ""),
                        "source_join_hash": d.get("source_join_hash", ""),
                        "element_name": d.get("element_name", ""),
                        "sig_hash": d.get("sig_hash", ""),
                        "param_names": d.get("param_names", ""),
                        "category_names": d.get("category_names", ""),
                        "n_signals_fired": n_signals_fired,
                        "all_signals_fired": all_signals_fired,
                        "coverage_type": coverage_type,
                    })
                else:
                    # No review CSV data: emit stub with join_hash from definitions
                    rows.append({
                        "priority": priority,
                        "governance_role": role,
                        "file_path": fp,
                        "export_run_id": eid,
                        "client_label": client,
                        "discipline_label": discipline,
                        "cluster_id": cluster_id,
                        "governance_question": gq,
                        "approach_label": approach,
                        "n_signals_in_cluster": n_sigs_in_cluster,
                        "signal_id": signal_id,
                        "source_domain": "",
                        "source_join_hash": signal_join_hash.get(signal_id, ""),
                        "element_name": "(unresolved)",
                        "sig_hash": "",
                        "param_names": "",
                        "category_names": "",
                        "n_signals_fired": n_signals_fired,
                        "all_signals_fired": all_signals_fired,
                        "coverage_type": coverage_type,
                    })

    return rows


def _identify_gaps(
    all_cluster_ids: List[str],
    cluster_gq: Dict[str, str],
    by_cluster_role: Dict[str, Dict[str, List]],
) -> List[Dict[str, str]]:
    gaps: List[Dict[str, str]] = []
    for cid in all_cluster_ids:
        role_map = by_cluster_role.get(cid, {})
        if not role_map:
            gaps.append({"cluster_id": cid, "governance_question": cluster_gq.get(cid, ""), "reason": "no_files_any_role"})
        elif not role_map.get("Project"):
            has_full = any(
                row.get("all_signals_fired", "").lower() in ("true", "1", "yes")
                for rows in role_map.values()
                for row in rows
            )
            gaps.append({
                "cluster_id": cid,
                "governance_question": cluster_gq.get(cid, ""),
                "reason": "no_project_files" if has_full else "partial_only_no_project",
            })
    return gaps


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=".", help="Repository root")
    ap.add_argument("--cluster-classifications", default=None)
    ap.add_argument("--signal-clusters", default=None)
    ap.add_argument("--archetype-definitions", default=None)
    ap.add_argument("--review-dir", default=None, help="Directory containing review_<cluster_id>.csv files")
    ap.add_argument("--file-metadata", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    analysis_dir = repo_root / "Fingerprint_Out" / "archetype_analysis"
    out_dir = Path(args.out) if args.out else analysis_dir / "archetype_review"

    cc_path       = Path(args.cluster_classifications) if args.cluster_classifications \
                    else analysis_dir / "archetype_cluster_classifications.csv"
    sc_path       = Path(args.signal_clusters) if args.signal_clusters \
                    else analysis_dir / "signal_clusters.json"
    defs_path     = Path(args.archetype_definitions) if args.archetype_definitions \
                    else repo_root / "config" / "archetype" / "archetype_definitions.json"
    review_dir    = Path(args.review_dir) if args.review_dir \
                    else analysis_dir / "archetype_review"
    fm_path       = Path(args.file_metadata) if args.file_metadata \
                    else repo_root / "results" / "records" / "file_metadata.csv"

    schedule_path = out_dir / "archetype_review_schedule.csv"
    gaps_path     = out_dir / "archetype_review_gaps.csv"

    # Load
    cc_rows = read_csv_rows(cc_path)
    log(f"loaded {len(cc_rows)} rows from {cc_path.name}")

    signal_clusters = read_json(sc_path)
    all_pairs = _all_cluster_pairs(signal_clusters)
    all_cluster_ids = [c for c, _ in all_pairs]
    log(f"found {len(all_cluster_ids)} clusters")

    archetype_defs = read_json(defs_path)
    log(f"loaded {len(archetype_defs.get('archetypes', []))} promoted archetypes from {defs_path.name}")

    file_paths = _load_file_paths(fm_path)
    log(f"loaded {len(file_paths)} file paths")

    review_data = _load_review_csvs(review_dir)
    log(f"loaded review CSVs for {len(review_data)} clusters from {review_dir}")

    # Build derived lookups
    (
        by_cluster_role,
        file_clusters,
        file_role,
        file_client,
        file_discipline,
        cluster_gq,
        file_cluster_signals_fired,
        file_cluster_all_fired,
    ) = _build_file_cluster_index(cc_rows)

    for cid, gq in all_pairs:
        if cid not in cluster_gq:
            cluster_gq[cid] = gq

    cluster_signal_ids = _cluster_signal_ids(signal_clusters)
    approach_label_map = _build_approach_label_map(archetype_defs, signal_clusters)

    # signal_id -> join_hash from definitions (for stubs)
    signal_join_hash: Dict[str, str] = {}
    for arch in (archetype_defs.get("archetypes") or []):
        for sig in (arch.get("signals") or []):
            sid = sig.get("signal_id", "")
            jh = sig.get("join_hash", "")
            if sid and jh and sid not in signal_join_hash:
                signal_join_hash[sid] = jh

    # Greedy cover
    log("running greedy set cover (full coverage mode)...")
    selected_full = _greedy_cover(
        all_cluster_ids, file_clusters, file_role, file_client,
        file_discipline, cluster_gq, file_paths, "full",
    )

    covered = {c for _, _, cs, _ in selected_full for c in cs}
    remaining = [c for c in all_cluster_ids if c not in covered]

    if remaining:
        log(f"{len(remaining)} clusters need partial fallback...")
        partial_fc = {
            eid: {c: ct for c, ct in clusters.items() if c in remaining}
            for eid, clusters in file_clusters.items()
        }
        partial_fc = {k: v for k, v in partial_fc.items() if v}
        selected_partial = _greedy_cover(
            remaining, partial_fc, file_role, file_client,
            file_discipline, cluster_gq, file_paths, "any",
        )
    else:
        selected_partial = []

    all_selected = selected_full + selected_partial

    # Build output rows
    output_rows = _build_output_rows(
        all_selected,
        file_clusters,
        file_role,
        file_client,
        file_discipline,
        cluster_gq,
        cluster_signal_ids,
        approach_label_map,
        review_data,
        file_cluster_signals_fired,
        file_cluster_all_fired,
        signal_join_hash,
    )

    gaps = _identify_gaps(all_cluster_ids, cluster_gq, by_cluster_role)

    # Summary
    n_files = len(all_selected)
    n_clusters_covered = len({c for _, _, cs, _ in all_selected for c in cs})
    log(f"schedule: {n_files} files, {len(output_rows)} signal rows, covering {n_clusters_covered}/{len(all_cluster_ids)} clusters")
    log(f"gaps: {len(gaps)} clusters with no project-level full coverage")
    for priority, (eid, fp, new_clusters, ct) in enumerate(all_selected, start=1):
        log(f"  [{priority}] {file_role.get(eid):<12} clusters={len(new_clusters)}  {Path(fp).name}")

    if args.dry_run:
        log(f"dry-run: would write {schedule_path} ({len(output_rows)} rows)")
        return 0

    atomic_write_csv(schedule_path, SCHEDULE_FIELDS, output_rows)
    log(f"wrote {len(output_rows)} rows to {schedule_path}")

    atomic_write_csv(gaps_path, GAPS_FIELDS, gaps)
    log(f"wrote {len(gaps)} rows to {gaps_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())