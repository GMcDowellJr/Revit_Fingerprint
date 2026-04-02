from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, make_bundle_id, read_csv_rows, resolve_analysis_run_id
    from step2_find_bundles import compute_auto_threshold
    from utils import find_root_bundles
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, make_bundle_id, read_csv_rows, resolve_analysis_run_id
    from .step2_find_bundles import compute_auto_threshold
    from .utils import find_root_bundles

# Population discovery considers only the top 20 roots by file count.
# Any configuration family not in the top 20 is too minor to warrant
# a separate governance analysis track at any corpus scale. This cap
# also bounds computational complexity: pairwise tests are O(n^2) in
# candidate root count, so capping at 20 limits to 190 pairwise tests
# regardless of how many roots survive the support threshold.
MAX_POPULATION_CANDIDATES = 20


def _pattern_token(pattern_ids: Set[str]) -> str:
    return "|".join(sorted(pattern_ids))


def _pattern_summary(pattern_ids: Set[str]) -> str:
    return "|".join(sorted(pattern_ids)[:3])


def _population_id(domain: str, pattern_ids: Set[str]) -> str:
    return make_bundle_id(domain, "", sorted(pattern_ids)).replace("bnd_", "pop_", 1)


def _select_populations(
    substantial_roots: List[Dict[str, object]],
    max_population_overlap: float,
    min_population_jaccard: float,
) -> List[Dict[str, object]]:
    survivors = sorted(
        substantial_roots,
        key=lambda r: (-int(r["files_present"]), -len(r["pattern_ids"]), tuple(sorted(r["pattern_ids"]))),
    )
    selected: List[Dict[str, object]] = []
    for candidate in survivors:
        keep = True
        for existing in selected:
            files_a = set(candidate["file_ids"])
            files_b = set(existing["file_ids"])
            in_both = len(files_a & files_b)
            overlap_a = in_both / len(files_a) if files_a else 0.0
            overlap_b = in_both / len(files_b) if files_b else 0.0
            overlap = max(overlap_a, overlap_b)

            pa = set(candidate["pattern_ids"])
            pb = set(existing["pattern_ids"])
            union = len(pa | pb)
            jaccard_similarity = (len(pa & pb) / union) if union else 1.0
            jaccard_dissimilarity = 1.0 - jaccard_similarity

            distinct = overlap < max_population_overlap and jaccard_dissimilarity >= min_population_jaccard
            if not distinct:
                keep = False
                break
        if keep:
            selected.append(candidate)
    return selected


def discover_populations(
    analysis_dir: Path,
    out_dir: Path,
    domain: str = "",
    analysis_run_id: str = "",
    min_population_size: int = 0,
    max_population_overlap: float = 0.20,
    min_population_jaccard: float = 0.30,
    discovery_support_pct: float = 0.50,
) -> Dict[str, int]:
    if discovery_support_pct < 0.05:
        raise ValueError(
            f"--discovery-support-pct must be >= 0.05, got {discovery_support_pct}. "
            "Values below 0.05 produce degenerate behavior with hundreds of "
            "candidate roots and prohibitive runtime."
        )

    pattern_presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    domain_pattern_rows = read_csv_rows(analysis_dir / "domain_patterns.csv")
    run_id = resolve_analysis_run_id(pattern_presence_rows, analysis_run_id)
    domains = [domain] if domain else sorted(
        {r.get("domain", "") for r in pattern_presence_rows if r.get("analysis_run_id", "") == run_id and r.get("domain", "")}
    )

    cad_patterns_by_domain: Dict[str, Set[str]] = {}
    for row in domain_pattern_rows:
        if row.get("analysis_run_id", "") != run_id:
            continue
        dom = row.get("domain", "")
        if not dom:
            continue
        if (row.get("is_cad_import", "") or "").strip().lower() == "true" and row.get("pattern_id", ""):
            cad_patterns_by_domain.setdefault(dom, set()).add(row["pattern_id"])

    discovery_dir = out_dir / "_population_discovery"
    all_population_rows: List[Dict[str, str]] = []
    all_summary_rows: List[Dict[str, str]] = []
    all_parameter_rows: List[Dict[str, str]] = []
    all_root_pattern_rows: List[Dict[str, str]] = []

    for dom in domains:
        cad_patterns = cad_patterns_by_domain.get(dom, set())
        per_file_patterns: Dict[str, Set[str]] = {}
        for row in pattern_presence_rows:
            if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != dom:
                continue
            fid = (row.get("export_run_id", "") or "").strip()
            pid = (row.get("pattern_id", "") or "").strip()
            if not fid or not pid or pid in cad_patterns:
                continue
            per_file_patterns.setdefault(fid, set()).add(pid)

        file_sets = {fid: frozenset(pids) for fid, pids in per_file_patterns.items() if pids}
        files_total = len(file_sets)
        discovery_support = max(3, int(math.ceil(files_total * discovery_support_pct))) if files_total else 3
        effective_min_population_size = max(int(min_population_size or 0), max(5, int(math.ceil(files_total * 0.05)))) if files_total else 5

        roots = find_root_bundles(file_sets, min_support=discovery_support, min_bundle_size=2) if files_total >= 2 else []
        substantial_roots = [r for r in roots if int(r["files_present"]) >= effective_min_population_size]
        substantial_roots_before_cap = len(substantial_roots)
        substantial_roots_sorted = sorted(
            substantial_roots,
            key=lambda r: (-int(r["files_present"]), -len(r["pattern_ids"]), tuple(sorted(r["pattern_ids"]))),
        )
        capped_roots = substantial_roots_sorted[:MAX_POPULATION_CANDIDATES]
        substantial_roots_after_cap = len(capped_roots)
        capped_out_roots = substantial_roots_sorted[MAX_POPULATION_CANDIDATES:]
        if substantial_roots_before_cap > MAX_POPULATION_CANDIDATES:
            lowest_retained_support = int(capped_roots[-1]["files_present"]) if capped_roots else 0
            print(
                f"[step0_cap] domain={dom} substantial_roots_before_cap={substantial_roots_before_cap} "
                f"capped_to={MAX_POPULATION_CANDIDATES} lowest_retained_support={lowest_retained_support} "
                f"discarded={substantial_roots_before_cap - MAX_POPULATION_CANDIDATES} roots below cap"
            )
        candidate_populations = _select_populations(capped_roots, max_population_overlap, min_population_jaccard)
        for pop in candidate_populations:
            pop["population_id"] = _population_id(dom, set(pop["pattern_ids"]))

        viability_checks_run = 0
        viability_checks_passed = 0
        viability_checks_failed = 0
        viable_populations: List[Dict[str, object]] = []
        failed_population_notes: Dict[str, str] = {}

        for pop in candidate_populations:
            pid = str(pop["population_id"])
            pop_patterns = set(pop["pattern_ids"])
            population_file_sets = {
                fid: pset for fid, pset in file_sets.items() if set(pset).issuperset(pop_patterns)
            }
            population_size = len(population_file_sets)

            viability_checks_run += 1
            threshold_value: int
            viability_notes = ""
            try:
                threshold_result = compute_auto_threshold(population_file_sets, files_total=population_size)
                threshold_value = int(threshold_result.get("chosen", 0))
            except Exception as exc:
                truncated = str(exc)[:100]
                print(f"[step0_viability_warn] domain={dom} auto_threshold_failed={truncated} falling_back_to_heuristic")
                threshold_value = max(3, int(math.ceil(population_size * 0.10)))
                viability_notes = f"fallback_heuristic: auto_threshold_failed={truncated}"

            viable = population_size >= threshold_value
            print(
                f"[step0_viability] domain={dom} population_id={pid} "
                f"population_files={population_size} estimated_threshold={threshold_value} viable={viable}"
            )
            if viable:
                viability_checks_passed += 1
                viable_populations.append(pop)
                viability_result = "viable"
            else:
                viability_checks_failed += 1
                viability_result = "not_viable"
                fail_note = (
                    "insufficient_files_for_bundle_analysis: "
                    f"population_size={population_size} estimated_threshold={threshold_value}"
                )
                failed_population_notes[pid] = fail_note
                print(
                    f"[step0_viability_fail] domain={dom} candidate_root={_pattern_summary(pop_patterns)} "
                    f"population_files={population_size} estimated_threshold={threshold_value} "
                    f"files_demoted_to_outlier={population_size} "
                    f"reason=\"population_size < estimated_bundle_threshold\""
                )

            all_parameter_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "files_total": str(files_total),
                    "discovery_support_pct": f"{discovery_support_pct:.6f}",
                    "discovery_support_count": str(discovery_support),
                    "min_population_size_effective": str(effective_min_population_size),
                    "max_population_overlap": f"{max_population_overlap:.6f}",
                    "min_population_jaccard": f"{min_population_jaccard:.6f}",
                    "roots_found": str(len(roots)),
                    "substantial_roots_before_cap": str(substantial_roots_before_cap),
                    "substantial_roots_after_cap": str(substantial_roots_after_cap),
                    "populations_identified": str(0),
                    "viability_checks_run": str(0),
                    "viability_checks_passed": str(0),
                    "viability_checks_failed": str(0),
                    "outlier_file_count": str(0),
                    "viability_estimated_threshold": str(threshold_value),
                    "viability_result": viability_result,
                    "viability_notes": viability_notes,
                }
            )

        root_debug_rows: List[Dict[str, str]] = []
        for root in roots:
            root_debug_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "root_pattern_ids": _pattern_token(set(root["pattern_ids"])),
                    "root_pattern_count": str(len(root["pattern_ids"])),
                    "files_present": str(int(root["files_present"])),
                    "is_substantial": "true" if int(root["files_present"]) >= effective_min_population_size else "false",
                }
            )
        atomic_write_csv(
            discovery_dir / f"{dom}_roots.csv",
            ["schema_version", "analysis_run_id", "domain", "root_pattern_ids", "root_pattern_count", "files_present", "is_substantial"],
            sorted(root_debug_rows, key=lambda r: (r["analysis_run_id"], r["domain"], r["root_pattern_ids"])),
        )

        assignments: List[Dict[str, str]] = []
        outlier_count = 0
        for fid in sorted(file_sets.keys()):
            file_patterns = set(file_sets[fid])
            matches: List[Dict[str, object]] = [
                p for p in viable_populations if file_patterns.issuperset(set(p["pattern_ids"]))
            ]
            if not matches:
                failed_matches = [
                    p
                    for p in candidate_populations
                    if str(p["population_id"]) in failed_population_notes and file_patterns.issuperset(set(p["pattern_ids"]))
                ]
                if failed_matches:
                    best_failed = max(failed_matches, key=lambda p: int(p["files_present"]))
                    note = failed_population_notes.get(str(best_failed["population_id"]), "no_substantial_root_found")
                else:
                    note = "no_substantial_root_found"
                    for root in roots:
                        if int(root["files_present"]) >= effective_min_population_size:
                            continue
                        if file_patterns.issuperset(set(root["pattern_ids"])):
                            note = f"matched_non_substantial_root: {_pattern_summary(set(root['pattern_ids']))}"
                            break
                    if note == "no_substantial_root_found":
                        for root in capped_out_roots:
                            if file_patterns.issuperset(set(root["pattern_ids"])):
                                note = f"root_below_candidate_cap: support={int(root['files_present'])}"
                                break
                assignments.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "export_run_id": fid,
                        "population_id": "outlier",
                        "population_role": "outlier",
                        "is_ambiguous": "false",
                        "population_notes": note,
                    }
                )
                outlier_count += 1
                continue

            best = max(
                matches,
                key=lambda p: (
                    len(file_patterns & set(p["pattern_ids"]))
                    / len(file_patterns | set(p["pattern_ids"]))
                    if (file_patterns | set(p["pattern_ids"]))
                    else 0.0,
                    int(p["files_present"]),
                ),
            )
            ambiguous = len(matches) > 1
            notes = f"ambiguous_assignment_resolved_to: {best['population_id']}" if ambiguous else ""
            assignments.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "export_run_id": fid,
                    "population_id": str(best["population_id"]),
                    "population_role": "primary",
                    "is_ambiguous": "true" if ambiguous else "false",
                    "population_notes": notes,
                }
            )

        assignments.sort(key=lambda r: (r["analysis_run_id"], r["domain"], r["population_id"], r["export_run_id"]))
        all_population_rows.extend(assignments)

        pop_counts: Dict[str, int] = {}
        for row in assignments:
            if row["population_role"] == "primary":
                pop_counts[row["population_id"]] = pop_counts.get(row["population_id"], 0) + 1

        for pop in viable_populations:
            pid = str(pop["population_id"])
            count = pop_counts.get(pid, 0)
            root_patterns_sorted = sorted(set(pop["pattern_ids"]))
            all_summary_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "population_id": pid,
                    "population_role": "primary",
                    "file_count": str(count),
                    "pct_of_corpus": f"{((100.0 * count / files_total) if files_total else 0.0):.6f}",
                    "root_pattern_count": str(len(root_patterns_sorted)),
                    "root_bundle_id": make_bundle_id(dom, "", root_patterns_sorted),
                    "discovery_support_used": str(discovery_support),
                    "min_population_size_used": str(effective_min_population_size),
                    "population_notes": "",
                }
            )
            for idx, pattern_id in enumerate(root_patterns_sorted, start=1):
                all_root_pattern_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "population_id": pid,
                        "pattern_id": pattern_id,
                        "pattern_rank": str(idx),
                    }
                )
            print(
                f"[step0_population] domain={dom} population_id={pid} file_count={count} "
                f"pct_of_corpus={(100.0 * count / files_total) if files_total else 0.0:.1f}% "
                f"root_pattern_count={len(pop['pattern_ids'])} role=primary"
            )

        if outlier_count > 0:
            all_summary_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "population_id": "outlier",
                    "population_role": "outlier",
                    "file_count": str(outlier_count),
                    "pct_of_corpus": f"{((100.0 * outlier_count / files_total) if files_total else 0.0):.6f}",
                    "root_pattern_count": "0",
                    "root_bundle_id": "",
                    "discovery_support_used": str(discovery_support),
                    "min_population_size_used": str(effective_min_population_size),
                    "population_notes": "files unmatched_or_non_substantial",
                }
            )
            print(
                f"[step0_outliers] domain={dom} outlier_count={outlier_count} "
                f"reason_summary=\"0 linked/reference files, {outlier_count} unmatched files\""
            )

        for row in all_parameter_rows:
            if row["analysis_run_id"] == run_id and row["domain"] == dom:
                row["populations_identified"] = str(len(viable_populations))
                row["viability_checks_run"] = str(viability_checks_run)
                row["viability_checks_passed"] = str(viability_checks_passed)
                row["viability_checks_failed"] = str(viability_checks_failed)
                row["outlier_file_count"] = str(outlier_count)

        if not candidate_populations:
            all_parameter_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "files_total": str(files_total),
                    "discovery_support_pct": f"{discovery_support_pct:.6f}",
                    "discovery_support_count": str(discovery_support),
                    "min_population_size_effective": str(effective_min_population_size),
                    "max_population_overlap": f"{max_population_overlap:.6f}",
                    "min_population_jaccard": f"{min_population_jaccard:.6f}",
                    "roots_found": str(len(roots)),
                    "substantial_roots_before_cap": str(substantial_roots_before_cap),
                    "substantial_roots_after_cap": str(substantial_roots_after_cap),
                    "populations_identified": "0",
                    "viability_checks_run": "0",
                    "viability_checks_passed": "0",
                    "viability_checks_failed": "0",
                    "outlier_file_count": str(outlier_count),
                    "viability_estimated_threshold": "",
                    "viability_result": "skipped",
                    "viability_notes": "no_candidate_populations",
                }
            )

        print(
            f"[step0] domain={dom} files_total={files_total} roots_found={len(roots)} "
            f"substantial_roots={substantial_roots_after_cap} populations_identified={len(viable_populations)} "
            f"outlier_files={outlier_count} discovery_support={discovery_support}"
        )

    processed_domains = set(domains)

    def _merge(path: Path, new_rows: List[Dict[str, str]], sort_key):
        existing = read_csv_rows(path) if path.exists() else []
        keep_existing = [r for r in existing if r.get("analysis_run_id", "") != run_id or r.get("domain", "") not in processed_domains]
        merged = keep_existing + new_rows
        merged.sort(key=sort_key)
        return merged

    populations_merged = _merge(
        out_dir / "corpus_populations.csv",
        all_population_rows,
        lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("export_run_id", "")),
    )
    summaries_merged = _merge(
        out_dir / "corpus_population_summary.csv",
        all_summary_rows,
        lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")),
    )
    root_patterns_merged = _merge(
        out_dir / "corpus_population_root_patterns.csv",
        all_root_pattern_rows,
        lambda r: (
            r.get("analysis_run_id", ""),
            r.get("domain", ""),
            r.get("population_id", ""),
            int(r.get("pattern_rank", "0") or "0"),
        ),
    )
    params_merged = _merge(
        out_dir / "corpus_population_parameters.csv",
        all_parameter_rows,
        lambda r: (
            r.get("analysis_run_id", ""),
            r.get("domain", ""),
            r.get("viability_result", ""),
            r.get("viability_estimated_threshold", ""),
        ),
    )

    atomic_write_csv(
        out_dir / "corpus_populations.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "export_run_id",
            "population_id",
            "population_role",
            "is_ambiguous",
            "population_notes",
        ],
        populations_merged,
    )
    atomic_write_csv(
        out_dir / "corpus_population_summary.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "population_id",
            "population_role",
            "file_count",
            "pct_of_corpus",
            "root_pattern_count",
            "root_bundle_id",
            "discovery_support_used",
            "min_population_size_used",
            "population_notes",
        ],
        summaries_merged,
    )
    atomic_write_csv(
        out_dir / "corpus_population_root_patterns.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "population_id",
            "pattern_id",
            "pattern_rank",
        ],
        root_patterns_merged,
    )
    atomic_write_csv(
        out_dir / "corpus_population_parameters.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "files_total",
            "discovery_support_pct",
            "discovery_support_count",
            "min_population_size_effective",
            "max_population_overlap",
            "min_population_jaccard",
            "roots_found",
            "substantial_roots_before_cap",
            "substantial_roots_after_cap",
            "populations_identified",
            "viability_checks_run",
            "viability_checks_passed",
            "viability_checks_failed",
            "outlier_file_count",
            "viability_estimated_threshold",
            "viability_result",
            "viability_notes",
        ],
        params_merged,
    )

    return {"domains": len(domains), "population_rows": len(all_population_rows)}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover corpus populations for bundle analysis")
    parser.add_argument("--analysis-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--domain", default="")
    parser.add_argument("--analysis-run-id", default="")
    parser.add_argument("--min-population-size", type=int, default=0)
    parser.add_argument("--max-population-overlap", type=float, default=0.20)
    parser.add_argument("--min-population-jaccard", type=float, default=0.30)
    parser.add_argument(
        "--discovery-support-pct",
        type=float,
        default=0.50,
        help=(
            "Minimum fraction of corpus files a root bundle must appear in to be considered a population "
            "candidate. Higher values find only major configuration families; lower values find minor "
            "subgroups at the cost of significantly more computation. Default: 0.50 "
            "(root must appear in 50%+ of files). Minimum: 0.05. Note: also bounded by --min-population-size floor."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    discover_populations(
        analysis_dir=args.analysis_dir,
        out_dir=args.out_dir,
        domain=args.domain,
        analysis_run_id=args.analysis_run_id,
        min_population_size=args.min_population_size,
        max_population_overlap=args.max_population_overlap,
        min_population_jaccard=args.min_population_jaccard,
        discovery_support_pct=args.discovery_support_pct,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
