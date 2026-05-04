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
    from common import (
        ROW_KEY_DOMAINS,
        SCHEMA_VERSION,
        atomic_write_csv,
        derive_scope_key,
        make_bundle_id,
        read_csv_rows,
        resolve_analysis_run_id,
    )
    from step2_find_bundles import compute_auto_threshold
    from utils import find_root_bundles
else:
    from .common import (
        ROW_KEY_DOMAINS,
        SCHEMA_VERSION,
        atomic_write_csv,
        derive_scope_key,
        make_bundle_id,
        read_csv_rows,
        resolve_analysis_run_id,
    )
    from .step2_find_bundles import compute_auto_threshold
    from .utils import find_root_bundles

MAX_POPULATION_CANDIDATES = 20


def _pattern_summary(pattern_ids: Set[str]) -> str:
    return "|".join(sorted(pattern_ids)[:3])


def _population_id(domain: str, scope_key: str, pattern_ids: Set[str]) -> str:
    # Population ID includes scope_key to prevent collisions between
    # element_label scopes within row-key domains. scope_key is "" for
    # unscoped domains. NOTE: adding scope_key to hash is a breaking
    # change from runs prior to this implementation — existing
    # population IDs are not forward-compatible.
    return make_bundle_id(domain, scope_key, sorted(pattern_ids)).replace("bnd_", "pop_", 1)


def _select_populations(substantial_roots: List[Dict[str, object]], max_population_overlap: float, min_population_jaccard: float) -> List[Dict[str, object]]:
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
            overlap = max(in_both / len(files_a) if files_a else 0.0, in_both / len(files_b) if files_b else 0.0)

            pa = set(candidate["pattern_ids"])
            pb = set(existing["pattern_ids"])
            union = len(pa | pb)
            dissim = 1.0 - ((len(pa & pb) / union) if union else 1.0)
            if not (overlap < max_population_overlap and dissim >= min_population_jaccard):
                keep = False
                break
        if keep:
            selected.append(candidate)
    return selected


def _collapse_subset_related_roots(substantial_roots: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Collapse subset/superset-related roots to most general representatives.

    This function assumes root candidates are close to antichain behavior
    (as produced by `find_root_bundles`), and uses a postcondition check to
    fail fast if any subset relation survives.
    """
    ordered = sorted(
        substantial_roots,
        key=lambda r: (len(set(r["pattern_ids"])), -int(r["files_present"]), tuple(sorted(set(r["pattern_ids"])))),
    )
    collapsed: List[Dict[str, object]] = []
    for root in ordered:
        root_patterns = set(root["pattern_ids"])
        is_redundant = False
        replaced_idx: Optional[int] = None
        for idx, kept in enumerate(collapsed):
            kept_patterns = set(kept["pattern_ids"])
            if root_patterns > kept_patterns:
                is_redundant = True
                break
            if root_patterns < kept_patterns:
                replaced_idx = idx
                break
        if is_redundant:
            continue
        if replaced_idx is not None:
            collapsed.pop(replaced_idx)
        collapsed.append(root)

    # postcondition: no subset relationships remain
    for i, a in enumerate(collapsed):
        pa = set(a["pattern_ids"])
        for j, b in enumerate(collapsed):
            if i == j:
                continue
            pb = set(b["pattern_ids"])
            if pa < pb or pb < pa:
                raise ValueError("Subset-collapse invariant violated: retained roots remain subset-related.")
    return collapsed


def discover_populations(
    analysis_dir: Path,
    out_dir: Path,
    domain: str = "",
    analysis_run_id: str = "",
    min_population_size: int = 0,
    max_population_overlap: float = 0.20,
    min_population_jaccard: float = 0.30,
    discovery_support_pct: float = 0.50,
    placeholder_exclusions_path: Optional[Path] = None,
    allowed_export_run_ids: Optional[Set[str]] = None,
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
    domains = [domain] if domain else sorted({r.get("domain", "") for r in pattern_presence_rows if r.get("analysis_run_id", "") == run_id and r.get("domain", "")})

    all_population_rows: List[Dict[str, str]] = []
    all_summary_rows: List[Dict[str, str]] = []
    all_parameter_rows: List[Dict[str, str]] = []
    all_root_pattern_rows: List[Dict[str, str]] = []

    discovery_dir = out_dir / "_population_discovery"

    excluded_pairs: Set[tuple[str, str]] = set()
    if placeholder_exclusions_path and placeholder_exclusions_path.exists():
        for row in read_csv_rows(placeholder_exclusions_path):
            dom = (row.get("domain", "") or "").strip()
            fid = (row.get("file_id", "") or "").strip()
            excluded = (row.get("excluded", "") or "").strip().lower()
            if dom and fid and excluded == "true":
                excluded_pairs.add((dom, fid))


    for dom in domains:
        pattern_meta: Dict[str, Dict[str, str]] = {}
        cad_patterns: Set[str] = set()
        for row in domain_pattern_rows:
            if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != dom:
                continue
            pid = row.get("pattern_id", "")
            if not pid:
                continue
            pattern_meta[pid] = row
            if (row.get("is_cad_import", "") or "").strip().lower() == "true":
                cad_patterns.add(pid)

        file_patterns_by_scope: Dict[str, Dict[str, Set[str]]] = {}
        excluded_for_domain: Set[str] = set()
        for row in pattern_presence_rows:
            if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != dom:
                continue
            fid = (row.get("export_run_id", "") or "").strip()
            pid = (row.get("pattern_id", "") or "").strip()
            if not fid or not pid or pid in cad_patterns:
                continue
            if allowed_export_run_ids is not None and fid not in allowed_export_run_ids:
                continue
            if (dom, fid) in excluded_pairs:
                excluded_for_domain.add(fid)
                continue
            meta = pattern_meta.get(pid)
            if meta is None:
                continue
            scope_key = derive_scope_key(dom, meta)
            file_patterns_by_scope.setdefault(scope_key, {}).setdefault(fid, set()).add(pid)

        if excluded_for_domain:
            print(f"[step0] domain={dom} excluded {len(excluded_for_domain)} placeholder files via purgeable_pct threshold")

        domain_populations = 0
        domain_outliers = 0
        scopes_processed = 0

        for scope_key in sorted(file_patterns_by_scope.keys()):
            scopes_processed += 1
            file_sets = {fid: frozenset(pids) for fid, pids in file_patterns_by_scope[scope_key].items() if pids}
            files_total = len(file_sets)
            discovery_support = max(3, int(math.ceil(files_total * discovery_support_pct))) if files_total else 3
            min_pop_size_effective = max(int(min_population_size or 0), max(5, int(math.ceil(files_total * 0.05)))) if files_total else 5

            roots = find_root_bundles(file_sets, min_support=discovery_support, min_bundle_size=2) if files_total >= 2 else []
            substantial_roots = [r for r in roots if int(r["files_present"]) >= min_pop_size_effective]
            substantial_roots_count = len(substantial_roots)
            before_collapse = len(substantial_roots)
            substantial_roots = _collapse_subset_related_roots(substantial_roots)
            after_collapse = len(substantial_roots)
            print(
                f"[step0_collapse] domain={dom} scope={scope_key!r} "
                f"substantial_roots_before_collapse={before_collapse} "
                f"substantial_roots_after_collapse={after_collapse} "
                f"collapsed={before_collapse - after_collapse}"
            )
            before_cap = len(substantial_roots)
            substantial_roots = sorted(substantial_roots, key=lambda r: (-int(r["files_present"]), tuple(sorted(r["pattern_ids"]))))

            is_row_key_domain = dom in ROW_KEY_DOMAINS
            if is_row_key_domain and before_cap > MAX_POPULATION_CANDIDATES:
                raise ValueError(
                    f"Row-key domain {dom} scope {scope_key!r} has {before_cap} substantial roots after scoping. "
                    f"Expected <= {MAX_POPULATION_CANDIDATES}. Check scope derivation logic — row-key domains should produce "
                    "few roots per element_label scope."
                )

            capped_out_roots = substantial_roots[MAX_POPULATION_CANDIDATES:]
            if before_cap > MAX_POPULATION_CANDIDATES:
                retained = int(substantial_roots[MAX_POPULATION_CANDIDATES - 1]["files_present"])
                discarded = before_cap - MAX_POPULATION_CANDIDATES
                print(
                    f"[step0_cap_WARNING] domain={dom} scope={scope_key!r} substantial_roots_after_collapse={before_cap} "
                    f"capped_to={MAX_POPULATION_CANDIDATES} lowest_retained_support={retained} discarded={discarded} "
                    "ACTION_REQUIRED: genuinely distinct roots (not subset-related) were discarded. "
                    "This indicates unexpected configuration diversity that warrants investigation. "
                    "Consider raising --discovery-support-pct or inspecting this domain/scope manually."
                )
            substantial_roots = substantial_roots[:MAX_POPULATION_CANDIDATES]
            after_cap = len(substantial_roots)

            candidate_populations = _select_populations(substantial_roots, max_population_overlap, min_population_jaccard)
            for pop in candidate_populations:
                pop["population_id"] = _population_id(dom, scope_key, set(pop["pattern_ids"]))

            viable_populations: List[Dict[str, object]] = []
            failed_notes: Dict[str, str] = {}
            checks_run = checks_passed = checks_failed = 0
            if candidate_populations:
                threshold_values: List[int] = []
                for pop in candidate_populations:
                    checks_run += 1
                    pop_patterns = set(pop["pattern_ids"])
                    pop_size = len([1 for pset in file_sets.values() if set(pset).issuperset(pop_patterns)])
                    try:
                        th = int(compute_auto_threshold({fid: p for fid, p in file_sets.items() if set(p).issuperset(pop_patterns)}, files_total=pop_size).get("chosen", 0))
                    except Exception as exc:
                        truncated = str(exc)[:100]
                        print(f"[step0_viability_warn] domain={dom} scope={scope_key!r} auto_threshold_failed={truncated} falling_back_to_heuristic")
                        th = max(3, int(math.ceil(pop_size * 0.10)))
                    threshold_values.append(th)
                    viable = pop_size >= th
                    print(f"[step0_viability] domain={dom} scope={scope_key!r} population_id={pop['population_id']} population_files={pop_size} estimated_threshold={th} viable={viable}")
                    if viable:
                        checks_passed += 1
                        viable_populations.append(pop)
                    else:
                        checks_failed += 1
                        note = f"insufficient_files_for_bundle_analysis: population_size={pop_size} estimated_threshold={th}"
                        failed_notes[str(pop["population_id"])] = note
                        print(
                            f"[step0_viability_fail] domain={dom} scope={scope_key!r} candidate_root={_pattern_summary(pop_patterns)} "
                            f"population_files={pop_size} estimated_threshold={th} files_demoted_to_outlier={pop_size} "
                            f"reason=\"population_size < estimated_bundle_threshold\""
                        )
                viability_threshold = "|".join(str(v) for v in sorted(set(threshold_values)))
                viability_result = "mixed" if checks_passed and checks_failed else ("viable" if checks_passed else "not_viable")
                viability_notes = ""
            else:
                viability_threshold = ""
                viability_result = "skipped"
                viability_notes = "no_candidate_populations"

            assignments: List[Dict[str, str]] = []
            outlier_count = 0
            for fid, file_patterns in sorted(file_sets.items()):
                file_pattern_set = set(file_patterns)
                matches = [p for p in viable_populations if file_pattern_set.issuperset(set(p["pattern_ids"]))]
                if not matches:
                    note = "no_substantial_root_found"
                    for root in roots:
                        if int(root["files_present"]) < min_pop_size_effective and file_pattern_set.issuperset(set(root["pattern_ids"])):
                            note = f"matched_non_substantial_root: {_pattern_summary(set(root['pattern_ids']))}"
                            break
                    if note == "no_substantial_root_found":
                        for root in capped_out_roots:
                            if file_pattern_set.issuperset(set(root["pattern_ids"])):
                                note = f"root_below_candidate_cap: support={int(root['files_present'])}"
                                break
                    assignments.append({
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "scope_key": scope_key,
                        "export_run_id": fid,
                        "population_id": "outlier",
                        "population_role": "outlier",
                        "is_ambiguous": "false",
                        "population_notes": note,
                    })
                    outlier_count += 1
                else:
                    best = max(matches, key=lambda p: int(p["files_present"]))
                    ambiguous = len(matches) > 1
                    assignments.append({
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "scope_key": scope_key,
                        "export_run_id": fid,
                        "population_id": str(best["population_id"]),
                        "population_role": "primary",
                        "is_ambiguous": "true" if ambiguous else "false",
                        "population_notes": f"ambiguous_assignment_resolved_to: {best['population_id']}" if ambiguous else "",
                    })

            assignments.sort(key=lambda r: (r["analysis_run_id"], r["domain"], r["scope_key"], r["export_run_id"], r["population_id"]))
            all_population_rows.extend(assignments)

            pop_counts: Dict[str, int] = {}
            for row in assignments:
                if row["population_role"] == "primary":
                    pop_counts[row["population_id"]] = pop_counts.get(row["population_id"], 0) + 1

            for pop in viable_populations:
                pid = str(pop["population_id"])
                root_patterns_sorted = sorted(set(pop["pattern_ids"]))
                count = pop_counts.get(pid, 0)
                all_summary_rows.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "scope_key": scope_key,
                    "population_id": pid,
                    "population_role": "primary",
                    "file_count": str(count),
                    "pct_of_corpus": f"{((100.0 * count / files_total) if files_total else 0.0):.6f}",
                    "root_pattern_count": str(len(root_patterns_sorted)),
                    "root_bundle_id": make_bundle_id(dom, scope_key, root_patterns_sorted),
                    "discovery_support_used": str(discovery_support),
                    "min_population_size_used": str(min_pop_size_effective),
                    "population_notes": "",
                })
                for idx, pattern_id in enumerate(root_patterns_sorted, start=1):
                    all_root_pattern_rows.append({
                        "schema_version": SCHEMA_VERSION,
                        "analysis_run_id": run_id,
                        "domain": dom,
                        "scope_key": scope_key,
                        "population_id": pid,
                        "pattern_id": pattern_id,
                        "pattern_rank": str(idx),
                    })
                print(f"[step0_population] domain={dom} scope={scope_key!r} population_id={pid} file_count={count} pct_of_corpus={(100.0 * count / files_total) if files_total else 0.0:.1f}% root_pattern_count={len(root_patterns_sorted)} role=primary")

            if outlier_count > 0:
                all_summary_rows.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "scope_key": scope_key,
                    "population_id": "outlier",
                    "population_role": "outlier",
                    "file_count": str(outlier_count),
                    "pct_of_corpus": f"{((100.0 * outlier_count / files_total) if files_total else 0.0):.6f}",
                    "root_pattern_count": "0",
                    "root_bundle_id": "",
                    "discovery_support_used": str(discovery_support),
                    "min_population_size_used": str(min_pop_size_effective),
                    "population_notes": "files unmatched_or_non_substantial",
                })

            all_parameter_rows.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": run_id,
                "domain": dom,
                "scope_key": scope_key,
                "files_total": str(files_total),
                "discovery_support_pct": f"{discovery_support_pct:.6f}",
                "discovery_support_count": str(discovery_support),
                "min_population_size_effective": str(min_pop_size_effective),
                "max_population_overlap": f"{max_population_overlap:.6f}",
                "min_population_jaccard": f"{min_population_jaccard:.6f}",
                "roots_found": str(len(roots)),
                "substantial_roots": str(substantial_roots_count),
                "substantial_roots_before_collapse": str(before_collapse),
                "substantial_roots_after_collapse": str(after_collapse),
                "substantial_roots_before_cap": str(before_cap),
                "substantial_roots_after_cap": str(after_cap),
                "populations_identified": str(len(viable_populations)),
                "viability_checks_run": str(checks_run),
                "viability_checks_passed": str(checks_passed),
                "viability_checks_failed": str(checks_failed),
                "outlier_file_count": str(outlier_count),
                "viability_estimated_threshold": viability_threshold,
                "viability_result": viability_result,
                "viability_notes": viability_notes,
            })

            print(f"[step0] domain={dom} scope={scope_key!r} files_total={files_total} roots_found={len(roots)} substantial_roots={after_cap} populations_identified={len(viable_populations)} outlier_files={outlier_count} discovery_support={discovery_support}")

            domain_populations += len(viable_populations)
            domain_outliers += outlier_count

        print(f"[step0_domain_summary] domain={dom} scopes_processed={scopes_processed} total_populations={domain_populations} total_outlier_files={domain_outliers}")

        root_debug_rows = []
        for scope_key, scoped_files in sorted(file_patterns_by_scope.items()):
            for fid, pids in scoped_files.items():
                root_debug_rows.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": run_id,
                    "domain": dom,
                    "scope_key": scope_key,
                    "export_run_id": fid,
                    "pattern_count": str(len(pids)),
                })
        atomic_write_csv(
            discovery_dir / f"{dom}_roots.csv",
            ["schema_version", "analysis_run_id", "domain", "scope_key", "export_run_id", "pattern_count"],
            sorted(root_debug_rows, key=lambda r: (r["analysis_run_id"], r["domain"], r["scope_key"], r["export_run_id"])),
        )

    processed_domains = set(domains)

    def _merge(path: Path, new_rows: List[Dict[str, str]], sort_key):
        existing = read_csv_rows(path) if path.exists() else []
        keep_existing = [r for r in existing if r.get("analysis_run_id", "") != run_id or r.get("domain", "") not in processed_domains]
        merged = keep_existing + new_rows
        merged.sort(key=sort_key)
        return merged

    populations_merged = _merge(out_dir / "corpus_populations.csv", all_population_rows, lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("scope_key", ""), r.get("export_run_id", ""), r.get("population_id", "")))
    summaries_merged = _merge(out_dir / "corpus_population_summary.csv", all_summary_rows, lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("scope_key", ""), r.get("population_id", "")))
    root_patterns_merged = _merge(out_dir / "corpus_population_root_patterns.csv", all_root_pattern_rows, lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("scope_key", ""), r.get("population_id", ""), int(r.get("pattern_rank", "0") or "0")))
    params_merged = _merge(out_dir / "corpus_population_parameters.csv", all_parameter_rows, lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("scope_key", "")))

    atomic_write_csv(out_dir / "corpus_populations.csv", ["schema_version", "analysis_run_id", "domain", "scope_key", "export_run_id", "population_id", "population_role", "is_ambiguous", "population_notes"], populations_merged)
    atomic_write_csv(out_dir / "corpus_population_summary.csv", ["schema_version", "analysis_run_id", "domain", "scope_key", "population_id", "population_role", "file_count", "pct_of_corpus", "root_pattern_count", "root_bundle_id", "discovery_support_used", "min_population_size_used", "population_notes"], summaries_merged)
    atomic_write_csv(out_dir / "corpus_population_root_patterns.csv", ["schema_version", "analysis_run_id", "domain", "scope_key", "population_id", "pattern_id", "pattern_rank"], root_patterns_merged)
    atomic_write_csv(out_dir / "corpus_population_parameters.csv", ["schema_version", "analysis_run_id", "domain", "scope_key", "files_total", "discovery_support_pct", "discovery_support_count", "min_population_size_effective", "max_population_overlap", "min_population_jaccard", "roots_found", "substantial_roots", "substantial_roots_before_collapse", "substantial_roots_after_collapse", "substantial_roots_before_cap", "substantial_roots_after_cap", "populations_identified", "viability_checks_run", "viability_checks_passed", "viability_checks_failed", "outlier_file_count", "viability_estimated_threshold", "viability_result", "viability_notes"], params_merged)

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
    parser.add_argument("--discovery-support-pct", type=float, default=0.50)
    parser.add_argument("--placeholder-exclusions", type=Path, default=None)
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
        placeholder_exclusions_path=args.placeholder_exclusions,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
