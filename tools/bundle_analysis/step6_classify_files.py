from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv
else:
    from .common import atomic_write_csv


def emit_stub(out_dir: Path, domain: str) -> Dict[str, int]:
    import csv
    from collections import defaultdict

    domain_dir = out_dir / domain

    def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
        if not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            return [dict(row) for row in reader]

    def _safe_int(value: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    membership_rows = _read_csv_rows(domain_dir / "membership_matrix.csv")
    bundle_file_rows = _read_csv_rows(domain_dir / "bundle_file_membership.csv")
    dag_node_rows = _read_csv_rows(domain_dir / "bundle_dag_nodes.csv")
    dag_edge_rows = _read_csv_rows(domain_dir / "bundle_dag_edges.csv")
    bundle_membership_rows = _read_csv_rows(domain_dir / "bundle_membership.csv")
    bundles_rows = _read_csv_rows(domain_dir / "bundles.csv")
    _ = _read_csv_rows(domain_dir / "scope_registry.csv")

    analysis_run_id = membership_rows[0].get("analysis_run_id", "") if membership_rows else ""

    file_patterns_by_scope_file: Dict[tuple, set] = defaultdict(set)
    file_keys = set()
    for row in membership_rows:
        scope_key = row.get("scope_key", "")
        export_run_id = row.get("export_run_id", "")
        pattern_id = row.get("pattern_id", "")
        key = (scope_key, export_run_id)
        file_keys.add(key)
        if pattern_id:
            file_patterns_by_scope_file[key].add(pattern_id)

    file_bundles_by_scope_file: Dict[tuple, set] = defaultdict(set)
    for row in bundle_file_rows:
        scope_key = row.get("scope_key", "")
        export_run_id = row.get("export_run_id", "")
        bundle_id = row.get("bundle_id", "")
        if bundle_id:
            file_bundles_by_scope_file[(scope_key, export_run_id)].add(bundle_id)

    bundle_patterns_by_scope: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    any_bundle_patterns_by_scope: Dict[str, set] = defaultdict(set)
    for row in bundle_membership_rows:
        scope_key = row.get("scope_key", "")
        bundle_id = row.get("bundle_id", "")
        pattern_id = row.get("pattern_id", "")
        if not bundle_id or not pattern_id:
            continue
        bundle_patterns_by_scope[scope_key][bundle_id].add(pattern_id)
        any_bundle_patterns_by_scope[scope_key].add(pattern_id)

    bundle_depth_by_scope: Dict[str, Dict[str, int]] = defaultdict(dict)
    for row in dag_node_rows:
        scope_key = row.get("scope_key", "")
        bundle_id = row.get("bundle_id", "")
        if not bundle_id:
            continue
        bundle_depth_by_scope[scope_key][bundle_id] = _safe_int(row.get("depth", "0"))
    has_dag_nodes = bool(dag_node_rows)

    bundle_pattern_count_by_scope: Dict[str, Dict[str, int]] = defaultdict(dict)
    for row in bundles_rows:
        scope_key = row.get("scope_key", "")
        bundle_id = row.get("bundle_id", "")
        if not bundle_id:
            continue
        bundle_pattern_count_by_scope[scope_key][bundle_id] = _safe_int(row.get("pattern_count", "0"))

    parents_by_scope_child: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for row in dag_edge_rows:
        scope_key = row.get("scope_key", "")
        child = row.get("child_bundle_id", "")
        parent = row.get("parent_bundle_id", "")
        if child and parent:
            parents_by_scope_child[scope_key][child].add(parent)

    ancestor_cache: Dict[tuple, set] = {}

    def _ancestors(scope_key: str, bundle_id: str) -> set:
        cache_key = (scope_key, bundle_id)
        if cache_key in ancestor_cache:
            return ancestor_cache[cache_key]
        out = set()
        stack = list(parents_by_scope_child[scope_key].get(bundle_id, set()))
        while stack:
            parent = stack.pop()
            if parent in out:
                continue
            out.add(parent)
            stack.extend(parents_by_scope_child[scope_key].get(parent, set()))
        ancestor_cache[cache_key] = out
        return out

    def _select_primary(scope_key: str, candidates: List[str]) -> tuple:
        if not candidates:
            return ("", "", "false", "0")
        unique_candidates = sorted(set(candidates))
        if has_dag_nodes:
            max_depth = max(bundle_depth_by_scope[scope_key].get(b, 0) for b in unique_candidates)
            top = [b for b in unique_candidates if bundle_depth_by_scope[scope_key].get(b, 0) == max_depth]
            ambiguous = False
            if len(top) > 1:
                for i, left in enumerate(top):
                    left_anc = _ancestors(scope_key, left)
                    for right in top[i + 1 :]:
                        right_anc = _ancestors(scope_key, right)
                        if right not in left_anc and left not in right_anc:
                            ambiguous = True
                            break
                    if ambiguous:
                        break
            return (top[0], str(max_depth), "true" if ambiguous else "false", str(len(unique_candidates)))

        max_patterns = max(bundle_pattern_count_by_scope[scope_key].get(b, 0) for b in unique_candidates)
        top = [b for b in unique_candidates if bundle_pattern_count_by_scope[scope_key].get(b, 0) == max_patterns]
        return (top[0], "0", "false", str(len(unique_candidates)))

    out_rows: List[Dict[str, str]] = []
    files_no_bundle = 0
    for scope_key, export_run_id in sorted(file_keys, key=lambda k: (domain, k[0], k[1])):
        file_patterns = file_patterns_by_scope_file.get((scope_key, export_run_id), set())
        file_pattern_count = len(file_patterns)
        candidates = sorted(file_bundles_by_scope_file.get((scope_key, export_run_id), set()))
        primary_bundle_id, primary_depth, is_ambiguous, bundle_count = _select_primary(scope_key, candidates)

        if bundle_count == "0":
            files_no_bundle += 1
            noise_count_primary = file_pattern_count
            noise_count_any = file_pattern_count
        else:
            primary_patterns = bundle_patterns_by_scope[scope_key].get(primary_bundle_id, set())
            any_patterns = any_bundle_patterns_by_scope.get(scope_key, set())
            noise_count_primary = sum(1 for p in file_patterns if p not in primary_patterns)
            noise_count_any = sum(1 for p in file_patterns if p not in any_patterns)

        if file_pattern_count == 0:
            noise_pct_primary = "0.000000"
            noise_pct_any = "0.000000"
        else:
            noise_pct_primary = f"{(noise_count_primary / file_pattern_count):.6f}"
            noise_pct_any = f"{(noise_count_any / file_pattern_count):.6f}"

        out_rows.append(
            {
                "schema_version": "1.0",
                "analysis_run_id": analysis_run_id,
                "domain": domain,
                "export_run_id": export_run_id,
                "scope_key": scope_key,
                "primary_bundle_id": primary_bundle_id,
                "primary_bundle_depth": primary_depth,
                "is_ambiguous": is_ambiguous,
                "bundle_count": bundle_count,
                "file_pattern_count": str(file_pattern_count),
                "noise_count_primary": str(noise_count_primary),
                "noise_count_any": str(noise_count_any),
                "noise_pct_primary": noise_pct_primary,
                "noise_pct_any": noise_pct_any,
            }
        )

    atomic_write_csv(
        domain_dir / "file_bundle_classification.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "export_run_id",
            "scope_key",
            "primary_bundle_id",
            "primary_bundle_depth",
            "is_ambiguous",
            "bundle_count",
            "file_pattern_count",
            "noise_count_primary",
            "noise_count_any",
            "noise_pct_primary",
            "noise_pct_any",
        ],
        out_rows,
    )
    return {"rows": len(out_rows), "files_no_bundle": files_no_bundle}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 6")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
