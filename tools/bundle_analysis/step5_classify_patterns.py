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
    from collections import defaultdict, deque

    output_path = out_dir / domain / "pattern_bundle_classification.csv"
    fieldnames = [
        "schema_version",
        "analysis_run_id",
        "domain",
        "scope_key",
        "pattern_id",
        "bundle_role",
        "bundle_count",
        "is_cross_branch_shared",
        "corpus_presence_pct",
    ]

    base_dir = out_dir / domain
    membership_path = base_dir / "bundle_membership.csv"
    nodes_path = base_dir / "bundle_dag_nodes.csv"
    edges_path = base_dir / "bundle_dag_edges.csv"
    matrix_path = base_dir / "membership_matrix.csv"
    scope_registry_path = base_dir / "scope_registry.csv"

    def _norm(v: str) -> str:
        return (v or "").strip()

    def _truthy(v: str) -> bool:
        return _norm(v).lower() == "true"

    if not membership_path.exists():
        atomic_write_csv(output_path, fieldnames, [])
        return {"rows": 0}

    with membership_path.open("r", encoding="utf-8", newline="") as f:
        membership_rows = list(csv.DictReader(f))

    if not membership_rows:
        atomic_write_csv(output_path, fieldnames, [])
        return {"rows": 0}

    analysis_run_id = _norm(membership_rows[0].get("analysis_run_id", ""))
    schema_version = _norm(membership_rows[0].get("schema_version", ""))

    pattern_to_bundles: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    scope_bundles: Dict[str, set[str]] = defaultdict(set)
    for row in membership_rows:
        scope_key = _norm(row.get("scope_key", ""))
        bundle_id = _norm(row.get("bundle_id", ""))
        pattern_id = _norm(row.get("pattern_id", ""))
        if not scope_key or not bundle_id or not pattern_id:
            continue
        pattern_to_bundles[scope_key][pattern_id].add(bundle_id)
        scope_bundles[scope_key].add(bundle_id)

    nodes_rows: List[dict] = []
    if nodes_path.exists():
        with nodes_path.open("r", encoding="utf-8", newline="") as f:
            nodes_rows = list(csv.DictReader(f))
    has_nodes = bool(nodes_rows)

    roots: Dict[str, set[str]] = defaultdict(set)
    is_leaf: Dict[str, Dict[str, bool]] = defaultdict(dict)
    is_root: Dict[str, Dict[str, bool]] = defaultdict(dict)
    if has_nodes:
        for row in nodes_rows:
            scope_key = _norm(row.get("scope_key", ""))
            bundle_id = _norm(row.get("bundle_id", ""))
            if not scope_key or not bundle_id:
                continue
            root_flag = _truthy(row.get("is_root", ""))
            leaf_flag = _truthy(row.get("is_leaf", ""))
            if root_flag:
                roots[scope_key].add(bundle_id)
            is_root[scope_key][bundle_id] = root_flag
            is_leaf[scope_key][bundle_id] = leaf_flag

    descendants: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    if has_nodes and edges_path.exists():
        with edges_path.open("r", encoding="utf-8", newline="") as f:
            edge_rows = list(csv.DictReader(f))
        parents_of: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        children_of: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for row in edge_rows:
            scope_key = _norm(row.get("scope_key", ""))
            child = _norm(row.get("child_bundle_id", ""))
            parent = _norm(row.get("parent_bundle_id", ""))
            if not scope_key or not child or not parent:
                continue
            parents_of[scope_key][child].add(parent)
            children_of[scope_key][parent].add(child)
        for scope_key, bundles in scope_bundles.items():
            for bundle in bundles:
                if bundle not in children_of[scope_key]:
                    continue
                seen: set[str] = set()
                q: deque[str] = deque(children_of[scope_key].get(bundle, set()))
                while q:
                    cur = q.popleft()
                    if cur in seen:
                        continue
                    seen.add(cur)
                    for nxt in children_of[scope_key].get(cur, set()):
                        if nxt not in seen:
                            q.append(nxt)
                descendants[scope_key][bundle] = seen

    pattern_presence_runs: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    if matrix_path.exists():
        with matrix_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                scope_key = _norm(row.get("scope_key", ""))
                pattern_id = _norm(row.get("pattern_id", ""))
                export_run_id = _norm(row.get("export_run_id", ""))
                if not scope_key or not pattern_id or not export_run_id:
                    continue
                pattern_presence_runs[scope_key][pattern_id].add(export_run_id)

    files_in_scope: Dict[str, int] = {}
    if scope_registry_path.exists():
        with scope_registry_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                scope_key = _norm(row.get("scope_key", ""))
                if not scope_key:
                    continue
                try:
                    files_in_scope[scope_key] = int(_norm(row.get("files_in_scope", "0")) or "0")
                except ValueError:
                    files_in_scope[scope_key] = 0

    out_rows: List[Dict[str, str]] = []
    for scope_key, pattern_map in pattern_to_bundles.items():
        total_bundles = len(scope_bundles.get(scope_key, set()))
        for pattern_id, bundles in pattern_map.items():
            bundle_count = len(bundles)

            if not has_nodes:
                bundle_role = "orphan"
                cross_branch = "false"
            else:
                if total_bundles > 0 and bundle_count == total_bundles:
                    bundle_role = "universal"
                elif any(b in roots.get(scope_key, set()) for b in bundles):
                    bundle_role = "foundation"
                elif bundles and all(is_leaf[scope_key].get(b, False) for b in bundles):
                    bundle_role = "differentiating"
                elif bundles and all(
                    (not is_root[scope_key].get(b, False)) and (not is_leaf[scope_key].get(b, False))
                    for b in bundles
                ):
                    bundle_role = "intermediate"
                else:
                    bundle_role = "orphan"

                bundle_list = list(bundles)
                cross_branch_flag = False
                for i in range(len(bundle_list)):
                    if cross_branch_flag:
                        break
                    for j in range(i + 1, len(bundle_list)):
                        a = bundle_list[i]
                        b = bundle_list[j]
                        a_desc = descendants[scope_key].get(a, set())
                        b_desc = descendants[scope_key].get(b, set())
                        if b not in a_desc and a not in b_desc:
                            cross_branch_flag = True
                            break
                cross_branch = "true" if cross_branch_flag else "false"

            run_count = len(pattern_presence_runs[scope_key].get(pattern_id, set()))
            scope_files = files_in_scope.get(scope_key, 0)
            if scope_files > 0:
                presence_pct = f"{(run_count / scope_files):.6f}"
            else:
                presence_pct = "0.000000"

            out_rows.append(
                {
                    "schema_version": schema_version,
                    "analysis_run_id": analysis_run_id,
                    "domain": domain,
                    "scope_key": scope_key,
                    "pattern_id": pattern_id,
                    "bundle_role": bundle_role,
                    "bundle_count": str(bundle_count),
                    "is_cross_branch_shared": cross_branch,
                    "corpus_presence_pct": presence_pct,
                }
            )

    role_order = {
        "universal": 0,
        "foundation": 1,
        "differentiating": 2,
        "intermediate": 3,
        "orphan": 4,
    }
    out_rows.sort(
        key=lambda r: (
            r["domain"],
            r["scope_key"],
            role_order.get(r["bundle_role"], 99),
            r["pattern_id"],
        )
    )
    atomic_write_csv(output_path, fieldnames, out_rows)
    return {"rows": len(out_rows)}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stub step 5")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    emit_stub(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
