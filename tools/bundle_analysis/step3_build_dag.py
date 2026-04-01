from __future__ import annotations

import argparse
import sys
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows


def build_dag_for_domain(out_dir: Path, domain: str) -> Dict[str, int]:
    domain_out_dir = out_dir / domain
    bundles_rows = read_csv_rows(domain_out_dir / "bundles.csv")
    membership_rows = read_csv_rows(domain_out_dir / "bundle_membership.csv")
    if not bundles_rows:
        analysis_run_id = membership_rows[0].get("analysis_run_id", "") if membership_rows else ""
        atomic_write_csv(
            domain_out_dir / "bundle_dag_edges.csv",
            [
                "schema_version",
                "analysis_run_id",
                "domain",
                "scope_key",
                "child_bundle_id",
                "parent_bundle_id",
                "child_files_present",
                "parent_files_present",
                "is_direct_parent",
            ],
            [],
        )
        atomic_write_csv(
            domain_out_dir / "bundle_dag_nodes.csv",
            [
                "schema_version",
                "analysis_run_id",
                "domain",
                "scope_key",
                "bundle_id",
                "is_root",
                "is_leaf",
                "depth",
                "parent_count",
                "child_count",
            ],
            [],
        )
        return {"edges": 0, "nodes": 0, "analysis_run_id": analysis_run_id}

    analysis_run_id = bundles_rows[0].get("analysis_run_id", "")
    bundle_patterns: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    bundle_files_present: Dict[Tuple[str, str], int] = {}

    for row in bundles_rows:
        key = (row["scope_key"], row["bundle_id"])
        bundle_files_present[key] = int(row.get("files_present", "0") or "0")
    for row in membership_rows:
        bundle_patterns[(row["scope_key"], row["bundle_id"])].add(row["pattern_id"])

    edges: Set[Tuple[str, str, str]] = set()  # scope, child, parent
    for scope_key in sorted({r["scope_key"] for r in bundles_rows}):
        bundle_ids = [r["bundle_id"] for r in bundles_rows if r["scope_key"] == scope_key]
        for child in bundle_ids:
            child_set = bundle_patterns[(scope_key, child)]
            for parent in bundle_ids:
                if child == parent:
                    continue
                parent_set = bundle_patterns[(scope_key, parent)]
                if len(child_set) > len(parent_set) and parent_set.issubset(child_set):
                    edges.add((scope_key, child, parent))

    reduced_edges: Set[Tuple[str, str, str]] = set()
    for scope_key in sorted({e[0] for e in edges}):
        scope_edges = {(c, p) for s, c, p in edges if s == scope_key}
        adjacency: Dict[str, Set[str]] = defaultdict(set)
        for child, parent in scope_edges:
            adjacency[child].add(parent)

        for child, parent in sorted(scope_edges):
            tmp_adj = {k: set(v) for k, v in adjacency.items()}
            tmp_adj[child].discard(parent)
            queue = deque([child])
            seen: Set[str] = set()
            reachable = False
            while queue:
                node = queue.popleft()
                if node == parent:
                    reachable = True
                    break
                for nxt in tmp_adj.get(node, set()):
                    if nxt not in seen:
                        seen.add(nxt)
                        queue.append(nxt)
            if not reachable:
                reduced_edges.add((scope_key, child, parent))

    # cycle guard
    safe_edges: Set[Tuple[str, str, str]] = set()
    for scope_key in sorted({r["scope_key"] for r in bundles_rows}):
        scope_edges = [(c, p) for s, c, p in reduced_edges if s == scope_key]
        indegree: Dict[str, int] = defaultdict(int)
        nodes = {r["bundle_id"] for r in bundles_rows if r["scope_key"] == scope_key}
        children_of: Dict[str, Set[str]] = defaultdict(set)
        for child, parent in scope_edges:
            indegree[child] += 1
            children_of[parent].add(child)
            indegree.setdefault(parent, indegree.get(parent, 0))
        q = deque([n for n in nodes if indegree.get(n, 0) == 0])
        visited = 0
        while q:
            n = q.popleft()
            visited += 1
            for c in children_of.get(n, set()):
                indegree[c] -= 1
                if indegree[c] == 0:
                    q.append(c)
        if visited != len(nodes):
            print(f"[step3][error] cycle detected in domain={domain} scope={scope_key!r}; skipping cyclic edges")
            continue
        for edge in scope_edges:
            safe_edges.add((scope_key, edge[0], edge[1]))

    parent_by_child: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    child_by_parent: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for scope_key, child, parent in safe_edges:
        parent_by_child[(scope_key, child)].add(parent)
        child_by_parent[(scope_key, parent)].add(child)

    def _depth(scope_key: str, bundle_id: str, memo: Dict[str, int]) -> int:
        if bundle_id in memo:
            return memo[bundle_id]
        parents = parent_by_child.get((scope_key, bundle_id), set())
        if not parents:
            memo[bundle_id] = 0
        else:
            memo[bundle_id] = 1 + max(_depth(scope_key, p, memo) for p in parents)
        return memo[bundle_id]

    edge_rows: List[Dict[str, str]] = []
    for scope_key, child, parent in sorted(safe_edges, key=lambda x: (domain, x[0], x[2], x[1])):
        edge_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": domain,
                "scope_key": scope_key,
                "child_bundle_id": child,
                "parent_bundle_id": parent,
                "child_files_present": str(bundle_files_present.get((scope_key, child), 0)),
                "parent_files_present": str(bundle_files_present.get((scope_key, parent), 0)),
                "is_direct_parent": "true",
            }
        )

    node_rows: List[Dict[str, str]] = []
    for scope_key in sorted({r["scope_key"] for r in bundles_rows}):
        memo: Dict[str, int] = {}
        scope_bundle_ids = sorted([r["bundle_id"] for r in bundles_rows if r["scope_key"] == scope_key])
        for bundle_id in scope_bundle_ids:
            parents = parent_by_child.get((scope_key, bundle_id), set())
            children = child_by_parent.get((scope_key, bundle_id), set())
            node_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": analysis_run_id,
                    "domain": domain,
                    "scope_key": scope_key,
                    "bundle_id": bundle_id,
                    "is_root": "true" if not parents else "false",
                    "is_leaf": "true" if not children else "false",
                    "depth": str(_depth(scope_key, bundle_id, memo)),
                    "parent_count": str(len(parents)),
                    "child_count": str(len(children)),
                }
            )
        max_depth = max((int(r["depth"]) for r in node_rows if r["scope_key"] == scope_key), default=0)
        root_count = len([r for r in node_rows if r["scope_key"] == scope_key and r["is_root"] == "true"])
        leaf_count = len([r for r in node_rows if r["scope_key"] == scope_key and r["is_leaf"] == "true"])
        edge_count = len([r for r in edge_rows if r["scope_key"] == scope_key])
        print(
            f"[step3] domain={domain} scope={scope_key!r} root_count={root_count} leaf_count={leaf_count} max_depth={max_depth} edge_count={edge_count}"
        )

    edge_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["parent_bundle_id"], r["child_bundle_id"]))
    node_rows.sort(key=lambda r: (r["domain"], r["scope_key"], int(r["depth"]), r["bundle_id"]))

    atomic_write_csv(
        domain_out_dir / "bundle_dag_edges.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "child_bundle_id",
            "parent_bundle_id",
            "child_files_present",
            "parent_files_present",
            "is_direct_parent",
        ],
        edge_rows,
    )
    atomic_write_csv(
        domain_out_dir / "bundle_dag_nodes.csv",
        [
            "schema_version",
            "analysis_run_id",
            "domain",
            "scope_key",
            "bundle_id",
            "is_root",
            "is_leaf",
            "depth",
            "parent_count",
            "child_count",
        ],
        node_rows,
    )
    return {"edges": len(edge_rows), "nodes": len(node_rows)}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build bundle DAG")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    build_dag_for_domain(args.out_dir, args.domain)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
