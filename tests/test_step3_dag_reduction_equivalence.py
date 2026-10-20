"""Equivalence + scaling tests for step3_build_dag.py's transitive reduction.

The reduction was rewritten (Run C stall, 2026-10-20) from a per-edge
"copy adjacency, drop edge, BFS for alternate path" loop to a direct-parent
check that relies on the edge relation being strict-subset (transitively closed,
acyclic). These tests pin the new block to the original algorithm byte-for-byte
on bundle_dag_edges.csv / bundle_dag_nodes.csv.

The reference run is produced by substituting the reduction block in the real
step3 source with a call to ``_reduce_reference`` (a verbatim copy of the old
loop), so both runs share the same edge construction, cycle guard, depth and
CSV writers. No helper is extracted from production code.
"""

import csv
import random
import sys
import textwrap
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_DIR = REPO_ROOT / "tools" / "bundle_analysis"
if str(BUNDLE_DIR) not in sys.path:
    sys.path.insert(0, str(BUNDLE_DIR))

import step3_build_dag  # noqa: E402

STEP3_SRC = (BUNDLE_DIR / "step3_build_dag.py").read_text(encoding="utf-8")
ANCHOR = "    reduced_edges: Set[Tuple[str, str, str]] = set()\n"
END = "    # cycle guard\n"


def _reduce_reference(edges):
    # Verbatim copy of the ORIGINAL reduction loop (main @ 5ee0b19, lines 79-102).
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
    return reduced_edges


def _split_source():
    start = STEP3_SRC.index(ANCHOR)
    end = STEP3_SRC.index(END, start)
    return STEP3_SRC[:start], STEP3_SRC[start:end], STEP3_SRC[end:]


def _load_reference_build():
    head, _block, tail = _split_source()
    src = head + "    reduced_edges = _reduce_reference(edges)\n\n" + tail
    ns = {"__name__": "step3_reference", "__package__": None, "__file__": str(BUNDLE_DIR / "step3_build_dag.py")}
    ns["_reduce_reference"] = _reduce_reference
    exec(compile(src, "step3_reference", "exec"), ns)
    return ns["build_dag_for_domain"]


def _run_new_block(edges):
    _head, block, _tail = _split_source()
    ns = {"edges": edges, "defaultdict": defaultdict, "Dict": Dict, "Set": Set, "Tuple": Tuple}
    exec(compile(textwrap.dedent(block), "step3_block", "exec"), ns)
    return ns["reduced_edges"]


_reference_build = _load_reference_build()


def _write_inputs(domain_dir: Path, bundles):
    domain_dir.mkdir(parents=True, exist_ok=True)
    with (domain_dir / "bundles.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["analysis_run_id", "scope_key", "bundle_id", "files_present"])
        w.writeheader()
        for scope, bid, _pats, fp in bundles:
            w.writerow({"analysis_run_id": "run1", "scope_key": scope, "bundle_id": bid, "files_present": fp})
    with (domain_dir / "bundle_membership.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["analysis_run_id", "scope_key", "bundle_id", "pattern_id"])
        w.writeheader()
        for scope, bid, pats, _fp in bundles:
            for p in sorted(pats):
                w.writerow({"analysis_run_id": "run1", "scope_key": scope, "bundle_id": bid, "pattern_id": p})


def _random_bundles(rng: random.Random):
    bundles = []
    for s in range(rng.randint(2, 3)):
        scope = f"scope_{s}"
        universe = [f"p{i}" for i in range(rng.randint(1, 8))]
        n = rng.randint(0, 30)
        made = []
        for b in range(n):
            if made and rng.random() < 0.2:
                pats = set(rng.choice(made))  # duplicate pattern set under a new id
            elif rng.random() < 0.05:
                pats = set()  # empty set (no membership rows)
            else:
                pats = {u for u in universe if rng.random() < rng.random()}
            made.append(pats)
            bundles.append((scope, f"b{b:02d}", pats, str(rng.randint(0, 50))))
    return bundles


def test_reduction_equivalent_to_reference_bytes(tmp_path):
    for seed in range(500):
        rng = random.Random(seed)
        bundles = _random_bundles(rng)
        new_dir, ref_dir = tmp_path / f"{seed}_new", tmp_path / f"{seed}_ref"
        _write_inputs(new_dir / "dom", bundles)
        _write_inputs(ref_dir / "dom", bundles)
        step3_build_dag.build_dag_for_domain(new_dir, "dom")
        _reference_build(ref_dir, "dom")
        for name in ("bundle_dag_edges.csv", "bundle_dag_nodes.csv"):
            assert (new_dir / "dom" / name).read_bytes() == (ref_dir / "dom" / name).read_bytes(), (seed, name)


def test_reduction_block_matches_reference_on_raw_edges():
    rng = random.Random(12345)
    for _ in range(200):
        edges = set()
        for s in range(2):
            sets = {f"b{i}": frozenset(x for x in range(6) if rng.random() < 0.5) for i in range(rng.randint(0, 20))}
            for c, cs in sets.items():
                for p, ps in sets.items():
                    if c != p and len(cs) > len(ps) and ps.issubset(cs):
                        edges.add((f"s{s}", c, p))
        assert _run_new_block(set(edges)) == _reduce_reference(edges)


def test_reduction_scales_on_nested_chain():
    # Nested chain b0 < b1 < ... < b1999 within one scope: E = B*(B-1)/2 ~= 2M edges.
    B = 2000
    names = [f"b{i:04d}" for i in range(B)]
    edges = {("s", names[c], names[p]) for c in range(B) for p in range(c)}
    assert len(edges) == B * (B - 1) // 2
    t0 = time.monotonic()
    reduced = _run_new_block(edges)
    elapsed = time.monotonic() - t0
    assert reduced == {("s", names[i], names[i - 1]) for i in range(1, B)}
    assert elapsed < 60, elapsed
