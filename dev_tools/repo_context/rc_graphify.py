"""Optional Graphify adapter.

Graphify (graphify-out/graph.json, if present) is a separate, pre-existing
knowledge-graph tool with its own community-detection algorithm. Everything
this module returns is clearly labeled as Graphify-derived, best-effort
*candidate* routing evidence -- never authoritative, never required, and
never a replacement for the deterministic source excerpts the rest of this
tool produces.

Read-only: this module only reads graphify-out/graph.json (if present) and
never executes or imports scanned/graph content.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


def load_graphify_communities(root: Path, current_commit: Optional[str],
                               allow_stale: bool = False) -> tuple[dict, list]:
    """Return (communities_by_source_file, warnings).

    communities_by_source_file: {relative_path: [(community_id, community_name), ...]}
    sorted and deduplicated, using the graph's own `source_file` field
    (already repo-relative, matching this tool's path convention).

    If graphify-out/graph.json is missing, unparsable, or was built at a
    different git commit than the current scan (and allow_stale is False),
    returns ({}, [warning]) -- callers must treat that as "no Graphify
    evidence available", not an error.
    """
    graph_path = root / "graphify-out" / "graph.json"
    if not graph_path.exists():
        return {}, []

    try:
        data = json.loads(graph_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {}, [f"graphify-out/graph.json could not be read/parsed ({exc}); Graphify routing evidence omitted."]

    built_at_commit = data.get("built_at_commit")
    if current_commit and built_at_commit and built_at_commit != current_commit and not allow_stale:
        return {}, [
            f"graphify-out/graph.json was built at commit {built_at_commit[:12]}, which does not match "
            f"the current scan's HEAD commit {current_commit[:12]}; Graphify-derived routing/expansion "
            f"evidence omitted by default (revision alignment could not be proven)."
        ]
    if not built_at_commit:
        return {}, [
            "graphify-out/graph.json has no built_at_commit field; revision alignment cannot be proven, "
            "so Graphify-derived routing/expansion evidence is omitted by default."
        ]

    by_file: dict = {}
    for node in data.get("nodes", []) or []:
        if not isinstance(node, dict):
            continue
        source_file = node.get("source_file")
        community = node.get("community")
        if not source_file or community is None:
            continue
        name = node.get("community_name") or ""
        by_file.setdefault(source_file, set()).add((community, name))

    return {k: sorted(v) for k, v in by_file.items()}, []


def format_communities(communities: list) -> str:
    """communities: list of (id, name) tuples, as returned per-file by
    load_graphify_communities. Rendered with an explicit "Graphify" label
    so it's never mistaken for deterministic evidence."""
    if not communities:
        return ""
    parts = [f"community {cid}" + (f" (\"{name}\")" if name else "") for cid, name in communities]
    return "Graphify-derived, candidate only: " + "; ".join(parts)
