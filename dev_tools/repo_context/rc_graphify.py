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
                               allow_stale: bool = False,
                               current_dirty: Optional[bool] = None) -> tuple[dict, list]:
    """Return (communities_by_source_file, warnings).

    communities_by_source_file: {relative_path: [(community_id, community_name), ...]}
    sorted and deduplicated, using the graph's own `source_file` field
    (already repo-relative, matching this tool's path convention).

    If graphify-out/graph.json is missing, unparsable, or was built at a
    different git commit than the current scan (and allow_stale is False),
    returns ({}, [warning]) -- callers must treat that as "no Graphify
    evidence available", not an error.

    current_dirty: the scanned worktree's dirty-state (get_git_info()'s
    "dirty" field) -- True/False if known, None if it couldn't be
    determined. A matching commit hash alone doesn't prove the graph
    still describes what's on disk if uncommitted local edits exist (or
    if dirty-state itself couldn't be checked), so by default (not
    allow_stale) either case withholds Graphify evidence too.
    """
    graph_path = root / "graphify-out" / "graph.json"
    if not graph_path.exists():
        return {}, []

    try:
        data = json.loads(graph_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {}, [f"graphify-out/graph.json could not be read/parsed ({exc}); Graphify routing evidence omitted."]

    if not isinstance(data, dict):
        # Valid JSON but the wrong shape (e.g. a bare list or scalar) --
        # since routing loads Graphify during a normal `scan`, letting
        # `.get()` raise AttributeError here would crash the whole scan
        # over one optional, malformed artifact instead of just treating
        # it as unavailable evidence like any other bad graph.json.
        return {}, [
            "graphify-out/graph.json is not a JSON object at its top level; Graphify routing/expansion "
            "evidence is omitted (the file is malformed, not just stale)."
        ]

    built_at_commit = data.get("built_at_commit")
    if not built_at_commit:
        return {}, [
            "graphify-out/graph.json has no built_at_commit field; revision alignment cannot be proven, "
            "so Graphify-derived routing/expansion evidence is omitted by default."
        ]
    if not allow_stale:
        # Revision alignment can only be *proven* when both the scanned
        # repository's current commit and the graph's built_at_commit are
        # known and equal -- if current_commit is unknown (e.g. scanning a
        # folder that isn't a git repo), that is exactly the "cannot be
        # proven" case, not a free pass. A prior version treated a missing
        # current_commit as vacuously compatible, which would accept a
        # copied/stale graph against a non-git checkout with no way to
        # verify it.
        if not current_commit:
            return {}, [
                "the scanned repository's current commit could not be determined (not a git repository, "
                "or git is unavailable); Graphify revision alignment cannot be proven, so Graphify-derived "
                "routing/expansion evidence is omitted by default."
            ]
        if built_at_commit != current_commit:
            return {}, [
                f"graphify-out/graph.json was built at commit {built_at_commit[:12]}, which does not match "
                f"the current scan's HEAD commit {current_commit[:12]}; Graphify-derived routing/expansion "
                f"evidence omitted by default (revision alignment could not be proven)."
            ]
        if current_dirty or current_dirty is None:
            # A matching commit hash on a *dirty* worktree (or one whose
            # dirty-state couldn't even be checked) doesn't prove the
            # graph's communities still describe the files being scanned
            # -- an uncommitted local edit to a tracked file is exactly
            # the "looks aligned but isn't" case this check exists for.
            return {}, [
                "the scanned repository's worktree is dirty (or its clean/dirty state could not be "
                "determined), so a matching commit hash does not prove graphify-out/graph.json still "
                "describes the files on disk; Graphify-derived routing/expansion evidence is omitted "
                "by default."
            ]

    by_file: dict = {}
    for node in data.get("nodes", []) or []:
        if not isinstance(node, dict):
            continue
        source_file = node.get("source_file")
        community = node.get("community")
        if not isinstance(source_file, str) or not source_file or community is None:
            continue
        # community/community_name must be hashable (for the set below)
        # and mutually comparable (for the sorted() call afterward) -- a
        # malformed graph.json (e.g. "community": []) would otherwise
        # crash the whole scan with TypeError: unhashable type, over one
        # optional artifact. Always normalizing to a string (not just
        # when the value isn't already str/int) matters because a single
        # source_file can appear in multiple nodes -- leaving a legitimate
        # int community (e.g. 1) and a legitimate str community (e.g.
        # "2") both as-is would still crash sorted() on that file's tuple
        # set (int and str aren't mutually comparable) even though neither
        # node was individually malformed. A string renders identically to
        # a plain int/str in the f-strings that display it
        # (format_communities).
        community = str(community)
        name = node.get("community_name")
        if not isinstance(name, str):
            name = str(name) if name is not None else ""
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
