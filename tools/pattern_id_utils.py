#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared pattern_id / pattern_label helpers per docs/PATTERN_ID_AND_LABEL_RULES.md.

    pattern_id = "pat_" + base32lower_nopad(sha1(f"{domain}|{join_key_schema}|{join_hash}"))[:16]
    pattern_label = f"{join_key_schema} — Variant {pattern_rank} of {N}"

This is a deliberately independent reimplementation of the same formula
`tools/extractor.py`'s private `_stable_pattern_id()` already uses for the production
join_hash-based pattern pipeline -- kept as its own stdlib-only module rather than importing
from `tools/extractor.py` so that nothing in `tools/generate_name_key_patterns.py` (PR2)
can accidentally couple to, or destabilize, that pipeline's byte-identical production
output. Both implementations follow the same documented contract; a change to one without
the other would be caught by tests/test_pattern_id_utils.py asserting the formula directly.
"""
from __future__ import annotations

import base64
import hashlib
from typing import Any, Dict, List, Set, Tuple


def stable_pattern_id(domain: str, join_key_schema: str, join_hash: str, taken: Set[str]) -> str:
    """Deterministic pattern_id, extended on collision within `taken` (mutated in place).

    --- trace ---
    reads: `domain`, `join_key_schema`, `join_hash` -- caller-supplied strings, from
        build_clusters()'s per-group loop; `taken` -- a Set[str] threaded across every call
        within one build_clusters() run, mutated in place to dedupe pattern_ids batch-wide.
    calls: hashlib.sha1(), base64.b32encode() (stdlib).
    thresholds: `16` -- minimum base32 token length before the `pat_` prefix is accepted
        (hardcoded literal, per docs/PATTERN_ID_AND_LABEL_RULES.md's formula); `"pat_"`
        prefix literal.
    returns: a `pat_<base32-lower-nopad>` string unique within `taken` (also mutates
        `taken` by adding the returned id); consumed by build_clusters() as each cluster's
        `pattern_id`.

    NOTE (mechanical-extraction risk): `taken` is caller-owned mutable state passed by
    reference and mutated as a side effect across repeated calls in one batch -- not a
    closure, but a naive per-function static parser reading only this function's own
    reads/returns would miss that its behavior depends on accumulated state from prior
    calls in the same build_clusters() invocation.
    """
    raw = f"{domain}|{join_key_schema}|{join_hash}"
    digest = hashlib.sha1(raw.encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").lower().rstrip("=")
    for n in range(16, len(token) + 1):
        candidate = f"pat_{token[:n]}"
        if candidate not in taken:
            taken.add(candidate)
            return candidate
    candidate = f"pat_{token}"
    taken.add(candidate)
    return candidate


def pattern_label(join_key_schema: str, pattern_rank: int, pattern_count: int) -> str:
    """--- trace ---
    reads: `join_key_schema`, `pattern_rank`, `pattern_count` -- all caller-supplied, from
        tools/generate_name_key_patterns.py's build_name_patterns() (rank/N from
        enumerate(rank_clusters(...))).
    calls: none.
    thresholds: none named -- the format string itself ("{schema} — Variant {rank} of
        {count}") is a hardcoded literal matching docs/PATTERN_ID_AND_LABEL_RULES.md.
    returns: formatted label string; consumed by build_name_patterns() as the
        `pattern_label` output column.
    """
    return f"{join_key_schema} — Variant {pattern_rank} of {pattern_count}"


def rank_clusters(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort clusters by pattern_size_files desc, pattern_size_records desc, pattern_id asc
    (docs/PATTERN_ID_AND_LABEL_RULES.md's pattern_rank ordering).

    --- trace ---
    reads: `clusters` -- list of cluster dicts, caller-supplied (build_clusters()'s output,
        grouped per domain and passed in by
        tools/generate_name_key_patterns.py's build_name_patterns()).
    calls: sorted() (builtin) keyed on cluster fields.
    thresholds: the 3-tuple sort key (files desc, records desc, pattern_id asc) is a
        hardcoded tuple here, matching docs/PATTERN_ID_AND_LABEL_RULES.md's pattern_rank
        rule -- not read from a policy file or named constant.
    returns: same cluster dicts, reordered list; consumed by build_name_patterns() via
        enumerate(..., start=1) to assign each cluster's `pattern_rank`.
    """
    return sorted(
        clusters,
        key=lambda c: (-int(c["pattern_size_files"]), -int(c["pattern_size_records"]), c["pattern_id"]),
    )


def build_clusters(
    rows: List[Dict[str, Any]],
    *,
    domain_key: str = "domain",
    schema_key: str = "join_key_schema",
    hash_key: str = "join_hash",
    file_key: str = "export_file",
) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    """Group rows into clusters keyed by (domain, join_key_schema, join_hash), assigning a
    stable pattern_id per cluster. Rows with an empty/missing join_hash are excluded (no
    pattern can be formed from a record that has no join_hash).

    --- trace ---
    reads: `rows` -- list of row dicts, caller-supplied (in this pipeline,
        tools/generate_name_key_patterns.py's build_name_patterns() passes its
        eligible-domain-filtered name-key rows); `domain_key`/`schema_key`/`hash_key`/`file_key`
        -- keyword params naming which row fields to group on, defaulting to
        the name-key CSV's own column names ("domain"/"join_key_schema"/"join_hash"/
        "export_file").
    calls: stable_pattern_id() (once per distinct group key, in sorted-key order so
        pattern_id assignment is deterministic across runs).
    thresholds: the four default column-name literals in the signature are hardcoded, not
        policy-sourced -- callers may override them, but no caller in this repo currently
        does.
    returns: Dict[(domain, schema, join_hash), cluster dict] with keys
        domain/join_key_schema/join_hash/pattern_id/pattern_size_records/pattern_size_files/rows;
        consumed by tools/generate_name_key_patterns.py's build_name_patterns().
    """
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        jh = row.get(hash_key)
        if not jh:
            continue
        key = (str(row.get(domain_key, "")), str(row.get(schema_key, "")), str(jh))
        groups.setdefault(key, []).append(row)

    taken: Set[str] = set()
    clusters: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for key in sorted(groups.keys()):
        domain, schema, join_hash = key
        member_rows = groups[key]
        files_present = len({r.get(file_key, "") for r in member_rows})
        pid = stable_pattern_id(domain, schema, join_hash, taken)
        clusters[key] = {
            "domain": domain,
            "join_key_schema": schema,
            "join_hash": join_hash,
            "pattern_id": pid,
            "pattern_size_records": len(member_rows),
            "pattern_size_files": files_present,
            "rows": member_rows,
        }
    return clusters
