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
    """Deterministic pattern_id, extended on collision within `taken` (mutated in place)."""
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
    return f"{join_key_schema} — Variant {pattern_rank} of {pattern_count}"


def rank_clusters(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort clusters by pattern_size_files desc, pattern_size_records desc, pattern_id asc
    (docs/PATTERN_ID_AND_LABEL_RULES.md's pattern_rank ordering)."""
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
    pattern can be formed from a record that has no join_hash)."""
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
