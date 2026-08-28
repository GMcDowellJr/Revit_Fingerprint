#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared name-key <-> config-hash rollup helpers (Step 1: pattern_name_fragmentation +
compare_reference.py's name-set-overlap classifier).

Builds, from one segment's own already-materialized `results/records/records.csv`
(config `join_hash` per record) and `results/name_key/name_key_results.csv`
(tools/apply_name_key_policy.py's per-record name-key `join_hash`, segment-filtered by
tools/run_segment_orchestrator.py's Step 2b / `_filter_name_key_csv_to_segment()`), the set
of distinct name-key `join_hash` values observed per (domain, config `join_hash`) -- i.e.
per config pattern identity, expressed directly in join_hash terms (never pattern_id, which
is segment-local and only meaningful after translation through that segment's own
`results/analysis/domain_patterns.csv`).

Consumed by:
  - tools/generate_pattern_name_fragmentation.py (Part A: one segment's own
    pattern_name_fragmentation.csv / _summary.csv, keyed by that segment's local pattern_id
    after translating through domain_patterns.csv's `source_cluster_id`).
  - tools/compare_reference.py (Part B: cross-segment name-set-overlap classification,
    where the comparison identity is already join_hash -- see
    resolve_cross_segment_pattern_identity() -- so no pattern_id translation is needed there
    at all).

Join mechanics (see docs/namekey_crosssegment_step0_findings.md section B for the full
derivation): records.csv and name_key_results.csv are joined on
`(normalize_export_run_id(export_file, known_ids), record_id)`, reusing
tools/bundle_analysis/name_projection_adapter.py::normalize_export_run_id() rather than a
literal `export_run_id == export_file` string comparison, which is wrong for any
split-export model (records.csv's export_run_id is the `.index.json` name; name_key_results
.csv's export_file is the `.details.json` name for the same model -- confirmed at
tools/extractor.py:90-91 and tools/apply_name_key_policy.py:75-77).
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Sequence, Set, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from bundle_analysis.name_projection_adapter import normalize_export_run_id  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.name_key_coverage import ELIGIBLE_DOMAINS  # noqa: E402


def parse_source_cluster_id(source_cluster_id: str):
    """Split a `domain|join_key_schema|join_hash` source_cluster_id -- the identical
    convention on both the config side (tools/extractor.py:723) and the name-key side
    (tools/generate_name_key_patterns.py:216). `join_key_schema` may itself legitimately
    contain no '|' (it never has in either producer). Tolerates a source_cluster_id with
    fewer than 3 parts the same way tools/compare_reference.py's own
    load_domain_pattern_join_hash_map() does (`scid.split("|")[-1]`, no part-count check) --
    domain/schema come back degenerate in that case, but join_hash (the only part any caller
    actually keys facets on) is still correct. Returns None only for an empty/missing value.
    """
    if not source_cluster_id:
        return None
    parts = source_cluster_id.split("|")
    domain = parts[0]
    join_hash = parts[-1]
    schema = "|".join(parts[1:-1])
    return domain, schema, join_hash


def known_export_run_ids(records_rows: Iterable[Dict[str, str]]) -> Set[str]:
    """This segment's own real export_run_id set, straight from records.csv -- the
    `known_ids` normalize_export_run_id() needs to tell a details-only export (keeps its own
    `.details.json` name as export_run_id) apart from a split-export pair (whose canonical
    export_run_id is the sibling `.index.json` name). See
    docs/namekey_crosssegment_step0_findings.md section B.4.
    """
    return {
        (r.get("export_run_id") or "").strip()
        for r in records_rows
        if (r.get("export_run_id") or "").strip()
    }


class DomainNameHashFacets:
    """Per-(domain, config join_hash) rollup of the distinct name-key join_hash values
    observed among that config pattern's member records, for one segment.

    `facets[(domain, config_join_hash)]` is keyed by name-key join_hash; each value is
    `{"record_count": int, "label_counts": Counter[str]}` -- `record_count` is how many of
    this segment's records share this exact (config identity, name-key identity) pair;
    `label_counts` tracks the raw `label_display` strings observed for that name-key
    join_hash (more than one raw string can canonicalize to the same hash -- e.g. a
    trailing-whitespace-only difference; see core/record_v2.py::canonicalize_str).
    """

    def __init__(self) -> None:
        self.facets: Dict[Tuple[str, str], Dict[str, Dict[str, object]]] = {}
        # Same shape as `facets`, but keyed one level finer: (domain, config_join_hash,
        # export_run_id). `facets` aggregates across every export in the segment (what Part A's
        # same-segment fragmentation metric wants); a caller comparing two SPECIFIC files
        # (Part B's compare_reference.py --include-name-overlap, one reference export vs one
        # target export) must use this instead -- looking a pattern's name-hash set up in the
        # aggregate `facets` would silently mix in names from every other file that also
        # happens to carry this same config identity, which is wrong for a file-scoped
        # comparison (PR #476 review: in same-segment mode this bug made every comparison
        # report name_sets_identical, since both sides queried the same segment-wide
        # aggregate for the same key).
        self.facets_by_export: Dict[Tuple[str, str, str], Dict[str, Dict[str, object]]] = {}
        # Every (domain, config_join_hash) this segment's own domain_patterns.csv defines,
        # whether or not any name evidence resolved for it -- callers use this for the
        # "never silently absent" guarantee (CLAUDE.md Fail-Soft Policy).
        self.all_pattern_join_hashes: Dict[str, Set[str]] = {}
        self.domains_observed: Set[str] = set()

    def add_pattern(self, domain: str, join_hash: str) -> None:
        self.domains_observed.add(domain)
        self.all_pattern_join_hashes.setdefault(domain, set()).add(join_hash)

    def record_name(
        self, domain: str, config_join_hash: str, export_run_id: str, name_join_hash: str, label_display: str
    ) -> None:
        key = (domain, config_join_hash)
        bucket = self.facets.setdefault(key, {})
        entry = bucket.setdefault(name_join_hash, {"record_count": 0, "label_counts": Counter()})
        entry["record_count"] += 1
        entry["label_counts"][label_display] += 1

        export_key = (domain, config_join_hash, export_run_id)
        export_bucket = self.facets_by_export.setdefault(export_key, {})
        export_entry = export_bucket.setdefault(name_join_hash, {"record_count": 0, "label_counts": Counter()})
        export_entry["record_count"] += 1
        export_entry["label_counts"][label_display] += 1

    def name_hashes_for(self, domain: str, config_join_hash: str) -> Dict[str, Dict[str, object]]:
        """Segment-wide aggregate across every export -- Part A's fragmentation metric."""
        return self.facets.get((domain, config_join_hash), {})

    def name_hashes_for_export(self, domain: str, config_join_hash: str, export_run_id: str) -> Dict[str, Dict[str, object]]:
        """Scoped to one specific export -- Part B's name-overlap classifier, where the
        reference side and target side must each see only their own compared file's names,
        never the rest of the segment's."""
        return self.facets_by_export.get((domain, config_join_hash, export_run_id), {})


def build_domain_name_hash_facets(
    records_rows: Sequence[Dict[str, str]],
    domain_patterns_rows: Sequence[Dict[str, str]],
    name_key_rows: Sequence[Dict[str, str]],
    eligible_domains: Set[str] = ELIGIBLE_DOMAINS,
) -> DomainNameHashFacets:
    """Build one segment's DomainNameHashFacets from its own already-materialized
    records.csv / domain_patterns.csv (config) / name_key_results.csv rows.

    Every (domain, join_hash) pattern this segment's own domain_patterns.csv defines is
    registered in `all_pattern_join_hashes`/`domains_observed`, regardless of eligibility --
    callers need this to emit an explicit row for every pattern, including one in an
    ineligible domain (which can never resolve name evidence at all) or one with zero
    resolved name evidence in an eligible domain, rather than silently omitting it.

    Only records whose domain is in `eligible_domains` are indexed into `facets`; a domain
    outside `core.name_key_coverage.ELIGIBLE_DOMAINS` has no name-like key by construction
    (core/name_key_coverage.py), so attempting to resolve name evidence for it would be
    meaningless, not merely absent.
    """
    result = DomainNameHashFacets()

    for row in domain_patterns_rows:
        parsed = parse_source_cluster_id(row.get("source_cluster_id", ""))
        if parsed is None:
            continue
        domain, _schema, join_hash = parsed
        result.add_pattern(domain, join_hash)

    known_ids = known_export_run_ids(records_rows)

    # (domain, export_run_id, record_id) -> config join_hash, restricted to eligible
    # domains only (no point indexing a domain that can never resolve name evidence).
    record_join_hash: Dict[Tuple[str, str, str], str] = {}
    for row in records_rows:
        domain = (row.get("domain") or "").strip()
        if domain not in eligible_domains:
            continue
        record_id = (row.get("record_id") or "").strip()
        export_run_id = (row.get("export_run_id") or "").strip()
        join_hash = (row.get("join_hash") or "").strip()
        if not record_id or not export_run_id or not join_hash:
            continue
        record_join_hash[(domain, export_run_id, record_id)] = join_hash

    for row in name_key_rows:
        domain = (row.get("domain") or "").strip()
        if domain not in eligible_domains:
            continue
        if (row.get("status") or "").strip() != "ok":
            continue
        record_id = (row.get("record_id") or "").strip()
        raw_export_file = (row.get("export_file") or "").strip()
        name_join_hash = (row.get("join_hash") or "").strip()
        if not record_id or not raw_export_file or not name_join_hash:
            continue
        normalized = normalize_export_run_id(raw_export_file, known_ids)
        config_join_hash = record_join_hash.get((domain, normalized, record_id))
        if config_join_hash is None:
            # No resolvable config join_hash for this record in this segment's own
            # records.csv (e.g. the record itself is join-key-blocked) -- no config
            # pattern to attach this name evidence to.
            continue
        label_display = (row.get("label_display") or "").strip()
        result.record_name(domain, config_join_hash, normalized, name_join_hash, label_display)

    return result


def representative_label(label_counts: Counter) -> str:
    """Pick one human-readable label to represent a name-hash group: the most frequently
    observed raw `label_display` string, tie-broken lexicographically for determinism
    (Counter iteration order is insertion order, not frequency order, so an explicit sort is
    required for a stable result across runs)."""
    if not label_counts:
        return ""
    return sorted(label_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
