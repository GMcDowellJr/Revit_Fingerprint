#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Same-Name-Different-Config Collision Detection.

tools/name_key_rollup.py::DomainNameHashFacets indexes name evidence config-first:
`(domain, config_join_hash) -> {name_join_hash: {record_count, label_counts}}`. That answers
"given the same config, do the names agree?" (tools/compare_reference.py's
--include-name-overlap / NAME_SETS_*). This module answers the inverse question: "given the
same name, does the config agree?" -- the case where a naive person searching or reading by
name alone would assume two things are the same governance object when they are not.

Design choice (see docs write-up in the PR description / Step 0 findings): rather than
extending `DomainNameHashFacets` in place or duplicating
`build_domain_name_hash_facets()`'s CSV-parsing/join loop, this module inverts the ALREADY-
BUILT `DomainNameHashFacets` object produced by that (unmodified, imported) function. Both
hash values are already present together in `facets.facets`/`facets.facets_by_export` (the
outer key is `config_join_hash`, the inner key is `name_join_hash`) -- inverting them is a
pure in-memory transformation, not a second reimplementation of the join. This means:
  - tools/name_key_rollup.py is never modified (tools/compare_reference.py now imports and
    calls classify_name_config_collisions() below, but its own comparison logic and its
    compute_name_overlap_rows() function are untouched -- see the wiring note further down).
  - The reverse index can never disagree with the forward index's join/eligibility/status
    semantics, because it is derived from the forward index's own output, not from a second
    pass over raw CSVs.

This mirrors, in spirit, the codebase's existing "deliberately independent reimplementation"
precedent (tools/pattern_id_utils.py vs tools/extractor.py's `_stable_pattern_id()`) but goes
one step further: no reimplementation of parsing/join logic is needed at all here, only of
the segment-materialization status gate (`name_key_side_status()` below), which is a thin,
side-effect-free path-existence/mtime check -- not comparison logic -- so a local mirror of
tools/compare_reference.py::_name_key_side_status() carries no drift risk.

Wired into tools/compare_reference.py's production output contract via
classify_name_config_collisions() (--no-name-config-collisions to opt out; on by default) --
see that module's own name_config_collision-related block in assemble_final_outputs() and
write_top_level_blocked(), and docs/reference_comparison_tool.md. tools/diagnose_name_config_
collisions.py remains a separate, read-only, single-segment standalone tool built on
find_within_side_name_ambiguities() below, not on classify_name_config_collisions().
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from bundle_analysis.common import read_csv_rows  # noqa: E402
from name_key_rollup import DomainNameHashFacets, build_domain_name_hash_facets, representative_label  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.name_key_coverage import ELIGIBLE_DOMAINS, exclusion_reason  # noqa: E402


# ---------------------------------------------------------------------------
# Classification vocabulary (name-first; mirrors, but is distinct from,
# tools/compare_reference.py's config-first NAME_SETS_*/NAME_EVIDENCE_* constants).
# ---------------------------------------------------------------------------

CONFIG_SETS_IDENTICAL = "config_sets_identical"
CONFIG_SETS_OVERLAP = "config_sets_overlap"
CONFIG_SETS_DISJOINT = "config_sets_disjoint"  # the hazard this module exists to surface
NAME_AMBIGUOUS_WITHIN_SIDE = "name_ambiguous_within_side"
NAME_EVIDENCE_MISSING = "name_evidence_missing"
NAME_EVIDENCE_EXCLUDED = "name_evidence_excluded"

# Mirrors tools/compare_reference.py's NAME_KEY_STATUS_* constants exactly (same string
# values); not imported from there to keep this module decoupled from that protected file.
NAME_KEY_STATUS_OK = "ok"
NAME_KEY_STATUS_NOT_MATERIALIZED = "not_materialized"
NAME_KEY_STATUS_STALE = "stale"


class DomainConfigCollisionFacets:
    """Per-(domain, name-key join_hash) rollup of the distinct config join_hash values
    observed among that name identity's member records, for one segment. Mirror image of
    name_key_rollup.DomainNameHashFacets, with the config/name axes swapped.

    Built exclusively via `invert_domain_name_hash_facets()` -- never populated by a second
    CSV-parsing pass.
    """

    def __init__(self) -> None:
        # (domain, name_join_hash) -> {config_join_hash: {"record_count": int, "label_counts": Counter}}
        self.facets: Dict[Tuple[str, str], Dict[str, Dict[str, object]]] = {}
        # (domain, name_join_hash, export_run_id) -> {config_join_hash: {...}} -- per-export
        # scoped, mirroring DomainNameHashFacets.facets_by_export's rationale exactly: a
        # cross-segment/cross-file comparison must never let unrelated files' config
        # identities leak into a name's apparent config set.
        self.facets_by_export: Dict[Tuple[str, str, str], Dict[str, Dict[str, object]]] = {}
        # (domain) -> set of every name_join_hash observed anywhere in this segment.
        self.names_by_domain: Dict[str, Set[str]] = {}
        # (domain, export_run_id) -> set of name_join_hash values observed in that one export.
        self.names_by_domain_export: Dict[Tuple[str, str], Set[str]] = {}
        self.domains_observed: Set[str] = set()

    def config_hashes_for(self, domain: str, name_join_hash: str) -> Dict[str, Dict[str, object]]:
        """Segment-wide aggregate across every export."""
        return self.facets.get((domain, name_join_hash), {})

    def config_hashes_for_export(self, domain: str, name_join_hash: str, export_run_id: str) -> Dict[str, Dict[str, object]]:
        """Scoped to one specific export."""
        return self.facets_by_export.get((domain, name_join_hash, export_run_id), {})


def invert_domain_name_hash_facets(facets: DomainNameHashFacets) -> DomainConfigCollisionFacets:
    """Pure in-memory inversion of an already-built DomainNameHashFacets: swaps the
    (domain, config_join_hash) -> {name_join_hash: entry} keying to
    (domain, name_join_hash) -> {config_join_hash: entry}, for both the segment-wide
    aggregate and the per-export index. Re-aggregates record_count/label_counts rather than
    reusing the entry dicts by reference (a given (domain, name_join_hash, config_join_hash)
    triple can be reached from more than one source key only in the degenerate case where the
    same pair repeats, which does not happen here, but summing rather than assigning is the
    safe default for an aggregation step).
    """
    result = DomainConfigCollisionFacets()

    for (domain, config_join_hash), name_bucket in facets.facets.items():
        result.domains_observed.add(domain)
        for name_join_hash, entry in name_bucket.items():
            result.names_by_domain.setdefault(domain, set()).add(name_join_hash)
            rev_bucket = result.facets.setdefault((domain, name_join_hash), {})
            rev_entry = rev_bucket.setdefault(config_join_hash, {"record_count": 0, "label_counts": Counter()})
            rev_entry["record_count"] += entry["record_count"]
            rev_entry["label_counts"].update(entry["label_counts"])

    for (domain, config_join_hash, export_run_id), name_bucket in facets.facets_by_export.items():
        for name_join_hash, entry in name_bucket.items():
            result.names_by_domain_export.setdefault((domain, export_run_id), set()).add(name_join_hash)
            rev_bucket = result.facets_by_export.setdefault((domain, name_join_hash, export_run_id), {})
            rev_entry = rev_bucket.setdefault(config_join_hash, {"record_count": 0, "label_counts": Counter()})
            rev_entry["record_count"] += entry["record_count"]
            rev_entry["label_counts"].update(entry["label_counts"])

    return result


def build_domain_config_collision_facets(
    records_rows: Sequence[Dict[str, str]],
    domain_patterns_rows: Sequence[Dict[str, str]],
    name_key_rows: Sequence[Dict[str, str]],
    eligible_domains: Set[str] = ELIGIBLE_DOMAINS,
) -> DomainConfigCollisionFacets:
    """Convenience one-shot: build the forward DomainNameHashFacets (via the unmodified,
    imported tools/name_key_rollup.build_domain_name_hash_facets()) and invert it. Equivalent
    to calling both functions separately; provided so callers who don't need the
    intermediate forward object can skip a line.
    """
    forward = build_domain_name_hash_facets(records_rows, domain_patterns_rows, name_key_rows, eligible_domains)
    return invert_domain_name_hash_facets(forward)


def name_key_side_status(segment_root: Path) -> Tuple[str, Optional[Path], Optional[Path], Optional[Path]]:
    """Mirrors tools/compare_reference.py::_name_key_side_status() exactly (same return
    shape/semantics, same three status strings) without importing it, keeping this module
    decoupled from that protected production file. Purely a path-existence/mtime check --
    no comparison logic to drift out of sync.
    """
    records_csv = segment_root / "results" / "records" / "records.csv"
    domain_patterns_csv = segment_root / "results" / "analysis" / "domain_patterns.csv"
    name_key_csv = segment_root / "results" / "name_key" / "name_key_results.csv"
    if not records_csv.is_file() or not domain_patterns_csv.is_file():
        return NAME_KEY_STATUS_NOT_MATERIALIZED, None, None, None
    if not name_key_csv.is_file():
        return NAME_KEY_STATUS_NOT_MATERIALIZED, records_csv, domain_patterns_csv, None
    if name_key_csv.stat().st_mtime < records_csv.stat().st_mtime:
        return NAME_KEY_STATUS_STALE, records_csv, domain_patterns_csv, name_key_csv
    return NAME_KEY_STATUS_OK, records_csv, domain_patterns_csv, name_key_csv


def load_side_collision_facets(
    segment_root: Path, eligible_domains: Set[str] = ELIGIBLE_DOMAINS
) -> Tuple[str, DomainConfigCollisionFacets]:
    """Load one segment's DomainConfigCollisionFacets (empty if name-key evidence is not
    materialized or is stale for that segment -- see name_key_side_status()). Fail-soft: a
    segment missing name-key materialization degrades to empty facets, never an exception."""
    status, records_csv, domain_patterns_csv, name_key_csv = name_key_side_status(segment_root)
    if status != NAME_KEY_STATUS_OK:
        return status, DomainConfigCollisionFacets()
    records_rows = read_csv_rows(records_csv)
    domain_patterns_rows = read_csv_rows(domain_patterns_csv)
    name_key_rows = read_csv_rows(name_key_csv)
    return status, build_domain_config_collision_facets(records_rows, domain_patterns_rows, name_key_rows, eligible_domains)


def classify_name_config_collisions(
    domains: Sequence[str],
    reference_segment_root: Path,
    target_segment_root: Path,
    same_segment: bool,
    reference_export_run_id: str,
    target_export_run_id: str,
    eligible_domains: Set[str] = ELIGIBLE_DOMAINS,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """For every name-key join_hash observed on either side (scoped to one specific
    reference export and one specific target export -- never a segment-wide aggregate, for
    the same reason tools/compare_reference.py::compute_name_overlap_rows() scopes per
    export), classify the SET relationship between the reference side's and target side's
    config join_hash values for that name identity.

    Returns (out_rows, config_rows). `out_rows` is one row per (domain, name_hash) --
    counts and classification only. `config_rows` is one row per (domain, name_hash, side,
    config_hash) -- the actual hash values, kept out of `out_rows` for the same reason
    tools/compare_reference.py keeps name hashes out of its summary rows (a name can carry
    many distinct configs; pipe-joining them into one cell risks the same Excel-cell-limit
    failure PR #476 fixed for the config-first direction).

    Classification precedence, per name:
      1. Domain not in eligible_domains -> NAME_EVIDENCE_EXCLUDED (matches
         compute_name_overlap_rows's existing exclusion convention).
      2. Neither side has any name evidence for this domain+export -> one NAME_EVIDENCE_MISSING
         row for the whole domain (nothing to iterate).
      3. For a given name: one side has zero configs (the other side never produced this
         name at all in the compared export) -- checked BEFORE any cross-side set
         comparison. If the side that DOES have evidence maps this name to >1 config,
         that's NAME_AMBIGUOUS_WITHIN_SIDE (a real governance hazard visible without any
         cross-side comparison at all -- e.g. tools/diagnose_name_config_collisions.py's
         single-segment scan). If it maps to exactly 1 config, it's an ordinary
         NAME_EVIDENCE_MISSING (nothing to compare).
      4. Both sides have >=1 config for this name -> CONFIG_SETS_IDENTICAL (equal sets,
         including the case where both sides independently agree on the same multi-config
         set), CONFIG_SETS_OVERLAP (sets intersect but differ), or CONFIG_SETS_DISJOINT (no
         intersection at all -- the hazard this module exists to catch: the same name
         legitimately resolving to two completely unrelated configs).
    """
    reference_status, reference_facets = load_side_collision_facets(reference_segment_root, eligible_domains)
    if same_segment:
        target_status, target_facets = reference_status, reference_facets
    else:
        target_status, target_facets = load_side_collision_facets(target_segment_root, eligible_domains)

    out_rows: List[Dict[str, str]] = []
    config_rows: List[Dict[str, str]] = []

    for domain in domains:
        base = {
            "domain": domain,
            "reference_export_run_id": reference_export_run_id,
            "target_export_run_id": target_export_run_id,
            "reference_name_key_status": reference_status,
            "target_name_key_status": target_status,
        }

        if domain not in eligible_domains:
            out_rows.append({
                **base,
                "name_hash": "",
                "representative_label": "",
                "name_config_classification": NAME_EVIDENCE_EXCLUDED,
                "exclusion_reason": exclusion_reason(domain),
                "reference_config_hash_count": "0",
                "target_config_hash_count": "0",
                "shared_config_hash_count": "0",
            })
            continue

        ref_names = (
            reference_facets.names_by_domain_export.get((domain, reference_export_run_id), set())
            if reference_status == NAME_KEY_STATUS_OK else set()
        )
        tgt_names = (
            target_facets.names_by_domain_export.get((domain, target_export_run_id), set())
            if target_status == NAME_KEY_STATUS_OK else set()
        )
        all_names = sorted(ref_names | tgt_names)

        if not all_names:
            out_rows.append({
                **base,
                "name_hash": "",
                "representative_label": "",
                "name_config_classification": NAME_EVIDENCE_MISSING,
                "exclusion_reason": "no_name_evidence",
                "reference_config_hash_count": "0",
                "target_config_hash_count": "0",
                "shared_config_hash_count": "0",
            })
            continue

        for name_hash in all_names:
            ref_bucket = reference_facets.config_hashes_for_export(domain, name_hash, reference_export_run_id)
            tgt_bucket = target_facets.config_hashes_for_export(domain, name_hash, target_export_run_id)
            ref_configs = set(ref_bucket.keys())
            tgt_configs = set(tgt_bucket.keys())

            label_counts: Counter = Counter()
            for entry in ref_bucket.values():
                label_counts.update(entry["label_counts"])
            for entry in tgt_bucket.values():
                label_counts.update(entry["label_counts"])

            if not ref_configs and not tgt_configs:
                classification = NAME_EVIDENCE_MISSING  # unreachable given all_names construction; kept explicit
            elif not tgt_configs:
                classification = NAME_AMBIGUOUS_WITHIN_SIDE if len(ref_configs) > 1 else NAME_EVIDENCE_MISSING
            elif not ref_configs:
                classification = NAME_AMBIGUOUS_WITHIN_SIDE if len(tgt_configs) > 1 else NAME_EVIDENCE_MISSING
            elif ref_configs == tgt_configs:
                classification = CONFIG_SETS_IDENTICAL
            elif ref_configs & tgt_configs:
                classification = CONFIG_SETS_OVERLAP
            else:
                classification = CONFIG_SETS_DISJOINT

            out_rows.append({
                **base,
                "name_hash": name_hash,
                "representative_label": representative_label(label_counts),
                "name_config_classification": classification,
                "exclusion_reason": "",
                "reference_config_hash_count": str(len(ref_configs)),
                "target_config_hash_count": str(len(tgt_configs)),
                "shared_config_hash_count": str(len(ref_configs & tgt_configs)),
            })

            config_base = {
                "domain": domain,
                "reference_export_run_id": reference_export_run_id,
                "target_export_run_id": target_export_run_id,
                "name_hash": name_hash,
            }
            for config_hash in sorted(ref_configs):
                config_rows.append({**config_base, "side": "reference", "config_hash": config_hash})
            for config_hash in sorted(tgt_configs):
                config_rows.append({**config_base, "side": "target", "config_hash": config_hash})

    out_rows.sort(key=lambda r: (r["domain"], r["name_hash"]))
    config_rows.sort(key=lambda r: (r["domain"], r["name_hash"], r["side"], r["config_hash"]))
    return out_rows, config_rows


def find_within_side_name_ambiguities(
    facets: DomainConfigCollisionFacets,
    eligible_domains: Set[str] = ELIGIBLE_DOMAINS,
) -> List[Dict[str, object]]:
    """Single-side (single-segment) scan: for every (domain, name_join_hash) this segment's
    own DomainConfigCollisionFacets carries (segment-wide aggregate, not per-export --
    matching Part A's fragmentation-metric scoping, since this is not a cross-segment/
    cross-file comparison), report the distinct config-hash count observed for that name.

    This is the primitive tools/diagnose_name_config_collisions.py builds its summary table
    from: it needs no "other side" at all, unlike classify_name_config_collisions() above.
    """
    rows: List[Dict[str, object]] = []
    for domain in sorted(facets.names_by_domain):
        if domain not in eligible_domains:
            continue
        for name_hash in sorted(facets.names_by_domain[domain]):
            config_bucket = facets.config_hashes_for(domain, name_hash)
            distinct_configs = sorted(config_bucket.keys())
            label_counts: Counter = Counter()
            for entry in config_bucket.values():
                label_counts.update(entry["label_counts"])
            total_records = sum(entry["record_count"] for entry in config_bucket.values())
            rows.append({
                "domain": domain,
                "name_hash": name_hash,
                "representative_label": representative_label(label_counts),
                "distinct_config_count": len(distinct_configs),
                "config_hashes": distinct_configs,
                "record_count": total_records,
                "is_ambiguous": len(distinct_configs) > 1,
            })
    return rows
