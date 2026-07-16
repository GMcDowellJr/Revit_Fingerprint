#!/usr/bin/env python3
"""
tools/compare_governance_populations.py

Compares governance populations produced by tools/governance_manifest.py
(governance_manifest.csv / governance_membership.csv) — Enterprise, each
business center, each client, each named project, and Generic — using the
same file-grain join_hash containment/Jaccard mechanics as
tools/compare_cross_segment.py, imported rather than reimplemented.

This is a separate file from compare_cross_segment.py and does not touch the
segment lattice, ancestor-suppression logic, or used/all purge-view split.
Governance populations are disjoint by construction (see governance_manifest.
py's scope_key partitioning), so there is no nesting/lineage relationship
between them to suppress the way _is_lineage_related() suppresses
parent/child segment pairs — if a real need for that logic ever surfaces
here, that would mean the populations aren't actually disjoint and something
upstream is wrong, not that this module needs to grow that machinery.

Three comparison shapes:
  - same-role peers: any two populations sharing (unit_system, governance_
    role), regardless of scope_key, compared symmetrically (Jaccard +
    containment). Produces bc_to_bc, bc_to_client, client_to_client,
    enterprise_to_bc, enterprise_to_client, project_to_project, etc. from one
    function instead of several bespoke ones.
  - directed Template/Container -> Project: populations sharing the same
    scope_key (client/bc identity), or an Enterprise-scoped Template/
    Container population against every Project population unconditionally,
    compared with containment (Template/Container as the provided-vocabulary
    reference, Project as the target).
  - Generic/Generic-Host -> Template/Container/Project: paired
    unconditionally (no scope_key on the Generic side at all), matching
    compare_cross_segment.py's existing generic-pairing loop's conditions —
    same non-blank unit_system, discipline_label match-or-wildcard (blank on
    either side matches anything) — compared with containment.

Usage:
    python tools/compare_governance_populations.py \\
        --records-dir "path/to/exports/results/records" \\
        --out-dir     "path/to/exports/results/records"
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from bundle_analysis.common import atomic_write_csv, read_csv_rows
from compare_cross_segment import compare_symmetric_file, compare_directed_file, make_comparison_run_id

SCOPE_LEVEL_RANK = {"enterprise": 0, "bc": 1, "client": 2, "project": 3}

CROSS_GOVERNANCE_FIELDNAMES = [
    "comparison_run_id",
    "governance_id_a", "governance_id_b",
    "governance_role_a", "governance_role_b",
    "scope_key_a", "scope_key_b",
    "scope_level_a", "scope_level_b",
    "client_label_a", "client_label_b",
    "business_center_label_a", "business_center_label_b",
    "discipline_label_a", "discipline_label_b",
    "unit_system", "domain", "comparison_type",
    "n_files_a", "n_files_b", "n_pairs",
    "n_shared_join_hash",
    "all_containment_a_in_b_mean", "all_containment_a_in_b_min",
    "all_containment_b_in_a_mean", "all_containment_b_in_a_min",
    "all_jaccard_mean", "all_jaccard_p10", "all_jaccard_p90",
    "executed_utc",
]


# ---------------------------------------------------------------------------
# join_hash inventory — built directly from corpus-level records.csv rather
# than membership_matrix.csv. There is no corpus-level (unsegmented)
# membership_matrix.csv anywhere in the pipeline; it is only ever produced
# per-segment by run_bundle_analysis.py, driven off segment_manifest.csv/
# run_registry.csv. Governance populations here are not registered segments,
# so segment-scoped lookups (load_file_join_hashes, get_role_jh_set) do not
# apply. join_hash is already a stable, corpus-wide identity value on every
# records.csv row (set during the flatten stage's sig_hash-as-join-key
# bootstrap, then possibly overwritten by the apply stage's real join-key
# policy) — reading it straight from records.csv reproduces the same
# all-view file-grain join_hash inventory membership_matrix.csv would carry,
# minus the used-view purge filter, which this module does not need (only
# all-view containment/Jaccard is in scope here).
# ---------------------------------------------------------------------------

def load_join_hashes_by_domain(records_rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Set[str]]]:
    """Returns {domain: {export_run_id: set(join_hash)}}, blank join_hash excluded."""
    result: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in records_rows:
        join_hash = row.get("join_hash", "").strip()
        if not join_hash:
            continue
        domain = row.get("domain", "").strip()
        export_run_id = row.get("export_run_id", "").strip()
        if not domain or not export_run_id:
            continue
        result[domain][export_run_id].add(join_hash)
    return {domain: dict(eids) for domain, eids in result.items()}


def _files_for_population(
    export_run_ids: List[str], jh_by_eid: Dict[str, Set[str]]
) -> Dict[str, Set[str]]:
    return {eid: jh_by_eid.get(eid, set()) for eid in export_run_ids}


# ---------------------------------------------------------------------------
# Pair discovery
# ---------------------------------------------------------------------------

def _disc_match(disc_a: str, disc_b: str) -> bool:
    if not disc_a or not disc_b:
        return True
    return disc_a == disc_b


def discover_same_role_peer_pairs(
    manifest_rows: List[Dict[str, str]],
) -> List[Tuple[Dict[str, str], Dict[str, str], str]]:
    """Any two populations sharing (unit_system, governance_role), regardless
    of scope_key. Generic/Generic-Host is excluded — it has no scope_key and
    is compared separately via discover_generic_pairs(). Project is also
    excluded: Project populations are consumption-end outputs, not
    standards-authority pools, and are only ever the target side of the
    directed Template/Container -> Project containment comparison (see
    discover_directed_tc_to_project_pairs()). Peer-comparing two Project
    populations of different scope levels would produce the exact same
    comparison_type string (e.g. "bc_to_project") as that directed
    comparison, making the output ambiguous without also keying off
    governance_role_a/governance_role_b."""
    by_key: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in manifest_rows:
        if row.get("scope_level") == "generic" or row["governance_role"] == "Project":
            continue
        by_key[(row["unit_system"], row["governance_role"])].append(row)

    pairs: List[Tuple[Dict[str, str], Dict[str, str], str]] = []
    for _key, populations in by_key.items():
        ordered = sorted(populations, key=lambda r: r["governance_id"])
        for i in range(len(ordered)):
            for j in range(i + 1, len(ordered)):
                pop_a, pop_b = ordered[i], ordered[j]
                if not _disc_match(pop_a["discipline_label"], pop_b["discipline_label"]):
                    continue
                rank_a = SCOPE_LEVEL_RANK.get(pop_a["scope_level"], 99)
                rank_b = SCOPE_LEVEL_RANK.get(pop_b["scope_level"], 99)
                if rank_a > rank_b or (rank_a == rank_b and pop_a["scope_key"] > pop_b["scope_key"]):
                    pop_a, pop_b = pop_b, pop_a
                comparison_type = f"{pop_a['scope_level']}_to_{pop_b['scope_level']}"
                pairs.append((pop_a, pop_b, comparison_type))
    return pairs


def discover_directed_tc_to_project_pairs(
    manifest_rows: List[Dict[str, str]],
) -> List[Tuple[Dict[str, str], Dict[str, str], str]]:
    """Template/Container populations vs. Project populations sharing the
    same scope_key (client/bc identity) — or an Enterprise-scoped Template/
    Container population against every Project population, unconditionally,
    mirroring enterprise_to_project/bc_to_project's existing intent."""
    tc_pops = [r for r in manifest_rows if r["governance_role"] in ("Template", "Container")]
    project_pops = [r for r in manifest_rows if r["governance_role"] == "Project"]

    pairs: List[Tuple[Dict[str, str], Dict[str, str], str]] = []
    for tc in tc_pops:
        for proj in project_pops:
            if tc["unit_system"] != proj["unit_system"]:
                continue
            if not _disc_match(tc["discipline_label"], proj["discipline_label"]):
                continue
            if tc["scope_level"] != "enterprise" and tc["scope_key"] != proj["scope_key"]:
                continue
            pairs.append((tc, proj, f"{tc['scope_level']}_to_project"))
    return pairs


def discover_generic_pairs(
    manifest_rows: List[Dict[str, str]],
) -> List[Tuple[Dict[str, str], Dict[str, str], str]]:
    """Generic/Generic-Host paired unconditionally against every Template/
    Container/Project population — same non-blank unit_system, discipline_
    label match-or-wildcard, no client/bc scoping at all. Mirrors
    compare_cross_segment.py's unconditional generic-pairing loop (its
    generic_ids loop, not the by_key-scoped one)."""
    generic_pops = [r for r in manifest_rows if r["scope_level"] == "generic"]
    target_pops = [r for r in manifest_rows if r["governance_role"] in ("Template", "Container", "Project")]

    pairs: List[Tuple[Dict[str, str], Dict[str, str], str]] = []
    for g in generic_pops:
        for target in target_pops:
            if not g["unit_system"] or g["unit_system"] != target["unit_system"]:
                continue
            if not _disc_match(g["discipline_label"], target["discipline_label"]):
                continue
            role_key = target["governance_role"].lower()
            pairs.append((g, target, f"generic_to_{role_key}"))
    return pairs


# ---------------------------------------------------------------------------
# Comparison execution
# ---------------------------------------------------------------------------

def _pop_export_run_ids(governance_id: str, membership_rows: List[Dict[str, str]]) -> List[str]:
    return [r["export_run_id"] for r in membership_rows if r["governance_id"] == governance_id]


def run_comparisons(
    manifest_rows: List[Dict[str, str]],
    membership_rows: List[Dict[str, str]],
    records_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    jh_by_domain = load_join_hashes_by_domain(records_rows)
    domains = sorted(jh_by_domain.keys())

    membership_by_gov_id: Dict[str, List[str]] = defaultdict(list)
    for row in membership_rows:
        membership_by_gov_id[row["governance_id"]].append(row["export_run_id"])

    symmetric_pairs = discover_same_role_peer_pairs(manifest_rows)
    directed_pairs = (
        discover_directed_tc_to_project_pairs(manifest_rows)
        + discover_generic_pairs(manifest_rows)
    )

    out_rows: List[Dict[str, str]] = []

    def _base_row(pop_a, pop_b, comparison_type, domain) -> Dict[str, str]:
        comparison_run_id = make_comparison_run_id(pop_a["governance_id"], pop_b["governance_id"], executed_utc)
        return {
            "comparison_run_id": comparison_run_id,
            "governance_id_a": pop_a["governance_id"], "governance_id_b": pop_b["governance_id"],
            "governance_role_a": pop_a["governance_role"], "governance_role_b": pop_b["governance_role"],
            "scope_key_a": pop_a["scope_key"], "scope_key_b": pop_b["scope_key"],
            "scope_level_a": pop_a["scope_level"], "scope_level_b": pop_b["scope_level"],
            "client_label_a": pop_a["client_label"], "client_label_b": pop_b["client_label"],
            "business_center_label_a": pop_a["business_center_label"],
            "business_center_label_b": pop_b["business_center_label"],
            "discipline_label_a": pop_a["discipline_label"], "discipline_label_b": pop_b["discipline_label"],
            "unit_system": pop_a["unit_system"], "domain": domain, "comparison_type": comparison_type,
            "executed_utc": executed_utc,
        }

    for pop_a, pop_b, comparison_type in symmetric_pairs:
        eids_a = membership_by_gov_id.get(pop_a["governance_id"], [])
        eids_b = membership_by_gov_id.get(pop_b["governance_id"], [])
        for domain in domains:
            files_a = _files_for_population(eids_a, jh_by_domain[domain])
            files_b = _files_for_population(eids_b, jh_by_domain[domain])
            metrics, _pair_rows = compare_symmetric_file(files_a, files_b)
            row = _base_row(pop_a, pop_b, comparison_type, domain)
            row.update(metrics)
            out_rows.append(row)

    for pop_a, pop_b, comparison_type in directed_pairs:
        eids_a = membership_by_gov_id.get(pop_a["governance_id"], [])
        eids_b = membership_by_gov_id.get(pop_b["governance_id"], [])
        for domain in domains:
            files_a = _files_for_population(eids_a, jh_by_domain[domain])
            files_b = _files_for_population(eids_b, jh_by_domain[domain])
            metrics = compare_directed_file(files_a, files_b)
            row = _base_row(pop_a, pop_b, comparison_type, domain)
            row.update(metrics)
            out_rows.append(row)

    return out_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--records-dir", required=True, help="Directory containing file_metadata.csv/records.csv")
    ap.add_argument("--manifest-dir", default=None, help="Directory containing governance_manifest.csv/governance_membership.csv (default: --records-dir)")
    ap.add_argument("--out-dir", default=None, help="Output directory (default: --records-dir)")
    args = ap.parse_args()

    records_dir = Path(args.records_dir).resolve()
    manifest_dir = Path(args.manifest_dir).resolve() if args.manifest_dir else records_dir
    out_dir = Path(args.out_dir).resolve() if args.out_dir else records_dir

    manifest_path = manifest_dir / "governance_manifest.csv"
    membership_path = manifest_dir / "governance_membership.csv"
    records_path = records_dir / "records.csv"
    for p in (manifest_path, membership_path, records_path):
        if not p.is_file():
            raise SystemExit(f"required input not found: {p}")

    manifest_rows = read_csv_rows(manifest_path)
    membership_rows = read_csv_rows(membership_path)
    records_rows = read_csv_rows(records_path)

    out_rows = run_comparisons(manifest_rows, membership_rows, records_rows)
    atomic_write_csv(out_dir / "cross_governance_comparison.csv", CROSS_GOVERNANCE_FIELDNAMES, out_rows)

    comparison_types = sorted({r["comparison_type"] for r in out_rows})
    print(
        f"[compare_governance_populations] {len(out_rows)} comparison row(s) across "
        f"{len(comparison_types)} comparison_type(s): {', '.join(comparison_types)}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
