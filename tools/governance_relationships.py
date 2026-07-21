#!/usr/bin/env python3
"""
tools/governance_relationships.py

Relationship/topology evidence layer, Deliverables 1-3: exports project-level
composition (which projects exist, which client/business-center they belong
to, how many files they carry) and the BC<->client rollups derived from it.

This is presentation/aggregation over data that already exists in
file_metadata.csv -- not new derivation logic, not new comparison math. It
does NOT read cross_segment_summary.csv's cascade-producing comparison types
(enterprise_to_bc/bc_to_project/bc_to_bc/client_cross_bc): those rows carry
only pooled behavioral floats (containment_a_in_b_mean / union_jaccard means,
see build_cascade() in generate_governance_narrative.py) keyed by domain and
BC/client label -- they carry no per-project identity, file counts, or
project rosters, so they cannot supply this module's project_count/
project_file_count/percentage fields. Nor does governance_manifest.csv work
as the project roster source: its rows are governance POPULATIONS grouped by
(unit_system, governance_role, discipline_label, scope_key) -- a single
scope_key="project:{client}:{bc}" row pools every project sharing that
client+bc+discipline+unit combination into one file_count, with no
project-identity column at all (project_label is explicitly not read by
build_segment_manifest.py/governance_manifest.py -- see CLAUDE.md).

The only field in the pipeline that actually identifies one physical project
is file_metadata.csv's `project_label` column, already used for exactly this
grouping purpose by compare_cross_segment.py's `_project_label_for_file()`
(within_project pairing / union-inventory project denominators). This module
reuses that same convention: a blank/NA project_label falls back to that
file's own export_run_id (so an unlabeled file becomes its own single-file
project, matching _project_label_for_file()'s established behavior, not a
new rule invented here).

Project identity is (client_label, business_center_label, project_key), not
project_key alone -- a real production file_metadata.csv can contain the
same project_label string used by two different clients (verified against a
real export: "MPMC" appears under both a Sutter-owned project and an
unrelated Duke LifePoint-owned project in BC 2014). Keying on project_label
alone would silently merge those two distinct projects' file counts into one
row; the composite key keeps them apart without needing a special case for
that collision.

Scope: governance_role == "Project" rows only. Templates/Containers/Generic
are not projects.

Deliverables:
  governance_relationships.csv   -- one row per project
  governance_bc_client_matrix.csv -- one row per (business_center_label,
                                      client_label) pair actually present;
                                      percentage_of_bc/percentage_of_client
                                      are computed HERE ONLY -- this is the
                                      single source of truth for both facts
  governance_client_bc_matrix.csv -- one row per client, aggregated from
                                      governance_bc_client_matrix.csv (no
                                      independent percentage computation)

Usage:
    python tools/governance_relationships.py \\
        --file-meta path/to/file_metadata.csv \\
        --out-dir   path/to/out
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from na_token import is_blank_or_na
from bundle_analysis.common import atomic_write_csv, read_csv_rows

RELATIONSHIPS_FIELDNAMES = [
    "project_id", "project_name", "project_name_is_fallback",
    "client_label", "business_center_label", "discipline_labels", "unit_system",
    "project_file_count", "export_run_ids",
]
BC_CLIENT_MATRIX_FIELDNAMES = [
    "business_center_label", "client_label",
    "project_count", "project_file_count",
    "percentage_of_bc", "percentage_of_client",
]
CLIENT_BC_MATRIX_FIELDNAMES = [
    "client_label", "business_center_count", "business_centers",
    "project_count", "project_file_count",
]


def _project_key(row: Dict[str, str]) -> Tuple[str, bool]:
    """Returns (key, is_fallback). Mirrors compare_cross_segment.py's
    _project_label_for_file(): a blank/NA project_label falls back to this
    file's own export_run_id, so an unlabeled file becomes its own
    single-file project rather than being silently dropped or pooled with
    every other unlabeled file."""
    label = (row.get("project_label", "") or "").strip()
    if is_blank_or_na(label):
        return row.get("export_run_id", "").strip(), True
    return label, False


def _project_id(client_label: str, business_center_label: str, project_key: str) -> str:
    token = f"{client_label}|{business_center_label}|{project_key}"
    return "proj_" + hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]


def build_relationships_rows(
    file_meta_rows: List[Dict[str, str]],
) -> Tuple[List[Dict[str, str]], List[str]]:
    """Group file_metadata.csv's governance_role == "Project" rows into one
    row per (client_label, business_center_label, project_key) identity.

    Returns (rows, warnings). warnings are non-blocking data-quality notes
    (e.g. inconsistent unit_system within one project identity) -- fail-soft,
    not fail-hard: a project with an internal inconsistency is still real
    and still gets a row, but the caller should see the note.
    """
    groups: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    warnings: List[str] = []

    for row in file_meta_rows:
        if (row.get("governance_role", "") or "").strip() != "Project":
            continue
        client_label = (row.get("client_label", "") or "").strip()
        business_center_label = (row.get("business_center_label", "") or "").strip()
        project_key, is_fallback = _project_key(row)
        discipline_label = (row.get("discipline_label", "") or "").strip()
        unit_system = (row.get("unit_system", "") or "").strip()
        export_run_id = (row.get("export_run_id", "") or "").strip()

        key = (client_label, business_center_label, project_key)
        bucket = groups.setdefault(key, {
            "project_name": project_key,
            "project_name_is_fallback": is_fallback,
            "discipline_labels": set(),
            "unit_systems": set(),
            "export_run_ids": [],
        })
        if discipline_label:
            bucket["discipline_labels"].add(discipline_label)
        if unit_system:
            bucket["unit_systems"].add(unit_system)
        if export_run_id:
            bucket["export_run_ids"].append(export_run_id)

    out_rows: List[Dict[str, str]] = []
    for (client_label, business_center_label, project_key), bucket in sorted(groups.items()):
        unit_systems = sorted(bucket["unit_systems"])
        if len(unit_systems) > 1:
            warnings.append(
                f"project identity (client={client_label!r}, bc={business_center_label!r}, "
                f"project={project_key!r}) spans more than one unit_system: {unit_systems} "
                "-- reporting the first (sorted) value; this is a data-quality issue in "
                "file_metadata.csv, not resolved silently by this tool."
            )
        export_run_ids = sorted(bucket["export_run_ids"])
        out_rows.append({
            "project_id": _project_id(client_label, business_center_label, project_key),
            "project_name": bucket["project_name"],
            "project_name_is_fallback": "true" if bucket["project_name_is_fallback"] else "false",
            "client_label": client_label,
            "business_center_label": business_center_label,
            "discipline_labels": "|".join(sorted(bucket["discipline_labels"])),
            "unit_system": unit_systems[0] if unit_systems else "",
            "project_file_count": str(len(export_run_ids)),
            "export_run_ids": "|".join(export_run_ids),
        })

    return out_rows, warnings


def build_bc_client_matrix_rows(relationship_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """One row per (business_center_label, client_label) pair actually
    present in relationship_rows. percentage_of_bc/percentage_of_client are
    computed here and only here -- downstream consumers (client_bc_matrix,
    narrative sections) must read these values, not recompute them."""
    pair_projects: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    pair_files: Dict[Tuple[str, str], int] = defaultdict(int)
    bc_totals: Dict[str, int] = defaultdict(int)
    client_totals: Dict[str, int] = defaultdict(int)

    for r in relationship_rows:
        bc = r["business_center_label"]
        client = r["client_label"]
        n = int(r["project_file_count"])
        key = (bc, client)
        pair_projects[key].append(r["project_id"])
        pair_files[key] += n
        bc_totals[bc] += n
        client_totals[client] += n

    out_rows: List[Dict[str, str]] = []
    for (bc, client) in sorted(pair_files.keys()):
        n_files = pair_files[(bc, client)]
        bc_total = bc_totals[bc]
        client_total = client_totals[client]
        out_rows.append({
            "business_center_label": bc,
            "client_label": client,
            "project_count": str(len(pair_projects[(bc, client)])),
            "project_file_count": str(n_files),
            "percentage_of_bc": f"{(n_files / bc_total):.6f}" if bc_total else "",
            "percentage_of_client": f"{(n_files / client_total):.6f}" if client_total else "",
        })
    return out_rows


def build_client_bc_matrix_rows(bc_client_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """One row per client, aggregated from bc_client_rows. No independent
    percentage computation -- business_centers is ordered by the already-
    computed percentage_of_client, descending."""
    by_client: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in bc_client_rows:
        by_client[r["client_label"]].append(r)

    out_rows: List[Dict[str, str]] = []
    for client in sorted(by_client.keys()):
        rows = by_client[client]
        rows_sorted = sorted(
            rows,
            key=lambda r: (-float(r["percentage_of_client"]) if r["percentage_of_client"] else 0.0, r["business_center_label"]),
        )
        out_rows.append({
            "client_label": client,
            "business_center_count": str(len(rows)),
            "business_centers": "|".join(r["business_center_label"] for r in rows_sorted),
            "project_count": str(sum(int(r["project_count"]) for r in rows)),
            "project_file_count": str(sum(int(r["project_file_count"]) for r in rows)),
        })
    return out_rows


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build governance_relationships.csv / governance_bc_client_matrix.csv / "
                    "governance_client_bc_matrix.csv from file_metadata.csv.",
    )
    parser.add_argument("--file-meta", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--out-dir", required=True, help="Directory to write output files")
    args = parser.parse_args(argv)

    file_meta_path = Path(args.file_meta)
    if not file_meta_path.is_file():
        sys.stderr.write(f"[ERROR] --file-meta not found: {file_meta_path}\n")
        return 1

    file_meta_rows = read_csv_rows(file_meta_path)
    relationship_rows, warnings = build_relationships_rows(file_meta_rows)
    for w in warnings:
        sys.stderr.write(f"[WARN governance_relationships] {w}\n")

    bc_client_rows = build_bc_client_matrix_rows(relationship_rows)
    client_bc_rows = build_client_bc_matrix_rows(bc_client_rows)

    out_dir = Path(args.out_dir)
    atomic_write_csv(out_dir / "governance_relationships.csv", RELATIONSHIPS_FIELDNAMES, relationship_rows)
    atomic_write_csv(out_dir / "governance_bc_client_matrix.csv", BC_CLIENT_MATRIX_FIELDNAMES, bc_client_rows)
    atomic_write_csv(out_dir / "governance_client_bc_matrix.csv", CLIENT_BC_MATRIX_FIELDNAMES, client_bc_rows)

    print(
        f"[governance_relationships] {len(relationship_rows)} project(s), "
        f"{len(bc_client_rows)} BC/client pair(s), {len(client_bc_rows)} client(s), "
        f"{len(warnings)} warning(s)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
