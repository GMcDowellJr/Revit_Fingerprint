#!/usr/bin/env python3
"""
tools/governance_manifest.py

Builds disjoint, independently-comparable governance populations (Enterprise,
each business center, each client, each named project, and Generic) directly
from file_metadata.csv.

This is deliberately NOT built on top of tools/build_segment_manifest.py's
powerset lattice. That lattice enumerates every subset of cut dimensions
(client_label x discipline_label x business_center_label x collection_label)
for corpus rollup/segment-orchestration purposes and stays exactly as-is;
it is not the source for this module. Governance populations here are a
single, flat, disjoint partition of Template/Container/Project rows by
(unit_system, governance_role, discipline_label, scope_key), plus a separate
unscoped Generic/Generic-Host population set.

Depends on the Run B governance-field completeness gate
(run_extract_all._check_governance_field_completeness) having already run
against file_metadata.csv, so client_label/business_center_label are never
blank or an N/A spelling by the time rows reach this module. This module
re-runs that same check itself (defense in depth) rather than trusting the
gate blindly.

Usage:
    python tools/governance_manifest.py \\
        --records-dir "path/to/exports/results/records" \\
        --out-dir     "path/to/exports/results/records"
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

from na_token import ENTERPRISE_BC_BOOKKEEPING_TOKENS
from enterprise_policy import EnterprisePolicy, load_enterprise_policy, write_enterprise_policy_provenance
from run_extract_all import _check_governance_field_completeness
from bundle_analysis.common import atomic_write_csv, read_csv_rows

# governance_role values (case-insensitive) this module knows how to scope.
# Generic/Generic-Host is handled separately (no scope_key at all) — mirrors
# compare_cross_segment.py's GENERIC_ROLE_KEYS/_is_generic_role().
KNOWN_TCP_ROLES = {"template", "container", "project"}
GENERIC_ROLE_KEYS = {"generic", "generic-host", "generic_host"}

# Canonical casing for recognized roles, mirroring build_segment_manifest.py's
# _GOVERNANCE_ROLE_CANONICAL fold (a manual-edit case variant like "container"
# must not silently fragment into a population separate from "Container").
# Generic-Host variants fold all the way to "Generic" — this module already
# treats every generic-role spelling identically everywhere downstream (no
# scope_key, scope_level == "generic", unconditional pairing against every
# Template/Container/Project population), so there is no behavior a distinct
# "Generic-Host" label would preserve.
_GOVERNANCE_ROLE_CANONICAL = {
    "template": "Template",
    "container": "Container",
    "project": "Project",
    "generic": "Generic",
    "generic-host": "Generic",
    "generic_host": "Generic",
}

MANIFEST_FIELDNAMES = [
    "governance_id", "unit_system", "governance_role", "discipline_label",
    "scope_key", "scope_level", "client_label", "business_center_label",
    "file_count", "population_hash", "notes",
]
MEMBERSHIP_FIELDNAMES = ["governance_id", "export_run_id"]
EXCLUDED_FIELDNAMES = ["export_run_id", "governance_role", "client_label", "business_center_label", "reason"]


# ---------------------------------------------------------------------------
# BC-label normalization (Change A)
#
# Real business-center identifiers are bare numeric going forward (e.g.
# "2014"), but legacy rows filled in before that decision may still carry a
# "BC_" prefix (e.g. "BC_2014"). This strips a leading "BC_" (case-
# insensitive) so both spellings collapse into one population instead of
# fragmenting into two. It is intentionally narrow — "strip BC_ prefix if
# present," nothing more; it is not a general-purpose format detector.
#
# A purely-numeric value shorter than 4 digits (e.g. "0" or "796") is
# zero-padded to 4 digits before anything else -- the same fix
# build_segment_manifest.py's _normalize_rows() applies (see CHANGELOG.md),
# for the same reason: opening file_metadata.csv in Excel without importing
# business_center_label as Text collapses "0000" to "0" on save. This
# module reads file_metadata.csv directly and independently of build_
# segment_manifest.py (see this file's own module docstring -- disjoint
# governance populations are deliberately NOT built on the segment lattice),
# so it needs its own copy of the same padding fix rather than inheriting it.
#
# The enterprise-bookkeeping check ("0000"/"BC_0000") is evaluated BEFORE any
# prefix stripping (but AFTER zero-padding, so a collapsed "0" is correctly
# recognized as the "0000" enterprise token, not treated as a real 1-digit
# business center) and reuses the shared ENTERPRISE_BC_BOOKKEEPING_TOKENS set
# from na_token.py (the same set compare_cross_segment.py's _normalize_bc_
# label() and build_segment_manifest.py's normalization already use) rather
# than reimplementing it. It is a separate concept from BC-prefix stripping:
# an enterprise-bookkeeping tag is not a real business center number at all.
# ---------------------------------------------------------------------------

def normalize_business_center_label(raw: str) -> Tuple[str, bool]:
    """Returns (normalized_value, is_enterprise_bookkeeping)."""
    value = (raw or "").strip()
    if value.isdigit() and len(value) < 4:
        value = value.zfill(4)
    if value.lower() in ENTERPRISE_BC_BOOKKEEPING_TOKENS:
        return value, True
    if value[:3].lower() == "bc_":
        return value[3:], False
    return value, False


def _is_enterprise_client(client_label: str, policy: EnterprisePolicy) -> bool:
    # Case-insensitive fold, matching the casing convention already used
    # elsewhere in this pipeline (see build_segment_manifest.py's
    # first-seen-casing fold).
    return policy.is_enterprise(client_label)


def _governance_role_key(role: str) -> str:
    return role.strip().lower()


def _is_generic_role(role: str) -> bool:
    return _governance_role_key(role) in GENERIC_ROLE_KEYS


# ---------------------------------------------------------------------------
# scope_key composition (Change A)
# ---------------------------------------------------------------------------

def compute_scope_key(client_label: str, business_center_label: str, policy: EnterprisePolicy = None) -> Tuple[str, str, str, str]:
    """Returns (scope_key, scope_level, normalized_client_label, normalized_bc).

    A row is Enterprise-scoped only when BOTH client_label == "InternalEnterprise" AND
    business_center_label normalizes to an enterprise-bookkeeping token —
    either condition alone is not sufficient (e.g. a real external client
    can still be enterprise-bookkeeping-tagged; a InternalEnterprise-internal file can
    still carry a real business center).
    """
    if isinstance(policy, str):
        policy = load_enterprise_policy(enterprise_label=policy)
    policy = policy or load_enterprise_policy()
    normalized_bc, is_enterprise_bc = normalize_business_center_label(business_center_label)
    is_internal = _is_enterprise_client(client_label, policy)
    client = client_label.strip()

    if is_internal and is_enterprise_bc:
        return "enterprise", "enterprise", client, ""
    if is_internal and not is_enterprise_bc:
        return f"bc:{normalized_bc}", "bc", client, normalized_bc
    if not is_internal and is_enterprise_bc:
        return f"client:{client}", "client", client, ""
    return f"project:{client}:{normalized_bc}", "project", client, normalized_bc


def _governance_id(unit_system: str, role: str, discipline_label: str, scope_key: str) -> str:
    token = f"{unit_system}|{role}|{discipline_label}|{scope_key}"
    return "gov_" + hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]


def _population_hash(export_run_ids: List[str]) -> str:
    token = "|".join(sorted(export_run_ids))
    return hashlib.sha1(token.encode("utf-8")).hexdigest()


def _normalize_manual_metadata(meta_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Case-fold manually-entered file_metadata.csv fields before they enter
    scope_key/population-key construction, mirroring build_segment_manifest.
    py's _normalize_rows(): the Run A -> B annotation pause is manual, so a
    casing variant like "Imperial" vs "imperial" or "Acme" vs "acme" must not
    silently fragment one population into two governance_ids, and later
    exact-string unit/client/discipline matching in compare_governance_
    populations.py's pair discovery must not miss a valid pair over a casing
    difference alone.

      - unit_system: folds to lowercase, the canonical form used throughout
        the pipeline (same fixed rule build_segment_manifest.py applies).
      - client_label / discipline_label: no fixed enum. Case-insensitive
        fold to the casing of the first occurrence in row order — same rule,
        same tie-break, as build_segment_manifest.py's _normalize_rows().
      - business_center_label: the same first-seen-casing fold, applied
        AFTER normalize_business_center_label()'s BC_-prefix strip (so
        "BC_2014"/"bc_2014"/"2014" already collapse regardless of this fold)
        and skipped entirely for enterprise-bookkeeping tokens (those are
        already recognized case-insensitively by compute_scope_key() and
        never appear in scope_key verbatim, so folding their casing here
        would be a no-op at best).

    governance_role is intentionally not handled here — it already folds to
    a small fixed canonical set (_GOVERNANCE_ROLE_CANONICAL), not a first-
    seen casing, since it is a closed enum, not free text.
    """
    first_seen: Dict[str, Dict[str, str]] = defaultdict(dict)
    normalized_rows: List[Dict[str, str]] = []
    for row in meta_rows:
        new_row = dict(row)
        new_row["unit_system"] = row.get("unit_system", "").strip().lower()

        for field in ("client_label", "discipline_label"):
            raw = row.get(field, "").strip()
            if raw:
                new_row[field] = first_seen[field].setdefault(raw.lower(), raw)

        raw_bc = row.get("business_center_label", "").strip()
        stripped_bc, is_enterprise_bc = normalize_business_center_label(raw_bc)
        if stripped_bc and not is_enterprise_bc:
            stripped_bc = first_seen["business_center_label"].setdefault(stripped_bc.lower(), stripped_bc)
        new_row["business_center_label"] = raw_bc if is_enterprise_bc else stripped_bc

        normalized_rows.append(new_row)
    return normalized_rows


# ---------------------------------------------------------------------------
# Population builder (Changes A, B, D)
# ---------------------------------------------------------------------------

def build_governance_populations(
    meta_rows: List[Dict[str, str]],
    policy: EnterprisePolicy = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    """Group file_metadata.csv rows into disjoint governance populations.

    Returns (manifest_rows, membership_rows, excluded_rows).

    Two distinct failure modes:
      - blank/N-A client_label or business_center_label: this should never
        happen post-gate, so it raises (SystemExit) rather than excluding —
        defense in depth via the same check run_extract_all.py's Run B
        stage already runs, not a reimplementation of it.
      - a governance_role that is not Template/Container/Project/Generic/
        Generic-Host (case-insensitive) cannot be scoped by this module's
        rules at all. That is a more local, plausible data-entry issue (a
        typo in one row), not a systemic precondition failure, so those
        rows are excluded from the populations they can't resolve into, and
        reported loudly with a count and their export_run_ids — never
        silently dropped or guessed into a bucket.
    """
    _check_governance_field_completeness(meta_rows)
    meta_rows = _normalize_manual_metadata(meta_rows)

    groups: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    excluded_rows: List[Dict[str, str]] = []

    for row in meta_rows:
        export_run_id = row.get("export_run_id", "").strip() or "<missing export_run_id>"
        role = row.get("governance_role", "").strip()
        unit_system = row.get("unit_system", "").strip()
        discipline_label = row.get("discipline_label", "").strip()
        client_label = row.get("client_label", "").strip()
        business_center_label = row.get("business_center_label", "").strip()
        role_key = _governance_role_key(role)

        if _is_generic_role(role):
            scope_key, norm_client, norm_bc = "", "", ""
        elif role_key in KNOWN_TCP_ROLES:
            scope_key, _scope_level, norm_client, norm_bc = compute_scope_key(
                client_label, business_center_label, policy or load_enterprise_policy()
            )
        else:
            excluded_rows.append({
                "export_run_id": export_run_id,
                "governance_role": role,
                "client_label": client_label,
                "business_center_label": business_center_label,
                "reason": f"unrecognized governance_role {role!r}",
            })
            continue

        # Canonicalize casing (e.g. "container" -> "Container") before it enters
        # the population key or the manifest output — otherwise a manual-edit
        # case variant fragments into a separate governance_id, and downstream
        # pair discovery's exact-string role checks ("Template"/"Container"/
        # "Project") silently drop the lower-case population entirely.
        canonical_role = _GOVERNANCE_ROLE_CANONICAL[role_key]
        dim_key = (unit_system, canonical_role, discipline_label, scope_key)
        bucket = groups.setdefault(dim_key, {
            "export_run_ids": [],
            "client_label": norm_client,
            "business_center_label": norm_bc,
        })
        bucket["export_run_ids"].append(export_run_id)

    manifest_rows: List[Dict[str, str]] = []
    membership_rows: List[Dict[str, str]] = []

    for (unit_system, role, discipline_label, scope_key) in sorted(groups.keys()):
        bucket = groups[(unit_system, role, discipline_label, scope_key)]
        export_run_ids = sorted(bucket["export_run_ids"])
        governance_id = _governance_id(unit_system, role, discipline_label, scope_key)
        scope_level = "generic" if scope_key == "" else scope_key.split(":", 1)[0]
        manifest_rows.append({
            "governance_id": governance_id,
            "unit_system": unit_system,
            "governance_role": role,
            "discipline_label": discipline_label,
            "scope_key": scope_key,
            "scope_level": scope_level,
            "client_label": bucket["client_label"],
            "business_center_label": bucket["business_center_label"],
            "file_count": str(len(export_run_ids)),
            "population_hash": _population_hash(export_run_ids),
            "notes": "",
        })
        for eid in export_run_ids:
            membership_rows.append({"governance_id": governance_id, "export_run_id": eid})

    if excluded_rows:
        sys.stderr.write(
            f"[WARN governance_manifest] {len(excluded_rows)} row(s) excluded — "
            "governance_role not recognized as Template/Container/Project/"
            "Generic/Generic-Host:\n"
        )
        for r in sorted(excluded_rows, key=lambda r: r["export_run_id"]):
            sys.stderr.write(
                f"  {r['export_run_id']}: governance_role={r['governance_role']!r}\n"
            )

    return manifest_rows, membership_rows, excluded_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--records-dir", required=True, help="Directory containing file_metadata.csv")
    ap.add_argument("--out-dir", default=None, help="Output directory (default: same as --records-dir)")
    ap.add_argument("--enterprise-policy", help="Deployment-local enterprise policy JSON")
    ap.add_argument("--enterprise-label", default=None, help="Enterprise label override (takes precedence over policy file)")
    args = ap.parse_args()

    records_dir = Path(args.records_dir).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else records_dir

    meta_path = records_dir / "file_metadata.csv"
    if not meta_path.is_file():
        raise SystemExit(f"file_metadata.csv not found: {meta_path}")

    meta_rows = read_csv_rows(meta_path)
    policy = load_enterprise_policy(args.enterprise_policy, args.enterprise_label)
    manifest_rows, membership_rows, excluded_rows = build_governance_populations(meta_rows, policy)
    write_enterprise_policy_provenance(out_dir, policy)

    atomic_write_csv(out_dir / "governance_manifest.csv", MANIFEST_FIELDNAMES, manifest_rows)
    atomic_write_csv(out_dir / "governance_membership.csv", MEMBERSHIP_FIELDNAMES, membership_rows)
    if excluded_rows:
        atomic_write_csv(out_dir / "governance_manifest_excluded.csv", EXCLUDED_FIELDNAMES, excluded_rows)

    print(
        f"[governance_manifest] {len(manifest_rows)} population(s), "
        f"{len(membership_rows)} file(s), {len(excluded_rows)} excluded row(s)",
        flush=True,
    )
    for row in manifest_rows:
        print(
            f"  {row['governance_id']} unit={row['unit_system']} role={row['governance_role']} "
            f"discipline={row['discipline_label']} scope={row['scope_key'] or '<generic>'} "
            f"file_count={row['file_count']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
