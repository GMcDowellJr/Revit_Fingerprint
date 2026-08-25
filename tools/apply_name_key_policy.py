#!/usr/bin/env python3
"""Analysis-side reconstruction of the Canonical Name Identity Projection (PR1).

Computes join_key_name_identity for every eligible-domain record directly from an
already-exported *.details.json file -- no re-extraction through domains/*.py +
runner/run_dynamo.py required, since every value this projection needs (identity_basis.items,
phase2 bucket items, label.display) is already present in existing exports. Mirrors
core/sig_hash_builder.py's role for sig_hash (a policy-driven, read-only reconstruction path
alongside the inline extractor computation), via core/name_key_builder.py.

Input format priority: *.details.json preferred; *.index.json is
summary-only (no identity_basis/phase2, degraded semantics -- records without them are
skipped, not silently treated as complete); never *.legacy.json.

Usage:
    python tools/apply_name_key_policy.py --export path/to/model.details.json
    python tools/apply_name_key_policy.py --export-dir path/to/exports/
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

try:
    from core.name_key_builder import build_name_key_for_record
    from core.join_key_policy import load_join_key_policies
except ModuleNotFoundError:  # pragma: no cover - path bootstrap for direct script execution
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from core.name_key_builder import build_name_key_for_record
    from core.join_key_policy import load_join_key_policies


_DEFAULT_POLICY_PATH = Path(__file__).resolve().parent.parent / "policies" / "domain_name_key_policies.json"
_OUTPUT_FIELDS = [
    "export_file",
    "domain",
    "record_id",
    "label_display",
    "join_key_schema",
    "join_hash",
    "status",
    "missing_required",
]


def _iter_export_paths(export: str, export_dir: str) -> List[Path]:
    """Resolve the CLI's --export/--export-dir args to a concrete, ordered list of export
    files to process, applying the input-format-priority rule.

    --- trace ---
    reads: `export` (CLI --export, a single file path) and `export_dir` (CLI --export-dir,
        a directory), both from main()'s argparse Namespace.
    calls: none (pathlib.Path.glob only).
    thresholds: the details > index > fallback(excluding *.legacy.json) priority order is
        hardcoded control flow here, not a named constant or policy file (CLAUDE.md's
        input-format-priority rule, restated as code) -- a table-driven policy scanner would
        miss this as a "threshold" since it is not assigned to a name.
    returns: sorted list[Path], one tier only (never a mix of details/index/fallback);
        consumed by main(), which passes each Path to _rows_for_export().
    """
    if export:
        return [Path(export)]
    root = Path(export_dir)
    if not root.is_dir():
        raise FileNotFoundError(f"Not a directory: {root}")
    # *.details.json preferred; fall back to *.index.json only when no details files exist.
    # Never *.legacy.json (CLAUDE.md: tools that glob *.json without filtering are unsafe
    # under split exports).
    details = sorted(p for p in root.glob("*.details.json") if p.is_file())
    if details:
        return details
    index = sorted(p for p in root.glob("*.index.json") if p.is_file())
    if index:
        return index
    fallback = sorted(
        p for p in root.glob("*.json")
        if p.is_file() and not p.name.lower().endswith(".legacy.json")
    )
    if not fallback:
        raise FileNotFoundError(f"No export JSON found under {root} (excluding *.legacy.json)")
    return fallback


def _iter_domain_payloads(export_data: Dict[str, Any]):
    """Yield (top_key, payload) pairs for each top-level domain block that carries a
    `records` list, skipping metadata keys.

    --- trace ---
    reads: `export_data` -- the parsed JSON dict of one *.details.json/*.index.json file,
        passed in by _rows_for_export() (which just json.load()'d it).
    calls: none.
    thresholds: the filter itself (key not starting with "_", value is dict with a list
        "records") is inline structural logic, not a named constant -- there is no schema
        registry this reads against.
    returns: generator of (str, dict) pairs; consumed only by _rows_for_export()'s loop.
    """
    for key, payload in export_data.items():
        if not isinstance(key, str) or key.startswith("_"):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("records"), list):
            yield key, payload


def _rows_for_export(export_path: Path, name_key_policies: Dict[str, Any]) -> List[Dict[str, str]]:
    """Load one export JSON file and reconstruct a join_key_name_identity output row for
    every eligible-domain record it contains.

    --- trace ---
    reads: JSON file at `export_path` (one Path yielded by _iter_export_paths(), opened
        here); `name_key_policies` (main()'s loaded policies/domain_name_key_policies.json,
        passed through unchanged).
    calls: _iter_domain_payloads(); core.name_key_builder.build_name_key_for_record()
        (does the actual per-record reconstruction -- returns None for a domain with no
        policy entry, which this function treats as "skip, out of scope").
    thresholds: output row shape is the module-level _OUTPUT_FIELDS constant (l.40-49) --
        not read here directly, but this function's dict literal must stay in sync with it
        by convention (no assertion ties them together).
    returns: list[dict[str,str]], one row per reconstructed record; consumed by main(),
        which extends `all_rows` across every export file and writes them to the output CSV.
    """
    with export_path.open("r", encoding="utf-8") as f:
        export_data = json.load(f)
    if not isinstance(export_data, dict):
        return []

    rows: List[Dict[str, str]] = []
    for top_key, payload in _iter_domain_payloads(export_data):
        for record in payload.get("records", []):
            if not isinstance(record, dict):
                continue
            domain_name = record.get("domain") if isinstance(record.get("domain"), str) else top_key
            name_key = build_name_key_for_record(record, domain_name, name_key_policies)
            if name_key is None:
                continue  # domain has no entry in the name-key policy -- out of scope
            label = record.get("label") if isinstance(record.get("label"), dict) else {}
            rows.append({
                "export_file": export_path.name,
                "domain": domain_name,
                "record_id": str(record.get("record_id", "")),
                "label_display": str(label.get("display", "")),
                "join_key_schema": str(name_key.get("schema", "")),
                "join_hash": str(name_key.get("join_hash") or ""),
                "status": str(name_key.get("status", "")),
                "missing_required": "|".join(name_key.get("missing_required") or []),
            })
    return rows


def main() -> None:
    """CLI entry point: resolve export inputs, load the name-key policy, reconstruct rows
    for every export, and write the combined CSV.

    --- trace ---
    reads: CLI args --export, --export-dir, --name-key-policy (default
        `_DEFAULT_POLICY_PATH`, l.39 -> policies/domain_name_key_policies.json),
        --out (default "Results_v21/name_key/name_key_results.csv").
    calls: core.join_key_policy.load_join_key_policies(); _iter_export_paths();
        _rows_for_export() (once per resolved export path).
    thresholds: `_DEFAULT_POLICY_PATH` (module constant, l.39, sourced from the repo-root
        policies/ directory); `_OUTPUT_FIELDS` (module constant, l.40-49, fixes CSV column
        order); default --out path literal.
    returns: writes CSV to `args.out`; consumed downstream by
        tools/generate_name_key_patterns.py's --name-key-csv arg and by
        tools/run_segment_orchestrator.py's Step 2b per-segment filter
        (_filter_name_key_csv_to_segment(), run_segment_orchestrator.py l.1139+). Prints a
        row-count summary to stdout; returns None.
    """
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--export", default=None, help="Single *.details.json export file.")
    ap.add_argument("--export-dir", default=None, help="Directory of export JSON files (non-recursive).")
    ap.add_argument("--name-key-policy", default=str(_DEFAULT_POLICY_PATH), help="Path to domain_name_key_policies.json.")
    ap.add_argument("--out", default="Results_v21/name_key/name_key_results.csv", help="Output CSV path.")
    args = ap.parse_args()

    if not args.export and not args.export_dir:
        raise SystemExit("Provide --export or --export-dir")

    name_key_policies = load_join_key_policies(args.name_key_policy)

    all_rows: List[Dict[str, str]] = []
    for export_path in _iter_export_paths(args.export, args.export_dir):
        all_rows.extend(_rows_for_export(export_path, name_key_policies))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_OUTPUT_FIELDS)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"[apply_name_key_policy] wrote {len(all_rows)} rows -> {out_path}")


if __name__ == "__main__":
    main()
