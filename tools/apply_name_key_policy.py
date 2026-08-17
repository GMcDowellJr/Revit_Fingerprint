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
    for key, payload in export_data.items():
        if not isinstance(key, str) or key.startswith("_"):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("records"), list):
            yield key, payload


def _rows_for_export(export_path: Path, name_key_policies: Dict[str, Any]) -> List[Dict[str, str]]:
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
