#!/usr/bin/env python3
"""Reformat nested fingerprint record buckets into canonical flat item lists."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from core.canonical_items import build_flat_items


def parse_domains(domains_arg: str | None) -> set[str] | None:
    if not domains_arg:
        return None
    parsed = {part.strip() for part in domains_arg.split(",") if part.strip()}
    return parsed if parsed else None


def transform_record(record: dict[str, Any], domain: str) -> tuple[dict[str, Any], int, Counter[str], int, Counter[str]]:
    out = dict(record)
    role_counts: Counter[str] = Counter()
    unknown_key_counts: Counter[str] = Counter()

    identity_basis = record.get("identity_basis")
    identity_items = identity_basis.get("items") if isinstance(identity_basis, dict) else []
    phase2_dict = record.get("phase2") if isinstance(record.get("phase2"), dict) else {}
    existing_items = record.get("items") if isinstance(record.get("items"), list) else []

    flat_items = build_flat_items(
        existing_items,
        identity_items if isinstance(identity_items, list) else [],
        phase2_dict.get("semantic_items", []) if isinstance(phase2_dict.get("semantic_items"), list) else [],
        phase2_dict.get("lineage_items", []) if isinstance(phase2_dict.get("lineage_items"), list) else [],
        phase2_dict.get("cosmetic_items", []) if isinstance(phase2_dict.get("cosmetic_items"), list) else [],
        phase2_dict.get("coordination_items", []) if isinstance(phase2_dict.get("coordination_items"), list) else [],
        phase2_dict.get("unknown_items", []) if isinstance(phase2_dict.get("unknown_items"), list) else [],
    )
    for item in flat_items:
        if isinstance(item, dict):
            item.pop("role", None)

    out["items"] = flat_items
    for key in ("identity_basis", "phase2", "join_key", "sig_hash", "sig_basis"):
        out.pop(key, None)

    return out, len(flat_items), role_counts, 0, unknown_key_counts


def process_payload(payload: Any, allowed_domains: set[str] | None, source_path: Path):
    if not isinstance(payload, dict):
        return payload, 0, 0, Counter(), 0, {}, {}

    out_payload = dict(payload)
    records_total = 0
    items_total = 0
    role_counts: Counter[str] = Counter()
    domain_stats: dict[str, dict[str, int]] = {}
    domain_unknown_keys: dict[str, Counter[str]] = {}

    for domain_name, domain_payload in payload.items():
        if not isinstance(domain_payload, dict):
            continue
        if allowed_domains is not None and domain_name not in allowed_domains:
            continue
        records = domain_payload.get("records")
        if not isinstance(records, list):
            continue

        transformed_records = []
        domain_records = 0
        domain_items = 0
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                sys.stderr.write(f"WARN: {source_path} domain={domain_name} record_index={idx} is not an object; skipping\n")
                continue
            transformed, item_count, per_record_roles, overrides, record_unknown_keys = transform_record(record, domain_name)
            transformed_records.append(transformed)
            records_total += 1
            domain_records += 1
            items_total += item_count
            domain_items += item_count
            role_counts.update(per_record_roles)
            domain_unknown_keys.setdefault(domain_name, Counter()).update(record_unknown_keys)

        new_domain = dict(domain_payload)
        new_domain["records"] = transformed_records
        out_payload[domain_name] = new_domain
        domain_stats[domain_name] = {"records": domain_records, "items": domain_items, "identity": 0, "cosmetic": 0, "coordination": 0, "unknown": 0}

    return out_payload, records_total, items_total, role_counts, 0, domain_stats, domain_unknown_keys


def iter_input_files(input_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(input_dir.glob("*.json")):
        if path.name.endswith(".legacy.json") or path.name.endswith(".flat.json"):
            continue
        files.append(path)
    return files


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--output_dir", required=True)
    ap.add_argument("--domains", help="Comma-separated domains to transform; others are left unchanged")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args(argv)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        sys.stderr.write(f"ERROR: input_dir is not a directory: {input_dir}\n")
        return 2

    allowed_domains = parse_domains(args.domains)
    files = iter_input_files(input_dir)
    if not files:
        print("No input files found.")
        return 0

    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    for src in files:
        try:
            with src.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            sys.stderr.write(f"[WARN reformat] skipping {src.name}: {exc}\n")
            continue
        transformed, *_ = process_payload(payload, allowed_domains, src)
        if not args.dry_run:
            # Use a distinct stem to avoid overwriting sources when output_dir == input_dir.
            if output_dir.resolve() == src.parent.resolve():
                out_name = src.stem + ".canonical" + src.suffix
            else:
                out_name = src.name
            out_path = output_dir / out_name
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(transformed, f, indent=2, sort_keys=True)
                f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
