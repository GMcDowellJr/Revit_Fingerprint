#!/usr/bin/env python3
"""Reformat nested fingerprint record buckets into flat item lists."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

PHASE2_BUCKETS = (
    ("semantic_items", "identity"),
    ("cosmetic_items", "cosmetic"),
    ("coordination_items", "coordination"),
    ("unknown_items", "unknown"),
)

VALID_ROLES = {"identity", "cosmetic", "coordination", "unknown"}


def parse_domains(domains_arg: str | None) -> set[str] | None:
    if not domains_arg:
        return None
    parsed = {part.strip() for part in domains_arg.split(",") if part.strip()}
    return parsed if parsed else None


def load_role_policy(path: str | None) -> dict[str, dict[str, str]] | None:
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("role_policy must be a JSON object keyed by domain")
    policy: dict[str, dict[str, str]] = {}
    for domain, mapping in raw.items():
        if not isinstance(domain, str) or not isinstance(mapping, dict):
            continue
        typed_mapping: dict[str, str] = {}
        for k, role in mapping.items():
            if isinstance(k, str) and isinstance(role, str) and role in VALID_ROLES:
                typed_mapping[k] = role
        policy[domain] = typed_mapping
    return policy


def transform_record(record: dict[str, Any], domain: str, role_policy: dict[str, dict[str, str]] | None) -> tuple[dict[str, Any], int, Counter[str], int]:
    out = dict(record)
    flat_items: list[dict[str, Any]] = []
    seen_keys: set[Any] = set()
    role_counts: Counter[str] = Counter()
    policy_overrides = 0

    identity_basis = record.get("identity_basis")
    identity_items: list[Any] = []
    missing_identity_basis = False
    if isinstance(identity_basis, dict) and isinstance(identity_basis.get("items"), list):
        identity_items = identity_basis["items"]
    else:
        missing_identity_basis = True

    def emit(items: list[Any], role: str) -> None:
        for item in items:
            if not isinstance(item, dict):
                continue
            key = item.get("k")
            if not isinstance(key, str):
                continue
            try:
                hash(key)
            except TypeError:
                continue
            if key in seen_keys:
                continue
            seen_keys.add(key)
            flat_items.append({"k": key, "v": item.get("v"), "q": item.get("q"), "role": role})
            role_counts[role] += 1

    emit(identity_items, "identity")

    phase2 = record.get("phase2")
    phase2_dict = phase2 if isinstance(phase2, dict) else {}
    for bucket_name, role in PHASE2_BUCKETS:
        bucket_items = phase2_dict.get(bucket_name)
        emit(bucket_items if isinstance(bucket_items, list) else [], role)

    if role_policy and domain in role_policy:
        policy_for_domain = role_policy[domain]
        for item in flat_items:
            override_role = policy_for_domain.get(item.get("k"))
            if override_role and override_role != item["role"]:
                role_counts[item["role"]] -= 1
                item["role"] = override_role
                role_counts[override_role] += 1
                policy_overrides += 1

    out["items"] = flat_items
    out.pop("identity_basis", None)
    out.pop("phase2", None)
    if missing_identity_basis:
        out["flat_items_warning"] = "missing_identity_basis"

    return out, len(flat_items), role_counts, policy_overrides


def process_payload(
    payload: Any,
    allowed_domains: set[str] | None,
    role_policy: dict[str, dict[str, str]] | None,
    source_path: Path,
) -> tuple[Any, int, int, Counter[str], int]:
    if not isinstance(payload, dict):
        return payload, 0, 0, Counter(), 0

    out_payload = dict(payload)
    records_total = 0
    items_total = 0
    role_counts: Counter[str] = Counter()
    policy_overrides_total = 0

    for domain_name, domain_payload in payload.items():
        if not isinstance(domain_payload, dict):
            continue
        if allowed_domains is not None and domain_name not in allowed_domains:
            continue
        records = domain_payload.get("records")
        if not isinstance(records, list):
            continue

        transformed_records: list[Any] = []
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                sys.stderr.write(
                    "WARN: {} domain={} record_index={} is not an object; skipping\n".format(source_path, domain_name, idx)
                )
                continue
            transformed, item_count, per_record_roles, overrides = transform_record(record, domain_name, role_policy)
            transformed_records.append(transformed)
            records_total += 1
            items_total += item_count
            role_counts.update(per_record_roles)
            policy_overrides_total += overrides

        new_domain = dict(domain_payload)
        new_domain["records"] = transformed_records
        out_payload[domain_name] = new_domain

    return out_payload, records_total, items_total, role_counts, policy_overrides_total


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
    ap.add_argument("--role_policy")
    ap.add_argument("--domains", help="Comma-separated domains to transform; others are left unchanged")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args(argv)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        sys.stderr.write("ERROR: input_dir is not a directory: {}\n".format(input_dir))
        return 2

    try:
        role_policy = load_role_policy(args.role_policy)
    except Exception as exc:
        sys.stderr.write("ERROR: failed to load role policy: {}\n".format(exc))
        return 2

    allowed_domains = parse_domains(args.domains)

    files = iter_input_files(input_dir)

    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    total_files = 0
    total_records = 0
    total_items = 0

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            sys.stderr.write("WARN: failed to parse JSON {}: {}\n".format(path, exc))
            continue

        transformed, record_count, item_count, role_counts, policy_overrides = process_payload(
            payload,
            allowed_domains,
            role_policy,
            path,
        )

        total_files += 1
        total_records += record_count
        total_items += item_count

        output_name = "{}.flat.json".format(path.stem)
        if args.dry_run:
            role_summary = ",".join(
                "{}={}".format(role, role_counts[role]) for role in ("identity", "cosmetic", "coordination", "unknown") if role_counts[role]
            )
            print(
                "{} records={} items={} roles=[{}]{}".format(
                    path.name,
                    record_count,
                    item_count,
                    role_summary,
                    " policy_overrides={}".format(policy_overrides) if role_policy else "",
                )
            )
            continue

        out_path = output_dir / output_name
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, separators=(",", ":"), ensure_ascii=False)
            f.write("\n")

        line = "{}  domains={}  records={}  items={}".format(output_name, len([k for k, v in transformed.items() if isinstance(v, dict)]), record_count, item_count)
        if role_policy:
            line += "  policy_overrides={}".format(policy_overrides)
        print(line)

    print("total files={} total records={} total items={}".format(total_files, total_records, total_items))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
