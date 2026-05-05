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


def transform_record(record: dict[str, Any], domain: str, role_policy: dict[str, dict[str, str]] | None) -> tuple[dict[str, Any], int, Counter[str], int, Counter[str]]:
    out = dict(record)
    flat_items: list[dict[str, Any]] = []
    seen_keys: set[Any] = set()
    role_counts: Counter[str] = Counter()
    unknown_key_counts: Counter[str] = Counter()
    policy_overrides = 0

    identity_basis = record.get("identity_basis")
    identity_items: list[Any] = []
    missing_identity_basis = False
    if isinstance(identity_basis, dict) and isinstance(identity_basis.get("items"), list):
        identity_items = identity_basis["items"]
    else:
        missing_identity_basis = True

    existing_items = record.get("items")
    preserved_existing_items = False
    if missing_identity_basis and isinstance(existing_items, list):
        for item in existing_items:
            if not isinstance(item, dict):
                continue
            key = item.get("k")
            if not isinstance(key, str):
                continue
            if key in seen_keys:
                continue
            role = item.get("role")
            if role not in VALID_ROLES:
                role = "unknown"
            seen_keys.add(key)
            flat_items.append({"k": key, "v": item.get("v"), "q": item.get("q"), "role": role})
            role_counts[role] += 1
        if flat_items:
            preserved_existing_items = True

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

    if not preserved_existing_items:
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

    for item in flat_items:
        if item["role"] == "unknown":
            unknown_key_counts[item["k"]] += 1

    out["items"] = flat_items
    out.pop("identity_basis", None)
    out.pop("phase2", None)
    if missing_identity_basis and not preserved_existing_items:
        out["flat_items_warning"] = "missing_identity_basis"

    return out, len(flat_items), role_counts, policy_overrides, unknown_key_counts


def process_payload(
    payload: Any,
    allowed_domains: set[str] | None,
    role_policy: dict[str, dict[str, str]] | None,
    source_path: Path,
) -> tuple[Any, int, int, Counter[str], int, dict[str, dict[str, int]], dict[str, Counter[str]]]:
    if not isinstance(payload, dict):
        return payload, 0, 0, Counter(), 0, {}, {}

    out_payload = dict(payload)
    records_total = 0
    items_total = 0
    role_counts: Counter[str] = Counter()
    policy_overrides_total = 0
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

        transformed_records: list[Any] = []
        domain_role_counts: Counter[str] = Counter()
        domain_records = 0
        domain_items = 0
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                sys.stderr.write(
                    "WARN: {} domain={} record_index={} is not an object; skipping\n".format(source_path, domain_name, idx)
                )
                continue
            transformed, item_count, per_record_roles, overrides, record_unknown_keys = transform_record(record, domain_name, role_policy)
            transformed_records.append(transformed)
            records_total += 1
            domain_records += 1
            items_total += item_count
            domain_items += item_count
            role_counts.update(per_record_roles)
            domain_role_counts.update(per_record_roles)
            domain_unknown_keys.setdefault(domain_name, Counter()).update(record_unknown_keys)
            policy_overrides_total += overrides

        new_domain = dict(domain_payload)
        new_domain["records"] = transformed_records
        out_payload[domain_name] = new_domain
        domain_stats[domain_name] = {
            "records": domain_records,
            "items": domain_items,
            "identity": domain_role_counts["identity"],
            "cosmetic": domain_role_counts["cosmetic"],
            "coordination": domain_role_counts["coordination"],
            "unknown": domain_role_counts["unknown"],
        }

    return out_payload, records_total, items_total, role_counts, policy_overrides_total, domain_stats, domain_unknown_keys


def print_domain_breakdown(domain_stats: dict[str, dict[str, int]]) -> None:
    print("  {domain:>38} {records:>10} {identity:>10} {cosmetic:>10} {coordination:>13} {unknown:>8}".format(
        domain="domain", records="records", identity="identity", cosmetic="cosmetic", coordination="coordination", unknown="unknown"
    ))
    print("  " + "-" * 92)
    rows = sorted(domain_stats.items(), key=lambda kv: kv[1].get("items", 0), reverse=True)
    for domain, stats in rows:
        print(
            "  {domain:>38} {records:>10} {identity:>10} {cosmetic:>10} {coordination:>13} {unknown:>8}".format(
                domain=domain,
                records=stats.get("records", 0),
                identity=stats.get("identity", 0),
                cosmetic=stats.get("cosmetic", 0),
                coordination=stats.get("coordination", 0),
                unknown=stats.get("unknown", 0),
            )
        )


def print_unknown_keys_by_domain(domain_unknown_keys: dict[str, Counter[str]]) -> None:
    print("  UNKNOWN KEYS BY DOMAIN:")
    rows = sorted(
        ((domain, counts) for domain, counts in domain_unknown_keys.items() if sum(counts.values()) > 0),
        key=lambda kv: sum(kv[1].values()),
        reverse=True,
    )
    for domain, counts in rows:
        print("  {}".format(domain))
        for key, count in sorted(counts.items(), key=lambda kv: kv[1], reverse=True):
            print("    {key:<38} {count:>10}".format(key=key, count=count))


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
    ap.add_argument("--breakdown_by_domain", action="store_true")
    ap.add_argument("--list_unknown_keys", action="store_true")
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

    if args.breakdown_by_domain and not args.dry_run:
        sys.stderr.write("WARN: --breakdown_by_domain is only supported with --dry_run; ignoring\n")
    if args.list_unknown_keys and not args.dry_run:
        sys.stderr.write("WARN: --list_unknown_keys is only supported with --dry_run; ignoring\n")
    if args.list_unknown_keys and args.dry_run:
        args.breakdown_by_domain = True

    files = iter_input_files(input_dir)

    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    total_files = 0
    total_records = 0
    total_items = 0
    corpus_domain_stats: dict[str, dict[str, int]] = {}
    corpus_unknown_keys: dict[str, Counter[str]] = {}

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            sys.stderr.write("WARN: failed to parse JSON {}: {}\n".format(path, exc))
            continue

        transformed, record_count, item_count, role_counts, policy_overrides, domain_stats, domain_unknown_keys = process_payload(
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
            print("{}  domains={}  records={}  items={}{}".format(
                output_name,
                len(domain_stats),
                record_count,
                item_count,
                "  policy_overrides={}".format(policy_overrides) if role_policy else "",
            ))
            if args.breakdown_by_domain:
                print_domain_breakdown(domain_stats)
                for domain_name, stats in domain_stats.items():
                    if domain_name not in corpus_domain_stats:
                        corpus_domain_stats[domain_name] = {"records": 0, "items": 0, "identity": 0, "cosmetic": 0, "coordination": 0, "unknown": 0}
                    for k, v in stats.items():
                        corpus_domain_stats[domain_name][k] += v
                if args.list_unknown_keys:
                    print_unknown_keys_by_domain(domain_unknown_keys)
                    for domain_name, counts in domain_unknown_keys.items():
                        corpus_unknown_keys.setdefault(domain_name, Counter()).update(counts)
            continue

        out_path = output_dir / output_name
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, separators=(",", ":"), ensure_ascii=False)
            f.write("\n")

        if isinstance(transformed, dict):
            domain_count = len([k for k, v in transformed.items() if isinstance(v, dict)])
        else:
            domain_count = 0
            sys.stderr.write("WARN: {} has non-object top-level payload; wrote unchanged output\n".format(path))
        line = "{}  domains={}  records={}  items={}".format(output_name, domain_count, record_count, item_count)
        if role_policy:
            line += "  policy_overrides={}".format(policy_overrides)
        print(line)

    print("total files={} total records={} total items={}".format(total_files, total_records, total_items))
    if args.dry_run and args.breakdown_by_domain:
        print("CORPUS TOTALS  files={}  domains={}  records={}  items={}".format(
            total_files,
            len(corpus_domain_stats),
            total_records,
            total_items,
        ))
        print_domain_breakdown(corpus_domain_stats)
        if args.list_unknown_keys:
            print("CORPUS UNKNOWN KEYS BY DOMAIN:")
            print_unknown_keys_by_domain(corpus_unknown_keys)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
