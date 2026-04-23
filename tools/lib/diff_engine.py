import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from tools.lib.domain_profile import DomainProfile

SUMMARY_HEADERS = [
    "template_name",
    "partition_a",
    "partition_b",
    "match_status",
    "sig_hash_a",
    "sig_hash_b",
    "sig_match",
    "total_items_a",
    "total_items_b",
    "items_only_in_a",
    "items_only_in_b",
    "items_changed",
    "items_same",
    "semantic_diffs",
    "status_a",
    "status_b",
    "label_quality",
]

DETAIL_HEADERS = [
    "template_name",
    "partition_a",
    "partition_b",
    "domain",
    "item_key",
    "bucket",
    "diff_status",
    "value_a",
    "value_b",
    "q_a",
    "q_b",
]

UNMATCHED_HEADERS = [
    "template_name",
    "partition",
    "source_file",
    "sig_hash",
    "record_id",
    "status",
    "item_count",
]

MATCH_SORT_ORDER = {
    "matched": 0,
    "partition_mismatch": 1,
    "only_in_a": 2,
    "only_in_b": 3,
    "duplicate_name_in_file": 4,
}

BUCKET_SORT_ORDER = {
    "semantic": 0,
    "cosmetic": 1,
    "coordination": 2,
    "unknown": 3,
    "other": 4,
}


def normalize_name(name):
    return (name or "").strip().casefold()


def ensure_str(value):
    if value is None:
        return ""
    return str(value)


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_domain_payload(raw, domain):
    if not isinstance(raw, dict):
        return None
    if "_domains" in raw:
        return raw.get(domain)
    domains_obj = raw.get("domains")
    if isinstance(domains_obj, dict):
        return domains_obj.get(domain)
    return raw.get(domain)


def get_label_and_quality(record):
    label = record.get("label")
    display = ""
    quality = ""
    if isinstance(label, dict):
        display = ensure_str(label.get("display")).strip()
        quality = ensure_str(label.get("quality"))

    if display:
        return display, quality
    fallback = ensure_str(record.get("record_id"))
    return fallback, "missing_label"


def get_items(record, suppress_keys):
    identity_basis = record.get("identity_basis")
    if not isinstance(identity_basis, dict):
        return []
    items = identity_basis.get("items")
    if not isinstance(items, list):
        return []
    cleaned = []
    for item in items:
        if not isinstance(item, dict):
            continue
        key = ensure_str(item.get("k"))
        if not key:
            continue
        if key in suppress_keys:
            continue
        cleaned.append({"k": key, "q": ensure_str(item.get("q")), "v": ensure_str(item.get("v"))})
    return cleaned


def extract_records(raw, file_key, domains):
    records = []
    included_domains = []
    skipped_domains = []

    for domain in domains:
        payload = get_domain_payload(raw, domain)
        if not isinstance(payload, dict):
            skipped_domains.append(domain)
            continue

        domain_records = payload.get("records")
        if not isinstance(domain_records, list):
            domain_records = []

        included_domains.append(domain)
        for rec in domain_records:
            if not isinstance(rec, dict):
                continue
            rec_domain = ensure_str(rec.get("domain")) or domain
            records.append({"file_key": file_key, "domain": rec_domain, "record": rec})

    return records, included_domains, skipped_domains


def build_index(records, suppress_keys):
    grouped = defaultdict(list)
    for wrapped in records:
        record = wrapped["record"]
        display_name, label_quality = get_label_and_quality(record)
        norm_name = normalize_name(display_name)
        grouped[norm_name].append(
            {
                "norm_name": norm_name,
                "display_name": display_name,
                "label_quality": label_quality,
                "domain": ensure_str(record.get("domain")) or wrapped["domain"],
                "record": record,
                "sig_hash": ensure_str(record.get("sig_hash")),
                "record_id": ensure_str(record.get("record_id")),
                "status": ensure_str(record.get("status")),
                "items": get_items(record, suppress_keys),
            }
        )

    unique = {}
    duplicates = {}
    for norm_name, entries in grouped.items():
        if len(entries) == 1:
            unique[norm_name] = entries[0]
        else:
            duplicates[norm_name] = entries
    return unique, duplicates


def parse_name_map(path):
    if not path:
        return {}
    data = load_json(path)
    if not isinstance(data, dict):
        raise ValueError("name_map must be a JSON object mapping names from file_a to file_b")

    normalized = {}
    for key, value in data.items():
        nk = normalize_name(ensure_str(key))
        nv = normalize_name(ensure_str(value))
        if nk and nv:
            normalized[nk] = nv
    return normalized


def index_items_by_key(items):
    by_key = {}
    for item in items:
        by_key[item["k"]] = item
    return by_key


def compare_entries(profile, entry_a, entry_b, include_same, maps_a, maps_b):
    items_a = index_items_by_key(entry_a["items"])
    items_b = index_items_by_key(entry_b["items"])
    all_keys = sorted(set(items_a.keys()) | set(items_b.keys()))

    stats = {
        "items_only_in_a": 0,
        "items_only_in_b": 0,
        "items_changed": 0,
        "items_same": 0,
        "semantic_diffs": 0,
    }
    details = []

    partition_prefix = ""
    if entry_a["domain"] != entry_b["domain"]:
        partition_prefix = "partition_mismatch|"

    for key in all_keys:
        a_item = items_a.get(key)
        b_item = items_b.get(key)

        if a_item and b_item:
            if a_item["v"] == b_item["v"] and a_item["q"] == b_item["q"]:
                diff = "same"
                stats["items_same"] += 1
            else:
                diff = "changed"
                stats["items_changed"] += 1
        elif a_item:
            if not profile.is_key_valid_for_domain(key, entry_b["domain"]):
                continue
            diff = "only_in_a"
            stats["items_only_in_a"] += 1
        else:
            if not profile.is_key_valid_for_domain(key, entry_a["domain"]):
                continue
            diff = "only_in_b"
            stats["items_only_in_b"] += 1

        bucket = profile.classify_bucket(key, entry_a["record"], entry_b["record"])
        if diff != "same" and bucket == "semantic":
            stats["semantic_diffs"] += 1

        output_diff = partition_prefix + diff
        value_a = a_item["v"] if a_item else ""
        value_b = b_item["v"] if b_item else ""
        value_a = profile.resolve_value(value_a, key, maps_a)
        value_b = profile.resolve_value(value_b, key, maps_b)

        if include_same or diff != "same":
            details.append(
                {
                    "item_key": key,
                    "domain": entry_a["domain"],
                    "bucket": bucket,
                    "diff_status": output_diff,
                    "value_a": value_a,
                    "value_b": value_b,
                    "q_a": a_item["q"] if a_item else "",
                    "q_b": b_item["q"] if b_item else "",
                }
            )

    return stats, details


def write_csv(path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            out = {k: ensure_str(row.get(k, "")) for k in headers}
            writer.writerow(out)


def _pair_name(pair, explicit_name_key, entry_key, unique_index):
    name = pair.get(explicit_name_key)
    if name and name in unique_index:
        return name

    entry = pair.get(entry_key) or {}
    candidate = entry.get("norm_name")
    if candidate and candidate in unique_index:
        return candidate
    return ""


def rebuild_unmatched(unique_a, unique_b, dup_a, dup_b, a_to_b, matched_pairs):
    matched_a_names = set()
    matched_b_names = set()
    for pair in matched_pairs:
        a_name = _pair_name(pair, "a_name", "entry_a", unique_a)
        b_name = _pair_name(pair, "b_name", "entry_b", unique_b)
        if a_name:
            matched_a_names.add(a_name)
        if b_name:
            matched_b_names.add(b_name)

    only_a = []
    for a_name, entry_a in unique_a.items():
        target_b_name = a_to_b.get(a_name, a_name)
        if target_b_name in dup_b:
            continue
        if a_name not in matched_a_names:
            only_a.append((a_name, entry_a))

    only_b = []
    blocked_b_keys = set(dup_a.keys()) | set(dup_b.keys())
    for b_name, entry_b in unique_b.items():
        if b_name in blocked_b_keys:
            continue
        if b_name not in matched_b_names:
            only_b.append((b_name, entry_b))

    return only_a, only_b


def _validate_paths(args):
    file_a = Path(args.file_a).expanduser().resolve()
    file_b = Path(args.file_b).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    name_map_path = Path(args.name_map_path).expanduser().resolve() if args.name_map_path else None

    if not file_a.exists():
        raise ValueError("--file_a does not exist: {}".format(file_a))
    if not file_b.exists():
        raise ValueError("--file_b does not exist: {}".format(file_b))
    if name_map_path and not name_map_path.exists():
        raise ValueError("--name_map does not exist: {}".format(name_map_path))

    out_dir.mkdir(parents=True, exist_ok=True)
    return file_a, file_b, out_dir, name_map_path


def run_comparison(profile: DomainProfile, args: argparse.Namespace) -> None:
    file_a, file_b, out_dir, name_map_path = _validate_paths(args)

    raw_a = load_json(file_a)
    raw_b = load_json(file_b)

    maps_a, maps_b = profile.build_resolution_maps(raw_a, raw_b)

    records_a, included_a, skipped_a = extract_records(raw_a, "file_a", profile.domains)
    records_b, included_b, skipped_b = extract_records(raw_b, "file_b", profile.domains)

    unique_a, dup_a = build_index(records_a, profile.suppress_keys)
    unique_b, dup_b = build_index(records_b, profile.suppress_keys)

    name_map = parse_name_map(name_map_path)

    a_to_b = {}
    for a_name in unique_a.keys():
        mapped = name_map.get(a_name, a_name)
        a_to_b[a_name] = mapped

    matched_pairs = []
    only_a = []
    only_b = []
    used_b_names = set()

    for a_name, entry_a in unique_a.items():
        target_b_name = a_to_b[a_name]
        if target_b_name in dup_b:
            continue
        entry_b = unique_b.get(target_b_name)
        if entry_b and target_b_name not in used_b_names:
            matched_pairs.append({"a_name": a_name, "b_name": target_b_name, "entry_a": entry_a, "entry_b": entry_b})
            used_b_names.add(target_b_name)
        else:
            only_a.append((a_name, entry_a))

    matched_b_keys = set([pair["b_name"] for pair in matched_pairs])
    blocked_b_keys = set(dup_a.keys()) | set(dup_b.keys())
    for b_name, entry_b in unique_b.items():
        if b_name not in matched_b_keys and b_name not in blocked_b_keys:
            only_b.append((b_name, entry_b))

    matched_pairs = profile.reconstruct(matched_pairs, raw_a, raw_b)
    only_a, only_b = rebuild_unmatched(unique_a, unique_b, dup_a, dup_b, a_to_b, matched_pairs)

    summary_rows = []
    detail_rows = []
    unmatched_rows = []

    sig_identical = 0
    partition_mismatch_count = 0
    total_item_diffs = 0

    for pair in matched_pairs:
        entry_a = pair["entry_a"]
        entry_b = pair["entry_b"]
        stats, details = compare_entries(profile, entry_a, entry_b, args.include_same, maps_a, maps_b)
        is_partition_mismatch = entry_a["domain"] != entry_b["domain"]
        if is_partition_mismatch:
            match_status = "partition_mismatch"
            partition_mismatch_count += 1
        else:
            match_status = "matched"

        sig_match = "TRUE" if entry_a["sig_hash"] and entry_a["sig_hash"] == entry_b["sig_hash"] else "FALSE"
        if sig_match == "TRUE":
            sig_identical += 1

        summary_rows.append(
            {
                "template_name": entry_a["display_name"] or entry_b["display_name"],
                "partition_a": entry_a["domain"],
                "partition_b": entry_b["domain"],
                "match_status": match_status,
                "sig_hash_a": entry_a["sig_hash"],
                "sig_hash_b": entry_b["sig_hash"],
                "sig_match": sig_match,
                "total_items_a": len(entry_a["items"]),
                "total_items_b": len(entry_b["items"]),
                "items_only_in_a": stats["items_only_in_a"],
                "items_only_in_b": stats["items_only_in_b"],
                "items_changed": stats["items_changed"],
                "items_same": stats["items_same"],
                "semantic_diffs": stats["semantic_diffs"],
                "status_a": entry_a["status"],
                "status_b": entry_b["status"],
                "label_quality": entry_a["label_quality"] or entry_b["label_quality"],
            }
        )

        for detail in details:
            if detail["diff_status"].endswith("changed") or detail["diff_status"].endswith("only_in_a") or detail[
                "diff_status"
            ].endswith("only_in_b"):
                total_item_diffs += 1
            detail_rows.append(
                {
                    "template_name": entry_a["norm_name"],
                    "partition_a": entry_a["domain"],
                    "partition_b": entry_b["domain"],
                    **detail,
                }
            )

        # P3: process extra entry pairs injected by reconstruct()
        # extra_entries contribute to detail_rows only — not to summary_rows.
        for (xentry_a, xentry_b) in pair.get("extra_entries", []):
            _, xdetails = compare_entries(profile, xentry_a, xentry_b, args.include_same, maps_a, maps_b)
            for xdetail in xdetails:
                if xdetail["diff_status"].endswith("changed") or xdetail["diff_status"].endswith(
                    "only_in_a"
                ) or xdetail["diff_status"].endswith("only_in_b"):
                    total_item_diffs += 1
                detail_rows.append(
                    {
                        "template_name": xentry_a["norm_name"],
                        "partition_a": xentry_a["domain"],
                        "partition_b": xentry_b["domain"],
                        **xdetail,
                    }
                )


    for _, entry_a in only_a:
        summary_rows.append(
            {
                "template_name": entry_a["display_name"],
                "partition_a": entry_a["domain"],
                "partition_b": "",
                "match_status": "only_in_a",
                "sig_hash_a": entry_a["sig_hash"],
                "sig_hash_b": "",
                "sig_match": "NA",
                "total_items_a": len(entry_a["items"]),
                "total_items_b": "",
                "items_only_in_a": "",
                "items_only_in_b": "",
                "items_changed": "",
                "items_same": "",
                "semantic_diffs": "",
                "status_a": entry_a["status"],
                "status_b": "",
                "label_quality": entry_a["label_quality"],
            }
        )
        unmatched_rows.append(
            {
                "template_name": entry_a["display_name"],
                "partition": entry_a["domain"],
                "source_file": "file_a",
                "sig_hash": entry_a["sig_hash"],
                "record_id": entry_a["record_id"],
                "status": entry_a["status"],
                "item_count": len(entry_a["items"]),
            }
        )

    for _, entry_b in only_b:
        summary_rows.append(
            {
                "template_name": entry_b["display_name"],
                "partition_a": "",
                "partition_b": entry_b["domain"],
                "match_status": "only_in_b",
                "sig_hash_a": "",
                "sig_hash_b": entry_b["sig_hash"],
                "sig_match": "NA",
                "total_items_a": "",
                "total_items_b": len(entry_b["items"]),
                "items_only_in_a": "",
                "items_only_in_b": "",
                "items_changed": "",
                "items_same": "",
                "semantic_diffs": "",
                "status_a": "",
                "status_b": entry_b["status"],
                "label_quality": entry_b["label_quality"],
            }
        )
        unmatched_rows.append(
            {
                "template_name": entry_b["display_name"],
                "partition": entry_b["domain"],
                "source_file": "file_b",
                "sig_hash": entry_b["sig_hash"],
                "record_id": entry_b["record_id"],
                "status": entry_b["status"],
                "item_count": len(entry_b["items"]),
            }
        )

    duplicate_names = sorted(set(dup_a.keys()) | set(dup_b.keys()))
    for dup_name in duplicate_names:
        entries_a = dup_a.get(dup_name, [])
        entries_b = dup_b.get(dup_name, [])
        entries = []
        entries.extend(entries_a)
        entries.extend(entries_b)

        template_name = entries[0]["display_name"] if entries else dup_name
        summary_rows.append(
            {
                "template_name": template_name,
                "partition_a": "",
                "partition_b": "",
                "match_status": "duplicate_name_in_file",
                "sig_hash_a": "",
                "sig_hash_b": "",
                "sig_match": "NA",
                "total_items_a": "",
                "total_items_b": "",
                "items_only_in_a": "",
                "items_only_in_b": "",
                "items_changed": "",
                "items_same": "",
                "semantic_diffs": "",
                "status_a": "",
                "status_b": "",
                "label_quality": "duplicate_name_in_file",
            }
        )

        if entries_a and entries_b:
            duplicate_source = "both_duplicate"
        elif entries_a:
            duplicate_source = "file_a"
        else:
            duplicate_source = "file_b"

        for entry in entries:
            unmatched_rows.append(
                {
                    "template_name": entry["display_name"],
                    "partition": entry["domain"],
                    "source_file": duplicate_source,
                    "sig_hash": entry["sig_hash"],
                    "record_id": entry["record_id"],
                    "status": entry["status"],
                    "item_count": len(entry["items"]),
                }
            )

    summary_rows.sort(key=lambda row: (MATCH_SORT_ORDER.get(row["match_status"], 99), normalize_name(row["template_name"])))
    detail_rows.sort(
        key=lambda row: (normalize_name(row["template_name"]), BUCKET_SORT_ORDER.get(row["bucket"], 99), row["item_key"])
    )
    unmatched_rows.sort(key=lambda row: (row["source_file"], row["partition"], normalize_name(row["template_name"])))

    summary_path = out_dir / "vt_comparison_summary.csv"
    details_path = out_dir / "vt_comparison_details.csv"
    unmatched_path = out_dir / "vt_comparison_unmatched.csv"
    run_meta_path = out_dir / "vt_comparison_run_meta.json"

    write_csv(summary_path, SUMMARY_HEADERS, summary_rows)
    write_csv(details_path, DETAIL_HEADERS, detail_rows)
    write_csv(unmatched_path, UNMATCHED_HEADERS, unmatched_rows)

    label_a = args.label_a or file_a.stem
    label_b = args.label_b or file_b.stem

    matched_count = len(matched_pairs) - partition_mismatch_count

    run_meta = {
        "tool": "compare_view_templates",
        "schema_version": "vt_compare.v1",
        "profile": profile.name,
        "engine_version": "diff_engine.v1",
        "run_timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "file_a": str(file_a),
        "file_b": str(file_b),
        "label_a": label_a,
        "label_b": label_b,
        "name_map_path": str(name_map_path) if name_map_path else None,
        "include_same": bool(args.include_same),
        "domains_included": list(profile.domains),
        "domains_skipped": sorted(set(skipped_a) | set(skipped_b)),
        "domains_deferred": profile.get_deferred_domains(),
        "hash_resolution": profile.get_hash_resolution_meta(maps_a, maps_b),
        "summary": {
            "matched": matched_count,
            "only_in_a": len(only_a),
            "only_in_b": len(only_b),
            "partition_mismatch": partition_mismatch_count,
            "sig_identical": sig_identical,
            "total_item_diffs": total_item_diffs,
        },
    }

    with run_meta_path.open("w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2)
        f.write("\n")

    missing_union = sorted(set(skipped_a) | set(skipped_b))
    for domain in missing_union:
        missing_in = []
        if domain in skipped_a:
            missing_in.append("file_a")
        if domain in skipped_b:
            missing_in.append("file_b")
        print("warning: skipped missing domain '{}' in {}".format(domain, ",".join(missing_in)))

    print("compare_view_templates")
    print("  file_a:  {}  ({} templates across {} partitions)".format(label_a, len(records_a), len(included_a)))
    print("  file_b:  {}  ({} templates across {} partitions)".format(label_b, len(records_b), len(included_b)))
    print(
        "  matched: {}  |  only_in_a: {}  |  only_in_b: {}  |  partition_mismatch: {}".format(
            matched_count, len(only_a), len(only_b), partition_mismatch_count
        )
    )
    print("  sig-identical: {} / {} matched".format(sig_identical, len(matched_pairs)))
    print("  outputs written to: {}".format(out_dir))
