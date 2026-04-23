#!/usr/bin/env python3
"""Compare view template fingerprint records between two monolithic JSON exports."""

import argparse
import csv
import json
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path


VIEW_TEMPLATE_DOMAINS = [
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
]

DEFERRED_DOMAINS = [
    "view_category_overrides",
    "view_filter_applications_view_templates",
]

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


def extract_records(raw, file_key):
    records = []
    included_domains = []
    skipped_domains = []

    for domain in VIEW_TEMPLATE_DOMAINS:
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
            records.append(
                {
                    "file_key": file_key,
                    "domain": rec_domain,
                    "record": rec,
                }
            )

    return records, included_domains, skipped_domains


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


def get_items(record):
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
        if key == "view_template.def_hash":
            continue
        cleaned.append(
            {
                "k": key,
                "q": ensure_str(item.get("q")),
                "v": ensure_str(item.get("v")),
            }
        )
    return cleaned


def extract_key_set(raw_items):
    key_set = set()
    if not isinstance(raw_items, list):
        return key_set

    for raw_item in raw_items:
        key = ""
        if isinstance(raw_item, dict):
            key = ensure_str(raw_item.get("k"))
        else:
            key = ensure_str(raw_item)
        if key:
            key_set.add(key)
    return key_set


def build_bucket_lookup(record):
    phase2 = record.get("phase2")
    phase2 = phase2 if isinstance(phase2, dict) else {}
    sig_basis = record.get("sig_basis")
    sig_basis = sig_basis if isinstance(sig_basis, dict) else {}

    return {
        "semantic": extract_key_set(sig_basis.get("keys_used")),
        "coordination": extract_key_set(phase2.get("coordination_items")),
        "unknown": extract_key_set(phase2.get("unknown_items")),
        "cosmetic": extract_key_set(phase2.get("cosmetic_items")),
    }


def get_resolution_name(record, domain):
    label = record.get("label")
    if not isinstance(label, dict):
        return ""

    if domain == "view_filter_definitions":
        components = label.get("components")
        if isinstance(components, dict):
            comp_name = ensure_str(components.get("name")).strip()
            if comp_name:
                return comp_name

    return ensure_str(label.get("display")).strip()


def build_hash_resolution_map(raw, domain, file_key):
    payload = get_domain_payload(raw, domain)
    if not isinstance(payload, dict):
        print("  WARNING: domain '{}' not found in {} — hash resolution unavailable".format(domain, file_key))
        return {}

    records = payload.get("records")
    if not isinstance(records, list):
        print("  WARNING: domain '{}' not found in {} — hash resolution unavailable".format(domain, file_key))
        return {}

    mapping = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        sig_hash = ensure_str(record.get("sig_hash"))
        if not sig_hash:
            continue
        resolved = get_resolution_name(record, domain)
        if resolved:
            mapping[sig_hash] = resolved
    return mapping


def build_index(records):
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
                "items": get_items(record),
                "bucket_lookup": build_bucket_lookup(record),
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


def pick_bucket(item_key, a_lookup, b_lookup):
    semantic = set()
    semantic.update(a_lookup.get("semantic", set()))
    semantic.update(b_lookup.get("semantic", set()))
    if item_key in semantic:
        return "semantic"

    coordination = set()
    coordination.update(a_lookup.get("coordination", set()))
    coordination.update(b_lookup.get("coordination", set()))
    if item_key in coordination:
        return "coordination"

    unknown = set()
    unknown.update(a_lookup.get("unknown", set()))
    unknown.update(b_lookup.get("unknown", set()))
    if item_key in unknown:
        return "unknown"

    cosmetic = set()
    cosmetic.update(a_lookup.get("cosmetic", set()))
    cosmetic.update(b_lookup.get("cosmetic", set()))
    if item_key in cosmetic:
        return "cosmetic"

    return "other"


def resolve_hash(value, key, map_a, map_b):
    if value in ("", None):
        return value, value

    resolved_a = map_a.get(value)
    resolved_b = map_b.get(value)

    if not resolved_a:
        resolved_a = "{} (unresolved)".format(value)
    if not resolved_b:
        resolved_b = "{} (unresolved)".format(value)
    return resolved_a, resolved_b


def compare_entries(entry_a, entry_b, include_same, phase_filter_map_a, phase_filter_map_b, vf_def_map_a, vf_def_map_b):
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
            diff = "only_in_a"
            stats["items_only_in_a"] += 1
        else:
            diff = "only_in_b"
            stats["items_only_in_b"] += 1

        bucket = pick_bucket(key, entry_a["bucket_lookup"], entry_b["bucket_lookup"])
        if diff != "same" and bucket == "semantic":
            stats["semantic_diffs"] += 1

        output_diff = partition_prefix + diff
        value_a = a_item["v"] if a_item else ""
        value_b = b_item["v"] if b_item else ""
        if key == "view_template.sig.phase_filter":
            value_a, _ = resolve_hash(value_a, key, phase_filter_map_a, phase_filter_map_b)
            _, value_b = resolve_hash(value_b, key, phase_filter_map_a, phase_filter_map_b)
        elif key.startswith("view_template.sig.filter[") and key.endswith(".def_sig"):
            value_a, _ = resolve_hash(value_a, key, vf_def_map_a, vf_def_map_b)
            _, value_b = resolve_hash(value_b, key, vf_def_map_a, vf_def_map_b)

        if include_same or diff != "same":
            details.append(
                {
                    "item_key": key,
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


def build_parser():
    parser = argparse.ArgumentParser(description="Compare view template records between two fingerprint JSON files.")
    parser.add_argument("--file_a", required=True, help="Path to fingerprint JSON for file A")
    parser.add_argument("--file_b", required=True, help="Path to fingerprint JSON for file B")
    parser.add_argument("--out_dir", required=True, help="Directory where comparison outputs are written")
    parser.add_argument("--label_a", default=None, help="Display label for file A in logs")
    parser.add_argument("--label_b", default=None, help="Display label for file B in logs")
    parser.add_argument("--name_map", default=None, help="Optional JSON map of normalized names from file A to file B")
    parser.add_argument("--include_same", action="store_true", help="Include same-value items in details output")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    file_a = Path(args.file_a).expanduser().resolve()
    file_b = Path(args.file_b).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    name_map_path = Path(args.name_map).expanduser().resolve() if args.name_map else None

    if not file_a.exists():
        parser.error("--file_a does not exist: {}".format(file_a))
    if not file_b.exists():
        parser.error("--file_b does not exist: {}".format(file_b))
    if name_map_path and not name_map_path.exists():
        parser.error("--name_map does not exist: {}".format(name_map_path))

    out_dir.mkdir(parents=True, exist_ok=True)

    raw_a = load_json(file_a)
    raw_b = load_json(file_b)

    phase_filter_map_a = build_hash_resolution_map(raw_a, "phase_filters", "file_a")
    phase_filter_map_b = build_hash_resolution_map(raw_b, "phase_filters", "file_b")
    vf_def_map_a = build_hash_resolution_map(raw_a, "view_filter_definitions", "file_a")
    vf_def_map_b = build_hash_resolution_map(raw_b, "view_filter_definitions", "file_b")

    records_a, included_a, skipped_a = extract_records(raw_a, "file_a")
    records_b, included_b, skipped_b = extract_records(raw_b, "file_b")

    unique_a, dup_a = build_index(records_a)
    unique_b, dup_b = build_index(records_b)

    name_map = parse_name_map(name_map_path)

    a_to_b = {}
    for a_name in unique_a.keys():
        mapped = name_map.get(a_name, a_name)
        a_to_b[a_name] = mapped

    matched_pairs = []
    only_a = []
    only_b = []

    for a_name, entry_a in unique_a.items():
        target_b_name = a_to_b[a_name]
        entry_b = unique_b.get(target_b_name)
        if entry_b:
            matched_pairs.append((a_name, target_b_name, entry_a, entry_b))
        else:
            only_a.append((a_name, entry_a))

    matched_b_keys = {b_name for _, b_name, _, _ in matched_pairs}
    for b_name, entry_b in unique_b.items():
        if b_name not in matched_b_keys:
            only_b.append((b_name, entry_b))

    summary_rows = []
    detail_rows = []
    unmatched_rows = []

    sig_identical = 0
    partition_mismatch_count = 0
    total_item_diffs = 0

    for _, _, entry_a, entry_b in matched_pairs:
        stats, details = compare_entries(
            entry_a,
            entry_b,
            args.include_same,
            phase_filter_map_a,
            phase_filter_map_b,
            vf_def_map_a,
            vf_def_map_b,
        )
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
            if detail["diff_status"].endswith("changed") or detail["diff_status"].endswith("only_in_a") or detail["diff_status"].endswith("only_in_b"):
                total_item_diffs += 1
            detail_rows.append(
                {
                    "template_name": entry_a["norm_name"],
                    "partition_a": entry_a["domain"],
                    "partition_b": entry_b["domain"],
                    **detail,
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
        entries = []
        entries.extend(dup_a.get(dup_name, []))
        entries.extend(dup_b.get(dup_name, []))

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

        for entry in entries:
            unmatched_rows.append(
                {
                    "template_name": entry["display_name"],
                    "partition": entry["domain"],
                    "source_file": "both_duplicate",
                    "sig_hash": entry["sig_hash"],
                    "record_id": entry["record_id"],
                    "status": entry["status"],
                    "item_count": len(entry["items"]),
                }
            )

    summary_rows.sort(key=lambda row: (MATCH_SORT_ORDER.get(row["match_status"], 99), normalize_name(row["template_name"])))
    detail_rows.sort(
        key=lambda row: (
            normalize_name(row["template_name"]),
            BUCKET_SORT_ORDER.get(row["bucket"], 99),
            row["item_key"],
        )
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
        "run_timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "file_a": str(file_a),
        "file_b": str(file_b),
        "label_a": label_a,
        "label_b": label_b,
        "name_map_path": str(name_map_path) if name_map_path else None,
        "include_same": bool(args.include_same),
        "domains_included": VIEW_TEMPLATE_DOMAINS,
        "domains_skipped": sorted(set(skipped_a) | set(skipped_b)),
        "domains_deferred": DEFERRED_DOMAINS,
        "hash_resolution": {
            "phase_filter_map_a_size": len(phase_filter_map_a),
            "phase_filter_map_b_size": len(phase_filter_map_b),
            "vf_def_map_a_size": len(vf_def_map_a),
            "vf_def_map_b_size": len(vf_def_map_b),
        },
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


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("ERROR: unexpected failure in compare_view_templates", file=sys.stderr)
        traceback.print_exc()
        raise
