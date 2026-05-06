#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example demonstrating how to use split export (index.json + details.json).

This script shows:
1. Loading index.json for fast contract access
2. Loading details.json for record-level analysis
3. Using index with build_manifest() and build_features()
"""

import json
import sys
from pathlib import Path

# Add repo root to path
_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.manifest import build_manifest
from core.features import build_features


def example_index_only_workflow(index_path: Path):
    """
    Example: Fast contract validation using only index.json.

    Use case: CI/CD pipelines that only need to check domain statuses.
    """
    print(f"\n{'='*60}")
    print("Example 1: Index-only workflow (fast contract validation)")
    print(f"{'='*60}\n")

    # Load index (small file, fast)
    with open(index_path) as f:
        index = json.load(f)

    print(f"✓ Loaded index from: {index_path}")
    print(f"  File size: {index_path.stat().st_size:,} bytes")

    # Extract key metadata
    contract = index["_contract"]
    hash_mode = index["_hash_mode"]

    print(f"\n📋 Contract Summary:")
    print(f"  Schema version: {contract['schema_version']}")
    print(f"  Run status: {contract['run_status']}")
    print(f"  Hash mode: {hash_mode}")

    # Count domains by status
    domains = contract["domains"]
    status_counts = {}
    for domain_name, domain_data in domains.items():
        status = domain_data["status"]
        status_counts[status] = status_counts.get(status, 0) + 1

    print(f"\n📊 Domain Status Distribution:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    # Build manifest (works with index only)
    print(f"\n🔨 Building manifest from index...")
    manifest = build_manifest(index, include_identity=True)
    print(f"  ✓ Manifest built successfully")
    print(f"  Domains in manifest: {len(manifest['domains'])}")

    # Build features (works with index only - reads counts from contract.diag)
    print(f"\n🔨 Building features from index...")
    features = build_features(index)
    print(f"  ✓ Features built successfully")
    print(f"  Domains in features: {len(features['domains'])}")

    # Show example counts (read from contract.diag)
    print(f"\n📈 Sample Domain Counts (from contract.diag):")
    for domain_name in sorted(list(domains.keys())[:3]):  # Show first 3
        domain_data = domains[domain_name]
        diag = domain_data.get("diag", {})
        count = diag.get("count")
        raw_count = diag.get("raw_count")
        if count is not None:
            print(f"  {domain_name}: count={count}, raw_count={raw_count}")

    print(f"\n✅ Index-only workflow complete!")
    return index, manifest, features


def example_details_workflow(details_path: Path):
    """
    Example: Record-level analysis using details.json.

    Use case: Similarity comparison that needs per-record signature hashes.
    """
    print(f"\n{'='*60}")
    print("Example 2: Details workflow (record-level analysis)")
    print(f"{'='*60}\n")

    # Load details (large file, contains all records)
    with open(details_path) as f:
        details = json.load(f)

    print(f"✓ Loaded details from: {details_path}")
    print(f"  File size: {details_path.stat().st_size:,} bytes")

    # Analyze records
    print(f"\n📦 Domain Record Counts:")
    total_records = 0
    for domain_name in sorted(details.keys()):
        domain_data = details[domain_name]
        if isinstance(domain_data, dict) and "records" in domain_data:
            records = domain_data["records"]
            count = len(records) if isinstance(records, list) else 0
            total_records += count
            print(f"  {domain_name}: {count} records")

    print(f"\n📊 Total records across all domains: {total_records}")

    # Show example record structure (from first domain with records)
    for domain_name in sorted(details.keys()):
        domain_data = details[domain_name]
        if isinstance(domain_data, dict) and "records" in domain_data:
            records = domain_data["records"]
            if isinstance(records, list) and len(records) > 0:
                print(f"\n🔍 Example record from '{domain_name}':")
                example_record = records[0]
                print(f"  Keys: {list(example_record.keys())}")
                if "sig_hash" in example_record:
                    print(f"  sig_hash: {example_record['sig_hash']}")
                if "status" in example_record:
                    print(f"  status: {example_record['status']}")
                break

    print(f"\n✅ Details workflow complete!")
    return details


def example_combined_workflow(index_path: Path, details_path: Path):
    """
    Example: Using both index and details together.

    Use case: Full analysis pipeline that needs both contract metadata and records.
    """
    print(f"\n{'='*60}")
    print("Example 3: Combined workflow (index + details)")
    print(f"{'='*60}\n")

    # Load index for fast contract access
    with open(index_path) as f:
        index = json.load(f)

    print(f"✓ Loaded index for contract metadata")

    # Load details only if needed for record analysis
    with open(details_path) as f:
        details = json.load(f)

    print(f"✓ Loaded details for record-level analysis")

    # Use index for contract validation
    contract = index["_contract"]
    run_status = contract["run_status"]

    print(f"\n🎯 Combined Analysis:")
    print(f"  Run status (from index): {run_status}")

    # Use details for record counts
    ok_domains = []
    for domain_name, domain_data in contract["domains"].items():
        if domain_data["status"] == "ok":
            ok_domains.append(domain_name)

    print(f"  OK domains (from index): {len(ok_domains)}")

    # Get record counts from details
    total_ok_records = 0
    for domain_name in ok_domains:
        if domain_name in details:
            domain_data = details[domain_name]
            if isinstance(domain_data, dict) and "records" in domain_data:
                records = domain_data["records"]
                if isinstance(records, list):
                    total_ok_records += len(records)

    print(f"  Total records in OK domains (from details): {total_ok_records}")

    print(f"\n✅ Combined workflow complete!")


def main():
    """
    Run example workflows demonstrating split export usage.
    """
    import argparse

    ap = argparse.ArgumentParser(
        description="Example demonstrating split export usage (index + details)"
    )
    ap.add_argument(
        "--index",
        required=True,
        help="Path to .index.json file"
    )
    ap.add_argument(
        "--details",
        required=False,
        help="Path to .details.json file (optional, for examples 2 and 3)"
    )

    args = ap.parse_args()

    index_path = Path(args.index)
    if not index_path.exists():
        print(f"❌ Error: Index file not found: {index_path}")
        return 1

    # Example 1: Index-only workflow (always runs)
    try:
        example_index_only_workflow(index_path)
    except Exception as e:
        print(f"❌ Error in index-only workflow: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Examples 2 and 3: Details workflow (only if details file provided)
    if args.details:
        details_path = Path(args.details)
        if not details_path.exists():
            print(f"❌ Error: Details file not found: {details_path}")
            return 1

        try:
            example_details_workflow(details_path)
        except Exception as e:
            print(f"❌ Error in details workflow: {e}")
            import traceback
            traceback.print_exc()
            return 1

        try:
            example_combined_workflow(index_path, details_path)
        except Exception as e:
            print(f"❌ Error in combined workflow: {e}")
            import traceback
            traceback.print_exc()
            return 1
    else:
        print(f"\n💡 Tip: Provide --details to see examples 2 and 3")

    print(f"\n{'='*60}")
    print("✅ All examples completed successfully!")
    print(f"{'='*60}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
