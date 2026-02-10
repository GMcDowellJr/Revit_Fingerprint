#!/usr/bin/env python3
"""
Merge split export files (index.json + details.json) into monolithic format.

Usage:
    python merge_split_exports.py <input_dir> <output_dir> [--dry-run] [--verify]

Example:
    python merge_split_exports.py "C:/Fingerprint_Exports" "C:/Fingerprint_Merged"
    
This script:
1. Finds all *.index.json files in input_dir
2. Matches each with corresponding *.details.json
3. Merges them into a single JSON with all metadata + domain payloads
4. Writes merged file to output_dir as <basename>.json
5. Optionally verifies merged files contain both metadata and domains
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file safely."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"JSON root must be an object: {path}")
    return data


def write_json(path: Path, data: Dict[str, Any], indent: int = 2) -> None:
    """Write JSON file with formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
        f.write("\n")  # Add trailing newline


def find_split_pairs(input_dir: Path) -> List[Tuple[Path, Path, str]]:
    """
    Find all index/details pairs in input directory.
    
    Returns: List of (index_path, details_path, stem) tuples
    """
    pairs = []
    
    # Find all index files
    index_files = sorted(input_dir.glob("*.index.json"))
    
    for index_path in index_files:
        # Determine stem (base name without .index.json)
        stem = index_path.stem.replace(".index", "")
        
        # Look for corresponding details file
        # Try multiple patterns in case naming isn't perfectly consistent
        details_candidates = [
            input_dir / f"{stem}.details.json",
            input_dir / index_path.name.replace(".index.json", ".details.json"),
        ]
        
        details_path = None
        for candidate in details_candidates:
            if candidate.exists():
                details_path = candidate
                break
        
        if details_path:
            pairs.append((index_path, details_path, stem))
        else:
            print(f"WARNING: No details file found for index: {index_path.name}")
            print(f"  Tried: {[c.name for c in details_candidates]}")
    
    return pairs


def merge_fingerprints(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge index and details into monolithic format.
    
    Strategy:
    - Start with index (has all metadata: _contract, _manifest, _features, _meta, _notes, identity)
    - Add domain payloads from details (arrowheads, dimension_types, fill_patterns, etc.)
    - Index metadata takes precedence over details for any conflicts
    
    Returns: Merged fingerprint dict
    """
    # Start with complete index (has all metadata)
    merged = dict(index_fp)
    
    # Add domain payloads from details
    # Domain payloads are top-level keys that don't start with "_"
    for key, value in details_fp.items():
        # Skip metadata keys (they should come from index)
        if key.startswith("_"):
            continue
        
        # Skip special keys that aren't domain payloads
        if key in ["identity", "runner_notes", "artifacts"]:
            continue
        
        # Add domain payload (only if not already in merged from index)
        if key not in merged:
            merged[key] = value
        # else: index already had it, keep index version
    
    return merged


def verify_merged(merged_fp: Dict[str, Any], stem: str) -> List[str]:
    """
    Verify merged fingerprint has expected structure.
    
    Returns: List of issues found (empty if valid)
    """
    issues = []
    
    # Check for essential metadata
    if "_contract" not in merged_fp:
        issues.append(f"{stem}: Missing _contract")
    elif not isinstance(merged_fp["_contract"], dict):
        issues.append(f"{stem}: _contract is not a dict")
    elif "domains" not in merged_fp["_contract"]:
        issues.append(f"{stem}: _contract missing domains")
    
    # Check that we have some domain payloads
    domain_count = 0
    for key, value in merged_fp.items():
        if not key.startswith("_") and key not in ["identity", "runner_notes", "artifacts"]:
            if isinstance(value, dict) and "records" in value:
                domain_count += 1
    
    if domain_count == 0:
        issues.append(f"{stem}: No domain payloads found")
    
    # Check metadata domains match actual domain payloads
    if "_contract" in merged_fp and "domains" in merged_fp.get("_contract", {}):
        contract_domains = merged_fp["_contract"]["domains"]
        payload_domains = {
            k for k, v in merged_fp.items() 
            if not k.startswith("_") 
            and k not in ["identity", "runner_notes", "artifacts"]
            and isinstance(v, dict)
            and "records" in v
        }
        
        # Find domains in contract but not in payloads
        missing_payloads = []
        for domain_name in sorted(contract_domains.keys()):
            # Special case: 'identity' is metadata, not a domain payload
            if domain_name == "identity":
                continue
                
            if domain_name not in payload_domains:
                # Check if this is expected (blocked/unsupported domains may have no payload)
                domain_info = contract_domains[domain_name]
                status = domain_info.get("status")
                
                # Expected cases for missing payload:
                # - status is blocked, unsupported, or failed
                # - domain has status ok but count is 0
                if status in ["blocked", "unsupported", "failed"]:
                    # This is fine - blocked domains don't have payloads
                    continue
                elif status == "ok" or status == "degraded":
                    # Check if domain has 0 records (ok but empty)
                    diag = domain_info.get("diag", {})
                    count = diag.get("count", 0)
                    if count == 0:
                        # OK/degraded domain with 0 records - payload may be legitimately absent
                        continue
                
                # If we get here, it's unexpected
                missing_payloads.append(f"{domain_name} (status={status})")
        
        if missing_payloads:
            issues.append(
                f"{stem}: Contract lists domains without payloads: {missing_payloads}"
            )
    
    return issues


def merge_split_exports(
    input_dir: Path,
    output_dir: Path,
    *,
    dry_run: bool = False,
    verify: bool = True,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Merge all split exports in input_dir into monolithic format in output_dir.
    
    Returns: Stats dict with counts and issues
    """
    stats = {
        "pairs_found": 0,
        "merged_success": 0,
        "merged_failed": 0,
        "verification_issues": [],
        "errors": []
    }
    
    # Find all index/details pairs
    pairs = find_split_pairs(input_dir)
    stats["pairs_found"] = len(pairs)
    
    if verbose:
        print(f"\nFound {len(pairs)} index/details pairs in {input_dir}\n")
    
    if not pairs:
        print("No split export pairs found. Nothing to do.")
        return stats
    
    # Process each pair
    for index_path, details_path, stem in pairs:
        try:
            if verbose:
                print(f"Processing: {stem}")
                print(f"  Index:   {index_path.name}")
                print(f"  Details: {details_path.name}")
            
            # Load both files
            index_fp = load_json(index_path)
            details_fp = load_json(details_path)
            
            # Merge
            merged_fp = merge_fingerprints(index_fp, details_fp)
            
            # Verify if requested
            if verify:
                issues = verify_merged(merged_fp, stem)
                if issues:
                    stats["verification_issues"].extend(issues)
                    if verbose:
                        for issue in issues:
                            print(f"  ⚠️  {issue}")
            
            # Write merged file
            output_path = output_dir / f"{stem}.json"
            
            if dry_run:
                if verbose:
                    print(f"  [DRY RUN] Would write: {output_path.name}")
            else:
                write_json(output_path, merged_fp)
                if verbose:
                    file_size = output_path.stat().st_size
                    print(f"  ✓ Wrote: {output_path.name} ({file_size:,} bytes)")
            
            stats["merged_success"] += 1
            
        except Exception as e:
            error_msg = f"{stem}: {type(e).__name__}: {e}"
            stats["errors"].append(error_msg)
            stats["merged_failed"] += 1
            print(f"  ✗ ERROR: {error_msg}")
        
        if verbose:
            print()
    
    return stats


def print_summary(stats: Dict[str, Any]) -> None:
    """Print summary of merge operation."""
    print("="*80)
    print("MERGE SUMMARY")
    print("="*80)
    print(f"Pairs found:      {stats['pairs_found']}")
    print(f"Merged success:   {stats['merged_success']}")
    print(f"Merged failed:    {stats['merged_failed']}")
    print()
    
    if stats["verification_issues"]:
        print(f"Verification Issues ({len(stats['verification_issues'])}):")
        for issue in stats["verification_issues"]:
            print(f"  - {issue}")
        print()
    
    if stats["errors"]:
        print(f"Errors ({len(stats['errors'])}):")
        for error in stats["errors"]:
            print(f"  - {error}")
        print()
    
    if stats["merged_success"] == stats["pairs_found"] and not stats["verification_issues"]:
        print("✅ All files merged successfully with no issues!")
    elif stats["merged_success"] > 0:
        print(f"⚠️  {stats['merged_success']} files merged but with some issues (see above)")
    else:
        print("❌ Merge failed")


def main():
    parser = argparse.ArgumentParser(
        description="Merge split export files (index.json + details.json) into monolithic format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge all split exports from exports/ to merged/
  python merge_split_exports.py exports/ merged/
  
  # Dry run to see what would happen
  python merge_split_exports.py exports/ merged/ --dry-run
  
  # Merge without verification checks
  python merge_split_exports.py exports/ merged/ --no-verify
  
  # Quiet mode (minimal output)
  python merge_split_exports.py exports/ merged/ --quiet

Output files:
  Input:  project.index.json + project.details.json
  Output: project.json (monolithic, contains everything)
        """
    )
    
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing split export files (*.index.json + *.details.json)"
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory where merged monolithic .json files will be written"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually writing files"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verification checks on merged files"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimal output (only errors and summary)"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not args.input_dir.exists():
        print(f"ERROR: Input directory does not exist: {args.input_dir}")
        return 1
    
    if not args.input_dir.is_dir():
        print(f"ERROR: Input path is not a directory: {args.input_dir}")
        return 1
    
    # Create output directory if needed
    if not args.dry_run:
        args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run merge
    stats = merge_split_exports(
        input_dir=args.input_dir.resolve(),
        output_dir=args.output_dir.resolve(),
        dry_run=args.dry_run,
        verify=not args.no_verify,
        verbose=not args.quiet
    )
    
    # Print summary
    print_summary(stats)
    
    # Exit code based on results
    if stats["merged_failed"] > 0:
        return 1
    elif stats["verification_issues"]:
        return 2  # Success but with warnings
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())