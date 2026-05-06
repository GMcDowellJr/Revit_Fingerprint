#!/usr/bin/env python3
"""
Diagnostic script to identify why Phase 1 CSVs are empty.

Usage:
    python diagnose_authority_empty.py --exports <exports_dir> --config <config_json> [--phase1-out <phase1_dir>]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file safely."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            print(f"ERROR: {path} is not a JSON object")
            return {}
        return data
    except Exception as e:
        print(f"ERROR loading {path}: {e}")
        return {}


def extract_domains_from_fp(fp: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Extract domain summary from fingerprint (mimics domain_authority logic)."""
    out: Dict[str, Dict[str, Any]] = {}
    
    # Try _manifest.domains first
    manifest_domains = fp.get("_manifest", {}).get("domains", {})
    if isinstance(manifest_domains, dict):
        for dom, rec in manifest_domains.items():
            if not isinstance(rec, dict):
                continue
            out[dom] = {
                "status": rec.get("status"),
                "hash": rec.get("hash"),
                "domain_version": None,
                "block_reasons": rec.get("block_reasons", []),
                "source": "_manifest"
            }
    
    # Then _contract.domains (can override)
    contract_domains = fp.get("_contract", {}).get("domains", {})
    if isinstance(contract_domains, dict):
        for dom, rec in contract_domains.items():
            if not isinstance(rec, dict):
                continue
            if dom not in out:
                out[dom] = {}
            out[dom].update({
                "status": rec.get("status"),
                "hash": rec.get("hash"),
                "domain_version": rec.get("domain_version"),
                "block_reasons": rec.get("block_reasons", []),
                "source": "_contract"
            })
    
    # Finally _features.domains (lowest priority)
    features_domains = fp.get("_features", {}).get("domains", {})
    if isinstance(features_domains, dict):
        for dom, rec in features_domains.items():
            if not isinstance(rec, dict):
                continue
            if dom not in out:
                out[dom] = {
                    "status": rec.get("status"),
                    "hash": rec.get("hash"),
                    "domain_version": None,
                    "block_reasons": rec.get("block_reasons", []),
                    "source": "_features"
                }
    
    return out


def check_domain_records(fp: Dict[str, Any], domain: str) -> Optional[int]:
    """Check if domain has records in the fingerprint."""
    # Try domain payload
    payload = fp.get(domain)
    if isinstance(payload, dict):
        records = payload.get("records")
        if isinstance(records, list):
            return len(records)
    return None


def diagnose_exports(exports_dir: Path) -> Dict[str, Any]:
    """Diagnose export directory and return findings."""
    findings = {
        "exports_dir": str(exports_dir),
        "exists": exports_dir.exists(),
        "is_dir": exports_dir.is_dir() if exports_dir.exists() else False,
        "files": {
            "details": [],
            "index": [],
            "legacy": [],
            "other_json": []
        },
        "projects": [],
        "domains_across_all": set(),
        "issues": []
    }
    
    if not findings["exists"]:
        findings["issues"].append(f"CRITICAL: exports_dir does not exist: {exports_dir}")
        return findings
    
    if not findings["is_dir"]:
        findings["issues"].append(f"CRITICAL: exports_dir is not a directory: {exports_dir}")
        return findings
    
    # Scan for JSON files
    for p in exports_dir.glob("*.json"):
        if not p.is_file():
            continue
        
        name_lower = str(p.name).lower()
        if name_lower.endswith(".details.json"):
            findings["files"]["details"].append(p.name)
        elif name_lower.endswith(".index.json"):
            findings["files"]["index"].append(p.name)
        elif name_lower.endswith(".legacy.json"):
            findings["files"]["legacy"].append(p.name)
        else:
            findings["files"]["other_json"].append(p.name)
    
    # Determine which files will be loaded by domain_authority
    if findings["files"]["details"]:
        json_paths = [exports_dir / f for f in sorted(findings["files"]["details"])]
        findings["surface_used"] = "details"
        findings["issues"].append("INFO: Using *.details.json files (correct)")
    elif findings["files"]["index"]:
        json_paths = [exports_dir / f for f in sorted(findings["files"]["index"])]
        findings["surface_used"] = "index"
        findings["issues"].append("WARNING: Using *.index.json files (degraded - no records)")
    else:
        # Fallback to *.json excluding legacy
        others = [p for p in findings["files"]["other_json"] 
                 if not p.lower().endswith(".legacy.json")]
        json_paths = [exports_dir / f for f in sorted(others)]
        findings["surface_used"] = "other_json"
        if others:
            findings["issues"].append("WARNING: Using generic *.json files (may be incorrect)")
        else:
            findings["issues"].append("CRITICAL: No compatible JSON files found")
            return findings
    
    # Load each project
    for json_path in json_paths:
        fp = load_json(json_path)
        if not fp:
            findings["issues"].append(f"ERROR: Could not load {json_path.name}")
            continue
        
        # Extract project ID
        identity = fp.get("identity", {})
        pid = identity.get("project_title") or identity.get("file_path") or json_path.stem
        
        # Extract domains
        domains = extract_domains_from_fp(fp)
        
        project_info = {
            "file": json_path.name,
            "project_id": pid,
            "domains_count": len(domains),
            "domains": {}
        }
        
        for dom, info in domains.items():
            findings["domains_across_all"].add(dom)
            
            rec_count = check_domain_records(fp, dom)
            
            project_info["domains"][dom] = {
                "status": info.get("status"),
                "hash": info.get("hash"),
                "has_hash": isinstance(info.get("hash"), str) and bool(info.get("hash")),
                "record_count": rec_count,
                "source": info.get("source")
            }
        
        findings["projects"].append(project_info)
    
    findings["domains_across_all"] = sorted(findings["domains_across_all"])
    findings["total_projects"] = len(findings["projects"])
    
    return findings


def diagnose_config(config_path: Path) -> Dict[str, Any]:
    """Diagnose config JSON."""
    findings = {
        "config_path": str(config_path),
        "exists": config_path.exists(),
        "issues": []
    }
    
    if not findings["exists"]:
        findings["issues"].append(f"CRITICAL: config file does not exist: {config_path}")
        return findings
    
    cfg = load_json(config_path)
    if not cfg:
        findings["issues"].append("CRITICAL: Could not load config JSON")
        return findings
    
    findings["config"] = cfg
    findings["domains_in_scope"] = cfg.get("domains_in_scope", [])
    findings["has_domains_in_scope"] = bool(findings["domains_in_scope"])
    
    if not findings["has_domains_in_scope"]:
        findings["issues"].append(
            "WARNING: domains_in_scope is empty - Phase 1 may use auto-discovery fallback"
        )
    else:
        findings["issues"].append(
            f"INFO: domains_in_scope has {len(findings['domains_in_scope'])} domains"
        )
    
    findings["cluster_method"] = cfg.get("cluster_method", "semantic_multiset_exact")
    findings["status_comparable_set"] = cfg.get("status_comparable_set", ["ok"])
    
    return findings


def check_phase1_compatibility(
    exports_findings: Dict[str, Any], 
    config_findings: Dict[str, Any]
) -> List[str]:
    """Check compatibility between exports and config for Phase 1."""
    issues = []
    
    # Get domains from config
    config_domains = set(config_findings.get("domains_in_scope", []))
    
    # Get domains from exports
    export_domains = set(exports_findings.get("domains_across_all", []))
    
    if not config_domains:
        issues.append(
            "WARNING: Config has no domains_in_scope - Phase 1 will discover domains from exports"
        )
        config_domains = export_domains  # Use discovered for further analysis
    
    # Check domain overlap
    missing_in_exports = config_domains - export_domains
    if missing_in_exports:
        issues.append(
            f"WARNING: {len(missing_in_exports)} domains in config not found in exports: "
            f"{sorted(missing_in_exports)}"
        )
    
    # For each project, check domain coverage
    status_comparable = set(config_findings.get("status_comparable_set", ["ok"]))
    cluster_method = config_findings.get("cluster_method", "semantic_multiset_exact")
    
    for proj in exports_findings.get("projects", []):
        proj_id = proj["project_id"]
        for dom in config_domains:
            if dom not in proj["domains"]:
                issues.append(
                    f"INFO: Project '{proj_id}' missing domain '{dom}'"
                )
                continue
            
            dom_info = proj["domains"][dom]
            status = dom_info.get("status")
            has_hash = dom_info.get("has_hash")
            rec_count = dom_info.get("record_count")
            
            # Check if domain is comparable
            is_comparable_status = status in status_comparable
            
            if is_comparable_status and not has_hash:
                issues.append(
                    f"WARNING: Project '{proj_id}' domain '{dom}' has status '{status}' "
                    f"but no hash - will not be comparable"
                )
            
            if is_comparable_status and has_hash and rec_count == 0:
                issues.append(
                    f"INFO: Project '{proj_id}' domain '{dom}' has hash but 0 records "
                    f"(may affect semantic clustering)"
                )
            
            if cluster_method == "semantic_multiset_exact" and rec_count is None:
                issues.append(
                    f"WARNING: Project '{proj_id}' domain '{dom}' has no records payload "
                    f"- semantic clustering will be degraded"
                )
    
    return issues


def predict_phase1_output(
    exports_findings: Dict[str, Any],
    config_findings: Dict[str, Any]
) -> Dict[str, Any]:
    """Predict what Phase 1 output would look like."""
    prediction = {
        "baseline_coverage_rows": 0,
        "domain_cluster_rows": 0,
        "domain_authority_rows": 0,
        "by_domain": {}
    }
    
    config_domains = set(config_findings.get("domains_in_scope", []))
    if not config_domains:
        config_domains = set(exports_findings.get("domains_across_all", []))
    
    status_comparable = set(config_findings.get("status_comparable_set", ["ok"]))
    
    for dom in sorted(config_domains):
        dom_pred = {
            "projects_total": 0,
            "projects_comparable": 0,
            "projects_blocked": 0,
            "projects_unsupported": 0,
            "projects_failed": 0,
            "projects_other": 0
        }
        
        for proj in exports_findings.get("projects", []):
            dom_pred["projects_total"] += 1
            
            if dom not in proj["domains"]:
                dom_pred["projects_other"] += 1
                continue
            
            dom_info = proj["domains"][dom]
            status = dom_info.get("status")
            has_hash = dom_info.get("has_hash")
            
            # Count as comparable only if: comparable status AND has hash
            if status in status_comparable and has_hash:
                dom_pred["projects_comparable"] += 1
            elif status == "blocked":
                dom_pred["projects_blocked"] += 1
            elif status == "unsupported":
                dom_pred["projects_unsupported"] += 1
            elif status == "failed":
                dom_pred["projects_failed"] += 1
            else:
                dom_pred["projects_other"] += 1
        
        # Predict CSV rows
        # baseline_coverage: 1 row per project per domain
        prediction["baseline_coverage_rows"] += dom_pred["projects_total"]
        
        # domain_cluster: depends on comparable count (simplified)
        if dom_pred["projects_comparable"] > 0:
            prediction["domain_cluster_rows"] += 1  # At least 1 cluster per domain
        
        # domain_authority: 1 row per domain
        prediction["domain_authority_rows"] += 1
        
        prediction["by_domain"][dom] = dom_pred
    
    return prediction


def main():
    ap = argparse.ArgumentParser(description="Diagnose why Phase 1 CSVs are empty")
    ap.add_argument("--exports", required=True, help="Exports directory")
    ap.add_argument("--config", required=True, help="Phase 1 config JSON")
    ap.add_argument("--phase1-out", help="Phase 1 output directory (optional, to check actual CSVs)")
    args = ap.parse_args()
    
    exports_dir = Path(args.exports).resolve()
    config_path = Path(args.config).resolve()
    
    print("="*80)
    print("PHASE 1 DIAGNOSTIC REPORT")
    print("="*80)
    print()
    
    # Diagnose exports
    print("1. EXPORTS DIRECTORY ANALYSIS")
    print("-"*80)
    exports_findings = diagnose_exports(exports_dir)
    
    print(f"Path: {exports_findings['exports_dir']}")
    print(f"Exists: {exports_findings['exists']}")
    print(f"Surface: {exports_findings.get('surface_used', 'N/A')}")
    print(f"Files found:")
    print(f"  - *.details.json: {len(exports_findings['files']['details'])}")
    print(f"  - *.index.json: {len(exports_findings['files']['index'])}")
    print(f"  - *.legacy.json: {len(exports_findings['files']['legacy'])}")
    print(f"  - other *.json: {len(exports_findings['files']['other_json'])}")
    print(f"Total projects: {exports_findings.get('total_projects', 0)}")
    print(f"Domains across all: {len(exports_findings.get('domains_across_all', []))}")
    print(f"  {', '.join(exports_findings.get('domains_across_all', []))}")
    print()
    
    if exports_findings.get("issues"):
        print("Issues:")
        for issue in exports_findings["issues"]:
            print(f"  - {issue}")
        print()
    
    # Diagnose config
    print("2. CONFIG JSON ANALYSIS")
    print("-"*80)
    config_findings = diagnose_config(config_path)
    
    print(f"Path: {config_findings['config_path']}")
    print(f"Exists: {config_findings['exists']}")
    print(f"domains_in_scope count: {len(config_findings.get('domains_in_scope', []))}")
    if config_findings.get("domains_in_scope"):
        print(f"  {', '.join(config_findings['domains_in_scope'])}")
    print(f"cluster_method: {config_findings.get('cluster_method')}")
    print(f"status_comparable_set: {config_findings.get('status_comparable_set')}")
    print()
    
    if config_findings.get("issues"):
        print("Issues:")
        for issue in config_findings["issues"]:
            print(f"  - {issue}")
        print()
    
    # Check compatibility
    print("3. COMPATIBILITY ANALYSIS")
    print("-"*80)
    compat_issues = check_phase1_compatibility(exports_findings, config_findings)
    if compat_issues:
        for issue in compat_issues:
            print(f"  - {issue}")
    else:
        print("  No compatibility issues found")
    print()
    
    # Predict output
    print("4. PREDICTED PHASE 1 OUTPUT")
    print("-"*80)
    prediction = predict_phase1_output(exports_findings, config_findings)
    
    print(f"Predicted CSV row counts:")
    print(f"  - baseline_coverage_by_project.csv: {prediction['baseline_coverage_rows']} rows")
    print(f"  - domain_cluster_summary.csv: {prediction['domain_cluster_rows']} rows")
    print(f"  - domain_authority_summary.csv: {prediction['domain_authority_rows']} rows")
    print()
    
    print("Per-domain predictions:")
    for dom, pred in prediction["by_domain"].items():
        print(f"  {dom}:")
        print(f"    - Total: {pred['projects_total']}")
        print(f"    - Comparable: {pred['projects_comparable']}")
        print(f"    - Blocked: {pred['projects_blocked']}")
        print(f"    - Other: {pred['projects_other']}")
    print()
    
    # Check actual output if provided
    if args.phase1_out:
        print("5. ACTUAL PHASE 1 OUTPUT CHECK")
        print("-"*80)
        phase1_dir = Path(args.phase1_out).resolve()
        
        if not phase1_dir.exists():
            print(f"  Phase 1 output directory does not exist: {phase1_dir}")
        else:
            for csv_name in ["baseline_coverage_by_project.csv", 
                           "domain_cluster_summary.csv", 
                           "domain_authority_summary.csv"]:
                csv_path = phase1_dir / csv_name
                if csv_path.exists():
                    # Count lines (header + data)
                    with csv_path.open("r", encoding="utf-8") as f:
                        lines = sum(1 for _ in f)
                    data_rows = max(0, lines - 1)  # Subtract header
                    print(f"  {csv_name}: {data_rows} data rows (predicted: varies)")
                else:
                    print(f"  {csv_name}: FILE NOT FOUND")
        print()
    
    # Summary diagnosis
    print("="*80)
    print("DIAGNOSIS SUMMARY")
    print("="*80)
    
    critical_issues = [i for i in exports_findings.get("issues", []) + config_findings.get("issues", []) + compat_issues
                      if "CRITICAL" in i]
    warning_issues = [i for i in exports_findings.get("issues", []) + config_findings.get("issues", []) + compat_issues
                     if "WARNING" in i]
    
    if critical_issues:
        print("CRITICAL ISSUES FOUND:")
        for issue in critical_issues:
            print(f"  - {issue}")
        print()
        print("Phase 1 will likely fail or produce no output.")
    elif prediction["baseline_coverage_rows"] == 0:
        print("PROBLEM IDENTIFIED:")
        print("  No comparable projects found across all domains.")
        print("  This will result in empty Phase 1 CSVs.")
        print()
        print("LIKELY CAUSES:")
        print("  1. No projects have status='ok' for any domain")
        print("  2. Projects with status='ok' are missing domain hashes")
        print("  3. Using *.index.json files (no records)")
        print()
    elif warning_issues:
        print("WARNINGS FOUND:")
        for issue in warning_issues:
            print(f"  - {issue}")
        print()
        print("Phase 1 may produce degraded or incomplete output.")
    else:
        print("No critical issues found.")
        print(f"Phase 1 should produce output with ~{prediction['baseline_coverage_rows']} baseline coverage rows.")
    
    return 0 if not critical_issues else 1


if __name__ == "__main__":
    sys.exit(main())
