#!/usr/bin/env python3
"""
Validate that every field a sig-hash or join-key policy declares actually
appears as an observed item_key somewhere in the real corpus.

Motivation
----------
core/sig_hash_builder.py's build_sig_hash_from_policy() filters hash_items by
`allowed_items`/`allowed_item_prefixes` -- but a field name in `allowed_items`
that the extractor never actually emits doesn't error: _key_allowed() simply
finds nothing to match, silently. Same story for `required_items`/
`optional_items`/`explicitly_excluded_items` in a join-key policy, and for
`shape_gating`'s `discriminator_key`/`additional_required`/`additional_optional`
in either. Nothing in either builder, and nothing in discover_hash_policy.py
or discover_join_policy.py, confirms a declared field name is a real,
observed field -- discover_*.py's own req_missing_from_data check covers
*required* fields during a discovery run, but only required fields, and only
as a side effect of actually running discovery.

This is a standalone, single-pass scan (no sampling, no combinatorial
search) over the full population per domain -- cheap even at corpus scale --
checking every declared field name in a policy against every item_key
actually observed for that domain. It's the same class of drift this session
found by hand (domain_sig_hash_policies.json's identity block briefly said
`project_info.office` where the documented contract key is
`project_info.business_center`) -- this makes checking for it repeatable
instead of ad hoc.

Non-blocking, matching this project's existing posture (the sample-vs-full
divergence WARNING in discover_join_policy.py / discover_hash_policy.py, the
sig/join convergence check in run_discovery_sweep.py): this tool flags,
it never fails a build or blocks a pinning decision on its own.

Examples
--------
Check both policies against a records root:
    python tools/validate_policy_field_coverage.py \\
        --phase0-dir Fingerprint_Data/records \\
        --sig-policy-json policies/domain_sig_hash_policies.json \\
        --join-policy-json policies/domain_join_key_policies.json

Check just one policy, one domain:
    python tools/validate_policy_field_coverage.py \\
        --phase0-dir Fingerprint_Data/records \\
        --sig-policy-json policies/domain_sig_hash_policies.json \\
        --domains arrowheads,identity
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Set

try:
    from tools.discover_join_policy import _write_csv, _diagnostics_domain_suffix
except ModuleNotFoundError:
    from discover_join_policy import _write_csv, _diagnostics_domain_suffix


TARGET_FILES = {
    "sig": ["signature_items.csv", "identity_items.csv", "phase0_identity_items.csv"],
    "join": ["join_items.csv", "identity_items.csv", "phase0_identity_items.csv"],
}


def _resolve_phase0_dir(path: Path) -> Path:
    """Same resolution discover_hash_policy.py/discover_join_policy.py use."""
    if (path / "records.csv").exists():
        return path
    records_dir = path / "records"
    if (records_dir / "records.csv").exists():
        return records_dir
    results_records = path / "results" / "records"
    if (results_records / "records.csv").exists():
        return results_records
    nested = path / "phase0_v21"
    if (nested / "records.csv").exists():
        return nested
    return path


def _stream_domain_item_keys(path: Path, domains_allow: Set[str]) -> Dict[str, Set[str]]:
    """Lean single-purpose reader: extracts only `domain`/`item_key` per row,
    skipping every other column (schema_version, export_run_id, record_pk,
    item_value, item_value_type, item_role -- none of which this tool needs).
    _read_csv() builds a full dict of every column per row, which is fine for
    discover_*.py's own use (it needs record_pk/item_value/item_value_type
    for actual scoring) but wasteful here at multi-million-row corpus scale --
    this coverage check only ever asks "does this domain/item_key pair exist
    at all." When domains_allow is non-empty, rows for any other domain are
    discarded immediately rather than accumulated, since --domains scoping
    should mean skipped work, not just a filter applied after the fact.
    """
    out: Dict[str, Set[str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return out
        try:
            domain_idx = header.index("domain")
            key_idx = header.index("item_key")
        except ValueError:
            return out
        width = max(domain_idx, key_idx) + 1
        for row in reader:
            if len(row) < width:
                continue
            domain = row[domain_idx].strip()
            if not domain or (domains_allow and domain not in domains_allow):
                continue
            key = row[key_idx].strip()
            if not key:
                continue
            out.setdefault(domain, set()).add(key)
    return out


def _observed_item_keys_by_domain(
    phase0_dir: Path, target: str, domains_allow: Set[str], cache: Dict[Path, Dict[str, Set[str]]]
) -> Dict[str, Set[str]]:
    """domain -> set of every item_key actually observed for it, full population,
    single streaming pass. Prefers per-domain shards (identity_items_by_domain/)
    when a completed shard set is present, same as discover_hash_policy.py --
    and skips shard files for domains outside domains_allow entirely when scoped.

    `cache` is keyed by resolved source path: TARGET_FILES["sig"] and
    TARGET_FILES["join"] both fall back to identity_items.csv when a
    deployment has no separate signature_items.csv/join_items.csv, so calling
    this once for sig and once for join would otherwise re-read and re-parse
    the identical multi-million-row file twice in the same run.
    """
    shard_dir = phase0_dir / "identity_items_by_domain"
    use_shards = (shard_dir / ".complete").is_file()

    if use_shards:
        out: Dict[str, Set[str]] = {}
        shard_paths = sorted(shard_dir.glob("*.csv"))
        if domains_allow:
            shard_paths = [p for p in shard_paths if p.stem in domains_allow]
        for shard_path in shard_paths:
            if shard_path in cache:
                out.update(cache[shard_path])
                continue
            domain = shard_path.stem
            per_shard = _stream_domain_item_keys(shard_path, set())
            keys = per_shard.get(domain, set())
            if keys:
                cache[shard_path] = {domain: keys}
                out.setdefault(domain, set()).update(keys)
        if out:
            return out
        # Fall through to monolithic if shards existed but were empty/unreadable.

    for name in TARGET_FILES[target]:
        p = phase0_dir / name
        if not p.exists():
            continue
        if p in cache:
            return cache[p]
        result = _stream_domain_item_keys(p, domains_allow)
        cache[p] = result
        return result
    return {}


def _field_covered(field: str, observed: Set[str], prefixes: List[str]) -> bool:
    if field in observed:
        return True
    return any(field.startswith(p) for p in prefixes if p)


def _shape_gate_fields(shape_gating: Dict[str, Any]) -> Set[str]:
    """discriminator_key plus every additional_required/additional_optional
    field named across every declared shape."""
    if not isinstance(shape_gating, dict):
        return set()
    out: Set[str] = set()
    disc = shape_gating.get("discriminator_key")
    if isinstance(disc, str) and disc.strip():
        out.add(disc.strip())
    shape_requirements = shape_gating.get("shape_requirements")
    if isinstance(shape_requirements, dict):
        for shape_cfg in shape_requirements.values():
            if not isinstance(shape_cfg, dict):
                continue
            for key in ("additional_required", "additional_optional"):
                for f in shape_cfg.get(key) or []:
                    if isinstance(f, str) and f.strip():
                        out.add(f.strip())
    return out


def validate_sig_policy(
    policy: Dict[str, Any], observed_by_domain: Dict[str, Set[str]], domains_allow: Set[str], policy_file: str
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    domains = policy.get("domains") if isinstance(policy.get("domains"), dict) else {}
    for domain, pol in sorted(domains.items()):
        if domains_allow and domain not in domains_allow:
            continue
        if not isinstance(pol, dict):
            continue
        observed = observed_by_domain.get(domain, set())
        if not observed:
            rows.append({
                "policy_type": "sig", "policy_file": policy_file, "domain": domain, "field": "", "declared_in": "",
                "severity": "warning", "issue": "no_observed_data_for_domain",
                "detail": "domain has no observed item_key data at all -- cannot validate coverage (empty/missing export for this domain, or domain name mismatch).",
            })
            continue

        prefixes = [p for p in (pol.get("allowed_item_prefixes") or []) if isinstance(p, str)]
        allowed = [f for f in (pol.get("allowed_items") or []) if isinstance(f, str)]
        required = [f for f in (pol.get("required_items") or []) if isinstance(f, str)]

        for field in allowed:
            if not _field_covered(field, observed, prefixes):
                rows.append({
                    "policy_type": "sig", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "allowed_items",
                    "severity": "warning", "issue": "declared_field_never_observed",
                    "detail": "listed in allowed_items but never appears as an item_key in the real corpus for this domain -- silently contributes nothing to sig_hash (core/sig_hash_builder.py's _key_allowed() just finds no match).",
                })

        for field in required:
            if not _field_covered(field, observed, prefixes):
                rows.append({
                    "policy_type": "sig", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "required_items",
                    "severity": "error", "issue": "declared_field_never_observed",
                    "detail": "listed in required_items but never appears as an item_key in the real corpus for this domain -- every record in this domain will fail minima.block_if_any_required_not_ok (build_sig_hash_from_policy: required_not_ok always true for this field) if that gate is honored at production time.",
                })

        shape_gate_fields = _shape_gate_fields(pol.get("shape_gating") or {})
        for field in sorted(shape_gate_fields):
            if not _field_covered(field, observed, prefixes):
                rows.append({
                    "policy_type": "sig", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "shape_gating",
                    "severity": "warning", "issue": "declared_field_never_observed",
                    "detail": "referenced by shape_gating (discriminator_key or a shape's additional_required/additional_optional) but never appears as an item_key in the real corpus for this domain. Note: core/sig_hash_builder.py does not consume shape_gating today, so this has no production effect yet -- but the same drift would matter the moment it does.",
                })
    return rows


def validate_join_policy(
    policy: Dict[str, Any], observed_by_domain: Dict[str, Set[str]], domains_allow: Set[str], policy_file: str
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    domains = policy.get("domains") if isinstance(policy.get("domains"), dict) else {}
    for domain, pol in sorted(domains.items()):
        if domains_allow and domain not in domains_allow:
            continue
        if not isinstance(pol, dict):
            continue
        observed = observed_by_domain.get(domain, set())
        if not observed:
            rows.append({
                "policy_type": "join", "policy_file": policy_file, "domain": domain, "field": "", "declared_in": "",
                "severity": "warning", "issue": "no_observed_data_for_domain",
                "detail": "domain has no observed item_key data at all -- cannot validate coverage (empty/missing export for this domain, or domain name mismatch).",
            })
            continue

        # No allowed_items/allowed_item_prefixes concept in the join-key schema --
        # every declared field is checked directly against observed item_keys.
        required = [f for f in (pol.get("required_items") or []) if isinstance(f, str)]
        optional = [f for f in (pol.get("optional_items") or []) if isinstance(f, str)]
        excluded = [f for f in (pol.get("explicitly_excluded_items") or []) if isinstance(f, str)]

        for field in required:
            if field not in observed:
                rows.append({
                    "policy_type": "join", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "required_items",
                    "severity": "error", "issue": "declared_field_never_observed",
                    "detail": "listed in required_items but never appears as an item_key in the real corpus for this domain -- every record in this domain will report missing_required (core/join_key_builder.py: field not in kqv) if this policy is applied at production time.",
                })

        for field in optional:
            if field not in observed:
                rows.append({
                    "policy_type": "join", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "optional_items",
                    "severity": "warning", "issue": "declared_field_never_observed",
                    "detail": "listed in optional_items but never appears as an item_key in the real corpus for this domain -- silently contributes nothing to the join key (never found in kqv, so never emitted).",
                })

        for field in excluded:
            if field not in observed:
                rows.append({
                    "policy_type": "join", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "explicitly_excluded_items",
                    "severity": "info", "issue": "declared_field_never_observed",
                    "detail": "listed in explicitly_excluded_items but never appears as an item_key in the real corpus for this domain -- may be a stale exclusion (field was removed from extraction) rather than an active guard against something real; usually harmless but worth a glance.",
                })

        shape_gate_fields = _shape_gate_fields(pol.get("shape_gating") or {})
        for field in sorted(shape_gate_fields):
            if field not in observed:
                rows.append({
                    "policy_type": "join", "policy_file": policy_file, "domain": domain, "field": field, "declared_in": "shape_gating",
                    "severity": "warning", "issue": "declared_field_never_observed",
                    "detail": "referenced by shape_gating (discriminator_key or a shape's additional_required/additional_optional) but never appears as an item_key in the real corpus for this domain -- core/join_key_builder.py DOES consume this at production time, so an unobserved discriminator_key means shape routing silently never fires (falls through to default_shape_behavior for every record).",
                })
    return rows


_NAME_KEY_POLICY_MARKERS = ("name_key", "name-key")


def _reject_if_name_key_policy(path_str: str, flag_name: str) -> None:
    """Hard stop, not a warning: domain_name_key_policies.json's declared
    fields (e.g. arrowhead.name, dim_type.name) are synthesized in memory,
    per record, immediately before the name-key join_hash is computed --
    domains/arrowheads.py's extract() builds `name_key_items = identity_items
    + [make_identity_item("arrowhead.name", ...)]` as a LOCAL, throwaway
    widened list, used for that one call only. That field is never written to
    identity_items.csv or any shard. This validator only ever scans the
    persisted items table, so pointed at a name-key policy it will report
    every single declared field as "never observed" unconditionally -- not
    because the policy is wrong, but because it's checking the wrong data
    source for that file by construction. Confirmed against this repo's own
    policies/domain_name_key_policies.json + domains/arrowheads.py this
    session -- this is a scope mismatch, not something a smarter check can
    fix, so it's rejected outright rather than left to produce guaranteed
    false positives.
    """
    name = Path(path_str).name.lower()
    if any(marker in name for marker in _NAME_KEY_POLICY_MARKERS):
        raise SystemExit(
            f"ERROR: {flag_name} points at what looks like a name-key policy ({path_str}). "
            "This validator cannot check domain_name_key_policies.json: its declared fields "
            "(e.g. arrowhead.name, dim_type.name) are synthesized in memory per record, "
            "immediately before the name-key join_hash is computed, and are never written "
            "to identity_items.csv or any shard -- this tool only scans the persisted items "
            "table, so it would report every single field as unobserved regardless of "
            "whether the policy is actually correct. Point --sig-policy-json/--join-policy-json "
            "at domain_sig_hash_policies.json / domain_join_key_policies.json instead."
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase0-dir", required=True, help="Directory containing records.csv and identity_items.csv (or a root that resolves to one -- see _resolve_phase0_dir).")
    ap.add_argument("--sig-policy-json", default=None, help="Path to domain_sig_hash_policies.json. Omit to skip sig validation.")
    ap.add_argument("--join-policy-json", default=None, help="Path to domain_join_key_policies.json. Omit to skip join validation. NOT valid for domain_name_key_policies.json -- see module docstring.")
    ap.add_argument("--domains", default="", help="Optional comma-separated domain allow-list.")
    ap.add_argument("--out-csv", default=None, help="Output CSV path. Default: <phase0-dir's parent>/diagnostics/policy_field_coverage.csv (domain-suffixed when --domains is set).")
    args = ap.parse_args()

    if not args.sig_policy_json and not args.join_policy_json:
        print("ERROR: pass at least one of --sig-policy-json / --join-policy-json.")
        return 2

    if args.sig_policy_json:
        _reject_if_name_key_policy(args.sig_policy_json, "--sig-policy-json")
    if args.join_policy_json:
        _reject_if_name_key_policy(args.join_policy_json, "--join-policy-json")

    phase0 = _resolve_phase0_dir(Path(args.phase0_dir))
    if not (phase0 / "records.csv").exists():
        print(f"ERROR: records.csv not found under phase0 dir: {phase0}")
        return 2

    domains_allow = {d.strip() for d in args.domains.split(",") if d.strip()}

    all_rows: List[Dict[str, str]] = []

    read_cache: Dict[Path, Dict[str, Set[str]]] = {}

    if args.sig_policy_json:
        observed_sig = _observed_item_keys_by_domain(phase0, "sig", domains_allow, read_cache)
        sig_policy = json.loads(Path(args.sig_policy_json).read_text(encoding="utf-8"))
        all_rows.extend(validate_sig_policy(sig_policy, observed_sig, domains_allow, str(Path(args.sig_policy_json))))

    if args.join_policy_json:
        observed_join = _observed_item_keys_by_domain(phase0, "join", domains_allow, read_cache)
        join_policy = json.loads(Path(args.join_policy_json).read_text(encoding="utf-8"))
        all_rows.extend(validate_join_policy(join_policy, observed_join, domains_allow, str(Path(args.join_policy_json))))

    errors = [r for r in all_rows if r["severity"] == "error"]
    warnings = [r for r in all_rows if r["severity"] == "warning"]
    infos = [r for r in all_rows if r["severity"] == "info"]

    if not all_rows:
        print("[validate] No issues found -- every declared field was observed in the real corpus.")
    else:
        for r in all_rows:
            label = "ERROR" if r["severity"] == "error" else ("WARNING" if r["severity"] == "warning" else "info")
            field_part = f" field={r['field']} declared_in={r['declared_in']}" if r["field"] else ""
            print(f"[validate] {label} policy_type={r['policy_type']} policy_file={r['policy_file']} domain={r['domain']}{field_part} -- {r['detail']}")
        print(f"\n[validate] {len(errors)} error(s), {len(warnings)} warning(s), {len(infos)} info -- see CSV for full detail.")

    domain_suffix = _diagnostics_domain_suffix(domains_allow, [])
    out_csv = Path(args.out_csv) if args.out_csv else (phase0.parent / "diagnostics" / f"policy_field_coverage{domain_suffix}.csv")
    fields = ["policy_type", "policy_file", "domain", "field", "declared_in", "severity", "issue", "detail"]
    _write_csv(out_csv, fields, all_rows)
    print(f"\n[validate] Wrote {out_csv}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
