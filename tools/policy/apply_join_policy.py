#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List

try:
    from tools.join_key_derivation_phase05 import md5_utf8_join_pipe, serialize_identity_items
    from tools.join_key_discovery.eval import build_candidate_join_key_with_details, build_identity_index, normalize_policy_block
except ModuleNotFoundError:
    from join_key_derivation_phase05 import md5_utf8_join_pipe, serialize_identity_items
    from join_key_discovery.eval import build_candidate_join_key_with_details, build_identity_index, normalize_policy_block


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _write_csv(path: Path, fields: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply stage (T2): apply discovered/provided policy and overwrite flatten output phase0_records.csv")
    ap.add_argument("--phase0-dir", default="out/current/flatten", help="Flatten output directory to update in place.")
    ap.add_argument("--join-policy", default="out/current/policies/domain_join_key_policies.json", help="Join policy JSON produced by discover stage.")
    args = ap.parse_args()

    phase0_dir = Path(args.phase0_dir)

    policy_path = Path(args.join_policy).resolve()
    print(f"[apply] using flatten dir: {phase0_dir}")
    print(f"[apply] using policy: {policy_path}")
    if not policy_path.exists():
        raise SystemExit(f"Join policy not found: {policy_path}")
    records_path = phase0_dir / "phase0_records.csv"
    records = _read_csv(records_path)
    items = _read_csv(phase0_dir / "phase0_identity_items.csv")
    identity_index = build_identity_index(items)

    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    dom_policies = policy.get("domains") if isinstance(policy, dict) else {}
    if not isinstance(dom_policies, dict):
        raise SystemExit("Invalid policy format: missing domains")

    failures: List[Dict[str, str]] = []

    for r in records:
        domain = (r.get("domain") or "").strip()
        record_pk = (r.get("record_pk") or "").strip()
        p = dom_policies.get(domain)
        if not isinstance(p, dict):
            r["join_hash"] = r.get("join_hash", "")
            r["join_key_schema"] = "sig_hash_as_join_key.v1"
            r["join_key_status"] = "missing_policy"
            r["join_key_policy_id"] = ""
            r["join_key_policy_version"] = ""
            failures.append({"domain": domain, "file_id": r.get("file_id", ""), "record_pk": record_pk, "reason": "missing_policy", "missing_keys": "", "effective_required_keys": "", "discriminator_key": "", "discriminator_value": "", "policy_id": "", "policy_version": ""})
            continue

        normalized = normalize_policy_block(p)
        selected_fields = normalized["selected_fields"]
        required_fields = normalized["required_fields"]
        status, selected_items, reason, details = build_candidate_join_key_with_details(
            identity_index,
            record_pk,
            selected_fields,
            {
                "required_fields": required_fields,
                "discriminator_key": normalized["gates"].get("discriminator_key"),
                "shape_requirements": normalized["gates"].get("shape_requirements"),
                "default_shape_behavior": normalized["gates"].get("default_shape_behavior"),
            },
        )
        policy_id = str(p.get("policy_id") or f"{domain}.join_key.v21")
        policy_version = str(p.get("policy_version") or "1")
        r["join_key_policy_id"] = policy_id
        r["join_key_policy_version"] = policy_version
        r["join_key_schema"] = f"policy.{policy_id}.v{policy_version}"

        if status != "ok":
            r["join_hash"] = ""
            r["join_key_status"] = status
            failures.append({
                "domain": domain,
                "file_id": r.get("file_id", ""),
                "record_pk": record_pk,
                "reason": status,
                "missing_keys": reason,
                "effective_required_keys": "|".join(details.get("effective_required_fields", [])),
                "discriminator_key": str(details.get("discriminator_key") or ""),
                "discriminator_value": str(details.get("discriminator_value") or ""),
                "policy_id": policy_id,
                "policy_version": policy_version,
            })
            continue

        preimage = serialize_identity_items(selected_items)
        r["join_hash"] = md5_utf8_join_pipe(preimage)
        r["join_key_status"] = "ok"

    fieldnames = list(records[0].keys()) if records else []
    for required in ["join_key_status", "join_key_policy_id", "join_key_policy_version"]:
        if required not in fieldnames:
            fieldnames.append(required)
    _write_csv(records_path, fieldnames, sorted(records, key=lambda x: (x.get("export_run_id", ""), x.get("domain", ""), x.get("record_pk", ""))))

    _write_csv(
        phase0_dir.parent / "diagnostics" / "join_policy_failures.csv",
        ["domain", "file_id", "record_pk", "reason", "missing_keys", "effective_required_keys", "discriminator_key", "discriminator_value", "policy_id", "policy_version"],
        sorted(failures, key=lambda x: (x["domain"], x["file_id"], x["record_pk"])),
    )


if __name__ == "__main__":
    main()
