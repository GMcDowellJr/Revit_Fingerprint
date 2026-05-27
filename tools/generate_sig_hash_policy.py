#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


def build_policy(registry: Dict[str, Any]) -> Dict[str, Any]:
    domains = registry.get("domains") if isinstance(registry, dict) else {}
    if not isinstance(domains, dict):
        raise ValueError("registry missing domains object")
    out = {
        "version": "domain_sig_hash_policies.v1",
        "source_registry_version": registry.get("version"),
        "record_schema_version": registry.get("record_schema_version", "record.v2"),
        "identity_item_schema": registry.get("identity_item_schema", "identity_items.v1"),
        "domains": {},
    }
    for name, block in sorted(domains.items(), key=lambda kv: str(kv[0])):
        if not isinstance(block, dict):
            continue
        out["domains"][str(name)] = {
            "sig_hash_schema": block.get("sig_hash_schema") or ("%s.sig_hash.v1" % name),
            "hash_alg": "md5_utf8_join_pipe",
            "allowed_items": list(block.get("allowed_keys") or []),
            "allowed_item_prefixes": list(block.get("allowed_key_prefixes") or []),
            "required_items": list(block.get("required_keys") or []),
            "minima": dict(block.get("minima") or {}),
            "notes": [
                "Generated from contracts/domain_identity_keys_v2.json.",
                "sig_hash is computed post-extraction from canonical identity_basis.items."
            ],
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate policy-driven sig_hash selectors from governance registry")
    ap.add_argument("--registry", default="contracts/domain_identity_keys_v2.json")
    ap.add_argument("--out", default="policies/domain_sig_hash_policies.json")
    args = ap.parse_args()

    with Path(args.registry).open("r", encoding="utf-8") as f:
        registry = json.load(f)
    policy = build_policy(registry)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(policy, f, indent=2, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main()
