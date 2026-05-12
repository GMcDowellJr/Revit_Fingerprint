# -*- coding: utf-8 -*-
"""Sig-hash policy loader.

The sig_hash policy is the authoritative post-extraction selector for record.v2
identity hashing.  It mirrors the join-key policy pattern: extractors emit
canonical evidence; a deterministic builder computes hashes from a pinned policy.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


def _is_list_of_str(x: Any) -> bool:
    return isinstance(x, list) and all(isinstance(s, str) for s in x)


def validate_domain_sig_hash_policy(domain_name: str, pol: Dict[str, Any]) -> None:
    if not isinstance(pol, dict):
        raise ValueError("sig-hash policy for '%s' must be an object" % domain_name)
    if not isinstance(pol.get("sig_hash_schema"), str):
        raise ValueError("sig-hash policy '%s' missing sig_hash_schema" % domain_name)
    if not isinstance(pol.get("hash_alg"), str):
        raise ValueError("sig-hash policy '%s' missing hash_alg" % domain_name)
    if not _is_list_of_str(pol.get("allowed_items")):
        raise ValueError("sig-hash policy '%s' allowed_items must be list[str]" % domain_name)
    prefixes = pol.get("allowed_item_prefixes", [])
    if prefixes is not None and not _is_list_of_str(prefixes):
        raise ValueError("sig-hash policy '%s' allowed_item_prefixes must be list[str]" % domain_name)
    if not _is_list_of_str(pol.get("required_items")):
        raise ValueError("sig-hash policy '%s' required_items must be list[str]" % domain_name)
    minima = pol.get("minima", {})
    if minima is not None and not isinstance(minima, dict):
        raise ValueError("sig-hash policy '%s' minima must be object" % domain_name)


def load_sig_hash_policies(policy_path: str) -> Dict[str, Any]:
    with open(policy_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("sig-hash policy file must be a JSON object")
    domains = data.get("domains")
    if not isinstance(domains, dict):
        raise ValueError("sig-hash policy file must contain a top-level 'domains' object")
    for domain_name, pol in domains.items():
        if not isinstance(domain_name, str):
            raise ValueError("sig-hash policy domain keys must be strings")
        validate_domain_sig_hash_policy(domain_name, pol)
    return data


def get_domain_sig_hash_policy(policies: Any, domain_name: str) -> Optional[Dict[str, Any]]:
    if not isinstance(policies, dict):
        return None
    domains = policies.get("domains")
    if not isinstance(domains, dict):
        return None
    pol = domains.get(domain_name)
    return pol if isinstance(pol, dict) else None
