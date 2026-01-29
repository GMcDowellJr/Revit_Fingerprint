# -*- coding: utf-8 -*-
"""Join-key policy loader (explicit ctx injection only).

This module is intentionally small and deterministic.
"""

import json


def _is_list_of_str(x):
    return isinstance(x, list) and all(isinstance(s, str) for s in x)


def load_join_key_policies(policy_path):
    """Load and minimally validate the join-key policy JSON.

    Returns: dict (parsed JSON)
    Raises: Exception on IO/parse/shape errors.
    """
    with open(policy_path, "r") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("join-key policy file must be a JSON object")

    domains = data.get("domains")
    if not isinstance(domains, dict):
        raise ValueError("join-key policy file must contain a top-level 'domains' object")

    for domain_name, pol in domains.items():
        if not isinstance(domain_name, str):
            raise ValueError("join-key policy domain keys must be strings")
        if not isinstance(pol, dict):
            raise ValueError("join-key policy for '%s' must be an object" % domain_name)

        # Required fields
        if not isinstance(pol.get("join_key_schema"), str):
            raise ValueError("join-key policy '%s' missing join_key_schema" % domain_name)
        if not isinstance(pol.get("hash_alg"), str):
            raise ValueError("join-key policy '%s' missing hash_alg" % domain_name)
        if not _is_list_of_str(pol.get("required_items")):
            raise ValueError("join-key policy '%s' required_items must be list[str]" % domain_name)
        if not _is_list_of_str(pol.get("optional_items")):
            raise ValueError("join-key policy '%s' optional_items must be list[str]" % domain_name)
        if not _is_list_of_str(pol.get("explicitly_excluded_items")):
            raise ValueError("join-key policy '%s' explicitly_excluded_items must be list[str]" % domain_name)

        # Optional fields
        notes = pol.get("notes", [])
        if notes is not None and not _is_list_of_str(notes):
            raise ValueError("join-key policy '%s' notes must be list[str]" % domain_name)

    return data


def get_domain_join_key_policy(policies, domain_name):
    """Return the per-domain join-key policy or None."""
    if not isinstance(policies, dict):
        return None
    domains = policies.get("domains")
    if not isinstance(domains, dict):
        return None
    pol = domains.get(domain_name)
    return pol if isinstance(pol, dict) else None
