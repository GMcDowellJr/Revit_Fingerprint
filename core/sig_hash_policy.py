# -*- coding: utf-8 -*-
"""Sig-hash policy loader.

The sig_hash policy is the authoritative post-extraction selector for record.v2
identity hashing.  It mirrors the join-key policy pattern: extractors emit
canonical evidence; a deterministic builder computes hashes from a pinned policy.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence


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


def resolve_sig_hash_keys(
    policies: Any,
    domain_name: str,
    candidate_keys: Sequence[str],
    fallback: Sequence[str],
) -> List[str]:
    """Resolve the sig_hash preimage key set for a domain at extraction time.

    Extractors that hand-classify captured fields into a narrower "semantic"
    bucket for sig_hash (as opposed to the full field set exported to
    identity_basis.items) previously hardcoded that key set as a Python
    literal, independent of policies/domain_sig_hash_policies.json's
    allowed_items for the same domain -- two independently-maintained copies
    of "what counts as behavioral" that could silently drift apart (see
    DECISIONS.md D-039, where they did). This resolves the key set from
    ctx["sig_hash_policies"] (populated by runner/extraction_context.py from
    the same JSON file core/sig_hash_builder.py's post-stage recompute reads)
    when available, so the inline extractor and the post-stage recompute
    consume a single source of truth going forward.

    Args:
        candidate_keys: the keys actually present on the record about to be
            hashed (e.g. [it["k"] for it in identity_items]). Needed to
            resolve `allowed_item_prefixes` into concrete key matches --
            mirrors core/sig_hash_builder.py's `_key_allowed()`, which checks
            both an exact `allowed_items` membership test and a prefix test
            against each item's own key. Without this, a policy that relies
            on `allowed_item_prefixes` (e.g. view_filter_definitions'
            "vf.rule[") would silently lose prefix-matched keys the moment a
            domain adopted this resolver, recreating the exact inline-vs-
            post-stage drift this function exists to eliminate.

    Falls back to the caller's hardcoded default when the policy isn't
    present in ctx (e.g. a unit test that builds a minimal ctx by hand, or a
    domain not yet migrated to this pattern). A policy-validated empty
    `allowed_items` list is a legitimate configuration (e.g. a domain relying
    entirely on `allowed_item_prefixes`) and is honored, not treated as
    absence -- only a missing/malformed policy triggers the fallback.

    The fallback must be kept in sync with the policy's allowed_items for
    that domain -- this function does not detect a stale fallback;
    tests/test_sig_hash_join_key_policy_consistency.py and each domain's own
    inline-vs-policy regression test are the guard for that.
    """
    pol = get_domain_sig_hash_policy(policies, domain_name)
    if isinstance(pol, dict):
        allowed = pol.get("allowed_items")
        prefixes = pol.get("allowed_item_prefixes")
        if prefixes is None:
            prefixes = []
        if _is_list_of_str(allowed) and _is_list_of_str(prefixes):
            resolved = set(allowed)
            if prefixes:
                for k in candidate_keys:
                    if isinstance(k, str) and any(isinstance(p, str) and p and k.startswith(p) for p in prefixes):
                        resolved.add(k)
            return sorted(resolved)
    return list(fallback)
