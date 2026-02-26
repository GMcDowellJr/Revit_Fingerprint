# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import serialize_identity_items


def test_view_filter_definitions_join_hash_uses_policy_required_keys_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    policy = get_domain_join_key_policy(policies, "view_filter_definitions")

    canonical_items = sorted(
        [
            {"k": "vf.logic_root", "q": "ok", "v": "Linear"},
            {"k": "vf.rule_count", "q": "ok", "v": 3},
            {"k": "vf.categories", "q": "ok", "v": "-2000011"},
            {"k": "vf.def_hash", "q": "ok", "v": "0123456789abcdef0123456789abcdef"},
        ],
        key=lambda it: it["k"],
    )

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=True,
        emit_selectors=True,
    )

    assert missing == []
    if policy.get("join_key_schema") == "view_filter_definitions.join_key.v3":
        assert "vf.logic_root" in join_key["keys_used"]
        assert "vf.rule_count" in join_key["keys_used"]
        assert "vf.def_hash" not in join_key["keys_used"]
    else:
        assert join_key["keys_used"] == ["vf.def_hash"]

    join_items = [it for it in canonical_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)
