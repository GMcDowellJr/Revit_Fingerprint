# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import serialize_identity_items


def test_phase_filters_selectors_and_hashing_use_policy_required_presentation_ids():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    policy = get_domain_join_key_policy(policies, "phase_filters")

    identity_items = sorted(
        [
            {"k": "phase_filter.new.presentation_id", "q": "ok", "v": 2},
            {"k": "phase_filter.existing.presentation_id", "q": "ok", "v": 1},
            {"k": "phase_filter.demolished.presentation_id", "q": "ok", "v": 0},
            {"k": "phase_filter.temporary.presentation_id", "q": "ok", "v": 3},
            {"k": "phase_filter.name", "q": "ok", "v": "Show Complete"},
        ],
        key=lambda it: it["k"],
    )

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=True,
        emit_selectors=True,
    )

    assert missing == []
    if policy.get("join_key_schema") == "phase_filters.join_key.v2":
        expected_keys = sorted([
            "phase_filter.new.presentation_id",
            "phase_filter.existing.presentation_id",
            "phase_filter.demolished.presentation_id",
            "phase_filter.temporary.presentation_id",
        ])
        assert join_key["keys_used"] == expected_keys
        assert "phase_filter.name" not in join_key["keys_used"]
    else:
        assert join_key["keys_used"] == ["phase_filter.name"]

    join_items = [it for it in identity_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)
