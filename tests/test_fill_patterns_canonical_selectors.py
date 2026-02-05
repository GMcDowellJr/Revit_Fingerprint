# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


def test_fill_patterns_join_key_uses_policy_required_keys_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "fill_patterns")

    # Canonical evidence superset (identity_basis.items in exporter) includes required + optional keys.
    identity_items = [
        make_identity_item("fill_pattern.grids_def_hash", "0123456789abcdef0123456789abcdef", ITEM_Q_OK),
        make_identity_item("fill_pattern.is_solid", "false", ITEM_Q_OK),
        make_identity_item("fill_pattern.target_id", "1", ITEM_Q_OK),
        make_identity_item("fill_pattern.grid_count", "2", ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == ["fill_pattern.grids_def_hash"]

    # Back-compat: join_key.items is retained but contains hashed items only.
    assert [it["k"] for it in join_key["items"]] == ["fill_pattern.grids_def_hash"]

    join_preimage = serialize_identity_items([it for it in identity_items if it["k"] in join_key["keys_used"]])
    assert join_key["join_hash"] == make_hash(join_preimage)

    # Semantic signature (larger basis) is intentionally distinct from join hash.
    sig_hash = make_hash(serialize_identity_items(identity_items))
    assert sig_hash != join_key["join_hash"]
