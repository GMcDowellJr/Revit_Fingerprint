# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


def test_view_category_overrides_join_and_sig_selectors_are_distinct():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "view_category_overrides")

    # Canonical evidence superset (identity_basis.items in exporter).
    identity_items = [
        make_identity_item("vco.baseline_category_path", "Walls|self", ITEM_Q_OK),
        make_identity_item("vco.baseline_sig_hash", "0123456789abcdef0123456789abcdef", ITEM_Q_OK),
        make_identity_item("vco.override_properties_hash", "fedcba9876543210fedcba9876543210", ITEM_Q_OK),
        # Optional forensic evidence remains in the canonical superset but is not hashed for join.
        make_identity_item("vco.projection.line_weight", "5", ITEM_Q_OK),
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
    assert join_key["keys_used"] == sorted([
        "vco.baseline_category_path",
        "vco.baseline_sig_hash",
        "vco.override_properties_hash",
    ])

    # Back-compat: join_key.items contains only hashed items.
    assert [it["k"] for it in join_key["items"]] == join_key["keys_used"]

    join_preimage = serialize_identity_items([it for it in identity_items if it["k"] in set(join_key["keys_used"])])
    assert join_key["join_hash"] == make_hash(join_preimage)

    semantic_keys = sorted({it["k"] for it in identity_items})
    sig_preimage = serialize_identity_items([it for it in identity_items if it["k"] in set(semantic_keys)])
    sig_hash = make_hash(sig_preimage)
    assert sig_hash != join_key["join_hash"]
