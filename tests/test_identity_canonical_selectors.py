# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


def test_identity_join_key_uses_required_plus_gated_required_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "identity")

    # Canonical evidence superset (identity_basis.items) includes required + optional keys.
    identity_items = [
        make_identity_item("identity.is_workshared", "true", ITEM_Q_OK),
        make_identity_item("identity.revit_version_number", "2025", ITEM_Q_OK),
        make_identity_item("identity.revit_build", "25.0.0.123", ITEM_Q_OK),
        make_identity_item("identity.revit_version_name", "Autodesk Revit 2025", ITEM_Q_OK),
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
    # Base required + discriminator-gated required only.
    assert join_key["keys_used"] == ["identity.is_workshared", "identity.revit_version_number"]

    # Back-compat: join_key.items is retained but contains hashed items only.
    assert [it["k"] for it in join_key["items"]] == [
        "identity.is_workshared",
        "identity.revit_version_number",
    ]

    join_preimage = serialize_identity_items([it for it in identity_items if it["k"] in join_key["keys_used"]])
    assert join_key["join_hash"] == make_hash(join_preimage)

    # Semantic signature basis is intentionally larger than the join basis.
    sig_keys = ["identity.is_workshared", "identity.revit_version_number", "identity.revit_build"]
    sig_hash = make_hash(serialize_identity_items([it for it in identity_items if it["k"] in sig_keys]))
    assert sig_hash != join_key["join_hash"]
