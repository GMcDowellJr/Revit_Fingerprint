# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


def _object_styles_model_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "object_styles_model")


def test_object_styles_model_canonical_evidence_selectors_and_hashing():
    # Canonical evidence superset (identity_basis.items) includes required + optional keys.
    # New flat policy for object_styles_model: no shape_gating, no pattern_ref.kind.
    canonical_items = [
        make_identity_item("obj_style.row_key", "Walls|self", ITEM_Q_OK),
        make_identity_item("obj_style.weight.projection", "2", ITEM_Q_OK),
        make_identity_item("obj_style.weight.cut", "3", ITEM_Q_OK),
        make_identity_item("obj_style.color.rgb", "10-20-30", ITEM_Q_OK),
        make_identity_item("obj_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
        make_identity_item("obj_style.material_sig_hash", "b" * 32, ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_object_styles_model_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    # Policy requires: row_key, weight.projection, weight.cut, color.rgb, pattern_ref.sig_hash
    # material_sig_hash is optional and should NOT appear in keys_used when include_optional_items=False
    assert sorted(join_key["keys_used"]) == sorted([
        "obj_style.row_key",
        "obj_style.weight.projection",
        "obj_style.weight.cut",
        "obj_style.color.rgb",
        "obj_style.pattern_ref.sig_hash",
    ])
    assert sorted([it["k"] for it in join_key["items"]]) == sorted(join_key["keys_used"])

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    # Semantic signature (full canonical basis including optional) differs from join hash.
    sig_hash = make_hash(serialize_identity_items(canonical_items))
    assert sig_hash != join_key["join_hash"]
