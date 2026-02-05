# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


OBJECT_STYLE_SEMANTIC_KEYS = sorted([
    "obj_style.color.rgb",
    "obj_style.pattern_ref.kind",
    "obj_style.pattern_ref.sig_hash",
    "obj_style.weight.cut",
    "obj_style.weight.projection",
])


def _object_styles_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "object_styles")


def test_object_styles_canonical_evidence_selectors_and_hashing():
    # Canonical evidence superset (identity_basis.items) includes join + semantic + cosmetic evidence.
    canonical_items = [
        make_identity_item("obj_style.row_key", "Walls|self", ITEM_Q_OK),
        make_identity_item("obj_style.weight.projection", "2", ITEM_Q_OK),
        make_identity_item("obj_style.weight.cut", "3", ITEM_Q_OK),
        make_identity_item("obj_style.color.rgb", "10-20-30", ITEM_Q_OK),
        make_identity_item("obj_style.pattern_ref.kind", "ref", ITEM_Q_OK),
        make_identity_item("obj_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_object_styles_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "obj_style.pattern_ref.sig_hash",
        "obj_style.row_key",
        "obj_style.weight.projection",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(OBJECT_STYLE_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Semantic basis intentionally differs from policy join basis for this pilot.
    assert sig_hash != join_key["join_hash"]
