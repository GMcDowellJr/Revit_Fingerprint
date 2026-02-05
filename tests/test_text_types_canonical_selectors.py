# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items
from domains.text_types import TEXT_TYPE_SEMANTIC_KEYS


def _text_types_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "text_types")


def test_text_types_canonical_evidence_selectors_and_hashing():
    # Canonical evidence superset (identity_basis.items) includes semantic and optional keys.
    canonical_items = [
        make_identity_item("text_type.name", "Notes-Medium", ITEM_Q_OK),
        make_identity_item("text_type.font", "Arial", ITEM_Q_OK),
        make_identity_item("text_type.size_in", "0.125000", ITEM_Q_OK),
        make_identity_item("text_type.width_factor", "1.000000", ITEM_Q_OK),
        make_identity_item("text_type.background", "0", ITEM_Q_OK),
        make_identity_item("text_type.line_weight", "1", ITEM_Q_OK),
        make_identity_item("text_type.color_rgb", "0-0-0", ITEM_Q_OK),
        make_identity_item("text_type.show_border", "false", ITEM_Q_OK),
        make_identity_item("text_type.leader_border_offset_in", "0.031250", ITEM_Q_OK),
        make_identity_item("text_type.tab_size_in", "0.500000", ITEM_Q_OK),
        make_identity_item("text_type.bold", "false", ITEM_Q_OK),
        make_identity_item("text_type.italic", "false", ITEM_Q_OK),
        make_identity_item("text_type.underline", "false", ITEM_Q_OK),
        make_identity_item("text_type.leader_arrowhead_sig_hash", "a" * 32, ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_text_types_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "text_type.bold",
        "text_type.color_rgb",
        "text_type.font",
        "text_type.italic",
        "text_type.size_in",
        "text_type.underline",
        "text_type.width_factor",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(TEXT_TYPE_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Semantic basis includes identity-bearing name/background and should differ from join basis.
    assert sig_hash != join_key["join_hash"]
