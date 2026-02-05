# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items
from domains.line_styles import LINE_STYLE_SEMANTIC_KEYS


def _line_styles_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "line_styles")


def test_line_styles_canonical_evidence_selectors_and_hashing():
    # Canonical evidence superset (identity_basis.items) includes join + semantic + cosmetic evidence.
    canonical_items = [
        make_identity_item("line_style.path", "Lines|Thin Lines", ITEM_Q_OK),
        make_identity_item("line_style.weight.projection", "1", ITEM_Q_OK),
        make_identity_item("line_style.color.rgb", "255-0-0", ITEM_Q_OK),
        make_identity_item("line_style.pattern_ref.kind", "ref", ITEM_Q_OK),
        make_identity_item("line_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_line_styles_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "line_style.color.rgb",
        "line_style.pattern_ref.sig_hash",
        "line_style.weight.projection",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(LINE_STYLE_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Semantic basis intentionally differs from policy join basis for this pilot.
    assert sig_hash != join_key["join_hash"]
