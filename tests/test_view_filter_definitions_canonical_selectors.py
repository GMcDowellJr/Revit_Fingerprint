# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.record_v2 import serialize_identity_items


def test_view_filter_definitions_join_hash_uses_policy_required_and_gated_keys_only():
    # Canonical evidence superset (identity_basis.items).
    canonical_items = sorted(
        [
            {"k": "vf.categories", "q": "ok", "v": "-2000011"},
            {"k": "vf.logic_root", "q": "ok", "v": "Linear"},
            {"k": "vf.rule_count", "q": "ok", "v": 3},
            {"k": "vf.rule[000].sig", "q": "ok", "v": "A"},
            {"k": "vf.rule[001].sig", "q": "ok", "v": "B"},
            {"k": "vf.rule[002].sig", "q": "ok", "v": "C"},
            {"k": "vf.def_hash", "q": "ok", "v": "0123456789abcdef0123456789abcdef"},
        ],
        key=lambda it: it["k"],
    )

    # Policy shape-gates vf.rule_count as additionally required for Linear.
    policy = {
        "join_key_schema": "view_filter_definitions.join_key.v2",
        "hash_alg": "md5_utf8_join_pipe",
        "required_items": ["vf.def_hash", "vf.logic_root"],
        "optional_items": ["vf.categories", "vf.rule_count"],
        "shape_gating": {
            "discriminator_key": "vf.logic_root",
            "shape_requirements": {
                "Linear": {
                    "additional_required": ["vf.rule_count"],
                    "additional_optional": [],
                }
            },
            "default_shape_behavior": "common_only",
        },
    }

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == ["vf.def_hash", "vf.logic_root", "vf.rule_count"]
    assert [it["k"] for it in join_key["items"]] == ["vf.def_hash", "vf.logic_root", "vf.rule_count"]

    # join_hash recomputes from canonical evidence + selector keys_used.
    join_items = [it for it in canonical_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)

    # Semantic basis is larger than join basis in this pilot.
    semantic_keys = sorted([it["k"] for it in canonical_items if it["k"] != "vf.def_hash"])
    semantic_items = [it for it in canonical_items if it["k"] in set(semantic_keys)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))
    assert sig_hash != join_key["join_hash"]
