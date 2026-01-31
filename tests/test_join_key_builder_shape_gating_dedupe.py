# -*- coding: utf-8 -*-

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

from core.record_v2 import ITEM_Q_OK, make_identity_item
from core.join_key_builder import build_join_key_from_policy


def _policy_with_overlap():
    return {
        "join_key_schema": "test.join_key.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "required_items": ["shape", "common", "dup"],
        "optional_items": ["opt_a", "dup"],
        "explicitly_excluded_items": [],
        "shape_gating": {
            "discriminator_key": "shape",
            "shape_requirements": {
                "A": {
                    "additional_required": ["dup", "shape_req"],
                    "additional_optional": ["opt_a"],
                }
            },
            "default_shape_behavior": "common_only",
        },
    }


def test_join_key_builder_dedupes_required_and_optional():
    pol = _policy_with_overlap()
    items = [
        make_identity_item("shape", "A", ITEM_Q_OK),
        make_identity_item("common", "c", ITEM_Q_OK),
        make_identity_item("dup", "d", ITEM_Q_OK),
        make_identity_item("shape_req", "s", ITEM_Q_OK),
        make_identity_item("opt_a", "o", ITEM_Q_OK),
    ]

    jk, missing = build_join_key_from_policy(domain_policy=pol, identity_items=items)
    assert missing == []

    keys = [it["k"] for it in jk["items"]]
    assert keys.count("dup") == 1

    req_order = ["shape", "common", "dup", "shape_req"]
    assert keys[: len(req_order)] == req_order

    opt_order = ["opt_a"]
    assert keys[len(req_order) :] == opt_order

    assert jk["shape_gating"]["shape_value"] == "A"
    assert jk["shape_gating"]["shape_matched"] is True
