# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items


def test_fill_patterns_drafting_join_key_uses_policy_required_keys_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "fill_patterns_drafting")

    # Canonical evidence superset (identity_basis.items in exporter) includes required + optional keys.
    identity_items = [
        make_identity_item("fill_pattern.grids_def_hash", "0123456789abcdef0123456789abcdef", ITEM_Q_OK),
        make_identity_item("fill_pattern.target", "0", ITEM_Q_OK),
        make_identity_item("fill_pattern.grid_count", "2", ITEM_Q_OK),
        make_identity_item("fill_pattern.is_solid", "false", ITEM_Q_OK),
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
        "fill_pattern.target",
        "fill_pattern.grid_count",
        "fill_pattern.grids_def_hash",
    ])

    join_items = [it for it in identity_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)

    # Semantic signature (larger basis) is intentionally distinct from join hash.
    sig_hash = make_hash(serialize_identity_items(identity_items))
    assert sig_hash != join_key["join_hash"]


def test_fill_patterns_model_join_key_uses_policy_required_keys_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "fill_patterns_model")

    identity_items = [
        make_identity_item("fill_pattern.grids_def_hash", "0123456789abcdef0123456789abcdef", ITEM_Q_OK),
        make_identity_item("fill_pattern.target", "1", ITEM_Q_OK),
        make_identity_item("fill_pattern.grid_count", "3", ITEM_Q_OK),
        make_identity_item("fill_pattern.is_solid", "false", ITEM_Q_OK),
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
    assert set(join_key["keys_used"]) == {
        "fill_pattern.target",
        "fill_pattern.grid_count",
        "fill_pattern.grids_def_hash",
    }


def test_fill_pattern_ctx_contract_exports_specials_and_preserves_uid_map():
    from domains.fill_patterns import _export_fill_pattern_ctx

    ctx = {
        "fill_pattern_uid_to_hash": {"u-existing": "h-existing"},
        "fill_pattern_id_to_value": {"10": "h-10"},
        "fill_pattern_special_values": {"legacy": "kept"},
    }
    _export_fill_pattern_ctx(
        ctx,
        uid_to_hash_v2={"u-new": "h-new"},
        id_to_value={"20": "h-20", "99": "<Solid>"},
    )

    assert ctx["fill_pattern_uid_to_hash"]["u-existing"] == "h-existing"
    assert ctx["fill_pattern_uid_to_hash"]["u-new"] == "h-new"
    assert ctx["fill_pattern_id_to_value"]["10"] == "h-10"
    assert ctx["fill_pattern_id_to_value"]["20"] == "h-20"
    assert ctx["fill_pattern_special_values"]["no_pattern"] == "<No Pattern>"
    assert ctx["fill_pattern_special_values"]["solid"] == "<Solid>"
    assert ctx["fill_pattern_special_values"]["legacy"] == "kept"
