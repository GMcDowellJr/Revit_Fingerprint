# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items
from domains.units import UNITS_SEMANTIC_KEYS


def _units_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "units")


def test_units_join_selectors_and_sig_basis_are_distinct():
    # identity_basis.items is the canonical evidence superset for units.
    canonical_items = [
        make_identity_item("units.spec", "length", ITEM_Q_OK),
        make_identity_item("units.unit_type_id", "autodesk.unit.unit:feetFractionalInches-1.0.1", ITEM_Q_OK),
        make_identity_item("units.rounding_method", "nearest", ITEM_Q_OK),
        make_identity_item("units.accuracy", "0.125000000", ITEM_Q_OK),
        make_identity_item("units.symbol_type_id", "autodesk.unit.symbol:ft-1.0.1", ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_units_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "units.rounding_method",
        "units.spec",
        "units.unit_type_id",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(UNITS_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Pilot requirement: semantic hash should remain independent from join hash.
    assert sig_hash != join_key["join_hash"]
