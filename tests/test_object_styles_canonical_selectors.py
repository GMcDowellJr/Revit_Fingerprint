# -*- coding: utf-8 -*-

import json

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK, build_record_v2, make_identity_item, serialize_identity_items
from validators.record_v2 import validate_record_v2


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


def _domain_identity_registry_v2():
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        return json.load(f)


def _area9_identity_items(*, is_subcategory):
    items = [
        make_identity_item("obj_style.row_key", "Walls|Wall Tags" if is_subcategory else "Walls|self", ITEM_Q_OK),
        make_identity_item("obj_style.weight.projection", "2", ITEM_Q_OK),
        make_identity_item("obj_style.weight.cut", "3", ITEM_Q_OK),
        make_identity_item("obj_style.color.rgb", "10-20-30", ITEM_Q_OK),
        make_identity_item("obj_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
        make_identity_item("obj_style.material_sig_hash", "b" * 32, ITEM_Q_OK),
        make_identity_item("obj_style.can_add_subcategory", "true", ITEM_Q_OK),
        make_identity_item("obj_style.has_material_quantities", "false", ITEM_Q_OK),
        make_identity_item("obj_style.is_cuttable", "false", ITEM_Q_OK),
        make_identity_item(
            "obj_style.parent_name",
            "Walls" if is_subcategory else None,
            ITEM_Q_OK if is_subcategory else ITEM_Q_MISSING,
        ),
    ]
    return sorted(items, key=lambda d: str(d.get("k", "")))


def test_object_styles_model_area9_fields_pass_contract_validation_for_subcategory():
    registry = _domain_identity_registry_v2()
    identity_items = _area9_identity_items(is_subcategory=True)
    rec = build_record_v2(
        domain="object_styles_model",
        record_id="Walls|Wall Tags",
        status="ok",
        status_reasons=[],
        sig_hash=make_hash(serialize_identity_items(identity_items)),
        identity_items=identity_items,
        required_qs=[ITEM_Q_OK],
        label={"display": "Walls|Wall Tags", "quality": "human", "provenance": "computed.path", "components": {}},
    )
    assert validate_record_v2(rec, registry) == []


def test_object_styles_model_parent_name_missing_and_none_for_top_level_category():
    # A genuinely top-level category (no parent) is None/unset -- per the record.v2 sentinel
    # policy ("Identity values (v) MUST NOT contain sentinel literals -- use
    # v: null + q: 'missing' instead"), that's q=missing, not q=ok and not q=unreadable
    # (unreadable is reserved for actual read failures/exceptions). Matches the probe's own
    # classification (audit_results/audit_11_domain_extractor_delta_step0_findings.md §9.3:
    # ok=364; missing=279 for top-level rows).
    registry = _domain_identity_registry_v2()
    identity_items = _area9_identity_items(is_subcategory=False)
    rec = build_record_v2(
        domain="object_styles_model",
        record_id="Walls|self",
        status="ok",
        status_reasons=[],
        sig_hash=make_hash(serialize_identity_items(identity_items)),
        identity_items=identity_items,
        required_qs=[ITEM_Q_OK],
        label={"display": "Walls|self", "quality": "human", "provenance": "computed.path", "components": {}},
    )
    assert validate_record_v2(rec, registry) == []
    parent_name_item = next(it for it in rec["identity_basis"]["items"] if it["k"] == "obj_style.parent_name")
    assert parent_name_item["v"] is None
    assert parent_name_item["q"] == ITEM_Q_MISSING
