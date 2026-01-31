# -*- coding: utf-8 -*-

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from domains.arrowheads import (
    _build_common_identity_items,
    _build_arrow_identity_items,
    _build_tick_identity_items,
    _get_arrowhead_style,
)


def test_style_discriminator_first():
    common = _build_common_identity_items(
        style_v="Arrow",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    assert common[0]["k"] == "arrowhead.style"
    assert common[0]["v"] == "Arrow"


def test_style_specific_keys_are_omitted_when_not_applicable():
    common = _build_common_identity_items(
        style_v="Tick",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    tick_specific = _build_tick_identity_items(
        centered_v="true",
        centered_q=ITEM_Q_OK,
        pen_v="2",
        pen_q=ITEM_Q_OK,
    )
    keys = [it["k"] for it in (common + tick_specific)]
    assert "arrowhead.width_angle_deg" not in keys
    assert "arrowhead.fill_tick" not in keys
    assert "arrowhead.arrow_closed" not in keys


def test_no_missing_for_unrelated_style_properties():
    common = _build_common_identity_items(
        style_v="Arrow",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    arrow_specific = _build_arrow_identity_items(
        width_angle_v="45",
        width_angle_q=ITEM_Q_OK,
        fill_v="true",
        fill_q=ITEM_Q_OK,
        closed_v="false",
        closed_q=ITEM_Q_OK,
    )
    items = common + arrow_specific
    assert all(it["q"] != ITEM_Q_MISSING for it in items)


def test_join_key_builder_additional_required_only_for_shape():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "arrowheads")

    items = [
        make_identity_item("arrowhead.style", "Tick", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.25", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_mark_centered", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.heavy_end_pen_weight", "2", ITEM_Q_OK),
    ]

    jk, missing = build_join_key_from_policy(domain_policy=pol, identity_items=items)
    assert "arrowhead.tick_mark_centered" not in missing
    assert "arrowhead.heavy_end_pen_weight" not in missing
    assert "arrowhead.width_angle_deg" not in missing
    assert jk["shape_gating"]["shape_value"] == "Tick"


def test_get_arrowhead_style_fallback():
    style_v, style_q = _get_arrowhead_style("UnknownStyle", ITEM_Q_OK)
    assert style_v == "Other"
    assert style_q == ITEM_Q_OK


def test_join_key_builder_other_style():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "arrowheads")

    items = [
        make_identity_item("arrowhead.style", "Other", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.25", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_mark_centered", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.heavy_end_pen_weight", "2", ITEM_Q_OK),
    ]

    jk, missing = build_join_key_from_policy(domain_policy=pol, identity_items=items)
    assert "arrowhead.tick_mark_centered" not in missing
    assert "arrowhead.heavy_end_pen_weight" not in missing
    assert jk["shape_gating"]["shape_value"] == "Other"
