# -*- coding: utf-8 -*-

from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item


def _load_policy(domain_name):
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, domain_name)


def test_view_templates_floor_policy_has_required_keys():
    """Floor/structural/area plans policy must have view-specific required keys."""
    pol = _load_policy("view_templates_floor_structural_area_plans")
    assert "vt.view_type_family" in pol["required_items"]
    assert "vt.phase_filter_sig_hash" in pol["required_items"]
    assert "vt.filter_stack_sig_hash" in pol["required_items"]


def test_view_templates_ceiling_policy_has_required_keys():
    """Ceiling plans policy must have required keys."""
    pol = _load_policy("view_templates_ceiling_plans")
    assert "vt.view_type_family" in pol["required_items"]
    assert "vt.phase_filter_sig_hash" in pol["required_items"]


def test_view_templates_elevations_policy_has_required_keys():
    """Elevations/sections/detail policy must have required keys."""
    pol = _load_policy("view_templates_elevations_sections_detail")
    assert "vt.view_type_family" in pol["required_items"]
    assert "vt.phase_filter_sig_hash" in pol["required_items"]


def test_view_templates_renderings_policy_has_required_keys():
    """Renderings/drafting policy must have required keys."""
    pol = _load_policy("view_templates_renderings_drafting")
    assert "vt.view_type_family" in pol["required_items"]


def test_view_templates_schedules_policy_has_required_keys():
    """Schedules policy must have required keys."""
    pol = _load_policy("view_templates_schedules")
    assert "vt.view_type_family" in pol["required_items"]
    assert "vt.phase_filter_sig_hash" in pol["required_items"]


def test_view_templates_all_split_domains_have_schemas():
    """All view_templates split domains must have valid join_key_schema."""
    split_domains = [
        "view_templates_floor_structural_area_plans",
        "view_templates_ceiling_plans",
        "view_templates_elevations_sections_detail",
        "view_templates_renderings_drafting",
        "view_templates_schedules",
    ]
    for dom in split_domains:
        pol = _load_policy(dom)
        assert pol is not None, "Policy for {} is None".format(dom)
        assert pol.get("join_key_schema", "").startswith(dom), \
            "join_key_schema mismatch for {}".format(dom)
        assert pol.get("hash_alg") == "md5_utf8_join_pipe"
        assert "required_items" in pol
        assert len(pol["required_items"]) > 0
