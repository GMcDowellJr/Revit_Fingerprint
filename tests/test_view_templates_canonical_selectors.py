# -*- coding: utf-8 -*-

from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item


def _load_policy(domain_name):
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, domain_name)


_SPLIT_DOMAINS = [
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
]


def test_view_templates_all_split_domains_have_schemas():
    """All view_templates split domains must have a valid join_key_schema."""
    for dom in _SPLIT_DOMAINS:
        pol = _load_policy(dom)
        assert pol is not None, "Policy for {} is None".format(dom)
        schema = pol.get("join_key_schema", "")
        # Split domains share the view_templates.join_key.v1 schema
        assert schema == "view_templates.join_key.v1" or schema.startswith(dom), \
            "join_key_schema mismatch for {}: {}".format(dom, schema)
        assert pol.get("hash_alg") == "md5_utf8_join_pipe"
        assert "required_items" in pol
        assert len(pol["required_items"]) > 0


def test_view_templates_all_split_domains_require_def_hash():
    """All split view_template domains must require view_template.def_hash in join key."""
    for dom in _SPLIT_DOMAINS:
        pol = _load_policy(dom)
        assert "view_template.def_hash" in pol["required_items"], \
            "{}: view_template.def_hash not in required_items: {}".format(
                dom, pol["required_items"])


def test_view_templates_floor_policy():
    """Floor/structural/area plans policy must use view_template.def_hash."""
    pol = _load_policy("view_templates_floor_structural_area_plans")
    assert "view_template.def_hash" in pol["required_items"]


def test_view_templates_ceiling_policy():
    """Ceiling plans policy must use view_template.def_hash."""
    pol = _load_policy("view_templates_ceiling_plans")
    assert "view_template.def_hash" in pol["required_items"]


def test_view_templates_elevations_policy():
    """Elevations/sections/detail policy must use view_template.def_hash."""
    pol = _load_policy("view_templates_elevations_sections_detail")
    assert "view_template.def_hash" in pol["required_items"]


def test_view_templates_renderings_policy():
    """Renderings/drafting policy must use view_template.def_hash."""
    pol = _load_policy("view_templates_renderings_drafting")
    assert "view_template.def_hash" in pol["required_items"]


def test_view_templates_schedules_policy():
    """Schedules policy must use view_template.def_hash."""
    pol = _load_policy("view_templates_schedules")
    assert "view_template.def_hash" in pol["required_items"]


def test_view_templates_name_uid_excluded():
    """Name, uid, and element_id must be excluded from all split domain join keys."""
    excluded_keys = ["view_template.name", "view_template.uid", "view_template.element_id"]
    for dom in _SPLIT_DOMAINS:
        pol = _load_policy(dom)
        excluded = pol.get("explicitly_excluded_items", [])
        for key in excluded_keys:
            assert key in excluded, \
                "{}: {} not in explicitly_excluded_items".format(dom, key)


def test_view_templates_join_key_build_with_def_hash():
    """Build join key from policy using view_template.def_hash identity item."""
    pol = _load_policy("view_templates_floor_structural_area_plans")
    identity_items = [
        make_identity_item("view_template.def_hash", "abc123def456", ITEM_Q_OK),
        make_identity_item("view_template.sig.include_phase_filter", "True", ITEM_Q_OK),
    ]
    join_key, missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    assert join_key is not None
    assert len(missing) == 0, "Unexpected missing keys: {}".format(missing)
    keys_used = join_key.get("keys_used", [])
    assert "view_template.def_hash" in keys_used


def test_view_templates_join_key_missing_def_hash():
    """Join key build must report missing when view_template.def_hash is absent."""
    pol = _load_policy("view_templates_schedules")
    identity_items = [
        make_identity_item("view_template.sig.include_vg", "True", ITEM_Q_OK),
    ]
    join_key, missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    assert "view_template.def_hash" in missing
