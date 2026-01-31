# -*- coding: utf-8 -*-

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

from core.join_key_policy import validate_domain_join_key_policy


_VALID_POLICY = {
    "join_key_schema": "test.schema",
    "hash_alg": "md5",
    "required_items": ["shape", "common_a"],
    "optional_items": ["common_b", "shape_x", "shape_y", "common_a"],
    "explicitly_excluded_items": [],
    "shape_gating": {
        "discriminator_key": "shape",
        "shape_requirements": {
            "X": {"additional_required": ["shape_x"], "additional_optional": []},
            "Y": {"additional_required": ["shape_y"], "additional_optional": []},
        },
        "default_shape_behavior": "common_only",
    },
}


def test_valid_shape_gated_policy_has_no_errors():
    issues = validate_domain_join_key_policy("demo", _VALID_POLICY)
    assert issues == []


def test_rule_a1_discriminator_first_required():
    pol = dict(_VALID_POLICY)
    pol["required_items"] = ["common_a", "shape"]
    issues = validate_domain_join_key_policy("demo", pol)
    codes = {i["code"] for i in issues}
    paths = {i["path"] for i in issues}
    assert "A1_DISCRIMINATOR_FIRST" in codes
    assert "required_items" in paths


def test_rule_a2_no_overlap_common_required():
    pol = dict(_VALID_POLICY)
    pol["shape_gating"] = dict(pol["shape_gating"])
    pol["shape_gating"]["shape_requirements"] = {
        "X": {"additional_required": ["common_a"], "additional_optional": []}
    }
    issues = validate_domain_join_key_policy("demo", pol)
    codes = {i["code"] for i in issues}
    paths = {i["path"] for i in issues}
    assert "A2_OVERLAP_COMMON_REQUIRED" in codes
    assert "required_items" in paths


def test_rule_a3_additional_required_in_optional_items():
    pol = dict(_VALID_POLICY)
    pol["optional_items"] = ["common_b", "common_a"]
    issues = validate_domain_join_key_policy("demo", pol)
    codes = {i["code"] for i in issues}
    paths = {i["path"] for i in issues}
    assert "A3_REQUIRED_NOT_OPTIONAL" in codes
    assert "optional_items" in paths


def test_rule_a4_requires_non_empty_additional_required():
    pol = dict(_VALID_POLICY)
    pol["shape_gating"] = dict(pol["shape_gating"])
    pol["shape_gating"]["shape_requirements"] = {
        "X": {"additional_required": [], "additional_optional": []}
    }
    issues = validate_domain_join_key_policy("demo", pol)
    codes = {i["code"] for i in issues}
    paths = {i["path"] for i in issues}
    assert "A4_SHAPE_REQUIRED_EMPTY" in codes
    assert any(p.endswith(".additional_required") for p in paths)


def test_rule_a5_orphaned_keys_warning_only():
    pol = dict(_VALID_POLICY)
    pol["shape_gating"] = dict(pol["shape_gating"])
    pol["shape_gating"]["shape_requirements"] = {
        "X": {"additional_required": ["shape_x"], "additional_optional": []}
    }
    issues = validate_domain_join_key_policy("demo", pol, exported_keys={"shape"})
    severities = {i["severity"] for i in issues}
    codes = {i["code"] for i in issues}
    assert "warning" in severities
    assert "A5_ORPHANED_KEY" in codes
    assert all(i["severity"] == "warning" for i in issues)
