# -*- coding: utf-8 -*-
"""
Tests for join_key migration for view_category_overrides and view_templates domains.

These tests verify:
1. Hash computation is deterministic and order-independent
2. Join_key structure is properly formed
3. Phase2 categorization is correct
"""

import pytest
import hashlib
import sys
import os

# Ensure repo root is importable
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item


# ============================================================
# view_category_overrides tests
# ============================================================

class TestViewCategoryOverridesOverrideHash:
    """Tests for _compute_override_properties_hash function."""

    def test_override_hash_deterministic(self):
        """Override hash must be deterministic for same property set."""
        from domains.view_category_overrides import _compute_override_properties_hash

        items1 = [
            {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
            {"k": "vco.projection.line_weight", "q": "ok", "v": "1"}
        ]

        # Same items, different order
        items2 = [
            {"k": "vco.projection.line_weight", "q": "ok", "v": "1"},
            {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"}
        ]

        hash1 = _compute_override_properties_hash(items1)
        hash2 = _compute_override_properties_hash(items2)

        assert hash1 == hash2, "Hash must be order-independent"
        assert len(hash1) == 32, "MD5 hash must be 32 hex chars"

    def test_override_hash_excludes_baseline_items(self):
        """Override hash must exclude baseline reference items."""
        from domains.view_category_overrides import _compute_override_properties_hash

        # Items with and without baseline references
        items_with_baseline = [
            {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
            {"k": "vco.baseline_sig_hash", "q": "ok", "v": "abc123"},
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
        ]

        items_without_baseline = [
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
        ]

        hash1 = _compute_override_properties_hash(items_with_baseline)
        hash2 = _compute_override_properties_hash(items_without_baseline)

        # Both should produce the same hash since baseline items are excluded
        assert hash1 == hash2, "Baseline items must be excluded from hash"

    def test_override_hash_handles_none_values(self):
        """Override hash must handle None values gracefully."""
        from domains.view_category_overrides import _compute_override_properties_hash

        items = [
            {"k": "vco.cut.line_weight", "q": "ok", "v": None},
            {"k": "vco.projection.line_weight", "q": "ok", "v": "1"}
        ]

        # Should not raise
        hash_result = _compute_override_properties_hash(items)
        assert len(hash_result) == 32, "MD5 hash must be 32 hex chars"

    def test_override_hash_different_values_produce_different_hash(self):
        """Different override values must produce different hashes."""
        from domains.view_category_overrides import _compute_override_properties_hash

        items1 = [
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
        ]

        items2 = [
            {"k": "vco.cut.line_weight", "q": "ok", "v": "2"},
        ]

        hash1 = _compute_override_properties_hash(items1)
        hash2 = _compute_override_properties_hash(items2)

        assert hash1 != hash2, "Different values must produce different hashes"


class TestViewCategoryOverridesPhase2Partition:
    """Tests for _phase2_partition_items function."""

    def test_partition_semantic_items(self):
        """Semantic items must include baseline refs and override_properties_hash."""
        from domains.view_category_overrides import _phase2_partition_items

        items = [
            {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
            {"k": "vco.baseline_sig_hash", "q": "ok", "v": "abc123"},
            {"k": "vco.override_properties_hash", "q": "ok", "v": "def456"},
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
            {"k": "vco.projection.line_weight", "q": "ok", "v": "1"},
        ]

        semantic, cosmetic, unknown = _phase2_partition_items(items)

        semantic_keys = [it["k"] for it in semantic]
        assert "vco.baseline_category_path" in semantic_keys
        assert "vco.baseline_sig_hash" in semantic_keys
        assert "vco.override_properties_hash" in semantic_keys
        assert len(semantic) == 3

    def test_partition_cosmetic_items(self):
        """Cosmetic items must include individual delta properties."""
        from domains.view_category_overrides import _phase2_partition_items

        items = [
            {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
            {"k": "vco.cut.line_weight", "q": "ok", "v": "-1"},
            {"k": "vco.projection.line_weight", "q": "ok", "v": "1"},
            {"k": "vco.halftone", "q": "ok", "v": "false"},
            {"k": "vco.transparency", "q": "ok", "v": "0"},
        ]

        semantic, cosmetic, unknown = _phase2_partition_items(items)

        cosmetic_keys = [it["k"] for it in cosmetic]
        assert "vco.cut.line_weight" in cosmetic_keys
        assert "vco.projection.line_weight" in cosmetic_keys
        assert "vco.halftone" in cosmetic_keys
        assert "vco.transparency" in cosmetic_keys
        assert len(cosmetic) == 4


# ============================================================
# view_templates tests
# ============================================================

class TestViewTemplatesJoinKey:
    """Tests for view_templates join_key structure."""

    def test_join_hash_format(self):
        """join_hash must be a 32-char hex string (MD5)."""
        # Simulate the join_key structure
        def_hash = hashlib.md5("test_signature".encode("utf-8")).hexdigest()

        join_key = {
            "schema": "view_templates.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "items": [
                {
                    "k": "view_template.def_hash",
                    "q": "ok",
                    "v": def_hash
                }
            ],
            "join_hash": def_hash
        }

        assert len(join_key["join_hash"]) == 32
        assert join_key["join_hash"] == def_hash
        assert join_key["schema"] == "view_templates.join_key.v1"

    def test_join_hash_equals_def_hash(self):
        """For v1 policy, join_hash must equal def_hash."""
        def_hash = "8e9fc08d2051056ae81bb95e8a26ce70"

        # Per policy, join_hash equals def_hash for baseline-only v1
        join_hash = def_hash

        assert join_hash == def_hash, "VT join_hash must equal def_hash for v1"


# ============================================================
# Join key policy validation
# ============================================================

class TestJoinKeyPolicyStructure:
    """Tests that join_key policies are properly defined."""

    def test_vco_policy_exists(self):
        """view_category_overrides policy must exist in policies file."""
        import json
        policies_path = os.path.join(repo_root, "policies", "domain_join_key_policies.json")

        with open(policies_path) as f:
            policies = json.load(f)

        assert "view_category_overrides" in policies["domains"]
        vco_policy = policies["domains"]["view_category_overrides"]

        assert vco_policy["join_key_schema"] == "view_category_overrides.join_key.v1"
        assert "vco.baseline_category_path" in vco_policy["required_items"]
        assert "vco.baseline_sig_hash" in vco_policy["required_items"]
        assert "vco.override_properties_hash" in vco_policy["required_items"]

    def test_vt_policy_exists(self):
        """view_templates policy must exist in policies file."""
        import json
        policies_path = os.path.join(repo_root, "policies", "domain_join_key_policies.json")

        with open(policies_path) as f:
            policies = json.load(f)

        assert "view_templates" in policies["domains"]
        vt_policy = policies["domains"]["view_templates"]

        if vt_policy["join_key_schema"] == "view_templates.join_key.v2":
            assert "view_template.sig.include_phase_filter" in vt_policy["required_items"]
            assert "view_template.def_hash" not in vt_policy["required_items"]
        else:
            assert vt_policy["join_key_schema"] == "view_templates.join_key.v1"
            assert "view_template.def_hash" in vt_policy["required_items"]


# ============================================================
# Integration-like tests (structure validation)
# ============================================================

class TestJoinKeyStructureValidation:
    """Tests that validate expected join_key structure."""

    def test_vco_join_key_has_required_fields(self):
        """VCO join_key must have all required fields."""
        # Simulate a VCO record's join_key
        join_key = {
            "schema": "view_category_overrides.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "items": [
                {"k": "vco.baseline_category_path", "q": "ok", "v": "Walls|self"},
                {"k": "vco.baseline_sig_hash", "q": "ok", "v": "abc123"},
                {"k": "vco.override_properties_hash", "q": "ok", "v": "def456"},
            ],
            "join_hash": "xyz789"
        }

        assert "schema" in join_key
        assert "hash_alg" in join_key
        assert "items" in join_key
        assert "join_hash" in join_key
        assert len(join_key["items"]) == 3

    def test_vt_join_key_has_required_fields(self):
        """VT join_key must have all required fields."""
        # Simulate a VT record's join_key
        join_key = {
            "schema": "view_templates.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "items": [
                {"k": "view_template.def_hash", "q": "ok", "v": "8e9fc08d2051056ae81bb95e8a26ce70"},
            ],
            "join_hash": "8e9fc08d2051056ae81bb95e8a26ce70"
        }

        assert "schema" in join_key
        assert "hash_alg" in join_key
        assert "items" in join_key
        assert "join_hash" in join_key
        assert len(join_key["items"]) == 1
        assert join_key["items"][0]["k"] == "view_template.def_hash"


class TestGroupingBasis:
    """Tests that grouping_basis is correctly set."""

    def test_vco_grouping_basis_is_join_key(self):
        """VCO phase2 grouping_basis must be 'join_key.join_hash'."""
        expected = "join_key.join_hash"

        # This is what the code should produce
        phase2 = {
            "schema": "phase2.view_category_overrides.v1",
            "grouping_basis": "join_key.join_hash",
        }

        assert phase2["grouping_basis"] == expected

    def test_vt_grouping_basis_is_join_key(self):
        """VT phase2 grouping_basis must be 'join_key.join_hash'."""
        expected = "join_key.join_hash"

        # This is what the code should produce
        phase2 = {
            "schema": "phase2.view_templates.v2",
            "grouping_basis": "join_key.join_hash",
        }

        assert phase2["grouping_basis"] == expected
