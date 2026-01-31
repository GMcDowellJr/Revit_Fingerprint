# -*- coding: utf-8 -*-
"""Tests for shape-gated dimension type property export.

This module tests the shape-gating logic for dimension_types domain:
- Shape discriminator position and values
- Shape-specific property inclusion/exclusion
- Common property presence across all shapes
- Quality value correctness (no UNSUPPORTED_NOT_APPLICABLE for shape-gated properties)

Tests are organized into:
1. Unit tests for helper functions (no Revit required)
2. Unit tests for identity item builders (no Revit required)
3. Integration test patterns for Revit validation (documented)
"""

try:
    import pytest
except ImportError:
    pytest = None

from core.record_v2 import (
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    make_identity_item,
)
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

# Import dimension_types module components for testing
from domains.dimension_types import (
    # Shape constants
    SHAPE_LINEAR,
    SHAPE_ANGULAR,
    SHAPE_RADIAL,
    SHAPE_DIAMETER,
    SHAPE_ARC_LENGTH,
    SHAPE_SPOT_ELEVATION,
    SHAPE_SPOT_COORDINATE,
    SHAPE_SPOT_SLOPE,
    SHAPE_LINEAR_FIXED,
    SHAPE_SPOT_ELEVATION_FIXED,
    SHAPE_DIAMETER_LINKED,
    SHAPE_UNKNOWN,
    # Family constants
    FAMILY_LINEAR,
    FAMILY_RADIAL,
    FAMILY_ANGULAR,
    FAMILY_SPOT,
    FAMILY_UNKNOWN,
    # Mappings
    SHAPE_TO_FAMILY,
    SHAPE_INT_TO_NAME,
    # Helper functions
    _is_linear_family,
    _is_radial_family,
    _is_angular_family,
    _is_spot_family,
    # Identity item builders
    _build_common_identity_items,
    _build_linear_identity_items,
    _build_radial_identity_items,
    _build_angular_identity_items,
    _build_spot_identity_items,
    _build_identity_items,
)


# =============================================================================
# Shape Constants and Mappings Tests
# =============================================================================

class TestShapeConstants:
    """Test shape constant definitions and mappings."""

    def test_all_shape_constants_defined(self):
        """All expected shape constants must be defined."""
        expected_shapes = [
            "Linear", "Angular", "Radial", "Diameter", "ArcLength",
            "SpotElevation", "SpotCoordinate", "SpotSlope",
            "LinearFixed", "SpotElevationFixed", "DiameterLinked", "Unknown"
        ]
        actual_shapes = [
            SHAPE_LINEAR, SHAPE_ANGULAR, SHAPE_RADIAL, SHAPE_DIAMETER,
            SHAPE_ARC_LENGTH, SHAPE_SPOT_ELEVATION, SHAPE_SPOT_COORDINATE,
            SHAPE_SPOT_SLOPE, SHAPE_LINEAR_FIXED, SHAPE_SPOT_ELEVATION_FIXED,
            SHAPE_DIAMETER_LINKED, SHAPE_UNKNOWN
        ]
        assert actual_shapes == expected_shapes

    def test_all_family_constants_defined(self):
        """All expected family constants must be defined."""
        assert FAMILY_LINEAR == "linear"
        assert FAMILY_RADIAL == "radial"
        assert FAMILY_ANGULAR == "angular"
        assert FAMILY_SPOT == "spot"
        assert FAMILY_UNKNOWN == "unknown"

    def test_shape_to_family_mapping_complete(self):
        """SHAPE_TO_FAMILY must map all shapes to families."""
        # Linear family
        assert SHAPE_TO_FAMILY[SHAPE_LINEAR] == FAMILY_LINEAR
        assert SHAPE_TO_FAMILY[SHAPE_LINEAR_FIXED] == FAMILY_LINEAR

        # Radial family
        assert SHAPE_TO_FAMILY[SHAPE_RADIAL] == FAMILY_RADIAL
        assert SHAPE_TO_FAMILY[SHAPE_DIAMETER] == FAMILY_RADIAL
        assert SHAPE_TO_FAMILY[SHAPE_DIAMETER_LINKED] == FAMILY_RADIAL

        # Angular family
        assert SHAPE_TO_FAMILY[SHAPE_ANGULAR] == FAMILY_ANGULAR
        assert SHAPE_TO_FAMILY[SHAPE_ARC_LENGTH] == FAMILY_ANGULAR

        # Spot family
        assert SHAPE_TO_FAMILY[SHAPE_SPOT_ELEVATION] == FAMILY_SPOT
        assert SHAPE_TO_FAMILY[SHAPE_SPOT_COORDINATE] == FAMILY_SPOT
        assert SHAPE_TO_FAMILY[SHAPE_SPOT_SLOPE] == FAMILY_SPOT
        assert SHAPE_TO_FAMILY[SHAPE_SPOT_ELEVATION_FIXED] == FAMILY_SPOT

        # Unknown
        assert SHAPE_TO_FAMILY[SHAPE_UNKNOWN] == FAMILY_UNKNOWN

    def test_shape_int_to_name_mapping(self):
        """SHAPE_INT_TO_NAME must map DimensionStyleType enum values correctly."""
        assert SHAPE_INT_TO_NAME[0] == SHAPE_LINEAR
        assert SHAPE_INT_TO_NAME[1] == SHAPE_ANGULAR
        assert SHAPE_INT_TO_NAME[2] == SHAPE_RADIAL
        assert SHAPE_INT_TO_NAME[3] == SHAPE_DIAMETER
        assert SHAPE_INT_TO_NAME[4] == SHAPE_ARC_LENGTH
        assert SHAPE_INT_TO_NAME[5] == SHAPE_SPOT_ELEVATION
        assert SHAPE_INT_TO_NAME[6] == SHAPE_SPOT_COORDINATE
        assert SHAPE_INT_TO_NAME[7] == SHAPE_SPOT_SLOPE
        assert SHAPE_INT_TO_NAME[8] == SHAPE_LINEAR_FIXED
        assert SHAPE_INT_TO_NAME[9] == SHAPE_SPOT_ELEVATION_FIXED
        assert SHAPE_INT_TO_NAME[10] == SHAPE_DIAMETER_LINKED


# =============================================================================
# Family Helper Function Tests
# =============================================================================

class TestFamilyHelpers:
    """Test family detection helper functions."""

    def test_is_linear_family(self):
        """_is_linear_family must return True only for linear family."""
        assert _is_linear_family(FAMILY_LINEAR) is True
        assert _is_linear_family(FAMILY_RADIAL) is False
        assert _is_linear_family(FAMILY_ANGULAR) is False
        assert _is_linear_family(FAMILY_SPOT) is False
        assert _is_linear_family(FAMILY_UNKNOWN) is False

    def test_is_radial_family(self):
        """_is_radial_family must return True only for radial family."""
        assert _is_radial_family(FAMILY_RADIAL) is True
        assert _is_radial_family(FAMILY_LINEAR) is False
        assert _is_radial_family(FAMILY_ANGULAR) is False
        assert _is_radial_family(FAMILY_SPOT) is False
        assert _is_radial_family(FAMILY_UNKNOWN) is False

    def test_is_angular_family(self):
        """_is_angular_family must return True only for angular family."""
        assert _is_angular_family(FAMILY_ANGULAR) is True
        assert _is_angular_family(FAMILY_LINEAR) is False
        assert _is_angular_family(FAMILY_RADIAL) is False
        assert _is_angular_family(FAMILY_SPOT) is False
        assert _is_angular_family(FAMILY_UNKNOWN) is False

    def test_is_spot_family(self):
        """_is_spot_family must return True only for spot family."""
        assert _is_spot_family(FAMILY_SPOT) is True
        assert _is_spot_family(FAMILY_LINEAR) is False
        assert _is_spot_family(FAMILY_RADIAL) is False
        assert _is_spot_family(FAMILY_ANGULAR) is False
        assert _is_spot_family(FAMILY_UNKNOWN) is False


# =============================================================================
# Identity Item Builder Tests
# =============================================================================

class TestCommonIdentityItems:
    """Test _build_common_identity_items function."""

    def test_common_items_count(self):
        """Common identity items must include exactly 7 properties."""
        items = _build_common_identity_items(
            shape_v="Linear", shape_q=ITEM_Q_OK,
            unit_format_id_v="autodesk.unit.formatOption:length", unit_format_id_q=ITEM_Q_OK,
            rounding_v="Nearest", rounding_q=ITEM_Q_OK,
            accuracy_v="0.01", accuracy_q=ITEM_Q_OK,
            prefix_v="", prefix_q=ITEM_Q_OK,
            suffix_v="", suffix_q=ITEM_Q_OK,
            tick_sig_hash="abc123",
        )
        assert len(items) == 7

    def test_common_items_keys(self):
        """Common identity items must have correct keys."""
        items = _build_common_identity_items(
            shape_v="Linear", shape_q=ITEM_Q_OK,
            unit_format_id_v="autodesk.unit.formatOption:length", unit_format_id_q=ITEM_Q_OK,
            rounding_v="Nearest", rounding_q=ITEM_Q_OK,
            accuracy_v="0.01", accuracy_q=ITEM_Q_OK,
            prefix_v="", prefix_q=ITEM_Q_OK,
            suffix_v="", suffix_q=ITEM_Q_OK,
            tick_sig_hash="abc123",
        )
        keys = {it["k"] for it in items}
        expected_keys = {
            "dim_type.shape",
            "dim_type.unit_format_id",
            "dim_type.rounding",
            "dim_type.accuracy",
            "dim_type.prefix",
            "dim_type.suffix",
            "dim_type.tick_mark_sig_hash",
        }
        assert keys == expected_keys

    def test_shape_is_first_item(self):
        """Shape must be the first common identity item."""
        items = _build_common_identity_items(
            shape_v="Linear", shape_q=ITEM_Q_OK,
            unit_format_id_v=None, unit_format_id_q=ITEM_Q_MISSING,
            rounding_v=None, rounding_q=ITEM_Q_MISSING,
            accuracy_v="0.01", accuracy_q=ITEM_Q_OK,
            prefix_v=None, prefix_q=ITEM_Q_MISSING,
            suffix_v=None, suffix_q=ITEM_Q_MISSING,
            tick_sig_hash=None,
        )
        assert items[0]["k"] == "dim_type.shape"
        assert items[0]["v"] == "Linear"
        assert items[0]["q"] == ITEM_Q_OK

    def test_tick_sig_hash_missing_when_none(self):
        """Tick mark sig hash must be MISSING when not provided."""
        items = _build_common_identity_items(
            shape_v="Linear", shape_q=ITEM_Q_OK,
            unit_format_id_v=None, unit_format_id_q=ITEM_Q_MISSING,
            rounding_v=None, rounding_q=ITEM_Q_MISSING,
            accuracy_v="0.01", accuracy_q=ITEM_Q_OK,
            prefix_v=None, prefix_q=ITEM_Q_MISSING,
            suffix_v=None, suffix_q=ITEM_Q_MISSING,
            tick_sig_hash=None,
        )
        tick_item = next(it for it in items if it["k"] == "dim_type.tick_mark_sig_hash")
        assert tick_item["v"] is None
        assert tick_item["q"] == ITEM_Q_MISSING


class TestLinearIdentityItems:
    """Test _build_linear_identity_items function."""

    def test_linear_items_count(self):
        """Linear identity items must include exactly 1 property."""
        items = _build_linear_identity_items(
            witness_v="Gap to Element",
            witness_q=ITEM_Q_OK,
        )
        assert len(items) == 1

    def test_linear_items_key(self):
        """Linear identity items must use the correct key."""
        items = _build_linear_identity_items(
            witness_v="Gap to Element",
            witness_q=ITEM_Q_OK,
        )
        assert items[0]["k"] == "dim_type.witness_line_control"


class TestRadialIdentityItems:
    """Test _build_radial_identity_items function."""

    def test_radial_items_count(self):
        """Radial identity items must include exactly 2 properties."""
        items = _build_radial_identity_items(
            center_marks_v="1",
            center_marks_q=ITEM_Q_OK,
            center_mark_size_v="0.125",
            center_mark_size_q=ITEM_Q_OK,
        )
        assert len(items) == 2

    def test_radial_items_keys(self):
        """Radial identity items must use correct keys."""
        items = _build_radial_identity_items(
            center_marks_v="1",
            center_marks_q=ITEM_Q_OK,
            center_mark_size_v="0.125",
            center_mark_size_q=ITEM_Q_OK,
        )
        keys = {it["k"] for it in items}
        assert "dim_type.center_marks" in keys
        assert "dim_type.center_mark_size" in keys


class TestAngularIdentityItems:
    """Test _build_angular_identity_items function."""

    def test_angular_items_empty(self):
        """Angular identity items list should be empty (no shape-specific params)."""
        items = _build_angular_identity_items()
        assert items == []


class TestSpotIdentityItems:
    """Test _build_spot_identity_items function."""

    def test_spot_items_empty(self):
        """Spot identity items list should be empty (no shape-specific params)."""
        items = _build_spot_identity_items()
        assert items == []


# =============================================================================
# Shape-Gating Integration Tests
# =============================================================================

class TestJoinKeyShapeGating:
    """Test join key policy shape gating integration."""

    def _load_dim_policy(self):
        """Load dimension_types join key policy."""
        policies = load_join_key_policies("policies/domain_join_key_policies.json")
        return get_domain_join_key_policy(policies, "dimension_types")

    def test_policy_has_shape_gating(self):
        """Dimension types policy must have shape_gating section."""
        dim_policy = self._load_dim_policy()
        assert "shape_gating" in dim_policy
        assert dim_policy["shape_gating"]["discriminator_key"] == "dim_type.shape"

    def test_policy_schema_version(self):
        """Dimension types policy must be at v3."""
        dim_policy = self._load_dim_policy()
        assert dim_policy["join_key_schema"] == "dimension_types.join_key.v3"

    def test_linear_requires_witness_line_control(self):
        """Linear shape must require witness_line_control."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Linear", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "uf", ITEM_Q_OK),
            # Missing witness_line_control
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.witness_line_control" in missing
        assert jk["shape_gating"]["shape_value"] == "Linear"
        assert jk["shape_gating"]["shape_matched"] is True
        assert "dim_type.witness_line_control" in jk["shape_gating"]["additional_required_keys"]

    def test_linear_with_witness_line_control(self):
        """Linear shape with witness_line_control should have no missing."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Linear", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "uf", ITEM_Q_OK),
            make_identity_item("dim_type.witness_line_control", "Gap to Element", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.witness_line_control" not in missing
        assert jk["shape_gating"]["shape_matched"] is True

    def test_radial_requires_center_marks(self):
        """Radial shape must require center_marks and center_mark_size."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Radial", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "uf", ITEM_Q_OK),
            # Missing center_marks and center_mark_size
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.center_marks" in missing
        assert "dim_type.center_mark_size" in missing
        assert jk["shape_gating"]["shape_value"] == "Radial"

    def test_radial_with_center_marks(self):
        """Radial shape with center marks should have no missing."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Radial", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "uf", ITEM_Q_OK),
            make_identity_item("dim_type.center_marks", "1", ITEM_Q_OK),
            make_identity_item("dim_type.center_mark_size", "0.125", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.center_marks" not in missing
        assert "dim_type.center_mark_size" not in missing

    def test_angular_requires_unit_format_id(self):
        """Angular shape should require unit_format_id now."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Angular", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.unit_format_id" in missing
        assert jk["shape_gating"]["additional_required_keys"] == ["dim_type.unit_format_id"]

    def test_angular_with_unit_format_id(self):
        """Angular shape with unit_format_id should have no missing."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Angular", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "uf", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.unit_format_id" not in missing

    def test_unknown_shape_uses_common_only(self):
        """Unknown shapes should use common properties only (default_shape_behavior=common_only)."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "SomeNewShape", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        # Shape not matched, but still works with common only
        assert jk["shape_gating"]["shape_value"] == "SomeNewShape"
        assert jk["shape_gating"]["shape_matched"] is False
        assert jk["shape_gating"]["additional_required_keys"] == []

    def test_all_shapes_have_definitions(self):
        """All known shapes must have shape_requirements definitions."""
        dim_policy = self._load_dim_policy()
        all_shapes = [
            "Linear", "LinearFixed", "Radial", "Diameter", "DiameterLinked",
            "Angular", "ArcLength", "SpotElevation", "SpotCoordinate",
            "SpotSlope", "SpotElevationFixed"
        ]
        shape_reqs = dim_policy["shape_gating"]["shape_requirements"]
        for shape in all_shapes:
            assert shape in shape_reqs, f"Shape {shape} missing from shape_requirements"


# =============================================================================
# Property Exclusion Tests
# =============================================================================

class TestPropertyExclusion:
    """Test that shape-gated properties are correctly excluded."""

    def _get_keys_for_shape(self, shape_v, shape_family, **shape_specific_args):
        """Helper to get identity item keys for a given shape."""
        args = {
            "shape_family": shape_family,
            "shape_v": shape_v,
            "shape_q": ITEM_Q_OK,
            "unit_format_id_v": "test",
            "unit_format_id_q": ITEM_Q_OK,
            "rounding_v": "Nearest",
            "rounding_q": ITEM_Q_OK,
            "accuracy_v": "0.01",
            "accuracy_q": ITEM_Q_OK,
            "prefix_v": "",
            "prefix_q": ITEM_Q_OK,
            "suffix_v": "",
            "suffix_q": ITEM_Q_OK,
            "tick_sig_hash": "abc123",
        }
        args.update(shape_specific_args)
        items, _ = _build_identity_items(**args)
        return {it["k"] for it in items}

    def test_linear_excludes_radial_properties(self):
        """Linear dimensions must NOT include radial-specific properties."""
        keys = self._get_keys_for_shape(
            SHAPE_LINEAR, FAMILY_LINEAR,
            witness_v="Gap to Element", witness_q=ITEM_Q_OK
        )

        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys

    def test_linear_excludes_angular_properties(self):
        """Linear dimensions must NOT include angular-specific properties."""
        # Angular has no specific properties, but verify linear doesn't add spurious ones
        keys = self._get_keys_for_shape(
            SHAPE_LINEAR, FAMILY_LINEAR,
            witness_v="Gap to Element", witness_q=ITEM_Q_OK
        )

        # Should only have common + linear-specific
        expected_prefixes = ["dim_type."]
        for key in keys:
            assert key.startswith("dim_type.")

    def test_radial_excludes_linear_properties(self):
        """Radial dimensions must NOT include linear-specific properties."""
        keys = self._get_keys_for_shape(
            SHAPE_RADIAL, FAMILY_RADIAL,
            center_marks_v="1", center_marks_q=ITEM_Q_OK,
            center_mark_size_v="0.125", center_mark_size_q=ITEM_Q_OK
        )

        assert "dim_type.witness_line_control" not in keys

    def test_angular_excludes_linear_and_radial_properties(self):
        """Angular dimensions must NOT include linear or radial-specific properties."""
        keys = self._get_keys_for_shape(
            SHAPE_ANGULAR, FAMILY_ANGULAR
        )

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys

    def test_spot_excludes_linear_and_radial_properties(self):
        """Spot dimensions must NOT include linear or radial-specific properties."""
        keys = self._get_keys_for_shape(
            SHAPE_SPOT_ELEVATION, FAMILY_SPOT
        )

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys


# =============================================================================
# Policy-level Integration Test Pattern (Documented)
# =============================================================================

class TestPolicyLoadPattern:
    """Documented integration test patterns for full Revit validation."""

    def test_policy_load_integration_pattern(self):
        """Policy load integration pattern placeholder.

        This test documents the pattern for integration tests when running
        inside Revit environment.
        """
        # This is a placeholder for Revit-based integration tests
        # In a Revit environment, you would:
        # 1. Export actual dimension types
        # 2. Verify shape-specific keys present/absent based on shape
        # 3. Validate join key construction across shapes
        assert True
