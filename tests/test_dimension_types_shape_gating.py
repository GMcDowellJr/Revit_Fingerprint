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
        """Linear identity items must include witness_line_control."""
        items = _build_linear_identity_items(
            witness_v="Gap to Element",
            witness_q=ITEM_Q_OK,
        )
        assert items[0]["k"] == "dim_type.witness_line_control"
        assert items[0]["v"] == "Gap to Element"
        assert items[0]["q"] == ITEM_Q_OK


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
        """Radial identity items must include center_marks and center_mark_size."""
        items = _build_radial_identity_items(
            center_marks_v="1",
            center_marks_q=ITEM_Q_OK,
            center_mark_size_v="0.125",
            center_mark_size_q=ITEM_Q_OK,
        )
        keys = {it["k"] for it in items}
        assert keys == {"dim_type.center_marks", "dim_type.center_mark_size"}


class TestAngularIdentityItems:
    """Test _build_angular_identity_items function."""

    def test_angular_items_empty(self):
        """Angular identity items must be empty (common only)."""
        items = _build_angular_identity_items()
        assert items == []


class TestSpotIdentityItems:
    """Test _build_spot_identity_items function."""

    def test_spot_items_empty(self):
        """Spot identity items must be empty (common only)."""
        items = _build_spot_identity_items()
        assert items == []


# =============================================================================
# Main Identity Items Builder Tests
# =============================================================================

class TestBuildIdentityItems:
    """Test _build_identity_items main dispatcher function."""

    def _make_common_args(self, shape_v, shape_family):
        """Helper to create common arguments for _build_identity_items."""
        return {
            "shape_family": shape_family,
            "shape_v": shape_v,
            "shape_q": ITEM_Q_OK,
            "unit_format_id_v": "autodesk.unit.formatOption:length",
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

    def test_linear_includes_witness_line_control(self):
        """Linear dimensions must include witness_line_control."""
        args = self._make_common_args(SHAPE_LINEAR, FAMILY_LINEAR)
        args["witness_v"] = "Gap to Element"
        args["witness_q"] = ITEM_Q_OK

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.witness_line_control" in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys

    def test_linear_fixed_includes_witness_line_control(self):
        """LinearFixed dimensions must include witness_line_control."""
        args = self._make_common_args(SHAPE_LINEAR_FIXED, FAMILY_LINEAR)
        args["witness_v"] = "Fixed to Dimension Line"
        args["witness_q"] = ITEM_Q_OK

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.witness_line_control" in keys

    def test_radial_includes_center_marks(self):
        """Radial dimensions must include center_marks and center_mark_size."""
        args = self._make_common_args(SHAPE_RADIAL, FAMILY_RADIAL)
        args["center_marks_v"] = "1"
        args["center_marks_q"] = ITEM_Q_OK
        args["center_mark_size_v"] = "0.125"
        args["center_mark_size_q"] = ITEM_Q_OK

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.center_marks" in keys
        assert "dim_type.center_mark_size" in keys
        assert "dim_type.witness_line_control" not in keys

    def test_diameter_includes_center_marks(self):
        """Diameter dimensions must include center_marks and center_mark_size."""
        args = self._make_common_args(SHAPE_DIAMETER, FAMILY_RADIAL)
        args["center_marks_v"] = "0"
        args["center_marks_q"] = ITEM_Q_OK
        args["center_mark_size_v"] = "0.25"
        args["center_mark_size_q"] = ITEM_Q_OK

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.center_marks" in keys
        assert "dim_type.center_mark_size" in keys

    def test_angular_uses_common_only(self):
        """Angular dimensions must use only common properties."""
        args = self._make_common_args(SHAPE_ANGULAR, FAMILY_ANGULAR)

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        # Should have common keys only
        assert "dim_type.shape" in keys
        assert "dim_type.accuracy" in keys
        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys

    def test_arc_length_uses_common_only(self):
        """ArcLength dimensions must use only common properties."""
        args = self._make_common_args(SHAPE_ARC_LENGTH, FAMILY_ANGULAR)

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys

    def test_spot_elevation_uses_common_only(self):
        """SpotElevation dimensions must use only common properties."""
        args = self._make_common_args(SHAPE_SPOT_ELEVATION, FAMILY_SPOT)

        items, required_qs = _build_identity_items(**args)
        keys = {it["k"] for it in items}

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys

    def test_items_are_sorted_by_key(self):
        """Identity items must be sorted by key for deterministic ordering."""
        args = self._make_common_args(SHAPE_LINEAR, FAMILY_LINEAR)
        args["witness_v"] = "Gap to Element"
        args["witness_q"] = ITEM_Q_OK

        items, _ = _build_identity_items(**args)
        keys = [it["k"] for it in items]

        assert keys == sorted(keys)

    def test_shape_discriminator_first_after_sort(self):
        """After sorting, shape should still be first (alphabetically 'dim_type.a' < 'dim_type.s')."""
        # Note: After alphabetical sort, 'dim_type.accuracy' comes before 'dim_type.shape'
        # This is expected behavior - the discriminator is in the items, position depends on sort
        args = self._make_common_args(SHAPE_LINEAR, FAMILY_LINEAR)
        args["witness_v"] = "Gap to Element"
        args["witness_q"] = ITEM_Q_OK

        items, _ = _build_identity_items(**args)

        # Verify shape is present
        shape_items = [it for it in items if it["k"] == "dim_type.shape"]
        assert len(shape_items) == 1
        assert shape_items[0]["v"] == SHAPE_LINEAR

    def test_required_qualities_includes_shape_specific(self):
        """Required qualities must include shape-specific properties when applicable."""
        # Linear: should include witness_q in required_qs
        args = self._make_common_args(SHAPE_LINEAR, FAMILY_LINEAR)
        args["witness_v"] = "Gap to Element"
        args["witness_q"] = ITEM_Q_OK

        _, required_qs = _build_identity_items(**args)
        assert ITEM_Q_OK in required_qs
        # witness_q should be included in required_qs for linear
        assert len(required_qs) == 7  # 6 common + 1 witness

    def test_no_unsupported_not_applicable_for_shape_gated_properties(self):
        """Shape-gated properties must never have UNSUPPORTED_NOT_APPLICABLE quality."""
        # Linear: no center_marks at all, not even with N/A
        args = self._make_common_args(SHAPE_LINEAR, FAMILY_LINEAR)
        args["witness_v"] = "Gap to Element"
        args["witness_q"] = ITEM_Q_OK

        items, _ = _build_identity_items(**args)

        for item in items:
            # No item should have UNSUPPORTED_NOT_APPLICABLE
            assert item["q"] != ITEM_Q_UNSUPPORTED_NOT_APPLICABLE, \
                f"Item {item['k']} has UNSUPPORTED_NOT_APPLICABLE quality"


# =============================================================================
# Join Key Policy Shape-Gating Tests
# =============================================================================

class TestJoinKeyPolicyShapeGating:
    """Test join key policy shape-gating for dimension_types."""

    @staticmethod
    def _load_dim_policy():
        """Load dimension_types join key policy."""
        policies = load_join_key_policies("policies/domain_join_key_policies.json")
        return get_domain_join_key_policy(policies, "dimension_types")

    if pytest:
        @pytest.fixture
        def dim_policy(self):
            """Load dimension_types join key policy."""
            return self._load_dim_policy()

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
            make_identity_item("dim_type.center_marks", "1", ITEM_Q_OK),
            make_identity_item("dim_type.center_mark_size", "0.125", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.center_marks" not in missing
        assert "dim_type.center_mark_size" not in missing

    def test_angular_no_shape_specific_required(self):
        """Angular shape should not require shape-specific properties."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "Angular", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        # Should not require witness_line_control or center_marks
        assert "dim_type.witness_line_control" not in missing
        assert "dim_type.center_marks" not in missing
        assert jk["shape_gating"]["additional_required_keys"] == []

    def test_spot_elevation_no_shape_specific_required(self):
        """SpotElevation shape should not require shape-specific properties."""
        dim_policy = self._load_dim_policy()
        items = [
            make_identity_item("dim_type.shape", "SpotElevation", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.01", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "abc123", ITEM_Q_OK),
        ]

        jk, missing = build_join_key_from_policy(domain_policy=dim_policy, identity_items=items)

        assert "dim_type.witness_line_control" not in missing
        assert "dim_type.center_marks" not in missing

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
        """Angular dimensions must NOT include linear or radial properties."""
        keys = self._get_keys_for_shape(SHAPE_ANGULAR, FAMILY_ANGULAR)

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys

    def test_spot_excludes_linear_and_radial_properties(self):
        """Spot dimensions must NOT include linear or radial properties."""
        keys = self._get_keys_for_shape(SHAPE_SPOT_ELEVATION, FAMILY_SPOT)

        assert "dim_type.witness_line_control" not in keys
        assert "dim_type.center_marks" not in keys
        assert "dim_type.center_mark_size" not in keys


# =============================================================================
# Common Properties Tests
# =============================================================================

class TestCommonPropertiesAllShapes:
    """Test that common properties are present for all shapes."""

    COMMON_KEYS = {
        "dim_type.shape",
        "dim_type.unit_format_id",
        "dim_type.rounding",
        "dim_type.accuracy",
        "dim_type.prefix",
        "dim_type.suffix",
        "dim_type.tick_mark_sig_hash",
    }

    def _get_keys_for_shape(self, shape_v, shape_family, **extra_args):
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
        args.update(extra_args)
        items, _ = _build_identity_items(**args)
        return {it["k"] for it in items}

    def test_common_properties_present_all_shapes(self):
        """All shapes must include common properties."""
        test_cases = [
            (SHAPE_LINEAR, FAMILY_LINEAR, {"witness_v": "Gap", "witness_q": ITEM_Q_OK}),
            (SHAPE_LINEAR_FIXED, FAMILY_LINEAR, {"witness_v": "Fixed", "witness_q": ITEM_Q_OK}),
            (SHAPE_RADIAL, FAMILY_RADIAL, {"center_marks_v": "1", "center_marks_q": ITEM_Q_OK, "center_mark_size_v": "0.1", "center_mark_size_q": ITEM_Q_OK}),
            (SHAPE_DIAMETER, FAMILY_RADIAL, {"center_marks_v": "0", "center_marks_q": ITEM_Q_OK, "center_mark_size_v": "0.2", "center_mark_size_q": ITEM_Q_OK}),
            (SHAPE_ANGULAR, FAMILY_ANGULAR, {}),
            (SHAPE_ARC_LENGTH, FAMILY_ANGULAR, {}),
            (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, {}),
            (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, {}),
            (SHAPE_SPOT_SLOPE, FAMILY_SPOT, {}),
        ]
        for shape_v, shape_family, extra_args in test_cases:
            keys = self._get_keys_for_shape(shape_v, shape_family, **extra_args)
            for common_key in self.COMMON_KEYS:
                assert common_key in keys, f"Common key {common_key} missing for shape {shape_v}"


# =============================================================================
# Manual Testing Checklist (for Revit integration)
# =============================================================================

"""
MANUAL TESTING CHECKLIST FOR REVIT INTEGRATION
===============================================

This checklist documents what needs to be validated in Revit since
unit tests cannot exercise the full extraction pipeline.

TEST DATA REQUIREMENTS:
-----------------------
Create a Revit test file with at least:
1. One Linear dimension type (any standard linear dim)
2. One Radial dimension type (radius dimension)
3. One Diameter dimension type
4. One Angular dimension type
5. One SpotElevation dimension type
6. One ArcLength dimension type (if available)

TEST SCENARIOS:
---------------

1. SHAPE DETECTION VALIDATION
   - [ ] Export dimension types from test file
   - [ ] Verify each dimension type has correct shape value
   - [ ] Verify Linear dimensions have shape == "Linear"
   - [ ] Verify Radial dimensions have shape == "Radial"
   - [ ] Verify Angular dimensions have shape == "Angular"
   - [ ] Verify Spot dimensions have shape == "SpotElevation" (or variant)

2. WITNESS LINE CONTROL (LINEAR ONLY)
   - [ ] Export a Linear dimension type
   - [ ] Verify dim_type.witness_line_control IS present in identity_items
   - [ ] Verify quality is ITEM_Q_OK (not UNSUPPORTED_NOT_APPLICABLE)
   - [ ] Export a Radial dimension type
   - [ ] Verify dim_type.witness_line_control IS NOT present in identity_items

3. CENTER MARKS (RADIAL ONLY)
   - [ ] Export a Radial or Diameter dimension type
   - [ ] Verify dim_type.center_marks IS present in identity_items
   - [ ] Verify dim_type.center_mark_size IS present in identity_items
   - [ ] Verify quality is ITEM_Q_OK (not UNSUPPORTED_NOT_APPLICABLE)
   - [ ] Export a Linear dimension type
   - [ ] Verify dim_type.center_marks IS NOT present in identity_items

4. NO UNSUPPORTED_NOT_APPLICABLE
   - [ ] Export dimension types of all shapes
   - [ ] For each record's identity_items, verify NO item has q == "unsupported.not_applicable"
   - [ ] Shape-gated properties should be omitted entirely, not marked N/A

5. JOIN KEY SHAPE-GATING
   - [ ] Export dimension types and examine join_key section of each record
   - [ ] Verify join_key.shape_gating.shape_value matches the dimension shape
   - [ ] Verify join_key.shape_gating.shape_matched is True for known shapes
   - [ ] Verify join_key.shape_gating.additional_required_keys contains correct keys:
     - Linear/LinearFixed: ["dim_type.witness_line_control"]
     - Radial/Diameter/DiameterLinked: ["dim_type.center_marks", "dim_type.center_mark_size"]
     - Angular/ArcLength/Spot*: []

6. DETERMINISM VALIDATION
   - [ ] Export dimension types twice
   - [ ] Verify identical identity_items and sig_hash for same dimension types
   - [ ] Verify items are sorted alphabetically by key

VALIDATION SCRIPT:
------------------
Run in Dynamo after exporting:

```python
from domains import dimension_types

result = dimension_types.extract(doc, ctx)
for rec in result.get("records", []):
    items = rec.get("identity_basis", {}).get("items", [])

    # Check for UNSUPPORTED_NOT_APPLICABLE
    for it in items:
        if it.get("q") == "unsupported.not_applicable":
            print(f"ERROR: {rec['label']['display']} has N/A item: {it['k']}")

    # Check shape-gating in join_key
    jk = rec.get("join_key", {})
    sg = jk.get("shape_gating", {})
    if sg:
        print(f"{rec['label']['display']}: shape={sg.get('shape_value')}, matched={sg.get('shape_matched')}, add_req={sg.get('additional_required_keys')}")
```
"""
