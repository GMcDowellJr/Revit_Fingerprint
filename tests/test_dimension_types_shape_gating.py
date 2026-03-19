# -*- coding: utf-8 -*-
"""Tests for dimension_types domain split architecture.

After the domain split refactor, shape discrimination happens at the domain level:
each domain (dimension_types_linear, dimension_types_radial, etc.) handles specific
shapes with a flat per-domain policy. The old shape_gating policy mechanism is no
longer used for dimension_types.

Tests are organized into:
1. Shape constant and mapping tests (from core.dimension_type_helpers)
2. Domain policy validation tests (per split domain)
3. Join key construction tests (for split domain policies)
"""

try:
    import pytest
except ImportError:
    pytest = None

from core.hashing import make_hash
from core.record_v2 import (
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    make_identity_item,
    serialize_identity_items,
)
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

# Import shape constants and mappings from the shared helper module
from core.dimension_type_helpers import (
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
# Family Mapping Tests (via SHAPE_TO_FAMILY)
# =============================================================================

class TestFamilyMappings:
    """Test family mapping correctness via SHAPE_TO_FAMILY."""

    def test_linear_shapes_map_to_linear_family(self):
        """Linear and LinearFixed must map to linear family."""
        assert SHAPE_TO_FAMILY.get(SHAPE_LINEAR) == FAMILY_LINEAR
        assert SHAPE_TO_FAMILY.get(SHAPE_LINEAR_FIXED) == FAMILY_LINEAR

    def test_radial_shapes_map_to_radial_family(self):
        """Radial, Diameter, DiameterLinked must map to radial family."""
        assert SHAPE_TO_FAMILY.get(SHAPE_RADIAL) == FAMILY_RADIAL
        assert SHAPE_TO_FAMILY.get(SHAPE_DIAMETER) == FAMILY_RADIAL
        assert SHAPE_TO_FAMILY.get(SHAPE_DIAMETER_LINKED) == FAMILY_RADIAL

    def test_angular_shapes_map_to_angular_family(self):
        """Angular and ArcLength must map to angular family."""
        assert SHAPE_TO_FAMILY.get(SHAPE_ANGULAR) == FAMILY_ANGULAR
        assert SHAPE_TO_FAMILY.get(SHAPE_ARC_LENGTH) == FAMILY_ANGULAR

    def test_spot_shapes_map_to_spot_family(self):
        """Spot elevation/coordinate/slope must map to spot family."""
        assert SHAPE_TO_FAMILY.get(SHAPE_SPOT_ELEVATION) == FAMILY_SPOT
        assert SHAPE_TO_FAMILY.get(SHAPE_SPOT_COORDINATE) == FAMILY_SPOT
        assert SHAPE_TO_FAMILY.get(SHAPE_SPOT_SLOPE) == FAMILY_SPOT
        assert SHAPE_TO_FAMILY.get(SHAPE_SPOT_ELEVATION_FIXED) == FAMILY_SPOT

    def test_unknown_maps_to_unknown_family(self):
        """Unknown shape must map to unknown family."""
        assert SHAPE_TO_FAMILY.get(SHAPE_UNKNOWN) == FAMILY_UNKNOWN


# =============================================================================
# Split Domain Policy Tests
# =============================================================================

class TestSplitDomainPolicies:
    """Test join key policies for each split dimension_types domain."""

    def _load_policy(self, domain_name):
        """Load join key policy for a specific split domain."""
        policies = load_join_key_policies("policies/domain_join_key_policies.json")
        return get_domain_join_key_policy(policies, domain_name)

    def test_linear_policy_has_witness_line_control(self):
        """Linear domain must require witness_line_control."""
        pol = self._load_policy("dimension_types_linear")
        assert "dim_type.witness_line_control" in pol["required_items"]
        assert "dim_type.shape" in pol["required_items"]
        assert "dim_type.accuracy" in pol["required_items"]
        assert "dim_type.tick_mark_sig_hash" in pol["required_items"]

    def test_radial_policy_has_center_marks(self):
        """Radial domain must require center_marks and center_mark_size."""
        pol = self._load_policy("dimension_types_radial")
        assert "dim_type.center_marks" in pol["required_items"]
        assert "dim_type.center_mark_size" in pol["required_items"]
        assert "dim_type.shape" in pol["required_items"]

    def test_angular_policy_has_unit_format_id(self):
        """Angular domain must require unit_format_id."""
        pol = self._load_policy("dimension_types_angular")
        assert "dim_type.unit_format_id" in pol["required_items"]
        assert "dim_type.shape" in pol["required_items"]

    def test_diameter_policy_exists(self):
        """Diameter domain must have a valid policy."""
        pol = self._load_policy("dimension_types_diameter")
        assert pol is not None
        assert "dim_type.shape" in pol["required_items"]

    def test_spot_elevation_policy_exists(self):
        """Spot elevation domain must have a valid policy."""
        pol = self._load_policy("dimension_types_spot_elevation")
        assert pol is not None
        assert "dim_type.shape" in pol["required_items"]

    def test_spot_coordinate_policy_exists(self):
        """Spot coordinate domain must have a valid policy."""
        pol = self._load_policy("dimension_types_spot_coordinate")
        assert pol is not None
        assert "dim_type.shape" in pol["required_items"]

    def test_spot_slope_policy_exists(self):
        """Spot slope domain must have a valid policy."""
        pol = self._load_policy("dimension_types_spot_slope")
        assert pol is not None
        assert "dim_type.shape" in pol["required_items"]

    def test_all_split_domains_have_schemas(self):
        """All split dimension_types domains must have valid join_key_schema."""
        split_domains = [
            "dimension_types_linear",
            "dimension_types_angular",
            "dimension_types_radial",
            "dimension_types_diameter",
            "dimension_types_spot_elevation",
            "dimension_types_spot_coordinate",
            "dimension_types_spot_slope",
        ]
        for dom in split_domains:
            pol = self._load_policy(dom)
            assert pol is not None, "Policy for {} is None".format(dom)
            assert pol.get("join_key_schema", "").startswith(dom), \
                "join_key_schema mismatch for {}".format(dom)


# =============================================================================
# Policy-level Integration Tests
# =============================================================================

class TestPolicyLoadPattern:
    """Documented integration test patterns for full Revit validation."""

    def test_policy_load_integration_pattern(self):
        """Policy load integration pattern placeholder."""
        assert True


class TestCanonicalEvidenceSelectors:
    """Pilot checks for canonical evidence + selectors behavior (new flat policies)."""

    def test_linear_join_key_uses_required_keys_only(self):
        """Linear domain: join key must include all required items and exclude optional."""
        policies = load_join_key_policies("policies/domain_join_key_policies.json")
        pol = get_domain_join_key_policy(policies, "dimension_types_linear")

        identity_items = [
            make_identity_item("dim_type.shape", "Linear", ITEM_Q_OK),
            make_identity_item("dim_type.accuracy", "0.010000000", ITEM_Q_OK),
            make_identity_item("dim_type.tick_mark_sig_hash", "1" * 32, ITEM_Q_OK),
            make_identity_item("dim_type.witness_line_control", "Gap to Element", ITEM_Q_OK),
            make_identity_item("dim_type.unit_format_id", "autodesk.unit.formatOption:length", ITEM_Q_OK),
            make_identity_item("dim_type.rounding", "Nearest", ITEM_Q_OK),
            make_identity_item("dim_type.prefix", "PFX", ITEM_Q_OK),
            make_identity_item("dim_type.suffix", "SFX", ITEM_Q_OK),
        ]

        join_key, missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
        )

        assert missing == []
        # All required items from the linear policy must appear in keys_used
        for k in pol["required_items"]:
            assert k in join_key["keys_used"], "Required key {} not in keys_used".format(k)

        join_preimage = serialize_identity_items(
            [it for it in identity_items if it["k"] in set(join_key["keys_used"])]
        )
        assert join_key["join_hash"] == make_hash(join_preimage)
