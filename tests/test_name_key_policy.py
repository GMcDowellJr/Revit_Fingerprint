# -*- coding: utf-8 -*-
"""Tests for the Canonical Name Identity Projection (PR1).

Covers: eligibility allow-list, phases redundancy marker, status-field vocabulary,
non-inclusion in segmentation (DIMENSION_CONFIG), and the analysis-side reconstruction
path (core/name_key_builder.py).
"""
import json
from pathlib import Path

import pytest

from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy, compute_projection_status
from core.name_key_builder import build_name_key_for_record, flat_items_for_record

REPO_ROOT = Path(__file__).resolve().parent.parent
POLICY_PATH = REPO_ROOT / "policies" / "domain_name_key_policies.json"

EXPECTED_ELIGIBLE_DOMAINS = {
    "phases",
    "materials",
    "text_types",
    "wall_types",
    "floor_types",
    "roof_types",
    "ceiling_types",
    "identity",
    "phase_filters",
    "line_patterns",
    "fill_patterns_drafting",
    "fill_patterns_model",
    "arrowheads",
    "loaded_family_types",
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
    "view_filter_definitions",
    "dimension_types_linear",
    "dimension_types_angular",
    "dimension_types_radial",
    "dimension_types_diameter",
    "dimension_types_spot_slope",
}

EXCLUDED_NO_NAME_LIKE_KEY = {
    "units",
    "line_styles",
    "object_styles_analytical",
    "object_styles_annotation",
    "object_styles_imported",
    "object_styles_model",
    "view_category_overrides",
    "view_category_overrides_annotation",
    "view_category_overrides_model",
}

EXCLUDED_REFERENCED_ELEMENT_NAME = {
    "dimension_types_spot_coordinate",
    "dimension_types_spot_elevation",
}

EXCLUDED_UID_SHAPED_ONLY_CANDIDATE = {
    "view_filter_applications_view_templates",
}


@pytest.fixture(scope="module")
def name_key_policies():
    return load_join_key_policies(str(POLICY_PATH))


def test_policy_file_loads_and_validates(name_key_policies):
    assert isinstance(name_key_policies, dict)
    assert isinstance(name_key_policies.get("domains"), dict)


def test_eligibility_allow_list_matches_exactly(name_key_policies):
    domains = set(name_key_policies["domains"].keys())
    assert domains == EXPECTED_ELIGIBLE_DOMAINS


def test_excluded_domains_have_no_policy_entry(name_key_policies):
    excluded = (
        EXCLUDED_NO_NAME_LIKE_KEY
        | EXCLUDED_REFERENCED_ELEMENT_NAME
        | EXCLUDED_UID_SHAPED_ONLY_CANDIDATE
    )
    domains = set(name_key_policies["domains"].keys())
    assert not (domains & excluded), "excluded domains must not appear in the policy file at all"


def test_every_eligible_entry_has_exactly_one_required_item(name_key_policies):
    # Every entry in this PR keys off a single own-name identity item.
    for domain_name, pol in name_key_policies["domains"].items():
        assert pol["required_items"], f"{domain_name} must have a required item"
        assert len(pol["required_items"]) == 1, f"{domain_name} should have exactly one required item"


def test_phases_carries_explicit_redundancy_marker(name_key_policies):
    pol = get_domain_join_key_policy(name_key_policies, "phases")
    assert pol is not None
    assert pol["join_key_schema"] == "phases.name_identity.join_key.v1.redundant"
    assert pol["required_items"] == ["phase.name"]


def test_non_phases_entries_use_bare_schema(name_key_policies):
    for domain_name, pol in name_key_policies["domains"].items():
        if domain_name == "phases":
            continue
        assert pol["join_key_schema"] == "name_identity.join_key.v1", domain_name


class TestStatusVocabulary:
    def test_missing_policy(self):
        assert compute_projection_status(None, []) == "missing_policy"

    def test_blocked_when_no_required_items_configured(self):
        assert compute_projection_status({"required_items": []}, []) == "blocked"

    def test_missing_required_when_required_item_absent(self):
        pol = {"required_items": ["material.name"]}
        assert compute_projection_status(pol, ["material.name"]) == "missing_required"

    def test_ok_when_required_items_present(self):
        pol = {"required_items": ["material.name"]}
        assert compute_projection_status(pol, []) == "ok"


class TestDimensionConfigNonInclusion:
    def test_dimension_config_has_no_name_key_field(self):
        segment_manifest_src = (REPO_ROOT / "tools" / "build_segment_manifest.py").read_text(encoding="utf-8")
        import re
        m = re.search(r"DIMENSION_CONFIG\s*=\s*\[(.*?)\n\]", segment_manifest_src, re.DOTALL)
        assert m, "DIMENSION_CONFIG not found in tools/build_segment_manifest.py"
        block = m.group(1)
        assert "name_key" not in block
        assert "name_identity" not in block

    def test_dimension_config_fields_unchanged(self):
        # Read-only verification per PR1 scope: DIMENSION_CONFIG itself must not change.
        import importlib
        bsm = importlib.import_module("tools.build_segment_manifest")
        fields = {d["field"] for d in bsm.DIMENSION_CONFIG}
        assert fields == {
            "unit_system",
            "governance_role",
            "client_label",
            "discipline_label",
            "business_center_label",
        }


class TestAnalysisSideReconstruction:
    def test_native_domain_materials(self, name_key_policies):
        rec = {
            "domain": "materials",
            "label": {"display": "Concrete, Cast-in-Place"},
            "identity_basis": {"items": [{"k": "material.name", "v": "Concrete, Cast-in-Place", "q": "ok"}]},
        }
        name_key = build_name_key_for_record(rec, "materials", name_key_policies)
        assert name_key["status"] == "ok"
        assert name_key["join_hash"] is not None
        assert name_key["schema"] == "name_identity.join_key.v1"

    def test_widened_domain_phase_filters_reads_coordination_bucket(self, name_key_policies):
        rec = {
            "domain": "phase_filters",
            "label": {"display": "Existing"},
            "identity_basis": {"items": [{"k": "phase_filter.new.presentation_id", "v": "1", "q": "ok"}]},
            "phase2": {"coordination_items": [{"k": "phase_filter.name", "v": "Existing", "q": "ok"}]},
        }
        name_key = build_name_key_for_record(rec, "phase_filters", name_key_policies)
        assert name_key["status"] == "ok"
        assert name_key["join_hash"] is not None

    def test_label_only_domain_arrowheads_reads_raw_component(self, name_key_policies):
        rec = {
            "domain": "arrowheads",
            "label": {
                "display": "Arrow Filled 15deg",
                "components": {"type_id": "123", "type_name": "Arrow Filled 15deg"},
            },
            "identity_basis": {"items": [{"k": "arrowhead.style", "v": "Arrow", "q": "ok"}]},
        }
        name_key = build_name_key_for_record(rec, "arrowheads", name_key_policies)
        assert name_key["status"] == "ok"
        assert name_key["join_hash"] is not None

    def test_loaded_family_types_reads_raw_family_name_not_decorated_display(self, name_key_policies):
        # label.display is decorated ("category : family"); the inline extractor hashes the
        # raw family name only (label.components.family_name) -- the reconstruction must match.
        rec_decorated = {
            "domain": "loaded_family_types",
            "label": {
                "display": "Doors : Single-Flush",
                "components": {"category": "Doors", "family_name": "Single-Flush"},
            },
            "identity_basis": {"items": [{"k": "lft.shape_gate.category", "v": "Doors", "q": "ok"}]},
        }
        rec_bare = {
            "domain": "loaded_family_types",
            "label": {"display": "Single-Flush", "components": {"category": "", "family_name": "Single-Flush"}},
            "identity_basis": {"items": [{"k": "lft.shape_gate.category", "v": "Doors", "q": "ok"}]},
        }
        name_key_decorated = build_name_key_for_record(rec_decorated, "loaded_family_types", name_key_policies)
        name_key_bare = build_name_key_for_record(rec_bare, "loaded_family_types", name_key_policies)
        assert name_key_decorated["join_hash"] == name_key_bare["join_hash"]

    def test_view_filter_definitions_reads_raw_name_not_decorated_display(self, name_key_policies):
        # label.display is decorated ("View Filter Definition (Foo)"); the inline extractor
        # hashes label.components.name only -- the reconstruction must match.
        rec = {
            "domain": "view_filter_definitions",
            "label": {"display": "View Filter Definition (Foo)", "components": {"name": "Foo"}},
            "identity_basis": {"items": [{"k": "vf.def_hash", "v": "abc123", "q": "ok"}]},
        }
        name_key = build_name_key_for_record(rec, "view_filter_definitions", name_key_policies)
        assert name_key["status"] == "ok"

        from core.record_v2 import canonicalize_str
        from core.join_key_builder import build_join_key_from_policy
        from core.join_key_policy import get_domain_join_key_policy

        raw_v, raw_q = canonicalize_str("Foo")
        pol = get_domain_join_key_policy(name_key_policies, "view_filter_definitions")
        expected, _ = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=[
                {"k": "vf.def_hash", "v": "abc123", "q": "ok"},
                {"k": "vf.name", "v": raw_v, "q": raw_q},
            ],
            include_optional_items=False,
            hash_optional_items=False,
        )
        assert name_key["join_hash"] == expected["join_hash"]

    def test_ineligible_domain_returns_none(self, name_key_policies):
        rec = {
            "domain": "units",
            "label": {"display": "Units (Area)"},
            "identity_basis": {"items": [{"k": "units.spec", "v": "Area", "q": "ok"}]},
        }
        assert build_name_key_for_record(rec, "units", name_key_policies) is None

    def test_missing_name_yields_missing_required_status(self, name_key_policies):
        rec = {
            "domain": "materials",
            "label": {"display": ""},
            "identity_basis": {"items": []},
        }
        name_key = build_name_key_for_record(rec, "materials", name_key_policies)
        assert name_key["status"] == "missing_required"
        assert "material.name" in name_key["missing_required"]

    def test_flat_items_for_record_merges_all_buckets(self):
        rec = {
            "identity_basis": {"items": [{"k": "a", "v": "1", "q": "ok"}]},
            "phase2": {
                "cosmetic_items": [{"k": "b", "v": "2", "q": "ok"}],
                "coordination_items": [{"k": "c", "v": "3", "q": "ok"}],
                "unknown_items": [{"k": "d", "v": "4", "q": "ok"}],
            },
        }
        items = flat_items_for_record(rec)
        keys = {it["k"] for it in items}
        assert keys == {"a", "b", "c", "d"}
