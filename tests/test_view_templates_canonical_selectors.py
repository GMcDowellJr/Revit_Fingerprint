# -*- coding: utf-8 -*-

import sys
import types

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import serialize_identity_items

# Domain module imports Autodesk.Revit.DB at import time; stub for unit-test environment.
autodesk = types.ModuleType("Autodesk")
revit = types.ModuleType("Autodesk.Revit")
db = types.ModuleType("Autodesk.Revit.DB")
setattr(db, "View", object)
setattr(db, "ViewSchedule", object)
setattr(db, "BuiltInParameter", object)
setattr(db, "GraphicsStyleType", object)
setattr(db, "Category", object)
setattr(db, "OverrideGraphicSettings", object)
setattr(db, "ElementId", object)
sys.modules.setdefault("Autodesk", autodesk)
sys.modules.setdefault("Autodesk.Revit", revit)
sys.modules.setdefault("Autodesk.Revit.DB", db)

from domains.view_templates import _canonical_identity_items_from_signature, _semantic_keys_from_identity_items


def test_view_templates_shape_gate_requires_phase_filter_when_include_phase_filter_true():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    policy = get_domain_join_key_policy(policies, "view_templates")

    canonical_items = _canonical_identity_items_from_signature(
        "0123456789abcdef0123456789abcdef",
        [
            "include_phase_filter=True",
            "phase_filter=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "include_filters=True",
            "include_vg=False",
        ],
        override_stack_hash="fedcba9876543210fedcba9876543210",
    )

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=True,
        emit_selectors=True,
    )

    assert missing == []
    if policy.get("join_key_schema") == "view_templates.join_key.v2":
        assert "view_template.def_hash" not in join_key["keys_used"]
        assert "view_template.sig.include_phase_filter" in join_key["keys_used"]
        assert "view_template.sig.phase_filter" in join_key["keys_used"]

        semantic_keys = _semantic_keys_from_identity_items(canonical_items)
        semantic_items = [it for it in canonical_items if it.get("k") in set(semantic_keys)]
        sig_hash = make_hash(serialize_identity_items(semantic_items))
        assert sig_hash != join_key["join_hash"]
    else:
        assert join_key["keys_used"] == ["view_template.def_hash"]


def test_view_templates_shape_gate_skips_phase_filter_when_include_phase_filter_false():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    policy = get_domain_join_key_policy(policies, "view_templates")

    canonical_items = _canonical_identity_items_from_signature(
        "0123456789abcdef0123456789abcdef",
        [
            "include_phase_filter=False",
            "include_filters=True",
            "include_vg=False",
        ],
    )

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=True,
        emit_selectors=True,
    )

    assert missing == []
    if policy.get("join_key_schema") == "view_templates.join_key.v2":
        assert "view_template.sig.include_phase_filter" in join_key["keys_used"]
        assert "view_template.sig.phase_filter" not in join_key["keys_used"]
    else:
        assert join_key["keys_used"] == ["view_template.def_hash"]
