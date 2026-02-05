# -*- coding: utf-8 -*-

import sys
import types

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items

# Domain module imports Autodesk.Revit.DB at import time; stub for unit-test environment.
autodesk = types.ModuleType("Autodesk")
revit = types.ModuleType("Autodesk.Revit")
db = types.ModuleType("Autodesk.Revit.DB")
setattr(db, "ElementId", object)
setattr(db, "View", object)
setattr(db, "ViewSchedule", object)
sys.modules.setdefault("Autodesk", autodesk)
sys.modules.setdefault("Autodesk.Revit", revit)
sys.modules.setdefault("Autodesk.Revit.DB", db)

from domains.view_filter_applications_view_templates import _semantic_keys_from_identity_items


def _policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "view_filter_applications_view_templates")


def test_view_filter_applications_view_templates_uses_canonical_selectors_for_join_and_sig():
    canonical_items = [
        make_identity_item("vfa.template_uid_or_namekey", "uid-1", ITEM_Q_OK),
        make_identity_item("vfa.filter_stack_count", "1", ITEM_Q_OK),
        make_identity_item("vfa.stack[000].filter_sig_hash", "abc", ITEM_Q_OK),
        make_identity_item("vfa.stack[000].visible", "true", ITEM_Q_OK),
        make_identity_item("vfa.stack[000].enabled", "true", ITEM_Q_OK),
        make_identity_item("vfa.stack_def_hash", "0123456789abcdef0123456789abcdef", ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == ["vfa.stack_def_hash"]
    # Back-compat retained field contains only hashed items.
    assert [it["k"] for it in join_key["items"]] == ["vfa.stack_def_hash"]

    hashed_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(hashed_items))

    semantic_keys = _semantic_keys_from_identity_items(canonical_items)
    assert "vfa.stack_def_hash" not in semantic_keys

    semantic_items = [it for it in canonical_items if it.get("k") in set(semantic_keys)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))
    assert sig_hash != join_key["join_hash"]
