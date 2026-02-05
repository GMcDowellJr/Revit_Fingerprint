# -*- coding: utf-8 -*-

import sys
import types

from core.hashing import make_hash
from core.phase2 import phase2_join_hash
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

from domains.view_templates import (
    _canonical_identity_items_from_signature,
    _join_key_from_canonical_items,
    _semantic_keys_from_identity_items,
)


def test_view_templates_uses_canonical_selectors_for_join_and_sig():
    canonical_items = _canonical_identity_items_from_signature(
        "0123456789abcdef0123456789abcdef",
        [
            "include_filters=True",
            "include_vg=False",
            "filter_count=2",
        ],
        override_stack_hash="fedcba9876543210fedcba9876543210",
    )

    join_key = _join_key_from_canonical_items(canonical_items)
    assert join_key["keys_used"] == ["view_template.def_hash"]
    assert [it["k"] for it in join_key["items"]] == ["view_template.def_hash"]

    hashed_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == phase2_join_hash(hashed_items)

    semantic_keys = _semantic_keys_from_identity_items(canonical_items)
    assert "view_template.def_hash" not in semantic_keys
    assert "view_template.sig.include_filters" in semantic_keys

    semantic_items = [it for it in canonical_items if it.get("k") in set(semantic_keys)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))
    assert sig_hash != join_key["join_hash"]
