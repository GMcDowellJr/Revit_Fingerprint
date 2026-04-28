# -*- coding: utf-8 -*-

import sys
import types
from pathlib import Path

from core.record_v2 import ITEM_Q_OK, make_identity_item


def _install_revit_stubs():
    autodesk = types.ModuleType("Autodesk")
    revit = types.ModuleType("Autodesk.Revit")
    db = types.ModuleType("Autodesk.Revit.DB")
    setattr(db, "ElementId", object)
    setattr(db, "View", object)
    setattr(db, "ViewSchedule", object)
    setattr(db, "OverrideGraphicSettings", object)
    setattr(db, "CategoryType", object)
    setattr(db, "GraphicsStyleType", object)
    sys.modules.setdefault("Autodesk", autodesk)
    sys.modules.setdefault("Autodesk.Revit", revit)
    sys.modules.setdefault("Autodesk.Revit.DB", db)


def test_vfa_phase2_payload_carries_semantic_keys():
    text = Path("domains/view_filter_applications_view_templates.py").read_text(encoding="utf-8")
    assert '"semantic_keys": semantic_keys' in text


def test_view_templates_signatures_capture_filter_enabled_and_workset_visibility():
    text = Path("domains/view_templates.py").read_text(encoding="utf-8")
    assert "GetIsFilterEnabled" in text
    assert "filter[{}].enabled" in text
    assert "GetWorksetVisibility" in text
    assert "vts.workset[{}].visibility" in text


def test_vco_category_hidden_is_semantic_for_model_and_annotation():
    _install_revit_stubs()
    from domains import view_category_overrides_model as vco_model
    from domains import view_category_overrides_annotation as vco_annotation

    items = [
        make_identity_item("vco.category_hidden", "true", ITEM_Q_OK),
        make_identity_item("vco.projection.line_weight", "2", ITEM_Q_OK),
    ]

    model_semantic, model_cosmetic, _ = vco_model._phase2_partition_items(items)
    annotation_semantic, annotation_cosmetic, _ = vco_annotation._phase2_partition_items(items)

    assert any(it.get("k") == "vco.category_hidden" for it in model_semantic)
    assert any(it.get("k") == "vco.category_hidden" for it in annotation_semantic)
    assert any(it.get("k") == "vco.projection.line_weight" for it in model_cosmetic)
    assert any(it.get("k") == "vco.projection.line_weight" for it in annotation_cosmetic)


def test_object_styles_model_semantic_keys_include_material_sig_hash():
    _install_revit_stubs()
    from domains import object_styles

    assert "obj_style.material_sig_hash" in object_styles._MODEL_SEMANTIC_KEYS
