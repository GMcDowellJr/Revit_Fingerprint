# -*- coding: utf-8 -*-
"""Coverage for domains/object_styles.py's D-040 policy-driven sig_hash
resolution. extract_annotation() is exercised end to end with a minimal
mock category, bypassing doc.Settings.Categories traversal via the
extractor's own _collect_categories ctx-cache key, to prove: (1) the
existing _NON_MODEL_SEMANTIC_KEYS fallback still reproduces today's
sig_hash, and (2) ctx["sig_hash_policies"] actually drives the computed
hash when present -- keyed correctly off domain_name (there are 4 distinct
object_styles_* sig_hash policy entries, not one shared "object_styles").
"""
import importlib

from core.hashing import make_hash
from core.record_v2 import serialize_identity_items


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _Category(object):
    def __init__(self, name, category_type):
        self.Name = name
        self.CategoryType = category_type
        self.Id = _Id(42)


def _setup_module(monkeypatch):
    m = importlib.import_module("domains.object_styles")
    fake_gst = type("_GST", (), {"Projection": "Projection", "Cut": "Cut"})
    monkeypatch.setattr(m, "GraphicsStyleType", fake_gst)
    monkeypatch.setattr(m, "CategoryType", type("_CT", (), {"Model": "Model", "Annotation": "Annotation"}))
    # core/graphic_overrides.py holds its own module-level GraphicsStyleType
    # reference (imported independently from Autodesk.Revit.DB), read by
    # extract_projection_graphics()/extract_cut_graphics(). Some other test
    # in this suite installs a permanent process-wide fake Autodesk.Revit.DB
    # stub (module-level code, `sys.modules.setdefault(...)`, no teardown --
    # see tests/test_view_filter_applications_view_templates_canonical_selectors.py)
    # with GraphicsStyleType=object, so this module's own reference cannot be
    # assumed to start clean/None -- patch it directly rather than relying on
    # domains.object_styles' copy.
    import core.graphic_overrides as graphic_overrides
    monkeypatch.setattr(graphic_overrides, "GraphicsStyleType", fake_gst, raising=False)
    return m


class _Doc(object):
    def GetElement(self, eid):
        return None


def _ctx_with_category(m, name="Tags"):
    cat = _Category(name=name, category_type="Annotation")
    return {
        "_object_styles_categories_cache::annotation": [(cat, False, None)],
        "_domains": {"line_patterns": {"status": "ok"}},
    }


def test_basic_annotation_category_produces_record(monkeypatch):
    m = _setup_module(monkeypatch)
    ctx = _ctx_with_category(m)
    out = m.extract_annotation(_Doc(), ctx)
    assert out["count"] == 1
    rec = out["records"][0]
    assert rec["sig_hash"] is not None


def test_row_key_reflects_category_name_but_material_fields_absent_for_non_model(monkeypatch):
    m = _setup_module(monkeypatch)
    ctx = _ctx_with_category(m, name="Tags")
    rec = m.extract_annotation(_Doc(), ctx)["records"][0]
    keys = {it["k"] for it in rec["identity_basis"]["items"]}
    assert "obj_style.material_sig_hash" not in keys  # model-only field


def test_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_keyed_by_domain_name(monkeypatch):
    """Prove per-partition domain_name resolution: pointing a policy at
    "object_styles_annotation" (not a generic "object_styles" key) must be
    what actually changes extract_annotation()'s computed sig_hash."""
    m = _setup_module(monkeypatch)
    ctx = _ctx_with_category(m)
    ctx["sig_hash_policies"] = {
        "domains": {
            "object_styles_annotation": {
                "sig_hash_schema": "object_styles_annotation.sig_hash.v1",
                "hash_alg": "md5_utf8_join_pipe",
                "allowed_items": ["obj_style.row_key"],
                "allowed_item_prefixes": [],
                "required_items": [],
                "minima": {"block_if_any_required_not_ok": True},
            }
        }
    }
    rec = m.extract_annotation(_Doc(), ctx)["records"][0]
    row_key_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "obj_style.row_key"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(row_key_item))

    default_ctx = _ctx_with_category(m)
    default_rec = m.extract_annotation(_Doc(), default_ctx)["records"][0]
    assert rec["sig_hash"] != default_rec["sig_hash"]
