# -*- coding: utf-8 -*-

import importlib


class _Id(object):
    def __init__(self, iv):
        self.IntegerValue = iv


class _Color(object):
    def __init__(self, r, g, b):
        self.Red = r
        self.Green = g
        self.Blue = b


class _Param(object):
    def __init__(self, value):
        self._value = value
        self.HasValue = value is not None

    def AsString(self):
        return self._value

    def AsValueString(self):
        return self._value


class _FillPatternElem(object):
    def __init__(self, uid, name):
        self.UniqueId = uid
        self.Name = name


class _Mat(object):
    def __init__(self, *, uid="uid-1", eid=1, name="Concrete", material_class="Structure", use_render=True):
        self.UniqueId = uid
        self.Id = _Id(eid)
        self.Name = name
        self.MaterialClass = material_class

        self.UseRenderAppearanceForShading = use_render
        self.Color = _Color(12, 34, 56)
        self.Transparency = 15

        self.SurfaceForegroundPatternId = _Id(11)
        self.SurfaceForegroundPatternColor = _Color(1, 2, 3)
        self.SurfaceBackgroundPatternId = _Id(12)
        self.SurfaceBackgroundPatternColor = _Color(4, 5, 6)
        self.CutForegroundPatternId = _Id(13)
        self.CutForegroundPatternColor = _Color(7, 8, 9)
        self.CutBackgroundPatternId = _Id(14)
        self.CutBackgroundPatternColor = _Color(10, 11, 12)

        self._params = {
            "Description": _Param("desc"),
            "Comments": _Param("cmt"),
            "Keywords": _Param("kw"),
            "Manufacturer": _Param("mfg"),
            "Model": _Param("mdl"),
            "Cost": _Param("$1"),
            "URL": _Param("https://example.com"),
            "Keynote": _Param("key"),
            "Mark": _Param("mk"),
        }

    def LookupParameter(self, name):
        return self._params.get(name)


class _Doc(object):
    def __init__(self, elems_by_id=None):
        self._elems_by_id = elems_by_id or {}

    def GetElement(self, eid):
        iv = getattr(eid, "IntegerValue", None)
        return self._elems_by_id.get(iv)


def _identity_map(rec):
    items = (((rec or {}).get("identity_basis", {}) or {}).get("items", [])) or []
    return {it["k"]: it["v"] for it in items}


def _make_ctx_with_fill_patterns(m):
    return {
        m.CTX_FILL_PATTERN_ID_TO_VALUE: {
            "11": "hash-fg-s",
            "12": "hash-bg-s",
            "13": "hash-fg-c",
            "14": "hash-bg-c",
        }
    }


def test_materials_emits_records_and_hash(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [_Mat()])

    doc = _Doc({11: _FillPatternElem("fp-11", "FG"), 12: _FillPatternElem("fp-12", "BG"), 13: _FillPatternElem("fp-13", "CFG"), 14: _FillPatternElem("fp-14", "CBG")})
    ctx = _make_ctx_with_fill_patterns(m)

    result = m.extract(doc=doc, ctx=ctx)

    assert result["records"]
    assert result["record_rows"]
    assert result["hash_v2"] is not None
    assert result["count"] == 1


def test_identity_fields_captured_but_excluded_from_graphics_hash(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)

    a = _Mat(name="Concrete", material_class="A")
    b = _Mat(name="Renamed", material_class="B")
    monkeypatch.setattr(m, "collect_instances", lambda *a_, **k: [a])

    doc = _Doc({11: _FillPatternElem("fp-11", "FG"), 12: _FillPatternElem("fp-12", "BG"), 13: _FillPatternElem("fp-13", "CFG"), 14: _FillPatternElem("fp-14", "CBG")})
    ctx = _make_ctx_with_fill_patterns(m)
    r1 = m.extract(doc=doc, ctx=ctx)

    monkeypatch.setattr(m, "collect_instances", lambda *a_, **k: [b])
    r2 = m.extract(doc=doc, ctx=_make_ctx_with_fill_patterns(m))

    rec = r1["records"][0]
    im = _identity_map(rec)
    assert im["material.name"] == "Concrete"
    assert im["material.class"] == "A"
    assert rec["graphics_sig_hash_v2"] == r2["records"][0]["graphics_sig_hash_v2"]


def test_use_render_appearance_captured_but_not_hashed(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)

    a = _Mat(use_render=True)
    b = _Mat(use_render=False)
    doc = _Doc({11: _FillPatternElem("fp-11", "FG"), 12: _FillPatternElem("fp-12", "BG"), 13: _FillPatternElem("fp-13", "CFG"), 14: _FillPatternElem("fp-14", "CBG")})

    monkeypatch.setattr(m, "collect_instances", lambda *a_, **k: [a])
    r1 = m.extract(doc=doc, ctx=_make_ctx_with_fill_patterns(m))

    monkeypatch.setattr(m, "collect_instances", lambda *a_, **k: [b])
    r2 = m.extract(doc=doc, ctx=_make_ctx_with_fill_patterns(m))

    assert _identity_map(r1["records"][0])["material.use_render_appearance"] == "True"
    assert _identity_map(r2["records"][0])["material.use_render_appearance"] == "False"
    assert r1["records"][0]["graphics_sig_hash_v2"] == r2["records"][0]["graphics_sig_hash_v2"]


def test_color_and_transparency_are_displayed_values(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    mat = _Mat()
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [mat])
    doc = _Doc({11: _FillPatternElem("fp-11", "FG"), 12: _FillPatternElem("fp-12", "BG"), 13: _FillPatternElem("fp-13", "CFG"), 14: _FillPatternElem("fp-14", "CBG")})

    result = m.extract(doc=doc, ctx=_make_ctx_with_fill_patterns(m))
    im = _identity_map(result["records"][0])
    assert im["material.shading_color_rgb"] == "12,34,56"
    assert im["material.shading_transparency"] == "15"


def test_no_pattern_element_id_minus_one_maps_to_none(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    mat = _Mat()
    mat.SurfaceForegroundPatternId = _Id(-1)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [mat])

    result = m.extract(doc=_Doc({}), ctx=_make_ctx_with_fill_patterns(m))
    im = _identity_map(result["records"][0])
    assert im["material.surface_foreground_pattern.uid"] == "<NONE>"
    assert im["material.surface_foreground_pattern.name"] == "<NONE>"
    assert im["material.surface_foreground_pattern.sig_hash"] == "<NONE>"


def test_missing_fill_pattern_ctx_degrades_not_blocks(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [_Mat()])

    result = m.extract(doc=_Doc({}), ctx={})

    assert result["status"] == "degraded"
    assert result["records"]
    im = _identity_map(result["records"][0])
    assert im["material.surface_foreground_pattern.sig_hash"] == "<UNRESOLVED>"


def test_fill_pattern_ctx_resolution_populates_sig_hash(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [_Mat()])

    result = m.extract(doc=_Doc({11: _FillPatternElem("fp-11", "FG")}), ctx=_make_ctx_with_fill_patterns(m))
    im = _identity_map(result["records"][0])

    assert im["material.surface_foreground_pattern.sig_hash"] == "hash-fg-s"


def test_material_ctx_maps_populated(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [_Mat(uid="uid-1", eid=101, name="Concrete", material_class="Structure")])

    ctx = _make_ctx_with_fill_patterns(m)
    _ = m.extract(doc=_Doc({11: _FillPatternElem("fp-11", "FG")}), ctx=ctx)

    assert ctx[m.CTX_MATERIAL_ID_TO_UID]["101"] == "uid-1"
    assert ctx[m.CTX_MATERIAL_ID_TO_NAME]["101"] == "Concrete"
    assert ctx[m.CTX_MATERIAL_ID_TO_SIG_HASH]["101"]
    assert ctx[m.CTX_MATERIAL_UID_TO_NAME]["uid-1"] == "Concrete"
    assert ctx[m.CTX_MATERIAL_UID_TO_CLASS]["uid-1"] == "Structure"
    assert ctx[m.CTX_MATERIAL_UID_TO_RECORD]["uid-1"]["record_id"] == "uid:uid-1"
    assert ctx[m.CTX_MATERIAL_UID_TO_SIG_HASH]["uid-1"]
    assert ctx[m.CTX_MATERIAL_UID_TO_GRAPHICS_SIG_HASH]["uid-1"]


def test_optional_identity_fields_do_not_emit_canonical_sentinel_literals(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    mat = _Mat()
    mat._params["Description"] = _Param(None)
    mat._params["Comments"] = _Param(None)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [mat])

    result = m.extract(doc=_Doc({11: _FillPatternElem("fp-11", "FG")}), ctx=_make_ctx_with_fill_patterns(m))
    im_items = (((result["records"][0] or {}).get("identity_basis", {}) or {}).get("items", [])) or []
    im = {it["k"]: it for it in im_items}

    assert im["material.description"]["v"] is None
    assert im["material.comments"]["v"] is None


def test_blocked_when_api_unavailable(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", None)

    ctx = {}
    result = m.extract(doc=object(), ctx=ctx)

    assert result["status"] == "blocked"
    assert result["hash_v2"] is None
    assert ctx[m.CTX_MATERIAL_UID_TO_NAME] == {}
    assert ctx[m.CTX_MATERIAL_UID_TO_CLASS] == {}
