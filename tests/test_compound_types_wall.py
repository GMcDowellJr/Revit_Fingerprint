# -*- coding: utf-8 -*-

import importlib


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _MatElem(object):
    def __init__(self, uid):
        self.UniqueId = uid


class _Param(object):
    def __init__(self, elem_id=None, intval=None):
        self._eid = elem_id
        self._ival = intval

    def AsElementId(self):
        return self._eid

    def AsInteger(self):
        return self._ival


class _Layer(object):
    def __init__(self, fn, width_ft, mat_id, structural=True, variable=False):
        self.Function = fn
        self.Width = width_ft
        self.MaterialId = _Id(mat_id) if isinstance(mat_id, int) else mat_id
        self.IsStructuralMaterial = structural
        self.IsVariableWidth = variable


class _LayerWidthError(_Layer):
    def __init__(self, fn, width_ft, mat_id, structural=True, variable=False):
        self.Function = fn
        self.MaterialId = _Id(mat_id) if isinstance(mat_id, int) else mat_id
        self.IsStructuralMaterial = structural
        self.IsVariableWidth = variable

    @property
    def Width(self):
        raise RuntimeError("width unreadable")


class _CS(object):
    def __init__(self, layers, ext_idx, int_idx, wraps_i="Both", wraps_e="Exterior", sweeps=None):
        self._layers = list(layers)
        self._ext_idx = ext_idx
        self._int_idx = int_idx
        self.WrapAtInserts = wraps_i
        self.WrapAtEnds = wraps_e
        self._sweeps = list(sweeps or [])

    def GetLayers(self):
        return self._layers

    def GetCoreBoundaryLayerIndex(self, shell_layer_type):
        if shell_layer_type == "Exterior":
            return self._ext_idx
        return self._int_idx

    def ParticipatesInWrapping(self, idx):
        return idx not in (self._ext_idx, self._int_idx)

    def GetWallSweepsInfo(self):
        return self._sweeps


class _WallType(object):
    def __init__(self, name, kind, cs, fn="Interior"):
        self.Name = name
        self.Kind = kind
        self._fn = fn
        self._cs = cs

    @property
    def Function(self):
        if isinstance(self._fn, Exception):
            raise self._fn
        return self._fn

    def GetCompoundStructure(self):
        return self._cs

    def get_Parameter(self, bip):
        if bip == "BIP_FILL_PATTERN":
            return _Param(elem_id=_Id(-1))
        if bip == "BIP_FILL_COLOR":
            # BGR int encoding expected by extractor
            return _Param(intval=(3 << 16) + (2 << 8) + 1)
        if bip == "BIP_TYPE_NAME":
            return _ParamString("Fallback Type Name")
        return None


class _ParamString(object):
    def __init__(self, s):
        self._s = s

    def AsString(self):
        return self._s


class _Doc(object):
    def __init__(self, id_to_uid=None):
        self._id_to_uid = id_to_uid or {}

    def GetElement(self, eid):
        i = getattr(eid, "IntegerValue", None)
        if i in self._id_to_uid:
            return _MatElem(self._id_to_uid[i])
        return None


def _setup_module(monkeypatch):
    m = importlib.import_module("domains.compound_types")
    monkeypatch.setattr(m, "WallType", object)
    monkeypatch.setattr(m, "ShellLayerType", type("_SLT", (), {"Exterior": "Exterior", "Interior": "Interior"}))
    monkeypatch.setattr(m, "BuiltInParameter", type("_BIP", (), {
        "COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND": "BIP_FILL_PATTERN",
        "COARSE_SCALE_FILL_COLOR": "BIP_FILL_COLOR",
        "ALL_MODEL_TYPE_NAME": "BIP_TYPE_NAME",
    }))
    return m


def _default_ctx(m):
    return {
        m.CTX_MATERIAL_UID_TO_NAME: {"m1": "Concrete A", "m2": "Gypsum", "m3": "Brick"},
        m.CTX_MATERIAL_UID_TO_CLASS: {"m1": "Structure", "m2": "Finish", "m3": "Masonry"},
        "fill_pattern_uid_to_sig_hash_v2": {},
    }


def _basic_wall(name="Wall A"):
    layers = [
        _Layer("Structure", 0.5, 101),
        _Layer("Substrate", 0.25, 102),
        _Layer("Finish1", 0.125, 103),
    ]
    cs = _CS(layers=layers, ext_idx=1, int_idx=2, sweeps=["sweep1"])
    return _WallType(name=name, kind=0, cs=cs)


def test_basic_wall_produces_record(monkeypatch):
    m = _setup_module(monkeypatch)
    wall = _basic_wall("W1")
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [wall])

    out = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))

    assert out["count"] == 1
    rec = out["records"][0]
    assert rec["status"] == "ok"
    keys = {it["k"] for it in rec["identity_basis"]["items"]}
    assert "wt.stack_hash_loose" in keys
    assert rec["sig_hash"] is not None


def test_non_basic_wall_produces_blocked_record(monkeypatch):
    m = _setup_module(monkeypatch)
    wall = _WallType(name="Stacked", kind=1, cs=None)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [wall])

    out = m.extract_wall_types(_Doc(), _default_ctx(m))

    assert out["count"] == 0
    assert out["debug_blocked_kind"] == 1
    rec = out["records"][0]
    assert rec["status"] == "blocked"
    keys = {it["k"] for it in rec["identity_basis"]["items"]}
    assert "wt.function" in keys
    assert "wt.layer_count" in keys
    assert "wt.total_thickness_in" in keys
    assert "wt.stack_hash_loose" in keys


def test_core_boundary_in_layer_rows(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])

    rec = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))["records"][0]
    boundaries = [r for r in rec["layer_rows"] if r["is_core_boundary"]]

    assert len(boundaries) == 2
    assert all(r["wl.function"] == "CORE_BOUNDARY" for r in boundaries)


def test_layer_count_excludes_core_boundaries(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])

    rec = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))["records"][0]
    item = [it for it in rec["identity_basis"]["items"] if it["k"] == "wt.layer_count"][0]

    assert item["v"] == "3"


def test_stack_hash_loose_excludes_material_name(monkeypatch):
    m = _setup_module(monkeypatch)
    w1 = _basic_wall("A")
    w2 = _basic_wall("B")
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [w1, w2])

    doc = _Doc({101: "m1", 102: "m2", 103: "m3"})
    ctx = _default_ctx(m)
    ctx[m.CTX_MATERIAL_UID_TO_NAME] = {"m1": "Concrete A", "m2": "Gypsum", "m3": "Brick X"}
    out1 = m.extract_wall_types(doc, ctx)

    ctx2 = _default_ctx(m)
    ctx2[m.CTX_MATERIAL_UID_TO_NAME] = {"m1": "Concrete B", "m2": "Gypsum", "m3": "Brick Y"}
    out2 = m.extract_wall_types(doc, ctx2)

    def get(rec, key):
        return [it for it in rec["identity_basis"]["items"] if it["k"] == key][0]["v"]

    assert get(out1["records"][0], "wt.stack_hash_loose") == get(out2["records"][0], "wt.stack_hash_loose")


def test_stack_hash_strict_includes_material_name(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])
    doc = _Doc({101: "m1", 102: "m2", 103: "m3"})

    ctx1 = _default_ctx(m)
    ctx1[m.CTX_MATERIAL_UID_TO_NAME] = {"m1": "N1", "m2": "N2", "m3": "N3"}
    ctx2 = _default_ctx(m)
    ctx2[m.CTX_MATERIAL_UID_TO_NAME] = {"m1": "X1", "m2": "X2", "m3": "X3"}

    r1 = m.extract_wall_types(doc, ctx1)["records"][0]
    r2 = m.extract_wall_types(doc, ctx2)["records"][0]
    get = lambda rec, k: [it for it in rec["identity_basis"]["items"] if it["k"] == k][0]["v"]

    assert get(r1, "wt.stack_hash_strict") != get(r2, "wt.stack_hash_strict")


def test_stack_hash_order_sensitive(monkeypatch):
    m = _setup_module(monkeypatch)
    w1 = _basic_wall("A")
    rev_layers = list(reversed(_basic_wall("B")._cs.GetLayers()))
    w2 = _WallType("B", 0, _CS(rev_layers, ext_idx=1, int_idx=3, sweeps=[]))
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [w1, w2])

    out = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))
    get = lambda rec, k: [it for it in rec["identity_basis"]["items"] if it["k"] == k][0]["v"]

    assert get(out["records"][0], "wt.stack_hash_loose") != get(out["records"][1], "wt.stack_hash_loose")


def test_material_ctx_miss_emits_missing_sentinel(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])

    rec = m.extract_wall_types(_Doc({101: "unknown", 102: "unknown", 103: "unknown"}), _default_ctx(m))["records"][0]
    real_rows = [r for r in rec["layer_rows"] if not r["is_core_boundary"]]

    assert all(r["wl.material_name"] == "<MISSING>" for r in real_rows)
    assert all(r["wl.material_class"] == "<MISSING>" for r in real_rows)


def test_type_name_not_in_sig_hash(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall("Name A"), _basic_wall("Name B")])

    out = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))
    assert out["records"][0]["sig_hash"] == out["records"][1]["sig_hash"]


def test_layer_rows_attached_to_record(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])

    rec = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))["records"][0]

    assert "layer_rows" in rec
    assert len(rec["layer_rows"]) == 5


def test_identity_items_sorted_and_sig_basis_declared(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall()])
    rec = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))["records"][0]

    keys = [it["k"] for it in rec["identity_basis"]["items"]]
    assert keys == sorted(keys)
    assert rec["sig_basis"]["schema"] == "wall_types.sig_basis.v1"
    assert rec["sig_basis"]["keys_used"] == [
        "wt.function",
        "wt.wraps_at_inserts",
        "wt.wraps_at_ends",
        "wt.layer_count",
        "wt.total_thickness_in",
        "wt.stack_hash_loose",
    ]


def test_label_has_quality_provenance_and_components(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_wall("Named Wall")])
    rec = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))["records"][0]

    label = rec["label"]
    assert label["display"] == "Named Wall"
    assert label["quality"] == "human"
    assert label["provenance"] == "revit.WallType.Name"
    assert label["components"]["type_name"] == "Named Wall"


def test_stack_hash_preserves_zero_vs_unreadable_thickness(monkeypatch):
    m = _setup_module(monkeypatch)
    zero_layers = [
        _Layer("Membrane", 0.0, 101),
        _Layer("Finish1", 0.125, 103),
    ]
    err_layers = [
        _LayerWidthError("Membrane", 0.0, 101),
        _Layer("Finish1", 0.125, 103),
    ]
    w1 = _WallType("Zero", 0, _CS(zero_layers, ext_idx=0, int_idx=1))
    w2 = _WallType("Err", 0, _CS(err_layers, ext_idx=0, int_idx=1))
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [w1, w2])
    out = m.extract_wall_types(_Doc({101: "m1", 103: "m3"}), _default_ctx(m))

    get = lambda rec, k: [it for it in rec["identity_basis"]["items"] if it["k"] == k][0]["v"]
    assert get(out["records"][0], "wt.stack_hash_loose") != get(out["records"][1], "wt.stack_hash_loose")


def test_required_identity_not_ok_blocks_record(monkeypatch):
    m = _setup_module(monkeypatch)
    wall = _basic_wall("BadFn")
    wall._fn = RuntimeError("cannot read function")
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [wall])

    out = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))
    rec = out["records"][0]

    assert rec["status"] == "blocked"
    assert rec["sig_hash"] is None
    assert "required_identity_not_ok" in rec["status_reasons"]
    assert out["count"] == 0


def test_no_compound_structure_blocked_record_includes_required_keys(monkeypatch):
    m = _setup_module(monkeypatch)
    wall = _WallType("NoCS", 0, None)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [wall])
    out = m.extract_wall_types(_Doc(), _default_ctx(m))
    rec = out["records"][0]
    keys = {it["k"] for it in rec["identity_basis"]["items"]}
    assert rec["status"] == "blocked"
    assert "wt.function" in keys
    assert "wt.layer_count" in keys
    assert "wt.total_thickness_in" in keys
    assert "wt.stack_hash_loose" in keys


def test_type_name_fallback_to_all_model_type_name(monkeypatch):
    m = _setup_module(monkeypatch)
    wall = _basic_wall("")
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [wall])
    out = m.extract_wall_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx(m))
    rec = out["records"][0]
    assert rec["label"]["display"] == "Fallback Type Name"
