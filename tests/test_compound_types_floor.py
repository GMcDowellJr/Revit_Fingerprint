# -*- coding: utf-8 -*-
"""Minimal coverage for domains/floor_types.py's sig_hash computation,
mirroring tests/test_compound_types_wall.py's mock scaffolding. Added
alongside D-040 (policy-driven inline sig_hash) specifically to prove: (1)
the D-039 fix (type_name/color excluded from sig_hash) still holds after
routing the preimage key set through core/sig_hash_policy.resolve_sig_hash_keys(),
and (2) ctx["sig_hash_policies"] actually drives the computed hash when present.
"""
import importlib

from domains import compound_layers


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


class _ParamString(object):
    def __init__(self, s):
        self._s = s

    def AsString(self):
        return self._s


class _Layer(object):
    def __init__(self, fn, width_ft, mat_id, structural=True, variable=False):
        _fn_map = {"None": 0, "Structure": 1, "Substrate": 2, "Insulation": 3, "Finish1": 4, "Finish2": 5}
        self.Function = _fn_map.get(fn, fn)
        self.Width = width_ft
        self.MaterialId = _Id(mat_id) if isinstance(mat_id, int) else mat_id
        self.IsStructuralMaterial = structural
        self.IsVariableWidth = variable


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
        return self._ext_idx if shell_layer_type == "Exterior" else self._int_idx

    def ParticipatesInWrapping(self, idx):
        return idx not in (self._ext_idx, self._int_idx)

    def GetWallSweepsInfo(self):
        return self._sweeps


class _FloorType(object):
    def __init__(self, name, cs, fn="Interior", coarse_fill_id=-1):
        self.Name = name
        _fn_map = {"Interior": 0, "Exterior": 1}
        self._fn = _fn_map.get(fn, fn)
        self._cs = cs
        self._coarse_fill_id = coarse_fill_id

    @property
    def Function(self):
        if isinstance(self._fn, Exception):
            raise self._fn
        return self._fn

    def GetCompoundStructure(self):
        return self._cs

    def get_Parameter(self, bip):
        if bip == "BIP_FILL_PATTERN":
            return _Param(elem_id=_Id(self._coarse_fill_id))
        if bip == "BIP_FILL_COLOR":
            return _Param(intval=(3 << 16) + (2 << 8) + 1)
        if bip == "BIP_TYPE_NAME":
            return _ParamString("Fallback Type Name")
        return None


class _Doc(object):
    def __init__(self, id_to_uid=None):
        self._id_to_uid = id_to_uid or {}

    def GetElement(self, eid):
        i = getattr(eid, "IntegerValue", None)
        if i in self._id_to_uid:
            return _MatElem(self._id_to_uid[i])
        return None


def _setup_module(monkeypatch):
    m = importlib.import_module("domains.floor_types")
    monkeypatch.setattr(m, "FloorType", object)
    monkeypatch.setattr(compound_layers, "ShellLayerType", type("_SLT", (), {"Exterior": "Exterior", "Interior": "Interior"}))
    monkeypatch.setattr(compound_layers, "BuiltInParameter", type("_BIP", (), {
        "COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND": "BIP_FILL_PATTERN",
        "COARSE_SCALE_FILL_COLOR": "BIP_FILL_COLOR",
        "ALL_MODEL_TYPE_NAME": "BIP_TYPE_NAME",
        "SYMBOL_NAME_PARAM": "BIP_TYPE_NAME",
    }))
    monkeypatch.setattr(m, "BuiltInCategory", type("_BIC", (), {"OST_Floors": "OST_Floors"}))
    return m


def _default_ctx():
    return {
        "fill_pattern_uid_to_sig_hash_v2": {},
        "fill_pattern_special_values": {"no_pattern": "<No Pattern>", "solid": "<Solid>"},
        "fill_pattern_id_to_value": {},
    }


def _basic_floor(name="Floor A"):
    layers = [_Layer("Structure", 0.5, 101), _Layer("Substrate", 0.25, 102), _Layer("Finish1", 0.125, 103)]
    cs = _CS(layers=layers, ext_idx=1, int_idx=2, sweeps=["sweep1"])
    return _FloorType(name=name, cs=cs)


def test_basic_floor_produces_ok_record(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_floor("F1")])
    out = m.extract_floor_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx())
    rec = out["records"][0]
    assert rec["status"] == "ok"
    assert rec["sig_hash"] is not None


def test_type_name_not_in_sig_hash(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_floor("Name A"), _basic_floor("Name B")])
    out = m.extract_floor_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx())
    assert out["records"][0]["sig_hash"] == out["records"][1]["sig_hash"]


def test_sig_basis_keys_used_matches_fallback(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_floor()])
    rec = m.extract_floor_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx())["records"][0]
    assert set(rec["sig_basis"]["keys_used"]) == set(m._FLOOR_TYPES_SIG_HASH_KEYS_FALLBACK)


def test_no_compound_structure_blocked(monkeypatch):
    m = _setup_module(monkeypatch)
    floor = _FloorType(name="NoCS", cs=None)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [floor])
    out = m.extract_floor_types(_Doc(), _default_ctx())
    rec = out["records"][0]
    assert rec["status"] == "blocked"
    assert rec["sig_hash"] is None


def test_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present(monkeypatch):
    m = _setup_module(monkeypatch)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [_basic_floor("Name A")])

    ctx = _default_ctx()
    ctx["sig_hash_policies"] = {
        "domains": {
            "floor_types": {
                "sig_hash_schema": "floor_types.sig_hash.v1",
                "hash_alg": "md5_utf8_join_pipe",
                "allowed_items": ["ft.layer_count"],
                "allowed_item_prefixes": [],
                "required_items": [],
                "minima": {"block_if_any_required_not_ok": True},
            }
        }
    }
    out = m.extract_floor_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), ctx)
    rec = out["records"][0]

    from core.hashing import make_hash
    from core.record_v2 import serialize_identity_items

    layer_count_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "ft.layer_count"]
    expected = make_hash(serialize_identity_items(layer_count_item))
    assert rec["sig_hash"] == expected

    default_out = m.extract_floor_types(_Doc({101: "m1", 102: "m2", 103: "m3"}), _default_ctx())
    assert rec["sig_hash"] != default_out["records"][0]["sig_hash"]
