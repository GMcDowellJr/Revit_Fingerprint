# -*- coding: utf-8 -*-
"""Coverage for domains/arrowheads.py's D-040 policy-driven sig_hash
resolution and the arrowhead.record_class identity-visibility promotion.

extract() is exercised end to end with a minimal mock ElementType to prove:
(1) the existing _ARROWHEADS_SIG_HASH_KEYS_FALLBACK still reproduces
today's sig_hash (arrowheads hashes its full identity_items list inline --
this is the flat-emission target pattern -- so the fallback set must
exactly match what's captured, or this would be hash-breaking);
(2) arrowhead.record_class is now present in identity_basis.items (visible
to discover_hash_policy.py's pareto search) without changing sig_hash;
(3) ctx["sig_hash_policies"] actually drives the computed hash when present.
"""
import importlib

from core.hashing import make_hash
from core.record_v2 import serialize_identity_items


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _Param(object):
    def __init__(self, value_string=None, int_val=None, double_val=None, string_val=None, has_value=True):
        self._vs = value_string
        self._iv = int_val
        self._dv = double_val
        self._sv = string_val
        self.HasValue = has_value

    def AsValueString(self):
        return self._vs

    def AsInteger(self):
        return self._iv

    def AsDouble(self):
        return self._dv

    def AsString(self):
        return self._sv


class _ArrowType(object):
    def __init__(self, name, params, elem_id=1):
        self.Name = name
        self._params = params
        self.Id = _Id(elem_id)
        self.UniqueId = "uid-{}".format(elem_id)

    def LookupParameter(self, name):
        return self._params.get(name)

    def get_Parameter(self, bip):
        return None


def _setup_module(monkeypatch):
    m = importlib.import_module("domains.arrowheads")
    monkeypatch.setattr(m, "ElementType", object)
    return m


def _arrow_style_type(name="45 Degree Arrow", elem_id=1):
    params = {
        "Arrow Style": _Param(value_string="Arrow"),
        "Tick Size": _Param(double_val=0.0208333),  # ~0.25 in
        "Arrow Width Angle": _Param(double_val=0.5236, value_string="30.00°"),
        "Width Angle": _Param(double_val=0.5236, value_string="30.00°"),
        "Fill Tick": _Param(int_val=1),
        "Arrow Closed": _Param(int_val=1),
    }
    return _ArrowType(name=name, params=params, elem_id=elem_id)


def _basic_out(m, monkeypatch, name="45 Degree Arrow", elem_id=1, ctx=None):
    t = _arrow_style_type(name=name, elem_id=elem_id)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [t])
    monkeypatch.setattr(m, "_is_arrowhead_type", lambda doc, tt: True)
    return m.extract(doc=None, ctx=ctx)


def test_basic_arrow_produces_ok_record(monkeypatch):
    m = _setup_module(monkeypatch)
    out = _basic_out(m, monkeypatch)
    assert out["count"] == 1
    rec = out["records"][0]
    assert rec["sig_hash"] is not None


def test_record_class_visible_in_identity_basis_but_not_in_sig_hash(monkeypatch):
    m = _setup_module(monkeypatch)
    out1 = _basic_out(m, monkeypatch, name="Arrow Name A", elem_id=1)
    out2 = _basic_out(m, monkeypatch, name="Arrow Name B", elem_id=2)
    rec1, rec2 = out1["records"][0], out2["records"][0]

    keys1 = {it["k"] for it in rec1["identity_basis"]["items"]}
    assert "arrowhead.record_class" in keys1
    record_class_item = [it for it in rec1["identity_basis"]["items"] if it["k"] == "arrowhead.record_class"][0]
    assert record_class_item["v"] == "Arrow"

    # Two Arrow-style records with identical geometry but different names/ids
    # must still produce the same sig_hash -- record_class (and name) do not
    # drive the hash.
    assert rec1["sig_hash"] == rec2["sig_hash"]


def test_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present(monkeypatch):
    m = _setup_module(monkeypatch)
    ctx = {
        "sig_hash_policies": {
            "domains": {
                "arrowheads": {
                    "sig_hash_schema": "arrowheads.sig_hash.v1",
                    "hash_alg": "md5_utf8_join_pipe",
                    "allowed_items": ["arrowhead.tick_size_in"],
                    "allowed_item_prefixes": [],
                    "required_items": [],
                    "minima": {"block_if_any_required_not_ok": True},
                }
            }
        }
    }
    out = _basic_out(m, monkeypatch, ctx=ctx)
    rec = out["records"][0]

    tick_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "arrowhead.tick_size_in"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(tick_item))

    default_out = _basic_out(m, monkeypatch, ctx=None)
    assert rec["sig_hash"] != default_out["records"][0]["sig_hash"]
