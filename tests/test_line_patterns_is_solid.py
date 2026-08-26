# -*- coding: utf-8 -*-
"""Coverage for lp.is_solid (coordination-only, mirrors fill_pattern.is_solid).

lp.is_solid must never appear in identity_basis.items or sig_basis.keys_used --
it is a filter criterion, not identity, exactly like fill_pattern.is_solid in
domains/fill_patterns.py.
"""

import importlib


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _Seg(object):
    def __init__(self, seg_type, length):
        self.Type = seg_type
        self.Length = length


class _LinePatternDef(object):
    """Minimal LinePattern double (the object GetLinePattern() returns)."""

    def __init__(self, segments):
        self._segments = segments

    def GetSegments(self):
        return self._segments


class _LinePatternElem(object):
    """Minimal LinePatternElement double -- no Revit API dependency."""

    def __init__(self, elem_id, uid, name, lp):
        self.Id = _Id(elem_id)
        self.UniqueId = uid
        self.Name = name
        self._lp = lp

    def GetLinePattern(self):
        return self._lp


def _module():
    return importlib.import_module("domains.line_patterns")


def _coordination_item(rec, key):
    items = rec["phase2"]["coordination_items"]
    return next(it for it in items if it["k"] == key)


def _extract_one(monkeypatch, segments):
    m = _module()
    lp = _LinePatternDef(segments)
    elem = _LinePatternElem(1, "uid-1", "Some Pattern", lp)
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: [elem])

    out = m.extract(None, {})
    assert out["records"], "expected at least one record"
    return out["records"][0]


def test_zero_segment_pattern_reports_is_solid_true(monkeypatch):
    rec = _extract_one(monkeypatch, segments=[])

    item = _coordination_item(rec, "lp.is_solid")
    assert item["v"] == "true"
    assert item["q"] == "ok"


def test_one_segment_pattern_reports_is_solid_true(monkeypatch):
    # A single-segment pattern (e.g. one long Dash) renders as a continuous
    # line -- the real-world "solid" case, since the true built-in Solid
    # pattern has no LinePatternElement and never reaches this loop at all.
    rec = _extract_one(monkeypatch, segments=[_Seg(0, 1.0)])

    item = _coordination_item(rec, "lp.is_solid")
    assert item["v"] == "true"
    assert item["q"] == "ok"


def test_segmented_pattern_reports_is_solid_false(monkeypatch):
    rec = _extract_one(monkeypatch, segments=[_Seg(0, 1.0), _Seg(1, 2.0)])

    item = _coordination_item(rec, "lp.is_solid")
    assert item["v"] == "false"
    assert item["q"] == "ok"


def test_is_solid_not_in_identity_or_sig_basis(monkeypatch):
    rec = _extract_one(monkeypatch, segments=[])

    identity_keys = [it["k"] for it in rec["identity_basis"]["items"]]
    assert "lp.is_solid" not in identity_keys
    assert "lp.is_solid" not in rec["sig_basis"]["keys_used"]

    # lp.is_import stays the only coordination item promoted into identity.
    assert "lp.is_import" in identity_keys
