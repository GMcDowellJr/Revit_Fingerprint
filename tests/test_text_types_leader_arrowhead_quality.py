# -*- coding: utf-8 -*-
"""D-043 (P2 review finding): text_type.leader_arrowhead_sig_hash must not
collapse "reference exists but sig_hash unresolved" into the same q=ok state
as "genuinely no leader arrowhead" -- the two are distinct per the project's
fail-soft policy (distinct states must not be silently collapsed).
"""
from domains import text_types


class _Id:
    def __init__(self, v):
        self.IntegerValue = v


class _Type:
    def __init__(self):
        self.Id = _Id(101)
        self.UniqueId = "uid-101"


class _ArrowElem:
    def __init__(self, uid="arrow-uid-1"):
        self.UniqueId = uid
        self.Name = "Arrow Type A"


class _LeaderArrowParam:
    def AsElementId(self):
        return _Id(555)


class _RaisingLeaderArrowParam:
    def AsElementId(self):
        raise RuntimeError("AsElementId failed")


class _Doc:
    def __init__(self, elem_by_int_id):
        self._elem_by_int_id = elem_by_int_id

    def GetElement(self, elem_id):
        return self._elem_by_int_id.get(getattr(elem_id, "IntegerValue", None))


class _RaisingDoc:
    def GetElement(self, elem_id):
        raise RuntimeError("element lookup failed")


def _first_param_with_leader_arrowhead(t, bip_names=None, ui_names=None):
    if bip_names and "LEADER_ARROWHEAD" in bip_names:
        return _LeaderArrowParam()
    return None


def _extract_record(monkeypatch, *, doc, ctx, first_param_fn):
    monkeypatch.setattr(text_types, "collect_types", lambda *a, **k: [_Type()])
    monkeypatch.setattr(text_types, "collect_instances", lambda *a, **k: [])
    monkeypatch.setattr(text_types, "get_type_display_name", lambda *a, **k: "Notes-Medium")
    monkeypatch.setattr(text_types, "first_param", first_param_fn)
    monkeypatch.setattr(text_types, "_as_string", lambda *a, **k: "Arial")
    monkeypatch.setattr(text_types, "_as_double", lambda *a, **k: 1.0)
    monkeypatch.setattr(text_types, "_as_int", lambda *a, **k: 1)
    monkeypatch.setattr(text_types, "_as_bool_from_param", lambda *a, **k: False)
    monkeypatch.setattr(text_types, "format_len_inches", lambda v: 1.0)
    monkeypatch.setattr(text_types, "try_get_color_rgb_from_elem", lambda *a, **k: (0, "0-0-0"))
    monkeypatch.setattr(text_types, "purge_lookup", lambda *a, **k: (False, "ok"))
    monkeypatch.setattr(text_types, "get_domain_join_key_policy", lambda *a, **k: {})
    return text_types.extract(doc=doc, ctx=ctx)["records"][0]


def _leader_item(rec):
    return [it for it in rec["items"] if it["k"] == "text_type.leader_arrowhead_sig_hash"][0]


def test_no_leader_arrowhead_reference_is_q_ok_v_none(monkeypatch):
    rec = _extract_record(
        monkeypatch,
        doc=object(),
        ctx={},
        first_param_fn=lambda t, bip_names=None, ui_names=None: None,
    )
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "ok"


def test_leader_arrowhead_reference_resolved_is_q_ok_with_hash(monkeypatch):
    doc = _Doc({555: _ArrowElem()})
    ctx = {"arrowheads_by_type_id": {"555": {"sig_hash": "a" * 32}}}
    rec = _extract_record(monkeypatch, doc=doc, ctx=ctx, first_param_fn=_first_param_with_leader_arrowhead)
    item = _leader_item(rec)
    assert item["v"] == "a" * 32
    assert item["q"] == "ok"


def test_leader_arrowhead_reference_unresolved_is_missing_not_ok(monkeypatch):
    # A leader arrowhead IS set on the text type, but ctx["arrowheads_by_type_id"]
    # doesn't have an entry for it (e.g. that arrowhead's own record was blocked,
    # or the dependency map wasn't provided) -- must not be reported the same as
    # "no leader arrowhead at all".
    doc = _Doc({555: _ArrowElem()})
    ctx = {"arrowheads_by_type_id": {}}
    rec = _extract_record(monkeypatch, doc=doc, ctx=ctx, first_param_fn=_first_param_with_leader_arrowhead)
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "missing"


def test_leader_arrowhead_reference_present_without_dependency_map_is_missing(monkeypatch):
    # extract() called without ctx["arrowheads_by_type_id"] at all (e.g. a
    # caller that hasn't wired the dependency map) -- same "unresolved", not
    # "none" outcome as an explicit-but-empty map.
    doc = _Doc({555: _ArrowElem()})
    rec = _extract_record(monkeypatch, doc=doc, ctx={}, first_param_fn=_first_param_with_leader_arrowhead)
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "missing"


def test_stale_leader_arrowhead_element_reference_is_missing_not_ok(monkeypatch):
    # P2 review follow-up on D-044: a positive AsElementId() is itself evidence
    # a leader arrowhead is assigned, even if doc.GetElement() then can't
    # resolve it (a stale/deleted reference). This must not be reported as
    # "no leader arrowhead" just because element resolution came back empty.
    doc = _Doc({})  # GetElement(555) -> None: stale reference
    rec = _extract_record(monkeypatch, doc=doc, ctx={}, first_param_fn=_first_param_with_leader_arrowhead)
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "missing"


def test_leader_arrowhead_element_lookup_exception_is_unreadable(monkeypatch):
    # Same positive-reference case, but doc.GetElement() itself raises --
    # distinct from "not found," reported as unreadable rather than missing.
    rec = _extract_record(monkeypatch, doc=_RaisingDoc(), ctx={}, first_param_fn=_first_param_with_leader_arrowhead)
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "unreadable"


def test_leader_arrowhead_reference_read_exception_is_unreadable(monkeypatch):
    # P2 review follow-up on D-046: if p_arrow.AsElementId() (or reading its
    # IntegerValue) itself raises, we can't even determine whether a leader
    # arrowhead is assigned -- must not fall through to "no leader arrowhead"
    # (q=ok) just because the earlier flags were never set.
    def _first_param_raising(t, bip_names=None, ui_names=None):
        if bip_names and "LEADER_ARROWHEAD" in bip_names:
            return _RaisingLeaderArrowParam()
        return None

    rec = _extract_record(monkeypatch, doc=object(), ctx={}, first_param_fn=_first_param_raising)
    item = _leader_item(rec)
    assert item["v"] is None
    assert item["q"] == "unreadable"
