# -*- coding: utf-8 -*-

import importlib

from core.canon import S_MISSING, S_UNREADABLE


class _Elem(object):
    def __init__(self, uid, name, material_class):
        self.UniqueId = uid
        self._name = name
        self._class = material_class

    @property
    def Name(self):
        if isinstance(self._name, Exception):
            raise self._name
        return self._name

    @property
    def MaterialClass(self):
        if isinstance(self._class, Exception):
            raise self._class
        return self._class


def test_ctx_keys_populated(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(
        m,
        "collect_instances",
        lambda *a, **k: [
            _Elem("uid-1", "Concrete", "Structure"),
            _Elem("uid-2", "Steel", "Metal"),
        ],
    )

    ctx = {}
    result = m.extract(doc=object(), ctx=ctx)

    assert result["status"] == "ok"
    assert ctx[m.CTX_MATERIAL_UID_TO_NAME] == {"uid-1": "Concrete", "uid-2": "Steel"}
    assert ctx[m.CTX_MATERIAL_UID_TO_CLASS] == {"uid-1": "Structure", "uid-2": "Metal"}


def test_unreadable_name_stored_as_sentinel(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(
        m,
        "collect_instances",
        lambda *a, **k: [_Elem("uid-1", RuntimeError("boom"), "Structure")],
    )

    ctx = {}
    result = m.extract(doc=object(), ctx=ctx)

    assert result["status"] == "degraded"
    assert ctx[m.CTX_MATERIAL_UID_TO_NAME]["uid-1"] == S_UNREADABLE


def test_empty_class_stored_as_missing(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(
        m,
        "collect_instances",
        lambda *a, **k: [_Elem("uid-1", "Concrete", "")],
    )

    ctx = {}
    _ = m.extract(doc=object(), ctx=ctx)

    assert ctx[m.CTX_MATERIAL_UID_TO_CLASS]["uid-1"] == S_MISSING


def test_blocked_when_api_unavailable(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", None)

    ctx = {}
    result = m.extract(doc=object(), ctx=ctx)

    assert result["status"] == "blocked"
    assert result["hash_v2"] is None
    assert ctx[m.CTX_MATERIAL_UID_TO_NAME] == {}
    assert ctx[m.CTX_MATERIAL_UID_TO_CLASS] == {}


def test_hash_v2_always_none(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(
        m,
        "collect_instances",
        lambda *a, **k: [_Elem("uid-1", "Concrete", "Structure")],
    )

    result = m.extract(doc=object(), ctx={})

    assert result["hash_v2"] is None


def test_records_always_empty(monkeypatch):
    m = importlib.import_module("domains.materials")
    monkeypatch.setattr(m, "Material", object)
    monkeypatch.setattr(
        m,
        "collect_instances",
        lambda *a, **k: [_Elem("uid-1", "Concrete", "Structure")],
    )

    result = m.extract(doc=object(), ctx={})

    assert result["records"] == []
    assert result["record_rows"] == []
