import importlib
import sys
import types

from core.hashing import make_hash


def _install_fake_revit_db():
    autodesk = types.ModuleType("Autodesk")
    revit = types.ModuleType("Autodesk.Revit")
    db = types.ModuleType("Autodesk.Revit.DB")

    class _T(object):
        pass

    db.ElementId = _T
    db.ElementParameterFilter = _T
    db.LogicalAndFilter = _T
    db.LogicalOrFilter = _T
    db.ParameterFilterElement = _T
    db.SharedParameterElement = _T

    autodesk.Revit = revit
    revit.DB = db

    sys.modules["Autodesk"] = autodesk
    sys.modules["Autodesk.Revit"] = revit
    sys.modules["Autodesk.Revit.DB"] = db


def test_view_filter_definitions_empty_collection_is_not_blocked(monkeypatch):
    _install_fake_revit_db()
    vfd = importlib.import_module("domains.view_filter_definitions")

    monkeypatch.setattr(vfd, "collect_instances", lambda *a, **k: [])

    result = vfd.extract(doc=None, ctx={})

    assert result["count"] == 0
    assert result["raw_count"] == 0
    assert result["debug_v2_blocked"] is False
    assert result["debug_v2_block_reasons"] == {}
    assert result["hash_v2"] == make_hash([])
