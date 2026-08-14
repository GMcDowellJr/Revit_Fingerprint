# -*- coding: utf-8 -*-
"""Regression test for the fill_pattern.is_import bool -> string coercion bug.

Prior to the fix, _phase2_fill_pattern_is_import() returned a raw Python bool
on every success path, and both extract_drafting and extract_model passed that
bool straight into make_identity_item(), which raises
ValueError("IdentityItem.v must be a string or None") for any non-str,
non-None value (core/record_v2.py). See
docs/findings_fill_patterns_drafting_investigation.md Section 1 for the full
root-cause trace.
"""

import importlib


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _FillPatternDef(object):
    """Minimal FillPattern double (the object GetFillPattern() returns)."""

    def __init__(self, target, is_solid=False, grid_count=0):
        self.Target = target
        self.IsSolidFill = bool(is_solid)
        self.GridCount = grid_count


class _FillPatternElem(object):
    """Minimal FillPatternElement double -- no Revit API dependency.

    is_imported=None omits the IsImported attribute entirely, forcing
    _phase2_fill_pattern_is_import() to fall through to the name-regex branch.
    """

    def __init__(self, elem_id, uid, name, fp, is_imported=None):
        self.Id = _Id(elem_id)
        self.UniqueId = uid
        self.Name = name
        self._fp = fp
        if is_imported is not None:
            self.IsImported = is_imported

    def GetFillPattern(self):
        return self._fp


def _module():
    return importlib.import_module("domains.fill_patterns")


def _coordination_item(rec, key):
    items = rec["phase2"]["coordination_items"]
    return next(it for it in items if it["k"] == key)


def test_extract_drafting_is_import_direct_attribute_true_is_string_not_bool():
    m = _module()
    fp = _FillPatternDef(target=m._TARGET_DRAFTING_INT, is_solid=False, grid_count=0)
    elem = _FillPatternElem(1, "uid-1", "Diagonal crosshatch", fp, is_imported=True)
    ctx = {m._CTX_FILL_PATTERNS_CACHE_KEY: [elem]}

    out = m.extract_drafting(None, ctx)  # doc unused once the collector cache is pre-populated

    assert out["records"], "expected at least one record"
    rec = out["records"][0]
    assert rec["status"] == "ok"
    assert rec["sig_hash"] is not None
    item = _coordination_item(rec, "fill_pattern.is_import")
    assert isinstance(item["v"], str)
    assert item["v"] == "true"
    assert item["q"] == "ok"


def test_extract_drafting_is_import_direct_attribute_false_is_string_not_bool():
    # False is the case most likely to slip through a truthiness-only check --
    # the pre-fix code raised on this path too (bool(v) is False, not None).
    m = _module()
    fp = _FillPatternDef(target=m._TARGET_DRAFTING_INT, is_solid=False, grid_count=0)
    elem = _FillPatternElem(2, "uid-2", "Diagonal crosshatch", fp, is_imported=False)
    ctx = {m._CTX_FILL_PATTERNS_CACHE_KEY: [elem]}

    out = m.extract_drafting(None, ctx)

    rec = out["records"][0]
    assert rec["status"] == "ok"
    item = _coordination_item(rec, "fill_pattern.is_import")
    assert isinstance(item["v"], str)
    assert item["v"] == "false"


def test_extract_drafting_is_import_name_regex_match_is_string_not_bool():
    m = _module()
    fp = _FillPatternDef(target=m._TARGET_DRAFTING_INT, is_solid=False, grid_count=0)
    # No IsImported-style attribute set -- falls through to the name-regex branch.
    elem = _FillPatternElem(3, "uid-3", "ANSI-31 Iron BrkFill", fp)
    ctx = {m._CTX_FILL_PATTERNS_CACHE_KEY: [elem]}

    out = m.extract_drafting(None, ctx)

    rec = out["records"][0]
    assert rec["status"] == "ok"
    item = _coordination_item(rec, "fill_pattern.is_import")
    assert isinstance(item["v"], str)
    assert item["v"] == "true"


def test_extract_model_is_import_direct_attribute_is_string_not_bool():
    m = _module()
    fp = _FillPatternDef(target=m._TARGET_MODEL_INT, is_solid=False, grid_count=0)
    elem = _FillPatternElem(4, "uid-4", "Model Hatch A", fp, is_imported=True)
    ctx = {m._CTX_FILL_PATTERNS_CACHE_KEY: [elem]}

    out = m.extract_model(None, ctx)

    assert out["records"], "expected at least one record"
    rec = out["records"][0]
    assert rec["status"] == "ok"
    assert rec["sig_hash"] is not None
    item = _coordination_item(rec, "fill_pattern.is_import")
    assert isinstance(item["v"], str)
    assert item["v"] == "true"


def test_extract_model_is_import_name_regex_match_is_string_not_bool():
    m = _module()
    fp = _FillPatternDef(target=m._TARGET_MODEL_INT, is_solid=False, grid_count=0)
    elem = _FillPatternElem(5, "uid-5", "IMPORT-Reference Hatch", fp)
    ctx = {m._CTX_FILL_PATTERNS_CACHE_KEY: [elem]}

    out = m.extract_model(None, ctx)

    rec = out["records"][0]
    assert rec["status"] == "ok"
    item = _coordination_item(rec, "fill_pattern.is_import")
    assert isinstance(item["v"], str)
    assert item["v"] == "true"
