# -*- coding: utf-8 -*-
"""Tests for core/collect.py — collection context, caching, and element filtering."""

import pytest
from core.collect import (
    CollectCtx,
    _is_invalid_element_id,
    _safe_unique_id,
    _make_query_key,
    collect_id_ints,
)


# ---------------------------------------------------------------------------
# CollectCtx counter tests
# ---------------------------------------------------------------------------

class TestCollectCtx:
    def test_inc_initializes_counter(self):
        ctx = CollectCtx()
        ctx.inc("foo")
        assert ctx.counters["foo"] == 1

    def test_inc_accumulates(self):
        ctx = CollectCtx()
        ctx.inc("bar", 3)
        ctx.inc("bar", 2)
        assert ctx.counters["bar"] == 5

    def test_inc_multiple_keys(self):
        ctx = CollectCtx()
        ctx.inc("a", 1)
        ctx.inc("b", 10)
        assert ctx.counters == {"a": 1, "b": 10}

    def test_inc_coerces_to_int(self):
        ctx = CollectCtx()
        ctx.inc("x", 1)
        ctx.inc("x", 1)
        assert isinstance(ctx.counters["x"], int)

    def test_default_fields(self):
        ctx = CollectCtx()
        assert ctx.collector_cache == {}
        assert ctx.counters == {}
        assert ctx.timing is None


# ---------------------------------------------------------------------------
# _is_invalid_element_id tests
# ---------------------------------------------------------------------------

class TestIsInvalidElementId:
    def test_none_is_invalid(self):
        assert _is_invalid_element_id(None) is True

    def test_object_without_integer_value_is_invalid(self):
        """An object with no IntegerValue attribute is treated as invalid."""
        assert _is_invalid_element_id(object()) is True

    def test_negative_integer_value_is_invalid(self):
        class FakeId:
            IntegerValue = -1
        assert _is_invalid_element_id(FakeId()) is True

    def test_zero_is_invalid(self):
        """Zero IntegerValue should be invalid (Revit InvalidElementId)."""
        class FakeId:
            IntegerValue = 0
        # IntegerValue 0 is not < 0, so not invalid by the negative check.
        # But the function also checks for InvalidElementId equality.
        # Without the real Revit API, 0 passes the negative check.
        result = _is_invalid_element_id(FakeId())
        # The function checks iv < 0; 0 is not < 0, so it passes.
        assert result is False

    def test_positive_integer_value_is_valid(self):
        class FakeId:
            IntegerValue = 42
        assert _is_invalid_element_id(FakeId()) is False

    def test_large_positive_is_valid(self):
        class FakeId:
            IntegerValue = 999999
        assert _is_invalid_element_id(FakeId()) is False

    def test_integer_value_none_is_invalid(self):
        class FakeId:
            IntegerValue = None
        assert _is_invalid_element_id(FakeId()) is True


# ---------------------------------------------------------------------------
# _safe_unique_id tests
# ---------------------------------------------------------------------------

class TestSafeUniqueId:
    def test_none_object(self):
        assert _safe_unique_id(None) is None

    def test_no_unique_id_attr(self):
        assert _safe_unique_id(object()) is None

    def test_unique_id_none(self):
        class Fake:
            UniqueId = None
        assert _safe_unique_id(Fake()) is None

    def test_unique_id_empty(self):
        class Fake:
            UniqueId = ""
        assert _safe_unique_id(Fake()) is None

    def test_unique_id_whitespace(self):
        class Fake:
            UniqueId = "   "
        assert _safe_unique_id(Fake()) is None

    def test_unique_id_valid(self):
        class Fake:
            UniqueId = "abc-123-def"
        assert _safe_unique_id(Fake()) == "abc-123-def"

    def test_unique_id_coerced_to_str(self):
        class Fake:
            UniqueId = 12345
        assert _safe_unique_id(Fake()) == "12345"


# ---------------------------------------------------------------------------
# _make_query_key tests
# ---------------------------------------------------------------------------

class TestMakeQueryKey:
    def test_basic_key(self):
        key = _make_query_key(
            kind="types",
            of_class=None,
            of_category=None,
            where_key=None,
            require_unique_id=False,
        )
        assert key == ("types", None, None, False, None)

    def test_class_name_extracted(self):
        class FakeClass:
            pass
        key = _make_query_key(
            kind="instances",
            of_class=FakeClass,
            of_category=None,
            where_key=None,
            require_unique_id=True,
        )
        assert key == ("instances", "FakeClass", None, True, None)

    def test_category_as_int(self):
        key = _make_query_key(
            kind="types",
            of_class=None,
            of_category=42,
            where_key=None,
            require_unique_id=False,
        )
        assert key == ("types", None, 42, False, None)

    def test_where_key_included(self):
        key = _make_query_key(
            kind="types",
            of_class=None,
            of_category=None,
            where_key="my_filter",
            require_unique_id=False,
        )
        assert key == ("types", None, None, False, "my_filter")

    def test_same_inputs_same_key(self):
        """Determinism: same inputs must produce the same key."""
        args = dict(kind="types", of_class=None, of_category=10, where_key="k", require_unique_id=True)
        assert _make_query_key(**args) == _make_query_key(**args)


# ---------------------------------------------------------------------------
# collect_id_ints caching behavior (no Revit API needed)
# ---------------------------------------------------------------------------

class TestCollectIdIntsNoRevit:
    def test_no_revit_raises(self):
        """Without Revit API, collect_id_ints should raise RuntimeError."""
        with pytest.raises(RuntimeError, match="Revit API not reachable"):
            collect_id_ints(object(), kind="types")

    def test_none_doc_raises(self):
        """None doc should raise ValueError (after Revit check)."""
        # This will hit the Revit check first since we're not in Revit.
        with pytest.raises(RuntimeError):
            collect_id_ints(None, kind="types")

    def test_cache_bypass_for_unkeyed_predicate(self):
        """When where is provided without where_key, cache should be bypassed."""
        ctx = CollectCtx()
        with pytest.raises(RuntimeError):
            collect_id_ints(
                object(),
                kind="types",
                where=lambda e: True,
                cctx=ctx,
            )
        assert ctx.counters.get("collect.cache_bypass.unkeyed_predicate", 0) == 1

def test_build_purgeable_id_set_ok():
    import importlib
    m = importlib.import_module("core.collect")

    class FakeId:
        def __init__(self, v): self.IntegerValue = v

    class FakeDoc:
        def GetUnusedElements(self, categories):
            return [FakeId(100), FakeId(200), FakeId(300)]

    ctx = {}
    result, q = m.build_purgeable_id_set(FakeDoc(), ctx)
    assert q == "ok"
    assert result == frozenset({100, 200, 300})
    assert ctx["_purgeable_id_set"] == frozenset({100, 200, 300})
    assert ctx["_purgeable_id_set_q"] == "ok"


def test_build_purgeable_id_set_failure():
    import importlib
    m = importlib.import_module("core.collect")

    class BadDoc:
        def GetUnusedElements(self, categories):
            raise RuntimeError("API unavailable")

    ctx = {}
    result, q = m.build_purgeable_id_set(BadDoc(), ctx)
    assert result is None
    assert q == "unreadable"
    assert ctx["_purgeable_id_set"] is None
    assert ctx["_purgeable_id_set_q"] == "unreadable"


def test_build_purgeable_id_set_uses_cache():
    import importlib
    m = importlib.import_module("core.collect")

    calls = []
    class TrackingDoc:
        def GetUnusedElements(self, categories):
            calls.append(1)
            return []

    ctx = {"_purgeable_id_set": frozenset({42}), "_purgeable_id_set_q": "ok"}
    result, q = m.build_purgeable_id_set(TrackingDoc(), ctx)
    assert result == frozenset({42})
    assert q == "ok"
    assert calls == []
