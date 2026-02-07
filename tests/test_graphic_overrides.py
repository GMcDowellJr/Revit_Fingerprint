# -*- coding: utf-8 -*-
"""Tests for core/graphic_overrides.py — graphics extraction helpers.

These tests exercise the pure-Python helper functions without requiring
the Revit API. Functions that dispatch on Category/OverrideGraphicSettings
are tested via the "unknown source type" fallback path.
"""

import pytest
from core.graphic_overrides import (
    _is_invalid_element_id,
    _rgb_from_color,
    _read_attr,
    _read_first_attr,
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)
from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, ITEM_Q_UNREADABLE, ITEM_Q_UNSUPPORTED


# ---------------------------------------------------------------------------
# _is_invalid_element_id tests (graphic_overrides version)
# ---------------------------------------------------------------------------

class TestIsInvalidElementId:
    def test_none_is_invalid(self):
        assert _is_invalid_element_id(None) is True

    def test_positive_integer_value_is_valid(self):
        class FakeId:
            IntegerValue = 42
        assert _is_invalid_element_id(FakeId()) is False

    def test_zero_is_invalid(self):
        """In OGS context, 0 means 'no override' and should be invalid."""
        class FakeId:
            IntegerValue = 0
        assert _is_invalid_element_id(FakeId()) is True

    def test_negative_is_invalid(self):
        class FakeId:
            IntegerValue = -1
        assert _is_invalid_element_id(FakeId()) is True

    def test_no_integer_value_attr_is_valid(self):
        """Object without IntegerValue should not be considered invalid."""
        assert _is_invalid_element_id(object()) is False


# ---------------------------------------------------------------------------
# _rgb_from_color tests
# ---------------------------------------------------------------------------

class TestRgbFromColor:
    def test_none_returns_missing(self):
        v, q = _rgb_from_color(None)
        assert v is None
        assert q == ITEM_Q_MISSING

    def test_valid_color(self):
        class FakeColor:
            Red = 255
            Green = 128
            Blue = 0
        v, q = _rgb_from_color(FakeColor())
        assert v == "255-128-0"
        assert q == ITEM_Q_OK

    def test_black(self):
        class FakeColor:
            Red = 0
            Green = 0
            Blue = 0
        v, q = _rgb_from_color(FakeColor())
        assert v == "0-0-0"
        assert q == ITEM_Q_OK

    def test_white(self):
        class FakeColor:
            Red = 255
            Green = 255
            Blue = 255
        v, q = _rgb_from_color(FakeColor())
        assert v == "255-255-255"
        assert q == ITEM_Q_OK

    def test_unreadable_color(self):
        """Color that raises on attribute access should return unreadable."""
        class BadColor:
            @property
            def Red(self):
                raise RuntimeError("broken")
        v, q = _rgb_from_color(BadColor())
        assert v is None
        assert q == ITEM_Q_UNREADABLE


# ---------------------------------------------------------------------------
# _read_attr tests
# ---------------------------------------------------------------------------

class TestReadAttr:
    def test_existing_attr(self):
        class Obj:
            foo = 42
        v, q = _read_attr(Obj(), "foo")
        assert v == 42
        assert q == ITEM_Q_OK

    def test_missing_attr(self):
        v, q = _read_attr(object(), "nonexistent")
        assert v is None
        assert q == ITEM_Q_UNSUPPORTED

    def test_raising_attr(self):
        class Obj:
            @property
            def broken(self):
                raise ValueError("oops")
        v, q = _read_attr(Obj(), "broken")
        assert v is None
        assert q == ITEM_Q_UNREADABLE


# ---------------------------------------------------------------------------
# _read_first_attr tests
# ---------------------------------------------------------------------------

class TestReadFirstAttr:
    def test_first_found(self):
        class Obj:
            second = "val"
        v, q, name = _read_first_attr(Obj(), ["first", "second", "third"])
        assert v == "val"
        assert q == ITEM_Q_OK
        assert name == "second"

    def test_none_found(self):
        v, q, name = _read_first_attr(object(), ["a", "b", "c"])
        assert v is None
        assert q == ITEM_Q_UNSUPPORTED
        assert name is None

    def test_first_raises_returns_unreadable(self):
        class Obj:
            @property
            def a(self):
                raise RuntimeError("broken")
        v, q, name = _read_first_attr(Obj(), ["a", "b"])
        assert v is None
        assert q == ITEM_Q_UNREADABLE
        assert name == "a"


# ---------------------------------------------------------------------------
# extract_* with unknown source type (not Category, not OGS)
# ---------------------------------------------------------------------------

class TestExtractUnknownSource:
    """When source is neither Category nor OverrideGraphicSettings,
    all extract functions should return UNSUPPORTED items."""

    def test_projection_graphics_unknown_source(self):
        items = extract_projection_graphics(doc=None, source=object(), ctx=None)
        assert len(items) == 5
        keys = [item["k"] for item in items]
        assert "projection.line_weight" in keys
        assert "projection.color.rgb" in keys
        assert "projection.pattern_ref.sig_hash" in keys
        assert "projection.fill_pattern_ref.sig_hash" in keys
        assert "projection.fill_color.rgb" in keys
        for item in items:
            assert item["q"] == ITEM_Q_UNSUPPORTED

    def test_cut_graphics_unknown_source(self):
        items = extract_cut_graphics(doc=None, source=object(), ctx=None)
        assert len(items) == 5
        keys = [item["k"] for item in items]
        assert "cut.line_weight" in keys
        assert "cut.color.rgb" in keys
        assert "cut.pattern_ref.sig_hash" in keys
        assert "cut.fill_pattern_ref.sig_hash" in keys
        assert "cut.fill_color.rgb" in keys
        for item in items:
            assert item["q"] == ITEM_Q_UNSUPPORTED

    def test_halftone_unknown_source(self):
        items = extract_halftone(source=object())
        assert len(items) == 1
        assert items[0]["k"] == "halftone"
        assert items[0]["q"] == ITEM_Q_UNSUPPORTED

    def test_transparency_unknown_source(self):
        items = extract_transparency(source=object())
        assert len(items) == 1
        assert items[0]["k"] == "transparency"
        assert items[0]["q"] == ITEM_Q_UNSUPPORTED

    def test_custom_key_prefix(self):
        items = extract_projection_graphics(
            doc=None, source=object(), ctx=None, key_prefix="custom"
        )
        keys = [item["k"] for item in items]
        assert all(k.startswith("custom.") for k in keys)

    def test_halftone_custom_prefix(self):
        items = extract_halftone(source=object(), key_prefix="ht_override")
        assert items[0]["k"] == "ht_override"

    def test_transparency_custom_prefix(self):
        items = extract_transparency(source=object(), key_prefix="trans_override")
        assert items[0]["k"] == "trans_override"
