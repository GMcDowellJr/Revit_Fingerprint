# tests/test_record_v2_utils.py

import pytest

from core import record_v2


def test_canonicalize_str_rules():
    assert record_v2.canonicalize_str(None) == (None, record_v2.ITEM_Q_MISSING)
    assert record_v2.canonicalize_str("   ") == (None, record_v2.ITEM_Q_MISSING)
    assert record_v2.canonicalize_str("  abc  ") == ("abc", record_v2.ITEM_Q_OK)


def test_canonicalize_int_rules():
    assert record_v2.canonicalize_int(None) == (None, record_v2.ITEM_Q_MISSING)
    assert record_v2.canonicalize_int(True)[1] == record_v2.ITEM_Q_UNREADABLE
    assert record_v2.canonicalize_int(123) == ("123", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_int(12.0) == ("12", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_int(12.5)[1] == record_v2.ITEM_Q_UNREADABLE
    assert record_v2.canonicalize_int(" 0042 ") == ("42", record_v2.ITEM_Q_OK)


def test_canonicalize_float_rules():
    assert record_v2.canonicalize_float(None) == (None, record_v2.ITEM_Q_MISSING)
    assert record_v2.canonicalize_float(False)[1] == record_v2.ITEM_Q_UNREADABLE
    v, q = record_v2.canonicalize_float(1.25, nd=3)
    assert (v, q) == ("1.250", record_v2.ITEM_Q_OK)


def test_canonicalize_bool_rules():
    assert record_v2.canonicalize_bool(None) == (None, record_v2.ITEM_Q_MISSING)
    assert record_v2.canonicalize_bool(True) == ("true", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_bool(0) == ("false", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_bool(1) == ("true", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_bool(2)[1] == record_v2.ITEM_Q_UNREADABLE
    assert record_v2.canonicalize_bool(" YES ") == ("true", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_bool("no") == ("false", record_v2.ITEM_Q_OK)
    assert record_v2.canonicalize_bool("maybe")[1] == record_v2.ITEM_Q_UNREADABLE


def test_make_identity_item_banned_substring_guard():
    with pytest.raises(ValueError):
        record_v2.make_identity_item("k", "x<MISSING>y", "ok")


def test_make_identity_item_empty_string_becomes_null_missing():
    it = record_v2.make_identity_item("k", "   ", "ok")
    assert it["v"] == ""
    assert it["q"] == record_v2.ITEM_Q_OK


def test_serialize_identity_items_is_sorted_and_deterministic():
    items = [
        {"k": "b", "q": "ok", "v": "2"},
        {"k": "a", "q": "missing", "v": None},
    ]
    pre1 = record_v2.serialize_identity_items(items)
    pre2 = record_v2.serialize_identity_items(list(reversed(items)))
    assert pre1 == pre2
    assert pre1 == [
        "k=a|q=missing|v=",
        "k=b|q=ok|v=2",
    ]


@pytest.mark.parametrize(
    "required_qs, expected",
    [
        ([], "complete"),
        (["ok", "ok"], "complete"),
        (["missing"], "incomplete_missing"),
        (["unsupported"], "incomplete_unsupported"),
        (["unreadable"], "incomplete_unreadable"),
        (["missing", "unsupported"], "incomplete_unsupported"),
        (["missing", "unreadable"], "incomplete_unreadable"),
    ],
)
def test_compute_identity_quality_dominance(required_qs, expected):
    got = record_v2.compute_identity_quality("ok", required_qs)
    assert got == expected


def test_compute_identity_quality_blocked_short_circuit():
    assert record_v2.compute_identity_quality("blocked", ["unreadable"]) == "none_blocked"
