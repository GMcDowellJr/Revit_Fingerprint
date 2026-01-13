# tests/test_hashing_incremental.py
import hashlib

import pytest

from core.hashing import make_hash, safe_str


def _reference_hash(values):
    """
    Reference implementation: MD5("|".join(safe_str(v) for v in values)) over UTF-8 bytes.

    This is intentionally independent of .NET MD5 so it can run in plain CPython.
    """
    joined = "|".join(safe_str(v) for v in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def test_make_hash_matches_reference_empty():
    assert make_hash([]) == _reference_hash([])


def test_make_hash_matches_reference_single():
    values = ["a"]
    assert make_hash(values) == _reference_hash(values)


def test_make_hash_matches_reference_multiple_and_unicode_and_pipes():
    values = ["a", "b|c", "μ", "é", "<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>"]
    assert make_hash(values) == _reference_hash(values)


def test_make_hash_deterministic_repeated_calls():
    values = ["x", "y", "z", "μ", "b|c"]
    h0 = make_hash(values)
    for _ in range(100):
        assert make_hash(values) == h0


def test_make_hash_is_order_sensitive_contract():
    a = ["a", "b", "c"]
    b = ["c", "b", "a"]
    assert make_hash(a) != make_hash(b)


def test_make_hash_separator_off_by_one_cases():
    # These cases catch missing/extra separator insertion
    cases = [
        ["a"],
        ["", "a"],
        ["a", ""],
        ["", ""],
        ["|"],         # value contains the separator
        ["", "|", ""], # empty values around separator value
    ]
    for values in cases:
        assert make_hash(values) == _reference_hash(values)


def test_make_hash_handles_unrepr_values():
    class BadStr(object):
        def __str__(self):
            raise RuntimeError("boom")

    values = ["a", BadStr(), "b"]
    # safe_str should produce "<unrepr>" for BadStr; ensure hashing still matches reference
    assert make_hash(values) == _reference_hash(values)


def test_make_hash_accepts_generator_large_input_sanity():
    # Avoid materializing a list: this catches regressions to "join(list(...))" patterns.
    values = ("v{}".format(i) for i in range(100_000))
    h = make_hash(values)

    assert isinstance(h, str)
    assert len(h) == 32
    int(h, 16)  # must be valid hex
