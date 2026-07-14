"""Tests for resolve_worker_count() in tools/compare_cross_segment.py."""
from __future__ import annotations

from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from compare_cross_segment import resolve_worker_count


def test_auto_derives_from_cpu_count(monkeypatch):
    import compare_cross_segment as mod
    monkeypatch.setattr(mod.os, "cpu_count", lambda: 8)
    assert resolve_worker_count("auto") == 6  # 8 - headroom(2)


def test_auto_never_returns_zero_on_low_core_count(monkeypatch):
    import compare_cross_segment as mod
    monkeypatch.setattr(mod.os, "cpu_count", lambda: 1)
    assert resolve_worker_count("auto") >= 1


def test_auto_with_no_cpu_count_falls_back_to_four(monkeypatch):
    import compare_cross_segment as mod
    monkeypatch.setattr(mod.os, "cpu_count", lambda: None)
    assert resolve_worker_count("auto") == 4


def test_auto_is_case_insensitive_and_trims_whitespace():
    assert resolve_worker_count(" AUTO ") == resolve_worker_count("auto")


def test_explicit_string_int_is_parsed():
    assert resolve_worker_count("8") == 8


def test_explicit_int_passthrough():
    assert resolve_worker_count(8) == 8
