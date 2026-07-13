"""Tests for compute_worker_split() in tools/run_segment_orchestrator.py."""
from __future__ import annotations

from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from run_segment_orchestrator import compute_worker_split


def test_small_budget_gives_low_single_digits():
    segment_workers, domain_workers = compute_worker_split(total_budget=4)
    assert 1 <= segment_workers <= 9
    assert 1 <= domain_workers <= 9


def test_large_budget_gives_expected_split():
    segment_workers, domain_workers = compute_worker_split(total_budget=22)
    assert (segment_workers, domain_workers) == (5, 4)


def test_budget_of_one_never_returns_zero():
    segment_workers, domain_workers = compute_worker_split(total_budget=1)
    assert segment_workers >= 1
    assert domain_workers >= 1


def test_explicit_segment_workers_coordinates_domain_workers():
    # Large explicit N must not multiply against a fixed default (e.g. N x 4) —
    # domain_workers shrinks to keep total concurrency near the budget.
    segment_workers, domain_workers = compute_worker_split(total_budget=22, segment_workers=21)
    assert segment_workers == 21
    assert domain_workers == 1


def test_explicit_segment_workers_never_returns_zero_domain_workers():
    segment_workers, domain_workers = compute_worker_split(total_budget=2, segment_workers=4)
    assert segment_workers == 4
    assert domain_workers >= 1


def test_explicit_segment_workers_small_n_gets_larger_domain_share():
    segment_workers, domain_workers = compute_worker_split(total_budget=22, segment_workers=1)
    assert segment_workers == 1
    assert domain_workers == 22


def test_explicit_segment_workers_with_no_cpu_count_falls_back_to_four_budget(monkeypatch):
    import run_segment_orchestrator as mod
    monkeypatch.setattr(mod.os, "cpu_count", lambda: None)
    segment_workers, domain_workers = compute_worker_split(segment_workers=4)
    assert segment_workers == 4
    assert domain_workers == 1  # budget falls back to 4; 4 // 4 == 1


def test_auto_with_no_cpu_count_falls_back_to_hardcoded_four_four(monkeypatch):
    import run_segment_orchestrator as mod
    monkeypatch.setattr(mod.os, "cpu_count", lambda: None)
    assert compute_worker_split() == (4, 4)
