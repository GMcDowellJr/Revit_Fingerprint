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
