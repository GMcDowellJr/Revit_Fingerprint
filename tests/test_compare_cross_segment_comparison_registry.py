"""Tests for comparison_registry.csv staleness tracking in tools/compare_cross_segment.py."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import (  # noqa: E402
    COMPARISON_REGISTRY_FIELDS,
    atomic_write_csv,
    build_comparison_registry_rows,
    comparison_is_stale,
    load_comparison_registry,
)


def _reg_row(population_hash="h1", last_run_utc="2026-01-01T00:00:00Z", conformance_reference_mode="latest"):
    return {
        "population_hash": population_hash,
        "last_run_utc": last_run_utc,
        "conformance_reference_mode": conformance_reference_mode,
    }


def test_comparison_is_stale_when_never_computed():
    registry = {"t": _reg_row(), "p": _reg_row()}
    assert comparison_is_stale("t", "p", "template_to_project", registry, {}) is True


def test_comparison_not_stale_when_both_sides_unchanged():
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project"): {
            "population_hash_a": "h1", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", registry, existing) is False


def test_comparison_stale_when_reference_side_population_changed():
    # Template re-ran with new files; Project (target) untouched. This is the
    # "parent output changed" case from the staleness-model design: the
    # project's own bundle/patterns output is unaffected, but the comparison
    # result between them is now outdated because the template side moved.
    registry = {"t": _reg_row(population_hash="h1-new"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project"): {
            "population_hash_a": "h1-old", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", registry, existing) is True


def test_comparison_stale_when_target_side_population_changed():
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2-new")}
    existing = {
        ("t", "p", "template_to_project"): {
            "population_hash_a": "h1", "population_hash_b": "h2-old",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", registry, existing) is True


def test_comparison_stale_when_forced_rerun_changes_last_run_utc_without_population_change():
    # A --force segment re-run can regenerate bundle output (e.g. after a
    # policy change) without the population_hash itself changing.
    registry = {"t": _reg_row(population_hash="h1", last_run_utc="2026-02-01T00:00:00Z"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project"): {
            "population_hash_a": "h1", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", registry, existing) is True


def test_build_comparison_registry_rows_stamps_current_pairs():
    registry = {
        "t": _reg_row(population_hash="h1", conformance_reference_mode="latest"),
        "p": _reg_row(population_hash="h2"),
    }
    pairs = [("t", "p", "template_to_project")]
    rows = build_comparison_registry_rows(pairs, registry, existing={}, computed_utc="2026-07-10T00:00:00Z")
    assert len(rows) == 1
    row = rows[0]
    assert row["segment_id_a"] == "t"
    assert row["segment_id_b"] == "p"
    assert row["population_hash_a"] == "h1"
    assert row["population_hash_b"] == "h2"
    assert row["conformance_reference_mode"] == "latest"
    assert row["computed_utc"] == "2026-07-10T00:00:00Z"


def test_build_comparison_registry_rows_carries_over_pairs_not_recomputed():
    # A --segment-a filtered run only recomputes a subset of pairs; previously
    # recorded pairs outside that filter must survive untouched.
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("other_a", "other_b", "sibling_projects"): {
            "segment_id_a": "other_a", "segment_id_b": "other_b", "comparison_type": "sibling_projects",
            "population_hash_a": "x1", "population_hash_b": "x2",
            "last_run_utc_a": "", "last_run_utc_b": "",
            "conformance_reference_mode": "latest", "computed_utc": "2026-06-01T00:00:00Z",
        }
    }
    pairs = [("t", "p", "template_to_project")]
    rows = build_comparison_registry_rows(pairs, registry, existing, computed_utc="2026-07-10T00:00:00Z")
    keys = {(r["segment_id_a"], r["segment_id_b"], r["comparison_type"]) for r in rows}
    assert ("t", "p", "template_to_project") in keys
    assert ("other_a", "other_b", "sibling_projects") in keys
    carried = next(r for r in rows if r["segment_id_a"] == "other_a")
    assert carried["computed_utc"] == "2026-06-01T00:00:00Z"


def test_load_comparison_registry_roundtrip(tmp_path):
    out_dir = tmp_path / "out"
    rows = [{
        "segment_id_a": "t", "segment_id_b": "p", "comparison_type": "template_to_project",
        "population_hash_a": "h1", "population_hash_b": "h2",
        "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        "conformance_reference_mode": "latest", "computed_utc": "2026-07-10T00:00:00Z",
    }]
    atomic_write_csv(out_dir / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS, rows)
    loaded = load_comparison_registry(out_dir)
    assert ("t", "p", "template_to_project") in loaded
    assert loaded[("t", "p", "template_to_project")]["population_hash_a"] == "h1"


def test_load_comparison_registry_missing_file_returns_empty(tmp_path):
    assert load_comparison_registry(tmp_path / "does_not_exist") == {}
