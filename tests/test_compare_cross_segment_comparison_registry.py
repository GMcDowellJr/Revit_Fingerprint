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
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, {}) is True


def test_comparison_not_stale_when_both_sides_unchanged():
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "population_hash_a": "h1", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, existing) is False


def test_comparison_stale_when_reference_side_population_changed():
    # Template re-ran with new files; Project (target) untouched. This is the
    # "parent output changed" case from the staleness-model design: the
    # project's own bundle/patterns output is unaffected, but the comparison
    # result between them is now outdated because the template side moved.
    registry = {"t": _reg_row(population_hash="h1-new"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "population_hash_a": "h1-old", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, existing) is True


def test_comparison_stale_when_target_side_population_changed():
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2-new")}
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "population_hash_a": "h1", "population_hash_b": "h2-old",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, existing) is True


def test_comparison_stale_when_forced_rerun_changes_last_run_utc_without_population_change():
    # A --force segment re-run can regenerate bundle output (e.g. after a
    # policy change) without the population_hash itself changing.
    registry = {"t": _reg_row(population_hash="h1", last_run_utc="2026-02-01T00:00:00Z"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "population_hash_a": "h1", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, existing) is True


def test_comparison_staleness_is_isolated_per_domain():
    # A recorded stamp for one domain must not answer for a different domain
    # on the same pair — each (pair, domain) is tracked independently.
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "population_hash_a": "h1", "population_hash_b": "h2",
            "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        }
    }
    assert comparison_is_stale("t", "p", "template_to_project", "line_patterns", registry, existing) is False
    assert comparison_is_stale("t", "p", "template_to_project", "object_styles_model", registry, existing) is True


def test_build_comparison_registry_rows_stamps_current_work_items():
    registry = {
        "t": _reg_row(population_hash="h1", conformance_reference_mode="latest"),
        "p": _reg_row(population_hash="h2"),
    }
    work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(work_items, registry, existing={}, computed_utc="2026-07-10T00:00:00Z")
    assert len(rows) == 1
    row = rows[0]
    assert row["segment_id_a"] == "t"
    assert row["segment_id_b"] == "p"
    assert row["domain"] == "line_patterns"
    assert row["population_hash_a"] == "h1"
    assert row["population_hash_b"] == "h2"
    assert row["conformance_reference_mode"] == "latest"
    assert row["computed_utc"] == "2026-07-10T00:00:00Z"


def test_build_comparison_registry_rows_carries_over_work_items_not_recomputed():
    # A --segment-a filtered run only recomputes a subset of work items;
    # previously recorded (pair, domain) entries outside that filter must
    # survive untouched.
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    existing = {
        ("other_a", "other_b", "sibling_projects", "line_patterns"): {
            "segment_id_a": "other_a", "segment_id_b": "other_b",
            "comparison_type": "sibling_projects", "domain": "line_patterns",
            "population_hash_a": "x1", "population_hash_b": "x2",
            "last_run_utc_a": "", "last_run_utc_b": "",
            "conformance_reference_mode": "latest", "computed_utc": "2026-06-01T00:00:00Z",
        }
    }
    work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(work_items, registry, existing, computed_utc="2026-07-10T00:00:00Z")
    keys = {(r["segment_id_a"], r["segment_id_b"], r["comparison_type"], r["domain"]) for r in rows}
    assert ("t", "p", "template_to_project", "line_patterns") in keys
    assert ("other_a", "other_b", "sibling_projects", "line_patterns") in keys
    carried = next(r for r in rows if r["segment_id_a"] == "other_a")
    assert carried["computed_utc"] == "2026-06-01T00:00:00Z"


def test_build_comparison_registry_rows_domain_scoped_run_does_not_stamp_other_domains():
    # This is the exact scenario the staleness tracking must get right: a
    # --domain line_patterns invocation only recomputes line_patterns for a
    # pair that also has object_styles_model comparisons on record. The
    # object_styles_model row must survive with its OLD stamp — not be
    # silently re-stamped current — or a later --dry-run would wrongly report
    # it as up to date despite never having been recomputed.
    registry = {
        "t": _reg_row(population_hash="h1-new"),  # template just re-ran with new files
        "p": _reg_row(population_hash="h2"),
    }
    existing = {
        ("t", "p", "template_to_project", "line_patterns"): {
            "segment_id_a": "t", "segment_id_b": "p",
            "comparison_type": "template_to_project", "domain": "line_patterns",
            "population_hash_a": "h1-old", "population_hash_b": "h2",
            "last_run_utc_a": "", "last_run_utc_b": "",
            "conformance_reference_mode": "latest", "computed_utc": "2026-06-01T00:00:00Z",
        },
        ("t", "p", "template_to_project", "object_styles_model"): {
            "segment_id_a": "t", "segment_id_b": "p",
            "comparison_type": "template_to_project", "domain": "object_styles_model",
            "population_hash_a": "h1-old", "population_hash_b": "h2",
            "last_run_utc_a": "", "last_run_utc_b": "",
            "conformance_reference_mode": "latest", "computed_utc": "2026-06-01T00:00:00Z",
        },
    }
    # Only line_patterns was recomputed this run (e.g. --domain line_patterns).
    work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(work_items, registry, existing, computed_utc="2026-07-10T00:00:00Z")

    line_patterns_row = next(r for r in rows if r["domain"] == "line_patterns")
    object_styles_row = next(r for r in rows if r["domain"] == "object_styles_model")

    # The recomputed domain gets a fresh stamp reflecting the template's new population.
    assert line_patterns_row["population_hash_a"] == "h1-new"
    assert line_patterns_row["computed_utc"] == "2026-07-10T00:00:00Z"

    # The domain that was NOT recomputed keeps its old stamp untouched, so a
    # later --dry-run still reports it stale (population_hash_a stayed at the
    # old value even though the live registry has moved to h1-new).
    assert object_styles_row["population_hash_a"] == "h1-old"
    assert object_styles_row["computed_utc"] == "2026-06-01T00:00:00Z"
    stale_check_registry = {"t": _reg_row(population_hash="h1-new"), "p": _reg_row(population_hash="h2")}
    rebuilt_registry = {
        (r["segment_id_a"], r["segment_id_b"], r["comparison_type"], r["domain"]): r for r in rows
    }
    assert comparison_is_stale(
        "t", "p", "template_to_project", "object_styles_model", stale_check_registry, rebuilt_registry
    ) is True


def test_load_comparison_registry_roundtrip(tmp_path):
    out_dir = tmp_path / "out"
    rows = [{
        "segment_id_a": "t", "segment_id_b": "p",
        "comparison_type": "template_to_project", "domain": "line_patterns",
        "population_hash_a": "h1", "population_hash_b": "h2",
        "last_run_utc_a": "2026-01-01T00:00:00Z", "last_run_utc_b": "2026-01-01T00:00:00Z",
        "conformance_reference_mode": "latest", "computed_utc": "2026-07-10T00:00:00Z",
    }]
    atomic_write_csv(out_dir / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS, rows)
    loaded = load_comparison_registry(out_dir)
    assert ("t", "p", "template_to_project", "line_patterns") in loaded
    assert loaded[("t", "p", "template_to_project", "line_patterns")]["population_hash_a"] == "h1"


def test_load_comparison_registry_missing_file_returns_empty(tmp_path):
    assert load_comparison_registry(tmp_path / "does_not_exist") == {}
