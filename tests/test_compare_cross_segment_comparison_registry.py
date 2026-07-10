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


def _reg_row(population_hash="h1", last_run_utc="2026-01-01T00:00:00Z", conformance_reference_mode="latest", status="complete"):
    return {
        "population_hash": population_hash,
        "last_run_utc": last_run_utc,
        "conformance_reference_mode": conformance_reference_mode,
        "status": status,
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


def test_build_comparison_registry_rows_stamps_completed_work_items():
    registry = {
        "t": _reg_row(population_hash="h1", conformance_reference_mode="latest"),
        "p": _reg_row(population_hash="h2"),
    }
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    assert len(rows) == 1
    row = rows[0]
    assert row["segment_id_a"] == "t"
    assert row["segment_id_b"] == "p"
    assert row["domain"] == "line_patterns"
    assert row["population_hash_a"] == "h1"
    assert row["population_hash_b"] == "h2"
    assert row["conformance_reference_mode"] == "latest"
    assert row["computed_utc"] == "2026-07-10T00:00:00Z"


def test_build_comparison_registry_rows_is_a_full_snapshot_no_carryover():
    # Every other output this tool writes (cross_segment_summary.csv etc.) is
    # a full atomic_write_csv replace from only this invocation's rows, never
    # a merge with a prior file. comparison_registry.csv must match that
    # semantics exactly: entries from a prior run that were NOT recomputed
    # this run must not appear in the output at all — carrying them forward
    # would claim data is current when the underlying output row was already
    # destroyed by this same (possibly scoped) invocation.
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    keys = {(r["segment_id_a"], r["segment_id_b"], r["comparison_type"], r["domain"]) for r in rows}
    assert keys == {("t", "p", "template_to_project", "line_patterns")}


def test_build_comparison_registry_rows_domain_scoped_run_omits_other_domains():
    # A --domain line_patterns invocation only recomputes line_patterns for a
    # pair that also has object_styles_model comparisons on record. The prior
    # object_styles_model output row was already wiped by this run sharing
    # the same --out-dir (cross_segment_summary.csv is fully overwritten from
    # only this run's rows), so its registry entry must be OMITTED — not
    # carried over with a stale-but-present stamp — so the next --dry-run
    # correctly reports it as never-computed/stale rather than current.
    registry = {
        "t": _reg_row(population_hash="h1-new"),  # template just re-ran with new files
        "p": _reg_row(population_hash="h2"),
    }
    # Only line_patterns was recomputed this run (e.g. --domain line_patterns).
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")

    domains = {r["domain"] for r in rows}
    assert domains == {"line_patterns"}

    rebuilt_registry = {
        (r["segment_id_a"], r["segment_id_b"], r["comparison_type"], r["domain"]): r for r in rows
    }
    # No recorded stamp at all for object_styles_model on this pair — a later
    # --dry-run must treat it as stale (never computed), never "current".
    assert comparison_is_stale(
        "t", "p", "template_to_project", "object_styles_model", registry, rebuilt_registry
    ) is True


def test_build_comparison_registry_rows_omits_work_items_with_no_output():
    # run_pair()/_run_pair_domain() returning None (e.g. a domain below
    # --min-patterns, or a within-project pair with no eligible file pairs)
    # means no output row was ever written for that (pair, domain) — the
    # caller must exclude such items from completed_work_items before calling
    # this function, so it never gets a "current" stamp for data that
    # doesn't exist.
    registry = {"t": _reg_row(population_hash="h1"), "p": _reg_row(population_hash="h2")}
    completed_work_items = []  # nothing produced output this run
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    assert rows == []


def test_build_comparison_registry_rows_omits_pair_when_reference_segment_is_pending():
    # build_segment_manifest.py updates population_hash to a segment's new
    # file population immediately on manifest rebuild, resetting status to
    # "pending" (and clearing last_run_utc) until the orchestrator actually
    # re-runs it. The segment's output folder on disk still holds the OLD
    # population's results in that window. A compare run then reads stale
    # on-disk data but must not get stamped with the segment's already-updated
    # (new) population_hash — once the segment finally reaches "complete"
    # with that same hash, a naive stamp would make a later --dry-run wrongly
    # report the pair as already current.
    registry = {
        "t": _reg_row(population_hash="h1-new", last_run_utc="", status="pending"),
        "p": _reg_row(population_hash="h2", status="complete"),
    }
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    assert rows == []


def test_build_comparison_registry_rows_omits_pair_when_target_segment_is_failed():
    registry = {
        "t": _reg_row(population_hash="h1", status="complete"),
        "p": _reg_row(population_hash="h2", status="failed"),
    }
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    assert rows == []


def test_build_comparison_registry_rows_stamps_when_both_sides_complete():
    registry = {
        "t": _reg_row(population_hash="h1", status="complete"),
        "p": _reg_row(population_hash="h2", status="complete"),
    }
    completed_work_items = [("t", "p", "template_to_project", "line_patterns")]
    rows = build_comparison_registry_rows(completed_work_items, registry, computed_utc="2026-07-10T00:00:00Z")
    assert len(rows) == 1


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
