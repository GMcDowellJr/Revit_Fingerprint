"""Tests for explicit cardinality/status and aggregation-method semantics in
tools/compare_cross_segment.py.

Replaces the removed n_files >= 5 data_sufficient gate (already stripped by a
prior PR before this one) with non-suppressive comparison_status/
cardinality_shape/file_count_ratio fields, adds population-union metrics
(all_union_*/used_union_*) alongside the renamed cartesian-file-pair-mean
fields (all_pairwise_*/used_pairwise_*), adds side-balanced summaries, and
adds directed-reference heterogeneity diagnostics. See the "Required tests"
section of the cross-compare cardinality/aggregation spec this file
implements.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

import compare_cross_segment as ccs  # noqa: E402
from compare_cross_segment import (  # noqa: E402
    compare_directed_file,
    compare_symmetric_file,
    run_pair,
    run_pooled_comparison,
    _segment_domain_source_status,
)


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = list(rows[0].keys()) if rows else ["_empty"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_segment(seg_root: Path, folder: str, domain: str, files):
    """files: {export_run_id: [join_hash, ...]} -- writes domain_patterns.csv
    and an all-view membership_matrix.csv covering every file/join_hash."""
    base = seg_root / folder / "results"
    patterns = []
    mm_rows = []
    for eid, jhs in files.items():
        for i, jh in enumerate(jhs):
            pid = f"{eid}_p{i}"
            patterns.append({
                "domain": domain, "pattern_id": pid,
                "source_cluster_id": f"src|{jh}",
                "pattern_label_human": jh, "pattern_label": jh,
            })
            mm_rows.append({"export_run_id": eid, "pattern_id": pid})
    _write_csv(base / "analysis" / "domain_patterns.csv", patterns)
    _write_csv(base / "bundle_analysis" / "all" / domain / "membership_matrix.csv", mm_rows)
    _write_csv(base / "bundle_analysis" / "used" / domain / "membership_matrix.csv", mm_rows)


def _manifest_entry(role="Project", client="Acme", bc="", unit="imperial"):
    return {
        "segment_label": "seg", "governance_role": role, "client_label": client,
        "business_center_label": bc, "unit_system": unit, "discipline_label": "",
    }


def _registry_entry(folder):
    return {"output_folder": folder, "run_type": "bundle"}


def _clear_caches():
    ccs._jh_cache.clear()
    ccs._pattern_label_cache.clear()
    ccs._bundle_jh_cache.clear()


# ---------------------------------------------------------------------------
# 1. 1x1 comparison
# ---------------------------------------------------------------------------

def test_one_by_one_comparison_is_ok_and_populates_union_and_pairwise(tmp_path):
    _clear_caches()
    domain = "d1x1"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "a", domain, {"fa1": ["jh1", "jh2"]})
    _write_segment(segments_root, "b", domain, {"fb1": ["jh1", "jh3"]})
    manifest = {"a": _manifest_entry(), "b": _manifest_entry()}
    registry = {"a": _registry_entry("a"), "b": _registry_entry("b")}

    row, pairs = run_pair(
        "a", "b", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row is not None
    assert row["comparison_status"] == "ok"
    assert row["cardinality_shape"] == "balanced"
    assert row["file_count_ratio"] == "1.000000"
    assert row["all_union_jaccard"] != ""
    assert row["all_pairwise_jaccard_mean"] != ""
    assert row["aggregation_method"] == "cartesian_file_pair_mean"
    assert len(pairs) == 1


# ---------------------------------------------------------------------------
# 2. 1x20 comparison
# ---------------------------------------------------------------------------

def test_one_by_twenty_comparison_is_degraded_single_a(tmp_path):
    _clear_caches()
    domain = "d1x20"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "a", domain, {"fa1": ["jh1", "jh2"]})
    b_files = {f"fb{i}": [f"jh{i}", "jh1"] for i in range(20)}
    _write_segment(segments_root, "b", domain, b_files)
    manifest = {"a": _manifest_entry(), "b": _manifest_entry()}
    registry = {"a": _registry_entry("a"), "b": _registry_entry("b")}

    row, pairs = run_pair(
        "a", "b", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row is not None
    assert row["n_files_a"] == "1" and row["n_files_b"] == "20"
    assert row["cardinality_shape"] == "single_a"
    assert row["comparison_status"] == "degraded"
    assert len(pairs) == 20
    assert row["all_union_containment_a_in_b"] != ""
    assert row["all_a_file_mean_similarity_to_b_mean"] != ""
    assert row["all_b_file_mean_similarity_to_a_mean"] != ""


# ---------------------------------------------------------------------------
# 3. 3x20 comparison -- n_pairs must not be treated as evidence sufficiency
# ---------------------------------------------------------------------------

def test_three_by_twenty_comparison_n_pairs_not_used_for_status(tmp_path):
    _clear_caches()
    domain = "d3x20"
    segments_root = tmp_path / "segments"
    a_files = {f"fa{i}": [f"jha{i}", "shared"] for i in range(3)}
    b_files = {f"fb{i}": [f"jhb{i}", "shared"] for i in range(20)}
    _write_segment(segments_root, "a", domain, a_files)
    _write_segment(segments_root, "b", domain, b_files)
    manifest = {"a": _manifest_entry(), "b": _manifest_entry()}
    registry = {"a": _registry_entry("a"), "b": _registry_entry("b")}

    row, pairs = run_pair(
        "a", "b", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row["n_pairs"] == "60"
    assert row["cardinality_shape"] == "imbalanced"
    # 60 file pairs is not treated as narrow evidence -- status stays ok,
    # not degraded, because neither side is a lone file.
    assert row["comparison_status"] == "ok"


# ---------------------------------------------------------------------------
# 4/5. Union metrics stable under duplication; pairwise mean is not
# ---------------------------------------------------------------------------

def test_union_metrics_stable_pairwise_mean_shifts_under_duplication(tmp_path):
    _clear_caches()
    domain = "ddup1"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "a", domain, {"fa1": ["jh1", "jh2"]})
    # fb1 matches fa1 exactly (jaccard 1.0); fb2 shares nothing (jaccard 0.0).
    _write_segment(segments_root, "b", domain, {"fb1": ["jh1", "jh2"], "fb2": ["jh9"]})
    manifest = {"a": _manifest_entry(), "b": _manifest_entry()}
    registry = {"a": _registry_entry("a"), "b": _registry_entry("b")}
    row1, _ = run_pair(
        "a", "b", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    _clear_caches()
    domain2 = "ddup2"
    # Same A side and same B union footprint (jh1,jh2,jh9) as row1, but with
    # an added fb3 that exactly duplicates fb1's vocabulary -- a duplicate
    # file on the B side that doesn't change B's union footprint at all, but
    # does add another perfect-match pair to the Cartesian mean.
    _write_segment(segments_root, "a2", domain2, {"fa1": ["jh1", "jh2"]})
    _write_segment(segments_root, "b2", domain2, {
        "fb1": ["jh1", "jh2"], "fb2": ["jh9"], "fb3": ["jh1", "jh2"],
    })
    manifest2 = {"a2": _manifest_entry(), "b2": _manifest_entry()}
    registry2 = {"a2": _registry_entry("a2"), "b2": _registry_entry("b2")}
    row2, _ = run_pair(
        "a2", "b2", "cross_client", domain2, manifest2, registry2, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    # Union footprint of B is identical (jh1,jh3) whether it has 2 or 3
    # copies of the same file -- all_union_* is unchanged.
    assert row1["all_union_jaccard"] == row2["all_union_jaccard"]
    assert row1["all_union_containment_a_in_b"] == row2["all_union_containment_a_in_b"]
    assert row1["all_union_containment_b_in_a"] == row2["all_union_containment_b_in_a"]

    # Cartesian pairwise mean DOES shift: more duplicate high-similarity
    # pairs pull the mean toward that duplicate's score.
    assert row1["all_pairwise_jaccard_mean"] != row2["all_pairwise_jaccard_mean"]
    assert row1["aggregation_method"] == "cartesian_file_pair_mean"
    assert row2["aggregation_method"] == "cartesian_file_pair_mean"


# ---------------------------------------------------------------------------
# 6. Directed multi-reference heterogeneity
# ---------------------------------------------------------------------------

def test_directed_reference_heterogeneity_core_share_below_one():
    ref_files = {
        "r1": {"jh1", "jh2", "jh3"},
        "r2": {"jh1", "jh2", "jh3"},  # near-identical to r1
        "r3": {"jh9"},  # divergent
    }
    tgt_files = {"t1": {"jh1", "jh2"}}

    metrics = compare_directed_file(ref_files, tgt_files)

    assert metrics["n_reference_files"] == "3"
    assert float(metrics["reference_core_share"]) < 1.0
    # Heterogeneity diagnostics don't change the existing containment math.
    assert metrics["all_pairwise_containment_a_in_b_mean"] != ""
    assert metrics["all_pairwise_containment_b_in_a_mean"] != ""


# ---------------------------------------------------------------------------
# 7. One-file reference -- no artificial failure
# ---------------------------------------------------------------------------

def test_directed_single_file_reference_produces_normal_output(tmp_path):
    _clear_caches()
    domain = "d1ref"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "ref", domain, {"fr1": ["jh1", "jh2"]})
    _write_segment(segments_root, "tgt", domain, {"ft1": ["jh1"]})
    manifest = {"ref": _manifest_entry(), "tgt": _manifest_entry()}
    registry = {"ref": _registry_entry("ref"), "tgt": _registry_entry("tgt")}

    row, pairs = run_pair(
        "ref", "tgt", "template_to_project", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row is not None
    assert row["n_reference_files"] == "1"
    assert row["comparison_status"] != "blocked"
    assert row["reference_aggregation"] == "union"
    assert row["target_aggregation"] == "per_file_distribution"
    # A single-file reference is trivially coherent with itself -- core
    # share degrades gracefully to 1.0, not an artificial failure.
    assert row["reference_core_share"] == "1.000000"


# ---------------------------------------------------------------------------
# 8. Zero readable files -- actually blocked
# ---------------------------------------------------------------------------

def test_zero_files_on_either_side_is_blocked_not_zero_valued(tmp_path):
    _clear_caches()
    domain = "dblocked"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "populated", domain, {"fp1": ["jh1", "jh2"]})
    manifest = {
        "populated": _manifest_entry(),
        "missing": _manifest_entry(),
    }
    # "missing" has a registry entry but no output_folder on disk at all --
    # domain_patterns.csv can't be found.
    registry = {
        "populated": _registry_entry("populated"),
        "missing": _registry_entry("does_not_exist"),
    }

    row, pairs = run_pair(
        "missing", "populated", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row is not None
    assert pairs == []
    assert row["comparison_status"] == "blocked"
    assert row["n_files_a"] == "0"
    assert row["n_files_b"] == "1"
    # Not a zero-valued similarity row -- blank, not "0.000000".
    assert row["all_pairwise_jaccard_mean"] == ""
    assert row.get("all_union_jaccard", "") == ""
    # The populated side's real pattern counts must survive on a blocked
    # row -- only the blocked side is legitimately zero. "populated" has 2
    # patterns (jh1, jh2); reporting n_patterns_b/n_unique_patterns_b as 0
    # here would corrupt the raw inventory count a downstream reader needs
    # to understand what was actually blocked.
    assert row["n_patterns_a"] == "0"
    assert row["n_patterns_b"] == "2"
    assert row["n_unique_patterns_a"] == "0"
    assert row["n_unique_patterns_b"] == "2"


# ---------------------------------------------------------------------------
# 9. Empty vs. unreadable inventory distinguished
# ---------------------------------------------------------------------------

def test_empty_domain_and_unreadable_segment_get_different_inventory_status(tmp_path):
    _clear_caches()
    domain = "dinv"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "populated", domain, {"fp1": ["jh1"]})
    # "empty": segment output exists, domain_patterns.csv exists, but has
    # zero rows for this domain -- a confirmed, successfully-read empty
    # inventory (legitimately doesn't use this domain).
    _write_csv(
        segments_root / "empty" / "results" / "analysis" / "domain_patterns.csv",
        [{"domain": "other_domain", "pattern_id": "x", "source_cluster_id": "src|jhX",
          "pattern_label_human": "x", "pattern_label": "x"}],
    )
    # "unreadable": registry entry present, but nothing at all was written to
    # disk for it -- domain_patterns.csv itself is missing.
    manifest = {
        "populated": _manifest_entry(), "empty": _manifest_entry(), "unreadable": _manifest_entry(),
    }
    registry = {
        "populated": _registry_entry("populated"),
        "empty": _registry_entry("empty"),
        "unreadable": _registry_entry("nonexistent_folder"),
    }

    status_empty, _ = _segment_domain_source_status(segments_root, registry, "empty", domain)
    status_unreadable, _ = _segment_domain_source_status(segments_root, registry, "unreadable", domain)
    assert status_empty == "no_patterns"
    assert status_unreadable == "missing_domain_patterns"
    assert status_empty != status_unreadable

    row_empty, _ = run_pair(
        "empty", "populated", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )
    row_unreadable, _ = run_pair(
        "unreadable", "populated", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row_empty["comparison_status"] == "blocked"
    assert row_unreadable["comparison_status"] == "blocked"
    assert row_empty["inventory_status_a"] == "no_patterns"
    assert row_unreadable["inventory_status_a"] == "missing_domain_patterns"
    assert row_empty["inventory_status_a"] != row_unreadable["inventory_status_a"]


# ---------------------------------------------------------------------------
# 10. cross_client / client_cross_bc imbalance check (Kaiser-style fixture)
# ---------------------------------------------------------------------------

def test_union_containment_does_not_track_file_count_ratio_like_pairwise_mean(tmp_path):
    """A client population dominated by one business center (imbalanced file
    counts across BCs) should not have its union containment swing just
    because one side happens to have more files -- unlike the pairwise mean,
    which is dominated by whichever side has more files contributing pairs."""
    _clear_caches()
    domain = "dkaiser"
    segments_root = tmp_path / "segments"
    # bc_dominant: 18 files, all near-identical vocabulary {core1, core2}.
    dominant_files = {f"fd{i}": ["core1", "core2"] for i in range(18)}
    # bc_small: 2 files with a distinct but overlapping vocabulary.
    small_files = {"fs1": ["core1", "unique1"], "fs2": ["core1", "unique2"]}
    _write_segment(segments_root, "bc_dominant", domain, dominant_files)
    _write_segment(segments_root, "bc_small", domain, small_files)
    manifest = {"bc_dominant": _manifest_entry(), "bc_small": _manifest_entry()}
    registry = {"bc_dominant": _registry_entry("bc_dominant"), "bc_small": _registry_entry("bc_small")}

    row, _ = run_pair(
        "bc_dominant", "bc_small", "client_cross_bc", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row["file_count_ratio"] == "9.000000"
    # Union containment reflects the true footprint overlap (core1 is shared,
    # core2/unique1/unique2 are not) -- independent of the 9:1 file skew.
    union_c_a_in_b = float(row["all_union_containment_a_in_b"])
    union_c_b_in_a = float(row["all_union_containment_b_in_a"])
    assert union_c_a_in_b == 0.5  # {core1,core2} ∩ {core1,unique1,unique2} = {core1} -> 1/2
    assert abs(union_c_b_in_a - (1 / 3)) < 1e-5  # {core1} / {core1,unique1,unique2} = 1/3

    # The pairwise mean, by contrast, is computed over 36 file pairs, all of
    # which share exactly the same per-pair shape -- it does not equal the
    # union view's answer to "how much of B's footprint is covered by A".
    assert row["all_pairwise_jaccard_mean"] != row["all_union_jaccard"]


# ---------------------------------------------------------------------------
# 11. No new suppression reintroduced
# ---------------------------------------------------------------------------

def test_single_file_side_is_never_blocked(tmp_path):
    _clear_caches()
    domain = "dsingle"
    segments_root = tmp_path / "segments"
    _write_segment(segments_root, "a", domain, {"fa1": ["jh1"]})
    _write_segment(segments_root, "b", domain, {f"fb{i}": [f"jh{i}"] for i in range(5)})
    manifest = {"a": _manifest_entry(), "b": _manifest_entry()}
    registry = {"a": _registry_entry("a"), "b": _registry_entry("b")}

    row, _ = run_pair(
        "a", "b", "cross_client", domain, manifest, registry, {},
        segments_root, min_patterns=1, executed_utc="2026-07-20T00:00:00Z",
    )

    assert row["comparison_status"] != "blocked"
    assert row["comparison_status"] == "degraded"


# ---------------------------------------------------------------------------
# Pooled comparison: pool-only domains must still be scheduled for a
# zero-inventory focal segment (found in PR review; run_pooled_comparison()
# only iterated the focal segment's own domains, so a domain the focal has
# zero inventory for but its pool has real data in was never scheduled at
# all -- the blocked-row path added to _build_pooled_row() never got a
# chance to run for that case).
# ---------------------------------------------------------------------------

def test_pooled_comparison_schedules_pool_only_domain_for_empty_focal(tmp_path):
    _clear_caches()
    domain = "pooldom"
    segments_root = tmp_path / "segments"
    # focal_empty has literally no domain_patterns.csv at all -- zero
    # inventory for every domain, including "pooldom", which only the pool
    # sibling has.
    (segments_root / "focal_empty").mkdir(parents=True)
    _write_segment(segments_root, "pool_sib", domain, {"fp1": ["jh1", "jh2"]})

    manifest = {
        "focal_empty": {**_manifest_entry(), "parent_segment_id": "p1"},
        "pool_sib": {**_manifest_entry(), "parent_segment_id": "p1"},
    }
    registry = {
        "focal_empty": _registry_entry("focal_empty"),
        "pool_sib": _registry_entry("pool_sib"),
    }

    rows = run_pooled_comparison(
        manifest, registry, segments_root, min_patterns=1,
        executed_utc="2026-07-20T00:00:00Z", focal_segment_ids={"focal_empty"},
    )

    # Both segments share client_label="Acme" too (via _manifest_entry()'s
    # default), so this also produces a "client" pool_scope row alongside
    # "parent_sibling" -- restrict to the grain under test.
    pooldom_rows = [
        r for r in rows if r["domain"] == domain and r["pool_scope"] == "parent_sibling"
    ]
    assert len(pooldom_rows) == 1
    row = pooldom_rows[0]
    assert row["segment_id"] == "focal_empty"
    assert row["n_files_focal"] == "0"
    assert row["n_files_pool"] == "1"
    assert row["comparison_status"] == "blocked"
