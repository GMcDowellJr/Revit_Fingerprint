"""Regression test for streamed (incremental) cross_segment_file_pairs.csv writes.

Verifies main() writes real, matched file-pair rows directly to disk during the
comparison loop instead of accumulating them all in memory and sorting at the
end (see compare_cross_segment.py's pair_detail_tmp streaming writer).
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import main as compare_main, PAIRS_FIELDS  # noqa: E402


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_segment(seg_root: Path, folder: str, domain: str, patterns, all_rows, used_rows, bundle_all):
    base = seg_root / folder / "results"
    _write_csv(
        base / "analysis" / "domain_patterns.csv",
        [
            {
                "domain": domain,
                "pattern_id": pid,
                "source_cluster_id": f"src|{jh}",
                "pattern_label_human": label,
                "pattern_label": label,
            }
            for pid, jh, label in patterns
        ],
    )
    _write_csv(base / "bundle_analysis" / "all" / domain / "membership_matrix.csv", all_rows)
    _write_csv(base / "bundle_analysis" / "used" / domain / "membership_matrix.csv", used_rows)
    _write_csv(
        base / "bundle_analysis" / "all" / domain / "bundle_membership.csv",
        [{"pattern_id": pid} for pid in bundle_all],
    )


def test_main_streams_real_file_pair_rows_to_disk(tmp_path, monkeypatch):
    domain = "line_patterns"
    records_dir = tmp_path / "records"
    segments_root = tmp_path / "segments"
    out_dir = tmp_path / "out"
    records_dir.mkdir()

    _write_csv(
        records_dir / "segment_manifest.csv",
        [
            {
                "segment_id": "proj_a",
                "segment_label": "Project A",
                "governance_role": "Project",
                "client_label": "Acme",
                "discipline_label": "Arch",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
            {
                "segment_id": "proj_b",
                "segment_label": "Project B",
                "governance_role": "Project",
                "client_label": "Acme",
                "discipline_label": "Arch",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
        ],
    )
    _write_csv(
        records_dir / "run_registry.csv",
        [
            {"segment_id": "proj_a", "output_folder": "proj_a", "run_type": "bundle"},
            {"segment_id": "proj_b", "output_folder": "proj_b", "run_type": "bundle"},
        ],
    )
    _write_csv(
        records_dir / "file_metadata.csv",
        [
            {"export_run_id": "fa1", "project_label": ""},
            {"export_run_id": "fb1", "project_label": ""},
        ],
    )

    # Both segments share join hashes jh1/jh2/jh3 (same source_cluster_id suffix)
    # under different local pattern_ids, so run_pair() finds real overlap.
    _write_segment(
        segments_root, "proj_a", domain,
        [("p1", "jh1", "L1"), ("p2", "jh2", "L2"), ("p3", "jh3", "L3")],
        [{"export_run_id": "fa1", "pattern_id": pid} for pid in ("p1", "p2", "p3")],
        [{"export_run_id": "fa1", "pattern_id": "p1"}],
        ["p1", "p2", "p3"],
    )
    _write_segment(
        segments_root, "proj_b", domain,
        [("q1", "jh1", "L1"), ("q2", "jh2", "L2"), ("q3", "jh3", "L3")],
        [{"export_run_id": "fb1", "pattern_id": pid} for pid in ("q1", "q2", "q3")],
        [{"export_run_id": "fb1", "pattern_id": "q1"}],
        ["q1", "q2", "q3"],
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_cross_segment.py",
            "--segments-root", str(segments_root),
            "--records-dir", str(records_dir),
            "--out-dir", str(out_dir),
            "--sibling-segments",
            "--domain", domain,
            "--min-patterns", "1",
            "--workers", "1",
            "--no-delta",
        ],
    )

    assert compare_main() == 0

    pairs_path = out_dir / "cross_segment_file_pairs.csv"
    assert pairs_path.is_file()
    # No leftover temp file from the streaming writer.
    assert list(out_dir.glob("*.tmp")) == []

    with pairs_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == PAIRS_FIELDS
        rows = list(reader)

    assert len(rows) >= 1
    assert {r["segment_id_a"] for r in rows} == {"proj_a"}
    assert {r["segment_id_b"] for r in rows} == {"proj_b"}
    assert {r["domain"] for r in rows} == {domain}


def _build_sibling_fixture(tmp_path, domain):
    records_dir = tmp_path / "records"
    segments_root = tmp_path / "segments"
    out_dir = tmp_path / "out"
    records_dir.mkdir()

    _write_csv(
        records_dir / "segment_manifest.csv",
        [
            {
                "segment_id": "proj_a", "segment_label": "Project A",
                "governance_role": "Project", "client_label": "Acme",
                "discipline_label": "Arch", "unit_system": "imperial",
                "run_type": "bundle", "segment_level": "2",
                "parent_segment_id": "imperial",
            },
            {
                "segment_id": "proj_b", "segment_label": "Project B",
                "governance_role": "Project", "client_label": "Acme",
                "discipline_label": "Arch", "unit_system": "imperial",
                "run_type": "bundle", "segment_level": "2",
                "parent_segment_id": "imperial",
            },
        ],
    )
    _write_csv(
        records_dir / "run_registry.csv",
        [
            {"segment_id": "proj_a", "output_folder": "proj_a", "run_type": "bundle"},
            {"segment_id": "proj_b", "output_folder": "proj_b", "run_type": "bundle"},
        ],
    )
    _write_csv(
        records_dir / "file_metadata.csv",
        [
            {"export_run_id": "fa1", "project_label": ""},
            {"export_run_id": "fb1", "project_label": ""},
        ],
    )
    _write_segment(
        segments_root, "proj_a", domain,
        [("p1", "jh1", "L1"), ("p2", "jh2", "L2"), ("p3", "jh3", "L3")],
        [{"export_run_id": "fa1", "pattern_id": pid} for pid in ("p1", "p2", "p3")],
        [{"export_run_id": "fa1", "pattern_id": "p1"}],
        ["p1", "p2", "p3"],
    )
    _write_segment(
        segments_root, "proj_b", domain,
        [("q1", "jh1", "L1"), ("q2", "jh2", "L2"), ("q3", "jh3", "L3")],
        [{"export_run_id": "fb1", "pattern_id": pid} for pid in ("q1", "q2", "q3")],
        [{"export_run_id": "fb1", "pattern_id": "q1"}],
        ["q1", "q2", "q3"],
    )
    return records_dir, segments_root, out_dir


def test_failure_after_streaming_leaves_previous_pairs_file_untouched(tmp_path, monkeypatch):
    domain = "line_patterns"
    records_dir, segments_root, out_dir = _build_sibling_fixture(tmp_path, domain)

    argv = [
        "compare_cross_segment.py",
        "--segments-root", str(segments_root),
        "--records-dir", str(records_dir),
        "--out-dir", str(out_dir),
        "--sibling-segments",
        "--domain", domain,
        "--min-patterns", "1",
        "--workers", "1",
        "--no-delta",
    ]

    # First run succeeds and publishes a "previous run" pairs file.
    monkeypatch.setattr(sys, "argv", argv)
    assert compare_main() == 0
    pairs_path = out_dir / "cross_segment_file_pairs.csv"
    assert pairs_path.is_file()
    previous_content = pairs_path.read_text(encoding="utf-8")
    assert previous_content  # sanity: first run actually produced rows

    # Second run: force a failure in a post-loop step (run_pooled_comparison),
    # which happens after pair-domain rows are fully streamed to the temp file
    # but before the deferred rename in the "Write outputs" section.
    import compare_cross_segment as mod

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated failure in a post-loop step")

    monkeypatch.setattr(mod, "run_pooled_comparison", _boom)
    monkeypatch.setattr(sys, "argv", argv)
    try:
        compare_main()
        raised = False
    except RuntimeError:
        raised = True
    assert raised, "expected the simulated post-loop failure to propagate"

    # The published pairs file must be untouched by the failed second run —
    # the new run's rows must never have replaced it before the crash.
    assert pairs_path.read_text(encoding="utf-8") == previous_content

    # An orphaned, unrenamed temp file from the failed run's streaming writer
    # is the expected leftover (matches the pre-existing atomic_write_csv
    # convention of not cleaning up on a mid-write exception).
    assert list(out_dir.glob("*.tmp")) != []
