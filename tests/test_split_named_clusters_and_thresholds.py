from __future__ import annotations

import csv
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from tools.compute_governance_thresholds import (
    compute_alignment_rates,
    compute_thresholds,
    jenks_natural_breaks,
)
from tools.phase2_analysis.split_detection_file_level import compute_named_cluster_flags
from tools.run_split_detection_all import _inject_split_contract_headers


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_compute_named_cluster_flags_largest_gap_and_equal_shares():
    summary = pd.DataFrame(
        [
            {"cluster_id": 0, "percentage": 60.0},
            {"cluster_id": 1, "percentage": 25.0},
            {"cluster_id": 2, "percentage": 15.0},
        ]
    )
    flags = compute_named_cluster_flags(summary)
    assert flags.dtype == bool
    assert flags.tolist() == [True, False, False]

    equal = pd.DataFrame(
        [
            {"cluster_id": 0, "percentage": 50.0},
            {"cluster_id": 1, "percentage": 50.0},
        ]
    )
    equal_flags = compute_named_cluster_flags(equal)
    assert equal_flags.tolist() == [True, True]


def test_thresholds_and_fallback_consistency(monkeypatch):
    values = [0.21, 0.24, 0.27, 0.55, 0.61, 0.66, 0.84, 0.9, 0.94]
    with_jenkspy = jenks_natural_breaks(values, n_classes=3)

    import builtins

    real_import = builtins.__import__

    def _block_jenkspy(name, *args, **kwargs):
        if name == "jenkspy":
            raise ImportError("blocked for fallback test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_jenkspy)
    fallback = jenks_natural_breaks(values, n_classes=3)

    assert fallback == with_jenkspy
    thresholds = compute_thresholds({f"d{i}": v for i, v in enumerate(values)})
    assert thresholds["stable_min"] > thresholds["emerging_min"] > 0


def test_compute_alignment_rates_and_contract_header_preserves_is_named_cluster(tmp_path: Path):
    split_root = tmp_path / "split"
    _write_csv(
        split_root / "domain_a" / "file_level" / "domain_a.cluster_summary.csv",
        ["cluster_id", "percentage", "is_named_cluster"],
        [
            {"cluster_id": "0", "percentage": "75.0", "is_named_cluster": "True"},
            {"cluster_id": "1", "percentage": "25.0", "is_named_cluster": "False"},
        ],
    )
    _write_csv(
        split_root / "domain_b" / "file_level" / "domain_b.cluster_summary.csv",
        ["cluster_id", "percentage", "is_named_cluster"],
        [
            {"cluster_id": "0", "percentage": "55.0", "is_named_cluster": "True"},
            {"cluster_id": "1", "percentage": "45.0", "is_named_cluster": "False"},
        ],
    )

    rates = compute_alignment_rates(split_root)
    assert rates["domain_a"] == 0.75
    assert rates["domain_b"] == 0.55

    _write_csv(
        split_root / "domain_a" / "file_level" / "domain_a.file_clusters.csv",
        ["file_id", "cluster_id", "cluster_size", "is_named_cluster"],
        [{"file_id": "f1", "cluster_id": "0", "cluster_size": "1", "is_named_cluster": "True"}],
    )
    _inject_split_contract_headers(
        split_root,
        domain="domain_a",
        analysis_run_id="ana_test",
        file_to_export={"f1": "exp1"},
    )

    with (split_root / "domain_a" / "file_level" / "domain_a.file_clusters.csv").open(
        encoding="utf-8", newline=""
    ) as f:
        row = next(csv.DictReader(f))
    assert "is_named_cluster" in row
    assert row["is_named_cluster"] == "True"
