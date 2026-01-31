# -*- coding: utf-8 -*-

import csv
from pathlib import Path

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

pandas = pytest.importorskip("pandas")

from tools import pareto_joinkey_search


def _write_csv(path: Path, header, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def test_pareto_shape_gating_per_shape(tmp_path: Path, monkeypatch):
    records_path = tmp_path / "records.csv"
    items_path = tmp_path / "identity_items.csv"
    policy_path = tmp_path / "policy.json"
    out_dir = tmp_path / "out"

    _write_csv(
        records_path,
        ["file_id", "domain", "record_id", "sig_hash"],
        [
            ["f1", "shapes", "1", "sig1"],
            ["f1", "shapes", "2", "sig2"],
        ],
    )

    _write_csv(
        items_path,
        ["file_id", "domain", "record_id", "k", "v"],
        [
            ["f1", "shapes", "1", "shape", "Alpha"],
            ["f1", "shapes", "1", "common", "c1"],
            ["f1", "shapes", "1", "opt", "o1"],
            ["f1", "shapes", "2", "shape", "Beta"],
            ["f1", "shapes", "2", "common", "c2"],
            ["f1", "shapes", "2", "opt", "o2"],
        ],
    )

    policy = {
        "domains": {
            "shapes": {
                "join_key_schema": "shapes.join_key.v1",
                "hash_alg": "md5_utf8_join_pipe",
                "required_items": ["shape", "common"],
                "optional_items": ["opt"],
                "explicitly_excluded_items": [],
                "shape_gating": {
                    "discriminator_key": "shape",
                    "shape_requirements": {
                        "Alpha": {"additional_required": ["opt"], "additional_optional": []},
                        "Beta": {"additional_required": ["opt"], "additional_optional": []},
                    },
                    "default_shape_behavior": "common_only",
                },
            }
        }
    }
    policy_path.write_text(__import__("json").dumps(policy))

    args = [
        "prog",
        "--records",
        str(records_path),
        "--items",
        str(items_path),
        "--domain",
        "shapes",
        "--policy_json",
        str(policy_path),
        "--mode",
        "validate",
        "--max_k",
        "3",
        "--shape_mode",
        "per_shape",
        "--out_dir",
        str(out_dir),
    ]
    monkeypatch.setattr("sys.argv", args)
    pareto_joinkey_search.main()

    rollup = out_dir / "pareto__shapes__shape_rollup.csv"
    assert rollup.exists()

    alpha_out = out_dir / "pareto__shapes__shape__Alpha.csv"
    beta_out = out_dir / "pareto__shapes__shape__Beta.csv"
    assert alpha_out.exists()
    assert beta_out.exists()

    pareto = out_dir / "pareto_front.csv"
    assert pareto.exists()

    pareto_text = pareto.read_text(encoding="utf-8")
    assert "shape|common" in pareto_text
    assert "common|shape" not in pareto_text
