import csv
import json
import subprocess
import sys
from pathlib import Path

from tools.join_key_discovery.eval import build_identity_index, score_candidate


def _item(pk: str, key: str, value: str) -> dict[str, str]:
    return {"record_pk": pk, "item_key": key, "item_value_type": "str", "item_value": value}


def _fixture():
    records = [
        {"record_pk": "1", "sig_hash": "s1"},
        {"record_pk": "2", "sig_hash": "s2"},
    ]
    items = []
    for pk, shape, policy, candidate, specific in (
        ("1", "Alpha", "same", "x1", "hidden1"),
        ("2", "Alpha", "same", "x2", "hidden2"),
    ):
        items.extend(
            _item(pk, key, value)
            for key, value in (
                ("shape", shape),
                ("A", policy),
                ("B", f"optional-{pk}"),
                ("C", candidate),
                ("alpha_specific", specific),
            )
        )
    gates = {
        "discriminator_key": "shape",
        "shape_requirements": {"Alpha": {"additional_required": ["alpha_specific"]}},
    }
    return records, build_identity_index(items), gates


def test_discovery_scores_only_the_candidate_not_policy_or_shape_requirements():
    records, index, gates = _fixture()
    metrics = score_candidate(
        records,
        index,
        ["C"],
        {
            "evaluation_mode": "candidate",
            "runtime_required_fields": ["A"],
            "gates": gates,
        },
    )

    assert metrics["selected_fields"] == ["C"]
    assert metrics["effective_fields_actually_scored"] == ["C"]
    assert metrics["collision_rate"] == 0.0


def test_validation_applies_base_and_shape_required_fields_exactly():
    records, index, gates = _fixture()
    metrics = score_candidate(
        records,
        index,
        ["A"],
        {
            "evaluation_mode": "runtime",
            "runtime_required_fields": ["A"],
            "gates": gates,
        },
    )

    assert metrics["selected_fields"] == ["A"]
    assert metrics["effective_fields_actually_scored"] == ["A", "alpha_specific"]
    assert metrics["collision_rate"] == 0.0


def test_per_shape_candidate_scoring_can_select_independent_evidence():
    records = [
        {"record_pk": "a1", "sig_hash": "a1"},
        {"record_pk": "a2", "sig_hash": "a2"},
        {"record_pk": "b1", "sig_hash": "b1"},
        {"record_pk": "b2", "sig_hash": "b2"},
    ]
    items = [
        _item("a1", "shape", "Alpha"), _item("a2", "shape", "Alpha"),
        _item("b1", "shape", "Beta"), _item("b2", "shape", "Beta"),
        _item("a1", "alpha_x", "1"), _item("a2", "alpha_x", "2"),
        _item("b1", "beta_x", "1"), _item("b2", "beta_x", "2"),
    ]
    index = build_identity_index(items)
    alpha = score_candidate(records[:2], index, ["alpha_x"], {"evaluation_mode": "candidate"})
    beta = score_candidate(records[2:], index, ["beta_x"], {"evaluation_mode": "candidate"})

    assert alpha["effective_fields_actually_scored"] == ["alpha_x"]
    assert beta["effective_fields_actually_scored"] == ["beta_x"]
    assert alpha["coverage"] == beta["coverage"] == 1.0


def test_join_discover_cli_does_not_seed_existing_required_field(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    phase0.mkdir(parents=True)
    with (phase0 / "records.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain", "record_pk", "sig_hash"])
        writer.writeheader()
        writer.writerows([
            {"domain": "demo", "record_pk": "1", "sig_hash": "s1"},
            {"domain": "demo", "record_pk": "2", "sig_hash": "s2"},
        ])
    with (phase0 / "identity_items.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain", "record_pk", "item_key", "item_value_type", "item_value"])
        writer.writeheader()
        writer.writerows([
            {"domain": "demo", "record_pk": "1", "item_key": "A", "item_value_type": "str", "item_value": "same"},
            {"domain": "demo", "record_pk": "1", "item_key": "C", "item_value_type": "str", "item_value": "one"},
            {"domain": "demo", "record_pk": "2", "item_key": "C", "item_value_type": "str", "item_value": "two"},
        ])
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"domains": {"demo": {"required_items": ["A"], "optional_items": []}}}), encoding="utf-8")

    subprocess.run(
        [sys.executable, "tools/discover_join_policy.py", "--phase0-dir", str(phase0),
         "--domains", "demo", "--policy-json", str(policy), "--policy-modes", "discover",
         "--search-modes", "greedy", "--max-candidate-fields", "1"],
        cwd=Path(__file__).resolve().parents[1], check=True,
    )
    output = phase0.parent / "diagnostics" / "join_key_discovery_exploration__demo__discover.csv"
    with output.open(newline="", encoding="utf-8") as f:
        row = next(csv.DictReader(f))

    assert row["selected_fields"] == "C"
    assert row["effective_fields_actually_scored"] == "C"
    assert row["policy_required_fields"] == "A"


def test_join_discover_cli_emits_independent_shape_partitions(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    phase0.mkdir(parents=True)
    records = [
        {"domain": "demo", "record_pk": pk, "sig_hash": pk}
        for pk in ("a1", "a2", "b1", "b2")
    ]
    with (phase0 / "records.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain", "record_pk", "sig_hash"])
        writer.writeheader(); writer.writerows(records)
    item_rows = []
    for pk, shape in (("a1", "Alpha"), ("a2", "Alpha"), ("b1", "Beta"), ("b2", "Beta")):
        item_rows.append({"domain": "demo", **_item(pk, "shape", shape)})
    item_rows += [
        {"domain": "demo", **_item("a1", "alpha_x", "1")}, {"domain": "demo", **_item("a2", "alpha_x", "2")},
        {"domain": "demo", **_item("b1", "beta_x", "1")}, {"domain": "demo", **_item("b2", "beta_x", "2")},
    ]
    with (phase0 / "identity_items.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain", "record_pk", "item_key", "item_value_type", "item_value"])
        writer.writeheader(); writer.writerows(item_rows)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"domains": {"demo": {
        "required_items": ["shape"],
        "shape_gating": {"discriminator_key": "shape", "shape_requirements": {
            "Alpha": {"additional_required": ["ratified_alpha"]},
            "Beta": {"additional_required": ["ratified_beta"]},
        }},
    }}}), encoding="utf-8")
    subprocess.run(
        [sys.executable, "tools/discover_join_policy.py", "--phase0-dir", str(phase0),
         "--domains", "demo", "--policy-json", str(policy), "--policy-modes", "discover",
         "--search-modes", "greedy", "--max-k", "2", "--sample-size", "1"],
        cwd=Path(__file__).resolve().parents[1], check=True,
    )
    output = phase0.parent / "diagnostics" / "join_key_discovery_exploration__demo__discover.csv"
    with output.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    partitions = {row["discriminator_value"]: row for row in rows if row["discriminator_value"] != "__all__"}

    assert "alpha_x" in partitions["Alpha"]["effective_fields_actually_scored"]
    assert "beta_x" in partitions["Beta"]["effective_fields_actually_scored"]
    assert "ratified_alpha" not in partitions["Alpha"]["effective_fields_actually_scored"]
    assert all(row["discriminator_source"] == "existing_policy" for row in partitions.values())
    assert all(row["records_total_partition"] == "2" for row in partitions.values())
    assert all(row["records_sampled_partition"] == "1" for row in partitions.values())
