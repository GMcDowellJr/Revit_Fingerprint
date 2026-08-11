from __future__ import annotations
import csv, subprocess, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.discover_join_policy import _full_population_verify, _stratified_sample
from tools.join_key_discovery.eval import build_identity_index, score_candidate


def _write_csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# _full_population_verify: direct unit tests (precise, no sampling-hash guessing)
# ---------------------------------------------------------------------------

def _items_row(pk, k, v, q="ok"):
    return {"record_pk": pk, "item_key": k, "item_value_type": q, "item_value": v}


def test_full_population_verify_detects_fragmentation_sample_missed():
    # Full population: two records share the SAME sig_hash (same semantic identity)
    # but resolve to DIFFERENT join-key values -- a genuine fragmentation (one
    # identity split across two join keys). A "sample" that only saw one of the
    # two would have reported fragmentation_rate=0.
    full_records = [
        {"record_pk": "1", "sig_hash": "sigA"},
        {"record_pk": "2", "sig_hash": "sigA"},
    ]
    items = [_items_row("1", "field", "A"), _items_row("2", "field", "B")]
    identity_index = build_identity_index(items)
    cfg = {"gates": {}}

    metrics_sample = {"fragmentation_rate": 0.0, "collision_rate": 0.0}
    metrics_full, diverges = _full_population_verify(full_records, identity_index, ["field"], cfg, metrics_sample, divergence_delta=0.01)

    assert metrics_full["fragmentation_rate"] > 0.0
    assert diverges is True


def test_full_population_verify_no_divergence_when_full_matches_sample():
    full_records = [
        {"record_pk": "1", "sig_hash": "sigA"},
        {"record_pk": "2", "sig_hash": "sigA"},
    ]
    items = [_items_row("1", "field", "A"), _items_row("2", "field", "A")]
    identity_index = build_identity_index(items)
    cfg = {"gates": {}}

    metrics_sample = {"fragmentation_rate": 0.0, "collision_rate": 0.0}
    metrics_full, diverges = _full_population_verify(full_records, identity_index, ["field"], cfg, metrics_sample, divergence_delta=0.01)

    assert metrics_full["fragmentation_rate"] == 0.0
    assert diverges is False


def test_full_population_verify_flags_collision_rate_delta_above_threshold():
    # Two distinct sig_hash groups collapsing onto the same join key -- a collision,
    # not a fragmentation. Sample claimed 0 collisions; full population shows 100%.
    full_records = [
        {"record_pk": "1", "sig_hash": "sigA"},
        {"record_pk": "2", "sig_hash": "sigB"},
        {"record_pk": "3", "sig_hash": "sigA"},
        {"record_pk": "4", "sig_hash": "sigB"},
    ]
    items = [_items_row(pk, "field", "same") for pk in ("1", "2", "3", "4")]
    identity_index = build_identity_index(items)
    cfg = {"gates": {}}

    metrics_sample = {"fragmentation_rate": 0.0, "collision_rate": 0.0}
    metrics_full, diverges = _full_population_verify(full_records, identity_index, ["field"], cfg, metrics_sample, divergence_delta=0.01)

    assert metrics_full["collision_rate"] > 0.01
    assert diverges is True


# ---------------------------------------------------------------------------
# _stratified_sample: file_id special case (new -- discover_hash_policy.py's
# original only supported identity-item-key stratification)
# ---------------------------------------------------------------------------

def test_stratified_sample_by_file_id_balances_across_files():
    # One file with 20 records, one file with 1 record. Unstratified pooled
    # sampling would very likely be dominated by the 20-record file.
    records = [{"record_pk": f"a{i}", "file_id": "big"} for i in range(20)]
    records.append({"record_pk": "b0", "file_id": "small"})
    out = _stratified_sample(records, [], "file_id", sample_size=4, seed=17)
    file_ids = {r["file_id"] for r in out}
    assert "small" in file_ids  # the 1-record file must not be starved out
    assert len(out) == 4


def test_stratified_sample_by_file_id_is_deterministic():
    records = [{"record_pk": f"a{i}", "file_id": "f1" if i % 2 == 0 else "f2"} for i in range(20)]
    out1 = _stratified_sample(records, [], "file_id", sample_size=6, seed=17)
    out2 = _stratified_sample(records, [], "file_id", sample_size=6, seed=17)
    assert [r["record_pk"] for r in out1] == [r["record_pk"] for r in out2]


def test_stratified_sample_falls_back_to_flat_when_key_uncovered():
    records = [{"record_pk": str(i), "file_id": "f1"} for i in range(10)]
    # stratify by an item_key that doesn't exist anywhere -> no coverage -> flat sample
    out = _stratified_sample(records, [], "no_such_item_key", sample_size=3, seed=17)
    assert len(out) == 3


# ---------------------------------------------------------------------------
# CLI integration: new columns/flags on discover_join_policy.py itself
# ---------------------------------------------------------------------------

def test_full_verify_columns_present_by_default(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
        {"file_id": "f1", "domain": "units", "record_pk": "2", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
        {"domain": "units", "record_pk": "2", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--search-modes", "greedy", "--policy-modes", "discover",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    row = rows[0]
    for col in ("coverage_full", "collision_rate_full", "fragmentation_rate_full", "full_verify_status", "sample_vs_full_diverges", "stratify_by"):
        assert col in row
    assert row["full_verify_status"] == "ok"
    assert row["sample_vs_full_diverges"] == "false"


def test_no_full_verify_flag_skips_verification(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--search-modes", "greedy", "--policy-modes", "discover", "--no-full-verify",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert rows[0]["full_verify_status"] == "skipped_no_full_verify_flag"


def test_stratify_by_file_id_end_to_end(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    records = [{"file_id": "big", "domain": "units", "record_pk": f"a{i}", "sig_hash": f"s{i % 3}"} for i in range(20)]
    records.append({"file_id": "small", "domain": "units", "record_pk": "b0", "sig_hash": "s0"})
    items = [{"domain": "units", "record_pk": r["record_pk"], "item_key": "units.spec", "item_value_type": "str", "item_value": "s"} for r in records]
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], records)
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], items)

    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--search-modes", "greedy", "--policy-modes", "discover",
        "--sample-size", "4", "--stratify-by", "file_id",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert rows[0]["stratify_by"] == "file_id"
    assert int(rows[0]["records_sampled_domain"]) == 4
