from __future__ import annotations
import csv, json, subprocess, sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.discover_join_policy import _diagnostics_domain_suffix, _full_population_verify, _rank_all, _stratified_sample
from tools.join_key_discovery.eval import build_identity_index, score_candidate
from tools.join_key_discovery.greedy import discover_greedy


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


def test_full_population_verify_flags_coverage_collapse_even_with_zero_collision_and_fragmentation():
    # Only 1 of 5 full-population records has the candidate field populated at
    # all -- the other 4 fail the implicit required-field gate and are simply
    # uncovered, not colliding or fragmenting. collision_rate/fragmentation_rate
    # are computed only over *covered* records, so both stay 0 even though this
    # candidate is only applicable to 20% of the population -- a candidate that
    # "looked" globally applicable on a sample where it happened to cover 100%.
    full_records = [{"record_pk": str(i), "sig_hash": f"sig{i}"} for i in range(5)]
    items = [_items_row("0", "field", "A")]  # only record_pk "0" has the field
    identity_index = build_identity_index(items)
    cfg = {"gates": {}}

    metrics_sample = {"coverage": 1.0, "fragmentation_rate": 0.0, "collision_rate": 0.0}
    metrics_full, diverges = _full_population_verify(
        full_records, identity_index, ["field"], cfg, metrics_sample, divergence_delta=0.01, coverage_drop_threshold=0.05,
    )

    assert metrics_full["coverage"] == pytest.approx(0.2)
    assert metrics_full["collision_rate"] == 0.0
    assert metrics_full["fragmentation_rate"] == 0.0
    assert diverges is True  # caught by the coverage check even though collision/fragmentation look clean


def test_full_population_verify_no_divergence_for_coverage_drop_within_threshold():
    # A small, expected coverage dip (well under the 0.05 default threshold)
    # must not trigger a false-positive warning. Distinct field values per
    # record (not a shared constant) so covered records don't collide.
    full_records = [{"record_pk": str(i), "sig_hash": f"sig{i}"} for i in range(100)]
    items = [_items_row(str(i), "field", f"v{i}") for i in range(98)]  # 2/100 missing -> coverage 0.98
    identity_index = build_identity_index(items)
    cfg = {"gates": {}}

    metrics_sample = {"coverage": 1.0, "fragmentation_rate": 0.0, "collision_rate": 0.0}
    metrics_full, diverges = _full_population_verify(
        full_records, identity_index, ["field"], cfg, metrics_sample, divergence_delta=0.01, coverage_drop_threshold=0.05,
    )
    assert metrics_full["coverage"] == pytest.approx(0.98)
    assert diverges is False


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


def test_stratified_sample_survivors_are_not_lexicographically_first_group_when_groups_exceed_cap():
    # 1000 single-record files, sample_size=10: per_group = ceil(10/1000) = 1, so
    # every group contributes exactly one record before the out[:sample_size]
    # truncation decides which 10 of the 1000 groups survive. Iterating groups
    # in sorted() order would always keep f0000..f0009 regardless of seed --
    # the seed must actually determine which groups survive.
    records = [{"record_pk": f"r{i}", "file_id": f"f{i:04d}"} for i in range(1000)]
    out = _stratified_sample(records, [], "file_id", sample_size=10, seed=17)
    file_ids = sorted(r["file_id"] for r in out)
    lexicographically_first_ten = [f"f{i:04d}" for i in range(10)]
    assert file_ids != lexicographically_first_ten


def test_stratified_sample_group_selection_varies_by_seed_when_groups_exceed_cap():
    records = [{"record_pk": f"r{i}", "file_id": f"f{i:04d}"} for i in range(1000)]
    out_a = _stratified_sample(records, [], "file_id", sample_size=10, seed=17)
    out_b = _stratified_sample(records, [], "file_id", sample_size=10, seed=99)
    files_a = {r["file_id"] for r in out_a}
    files_b = {r["file_id"] for r in out_b}
    assert files_a != files_b


def test_stratified_sample_does_not_starve_records_missing_the_stratifier():
    # 1000 one-record files (known groups alone already exceed sample_size=500,
    # so the old top-up-only "ungrouped" handling would never run at all) plus
    # 200 records with blank file_id. Missing-stratifier records must get a fair
    # (proportional-to-1/n_groups) chance of survival, not zero representation
    # just because the known groups alone were enough to fill the cap first.
    records = [{"record_pk": f"r{i}", "file_id": f"f{i:04d}"} for i in range(1000)]
    records += [{"record_pk": f"blank{i}", "file_id": ""} for i in range(200)]
    out = _stratified_sample(records, [], "file_id", sample_size=500, seed=17)
    assert len(out) == 500
    blank_count = sum(1 for r in out if not r.get("file_id", "").strip())
    assert blank_count > 0


def test_stratified_sample_ungrouped_stratum_is_deterministic():
    records = [{"record_pk": f"r{i}", "file_id": f"f{i:04d}"} for i in range(1000)]
    records += [{"record_pk": f"blank{i}", "file_id": ""} for i in range(200)]
    out1 = _stratified_sample(records, [], "file_id", sample_size=500, seed=17)
    out2 = _stratified_sample(records, [], "file_id", sample_size=500, seed=17)
    assert [r["record_pk"] for r in out1] == [r["record_pk"] for r in out2]


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

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__discover.csv").open(encoding="utf-8", newline="") as f:
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

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__discover.csv").open(encoding="utf-8", newline="") as f:
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

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__discover.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert rows[0]["stratify_by"] == "file_id"
    assert int(rows[0]["records_sampled_domain"]) == 4


# ---------------------------------------------------------------------------
# discover_greedy: required fields must be preserved, not discovered from scratch
# (build_candidate_join_key_with_details makes every candidate score identically
# once cfg.gates.required_fields is set, so a from-empty greedy search stops
# after one arbitrary field -- see tools/join_key_discovery/greedy.py's comment)
# ---------------------------------------------------------------------------

def test_discover_greedy_seeds_selected_with_required_fields():
    records = [{"record_pk": "1", "sig_hash": "s1"}]
    items = [
        _items_row("1", "req1", "A"),
        _items_row("1", "req2", "B"),
        _items_row("1", "req3", "C"),
        _items_row("1", "extra1", "D"),
        _items_row("1", "extra2", "E"),
    ]
    idx = build_identity_index(items)
    cfg = {"max_k": 5, "gates": {"required_fields": ["req1", "req2", "req3"]}}
    result = discover_greedy(records, idx, ["req1", "req2", "req3", "extra1", "extra2"], cfg)
    assert set(result["selected_fields"]) == {"req1", "req2", "req3"}


def test_discover_greedy_without_required_fields_behaves_as_before():
    # No cfg.gates.required_fields -> starts from empty selected, same as pre-fix.
    records = [
        {"record_pk": "1", "sig_hash": "s1"},
        {"record_pk": "2", "sig_hash": "s2"},
    ]
    items = [_items_row("1", "field", "A"), _items_row("2", "field", "B")]
    idx = build_identity_index(items)
    cfg = {"max_k": 2, "gates": {}}
    result = discover_greedy(records, idx, ["field"], cfg)
    assert result["selected_fields"] == ["field"]


def test_discover_greedy_required_seed_still_scores_challengers():
    # Codex 4th-round finding: seeding `selected` with required_fields (the fix
    # above) is not sufficient on its own -- score_candidate still composes the
    # join key from gates.required_fields (ignoring whatever candidate is
    # actually under test) unless that key is stripped for the search's own
    # scoring calls. Without the fix, every req1-vs-req1+extra1 candidate would
    # score identically (both collapse to req1 alone), so the loop's tie-break
    # would stop before ever adding extra1 -- even though extra1 actually
    # resolves a real collision (two records share req1's value but differ on
    # extra1). With the fix, the candidate under test drives the score, so
    # discover_greedy must pick up extra1.
    records = [
        {"record_pk": "1", "sig_hash": "sigA"},
        {"record_pk": "2", "sig_hash": "sigB"},
    ]
    items = [
        _items_row("1", "req1", "X"),
        _items_row("2", "req1", "X"),
        _items_row("1", "extra1", "A"),
        _items_row("2", "extra1", "B"),
    ]
    idx = build_identity_index(items)
    cfg = {"max_k": 3, "gates": {"required_fields": ["req1"]}}
    result = discover_greedy(records, idx, ["req1", "extra1"], cfg)
    assert set(result["selected_fields"]) == {"req1", "extra1"}
    assert result["metrics"]["collision_rate"] == 0.0


def test_full_population_verify_uses_same_effective_gates_as_greedy_search(tmp_path: Path):
    # Codex 5th-round finding: discover_greedy() now strips gates.required_fields
    # from its OWN scoring calls once `selected` is required-inclusive (see the
    # test above), but the CLI's full-population verification call was still
    # passing the original cfg (gates.required_fields still set) -- so
    # score_candidate's full-population re-score would silently fall back to
    # scoring req1 alone, ignoring the challenger field greedy actually picked.
    # Here req1 alone collides (both records share the same value); the
    # challenger extra1 resolves it. Sample and full population are the SAME
    # data (no sampling truncation at this size), so if verification used
    # consistent gates, collision_rate and collision_rate_full MUST agree --
    # any mismatch here is unambiguous evidence of the gates inconsistency.
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "sigA"},
        {"file_id": "f1", "domain": "units", "record_pk": "2", "sig_hash": "sigB"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.req1", "item_value_type": "str", "item_value": "X"},
        {"domain": "units", "record_pk": "2", "item_key": "units.req1", "item_value_type": "str", "item_value": "X"},
        {"domain": "units", "record_pk": "1", "item_key": "units.extra1", "item_value_type": "str", "item_value": "A"},
        {"domain": "units", "record_pk": "2", "item_key": "units.extra1", "item_value_type": "str", "item_value": "B"},
    ])
    policy_json = tmp_path / "policy.json"
    policy_json.write_text('{"domains": {"units": {"required_items": ["units.req1"]}}}', encoding="utf-8")

    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--policy-json", str(policy_json),
        "--search-modes", "greedy", "--policy-modes", "harsh",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_harsh__units__harsh.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    row = rows[0]
    assert "units.extra1" in row["selected_fields"]
    assert row["collision_rate"] == "0.000000"
    assert row["collision_rate_full"] == "0.000000"
    assert row["sample_vs_full_diverges"] == "false"


def test_full_population_verify_stays_consistent_with_fixed_pareto_search(tmp_path: Path):
    # pareto_search() (tools/pareto_joinkey_search.py) now strips
    # gates.required_fields from its OWN scoring too -- guaranteeing required-field
    # inclusion structurally (every subset tried is required_fields + a combination
    # of the remaining fields) instead of relying on the override -- so it can
    # actually discover that a challenger field (here: extra1) resolves a collision
    # the required baseline (req1) alone could not. Verification's unconditional
    # gate-stripping (same fix as for greedy) now naturally stays consistent with
    # Pareto's own sample scoring too, since both compute the composite key the same
    # way. This requires pandas (pareto_search()'s module-level dependency); skipped
    # when unavailable, same as tests/test_pareto_shape_gating.py.
    pytest.importorskip("pandas")
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "sigA"},
        {"file_id": "f1", "domain": "units", "record_pk": "2", "sig_hash": "sigB"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.req1", "item_value_type": "str", "item_value": "X"},
        {"domain": "units", "record_pk": "2", "item_key": "units.req1", "item_value_type": "str", "item_value": "X"},
        {"domain": "units", "record_pk": "1", "item_key": "units.extra1", "item_value_type": "str", "item_value": "A"},
        {"domain": "units", "record_pk": "2", "item_key": "units.extra1", "item_value_type": "str", "item_value": "B"},
    ])
    policy_json = tmp_path / "policy.json"
    policy_json.write_text('{"domains": {"units": {"required_items": ["units.req1"]}}}', encoding="utf-8")

    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--policy-json", str(policy_json),
        "--search-modes", "pareto", "--policy-modes", "harsh",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_harsh__units__harsh.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    row = rows[0]
    assert "units.extra1" in row["selected_fields"]
    assert row["collision_rate"] == "0.000000"
    # Sample and full population are the same data at this size -- whatever the
    # sample metrics say, the full re-score must agree, not diverge.
    assert row["collision_rate"] == row["collision_rate_full"]
    assert row["sample_vs_full_diverges"] == "false"


def test_out_policy_excludes_candidate_that_diverges_on_full_population(tmp_path: Path):
    # Codex 5th-round finding: --out-policy previously accepted any status=="ok"
    # row regardless of sample_vs_full_diverges, so a candidate the full-population
    # verification pass explicitly warns not to pin (printed as a [discover]
    # WARNING) would still get written out as the domain's required policy. Here
    # a sample of 1 record misses a genuine fragmentation (two records share a
    # sig_hash but the candidate field's values differ) that only the full
    # population reveals -- status is "ok" but sample_vs_full_diverges is "true",
    # so the domain must be excluded from --out-policy entirely (no non-diverging
    # fallback exists in this fixture).
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "sigA"},
        {"file_id": "f1", "domain": "units", "record_pk": "2", "sig_hash": "sigA"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "A"},
        {"domain": "units", "record_pk": "2", "item_key": "units.spec", "item_value_type": "str", "item_value": "B"},
    ])
    out_policy = tmp_path / "out_policy.json"

    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units", "--out-policy", str(out_policy),
        "--search-modes", "greedy", "--policy-modes", "discover",
        "--sample-size", "1", "--sample-seed", "17",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__discover.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    row = rows[0]
    assert row["status"] == "ok"
    assert row["sample_vs_full_diverges"] == "true"

    out = json.loads(out_policy.read_text(encoding="utf-8"))
    assert "units" not in out.get("domains", {})


# ---------------------------------------------------------------------------
# blocked_missing_required must still fire for a required field genuinely
# absent from the data, even though discover_greedy now always echoes the
# required baseline by name in selected_fields (the old detection relied on
# selected_fields never containing an unpopulated field name; that's no
# longer true after the seeding fix above, so req_missing_from_data is a
# direct, population-based check independent of search results)
# ---------------------------------------------------------------------------

def test_validate_mode_blocked_when_required_field_absent_from_data(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    policy_json = tmp_path / "policy.json"
    policy_json.write_text(
        '{"domains": {"units": {"required_items": ["units.spec", "units.never_populated"]}}}',
        encoding="utf-8",
    )
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--policy-json", str(policy_json),
        "--search-modes", "greedy", "--policy-modes", "validate", "--no-full-verify",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__validate.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    row = rows[0]
    assert row["status"] == "blocked_missing_required"
    assert "units.never_populated" in row["reason"]


def test_validate_mode_not_blocked_when_required_field_ranked_below_candidate_cap(tmp_path: Path):
    # Codex 4th-round finding: req_missing_from_data must be checked against the
    # FULL domain item set, not candidate_fields (_pick_candidate_fields' output
    # over the sampled item set, capped at --max-candidate-fields). A required
    # field that's genuinely populated in the data but ranked below the cap (here:
    # tied for last by frequency, alphabetically after the other fields) must NOT
    # be reported as "absent from the data" just because it didn't make the
    # top---max-candidate-fields cut.
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    items = [
        {"domain": "units", "record_pk": "1", "item_key": "units.a", "item_value_type": "str", "item_value": "1"},
        {"domain": "units", "record_pk": "1", "item_key": "units.b", "item_value_type": "str", "item_value": "1"},
        {"domain": "units", "record_pk": "1", "item_key": "units.c", "item_value_type": "str", "item_value": "1"},
        # The required field: alphabetically last, so with --max-candidate-fields 1
        # (which keeps only the single top-ranked field), it's the one trimmed away
        # from candidate_fields -- but it IS populated in the data.
        {"domain": "units", "record_pk": "1", "item_key": "units.required_but_low_ranked", "item_value_type": "str", "item_value": "1"},
    ]
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], items)
    policy_json = tmp_path / "policy.json"
    policy_json.write_text(
        '{"domains": {"units": {"required_items": ["units.required_but_low_ranked"]}}}',
        encoding="utf-8",
    )
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--policy-json", str(policy_json), "--max-candidate-fields", "1",
        "--search-modes", "greedy", "--policy-modes", "validate", "--no-full-verify",
    ], cwd=Path(__file__).resolve().parents[1], check=True)

    with (phase0.parent / "diagnostics" / "join_key_discovery_exploration__units__validate.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    row = rows[0]
    assert row["status"] != "blocked_missing_required", row["reason"]


# ---------------------------------------------------------------------------
# _stratified_sample: known groups too small to fill sample_size must top up
# from the ungrouped remainder instead of returning short
# (Codex third-round Finding B: 1 known record + 9999 blank-file_id records,
# sample_size=500 previously returned only 251)
# ---------------------------------------------------------------------------

def test_stratified_sample_tops_up_from_ungrouped_remainder_when_groups_are_small():
    records = [{"record_pk": "known0", "file_id": "onlyfile"}]
    records += [{"record_pk": f"blank{i}", "file_id": ""} for i in range(9999)]
    out = _stratified_sample(records, [], "file_id", sample_size=500, seed=17)
    assert len(out) == 500


def test_rank_all_is_deterministic_full_sort():
    records = [{"record_pk": f"r{i}"} for i in range(50)]
    ranked1 = _rank_all(records, seed=17)
    ranked2 = _rank_all(records, seed=17)
    assert [r["record_pk"] for r in ranked1] == [r["record_pk"] for r in ranked2]
    assert len(ranked1) == len(records)
    assert {r["record_pk"] for r in ranked1} == {r["record_pk"] for r in records}


# ---------------------------------------------------------------------------
# Diagnostics filename suffixing: sequential --emit-commands invocations
# scoped to different --domains must not clobber each other's CSVs
# (Codex third-round Finding C)
# ---------------------------------------------------------------------------

def test_diagnostics_domain_suffix_empty_when_unscoped():
    assert _diagnostics_domain_suffix(set()) == ""


def test_diagnostics_domain_suffix_short_domain_list():
    assert _diagnostics_domain_suffix({"units"}) == "__units"
    assert _diagnostics_domain_suffix({"b_domain", "a_domain"}) == "__a_domain_b_domain"


def test_diagnostics_domain_suffix_falls_back_to_hash_for_long_lists():
    many = {f"domain_name_{i}" for i in range(20)}
    suffix = _diagnostics_domain_suffix(many)
    assert suffix.startswith("__20domains_")
    assert len(suffix) < 40


def test_diagnostics_domain_suffix_includes_policy_modes_to_avoid_split_run_collisions():
    # Codex 5th-round finding: tools/suggest_discovery_params.py's --emit-commands
    # splits one domain into a discover-only command and a validate,harsh command,
    # both scoped to the SAME --domains -- domain alone isn't enough to keep their
    # diagnostics apart, since the second run's _write_csv would silently overwrite
    # the first run's aggregate exploration file.
    assert _diagnostics_domain_suffix({"units"}, ["discover"]) == "__units__discover"
    assert _diagnostics_domain_suffix({"units"}, ["validate", "harsh"]) == "__units__harsh_validate"
    assert _diagnostics_domain_suffix({"units"}, ["discover"]) != _diagnostics_domain_suffix({"units"}, ["validate", "harsh"])
    # No policy_modes given -> unchanged (backward compatible default).
    assert _diagnostics_domain_suffix({"units"}) == "__units"


def test_discover_join_policy_scoped_run_does_not_clobber_unscoped_filenames(tmp_path: Path):
    phase0 = tmp_path / "results" / "records"
    _write_csv(phase0 / "records.csv", ["file_id", "domain", "record_pk", "sig_hash"], [
        {"file_id": "f1", "domain": "units", "record_pk": "1", "sig_hash": "s1"},
    ])
    _write_csv(phase0 / "identity_items.csv", ["domain", "record_pk", "item_key", "item_value_type", "item_value"], [
        {"domain": "units", "record_pk": "1", "item_key": "units.spec", "item_value_type": "str", "item_value": "s"},
    ])
    diagnostics_dir = phase0.parent / "diagnostics"

    # First run: unscoped (no --domains) -> unsuffixed filename.
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0),
        "--search-modes", "greedy", "--policy-modes", "discover", "--no-full-verify",
    ], cwd=Path(__file__).resolve().parents[1], check=True)
    assert (diagnostics_dir / "join_key_discovery_exploration.csv").exists()

    # Second run: scoped to --domains units -> suffixed filename, does not
    # touch (or need to match) the unscoped file written above.
    subprocess.run([
        sys.executable, "tools/discover_join_policy.py",
        "--phase0-dir", str(phase0), "--domains", "units",
        "--search-modes", "greedy", "--policy-modes", "discover", "--no-full-verify",
    ], cwd=Path(__file__).resolve().parents[1], check=True)
    assert (diagnostics_dir / "join_key_discovery_exploration__units__discover.csv").exists()
    assert (diagnostics_dir / "join_key_discovery_exploration.csv").exists()
